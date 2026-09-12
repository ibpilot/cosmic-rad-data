#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Importador histórico GOES/SEISS desde NOAA/NCEI (sgps-l2-avg5m).

Convierte los NetCDF diarios de NCEI en JSON compacto e inmutable bajo `ncei/`,
con procedencia (SHA-256 del origen), cobertura explícita, selección determinista
de sensor y un manifiesto reproducible. El producto publicado es hermano de
`solar/` (capturas operativas de SWPC) y nunca lo toca.

Frontera HDF5: la única función que importa `h5py`/`numpy` es `read_netcdf`, y lo
hace dentro del cuerpo. Devuelve un "documento crudo" de listas y valores Python
puros; todo el resto del módulo (normalización, selección de sensor, manifiesto)
es stdlib puro sobre ese dict, de modo que los tests son herméticos y no tocan la
red ni HDF5.

Uso:
    python3 backfill_ncei.py DATA_ROOT --from YYYY-MM-DD --to YYYY-MM-DD \
        [--satellites g18,g19] [--dry-run] [--resume]
"""

import argparse
import datetime
import hashlib
import json
import os
import re
import sys
import tempfile
import time
import urllib.request

SCHEMA_VERSION = 1
BACKFILL_VERSION = "1.0.0"
PRODUCT = "sgps-l2-avg5m"
NCEI_BASE = ("https://data.ngdc.noaa.gov/platforms/"
             "solar-space-observing-satellites/goes")
NCEI_DIR = {"g16": "goes16", "g18": "goes18", "g19": "goes19"}
NCEI_ROOT = "ncei"
CADENCE_S = 300
SLOTS_PER_DAY = 288
SLOTS_FOR_COMPLETE = 274
FILL_LIMIT = -1e30
# Precision de publicacion: 7 cifras SIGNIFICATIVAS, no decimales fijos.
# Un redondeo a 6 decimales aniquila los canales duros (P9/P10 viven en 1e-7..1e-8,
# que redondean a 0.0) y con ellos el disparador del detector SEP. float32 resuelve
# ~7.2 cifras significativas: publicar mas digitos seria inventar precision.
SIGNIFICANT_DIGITS = 7
SENSOR_RULE_VERSION = 1
J2000_EPOCH_S = 946728000
CH_NAMES = ['P1', 'P2A', 'P2B', 'P3', 'P4', 'P5', 'P6', 'P7',
            'P8A', 'P8B', 'P8C', 'P9', 'P10']
HARD_CHANNELS = ('P8A', 'P8B', 'P8C', 'P9', 'P10')

DEFAULT_SATELLITES = ("g18", "g19")
RETRY_ATTEMPTS = 3
RETRY_DELAY_S = 1.0
# El catch-up automatico pide hasta el ultimo dia que NCEI ya deberia haber
# publicado. La latencia medida es ~2 dias (2026-09-13: NCEI publicaba G18/G19
# hasta el 09-11). Pedir "hoy" seria pedir un dia inexistente y dejaria un hueco
# en el manifiesto; el dia que aun no esta se reintenta en la pasada siguiente.
NCEI_LATENCY_DAYS = 2
# Tope de dias por pasada. El workflow corre cada 6 h, asi que en regimen normal
# son 0-1 dias; el tope solo acota una primera pasada o una caida larga de NCEI.
# Con backlog mayor que el tope se recorta `end` (no se mueve `start`): la pasada
# cubre el primer bloque cronologico y la siguiente continua en el dia inmediato
# posterior, sin saltar dias (idempotente y resumible).
CATCHUP_MAX_DAYS = 30

TIME_VARS = ("time", "L2_SciData_TimeStamp")
YAW_VARS = ("yaw_flip_flag", "YawFlipFlag")
DIFF_VARS = ("AvgDiffProtonFlux",)
INT_VARS = ("AvgIntProtonFlux",)
LOWER_VARS = ("DiffProtonLowerEnergy",)
UPPER_VARS = ("DiffProtonUpperEnergy",)
DIFF_VALID_VARS = ("DiffValidL1bSamplesInAvg",)
INT_VALID_VARS = ("IntValidL1bSamplesInAvg",)

_UTC = datetime.timezone.utc
_HREF_RE = re.compile(r'href\s*=\s*"([^"]+)"', re.IGNORECASE)
_ARTIFACT_RE = re.compile(r'^sci_sgps-l2-avg5m_.+\.nc$')
_NAME_RE = re.compile(
    r'^sci_sgps-l2-avg5m_(?P<sat>[a-z0-9]+)_d(?P<day>\d{8})_'
    r'(?P<ver>v\d+-\d+-\d+)\.nc$')


class SchemaError(Exception):
    """El documento crudo no cumple el contrato conocido: fallo cerrado."""


class HashConflict(Exception):
    """El artefacto de destino existe con otro contenido: no se sobrescribe."""


class ImportReport:
    """Resultado de una pasada de `import_range`."""

    def __init__(self):
        self.downloaded = 0
        self.already_present = 0
        self.partial = 0
        self.missing = 0
        self.error = 0
        self.hard_error = 0
        self.bytes_downloaded = 0
        self.files_created = 0
        self.errors = []
        self.days_requested = 0
        self.days_complete = 0
        self.days_partial = 0
        self.days_missing = 0


# ---------------------------------------------------------------------------
# Utilidades de tiempo, bytes y JSON
# ---------------------------------------------------------------------------

def _as_utc(value):
    if value.tzinfo is None:
        return value.replace(tzinfo=_UTC)
    return value.astimezone(_UTC)


def fmt_iso(value):
    return _as_utc(value).strftime("%Y-%m-%dT%H:%M:%SZ")


def _fmt_now(now):
    if now is None:
        return fmt_iso(datetime.datetime.now(_UTC))
    if isinstance(now, str):
        s = now.strip()
        if s[-1:] in ("Z", "z"):
            s = s[:-1] + "+00:00"
        try:
            value = datetime.datetime.fromisoformat(s)
        except ValueError:
            raise ValueError("instante `now` invalido: %r" % (now,))
        return fmt_iso(value)
    if isinstance(now, datetime.date) and not isinstance(now, datetime.datetime):
        now = datetime.datetime(now.year, now.month, now.day, tzinfo=_UTC)
    return fmt_iso(now)


def _iso_z(value):
    s = value.decode("utf-8", "replace") if isinstance(value, bytes) else str(value)
    s = s.strip()
    if s[-1:] in ("Z", "z"):
        s = s[:-1] + "+00:00"
    try:
        value = datetime.datetime.fromisoformat(s)
    except ValueError:
        raise SchemaError("instante ISO invalido: %r" % (value,))
    return fmt_iso(value)


def sha256_bytes(data):
    """SHA-256 en streaming de un bloque de bytes."""
    digest = hashlib.sha256()
    view = memoryview(data)
    for offset in range(0, len(view), 65536):
        digest.update(view[offset:offset + 65536])
    return digest.hexdigest()


def canonical_bytes(obj):
    return (json.dumps(obj, sort_keys=True, separators=(',', ':'),
                       ensure_ascii=False) + "\n").encode("utf-8")


def read_json(path, default=None):
    try:
        with open(path, "r", encoding="utf-8") as fh:
            return json.load(fh)
    except (OSError, ValueError):
        return default


def _as_bytes(data):
    if isinstance(data, bytes):
        return data
    if isinstance(data, str):
        return data.encode("utf-8")
    return bytes(data)


def _as_text(data):
    if isinstance(data, bytes):
        return data.decode("utf-8", "replace")
    return data if isinstance(data, str) else str(data)


def _attr_str(value):
    if value is None:
        return None
    if isinstance(value, bytes):
        return value.decode("utf-8", "replace")
    if isinstance(value, str):
        return value
    return str(value)


def _write_bytes_atomic(path, payload):
    directory = os.path.dirname(path)
    if directory:
        os.makedirs(directory, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=".tmp-", dir=directory or ".")
    try:
        with os.fdopen(fd, "wb") as fh:
            fh.write(payload)
        os.replace(tmp, path)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


# ---------------------------------------------------------------------------
# Limpieza y métricas puras
# ---------------------------------------------------------------------------

def clean_value(v):
    """_FillValue, no finitos y negativos -> None. 7 cifras significativas."""
    if v is None:
        return None
    try:
        x = float(v)
    except (TypeError, ValueError):
        return None
    if x != x or x in (float('inf'), float('-inf')):   # NaN o infinito
        return None
    # Redundante con el descarte de negativos de abajo (_FillValue es -1e31):
    # se conserva explicito para que un producto con relleno POSITIVO grande
    # siga fallando cerrado si algun dia aparece.
    if x < FILL_LIMIT:
        return None
    if x < 0:
        return None
    return float("%.*g" % (SIGNIFICANT_DIGITS, x))


def _count(v):
    if v is None:
        return None
    try:
        n = int(v)
    except (TypeError, ValueError):
        return None
    return n if n >= 0 else None


def sensor_metrics(raw, sensor):
    """Metricas de cobertura de un sensor. Puro, sin efectos.

    `joint_valid_slots` exige los 13 diferenciales Y P11 (solo sirve para elegir
    sensor). Es distinta de `coverage.valid_diff_slots` del artefacto, que cuenta
    los 13 diferenciales sin mirar P11 y es la que consume el manifiesto.
    """
    full = 0        # franjas con los 13 diferenciales validos Y P11 valido
    hard = 0        # franjas con P8A..P10 validos Y P11 valido
    l1b = 0         # suma de muestras L1b validas (diferencial + integral)
    for t in range(len(raw["time"])):
        row = [clean_value(x) for x in raw["diff"][t][sensor]]
        p11 = clean_value(raw["integral"][t][sensor])
        if p11 is not None:
            if all(v is not None for v in row):
                full += 1
            if all(row[CH_NAMES.index(c)] is not None for c in HARD_CHANNELS):
                hard += 1
        for n in raw["diff_valid"][t][sensor]:
            l1b += int(n) if n is not None and n >= 0 else 0
        n_int = raw["int_valid"][t][sensor]
        l1b += int(n_int) if n_int is not None and n_int >= 0 else 0
    return {"sensor": sensor, "joint_valid_slots": full,
            "valid_hard_slots": hard, "l1b_samples": l1b}


def select_sensor(raw):
    """Devuelve (sensor_elegido, [metricas de todos los sensores]).

    Reglas, en orden: mas franjas completas conjuntas (diferenciales + P11);
    mas franjas P8A..P10+P11; mas muestras L1b validas; indice de sensor menor.
    """
    cands = [sensor_metrics(raw, s) for s in range(raw["n_sensors"])]
    best = min(cands, key=lambda m: (-m["joint_valid_slots"],
                                     -m["valid_hard_slots"],
                                     -m["l1b_samples"],
                                     m["sensor"]))
    return best["sensor"], cands


def start_time_of(raw):
    """Instante UTC de la primera franja, en ISO Z.

    Se deriva de la variable temporal (epoca J2000), NO de una epoca fija Unix
    ni del nombre del fichero. `time_coverage_start` solo se usa para validar.
    """
    if not raw["time"]:
        raise SchemaError("sin eje temporal")
    unix = int(raw["time"][0]) + J2000_EPOCH_S
    dt = datetime.datetime.fromtimestamp(unix, tz=datetime.timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


# ---------------------------------------------------------------------------
# Normalizador
# ---------------------------------------------------------------------------

def normalize_day(raw, source_url, source_sha256, source_last_modified):
    """Documento crudo -> artefacto diario normalizado (puro y determinista)."""
    times = raw.get("time") or []
    if len(times) != SLOTS_PER_DAY:
        raise SchemaError("n_steps != %d" % SLOTS_PER_DAY)
    n_sensors = raw.get("n_sensors")
    if not isinstance(n_sensors, int) or n_sensors < 1:
        raise SchemaError("n_sensors invalido")
    diff = raw.get("diff") or []
    integral = raw.get("integral") or []
    lower = raw.get("lower_energy") or []
    upper = raw.get("upper_energy") or []
    if len(diff) != SLOTS_PER_DAY or len(integral) != SLOTS_PER_DAY:
        raise SchemaError("longitud temporal de diff/integral invalida")
    for t in range(SLOTS_PER_DAY):
        if len(diff[t]) != n_sensors or len(integral[t]) != n_sensors:
            raise SchemaError("n_sensors inconsistente")
        for s in range(n_sensors):
            if len(diff[t][s]) != len(CH_NAMES):
                raise SchemaError("longitud de canal != %d" % len(CH_NAMES))
    for table in (lower, upper):
        if len(table) != n_sensors:
            raise SchemaError("tabla de energia sin un canal por sensor")
        for row in table:
            if len(row) != len(CH_NAMES):
                raise SchemaError("longitud de canal != %d" % len(CH_NAMES))
    for i in range(1, SLOTS_PER_DAY):
        step = float(times[i]) - float(times[i - 1])
        if abs(step - CADENCE_S) > 1e-6:
            raise SchemaError("paso temporal != %d s" % CADENCE_S)

    sensor, candidates = select_sensor(raw)
    for c in range(len(CH_NAMES)):
        lo = float(lower[sensor][c])
        hi = float(upper[sensor][c])
        if not (lo < hi):
            raise SchemaError("energia no creciente en %s" % CH_NAMES[c])

    start = start_time_of(raw)
    tcs = raw.get("time_coverage_start")
    if tcs is not None and tcs != "" and _iso_z(tcs) != start:
        raise SchemaError("time_coverage_start discrepa del eje temporal")

    clean_diff = [[clean_value(v) for v in diff[t][sensor]]
                  for t in range(SLOTS_PER_DAY)]
    p11 = [clean_value(integral[t][sensor]) for t in range(SLOTS_PER_DAY)]
    samples = [[_count(v) for v in raw["diff_valid"][t][sensor]]
               for t in range(SLOTS_PER_DAY)]
    int_samples = [_count(raw["int_valid"][t][sensor])
                   for t in range(SLOTS_PER_DAY)]
    yaw = [_count(raw["yaw_flip"][t]) for t in range(SLOTS_PER_DAY)]

    missing_diff = [t for t in range(SLOTS_PER_DAY)
                    if any(v is None for v in clean_diff[t])]
    missing_p11 = [t for t in range(SLOTS_PER_DAY) if p11[t] is None]

    source_file = raw.get("file_id")
    version = version_of(source_file) if isinstance(source_file, str) else None
    if version is None:
        raise SchemaError("version de producto no reconocible en %r"
                          % (source_file,))

    channels = [{"name": CH_NAMES[c],
                 "lo_keV": float(lower[sensor][c]),
                 "hi_keV": float(upper[sensor][c])}
                for c in range(len(CH_NAMES))]

    return {
        "schema_version": SCHEMA_VERSION,
        "product": PRODUCT,
        "sat": raw.get("platform"),
        "sensor": sensor,
        "source_url": source_url,
        "source_file": source_file,
        "source_sha256": source_sha256,
        "source_last_modified": source_last_modified,
        "source_version": version,
        "day": start[:10],
        "n_steps": SLOTS_PER_DAY,
        "start_time": start,
        "time_step_s": CADENCE_S,
        "channels": channels,
        "integral_500_mev": p11,
        "diff": clean_diff,
        "samples_in_avg": samples,
        "int_samples_in_avg": int_samples,
        "yaw_flip": yaw,
        "coverage": {
            # Contrato publicado: franjas con los 13 diferenciales validos, SIN
            # mirar P11 (distinta de sensor_metrics.joint_valid_slots).
            "valid_diff_slots": SLOTS_PER_DAY - len(missing_diff),
            "valid_p11_slots": SLOTS_PER_DAY - len(missing_p11),
            "missing_diff_slots": missing_diff,
            "missing_p11_slots": missing_p11,
        },
        "sensor_selection": {
            "rule_version": SENSOR_RULE_VERSION,
            "chosen": sensor,
            "candidates": candidates,
        },
    }


# ---------------------------------------------------------------------------
# Lectura NetCDF (única frontera HDF5)
# ---------------------------------------------------------------------------

def resolve_var(container, candidates):
    """Primer nombre disponible del contenedor; SchemaError si no hay ninguno."""
    for name in candidates:
        if name in container:
            return name
    raise SchemaError("falta variable: %s" % " o ".join(candidates))


def _values(node):
    data = node[()]
    if hasattr(data, "tolist"):
        data = data.tolist()
    return data


def _energy_table(values, n_sensors):
    if not values:
        return []
    if isinstance(values[0], (list, tuple)):
        return [[float(v) for v in row] for row in values]
    row = [float(v) for v in values]
    return [list(row) for _ in range(max(n_sensors, 1))]


def read_source(fh, name=None):
    """Documento crudo a partir de un contenedor tipo HDF5 (mapeo + attrs)."""
    attrs = fh.attrs
    time = [float(x) for x in _values(fh[resolve_var(fh, TIME_VARS)])]
    yaw = [int(x) for x in _values(fh[resolve_var(fh, YAW_VARS)])]
    diff = _values(fh[resolve_var(fh, DIFF_VARS)])
    integral = _values(fh[resolve_var(fh, INT_VARS)])
    lower = _values(fh[resolve_var(fh, LOWER_VARS)])
    upper = _values(fh[resolve_var(fh, UPPER_VARS)])
    diff_valid = _values(fh[resolve_var(fh, DIFF_VALID_VARS)])
    int_valid = _values(fh[resolve_var(fh, INT_VALID_VARS)])
    if integral and isinstance(integral[0], (list, tuple)):
        n_sensors = len(integral[0])
    elif diff:
        n_sensors = len(diff[0])
    else:
        n_sensors = 0
    return {
        "file_id": _attr_str(attrs.get("id")) or name,
        "platform": _attr_str(attrs.get("platform")),
        "time_coverage_start": _attr_str(attrs.get("time_coverage_start")),
        "time": time,
        "diff": [[[float(v) for v in row] for row in frame] for frame in diff],
        "integral": [[float(v) for v in frame] for frame in integral],
        "lower_energy": _energy_table(lower, n_sensors),
        "upper_energy": _energy_table(upper, n_sensors),
        "diff_valid": [[[int(v) for v in row] for row in frame]
                       for frame in diff_valid],
        "int_valid": [[int(v) for v in frame] for frame in int_valid],
        "yaw_flip": yaw,
        "n_sensors": n_sensors,
    }


def read_netcdf(path):
    """Lee un NetCDF SGPS y devuelve el documento crudo. Único punto con h5py."""
    import h5py
    with h5py.File(path, "r") as fh:
        return read_source(fh, name=os.path.basename(path))


# ---------------------------------------------------------------------------
# Descubrimiento y nombres
# ---------------------------------------------------------------------------

def listing_url(sat, year, month):
    return "%s/%s/l2/data/%s/%04d/%02d/" % (
        NCEI_BASE, NCEI_DIR[sat], PRODUCT, year, month)


def data_url(sat, day, name):
    return "%s/%s/l2/data/%s/%s/%s/%s" % (
        NCEI_BASE, NCEI_DIR[sat], PRODUCT, day[:4], day[5:7], name)


def parse_listing(html):
    """Nombres `sci_sgps-l2-avg5m_*.nc` únicos de un listing Apache, en orden."""
    text = _as_text(html)
    out = []
    seen = set()
    for match in _HREF_RE.finditer(text):
        href = match.group(1)
        name = href.split("?", 1)[0].split("#", 1)[0]
        name = name.rstrip("/").rsplit("/", 1)[-1]
        if not _ARTIFACT_RE.match(name) or name in seen:
            continue
        seen.add(name)
        out.append(name)
    return out


def version_of(name):
    if not isinstance(name, str):
        return None
    match = _NAME_RE.match(name)
    return match.group("ver") if match else None


def sat_of(name):
    if not isinstance(name, str):
        return None
    match = _NAME_RE.match(name)
    return match.group("sat") if match else None


def day_of(name):
    if not isinstance(name, str):
        return None
    match = _NAME_RE.match(name)
    if not match:
        return None
    raw = match.group("day")
    try:
        return datetime.date(int(raw[:4]), int(raw[4:6]), int(raw[6:]))
    except ValueError:
        return None


def _day_of_str(name):
    day = day_of(name)
    return day.isoformat() if day is not None else None


def _version_tuple(version):
    return tuple(int(part) for part in version[1:].split("-"))


def pick_version(names):
    """Nombre con la versión más alta (comparación numérica de vA-B-C)."""
    best = None
    best_key = None
    for name in names:
        version = version_of(name)
        if version is None:
            continue
        key = _version_tuple(version)
        if best_key is None or key > best_key:
            best, best_key = name, key
    return best


def artifact_path(root, sat, day, version):
    return os.path.join(
        root, NCEI_ROOT, "sgps", sat, "%04d" % int(day[:4]),
        "%02d" % int(day[5:7]),
        "sci_sgps-l2-avg5m_%s_d%s_%s.json"
        % (sat, day.replace("-", ""), version))


def day_range(start_day, end_day):
    start = datetime.date.fromisoformat(start_day)
    end = datetime.date.fromisoformat(end_day)
    out = []
    day = start
    while day <= end:
        out.append(day.isoformat())
        day += datetime.timedelta(days=1)
    return out


def month_iter(start_day, end_day):
    start = datetime.date.fromisoformat(start_day).replace(day=1)
    end = datetime.date.fromisoformat(end_day).replace(day=1)
    out = []
    year, month = start.year, start.month
    while (year, month) <= (end.year, end.month):
        out.append((year, month))
        month += 1
        if month > 12:
            month = 1
            year += 1
    return out


# ---------------------------------------------------------------------------
# Descarga y escritura
# ---------------------------------------------------------------------------

def http_get(url, timeout=60):
    with urllib.request.urlopen(url, timeout=timeout) as resp:
        return resp.read()


def download(url, fetch, sleep, attempts=RETRY_ATTEMPTS):
    """Descarga con reintentos acotados; `sleep` inyectable (no-op en tests)."""
    last = None
    for attempt in range(attempts):
        try:
            return fetch(url)
        except Exception as exc:
            last = exc
            if attempt + 1 < attempts:
                sleep(RETRY_DELAY_S)
    raise last


def write_artifact(path, payload):
    """Escritura exclusiva y atómica. No sobrescribe: HashConflict si difiere."""
    if os.path.exists(path):
        with open(path, "rb") as fh:
            existing = fh.read()
        if sha256_bytes(existing) == sha256_bytes(payload):
            return "already_present"
        raise HashConflict(path)
    _write_bytes_atomic(path, payload)
    return "created"


def _read_download(data, read):
    fd, tmp = tempfile.mkstemp(suffix=".nc")
    try:
        with os.fdopen(fd, "wb") as fh:
            fh.write(data)
        return read(tmp)
    finally:
        try:
            os.unlink(tmp)
        except OSError:
            pass


# ---------------------------------------------------------------------------
# Manifiesto
# ---------------------------------------------------------------------------

def _candidate_key(entry):
    return (-(entry.get("valid_diff_slots") or 0),
            -(entry.get("valid_p11_slots") or 0),
            -(entry.get("_l1b") or 0),
            str(entry.get("sat")))


def _day_status(candidates):
    for entry in candidates:
        if ((entry.get("valid_diff_slots") or 0) >= SLOTS_FOR_COMPLETE
                and (entry.get("valid_p11_slots") or 0) >= SLOTS_FOR_COMPLETE):
            return "complete"
    return "partial"


def scan_artifacts(root):
    """Artefactos presentes en el árbol: (day, sat, path, ruta_relativa, obj)."""
    base = os.path.join(root, NCEI_ROOT, "sgps")
    out = []
    if not os.path.isdir(base):
        return out
    for dirpath, dirs, files in os.walk(base):
        dirs.sort()
        for name in sorted(files):
            if not name.endswith(".json"):
                continue
            path = os.path.join(dirpath, name)
            obj = read_json(path)
            if not isinstance(obj, dict):
                continue
            day = obj.get("day")
            sat = obj.get("sat")
            if not isinstance(day, str) or not isinstance(sat, str):
                continue
            out.append((day, sat, path, os.path.relpath(path, root), obj))
    return out


def _manifest_unchanged(path, manifest):
    try:
        with open(path, "rb") as fh:
            old_bytes = fh.read()
    except OSError:
        return False
    try:
        old = json.loads(old_bytes.decode("utf-8"))
    except ValueError:
        return False
    if not isinstance(old, dict):
        return False
    a = dict(old)
    b = dict(manifest)
    a.pop("generated_at", None)
    b.pop("generated_at", None)
    return canonical_bytes(a) == canonical_bytes(b)


def build_manifest(root, existing, generated_at):
    """Manifiesto determinista a partir de los artefactos ya presentes.

    Si el manifiesto recalculado es idéntico al existente salvo `generated_at`,
    se conserva el fichero byte a byte y no se reescribe.
    """
    existing = existing if isinstance(existing, dict) else {}
    by_day = {}
    for day, sat, path, rel, obj in scan_artifacts(root):
        coverage = obj.get("coverage") or {}
        selection = obj.get("sensor_selection") or {}
        candidates = selection.get("candidates") or []
        chosen = selection.get("chosen")
        l1b = 0
        if (isinstance(chosen, int) and 0 <= chosen < len(candidates)
                and isinstance(candidates[chosen], dict)):
            l1b = candidates[chosen].get("l1b_samples") or 0
        try:
            with open(path, "rb") as fh:
                file_bytes = fh.read()
        except OSError:
            continue
        by_day.setdefault(day, []).append({
            "sat": sat,
            "path": rel.replace(os.sep, "/"),
            "source_version": obj.get("source_version"),
            "source_sha256": obj.get("source_sha256"),
            "artifact_sha256": sha256_bytes(file_bytes),
            "valid_diff_slots": coverage.get("valid_diff_slots"),
            "valid_p11_slots": coverage.get("valid_p11_slots"),
            "_l1b": l1b,
        })

    days = {}
    for day in sorted(by_day):
        ordered = sorted(by_day[day], key=_candidate_key)
        final = []
        for index, entry in enumerate(ordered):
            entry.pop("_l1b", None)
            entry["recommended"] = (index == 0)
            final.append(entry)
        days[day] = {"status": _day_status(final), "candidates": final}

    rng = existing.get("range")
    first = rng.get("from") if isinstance(rng, dict) else None
    last = rng.get("to") if isinstance(rng, dict) else None
    if not first or not last:
        if by_day:
            first, last = min(by_day), max(by_day)
        else:
            first = last = None
    missing = []
    if first and last:
        missing = [d for d in day_range(first, last) if d not in days]

    manifest = {
        "schema_version": SCHEMA_VERSION,
        "backfill_version": BACKFILL_VERSION,
        "generated_at": generated_at,
        "product": PRODUCT,
        "cadence_seconds": CADENCE_S,
        "range": {"from": first, "to": last},
        "days": days,
        "missing_days": missing,
    }
    path = os.path.join(root, NCEI_ROOT, "manifest.json")
    if not _manifest_unchanged(path, manifest):
        _write_bytes_atomic(path, canonical_bytes(manifest))
    return manifest


# ---------------------------------------------------------------------------
# Orquestación
# ---------------------------------------------------------------------------

def _now_date(now):
    """Fecha UTC de `now` (None = ahora), reutilizando el parser de `_fmt_now`."""
    return datetime.datetime.strptime(_fmt_now(now), "%Y-%m-%dT%H:%M:%SZ").date()


def _valid_day(value):
    """`value` como dia ISO (YYYY-MM-DD) valido, o None."""
    if not isinstance(value, str):
        return None
    try:
        datetime.date.fromisoformat(value)
    except ValueError:
        return None
    return value


def _entry_satellites(entry):
    """Satelites con artefacto/candidato en una entrada de dia del manifiesto."""
    if not isinstance(entry, dict):
        return set()
    candidates = entry.get("candidates")
    sats = set()
    if isinstance(candidates, list):
        for candidate in candidates:
            if (isinstance(candidate, dict)
                    and isinstance(candidate.get("sat"), str)):
                sats.add(candidate["sat"])
    return sats


def presence_by_day(data_root, manifest=None):
    """{dia: set(satelites con artefacto/candidato)} de la evidencia disponible.

    Prefiere el manifiesto (barato y ya lista los candidatos por dia); si `days`
    no tiene entradas validas, deriva la presencia escaneando el arbol. Un dia
    ausente del mapa carece de artefacto para cualquier satelite.
    """
    if manifest is None:
        manifest = read_json(
            os.path.join(data_root, NCEI_ROOT, "manifest.json"), None)
    days = manifest.get("days") if isinstance(manifest, dict) else None
    if isinstance(days, dict):
        out = {}
        for day, entry in days.items():
            day = _valid_day(day)
            if day is not None:
                out[day] = _entry_satellites(entry)
        if out:
            return out
    out = {}
    for day, sat, _path, _rel, _obj in scan_artifacts(data_root):
        day = _valid_day(day)
        if day is not None and isinstance(sat, str):
            out.setdefault(day, set()).add(sat)
    return out


def _first_incomplete_day(presence, first, last, required):
    """Primer dia de [first, last] sin TODOS los satelites requeridos, o None."""
    day = first
    while day <= last:
        if not required <= presence.get(day.isoformat(), set()):
            return day
        day += datetime.timedelta(days=1)
    return None


def catchup_range(data_root, now, satellites=None,
                  latency_days=NCEI_LATENCY_DAYS, max_days=CATCHUP_MAX_DAYS):
    """Rango inclusivo (start_day, end_day) del catch-up, o None si ya al dia.

    `target_end` = hoy UTC - latencia: el ultimo dia razonablemente publicado por
    NCEI. `satellites` son los requeridos (por defecto, los del CLI). `start_day`
    es el PRIMER dia del historico con evidencia que no tiene artefacto/candidato
    para TODOS los requeridos: un ultimo dia con solo g18 (falta g19) se
    reintenta ese mismo dia, y un hueco intermedio no se salta aunque haya dias
    posteriores completos. Si no falta ningun dia, `start_day` = ultimo presente
    + 1. Sin manifiesto usable la presencia se deriva del arbol; sin evidencia
    real se cae al recurso legacy `range.to` o, en su defecto, a `max_days` hacia
    atras desde `target_end` para no intentar la historia entera de golpe. El
    tope se aplica SIEMPRE recortando `end_day` = min(target_end, start_day +
    max_days - 1), nunca moviendo `start_day`: con un backlog mayor que `max_days`
    la pasada devuelve el primer bloque cronologico y la siguiente continua en el
    dia inmediatamente posterior, sin saltar dias.
    """
    if latency_days < 0:
        raise ValueError("latency_days < 0")
    if max_days < 1:
        raise ValueError("max_days < 1")
    required = set(satellites) if satellites else set(DEFAULT_SATELLITES)
    target_end = _now_date(now) - datetime.timedelta(days=latency_days)
    manifest = read_json(
        os.path.join(data_root, NCEI_ROOT, "manifest.json"), None)
    presence = presence_by_day(data_root, manifest)
    if presence:
        # El historico empieza en el primer dia con evidencia: en un manifiesto
        # sano coincide con su `range.from`.
        first = min(presence)
        last = max(presence)
        start = _first_incomplete_day(
            presence, datetime.date.fromisoformat(first),
            datetime.date.fromisoformat(last), required)
        if start is None:
            start = (datetime.date.fromisoformat(last)
                     + datetime.timedelta(days=1))
    else:
        rng = manifest.get("range") if isinstance(manifest, dict) else None
        legacy_last = _valid_day(rng.get("to")) if isinstance(rng, dict) else None
        if legacy_last is not None:
            start = (datetime.date.fromisoformat(legacy_last)
                     + datetime.timedelta(days=1))
        else:
            start = target_end - datetime.timedelta(days=max_days - 1)
    if start > target_end:
        return None
    end = min(target_end, start + datetime.timedelta(days=max_days - 1))
    return start.isoformat(), end.isoformat()


def import_range(data_root, start_day, end_day, satellites, fetch,
                 now, read=None, sleep=None, dry_run=False, resume=False):
    """Importa el rango inclusivo. Fallos por satélite se registran y se sigue."""
    if start_day > end_day:
        raise ValueError("start_day > end_day")
    read = read or read_netcdf
    sleep = sleep or time.sleep
    generated_at = _fmt_now(now)
    report = ImportReport()
    days = day_range(start_day, end_day)
    report.days_requested = len(days)
    day_set = set(days)

    names_by = {}
    failed = set()
    for year, month in month_iter(start_day, end_day):
        month_days = [d for d in days if d.startswith("%04d-%02d" % (year, month))]
        for sat in satellites:
            url = listing_url(sat, year, month)
            try:
                body = download(url, fetch, sleep)
            except Exception as exc:
                for day in month_days:
                    failed.add((sat, day))
                    report.errors.append((sat, day, "listing: %s" % exc))
                    report.error += 1
                continue
            for name in parse_listing(body):
                day = _day_of_str(name)
                if day in day_set and sat_of(name) == sat:
                    names_by.setdefault((sat, day), []).append(name)

    for day in days:
        for sat in satellites:
            if (sat, day) in failed:
                continue
            names = names_by.get((sat, day))
            if not names:
                report.errors.append((sat, day, "sin fichero en el listing"))
                report.error += 1
                continue
            name = pick_version(names)
            version = version_of(name)
            path = artifact_path(data_root, sat, day, version)
            if resume and os.path.exists(path):
                report.already_present += 1
                continue
            url = data_url(sat, day, name)
            try:
                body = download(url, fetch, sleep)
            except Exception as exc:
                report.errors.append((sat, day, "fetch: %s" % exc))
                report.error += 1
                continue
            data = _as_bytes(body)
            report.bytes_downloaded += len(data)
            try:
                raw = _read_download(data, read)
                artifact = normalize_day(raw, url, sha256_bytes(data), None)
            except SchemaError as exc:
                report.errors.append((sat, day, "esquema: %s" % exc))
                report.error += 1
                report.hard_error += 1
                continue
            except Exception as exc:
                report.errors.append((sat, day, "lectura: %s" % exc))
                report.error += 1
                continue
            payload = canonical_bytes(artifact)

            if dry_run:
                if os.path.exists(path):
                    try:
                        with open(path, "rb") as fh:
                            existing_bytes = fh.read()
                    except OSError as exc:
                        report.errors.append((sat, day, "lectura destino: %s" % exc))
                        report.error += 1
                        continue
                    if sha256_bytes(existing_bytes) == sha256_bytes(payload):
                        report.already_present += 1
                    else:
                        report.errors.append((sat, day, "hash conflict"))
                        report.error += 1
                        report.hard_error += 1
                else:
                    report.downloaded += 1
                continue

            try:
                result = write_artifact(path, payload)
            except HashConflict:
                report.errors.append((sat, day, "hash conflict"))
                report.error += 1
                report.hard_error += 1
                continue
            if result == "created":
                report.downloaded += 1
                report.files_created += 1
            else:
                report.already_present += 1

    if not dry_run:
        previous = read_json(
            os.path.join(data_root, NCEI_ROOT, "manifest.json"), None)
        base = dict(previous) if isinstance(previous, dict) else {}
        # El rango publicado es MONOTONO: una pasada incremental no puede
        # encogerlo (perderia `missing_days` y la ventana que lee la app). Se
        # conserva el minimo `from` y el maximo `to` ya vistos.
        first, last = start_day, end_day
        rng = previous.get("range") if isinstance(previous, dict) else None
        if isinstance(rng, dict):
            if isinstance(rng.get("from"), str) and rng["from"] < first:
                first = rng["from"]
            if isinstance(rng.get("to"), str) and rng["to"] > last:
                last = rng["to"]
        base["range"] = {"from": first, "to": last}
        manifest = build_manifest(data_root, base, generated_at)
        for day in days:
            entry = manifest.get("days", {}).get(day)
            if entry is None:
                report.days_missing += 1
            elif entry.get("status") == "complete":
                report.days_complete += 1
            else:
                report.days_partial += 1
        report.partial = report.days_partial
        report.missing = report.days_missing

    return report


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _print_report(report, args, satellites):
    print("días pedidos: %d" % report.days_requested)
    print("completos: %d" % report.days_complete)
    print("parciales: %d" % report.days_partial)
    print("ausentes: %d" % report.days_missing)
    print("errores: %d" % report.error)
    print("descargados: %d" % report.downloaded)
    print("ya presentes: %d" % report.already_present)
    print("bytes descargados: %d" % report.bytes_downloaded)
    print("ficheros creados: %d" % report.files_created)
    print("satélites: %s" % ",".join(satellites))
    if args.dry_run:
        print("modo: dry-run (sin escrituras)")
    if args.resume or getattr(args, "catch_up", False):
        print("modo: resume")
    for sat, day, reason in report.errors:
        print("error: %s %s: %s" % (sat, day, reason), file=sys.stderr)


def main(argv, fetch=None, read=None, now=None):
    parser = argparse.ArgumentParser(prog="backfill_ncei.py")
    parser.add_argument("data_root")
    parser.add_argument("--from", dest="start_day")
    parser.add_argument("--to", dest="end_day")
    parser.add_argument("--catch-up", action="store_true",
                        help="rango automatico: desde el primer dia incompleto "
                             "(sin todos los satelites requeridos, o el "
                             "siguiente al ultimo importado si no hay huecos) "
                             "hasta el ultimo publicado por NCEI, recortado a "
                             "--max-days sin saltar dias")
    parser.add_argument("--latency-days", type=int, default=NCEI_LATENCY_DAYS)
    parser.add_argument("--max-days", type=int, default=CATCHUP_MAX_DAYS,
                        help="tope de dias por pasada; con backlog mayor se "
                             "recorta el final y la siguiente pasada continua")
    parser.add_argument("--satellites", default=",".join(DEFAULT_SATELLITES))
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--resume", action="store_true")
    try:
        args = parser.parse_args(argv)
    except SystemExit as exc:
        return 0 if exc.code in (0, None) else 1
    satellites = [s.strip() for s in args.satellites.split(",") if s.strip()]
    if not satellites or any(s not in NCEI_DIR for s in satellites):
        print("satélites inválidos: %s" % args.satellites, file=sys.stderr)
        return 1
    if args.catch_up:
        try:
            rng = catchup_range(args.data_root, now, satellites,
                                latency_days=args.latency_days,
                                max_days=args.max_days)
        except ValueError as exc:
            print("argumentos inválidos: %s" % exc, file=sys.stderr)
            return 1
        if rng is None:
            last_pub = _now_date(now) - datetime.timedelta(
                days=args.latency_days)
            print("catch-up: sin días nuevos (NCEI con latencia %d días "
                  "publica hasta el %s)" % (args.latency_days, last_pub))
            return 0
        args.start_day, args.end_day = rng
        print("catch-up: %s -> %s" % (args.start_day, args.end_day))
    if not args.start_day or not args.end_day:
        print("faltan --from/--to (o usa --catch-up)", file=sys.stderr)
        return 1
    # En catch-up se reanuda siempre: los dias ya presentes no se reescriben.
    resume = args.resume or args.catch_up
    try:
        report = import_range(
            args.data_root, args.start_day, args.end_day, satellites,
            fetch or http_get, now, read=read,
            dry_run=args.dry_run, resume=resume)
    except ValueError as exc:
        print("argumentos inválidos: %s" % exc, file=sys.stderr)
        return 1
    except Exception as exc:
        print("error: %s" % exc, file=sys.stderr)
        return 1
    _print_report(report, args, satellites)
    if report.hard_error:
        return 1
    if report.errors and not report.downloaded and not report.already_present:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
