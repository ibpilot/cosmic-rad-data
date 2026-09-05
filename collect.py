#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Recolector de protones solares integrales GOES (SWPC) para cosmic-rad-data.

Lee los feeds JSON de SWPC (primario con fallback a secundario) y archiva una
serie global de 5 minutos por dia UTC cerrado:

    solar/YYYY/MM/YYYY-MM-DD.json

Fuentes (Python 3 estandar, sin dependencias externas: urllib, json, hashlib):

- Feed de frescura:  `goes/primary/integral-protons-6-hour.json`, con fallback
  al equivalente `secondary`.
- Ventana de 7 dias: `goes/primary/integral-protons-7-day.json` fusionado con
  su `secondary`. SWPC rota a 7 dias; fusionar ambos satelites permite
  recuperar huecos dentro de la ventana en cada corrida.
- El satelite de cada muestra se lee del campo `satellite` de los propios
  registros del feed: no hay `instrument-sources.json` y nunca se fija
  GOES-18 ni GOES-19 en el codigo.

Reglas:

- Deduplicacion por timestamp: una (satelite, rejilla de 5 min) es una unica
  muestra, aunque el canal llegue duplicado o por varios pases del feed.
- Un dia cierra como COMPLETO al reunir >= 95 % de las 288 franjas (>= 274
  muestras, MIN_COVERAGE): NOAA deja huecos permanentes, asi que exigir 288/288
  dejaria el dia provisional para siempre. La cobertura real viaja en el
  fichero, asi que el consumidor puede ser mas estricto que el recolector sin
  volver a descargar.
- Mientras no alcance ese minimo y siga dentro de la ventana SWPC de 7 dias el
  dia queda PROVISIONAL y se reintenta en cada corrida; lo ya visto se
  persiste EN DISCO (manifest.pending_days) para poder cerrar el dia con datos
  reales cuando el feed deje de servirlos (a los 8 dias el dia ya no esta en
  el feed y sus muestras no se pueden volver a pedir).
- Al salir de la ventana sin llegar al 95 %, el dia se cierra como INCOMPLETO
  de forma permanente con lo acumulado en disco: se escribe su fichero
  igualmente, con `complete: false` y sus `samples`/`coverage` reales. Si no
  habia nada acumulado se cierra igualmente con `complete: false`,
  `sample_count: 0` y `coverage: 0.0`, y se registra en
  manifest.incomplete_days. Nunca se omite el fichero y nunca se escribe como
  si fuera un dia de flujo cero: un dia sin datos y un dia con flujo cero no
  son la misma cosa.
- Un dia cerrado (completo o incompleto) es INMUTABLE: el fichero se crea en
  exclusiva y nunca se reescribe ni se borra.
- Cada fichero diario lleva version de esquema, version del recolector,
  instante de adquisicion, `complete` (bool), `sample_count` (int: numero de
  muestras), `coverage` (float con 4 decimales), el array `samples` con las
  muestras (satelite y fuente por muestra), `satellites`/`sources` y el
  SHA-256 del array `samples`.
- `manifest.json` en la raiz publica ultimo exito/error, cobertura, dias
  incompletos, satelites vistos, cambios de satelite y version.

Uso:
    python3 collect.py [data_root]
"""

import datetime
import hashlib
import json
import math
import os
import sys
import tempfile
import urllib.error
import urllib.request

SCHEMA_VERSION = 1
COLLECTOR_VERSION = "1.0.0"

SOLAR_DIR = "solar"
MANIFEST_FILE = "manifest.json"

# Feeds SWPC. El papel ("primary"/"secondary") identifica la cadena de datos;
# el satelite de cada muestra viaja en el propio campo `satellite` del registro
# del feed. El feed "6h" da frescura; el "7d" cubre la ventana de retencion de
# SWPC y es el que permite cerrar dias y recuperar huecos.
SWPC_BASE = "https://services.swpc.noaa.gov/json/goes"
FEED_URLS = {
    ("6h", "primary"): SWPC_BASE + "/primary/integral-protons-6-hour.json",
    ("6h", "secondary"): SWPC_BASE + "/secondary/integral-protons-6-hour.json",
    ("7d", "primary"): SWPC_BASE + "/primary/integral-protons-7-day.json",
    ("7d", "secondary"): SWPC_BASE + "/secondary/integral-protons-7-day.json",
}

# Canales integrales que publica SWPC (pfu = protones cm-2 s-1 sr-1).
# Una fila del feed es {time_tag, satellite, flux, energy}: un canal por fila.
KNOWN_CHANNELS = (
    ">=1 MeV", ">=5 MeV", ">=10 MeV", ">=30 MeV",
    ">=50 MeV", ">=60 MeV", ">=100 MeV", ">=500 MeV",
)
# Canales que consume cosmic-rad (semafaro >=10; deteccion >=100/>=500; ajuste
# espectral sobre los cinco integrales). Una muestra solo es VALIDA si lleva
# estos cinco, y un dia es COMPLETO si >= MIN_COVERAGE de los slots de la
# rejilla tienen una muestra valida (NOAA deja huecos permanentes).
REQUIRED_CHANNELS = (">=10 MeV", ">=30 MeV", ">=50 MeV", ">=100 MeV", ">=500 MeV")

SLOT_STEP_MIN = 5                 # cadencia SWPC de protones integrales
SLOTS_PER_DAY = (24 * 60) // SLOT_STEP_MIN   # 288
WINDOW_DAYS = 7                   # ventana de retencion de SWPC
MIN_COVERAGE = 0.95               # cierre COMPLETO: >= 95 % de las franjas
SLOTS_FOR_COMPLETE = int(math.ceil(MIN_COVERAGE * SLOTS_PER_DAY))  # 274
MAX_SATELLITE_CHANGES = 20        # entradas historicas en el manifest

_UTC = datetime.timezone.utc
_ROLE_PRIO = {"primary": 0, "secondary": 1}


# ---------------------------------------------------------------------------
# Utilidades de tiempo y rejilla
# ---------------------------------------------------------------------------

def _as_utc(dt):
    if dt.tzinfo is None:
        return dt.replace(tzinfo=_UTC)
    return dt.astimezone(_UTC)


def utc_now():
    return datetime.datetime.now(_UTC)


def fmt_iso(dt):
    return _as_utc(dt).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_time_tag(raw):
    """Convierte un time_tag de SWPC a datetime UTC (o None si es invalido)."""
    if not isinstance(raw, str):
        return None
    s = raw.strip()
    if not s:
        return None
    if s[-1:] in ("Z", "z"):
        s = s[:-1] + "+00:00"
    try:
        return _as_utc(datetime.datetime.fromisoformat(s))
    except ValueError:
        return None


def parse_iso_day(raw):
    """Convierte 'YYYY-MM-DD' a datetime UTC (medianoche) o None."""
    if not isinstance(raw, str):
        return None
    try:
        d = datetime.date.fromisoformat(raw.strip())
    except ValueError:
        return None
    return datetime.datetime(d.year, d.month, d.day, tzinfo=_UTC)


def slot_key(dt):
    """Timestamp canonico de la rejilla de 5 min, o None si no cae en rejilla.

    SWPC emite en HH:00/HH:05 con segundos a cero. Una muestra fuera de la
    rejilla es una anomalia: no se archiva (evita contaminar la definicion de
    dia completo).
    """
    if dt.second != 0 or dt.microsecond != 0 or dt.minute % SLOT_STEP_MIN != 0:
        return None
    return _as_utc(dt).strftime("%Y-%m-%dT%H:%M:%SZ")


def expected_slot_times(day):
    """Los 288 timestamps UTC (00:00..23:55) de un dia YYYY-MM-DD."""
    d = datetime.date.fromisoformat(day)
    start = datetime.datetime(d.year, d.month, d.day, tzinfo=_UTC)
    return [fmt_iso(start + datetime.timedelta(minutes=i * SLOT_STEP_MIN))
            for i in range(SLOTS_PER_DAY)]


def day_span(day):
    """(inicio, fin) del dia como datetime UTC. Fin = inicio del dia siguiente."""
    d = datetime.date.fromisoformat(day)
    start = datetime.datetime(d.year, d.month, d.day, tzinfo=_UTC)
    return start, start + datetime.timedelta(days=1)


# ---------------------------------------------------------------------------
# Normalizacion de filas crudas del feed
# ---------------------------------------------------------------------------

def add_rows(pool, rows, role):
    """Vuelca filas crudas del feed en `pool`, deduplicando por timestamp.

    pool: dict {(satellite, slot_ts): {"t", "sat", "flux", "roles"}}.
    Una fila cruda es {time_tag, satellite, energy, flux}: un canal por fila.
    La misma (satellite, timestamp) llega por varias filas (un canal por fila)
    o por varios pases del feed (6h y 7d); se fusiona por canal con el ultimo
    valor visto (orden de llamada determinista) y se anota el papel del feed
    que la aporto. Filas duplicadas del mismo slot y canal quedan colapsadas.
    """
    for e in rows:
        if not isinstance(e, dict):
            continue
        dt = parse_time_tag(e.get("time_tag"))
        ts = dt and slot_key(dt)
        if not ts:
            continue
        try:
            sat = int(e["satellite"])
        except (KeyError, TypeError, ValueError):
            continue
        if sat <= 0:
            continue
        energy = e.get("energy")
        if energy not in KNOWN_CHANNELS:
            continue
        flux = e.get("flux")
        try:
            flux = float(flux)
        except (TypeError, ValueError):
            continue
        if not math.isfinite(flux) or flux < 0:
            # Valor no finito o negativo: centinela de medida no disponible.
            continue
        key = (sat, ts)
        sample = pool.get(key)
        if sample is None:
            sample = {"t": ts, "sat": sat, "flux": {}, "roles": set()}
            pool[key] = sample
        sample["flux"][energy] = flux
        sample["roles"].add(role)


def sample_role(sample):
    """Papel dominante del feed que aporto la muestra.

    Las muestras del pool llevan `roles` (set); las acumuladas en el manifest
    llevan `src` ya resuelto. Ambas formas son validas.
    """
    src = sample.get("src")
    if src:
        return src
    roles = sample.get("roles")
    if isinstance(roles, set):
        roles = tuple(roles)
    roles = roles or ()
    if "primary" in roles:
        return "primary"
    if "secondary" in roles:
        return "secondary"
    if roles:
        return roles[0]
    return "unknown"


def sample_is_valid(sample):
    fl = sample["flux"]
    return all(ch in fl for ch in REQUIRED_CHANNELS)


# ---------------------------------------------------------------------------
# Resolucion del satelite desde los registros del feed
# ---------------------------------------------------------------------------

def satellite_for_role(pool, role):
    """Satelite que sirve hoy la cadena `role` ("primary"/"secondary").

    Cada registro del feed lleva su propio `satellite`; la cadena la resuelve
    el satelite de su muestra mas reciente. Devuelve None si la cadena no
    aporto ninguna muestra en esta corrida. Nunca fija GOES-18/19.
    """
    best_sat = None
    best_ts = None
    for (sat, ts), sample in pool.items():
        if role in sample["roles"] and (best_ts is None or ts > best_ts):
            best_sat, best_ts = sat, ts
    return best_sat


# ---------------------------------------------------------------------------
# Analisis de un dia sobre la piscina de muestras
# ---------------------------------------------------------------------------

def select_best(candidates):
    """Mejor muestra para un slot entre las candidatas validas.

    Criterio determinista: 1) papel del feed (primary antes que secondary, asi
    el fallback no contamina mientras el primario esta sano); 2) satelite
    mayor como desempate estable. El satelite no se resuelve fuera: viene en
    cada registro del feed.
    """
    def rank(sample):
        role = sample_role(sample)
        return (_ROLE_PRIO.get(role, 2), -sample["sat"])

    return min(candidates, key=rank)


def analyze_day(day, by_ts):
    """Completitud de un dia UTC sobre las candidatas por slot ya reunidas.

    `by_ts` mapea cada slot del dia a las muestras validas disponibles
    (acumulado persistido del manifest + feed actual). Un dia es COMPLETO
    cuando >= 95 % de los 288 slots (SLOTS_FOR_COMPLETE = 274) tienen una
    muestra valida (cinco canales requeridos): NOAA deja huecos permanentes y
    no se exige 288/288. La muestra elegida por slot puede venir de cualquier
    satelite/papel: el hueco de un satelite lo rellena el otro y queda
    registrado (satelite y fuente por muestra).
    """
    chosen = {}
    missing = []
    satellites = set()
    sources = set()
    for ts in expected_slot_times(day):
        candidates = [s for s in by_ts.get(ts, ()) if sample_is_valid(s)]
        if not candidates:
            missing.append(ts)
            continue
        best = select_best(candidates)
        chosen[ts] = best
        satellites.add(best["sat"])
        sources.add(sample_role(best))
    covered = SLOTS_PER_DAY - len(missing)
    return {
        "day": day,
        "complete": covered >= SLOTS_FOR_COMPLETE,
        "coverage": round(covered / SLOTS_PER_DAY, 4),
        "missing": missing,
        "chosen": chosen,
        "satellites": sorted(satellites),
        "sources": sorted(sources),
    }


def _serialize_sample(sample):
    """Muestra tal como se acumula en el manifest (sin objetos no JSON)."""
    return {"t": sample["t"], "sat": sample["sat"],
            "src": sample_role(sample), "flux": sample["flux"]}


def _day_candidates(pool, day, pending_entry):
    """Candidatas del dia: lo acumulado (pending_days) + el feed actual.

    Por slot y satelite manda la muestra del feed actual (valores mas
    recientes); el acumulado conserva lo que el feed ya no sirve.
    """
    idx = {}
    stored = None
    if isinstance(pending_entry, dict):
        stored = pending_entry.get("samples")
    if isinstance(stored, dict):
        for ts, frame in stored.items():
            if isinstance(frame, dict) and isinstance(frame.get("flux"), dict) \
                    and isinstance(frame.get("sat"), int) and ts.startswith(day):
                idx[ts] = [frame]
    for (sat, ts), sample in pool.items():
        if not ts.startswith(day):
            continue
        cands = idx.get(ts)
        if cands is None:
            idx[ts] = [sample]
            continue
        rest = [c for c in cands if c.get("sat") != sat]
        rest.append(sample)
        idx[ts] = rest
    return idx


# ---------------------------------------------------------------------------
# Fichero diario y SHA-256
# ---------------------------------------------------------------------------

def canonical_bytes(obj):
    """Serializacion canonica (claves ordenadas, sin espacios) de un JSON."""
    return json.dumps(obj, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=True).encode("utf-8")


def samples_sha256(samples):
    """SHA-256 del array de muestras tal como se archiva."""
    return hashlib.sha256(canonical_bytes(samples)).hexdigest()


def build_day_file(day, chosen, acquired_at, complete):
    """Contenido del fichero diario de un dia cerrado (completo o incompleto).

    `chosen` es la muestra elegida por cada franja de la rejilla que tenia
    muestra valida (nunca se inventa una franja ausente). El array `samples`
    se archiva en orden de rejilla y el SHA-256 se calcula solo sobre ese
    array, de modo que identifica el contenido exacto e inmutable de la
    captura. Cada muestra lleva su satelite y el papel del feed (fuente) que
    la aporto; un cambio de satelite dentro del dia queda visible en
    `satellites`. Fuera del hash viajan `complete` (bool), `sample_count`
    (int: numero de muestras del array) y `coverage` (float con 4 decimales:
    fraccion de las 288 franjas presentes).
    """
    samples = []
    for ts in expected_slot_times(day):
        sample = chosen.get(ts)
        if sample is None:
            continue          # franja ausente: hueco real, no se rellena
        samples.append({
            "t": ts,
            "sat": sample["sat"],
            "src": sample_role(sample),
            "flux": sample["flux"],
        })
    satellites = sorted({s["sat"] for s in samples})
    sources = sorted({s["src"] for s in samples})
    return {
        "schema_version": SCHEMA_VERSION,
        "collector_version": COLLECTOR_VERSION,
        "acquired_at": acquired_at,
        "day": day,
        "satellites": satellites,
        "sources": sources,
        "complete": bool(complete),
        "sample_count": len(samples),
        "coverage": round(len(samples) / SLOTS_PER_DAY, 4),
        "samples_sha256": samples_sha256(samples),
        "samples": samples,
    }


# ---------------------------------------------------------------------------
# E/S de ficheros (raiz de datos)
# ---------------------------------------------------------------------------

def http_get_json(url, timeout=30):
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            return json.load(resp)
    except urllib.error.HTTPError as exc:
        raise OSError("HTTP %d en %s" % (exc.code, url)) from exc


def read_json(path, default=None):
    try:
        with open(path, "r", encoding="utf-8") as fh:
            return json.load(fh)
    except (OSError, ValueError):
        return default


def write_json_atomic(path, obj):
    """Escritura atomica (tmp + rename): nunca deja un fichero a medias."""
    directory = os.path.dirname(path)
    if directory:
        os.makedirs(directory, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=".tmp-", dir=directory or ".")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.write(json.dumps(obj, indent=2, sort_keys=True,
                                ensure_ascii=True) + "\n")
        os.replace(tmp, path)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def write_day_file_exclusive(path, content):
    """Crea el fichero diario SOLO si no existe. Devuelve True si se escribio.

    La inmutabilidad se impone aqui: un dia cerrado (completo o incompleto
    permanente) nunca se reescribe ni se pisa, ni siquiera bajo dos corridas
    concurrentes ('x' es atomico).
    """
    directory = os.path.dirname(path)
    if directory:
        os.makedirs(directory, exist_ok=True)
    payload = json.dumps(content, indent=2, sort_keys=True,
                         ensure_ascii=True) + "\n"
    try:
        with open(path, "x", encoding="utf-8") as fh:
            fh.write(payload)
        return True
    except FileExistsError:
        return False


def day_file_path(root, day):
    d = datetime.date.fromisoformat(day)
    return os.path.join(root, SOLAR_DIR, "%04d" % d.year, "%02d" % d.month,
                        "%s.json" % day)


def list_daily_files(root):
    """Ficheros diarios ya CERRADOS (completos o huecos permanentes).

    Devuelve {day: ruta}. La existencia de un fichero diario NO implica dia
    completo: un dia que salio de la ventana sin datos se cierra igualmente
    con `complete: false`.
    """
    base = os.path.join(root, SOLAR_DIR)
    out = {}
    if os.path.isdir(base):
        for dirpath, _dirs, files in os.walk(base):
            for name in files:
                if not name.endswith(".json"):
                    continue
                day = name[:-5]
                if parse_iso_day(day) is not None:
                    out[day] = os.path.join(dirpath, name)
    return out


def list_complete_days(root):
    """Dias archivados como COMPLETOS (fichero con complete != False), orden.

    Los ficheros anteriores a `complete` eran siempre dias completos, asi que
    la ausencia del campo se lee como True.
    """
    days = []
    for day, path in list_daily_files(root).items():
        obj = read_json(path)
        if isinstance(obj, dict) and obj.get("complete", True) is not False:
            days.append(day)
    return sorted(days)


def scan_day_satellites(root, days):
    """Satelite(s) declarados en los ficheros diarios existentes.

    Solo se usa para arrancar el manifest desde cero (incremental despues).
    """
    seen = set()
    for day in days:
        obj = read_json(day_file_path(root, day))
        if not isinstance(obj, dict):
            continue
        sats = obj.get("satellites")
        if isinstance(sats, list):
            for s in sats:
                if isinstance(s, int) and s > 0:
                    seen.add(s)
    return seen


def load_manifest(root):
    obj = read_json(os.path.join(root, MANIFEST_FILE), {})
    return obj if isinstance(obj, dict) else {}


def write_manifest(root, manifest):
    write_json_atomic(os.path.join(root, MANIFEST_FILE), manifest)


# ---------------------------------------------------------------------------
# Estado auxiliar del manifest
# ---------------------------------------------------------------------------

def _incomplete_index(manifest):
    return {e["day"]: e for e in manifest.get("incomplete_days", [])
            if isinstance(e, dict) and isinstance(e.get("day"), str)}


# ---------------------------------------------------------------------------
# Adquisicion
# ---------------------------------------------------------------------------

def _fetch_or_none(url, fetch, problems, label, log):
    try:
        data = fetch(url)
    except Exception as exc:
        problems.append("%s: %s" % (label, exc))
        if log:
            log("aviso: %s fallo: %s" % (label, exc))
        return None
    if not isinstance(data, list):
        problems.append("%s: respuesta no es una lista JSON" % label)
        if log:
            log("aviso: %s no es una lista" % label)
        return None
    return data


def acquire_pool(fetch, problems, log):
    """Descarga y fusiona los feeds en una unica piscina de muestras.

    - "6h": primario y, solo si falla, secundario (frescura).
    - "7d": primario Y secundario se fusionan siempre: cada satelite tapa los
      huecos del otro dentro de la ventana de 7 dias (recuperacion).
    """
    pool = {}
    # Pase 1: frescura (6 h), primary con fallback a secondary.
    got_6h = False
    for role in ("primary", "secondary"):
        url = FEED_URLS[("6h", role)]
        data = _fetch_or_none(url, fetch, problems, "feed 6h/%s" % role, log)
        if data is None:
            continue
        add_rows(pool, data, role)
        got_6h = True
        if log:
            log("feed 6h/%s: %d filas" % (role, len(data)))
        break          # fallback: el secundario solo si el primario fallo
    if not got_6h and log:
        log("sin feed 6h (ni primary ni secondary)")

    # Pase 2: ventana de 7 dias; ambos satelites se fusionan.
    for role in ("primary", "secondary"):
        url = FEED_URLS[("7d", role)]
        data = _fetch_or_none(url, fetch, problems, "feed 7d/%s" % role, log)
        if data is None:
            continue
        add_rows(pool, data, role)
        if log:
            log("feed 7d/%s: %d filas" % (role, len(data)))
    return pool


# ---------------------------------------------------------------------------
# Orquestacion de una corrida
# ---------------------------------------------------------------------------

def run(data_root=".", fetch=None, now=None, log=None):
    """Una corrida del recolector. Devuelve codigo de salida (0 ok, 1 fatal).

    `fetch(url)` es inyectable para tests hermeticos (por defecto, red real).
    `now` inyecta el instante UTC para tests deterministas. `log` recibe
    lineas de progreso.

    Cierre de dias:
    - Dentro de la ventana, un dia con cobertura >= MIN_COVERAGE y cuyo
      intervalo completo esta en el feed se cierra como COMPLETO (creacion
      exclusiva, inmutable).
    - Un dia por debajo del umbral sigue PROVISIONAL mientras esta en la
      ventana: se reintenta en cada corrida y lo ya visto se persiste en disco
      (manifest.pending_days).
    - Al salir de la ventana (o cuando el inicio del dia ya no esta en la
      retencion del feed) se cierra de forma definitiva con lo acumulado en
      disco: complete: true si llego al umbral; si no, complete: false con su
      cobertura real, y si no habia nada, un fichero igualmente con
      complete: false, sample_count 0 y coverage 0.0. El dia queda en
      manifest.incomplete_days. Nunca se omite el fichero y nunca se escribe
      como si fuera un dia de flujo cero.
    """
    fetch = fetch or http_get_json
    if isinstance(now, str):
        now_dt = parse_time_tag(now)
        if now_dt is None:
            raise ValueError("instante `now` invalido: %r" % (now,))
    else:
        now_dt = _as_utc(now or utc_now())
    now_iso = fmt_iso(now_dt)
    today = now_dt.date().isoformat()
    problems = []
    manifest = load_manifest(root=data_root)

    # 1) Adquisicion: 6h (frescura) + 7d (ventana), cada uno con su fallback.
    pool = acquire_pool(fetch, problems, log)
    if not pool:
        problems.append("sin datos de ningun feed")
        if log:
            log("FATAL: sin datos de ningun feed")
        manifest["updated_at"] = now_iso
        manifest["collector_version"] = COLLECTOR_VERSION
        manifest["version"] = COLLECTOR_VERSION
        manifest["last_error"] = "; ".join(problems)
        write_manifest(root=data_root, manifest=manifest)
        return 1

    pool_start = min(ts for (_sat, ts) in pool)
    pool_end = max(ts for (_sat, ts) in pool)
    if log:
        log("pool: %d muestras, ventana %s .. %s"
            % (len(pool), pool_start, pool_end))

    # 2) Satelite que sirve cada cadena, leido del campo `satellite` de los
    #    registros (nunca fija GOES-18/19). Sin dato nuevo en esta corrida se
    #    reutiliza la ultima resolucion conocida del manifest.
    resolved_primary = satellite_for_role(pool, "primary")
    resolved_secondary = satellite_for_role(pool, "secondary")
    prev_sats = manifest.get("satellites")
    prev_primary = (prev_sats or {}).get("primary")
    prev_secondary = (prev_sats or {}).get("secondary")
    if resolved_primary is None:
        resolved_primary = prev_primary
    if resolved_secondary is None:
        resolved_secondary = prev_secondary
    if prev_primary is not None and resolved_primary is not None \
            and prev_primary != resolved_primary:
        changes = list(manifest.get("satellite_changes") or [])
        changes.insert(0, {
            "detected_at": now_iso,
            "day": today,
            "from": prev_primary,
            "to": resolved_primary,
        })
        manifest["satellite_changes"] = changes[:MAX_SATELLITE_CHANGES]
        if log:
            log("cambio de satelite primario: %s -> %s"
                % (prev_primary, resolved_primary))
    if log:
        log("satelites desde los feeds: primary=%s secondary=%s"
            % (resolved_primary, resolved_secondary))

    # 3) Cerrar dias: dentro de la ventana de 7 dias se reintenta mientras no
    #    se alcance el 95 %; al salir de la ventana se decide el cierre
    #    definitivo con lo acumulado.
    closed = set(list_daily_files(root=data_root))
    complete_days = set(list_complete_days(root=data_root))
    incomplete = _incomplete_index(manifest)
    pending = manifest.get("pending_days")
    if not isinstance(pending, dict):
        pending = {}
    satellites_seen = set(manifest.get("satellites_seen") or [])
    if not satellites_seen:
        satellites_seen |= scan_day_satellites(data_root, sorted(complete_days))

    pool_start_dt = parse_time_tag(pool_start)
    pool_end_dt = parse_time_tag(pool_end)

    def eligible(day):
        """El feed actual cubre el dia entero (00:00..23:55)."""
        start_dt, end_dt = day_span(day)
        return pool_start_dt <= start_dt and end_dt <= pool_end_dt

    def head_out_of_retention(day):
        """El inicio del dia ya salio de la retencion del feed actual."""
        start_dt, end_dt = day_span(day)
        return pool_start_dt > start_dt and end_dt <= pool_end_dt

    def finalize(day, analysis):
        """Cierre definitivo (sin reintentos) de `day` con su analisis.

        Escribe SIEMPRE el fichero diario: complete: true si el analisis llega
        al umbral; si no, complete: false con las muestras reales acumuladas
        (y si no habia ninguna, un fichero vacio con sample_count 0 y
        coverage 0.0). Nunca se omite el fichero ni se escribe como un dia de
        flujo cero.
        """
        if day in closed:
            return
        content = build_day_file(day, analysis["chosen"], now_iso,
                                 analysis["complete"])
        path = day_file_path(root=data_root, day=day)
        if not write_day_file_exclusive(path, content):
            closed.add(day)
            pending.pop(day, None)
            return
        closed.add(day)
        pending.pop(day, None)
        if analysis["satellites"]:
            satellites_seen.update(analysis["satellites"])
        if analysis["complete"]:
            complete_days.add(day)
            incomplete.pop(day, None)
            if log:
                log("dia %s cerrado al salir de la ventana (%d muestras, "
                    "coverage %s)" % (day, content["sample_count"],
                                      content["coverage"]))
            return
        incomplete[day] = {
            "day": day,
            "missing_slots": SLOTS_PER_DAY - len(content["samples"]),
            "satellites_seen": analysis["satellites"],
            "permanent": True,
            "updated_at": now_iso,
        }
        if log:
            log("dia %s fuera de la ventana: hueco permanente (%d muestras, "
                "coverage %s)" % (day, content["sample_count"],
                                  content["coverage"]))

    for offset in range(WINDOW_DAYS, 0, -1):
        day = (now_dt.date() - datetime.timedelta(days=offset)).isoformat()
        if day in closed:
            pending.pop(day, None)
            continue                       # inmutable: nunca se reescribe
        idx = _day_candidates(pool, day, pending.get(day))
        analysis = analyze_day(day, idx)
        if eligible(day) and analysis["complete"]:
            path = day_file_path(root=data_root, day=day)
            content = build_day_file(day, analysis["chosen"], now_iso, True)
            if write_day_file_exclusive(path, content):
                closed.add(day)
                complete_days.add(day)
                satellites_seen.update(analysis["satellites"])
                incomplete.pop(day, None)
                pending.pop(day, None)
                if log:
                    log("dia %s cerrado (%d/%d, coverage %.4f, "
                        "satelites %s, %s)"
                        % (day, content["sample_count"], SLOTS_PER_DAY,
                           content["coverage"], content["satellites"],
                           content["sources"]))
            continue
        if head_out_of_retention(day) and day not in incomplete:
            # El inicio del dia ya salio de la retencion de SWPC y el dia
            # nunca llego a registrarse: sus primeras franjas no volveran a
            # aparecer, asi que se cierra ya, definitivamente, con lo que el
            # feed aun tenga (real, o vacio si no tiene nada).
            finalize(day, analysis)
            continue
        if eligible(day) or day in incomplete:
            # Provisional dentro de la ventana: se reintenta y se persiste en
            # disco (pending_days) lo ya visto para poder cerrar con datos
            # reales cuando el feed deje de servirlos.
            entry = incomplete.get(day)
            entry_sats = sorted(set((entry or {}).get(
                "satellites_seen") or []) | set(analysis["satellites"]))
            incomplete[day] = {
                "day": day,
                "missing_slots": len(analysis["missing"]),
                "satellites_seen": entry_sats,
                "permanent": False,
                "updated_at": now_iso,
            }
            if analysis["chosen"]:
                pending[day] = {"samples": {ts: _serialize_sample(s)
                                            for ts, s in
                                            analysis["chosen"].items()}}
            else:
                pending.pop(day, None)
            if log:
                log("dia %s incompleto en ventana (%d slots, coverage %s)"
                    % (day, len(analysis["chosen"]), analysis["coverage"]))

    # 4) Dias provisionales que salieron de la ventana: cierre definitivo con
    #    lo acumulado en disco. Nunca se omite el fichero: si no habia nada
    #    acumulado se escribe igualmente (complete false, sample_count 0,
    #    coverage 0.0) y el dia queda en manifest.incomplete_days.
    still = []
    window_start = (now_dt.date()
                    - datetime.timedelta(days=WINDOW_DAYS)).isoformat()
    for day, entry in list(incomplete.items()):
        if day in closed:
            pending.pop(day, None)
            if day not in complete_days:
                still.append(entry)      # cerrado incompleto: se mantiene
            continue
        if day >= window_start:
            still.append(entry)
            continue
        if entry.get("permanent") and not pending.get(day):
            # Hueco permanente de una version anterior, sin datos ni fichero:
            # no se reabre ni se inventa nada.
            still.append(entry)
            continue
        idx = _day_candidates(pool, day, pending.get(day))
        analysis = analyze_day(day, idx)
        finalize(day, analysis)
        if analysis["complete"]:
            continue                      # completo: fuera de incomplete_days
        current = incomplete.get(day)
        still.append(current if current is not None else entry)
    incomplete = {e["day"]: e for e in still}
    if pending:
        manifest["pending_days"] = pending
    else:
        manifest.pop("pending_days", None)

    # 5) Manifest: ultimo exito, cobertura, incompletos, satelites, version.
    days = list_complete_days(root=data_root)
    manifest["schema_version"] = SCHEMA_VERSION
    manifest["collector_version"] = COLLECTOR_VERSION
    manifest["version"] = COLLECTOR_VERSION
    manifest["updated_at"] = now_iso
    manifest["last_success"] = now_iso
    manifest["last_error"] = "; ".join(problems) if problems else None
    manifest["satellites"] = {
        "primary": resolved_primary,
        "secondary": resolved_secondary,
    }
    manifest["satellites_seen"] = sorted(satellites_seen)
    manifest["incomplete_days"] = sorted(incomplete.values(),
                                         key=lambda e: e["day"])
    if days:
        manifest["coverage"] = {
            "first_day": days[0],
            "last_day": days[-1],
            "days": days,
            "days_complete": len(days),
            "days_incomplete": len(manifest["incomplete_days"]),
        }
    else:
        manifest["coverage"] = {
            "first_day": None, "last_day": None, "days": [],
            "days_complete": 0,
            "days_incomplete": len(manifest["incomplete_days"]),
        }
    write_manifest(root=data_root, manifest=manifest)
    if log:
        log("manifest actualizado: %d dias completos, %d incompletos, "
            "satelites %s" % (len(days), len(manifest["incomplete_days"]),
                              manifest["satellites_seen"]))
    return 0


def main(argv):
    data_root = argv[0] if argv else "."
    return run(data_root=data_root, log=print)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
