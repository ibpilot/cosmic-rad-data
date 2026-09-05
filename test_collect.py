#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Tests HERMETICOS del recolector cosmic-rad-data (sin red).

Los datos son sinteticos y se definen en este mismo fichero con el formato
real de los feeds SWPC (una fila por canal: {time_tag, satellite, flux,
energy}). El satelite se lee de cada registro del feed: no hay
instrument-sources.json.

Ejecutar desde la raiz del repo:
    PYTHONDONTWRITEBYTECODE=1 python3 -m unittest test_collect -v
"""

import datetime
import hashlib
import json
import os
import shutil
import tempfile
import unittest

import collect

# Formato real SWPC (octubre 2023 en adelante): un canal por fila.
ENERGIES = [
    ">=1 MeV", ">=5 MeV", ">=10 MeV", ">=30 MeV",
    ">=50 MeV", ">=60 MeV", ">=100 MeV", ">=500 MeV",
]
REQUIRED = [">=10 MeV", ">=30 MeV", ">=50 MeV", ">=100 MeV", ">=500 MeV"]

SLOT_STEP = 5  # minutos
_UTC = datetime.timezone.utc


def parse(tag):
    return datetime.datetime.fromisoformat(tag.replace("Z", "+00:00"))


def fmt(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def day_dt(day):
    d = datetime.date.fromisoformat(day)
    return datetime.datetime(d.year, d.month, d.day, tzinfo=_UTC)


def slot_times(start_tag, end_tag):
    """Slots de 5 min entre dos time_tags (inclusive)."""
    out = []
    t = parse(start_tag)
    end = parse(end_tag)
    while t <= end:
        out.append(fmt(t))
        t += datetime.timedelta(minutes=SLOT_STEP)
    return out


def day_slots(day):
    start = day_dt(day)
    return [fmt(start + datetime.timedelta(minutes=i * SLOT_STEP))
            for i in range(288)]


def _rows_for(ts, satellite, flux):
    rows = []
    for energy in ENERGIES:
        rows.append({
            "time_tag": ts,
            "satellite": satellite,
            "energy": energy,
            "flux": flux,
        })
    return rows


def build_feed(days, satellites, flux=1.0, holes=None, extra=None):
    """Feed sintetico (lista de filas) con cobertura completa por dia/satelite.

    holes: set de (day, ts) -> se omiten TODAS las filas de ese slot
           (simula una caida total del satelite en ese instante).
    """
    holes = holes or set()
    rows = []
    for day in days:
        for ts in day_slots(day):
            for sat in satellites:
                if (day, ts) in holes:
                    continue
                rows.extend(_rows_for(ts, sat, flux))
    if extra:
        rows.extend(extra)
    return rows


class Fetcher:
    """Fetcher inyectable: URL del feed -> filas (None simula caida).

    Cualquier otra URL (p.ej. un hipotetico instrument-sources.json) es un
    error de red: el recolector no debe pedir nada fuera de los feeds.
    """

    def __init__(self, feeds):
        self.feeds = dict(feeds)     # {(kind, role): rows o None(->error)}
        self.calls = []

    def __call__(self, url):
        self.calls.append(url)
        for (kind, role), rows in self.feeds.items():
            if url == collect.FEED_URLS[(kind, role)]:
                if rows is None:
                    raise OSError("red simulada: %s/%s caido"
                                  % (kind, role))
                return rows
        raise OSError("URL desconocida: %s" % url)


def run_once(root, fetcher, now):
    return collect.run(data_root=root, fetch=fetcher, now=now, log=None)


def read_json(path):
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


def day_path(root, day):
    d = datetime.date.fromisoformat(day)
    return os.path.join(root, "solar", "%04d" % d.year, "%02d" % d.month,
                        "%s.json" % day)


def manifest_path(root):
    return os.path.join(root, "manifest.json")


class CollectorTestCase(unittest.TestCase):

    def setUp(self):
        self._tmp = tempfile.mkdtemp(prefix="cosmic-rad-data-test-")
        self.root = self._tmp
        # Una ventana de 7 dias UTC completa: 2026-08-30 .. 2026-09-05.
        self.days = [("2026-08-%02d" % d) for d in range(30, 32)]
        self.days += ["2026-09-%02d" % d for d in range(1, 6)]
        # Corrida a las 00:30Z del 2026-09-06: cierra el 2026-09-05.
        self.now = "2026-09-06T00:30:00Z"
        # Feed de 7 dias (ventana completa) y feed de 6 h (solapa el final del
        # 2026-09-05, igual que en produccion cerca de medianoche). El
        # satelite viaja en cada registro del feed (sin instrument-sources).
        self.feeds = {
            ("7d", "primary"): build_feed(self.days, [18], flux=1.0),
            ("6h", "primary"): build_feed(
                [], [18], extra=self._today_slots_6h()),
        }

    def tearDown(self):
        shutil.rmtree(self._tmp, ignore_errors=True)

    # -- utilidades ---------------------------------------------------------

    def _today_slots_6h(self, day="2026-09-06", start="2026-09-05T18:30:00Z",
                        end="2026-09-06T00:25:00Z"):
        rows = []
        for ts in slot_times(start, end):
            rows.extend(_rows_for(ts, 18, 1.0))
        return rows

    def _fetcher(self, feeds=None):
        feeds = feeds if feeds is not None else self.feeds
        return Fetcher(feeds)

    def _run(self, fetcher=None, now=None):
        fetcher = fetcher or self._fetcher()
        rc = run_once(self.root, fetcher, now or self.now)
        self.assertEqual(rc, 0)
        return fetcher

    def _manifest(self):
        return read_json(manifest_path(self.root))

    def _day_file(self, day):
        return read_json(day_path(self.root, day))

    def _build_with_hole(self, day="2026-09-04", whole=15, channel=True,
                         satellites=(18,), days=None):
        """Feed donde `day` queda bajo el umbral del 95 %.

        Caen `whole` slots enteros (sin filas) y, opcionalmente, un slot
        adicional sin el canal >=500 MeV: 15 + 1 = 16 slots invalidos
        (272/288, claramente por debajo de MIN_COVERAGE).
        """
        days = days if days is not None else self.days
        slots = day_slots(day)
        dropped = {(day, ts) for ts in slots[30:30 + whole]}
        bad_channel = (day, slots[30 + whole]) if channel else None
        rows = []
        for d in days:
            for ts in day_slots(d):
                if (d, ts) in dropped:
                    continue                       # sin filas: slot perdido
                for energy in ENERGIES:
                    if (d, ts) == bad_channel and energy == ">=500 MeV":
                        continue                   # muestra invalida
                    rows.append({"time_tag": ts, "satellite": satellites[0],
                                 "energy": energy, "flux": 1.0})
        return rows


class TestDedup(CollectorTestCase):
    """Deduplicacion por timestamp: un slot == una muestra."""

    def test_duplicates_collapse_to_one_sample_per_slot(self):
        # Duplicado dentro del feed de 7 dias (mismo slot, mismo canal, dos
        # valores): debe quedar el ultimo valor visto y una sola muestra.
        dup_ts = "2026-09-05T12:00:00Z"
        self.feeds[("7d", "primary")].extend([
            {"time_tag": dup_ts, "satellite": 18, "energy": ">=10 MeV",
             "flux": 500.0},
            {"time_tag": dup_ts, "satellite": 18, "energy": ">=10 MeV",
             "flux": 999.0},
        ])
        # Mismo slot tambien presente en el pase de 6 h (mismo valor):
        # la fusion entre pases no debe duplicar muestras.
        self._run()
        content = self._day_file("2026-09-05")
        self.assertEqual(content["sample_count"], 288)
        timestamps = [s["t"] for s in content["samples"]]
        self.assertEqual(len(timestamps), len(set(timestamps)))
        self.assertEqual(timestamps, sorted(timestamps))
        dup = next(s for s in content["samples"] if s["t"] == dup_ts)
        # Un solo canal >=10 tras el duplicado: ultimo valor (999.0).
        self.assertEqual(dup["flux"].get(">=10 MeV"), 999.0)
        # Ninguna muestra duplicada ni fuera de la rejilla de 5 min.
        for s in content["samples"]:
            self.assertIn(s["t"], collect.expected_slot_times("2026-09-05"))

    def test_two_runs_do_not_duplicate_samples(self):
        self._run()
        first = self._day_file("2026-09-04")
        self._run()
        second = self._day_file("2026-09-04")
        self.assertEqual(first["sample_count"], second["sample_count"])
        self.assertEqual(first["samples_sha256"],
                         second["samples_sha256"])


class TestIncompleteDay(CollectorTestCase):
    """Un dia con huecos no se escribe como completo."""

    def test_incomplete_day_not_written_as_complete(self):
        rows = self._build_with_hole()          # 09-04: 16 slots invalidos
        feeds = dict(self.feeds)
        feeds[("7d", "primary")] = rows
        self._run(fetcher=self._fetcher(feeds=feeds))
        # Los otros seis dias si se cerraron.
        for day in self.days:
            if day != "2026-09-04":
                self.assertTrue(os.path.exists(day_path(self.root, day)),
                                "falta el dia completo %s" % day)
        self.assertFalse(os.path.exists(day_path(self.root, "2026-09-04")),
                         "un dia bajo el umbral no se escribe mientras esta "
                         "en la ventana")
        manifest = self._manifest()
        self.assertEqual(manifest["coverage"]["days_complete"], 6)
        entries = {e["day"]: e for e in manifest["incomplete_days"]}
        entry = entries.get("2026-09-04")
        self.assertIsNotNone(entry, "el dia incompleto debe quedar registrado")
        self.assertEqual(entry["missing_slots"], 16)
        self.assertFalse(entry["permanent"],
                         "sigue dentro de la ventana: provisional, se reintenta")

    def test_provisional_day_is_retried_while_in_window(self):
        rows = self._build_with_hole()
        feeds = dict(self.feeds)
        feeds[("7d", "primary")] = rows
        fetcher = self._fetcher(feeds=feeds)
        self._run(fetcher=fetcher)
        # Segunda corrida, misma ventana, mismo hueco: se reintenta y sigue
        # sin escribirse como completo (provisional).
        self._run(fetcher=fetcher, now="2026-09-06T06:30:00Z")
        self.assertFalse(os.path.exists(day_path(self.root, "2026-09-04")))
        entries = {e["day"]: e for e in self._manifest()["incomplete_days"]}
        self.assertFalse(entries["2026-09-04"]["permanent"])


class TestImmutable(CollectorTestCase):
    """Un dia ya cerrado (completo) es inmutable: no se reescribe nunca."""

    def test_complete_day_never_rewritten(self):
        self._run()
        path = day_path(self.root, "2026-09-03")
        with open(path, "rb") as fh:
            before = fh.read()
        mtime_before = os.stat(path).st_mtime_ns

        # Corrida posterior con datos "corregidos" (valores distintos): el
        # fichero diario ya cerrado no puede cambiar.
        feeds = dict(self.feeds)
        rows = build_feed(self.days, [18], flux=2.0)   # valores distintos
        feeds[("7d", "primary")] = rows
        feeds[("6h", "primary")] = build_feed(
            [], [18], extra=[r for r in self._today_slots_6h()])
        self._run(fetcher=self._fetcher(feeds=feeds),
                  now="2026-09-06T06:30:00Z")

        with open(path, "rb") as fh:
            after = fh.read()
        self.assertEqual(before, after, "el dia completo fue reescrito")
        self.assertEqual(os.stat(path).st_mtime_ns, mtime_before)

    def test_exclusive_create_never_overwrites_existing_file(self):
        # La inmutabilidad se impone con creacion exclusiva: dos corridas
        # concurrentes que intenten cerrar el mismo dia no pueden pisarse.
        path = day_path(self.root, "2026-09-01")
        written = collect.write_day_file_exclusive(path, {"v": 1})
        self.assertTrue(written, "primer cierre: el fichero se crea")
        second = collect.write_day_file_exclusive(path, {"v": 2})
        self.assertFalse(second,
                         "segundo cierre: no se reescribe un dia ya cerrado")
        self.assertEqual(read_json(path), {"v": 1},
                         "el contenido original debe conservarse")

    def test_file_carries_schema_and_acquired_at(self):
        self._run()
        content = self._day_file("2026-09-02")
        self.assertEqual(content["schema_version"], 1)
        self.assertEqual(content["collector_version"],
                         collect.COLLECTOR_VERSION)
        self.assertEqual(content["acquired_at"], self.now)
        self.assertEqual(content["satellites"], [18])
        self.assertEqual(content["sources"], ["primary"])
        self.assertEqual(content["sample_count"], 288)
        self.assertIn("samples_sha256", content)


class TestFallback(CollectorTestCase):
    """Si el feed primario falla, se usa el equivalente secondary."""

    def test_secondary_feed_used_when_primary_down(self):
        feeds = {
            ("7d", "primary"): None,       # caido
            ("6h", "primary"): None,       # caido
            ("7d", "secondary"): build_feed(self.days, [19], flux=1.0),
            ("6h", "secondary"): build_feed(
                [], [19],
                extra=[r for r in slot_times("2026-09-05T18:30:00Z",
                                             "2026-09-06T00:25:00Z")
                       for r in _rows_for(r, 19, 1.0)]),
        }
        fetcher = self._fetcher(feeds=feeds)
        self._run(fetcher=fetcher)
        # Los dias se cierran con los datos del satelite secundario.
        content = self._day_file("2026-09-01")
        self.assertEqual(content["satellites"], [19])
        self.assertEqual(content["sources"], ["secondary"])
        manifest = self._manifest()
        self.assertEqual(manifest["coverage"]["days_complete"], 7)
        self.assertIsNotNone(manifest["last_error"])
        self.assertIn("7d/primary", manifest["last_error"])
        self.assertIn("6h/primary", manifest["last_error"])

    def test_primary_wins_when_both_available(self):
        # Secondary sano pero el primario manda: las muestras archivadas del
        # dia son del satelite primario (18), no del secundario (19).
        feeds = dict(self.feeds)
        feeds[("7d", "secondary")] = build_feed(self.days, [19], flux=5.0)
        self._run(fetcher=self._fetcher(feeds=feeds))
        content = self._day_file("2026-09-01")
        self.assertEqual(content["satellites"], [18])
        self.assertEqual(content["sources"], ["primary"])


class TestSha256(CollectorTestCase):
    """El SHA-256 se calcula sobre el array de muestras."""

    def test_hash_matches_independent_canonical_serialization(self):
        self._run()
        content = self._day_file("2026-09-05")
        independent = hashlib.sha256(
            json.dumps(content["samples"], sort_keys=True,
                       separators=(",", ":")).encode("utf-8")).hexdigest()
        self.assertEqual(content["samples_sha256"], independent)

    def test_hash_changes_if_any_sample_changes(self):
        self._run()
        content = self._day_file("2026-09-05")
        original = content["samples_sha256"]
        content["samples"][0]["flux"][">=10 MeV"] += 1.0
        mutated = hashlib.sha256(
            json.dumps(content["samples"], sort_keys=True,
                       separators=(",", ":")).encode("utf-8")).hexdigest()
        self.assertNotEqual(mutated, original)

    def test_collector_sha_helper_agrees(self):
        samples = [{"t": "2026-09-05T00:00:00Z", "sat": 18, "src": "primary",
                    "flux": {">=10 MeV": 1.5, ">=500 MeV": 0.1}}]
        self.assertEqual(collect.samples_sha256(samples),
                         hashlib.sha256(
                             json.dumps(samples, sort_keys=True,
                                        separators=(",", ":")).encode(
                             "utf-8")).hexdigest())


class TestSatelliteChange(CollectorTestCase):
    """Un cambio de satelite queda registrado (ficheros + manifest)."""

    def test_satellite_change_registered_across_runs(self):
        # Corrida 1: la cadena primaria sirve datos del satelite 17 (leido de
        # los propios registros del feed, no de instrument-sources.json).
        feeds1 = {
            ("7d", "primary"): build_feed(self.days, [17], flux=1.0),
            ("6h", "primary"): build_feed(
                [], [17], extra=[r for r in slot_times(
                    "2026-09-05T18:30:00Z", "2026-09-06T00:25:00Z")
                    for r in _rows_for(r, 17, 1.0)]),
        }
        self._run(fetcher=self._fetcher(feeds=feeds1))
        day_old = self._day_file("2026-09-05")
        self.assertEqual(day_old["satellites"], [17])
        manifest1 = self._manifest()
        self.assertEqual(manifest1["satellites"]["primary"], 17)
        self.assertEqual(manifest1["satellites_seen"], [17])

        # Corrida 2 (al dia siguiente): la cadena primaria sirve ahora datos
        # del satelite 18 y el nuevo dia (2026-09-06) se cierra con el 18.
        days2 = self.days[1:] + ["2026-09-06"]
        now2 = "2026-09-07T00:30:00Z"
        feeds2 = {
            ("7d", "primary"): build_feed(days2, [18], flux=1.0),
            ("6h", "primary"): build_feed(
                [], [18], extra=[r for r in slot_times(
                    "2026-09-06T18:30:00Z", "2026-09-07T00:25:00Z")
                    for r in _rows_for(r, 18, 1.0)]),
        }
        self._run(fetcher=self._fetcher(feeds=feeds2), now=now2)

        day_new = self._day_file("2026-09-06")
        self.assertEqual(day_new["satellites"], [18])
        # El dia viejo no se reescribe con el satelite nuevo (inmutable).
        self.assertEqual(self._day_file("2026-09-05")["satellites"], [17])
        manifest2 = self._manifest()
        self.assertEqual(manifest2["satellites_seen"], [17, 18])
        self.assertEqual(manifest2["satellites"]["primary"], 18)
        changes = manifest2.get("satellite_changes") or []
        self.assertTrue(any(c["from"] == 17 and c["to"] == 18
                            for c in changes),
                        "el cambio 17 -> 18 debe quedar registrado")

    def test_gap_filled_by_second_satellite_registers_both(self):
        # El primario (18) tiene un hueco el 09-04; el secundario (19) lo
        # tapa en la corrida siguiente: recuperacion dentro de la ventana.
        rows = self._build_with_hole()          # 09-04: 16 slots caidos
        feeds1 = dict(self.feeds)
        feeds1[("7d", "primary")] = rows
        self._run(fetcher=self._fetcher(feeds=feeds1))
        self.assertFalse(os.path.exists(day_path(self.root, "2026-09-04")))

        feeds2 = dict(feeds1)
        feeds2[("7d", "secondary")] = build_feed(self.days, [19], flux=1.0)
        self._run(fetcher=self._fetcher(feeds=feeds2),
                  now="2026-09-06T06:30:00Z")
        content = self._day_file("2026-09-04")
        self.assertEqual(content["sample_count"], 288)
        self.assertEqual(content["satellites"], [18, 19])
        self.assertEqual(content["sources"], ["primary", "secondary"])
        entries = {e["day"]: e for e in self._manifest()["incomplete_days"]}
        self.assertNotIn("2026-09-04", entries,
                         "el dia recuperado sale de la lista de incompletos")
        self.assertEqual(self._manifest()["satellites_seen"], [18, 19])


class TestManifest(CollectorTestCase):
    """El manifest publica ultimo exito, cobertura, incompletos y version."""

    def test_manifest_fields(self):
        # SWPC sano: los cuatro feeds (6h/7d x primary/secondary) disponibles.
        feeds = dict(self.feeds)
        feeds[("7d", "secondary")] = build_feed(self.days, [19], flux=1.0)
        feeds[("6h", "secondary")] = build_feed(
            [], [19], extra=[r for r in slot_times(
                "2026-09-05T18:30:00Z", "2026-09-06T00:25:00Z")
                for r in _rows_for(r, 19, 1.0)])
        self._run(fetcher=self._fetcher(feeds=feeds))
        manifest = self._manifest()
        self.assertEqual(manifest["last_success"], self.now)
        self.assertIsNone(manifest["last_error"])
        self.assertEqual(manifest["collector_version"],
                         collect.COLLECTOR_VERSION)
        self.assertEqual(manifest["version"], collect.COLLECTOR_VERSION)
        self.assertEqual(manifest["schema_version"], 1)
        cov = manifest["coverage"]
        self.assertEqual(cov["first_day"], "2026-08-30")
        self.assertEqual(cov["last_day"], "2026-09-05")
        self.assertEqual(cov["days_complete"], 7)
        self.assertEqual(len(cov["days"]), 7)
        self.assertEqual(manifest["satellites_seen"], [18])
        # El satelite primario/secundario se lee del feed (no hay
        # instrument-sources.json).
        self.assertEqual(manifest["satellites"], {"primary": 18,
                                                  "secondary": 19})
        self.assertEqual(cov["days_incomplete"], 0)

    def test_days_older_than_window_become_permanent_holes(self):
        rows = self._build_with_hole()        # hueco el 2026-09-04
        feeds = dict(self.feeds)
        feeds[("7d", "primary")] = rows
        self._run(fetcher=self._fetcher(feeds=feeds),
                  now="2026-09-06T00:30:00Z")
        # Mientras el 2026-09-04 siga dentro de la ventana se reintenta
        # (provisional). El 2026-09-12 la ventana es 09-05..09-11: el dia ya
        # salio sin completarse y pasa a hueco permanente con su fichero.
        self._run(fetcher=self._fetcher(feeds=feeds),
                  now="2026-09-12T00:30:00Z")
        entries = {e["day"]: e for e in self._manifest()["incomplete_days"]}
        entry = entries.get("2026-09-04")
        self.assertIsNotNone(entry)
        self.assertTrue(entry["permanent"],
                        "fuera de la ventana: hueco permanente")
        # Se cierra con lo acumulado en disco (272/288), nunca omitido.
        content = self._day_file("2026-09-04")
        self.assertFalse(content["complete"])
        self.assertEqual(content["sample_count"], 272)


class TestCoverageRule(CollectorTestCase):
    """Regla de cierre por cobertura >= 95 % (274/288 o mas)."""

    def _feed_for(self, day, missing_slots, include_day=True):
        """Feed donde `day` tiene `missing_slots` franjas sin datos.

        include_day=False simula que SWPC ya no sirve ese dia en absoluto.
        """
        days = list(self.days)
        if not include_day:
            days = [d for d in days if d != day]
        holes = {(day, ts) for ts in day_slots(day)[:missing_slots]}
        feeds = dict(self.feeds)
        feeds[("7d", "primary")] = build_feed(days, [18], holes=holes)
        return feeds

    def test_day_with_274_of_288_closes_complete(self):
        # 274/288 = 0.9514 >= 0.95: cierra como COMPLETO dentro de la ventana.
        feeds = self._feed_for("2026-09-04", missing_slots=14)
        self._run(fetcher=self._fetcher(feeds=feeds))
        content = self._day_file("2026-09-04")
        self.assertTrue(content["complete"])
        self.assertEqual(content["sample_count"], 274)
        self.assertEqual(content["coverage"], 0.9514)
        self.assertEqual(len(content["samples"]), 274)
        entries = {e["day"]: e for e in self._manifest()["incomplete_days"]}
        self.assertNotIn("2026-09-04", entries)

    def test_day_with_273_of_288_not_complete_while_in_window(self):
        # 273/288 = 0.9479 < 0.95: NO cierra mientras esta en la ventana.
        feeds = self._feed_for("2026-09-04", missing_slots=15)
        self._run(fetcher=self._fetcher(feeds=feeds))
        self.assertFalse(os.path.exists(day_path(self.root, "2026-09-04")),
                         "273/288 no debe cerrar dentro de la ventana")
        entries = {e["day"]: e for e in self._manifest()["incomplete_days"]}
        entry = entries.get("2026-09-04")
        self.assertIsNotNone(entry)
        self.assertEqual(entry["missing_slots"], 15)
        self.assertFalse(entry["permanent"])
        # El estado provisional persiste EN DISCO (manifest.pending_days):
        # las 273 muestras ya vistas no viven solo en memoria.
        pending = self._manifest().get("pending_days") or {}
        stored = (pending.get("2026-09-04") or {}).get("samples") or {}
        self.assertEqual(len(stored), 273)

    def test_day_leaving_window_closes_with_accumulated_on_disk(self):
        # Corrida 1: 09-04 queda provisional con 273/288 acumuladas en disco.
        feeds1 = self._feed_for("2026-09-04", missing_slots=15)
        self._run(fetcher=self._fetcher(feeds=feeds1))
        self.assertFalse(os.path.exists(day_path(self.root, "2026-09-04")))
        pending = self._manifest().get("pending_days") or {}
        stored = (pending.get("2026-09-04") or {}).get("samples") or {}
        self.assertEqual(len(stored), 273)

        # Corrida 2: ya fuera de la ventana y SWPC ya no sirve el 09-04 en el
        # feed: se cierra con lo acumulado en disco (273/288 reales).
        feeds2 = self._feed_for("2026-09-04", missing_slots=15,
                                include_day=False)
        self._run(fetcher=self._fetcher(feeds=feeds2),
                  now="2026-09-12T00:30:00Z")
        content = self._day_file("2026-09-04")
        self.assertFalse(content["complete"])
        self.assertEqual(content["sample_count"], 273)
        self.assertEqual(content["coverage"], 0.9479)
        self.assertEqual(len(content["samples"]), 273)
        self.assertEqual(content["satellites"], [18])
        entries = {e["day"]: e for e in self._manifest()["incomplete_days"]}
        entry = entries.get("2026-09-04")
        self.assertIsNotNone(entry)
        self.assertTrue(entry["permanent"])
        self.assertEqual(entry["missing_slots"], 15)

    def test_day_with_no_data_closes_empty_on_leaving_window(self):
        # El 09-04 no tiene NINGUNA muestra en ningun feed de la ventana.
        feeds = self._feed_for("2026-09-04", missing_slots=288,
                               include_day=False)
        self._run(fetcher=self._fetcher(feeds=feeds))
        self.assertFalse(os.path.exists(day_path(self.root, "2026-09-04")))
        entries = {e["day"]: e for e in self._manifest()["incomplete_days"]}
        entry = entries.get("2026-09-04")
        self.assertIsNotNone(entry)
        self.assertEqual(entry["missing_slots"], 288)
        self.assertFalse(entry["permanent"])

        # Al salir de la ventana se cierra IGUALMENTE, nunca se omite el
        # fichero: complete false, sample_count 0, coverage 0.0. Un dia sin
        # datos NO es un dia de flujo cero (que tendria 288 muestras a 0).
        self._run(fetcher=self._fetcher(feeds=feeds),
                  now="2026-09-12T00:30:00Z")
        content = self._day_file("2026-09-04")
        self.assertFalse(content["complete"])
        self.assertEqual(content["sample_count"], 0)
        self.assertEqual(content["coverage"], 0.0)
        self.assertEqual(content["samples"], [])
        entries = {e["day"]: e for e in self._manifest()["incomplete_days"]}
        entry = entries.get("2026-09-04")
        self.assertTrue(entry["permanent"])
        self.assertEqual(entry["missing_slots"], 288)

    def test_file_carries_sample_count_and_coverage(self):
        # Nombres finales del esquema: `samples` es el ARRAY de medidas,
        # `sample_count` el contador entero y `coverage` un float 4 decimales.
        self._run()
        content = self._day_file("2026-09-03")
        self.assertIsInstance(content["samples"], list)
        self.assertIsInstance(content["sample_count"], int)
        self.assertEqual(content["sample_count"], len(content["samples"]))
        self.assertEqual(content["sample_count"], 288)
        self.assertEqual(content["coverage"], 1.0)
        self.assertTrue(content["complete"])


class TestSatelliteFromFeed(CollectorTestCase):
    """El satelite se resuelve desde cada registro del feed."""

    def test_satellite_read_from_feed_records_without_sources_file(self):
        self.assertFalse(hasattr(collect, "SOURCES_URL"),
                         "no debe quedar ninguna URL de instrument-sources")
        fetcher = self._fetcher()
        self._run(fetcher=fetcher)
        # Ninguna peticion fuera de los feeds; en particular ninguna a
        # instrument-sources.json.
        self.assertTrue(fetcher.calls)
        for url in fetcher.calls:
            self.assertNotIn("instrument-sources", url)
            self.assertIn(url, collect.FEED_URLS.values())
        content = self._day_file("2026-09-05")
        self.assertEqual(content["satellites"], [18])
        manifest = self._manifest()
        self.assertEqual(manifest["satellites"]["primary"], 18)


if __name__ == "__main__":
    unittest.main()



class TestSentinelFlux(CollectorTestCase):
    """Un flujo negativo/no finito es un centinela de 'medida no disponible'.

    Si se archivara como flujo real, una lectura centinela entraria en la
    mediana de la linea base y en la dosis. Un slot cuyo unico dato es
    centinela cuenta como AUSENTE, no como cero ni como medida.
    """

    SENTINEL_SLOTS = 20   # > 288-274: hunde el dia por debajo del umbral
    DAY = "2026-09-05"

    def _sentinel_feed(self):
        slots = day_slots(self.DAY)[:self.SENTINEL_SLOTS]
        holes = {(self.DAY, ts) for ts in slots}
        sentinel = []
        for ts in slots:
            sentinel.extend(_rows_for(ts, 18, -100000.0))
        return build_feed(self.days, [18], flux=1.0,
                          holes=holes, extra=sentinel)

    def _feeds(self):
        feeds = dict(self.feeds)
        feeds[("7d", "primary")] = self._sentinel_feed()
        return feeds

    def test_slot_solo_con_centinela_no_cierra_el_dia(self):
        self._run(self._fetcher(self._feeds()))
        self.assertFalse(
            os.path.exists(day_path(self.root, self.DAY)),
            "268/288 esta bajo el umbral: el dia no puede cerrar todavia")
        pending = self._manifest().get("pending_days") or []
        self.assertIn(self.DAY, [p["day"] if isinstance(p, dict) else p
                                 for p in pending],
                      "el dia debe quedar provisional, no desaparecer")

    def test_ningun_flujo_negativo_llega_al_archivo(self):
        self._run(self._fetcher(self._feeds()))
        # Segunda corrida 8 dias despues: el dia sale de ventana y cierra
        # como incompleto permanente, con lo acumulado en disco.
        later_days = ["2026-09-%02d" % d for d in range(8, 14)]
        self._run(self._fetcher({
            ("7d", "primary"): build_feed(later_days, [18], flux=1.0),
            ("6h", "primary"): build_feed([], [18]),
        }), now="2026-09-14T00:30:00Z")
        data = read_json(day_path(self.root, self.DAY))
        self.assertFalse(data["complete"])
        self.assertEqual(data["sample_count"], 288 - self.SENTINEL_SLOTS)
        for sample in data["samples"]:
            for energy, flux in sample["flux"].items():
                self.assertGreaterEqual(
                    flux, 0.0,
                    "centinela archivado como flujo real en %s" % energy)
