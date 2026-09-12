#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Tests HERMETICOS de `backfill_ncei` (sin red y sin h5py).

Los documentos crudos se construyen a mano con `make_raw`, que por defecto
produce un día perfecto de 288 franjas y 2 sensores. Ejecutar desde la raíz:

    PYTHONDONTWRITEBYTECODE=1 python3 -m unittest test_backfill_ncei -v
"""

import contextlib
import datetime
import hashlib
import io
import os
import subprocess
import sys
import tempfile
import unittest

import backfill_ncei as bf

UTC = datetime.timezone.utc
FILL = -1e31
DAY = "2026-09-10"
T0 = "2026-09-10T00:00:00Z"
N_SLOTS = 288
J2000_MIDNIGHT = -43200  # 2000-01-01T00:00:00Z en segundos J2000
REPO_ROOT = os.path.dirname(os.path.abspath(__file__))

LOWER = [100.0 * (i + 1) for i in range(13)]
UPPER = [100.0 * (i + 1) + 50.0 for i in range(13)]

EXPECTED_CHANNELS = ["P1", "P2A", "P2B", "P3", "P4", "P5", "P6", "P7",
                     "P8A", "P8B", "P8C", "P9", "P10"]


def day_j2000(day):
    d = datetime.date.fromisoformat(day)
    return J2000_MIDNIGHT + (d - datetime.date(2000, 1, 1)).days * 86400


def make_raw(day=DAY, sat="g18", version="v3-0-3", **overrides):
    base = float(day_j2000(day))
    raw = {
        "file_id": "sci_sgps-l2-avg5m_%s_d%s_%s.nc"
                   % (sat, day.replace("-", ""), version),
        "platform": sat,
        "time_coverage_start": "%sT00:00:00.000Z" % day,
        "time": [base + 300.0 * i for i in range(N_SLOTS)],
        "diff": [[[1.0] * 13 for _ in range(2)] for _ in range(N_SLOTS)],
        "integral": [[1.0, 1.0] for _ in range(N_SLOTS)],
        "lower_energy": [list(LOWER), list(LOWER)],
        "upper_energy": [list(UPPER), list(UPPER)],
        "diff_valid": [[[300] * 13, [300] * 13] for _ in range(N_SLOTS)],
        "int_valid": [[300, 300] for _ in range(N_SLOTS)],
        "yaw_flip": [0] * N_SLOTS,
        "n_sensors": 2,
    }
    raw.update(overrides)
    return raw


def normalize(raw):
    return bf.normalize_day(raw, "https://example.invalid/%s" % raw["file_id"],
                            "ab" * 32, None)


def write_day(root, day=DAY, sat="g18", version="v3-0-3", valid=N_SLOTS):
    raw = make_raw(day=day, sat=sat, version=version)
    if valid < N_SLOTS:
        for t in range(valid, N_SLOTS):
            for sensor in range(2):
                raw["diff"][t][sensor] = [FILL] * 13
                raw["integral"][t][sensor] = FILL
    artifact = normalize(raw)
    path = bf.artifact_path(root, sat, day, version)
    bf.write_artifact(path, bf.canonical_bytes(artifact))
    return artifact, path


def snapshot(root):
    out = {}
    for dirpath, _dirs, files in os.walk(root):
        for name in files:
            path = os.path.join(dirpath, name)
            with open(path, "rb") as fh:
                out[os.path.relpath(path, root)] = hashlib.sha256(
                    fh.read()).hexdigest()
    return out


def _clear_slots(raw):
    for t in range(N_SLOTS):
        for sensor in range(2):
            raw["diff"][t][sensor] = [FILL] * 13
            raw["integral"][t][sensor] = FILL
    raw["diff_valid"] = [[[0] * 13, [0] * 13] for _ in range(N_SLOTS)]
    raw["int_valid"] = [[0, 0] for _ in range(N_SLOTS)]


def _set_slot(raw, t, sensor, row, p11):
    raw["diff"][t][sensor] = list(row)
    raw["integral"][t][sensor] = p11


class FakeNode(object):
    def __init__(self, data):
        self._data = data

    def __getitem__(self, key):
        return self._data


class FakeSource(object):
    def __init__(self, datasets, attrs):
        self._d = datasets
        self.attrs = attrs

    def __contains__(self, key):
        return key in self._d

    def __getitem__(self, key):
        return self._d[key]


class BackfillTest(unittest.TestCase):

    # -- T1 -----------------------------------------------------------------
    def test_t01_j2000_epoch(self):
        self.assertEqual(day_j2000(DAY), 842270400)
        self.assertEqual(bf.start_time_of(make_raw()), T0)
        self.assertEqual(bf.J2000_EPOCH_S, 946728000)

    # -- T2 -----------------------------------------------------------------
    def test_t02_pick_version_numeric(self):
        html = (
            '<a href="?C=N;O=D">Name</a>\n'
            '<a href="../../">Parent Directory</a>\n'
            '<a href="sci_sgps-l2-avg5m_g18_d20260910_v3-0-3.nc">a</a>\n'
            '<a href="sci_sgps-l2-avg5m_g18_d20260910_v3-0-10.nc">b</a>\n'
            '<a href="sci_sgps-l2-avg5m_g18_d20260910_v3-0-2.nc">c</a>\n'
            '<a href="notas.txt">d</a>\n'
            '<a href="sci_sgps-l2-avg5m_g18_d20260911_v3-0-3.nc">e</a>\n'
        )
        names = bf.parse_listing(html)
        self.assertEqual(names, [
            "sci_sgps-l2-avg5m_g18_d20260910_v3-0-3.nc",
            "sci_sgps-l2-avg5m_g18_d20260910_v3-0-10.nc",
            "sci_sgps-l2-avg5m_g18_d20260910_v3-0-2.nc",
            "sci_sgps-l2-avg5m_g18_d20260911_v3-0-3.nc",
        ])
        self.assertEqual(bf.parse_listing('<a href="?C=N;O=D">x</a>'), [])
        single = [names[0]]
        self.assertEqual(bf.pick_version([]), None)
        self.assertEqual(bf.pick_version(single), names[0])
        same_day = names[:3]
        self.assertEqual(
            bf.pick_version(same_day),
            "sci_sgps-l2-avg5m_g18_d20260910_v3-0-10.nc")

    # -- T3 -----------------------------------------------------------------
    def test_t03_legacy_variable_names(self):
        raw = make_raw()
        attrs = {"id": raw["file_id"], "platform": "g18",
                 "time_coverage_start": raw["time_coverage_start"]}

        def datasets(time_name, yaw_name):
            return {
                time_name: FakeNode(raw["time"]),
                yaw_name: FakeNode(raw["yaw_flip"]),
                "AvgDiffProtonFlux": FakeNode(raw["diff"]),
                "AvgIntProtonFlux": FakeNode(raw["integral"]),
                "DiffProtonLowerEnergy": FakeNode(raw["lower_energy"]),
                "DiffProtonUpperEnergy": FakeNode(raw["upper_energy"]),
                "DiffValidL1bSamplesInAvg": FakeNode(raw["diff_valid"]),
                "IntValidL1bSamplesInAvg": FakeNode(raw["int_valid"]),
            }

        old = bf.read_source(FakeSource(
            datasets("L2_SciData_TimeStamp", "YawFlipFlag"), attrs))
        new = bf.read_source(FakeSource(
            datasets("time", "yaw_flip_flag"), attrs))
        self.assertEqual(old, new)
        self.assertEqual(old["time"][0], 842270400.0)
        self.assertEqual(old["yaw_flip"][0], 0)
        with self.assertRaises(bf.SchemaError):
            bf.read_source(FakeSource({}, attrs))

    # -- T4 -----------------------------------------------------------------
    def test_t04_channel_order(self):
        artifact = normalize(make_raw())
        self.assertEqual([c["name"] for c in artifact["channels"]],
                         EXPECTED_CHANNELS)
        self.assertEqual(len(artifact["channels"]), 13)
        self.assertEqual(artifact["channels"][0]["lo_keV"], 100.0)
        self.assertEqual(artifact["channels"][0]["hi_keV"], 150.0)

    # -- T5 -----------------------------------------------------------------
    def test_t05_clean_value(self):
        self.assertIsNone(bf.clean_value(FILL))
        self.assertIsNone(bf.clean_value(float("nan")))
        self.assertIsNone(bf.clean_value(float("inf")))
        self.assertIsNone(bf.clean_value(float("-inf")))
        self.assertIsNone(bf.clean_value(-5.0))
        self.assertIsNone(bf.clean_value(-0.5))
        self.assertIsNone(bf.clean_value(-1e-7))
        self.assertIsNone(bf.clean_value(None))
        self.assertEqual(bf.clean_value(1.2345678), 1.234568)
        self.assertEqual(bf.clean_value(0.0), 0.0)
        self.assertEqual(bf.clean_value(2.0692908719865954e-08), 2.069291e-08)
        self.assertEqual(bf.clean_value(2.7708603056453285e-07), 2.77086e-07)
        self.assertEqual(bf.clean_value(1.0e-12), 1.0e-12)

        # No-regresion: ningun positivo distinto de cero puede salir como 0.0.
        for value in (1e-30, 1e-12, 2.0692908719865954e-08,
                      2.7708603056453285e-07, 1.2345678, 1.0, 1e30):
            self.assertNotEqual(bf.clean_value(value), 0.0)

    # -- T6 -----------------------------------------------------------------
    def test_t06_schema_failures(self):
        raw = make_raw(time=make_raw()["time"][:287])
        with self.assertRaises(bf.SchemaError):
            normalize(raw)

        times = [842270400.0 + 600.0 * i for i in range(N_SLOTS)]
        with self.assertRaises(bf.SchemaError):
            normalize(make_raw(time=times))

        raw = make_raw()
        raw["diff"] = [[[1.0] * 12 for _ in range(2)] for _ in range(N_SLOTS)]
        raw["lower_energy"] = [[100.0] * 12 for _ in range(2)]
        raw["upper_energy"] = [[150.0] * 12 for _ in range(2)]
        with self.assertRaises(bf.SchemaError):
            normalize(raw)

        raw = make_raw()
        raw["lower_energy"][0][0] = raw["upper_energy"][0][0]
        with self.assertRaises(bf.SchemaError):
            normalize(raw)

    # -- T7 -----------------------------------------------------------------
    def test_t07_sensor_tiebreak_rules(self):
        raw = make_raw()
        _clear_slots(raw)
        for t in range(5):
            _set_slot(raw, t, 0, [1.0] * 13, 1.0)
        for t in range(6):
            _set_slot(raw, t, 1, [1.0] * 13, 1.0)
        self.assertEqual(bf.select_sensor(raw)[0], 1)

        raw = make_raw()
        _clear_slots(raw)
        for t in range(5):
            row = [1.0] * 13
            row[0] = FILL
            _set_slot(raw, t, 1, row, 1.0)
        self.assertEqual(bf.select_sensor(raw)[0], 1)

        raw = make_raw()
        raw["diff_valid"] = [[[1] * 13, [2] * 13] for _ in range(N_SLOTS)]
        raw["int_valid"] = [[1, 2] for _ in range(N_SLOTS)]
        self.assertEqual(bf.select_sensor(raw)[0], 1)

        raw = make_raw()
        self.assertEqual(bf.select_sensor(raw)[0], 0)

    # -- T8 -----------------------------------------------------------------
    def test_t08_time_coverage_conflict(self):
        raw = make_raw(time_coverage_start="2026-09-10T06:00:00.000Z")
        with self.assertRaises(bf.SchemaError):
            normalize(raw)
        ok = make_raw(time_coverage_start="2026-09-10T00:00:00Z")
        self.assertEqual(normalize(ok)["start_time"], T0)

    # -- T9 -----------------------------------------------------------------
    def test_t09_exclusive_writer(self):
        root = tempfile.mkdtemp()
        path = os.path.join(root, "artifact.json")
        payload = bf.canonical_bytes({"a": 1, "b": 2})
        same = bf.canonical_bytes({"b": 2, "a": 1})
        different = bf.canonical_bytes({"a": 3, "b": 2})

        self.assertEqual(bf.write_artifact(path, payload), "created")
        self.assertEqual(_read(path), payload)

        mtime = os.stat(path).st_mtime_ns
        self.assertEqual(bf.write_artifact(path, same), "already_present")
        self.assertEqual(os.stat(path).st_mtime_ns, mtime)
        self.assertEqual(_read(path), payload)

        with self.assertRaises(bf.HashConflict):
            bf.write_artifact(path, different)
        self.assertEqual(_read(path), payload)

    # -- T10 ----------------------------------------------------------------
    def test_t10_revisions_coexist(self):
        root = tempfile.mkdtemp()
        _a, p2 = write_day(root, version="v3-0-2")
        _b, p3 = write_day(root, version="v3-0-3")
        self.assertNotEqual(p2, p3)
        self.assertTrue(os.path.exists(p2))
        self.assertTrue(os.path.exists(p3))
        manifest = bf.build_manifest(
            root, {"range": {"from": DAY, "to": DAY}}, T0)
        candidates = manifest["days"][DAY]["candidates"]
        self.assertEqual(len(candidates), 2)
        self.assertEqual(sum(1 for c in candidates if c["recommended"]), 1)
        self.assertEqual({c["path"] for c in candidates},
                         {os.path.relpath(p2, root), os.path.relpath(p3, root)})
        versions = sorted(c["source_version"] for c in candidates)
        self.assertEqual(versions, ["v3-0-2", "v3-0-3"])

    # -- T11 ----------------------------------------------------------------
    def test_t11_manifest_bytes_stable(self):
        root = tempfile.mkdtemp()
        write_day(root)
        rng = {"range": {"from": DAY, "to": DAY}}
        bf.build_manifest(root, rng, "2026-01-01T00:00:00Z")
        path = os.path.join(root, "ncei", "manifest.json")
        first = _read(path)
        manifest = bf.build_manifest(root, rng, "2026-06-06T00:00:00Z")
        second = _read(path)
        self.assertEqual(first, second)
        self.assertEqual(manifest["generated_at"], "2026-06-06T00:00:00Z")

    # -- T12 ----------------------------------------------------------------
    def test_t12_day_thresholds(self):
        root = tempfile.mkdtemp()
        write_day(root, day="2026-09-10", valid=274)
        write_day(root, day="2026-09-11", valid=273)
        manifest = bf.build_manifest(
            root, {"range": {"from": "2026-09-10", "to": "2026-09-12"}}, T0)
        self.assertEqual(manifest["days"]["2026-09-10"]["status"], "complete")
        self.assertEqual(manifest["days"]["2026-09-11"]["status"], "partial")
        self.assertNotIn("2026-09-12", manifest["days"])
        self.assertEqual(manifest["missing_days"], ["2026-09-12"])

    # -- T13 ----------------------------------------------------------------
    def test_t13_satellite_isolation(self):
        root = tempfile.mkdtemp()
        name = "sci_sgps-l2-avg5m_g19_d20260910_v3-0-3.nc"
        listings = {"g19": '<a href="%s">x</a>' % name}
        fetch = _make_fetch(listings, b"NC-BYTES", fail=("g18",))
        read = lambda path: make_raw(sat="g19")

        report = bf.import_range(root, DAY, DAY, ["g18", "g19"], fetch,
                                 "2026-09-11T00:00:00Z", read=read,
                                 sleep=lambda _s: None)
        self.assertEqual(report.downloaded, 1)
        self.assertTrue(any(e[0] == "g18" for e in report.errors))
        self.assertTrue(os.path.exists(
            bf.artifact_path(root, "g19", DAY, "v3-0-3")))
        self.assertFalse(os.path.exists(
            bf.artifact_path(root, "g18", DAY, "v3-0-3")))

        with contextlib.redirect_stdout(io.StringIO()), \
                contextlib.redirect_stderr(io.StringIO()):
            code = bf.main([root, "--from", DAY, "--to", DAY,
                            "--satellites", "g18,g19"],
                           fetch=fetch, read=read, now="2026-09-11T00:00:00Z")
        self.assertEqual(code, 0)

    # -- T14 ----------------------------------------------------------------
    def test_t14_import_without_h5py(self):
        self.assertIn("backfill_ncei", sys.modules)
        self.assertNotIn("h5py", sys.modules)
        result = subprocess.run(
            [sys.executable, "-c",
             "import backfill_ncei, sys; assert 'h5py' not in sys.modules"],
            cwd=REPO_ROOT, capture_output=True)
        self.assertEqual(result.returncode, 0, result.stderr)

    # -- T15 ----------------------------------------------------------------
    def test_t15_idempotent_second_run(self):
        root = tempfile.mkdtemp()
        name = "sci_sgps-l2-avg5m_g18_d20260910_v3-0-3.nc"
        listings = {"g18": '<a href="%s">x</a>' % name}
        fetch = _make_fetch(listings, b"NC-BYTES")
        read = lambda path: make_raw(sat="g18")

        first = bf.import_range(root, DAY, DAY, ["g18"], fetch,
                                "2026-09-11T00:00:00Z", read=read,
                                sleep=lambda _s: None)
        tree1 = snapshot(root)
        second = bf.import_range(root, DAY, DAY, ["g18"], fetch,
                                 "2026-09-12T00:00:00Z", read=read,
                                 sleep=lambda _s: None)
        tree2 = snapshot(root)
        self.assertEqual(first.files_created, 1)
        self.assertEqual(second.downloaded, 0)
        self.assertEqual(second.already_present, 1)
        self.assertEqual(tree1, tree2)

    # -- T16 ----------------------------------------------------------------
    def test_t16_diff_complete_p11_missing(self):
        raw = make_raw()
        for t in range(10):
            for sensor in range(2):
                raw["integral"][t][sensor] = FILL

        metrics = bf.sensor_metrics(raw, 0)
        self.assertEqual(metrics["joint_valid_slots"], N_SLOTS - 10)
        self.assertEqual(metrics["valid_hard_slots"], N_SLOTS - 10)

        artifact = normalize(raw)
        self.assertEqual(artifact["sensor"], 0)
        self.assertEqual(artifact["coverage"]["valid_p11_slots"], N_SLOTS - 10)
        self.assertEqual(artifact["coverage"]["valid_diff_slots"], N_SLOTS)
        self.assertEqual(artifact["coverage"]["missing_p11_slots"],
                         list(range(10)))
        self.assertEqual(artifact["coverage"]["missing_diff_slots"], [])

    # -- T17 ----------------------------------------------------------------
    def test_t17_partial_diff_coverage(self):
        raw = make_raw()
        for t in range(5):
            for sensor in range(2):
                row = [FILL] * 13
                row[0] = 1.0
                raw["diff"][t][sensor] = row
                raw["integral"][t][sensor] = 1.0

        metrics = bf.sensor_metrics(raw, 0)
        self.assertEqual(metrics["joint_valid_slots"], N_SLOTS - 5)
        self.assertEqual(metrics["valid_hard_slots"], N_SLOTS - 5)

        artifact = normalize(raw)
        self.assertEqual(artifact["sensor"], 0)
        self.assertEqual(artifact["coverage"]["valid_diff_slots"], N_SLOTS - 5)
        self.assertEqual(artifact["coverage"]["missing_diff_slots"],
                         [0, 1, 2, 3, 4])

        hard_gap = make_raw()
        for t in (100, 101, 102):
            for sensor in range(2):
                row = [FILL] * 13
                for c in range(7):
                    row[c] = 1.0
                hard_gap["diff"][t][sensor] = row
                hard_gap["integral"][t][sensor] = 1.0
        gap = bf.sensor_metrics(hard_gap, 0)
        self.assertEqual(gap["joint_valid_slots"], N_SLOTS - 3)
        self.assertEqual(gap["valid_hard_slots"], N_SLOTS - 3)

        soft_gap = make_raw()
        for t in (100, 101, 102):
            for sensor in range(2):
                row = [FILL] * 13
                for c in range(8, 13):
                    row[c] = 1.0
                soft_gap["diff"][t][sensor] = row
                soft_gap["integral"][t][sensor] = 1.0
        gap = bf.sensor_metrics(soft_gap, 0)
        self.assertEqual(gap["joint_valid_slots"], N_SLOTS - 3)
        self.assertEqual(gap["valid_hard_slots"], N_SLOTS)

    # -- T18 ----------------------------------------------------------------
    def test_t18_channel_energy_order(self):
        lower = [100.0 * (c + 1) for c in range(13)]
        upper = [lo + 50.0 for lo in lower]
        raw = make_raw(lower_energy=[list(lower), list(lower)],
                       upper_energy=[list(upper), list(upper)])
        artifact = normalize(raw)
        self.assertEqual([c["name"] for c in artifact["channels"]],
                         EXPECTED_CHANNELS)
        self.assertEqual(bf.CH_NAMES, EXPECTED_CHANNELS)
        for index, name in enumerate(EXPECTED_CHANNELS):
            channel = artifact["channels"][index]
            self.assertEqual(channel["name"], name)
            self.assertEqual(channel["lo_keV"], lower[index])
            self.assertEqual(channel["hi_keV"], upper[index])
        self.assertEqual(artifact["channels"][8]["name"], "P8A")
        self.assertEqual(artifact["channels"][8]["lo_keV"], 900.0)
        self.assertEqual(artifact["channels"][9]["name"], "P8B")
        self.assertEqual(artifact["channels"][9]["lo_keV"], 1000.0)

    # -- T19 ----------------------------------------------------------------
    def test_t19_diff_complete_p11_dead(self):
        root = tempfile.mkdtemp()
        raw = make_raw()
        for t in range(20):
            for sensor in range(2):
                raw["integral"][t][sensor] = FILL

        artifact = normalize(raw)
        self.assertEqual(artifact["coverage"]["valid_diff_slots"], N_SLOTS)
        self.assertEqual(artifact["coverage"]["valid_p11_slots"], N_SLOTS - 20)
        path = bf.artifact_path(root, "g18", DAY, "v3-0-3")
        bf.write_artifact(path, bf.canonical_bytes(artifact))

        manifest = bf.build_manifest(
            root, {"range": {"from": DAY, "to": DAY}}, T0)
        self.assertEqual(manifest["days"][DAY]["status"], "partial")
        candidate = manifest["days"][DAY]["candidates"][0]
        self.assertEqual(candidate["valid_diff_slots"], N_SLOTS)
        self.assertEqual(candidate["valid_p11_slots"], N_SLOTS - 20)

    # -- T20 ----------------------------------------------------------------
    def test_t20_sensor_primary_is_joint_not_hard(self):
        raw = make_raw()
        _clear_slots(raw)
        # Sensor 0: 100 franjas con los 13 diferenciales + P11 => joint=100, hard=100.
        for t in range(100):
            _set_slot(raw, t, 0, [1.0] * 13, 1.0)
        # Sensor 1: 200 franjas con P1 a FILL y P2A..P10 + P11 => joint=0, hard=200.
        row = [1.0] * 13
        row[0] = FILL
        for t in range(200):
            _set_slot(raw, t, 1, row, 1.0)

        m0 = bf.sensor_metrics(raw, 0)
        m1 = bf.sensor_metrics(raw, 1)
        self.assertEqual(m0["joint_valid_slots"], 100)
        self.assertEqual(m0["valid_hard_slots"], 100)
        self.assertEqual(m1["joint_valid_slots"], 0)
        self.assertEqual(m1["valid_hard_slots"], 200)

        # Con el criterio conjunto (joint) gana el sensor 0; si el primario
        # fuese el duro (hard) ganaria el sensor 1.
        self.assertEqual(bf.select_sensor(raw)[0], 0)

    # -- T21 ----------------------------------------------------------------
    def test_t21_diff_below_threshold_p11_healthy(self):
        root = tempfile.mkdtemp()
        raw = make_raw()
        # 15 franjas con un solo canal diferencial (P1) a FILL, integral intacto.
        for t in range(15):
            for sensor in range(2):
                raw["diff"][t][sensor][0] = FILL

        artifact = normalize(raw)
        self.assertEqual(artifact["coverage"]["valid_diff_slots"], 273)
        self.assertEqual(artifact["coverage"]["valid_p11_slots"], N_SLOTS)
        path = bf.artifact_path(root, "g18", DAY, "v3-0-3")
        bf.write_artifact(path, bf.canonical_bytes(artifact))

        manifest = bf.build_manifest(
            root, {"range": {"from": DAY, "to": DAY}}, T0)
        self.assertEqual(manifest["days"][DAY]["status"], "partial")


def _read(path):
    with open(path, "rb") as fh:
        return fh.read()


def _make_fetch(listings, nc_bytes, fail=()):
    def fetch(url):
        for sat, directory in bf.NCEI_DIR.items():
            if directory not in url:
                continue
            if sat in fail:
                raise OSError("red caída (%s)" % sat)
            if url.endswith("/"):
                return listings.get(sat, "").encode("utf-8")
            return nc_bytes
        raise OSError("URL desconocida: %s" % url)
    return fetch


if __name__ == "__main__":
    unittest.main()
