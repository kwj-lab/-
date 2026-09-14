"""Regressions for the September collection outage; all HTTP is mocked."""
import contextlib
import io
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import musinsa_collector_v9 as collector

ROOT = Path(__file__).resolve().parents[1]
ACTIVE_WORKFLOWS = (
    "collect-distributed-v9.yml", "adaptive-sampling-v9.yml",
    "midnight-anchor-v9.yml", "recover-distributed-v9.yml",
    "calendar-finalize-v9.yml",
)


class WorkflowTests(unittest.TestCase):
    def test_slot_output_is_a_real_newline_for_every_slot(self):
        for filename in ACTIVE_WORKFLOWS[:2]:
            data = yaml.safe_load((ROOT / ".github/workflows" / filename).read_text())
            steps = next(iter(data["jobs"].values()))["steps"]
            script = next(s["run"] for s in steps if s.get("id") == "slot")
            match = re.search(r"python - <<'PY2'\n(.*?)\nPY2", script, re.S)
            self.assertIsNotNone(match)
            for slot in range(8):
                with self.subTest(workflow=filename, slot=slot), tempfile.TemporaryDirectory() as tmp:
                    code = match[1]
                    for expression, value in {
                        "${{ inputs.slot }}": str(slot),
                        "${{ inputs.clock_slot }}": str(slot),
                        "${{ github.event_name }}": "workflow_dispatch",
                    }.items():
                        code = code.replace(expression, value)
                    output = Path(tmp) / "output"
                    env = {**os.environ, "GITHUB_OUTPUT": str(output),
                           "GITHUB_EVENT_NAME": "workflow_dispatch"}
                    subprocess.run([sys.executable, "-c", code], env=env,
                                   check=True, capture_output=True)
                    self.assertEqual(output.read_bytes(), f"slot={slot}\n".encode())

    def test_every_active_workflow_command_exists(self):
        commands = set()
        for filename in ACTIVE_WORKFLOWS:
            source = (ROOT / ".github/workflows" / filename).read_text()
            commands.update(re.findall(r"python musinsa_collector_v9.py ([a-z-]+)", source))
        for command in sorted(commands):
            with self.subTest(command=command):
                run = subprocess.run([sys.executable, str(ROOT / "musinsa_collector_v9.py"),
                                      command, "--help"], capture_output=True, text=True)
                self.assertEqual(run.returncode, 0, run.stderr)

    def test_writers_checkout_latest_data_after_fifo(self):
        for filename in ACTIVE_WORKFLOWS:
            data = yaml.safe_load((ROOT / ".github/workflows" / filename).read_text())
            self.assertNotIn("concurrency", data)
            jobs = data["jobs"]
            for job_name, job in jobs.items():
                if job_name == "collect":
                    continue
                steps = job["steps"]
                checkout = next(s for s in steps if s.get("uses", "").startswith("actions/checkout@"))
                self.assertEqual(checkout["with"]["ref"], "${{ github.ref_name }}")
                self.assertEqual(checkout["with"]["fetch-depth"], 1)
                if job_name != "aggregate":
                    self.assertIn("FIFO", steps[0]["name"])


class IsolatedCollectorTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name)
        self.stack = contextlib.ExitStack()
        self.addCleanup(self.stack.close)
        original_base = collector.BASE_DIR
        for name, value in list(vars(collector).items()):
            if isinstance(value, Path) and value.is_relative_to(original_base):
                self.stack.enter_context(patch.object(collector, name, self.base / value.relative_to(original_base)))
        self.now = collector.datetime(2026, 9, 14, 12, tzinfo=collector.KST)
        self.stack.enter_context(patch.object(collector, "now_kst", return_value=self.now))
        self.stack.enter_context(patch.object(collector, "http_get", side_effect=AssertionError("unexpected HTTP")))
        self.stack.enter_context(contextlib.redirect_stdout(io.StringIO()))
        self.catalog = {"goods_no": "1000001", "brand_name": "test-brand",
                        "product_name": "test knit", "first_seen_at": "2026-08-28T00:00:00+09:00",
                        "current_price": "35000", "lifecycle_status": "reactivated_pretracker"}

    def raw(self, timestamp, purchases=100):
        return {**self.catalog, "checked_at": timestamp, "purchase_total": purchases,
                "page_view_total": 1000, "errors": ""}

    def seed_selection(self):
        collector.write_csv(collector.CATALOG_FILE, [self.catalog], collector.CATALOG_FIELDS)
        collector.write_csv(collector.LATEST_PRODUCT_FILE,
                            [{**self.raw("2026-09-14T03:00:00+09:00"), "slot": 1, "daily_sales": 30}],
                            collector.LATEST_FIELDS)

    def test_adaptive_collects_and_calendar_reads_its_observation(self):
        self.seed_selection()
        row = self.raw(self.now.isoformat(), 112)
        with patch.object(collector, "collect_one", return_value=row) as fetch:
            self.assertEqual(collector.collect_adaptive(4, max_products=1), 0)
        fetch.assert_called_once()
        reports = list(collector.ADAPTIVE_REPORT_DIR.glob("*/*.json"))
        self.assertEqual(json.loads(reports[0].read_text())["success"], 1)
        observations = collector.load_calendar_observations(self.now.date())
        self.assertEqual(int(observations["1000001"][0]["purchase_total"]), 112)
        self.assertFalse(collector.SLOT_DIR.exists())

    def test_midnight_collects_without_overwriting_baseline(self):
        self.seed_selection()
        row = self.raw(self.now.isoformat(), 120)
        with patch.object(collector, "collect_one", return_value=row):
            self.assertEqual(collector.collect_midnight_anchor(max_products=1), 0)
        reports = list(collector.MIDNIGHT_REPORT_DIR.glob("*/*.json"))
        self.assertEqual(json.loads(reports[0].read_text())["success"], 1)
        self.assertEqual(len(collector.load_calendar_observations(self.now.date())["1000001"]), 1)
        self.assertFalse(collector.SLOT_DIR.exists())

    def test_all_failed_extra_requests_fail_but_keep_report(self):
        self.seed_selection()
        failed = self.raw(self.now.isoformat(), "")
        with patch.object(collector, "collect_one", return_value=failed):
            self.assertEqual(collector.collect_adaptive(4, max_products=1), 1)
            self.assertEqual(collector.collect_midnight_anchor(max_products=1), 1)
        self.assertEqual(len(list(collector.ADAPTIVE_REPORT_DIR.glob("*/*.json"))), 1)
        self.assertEqual(len(list(collector.MIDNIGHT_REPORT_DIR.glob("*/*.json"))), 1)

    def test_dry_run_never_requests_or_writes_observations(self):
        self.seed_selection()
        with patch.object(collector, "collect_one", side_effect=AssertionError("unexpected collection")):
            self.assertEqual(collector.collect_adaptive(4, max_products=1, dry_run=True), 0)
            self.assertEqual(collector.collect_midnight_anchor(max_products=1, dry_run=True), 0)
        self.assertFalse(collector.OBSERVATION_DIR.exists())

    def test_extra_observation_and_snapshot_are_deduplicated(self):
        day = self.now.date() - collector.timedelta(days=1)
        first = self.raw("2026-09-13T00:00:00+09:00", 100)
        middle = self.raw("2026-09-13T12:00:00+09:00", 112)
        last = self.raw("2026-09-14T00:00:00+09:00", 124)
        collector.write_csv(collector.SLOT_DIR / "slot-1" / f"{day}.csv.gz", [first, middle], collector.COMPACT_FIELDS)
        collector.write_csv(collector.OBSERVATION_DIR / str(day) / "extra.csv.gz", [middle], collector.ADAPTIVE_OBS_FIELDS)
        collector.write_csv(collector.OBSERVATION_DIR / "2026-09-14" / "anchor.csv.gz", [last], collector.ADAPTIVE_OBS_FIELDS)
        observations = collector.load_calendar_observations(day)["1000001"]
        self.assertEqual(len(observations), 3)
        result = collector.estimate_calendar_product(day, "1000001", observations, self.catalog)
        self.assertEqual(result["estimated_sales"], 24)
        self.assertEqual(result["coverage_pct"], 100)

    def test_rerun_archives_old_snapshot_and_keeps_both_times(self):
        path = collector.SLOT_DIR / "slot-1" / "2026-09-14.csv.gz"
        collector.write_csv(path, [self.raw("2026-09-14T03:00:00+09:00", 100)], collector.COMPACT_FIELDS)
        archived = collector.archive_existing_primary_snapshot(path, 1)
        self.assertEqual(len(archived), 1)
        collector.write_csv(path, [self.raw("2026-09-14T09:00:00+09:00", 105)], collector.COMPACT_FIELDS)
        observations = collector.load_calendar_observations(self.now.date())["1000001"]
        self.assertEqual([int(r["purchase_total"]) for r in observations], [100, 105])

    def test_primary_discover_collect_aggregate_preserves_lifecycle(self):
        collector.write_lines(collector.BRANDS_FILE, [self.catalog["brand_name"]])
        collector.write_lines(collector.WATCHLIST_FILE, ["1000001"])
        collector.write_csv(collector.CATALOG_FILE, [self.catalog], collector.CATALOG_FIELDS)
        slot = collector.brand_slot(self.catalog["brand_name"])
        state, shards = self.base / "run_state", self.base / "shards"
        with patch.object(collector, "search_brand_products", return_value=([self.catalog], {})):
            self.assertEqual(collector.discover_slot(state, slot), 0)
        with patch.object(collector, "collect_one", return_value=self.raw(self.now.isoformat(), 120)):
            self.assertEqual(collector.collect_slot_shard(state, slot, 0, 1, shards / "primary.csv"), 0)
        self.assertEqual(collector.aggregate_slot(state, shards), 0)
        coverage = json.loads(collector.COVERAGE_LATEST_FILE.read_text())
        self.assertEqual(coverage["overall"]["success"], 1)
        self.assertEqual(collector.read_csv(collector.CATALOG_FILE)[0]["lifecycle_status"], "reactivated_pretracker")
        self.assertEqual(collector.read_csv(collector.NEW_PRODUCTS_FILE), [])

    def test_no_calendar_data_is_not_full_coverage(self):
        result = collector.finalize_calendar_date(self.now.date() - collector.timedelta(days=1))
        self.assertEqual(result["product_count"], 0)
        self.assertEqual(result["product_coverage_pct"], 0)

    def test_primary_all_failed_still_exports_recovery_input(self):
        state = self.base / "run_state"
        output = self.base / "primary.csv"
        collector.write_lines(state / "musinsa_watchlist.txt", ["1000001"])
        collector.write_csv(state / "musinsa_catalog.csv", [self.catalog], collector.CATALOG_FIELDS)
        slot = collector.brand_slot(self.catalog["brand_name"])
        with patch.object(collector, "collect_one", return_value=self.raw(self.now.isoformat(), "")), patch.object(collector, "INLINE_RETRY_MAX", 0):
            self.assertEqual(collector.collect_slot_shard(state, slot, 0, 1, output), 1)
        self.assertEqual(len(collector.read_csv(output)), 1)


if __name__ == "__main__":
    unittest.main()
