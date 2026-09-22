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

    def test_observations_do_not_wait_for_writer_lock_and_writers_are_serialized(self):
        writer_jobs = {
            "collect-distributed-v9.yml": {"aggregate", "merge_discovery"},
            "adaptive-sampling-v9.yml": {"commit"},
            "midnight-anchor-v9.yml": {"commit"},
            "recover-distributed-v9.yml": {"recover"},
            "calendar-finalize-v9.yml": {"finalize"},
        }
        observation_jobs = {
            "collect-distributed-v9.yml": {"prepare", "discover", "collect"},
            "adaptive-sampling-v9.yml": {"collect"},
            "midnight-anchor-v9.yml": {"collect"},
        }

        for filename in ACTIVE_WORKFLOWS:
            source = (ROOT / ".github/workflows" / filename).read_text()
            self.assertNotIn("Wait for older Musinsa workflows (FIFO)", source)

            data = yaml.safe_load(source)
            self.assertNotIn("concurrency", data)
            jobs = data["jobs"]

            for job_name in writer_jobs[filename]:
                with self.subTest(workflow=filename, writer=job_name):
                    job = jobs[job_name]
                    concurrency = job.get("concurrency", {})
                    self.assertEqual(concurrency.get("group"), "musinsa-data-writer")
                    self.assertEqual(concurrency.get("queue"), "max")
                    self.assertFalse(concurrency.get("cancel-in-progress"))

                    steps = job["steps"]
                    checkout = next(
                        s for s in steps
                        if s.get("uses", "").startswith("actions/checkout@")
                    )
                    self.assertEqual(checkout["with"]["ref"], "${{ github.ref_name }}")
                    self.assertEqual(checkout["with"]["fetch-depth"], 1)

            for job_name in observation_jobs.get(filename, set()):
                with self.subTest(workflow=filename, observer=job_name):
                    self.assertNotIn("concurrency", jobs[job_name])

    def test_recovery_yields_to_stale_phase_and_midnight_anchor(self):
        source = (ROOT / ".github/workflows" / "recover-distributed-v9.yml").read_text()
        self.assertIn("cron: '30 2,5,8,11,14,17,20,23 * * *'", source)
        self.assertIn("now.hour % 3 == 2", source)
        self.assertIn("hour=23, minute=55", source)
        self.assertIn("minutes_to_next_critical", source)
        self.assertIn("Yield to active priority observations", source)
        self.assertIn('.name == "collect"', source)
        self.assertIn("steps.priority.outputs.safe == '1'", source)

    def test_primary_observation_does_not_wait_for_slow_discovery(self):
        data = yaml.safe_load(
            (ROOT / ".github/workflows" / "collect-distributed-v9.yml").read_text()
        )
        jobs = data["jobs"]

        self.assertEqual(jobs["collect"]["needs"], "prepare")
        self.assertEqual(set(jobs["aggregate"]["needs"]), {"prepare", "collect"})
        self.assertNotIn("discover", jobs["aggregate"]["needs"])
        self.assertEqual(
            set(jobs["merge_discovery"]["needs"]),
            {"prepare", "discover"},
        )

        collect_source = json.dumps(jobs["collect"], ensure_ascii=False)
        self.assertIn("v9-base-state-", collect_source)
        self.assertNotIn("v9-discovery-state", collect_source)

    def test_primary_aggregate_does_not_copy_stale_lifecycle_tree(self):
        source = (
            ROOT / ".github/workflows" / "collect-distributed-v9.yml"
        ).read_text()
        self.assertNotIn("cp -R run_state/lifecycle data/", source)
        self.assertIn(
            "aggregate-slot --state-dir run_state --shard-dir shard_results",
            source,
        )

    def test_primary_workflow_passes_pinned_snapshot_date(self):
        source = (ROOT / ".github/workflows" / "collect-distributed-v9.yml").read_text()
        self.assertIn("Resolve intended KST snapshot date", source)
        self.assertIn(
            '--snapshot-date "${{ needs.prepare.outputs.snapshot_date }}"',
            source,
        )
        self.assertIn(
            'printf \'%s\\n\' "${{ steps.snapshot.outputs.snapshot_date }}" > run_state/snapshot_date.txt',
            source,
        )

    def test_delayed_slot7_keeps_previous_kst_snapshot_date(self):
        data = yaml.safe_load(
            (ROOT / ".github/workflows" / "collect-distributed-v9.yml").read_text()
        )
        prepare = data["jobs"]["prepare"]
        step = next(s for s in prepare["steps"] if s.get("id") == "snapshot")
        match = re.search(r"python - <<'PY2'\n(.*?)\nPY2", step["run"], re.S)
        self.assertIsNotNone(match)

        code = match[1]
        code = code.replace(
            "now = datetime.now(KST)",
            "now = datetime(2026, 9, 23, 0, 5, tzinfo=KST)",
        )
        code = code.replace("${{ steps.slot.outputs.slot }}", "7")

        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / "output"
            env = {
                **os.environ,
                "GITHUB_OUTPUT": str(output),
                "GITHUB_EVENT_NAME": "schedule",
            }
            subprocess.run(
                [sys.executable, "-c", code],
                env=env,
                check=True,
                capture_output=True,
            )
            self.assertEqual(
                output.read_text().strip(),
                "snapshot_date=2026-09-22",
            )

    def test_all_shared_data_writers_use_the_same_non_canceling_queue(self):
        writer_files = (
            "collect-distributed-v9.yml",
            "adaptive-sampling-v9.yml",
            "recover-distributed-v9.yml",
            "midnight-anchor-v9.yml",
            "calendar-finalize-v9.yml",
            "canonicalize-musinsa-brands-v9.yml",
            "initialize-catalog-v9.yml",
            "repair-catalog-v9.yml",
        )
        for filename in writer_files:
            with self.subTest(workflow=filename):
                source = (ROOT / ".github/workflows" / filename).read_text()
                self.assertIn("group: musinsa-data-writer", source)
                self.assertIn("queue: max", source)
                self.assertIn("cancel-in-progress: false", source)

    def test_multislot_backfill_waits_for_scheduled_and_manual_primary_runs(self):
        source = (
            ROOT / ".github/workflows" / "multi-slot-backfill-v9.yml"
        ).read_text()
        self.assertIn("list_all_runs()", source)
        self.assertIn("list_manual_runs()", source)
        self.assertIn("list_all_runs | python -c", source)
        self.assertIn("list_manual_runs > /tmp/runs.json", source)

    def test_scheduled_primary_and_adaptive_slots_are_pinned_to_cron(self):
        cases = {
            "collect-distributed-v9.yml": {
                "15 15 * * *": 0, "15 18 * * *": 1,
                "15 21 * * *": 2, "15 0 * * *": 3,
                "15 3 * * *": 4, "15 6 * * *": 5,
                "15 9 * * *": 6, "15 12 * * *": 7,
            },
            "adaptive-sampling-v9.yml": {
                "45 16 * * *": 0, "45 19 * * *": 1,
                "45 22 * * *": 2, "45 1 * * *": 3,
                "45 4 * * *": 4, "45 7 * * *": 5,
                "45 10 * * *": 6, "45 13 * * *": 7,
            },
        }

        for filename, schedule_map in cases.items():
            data = yaml.safe_load((ROOT / ".github/workflows" / filename).read_text())
            first_job = next(iter(data["jobs"].values()))
            step = next(s for s in first_job["steps"] if s.get("id") == "slot")
            match = re.search(r"python - <<'PY2'\n(.*?)\nPY2", step["run"], re.S)
            self.assertIsNotNone(match)

            for schedule, expected in schedule_map.items():
                with self.subTest(workflow=filename, schedule=schedule), tempfile.TemporaryDirectory() as tmp:
                    code = match[1]
                    output = Path(tmp) / "output"
                    env = {
                        **os.environ,
                        "GITHUB_OUTPUT": str(output),
                        "GITHUB_EVENT_NAME": "schedule",
                        "EVENT_SCHEDULE": schedule,
                    }
                    subprocess.run(
                        [sys.executable, "-c", code],
                        env=env,
                        check=True,
                        capture_output=True,
                    )
                    self.assertEqual(
                        output.read_text().strip(),
                        f"slot={expected}",
                    )


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

    def test_discovery_merge_never_overwrites_newer_root_state(self):
        state = self.base / "run_state"
        state.mkdir(parents=True, exist_ok=True)

        def catalog_row(goods_no, seen, price, first_seen, lifecycle_checked, lifecycle_type):
            return {
                "goods_no": str(goods_no),
                "brand_name": "brand-a" if str(goods_no) == "100" else "brand-b",
                "product_name": f"goods-{goods_no}",
                "normal_price": price,
                "current_price": price,
                "sale_rate": 0,
                "review_count": 0,
                "rating": "",
                "availability": "SALE",
                "first_seen_at": first_seen,
                "last_seen_at": seen,
                "lifecycle_status": "reactivated_pretracker" if lifecycle_type else "",
                "lifecycle_evidence_type": lifecycle_type,
                "lifecycle_evidence_date": "2026-08-01" if lifecycle_type else "",
                "lifecycle_checked_at": lifecycle_checked,
                "product_url": f"https://www.musinsa.com/products/{goods_no}",
            }

        root_100 = catalog_row(
            100,
            "2026-09-22T14:00:00+09:00",
            39000,
            "2026-09-01T09:00:00+09:00",
            "2026-09-22T14:00:00+09:00",
            "root-newer",
        )
        stale_100 = catalog_row(
            100,
            "2026-09-22T13:00:00+09:00",
            35000,
            "2026-09-02T09:00:00+09:00",
            "2026-09-22T13:00:00+09:00",
            "state-stale",
        )
        new_200 = catalog_row(
            200,
            "2026-09-22T15:00:00+09:00",
            32000,
            "2026-09-22T15:00:00+09:00",
            "2026-09-22T15:00:00+09:00",
            "state-new",
        )

        collector.write_csv(collector.CATALOG_FILE, [root_100], collector.CATALOG_FIELDS)
        collector.write_csv(
            state / "musinsa_catalog.csv",
            [stale_100, new_200],
            collector.CATALOG_FIELDS,
        )
        collector.write_lines(collector.WATCHLIST_FILE, ["100", "999"])
        collector.write_lines(state / "musinsa_watchlist.txt", ["100", "200"])

        root_audit = {
            field: "" for field in collector.BRAND_AUDIT_FIELDS
        }
        root_audit.update({
            "checked_at": "2026-09-22T14:00:00+09:00",
            "requested_brand": "brand-a",
            "status": "root-newer",
        })
        stale_audit = dict(root_audit)
        stale_audit.update({
            "checked_at": "2026-09-22T13:00:00+09:00",
            "status": "state-stale",
        })
        new_audit = {
            field: "" for field in collector.BRAND_AUDIT_FIELDS
        }
        new_audit.update({
            "checked_at": "2026-09-22T15:00:00+09:00",
            "requested_brand": "brand-b",
            "status": "state-new",
        })
        collector.write_csv(
            collector.BRAND_AUDIT_FILE,
            [root_audit],
            collector.BRAND_AUDIT_FIELDS,
        )
        collector.write_csv(
            state / "musinsa_brand_audit.csv",
            [stale_audit, new_audit],
            collector.BRAND_AUDIT_FIELDS,
        )

        collector.LIFECYCLE_DIR.mkdir(parents=True, exist_ok=True)
        collector.LIFECYCLE_EVIDENCE_FILE.write_text(
            json.dumps({
                "100": {
                    "checked_at": "2026-09-22T14:00:00+09:00",
                    "status": "root-newer",
                },
                "999": {
                    "checked_at": "2026-09-22T12:00:00+09:00",
                    "status": "root-only",
                },
            }),
            encoding="utf-8",
        )
        state_lifecycle = state / "lifecycle"
        state_lifecycle.mkdir(parents=True, exist_ok=True)
        (state_lifecycle / "pretracker_evidence.json").write_text(
            json.dumps({
                "100": {
                    "checked_at": "2026-09-22T13:00:00+09:00",
                    "status": "state-stale",
                },
                "200": {
                    "checked_at": "2026-09-22T15:00:00+09:00",
                    "status": "state-new",
                },
            }),
            encoding="utf-8",
        )

        with patch.object(collector, "_LIFECYCLE_CACHE_MEMORY", None):
            collector.merge_discovery_state_into_root(state)

        merged = {
            r["goods_no"]: r for r in collector.read_csv(collector.CATALOG_FILE)
        }
        self.assertEqual(merged["100"]["current_price"], "39000")
        self.assertEqual(
            merged["100"]["lifecycle_evidence_type"],
            "root-newer",
        )
        self.assertEqual(
            merged["100"]["first_seen_at"],
            "2026-09-01T09:00:00+09:00",
        )
        self.assertEqual(merged["200"]["current_price"], "32000")
        self.assertEqual(
            collector.read_lines(collector.WATCHLIST_FILE),
            ["100", "200", "999"],
        )

        audits = {
            r["requested_brand"]: r
            for r in collector.read_csv(collector.BRAND_AUDIT_FILE)
        }
        self.assertEqual(audits["brand-a"]["status"], "root-newer")
        self.assertEqual(audits["brand-b"]["status"], "state-new")

        lifecycle = json.loads(
            collector.LIFECYCLE_EVIDENCE_FILE.read_text(encoding="utf-8")
        )
        self.assertEqual(lifecycle["100"]["status"], "root-newer")
        self.assertEqual(lifecycle["200"]["status"], "state-new")
        self.assertEqual(lifecycle["999"]["status"], "root-only")

    def test_discover_slot_honors_pinned_snapshot_date(self):
        state = self.base / "run_state"
        collector.write_lines(collector.BRANDS_FILE, [])
        self.assertEqual(
            collector.discover_slot(
                state,
                7,
                snapshot_date="2026-09-13",
            ),
            0,
        )
        self.assertEqual(
            (state / "snapshot_date.txt").read_text().strip(),
            "2026-09-13",
        )

    def test_recovery_rotates_across_large_queues(self):
        date_text = self.now.date().isoformat()
        folder = collector.RECOVERY_DIR / date_text
        paths = []
        for slot in (0, 1):
            path = folder / f"slot-{slot}-failed.csv"
            rows = [
                {
                    "date": date_text,
                    "slot": slot,
                    "goods_no": str(slot * 1000 + i + 1),
                    "brand_name": "test-brand",
                    "product_name": "test knit",
                    "first_failed_at": self.now.isoformat(),
                    "last_failed_at": self.now.isoformat(),
                    "attempts": 1,
                    "last_error": "mock",
                    "current_price": 35000,
                    "product_url": "",
                }
                for i in range(30)
            ]
            collector.write_csv(path, rows, collector.FAILURE_FIELDS)
            paths.append(path)

        calls = []
        counts = {str(path): 0 for path in paths}

        def fake_recover(path, **kwargs):
            path = Path(path)
            key = str(path)
            counts[key] += 1
            calls.append(path.name)
            slot = int(path.name.split("-")[1])
            return {
                "queue": key,
                "date": date_text,
                "slot": slot,
                "attempted": 25,
                "recovered": 25,
                "remaining": 5 if counts[key] == 1 else 0,
                "deferred": 5 if counts[key] == 1 else 0,
            }

        with (
            patch.object(collector.time, "monotonic", return_value=0.0),
            patch.object(collector, "recover_queue_file", side_effect=fake_recover),
            patch.object(collector, "refresh_latest_slot_from_snapshot"),
            patch.object(collector, "rebuild_latest_product_file"),
            patch.object(collector, "rebuild_date_aggregates"),
            patch.object(collector, "write_history_manifest"),
        ):
            self.assertEqual(
                collector.recover_pending(
                    lookback_days=2,
                    max_queues=32,
                    time_budget_minutes=50,
                ),
                0,
            )

        self.assertEqual(
            calls,
            [
                "slot-0-failed.csv",
                "slot-1-failed.csv",
                "slot-0-failed.csv",
                "slot-1-failed.csv",
            ],
        )

    def test_catalog_only_new_product_gets_one_adaptive_baseline(self):
        catalog = {
            **self.catalog,
            "first_seen_at": self.now.isoformat(),
            "last_seen_at": self.now.isoformat(),
            "lifecycle_status": "first_seen_unverified",
        }
        collector.write_csv(
            collector.CATALOG_FILE,
            [catalog],
            collector.CATALOG_FIELDS,
        )
        collector.write_csv(
            collector.LATEST_PRODUCT_FILE,
            [],
            collector.LATEST_FIELDS,
        )
        collector.write_csv(
            collector.CALENDAR_LATEST_PRODUCT_FILE,
            [],
            collector.CALENDAR_PRODUCT_FIELDS,
        )

        row = self.raw(self.now.isoformat(), 100)
        with patch.object(collector, "collect_one", return_value=row) as fetch:
            self.assertEqual(
                collector.collect_adaptive(4, max_products=10),
                0,
            )

        fetch.assert_called_once()
        observations = list(collector.OBSERVATION_DIR.glob("*/*.csv.gz"))
        self.assertEqual(len(observations), 1)
        saved = collector.read_csv(observations[0])
        self.assertEqual(len(saved), 1)
        self.assertEqual(saved[0]["goods_no"], "1000001")
        self.assertEqual(saved[0]["sampling_tier"], "probe_baseline")

        # A second adaptive run sees the saved probe and must not baseline it again.
        with patch.object(collector, "collect_one", return_value=row) as second_fetch:
            self.assertEqual(
                collector.collect_adaptive(5, max_products=10),
                0,
            )
        second_fetch.assert_not_called()

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
