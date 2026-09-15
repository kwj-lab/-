"""Sales attribution regressions: observations are not orders or launch dates."""
import contextlib
import io
from pathlib import Path
import sys
import tempfile
import json
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import musinsa_collector_v9 as c


class SalesBaselineTests(unittest.TestCase):
    def row(self, stamp, purchases, **extra):
        return dict(goods_no="3098417", brand_name="PLAc", current_price=71200,
                    checked_at=stamp, _checked_dt=c.parse_kst_datetime(stamp),
                    purchase_total=purchases, **extra)

    def latest(self, current, prev=None, week=None, month=None):
        return c.build_latest_row(current, prev, week, month,
                                  c.datetime(2026, 9, 14).date(), 3,
                                  {"first_seen_at": "2026-08-28T00:00:00+09:00"})

    def test_missing_daily_baseline_must_not_fall_back_to_week(self):
        result = self.latest(self.row("2026-09-14T11:28:04+09:00", 2459),
                             week=self.row("2026-09-07T10:00:00+09:00", 0))
        self.assertIsNone(result["daily_sales"])
        self.assertIsNone(result["daily_estimated_gmv"])
        self.assertIsNone(result["sales_7d"])

    def test_missing_week_must_not_fall_back_to_month(self):
        self.assertIsNone(c.guarded_purchase_delta(
            self.row("2026-09-14T12:00:00+09:00", 3000), None,
            (self.row("2026-08-14T12:00:00+09:00", 1000),)))

    def test_delayed_recovery_is_not_a_daily_baseline(self):
        result = self.latest(self.row("2026-09-14T12:00:00+09:00", 2600),
                             self.row("2026-09-10T12:00:00+09:00", 100))
        self.assertIsNone(result["daily_sales"])

    def test_old_or_unverified_zero_does_not_certify_no_prior_sales(self):
        for current in (3, 136, 2459):
            with self.subTest(current=current):
                result = self.latest(self.row("2026-09-14T12:00:00+09:00", current),
                                     self.row("2026-09-13T12:00:00+09:00", 0))
                self.assertIsNone(result["daily_sales"])

    def test_zero_zero_remains_valid_and_later_positive_increments_count(self):
        self.assertEqual(self.latest(self.row("2026-09-14T12:00:00+09:00", 0),
                                     self.row("2026-09-13T12:00:00+09:00", 0))["daily_sales"], 0)
        self.assertEqual(self.latest(self.row("2026-09-14T12:00:00+09:00", 2466),
                                     self.row("2026-09-13T12:00:00+09:00", 2459))["daily_sales"], 7)

    def test_unverified_purchase_counter_does_not_erase_independent_metrics(self):
        result = self.latest(self.row("2026-09-14T12:00:00+09:00", 136, page_view_total=1000, review_count=20),
                             self.row("2026-09-13T12:00:00+09:00", 0, page_view_total=980, review_count=18))
        self.assertIsNone(result["daily_sales"])
        self.assertEqual(result["daily_page_view_increase"], 20)
        self.assertEqual(result["daily_review_increase"], 2)

    def test_large_sales_with_valid_established_baseline_are_not_capped(self):
        result = self.latest(self.row("2026-09-14T12:00:00+09:00", 14000),
                             self.row("2026-09-13T12:00:00+09:00", 11000),
                             self.row("2026-09-07T12:00:00+09:00", 2000))
        self.assertEqual(result["daily_sales"], 3000)

    def test_current_jump_must_not_inflate_its_own_historical_threshold(self):
        result = self.latest(self.row("2026-09-14T12:00:00+09:00", 3000),
                             self.row("2026-09-13T12:00:00+09:00", 1000),
                             self.row("2026-09-07T12:00:00+09:00", 999))
        self.assertIsNone(result["daily_sales"])

    def test_multi_day_gap_is_not_calendar_coverage_or_sales(self):
        rows = [self.row("2026-09-10T10:00:00+09:00", 0),
                self.row("2026-09-14T11:43:45+09:00", 6356)]
        result = c.estimate_calendar_product(c.datetime(2026, 9, 13).date(), "3098417", rows)
        self.assertIsNone(result)

    def test_restored_counter_after_many_zero_observations_stays_excluded(self):
        rows = [self.row(f"2026-09-{day:02d}T00:00:00+09:00", value)
                for day, value in [(9, 0), (10, 0), (11, 0), (12, 2459), (13, 2466), (14, 2476)]]
        catalog = {"first_seen_at": "2026-08-28T00:00:00+09:00"}
        before = c.estimate_calendar_product(c.datetime(2026, 9, 11).date(), "3098417", rows, catalog)
        after = c.estimate_calendar_product(c.datetime(2026, 9, 12).date(), "3098417", rows, catalog)
        self.assertIsNone(before)
        self.assertEqual(after["estimated_sales"], 7)

    def test_followup_sales_do_not_prove_disputed_initial_jump_was_sales(self):
        rows = [self.row(f"2026-09-13T{hour:02d}:00:00+09:00", value)
                for hour, value in [(0, 10), (6, 2010), (12, 2210), (18, 2410)]]
        with patch.object(c, "product_confirmed_pretracker", return_value=False):
            clean, state, delta = c.resolve_initial_purchase_jump(
                rows, {"first_seen_at": "2026-09-13T00:00:00+09:00"})
        self.assertNotEqual(clean[0]["_counter_segment"], clean[1]["_counter_segment"])
        self.assertNotEqual(state, "genuine")
        self.assertEqual(delta, 2000)

    def test_transient_reset_preserves_small_verified_increment(self):
        rows = [self.row(f"2026-09-13T{hour:02d}:00:00+09:00", value)
                for hour, value in [(0, 1000), (6, 0), (12, 1005)]]
        result = c.estimate_calendar_product(c.datetime(2026, 9, 13).date(), "3098417", rows)
        self.assertEqual(result["estimated_sales"], 5)
        self.assertEqual(result["coverage_pct"], 50)

    def test_corrected_brand_total_replaces_old_total_when_no_intervals_remain(self):
        with tempfile.TemporaryDirectory() as tmp, contextlib.ExitStack() as stack:
            root = c.BASE_DIR
            for name, value in list(vars(c).items()):
                if isinstance(value, Path) and value.is_relative_to(root):
                    stack.enter_context(patch.object(c, name, Path(tmp) / value.relative_to(root)))
            stack.enter_context(contextlib.redirect_stdout(io.StringIO()))
            target = c.datetime(2026, 9, 13).date()
            c.write_csv(c.CALENDAR_BRAND_FILE, [dict(date=str(target), brand_name="PLAc",
                        product_count=1, estimated_sales=2000, estimated_gmv=142400000)], c.CALENDAR_BRAND_FIELDS)
            c.write_csv(c.CALENDAR_SUMMARY_FILE, [dict(date=str(target), product_count=1)], c.CALENDAR_SUMMARY_FIELDS)
            history = c.CALENDAR_HISTORY_DIR / "2026-09" / "bucket-49.csv"
            c.write_csv(history, [dict(date=str(target), goods_no="3098417", estimated_sales=2000),
                                 dict(date="2026-09-01", goods_no="3098417", estimated_sales=3)], c.CALENDAR_PRODUCT_FIELDS)
            raw_path = c.SLOT_DIR / "slot-3" / "2026-09-14.csv.gz"
            c.write_csv(raw_path, [self.row("2026-09-14T11:28:04+09:00", 2459)], c.COMPACT_FIELDS)
            original = raw_path.read_bytes()
            c.write_csv(c.LATEST_PRODUCT_FILE, [dict(date="2026-09-14", goods_no="3098417", daily_sales=2459)], c.LATEST_FIELDS)
            result = c.repair_sales_analytics()
            self.assertEqual(raw_path.read_bytes(), original)
            self.assertEqual(result["raw_observations_modified"], False)
            self.assertEqual(result["largest_corrections"][0]["before"], 2459)
            self.assertIsNone(result["largest_corrections"][0]["after"])
            rows = c.read_csv(c.CALENDAR_BRAND_FILE)
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["estimated_sales"], "")
            self.assertEqual(c.read_csv(c.CALENDAR_SUMMARY_FILE)[0]["estimated_sales"], "")
            self.assertEqual([r["date"] for r in c.read_csv(history)], ["2026-09-01"])
            marker_bytes = c.SALES_POLICY_FILE.read_bytes()
            with patch.object(c, "build_rows_for_date", side_effect=AssertionError("repair must be idempotent")):
                c.repair_sales_analytics()
            self.assertEqual(c.SALES_POLICY_FILE.read_bytes(), marker_bytes)


if __name__ == "__main__":
    unittest.main()
