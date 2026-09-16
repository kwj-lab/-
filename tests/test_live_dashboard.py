"""Daily totals must preserve real increments without admitting opening balances."""
import contextlib
import io
from collections import Counter
from datetime import date
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import musinsa_dashboard as d


class LiveDashboardTests(unittest.TestCase):
    def row(self, stamp, count, **fields):
        return dict(goods_no="98765432", brand_name="test", product_name="knit",
                    checked_at=stamp, purchase_total=count, current_price=30000, **fields)

    def daily(self, rows, today=date(2026, 9, 15), catalog=None):
        return d.product_days("98765432", rows, catalog or {}, today)

    def test_first_positive_counter_is_never_sales_even_without_reviews(self):
        days, _, _ = self.daily([self.row("2026-09-15T09:00:00+09:00", 2000)])
        self.assertEqual(days[-1]["estimated_sales"], "")

    def test_next_increment_publishes_immediately_on_current_day(self):
        days, latest, _ = self.daily([self.row("2026-09-15T09:00:00+09:00", 2000),
                                      self.row("2026-09-15T12:00:00+09:00", 2007)])
        self.assertEqual(days[-1]["estimated_sales"], 7)
        self.assertEqual(days[-1]["calendar_complete"], 0)
        self.assertEqual(latest["raw_daily_sales"], 7)

    def test_restored_zero_counter_excluded_but_later_sales_survive(self):
        rows = [self.row(f"2026-09-15T{hour:02d}:00:00+09:00", count)
                for hour, count in [(0,0), (3,2000), (6,2009)]]
        days, _, rejected = self.daily(rows)
        self.assertEqual(days[-1]["estimated_sales"], 9)
        self.assertEqual(rejected[0]["excluded_delta"], 2000)

    def test_dates_receive_only_their_fraction_and_do_not_double_count(self):
        rows = [self.row("2026-09-14T21:00:00+09:00", 1000),
                self.row("2026-09-15T03:00:00+09:00", 1012)]
        days, _, _ = self.daily(rows)
        self.assertEqual([r["estimated_sales"] for r in days], [6,6])
        _, _, rejected = self.daily(rows+[dict(rows[-1])])
        self.assertEqual(rejected, [])

    def test_gap_is_unknown_and_new_baseline_resumes_counting(self):
        rows = [self.row("2026-09-11T12:00:00+09:00",1000),
                self.row("2026-09-15T06:00:00+09:00",6000),
                self.row("2026-09-15T09:00:00+09:00",6005)]
        days, _, rejected = self.daily(rows)
        self.assertEqual(days[1]["estimated_sales"], "")
        self.assertEqual(days[-1]["estimated_sales"],5)
        self.assertEqual(rejected[0]["reason"], "collection_gap")

    def test_known_sold_out_boundary_is_a_baseline(self):
        rows = [self.row("2026-09-15T00:00:00+09:00",200,availability="OutOfStock"),
                self.row("2026-09-15T03:00:00+09:00",250,availability="InStock"),
                self.row("2026-09-15T06:00:00+09:00",253,availability="InStock")]
        days, _, rejected = self.daily(rows)
        self.assertEqual(days[-1]["estimated_sales"],3)
        self.assertEqual(rejected[0]["reason"],"reactivation_baseline")

    def test_transient_zero_does_not_erase_trusted_positive_baseline(self):
        days, _, _ = self.daily([self.row(f"2026-09-15T{hour:02d}:00:00+09:00",value)
                                for hour,value in [(0,1000),(3,0),(6,1008)]])
        self.assertEqual(days[-1]["estimated_sales"],8)

    def test_price_and_title_changes_survive_missing_sales(self):
        a=self.row("2026-09-15T00:00:00+09:00",0)
        b=self.row("2026-09-15T03:00:00+09:00",2000)
        b.update(current_price=25000, product_name="new knit")
        days, latest, _ = self.daily([a,b])
        self.assertEqual(days[-1]["estimated_sales"], "")
        self.assertEqual(days[-1]["price_change_amount"], -5000)
        self.assertEqual(latest["product_name_change_count"],1)

    def test_brand_sum_keeps_known_zero_separate_from_unknown(self):
        acc=Counter()
        for value in (7,0,""):
            d.add_total(acc,dict(estimated_sales=value))
        row=d.aggregate_row("2026-09-15","test",acc,"")
        self.assertEqual(row["estimated_sales"],7)
        self.assertEqual(row["calculated_product_count"],2)
        self.assertEqual(row["excluded_product_count"],1)
        unknown=Counter()
        d.add_total(unknown,dict(estimated_sales=""))
        self.assertEqual(d.aggregate_row("2026-09-15","test",unknown,"")["estimated_sales"],"")

    def test_full_publication_includes_today_and_leaves_raw_inputs_unchanged(self):
        with tempfile.TemporaryDirectory() as temp, contextlib.redirect_stdout(io.StringIO()):
            root=Path(temp)
            d.write_rows(root/"musinsa_catalog.csv",[dict(goods_no="98765432",brand_name="test",product_name="knit")],d.c.CATALOG_FIELDS)
            raw=root/"data/slots/slot-0/2026-09-15.csv.gz"
            rows=[self.row("2026-09-15T09:00:00+09:00",2000), self.row("2026-09-15T12:00:00+09:00",2007)]
            d.write_rows(raw,rows,d.c.COMPACT_FIELDS)
            before=raw.read_bytes()
            manifest=d.build(root, date(2026,9,15))
            self.assertEqual(manifest["latest_summary"]["estimated_sales"],7)
            self.assertEqual(raw.read_bytes(),before)
            self.assertEqual(list(d.read_rows(root/"data/dashboard/latest_products.csv.gz"))[0]["estimated_sales"],"7.0")
            self.assertEqual(d.build(root,date(2026,9,15)),manifest)


if __name__ == "__main__": unittest.main()
