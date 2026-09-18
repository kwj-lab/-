"""Publish KST daily estimates from observations, including the current day.

Read-only with respect to collection: writes exclusively data/dashboard/. Full
counter history is bucketed on disk so old reset boundaries never disappear
when a lookback window moves. No network calls and no invented opening balance.
"""
import argparse
import contextlib
import csv
import gzip
import hashlib
import io
import json
import os
from pathlib import Path
import shutil
import tempfile
from collections import defaultdict, Counter
from datetime import datetime, timedelta

import musinsa_collector_v9 as c

VERSION = "2026-09-18-gap-reconstruction-v2"
EXTRA_FIELDS = ["calculation_status", "last_observed_at", "covered_until",
                "sales_policy_version"]
DAY_FIELDS = c.CALENDAR_PRODUCT_FIELDS + EXTRA_FIELDS
LATEST_FIELDS = list(dict.fromkeys(DAY_FIELDS + c.CATALOG_FIELDS + [
    "purchase_total", "raw_date", "last_checked_at", "raw_collected",
    "raw_collected_today", "raw_daily_sales", "raw_daily_estimated_gmv",
    "raw_daily_sales_status", "raw_interval_hours", "previous_product_name",
    "product_name_changed_at", "product_name_change_count", "product_name_history",
]))
AGG_FIELDS = ["date", "checked_at", "brand_name", "product_count",
              "calculated_product_count", "excluded_product_count", "complete_product_count",
              "product_coverage_pct", "average_time_coverage_pct", "estimated_sales",
              "estimated_gmv", "price_change_products", "last_observed_at",
              "sales_policy_version"]


def read_rows(path):
    """Stream strictly: an unreadable/truncated input must fail publication."""
    opener = gzip.open if str(path).endswith(".gz") else open
    with opener(path, "rt", encoding="utf-8-sig", newline="") as stream:
        yield from csv.DictReader(stream)


def write_rows(path, rows, fields):
    path.parent.mkdir(parents=True, exist_ok=True)
    if str(path).endswith(".gz"):
        with path.open("wb") as raw, gzip.GzipFile(fileobj=raw, mode="wb", mtime=0, compresslevel=3) as gz:
            with io.TextIOWrapper(gz, encoding="utf-8", newline="") as stream:
                writer = csv.DictWriter(stream, fieldnames=fields, extrasaction="ignore")
                writer.writeheader()
                writer.writerows(rows)
    else:
        c.write_csv(path, rows, fields)


def day_start(day):
    return datetime.combine(day, datetime.min.time(), tzinfo=c.KST)


def product_days(goods, source, catalog, today):
    """Return daily records, current observation metadata, and rejected edges."""
    keyed = {}
    for row in source:
        stamp = c.parse_kst_datetime(row.get("checked_at"))
        if stamp is None or stamp.date() > today:
            continue
        old = keyed.get(stamp)
        if old is None or (c.to_int(old.get("purchase_total")) is None
                           and c.to_int(row.get("purchase_total")) is not None):
            keyed[stamp] = dict(row, _checked_dt=stamp)
    original = [keyed[t] for t in sorted(keyed)]
    meta = dict(catalog or {})
    if not original:
        return [], meta, []

    # The full series is sanitized once, not independently for each day.
    with contextlib.redirect_stderr(io.StringIO()):
        clean, anomaly, initial_status, initial_delta = c.sanitize_purchase_observations(original, meta)
    days = {}
    cursor = original[0]["_checked_dt"].date()
    while cursor <= today:
        days[cursor] = dict(sales=0., gmv=0., priced=0., seconds=0.,
                            attributed=0., reconstructed=0., intervals=0,
                            max_hours=0., count=0, until="", reason="uncollected",
                            price=None, start_price=None, price_changes=0, name="")
        cursor += timedelta(days=1)

    # Metadata is available even when a purchase counter cannot be used.
    changes = []
    previous_name = ""
    previous_price = None
    for row in original:
        stamp = row["_checked_dt"]
        bucket = days[stamp.date()]
        bucket["count"] += 1
        bucket["reason"] = "baseline_missing"
        price = c.to_int(row.get("current_price"))
        if price is not None:
            if bucket["start_price"] is None:
                bucket["start_price"] = previous_price if previous_price is not None else price
            if previous_price is not None and price != previous_price:
                bucket["price_changes"] += 1
            bucket["price"] = price
            previous_price = price
        name = str(row.get("product_name") or "")
        if name and previous_name and name != previous_name:
            changes.append(dict(old=previous_name, new=name, detected_at=stamp.isoformat()))
        if name:
            bucket["name"] = name
            previous_name = name

    rejected = []
    for a, b in zip(clean, clean[1:]):
        t0, t1 = a["_checked_dt"], b["_checked_dt"]
        duration = (t1-t0).total_seconds()
        if duration <= 0:
            continue
        delta = c.to_int(b["purchase_total"]) - c.to_int(a["purchase_total"])
        status = c.purchase_baseline_status(b, a, None)
        if a.get("_counter_segment") != b.get("_counter_segment") and status == "ok":
            status = "counter_jump_unverified"
        # An observed out-of-stock -> in-stock transition is a fresh baseline.
        if a.get("availability") == "OutOfStock" and b.get("availability") == "InStock":
            status = "reactivation_baseline"
        reconstructed = status == "ok" and duration > c.MAX_SALES_INTERVAL_HOURS * 3600.0
        if reconstructed:
            status = "reconstructed_gap"
        usable = status in ("ok", "reconstructed_gap")
        if not usable:
            rejected.append(dict(goods_no=goods, brand_name=meta.get("brand_name") or b.get("brand_name"),
                                 from_at=t0.isoformat(), to_at=t1.isoformat(),
                                 before=c.to_int(a["purchase_total"]), after=c.to_int(b["purchase_total"]),
                                 excluded_delta=max(0, delta), reason=status))
        cursor = t0.date()
        while cursor <= min(t1.date(), today):
            start = max(t0, day_start(cursor))
            end = min(t1, day_start(cursor)+timedelta(days=1))
            seconds = max(0., (end-start).total_seconds())
            if seconds > 0:
                acc = days[cursor]
                if usable:
                    value = delta * seconds / duration
                    acc["sales"] += value
                    acc["attributed"] += seconds
                    if reconstructed:
                        acc["reconstructed"] += seconds
                        acc["reason"] = "reconstructed_gap"
                    else:
                        acc["seconds"] += seconds
                    acc["intervals"] += 1
                    acc["max_hours"] = max(acc["max_hours"], duration/3600.)
                    acc["until"] = max(acc["until"], end.isoformat())
                    price = c.to_int(a.get("current_price"))
                    if price is not None:
                        acc["gmv"] += value * price
                        acc["priced"] += seconds
                else:
                    acc["reason"] = status
            cursor += timedelta(days=1)

    last = original[-1]
    brand = str(meta.get("brand_name") or last.get("brand_name") or "(브랜드 미확인)")
    last_price, last_name = None, ""
    output = []
    for day, acc in sorted(days.items()):
        start_price = last_price if last_price is not None else acc["start_price"]
        if acc["price"] is not None:
            last_price = acc["price"]
        last_name = acc["name"] or last_name
        valid = acc["attributed"] > 0
        coverage = min(100., acc["seconds"]/86400.)
        complete = day < today and coverage >= 99. and acc["reconstructed"] <= 0
        if complete:
            status = "complete"
        elif acc["reconstructed"] > 0 and acc["seconds"] > 0:
            status = "reconstructed_partial"
        elif acc["reconstructed"] > 0:
            status = "reconstructed_gap"
        elif valid:
            status = "observed_partial"
        else:
            status = acc["reason"]
        confidence = ("high" if complete and acc["max_hours"] <= 30 and not anomaly
                      else "medium" if complete
                      else "reconstructed" if acc["reconstructed"] > 0
                      else "partial" if valid else "pending")
        change = last_price-start_price if last_price is not None and start_price is not None else None
        output.append(dict(date=day.isoformat(), brand_name=brand, goods_no=goods,
                           product_name=last_name or meta.get("product_name", ""),
                           estimated_sales=round(acc["sales"], 4) if valid else "",
                           estimated_gmv=round(acc["gmv"]) if acc["priced"] else "",
                           estimated_avg_price=round(acc["gmv"]/acc["sales"]) if acc["sales"] and acc["priced"] else last_price,
                           display_price=last_price, previous_display_price=start_price,
                           price_change_detected=int(acc["price_changes"] > 0),
                           price_change_amount=change,
                           price_change_pct=round(change/start_price*100, 2) if change is not None and start_price else "",
                           coverage_pct=round(coverage, 2), calendar_complete=int(complete), confidence=confidence,
                           max_interval_hours=round(acc["max_hours"], 2), observation_count=acc["count"],
                           contributing_intervals=acc["intervals"], initial_delta_status=initial_status,
                           initial_delta_value=initial_delta or "", history_bucket=c.calendar_bucket(goods),
                           product_url=meta.get("product_url") or f"https://www.musinsa.com/products/{goods}",
                           calculation_status=status, last_observed_at=last["checked_at"],
                           covered_until=acc["until"], sales_policy_version=VERSION))
    # Latest raw metrics come from ALL observations, including adaptive/anchor.
    latest = dict(meta)
    latest.update({k: v for k, v in last.items() if not k.startswith("_")})
    latest.update(last_checked_at=last["checked_at"], raw_date=last["_checked_dt"].date().isoformat(),
                  raw_collected=int(c.to_int(last.get("purchase_total")) is not None),
                  raw_collected_today=int(last["_checked_dt"].date() == today and c.to_int(last.get("purchase_total")) is not None),
                  raw_daily_sales="", raw_daily_estimated_gmv="", raw_daily_sales_status="baseline_missing",
                  raw_interval_hours="", product_name_history=json.dumps(changes, ensure_ascii=False, separators=(",", ":")),
                  previous_product_name=changes[-1]["old"] if changes else "",
                  product_name_changed_at=changes[-1]["detected_at"] if changes else "",
                  product_name_change_count=len(changes))
    if len(clean) >= 2 and clean[-1]["_checked_dt"] == last["_checked_dt"]:
        a, b = clean[-2:]
        status = c.purchase_baseline_status(b, a, c.MAX_SALES_INTERVAL_HOURS)
        if a.get("_counter_segment") != b.get("_counter_segment") and status == "ok": status = "counter_jump_unverified"
        if a.get("availability") == "OutOfStock" and b.get("availability") == "InStock": status = "reactivation_baseline"
        latest["raw_daily_sales_status"] = status
        latest["raw_interval_hours"] = round((b["_checked_dt"]-a["_checked_dt"]).total_seconds()/3600, 2)
        if status == "ok":
            latest["raw_daily_sales"] = c.to_int(b["purchase_total"])-c.to_int(a["purchase_total"])
            price = c.to_int(a.get("current_price"))
            latest["raw_daily_estimated_gmv"] = latest["raw_daily_sales"]*price if price is not None else ""
    return output, latest, rejected


def add_total(acc, row):
    acc["product_count"] += 1
    acc["complete_product_count"] += int(row.get("calendar_complete") or 0)
    acc["time_sum"] += float(row.get("coverage_pct") or 0)
    acc["price_change_products"] += int(row.get("price_change_detected") or 0)
    value = c.to_float(row.get("estimated_sales"))
    if value is not None:
        acc["calculated_product_count"] += 1
        acc["estimated_sales"] += value
    value = c.to_float(row.get("estimated_gmv"))
    if value is not None:
        acc["priced_count"] += 1
        acc["estimated_gmv"] += value


def aggregate_row(day, brand, acc, generated):
    total, valid = acc["product_count"], acc["calculated_product_count"]
    return dict(date=day, brand_name=brand, checked_at=generated, product_count=total,
                calculated_product_count=valid, excluded_product_count=total-valid,
                complete_product_count=acc["complete_product_count"],
                product_coverage_pct=round(valid/total*100, 2) if total else 0,
                average_time_coverage_pct=round(acc["time_sum"]/total, 2) if total else 0,
                estimated_sales=round(acc["estimated_sales"], 2) if valid else "",
                estimated_gmv=round(acc["estimated_gmv"]) if acc["priced_count"] else "",
                price_change_products=acc["price_change_products"], sales_policy_version=VERSION)


def build(root=None, today=None):
    root = Path(root or c.BASE_DIR)
    today = today or c.now_kst().date()
    destination = root/"data/dashboard"
    inputs = sorted(set((root/"data/slots").glob("slot-*/*.csv*")) | set((root/"data/observations").glob("*/*.csv*")))
    inputs = [p for p in inputs if p.name.endswith((".csv", ".csv.gz"))]
    digest = hashlib.sha256(VERSION.encode())
    for path in inputs + [root/"musinsa_catalog.csv", root/"data/lifecycle/pretracker_evidence.json", Path(c.__file__), Path(__file__)]:
        if path.exists():
            digest.update(str(path.relative_to(root) if path.is_relative_to(root) else path.name).encode())
            with path.open("rb") as stream:
                for chunk in iter(lambda: stream.read(1024*1024), b""): digest.update(chunk)
    fingerprint = digest.hexdigest()
    manifest_path = destination/"manifest.json"
    if manifest_path.exists():
        prior = json.loads(manifest_path.read_text())
        if prior.get("source_fingerprint") == fingerprint and prior.get("as_of_date") == str(today):
            print("Dashboard already reflects these observations.", flush=True)
            return prior
    if not inputs:
        raise RuntimeError("No observation inputs; refusing to replace the dashboard")
    catalog = {r["goods_no"]: r for r in read_rows(root/"musinsa_catalog.csv")}
    generated = c.now_kst().isoformat(timespec="seconds")
    totals = defaultdict(Counter)
    overall = defaultdict(Counter)
    latest = []
    reject_counts = Counter()
    largest = []
    input_rows = 0
    latest_stamp = ""
    with tempfile.TemporaryDirectory(prefix="musinsa-dashboard-") as temporary:
        temp = Path(temporary)
        staged = temp/"published"
        with contextlib.ExitStack() as stack:
            streams = [stack.enter_context((temp/f"observations-{i:02d}.csv").open("w", encoding="utf-8", newline="")) for i in range(64)]
            writers = [csv.DictWriter(s, fieldnames=c.COMPACT_FIELDS, extrasaction="ignore") for s in streams]
            for w in writers: w.writeheader()
            for path in inputs:
                for row in read_rows(path):
                    g = str(row.get("goods_no") or "").strip()
                    if g:
                        writers[c.calendar_bucket(g)].writerow(row)
                        input_rows += 1
                        stamp = str(row.get("checked_at") or "")
                        if stamp[:10] <= str(today): latest_stamp = max(latest_stamp, stamp)
        print(f"Read {input_rows:,} observations from {len(inputs)} files", flush=True)
        display_date = latest_stamp[:10] or str(today)
        seen = set()
        for bucket in range(64):
            groups = defaultdict(list)
            for row in read_rows(temp/f"observations-{bucket:02d}.csv"):
                groups[row["goods_no"]].append(row)
            history = defaultdict(list)
            for g, source in groups.items():
                rows, metadata, rejected = product_days(g, source, catalog.get(g), today)
                if not rows: continue
                seen.add(g)
                displayed = next((r for r in reversed(rows) if r["date"] <= display_date), rows[-1])
                latest.append(dict(metadata, **displayed))
                for row in rows:
                    history[row["date"][:7]].append(row)
                    add_total(totals[(row["date"], row["brand_name"])], row)
                    add_total(overall[row["date"]], row)
                for event in rejected:
                    reject_counts[event["reason"]] += 1
                    largest.append(event)
            largest.sort(key=lambda x: x["excluded_delta"], reverse=True)
            del largest[100:]
            for month, rows in history.items():
                rows.sort(key=lambda r: (r["goods_no"], r["date"]))
                write_rows(staged/f"history/{month}/bucket-{bucket:02d}.csv.gz", rows, DAY_FIELDS)
            if bucket % 8 == 7: print(f"Published {bucket+1}/64 history buckets", flush=True)
        for g, meta in catalog.items():
            if g in seen: continue
            row = dict(meta, date=display_date, goods_no=g, history_bucket=c.calendar_bucket(g),
                       estimated_sales="", estimated_gmv="", confidence="pending", calculation_status="baseline_missing",
                       calendar_complete=0, coverage_pct=0, display_price=meta.get("current_price", ""),
                       sales_policy_version=VERSION)
            latest.append(row)
            add_total(totals[(display_date, row["brand_name"])], row)
            add_total(overall[display_date], row)
        latest.sort(key=lambda r: (r.get("brand_name", ""), r["goods_no"]))
        write_rows(staged/"latest_products.csv.gz", latest, LATEST_FIELDS)
        brand_rows = [aggregate_row(day, brand, acc, generated) for (day, brand), acc in sorted(totals.items())]
        summary = [aggregate_row(day, "", acc, generated) for day, acc in sorted(overall.items())]
        write_rows(staged/"brand_daily.csv", brand_rows, AGG_FIELDS)
        write_rows(staged/"summary.csv", summary, AGG_FIELDS)
        manifest = dict(sales_policy_version=VERSION, source_fingerprint=fingerprint,
                        updated_at=generated, as_of_date=str(today), latest_date=display_date, latest_finalized_date=display_date,
                        latest_closed_date=str(today-timedelta(days=1)),
                        dates=[r["date"] for r in summary], months=sorted({r["date"][:7] for r in summary}),
                        history_buckets=64, last_observed_at=max((r.get("last_checked_at", "") for r in latest), default=""),
                        source_files=len(inputs), source_rows=input_rows, product_count=len(latest),
                        excluded_boundaries=dict(reject_counts), latest_summary=next((r for r in summary if r["date"] == display_date), {}),
                        method="First cumulative value is a baseline; structurally valid deltas are allocated by KST day overlap. Gaps over 36h are reconstructed but excluded from observed coverage. Current day is partial through the latest observation. Unknown is not zero.")
        (staged/"audit.json").write_text(json.dumps(dict(largest_excluded_boundaries=largest), ensure_ascii=False, indent=2), encoding="utf-8")
        # Validate gzip CRC and complete row count before making any output visible.
        count = sum(1 for _ in read_rows(staged/"latest_products.csv.gz"))
        if count != len(latest): raise RuntimeError("Incomplete dashboard product output")
        for path in staged.rglob("*"):
            if path.is_file():
                target = destination/path.relative_to(staged)
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(path, target)
        destination.mkdir(parents=True, exist_ok=True)
        manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(manifest, ensure_ascii=False), flush=True)
    return manifest


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", help="KST as-of day for reproducible offline verification")
    args = parser.parse_args()
    build(today=datetime.strptime(args.date, "%Y-%m-%d").date() if args.date else None)
