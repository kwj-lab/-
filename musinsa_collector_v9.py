# -*- coding: utf-8 -*-
"""
Musinsa Distributed Collector v9.2 lifecycle-safe
================================
등록 브랜드 수백 개 / 상품코드 수만~10만+ 규모를 위한 분산 수집기.

핵심 구조
---------
- 전체 goodsNo를 8개 고정 시간대(slot 0~7)에 균등 배정
- 같은 goodsNo는 매일 같은 slot에서 조회 -> 거의 24시간 간격 비교
- GitHub Actions가 하루 8회(3시간 간격) 실행
- 각 실행은 해당 slot의 상품만 조회
- slot 안에서도 4~12개 shard로 자동 분산
- GitHub collect job 동시 실행은 최대 4개, shard당 worker 2개
  -> 평상시 최대 약 8개 stat 요청 병렬
- 429/403/5xx는 우회하지 않고 감속(backoff) 후 재시도
- 브랜드 신규상품 탐색도 브랜드를 8 slot으로 고정 분산
- 매일 해당 slot의 브랜드만 quick discovery
- 일요일에는 해당 slot 브랜드 full discovery
- 새 브랜드는 첫 담당 slot 실행 때 full discovery

데이터
------
data/slots/slot-N/YYYY-MM-DD.csv.gz
    해당 slot 상품의 일별 핵심 스냅샷

data/latest_slots/slot-N.csv.gz
    각 slot의 최신 계산 결과

data/daily/YYYY-MM-DD.csv.gz
    8개 slot이 모두 끝난 날 생성되는 전체 일별 compact snapshot

musinsa_daily_product_sales.csv
    모든 slot의 최신 상품 계산 결과를 합친 대시보드용 파일

musinsa_daily_brand_sales.csv
    날짜별 브랜드 합계. 하루 중에는 진행 중(partial), 마지막 slot 후 완성.

주의
----
purchaseTotal은 공개 PDP 통계 API의 누적 구매수입니다.
v8 호환 24시간 구간 수치는 내부 원본/복구용으로 유지합니다.
v9 대시보드의 핵심 지표는 KST 00:00~24:00 캘린더 날짜 기준 추정치입니다.
관측 시점 사이의 누적 증가량을 시간 비율로 날짜에 배분하므로 실제 주문수/결제매출과 다를 수 있습니다.
"""

import argparse
import csv
import gzip
import hashlib
import difflib
import unicodedata
import html as html_lib
import json
import os
import random
import re
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path
from queue import Empty, Queue
from zoneinfo import ZoneInfo

KST = ZoneInfo("Asia/Seoul")
BASE_DIR = Path(__file__).resolve().parent

BRANDS_FILE = BASE_DIR / "musinsa_brands.txt"
WATCHLIST_FILE = BASE_DIR / "musinsa_watchlist.txt"
CATALOG_FILE = BASE_DIR / "musinsa_catalog.csv"
NEW_PRODUCTS_FILE = BASE_DIR / "musinsa_new_products.csv"
BRAND_AUDIT_FILE = BASE_DIR / "musinsa_brand_audit.csv"

LATEST_PRODUCT_FILE = BASE_DIR / "musinsa_daily_product_sales.csv"
BRAND_HISTORY_FILE = BASE_DIR / "musinsa_daily_brand_sales.csv"
SUMMARY_FILE = BASE_DIR / "musinsa_daily_summary.csv"

SLOT_DIR = BASE_DIR / "data" / "slots"
LATEST_SLOT_DIR = BASE_DIR / "data" / "latest_slots"
DAILY_DIR = BASE_DIR / "data" / "daily"
HISTORY_MANIFEST_FILE = BASE_DIR / "data" / "history_manifest.json"
RECOVERY_DIR = BASE_DIR / "data" / "recovery"
COVERAGE_DIR = BASE_DIR / "data" / "coverage"
COVERAGE_LATEST_FILE = COVERAGE_DIR / "latest.json"

# lifecycle evidence cache: used to distinguish truly new goods from old goods
# that existed before this tracker and later became visible again.
LIFECYCLE_DIR = BASE_DIR / "data" / "lifecycle"
LIFECYCLE_EVIDENCE_FILE = LIFECYCLE_DIR / "pretracker_evidence.json"

# v9 calendar-day analytics
CALENDAR_DIR = BASE_DIR / "data" / "calendar"
CALENDAR_HISTORY_DIR = CALENDAR_DIR / "history"
CALENDAR_MANIFEST_FILE = CALENDAR_DIR / "calendar_manifest.json"
CALENDAR_LATEST_PRODUCT_FILE = BASE_DIR / "musinsa_calendar_latest_products.csv"
CALENDAR_BRAND_FILE = BASE_DIR / "musinsa_calendar_brand_daily.csv"
CALENDAR_SUMMARY_FILE = BASE_DIR / "musinsa_calendar_summary.csv"
CALENDAR_HISTORY_BUCKETS = 64

# Extra observations share the lifecycle-safe calendar estimator below.
OBSERVATION_DIR = BASE_DIR / "data" / "observations"
ADAPTIVE_REPORT_DIR = BASE_DIR / "data" / "adaptive"
MIDNIGHT_REPORT_DIR = BASE_DIR / "data" / "anchor"

SLOT_COUNT = 8
ADAPTIVE_MEDIUM_MIN = float(os.environ.get("MUSINSA_ADAPTIVE_MEDIUM_MIN", "3"))
ADAPTIVE_HIGH_MIN = float(os.environ.get("MUSINSA_ADAPTIVE_HIGH_MIN", "21"))
ADAPTIVE_MAX_PER_RUN = int(os.environ.get("MUSINSA_ADAPTIVE_MAX_PER_RUN", "15000"))
NEW_PRODUCT_PROBE_OFFSET = int(os.environ.get("MUSINSA_NEW_PRODUCT_PROBE_OFFSET", "2")) % SLOT_COUNT
NEW_PRODUCT_PROBE_MAX_PER_RUN = int(os.environ.get("MUSINSA_NEW_PRODUCT_PROBE_MAX_PER_RUN", "3000"))
NEW_PRODUCT_PROBE_LOOKBACK_DAYS = int(os.environ.get("MUSINSA_NEW_PRODUCT_PROBE_LOOKBACK_DAYS", "3"))
MIDNIGHT_ANCHOR_MIN = float(os.environ.get("MUSINSA_MIDNIGHT_ANCHOR_MIN", "3"))
MIDNIGHT_ANCHOR_MAX_PER_RUN = int(os.environ.get("MUSINSA_MIDNIGHT_ANCHOR_MAX_PER_RUN", "15000"))
DISCOVERY_WORKERS = int(os.environ.get("MUSINSA_DISCOVERY_WORKERS", "2"))
SHARD_WORKERS = int(os.environ.get("MUSINSA_SHARD_WORKERS", "2"))
RECOVERY_WORKERS = int(os.environ.get("MUSINSA_RECOVERY_WORKERS", "2"))
INLINE_RETRY_MAX = int(os.environ.get("MUSINSA_INLINE_RETRY_MAX", "300"))
COLLECT_BUDGET_SECONDS = int(os.environ.get("MUSINSA_COLLECT_BUDGET_SECONDS", "4800"))
REQUEST_MIN_INTERVAL = float(os.environ.get("MUSINSA_REQUEST_MIN_INTERVAL", "0.10"))
REQUEST_MAX_INTERVAL = float(os.environ.get("MUSINSA_REQUEST_MAX_INTERVAL", "3.0"))
MAX_SEARCH_PAGES = int(os.environ.get("MUSINSA_MAX_SEARCH_PAGES", "300"))
QUICK_SEARCH_MAX_PAGES = int(os.environ.get("MUSINSA_QUICK_SEARCH_MAX_PAGES", "40"))
QUICK_KNOWN_STOP_PAGES = int(os.environ.get("MUSINSA_QUICK_KNOWN_STOP_PAGES", "3"))


# v9.1 regression fix: short/generic brand search aliases.
# Registered/canonical names remain Korean in catalog and dashboards.
BRAND_SEARCH_KEYWORDS = {
    "음": "UMM",
    "리": "LEE",
}

# Counter lifecycle / schema-jump guards restored from the hardened v9 line.
NEW_PRODUCT_PROBATION_HOURS = int(os.environ.get("MUSINSA_NEW_PRODUCT_PROBATION_HOURS", "48"))
INITIAL_JUMP_MIN = int(os.environ.get("MUSINSA_INITIAL_JUMP_MIN", "100"))
INITIAL_JUMP_FIRST_SEEN_WINDOW_HOURS = float(os.environ.get("MUSINSA_INITIAL_JUMP_FIRST_SEEN_WINDOW_HOURS", "12"))
INITIAL_JUMP_PLATEAU_HOURS = float(os.environ.get("MUSINSA_INITIAL_JUMP_PLATEAU_HOURS", "12"))
INITIAL_JUMP_PLATEAU_CONFIRMATIONS = int(os.environ.get("MUSINSA_INITIAL_JUMP_PLATEAU_CONFIRMATIONS", "3"))
INITIAL_JUMP_GENUINE_MIN_FOLLOW = int(os.environ.get("MUSINSA_INITIAL_JUMP_GENUINE_MIN_FOLLOW", "20"))
INITIAL_JUMP_GENUINE_FOLLOW_RATIO = float(os.environ.get("MUSINSA_INITIAL_JUMP_GENUINE_FOLLOW_RATIO", "0.10"))
MIDSTREAM_JUMP_MIN = int(os.environ.get("MUSINSA_MIDSTREAM_JUMP_MIN", "500"))
MIDSTREAM_JUMP_PRIOR_INTERVALS = int(os.environ.get("MUSINSA_MIDSTREAM_JUMP_PRIOR_INTERVALS", "3"))
MIDSTREAM_JUMP_PRIOR_MULTIPLIER = float(os.environ.get("MUSINSA_MIDSTREAM_JUMP_PRIOR_MULTIPLIER", "50"))
MIDSTREAM_JUMP_CONFIRM_HOURS = float(os.environ.get("MUSINSA_MIDSTREAM_JUMP_CONFIRM_HOURS", "12"))
MIDSTREAM_JUMP_CONFIRMATIONS = int(os.environ.get("MUSINSA_MIDSTREAM_JUMP_CONFIRMATIONS", "3"))
MIDSTREAM_JUMP_PLATEAU_RATIO = float(os.environ.get("MUSINSA_MIDSTREAM_JUMP_PLATEAU_RATIO", "0.02"))
MIDSTREAM_JUMP_GENUINE_RATIO = float(os.environ.get("MUSINSA_MIDSTREAM_JUMP_GENUINE_RATIO", "0.10"))
MIDSTREAM_JUMP_GENUINE_MIN_FOLLOW = int(os.environ.get("MUSINSA_MIDSTREAM_JUMP_GENUINE_MIN_FOLLOW", "100"))

# Intervals up to this length count as directly observed coverage. Longer
# structurally valid intervals may still be allocated across KST calendar days
# as reconstructed estimates, but they never count as observed coverage.
MAX_SALES_INTERVAL_HOURS = 36.0
SALES_POLICY_VERSION = "2026-09-18-calendar-gap-reconstruction-v2"
SALES_POLICY_FILE = BASE_DIR / "data" / "sales_policy.json"

# Tracker-start boundary. In the real repository the earliest stored snapshot is
# preferred automatically; the env/default is only a fallback for fresh/partial copies.
TRACKER_START_DATE_FALLBACK = os.environ.get("MUSINSA_TRACKER_START_DATE", "2026-08-28")
LIFECYCLE_PROBE_TIMEOUT = int(os.environ.get("MUSINSA_LIFECYCLE_PROBE_TIMEOUT", "12"))
LIFECYCLE_DISCOVERY_PROBE_MAX = int(os.environ.get("MUSINSA_LIFECYCLE_DISCOVERY_PROBE_MAX", "120"))

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/151.0.0.0 Safari/537.36"
)

CATALOG_FIELDS = [
    "goods_no", "brand_name", "product_name", "normal_price",
    "current_price", "sale_rate", "review_count", "rating",
    "availability", "first_seen_at", "last_seen_at",
    "lifecycle_status", "lifecycle_evidence_type", "lifecycle_evidence_date",
    "lifecycle_checked_at", "product_url",
]
RAW_FIELDS = [
    "checked_at", "goods_no", "brand_name", "product_name",
    "purchase_total", "page_view_total", "normal_price", "current_price",
    "sale_rate", "review_count", "rating", "availability",
    "simple_gmv", "product_url", "errors",
]
COMPACT_FIELDS = [
    "date", "slot", "checked_at", "goods_no", "brand_name", "product_name",
    "purchase_total", "page_view_total", "current_price", "normal_price",
    "sale_rate", "review_count", "rating", "availability",
]
ADAPTIVE_OBS_FIELDS = COMPACT_FIELDS + [
    "sample_kind", "sampling_tier", "sampling_score", "base_slot", "clock_slot",
]
LATEST_FIELDS = [
    "date", "slot", "checked_at", "brand_name", "goods_no", "product_name",
    "purchase_total", "daily_sales", "normal_price", "current_price", "sale_rate",
    "daily_estimated_gmv", "simple_gmv",
    "page_view_total", "daily_page_view_increase",
    "review_count", "daily_review_increase",
    "like_count", "daily_like_increase",
    "sales_7d", "sales_7d_avg_per_day", "estimated_gmv_7d",
    "sales_30d", "sales_30d_avg_per_day", "estimated_gmv_30d",
    "availability", "product_url", "errors",
    "daily_sales_status", "daily_baseline_at", "daily_interval_hours", "sales_policy_version",
]
BRAND_FIELDS = [
    "date", "checked_at", "brand_name", "product_count",
    "daily_baseline_product_count", "purchase_total_sum", "simple_gmv_sum",
    "daily_sales_sum", "daily_estimated_gmv_sum",
    "daily_page_view_increase_sum", "daily_review_increase_sum",
    "daily_like_increase_sum", "sales_7d_sum", "sales_7d_avg_per_day",
    "estimated_gmv_7d", "sales_30d_sum", "sales_30d_avg_per_day",
    "estimated_gmv_30d", "products_with_7d_baseline",
    "products_with_30d_baseline", "new_products",
]
SUMMARY_FIELDS = [
    "checked_at", "date", "product_count", "daily_baseline_product_count",
    "purchase_total_sum", "simple_gmv_sum", "daily_sales_sum",
    "daily_estimated_gmv_sum", "sales_7d_sum", "sales_30d_sum",
    "new_products",
]
NEW_PRODUCT_FIELDS = [
    "first_seen_at", "brand_name", "goods_no", "product_name",
    "normal_price", "current_price", "sale_rate", "lifecycle_status",
]
BRAND_AUDIT_FIELDS = [
    "checked_at", "requested_brand", "matched_brand", "match_mode",
    "search_result_total", "matched_products", "pages_scanned",
    "top_candidate_brands", "status",
]

FAILURE_FIELDS = [
    "date", "slot", "goods_no", "brand_name", "product_name",
    "first_failed_at", "last_failed_at", "attempts", "last_error",
    "current_price", "product_url",
]

CALENDAR_PRODUCT_FIELDS = [
    "date", "brand_name", "goods_no", "product_name",
    "estimated_sales", "estimated_gmv", "estimated_avg_price",
    "display_price", "previous_display_price",
    "price_change_detected", "price_change_amount", "price_change_pct",
    "coverage_pct", "calendar_complete", "confidence",
    "max_interval_hours", "observation_count", "contributing_intervals",
    "initial_delta_status", "initial_delta_value",
    "history_bucket", "product_url",
]
CALENDAR_BRAND_FIELDS = [
    "date", "checked_at", "brand_name",
    "product_count", "complete_product_count", "product_coverage_pct",
    "average_time_coverage_pct",
    "estimated_sales", "estimated_gmv", "price_change_products",
    "high_confidence_products", "medium_confidence_products", "low_confidence_products",
]
CALENDAR_SUMMARY_FIELDS = [
    "date", "checked_at", "brand_count", "product_count",
    "complete_product_count", "product_coverage_pct", "average_time_coverage_pct",
    "estimated_sales", "estimated_gmv", "price_change_products",
]


def now_kst():
    return datetime.now(KST)


def to_int(value):
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (int, float)):
        return int(value)
    s = re.sub(r"[^\d.-]", "", str(value))
    if not s:
        return None
    try:
        return int(float(s))
    except Exception:
        return None


def goods_slot(goods_no):
    """같은 goodsNo는 영구적으로 같은 0~7 slot에 배정."""
    s = str(goods_no).strip()
    if s.isdigit():
        return int(s) % SLOT_COUNT
    digest = hashlib.sha1(s.encode("utf-8")).digest()
    return int.from_bytes(digest[:4], "big") % SLOT_COUNT


def brand_slot(brand_name):
    """Python hash() 대신 SHA1을 써서 실행마다 동일한 slot 유지."""
    digest = hashlib.sha1(str(brand_name).strip().casefold().encode("utf-8")).digest()
    return int.from_bytes(digest[:4], "big") % SLOT_COUNT


def effective_goods_slot(goods_no, catalog_row=None):
    """브랜드가 확인된 상품은 브랜드와 같은 slot에 배정합니다."""
    brand = str((catalog_row or {}).get("brand_name") or "").strip()
    if brand:
        return brand_slot(brand)
    return goods_slot(goods_no)


def recommended_slot_shards(product_count):
    n = max(0, int(product_count or 0))
    if n <= 5000:
        return 4
    if n <= 10000:
        return 6
    if n <= 20000:
        return 8
    return 12


def read_lines(path):
    path = Path(path)
    if not path.exists():
        return []
    out, seen = [], set()
    for line in path.read_text(encoding="utf-8-sig", errors="ignore").splitlines():
        v = line.strip()
        if v and not v.startswith("#") and v not in seen:
            out.append(v)
            seen.add(v)
    return out


def write_lines(path, values):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    out, seen = [], set()
    for x in values:
        v = str(x).strip()
        if v and not v.startswith("#") and v not in seen:
            out.append(v)
            seen.add(v)
    path.write_text("\n".join(out) + ("\n" if out else ""), encoding="utf-8")


def read_csv(path):
    path = Path(path)
    if not path.exists():
        return []
    try:
        if path.suffix.lower() == ".gz":
            with gzip.open(path, "rt", encoding="utf-8-sig", newline="") as f:
                return list(csv.DictReader(f))
        with path.open("r", encoding="utf-8-sig", newline="") as f:
            return list(csv.DictReader(f))
    except Exception:
        return []


def write_csv(path, rows, fields):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    # Publish a complete file only after the CSV and gzip footer are closed.
    # An interrupted rewrite must not leave a truncated snapshot in its place.
    fd, name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=path.suffix, dir=path.parent)
    os.close(fd)
    temporary = Path(name)
    try:
        if path.suffix.lower() == ".gz":
            stream = gzip.open(temporary, "wt", encoding="utf-8-sig", newline="", compresslevel=6)
        else:
            stream = temporary.open("w", encoding="utf-8-sig", newline="")
        with stream as f:
            w = csv.DictWriter(f, fieldnames=fields)
            w.writeheader()
            for row in rows:
                w.writerow({k: row.get(k, "") for k in fields})
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


class AdaptiveThrottle:
    """프로세스 전체의 동시성 + 요청 간격을 함께 조절합니다.

    primary는 하나의 Python process에서 최대 8 worker를 사용합니다.
    429/403이 오면 허용 동시성을 8 -> 4 -> 2 -> 1 식으로 즉시 낮추고,
    성공이 충분히 이어지면 1 -> 2 -> 4 -> 8 식으로 천천히 복귀합니다.
    """
    def __init__(self, base_interval=0.10, max_interval=3.0, max_concurrency=8):
        self.base = max(0.0, float(base_interval))
        self.maximum = max(self.base, float(max_interval))
        self.interval = self.base
        self.max_concurrency = max(1, int(max_concurrency))
        self.limit = self.max_concurrency
        self.in_flight = 0
        self.next_at = 0.0
        self.pause_until = 0.0
        self.penalty = 0
        self.success_streak = 0
        self.cond = threading.Condition()

    def acquire(self):
        while True:
            with self.cond:
                now = time.monotonic()
                time_wait = max(self.pause_until - now, self.next_at - now, 0.0)
                capacity = self.in_flight < self.limit
                if capacity and time_wait <= 0:
                    self.in_flight += 1
                    self.next_at = now + self.interval
                    return
                wait = max(0.02, min(time_wait if time_wait > 0 else 0.20, 5.0))
                self.cond.wait(timeout=wait)

    def _release(self):
        self.in_flight = max(0, self.in_flight - 1)
        self.cond.notify_all()

    def success(self):
        with self.cond:
            self._release()
            self.success_streak += 1
            if self.success_streak >= 80:
                self.success_streak = 0
                self.penalty = max(0, self.penalty - 1)
                if self.interval > self.base:
                    self.interval = max(self.base, self.interval * 0.72)
                if self.limit < self.max_concurrency:
                    self.limit = min(self.max_concurrency, max(self.limit + 1, self.limit * 2))
            self.cond.notify_all()

    def error(self, status=None, retry_after=None):
        with self.cond:
            self._release()
            self.success_streak = 0
            now = time.monotonic()
            if status == 429:
                self.penalty = min(7, self.penalty + 1)
                self.limit = max(1, self.limit // 2)
                self.interval = min(self.maximum, max(0.5, self.interval * 2.0))
                pause = min(120.0, 15.0 * (2 ** max(0, self.penalty - 1)))
            elif status == 403:
                self.penalty = min(7, self.penalty + 1)
                self.limit = max(1, self.limit // 2)
                self.interval = min(self.maximum, max(0.5, self.interval * 1.8))
                pause = min(90.0, 10.0 * (2 ** max(0, self.penalty - 1)))
            elif status in (500, 502, 503, 504):
                self.limit = max(1, self.limit - 1)
                self.interval = min(self.maximum, max(0.25, self.interval * 1.35))
                pause = min(30.0, 3.0 * (2 ** min(3, self.penalty)))
            else:
                self.interval = min(self.maximum, max(self.base, self.interval * 1.15))
                pause = 2.0
            if retry_after is not None:
                try:
                    pause = max(pause, float(retry_after))
                except Exception:
                    pass
            self.pause_until = max(self.pause_until, now + pause + random.uniform(0.2, 1.5))
            self.cond.notify_all()
            return pause

    def recovery_mode(self):
        with self.cond:
            self.limit = min(self.limit, max(1, min(2, self.max_concurrency)))
            self.interval = min(self.maximum, max(self.interval, 0.40))
            self.pause_until = max(self.pause_until, time.monotonic() + random.uniform(1.0, 2.5))
            self.cond.notify_all()

    def state(self):
        with self.cond:
            return {
                "limit": self.limit,
                "max_concurrency": self.max_concurrency,
                "interval": round(self.interval, 3),
                "penalty": self.penalty,
            }


THROTTLE = AdaptiveThrottle(REQUEST_MIN_INTERVAL, REQUEST_MAX_INTERVAL, SHARD_WORKERS)

def http_get(url, timeout=20, retries=4, referer="https://www.musinsa.com/"):
    """서버 제한 신호를 존중하는 적응형 HTTP GET."""
    last = None
    for attempt in range(retries + 1):
        THROTTLE.acquire()
        try:
            req = urllib.request.Request(
                url,
                headers={
                    "User-Agent": USER_AGENT,
                    "Accept": "application/json,text/html,application/xhtml+xml,*/*",
                    "Accept-Language": "ko-KR,ko;q=0.9,en;q=0.8",
                    "Referer": referer,
                    "Cache-Control": "no-cache",
                    "X-Musinsa-App": "MusinsaWeb",
                },
            )
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                raw = resp.read()
                charset = resp.headers.get_content_charset() or "utf-8"
                THROTTLE.success()
                return raw.decode(charset, errors="replace")
        except urllib.error.HTTPError as e:
            last = e
            retry_after = None
            try:
                retry_after = e.headers.get("Retry-After") if e.headers else None
            except Exception:
                pass
            THROTTLE.error(e.code, retry_after)
            print(
                f"[adaptive-backoff] HTTP {e.code}; state={THROTTLE.state()}; "
                f"attempt={attempt + 1}/{retries + 1}",
                file=sys.stderr,
            )
            if attempt >= retries:
                break
        except Exception as e:
            last = e
            THROTTLE.error(None, None)
            if attempt >= retries:
                break
    raise last

def search_json(url, brand_name):
    referer = (
        "https://www.musinsa.com/search/musinsa/integration?type=popular&q="
        + urllib.parse.quote(brand_name)
    )
    return json.loads(http_get(url, referer=referer))


def normalize_brand_name(value):
    s = unicodedata.normalize("NFKC", str(value or "")).casefold().strip()
    # 공백/하이픈/점/괄호 등 표기 차이는 무시
    return "".join(ch for ch in s if ch.isalnum() or ("가" <= ch <= "힣"))


def brand_match_mode(requested, candidate):
    r_raw = str(requested or "").strip()
    c_raw = str(candidate or "").strip()
    r = normalize_brand_name(r_raw)
    c = normalize_brand_name(c_raw)
    if not r or not c:
        return None
    if r_raw.casefold() == c_raw.casefold():
        return "exact"
    if r == c:
        return "normalized"

    alias = BRAND_SEARCH_KEYWORDS.get(r_raw)
    if alias and normalize_brand_name(alias) == c:
        return "alias"

    # 한 글자 오타/표기 흔들림 정도만 허용. 너무 짧은 이름은 fuzzy 금지.
    if min(len(r), len(c)) >= 5:
        ratio = difflib.SequenceMatcher(None, r, c).ratio()
        if ratio >= 0.88:
            return "fuzzy"
    return None


def search_brand_products(brand_name, known_goods=None, exhaustive=False):
    """
    브랜드 검색 결과를 페이지 끝까지 확인하고, 브랜드명은
    exact -> normalized(공백/기호 무시) -> 제한적인 fuzzy 순으로 매칭합니다.

    반환: (products, audit_row)
    """
    brand_name = brand_name.strip()
    if not brand_name:
        return [], {
            "checked_at": now_kst().isoformat(timespec="seconds"),
            "requested_brand": "", "matched_brand": "", "match_mode": "",
            "search_result_total": 0, "matched_products": 0, "pages_scanned": 0,
            "top_candidate_brands": "", "status": "empty_brand",
        }

    known_goods = set(str(x) for x in (known_goods or set()))
    search_term = BRAND_SEARCH_KEYWORDS.get(brand_name, brand_name)
    keyword = urllib.parse.quote(search_term)
    count_url = (
        "https://api.musinsa.com/api2/sc/v2/search/tab/count"
        f"?gf=A&keyword={keyword}&sendLog=true"
    )
    try:
        count_data = search_json(count_url, search_term)
        total = to_int(((((count_data or {}).get("data") or {}).get("goods") or {}).get("all"))) or 0
    except Exception:
        total = 0

    page_size = 60
    total_pages = max(1, (total + page_size - 1) // page_size) if total > 0 else MAX_SEARCH_PAGES
    page_limit = min(total_pages, MAX_SEARCH_PAGES if exhaustive else QUICK_SEARCH_MAX_PAGES)

    results, seen = [], set()
    seen_known = set()
    known_only_streak = 0
    candidate_counts = {}
    match_mode_counts = {}
    matched_name_counts = {}
    pages_scanned = 0

    for page in range(1, page_limit + 1):
        url = (
            "https://api.musinsa.com/api2/dp/v1/plp/goods"
            f"?gf=A&keyword={keyword}&sortCode=NEW&page={page}&size={page_size}&caller=SEARCH"
        )
        try:
            data = search_json(url, search_term)
        except Exception as e:
            print(f"[search] {brand_name} page {page}: {e}", file=sys.stderr)
            continue

        pages_scanned += 1
        items = (((data or {}).get("data") or {}).get("list") or [])
        if not isinstance(items, list) or not items:
            break

        matched_page = 0
        new_page = 0
        for item in items:
            if not isinstance(item, dict):
                continue
            item_brand = str(item.get("brandName") or "").strip()
            if item_brand:
                candidate_counts[item_brand] = candidate_counts.get(item_brand, 0) + 1

            mode = brand_match_mode(brand_name, item_brand)
            if not mode:
                continue

            goods_no = str(item.get("goodsNo") or "").strip()
            if not goods_no or goods_no in seen:
                continue

            seen.add(goods_no)
            matched_page += 1
            match_mode_counts[mode] = match_mode_counts.get(mode, 0) + 1
            matched_name_counts[item_brand] = matched_name_counts.get(item_brand, 0) + 1

            if goods_no in known_goods:
                seen_known.add(goods_no)
            else:
                new_page += 1

            current_price = to_int(item.get("finalPrice"))
            if current_price is None:
                current_price = to_int(item.get("price"))

            results.append({
                "goods_no": goods_no,
                "brand_name": item_brand,
                "product_name": item.get("goodsName") or "",
                "normal_price": to_int(item.get("normalPrice")),
                "current_price": current_price,
                "sale_rate": to_int(item.get("finalDiscount")) if to_int(item.get("finalDiscount")) is not None else to_int(item.get("saleRate")),
                "review_count": to_int(item.get("reviewCount")),
                "rating": item.get("reviewScore"),
                "availability": "OutOfStock" if item.get("isSoldOut") else "InStock",
                "product_url": f"https://www.musinsa.com/products/{goods_no}",
            })

        if not exhaustive and known_goods:
            all_known_seen = len(seen_known) >= len(known_goods)
            if matched_page > 0 and new_page == 0:
                known_only_streak += 1
            else:
                known_only_streak = 0
            if all_known_seen and known_only_streak >= 1:
                break

        if not exhaustive and not known_goods:
            if matched_page == 0:
                known_only_streak += 1
            else:
                known_only_streak = 0
            if known_only_streak >= QUICK_KNOWN_STOP_PAGES:
                break

        time.sleep(0.15 + random.uniform(0.03, 0.12))

    top_candidates = sorted(candidate_counts.items(), key=lambda x: (-x[1], x[0]))[:5]
    top_candidates_text = " | ".join(f"{name}:{cnt}" for name, cnt in top_candidates)
    matched_brand = ""
    if matched_name_counts:
        matched_brand = max(matched_name_counts.items(), key=lambda x: x[1])[0]
    mode = ""
    if match_mode_counts:
        for preferred in ("exact", "normalized", "alias", "fuzzy"):
            if match_mode_counts.get(preferred):
                mode = preferred
                break

    status = "ok" if results else ("no_brand_match" if total else "no_search_results")
    audit = {
        "checked_at": now_kst().isoformat(timespec="seconds"),
        "requested_brand": brand_name,
        "matched_brand": matched_brand,
        "match_mode": mode,
        "search_result_total": total,
        "matched_products": len(results),
        "pages_scanned": pages_scanned,
        "top_candidate_brands": top_candidates_text,
        "status": status,
    }
    return results, audit

def discover_slot(state_dir, slot, force_full=False):
    state_dir = Path(state_dir)
    state_dir.mkdir(parents=True, exist_ok=True)
    slot = int(slot)
    snapshot_date = now_kst().date()

    (state_dir / "snapshot_date.txt").write_text(snapshot_date.isoformat() + "\n", encoding="utf-8")
    (state_dir / "slot.txt").write_text(str(slot) + "\n", encoding="utf-8")

    brands = read_lines(BRANDS_FILE)
    assigned_brands = [b for b in brands if brand_slot(b) == slot]
    existing_watchlist = read_lines(WATCHLIST_FILE)

    catalog_rows = read_csv(CATALOG_FILE)
    catalog = {str(r.get("goods_no") or ""): dict(r) for r in catalog_rows if r.get("goods_no")}
    existing_goods = set(catalog)
    existing_audit_rows = read_csv(BRAND_AUDIT_FILE)

    known_by_brand = {}
    for g, row in catalog.items():
        b = str(row.get("brand_name") or "").strip().casefold()
        if b:
            known_by_brand.setdefault(b, set()).add(g)

    weekly_full = snapshot_date.weekday() == 6  # Sunday KST
    ts = now_kst().isoformat(timespec="seconds")

    found = {}
    modes = {}
    audit_by_brand = {}

    if assigned_brands:
        with ThreadPoolExecutor(max_workers=max(1, DISCOVERY_WORKERS)) as executor:
            futures = {}
            for brand in assigned_brands:
                known = known_by_brand.get(brand.casefold(), set())
                exhaustive = bool(force_full or weekly_full or not known)
                modes[brand] = "exhaustive" if exhaustive else "daily_price_scan"
                futures[executor.submit(search_brand_products, brand, known, exhaustive)] = brand

            for fut in as_completed(futures):
                brand = futures[fut]
                try:
                    products_found, audit_row = fut.result()
                    found[brand] = products_found
                    audit_by_brand[brand] = audit_row
                except Exception as e:
                    print(f"[discover] {brand}: {e}", file=sys.stderr)
                    found[brand] = []
                    audit_by_brand[brand] = {
                        "checked_at": ts, "requested_brand": brand, "matched_brand": "",
                        "match_mode": "", "search_result_total": "", "matched_products": 0,
                        "pages_scanned": "", "top_candidate_brands": "", "status": f"error: {e}",
                    }

    new_rows = []
    lifecycle_probes = 0
    # Do not explode API calls during a brand-new catalog bootstrap.  Once the
    # tracker already has a catalog, newly reappearing goods are few enough to
    # probe for historical existence evidence.
    lifecycle_probe_enabled = bool(existing_goods)
    for brand in assigned_brands:
        for p in found.get(brand, []):
            g = str(p["goods_no"])
            old = catalog.get(g, {})
            is_new_discovery = g not in existing_goods
            lifecycle_status = str(old.get("lifecycle_status") or "").strip()
            lifecycle_evidence_type = str(old.get("lifecycle_evidence_type") or "").strip()
            lifecycle_evidence_date = str(old.get("lifecycle_evidence_date") or "").strip()
            lifecycle_checked_at = str(old.get("lifecycle_checked_at") or "").strip()

            if (
                is_new_discovery
                and lifecycle_probe_enabled
                and lifecycle_probes < max(0, LIFECYCLE_DISCOVERY_PROBE_MAX)
            ):
                lifecycle_probes += 1
                ev = lifecycle_evidence(g)
                lifecycle_checked_at = ev.get("checked_at") or lifecycle_checked_at
                lifecycle_evidence_type = ev.get("evidence_type") or lifecycle_evidence_type
                lifecycle_evidence_date = ev.get("evidence_date") or lifecycle_evidence_date
                if ev.get("status") == "preexisting_confirmed":
                    lifecycle_status = "reactivated_pretracker"
                elif ev.get("status") == "preexisting_probable":
                    lifecycle_status = "reactivated_pretracker_probable"
                elif not lifecycle_status:
                    lifecycle_status = "first_seen_unverified"
            elif is_new_discovery and not lifecycle_status:
                lifecycle_status = "first_seen_unverified"

            row = {
                "goods_no": g,
                "brand_name": p.get("brand_name") or old.get("brand_name") or "",
                "product_name": p.get("product_name") or old.get("product_name") or "",
                "normal_price": p.get("normal_price") if p.get("normal_price") is not None else old.get("normal_price", ""),
                "current_price": p.get("current_price") if p.get("current_price") is not None else old.get("current_price", ""),
                "sale_rate": p.get("sale_rate") if p.get("sale_rate") is not None else old.get("sale_rate", ""),
                "review_count": p.get("review_count") if p.get("review_count") is not None else old.get("review_count", ""),
                "rating": p.get("rating") if p.get("rating") not in (None, "") else old.get("rating", ""),
                "availability": p.get("availability") or old.get("availability") or "",
                "first_seen_at": old.get("first_seen_at") or ts,
                "last_seen_at": ts,
                "lifecycle_status": lifecycle_status,
                "lifecycle_evidence_type": lifecycle_evidence_type,
                "lifecycle_evidence_date": lifecycle_evidence_date,
                "lifecycle_checked_at": lifecycle_checked_at,
                "product_url": p.get("product_url") or old.get("product_url") or f"https://www.musinsa.com/products/{g}",
            }
            catalog[g] = row
            if g not in existing_goods:
                # A goodsNo with evidence older than the tracker is a reactivated
                # existing item, not a genuine new product. Keep it in catalog,
                # but do not inflate the new-product feed/count.
                if lifecycle_status not in ("reactivated_pretracker", "reactivated_pretracker_probable"):
                    new_rows.append({
                        "first_seen_at": ts,
                        "brand_name": row["brand_name"],
                        "goods_no": g,
                        "product_name": row["product_name"],
                        "normal_price": row["normal_price"],
                        "current_price": row["current_price"],
                        "sale_rate": row["sale_rate"],
                        "lifecycle_status": lifecycle_status or "first_seen_unverified",
                    })
                existing_goods.add(g)

    watchlist = []
    seen = set()
    for g in existing_watchlist + list(catalog.keys()):
        g = str(g).strip()
        if g and g not in seen:
            watchlist.append(g)
            seen.add(g)
    watchlist.sort(key=lambda x: int(x) if x.isdigit() else 10**30)

    catalog_sorted = [
        catalog[g] for g in sorted(catalog, key=lambda x: int(x) if str(x).isdigit() else 10**30)
    ]
    slot_goods = [g for g in watchlist if effective_goods_slot(g, catalog.get(g)) == slot]
    shard_count = recommended_slot_shards(len(slot_goods))

    write_lines(state_dir / "musinsa_watchlist.txt", watchlist)
    write_csv(state_dir / "musinsa_catalog.csv", catalog_sorted, CATALOG_FIELDS)
    write_csv(state_dir / "new_products_delta.csv", new_rows, NEW_PRODUCT_FIELDS)

    # 브랜드별 discovery 결과를 누적 저장. 같은 requested_brand는 최신 결과로 교체.
    assigned_keys = {str(b).strip().casefold() for b in assigned_brands}
    audit_combined = [
        r for r in existing_audit_rows
        if str(r.get("requested_brand") or "").strip().casefold() not in assigned_keys
    ]
    audit_combined.extend(audit_by_brand.get(b, {
        "checked_at": ts, "requested_brand": b, "matched_brand": "", "match_mode": "",
        "search_result_total": "", "matched_products": len(found.get(b, [])),
        "pages_scanned": "", "top_candidate_brands": "", "status": "missing_audit",
    }) for b in assigned_brands)
    audit_combined.sort(key=lambda r: str(r.get("requested_brand") or "").casefold())
    write_csv(state_dir / "musinsa_brand_audit.csv", audit_combined, BRAND_AUDIT_FIELDS)

    stats = {
        "checked_at": ts,
        "snapshot_date": snapshot_date.isoformat(),
        "slot": slot,
        "registered_brands": len(brands),
        "assigned_brands": len(assigned_brands),
        "watchlist_count": len(watchlist),
        "slot_goods_count": len(slot_goods),
        "recommended_shards": shard_count,
        "new_products": len(new_rows),
        "weekly_full": weekly_full,
        "daily_price_refresh": True,
        "scan_modes": modes,
        "found_by_brand": {b: len(found.get(b, [])) for b in assigned_brands},
    }
    (state_dir / "discovery_stats.json").write_text(
        json.dumps(stats, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(json.dumps(stats, ensure_ascii=False))
    return 0


def fetch_stat(goods_no, retries=2):
    time.sleep(random.uniform(0.02, 0.12))
    url = f"https://goods-detail.musinsa.com/api2/goods/{goods_no}/stat"
    obj = json.loads(http_get(url, retries=retries))
    if isinstance(obj, dict) and "data" in obj:
        obj = obj.get("data")
    if not isinstance(obj, dict):
        raise ValueError("invalid stat response")
    return to_int(obj.get("purchaseTotal")), to_int(obj.get("pageViewTotal"))

def extract_script(html, attr_pattern):
    pattern = re.compile(rf"<script[^>]*{attr_pattern}[^>]*>(.*?)</script>", re.I | re.S)
    m = pattern.search(html)
    return html_lib.unescape(m.group(1).strip()) if m else None


def fallback_metadata(goods_no):
    result = {
        "goods_no": str(goods_no), "brand_name": "", "product_name": "",
        "normal_price": None, "current_price": None, "sale_rate": None,
        "review_count": None, "rating": None, "availability": "",
        "product_url": f"https://www.musinsa.com/products/{goods_no}",
    }
    try:
        page = http_get(result["product_url"], retries=2)
        raw = extract_script(page, r'type=["\']application/ld\+json["\']')
        if raw:
            obj = json.loads(raw)
            candidates = obj if isinstance(obj, list) else [obj]
            product = None
            for x in candidates:
                if isinstance(x, dict) and x.get("@type") == "Product":
                    product = x
                    break
                if isinstance(x, dict) and isinstance(x.get("@graph"), list):
                    for y in x["@graph"]:
                        if isinstance(y, dict) and y.get("@type") == "Product":
                            product = y
                            break
            if product:
                result["product_name"] = product.get("name") or ""
                brand = product.get("brand")
                if isinstance(brand, dict):
                    result["brand_name"] = brand.get("name") or ""
                offers = product.get("offers")
                if isinstance(offers, list) and offers:
                    offers = offers[0]
                if isinstance(offers, dict):
                    result["current_price"] = to_int(offers.get("price"))
                    result["normal_price"] = result["current_price"]
    except Exception:
        pass
    return result


def collect_one(goods_no, catalog_row, retries=2):
    meta = dict(catalog_row or {}) if catalog_row else fallback_metadata(goods_no)
    errors = []
    purchase_total = page_view_total = None
    try:
        purchase_total, page_view_total = fetch_stat(goods_no, retries=retries)
    except Exception as e:
        errors.append(f"stat: {e}")

    price = to_int(meta.get("current_price"))
    return {
        "checked_at": now_kst().isoformat(timespec="seconds"),
        "goods_no": str(goods_no),
        "brand_name": meta.get("brand_name") or "",
        "product_name": meta.get("product_name") or "",
        "purchase_total": purchase_total,
        "page_view_total": page_view_total,
        "normal_price": to_int(meta.get("normal_price")),
        "current_price": price,
        "sale_rate": to_int(meta.get("sale_rate")),
        "review_count": to_int(meta.get("review_count")),
        "rating": meta.get("rating") or "",
        "availability": meta.get("availability") or "",
        "simple_gmv": purchase_total * price if purchase_total is not None and price is not None else None,
        "product_url": meta.get("product_url") or f"https://www.musinsa.com/products/{goods_no}",
        "errors": "; ".join(errors),
    }

def collect_slot_shard(state_dir, slot, shard_index, shard_count, output):
    state_dir = Path(state_dir)
    slot, shard_index, shard_count = int(slot), int(shard_index), int(shard_count)

    watchlist = read_lines(state_dir / "musinsa_watchlist.txt")
    catalog_rows = read_csv(state_dir / "musinsa_catalog.csv")
    catalog = {str(r.get("goods_no") or ""): r for r in catalog_rows if r.get("goods_no")}

    slot_goods = [g for g in watchlist if effective_goods_slot(g, catalog.get(g)) == slot]
    selected = [g for i, g in enumerate(slot_goods) if i % shard_count == shard_index]
    deadline = time.monotonic() + max(300, COLLECT_BUDGET_SECONDS)

    work = Queue()
    for g in selected:
        work.put(g)
    rows = []
    rows_lock = threading.Lock()

    def worker():
        while time.monotonic() < deadline:
            try:
                g = work.get_nowait()
            except Empty:
                return
            try:
                r = collect_one(g, catalog.get(g), 2)
            except Exception as e:
                r = synthetic_failed_row(g, catalog.get(g), str(e))
            with rows_lock:
                rows.append(r)
                completed = len(rows)
                if completed % 1000 == 0:
                    print(f"[collect-progress] slot={slot} completed={completed}/{len(selected)}", flush=True)
            work.task_done()

    worker_count = max(1, SHARD_WORKERS)
    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        futures = [executor.submit(worker) for _ in range(worker_count)]
        for fut in futures:
            try:
                fut.result()
            except Exception as e:
                print(f"[worker-error] {e}", file=sys.stderr)

    by_goods = {str(r.get("goods_no") or ""): r for r in rows}
    failed = [g for g, r in by_goods.items() if to_int(r.get("purchase_total")) is None]

    # 종료 예산이 충분할 때만 실패 소수를 동일 실행에서 느린 2차 재시도.
    time_left = deadline - time.monotonic()
    inline = failed[:max(0, INLINE_RETRY_MAX)] if time_left > 180 else []
    if inline:
        THROTTLE.recovery_mode()
        print(f"[inline-retry] failed={len(failed)} retry_now={len(inline)} time_left={time_left:.0f}s")
        for g in inline:
            if time.monotonic() >= deadline - 60:
                break
            retry_row = collect_one(g, catalog.get(g), retries=3)
            if to_int(retry_row.get("purchase_total")) is not None:
                by_goods[g] = retry_row
            else:
                old = by_goods[g]
                old["checked_at"] = retry_row.get("checked_at") or old.get("checked_at")
                old["errors"] = retry_row.get("errors") or old.get("errors")

    rows = list(by_goods.values())
    rows.sort(key=lambda r: int(r["goods_no"]) if str(r.get("goods_no", "")).isdigit() else 10**30)
    write_csv(output, rows, RAW_FIELDS)
    failures = sum(1 for r in rows if to_int(r.get("purchase_total")) is None)
    not_attempted = max(0, len(selected) - len(rows))
    print(json.dumps({
        "slot": slot, "products_expected_in_job": len(selected),
        "rows_written": len(rows), "failures_after_inline_retry": failures,
        "not_attempted_before_budget": not_attempted,
        "adaptive_state": THROTTLE.state(),
    }, ensure_ascii=False))
    return 1 if selected and len(rows) == failures else 0

def adaptive_sales_score(row):
    """Stable sales-speed score used for sampling tier selection.

    Prefer the latest same-slot ~24h delta, but keep the 7d average as a
    stabilizer so a single quiet day does not immediately demote a fast seller.
    Negative/reset deltas are treated as zero.
    """
    vals = []
    for key in ("daily_sales", "sales_7d_avg_per_day"):
        v = to_float(row.get(key))
        if v is not None:
            vals.append(max(0.0, v))
    return max(vals) if vals else 0.0


def adaptive_sampling_tier(row):
    score = adaptive_sales_score(row)
    if score >= ADAPTIVE_HIGH_MIN:
        return "3h", score
    if score >= ADAPTIVE_MEDIUM_MIN:
        return "9h", score
    return "24h", score



def load_recent_extra_observations(goods_set, lookback_days=None):
    """Load only recent extra observations for the requested goodsNos.

    This scans data/observations, not the full 90k+ primary snapshots, so the
    adaptive selector can cheaply tell whether a new item has already received
    its one probe and whether purchaseTotal moved after the latest primary.
    """
    wanted = {str(g) for g in goods_set if str(g)}
    if not wanted:
        return {}

    days = max(
        1,
        int(
            NEW_PRODUCT_PROBE_LOOKBACK_DAYS
            if lookback_days in (None, 0)
            else lookback_days
        ),
    )
    today = now_kst().date()
    out = {}

    for back in range(days):
        folder = OBSERVATION_DIR / (today - timedelta(days=back)).isoformat()
        if not folder.exists():
            continue

        paths = sorted(list(folder.glob("*.csv.gz")) + list(folder.glob("*.csv")))
        for path in paths:
            for row in read_csv(path):
                g = str(row.get("goods_no") or "").strip()
                if g not in wanted:
                    continue
                checked = parse_kst_datetime(row.get("checked_at"))
                p = to_int(row.get("purchase_total"))
                if checked is None or p is None:
                    continue
                copied = dict(row)
                copied["_checked_dt"] = checked
                out.setdefault(g, []).append(copied)

    for rows in out.values():
        rows.sort(key=lambda r: r["_checked_dt"])
    return out


def probation_has_probe(catalog_row, extra_rows):
    """Whether at least one extra observation exists after first_seen."""
    first = _catalog_first_seen(catalog_row)
    if first is None:
        return False
    return any(
        (r.get("_checked_dt") or parse_kst_datetime(r.get("checked_at"))) >= first
        for r in (extra_rows or [])
    )


def probation_activity_score(latest_primary_row, extra_rows):
    """Observed sales pace after the latest primary baseline.

    No review-count inference and no assumed launch time.  We compare an actual
    primary purchaseTotal with a later observed purchaseTotal.  A positive delta
    is annualized only for sampling-tier selection; calendar sales remain based
    on the original observation intervals.
    """
    if not latest_primary_row:
        return 0.0

    base_p = to_int(latest_primary_row.get("purchase_total"))
    base_t = parse_kst_datetime(latest_primary_row.get("checked_at"))
    if base_p is None or base_t is None:
        return 0.0

    later = []
    for r in extra_rows or []:
        t = r.get("_checked_dt") or parse_kst_datetime(r.get("checked_at"))
        p = to_int(r.get("purchase_total"))
        if t is None or p is None or t <= base_t:
            continue
        later.append((t, p))

    if not later:
        return 0.0

    t1, p1 = max(later, key=lambda x: x[0])
    delta = p1 - base_p
    hours = (t1 - base_t).total_seconds() / 3600.0
    if delta <= 0 or hours <= 0:
        return 0.0

    return max(0.0, delta * 24.0 / hours)


def calendar_sampling_score(calendar_row):
    """Use the latest finalized calendar estimate as a stabilizer after day 1."""
    if not calendar_row:
        return 0.0
    v = to_float(calendar_row.get("estimated_sales"))
    return max(0.0, v) if v is not None else 0.0


def tier_for_score(score):
    score = max(0.0, float(score or 0.0))
    if score >= ADAPTIVE_HIGH_MIN:
        return "3h"
    if score >= ADAPTIVE_MEDIUM_MIN:
        return "9h"
    return "24h"


def adaptive_due(base_slot, clock_slot, tier):
    """Return True when an extra observation is due in this 3h clock block."""
    offset = (int(clock_slot) - int(base_slot)) % SLOT_COUNT
    if tier == "probe_6h":
        return offset == NEW_PRODUCT_PROBE_OFFSET
    if tier == "9h":
        return offset in (3, 6)
    if tier == "3h":
        return offset in (1, 2, 3, 4, 5, 6, 7)
    return False

def _adaptive_run_token(clock_slot):
    stamp = now_kst().strftime("%H%M%S")
    run_id = re.sub(r"[^0-9A-Za-z_.-]+", "-", os.environ.get("GITHUB_RUN_ID", "local"))
    return f"clock-{int(clock_slot)}-{stamp}-{run_id}"


def save_adaptive_observations(raw_rows, meta_by_goods, clock_slot):
    """Archive successful extra observations without overwriting daily baseline snapshots."""
    grouped = {}
    for r in raw_rows:
        if to_int(r.get("purchase_total")) is None:
            continue
        checked = parse_kst_datetime(r.get("checked_at")) or now_kst()
        d = checked.date().isoformat()
        g = str(r.get("goods_no") or "")
        meta = meta_by_goods.get(g, {})
        row = compact_from_raw(r, checked.date(), meta.get("base_slot", 0))
        row.update({
            "sample_kind": "adaptive",
            "sampling_tier": meta.get("tier", ""),
            "sampling_score": round(float(meta.get("score") or 0.0), 2),
            "base_slot": meta.get("base_slot", ""),
            "clock_slot": int(clock_slot),
        })
        grouped.setdefault(d, []).append(row)

    token = _adaptive_run_token(clock_slot)
    paths = []
    for date_text, rows in grouped.items():
        folder = OBSERVATION_DIR / date_text
        folder.mkdir(parents=True, exist_ok=True)
        path = folder / f"adaptive-{token}.csv.gz"
        rows.sort(key=lambda x: int(x["goods_no"]) if str(x.get("goods_no", "")).isdigit() else 10**30)
        write_csv(path, rows, ADAPTIVE_OBS_FIELDS)
        paths.append(str(path.relative_to(BASE_DIR)))
    return paths


def collect_adaptive(clock_slot, max_products=None, dry_run=False):
    """Collect extra observations with a bulk-safe discovery policy.

    Existing sellers keep the normal 3h/9h tiers.

    A newly discovered product is NOT automatically sampled every 3h. Instead:
      1. first primary observation = baseline
      2. one probe ~6h later (subject to a separate cap)
      3. if purchaseTotal actually increased, observed pace promotes it to 9h/3h
      4. latest finalized calendar sales also keeps a proven seller promoted

    This prevents hundreds of newly added brands from turning every newly
    discovered SKU into a 3-hour polling job.
    """
    clock_slot = int(clock_slot)
    max_products = ADAPTIVE_MAX_PER_RUN if max_products in (None, 0) else int(max_products)

    latest_rows = read_csv(LATEST_PRODUCT_FILE)
    catalog_rows = read_csv(CATALOG_FILE)
    calendar_rows = read_csv(CALENDAR_LATEST_PRODUCT_FILE)

    catalog = {
        str(r.get("goods_no") or ""): r
        for r in catalog_rows
        if r.get("goods_no")
    }
    calendar = {
        str(r.get("goods_no") or ""): r
        for r in calendar_rows
        if r.get("goods_no")
    }

    probation_goods = {
        str(row.get("goods_no") or "").strip()
        for row in latest_rows
        if str(row.get("goods_no") or "").strip()
        and product_in_probation(
            catalog.get(str(row.get("goods_no") or "").strip())
        )
    }
    recent_extra = load_recent_extra_observations(probation_goods)

    active_due = []
    probe_due = []

    for row in latest_rows:
        g = str(row.get("goods_no") or "").strip()
        if not g:
            continue

        cat = catalog.get(g)
        normal_score = adaptive_sales_score(row)
        cal_score = calendar_sampling_score(calendar.get(g))
        score = max(normal_score, cal_score)

        probation = g in probation_goods
        activity_score = (
            probation_activity_score(row, recent_extra.get(g, []))
            if probation
            else 0.0
        )
        score = max(score, activity_score)
        tier = tier_for_score(score)

        base_slot = to_int(row.get("slot"))
        if base_slot is None or not (0 <= base_slot < SLOT_COUNT):
            base_slot = effective_goods_slot(g, cat)

        if tier != "24h":
            if not adaptive_due(base_slot, clock_slot, tier):
                continue
            active_due.append({
                "goods_no": g,
                "tier": tier,
                "score": score,
                "base_slot": base_slot,
                "reason": "probation_activity" if activity_score >= max(normal_score, cal_score) and activity_score > 0 else "sales_score",
            })
            continue

        # Quiet/unknown newly discovered goods get only ONE lightweight probe.
        if not probation:
            continue
        if probation_has_probe(cat, recent_extra.get(g, [])):
            continue
        if not adaptive_due(base_slot, clock_slot, "probe_6h"):
            continue

        probe_due.append({
            "goods_no": g,
            "tier": "probe_6h",
            "score": 0.0,
            "base_slot": base_slot,
            "reason": "first_probe",
        })

    # Proven sellers always have priority over discovery probes.
    active_due.sort(
        key=lambda x: (
            0 if x["tier"] == "3h" else 1,
            -x["score"],
            int(x["goods_no"]) if x["goods_no"].isdigit() else 10**30,
        )
    )

    selected_active = active_due[:max_products] if max_products > 0 else active_due[:]

    remaining = (
        max(0, max_products - len(selected_active))
        if max_products > 0
        else len(probe_due)
    )
    probe_cap = max(0, NEW_PRODUCT_PROBE_MAX_PER_RUN)
    probe_take = min(remaining, probe_cap) if max_products > 0 else probe_cap

    # Oldest unprobed items first; goodsNo breaks ties deterministically.
    probe_due.sort(
        key=lambda x: (
            _catalog_first_seen(catalog.get(x["goods_no"])) or now_kst(),
            int(x["goods_no"]) if x["goods_no"].isdigit() else 10**30,
        )
    )
    selected_probe = probe_due[:probe_take]

    selected = selected_active + selected_probe
    total_due = len(active_due) + len(probe_due)

    stats = {
        "checked_at": now_kst().isoformat(timespec="seconds"),
        "clock_slot": clock_slot,
        "thresholds": {
            "medium_min": ADAPTIVE_MEDIUM_MIN,
            "high_min": ADAPTIVE_HIGH_MIN,
        },
        "bulk_safe_new_product_policy": {
            "probation_hours": NEW_PRODUCT_PROBATION_HOURS,
            "probe_offset_slots": NEW_PRODUCT_PROBE_OFFSET,
            "probe_approx_hours": NEW_PRODUCT_PROBE_OFFSET * 3,
            "probe_max_per_run": NEW_PRODUCT_PROBE_MAX_PER_RUN,
            "blanket_3h_probation": False,
        },
        "probation_goods": len(probation_goods),
        "active_due_before_cap": len(active_due),
        "probe_due_before_cap": len(probe_due),
        "due_before_cap": total_due,
        "selected": len(selected),
        "selected_probe_6h": len(selected_probe),
        "selected_3h": sum(1 for x in selected if x["tier"] == "3h"),
        "selected_9h": sum(1 for x in selected if x["tier"] == "9h"),
        "deferred_probes": max(0, len(probe_due) - len(selected_probe)),
        "max_products": max_products,
        "dry_run": bool(dry_run),
    }

    if dry_run or not selected:
        print(json.dumps(stats, ensure_ascii=False))
        return 0

    meta_by_goods = {x["goods_no"]: x for x in selected}
    deadline = time.monotonic() + max(300, COLLECT_BUDGET_SECONDS)
    work = Queue()
    for x in selected:
        work.put(x["goods_no"])

    rows = []
    rows_lock = threading.Lock()

    def worker():
        while time.monotonic() < deadline:
            try:
                g = work.get_nowait()
            except Empty:
                return
            try:
                r = collect_one(g, catalog.get(g), 2)
            except Exception as e:
                r = synthetic_failed_row(g, catalog.get(g), str(e))
            with rows_lock:
                rows.append(r)
            work.task_done()

    with ThreadPoolExecutor(max_workers=max(1, SHARD_WORKERS)) as executor:
        futures = [executor.submit(worker) for _ in range(max(1, SHARD_WORKERS))]
        for fut in futures:
            try:
                fut.result()
            except Exception as e:
                print(f"[adaptive-worker-error] {e}", file=sys.stderr)

    by_goods = {str(r.get("goods_no") or ""): r for r in rows}
    success_rows = [r for r in by_goods.values() if to_int(r.get("purchase_total")) is not None]
    failed_goods = [g for g in meta_by_goods if g not in by_goods or to_int(by_goods[g].get("purchase_total")) is None]
    not_attempted = max(0, len(selected) - len(by_goods))

    paths = save_adaptive_observations(success_rows, meta_by_goods, clock_slot)

    report_date = now_kst().date().isoformat()
    report_dir = ADAPTIVE_REPORT_DIR / report_date
    report_dir.mkdir(parents=True, exist_ok=True)
    token = _adaptive_run_token(clock_slot)
    stats.update({
        "success": len(success_rows),
        "failed_or_unattempted": len(failed_goods),
        "not_attempted_before_budget": not_attempted,
        "observation_files": paths,
        "adaptive_state": THROTTLE.state(),
        "failed_goods_sample": failed_goods[:100],
    })
    (report_dir / f"run-{token}.json").write_text(
        json.dumps(stats, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(json.dumps(stats, ensure_ascii=False))
    return 0 if success_rows else 1



def _midnight_anchor_token():
    stamp = now_kst().strftime("%Y%m%d-%H%M%S")
    run_id = re.sub(r"[^0-9A-Za-z_.-]+", "-", os.environ.get("GITHUB_RUN_ID", "local"))
    return f"{stamp}-{run_id}"


def save_midnight_anchor_observations(raw_rows, meta_by_goods):
    """Save the day-boundary observations without touching primary snapshots."""
    grouped = {}
    for r in raw_rows:
        if to_int(r.get("purchase_total")) is None:
            continue
        checked = parse_kst_datetime(r.get("checked_at")) or now_kst()
        date_text = checked.date().isoformat()
        g = str(r.get("goods_no") or "")
        meta = meta_by_goods.get(g, {})

        row = compact_from_raw(r, checked.date(), meta.get("base_slot", 0))
        row.update({
            "sample_kind": "midnight_anchor",
            "sampling_tier": meta.get("tier", ""),
            "sampling_score": round(float(meta.get("score") or 0.0), 2),
            "base_slot": meta.get("base_slot", ""),
            "clock_slot": 0,
        })
        grouped.setdefault(date_text, []).append(row)

    token = _midnight_anchor_token()
    paths = []
    for date_text, rows in grouped.items():
        folder = OBSERVATION_DIR / date_text
        folder.mkdir(parents=True, exist_ok=True)
        path = folder / f"midnight-anchor-{token}.csv.gz"
        rows.sort(
            key=lambda x: int(x["goods_no"])
            if str(x.get("goods_no", "")).isdigit()
            else 10**30
        )
        write_csv(path, rows, ADAPTIVE_OBS_FIELDS)
        paths.append(str(path.relative_to(BASE_DIR)))
    return paths


def collect_midnight_anchor(max_products=None, dry_run=False):
    """Collect an extra observation near the KST day boundary.

    Selection:
    - sales-speed score >= MIDNIGHT_ANCHOR_MIN (default 3/day)
    - base slot 0 is skipped because its primary observation is already near 00:15
    - faster sellers are collected first if the safety cap is reached

    These observations are used only by Calendar Finalizer and never overwrite
    the normal primary snapshots.
    """
    max_products = (
        MIDNIGHT_ANCHOR_MAX_PER_RUN
        if max_products in (None, 0)
        else int(max_products)
    )

    latest_rows = read_csv(LATEST_PRODUCT_FILE)
    catalog_rows = read_csv(CATALOG_FILE)
    catalog = {
        str(r.get("goods_no") or ""): r
        for r in catalog_rows
        if r.get("goods_no")
    }
    calendar_rows = read_csv(CALENDAR_LATEST_PRODUCT_FILE)
    calendar = {
        str(r.get("goods_no") or ""): r
        for r in calendar_rows
        if r.get("goods_no")
    }
    probation_goods = {
        str(row.get("goods_no") or "").strip()
        for row in latest_rows
        if str(row.get("goods_no") or "").strip()
        and product_in_probation(
            catalog.get(str(row.get("goods_no") or "").strip())
        )
    }
    recent_extra = load_recent_extra_observations(probation_goods)

    selected = []
    for row in latest_rows:
        g = str(row.get("goods_no") or "").strip()
        if not g:
            continue

        score = adaptive_sales_score(row)
        cal_row = calendar.get(g)
        score = max(score, calendar_sampling_score(cal_row))
        if g in probation_goods:
            score = max(
                score,
                probation_activity_score(row, recent_extra.get(g, [])),
            )
        if score < MIDNIGHT_ANCHOR_MIN:
            continue

        base_slot = to_int(row.get("slot"))
        if base_slot is None or not (0 <= base_slot < SLOT_COUNT):
            base_slot = effective_goods_slot(g, catalog.get(g))

        # slot0 primary is already around the day boundary.
        if base_slot == 0:
            continue

        tier = tier_for_score(score)
        selected.append({
            "goods_no": g,
            "score": score,
            "tier": tier,
            "base_slot": base_slot,
        })

    selected.sort(
        key=lambda x: (
            -x["score"],
            int(x["goods_no"]) if x["goods_no"].isdigit() else 10**30
        )
    )
    due_before_cap = len(selected)
    if max_products > 0:
        selected = selected[:max_products]

    stats = {
        "checked_at": now_kst().isoformat(timespec="seconds"),
        "anchor": "KST day-boundary",
        "threshold_min_sales_per_day": MIDNIGHT_ANCHOR_MIN,
        "due_before_cap": due_before_cap,
        "selected": len(selected),
        "max_products": max_products,
        "dry_run": bool(dry_run),
    }

    if dry_run or not selected:
        print(json.dumps(stats, ensure_ascii=False))
        return 0

    meta_by_goods = {x["goods_no"]: x for x in selected}
    deadline = time.monotonic() + max(300, COLLECT_BUDGET_SECONDS)
    work = Queue()
    for x in selected:
        work.put(x["goods_no"])

    rows = []
    rows_lock = threading.Lock()

    def worker():
        while time.monotonic() < deadline:
            try:
                g = work.get_nowait()
            except Empty:
                return
            try:
                r = collect_one(g, catalog.get(g), 2)
            except Exception as e:
                r = synthetic_failed_row(g, catalog.get(g), str(e))
            with rows_lock:
                rows.append(r)
            work.task_done()

    with ThreadPoolExecutor(max_workers=max(1, SHARD_WORKERS)) as executor:
        futures = [executor.submit(worker) for _ in range(max(1, SHARD_WORKERS))]
        for fut in futures:
            try:
                fut.result()
            except Exception as e:
                print(f"[midnight-anchor-worker-error] {e}", file=sys.stderr)

    by_goods = {str(r.get("goods_no") or ""): r for r in rows}
    success_rows = [
        r for r in by_goods.values()
        if to_int(r.get("purchase_total")) is not None
    ]
    failed_goods = [
        g for g in meta_by_goods
        if g not in by_goods or to_int(by_goods[g].get("purchase_total")) is None
    ]

    paths = save_midnight_anchor_observations(success_rows, meta_by_goods)

    report_date = now_kst().date().isoformat()
    report_dir = MIDNIGHT_REPORT_DIR / report_date
    report_dir.mkdir(parents=True, exist_ok=True)
    token = _midnight_anchor_token()

    stats.update({
        "success": len(success_rows),
        "failed_or_unattempted": len(failed_goods),
        "observation_files": paths,
        "adaptive_state": THROTTLE.state(),
        "failed_goods_sample": failed_goods[:100],
    })

    (report_dir / f"run-{token}.json").write_text(
        json.dumps(stats, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )
    print(json.dumps(stats, ensure_ascii=False))
    return 0 if success_rows else 1



def load_snapshot_any_slot(date_value):
    """v6→v7 slot 재배치와 과거 기록 호환을 위해 날짜별 8개 slot을 goodsNo로 합칩니다."""
    out = {}
    for s in range(SLOT_COUNT):
        path = SLOT_DIR / f"slot-{s}" / f"{date_value.isoformat()}.csv.gz"
        if not path.exists():
            path = SLOT_DIR / f"slot-{s}" / f"{date_value.isoformat()}.csv"
        for r in read_csv(path):
            g = str(r.get("goods_no") or "")
            if g:
                out[g] = r
    return out


def write_history_manifest():
    slots = {}
    for s in range(SLOT_COUNT):
        dates = []
        folder = SLOT_DIR / f"slot-{s}"
        if folder.exists():
            for p in folder.iterdir():
                m = re.match(r"(\d{4}-\d{2}-\d{2})\.csv(?:\.gz)?$", p.name)
                if m:
                    dates.append(m.group(1))
        slots[str(s)] = sorted(set(dates))
    payload = {"updated_at": now_kst().isoformat(timespec="seconds"), "slots": slots}
    HISTORY_MANIFEST_FILE.parent.mkdir(parents=True, exist_ok=True)
    HISTORY_MANIFEST_FILE.write_text(json.dumps(payload, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")


_TRACKER_START_DATE_CACHE = None
_TRACKER_START_GOODS_CEILING_CACHE = None
_LIFECYCLE_CACHE_MEMORY = None


def tracker_start_date():
    """Return the first actual date for which this repository has tracker snapshots.

    This avoids hard-coding the late-August 2026 start when the repository itself
    already contains the authoritative first snapshot date.
    """
    global _TRACKER_START_DATE_CACHE
    if _TRACKER_START_DATE_CACHE is not None:
        return _TRACKER_START_DATE_CACHE

    dates = []
    roots = [SLOT_DIR, DAILY_DIR]
    date_re = re.compile(r"(20\d{2}-\d{2}-\d{2})")
    for root in roots:
        if not root.exists():
            continue
        try:
            for path in root.rglob("*"):
                if not path.is_file():
                    continue
                m = date_re.search(path.name)
                if not m:
                    continue
                try:
                    dates.append(datetime.strptime(m.group(1), "%Y-%m-%d").date())
                except Exception:
                    pass
        except Exception:
            pass

    if dates:
        value = min(dates)
    else:
        try:
            value = datetime.strptime(TRACKER_START_DATE_FALLBACK, "%Y-%m-%d").date()
        except Exception:
            value = datetime(2026, 8, 28).date()

    _TRACKER_START_DATE_CACHE = value
    return value



def tracker_start_goods_no_ceiling():
    """Largest numeric goodsNo observed on the first tracker snapshot date.

    Musinsa goodsNo behaves as an allocation sequence in practice, so a goodsNo
    below a number already observed on the tracker start date is strong supporting
    evidence that the ID itself existed before our tracker began.  This is treated
    as *probable*, not as an absolute fact, and review/Q&A dates remain stronger.
    """
    global _TRACKER_START_GOODS_CEILING_CACHE
    if _TRACKER_START_GOODS_CEILING_CACHE is not None:
        return _TRACKER_START_GOODS_CEILING_CACHE

    start = tracker_start_date().isoformat()
    nums = []
    for slot in range(SLOT_COUNT):
        for suffix in (".csv.gz", ".csv"):
            path = SLOT_DIR / f"slot-{slot}" / f"{start}{suffix}"
            if not path.exists():
                continue
            for row in read_csv(path):
                g = str(row.get("goods_no") or "").strip()
                if g.isdigit():
                    nums.append(int(g))

    # DAILY_DIR is a useful fallback when the individual first-day slot files
    # have already been compacted/moved.
    if not nums:
        for suffix in (".csv.gz", ".csv"):
            path = DAILY_DIR / f"{start}{suffix}"
            if not path.exists():
                continue
            for row in read_csv(path):
                g = str(row.get("goods_no") or "").strip()
                if g.isdigit():
                    nums.append(int(g))

    value = max(nums) if nums else 0
    _TRACKER_START_GOODS_CEILING_CACHE = value
    return value



def _load_lifecycle_cache():
    global _LIFECYCLE_CACHE_MEMORY
    if _LIFECYCLE_CACHE_MEMORY is not None:
        return _LIFECYCLE_CACHE_MEMORY
    data = {}
    try:
        if LIFECYCLE_EVIDENCE_FILE.exists():
            obj = json.loads(LIFECYCLE_EVIDENCE_FILE.read_text(encoding="utf-8"))
            if isinstance(obj, dict):
                data = obj
    except Exception:
        data = {}
    _LIFECYCLE_CACHE_MEMORY = data
    return data


def _save_lifecycle_cache():
    cache = _load_lifecycle_cache()
    LIFECYCLE_DIR.mkdir(parents=True, exist_ok=True)
    tmp = LIFECYCLE_EVIDENCE_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(LIFECYCLE_EVIDENCE_FILE)


def _unwrap_api_data(obj):
    if isinstance(obj, dict) and "data" in obj:
        return obj.get("data")
    return obj


def _date_only(value):
    if value in (None, ""):
        return None
    s = str(value).strip()
    # Most Musinsa timestamps are ISO-8601; Q&A can be YYYY-MM-DD.
    try:
        d = datetime.fromisoformat(s.replace("Z", "+00:00"))
        return d.date()
    except Exception:
        pass
    m = re.search(r"(20\d{2})[-./](\d{1,2})[-./](\d{1,2})", s)
    if not m:
        return None
    try:
        return datetime(int(m.group(1)), int(m.group(2)), int(m.group(3))).date()
    except Exception:
        return None


def _oldest_review_date(goods_no):
    """Best-effort oldest Musinsa review date using the public review API.

    The list endpoint is 0-based and supports sort=new.  We first obtain total
    count, then jump directly to the last page instead of crawling all reviews.
    """
    g = str(goods_no).strip()
    if not g:
        return None
    referer = f"https://www.musinsa.com/products/{g}"
    try:
        summary_url = f"https://goods.musinsa.com/api2/review/v1/goods/{g}/reviews/summary"
        summary_obj = json.loads(http_get(summary_url, timeout=LIFECYCLE_PROBE_TIMEOUT, retries=1, referer=referer))
        summary = _unwrap_api_data(summary_obj) or {}
        total = to_int(summary.get("totalCount") if isinstance(summary, dict) else None) or 0
        if total <= 0:
            return None

        page_size = 100
        last_page = max(0, (total - 1) // page_size)
        params = urllib.parse.urlencode({
            "page": last_page,
            "pageSize": page_size,
            "goodsNo": g,
            "sort": "new",
            "selectedSimilarNo": g,
            "myFilter": "false",
            "hasPhoto": "false",
            "isExperience": "false",
        })
        list_url = "https://goods.musinsa.com/api2/review/v1/view/list?" + params
        list_obj = json.loads(http_get(list_url, timeout=LIFECYCLE_PROBE_TIMEOUT, retries=1, referer=referer))
        payload = _unwrap_api_data(list_obj) or {}
        items = payload.get("list") if isinstance(payload, dict) else None
        if not isinstance(items, list):
            return None
        dates = []
        for item in items:
            if not isinstance(item, dict):
                continue
            d = _date_only(item.get("createDate") or item.get("pastDate") or item.get("registrationDate"))
            if d:
                dates.append(d)
        return min(dates) if dates else None
    except Exception:
        return None


def _oldest_qna_date(goods_no):
    """Best-effort pre-existence evidence for products with no reviews."""
    g = str(goods_no).strip()
    if not g:
        return None
    referer = f"https://www.musinsa.com/products/{g}"
    try:
        url = f"https://goods-detail.musinsa.com/api2/goods/{g}/question-and-answer?isExceptedSecret=false"
        obj = json.loads(http_get(url, timeout=LIFECYCLE_PROBE_TIMEOUT, retries=1, referer=referer))
        payload = _unwrap_api_data(obj) or {}
        items = payload.get("list") if isinstance(payload, dict) else None
        if not isinstance(items, list):
            return None
        dates = []
        for item in items:
            if not isinstance(item, dict):
                continue
            d = _date_only(item.get("registrationDate") or item.get("createDate"))
            if d:
                dates.append(d)
        return min(dates) if dates else None
    except Exception:
        return None


def lifecycle_evidence(goods_no, force=False):
    """Classify whether a goodsNo demonstrably existed before tracker start.

    Evidence priority:
      1) review written before the first local tracker snapshot (confirmed);
      2) Q&A written before the first local tracker snapshot (confirmed);
      3) goodsNo already below the first-day observed ID ceiling (probable).

    No evidence is *not* treated as proof that the item is new.
    """
    g = str(goods_no).strip()
    if not g:
        return {"status": "unknown", "evidence_type": "", "evidence_date": ""}

    cache = _load_lifecycle_cache()
    cached = cache.get(g)
    if isinstance(cached, dict) and not force:
        return cached

    boundary = tracker_start_date()
    candidates = []
    review_date = _oldest_review_date(g)
    if review_date:
        candidates.append((review_date, "review"))
    qna_date = _oldest_qna_date(g)
    if qna_date:
        candidates.append((qna_date, "qna"))

    old = [(d, typ) for d, typ in candidates if d < boundary]
    if old:
        evidence_date, evidence_type = min(old, key=lambda x: x[0])
        status = "preexisting_confirmed"
    else:
        # Secondary evidence for products that had no reviews/Q&A before the
        # tracker. The first-day max goodsNo provides a repository-local age
        # boundary. Because Musinsa does not publicly guarantee monotonic IDs,
        # keep this as probable rather than confirmed.
        ceiling = tracker_start_goods_no_ceiling()
        gnum = int(g) if g.isdigit() else 0
        if ceiling > 0 and gnum > 0 and gnum <= ceiling:
            evidence_date, evidence_type = boundary, "goods_no_before_start_ceiling"
            status = "preexisting_probable"
        elif candidates:
            evidence_date, evidence_type = min(candidates, key=lambda x: x[0])
            status = "no_pretracker_evidence"
        else:
            evidence_date, evidence_type = None, ""
            status = "unknown"

    result = {
        "status": status,
        "evidence_type": evidence_type,
        "evidence_date": evidence_date.isoformat() if evidence_date else "",
        "tracker_start_date": boundary.isoformat(),
        "checked_at": now_kst().isoformat(timespec="seconds"),
    }
    cache[g] = result
    try:
        _save_lifecycle_cache()
    except Exception:
        pass
    return result


def catalog_lifecycle_status(catalog_row):
    return str((catalog_row or {}).get("lifecycle_status") or "").strip()


def product_confirmed_pretracker(goods_no, catalog_row=None, allow_network=True):
    """True for confirmed or high-confidence probable pre-tracker goods."""
    status = catalog_lifecycle_status(catalog_row)
    if status in (
        "reactivated_pretracker", "reactivated_pretracker_probable",
        "preexisting_confirmed", "preexisting_probable",
    ):
        return True
    if not allow_network:
        cached = _load_lifecycle_cache().get(str(goods_no).strip(), {})
        return isinstance(cached, dict) and cached.get("status") in (
            "preexisting_confirmed", "preexisting_probable"
        )
    ev = lifecycle_evidence(goods_no)
    return ev.get("status") in ("preexisting_confirmed", "preexisting_probable")


def metric_delta(cur, baseline, field):
    cv = to_int(cur.get(field))
    bv = to_int(baseline.get(field)) if baseline else None
    if cv is None or bv is None:
        return None
    return cv - bv


def product_in_probation(catalog_row, when=None):
    """Lifecycle observation window for a newly discovered/reactivated item."""
    if not catalog_row:
        return False
    first = parse_kst_datetime(catalog_row.get("first_seen_at"))
    if first is None:
        return False
    now = when or now_kst()
    age_h = (now - first).total_seconds() / 3600.0
    return 0 <= age_h <= max(1, NEW_PRODUCT_PROBATION_HOURS)

def purchase_counter_observation_inconsistent(row):
    """Basic structural validation only.

    review_count is deliberately NOT used as a hard classifier.
    A launch can legitimately have reviews by the time our first crawler arrives,
    and a reactivated old item can also have old reviews.  Lifecycle decisions are
    therefore made from the time-series itself, not from review_count.
    """
    p = to_int((row or {}).get("purchase_total"))
    return p is None or p < 0

def guarded_purchase_delta(cur, baseline, older_baselines=()):
    """Counter-safe rolling delta.

    This function handles historical resets when an older trusted baseline exists.
    It intentionally does not guess whether a first-ever large jump is genuine.
    That decision is made by the calendar lifecycle resolver, which has access to
    multiple observations before/after the jump.
    """
    # Older rows may detect resets; they cannot substitute for the requested
    # period's missing baseline. Zero -> positive does not establish that the
    # counter had no historical purchases (sold-out APIs can report zero).
    if purchase_baseline_status(cur, baseline) != "ok":
        return None

    cv = to_int((cur or {}).get("purchase_total"))
    if cv is None:
        return None

    candidates = []
    for row in (baseline,) + tuple(older_baselines or ()):
        if not row or purchase_counter_observation_inconsistent(row):
            continue
        value = to_int(row.get("purchase_total"))
        if value is not None:
            candidates.append(value)

    if not candidates:
        return None

    reference = max(candidates)
    if cv < reference:
        return None
    return cv - reference


def purchase_baseline_status(cur, baseline, max_hours=None):
    if not baseline:
        return "baseline_missing"
    if purchase_counter_observation_inconsistent(cur) or purchase_counter_observation_inconsistent(baseline):
        return "counter_missing"
    t0, t1 = _row_dt(baseline), _row_dt(cur)
    if t0 is None or t1 is None:
        return "timestamp_missing"
    if t1 <= t0:
        return "out_of_order"
    if max_hours is not None and (t1 - t0).total_seconds() > max_hours * 3600:
        return "collection_gap"
    p0, p1 = to_int(baseline.get("purchase_total")), to_int(cur.get("purchase_total"))
    if p0 == 0 and p1 > 0:
        return "zero_baseline_unverified"
    if p1 < p0:
        return "counter_reset"
    return "ok"

def _catalog_first_seen(catalog_row):
    if not catalog_row:
        return None
    return parse_kst_datetime(catalog_row.get("first_seen_at"))

def _row_dt(row):
    return row.get("_checked_dt") or parse_kst_datetime(row.get("checked_at"))

def _is_true_first_observation(rows, catalog_row):
    """Whether rows[0] is close enough to catalog.first_seen_at to be the first crawl."""
    if not rows:
        return False
    first_seen = _catalog_first_seen(catalog_row)
    first_dt = _row_dt(rows[0])
    if first_seen is None or first_dt is None:
        return False
    gap_h = abs((first_dt - first_seen).total_seconds()) / 3600.0
    return gap_h <= max(1.0, INITIAL_JUMP_FIRST_SEEN_WINDOW_HOURS)

def resolve_initial_purchase_jump(rows, catalog_row=None):
    """Resolve the first large purchaseTotal jump for a newly discovered goodsNo.

    Returns: (rows_with_segments, state, initial_delta)

    state:
      none      - no special first-jump condition
      pending   - not enough evidence yet; first jump excluded *for now*
      rebase    - jump behaved like historical cumulative-value restoration; excluded

    Important:
    - We do NOT use review_count.
    - We do NOT say "0 means invalid".
    - The first observed cumulative value is always just a baseline.
    - Later sales do not prove that the disputed earlier jump was also sales.
    - The disputed boundary stays excluded; later observed increments remain usable.
    """
    rows = [dict(r) for r in rows]
    if len(rows) < 2 or not _is_true_first_observation(rows, catalog_row):
        for r in rows:
            r.setdefault("_counter_segment", 0)
        return rows, "none", 0

    v0 = to_int(rows[0].get("purchase_total"))
    v1 = to_int(rows[1].get("purchase_total"))
    t0 = _row_dt(rows[0])
    t1 = _row_dt(rows[1])
    if v0 is None or v1 is None or t0 is None or t1 is None or t1 <= t0:
        for r in rows:
            r.setdefault("_counter_segment", 0)
        return rows, "none", 0

    jump = v1 - v0

    # Critical pre-tracker reactivation rule:
    # if Musinsa itself has a review/Q&A dated before this tracker existed, this
    # goodsNo cannot be a genuine new product. Any positive first-interval jump
    # after rediscovery may include restored historical cumulative purchases, so
    # that boundary is never counted as sales. Later deltas remain fully usable.
    if jump > 0 and product_confirmed_pretracker(
        (catalog_row or {}).get("goods_no") or (rows[0].get("goods_no") if rows else ""),
        catalog_row,
        allow_network=False,
    ):
        rows[0]["_counter_segment"] = 0
        for r in rows[1:]:
            r["_counter_segment"] = 1
        return rows, "preexisting_reactivation", jump

    if jump < INITIAL_JUMP_MIN:
        for r in rows:
            r.setdefault("_counter_segment", 0)
        return rows, "none", max(0, jump)

    # Evaluate only observations inside the temporary 48h probation window.
    decision_end = t0 + timedelta(hours=max(6, NEW_PRODUCT_PROBATION_HOURS))
    future = [r for r in rows[2:] if (_row_dt(r) or decision_end) <= decision_end]

    # Until enough follow-up exists, do not invent a decision.
    # Mark the first interval as a separate segment so it is excluded from totals,
    # while every later observed delta remains usable.
    state = "pending"

    if future:
        later_values = [to_int(r.get("purchase_total")) for r in future]
        later_values = [v for v in later_values if v is not None]

        if later_values:
            # Growth after the disputed jump.
            highest = max([v1] + later_values)
            follow_growth = max(0, highest - v1)

            # Count genuine positive steps after v1.
            prev = v1
            positive_steps = 0
            for val in later_values:
                if val > prev:
                    positive_steps += 1
                prev = max(prev, val)

            last_dt = max([_row_dt(r) for r in future if _row_dt(r) is not None], default=t1)
            elapsed_h = max(0.0, (last_dt - t1).total_seconds() / 3600.0)

            # "genuine" requires material continuing growth, not merely one tiny tick.
            genuine_follow = max(
                INITIAL_JUMP_GENUINE_MIN_FOLLOW,
                int(round(jump * INITIAL_JUMP_GENUINE_FOLLOW_RATIO)),
            )
            # Continuing sales can coexist with restored historical purchases.
            # They are not evidence for retroactively admitting this boundary.

            # "rebase" requires a long, repeatedly confirmed plateau.
            # This is what the NAUTICA 0 -> 1906 -> 1906... pattern looks like.
            plateau_tol = max(5, int(round(jump * 0.01)))
            if (
                state != "genuine"
                and elapsed_h >= INITIAL_JUMP_PLATEAU_HOURS
                and len(future) >= INITIAL_JUMP_PLATEAU_CONFIRMATIONS
                and follow_growth <= plateau_tol
            ):
                state = "rebase"

    # Pending/rebase => first observation and all later observations live in
    # different segments. This excludes only the disputed first jump.
    rows[0]["_counter_segment"] = 0
    for r in rows[1:]:
        r["_counter_segment"] = 1
    return rows, state, jump

def _median(values):
    vals = sorted(float(v) for v in values if v is not None)
    if not vals:
        return 0.0
    n = len(vals)
    mid = n // 2
    if n % 2:
        return vals[mid]
    return (vals[mid - 1] + vals[mid]) / 2.0

def resolve_midstream_purchase_jumps(rows):
    """Split extreme mid-stream positive counter rebases from real sales.

    Existing `_counter_segment` boundaries are preserved.  Within each segment,
    an interval is considered a candidate only when all of the following hold:

    1) at least a few trusted prior intervals already exist;
    2) the positive jump is at least MIDSTREAM_JUMP_MIN;
    3) the jump is >= MIDSTREAM_JUMP_PRIOR_MULTIPLIER times the product's
       recent median 24h-normalized observed pace.

    Decision:
      - pending: candidate is suspicious but follow-up is not sufficient yet.
                 Exclude only that disputed boundary for now.
      - rebase:  >= configured hours / confirmations later, post-jump growth
                 remains tiny relative to the jump. Exclude boundary permanently.
      Later growth is counted in its own intervals, never used as proof to
      restore the disputed earlier boundary.

    The function intentionally uses only the purchaseTotal time-series.  It does
    not use review_count, product name changes, or brand heuristics as hard rules.
    """
    if len(rows) < max(4, MIDSTREAM_JUMP_PRIOR_INTERVALS + 2):
        return [dict(r) for r in rows], False, []

    out = [dict(r) for r in rows]
    anomaly = False
    events = []

    # Work segment by segment because downward-reset and initial-lifecycle
    # boundaries have already been established.
    seg_ids = []
    for r in out:
        sid = int(r.get("_counter_segment", 0))
        if sid not in seg_ids:
            seg_ids.append(sid)

    next_segment_id = max(seg_ids, default=0) + 1

    for sid in list(seg_ids):
        idxs = [i for i, r in enumerate(out) if int(r.get("_counter_segment", 0)) == sid]
        if len(idxs) < max(4, MIDSTREAM_JUMP_PRIOR_INTERVALS + 2):
            continue

        # A segment can theoretically contain more than one schema jump.
        pos = MIDSTREAM_JUMP_PRIOR_INTERVALS
        while pos < len(idxs):
            i_prev = idxs[pos - 1]
            i_cur = idxs[pos]
            a, b = out[i_prev], out[i_cur]
            p0, p1 = to_int(a.get("purchase_total")), to_int(b.get("purchase_total"))
            t0, t1 = _row_dt(a), _row_dt(b)

            if p0 is None or p1 is None or t0 is None or t1 is None or t1 <= t0:
                pos += 1
                continue

            jump = p1 - p0
            if jump < MIDSTREAM_JUMP_MIN:
                pos += 1
                continue

            # Recent trusted pace, normalized to units/day.  This avoids treating
            # a 3h adaptive interval and a 24h primary interval as comparable raw deltas.
            prior_rates = []
            prior_start = max(1, pos - MIDSTREAM_JUMP_PRIOR_INTERVALS)
            for k in range(prior_start, pos):
                x0 = out[idxs[k - 1]]
                x1 = out[idxs[k]]
                q0, q1 = to_int(x0.get("purchase_total")), to_int(x1.get("purchase_total"))
                d0, d1 = _row_dt(x0), _row_dt(x1)
                if q0 is None or q1 is None or d0 is None or d1 is None or d1 <= d0:
                    continue
                d = q1 - q0
                if d < 0:
                    continue
                hours = (d1 - d0).total_seconds() / 3600.0
                if hours > 0:
                    prior_rates.append(d * 24.0 / hours)

            if len(prior_rates) < max(2, MIDSTREAM_JUMP_PRIOR_INTERVALS - 1):
                pos += 1
                continue

            prior_daily = _median(prior_rates)
            # 1/day historical pace => threshold >= 50; absolute 500 still dominates.
            # 100/day historical pace => threshold >= 5,000.
            dynamic_threshold = max(
                float(MIDSTREAM_JUMP_MIN),
                prior_daily * MIDSTREAM_JUMP_PRIOR_MULTIPLIER,
            )
            if jump < dynamic_threshold:
                pos += 1
                continue

            # Examine only later observations in this same current segment.
            future_idxs = idxs[pos + 1:]
            future = [out[j] for j in future_idxs]
            later_values = [to_int(r.get("purchase_total")) for r in future]
            later_values = [v for v in later_values if v is not None]

            state = "pending"
            follow_growth = 0
            positive_steps = 0
            elapsed_h = 0.0

            if later_values:
                highest = max([p1] + later_values)
                follow_growth = max(0, highest - p1)

                prev_val = p1
                for val in later_values:
                    if val > prev_val:
                        positive_steps += 1
                    prev_val = max(prev_val, val)

                future_dts = [_row_dt(r) for r in future if _row_dt(r) is not None]
                if future_dts:
                    elapsed_h = max(
                        0.0,
                        (max(future_dts) - t1).total_seconds() / 3600.0,
                    )

                genuine_follow = max(
                    MIDSTREAM_JUMP_GENUINE_MIN_FOLLOW,
                    int(round(jump * MIDSTREAM_JUMP_GENUINE_RATIO)),
                )
                # A resumed product may sell normally after a historical rebase.
                # Follow-through cannot certify the size of the preceding jump.

                plateau_tol = max(
                    10,
                    int(round(jump * MIDSTREAM_JUMP_PLATEAU_RATIO)),
                    int(round(prior_daily * max(1.0, elapsed_h / 24.0) * 4.0)),
                )
                if (
                    state != "genuine"
                    and elapsed_h >= MIDSTREAM_JUMP_CONFIRM_HOURS
                    and len(future) >= MIDSTREAM_JUMP_CONFIRMATIONS
                    and follow_growth <= plateau_tol
                ):
                    state = "rebase"

            events.append({
                "index": i_cur,
                "checked_at": str(b.get("checked_at") or ""),
                "jump": jump,
                "prior_daily_median": round(prior_daily, 3),
                "follow_growth": follow_growth,
                "positive_steps": positive_steps,
                "elapsed_hours": round(elapsed_h, 2),
                "state": state,
            })

            # pending/rebase: split exactly at the disputed boundary.
            anomaly = True
            new_sid = next_segment_id
            next_segment_id += 1

            for j in idxs[pos:]:
                out[j]["_counter_segment"] = new_sid

            # Continue scanning the newly created right-hand segment.  Rebuild its
            # index list so subsequent genuine data can still be checked safely.
            sid = new_sid
            idxs = [i for i, r in enumerate(out) if int(r.get("_counter_segment", 0)) == sid]
            pos = MIDSTREAM_JUMP_PRIOR_INTERVALS

    return out, anomaly, events

def build_latest_row(raw, prev, b7, b30, today, slot, catalog_row=None):
    price = to_int(raw.get("current_price"))
    daily_status = purchase_baseline_status(raw, prev, MAX_SALES_INTERVAL_HOURS)
    daily_sales = guarded_purchase_delta(raw, prev, (b7, b30)) if daily_status == "ok" else None

    # Mid-stream live guard: rolling 24h has no future observations yet.
    # If an established product suddenly jumps by an extreme amount while its
    # older 7d/30d baselines imply a radically lower pace, display unknown rather
    # than publishing a false five-digit 24h sale. Calendar Finalizer can later
    # evaluate later intervals without admitting an unverified earlier jump.
    if daily_sales is not None and daily_sales >= MIDSTREAM_JUMP_MIN:
        historical_rates = []
        # Historical pace ends BEFORE the disputed interval. Including today's
        # jump in its own threshold makes a 7-day baseline mathematically unable
        # to detect a 50x anomaly.
        raw_dt = parse_kst_datetime((prev or {}).get("checked_at"))
        for older in (b7, b30):
            if not older:
                continue
            ov = to_int(older.get("purchase_total"))
            cv = to_int((prev or {}).get("purchase_total"))
            odt = parse_kst_datetime(older.get("checked_at"))
            if ov is None or cv is None or odt is None or raw_dt is None or raw_dt <= odt:
                continue
            d = cv - ov
            days = (raw_dt - odt).total_seconds() / 86400.0
            if d >= 0 and days > 0:
                historical_rates.append(d / days)

        if historical_rates:
            hist_daily = min(historical_rates)
            threshold = max(
                float(MIDSTREAM_JUMP_MIN),
                hist_daily * MIDSTREAM_JUMP_PRIOR_MULTIPLIER,
            )
            if daily_sales >= threshold:
                daily_sales = None

    # Rolling 24h has no future observations available at build time.
    # For a newly discovered item, a large first-day jump with no older baseline
    # is therefore displayed as unknown instead of a false spike. Calendar Finalizer
    # counts later observed increments separately from this disputed boundary.
    raw_checked = parse_kst_datetime(raw.get("checked_at")) or now_kst()
    first_seen = _catalog_first_seen(catalog_row)
    prev_checked = parse_kst_datetime((prev or {}).get("checked_at"))
    first_baseline = (
        first_seen is not None
        and prev_checked is not None
        and abs((prev_checked - first_seen).total_seconds()) / 3600.0
            <= max(1.0, INITIAL_JUMP_FIRST_SEEN_WINDOW_HOURS)
    )
    # If the goodsNo demonstrably predates the tracker, suppress the first
    # positive interval regardless of its size. This covers old sold-out items
    # that were invisible when the tracker began and later reopen.
    if (
        daily_sales is not None
        and daily_sales > 0
        and first_baseline
        and b7 is None
        and b30 is None
        and product_confirmed_pretracker(raw.get("goods_no"), catalog_row, allow_network=False)
    ):
        daily_sales = None
    elif (
        daily_sales is not None
        and daily_sales >= INITIAL_JUMP_MIN
        and product_in_probation(catalog_row, raw_checked)
        and first_baseline
        and b7 is None
        and b30 is None
    ):
        daily_sales = None

    # The rolling file is keyed by calendar date, so an interval that crosses
    # midnight must contribute only its KST-day fraction instead of the entire
    # counter delta to the later date.
    if daily_sales is not None and prev_checked is not None:
        duration_seconds = (raw_checked - prev_checked).total_seconds()
        day_start = datetime(today.year, today.month, today.day, tzinfo=KST)
        day_end = day_start + timedelta(days=1)
        overlap = overlap_seconds(prev_checked, raw_checked, day_start, day_end)
        if duration_seconds > 0 and overlap > 0:
            daily_sales = round(daily_sales * overlap / duration_seconds, 4)
        else:
            daily_sales = None

    sales7 = guarded_purchase_delta(raw, b7, (b30,))
    sales30 = guarded_purchase_delta(raw, b30)
    if daily_status == "ok" and daily_sales is None:
        daily_status = "counter_jump_unverified"
    daily_hours = ((raw_checked - prev_checked).total_seconds() / 3600.0
                   if prev_checked is not None else None)
    valid_daily_time = (_row_dt(raw) is not None and daily_hours is not None
                        and 0 < daily_hours <= MAX_SALES_INTERVAL_HOURS)
    views = metric_delta(raw, prev, "page_view_total") if valid_daily_time else None
    reviews = metric_delta(raw, prev, "review_count") if valid_daily_time else None

    return {
        "date": today.isoformat(),
        "slot": slot,
        "checked_at": raw.get("checked_at") or now_kst().isoformat(timespec="seconds"),
        "brand_name": raw.get("brand_name") or "",
        "goods_no": raw.get("goods_no") or "",
        "product_name": raw.get("product_name") or "",
        "purchase_total": to_int(raw.get("purchase_total")),
        "daily_sales": daily_sales,
        "daily_sales_status": daily_status,
        "daily_baseline_at": (prev or {}).get("checked_at") or "",
        "daily_interval_hours": round(daily_hours, 2) if daily_hours is not None else "",
        "sales_policy_version": SALES_POLICY_VERSION,
        "normal_price": to_int(raw.get("normal_price")),
        "current_price": price,
        "sale_rate": to_int(raw.get("sale_rate")),
        "daily_estimated_gmv": daily_sales * price if daily_sales is not None and price is not None else None,
        "simple_gmv": to_int(raw.get("simple_gmv")),
        "page_view_total": to_int(raw.get("page_view_total")),
        "daily_page_view_increase": views,
        "review_count": to_int(raw.get("review_count")),
        "daily_review_increase": reviews,
        "like_count": "",
        "daily_like_increase": "",
        "sales_7d": sales7,
        "sales_7d_avg_per_day": round(sales7 / 7, 2) if sales7 is not None else "",
        "estimated_gmv_7d": sales7 * price if sales7 is not None and price is not None else "",
        "sales_30d": sales30,
        "sales_30d_avg_per_day": round(sales30 / 30, 2) if sales30 is not None else "",
        "estimated_gmv_30d": sales30 * price if sales30 is not None and price is not None else "",
        "availability": raw.get("availability") or "",
        "product_url": raw.get("product_url") or "",
        "errors": raw.get("errors") or "",
    }


def append_new_products(delta):
    if not delta:
        return
    old = read_csv(NEW_PRODUCTS_FILE)
    by_goods = {str(r.get("goods_no") or ""): r for r in old if r.get("goods_no")}
    for r in delta:
        g = str(r.get("goods_no") or "")
        if g and g not in by_goods:
            by_goods[g] = r
    rows = list(by_goods.values())
    rows.sort(key=lambda r: (str(r.get("first_seen_at") or ""), str(r.get("goods_no") or "")))
    write_csv(NEW_PRODUCTS_FILE, rows, NEW_PRODUCT_FIELDS)


def upsert_rows(path, new_rows, fields, key_func):
    old = read_csv(path)
    keys = {key_func(r) for r in new_rows}
    rows = [r for r in old if key_func(r) not in keys] + list(new_rows)
    return rows


def brand_rows_for_date(all_latest, new_delta, today):
    current = [r for r in all_latest if str(r.get("date") or "") == today.isoformat()]
    grouped = {}
    for r in current:
        b = str(r.get("brand_name") or "").strip() or "(브랜드 미확인)"
        grouped.setdefault(b, []).append(r)

    new_by_brand = {}
    for r in new_delta:
        b = str(r.get("brand_name") or "").strip() or "(브랜드 미확인)"
        new_by_brand[b] = new_by_brand.get(b, 0) + 1

    ts = now_kst().isoformat(timespec="seconds")
    result = []
    for brand, items in sorted(grouped.items()):
        def valid(key):
            return [to_int(x.get(key)) for x in items if to_int(x.get(key)) is not None]
        def sm(key):
            v = valid(key)
            return sum(v) if v else 0

        daily = [x for x in items if to_float(x.get("daily_sales")) is not None]
        d7 = [x for x in items if to_int(x.get("sales_7d")) is not None]
        d30 = [x for x in items if to_int(x.get("sales_30d")) is not None]
        s7 = sum(to_int(x.get("sales_7d")) or 0 for x in d7)
        s30 = sum(to_int(x.get("sales_30d")) or 0 for x in d30)

        result.append({
            "date": today.isoformat(), "checked_at": ts, "brand_name": brand,
            "product_count": len(items),
            "daily_baseline_product_count": len(daily),
            "purchase_total_sum": sm("purchase_total"),
            "simple_gmv_sum": sm("simple_gmv"),
            "daily_sales_sum": round(sum(to_float(x.get("daily_sales")) or 0.0 for x in daily), 2) if daily else "",
            "daily_estimated_gmv_sum": round(sum(to_float(x.get("daily_estimated_gmv")) or 0.0 for x in daily)) if daily else "",
            "daily_page_view_increase_sum": sum(to_int(x.get("daily_page_view_increase")) or 0 for x in daily),
            "daily_review_increase_sum": sum(to_int(x.get("daily_review_increase")) or 0 for x in daily),
            "daily_like_increase_sum": 0,
            "sales_7d_sum": s7 if d7 else "",
            "sales_7d_avg_per_day": round(s7 / 7, 2) if d7 else "",
            "estimated_gmv_7d": sum(to_int(x.get("estimated_gmv_7d")) or 0 for x in d7) if d7 else "",
            "sales_30d_sum": s30 if d30 else "",
            "sales_30d_avg_per_day": round(s30 / 30, 2) if d30 else "",
            "estimated_gmv_30d": sum(to_int(x.get("estimated_gmv_30d")) or 0 for x in d30) if d30 else "",
            "products_with_7d_baseline": len(d7),
            "products_with_30d_baseline": len(d30),
            "new_products": new_by_brand.get(brand, 0),
        })
    return result


def synthetic_failed_row(goods_no, catalog_row, reason="missing shard result"):
    meta = dict(catalog_row or {})
    return {
        "checked_at": now_kst().isoformat(timespec="seconds"),
        "goods_no": str(goods_no),
        "brand_name": meta.get("brand_name") or "",
        "product_name": meta.get("product_name") or "",
        "purchase_total": None,
        "page_view_total": None,
        "normal_price": to_int(meta.get("normal_price")),
        "current_price": to_int(meta.get("current_price")),
        "sale_rate": to_int(meta.get("sale_rate")),
        "review_count": to_int(meta.get("review_count")),
        "rating": meta.get("rating") or "",
        "availability": meta.get("availability") or "",
        "simple_gmv": None,
        "product_url": meta.get("product_url") or f"https://www.musinsa.com/products/{goods_no}",
        "errors": reason,
    }


def recovery_queue_path(date_value, slot):
    d = date_value.isoformat() if hasattr(date_value, "isoformat") else str(date_value)
    return RECOVERY_DIR / d / f"slot-{int(slot)}-failed.csv"


def save_failure_queue(date_value, slot, expected_goods, raw_by_goods, catalog):
    path = recovery_queue_path(date_value, slot)
    old = {str(r.get("goods_no") or ""): r for r in read_csv(path) if r.get("goods_no")}
    now = now_kst().isoformat(timespec="seconds")
    remaining = []
    for g in expected_goods:
        r = raw_by_goods.get(str(g))
        if r and to_int(r.get("purchase_total")) is not None:
            continue
        prev = old.get(str(g), {})
        meta = catalog.get(str(g), {})
        remaining.append({
            "date": date_value.isoformat(),
            "slot": int(slot),
            "goods_no": str(g),
            "brand_name": (r or {}).get("brand_name") or meta.get("brand_name") or "",
            "product_name": (r or {}).get("product_name") or meta.get("product_name") or "",
            "first_failed_at": prev.get("first_failed_at") or now,
            "last_failed_at": now,
            "attempts": (to_int(prev.get("attempts")) or 0) + 1,
            "last_error": (r or {}).get("errors") or "missing shard result",
            "current_price": to_int((r or {}).get("current_price")) if r else to_int(meta.get("current_price")),
            "product_url": (r or {}).get("product_url") or meta.get("product_url") or f"https://www.musinsa.com/products/{g}",
        })
    write_csv(path, remaining, FAILURE_FIELDS)
    return remaining


def update_coverage(date_value, slot, expected, success, failed_rows, stage):
    date_text = date_value.isoformat() if hasattr(date_value, "isoformat") else str(date_value)
    path = COVERAGE_DIR / f"{date_text}.json"
    payload = {}
    if path.exists():
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            payload = {}
    payload.setdefault("date", date_text)
    payload.setdefault("slots", {})
    prev = payload["slots"].get(str(slot), {})
    failed_rows = list(failed_rows or [])
    expected = int(expected or 0)
    success = int(success or 0)
    failed = max(0, expected - success)
    pct = round((success / expected * 100.0), 4) if expected else 100.0
    payload["slots"][str(slot)] = {
        "slot": int(slot),
        "expected": expected,
        "success": success,
        "failed": failed,
        "coverage_pct": pct,
        "status": "complete" if failed == 0 else "partial",
        "stage": stage,
        "first_collected_at": prev.get("first_collected_at") or now_kst().isoformat(timespec="seconds"),
        "last_updated_at": now_kst().isoformat(timespec="seconds"),
        "failed_goods_sample": [str(r.get("goods_no") or "") for r in failed_rows[:20]],
    }
    slots = payload["slots"]
    total_expected = sum(int((v or {}).get("expected") or 0) for v in slots.values())
    total_success = sum(int((v or {}).get("success") or 0) for v in slots.values())
    total_failed = max(0, total_expected - total_success)
    payload["overall"] = {
        "slots_collected": len(slots),
        "complete_slots": sum(1 for v in slots.values() if int((v or {}).get("failed") or 0) == 0),
        "expected": total_expected,
        "success": total_success,
        "failed": total_failed,
        "coverage_pct": round((total_success / total_expected * 100.0), 4) if total_expected else 100.0,
    }
    payload["updated_at"] = now_kst().isoformat(timespec="seconds")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    latest_payload = None
    if COVERAGE_LATEST_FILE.exists():
        try:
            latest_payload = json.loads(COVERAGE_LATEST_FILE.read_text(encoding="utf-8"))
        except Exception:
            latest_payload = None
    if not latest_payload or date_text >= str(latest_payload.get("date") or ""):
        COVERAGE_LATEST_FILE.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return payload


def compact_from_raw(r, date_value, slot):
    return {
        "date": date_value.isoformat(), "slot": int(slot),
        "checked_at": r.get("checked_at") or "",
        "goods_no": str(r.get("goods_no") or ""),
        "brand_name": r.get("brand_name") or "",
        "product_name": r.get("product_name") or "",
        "purchase_total": to_int(r.get("purchase_total")),
        "page_view_total": to_int(r.get("page_view_total")),
        "current_price": to_int(r.get("current_price")),
        "normal_price": to_int(r.get("normal_price")),
        "sale_rate": to_int(r.get("sale_rate")),
        "review_count": to_int(r.get("review_count")),
        "rating": r.get("rating") or "",
        "availability": r.get("availability") or "",
    }


def raw_from_compact(r, catalog):
    g = str(r.get("goods_no") or "")
    meta = catalog.get(g, {})
    price = to_int(r.get("current_price"))
    purchase = to_int(r.get("purchase_total"))
    return {
        "checked_at": r.get("checked_at") or "",
        "goods_no": g,
        "brand_name": r.get("brand_name") or meta.get("brand_name") or "",
        "product_name": r.get("product_name") or meta.get("product_name") or "",
        "purchase_total": purchase,
        "page_view_total": to_int(r.get("page_view_total")),
        "normal_price": to_int(r.get("normal_price")),
        "current_price": price,
        "sale_rate": to_int(r.get("sale_rate")),
        "review_count": to_int(r.get("review_count")),
        "rating": r.get("rating") or "",
        "availability": r.get("availability") or "",
        "simple_gmv": purchase * price if purchase is not None and price is not None else None,
        "product_url": meta.get("product_url") or f"https://www.musinsa.com/products/{g}",
        "errors": "",
    }


def new_products_for_date(date_value):
    date_text = date_value.isoformat() if hasattr(date_value, "isoformat") else str(date_value)
    return [r for r in read_csv(NEW_PRODUCTS_FILE) if str(r.get("first_seen_at") or "")[:10] == date_text]


def build_rows_for_date(date_value):
    catalog_rows = read_csv(CATALOG_FILE)
    catalog = {str(r.get("goods_no") or ""): r for r in catalog_rows if r.get("goods_no")}
    prev = load_snapshot_any_slot(date_value - timedelta(days=1))
    d7 = load_snapshot_any_slot(date_value - timedelta(days=7))
    d30 = load_snapshot_any_slot(date_value - timedelta(days=30))
    out = []
    for slot in range(SLOT_COUNT):
        path = SLOT_DIR / f"slot-{slot}" / f"{date_value.isoformat()}.csv.gz"
        if not path.exists():
            legacy = SLOT_DIR / f"slot-{slot}" / f"{date_value.isoformat()}.csv"
            path = legacy
        for c in read_csv(path):
            g = str(c.get("goods_no") or "")
            raw = raw_from_compact(c, catalog)
            out.append(build_latest_row(raw, prev.get(g), d7.get(g), d30.get(g), date_value, slot, catalog.get(g)))
    return out


def rebuild_latest_product_file():
    all_latest = []
    for s in range(SLOT_COUNT):
        all_latest.extend(read_csv(LATEST_SLOT_DIR / f"slot-{s}.csv.gz"))
    all_latest.sort(key=lambda r: (
        str(r.get("brand_name") or ""),
        int(r["goods_no"]) if str(r.get("goods_no", "")).isdigit() else 10**30,
    ))
    write_csv(LATEST_PRODUCT_FILE, all_latest, LATEST_FIELDS)
    return all_latest


def rebuild_date_aggregates(date_value, rows=None, write_daily_snapshot=True):
    if rows is None:
        rows = build_rows_for_date(date_value)
    new_delta = new_products_for_date(date_value)
    brand_today = brand_rows_for_date(rows, new_delta, date_value)
    brand_all = upsert_rows(
        BRAND_HISTORY_FILE, brand_today, BRAND_FIELDS,
        lambda r: (str(r.get("date") or ""), str(r.get("brand_name") or ""))
    )
    brand_all.sort(key=lambda r: (str(r.get("date") or ""), str(r.get("brand_name") or "")))
    write_csv(BRAND_HISTORY_FILE, brand_all, BRAND_FIELDS)

    daily_valid = [r for r in rows if to_float(r.get("daily_sales")) is not None]
    summary = {
        "checked_at": now_kst().isoformat(timespec="seconds"),
        "date": date_value.isoformat(),
        "product_count": len(rows),
        "daily_baseline_product_count": len(daily_valid),
        "purchase_total_sum": sum(to_int(r.get("purchase_total")) or 0 for r in rows),
        "simple_gmv_sum": sum(to_int(r.get("simple_gmv")) or 0 for r in rows),
        "daily_sales_sum": round(sum(to_float(r.get("daily_sales")) or 0.0 for r in daily_valid), 2) if daily_valid else "",
        "daily_estimated_gmv_sum": round(sum(to_float(r.get("daily_estimated_gmv")) or 0.0 for r in daily_valid)) if daily_valid else "",
        "sales_7d_sum": sum(to_int(r.get("sales_7d")) or 0 for r in rows if to_int(r.get("sales_7d")) is not None),
        "sales_30d_sum": sum(to_int(r.get("sales_30d")) or 0 for r in rows if to_int(r.get("sales_30d")) is not None),
        "new_products": len(new_delta),
    }
    summaries = upsert_rows(SUMMARY_FILE, [summary], SUMMARY_FIELDS, lambda r: str(r.get("date") or ""))
    summaries.sort(key=lambda r: str(r.get("date") or ""))
    write_csv(SUMMARY_FILE, summaries, SUMMARY_FIELDS)

    slot_files = [SLOT_DIR / f"slot-{s}" / f"{date_value.isoformat()}.csv.gz" for s in range(SLOT_COUNT)]
    if write_daily_snapshot and all(p.exists() for p in slot_files):
        day_rows = []
        for p in slot_files:
            day_rows.extend(read_csv(p))
        day_rows.sort(key=lambda r: int(r["goods_no"]) if str(r.get("goods_no", "")).isdigit() else 10**30)
        write_csv(DAILY_DIR / f"{date_value.isoformat()}.csv.gz", day_rows, COMPACT_FIELDS)
    return rows


def refresh_latest_slot_from_snapshot(date_value, slot):
    path = SLOT_DIR / f"slot-{slot}" / f"{date_value.isoformat()}.csv.gz"
    compact = read_csv(path)
    if not compact:
        return []
    existing_latest = read_csv(LATEST_SLOT_DIR / f"slot-{slot}.csv.gz")
    existing_date = max([str(r.get("date") or "") for r in existing_latest] or [""])
    if existing_date and existing_date > date_value.isoformat():
        return existing_latest

    catalog_rows = read_csv(CATALOG_FILE)
    catalog = {str(r.get("goods_no") or ""): r for r in catalog_rows if r.get("goods_no")}
    prev = load_snapshot_any_slot(date_value - timedelta(days=1))
    d7 = load_snapshot_any_slot(date_value - timedelta(days=7))
    d30 = load_snapshot_any_slot(date_value - timedelta(days=30))
    latest = []
    for c in compact:
        g = str(c.get("goods_no") or "")
        raw = raw_from_compact(c, catalog)
        latest.append(build_latest_row(raw, prev.get(g), d7.get(g), d30.get(g), date_value, slot, catalog.get(g)))
    write_csv(LATEST_SLOT_DIR / f"slot-{slot}.csv.gz", latest, LATEST_FIELDS)
    return latest


def recover_queue_file(path):
    path = Path(path)
    queue = read_csv(path)
    if not queue:
        return {"queue": str(path), "attempted": 0, "recovered": 0, "remaining": 0}

    first = queue[0]
    date_value = datetime.strptime(str(first.get("date")), "%Y-%m-%d").date()
    slot = int(first.get("slot"))
    catalog_rows = read_csv(CATALOG_FILE)
    catalog = {str(r.get("goods_no") or ""): r for r in catalog_rows if r.get("goods_no")}

    THROTTLE.recovery_mode()
    results = {}
    with ThreadPoolExecutor(max_workers=max(1, RECOVERY_WORKERS)) as executor:
        futures = {
            executor.submit(collect_one, str(r.get("goods_no")), catalog.get(str(r.get("goods_no"))), 4): r
            for r in queue if r.get("goods_no")
        }
        for fut in as_completed(futures):
            old = futures[fut]
            g = str(old.get("goods_no"))
            try:
                results[g] = fut.result()
            except Exception as e:
                results[g] = synthetic_failed_row(g, catalog.get(g), str(e))

    snap_path = SLOT_DIR / f"slot-{slot}" / f"{date_value.isoformat()}.csv.gz"
    snapshot = {str(r.get("goods_no") or ""): r for r in read_csv(snap_path) if r.get("goods_no")}
    remaining = []
    recovered = 0
    now = now_kst().isoformat(timespec="seconds")
    for old in queue:
        g = str(old.get("goods_no") or "")
        r = results.get(g) or synthetic_failed_row(g, catalog.get(g), "recovery result missing")
        if to_int(r.get("purchase_total")) is not None:
            snapshot[g] = compact_from_raw(r, date_value, slot)
            recovered += 1
        else:
            row = dict(old)
            row["last_failed_at"] = now
            row["attempts"] = (to_int(old.get("attempts")) or 0) + 1
            row["last_error"] = r.get("errors") or old.get("last_error") or "recovery failed"
            remaining.append(row)

    rows = list(snapshot.values())
    rows.sort(key=lambda r: int(r["goods_no"]) if str(r.get("goods_no", "")).isdigit() else 10**30)
    write_csv(snap_path, rows, COMPACT_FIELDS)
    write_csv(path, remaining, FAILURE_FIELDS)

    expected = len(rows)
    success = sum(1 for r in rows if to_int(r.get("purchase_total")) is not None)
    update_coverage(date_value, slot, expected, success, remaining, "recovery")
    refresh_latest_slot_from_snapshot(date_value, slot)
    rebuild_latest_product_file()
    rebuild_date_aggregates(date_value)
    write_history_manifest()

    return {
        "queue": str(path), "date": date_value.isoformat(), "slot": slot,
        "attempted": len(queue), "recovered": recovered, "remaining": len(remaining),
        "coverage_pct": round((success / expected * 100.0), 4) if expected else 100.0,
        "adaptive_interval_seconds": round(THROTTLE.interval, 3),
    }


def recover_pending(lookback_days=2, max_queues=8):
    today = now_kst().date()
    cutoff = today - timedelta(days=max(0, int(lookback_days)))
    candidates = []
    if RECOVERY_DIR.exists():
        for path in RECOVERY_DIR.glob("*/slot-*-failed.csv"):
            try:
                d = datetime.strptime(path.parent.name, "%Y-%m-%d").date()
            except Exception:
                continue
            if d < cutoff or d > today:
                continue
            if read_csv(path):
                candidates.append((d, path))
    candidates.sort(key=lambda x: (x[0], str(x[1])))
    results = []
    for _, path in candidates[:max(1, int(max_queues))]:
        results.append(recover_queue_file(path))
    print(json.dumps({"pending_queues": len(candidates), "processed": results}, ensure_ascii=False))
    return 0


def archive_existing_primary_snapshot(snapshot_path, slot):
    """
    같은 날짜/slot을 다시 수집할 때 기존 canonical snapshot이 사라지지 않도록
    data/observations/YYYY-MM-DD/ 아래에 이전 관측값을 불변 archive로 보존합니다.

    data/slots/.../YYYY-MM-DD.csv.gz 는 '해당 날짜/slot의 최신 snapshot cache'로 유지하고,
    Calendar Finalizer는 canonical + observations를 함께 읽으므로 모든 재수집 관측값을 사용합니다.
    """
    snapshot_path = Path(snapshot_path)
    if not snapshot_path.exists():
        legacy = snapshot_path.with_suffix("") if snapshot_path.suffix == ".gz" else snapshot_path
        if not legacy.exists():
            return None
        snapshot_path = legacy

    rows = read_csv(snapshot_path)
    if not rows:
        return None

    # 실제 checked_at 날짜 기준으로 나누어 저장합니다.
    grouped = {}
    for row in rows:
        checked = parse_kst_datetime(row.get("checked_at"))
        if checked is None:
            # 기존 canonical 파일명 날짜 fallback
            try:
                dtext = snapshot_path.name.split(".csv")[0]
                checked_date = datetime.strptime(dtext, "%Y-%m-%d").date()
            except Exception:
                checked_date = now_kst().date()
        else:
            checked_date = checked.date()

        copied = dict(row)
        copied.update({
            "sample_kind": "primary_rerun_archive",
            "sampling_tier": "baseline",
            "sampling_score": "",
            "base_slot": int(slot),
            "clock_slot": int((checked.hour * 60 + checked.minute) // 180) % SLOT_COUNT if checked else int(slot),
        })
        grouped.setdefault(checked_date.isoformat(), []).append(copied)

    run_id = re.sub(r"[^0-9A-Za-z_.-]+", "-", os.environ.get("GITHUB_RUN_ID", "local"))
    stamp = now_kst().strftime("%H%M%S")
    saved = []

    for date_text, out_rows in grouped.items():
        folder = OBSERVATION_DIR / date_text
        folder.mkdir(parents=True, exist_ok=True)

        # 이전 canonical의 checked_at 범위를 파일명에 넣어서 사람이 봐도 구분 가능하게 합니다.
        checked_vals = []
        for r in out_rows:
            dt = parse_kst_datetime(r.get("checked_at"))
            if dt is not None:
                checked_vals.append(dt)
        if checked_vals:
            first_stamp = min(checked_vals).strftime("%H%M%S")
            last_stamp = max(checked_vals).strftime("%H%M%S")
        else:
            first_stamp = last_stamp = stamp

        out_path = folder / (
            f"primary-rerun-slot-{int(slot)}-"
            f"{first_stamp}-{last_stamp}-archived-{stamp}-{run_id}.csv.gz"
        )
        write_csv(out_path, out_rows, ADAPTIVE_OBS_FIELDS)
        saved.append(str(out_path.relative_to(BASE_DIR)))

    return saved



def aggregate_slot(state_dir, shard_dir):
    state_dir, shard_dir = Path(state_dir), Path(shard_dir)
    today = datetime.strptime(
        (state_dir / "snapshot_date.txt").read_text(encoding="utf-8").strip(),
        "%Y-%m-%d"
    ).date()
    slot = int((state_dir / "slot.txt").read_text(encoding="utf-8").strip())

    catalog_rows = read_csv(state_dir / "musinsa_catalog.csv")
    catalog = {str(r.get("goods_no") or ""): r for r in catalog_rows if r.get("goods_no")}
    watchlist = read_lines(state_dir / "musinsa_watchlist.txt")
    expected_goods = [g for g in watchlist if effective_goods_slot(g, catalog.get(g)) == slot]

    raw = []
    for p in sorted(shard_dir.glob("*.csv")):
        raw.extend(read_csv(p))
    by_goods = {}
    for r in raw:
        g = str(r.get("goods_no") or "").strip()
        if g:
            by_goods[g] = r

    # matrix job 하나가 timeout/실패해 artifact 자체가 없더라도 누락 goodsNo를 복구 큐에 넣습니다.
    for g in expected_goods:
        if g not in by_goods:
            by_goods[g] = synthetic_failed_row(g, catalog.get(g), "missing shard artifact/result")

    raw = [by_goods[g] for g in expected_goods if g in by_goods]
    prev = load_snapshot_any_slot(today - timedelta(days=1))
    d7 = load_snapshot_any_slot(today - timedelta(days=7))
    d30 = load_snapshot_any_slot(today - timedelta(days=30))

    latest = []
    compact = []
    for r in raw:
        g = str(r.get("goods_no") or "")
        latest.append(build_latest_row(r, prev.get(g), d7.get(g), d30.get(g), today, slot, catalog.get(g)))
        compact.append(compact_from_raw(r, today, slot))

    canonical_snapshot = SLOT_DIR / f"slot-{slot}" / f"{today.isoformat()}.csv.gz"
    archived_observations = archive_existing_primary_snapshot(canonical_snapshot, slot)
    write_csv(canonical_snapshot, compact, COMPACT_FIELDS)
    write_csv(LATEST_SLOT_DIR / f"slot-{slot}.csv.gz", latest, LATEST_FIELDS)
    write_history_manifest()

    # discovery 상태 root 반영
    write_lines(WATCHLIST_FILE, watchlist)
    write_csv(CATALOG_FILE, catalog_rows, CATALOG_FIELDS)
    audit_rows = read_csv(state_dir / "musinsa_brand_audit.csv")
    if audit_rows:
        write_csv(BRAND_AUDIT_FILE, audit_rows, BRAND_AUDIT_FIELDS)
    new_delta = read_csv(state_dir / "new_products_delta.csv")
    append_new_products(new_delta)

    failures = save_failure_queue(today, slot, expected_goods, by_goods, catalog)
    success_count = len(expected_goods) - len(failures)
    coverage = update_coverage(today, slot, len(expected_goods), success_count, failures, "primary")

    all_latest = rebuild_latest_product_file()
    rebuild_date_aggregates(today)

    print(json.dumps({
        "date": today.isoformat(), "slot": slot,
        "expected_products": len(expected_goods),
        "success_products": success_count,
        "failed_products": len(failures),
        "coverage_pct": coverage.get("slots", {}).get(str(slot), {}).get("coverage_pct"),
        "today_latest_products": len([r for r in all_latest if str(r.get("date") or "") == today.isoformat()]),
        "recovery_queue": str(recovery_queue_path(today, slot)),
        "archived_previous_observations": archived_observations or [],
    }, ensure_ascii=False))
    return 0


# ---------------------------------------------------------------------------
# v9: KST calendar-day estimation
# ---------------------------------------------------------------------------

def to_float(value):
    if value is None or value == "":
        return None
    try:
        return float(str(value).replace(",", ""))
    except Exception:
        return None


def parse_kst_datetime(value):
    if not value:
        return None
    try:
        text = str(value).strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        dt = datetime.fromisoformat(text)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=KST)
        return dt.astimezone(KST)
    except Exception:
        return None


def calendar_bucket(goods_no):
    s = str(goods_no or "").strip()
    if s.isdigit():
        return int(s) % CALENDAR_HISTORY_BUCKETS
    digest = hashlib.sha1(s.encode("utf-8")).digest()
    return int.from_bytes(digest[:4], "big") % CALENDAR_HISTORY_BUCKETS


def overlap_seconds(a_start, a_end, b_start, b_end):
    left = max(a_start, b_start)
    right = min(a_end, b_end)
    return max(0.0, (right - left).total_seconds())


def load_calendar_observations(target_date, before_days=3, after_days=2):
    """
    snapshot 파일 날짜와 실제 checked_at 날짜가 recovery 때문에 다를 수 있어
    target 주변 여러 snapshot을 읽은 뒤 실제 checked_at 기준으로 정렬합니다.
    """
    start_snapshot = target_date - timedelta(days=before_days)
    end_snapshot = target_date + timedelta(days=after_days)
    by_goods = {}
    d = start_snapshot
    while d <= end_snapshot:
        for slot in range(SLOT_COUNT):
            path = SLOT_DIR / f"slot-{slot}" / f"{d.isoformat()}.csv.gz"
            if not path.exists():
                legacy = SLOT_DIR / f"slot-{slot}" / f"{d.isoformat()}.csv"
                path = legacy
            for row in read_csv(path):
                g = str(row.get("goods_no") or "").strip()
                checked = parse_kst_datetime(row.get("checked_at"))
                if not g or checked is None:
                    continue
                # 같은 timestamp 중복은 purchaseTotal 유효행을 우선
                key = checked.isoformat()
                bucket = by_goods.setdefault(g, {})
                old = bucket.get(key)
                if old is None or (
                    to_int(old.get("purchase_total")) is None
                    and to_int(row.get("purchase_total")) is not None
                ):
                    copied = dict(row)
                    copied["_checked_dt"] = checked
                    bucket[key] = copied

        # High-selling products may have extra 6h/12h observations archived separately.
        obs_folder = OBSERVATION_DIR / d.isoformat()
        if obs_folder.exists():
            obs_paths = sorted(list(obs_folder.glob("*.csv.gz")) + list(obs_folder.glob("*.csv")))
            for obs_path in obs_paths:
                for row in read_csv(obs_path):
                    g = str(row.get("goods_no") or "").strip()
                    checked = parse_kst_datetime(row.get("checked_at"))
                    if not g or checked is None:
                        continue
                    key = checked.isoformat()
                    bucket = by_goods.setdefault(g, {})
                    old = bucket.get(key)
                    if old is None or (
                        to_int(old.get("purchase_total")) is None
                        and to_int(row.get("purchase_total")) is not None
                    ):
                        copied = dict(row)
                        copied["_checked_dt"] = checked
                        bucket[key] = copied
        d += timedelta(days=1)

    out = {}
    for g, keyed in by_goods.items():
        rows = list(keyed.values())
        rows.sort(key=lambda r: r["_checked_dt"])
        out[g] = rows
    return out



def price_at_or_before(observations, when):
    chosen = None
    for row in observations:
        dt = row.get("_checked_dt")
        if dt is not None and dt <= when:
            p = to_int(row.get("current_price"))
            if p is not None:
                chosen = p
        elif dt is not None and dt > when:
            break
    return chosen


def interval_gmv_contribution(prev_row, cur_row, overlap_start, overlap_end, delta, duration_seconds):
    """
    가격은 '마지막 관측값 유지(LOCF)' 방식으로 적용합니다.
    새 가격은 cur_row의 checked_at에서 처음 확인된 것이므로 그 시점 전에는
    이전 관측가(prev_row)를 사용합니다. 실제 가격변경 시각을 임의로 과거로
    소급하지 않는 보수적인 방식입니다.
    """
    if duration_seconds <= 0:
        return None, 0.0

    p0 = to_int(prev_row.get("current_price"))
    p1 = to_int(cur_row.get("current_price"))
    if p0 is None and p1 is None:
        return None, 0.0
    price = p0 if p0 is not None else p1

    sec = max(0.0, (overlap_end - overlap_start).total_seconds())
    if sec <= 0:
        return 0.0, 0.0

    rate = delta / duration_seconds
    return rate * sec * price, sec


def sanitize_purchase_observations(observations, catalog_row=None):
    """Clean purchaseTotal resets, then resolve a newly discovered item's first jump.

    Step 1: remove transient downward resets (1000 -> 0 -> 1005).
    Step 2: if the very first tracked interval is a large jump, use later 48h
            observations to decide genuine / rebase / pending.

    review_count is never a hard decision rule.
    """
    source = [
        dict(r) for r in sorted(observations, key=lambda r: r["_checked_dt"])
        if not purchase_counter_observation_inconsistent(r)
    ]

    if not source:
        return [], False, "none", 0

    anomaly = False

    # First clean transient/persistent downward counter resets.
    clean = []
    segment = 0
    first = dict(source[0])
    first["_counter_segment"] = segment
    clean.append(first)
    last_value = to_int(first.get("purchase_total"))

    i = 1
    while i < len(source):
        row = source[i]
        value = to_int(row.get("purchase_total"))

        if value >= last_value:
            x = dict(row)
            x["_counter_segment"] = segment
            clean.append(x)
            last_value = value
            i += 1
            continue

        anomaly = True

        recovery_idx = None
        for j in range(i + 1, len(source)):
            future = to_int(source[j].get("purchase_total"))
            if future is not None and future >= last_value:
                recovery_idx = j
                break

        if recovery_idx is not None:
            # transient reset: skip low rows and resume from recovered level
            i = recovery_idx
            continue

        # Persistent lower counter: begin a new segment only after at least
        # two observations support the lower regime.
        remaining = source[i:]
        if len(remaining) >= 2:
            segment += 1
            x = dict(row)
            x["_counter_segment"] = segment
            clean.append(x)
            last_value = value
            i += 1
            continue

        i += 1

    # Repeated zeros and long outages can occur long after discovery/probation.
    # Split these boundaries regardless of catalog age, review count or jump size.
    # Downward-reset cleanup above still preserves 1000 -> 0 -> 1005 as +5 when
    # its two trusted positive observations are close enough in time.
    old_segment = None
    segment = -1
    previous = None
    for row in clean:
        original_segment = row.get("_counter_segment")
        structural_status = (
            purchase_baseline_status(row, previous, None)
            if previous is not None else "ok"
        )
        boundary = previous is not None and structural_status != "ok"
        if original_segment != old_segment or boundary:
            segment += 1
        if boundary:
            anomaly = True
        old_segment = original_segment
        row["_counter_segment"] = segment
        previous = row

    # Initial-jump resolver should only operate inside the first counter segment.
    # If there was an unrelated later reset, preserve its segment boundaries.
    if len(clean) >= 2:
        first_segment = clean[0].get("_counter_segment", 0)
        head = []
        tail = []
        switched = False
        for r in clean:
            if not switched and r.get("_counter_segment", 0) == first_segment:
                head.append(r)
            else:
                switched = True
                tail.append(r)

        resolved, initial_state, initial_delta = resolve_initial_purchase_jump(
            head, catalog_row
        )

        if initial_state in ("pending", "rebase"):
            anomaly = True

        # Keep later reset segments distinct by shifting them above resolved head.
        if tail:
            max_head_seg = max((int(r.get("_counter_segment", 0)) for r in resolved), default=0)
            old_tail_min = min(int(r.get("_counter_segment", 0)) for r in tail)
            shift = max_head_seg + 1 - old_tail_min
            for r in tail:
                r["_counter_segment"] = int(r.get("_counter_segment", 0)) + shift
            resolved.extend(tail)

        clean = resolved
    else:
        initial_state, initial_delta = "none", 0

    # Finally protect already-established products from an extreme positive
    # counter schema jump in the middle of their history.  This runs after the
    # initial lifecycle resolver so the two protections do not compete.
    clean, midstream_anomaly, midstream_events = resolve_midstream_purchase_jumps(clean)
    if midstream_anomaly:
        anomaly = True
        if midstream_events:
            sample = "; ".join(
                f"{e['state']}:+{e['jump']}@{e['checked_at']}"
                for e in midstream_events[:5]
            )
            print(f"[midstream-counter] {sample}", file=sys.stderr)

    return clean, anomaly, initial_state, initial_delta

def estimate_calendar_product(target_date, goods_no, observations, catalog_row=None):
    day_start = datetime(
        target_date.year, target_date.month, target_date.day, 0, 0, 0, tzinfo=KST
    )
    day_end = day_start + timedelta(days=1)

    # 가격/메타데이터는 원본 관측을 유지하되,
    # purchaseTotal 판매량 계산은 이상 하락/복구를 정제한 관측만 사용합니다.
    original_observations = sorted(observations, key=lambda r: r["_checked_dt"])
    observations, counter_anomaly, initial_delta_status, initial_delta_value = (
        sanitize_purchase_observations(original_observations, catalog_row)
    )
    if len(observations) < 2:
        return None

    sales_est = 0.0
    gmv_est = 0.0
    gmv_seconds = 0.0
    coverage_seconds = 0.0
    attributed_seconds = 0.0
    reconstructed_seconds = 0.0
    contributing = 0
    max_interval_hours = 0.0
    negative_delta = bool(counter_anomaly)

    for i in range(1, len(observations)):
        a = observations[i - 1]
        b = observations[i]
        t0, t1 = a["_checked_dt"], b["_checked_dt"]
        if t1 <= t0:
            continue

        # counter reset/rebase 경계는 서로 연결하지 않습니다.
        if a.get("_counter_segment") != b.get("_counter_segment"):
            negative_delta = True
            continue

        overlap = overlap_seconds(t0, t1, day_start, day_end)
        if overlap <= 0:
            continue

        p0 = to_int(a.get("purchase_total"))
        p1 = to_int(b.get("purchase_total"))
        if p0 is None or p1 is None:
            continue

        delta = p1 - p0
        if delta < 0:
            # 누적 purchaseTotal 감소 interval은 판매 취소로 해석하지 않는다.
            # 데이터 품질 이상 interval로 표시하고 판매/GMV/coverage 계산에서 제외한다.
            negative_delta = True
            continue

        duration = (t1 - t0).total_seconds()
        if duration <= 0:
            continue

        overlap_start = max(t0, day_start)
        overlap_end = min(t1, day_end)
        fraction = overlap / duration

        sales_est += delta * fraction
        attributed_seconds += overlap
        interval_hours = duration / 3600.0
        if interval_hours <= MAX_SALES_INTERVAL_HOURS:
            coverage_seconds += overlap
        else:
            reconstructed_seconds += overlap
        contributing += 1
        max_interval_hours = max(max_interval_hours, interval_hours)

        gmv_piece, priced_seconds = interval_gmv_contribution(
            a, b, overlap_start, overlap_end, delta, duration
        )
        if gmv_piece is not None:
            gmv_est += gmv_piece
            gmv_seconds += priced_seconds

    coverage_pct = min(100.0, coverage_seconds / 86400.0 * 100.0)
    if attributed_seconds <= 0:
        return None

    start_price = price_at_or_before(original_observations, day_start)
    end_price = price_at_or_before(original_observations, day_end - timedelta(microseconds=1))
    if end_price is None:
        # 당일 마지막 관측가 fallback
        in_day_prices = [
            to_int(r.get("current_price")) for r in original_observations
            if day_start <= r["_checked_dt"] < day_end and to_int(r.get("current_price")) is not None
        ]
        if in_day_prices:
            end_price = in_day_prices[-1]

    price_change = (
        start_price is not None and end_price is not None and start_price != end_price
    )
    change_amount = (
        end_price - start_price
        if start_price is not None and end_price is not None
        else None
    )
    change_pct = (
        change_amount / start_price * 100.0
        if change_amount is not None and start_price not in (None, 0)
        else None
    )

    complete = coverage_pct >= 99.0 and reconstructed_seconds <= 0
    if complete and max_interval_hours <= 30 and not price_change and not negative_delta:
        confidence = "high"
    elif complete and max_interval_hours <= 48 and not negative_delta:
        confidence = "medium"
    else:
        confidence = "low"

    meta = dict(catalog_row or {})
    sample = original_observations[-1] if original_observations else {}
    brand = (
        str(sample.get("brand_name") or "").strip()
        or str(meta.get("brand_name") or "").strip()
    )
    product_name = sample.get("product_name") or meta.get("product_name") or ""
    product_url = meta.get("product_url") or f"https://www.musinsa.com/products/{goods_no}"

    avg_price = None
    if abs(sales_est) > 1e-9 and gmv_seconds > 0:
        avg_price = gmv_est / sales_est if sales_est != 0 else end_price
    elif end_price is not None:
        avg_price = end_price

    return {
        "date": target_date.isoformat(),
        "brand_name": brand,
        "goods_no": str(goods_no),
        "product_name": product_name,
        "estimated_sales": round(sales_est, 2),
        "estimated_gmv": round(gmv_est) if gmv_seconds > 0 else "",
        "estimated_avg_price": round(avg_price) if avg_price is not None else "",
        "display_price": end_price if end_price is not None else "",
        "previous_display_price": start_price if start_price is not None else "",
        "price_change_detected": 1 if price_change else 0,
        "price_change_amount": change_amount if change_amount is not None else "",
        "price_change_pct": round(change_pct, 2) if change_pct is not None else "",
        "coverage_pct": round(coverage_pct, 2),
        "calendar_complete": 1 if complete else 0,
        "confidence": confidence,
        "max_interval_hours": round(max_interval_hours, 2),
        "observation_count": len(observations),
        "contributing_intervals": contributing,
        "initial_delta_status": initial_delta_status,
        "initial_delta_value": initial_delta_value if initial_delta_value else "",
        "history_bucket": calendar_bucket(goods_no),
        "product_url": product_url,
    }


def calendar_brand_rows(target_date, product_rows):
    grouped = {}
    for r in product_rows:
        brand = str(r.get("brand_name") or "").strip() or "(브랜드 미확인)"
        grouped.setdefault(brand, []).append(r)

    checked = now_kst().isoformat(timespec="seconds")
    out = []
    for brand, rows in sorted(grouped.items()):
        complete = [r for r in rows if to_int(r.get("calendar_complete")) == 1]
        coverages = [to_float(r.get("coverage_pct")) for r in rows]
        coverages = [x for x in coverages if x is not None]
        out.append({
            "date": target_date.isoformat(),
            "checked_at": checked,
            "brand_name": brand,
            "product_count": len(rows),
            "complete_product_count": len(complete),
            "product_coverage_pct": round(len(complete) / len(rows) * 100.0, 2) if rows else 100.0,
            "average_time_coverage_pct": round(sum(coverages) / len(coverages), 2) if coverages else 0.0,
            "estimated_sales": round(sum(to_float(r.get("estimated_sales")) or 0.0 for r in rows), 2),
            "estimated_gmv": round(sum(to_float(r.get("estimated_gmv")) or 0.0 for r in rows)),
            "price_change_products": sum(1 for r in rows if to_int(r.get("price_change_detected")) == 1),
            "high_confidence_products": sum(1 for r in rows if r.get("confidence") == "high"),
            "medium_confidence_products": sum(1 for r in rows if r.get("confidence") == "medium"),
            "low_confidence_products": sum(1 for r in rows if r.get("confidence") == "low"),
        })
    return out


def upsert_calendar_history(target_date, product_rows):
    """
    상품 상세 조회용 월별 64개 bucket CSV.
    goodsNo 하나를 클릭할 때 전체 5만개 일별파일을 읽지 않고
    해당 월의 bucket 하나만 읽도록 합니다.
    """
    month = target_date.strftime("%Y-%m")
    month_dir = CALENDAR_HISTORY_DIR / month
    month_dir.mkdir(parents=True, exist_ok=True)

    grouped = {}
    for row in product_rows:
        b = int(row.get("history_bucket") or 0)
        grouped.setdefault(b, []).append(row)

    existing_buckets = set()
    if month_dir.exists():
        for p in month_dir.glob("bucket-*.csv"):
            m = re.match(r"bucket-(\d+)\.csv$", p.name)
            if m:
                existing_buckets.add(int(m.group(1)))

    for b in sorted(existing_buckets | set(grouped)):
        path = month_dir / f"bucket-{b:02d}.csv"
        old = [
            r for r in read_csv(path)
            if str(r.get("date") or "") != target_date.isoformat()
        ]
        rows = old + grouped.get(b, [])
        rows.sort(key=lambda r: (
            str(r.get("date") or ""),
            int(r["goods_no"]) if str(r.get("goods_no", "")).isdigit() else 10**30,
        ))
        write_csv(path, rows, CALENDAR_PRODUCT_FIELDS)


def write_calendar_manifest():
    summaries = read_csv(CALENDAR_SUMMARY_FILE)
    dates = sorted({str(r.get("date") or "") for r in summaries if r.get("date")})
    months = sorted({
        p.name for p in CALENDAR_HISTORY_DIR.iterdir()
        if p.is_dir() and re.match(r"^\d{4}-\d{2}$", p.name)
    }) if CALENDAR_HISTORY_DIR.exists() else []
    payload = {
        "updated_at": now_kst().isoformat(timespec="seconds"),
        "latest_finalized_date": dates[-1] if dates else "",
        "finalized_dates": dates,
        "months": months,
        "history_buckets": CALENDAR_HISTORY_BUCKETS,
        "method": "purchaseTotal deltas within verified counter segments; KST overlap allocation; gaps over 36h are reconstructed estimates and excluded from observed coverage; price uses last-observation-carried-forward",
        "sales_policy_version": SALES_POLICY_VERSION,
    }
    CALENDAR_MANIFEST_FILE.parent.mkdir(parents=True, exist_ok=True)
    CALENDAR_MANIFEST_FILE.write_text(
        json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
        encoding="utf-8",
    )


def finalize_calendar_date(target_date, history_sink=None):
    observations = load_calendar_observations(target_date)
    catalog_rows = read_csv(CATALOG_FILE)
    catalog = {str(r.get("goods_no") or ""): r for r in catalog_rows if r.get("goods_no")}

    product_rows = []
    for g, obs in observations.items():
        row = estimate_calendar_product(target_date, g, obs, catalog.get(g))
        if row is not None:
            product_rows.append(row)

    product_rows.sort(key=lambda r: (
        str(r.get("brand_name") or ""),
        int(r["goods_no"]) if str(r.get("goods_no", "")).isdigit() else 10**30,
    ))

    brand_rows = calendar_brand_rows(target_date, product_rows)

    # A recalculation can remove every valid interval for a brand. Upsert alone
    # leaves its old inflated total behind, so replace this complete date slice.
    old_brand_rows = read_csv(CALENDAR_BRAND_FILE)
    observed_brands = {str(obs[-1].get("brand_name") or "") for obs in observations.values() if obs}
    observed_brands.update(r.get("brand_name") for r in old_brand_rows
                           if r.get("date") == target_date.isoformat())
    known_brands = {r["brand_name"] for r in brand_rows}
    for brand in sorted(observed_brands - known_brands - {"", None}):
        brand_rows.append({"date": target_date.isoformat(),
                           "checked_at": now_kst().isoformat(timespec="seconds"),
                           "brand_name": brand, "product_count": 0,
                           "complete_product_count": 0, "product_coverage_pct": 0,
                           "average_time_coverage_pct": 0,
                           "estimated_sales": "", "estimated_gmv": ""})
    brand_all = [r for r in old_brand_rows if r.get("date") != target_date.isoformat()] + brand_rows
    brand_all.sort(key=lambda r: (str(r.get("date") or ""), str(r.get("brand_name") or "")))
    write_csv(CALENDAR_BRAND_FILE, brand_all, CALENDAR_BRAND_FIELDS)

    complete_count = sum(1 for r in product_rows if to_int(r.get("calendar_complete")) == 1)
    coverages = [to_float(r.get("coverage_pct")) for r in product_rows]
    coverages = [x for x in coverages if x is not None]
    summary = {
        "date": target_date.isoformat(),
        "checked_at": now_kst().isoformat(timespec="seconds"),
        "brand_count": len({r.get("brand_name") for r in product_rows if r.get("brand_name")}),
        "product_count": len(product_rows),
        "complete_product_count": complete_count,
        "product_coverage_pct": round(complete_count / len(product_rows) * 100.0, 2) if product_rows else 0.0,
        "average_time_coverage_pct": round(sum(coverages) / len(coverages), 2) if coverages else 0.0,
        "estimated_sales": round(sum(to_float(r.get("estimated_sales")) or 0.0 for r in product_rows), 2) if product_rows else "",
        "estimated_gmv": round(sum(to_float(r.get("estimated_gmv")) or 0.0 for r in product_rows)) if product_rows else "",
        "price_change_products": sum(1 for r in product_rows if to_int(r.get("price_change_detected")) == 1),
    }
    summary_all = upsert_rows(
        CALENDAR_SUMMARY_FILE,
        [summary],
        CALENDAR_SUMMARY_FIELDS,
        lambda r: str(r.get("date") or ""),
    )
    summary_all.sort(key=lambda r: str(r.get("date") or ""))
    write_csv(CALENDAR_SUMMARY_FILE, summary_all, CALENDAR_SUMMARY_FIELDS)

    if history_sink is None:
        upsert_calendar_history(target_date, product_rows)
    else:
        history_sink(target_date, product_rows)

    # 가장 최근 finalize 날짜를 메인 상품표로 사용
    latest_date = max(
        [str(r.get("date") or "") for r in summary_all if r.get("date")] or [target_date.isoformat()]
    )
    if target_date.isoformat() == latest_date:
        write_csv(CALENDAR_LATEST_PRODUCT_FILE, product_rows, CALENDAR_PRODUCT_FIELDS)

    write_calendar_manifest()

    print(json.dumps({
        "calendar_date": target_date.isoformat(),
        "products": len(product_rows),
        "complete_products": complete_count,
        "product_coverage_pct": summary["product_coverage_pct"],
        "estimated_sales": summary["estimated_sales"],
        "estimated_gmv": summary["estimated_gmv"],
        "price_change_products": summary["price_change_products"],
    }, ensure_ascii=False))
    return summary


def finalize_calendar_recent(lookback_days=3, date_text=None):
    if date_text:
        target = datetime.strptime(date_text, "%Y-%m-%d").date()
        finalize_calendar_date(target)
        return 0

    today = now_kst().date()
    days = max(1, int(lookback_days))
    targets = [today - timedelta(days=i) for i in range(days, 0, -1)]
    for target in targets:
        # 오늘은 아직 00~24시가 끝나지 않았으므로 항상 어제까지만 finalize
        if target >= today:
            continue
        finalize_calendar_date(target)
    return 0


def repair_sales_analytics():
    """One-time, offline rebuild of derived analytics from unchanged observations.

    Called only by existing FIFO writers. Rebuild dates already stored in this
    repository, never create historical observations or re-fetch past counters.
    The marker is written last, so interrupted repairs retry safely.
    """
    if SALES_POLICY_FILE.exists():
        marker = json.loads(SALES_POLICY_FILE.read_text(encoding="utf-8"))
        if marker.get("version") == SALES_POLICY_VERSION:
            return marker

    original_latest = {r["goods_no"]: to_int(r.get("daily_sales"))
                       for r in read_csv(LATEST_PRODUCT_FILE) if r.get("goods_no")}
    snapshot_dates = set()
    latest_slot_dates = {}
    latest_slot_counts = {}
    for slot in range(SLOT_COUNT):
        folder = SLOT_DIR / f"slot-{slot}"
        dates = sorted({p.name[:10] for p in folder.glob("*.csv*")
                        if re.match(r"^\d{4}-\d{2}-\d{2}\.csv(?:\.gz)?$", p.name)})
        snapshot_dates.update(dates)
        if dates:
            latest_slot_dates[slot] = dates[-1]

    # Use every existing finalized day so an old product-detail spike cannot
    # survive merely because it falls outside the routine three-day lookback.
    calendar_dates = sorted({r.get("date") for r in read_csv(CALENDAR_SUMMARY_FILE) if r.get("date")})
    for date_text in sorted(snapshot_dates):
        target = datetime.strptime(date_text, "%Y-%m-%d").date()
        rows = build_rows_for_date(target)
        rebuild_date_aggregates(target, rows, write_daily_snapshot=False)
        for slot, latest_date in latest_slot_dates.items():
            if latest_date == date_text:
                slot_rows = [r for r in rows if to_int(r.get("slot")) == slot]
                write_csv(LATEST_SLOT_DIR / f"slot-{slot}.csv.gz",
                          slot_rows, LATEST_FIELDS)
                latest_slot_counts[slot] = len(slot_rows)
        print(f"[sales-repair] rolling {date_text}: {len(rows)} rows", flush=True)
    for slot, expected in latest_slot_counts.items():
        path = LATEST_SLOT_DIR / f"slot-{slot}.csv.gz"
        with gzip.open(path, "rt", encoding="utf-8-sig", newline="") as stream:
            actual = sum(1 for _ in csv.DictReader(stream))
        if actual != expected:
            raise RuntimeError(f"Incomplete sales repair for slot {slot}: expected {expected}, read {actual}")
    rebuilt_latest = rebuild_latest_product_file()

    changes = []
    for row in rebuilt_latest:
        before = original_latest.get(str(row.get("goods_no")))
        after = to_int(row.get("daily_sales"))
        if before is not None and before != after:
            changes.append({"goods_no": row.get("goods_no"), "brand_name": row.get("brand_name"),
                            "date": row.get("date"), "before": before, "after": after,
                            "reason": row.get("daily_sales_status")})
    changes.sort(key=lambda x: x["before"], reverse=True)
    # Calendar estimation loads several days of observations; release the large
    # rolling tables before that phase on the hosted runner.
    del original_latest, rebuilt_latest
    if snapshot_dates:
        del rows, slot_rows
    # Rewriting every monthly bucket for every day is quadratic in the history
    # size. Spool new rows by bucket, then replace each bucket once. Temporary
    # files are outside the repository and never become observation snapshots.
    closed_dates = {d for d in calendar_dates if d < now_kst().date().isoformat()}
    with tempfile.TemporaryDirectory(prefix="musinsa-sales-repair-") as tmp:
        staging = Path(tmp)

        def stage_history(target, product_rows):
            grouped = {}
            for row in product_rows:
                grouped.setdefault(int(row.get("history_bucket") or 0), []).append(row)
            for bucket, bucket_rows in grouped.items():
                path = staging / target.strftime("%Y-%m") / f"bucket-{bucket:02d}.csv"
                path.parent.mkdir(parents=True, exist_ok=True)
                exists = path.exists()
                with path.open("a", encoding="utf-8", newline="") as stream:
                    writer = csv.DictWriter(stream, fieldnames=CALENDAR_PRODUCT_FIELDS)
                    if not exists:
                        writer.writeheader()
                    writer.writerows(bucket_rows)

        for date_text in sorted(closed_dates):
            target = datetime.strptime(date_text, "%Y-%m-%d").date()
            finalize_calendar_date(target, history_sink=stage_history)
            print(f"[sales-repair] calendar {date_text}", flush=True)

        for month in sorted({d[:7] for d in closed_dates}):
            existing = CALENDAR_HISTORY_DIR / month
            staged = staging / month
            names = {p.name for p in existing.glob("bucket-*.csv")}
            names.update(p.name for p in staged.glob("bucket-*.csv"))
            for name in sorted(names):
                kept = [r for r in read_csv(existing / name) if r.get("date") not in closed_dates]
                merged = kept + read_csv(staged / name)
                merged.sort(key=lambda r: (str(r.get("date") or ""),
                                          int(r["goods_no"]) if str(r.get("goods_no", "")).isdigit() else 10**30))
                write_csv(existing / name, merged, CALENDAR_PRODUCT_FIELDS)

    write_calendar_manifest()
    marker = {"version": SALES_POLICY_VERSION,
              "updated_at": now_kst().isoformat(timespec="seconds"),
              "snapshot_dates_rebuilt": sorted(snapshot_dates),
              "calendar_dates_rebuilt": calendar_dates,
              "latest_values_corrected": len(changes),
              "largest_corrections": changes[:30],
              "raw_observations_modified": False,
              "max_attribution_interval_hours": MAX_SALES_INTERVAL_HOURS}
    SALES_POLICY_FILE.parent.mkdir(parents=True, exist_ok=True)
    SALES_POLICY_FILE.write_text(json.dumps(marker, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"sales_repair": marker}, ensure_ascii=False), flush=True)
    return marker

def main():
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("discover-slot")
    p.add_argument("--state-dir", default="run_state")
    p.add_argument("--slot", type=int, required=True, choices=range(8))
    p.add_argument("--full-discovery", action="store_true")

    p = sub.add_parser("collect-slot-shard")
    p.add_argument("--state-dir", default="run_state")
    p.add_argument("--slot", type=int, required=True, choices=range(8))
    p.add_argument("--shard-index", type=int, required=True)
    p.add_argument("--shard-count", type=int, required=True)
    p.add_argument("--output", required=True)

    p = sub.add_parser("aggregate-slot")
    p.add_argument("--state-dir", default="run_state")
    p.add_argument("--shard-dir", required=True)

    p = sub.add_parser("recover-pending")
    p.add_argument("--lookback-days", type=int, default=2)
    p.add_argument("--max-queues", type=int, default=8)

    p = sub.add_parser("collect-adaptive")
    p.add_argument("--clock-slot", type=int, required=True, choices=range(SLOT_COUNT))
    p.add_argument("--max-products", type=int, default=0)
    p.add_argument("--dry-run", action="store_true")

    p = sub.add_parser("collect-midnight-anchor")
    p.add_argument("--max-products", type=int, default=0)
    p.add_argument("--dry-run", action="store_true")

    p = sub.add_parser("finalize-calendar")
    p.add_argument("--lookback-days", type=int, default=3)
    p.add_argument("--date", default="")

    sub.add_parser("repair-sales", help="Rebuild derived sales from stored observations; no collection")

    args = parser.parse_args()
    # Historical repair must never hold up saving new observations. The live
    # dashboard publisher rebuilds its own outputs independently after each run.
    if args.cmd == "repair-sales":
        repair_sales_analytics()
    if args.cmd == "repair-sales":
        return 0
    if args.cmd == "discover-slot":
        return discover_slot(args.state_dir, args.slot, args.full_discovery)
    if args.cmd == "collect-slot-shard":
        return collect_slot_shard(
            args.state_dir, args.slot, args.shard_index, args.shard_count, args.output
        )
    if args.cmd == "aggregate-slot":
        return aggregate_slot(args.state_dir, args.shard_dir)
    if args.cmd == "recover-pending":
        return recover_pending(args.lookback_days, args.max_queues)
    if args.cmd == "collect-adaptive":
        return collect_adaptive(args.clock_slot, args.max_products, args.dry_run)
    if args.cmd == "collect-midnight-anchor":
        return collect_midnight_anchor(args.max_products, args.dry_run)
    if args.cmd == "finalize-calendar":
        return finalize_calendar_recent(args.lookback_days, args.date or None)
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
