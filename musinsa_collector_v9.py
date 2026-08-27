# -*- coding: utf-8 -*-
"""
Musinsa Distributed Collector v9
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
import html as html_lib
import json
import os
import random
import re
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import unicodedata
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

# v9 calendar-day analytics
CALENDAR_DIR = BASE_DIR / "data" / "calendar"
CALENDAR_HISTORY_DIR = CALENDAR_DIR / "history"
CALENDAR_MANIFEST_FILE = CALENDAR_DIR / "calendar_manifest.json"
CALENDAR_LATEST_PRODUCT_FILE = BASE_DIR / "musinsa_calendar_latest_products.csv"
CALENDAR_BRAND_FILE = BASE_DIR / "musinsa_calendar_brand_daily.csv"
CALENDAR_SUMMARY_FILE = BASE_DIR / "musinsa_calendar_summary.csv"
CALENDAR_HISTORY_BUCKETS = 64

SLOT_COUNT = 8
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

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/151.0.0.0 Safari/537.36"
)

CATALOG_FIELDS = [
    "goods_no", "brand_name", "product_name", "normal_price",
    "current_price", "sale_rate", "review_count", "rating",
    "availability", "first_seen_at", "last_seen_at", "product_url",
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
    "normal_price", "current_price", "sale_rate",
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
    if path.suffix.lower() == ".gz":
        with gzip.open(path, "wt", encoding="utf-8-sig", newline="", compresslevel=6) as f:
            w = csv.DictWriter(f, fieldnames=fields)
            w.writeheader()
            for row in rows:
                w.writerow({k: row.get(k, "") for k in fields})
        return
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for row in rows:
            w.writerow({k: row.get(k, "") for k in fields})


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


def _extract_brand_name(item):
    candidates = [
        item.get("brandName"),
        item.get("brandKorName"),
        item.get("brandKoreanName"),
        item.get("brand"),
    ]
    for value in candidates:
        if isinstance(value, dict):
            for key in ("name", "brandName", "korName", "koreanName"):
                v = value.get(key)
                if v:
                    return str(v).strip()
        elif value:
            return str(value).strip()
    return ""


def _brand_keys(value):
    """
    브랜드 표기 흔들림을 안전하게 흡수합니다.
    예:
      디미트리블랙
      디미트리 블랙
      디미트리블랙(DIMITRI BLACK)
    는 같은 브랜드로 인식합니다.

    prefix/fuzzy 매칭은 하지 않아 다른 브랜드가 섞이는 위험을 줄입니다.
    """
    s = unicodedata.normalize("NFKC", str(value or "")).strip().casefold()
    if not s:
        return set()

    keys = set()

    compact = re.sub(r"[^0-9a-z가-힣]+", "", s)
    if compact:
        keys.add(compact)

    korean = "".join(re.findall(r"[가-힣]+", s))
    if korean:
        keys.add(korean)

    latin = "".join(re.findall(r"[a-z0-9]+", s))
    if latin:
        keys.add(latin)

    base = re.sub(r"\([^)]*\)", "", s)
    base_compact = re.sub(r"[^0-9a-z가-힣]+", "", base)
    if base_compact:
        keys.add(base_compact)

    for part in re.findall(r"\(([^)]*)\)", s):
        p = re.sub(r"[^0-9a-z가-힣]+", "", part)
        if p:
            keys.add(p)

    return keys


def _brand_matches(requested, candidate):
    a = _brand_keys(requested)
    b = _brand_keys(candidate)
    return bool(a and b and (a & b))


def search_brand_products(brand_name, known_goods=None, exhaustive=False):
    """
    무신사 검색결과에서 요청 브랜드의 상품을 수집합니다.

    핵심 수정:
    - count API의 total 값을 '하드 페이지 제한'으로 사용하지 않습니다.
      일부 검색에서 count가 100처럼 제한되어도 exhaustive 모드는 실제 결과가
      끝날 때까지 계속 pagination 합니다.
    - 브랜드 표기 공백/괄호/한영 병기 차이를 정규화해 매칭합니다.
    - 같은 페이지가 반복되거나 해당 브랜드 결과가 연속해서 사라지면 안전 종료합니다.
    """
    brand_name = brand_name.strip()
    if not brand_name:
        return []

    known_goods = set(str(x) for x in (known_goods or set()))
    keyword = urllib.parse.quote(brand_name)

    # 참고용 count. 페이지 제한에는 사용하지 않습니다.
    count_url = (
        "https://api.musinsa.com/api2/sc/v2/search/tab/count"
        f"?gf=A&keyword={keyword}&sendLog=true"
    )
    try:
        count_data = search_json(count_url, brand_name)
        reported_total = to_int(
            ((((count_data or {}).get("data") or {}).get("goods") or {}).get("all"))
        ) or 0
    except Exception:
        reported_total = 0

    page_size = 60

    if exhaustive:
        page_limit = MAX_SEARCH_PAGES
    else:
        # 기존 catalog 상품을 모두 다시 볼 수 있을 만큼의 페이지는 최소 확보
        known_pages = ((len(known_goods) + page_size - 1) // page_size) + 5 if known_goods else 0
        page_limit = min(MAX_SEARCH_PAGES, max(QUICK_SEARCH_MAX_PAGES, known_pages))

    results = []
    seen_exact = set()
    seen_any = set()
    seen_known = set()

    no_exact_streak = 0
    repeated_page_streak = 0

    for page in range(1, page_limit + 1):
        url = (
            "https://api.musinsa.com/api2/dp/v1/plp/goods"
            f"?gf=A&keyword={keyword}&sortCode=NEW&page={page}&size={page_size}&caller=SEARCH"
        )

        try:
            data = search_json(url, brand_name)
        except Exception as e:
            print(f"[search] {brand_name} page {page}: {e}", file=sys.stderr)
            continue

        items = (((data or {}).get("data") or {}).get("list") or [])
        if not isinstance(items, list) or not items:
            break

        page_any_new = 0
        page_exact = 0

        for item in items:
            if not isinstance(item, dict):
                continue

            goods_no = str(item.get("goodsNo") or "").strip()
            if not goods_no:
                continue

            if goods_no not in seen_any:
                seen_any.add(goods_no)
                page_any_new += 1

            item_brand = _extract_brand_name(item)
            if not _brand_matches(brand_name, item_brand):
                continue

            if goods_no in seen_exact:
                continue

            seen_exact.add(goods_no)
            page_exact += 1

            if goods_no in known_goods:
                seen_known.add(goods_no)

            current_price = to_int(item.get("finalPrice"))
            if current_price is None:
                current_price = to_int(item.get("price"))

            results.append({
                "goods_no": goods_no,
                "brand_name": item_brand or brand_name,
                "product_name": item.get("goodsName") or "",
                "normal_price": to_int(item.get("normalPrice")),
                "current_price": current_price,
                "sale_rate": (
                    to_int(item.get("finalDiscount"))
                    if to_int(item.get("finalDiscount")) is not None
                    else to_int(item.get("saleRate"))
                ),
                "review_count": to_int(item.get("reviewCount")),
                "rating": item.get("reviewScore"),
                "availability": "OutOfStock" if item.get("isSoldOut") else "InStock",
                "product_url": f"https://www.musinsa.com/products/{goods_no}",
            })

        if page_any_new == 0:
            repeated_page_streak += 1
        else:
            repeated_page_streak = 0

        if page_exact == 0:
            no_exact_streak += 1
        else:
            no_exact_streak = 0

        # API가 같은 페이지를 반복하기 시작하면 무한 loop 방지
        if repeated_page_streak >= 2:
            break

        if exhaustive:
            # 관련없는 검색결과만 연속으로 나오는 구간까지 도달하면 종료
            if no_exact_streak >= 3:
                break
        else:
            # 기존 상품을 모두 확인했으면 daily price refresh 목적 달성
            if known_goods and len(seen_known) >= len(known_goods):
                break
            if not known_goods and no_exact_streak >= QUICK_KNOWN_STOP_PAGES:
                break

        time.sleep(0.15 + random.uniform(0.03, 0.12))

    print(
        f"[brand-search] {brand_name}: reported_total={reported_total}, "
        f"matched={len(results)}, pages_scanned={page}",
        file=sys.stderr,
    )
    return results
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

    known_by_brand = {}
    for g, row in catalog.items():
        b = str(row.get("brand_name") or "").strip().casefold()
        if b:
            known_by_brand.setdefault(b, set()).add(g)

    weekly_full = snapshot_date.weekday() == 6  # Sunday KST
    ts = now_kst().isoformat(timespec="seconds")

    found = {}
    modes = {}

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
                    found[brand] = fut.result()
                except Exception as e:
                    print(f"[discover] {brand}: {e}", file=sys.stderr)
                    found[brand] = []

    new_rows = []
    for brand in assigned_brands:
        for p in found.get(brand, []):
            g = str(p["goods_no"])
            old = catalog.get(g, {})
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
                "product_url": p.get("product_url") or old.get("product_url") or f"https://www.musinsa.com/products/{g}",
            }
            catalog[g] = row
            if g not in existing_goods:
                new_rows.append({
                    "first_seen_at": ts,
                    "brand_name": row["brand_name"],
                    "goods_no": g,
                    "product_name": row["product_name"],
                    "normal_price": row["normal_price"],
                    "current_price": row["current_price"],
                    "sale_rate": row["sale_rate"],
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
    return 0

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


def metric_delta(cur, baseline, field):
    cv = to_int(cur.get(field))
    bv = to_int(baseline.get(field)) if baseline else None
    if cv is None or bv is None:
        return None
    return cv - bv


def build_latest_row(raw, prev, b7, b30, today, slot):
    price = to_int(raw.get("current_price"))
    daily_sales = metric_delta(raw, prev, "purchase_total")
    sales7 = metric_delta(raw, b7, "purchase_total")
    sales30 = metric_delta(raw, b30, "purchase_total")
    views = metric_delta(raw, prev, "page_view_total")
    reviews = metric_delta(raw, prev, "review_count")

    return {
        "date": today.isoformat(),
        "slot": slot,
        "checked_at": raw.get("checked_at") or now_kst().isoformat(timespec="seconds"),
        "brand_name": raw.get("brand_name") or "",
        "goods_no": raw.get("goods_no") or "",
        "product_name": raw.get("product_name") or "",
        "purchase_total": to_int(raw.get("purchase_total")),
        "daily_sales": daily_sales,
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

        daily = [x for x in items if to_int(x.get("daily_sales")) is not None]
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
            "daily_sales_sum": sum(to_int(x.get("daily_sales")) or 0 for x in daily),
            "daily_estimated_gmv_sum": sum(to_int(x.get("daily_estimated_gmv")) or 0 for x in daily),
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
            out.append(build_latest_row(raw, prev.get(g), d7.get(g), d30.get(g), date_value, slot))
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


def rebuild_date_aggregates(date_value):
    rows = build_rows_for_date(date_value)
    new_delta = new_products_for_date(date_value)
    brand_today = brand_rows_for_date(rows, new_delta, date_value)
    brand_all = upsert_rows(
        BRAND_HISTORY_FILE, brand_today, BRAND_FIELDS,
        lambda r: (str(r.get("date") or ""), str(r.get("brand_name") or ""))
    )
    brand_all.sort(key=lambda r: (str(r.get("date") or ""), str(r.get("brand_name") or "")))
    write_csv(BRAND_HISTORY_FILE, brand_all, BRAND_FIELDS)

    daily_valid = [r for r in rows if to_int(r.get("daily_sales")) is not None]
    summary = {
        "checked_at": now_kst().isoformat(timespec="seconds"),
        "date": date_value.isoformat(),
        "product_count": len(rows),
        "daily_baseline_product_count": len(daily_valid),
        "purchase_total_sum": sum(to_int(r.get("purchase_total")) or 0 for r in rows),
        "simple_gmv_sum": sum(to_int(r.get("simple_gmv")) or 0 for r in rows),
        "daily_sales_sum": sum(to_int(r.get("daily_sales")) or 0 for r in daily_valid),
        "daily_estimated_gmv_sum": sum(to_int(r.get("daily_estimated_gmv")) or 0 for r in daily_valid),
        "sales_7d_sum": sum(to_int(r.get("sales_7d")) or 0 for r in rows if to_int(r.get("sales_7d")) is not None),
        "sales_30d_sum": sum(to_int(r.get("sales_30d")) or 0 for r in rows if to_int(r.get("sales_30d")) is not None),
        "new_products": len(new_delta),
    }
    summaries = upsert_rows(SUMMARY_FILE, [summary], SUMMARY_FIELDS, lambda r: str(r.get("date") or ""))
    summaries.sort(key=lambda r: str(r.get("date") or ""))
    write_csv(SUMMARY_FILE, summaries, SUMMARY_FIELDS)

    slot_files = [SLOT_DIR / f"slot-{s}" / f"{date_value.isoformat()}.csv.gz" for s in range(SLOT_COUNT)]
    if all(p.exists() for p in slot_files):
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
        latest.append(build_latest_row(raw, prev.get(g), d7.get(g), d30.get(g), date_value, slot))
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
        latest.append(build_latest_row(r, prev.get(g), d7.get(g), d30.get(g), today, slot))
        compact.append(compact_from_raw(r, today, slot))

    write_csv(SLOT_DIR / f"slot-{slot}" / f"{today.isoformat()}.csv.gz", compact, COMPACT_FIELDS)
    write_csv(LATEST_SLOT_DIR / f"slot-{slot}.csv.gz", latest, LATEST_FIELDS)
    write_history_manifest()

    # discovery 상태 root 반영
    write_lines(WATCHLIST_FILE, watchlist)
    write_csv(CATALOG_FILE, catalog_rows, CATALOG_FIELDS)
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


def estimate_calendar_product(target_date, goods_no, observations, catalog_row=None):
    day_start = datetime(
        target_date.year, target_date.month, target_date.day, 0, 0, 0, tzinfo=KST
    )
    day_end = day_start + timedelta(days=1)

    # target에 영향을 줄 수 있는 관측만 남기되 경계 이전/이후 관측은 반드시 유지
    observations = sorted(observations, key=lambda r: r["_checked_dt"])
    if len(observations) < 2:
        return None

    sales_est = 0.0
    gmv_est = 0.0
    gmv_seconds = 0.0
    coverage_seconds = 0.0
    contributing = 0
    max_interval_hours = 0.0
    negative_delta = False

    for i in range(1, len(observations)):
        a = observations[i - 1]
        b = observations[i]
        t0, t1 = a["_checked_dt"], b["_checked_dt"]
        if t1 <= t0:
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
            negative_delta = True

        duration = (t1 - t0).total_seconds()
        if duration <= 0:
            continue

        overlap_start = max(t0, day_start)
        overlap_end = min(t1, day_end)
        fraction = overlap / duration

        sales_est += delta * fraction
        coverage_seconds += overlap
        contributing += 1
        max_interval_hours = max(max_interval_hours, duration / 3600.0)

        gmv_piece, priced_seconds = interval_gmv_contribution(
            a, b, overlap_start, overlap_end, delta, duration
        )
        if gmv_piece is not None:
            gmv_est += gmv_piece
            gmv_seconds += priced_seconds

    coverage_pct = min(100.0, coverage_seconds / 86400.0 * 100.0)
    if coverage_seconds <= 0:
        return None

    start_price = price_at_or_before(observations, day_start)
    end_price = price_at_or_before(observations, day_end - timedelta(microseconds=1))
    if end_price is None:
        # 당일 마지막 관측가 fallback
        in_day_prices = [
            to_int(r.get("current_price")) for r in observations
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

    complete = coverage_pct >= 99.0
    if complete and max_interval_hours <= 30 and not price_change and not negative_delta:
        confidence = "high"
    elif coverage_pct >= 95.0 and max_interval_hours <= 48 and not negative_delta:
        confidence = "medium"
    else:
        confidence = "low"

    meta = dict(catalog_row or {})
    sample = observations[-1] if observations else {}
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
        "method": "uniform purchaseTotal delta allocation across KST calendar-day overlap; price uses last-observation-carried-forward",
    }
    CALENDAR_MANIFEST_FILE.parent.mkdir(parents=True, exist_ok=True)
    CALENDAR_MANIFEST_FILE.write_text(
        json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
        encoding="utf-8",
    )


def finalize_calendar_date(target_date):
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

    brand_all = upsert_rows(
        CALENDAR_BRAND_FILE,
        brand_rows,
        CALENDAR_BRAND_FIELDS,
        lambda r: (str(r.get("date") or ""), str(r.get("brand_name") or "")),
    )
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
        "product_coverage_pct": round(complete_count / len(product_rows) * 100.0, 2) if product_rows else 100.0,
        "average_time_coverage_pct": round(sum(coverages) / len(coverages), 2) if coverages else 0.0,
        "estimated_sales": round(sum(to_float(r.get("estimated_sales")) or 0.0 for r in product_rows), 2),
        "estimated_gmv": round(sum(to_float(r.get("estimated_gmv")) or 0.0 for r in product_rows)),
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

    upsert_calendar_history(target_date, product_rows)

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

    p = sub.add_parser("finalize-calendar")
    p.add_argument("--lookback-days", type=int, default=3)
    p.add_argument("--date", default="")

    args = parser.parse_args()
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
    if args.cmd == "finalize-calendar":
        return finalize_calendar_recent(args.lookback_days, args.date or None)
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
