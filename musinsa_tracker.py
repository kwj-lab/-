# -*- coding: utf-8 -*-
"""
무신사 상품번호 일괄 조회기
====================================================
기능
- 상품번호를 여러 개(줄바꿈/쉼표/공백/URL 혼합) 붙여넣기
- 상품명 / 브랜드 / 누적 구매수 / 조회수 / 정상가 / 현재가 / 할인율
  / 리뷰수 / 평점 / 좋아요 / 판매상태 조회
- 현재가 × 누적 구매수 = 단순 누적 GMV 추정
- 전체 상품의 누적 구매수 / 단순 누적 GMV 합계
- 이전 조회 대비 구매수 증가량 / 증가 GMV
- CSV 내보내기
- 조회 이력 자동 저장

주의
- purchaseTotal은 무신사 공개 PDP 통계 API의 '누적 구매수' 필드입니다.
- 단순 누적 GMV는 현재가 × 누적 구매수이며 실제 과거 결제액/정산매출과 다릅니다.
- 과거 할인, 쿠폰, 취소/반품, 수수료, 브랜드 정산액은 알 수 없습니다.
- 무신사 페이지/API 구조 변경 시 일부 필드가 조회되지 않을 수 있습니다.
"""

import csv
import json
import re
import time
import threading
import urllib.request
import urllib.error
import urllib.parse
import sys
import html as html_lib
from datetime import datetime
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import tkinter as tk
from tkinter import ttk, filedialog, messagebox

APP_TITLE = "무신사 판매 추적기"
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/151.0.0.0 Safari/537.36"
)
BASE_DIR = Path(__file__).resolve().parent
HISTORY_FILE = BASE_DIR / "musinsa_bulk_history.csv"
WATCHLIST_FILE = BASE_DIR / "musinsa_watchlist.txt"
BRANDS_FILE = BASE_DIR / "musinsa_brands.txt"
NEW_PRODUCTS_FILE = BASE_DIR / "musinsa_new_products.csv"
DAILY_SUMMARY_FILE = BASE_DIR / "musinsa_daily_summary.csv"
SNAPSHOT_DIR = BASE_DIR / "daily_snapshots"
AUTO_LOG_FILE = BASE_DIR / "musinsa_auto_update.log"
MAX_WATCHLIST = 1200

# 서버에 과도하게 요청하지 않도록 동시 요청 수를 낮게 유지
MAX_WORKERS = 4

HISTORY_FIELDS = [
    "checked_at", "goods_no", "product_name", "brand_name",
    "purchase_total", "page_view_total",
    "normal_price", "current_price", "sale_rate",
    "review_count", "rating", "like_count", "availability",
    "simple_gmv", "product_url",
]


# -----------------------------
# 공통 HTTP / 파싱 유틸
# -----------------------------
def http_get(url, timeout=15, retries=2):
    last_error = None
    for attempt in range(retries + 1):
        try:
            req = urllib.request.Request(
                url,
                headers={
                    "User-Agent": USER_AGENT,
                    "Accept": "application/json,text/html,application/xhtml+xml,*/*",
                    "Accept-Language": "ko-KR,ko;q=0.9,en;q=0.8",
                    "Referer": "https://www.musinsa.com/",
                    "Cache-Control": "no-cache",
                },
            )
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                raw = resp.read()
                charset = resp.headers.get_content_charset() or "utf-8"
                return raw.decode(charset, errors="replace")
        except Exception as e:
            last_error = e
            if attempt < retries:
                time.sleep(0.8 * (attempt + 1))
    raise last_error


def http_post_json(url, payload, timeout=15, retries=1):
    last_error = None
    body = json.dumps(payload).encode("utf-8")
    for attempt in range(retries + 1):
        try:
            req = urllib.request.Request(
                url,
                data=body,
                method="POST",
                headers={
                    "User-Agent": USER_AGENT,
                    "Accept": "application/json,*/*",
                    "Content-Type": "application/json",
                    "Origin": "https://www.musinsa.com",
                    "Referer": "https://www.musinsa.com/",
                },
            )
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                raw = resp.read()
                charset = resp.headers.get_content_charset() or "utf-8"
                return raw.decode(charset, errors="replace")
        except Exception as e:
            last_error = e
            if attempt < retries:
                time.sleep(0.8 * (attempt + 1))
    raise last_error


def unwrap_data(obj):
    if isinstance(obj, dict) and "data" in obj:
        return obj.get("data")
    return obj


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


def fmt_num(value):
    if value is None:
        return "-"
    try:
        return f"{int(value):,}"
    except Exception:
        return str(value)


def fmt_signed(value):
    if value is None:
        return "-"
    try:
        return f"{int(value):+,}"
    except Exception:
        return str(value)


def fmt_won(value):
    if value is None:
        return "-"
    try:
        return f"{int(value):,}원"
    except Exception:
        return str(value)


def extract_script(html, attr_pattern):
    pattern = re.compile(
        rf"<script[^>]*{attr_pattern}[^>]*>(.*?)</script>",
        re.I | re.S,
    )
    m = pattern.search(html)
    return html_lib.unescape(m.group(1).strip()) if m else None


def product_from_jsonld(obj):
    candidates = []
    if isinstance(obj, dict):
        if obj.get("@type") == "Product":
            candidates.append(obj)
        graph = obj.get("@graph")
        if isinstance(graph, list):
            candidates.extend(
                x for x in graph
                if isinstance(x, dict) and x.get("@type") == "Product"
            )
    elif isinstance(obj, list):
        candidates.extend(
            x for x in obj
            if isinstance(x, dict) and x.get("@type") == "Product"
        )
    return candidates[0] if candidates else None


def find_matching_goods_dict(obj, goods_no, out):
    if isinstance(obj, dict):
        identifiers = [
            obj.get("goodsNo"),
            obj.get("goods_no"),
            obj.get("musinsaProductId"),
            obj.get("productId"),
        ]
        if any(str(v) == str(goods_no) for v in identifiers if v is not None):
            out.append(obj)
        for v in obj.values():
            find_matching_goods_dict(v, goods_no, out)
    elif isinstance(obj, list):
        for v in obj:
            find_matching_goods_dict(v, goods_no, out)


def best_goods_dict(candidates):
    if not candidates:
        return {}
    preferred_keys = {
        "goodsName", "productName", "brandName", "normalPrice",
        "finalPrice", "saleRate", "reviewCount", "isSoldOut",
    }
    return max(
        candidates,
        key=lambda d: sum(1 for k in preferred_keys if k in d),
    )


def first_existing(d, keys):
    for k in keys:
        if isinstance(d, dict) and d.get(k) not in (None, ""):
            return d.get(k)
    return None


def parse_product_page(html, goods_no):
    result = {
        "product_name": None,
        "brand_name": None,
        "normal_price": None,
        "current_price": None,
        "sale_rate": None,
        "review_count": None,
        "rating": None,
        "availability": None,
    }

    # JSON-LD
    try:
        raw = extract_script(html, r'type=["\']application/ld\+json["\']')
        if raw:
            ld = json.loads(raw)
            p = product_from_jsonld(ld)
            if p:
                result["product_name"] = p.get("name")
                brand = p.get("brand")
                if isinstance(brand, dict):
                    result["brand_name"] = brand.get("name")
                elif brand:
                    result["brand_name"] = str(brand)

                offers = p.get("offers")
                if isinstance(offers, list) and offers:
                    offers = offers[0]
                if isinstance(offers, dict):
                    result["current_price"] = to_int(offers.get("price"))
                    av = offers.get("availability")
                    if av:
                        result["availability"] = str(av).rsplit("/", 1)[-1]

                rating = p.get("aggregateRating")
                if isinstance(rating, dict):
                    result["review_count"] = to_int(rating.get("reviewCount"))
                    result["rating"] = rating.get("ratingValue")
    except Exception:
        pass

    # __NEXT_DATA__
    try:
        raw_next = extract_script(html, r'id=["\']__NEXT_DATA__["\']')
        if raw_next:
            nxt = json.loads(raw_next)
            matches = []
            find_matching_goods_dict(nxt, goods_no, matches)
            g = best_goods_dict(matches)

            result["product_name"] = result["product_name"] or first_existing(
                g, ["goodsName", "productName", "name"]
            )
            result["brand_name"] = result["brand_name"] or first_existing(
                g, ["brandName", "brand_name"]
            )

            normal_price = to_int(first_existing(g, ["normalPrice", "originPrice", "originalPrice"]))
            final_price = to_int(first_existing(g, ["finalPrice", "price", "salePrice"]))
            sale_rate = to_int(first_existing(g, ["saleRate", "discountRate", "finalDiscount"]))
            review_count = to_int(first_existing(g, ["reviewCount", "review_count"]))
            is_sold_out = first_existing(g, ["isSoldOut", "soldOut"])

            result["normal_price"] = normal_price or result["normal_price"]
            result["current_price"] = final_price or result["current_price"]
            if sale_rate is not None:
                result["sale_rate"] = sale_rate
            if review_count is not None:
                result["review_count"] = review_count
            if is_sold_out is not None:
                result["availability"] = "OutOfStock" if bool(is_sold_out) else "InStock"
    except Exception:
        pass

    if result["normal_price"] is None and result["current_price"] is not None:
        result["normal_price"] = result["current_price"]

    if result["sale_rate"] is None:
        np = result["normal_price"]
        cp = result["current_price"]
        if np and cp is not None and np > 0:
            result["sale_rate"] = round((1 - cp / np) * 100)

    return result


def fetch_stat(goods_no):
    url = f"https://goods-detail.musinsa.com/api2/goods/{goods_no}/stat"
    obj = unwrap_data(json.loads(http_get(url)))
    if not isinstance(obj, dict):
        raise ValueError("통계 API 응답 형식을 읽을 수 없음")
    return {
        "purchase_total": to_int(obj.get("purchaseTotal")),
        "page_view_total": to_int(obj.get("pageViewTotal")),
    }


def fetch_review_summary(goods_no):
    try:
        url = f"https://goods-detail.musinsa.com/api2/review/v1/goods/{goods_no}/reviews/summary"
        obj = unwrap_data(json.loads(http_get(url, retries=1)))
        if not isinstance(obj, dict):
            return {}
        return {
            "review_count": to_int(obj.get("totalCount")),
            "rating": obj.get("satisfactionScore"),
        }
    except Exception:
        return {}


def fetch_like_count(goods_no):
    try:
        url = "https://like.musinsa.com/like/api/v2/liketypes/goods/counts"
        obj = json.loads(http_post_json(url, {"relationIds": [str(goods_no)]}))
        items = obj.get("contents", {}).get("items", []) if isinstance(obj, dict) else []
        for item in items:
            if str(item.get("relationId")) == str(goods_no):
                return to_int(item.get("count"))
    except Exception:
        pass
    return None


# -----------------------------
# 이력
# -----------------------------
def load_history_index():
    index = {}
    if not HISTORY_FILE.exists():
        return index

    try:
        with HISTORY_FILE.open("r", encoding="utf-8-sig", newline="") as f:
            for row in csv.DictReader(f):
                goods_no = str(row.get("goods_no", "")).strip()
                if goods_no:
                    index[goods_no] = row
    except Exception:
        pass
    return index


def append_history(rows):
    exists = HISTORY_FILE.exists()
    with HISTORY_FILE.open("a", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=HISTORY_FIELDS)
        if not exists:
            w.writeheader()
        for data in rows:
            w.writerow({k: data.get(k, "") for k in HISTORY_FIELDS})



# -----------------------------
# 브랜드 자동 발견 / 자동 업데이트
# -----------------------------
def _read_lines(path):
    if not path.exists():
        return []
    values = []
    seen = set()
    for line in path.read_text(encoding="utf-8-sig", errors="ignore").splitlines():
        v = line.strip()
        if v and v not in seen:
            values.append(v)
            seen.add(v)
    return values


def _write_lines(path, values):
    cleaned = []
    seen = set()
    for value in values:
        v = str(value).strip()
        if v and v not in seen:
            cleaned.append(v)
            seen.add(v)
    path.write_text("\n".join(cleaned) + ("\n" if cleaned else ""), encoding="utf-8")


def load_watchlist():
    return _read_lines(WATCHLIST_FILE)


def save_watchlist(values):
    _write_lines(WATCHLIST_FILE, values[:MAX_WATCHLIST])


def load_brands():
    return _read_lines(BRANDS_FILE)


def save_brands(values):
    _write_lines(BRANDS_FILE, values)


def musinsa_search_json(url, keyword, timeout=15, retries=2):
    last_error = None
    referer = (
        "https://www.musinsa.com/search/musinsa/integration?type=popular&q="
        + urllib.parse.quote(keyword)
    )
    for attempt in range(retries + 1):
        try:
            req = urllib.request.Request(
                url,
                headers={
                    "User-Agent": USER_AGENT,
                    "Accept": "application/json, text/plain, */*",
                    "Accept-Language": "ko-KR,ko;q=0.9,en;q=0.8",
                    "Referer": referer,
                    "Origin": "https://www.musinsa.com",
                    "X-Musinsa-App": "MusinsaWeb",
                    "Cache-Control": "no-cache",
                },
            )
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                raw = resp.read()
                charset = resp.headers.get_content_charset() or "utf-8"
                return json.loads(raw.decode(charset, errors="replace"))
        except Exception as e:
            last_error = e
            if attempt < retries:
                time.sleep(0.8 * (attempt + 1))
    raise last_error


def search_brand_products(brand_name, max_pages=50):
    """무신사 검색 API를 사용해 정확히 같은 brandName의 상품을 수집합니다."""
    brand_name = brand_name.strip()
    if not brand_name:
        return []

    keyword = urllib.parse.quote(brand_name)
    count_url = (
        "https://api.musinsa.com/api2/sc/v2/search/tab/count"
        f"?gf=A&keyword={keyword}&sendLog=true"
    )
    count_data = musinsa_search_json(count_url, brand_name)
    total = to_int(
        (((count_data or {}).get("data") or {}).get("goods") or {}).get("all")
    ) or 0
    if total <= 0:
        return []

    page_size = 60
    pages = min(max_pages, max(1, (total + page_size - 1) // page_size))
    results = []
    seen = set()
    target = brand_name.casefold().strip()

    for page in range(1, pages + 1):
        url = (
            "https://api.musinsa.com/api2/dp/v1/plp/goods"
            f"?gf=A&keyword={keyword}&sortCode=NEW&page={page}&size={page_size}&caller=SEARCH"
        )
        data = musinsa_search_json(url, brand_name)
        items = (((data or {}).get("data") or {}).get("list") or [])
        if not isinstance(items, list) or not items:
            break

        for item in items:
            if not isinstance(item, dict):
                continue
            item_brand = str(item.get("brandName") or "").strip()
            if item_brand.casefold() != target:
                continue
            goods_no = item.get("goodsNo")
            if goods_no is None:
                continue
            goods_no = str(goods_no)
            if goods_no in seen:
                continue
            seen.add(goods_no)
            results.append({
                "goods_no": goods_no,
                "brand_name": item_brand,
                "product_name": item.get("goodsName") or "",
                "normal_price": to_int(item.get("normalPrice")),
                "current_price": to_int(item.get("price")),
                "sale_rate": to_int(item.get("saleRate")),
                "review_count": to_int(item.get("reviewCount")),
                "rating": item.get("reviewScore"),
                "availability": "OutOfStock" if item.get("isSoldOut") else "InStock",
            })
        time.sleep(0.25)

    return results


def append_new_product_log(rows):
    if not rows:
        return
    fields = [
        "first_seen_at", "brand_name", "goods_no", "product_name",
        "normal_price", "current_price", "sale_rate",
    ]
    exists = NEW_PRODUCTS_FILE.exists()
    with NEW_PRODUCTS_FILE.open("a", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        if not exists:
            w.writeheader()
        for row in rows:
            w.writerow({k: row.get(k, "") for k in fields})


def discover_and_merge_brands(brands=None):
    brands = brands if brands is not None else load_brands()
    current = load_watchlist()
    current_set = set(current)
    new_rows = []
    found_by_brand = {}

    for brand in brands:
        products = search_brand_products(brand)
        found_by_brand[brand] = products
        for p in products:
            goods_no = str(p["goods_no"])
            if goods_no not in current_set and len(current) < MAX_WATCHLIST:
                current.append(goods_no)
                current_set.add(goods_no)
                new_rows.append({
                    "first_seen_at": datetime.now().astimezone().isoformat(timespec="seconds"),
                    **p,
                })

    save_watchlist(current)
    append_new_product_log(new_rows)
    return current, new_rows, found_by_brand


def write_snapshot(rows):
    SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    path = SNAPSHOT_DIR / f"musinsa_snapshot_{stamp}.csv"
    fields = [
        "checked_at", "goods_no", "brand_name", "product_name",
        "purchase_total", "delta_purchase", "normal_price", "current_price",
        "sale_rate", "simple_gmv", "delta_gmv", "page_view_total",
        "review_count", "rating", "like_count", "availability", "product_url", "errors",
    ]
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for row in rows:
            w.writerow({k: row.get(k, "") for k in fields})
    return path


def append_daily_summary(rows, new_product_count=0):
    valid_purchase = [r for r in rows if isinstance(r.get("purchase_total"), int)]
    valid_gmv = [r for r in rows if isinstance(r.get("simple_gmv"), int)]
    valid_delta = [r for r in rows if isinstance(r.get("delta_purchase"), int)]
    valid_delta_gmv = [r for r in rows if isinstance(r.get("delta_gmv"), int)]
    fields = [
        "checked_at", "date", "product_count", "purchase_total_sum",
        "simple_gmv_sum", "delta_purchase_sum", "delta_gmv_sum",
        "new_products",
    ]
    row = {
        "checked_at": datetime.now().astimezone().isoformat(timespec="seconds"),
        "date": datetime.now().astimezone().date().isoformat(),
        "product_count": len(rows),
        "purchase_total_sum": sum(r["purchase_total"] for r in valid_purchase),
        "simple_gmv_sum": sum(r["simple_gmv"] for r in valid_gmv),
        "delta_purchase_sum": sum(r["delta_purchase"] for r in valid_delta),
        "delta_gmv_sum": sum(r["delta_gmv"] for r in valid_delta_gmv),
        "new_products": new_product_count,
    }
    exists = DAILY_SUMMARY_FILE.exists()
    with DAILY_SUMMARY_FILE.open("a", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        if not exists:
            w.writeheader()
        w.writerow(row)
    return row


def auto_log(message):
    stamp = datetime.now().astimezone().isoformat(timespec="seconds")
    with AUTO_LOG_FILE.open("a", encoding="utf-8") as f:
        f.write(f"[{stamp}] {message}\n")


def run_auto_update():
    auto_log("자동 업데이트 시작")
    try:
        brands = load_brands()
        watchlist, new_rows, _ = discover_and_merge_brands(brands)
        if not watchlist:
            auto_log("조회할 상품번호가 없습니다. musinsa_watchlist.txt 또는 musinsa_brands.txt를 설정하세요.")
            return 0

        history = load_history_index()
        rows = []
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {
                executor.submit(query_goods, goods_no, history.get(goods_no)): goods_no
                for goods_no in watchlist[:MAX_WATCHLIST]
            }
            for fut in as_completed(futures):
                goods_no = futures[fut]
                try:
                    rows.append(fut.result())
                except Exception as e:
                    auto_log(f"{goods_no} 조회 실패: {e}")

        if rows:
            append_history(rows)
            snapshot = write_snapshot(rows)
            summary = append_daily_summary(rows, len(new_rows))
            auto_log(
                f"완료: {len(rows)}개 조회 / 신규 {len(new_rows)}개 / "
                f"구매수 증가 {summary['delta_purchase_sum']} / 스냅샷 {snapshot.name}"
            )
        return 0
    except Exception as e:
        auto_log(f"자동 업데이트 실패: {e}")
        return 1

# -----------------------------
# 조회
# -----------------------------
def query_goods(goods_no, previous=None):
    product_url = f"https://www.musinsa.com/products/{goods_no}"
    errors = []

    stat = {"purchase_total": None, "page_view_total": None}
    try:
        stat = fetch_stat(goods_no)
    except Exception as e:
        errors.append(f"stat: {e}")

    page_info = {}
    try:
        page_info = parse_product_page(http_get(product_url), goods_no)
    except Exception as e:
        errors.append(f"page: {e}")

    review = fetch_review_summary(goods_no)
    like_count = fetch_like_count(goods_no)

    current_price = page_info.get("current_price")
    purchase_total = stat.get("purchase_total")
    simple_gmv = (
        purchase_total * current_price
        if purchase_total is not None and current_price is not None
        else None
    )

    result = {
        "checked_at": datetime.now().astimezone().isoformat(timespec="seconds"),
        "goods_no": str(goods_no),
        "product_name": page_info.get("product_name"),
        "brand_name": page_info.get("brand_name"),
        "purchase_total": purchase_total,
        "page_view_total": stat.get("page_view_total"),
        "normal_price": page_info.get("normal_price"),
        "current_price": current_price,
        "sale_rate": page_info.get("sale_rate"),
        "review_count": (
            review.get("review_count")
            if review.get("review_count") is not None
            else page_info.get("review_count")
        ),
        "rating": (
            review.get("rating")
            if review.get("rating") is not None
            else page_info.get("rating")
        ),
        "like_count": like_count,
        "availability": page_info.get("availability"),
        "simple_gmv": simple_gmv,
        "product_url": product_url,
        "errors": "; ".join(errors),
        "delta_purchase": None,
        "delta_gmv": None,
        "previous_checked_at": None,
    }

    if previous:
        prev_purchase = to_int(previous.get("purchase_total"))
        if prev_purchase is not None and purchase_total is not None:
            result["delta_purchase"] = purchase_total - prev_purchase
            if current_price is not None:
                result["delta_gmv"] = result["delta_purchase"] * current_price
        result["previous_checked_at"] = previous.get("checked_at")

    return result


def extract_goods_numbers(raw_text):
    """
    다음을 모두 지원:
    7024843
    7024843, 7024844
    https://www.musinsa.com/products/7024843
    여러 줄 혼합
    """
    found = re.findall(r"(?:products/)?(\d{5,})", raw_text)
    result = []
    seen = set()
    for x in found:
        if x not in seen:
            result.append(x)
            seen.add(x)
    return result


# -----------------------------
# GUI
# -----------------------------
class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title(APP_TITLE)
        self.geometry("1500x820")
        self.minsize(1100, 650)

        self.history_index = load_history_index()
        self.results = []
        self.total_target = 0
        self.done_count = 0
        self.stop_requested = False

        self.status_var = tk.StringVar(value="상품번호를 붙여넣고 [일괄 조회]를 누르세요.")
        self.summary_var = tk.StringVar(value="")

        outer = ttk.Frame(self, padding=12)
        outer.pack(fill="both", expand=True)

        ttk.Label(
            outer,
            text=APP_TITLE,
            font=("맑은 고딕", 18, "bold"),
        ).pack(anchor="w")

        ttk.Label(
            outer,
            text="상품번호/상품 URL을 여러 개 붙여넣을 수 있습니다. 중복 상품번호는 자동 제거됩니다.",
        ).pack(anchor="w", pady=(3, 8))

        top = ttk.Frame(outer)
        top.pack(fill="x")

        self.input_text = tk.Text(
            top, height=6, wrap="word",
            font=("Consolas", 10),
            padx=8, pady=8,
        )
        self.input_text.pack(side="left", fill="x", expand=True)

        btnbox = ttk.Frame(top)
        btnbox.pack(side="left", fill="y", padx=(8, 0))

        self.lookup_btn = ttk.Button(btnbox, text="일괄 조회", command=self.start_lookup)
        self.lookup_btn.pack(fill="x")

        self.stop_btn = ttk.Button(btnbox, text="중지", command=self.request_stop, state="disabled")
        self.stop_btn.pack(fill="x", pady=(6, 0))

        ttk.Button(btnbox, text="입력 지우기", command=lambda: self.input_text.delete("1.0", "end")).pack(fill="x", pady=(6, 0))
        ttk.Button(btnbox, text="CSV 불러오기", command=self.load_csv_goods).pack(fill="x", pady=(6, 0))
        ttk.Button(btnbox, text="결과 CSV 저장", command=self.export_csv).pack(fill="x", pady=(6, 0))
        ttk.Button(btnbox, text="조회이력 열기", command=self.open_history).pack(fill="x", pady=(6, 0))

        tools = ttk.LabelFrame(outer, text="자동 추적", padding=8)
        tools.pack(fill="x", pady=(10, 0))

        left_auto = ttk.Frame(tools)
        left_auto.pack(side="left", fill="both", expand=True)
        ttk.Label(left_auto, text="추적 브랜드 (무신사 표시명과 동일하게, 한 줄에 하나)").pack(anchor="w")
        self.brand_text = tk.Text(left_auto, height=3, wrap="word", font=("맑은 고딕", 9))
        self.brand_text.pack(fill="x", pady=(3, 0))
        if BRANDS_FILE.exists():
            self.brand_text.insert("1.0", BRANDS_FILE.read_text(encoding="utf-8-sig", errors="ignore"))

        auto_btns = ttk.Frame(tools)
        auto_btns.pack(side="left", padx=(8, 0), fill="y")
        ttk.Button(auto_btns, text="자동조회 목록 저장", command=self.save_watchlist_from_input).pack(fill="x")
        ttk.Button(auto_btns, text="자동조회 목록 불러오기", command=self.load_watchlist_to_input).pack(fill="x", pady=(5, 0))
        ttk.Button(auto_btns, text="자동추가 브랜드 저장", command=self.save_brands_from_input).pack(fill="x", pady=(5, 0))
        ttk.Button(auto_btns, text="지금 새 상품 찾기", command=self.discover_brands_now).pack(fill="x", pady=(5, 0))

        self.progress = ttk.Progressbar(outer, mode="determinate")
        self.progress.pack(fill="x", pady=(10, 4))

        ttk.Label(outer, textvariable=self.status_var).pack(anchor="w")
        ttk.Label(
            outer,
            textvariable=self.summary_var,
            font=("맑은 고딕", 11, "bold"),
        ).pack(anchor="w", pady=(3, 8))

        table_frame = ttk.Frame(outer)
        table_frame.pack(fill="both", expand=True)

        columns = (
            "goods_no", "brand_name", "product_name",
            "purchase_total", "delta_purchase",
            "normal_price", "current_price", "sale_rate",
            "simple_gmv", "delta_gmv",
            "page_view_total", "review_count", "rating",
            "like_count", "availability", "errors",
        )

        headings = {
            "goods_no": "상품번호",
            "brand_name": "브랜드",
            "product_name": "상품명",
            "purchase_total": "누적구매수",
            "delta_purchase": "이전대비",
            "normal_price": "정상가",
            "current_price": "현재가",
            "sale_rate": "할인율",
            "simple_gmv": "단순 누적 GMV",
            "delta_gmv": "증가 GMV",
            "page_view_total": "누적조회수",
            "review_count": "리뷰수",
            "rating": "평점",
            "like_count": "좋아요",
            "availability": "상태",
            "errors": "경고",
        }

        widths = {
            "goods_no": 85,
            "brand_name": 120,
            "product_name": 260,
            "purchase_total": 90,
            "delta_purchase": 80,
            "normal_price": 90,
            "current_price": 90,
            "sale_rate": 70,
            "simple_gmv": 120,
            "delta_gmv": 110,
            "page_view_total": 100,
            "review_count": 75,
            "rating": 60,
            "like_count": 75,
            "availability": 80,
            "errors": 220,
        }

        self.tree = ttk.Treeview(
            table_frame,
            columns=columns,
            show="headings",
        )

        for c in columns:
            self.tree.heading(c, text=headings[c], command=lambda col=c: self.sort_by(col, False))
            self.tree.column(c, width=widths[c], anchor="center" if c not in ("product_name", "brand_name", "errors") else "w")

        vsb = ttk.Scrollbar(table_frame, orient="vertical", command=self.tree.yview)
        hsb = ttk.Scrollbar(table_frame, orient="horizontal", command=self.tree.xview)
        self.tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)

        self.tree.grid(row=0, column=0, sticky="nsew")
        vsb.grid(row=0, column=1, sticky="ns")
        hsb.grid(row=1, column=0, sticky="ew")
        table_frame.rowconfigure(0, weight=1)
        table_frame.columnconfigure(0, weight=1)

        note = (
            "※ 단순 누적 GMV = 현재가 × 누적 구매수. 실제 과거 결제액/정산매출과 동일하지 않습니다. "
            "이전대비 값은 이 프로그램으로 과거에 조회한 이력이 있을 때 표시됩니다."
        )
        ttk.Label(outer, text=note, wraplength=1450).pack(anchor="w", pady=(8, 0))

    def save_watchlist_from_input(self):
        goods = extract_goods_numbers(self.input_text.get("1.0", "end"))
        if not goods:
            messagebox.showwarning("자동조회 목록", "저장할 상품번호가 없습니다.")
            return
        save_watchlist(goods)
        self.status_var.set(f"자동조회 목록 {len(goods):,}개 저장 완료")

    def load_watchlist_to_input(self):
        goods = load_watchlist()
        self.input_text.delete("1.0", "end")
        self.input_text.insert("1.0", "\n".join(goods))
        self.status_var.set(f"자동조회 목록 {len(goods):,}개 불러옴")

    def save_brands_from_input(self):
        brands = [x.strip() for x in self.brand_text.get("1.0", "end").splitlines() if x.strip()]
        if not brands:
            messagebox.showwarning("브랜드", "저장할 브랜드명을 입력하세요.")
            return
        save_brands(brands)
        self.status_var.set(f"자동추가 브랜드 {len(brands):,}개 저장 완료")

    def discover_brands_now(self):
        brands = [x.strip() for x in self.brand_text.get("1.0", "end").splitlines() if x.strip()]
        if not brands:
            messagebox.showwarning("브랜드", "먼저 브랜드명을 입력하세요.")
            return
        save_brands(brands)
        self.status_var.set("브랜드 신상품 검색 중...")

        def worker():
            try:
                watchlist, new_rows, found = discover_and_merge_brands(brands)
                self.after(0, lambda: self._finish_discovery(watchlist, new_rows, found))
            except Exception as e:
                self.after(0, lambda: messagebox.showerror("브랜드 검색 오류", str(e)))
                self.after(0, lambda: self.status_var.set("브랜드 검색 실패"))

        threading.Thread(target=worker, daemon=True).start()

    def _finish_discovery(self, watchlist, new_rows, found):
        self.input_text.delete("1.0", "end")
        self.input_text.insert("1.0", "\n".join(watchlist))
        found_count = sum(len(v) for v in found.values())
        self.status_var.set(
            f"브랜드 검색 완료: 검색 일치 {found_count:,}개 / 신규 자동추가 {len(new_rows):,}개 / 전체 목록 {len(watchlist):,}개"
        )
        if new_rows:
            messagebox.showinfo("신규 상품", f"새 상품번호 {len(new_rows):,}개를 자동조회 목록에 추가했습니다.")

    def start_lookup(self):
        raw = self.input_text.get("1.0", "end")
        goods_numbers = extract_goods_numbers(raw)
        if not goods_numbers:
            messagebox.showwarning("입력 필요", "상품번호 또는 무신사 상품 URL을 입력하세요.")
            return

        self.results = []
        self.total_target = len(goods_numbers)
        self.done_count = 0
        self.stop_requested = False

        for item in self.tree.get_children():
            self.tree.delete(item)

        self.progress["maximum"] = self.total_target
        self.progress["value"] = 0
        self.lookup_btn.configure(state="disabled")
        self.stop_btn.configure(state="normal")
        self.status_var.set(f"{self.total_target:,}개 상품 조회 시작")
        self.summary_var.set("")

        thread = threading.Thread(
            target=self.lookup_all_worker,
            args=(goods_numbers,),
            daemon=True,
        )
        thread.start()

    def request_stop(self):
        self.stop_requested = True
        self.status_var.set("중지 요청됨 — 이미 시작된 요청만 마무리합니다.")

    def lookup_all_worker(self, goods_numbers):
        batch_for_history = []

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {}
            for goods_no in goods_numbers:
                if self.stop_requested:
                    break
                prev = self.history_index.get(goods_no)
                fut = executor.submit(query_goods, goods_no, prev)
                futures[fut] = goods_no

            for fut in as_completed(futures):
                if self.stop_requested:
                    # 이미 실행중인 요청의 결과는 받아서 표시
                    pass

                goods_no = futures[fut]
                try:
                    result = fut.result()
                except Exception as e:
                    result = {
                        "checked_at": datetime.now().astimezone().isoformat(timespec="seconds"),
                        "goods_no": goods_no,
                        "product_name": None,
                        "brand_name": None,
                        "purchase_total": None,
                        "page_view_total": None,
                        "normal_price": None,
                        "current_price": None,
                        "sale_rate": None,
                        "review_count": None,
                        "rating": None,
                        "like_count": None,
                        "availability": None,
                        "simple_gmv": None,
                        "delta_purchase": None,
                        "delta_gmv": None,
                        "product_url": f"https://www.musinsa.com/products/{goods_no}",
                        "errors": str(e),
                    }

                self.results.append(result)
                batch_for_history.append(result)
                self.done_count += 1
                self.after(0, self.add_result_row, result)
                self.after(0, self.update_progress)

        if batch_for_history:
            try:
                append_history(batch_for_history)
                for r in batch_for_history:
                    self.history_index[str(r["goods_no"])] = {
                        k: r.get(k, "") for k in HISTORY_FIELDS
                    }
            except Exception as e:
                self.after(0, lambda: messagebox.showwarning("이력 저장 오류", str(e)))

        self.after(0, self.finish_lookup)

    def add_result_row(self, r):
        availability_map = {
            "InStock": "판매중",
            "OutOfStock": "품절",
            "PreOrder": "예약판매",
        }
        availability = availability_map.get(
            r.get("availability"),
            r.get("availability") or "-"
        )

        vals = (
            r.get("goods_no"),
            r.get("brand_name") or "-",
            r.get("product_name") or "-",
            fmt_num(r.get("purchase_total")),
            fmt_signed(r.get("delta_purchase")),
            fmt_won(r.get("normal_price")),
            fmt_won(r.get("current_price")),
            f"{fmt_num(r.get('sale_rate'))}%" if r.get("sale_rate") is not None else "-",
            fmt_won(r.get("simple_gmv")),
            fmt_won(r.get("delta_gmv")),
            fmt_num(r.get("page_view_total")),
            fmt_num(r.get("review_count")),
            r.get("rating") if r.get("rating") is not None else "-",
            fmt_num(r.get("like_count")),
            availability,
            r.get("errors") or "",
        )
        self.tree.insert("", "end", values=vals)

    def update_progress(self):
        self.progress["value"] = self.done_count
        self.status_var.set(f"조회 중... {self.done_count:,} / {self.total_target:,}")
        self.update_summary()

    def update_summary(self):
        total_purchase = sum(
            r["purchase_total"]
            for r in self.results
            if isinstance(r.get("purchase_total"), int)
        )
        total_gmv = sum(
            r["simple_gmv"]
            for r in self.results
            if isinstance(r.get("simple_gmv"), int)
        )
        total_delta = sum(
            r["delta_purchase"]
            for r in self.results
            if isinstance(r.get("delta_purchase"), int)
        )
        total_delta_gmv = sum(
            r["delta_gmv"]
            for r in self.results
            if isinstance(r.get("delta_gmv"), int)
        )

        self.summary_var.set(
            f"조회 {len(self.results):,}개 | "
            f"누적 구매수 합계 {total_purchase:,} | "
            f"단순 누적 GMV 합계 {total_gmv:,}원 | "
            f"이전 대비 구매수 {total_delta:+,} | "
            f"증가 GMV {total_delta_gmv:+,}원"
        )

    def finish_lookup(self):
        self.lookup_btn.configure(state="normal")
        self.stop_btn.configure(state="disabled")
        self.update_summary()

        failed = sum(
            1 for r in self.results
            if r.get("purchase_total") is None
        )
        if self.stop_requested:
            self.status_var.set(f"중지됨 — {len(self.results):,}개 결과 수집")
        else:
            self.status_var.set(
                f"완료 — {len(self.results):,}개 조회, 구매수 미확인 {failed:,}개"
            )

    def export_csv(self):
        if not self.results:
            messagebox.showinfo("저장", "먼저 상품을 조회하세요.")
            return

        default_name = f"musinsa_lookup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        path = filedialog.asksaveasfilename(
            title="결과 CSV 저장",
            defaultextension=".csv",
            initialfile=default_name,
            filetypes=[("CSV 파일", "*.csv")],
        )
        if not path:
            return

        fields = [
            "checked_at", "goods_no", "brand_name", "product_name",
            "purchase_total", "delta_purchase",
            "normal_price", "current_price", "sale_rate",
            "simple_gmv", "delta_gmv",
            "page_view_total", "review_count", "rating",
            "like_count", "availability", "product_url", "errors",
        ]

        with open(path, "w", encoding="utf-8-sig", newline="") as f:
            w = csv.DictWriter(f, fieldnames=fields)
            w.writeheader()
            for r in self.results:
                w.writerow({k: r.get(k, "") for k in fields})

        messagebox.showinfo("저장 완료", path)

    def load_csv_goods(self):
        path = filedialog.askopenfilename(
            title="CSV에서 상품번호 불러오기",
            filetypes=[("CSV 파일", "*.csv"), ("모든 파일", "*.*")],
        )
        if not path:
            return

        candidates = []
        try:
            with open(path, "r", encoding="utf-8-sig", newline="") as f:
                reader = csv.reader(f)
                for row in reader:
                    for cell in row:
                        candidates.extend(extract_goods_numbers(str(cell)))
        except UnicodeDecodeError:
            with open(path, "r", encoding="cp949", newline="") as f:
                reader = csv.reader(f)
                for row in reader:
                    for cell in row:
                        candidates.extend(extract_goods_numbers(str(cell)))

        unique = []
        seen = set()
        for x in candidates:
            if x not in seen:
                unique.append(x)
                seen.add(x)

        if not unique:
            messagebox.showwarning("불러오기", "CSV에서 상품번호를 찾지 못했습니다.")
            return

        self.input_text.delete("1.0", "end")
        self.input_text.insert("1.0", "\n".join(unique))
        self.status_var.set(f"CSV에서 상품번호 {len(unique):,}개 불러옴")

    def open_history(self):
        if not HISTORY_FILE.exists():
            messagebox.showinfo("조회이력", "아직 저장된 이력이 없습니다.")
            return
        try:
            import os
            os.startfile(str(HISTORY_FILE))
        except Exception:
            messagebox.showinfo("조회이력 위치", str(HISTORY_FILE))

    def sort_by(self, col, descending):
        # 화면 표시 문자열에서 숫자 정렬 보정
        def parse(v):
            s = str(v).replace(",", "").replace("원", "").replace("%", "").strip()
            if s in ("", "-"):
                return (1, 0)
            try:
                return (0, float(s))
            except Exception:
                return (0, s.lower())

        data = [(self.tree.set(item, col), item) for item in self.tree.get_children("")]
        data.sort(key=lambda x: parse(x[0]), reverse=descending)

        for idx, (_, item) in enumerate(data):
            self.tree.move(item, "", idx)

        self.tree.heading(
            col,
            command=lambda: self.sort_by(col, not descending),
        )


if __name__ == "__main__":
    if "--auto" in sys.argv:
        raise SystemExit(run_auto_update())
    App().mainloop()
