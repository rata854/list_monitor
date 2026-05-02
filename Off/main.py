import os
import re
import sys
import random
import time
import traceback
from datetime import datetime
from pathlib import Path

import pytz
from curl_cffi import requests
from dotenv import load_dotenv
from supabase import create_client

load_dotenv(Path(__file__).parents[2] / ".secrets" / "github_actions.env")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "ja,en-US;q=0.7,en;q=0.3",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}

CONFIG = {
    "DEFAULT_FEE": 600,
    "FETCH_PAGES": 5,
    "MAX_PRODUCTS": 200,
    "SLEEP_LISTING_MIN": 3.0,
    "SLEEP_LISTING_MAX": 5.0,
    "SLEEP_DETAIL_MIN": 2.0,
    "SLEEP_DETAIL_MAX": 4.0,
    "SEARCH_PATH": "/search/?s=1&exso=1&min=4000&rank=3&rank=4&rank=5",
    "PRODUCT_PATH": "/product/",
    # CSS selectors (listing page)
    "CARD": "itemcolmn_item",
    "BRAND": r'<div class="item-brand-name">([^<]*)</div>',
    "NAME": r'<div class="item-name">([^<]*)</div>',
    "CODE": r'<div class="item-code">([^<]*)</div>',
    "PRICE": r'class="font-en item-price-en"[^>]*>\s*([\d,]+)',
    "IMAGE": r'<img[^>]+src="([^"]+)"[^>]*data-object-fit',
    "URL": r'<a href="([^"]+/product/\d+/[^"]*)"',
    "ID": r'data-goodsno="(\d+)"',
    # detail page
    "DETAIL_NUM": r'class="product-detail-num"[^>]*>([\s\S]*?)</[^>]+>',
    "DETAIL_NAME": r'class="product-detail-name"[^>]*>([\s\S]*?)</[^>]+>',
    "DETAIL_DESC": r'id="panel1"[^>]*>([\s\S]*?)</div>',
}


def random_sleep(min_sec, max_sec):
    time.sleep(random.uniform(min_sec, max_sec))


def make_session():
    session = requests.Session(impersonate="chrome120")
    session.headers.update(HEADERS)
    return session


def fetch_listing_page(session, base_url, page):
    url = f"{base_url}{CONFIG['SEARCH_PATH']}&p={page}"
    try:
        resp = session.get(url, timeout=15)
        if resp.status_code != 200:
            print(f"[WARN] 一覧取得 HTTP {resp.status_code} (page={page})", flush=True)
            return []
        html = resp.text

        products = []
        seen_ids = set()
        for card in re.split(r'(?=<div class="itemcolmn_item)', html):
            if 'itemcolmn_item' not in card:
                continue

            id_m = re.search(CONFIG["ID"], card)
            if not id_m:
                continue
            product_id = id_m.group(1)
            if product_id in seen_ids:
                continue
            seen_ids.add(product_id)

            url_m = re.search(CONFIG["URL"], card)
            img_m = re.search(CONFIG["IMAGE"], card)
            brand_m = re.search(CONFIG["BRAND"], card)
            name_m = re.search(CONFIG["NAME"], card)
            code_m = re.search(CONFIG["CODE"], card)
            price_m = re.search(CONFIG["PRICE"], card)

            if not (url_m and name_m and price_m):
                continue

            try:
                price = int(price_m.group(1).replace(",", ""))
            except ValueError:
                continue

            products.append({
                "id": product_id,
                "url": url_m.group(1),
                "image": img_m.group(1) if img_m else "",
                "brand": brand_m.group(1).strip() if brand_m else "",
                "name": name_m.group(1).strip() if name_m else "",
                "code": code_m.group(1).strip() if code_m else "",
                "price": price,
            })

        return products
    except Exception as e:
        print(f"[WARN] 一覧取得エラー (page={page}): {e}", flush=True)
        return []


def fetch_description(session, base_url, product_id):
    url = f"{base_url}{CONFIG['PRODUCT_PATH']}{product_id}/"
    try:
        resp = session.get(url, timeout=15)
        if resp.status_code != 200:
            return ""
        html = resp.text
        num_m = re.search(CONFIG["DETAIL_NUM"], html)
        dname_m = re.search(CONFIG["DETAIL_NAME"], html)
        desc_m = re.search(CONFIG["DETAIL_DESC"], html)
        detail_num = re.sub(r"<[^>]+>", "", num_m.group(1)).strip() if num_m else ""
        detail_name = re.sub(r"<[^>]+>", "", dname_m.group(1)).strip() if dname_m else ""
        desc_text = re.sub(r"\s+", " ", re.sub(r"<[^>]+>", "", desc_m.group(1))).strip() if desc_m else ""
        return f"{detail_num}_{detail_name}:{desc_text}"[:1500]
    except Exception as e:
        print(f"[WARN] 説明取得エラー id={product_id}: {e}", flush=True)
        return ""


def matches(product, watch):
    code_upper = watch["product_code_out"].upper()
    item_code_upper = product["code"].upper()
    name_upper = f"{product['brand']} {product['name']}".upper()

    if code_upper not in item_code_upper and code_upper not in name_upper:
        return False

    must_kw = watch.get("must_keywords") or ""
    for kw in must_kw.split():
        if kw.upper() not in name_upper:
            return False

    return (product["price"] + CONFIG["DEFAULT_FEE"]) <= float(watch["final_price"])


def load_watch_list(supabase):
    all_resp = supabase.table("product_list").select("asin_sell, auto_flag", count="exact").execute()
    print(f"  product_list 総行数: {all_resp.count}", flush=True)

    resp = supabase.table("product_list").select(
        "asin_sell, product_code_out, must_keywords, final_price, auto_flag"
    ).eq("auto_flag", True).execute()
    print(f"  auto_flag=true の行数: {len(resp.data)}", flush=True)

    filtered = [r for r in resp.data if r.get("asin_sell") and r.get("product_code_out") and r.get("final_price")]
    skipped = len(resp.data) - len(filtered)
    if skipped:
        print(f"  必須項目欠損でスキップ: {skipped}件", flush=True)
    return filtered


def insert_hits(supabase, rows):
    if not rows:
        return 0
    supabase.table("scrape_hits").upsert(rows, on_conflict="asin,url", ignore_duplicates=True).execute()
    return len(rows)


def log_execution(supabase_client, *, task_name, status, started_at,
                  severity=None, error_type=None, error_message=None,
                  stack_trace=None, debug_info=None):
    try:
        completed_at = datetime.now(pytz.utc)
        duration_ms = int((completed_at - started_at).total_seconds() * 1000)
        if severity is None:
            severity = "info" if status == "success" else "error"
        supabase_client.table("execution_logs").insert({
            "source_type": "github_actions",
            "task_name": task_name,
            "status": status,
            "severity": severity,
            "started_at": started_at.isoformat(),
            "completed_at": completed_at.isoformat(),
            "duration_ms": duration_ms,
            "error_type": error_type,
            "error_message": error_message,
            "stack_trace": stack_trace,
            "debug_info": debug_info,
        }).execute()
    except Exception as e:
        print(f"[WARN] ログ書き込み失敗（無視）: {e}", flush=True)


def _run(supabase, now_jst):
    base_url = os.environ["OFF_BASE_URL"].rstrip("/")

    watch_list = load_watch_list(supabase)
    print(f"監視リスト件数: {len(watch_list)}", flush=True)
    if not watch_list:
        print("有効な監視対象がありません", flush=True)
        return {
            "status": "skipped",
            "exit_code": 0,
            "severity": "info",
            "debug_info": {"watch_list_count": 0},
        }

    session = make_session()
    all_products = []
    seen_ids = set()
    for page in range(1, CONFIG["FETCH_PAGES"] + 1):
        products = fetch_listing_page(session, base_url, page)
        new_products = [p for p in products if p["id"] not in seen_ids]
        for p in new_products:
            seen_ids.add(p["id"])
        all_products.extend(new_products)
        print(f"取得中: page={page} → {len(products)}件", flush=True)
        if not products:
            break
        random_sleep(CONFIG["SLEEP_LISTING_MIN"], CONFIG["SLEEP_LISTING_MAX"])

    if len(all_products) > CONFIG["MAX_PRODUCTS"]:
        all_products = all_products[:CONFIG["MAX_PRODUCTS"]]

    print(f"取得商品数（重複除去後）: {len(all_products)}", flush=True)

    if not all_products:
        print("[ERROR] 商品取得0件", flush=True)
        return {
            "status": "failure",
            "exit_code": 1,
            "error_type": "NoProductsFetched",
            "error_message": "商品取得0件",
            "debug_info": {
                "watch_list_count": len(watch_list),
                "products_fetched": 0,
                "pages_fetched": CONFIG["FETCH_PAGES"],
            },
        }

    today = now_jst.strftime("%Y-%m-%d")
    hits = []
    pushed = set()

    for product in all_products:
        matched_watches = [w for w in watch_list if matches(product, w)]
        if not matched_watches:
            continue

        description = fetch_description(session, base_url, product["id"])
        random_sleep(CONFIG["SLEEP_DETAIL_MIN"], CONFIG["SLEEP_DETAIL_MAX"])

        title = f"{product['brand']} {product['name']}".strip()
        for watch in matched_watches:
            key = f"{watch['asin_sell']}\t{product['url']}"
            if key in pushed:
                continue
            pushed.add(key)
            print(f"★ HIT: {title} / ¥{product['price']}", flush=True)
            hits.append({
                "asin":        watch["asin_sell"],
                "url":         product["url"],
                "date":        today,
                "price":       product["price"],
                "fee":         CONFIG["DEFAULT_FEE"],
                "image":       product["image"],
                "title":       title,
                "description": description,
                "mall":        "Off",
            })

    if not hits:
        print("条件に合う商品はありませんでした", flush=True)
        return {
            "status": "skipped",
            "exit_code": 0,
            "severity": "info",
            "debug_info": {
                "watch_list_count": len(watch_list),
                "products_fetched": len(all_products),
                "hits_count": 0,
            },
        }

    try:
        inserted = insert_hits(supabase, hits)
        print(f"scrape_hits に {inserted} 件書き込みました（重複は除外）", flush=True)
        return {
            "status": "success",
            "exit_code": 0,
            "debug_info": {
                "watch_list_count": len(watch_list),
                "products_fetched": len(all_products),
                "hits_count": inserted,
            },
        }
    except Exception as e:
        print(f"[ERROR] Supabase書き込み失敗: {e}", flush=True)
        return {
            "status": "failure",
            "exit_code": 1,
            "error_type": type(e).__name__,
            "error_message": str(e),
            "stack_trace": traceback.format_exc(),
            "debug_info": {
                "watch_list_count": len(watch_list),
                "products_fetched": len(all_products),
                "hits_count": len(hits),
            },
        }


def main():
    jst = pytz.timezone("Asia/Tokyo")
    now_jst = datetime.now(jst)
    hour = now_jst.hour
    started_at = datetime.now(pytz.utc)

    for key in ("SUPABASE_URL", "SUPABASE_SERVICE_ROLE_KEY", "OFF_BASE_URL"):
        if not os.environ.get(key):
            print(f"[ERROR] 環境変数 {key} が未設定", flush=True)
            sys.exit(1)

    try:
        supabase = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_SERVICE_ROLE_KEY"])
    except Exception as e:
        print(f"[ERROR] Supabase接続失敗: {e}", flush=True)
        sys.exit(1)

    if hour < 6 or hour >= 20:
        print(f"実行時間外のためスキップ ({hour}時 JST)", flush=True)
        log_execution(
            supabase,
            task_name="scrape_off",
            status="skipped",
            severity="info",
            started_at=started_at,
            debug_info={"hour_jst": hour},
        )
        sys.exit(0)

    try:
        result = _run(supabase, now_jst)
    except Exception as e:
        print(f"[ERROR] 予期しないエラー: {e}", flush=True)
        result = {
            "status": "failure",
            "exit_code": 1,
            "error_type": type(e).__name__,
            "error_message": str(e),
            "stack_trace": traceback.format_exc(),
            "debug_info": {},
        }

    log_execution(
        supabase,
        task_name="scrape_off",
        status=result["status"],
        severity=result.get("severity"),
        started_at=started_at,
        error_type=result.get("error_type"),
        error_message=result.get("error_message"),
        stack_trace=result.get("stack_trace"),
        debug_info=result.get("debug_info"),
    )
    sys.exit(result["exit_code"])


if __name__ == "__main__":
    main()
