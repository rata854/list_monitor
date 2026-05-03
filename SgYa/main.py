import os
import sys
import random
import re
import time
import traceback
from datetime import datetime
from urllib.parse import urlparse, urlencode, parse_qs, urlunparse

import pytz
from pathlib import Path
from dotenv import load_dotenv
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from supabase import create_client

load_dotenv(Path(__file__).parents[2] / ".secrets" / "github_actions.env")

CONFIG = {
    "DEFAULT_FEE": 0,
    "FETCH_PAGES": 5,
    "PAGE_LOAD_TIMEOUT": 15,
    "SLEEP_MIN": 2.0,
    "SLEEP_MAX": 4.0,
    # CSS selectors — listing page
    "ITEM_CARD": "div.item",
    "ITEM_LINK": "div.title a",
    "ITEM_NAME": "h3.product-name",
    "ITEM_PRICE": "p.price_teika span.text-red strong",
    "ITEM_IMAGE": "div.photo_box img",
}

SEARCH_PATHS = [
    "/search?category=8&search_word=&adult_s=2&ck=true"
    "&hendou=%E6%96%B0%E5%85%A5%E8%8D%B7&price=[4000,]"
    "&rankBy=modificationTime%3Adescending&sale_classified=%E4%B8%AD%E5%8F%A4",
]


def random_sleep(min_sec, max_sec):
    time.sleep(random.uniform(min_sec, max_sec))


def set_page_param(url, page):
    parsed = urlparse(url)
    params = parse_qs(parsed.query, keep_blank_values=True)
    params["page"] = [str(page)]
    new_query = urlencode({k: v[0] for k, v in params.items()})
    return urlunparse(parsed._replace(query=new_query))


def make_driver():
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")
    options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    )
    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=options)


def fetch_listing_page(driver, url):
    driver.get(url)
    print(f"[DEBUG] title={driver.title!r} current_url={driver.current_url!r} html_len={len(driver.page_source)}", flush=True)
    try:
        WebDriverWait(driver, CONFIG["PAGE_LOAD_TIMEOUT"]).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, CONFIG["ITEM_CARD"]))
        )
    except Exception:
        print(f"[DEBUG] WebDriverWait timeout — no '{CONFIG['ITEM_CARD']}' found", flush=True)
        return []

    soup = BeautifulSoup(driver.page_source, "html.parser")
    cards = soup.select(CONFIG["ITEM_CARD"])
    if not cards:
        return []

    base = os.environ["SGYA_BASE_URL"].rstrip("/")
    products = []
    for card in cards:
        try:
            a_tag = card.select_one(CONFIG["ITEM_LINK"])
            if not a_tag:
                continue
            href = a_tag.get("href", "")
            product_url = f"{base}{href}" if href.startswith("/") else href

            name_tag = card.select_one(CONFIG["ITEM_NAME"])
            name = name_tag.get_text(strip=True) if name_tag else ""

            price_tag = card.select_one(CONFIG["ITEM_PRICE"])
            price_text = price_tag.get_text(strip=True) if price_tag else "0"
            # "￥12,100 ～ ￥16,000" → 最小値を使用
            min_price_str = price_text.split("～")[0]
            price_digits = re.search(r"[\d,]+", min_price_str)
            price = int(price_digits.group().replace(",", "")) if price_digits else 0

            img_tag = card.select_one(CONFIG["ITEM_IMAGE"])
            image = img_tag.get("src", "") if img_tag else ""

            if product_url and name:
                products.append({"url": product_url, "name": name, "price": price, "image": image})
        except Exception as e:
            print(f"[WARN] カード解析エラー: {e}", flush=True)
            continue

    return products



def matches(product, watch):
    name_upper = product["name"].upper()
    code_upper = watch["product_code_out"].upper()
    if code_upper not in name_upper:
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
    base_url = os.environ["SGYA_BASE_URL"].rstrip("/")
    search_urls = [base_url + path for path in SEARCH_PATHS]

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

    all_products = []
    deduped = []
    driver = make_driver()
    try:
        for i, search_url in enumerate(search_urls):
            for page in range(1, CONFIG["FETCH_PAGES"] + 1):
                page_url = set_page_param(search_url, page)
                print(f"取得中: page={page} (path={i+1}/{len(search_urls)})", flush=True)
                products = fetch_listing_page(driver, page_url)
                all_products.extend(products)
                print(f"  → {len(products)}件", flush=True)
                if not products:
                    break
                random_sleep(CONFIG["SLEEP_MIN"], CONFIG["SLEEP_MAX"])

        seen = {}
        deduped = [p for p in all_products if not seen.get(p["url"]) and not seen.update({p["url"]: True})]
        print(f"取得商品数（重複除去後）: {len(deduped)}", flush=True)

        if not deduped:
            print("[ERROR] 商品カード取得0件（HTML構造変更の可能性）", flush=True)
            return {
                "status": "failure",
                "exit_code": 1,
                "error_type": "NoProductsFetched",
                "error_message": "商品カード取得0件",
                "debug_info": {
                    "watch_list_count": len(watch_list),
                    "products_fetched": 0,
                    "paths_count": len(SEARCH_PATHS),
                    "pages_per_path": CONFIG["FETCH_PAGES"],
                },
            }

        today = now_jst.strftime("%Y-%m-%d")
        hits = []
        pushed = set()

        for product in deduped:
            for watch in watch_list:
                if not matches(product, watch):
                    continue
                key = f"{watch['asin_sell']}\t{product['url']}"
                if key in pushed:
                    continue
                pushed.add(key)
                print(f"★ HIT: {product['name']} / ¥{product['price']}", flush=True)
                hits.append({
                    "asin": watch["asin_sell"],
                    "url": product["url"],
                    "date": today,
                    "price": product["price"],
                    "fee": CONFIG["DEFAULT_FEE"],
                    "image": product["image"],
                    "title": product["name"],
                    "description": "",
                    "mall": "SgYa",
                })

    finally:
        driver.quit()

    if not hits:
        print("条件に合う商品はありませんでした", flush=True)
        return {
            "status": "skipped",
            "exit_code": 0,
            "severity": "info",
            "debug_info": {
                "watch_list_count": len(watch_list),
                "products_fetched": len(deduped),
                "hits_count": 0,
                "paths_count": len(SEARCH_PATHS),
                "pages_per_path": CONFIG["FETCH_PAGES"],
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
                "products_fetched": len(deduped),
                "hits_count": inserted,
                "paths_count": len(SEARCH_PATHS),
                "pages_per_path": CONFIG["FETCH_PAGES"],
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
                "products_fetched": len(deduped),
                "hits_count": len(hits),
                "paths_count": len(SEARCH_PATHS),
                "pages_per_path": CONFIG["FETCH_PAGES"],
            },
        }


def main():
    jst = pytz.timezone("Asia/Tokyo")
    now_jst = datetime.now(jst)
    hour = now_jst.hour
    started_at = datetime.now(pytz.utc)

    for key in ("SUPABASE_URL", "SUPABASE_SERVICE_ROLE_KEY", "SGYA_BASE_URL"):
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
            task_name="scrape_sgya",
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
        task_name="scrape_sgya",
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
