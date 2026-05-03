import os
import re
import sys
import random
import time
import traceback
from datetime import datetime
from pathlib import Path

import pytz
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
from dotenv import load_dotenv
from supabase import create_client

load_dotenv(Path(__file__).parents[2] / ".secrets" / "github_actions.env")

CONFIG = {
    "PRICE_THRESHOLD": 10000,
    "PAGE_LOAD_TIMEOUT": 15,
    "SLEEP_MIN": 8.0,
    "SLEEP_MAX": 12.0,
    "SKIP_PATTERN1": "に一致する商品はありません",
    "SKIP_PATTERN2": "条件に一致する商品は見つかりませんでした",
    "PRODUCT_WAIT":    "li.Product",
    "PRODUCT_CARD":    r'(?=<li[^>]+class="Product[ ">])',
    "PRODUCT_ID":      r'data-auction-id="([^"]+)"',
    "PRODUCT_URL":     r'<a[^>]+class="[^"]*Product__imageLink[^"]*"[^>]+href="([^"]+)"',
    "PRODUCT_TITLE":   r'data-auction-title="([^"]+)"',
    "PRODUCT_IMG":     r'data-auction-img="([^"]+)"',
    "PRODUCT_PRICE":   r'class="[^"]*Product__priceValue[^"]*u-textRed[^"]*"[^>]*>([\d,]+)',
    "PRODUCT_POSTAGE": r'class="[^"]*Product__postage[^"]*"[^>]*>(.*?)</p>',
    "PRODUCT_RATING":  r'class="[^"]*Product__ratingValue[^"]*"[^>]*>([^<]+)',
    # "PRODUCT_TIME":  r'class="[^"]*Product__time[^"]*"[^>]*>([^<]+)',  # 残り時間（未使用）
}


def random_sleep(min_sec, max_sec):
    time.sleep(random.uniform(min_sec, max_sec))


def make_driver(headless=True):
    options = Options()
    if headless:
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


def is_skip_page(html):
    return CONFIG["SKIP_PATTERN1"] in html or CONFIG["SKIP_PATTERN2"] in html


def parse_postage(text):
    clean = re.sub(r"<[^>]+>", "", text).strip()
    if "送料無料" in clean:
        return 0
    m = re.search(r"[\d,]+", clean)
    if m:
        try:
            return int(m.group(0).replace(",", ""))
        except ValueError:
            pass
    return None


def fetch_products(driver, url):
    try:
        driver.get(url)
        try:
            WebDriverWait(driver, CONFIG["PAGE_LOAD_TIMEOUT"]).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, CONFIG["PRODUCT_WAIT"]))
            )
        except Exception:
            pass
        html = driver.page_source
        print(f"[DEBUG] title={driver.title!r} html_len={len(html)}", flush=True)

        if is_skip_page(html):
            return []

        products = []
        seen_ids = set()
        for card in re.split(CONFIG["PRODUCT_CARD"], html):
            if 'data-auction-id' not in card:
                continue

            id_m = re.search(CONFIG["PRODUCT_ID"], card)
            if not id_m:
                continue
            product_id = id_m.group(1)
            if product_id in seen_ids:
                continue
            seen_ids.add(product_id)

            url_m = re.search(CONFIG["PRODUCT_URL"], card)
            title_m = re.search(CONFIG["PRODUCT_TITLE"], card)
            img_m = re.search(CONFIG["PRODUCT_IMG"], card)
            price_m = re.search(CONFIG["PRODUCT_PRICE"], card)
            postage_m = re.search(CONFIG["PRODUCT_POSTAGE"], card, re.DOTALL)
            rating_m = re.search(CONFIG["PRODUCT_RATING"], card)

            if not (url_m and title_m and price_m):
                continue

            try:
                price = int(price_m.group(1).replace(",", ""))
            except ValueError:
                continue

            fee = parse_postage(postage_m.group(1)) if postage_m else None
            rating = rating_m.group(1).strip() if rating_m else ""

            products.append({
                "id": product_id,
                "url": url_m.group(1),
                "title": title_m.group(1),
                "image": img_m.group(1) if img_m else "",
                "price": price,
                "fee": fee,
                "seller_rating": rating,
            })

        return products
    except Exception as e:
        print(f"[WARN] 商品取得エラー: {e}", flush=True)
        return []


def matches(product, watch):
    code_upper = watch["product_code_out"].upper()
    title_upper = product["title"].upper()

    if code_upper not in title_upper:
        return False

    must_kw = watch.get("must_keywords") or ""
    for kw in must_kw.split():
        if kw.upper() not in title_upper:
            return False

    effective_fee = product["fee"] if product["fee"] is not None else 0
    return (product["price"] + effective_fee) <= float(watch["final_price"])


def load_watch_list(supabase):
    all_resp = supabase.table("product_list").select("asin_sell, auto_flag", count="exact").execute()
    print(f"  product_list 総行数: {all_resp.count}", flush=True)

    resp = supabase.table("product_list").select(
        "asin_sell, product_code_out, must_keywords, final_price, auto_flag, yahuoc_store_url, yahuoc_all_url"
    ).eq("auto_flag", True).execute()
    print(f"  auto_flag=true の行数: {len(resp.data)}", flush=True)

    filtered = [r for r in resp.data if r.get("asin_sell") and r.get("product_code_out") and r.get("final_price")
                and (r.get("yahuoc_store_url") or r.get("yahuoc_all_url"))]
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

    driver = make_driver(headless=True)
    try:
        today = now_jst.strftime("%Y-%m-%d")
        hits = []
        pushed = set()

        for i, watch in enumerate(watch_list):
            search_url = (
                watch["yahuoc_store_url"] if float(watch["final_price"]) >= CONFIG["PRICE_THRESHOLD"]
                else watch["yahuoc_all_url"]
            ) or watch.get("yahuoc_store_url") or watch.get("yahuoc_all_url")
            if not search_url:
                continue

            products = fetch_products(driver, search_url)
            print(f"検索 {i + 1}/{len(watch_list)}: {len(products)}件取得", flush=True)

            for product in products:
                if not matches(product, watch):
                    continue
                key = f"{watch['asin_sell']}\t{product['url']}"
                if key in pushed:
                    continue
                pushed.add(key)
                print(f"★ HIT: {product['title']} / ¥{product['price']}", flush=True)
                hits.append({
                    "asin":        watch["asin_sell"],
                    "url":         product["url"],
                    "date":        today,
                    "price":       product["price"],
                    "fee":         product["fee"],
                    "image":       product["image"],
                    "title":       product["title"],
                    "description": f"評価:{product['seller_rating']}",
                    "mall":        "Yahuoc",
                })

            if i < len(watch_list) - 1:
                random_sleep(CONFIG["SLEEP_MIN"], CONFIG["SLEEP_MAX"])

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
                "hits_count": len(hits),
            },
        }


def main():
    jst = pytz.timezone("Asia/Tokyo")
    now_jst = datetime.now(jst)
    started_at = datetime.now(pytz.utc)

    for key in ("SUPABASE_URL", "SUPABASE_SERVICE_ROLE_KEY"):
        if not os.environ.get(key):
            print(f"[ERROR] 環境変数 {key} が未設定", flush=True)
            sys.exit(1)

    try:
        supabase = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_SERVICE_ROLE_KEY"])
    except Exception as e:
        print(f"[ERROR] Supabase接続失敗: {e}", flush=True)
        sys.exit(1)

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
        task_name="scrape_yahuoc",
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
