import os
import sys
import random
import re
import time
from datetime import datetime
from urllib.parse import urlparse, urlencode, parse_qs, urlunparse

import pytz
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

load_dotenv()

CONFIG = {
    "DEFAULT_FEE": 770,
    "FETCH_PAGES": 5,
    "PAGE_LOAD_TIMEOUT": 15,
    "SLEEP_MIN": 2.0,
    "SLEEP_MAX": 4.0,
    "DETAIL_SLEEP_MIN": 2.0,
    "DETAIL_SLEEP_MAX": 4.0,
}

SEARCH_PATHS = [
    "/search?keyword=&selected_category=990001&minPrice=4000&maxPrice=&other%5B%5D=nflg&sortBy=arrival&category=990001",
    "/search?category=100001&minPrice=4000&other%5B%5D=nflg&sortBy=arrival",
    "/search?keyword=&selected_category=110001&minPrice=4000&maxPrice=&other%5B%5D=nflg&sortBy=arrival&category=110001",
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
    try:
        WebDriverWait(driver, CONFIG["PAGE_LOAD_TIMEOUT"]).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "li.itemCard"))
        )
    except Exception:
        # 商品なし（0件ページ）は正常終了
        return []

    soup = BeautifulSoup(driver.page_source, "html.parser")
    cards = soup.select("li.itemCard")
    if not cards:
        return []

    base = os.environ["SECST_BASE_URL"].rstrip("/")
    products = []
    for card in cards:
        a_tag = card.select_one("a.itemCard_inner")
        if not a_tag:
            continue
        href = a_tag.get("href", "")
        product_url = f"{base}{href}" if href.startswith("/") else href

        name_tag = a_tag.select_one("p.itemCard_name")
        name = name_tag.get_text(strip=True) if name_tag else ""

        price_tag = a_tag.select_one("p.itemCard_price")
        price_text = price_tag.get_text(strip=True) if price_tag else "0"
        price_digits = re.search(r"[\d,]+", price_text)
        price = int(price_digits.group().replace(",", "")) if price_digits else 0

        img_tag = a_tag.select_one("img")
        image = img_tag.get("src", "") if img_tag else ""

        if product_url and name:
            products.append({"url": product_url, "name": name, "price": price, "image": image})

    return products


RANK_CLASS_MAP = {
    "rankA": "中古A", "rankB": "中古B", "rankC": "中古C",
    "rankD": "中古D", "rankS": "未使用品",
}

def fetch_detail_description(driver, url):
    try:
        driver.get(url)
        WebDriverWait(driver, CONFIG["PAGE_LOAD_TIMEOUT"]).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "body"))
        )
        soup = BeautifulSoup(driver.page_source, "html.parser")
        parts = []

        # 商品ランク
        rank_ul = soup.select_one("ul#conditionRank")
        if rank_ul:
            for cls, label in RANK_CLASS_MAP.items():
                if cls in (rank_ul.get("class") or []):
                    parts.append(f"【{label}】")
                    break

        # ショップコメント
        comment = soup.select_one("#shopComment")
        if comment:
            text = comment.get_text(strip=True)
            if text:
                parts.append(text)

        # 商品スペック（型番・年式・商品情報など）
        spec_dl = soup.select_one("dl.golf_info")
        if spec_dl:
            items = []
            for dt, dd in zip(spec_dl.select("dt"), spec_dl.select("dd")):
                key = dt.get_text(strip=True)
                val = dd.get_text(separator=" ", strip=True)
                if key and val:
                    items.append(f"{key}:{val}")
            if items:
                parts.append(" / ".join(items))

        return " ".join(parts)[:1500]
    except Exception as e:
        print(f"[WARN] 詳細ページ取得失敗 {url}: {e}", flush=True)
        return ""


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


def main():
    jst = pytz.timezone("Asia/Tokyo")
    now_jst = datetime.now(jst)
    hour = now_jst.hour
    if hour < 6 or hour >= 20:
        print(f"実行時間外のためスキップ ({hour}時 JST)", flush=True)
        sys.exit(0)

    for key in ("SUPABASE_URL", "SUPABASE_SERVICE_ROLE_KEY", "SECST_BASE_URL"):
        if not os.environ.get(key):
            print(f"[ERROR] 環境変数 {key} が未設定", flush=True)
            sys.exit(1)

    base_url = os.environ["SECST_BASE_URL"].rstrip("/")
    search_urls = [base_url + path for path in SEARCH_PATHS]

    try:
        supabase = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_SERVICE_ROLE_KEY"])
    except Exception as e:
        print(f"[ERROR] Supabase接続失敗: {e}", flush=True)
        sys.exit(1)

    watch_list = load_watch_list(supabase)
    print(f"監視リスト件数: {len(watch_list)}", flush=True)
    if not watch_list:
        print("有効な監視対象がありません", flush=True)
        sys.exit(0)

    driver = make_driver()
    try:
        all_products = []
        for search_url in search_urls:
            for page in range(1, CONFIG["FETCH_PAGES"] + 1):
                page_url = set_page_param(search_url, page)
                print(f"取得中: page={page} ({search_url[:60]}...)", flush=True)
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
            sys.exit(1)

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
                print(f"★ HIT: {product['name']} / ¥{product['price']} / {product['url']}", flush=True)
                hits.append({
                    "asin": watch["asin_sell"],
                    "url": product["url"],
                    "date": today,
                    "price": product["price"],
                    "fee": CONFIG["DEFAULT_FEE"],
                    "image": product["image"],
                    "title": product["name"],
                    "description": None,
                })

        for hit in hits:
            random_sleep(CONFIG["DETAIL_SLEEP_MIN"], CONFIG["DETAIL_SLEEP_MAX"])
            hit["description"] = fetch_detail_description(driver, hit["url"])

    finally:
        driver.quit()

    if not hits:
        print("条件に合う商品はありませんでした", flush=True)
        sys.exit(0)

    try:
        inserted = insert_hits(supabase, hits)
        print(f"scrape_hits に {inserted} 件書き込みました（重複は除外）", flush=True)
    except Exception as e:
        print(f"[ERROR] Supabase書き込み失敗: {e}", flush=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
