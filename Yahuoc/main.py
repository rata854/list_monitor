import html
import os
import re
import sys
import random
import time
import traceback
from collections import Counter
from datetime import datetime
from pathlib import Path

import pytz
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
from dotenv import load_dotenv
from supabase import create_client

load_dotenv(Path(__file__).parents[3] / ".secrets" / "github_actions.env")

CONFIG = {
    "PRICE_THRESHOLD": 10000,
    "PAGE_LOAD_TIMEOUT": 15,
    "SLEEP_MIN": 8.0,
    "SLEEP_MAX": 12.0,
    "SKIP_PATTERN1": "に一致する商品はありません",
    "SKIP_PATTERN2": "条件に一致する商品は見つかりませんでした",
    # 想定内の取得結果。これ以外のページが返ったら巡回を中断する
    "EXPECTED_REASONS": ("ok", "skip_ptn1", "skip_ptn2"),
    "PRODUCT_WAIT":    "li.Product, a[data-cl-params*='_cl_link:tc']",
    "PRODUCT_CARD":    r'(?=<li[^>]+class="Product[ ">])',
    "PRODUCT_ID":      r'data-auction-id="([^"]+)"',
    "PRODUCT_URL":     r'<a[^>]+class="[^"]*Product__imageLink[^"]*"[^>]+href="([^"]+)"',
    "PRODUCT_TITLE":   r'data-auction-title="([^"]+)"',
    "PRODUCT_IMG":     r'data-auction-img="([^"]+)"',
    "PRODUCT_PRICE":   r'class="[^"]*Product__priceValue[^"]*u-textRed[^"]*"[^>]*>([\d,]+)',
    "PRODUCT_POSTAGE": r'class="[^"]*Product__postage[^"]*"[^>]*>(.*?)</p>',
    "PRODUCT_RATING":  r'class="[^"]*Product__ratingValue[^"]*"[^>]*>([^<]+)',
    "PRODUCT_END_TIME": r'（(\d{1,2}/\d{1,2} \d{1,2}:\d{2})終了',
    # 新レイアウト（クラス名がハッシュ化されているため属性・文言で抽出する）
    "V2_TITLE_LINK":   r'<a[^>]+href="([^"]+)"[^>]+_cl_link:tc[^>]*?title="([^"]*)"',
    "V2_PRICE":        r'現在</span><span[^>]*>([\d,]+)',
    "V2_POSTAGE":      r'</span></span></div><p[^>]*>(.*?)</p>',
    "V2_RATING":       r'>(\d+(?:\.\d+)?)%</span>',
    # 取得失敗時にページ種別を推測するための語（認証/アクセス制限画面など）
    "BLOCK_HINTS": ["認証", "アクセスが集中", "ロボット", "captcha", "Access Denied"],
}


def parse_end_time(text):
    try:
        jst = pytz.timezone("Asia/Tokyo")
        now = datetime.now(jst)
        month, rest = text.split("/", 1)
        day, time_part = rest.split(" ", 1)
        end_month = int(month)
        end_day = int(day)
        year = now.year + (1 if end_month < now.month or (end_month == now.month and end_day < now.day - 7) else 0)
        return f"{year}-{end_month:02d}-{end_day:02d} {time_part.zfill(5)}"
    except Exception:
        return ""


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


def skip_reason(source):
    """該当なし系の表示なら reason を返す。文言が<br>等で分断されても拾えるようタグを除いて判定する。"""
    text = re.sub(r"<[^>]+>", "", source)
    if CONFIG["SKIP_PATTERN1"] in text:
        return "skip_ptn1"
    if CONFIG["SKIP_PATTERN2"] in text:
        return "skip_ptn2"
    return None


def is_skip_page(source):
    return skip_reason(source) is not None


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


def parse_products_v2(source):
    """新レイアウト用。タイトルリンクを起点に、次のタイトルリンクまでを1商品として抽出する。"""
    links = list(re.finditer(CONFIG["V2_TITLE_LINK"], source))
    products = []
    seen_ids = set()
    for idx, m in enumerate(links):
        end = links[idx + 1].start() if idx + 1 < len(links) else len(source)
        card = source[m.end():end]
        href = html.unescape(m.group(1))
        product_id = href.rstrip("/").rsplit("/", 1)[-1]
        if product_id in seen_ids:
            continue
        seen_ids.add(product_id)

        price_m = re.search(CONFIG["V2_PRICE"], card)
        if not price_m:
            continue
        try:
            price = int(price_m.group(1).replace(",", ""))
        except ValueError:
            continue

        title_raw = m.group(2)
        img_m = re.search(r'<img[^>]*?src="([^"]+)"[^>]*?alt="' + re.escape(title_raw) + '"', source)
        postage_m = re.search(CONFIG["V2_POSTAGE"], card, re.DOTALL)
        rating_m = re.search(CONFIG["V2_RATING"], card)
        end_time_m = re.search(CONFIG["PRODUCT_END_TIME"], card)

        products.append({
            "id": product_id,
            "url": href,
            "title": html.unescape(title_raw),
            "image": html.unescape(img_m.group(1)) if img_m else "",
            "price": price,
            "fee": parse_postage(postage_m.group(1)) if postage_m else None,
            "seller_rating": rating_m.group(1) if rating_m else "",
            "end_time": parse_end_time(end_time_m.group(1)) if end_time_m else "",
        })
    return products


def fetch_products(driver, url):
    """(products, reason) を返す。

    reason:
      ok            商品を1件以上取得
      skip_ptn1     「該当なし」ページ（SKIP_PATTERN1）
      skip_ptn2     「該当なし」ページ（SKIP_PATTERN2）
      blocked       待機タイムアウト＋ブロック/認証系の語を検出
      timeout_other 待機タイムアウト、商品も該当なし表示もない（不明なページ）
      parse_fail    商品IDはあるが必須項目の抽出に失敗（セレクタ不一致）
      no_cards      待機は成功したが商品IDが無い
      error         例外発生
    """
    try:
        driver.get(url)
        wait_ok = True
        try:
            WebDriverWait(driver, CONFIG["PAGE_LOAD_TIMEOUT"]).until(
                lambda d: d.find_elements(By.CSS_SELECTOR, CONFIG["PRODUCT_WAIT"])
                or is_skip_page(d.page_source)
            )
        except Exception:
            wait_ok = False
        if not wait_ok:
            time.sleep(3)
        source = driver.page_source
        redirected = driver.current_url != url
        stats = f"wait_ok={wait_ok} html_len={len(source)} redirected={redirected}"

        skip = skip_reason(source)
        if skip:
            print(f"[DEBUG] reason={skip} {stats}", flush=True)
            return [], skip

        products = []
        seen_ids = set()
        for card in re.split(CONFIG["PRODUCT_CARD"], source):
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
            end_time_m = re.search(CONFIG["PRODUCT_END_TIME"], card)

            if not (url_m and title_m and price_m):
                continue

            try:
                price = int(price_m.group(1).replace(",", ""))
            except ValueError:
                continue

            fee = parse_postage(postage_m.group(1)) if postage_m else None
            rating = rating_m.group(1).strip().rstrip("%") if rating_m else ""
            end_time = parse_end_time(end_time_m.group(1)) if end_time_m else ""

            products.append({
                "id": product_id,
                "url": url_m.group(1),
                "title": title_m.group(1),
                "image": html.unescape(img_m.group(1)) if img_m else "",
                "price": price,
                "fee": fee,
                "seller_rating": rating,
                "end_time": end_time,
            })

        if not products:
            products = parse_products_v2(source)
        if products:
            return products, "ok"

        id_matches = len(re.findall(CONFIG["PRODUCT_ID"], source))
        li_count = len(re.findall(CONFIG["PRODUCT_CARD"], source))
        hints = [h for h in CONFIG["BLOCK_HINTS"] if h.lower() in source.lower()]
        if not wait_ok and id_matches == 0:
            reason = "blocked" if hints else "timeout_other"
        elif id_matches > 0:
            reason = "parse_fail"
        else:
            reason = "no_cards"
        print(
            f"[DEBUG] reason={reason} {stats} id_matches={id_matches} "
            f"li_cards={li_count} block_hints={len(hints)}",
            flush=True,
        )
        return [], reason
    except Exception as e:
        print(f"[WARN] 商品取得エラー: {type(e).__name__}: {e}", flush=True)
        return [], "error"


def matches(product, watch):
    rating = product.get("seller_rating", "")
    if rating:
        try:
            if float(rating) < 98:
                return False
        except ValueError:
            pass

    excl_kw = watch.get("excluded_keywords") or ""
    name_upper = product.get("title", "").upper()
    for kw in excl_kw.split():
        if kw in name_upper:
            return False

    return True


def load_watch_list(supabase):
    resp = supabase.table("product_list").select(
        "asin_sell, product_code_out, must_keywords, excluded_keywords, final_price, yahuoc_store_url, yahuoc_all_url"
    ).execute()
    print(f"  product_list 取得件数: {len(resp.data)}", flush=True)

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


TASK_NAME = "scrape_yahuoc"


def already_completed_today(supabase, now_jst):
    """今日(JST)すでに全件巡回が完了していれば True。確認に失敗した場合は実行側に倒す。"""
    try:
        day_start = now_jst.replace(hour=0, minute=0, second=0, microsecond=0)
        resp = (
            supabase.table("execution_logs")
            .select("id")
            .eq("task_name", TASK_NAME)
            .in_("status", ["success", "skipped"])
            .eq("debug_info->>completed", "true")
            .gte("started_at", day_start.isoformat())
            .limit(1)
            .execute()
        )
        return bool(resp.data)
    except Exception as e:
        print(f"[WARN] 完了フラグ確認失敗（実行を続行）: {e}", flush=True)
        return False


def _run(supabase, now_jst, limit=None):
    watch_list = load_watch_list(supabase)
    print(f"監視リスト件数: {len(watch_list)}", flush=True)
    if limit is not None:
        watch_list = watch_list[:limit]
        print(f"[PARTIAL] 先頭 {len(watch_list)} 件のみ巡回します（完了フラグは立てません）", flush=True)
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
        reasons = Counter()
        aborted = None

        for i, watch in enumerate(watch_list):
            search_url = watch.get("yahuoc_all_url")
            if not search_url:
                continue

            products, reason = fetch_products(driver, search_url)
            if reason not in CONFIG["EXPECTED_REASONS"]:
                print(f"[RETRY] 検索 {i + 1}/{len(watch_list)} reason={reason}: 5秒後に1回だけ再取得します", flush=True)
                time.sleep(5)
                products, reason = fetch_products(driver, search_url)
            reasons[reason] += 1
            print(f"検索 {i + 1}/{len(watch_list)}: {len(products)}件取得 ({reason})", flush=True)

            if reason not in CONFIG["EXPECTED_REASONS"]:
                aborted = {"index": i + 1, "reason": reason}
                print(f"[ERROR] 想定外のページのため中断: 検索 {i + 1}/{len(watch_list)} reason={reason}", flush=True)
                break

            for product in products:
                if not matches(product, watch):
                    continue
                key = f"{watch['asin_sell']}\t{product['url']}"
                if key in pushed:
                    continue
                pushed.add(key)
                print(f"★ HIT: {product['title']} / ¥{product['price']}", flush=True)
                hits.append({
                    "asin":     watch["asin_sell"],
                    "url":      product["url"],
                    "date":     today,
                    "price":    product["price"],
                    "fee":      product["fee"],
                    "image":    product["image"],
                    "title":    product["title"],
                    "rating":   product["seller_rating"],
                    "end_time": product["end_time"],
                    "mall":     "Yahuoc",
                })
            if i < len(watch_list) - 1:
                random_sleep(CONFIG["SLEEP_MIN"], CONFIG["SLEEP_MAX"])

    finally:
        driver.quit()

    print(f"取得結果内訳: {dict(reasons)}", flush=True)

    debug_info = {
        "watch_list_count": len(watch_list),
        "hits_count": 0,
        "fetch_reasons": dict(reasons),
        "completed": False,
    }
    if aborted:
        debug_info["aborted_at"] = aborted["index"]
        debug_info["aborted_reason"] = aborted["reason"]

    insert_error = None
    if hits:
        try:
            inserted = insert_hits(supabase, hits)
            debug_info["hits_count"] = inserted
            print(f"scrape_hits に {inserted} 件書き込みました（重複は除外）", flush=True)
        except Exception as e:
            insert_error = e
            debug_info["hits_count"] = len(hits)
            print(f"[ERROR] Supabase書き込み失敗: {e}", flush=True)
            err_trace = traceback.format_exc()
    else:
        print("条件に合う商品はありませんでした", flush=True)

    if insert_error:
        return {
            "status": "failure",
            "exit_code": 1,
            "error_type": type(insert_error).__name__,
            "error_message": str(insert_error),
            "stack_trace": err_trace,
            "debug_info": debug_info,
        }
    if aborted:
        return {
            "status": "failure",
            "exit_code": 1,
            "error_type": "UnexpectedPage",
            "error_message": (
                f"想定外のページを検出して中断: 検索 {aborted['index']}/{len(watch_list)} "
                f"reason={aborted['reason']}（中断前のHIT {debug_info['hits_count']}件は書き込み済み）"
            ),
            "debug_info": debug_info,
        }
    debug_info["completed"] = limit is None
    return {
        "status": "success" if hits else "skipped",
        "exit_code": 0,
        "severity": None if hits else "info",
        "debug_info": debug_info,
    }


def main():
    jst = pytz.timezone("Asia/Tokyo")
    now_jst = datetime.now(jst)
    started_at = datetime.now(pytz.utc)

    dry_run = "--dry-run" in sys.argv

    for key in ("SUPABASE_URL", "SUPABASE_SERVICE_ROLE_KEY"):
        if not os.environ.get(key):
            print(f"[ERROR] 環境変数 {key} が未設定", flush=True)
            sys.exit(1)

    try:
        supabase = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_SERVICE_ROLE_KEY"])
    except Exception as e:
        print(f"[ERROR] Supabase接続失敗: {e}", flush=True)
        sys.exit(1)

    if dry_run:
        watch_list = load_watch_list(supabase)
        print(f"[DRY RUN] 監視リスト件数: {len(watch_list)}", flush=True)
        sys.exit(0)

    limit = None
    if "--limit" in sys.argv:
        try:
            limit = int(sys.argv[sys.argv.index("--limit") + 1])
            if limit < 1:
                raise ValueError
        except (IndexError, ValueError):
            print("[ERROR] --limit には1以上の整数を指定してください", flush=True)
            sys.exit(1)

    if limit is None and "--force" not in sys.argv and already_completed_today(supabase, now_jst):
        print("本日分は完了済みのため終了します（再実行する場合は --force）", flush=True)
        sys.exit(0)

    try:
        result = _run(supabase, now_jst, limit)
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
