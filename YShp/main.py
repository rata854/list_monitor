import os
import sys
import random
import time
import traceback
from datetime import datetime
from pathlib import Path

import pytz
import requests
from dotenv import load_dotenv
from supabase import create_client

load_dotenv(Path(__file__).parents[2] / ".secrets" / "github_actions.env")

CONFIG = {
    "DEFAULT_FEE": 0,
    "RESULTS_PER_QUERY": 30,
    "SLEEP_MIN": 3.0,
    "SLEEP_MAX": 5.0,
    "FLUSH_INTERVAL": 90,
    "PRICE_FROM_RATIO": 0.7,
    "NG_WORDS": [
        "難あり", "欠品", "訳あり", "ジャンク", "現状品",
        "互換", "のみ", "おまけ", "レンタル", "修理",
        "改造", "用", "純正", "保護", "収納",
        "パーツ", "防止", "同等品", "充電器", "対応",
    ],
}


def random_sleep(min_sec, max_sec):
    time.sleep(random.uniform(min_sec, max_sec))


def search_items(app_id, api_base, watch):
    final_price = float(watch["final_price"])
    price_from = int(final_price * CONFIG["PRICE_FROM_RATIO"])
    price_to = int(final_price)

    must_kw = watch.get("must_keywords") or ""
    query_parts = [watch["product_code_out"]]
    if must_kw.strip():
        query_parts.append(must_kw.strip())
    query = " ".join(query_parts)

    params = {
        "appid": app_id,
        "query": f'"{query}"',
        "results": CONFIG["RESULTS_PER_QUERY"],
        "in_stock": 1,
        "price_from": price_from,
        "price_to": price_to,
        "condition": "used",
    }

    try:
        resp = requests.get(api_base, params=params, timeout=15)
        resp.raise_for_status()
        hits = resp.json().get("hits", [])
    except Exception as e:
        print(f"[WARN] API呼び出し失敗: {e}", flush=True)
        return []

    results = []
    for item in hits:
        if not item.get("inStock", False):
            continue
        price = item.get("premiumPrice") or item.get("price")
        if price is None:
            continue
        results.append({
            "url":         item.get("url", ""),
            "name":        item.get("name", ""),
            "price":       int(price),
            "image":       item.get("image", {}).get("medium", ""),
            "condition":   item.get("condition", ""),
            "description": (item.get("description") or "")[:1500],
        })

    return results


def matches(item, watch):
    name_upper = item["name"].upper()
    code_upper = watch["product_code_out"].upper()
    if code_upper not in name_upper:
        return False

    must_kw = watch.get("must_keywords") or ""
    for kw in must_kw.split():
        if kw.upper() not in name_upper:
            return False

    if item.get("condition") != "used":
        return False

    for ng in CONFIG["NG_WORDS"]:
        if ng in item["name"]:
            return False

    return (item["price"] + CONFIG["DEFAULT_FEE"]) <= float(watch["final_price"])


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
    app_id = os.environ["YA_APP_ID"]
    api_base = os.environ["YA_API_BASE"]

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

    today = now_jst.strftime("%Y-%m-%d")
    hits = []
    pushed = set()
    total_searched = 0

    try:
        for i, watch in enumerate(watch_list):
            results = search_items(app_id, api_base, watch)
            total_searched += 1

            for item in results:
                if not matches(item, watch):
                    continue
                key = f"{watch['asin_sell']}\t{item['url']}"
                if key in pushed:
                    continue
                pushed.add(key)
                print(f"★ HIT: {item['name']} / ¥{item['price']}", flush=True)
                hits.append({
                    "asin":        watch["asin_sell"],
                    "url":         item["url"],
                    "date":        today,
                    "price":       item["price"],
                    "fee":         CONFIG["DEFAULT_FEE"],
                    "image":       item["image"],
                    "title":       item["name"],
                    "description": item["description"],
                    "mall":        "YShp",
                })

            if (i + 1) % CONFIG["FLUSH_INTERVAL"] == 0 and hits:
                print(f"中間フラッシュ: {len(hits)}件 ({i+1}/{len(watch_list)}型番完了)", flush=True)
                insert_hits(supabase, hits)
                hits = []

            random_sleep(CONFIG["SLEEP_MIN"], CONFIG["SLEEP_MAX"])

    except Exception as e:
        print(f"[ERROR] 検索ループ中にエラー: {e}", flush=True)
        return {
            "status": "failure",
            "exit_code": 1,
            "error_type": type(e).__name__,
            "error_message": str(e),
            "stack_trace": traceback.format_exc(),
            "debug_info": {
                "watch_list_count": len(watch_list),
                "searched_count": total_searched,
                "hits_count": len(hits),
            },
        }

    if hits:
        insert_hits(supabase, hits)

    total_hits = len(pushed)

    if total_hits == 0:
        print("条件に合う商品はありませんでした", flush=True)
        return {
            "status": "skipped",
            "exit_code": 0,
            "severity": "info",
            "debug_info": {
                "watch_list_count": len(watch_list),
                "searched_count": total_searched,
                "hits_count": 0,
            },
        }

    print(f"scrape_hits に {total_hits} 件書き込みました（重複は除外）", flush=True)
    return {
        "status": "success",
        "exit_code": 0,
        "debug_info": {
            "watch_list_count": len(watch_list),
            "searched_count": total_searched,
            "hits_count": total_hits,
        },
    }


def main():
    jst = pytz.timezone("Asia/Tokyo")
    now_jst = datetime.now(jst)
    hour = now_jst.hour
    started_at = datetime.now(pytz.utc)

    for key in ("SUPABASE_URL", "SUPABASE_SERVICE_ROLE_KEY", "YA_APP_ID", "YA_API_BASE"):
        if not os.environ.get(key):
            print(f"[ERROR] 環境変数 {key} が未設定", flush=True)
            sys.exit(1)

    try:
        supabase = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_SERVICE_ROLE_KEY"])
    except Exception as e:
        print(f"[ERROR] Supabase接続失敗: {e}", flush=True)
        sys.exit(1)

    if hour < 5 or hour >= 20:
        print(f"実行時間外のためスキップ ({hour}時 JST)", flush=True)
        log_execution(
            supabase,
            task_name="scrape_yshp",
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
        task_name="scrape_yshp",
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
