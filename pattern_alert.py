"""
Pionex 合約(PERP)型態訊號監控 + Telegram 通知
=============================================

跟「五條件監控」(fetch_and_alert.py)完全獨立的第二支程式,用另一個 Telegram
機器人(TELEGRAM_BOT_TOKEN_2 / TELEGRAM_CHAT_ID_2)發送通知。排程時間、資產
排除邏輯、多週期分層偵測(15M/60M/4H)的機制都跟第一支程式一樣。

偵測項目:
1. 三角收斂:取最近 triangle_window 根K線(預設 20 根),分成前後兩段比較——
   後半段最高點 < 前半段最高點(高點遞減)、後半段最低點 > 前半段最低點
   (低點遞增)、且後半段平均波動範圍(高-低)收窄到前半段的
   triangle_convergence_ratio(預設 60%)以下,三者同時成立才算三角收斂。
2. 盤整突破:取最新這根「之前」的 breakout_window 根K線(預設 15 根),
   若這段期間的高低價差 <= 平均收盤價的 max_consolidation_ratio(預設 3%),
   代表處於盤整;若最新這根K線收盤價突破這段區間的最高點,且成交量 >
   breakout_vol_multiplier 倍的 MAVOL(取這根之前 mavol_period 根平均成交量,
   不含本身),才算盤整突破。

執行環境需要兩個環境變數(在 GitHub Actions 裡用 Secrets 設定):
- TELEGRAM_BOT_TOKEN_2
- TELEGRAM_CHAT_ID_2
"""

import json
import os
import time
import statistics
from datetime import datetime, timezone, timedelta

import requests

PIONEX_BASE = "https://api.pionex.com"
STATE_FILE = os.path.join(os.path.dirname(__file__), "pattern_state.json")
CONFIG_FILE = os.path.join(os.path.dirname(__file__), "pattern_config.json")

TAIPEI_TZ = timezone(timedelta(hours=8))

DEFAULT_CONFIG = {
    "min_24h_amount_usdt": 20000,        # 條件一(共用):24 小時成交金額(USDT)門檻
    "mavol_period": 5,                   # 盤整突破用的 MAVOL 期數
    "triangle_window": 20,               # 三角收斂:取最近幾根K線來判斷
    "triangle_convergence_ratio": 0.6,   # 三角收斂:後半段波動範圍需收窄到前半段的比例
    "breakout_window": 15,               # 盤整突破:取最新這根「之前」幾根K線來判斷是否盤整
    "max_consolidation_ratio": 0.03,     # 盤整突破:盤整區間高低價差需 <= 平均收盤價的比例
    "breakout_vol_multiplier": 1.5,      # 盤整突破:成交量需超過 MAVOL 的倍數
    "kline_fetch_limit": 30,             # 每次抓取的 K 線根數(需 >= triangle_window 等)
    "request_sleep_sec": 0.15,           # 每次呼叫 klines API 之間的間隔,避免超過速率限制
    "settle_delay_sec": 45,              # 排程一開始先等待幾秒,確保交易所該收盤的K線已經寫入完成

    # 只偵測加密貨幣,排除美股代幣(xStocks)、貴金屬等非加密貨幣資產(與
    # fetch_and_alert.py 的 config.json 相同邏輯,兩份設定各自維護)。
    "excluded_base_currencies": [
        "AAPLX", "TSLAX", "NVDAX", "SPYX", "QQQX", "MSTRX", "CRCLX", "GOOGLX",
        "VTIX", "BRK.BX", "UNHX", "GMEX", "CMCSAX", "PGX", "NFLXX", "XOMX",
        "AMBRX", "LLYX", "ABBVX", "VX", "CSCOX", "MCDX", "NVOX", "KRAQX",
        "PFEX", "INTCX", "HOODX", "AMZNX", "METAX", "COINX", "MSFTX",
        "TQQQX", "DFDVX", "ASMLX",
        "AAOIX", "AXTIX", "CXMTX", "DRAMX", "SKHX",
        "PPLTX", "XAU", "XAG", "XPT", "XPD", "PAXG", "XAUT",
    ],
    "excluded_stablecoin_bases": [
        "USDT", "USDC", "BUSD", "DAI", "TUSD", "FDUSD", "USDD", "PYUSD", "USDE",
    ],
    "exclude_name_keywords": [
        "stock", "xstock", "gold", "silver", "platinum", "palladium", "metal",
    ],
}

INTERVAL_MS = {
    "1M": 1 * 60 * 1000,
    "5M": 5 * 60 * 1000,
    "15M": 15 * 60 * 1000,
    "30M": 30 * 60 * 1000,
    "60M": 60 * 60 * 1000,
    "4H": 4 * 60 * 60 * 1000,
    "8H": 8 * 60 * 60 * 1000,
    "12H": 12 * 60 * 60 * 1000,
    "1D": 24 * 60 * 60 * 1000,
}

INTERVAL_LABELS = {
    "15M": {"full": "15 分鐘級別", "short": "15m"},
    "60M": {"full": "1小時級別", "short": "1h"},
    "4H": {"full": "4小時級別", "short": "4h"},
}

REFERENCE_SYMBOL = "BTC_USDT_PERP"  # 用來偵測「1小時/4小時K線是否有新的一根收盤」的參考幣種


def load_json(path, default):
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return default


def save_json(path, obj):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)


def get_perp_symbols():
    """取得目前 Pionex 支援的所有合約(PERP)幣種,回傳 {symbol: {"base": ..., "name": ...}}"""
    resp = requests.get(
        f"{PIONEX_BASE}/api/v1/common/symbols",
        params={"type": "PERP"},
        timeout=15,
    )
    resp.raise_for_status()
    data = resp.json()
    if not data.get("result"):
        raise RuntimeError(f"Pionex symbols API error: {data}")
    return {
        s["symbol"]: {
            "base": s.get("baseCurrency", s["symbol"]),
            "name": s.get("name", ""),
        }
        for s in data["data"]["symbols"]
        if s.get("enable", True)
    }


def get_perp_tickers():
    """取得所有合約(PERP)幣種的 24 小時行情資料"""
    resp = requests.get(
        f"{PIONEX_BASE}/api/v1/market/tickers",
        params={"type": "PERP"},
        timeout=15,
    )
    resp.raise_for_status()
    data = resp.json()
    if not data.get("result"):
        raise RuntimeError(f"Pionex tickers API error: {data}")
    return {t["symbol"]: t for t in data["data"]["tickers"]}


def get_klines(session, symbol, interval, limit):
    resp = session.get(
        f"{PIONEX_BASE}/api/v1/market/klines",
        params={"symbol": symbol, "interval": interval, "limit": limit},
        timeout=15,
    )
    resp.raise_for_status()
    data = resp.json()
    if not data.get("result"):
        return []
    return data["data"]["klines"]


def format_price(x):
    s = f"{x:.6f}".rstrip("0").rstrip(".")
    return s if s else "0"


def send_telegram_message(text):
    token = os.environ.get("TELEGRAM_BOT_TOKEN_2")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID_2")
    if not token or not chat_id:
        print("[警告] 未設定 TELEGRAM_BOT_TOKEN_2 / TELEGRAM_CHAT_ID_2,跳過推播,僅印出訊息:")
        print(text)
        return

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    body = {
        "chat_id": chat_id,
        "text": text[:4000],
    }
    r = requests.post(url, json=body, timeout=15)
    if r.status_code != 200:
        print(f"[錯誤] Telegram 推播失敗: {r.status_code} {r.text}")
    else:
        print("[完成] Telegram 推播成功")


def is_excluded_asset(symbol_info, config):
    """判斷是否為要排除的非加密貨幣資產(美股代幣、貴金屬、外匯型合約等)"""
    base = symbol_info.get("base", "").upper()
    name = (symbol_info.get("name") or "").lower()

    excluded_bases = {b.upper() for b in config.get("excluded_base_currencies", [])}
    if base in excluded_bases:
        return True

    stablecoin_bases = {b.upper() for b in config.get("excluded_stablecoin_bases", [])}
    if base in stablecoin_bases:
        return True

    keywords = config.get("exclude_name_keywords", [])
    if name and any(kw.lower() in name for kw in keywords):
        return True

    return False


def get_closed_klines(klines, interval_ms, now_ms):
    """排序並只保留「已經收盤」的 K 線(排除還在形成中的最新一根)"""
    sorted_klines = sorted(klines, key=lambda k: k["time"])
    return [k for k in sorted_klines if k["time"] + interval_ms <= now_ms]


def detect_triangle(closed, config):
    """三角收斂判斷。回傳 True/False。"""
    window = config["triangle_window"]
    if len(closed) < window:
        return False

    segment = closed[-window:]
    half = window // 2
    first_half = segment[:half]
    second_half = segment[half:]

    first_high = max(float(k["high"]) for k in first_half)
    first_low = min(float(k["low"]) for k in first_half)
    second_high = max(float(k["high"]) for k in second_half)
    second_low = min(float(k["low"]) for k in second_half)

    first_avg_range = statistics.mean(float(k["high"]) - float(k["low"]) for k in first_half)
    second_avg_range = statistics.mean(float(k["high"]) - float(k["low"]) for k in second_half)

    if first_avg_range <= 0:
        return False

    higher_lows = second_low > first_low
    lower_highs = second_high < first_high
    narrowing = second_avg_range <= config["triangle_convergence_ratio"] * first_avg_range

    return higher_lows and lower_highs and narrowing


def detect_breakout(closed, config):
    """盤整突破判斷。回傳 (是否符合, 突破幅度%, 最新收盤價) 或 (False, None, None)。"""
    breakout_window = config["breakout_window"]
    mavol_period = config["mavol_period"]

    needed = max(breakout_window + 1, mavol_period + 1)
    if len(closed) < needed:
        return False, None, None

    latest = closed[-1]
    latest_close = float(latest["close"])
    latest_volume = float(latest["volume"])

    period = closed[-(breakout_window + 1):-1]
    period_high = max(float(k["high"]) for k in period)
    period_low = min(float(k["low"]) for k in period)
    avg_close = statistics.mean(float(k["close"]) for k in period)

    if avg_close <= 0:
        return False, None, None

    is_consolidating = (period_high - period_low) <= config["max_consolidation_ratio"] * avg_close
    if not is_consolidating:
        return False, None, None

    breaks_above = latest_close > period_high
    if not breaks_above:
        return False, None, None

    mavol_candles = closed[-(mavol_period + 1):-1]
    mavol = statistics.mean(float(k["volume"]) for k in mavol_candles)
    if mavol <= 0:
        return False, None, None

    vol_ok = latest_volume > config["breakout_vol_multiplier"] * mavol
    if not vol_ok:
        return False, None, None

    pct = (latest_close - period_high) / period_high * 100
    return True, pct, latest_close


def evaluate_symbol(klines, config, interval_ms, now_ms):
    """回傳 {"triangle": bool, "breakout": bool, "breakout_pct": float|None, "close": float|None}"""
    if not klines:
        return {"triangle": False, "breakout": False, "breakout_pct": None, "close": None}

    closed = get_closed_klines(klines, interval_ms, now_ms)

    triangle_matched = detect_triangle(closed, config)
    breakout_matched, breakout_pct, close_price = detect_breakout(closed, config)

    return {
        "triangle": triangle_matched,
        "breakout": breakout_matched,
        "breakout_pct": breakout_pct,
        "close": close_price,
    }


def get_latest_closed_candle_time(session, symbol, interval, now_ms):
    """回傳指定週期「最新一根已收盤K線」的開盤時間(ms),沒有資料則回傳 None"""
    interval_ms = INTERVAL_MS.get(interval, 15 * 60 * 1000)
    try:
        klines = get_klines(session, symbol, interval, limit=5)
    except Exception as e:
        print(f"[警告] 取得 {symbol} {interval} 參考K線失敗:{e}")
        return None
    closed = get_closed_klines(klines, interval_ms, now_ms)
    if not closed:
        return None
    return closed[-1]["time"]


def main():
    config = load_json(CONFIG_FILE, DEFAULT_CONFIG)
    for k, v in DEFAULT_CONFIG.items():
        config.setdefault(k, v)

    run_start_taipei = datetime.now(TAIPEI_TZ)

    settle_delay = config.get("settle_delay_sec", 45)
    if settle_delay > 0:
        print(f"等待 {settle_delay} 秒讓交易所K線資料寫入完成...")
        time.sleep(settle_delay)

    now_ms = int(time.time() * 1000)

    state = load_json(STATE_FILE, {})

    session = requests.Session()
    intervals = ["15M"]

    latest_60m_time = get_latest_closed_candle_time(session, REFERENCE_SYMBOL, "60M", now_ms)
    prev_60m_time = state.get("last_60m_boundary_ms")
    due_60m = latest_60m_time is not None and (prev_60m_time is None or latest_60m_time > prev_60m_time)
    if due_60m:
        intervals.append("60M")

    latest_4h_time = get_latest_closed_candle_time(session, REFERENCE_SYMBOL, "4H", now_ms)
    prev_4h_time = state.get("last_4h_boundary_ms")
    due_4h = latest_4h_time is not None and (prev_4h_time is None or latest_4h_time > prev_4h_time)
    if due_4h:
        intervals.append("4H")

    print(f"本次執行時間點:{run_start_taipei.strftime('%Y-%m-%d %H:%M:%S')} UTC+8,本次偵測週期:{intervals}")

    symbols_map = get_perp_symbols()
    tickers = get_perp_tickers()

    crypto_only = {
        symbol: info
        for symbol, info in symbols_map.items()
        if not is_excluded_asset(info, config)
    }
    excluded_count = len(symbols_map) - len(crypto_only)
    print(f"排除非加密貨幣資產(美股代幣/貴金屬等)數量:{excluded_count} / {len(symbols_map)}")

    candidates = []
    for symbol, info in crypto_only.items():
        ticker = tickers.get(symbol)
        if not ticker:
            continue
        try:
            amount_24h = float(ticker.get("amount", 0))
        except (TypeError, ValueError):
            continue
        if amount_24h > config["min_24h_amount_usdt"]:
            candidates.append((symbol, info["base"]))

    print(f"通過 24 小時成交金額篩選的幣種數量:{len(candidates)} / {len(crypto_only)}")

    triangle_by_interval = {}   # {interval: [base, ...]}
    breakout_by_interval = {}   # {interval: [(base, pct, close), ...]}

    for interval in intervals:
        interval_ms = INTERVAL_MS.get(interval, 15 * 60 * 1000)
        triangle_matches = []
        breakout_matches = []
        for symbol, base_currency in candidates:
            try:
                klines = get_klines(session, symbol, interval, config["kline_fetch_limit"])
            except Exception as e:
                print(f"[警告] 取得 {symbol} {interval} K 線失敗:{e}")
                continue
            finally:
                time.sleep(config["request_sleep_sec"])

            result = evaluate_symbol(klines, config, interval_ms, now_ms)
            if result["triangle"]:
                triangle_matches.append(base_currency)
            if result["breakout"]:
                breakout_matches.append((base_currency, result["breakout_pct"], result["close"]))

        triangle_by_interval[interval] = triangle_matches
        breakout_by_interval[interval] = breakout_matches
        print(f"[{interval}] 三角收斂:{len(triangle_matches)} 個,盤整突破:{len(breakout_matches)} 個")

    total_matches = sum(len(v) for v in triangle_by_interval.values()) + \
        sum(len(v) for v in breakout_by_interval.values())

    if due_60m:
        state["last_60m_boundary_ms"] = latest_60m_time
    if due_4h:
        state["last_4h_boundary_ms"] = latest_4h_time
    state["last_run_utc"] = datetime.now(timezone.utc).isoformat()
    state["last_run_intervals"] = intervals
    state["last_match_count"] = total_matches
    save_json(STATE_FILE, state)

    if total_matches == 0:
        print("本次偵測的週期都沒有符合型態的幣種,本次不發送通知。")
        return

    now_taipei_str = run_start_taipei.strftime("%Y-%m-%d %H:%M")
    triangle_window = config["triangle_window"]
    triangle_ratio_pct = int(config["triangle_convergence_ratio"] * 100)
    breakout_window = config["breakout_window"]
    max_consolidation_pct = config["max_consolidation_ratio"] * 100
    breakout_vol_multiplier = config["breakout_vol_multiplier"]
    mavol_period = config["mavol_period"]

    lines = [
        f"📐 Pionex 型態訊號快訊 ({now_taipei_str} UTC+8)",
        "偵測項目",
        f"1.三角收斂(近{triangle_window}根K線,高點遞減/低點遞增,波動收窄至前段的{triangle_ratio_pct}%以下)",
        f"2.盤整突破(近{breakout_window}根K線盤整區間<=平均價{max_consolidation_pct:g}%,"
        f"最新K線帶量>={breakout_vol_multiplier}倍MAVOL{mavol_period}突破區間高點)",
    ]

    for interval in intervals:
        triangle_matches = triangle_by_interval.get(interval, [])
        breakout_matches = breakout_by_interval.get(interval, [])
        if not triangle_matches and not breakout_matches:
            continue  # 這個週期沒有任何符合的型態,整段不顯示

        label = INTERVAL_LABELS.get(interval, {}).get("full", interval)
        lines.append("=============================")
        lines.append(f"(當前偵測 {label})")

        if triangle_matches:
            bases_str = ",".join(b.lower() for b in triangle_matches)
            lines.append(f"三角收斂:{bases_str}")

        for base_currency, pct, close_price in breakout_matches:
            base_lower = base_currency.lower()
            lines.append(
                f"盤整突破:{base_lower} 突破區間高點 {pct:.2f}%(現價{format_price(close_price)})"
            )

    message = "\n".join(lines)
    print(message)
    send_telegram_message(message)


if __name__ == "__main__":
    main()
