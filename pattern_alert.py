"""
Pionex 合約(PERP)型態訊號監控 + Telegram 通知
=============================================

跟「五條件監控」(fetch_and_alert.py)完全獨立的第二支程式,用另一個 Telegram
機器人(TELEGRAM_BOT_TOKEN_2 / TELEGRAM_CHAT_ID_2)發送通知。資產排除邏輯跟
第一支程式一樣。

只偵測 1 小時 / 4 小時 / 日線 三個週期(不含 15 分鐘)。每次執行時直接問
Pionex「這三個週期各自最新收盤的那一根,是不是比上次記錄的更新」,只有真的
有新的一根收盤,才會針對該週期重新判斷型態,不會因為排程延遲而漏掉或重複。

自適應窗口(型態實際做的時間長短不一,短則幾天,長則快1個月,不用固定天數
硬分類):每次都從「最新收盤價」往前抓 lookback_candles 根(預設 200 根)
K線,然後從最近的K線開始往前試著擴張窗口,找出型態實際延伸的範圍:

1. 盤整 + 突破 / 跌破:先用「最新兩根確認K線之前」的歷史資料,窗口從小
   (min_pattern_window,預設 6 根)一路往前擴張,只要「高低價差 <=
   平均收盤價的 max_consolidation_ratio(預設 3%)」這個條件還能維持,
   就繼續擴張,取「能撐住的最大窗口」當作這段盤整區間的完整範圍。
   找到區間後,要求最新連續兩根K線都確認站穩區間外,才算數:
     - 第一根確認K線帶量(成交量 > breakout_vol_multiplier 倍的
       MAVOL)衝出區間,且這根與下一根收盤價都 > 區間最高點,兩根的
       最低價都沒有跌回區間內,才算「盤整突破」
     - 反過來,兩根收盤價都 < 區間最低點、最高價都沒有漲回區間內,
       算「盤整跌破」
2. 三角收斂:窗口從大(lookback_candles)往小掃描,每個窗口大小都檢查
   高點遞減、低點遞增、波動收窄(前後兩段比較)、三段式單調收斂、最小
   波動門檻,並且額外要求:找出區段內的樞紐高點/低點,分別畫出壓力線
   跟支撐線,至少一邊的趨勢線要「摸到」triangle_min_touches(預設 3)
   個以上的樞紐點,確保是真的畫得出來的三角形,不是隨便湊出來的兩段
   資料。取符合條件的「最大窗口」當作這個三角收斂的完整範圍。

窗口大小最後會換算成「大約幾天/幾小時」顯示在通知裡(例如 eth(~9d)),方便
一眼看出這個型態抓到的時間量級。

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

# 這支程式偵測的週期,固定為這三個(不含 15M)
DETECT_INTERVALS = ["60M", "4H", "1D"]

DEFAULT_CONFIG = {
    "min_24h_amount_usdt": 20000,        # 共用:24 小時成交金額(USDT)門檻
    "mavol_period": 5,                   # 盤整突破/跌破用的 MAVOL 期數
    "triangle_convergence_ratio": 0.5,   # 三角收斂:後半段波動範圍需收窄到前半段的比例(越小越嚴格)
    "triangle_min_window": 16,           # 三角收斂最少要幾根K線才算數(比盤整突破的門檻高,避免小樣本碰巧命中)
    "triangle_min_range_pct": 0.01,      # 三角收斂:整段平均波動至少要佔平均價的比例,避免死盤誤判
    "triangle_min_touches": 3,           # 三角收斂:至少一邊趨勢線(高點壓力線或低點支撐線)要摸到幾個樞紐點
    "triangle_touch_tolerance_pct": 0.015,  # 樞紐點與趨勢線的容許誤差,佔平均價的比例
    "triangle_pivot_span": 1,            # 判斷樞紐高/低點時,左右各比較幾根K線
    "max_consolidation_ratio": 0.03,     # 盤整區間:高低價差需 <= 平均收盤價的比例
    "breakout_vol_multiplier": 1.5,      # 突破/跌破:成交量需超過 MAVOL 的倍數
    "lookback_candles": 200,             # 每次往前抓的K線根數上限(型態最長能抓到多遠)
    "min_pattern_window": 6,             # 盤整突破/跌破最少要幾根K線才算數
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
    "60M": {"full": "1小時級別", "short": "1h"},
    "4H": {"full": "4小時級別", "short": "4h"},
    "1D": {"full": "日線級別", "short": "1d"},
}

REFERENCE_SYMBOL = "BTC_USDT_PERP"  # 用來偵測「各週期K線是否有新的一根收盤」的參考幣種

DAY_MS = 24 * 60 * 60 * 1000
HOUR_MS = 60 * 60 * 1000


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


def format_window_label(window, interval_ms):
    """把K線根數換算成大約幾天/幾小時,用於通知訊息顯示,例如 ~9d 或 ~18h"""
    total_ms = window * interval_ms
    days = total_ms / DAY_MS
    if days >= 1:
        return f"~{days:.0f}d"
    hours = total_ms / HOUR_MS
    return f"~{hours:.0f}h"


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


def find_consolidation_range(history, min_window, max_window, max_consolidation_ratio):
    """
    從 history 這一段K線的最尾端開始,往前(往舊的方向)擴張窗口,找出能維持
    「盤整」條件(高低價差 <= max_consolidation_ratio * 平均收盤價)的最大窗口。
    history 本身不包含任何用來確認突破/跌破的K線,單純是拿來判斷盤整區間的
    歷史資料。

    回傳 {"window": int, "high": float, "low": float},若連最小窗口都不算
    盤整,回傳 None。
    """
    available = len(history)
    if available < min_window:
        return None
    max_window = min(max_window, available)

    def ratio_ok(high, low, close_sum, window):
        avg_close = close_sum / window
        return avg_close > 0 and (high - low) <= max_consolidation_ratio * avg_close

    window = min_window
    period = history[-window:]
    high = max(float(k["high"]) for k in period)
    low = min(float(k["low"]) for k in period)
    close_sum = sum(float(k["close"]) for k in period)

    if not ratio_ok(high, low, close_sum, window):
        return None  # 連最小窗口都不算盤整,放棄

    best = {"window": window, "high": high, "low": low}

    # 持續往舊的方向多納入一根,只要條件還能維持就繼續擴張
    while window < max_window:
        older_candle = history[-(window + 1)]
        new_high = max(high, float(older_candle["high"]))
        new_low = min(low, float(older_candle["low"]))
        new_close_sum = close_sum + float(older_candle["close"])
        new_window = window + 1

        if ratio_ok(new_high, new_low, new_close_sum, new_window):
            window, high, low, close_sum = new_window, new_high, new_low, new_close_sum
            best = {"window": window, "high": high, "low": low}
        else:
            break  # 擴張不下去了,停在目前這個最大範圍

    return best


def detect_breakout_or_breakdown(closed, config):
    """
    回傳 (方向, 窗口根數, 幅度%, 最新收盤價)。
    方向為 "breakout"(突破)/ "breakdown"(跌破)/ None(沒有符合)。

    確認方式:先用「最新兩根之前」的歷史K線找出盤整區間,然後要求最新
    連續兩根K線都收在區間之外,且這兩根的最低價(突破時)/最高價(跌破時)
    都沒有跌回/漲回區間內,才算數。成交量的帶量確認看第一根確認K線
    (也就是實際上真正衝出區間的那一根)。
    """
    min_window = config["min_pattern_window"]
    max_window = config["lookback_candles"]
    mavol_period = config["mavol_period"]

    if len(closed) < 2:
        return None, None, None, None

    confirm1 = closed[-2]  # 第一根確認K線(真正衝出區間的那一根)
    confirm2 = closed[-1]  # 第二根確認K線(最新這根,用來確認站穩)
    history = closed[:-2]  # 拿來判斷盤整區間的歷史資料,不含這兩根確認K線

    if len(history) < mavol_period:
        return None, None, None, None

    consolidation = find_consolidation_range(
        history, min_window, max_window, config["max_consolidation_ratio"]
    )
    if consolidation is None:
        return None, None, None, None

    range_high = consolidation["high"]
    range_low = consolidation["low"]

    mavol_candles = history[-mavol_period:]
    mavol = statistics.mean(float(k["volume"]) for k in mavol_candles)
    if mavol <= 0:
        return None, None, None, None

    c1_close = float(confirm1["close"])
    c1_low = float(confirm1["low"])
    c1_high = float(confirm1["high"])
    c1_volume = float(confirm1["volume"])
    c2_close = float(confirm2["close"])
    c2_low = float(confirm2["low"])
    c2_high = float(confirm2["high"])

    vol_ok = c1_volume > config["breakout_vol_multiplier"] * mavol
    if not vol_ok:
        return None, None, None, None

    breakout_ok = (
        c1_close > range_high and c1_low > range_high
        and c2_close > range_high and c2_low > range_high
    )
    if breakout_ok:
        pct = (c2_close - range_high) / range_high * 100
        return "breakout", consolidation["window"], pct, c2_close

    breakdown_ok = (
        c1_close < range_low and c1_high < range_low
        and c2_close < range_low and c2_high < range_low
    )
    if breakdown_ok:
        pct = (range_low - c2_close) / range_low * 100
        return "breakdown", consolidation["window"], pct, c2_close

    return None, None, None, None


def find_pivots(segment, kind, pivot_span=1):
    """
    找出區段內的樞紐高點/低點(pivot high/low):該根K線的高(或低)點,比左右
    各 pivot_span 根都高(或都低),才算一個真正的轉折點,用來畫趨勢線。
    回傳 [(index_in_segment, price), ...]
    """
    pivots = []
    n = len(segment)
    for i in range(pivot_span, n - pivot_span):
        if kind == "low":
            val = float(segment[i]["low"])
            neighbors = [float(segment[j]["low"]) for j in range(i - pivot_span, i + pivot_span + 1) if j != i]
            if all(val <= nv for nv in neighbors):
                pivots.append((i, val))
        else:
            val = float(segment[i]["high"])
            neighbors = [float(segment[j]["high"]) for j in range(i - pivot_span, i + pivot_span + 1) if j != i]
            if all(val >= nv for nv in neighbors):
                pivots.append((i, val))
    return pivots


def linear_regression(points):
    """簡單最小平方法直線回歸,回傳 (斜率, 截距) 或 None(點數不足)"""
    n = len(points)
    if n < 2:
        return None
    sum_x = sum(p[0] for p in points)
    sum_y = sum(p[1] for p in points)
    sum_xx = sum(p[0] * p[0] for p in points)
    sum_xy = sum(p[0] * p[1] for p in points)
    denom = n * sum_xx - sum_x * sum_x
    if denom == 0:
        return None
    slope = (n * sum_xy - sum_x * sum_y) / denom
    intercept = (sum_y - slope * sum_x) / n
    return slope, intercept


def count_trendline_touches(pivots, line, tolerance_abs):
    """算有多少個樞紐點落在趨勢線附近(容許誤差 tolerance_abs)"""
    if line is None:
        return 0
    slope, intercept = line
    count = 0
    for x, y in pivots:
        predicted = slope * x + intercept
        if abs(y - predicted) <= tolerance_abs:
            count += 1
    return count


def detect_triangle_single(closed, window, ratio, min_range_pct, min_touches, touch_tolerance_pct, pivot_span):
    """
    三角收斂判斷(單一窗口大小)。回傳 True/False。

    除了原本「前半段 vs 後半段」的高點遞減/低點遞增/波動收窄判斷之外,額外
    加上三個更嚴謹的檢查,避免被隨機波動或「波動率自然衰減」誤判:
    1. 三段式單調收斂:拆成前/中/後三段,要求波動範圍連續遞減(前>=中>=後,
       留一點容錯空間),而不是只看頭尾兩段,濾掉單次運氣矇中的假訊號。
    2. 最小波動門檻:整段資料本身要有一定波動幅度才算數,太平的死盤不算
       三角收斂(那種比較適合用盤整突破/跌破來看)。
    3. 趨勢線觸碰驗證:找出區段內的樞紐高點/低點,分別對高點畫一條壓力
       趨勢線、對低點畫一條支撐趨勢線,至少要有一邊的趨勢線「摸到」
       min_touches(預設 3)個以上的樞紐點,才算是真正畫得出來的三角形,
       不是隨便兩段資料湊出來的。
    """
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
    narrowing = second_avg_range <= ratio * first_avg_range

    if not (higher_lows and lower_highs and narrowing):
        return False

    # 最小波動門檻:整段平均價要有足夠波動才算有意義的三角收斂
    avg_price = statistics.mean(float(k["close"]) for k in segment)
    if avg_price <= 0 or first_avg_range < min_range_pct * avg_price:
        return False

    # 三段式單調收斂檢查(前/中/後),留 10% 容錯空間避免過度嚴苛
    third = window // 3
    if third < 2:
        return False
    seg1 = segment[:third]
    seg2 = segment[third:2 * third]
    seg3 = segment[2 * third:]

    seg1_avg_range = statistics.mean(float(k["high"]) - float(k["low"]) for k in seg1)
    seg2_avg_range = statistics.mean(float(k["high"]) - float(k["low"]) for k in seg2)
    seg3_avg_range = statistics.mean(float(k["high"]) - float(k["low"]) for k in seg3)

    tolerance = 1.1
    monotonic_narrowing = (
        seg2_avg_range <= seg1_avg_range * tolerance
        and seg3_avg_range <= seg2_avg_range * tolerance
    )
    if not monotonic_narrowing:
        return False

    # 趨勢線觸碰驗證:至少一邊(高點壓力線或低點支撐線)要摸到 min_touches 個樞紐點
    low_pivots = find_pivots(segment, "low", pivot_span)
    high_pivots = find_pivots(segment, "high", pivot_span)

    low_line = linear_regression(low_pivots)
    high_line = linear_regression(high_pivots)

    tolerance_abs = touch_tolerance_pct * avg_price
    touches_low = count_trendline_touches(low_pivots, low_line, tolerance_abs)
    touches_high = count_trendline_touches(high_pivots, high_line, tolerance_abs)

    return touches_low >= min_touches or touches_high >= min_touches


def find_max_triangle_window(closed, min_window, max_window, ratio, min_range_pct,
                              min_touches, touch_tolerance_pct, pivot_span):
    """
    從大窗口往小窗口掃描,找出符合三角收斂條件的「最大」窗口大小(根數需為
    偶數,方便均分前後半段)。回傳 window(int)或 None(完全沒有符合)。
    """
    available = len(closed)
    max_window = min(max_window, available)
    start = max_window if max_window % 2 == 0 else max_window - 1
    if min_window % 2 != 0:
        min_window += 1

    window = start
    while window >= min_window:
        if detect_triangle_single(closed, window, ratio, min_range_pct,
                                   min_touches, touch_tolerance_pct, pivot_span):
            return window
        window -= 2
    return None


def evaluate_symbol(klines, config, interval_ms, now_ms):
    """回傳 {"triangle_window": int|None, "breakout": (方向, 窗口根數, 幅度%, 收盤價)}"""
    if not klines:
        return {"triangle_window": None, "breakout": (None, None, None, None)}

    closed = get_closed_klines(klines, interval_ms, now_ms)

    triangle_window = find_max_triangle_window(
        closed, config["triangle_min_window"], config["lookback_candles"],
        config["triangle_convergence_ratio"], config["triangle_min_range_pct"],
        config["triangle_min_touches"], config["triangle_touch_tolerance_pct"],
        config["triangle_pivot_span"],
    )
    breakout = detect_breakout_or_breakdown(closed, config)

    return {"triangle_window": triangle_window, "breakout": breakout}


def get_latest_closed_candle_time(session, symbol, interval, now_ms):
    """回傳指定週期「最新一根已收盤K線」的開盤時間(ms),沒有資料則回傳 None"""
    interval_ms = INTERVAL_MS.get(interval, 60 * 60 * 1000)
    try:
        klines = get_klines(session, symbol, interval, limit=5)
    except Exception as e:
        print(f"[警告] 取得 {symbol} {interval} 參考K線失敗:{e}")
        return None
    closed = get_closed_klines(klines, interval_ms, now_ms)
    if not closed:
        return None
    return closed[-1]["time"]


STATE_BOUNDARY_KEYS = {
    "60M": "last_60m_boundary_ms",
    "4H": "last_4h_boundary_ms",
    "1D": "last_1d_boundary_ms",
}


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

    # 分別問 1H / 4H / 1D 各自「最新收盤那一根」是不是比上次記錄的更新,
    # 只有真的有新一根收盤,才把該週期加入這次要偵測的清單
    intervals = []
    latest_boundary_times = {}
    for interval in DETECT_INTERVALS:
        latest_time = get_latest_closed_candle_time(session, REFERENCE_SYMBOL, interval, now_ms)
        latest_boundary_times[interval] = latest_time
        boundary_key = STATE_BOUNDARY_KEYS[interval]
        prev_time = state.get(boundary_key)
        due = latest_time is not None and (prev_time is None or latest_time > prev_time)
        if due:
            intervals.append(interval)

    print(f"本次執行時間點:{run_start_taipei.strftime('%Y-%m-%d %H:%M:%S')} UTC+8,本次偵測週期:{intervals}")

    if not intervals:
        print("這次沒有任何週期出現新的收盤K線,跳過本次偵測。")
        state["last_run_utc"] = datetime.now(timezone.utc).isoformat()
        state["last_run_intervals"] = []
        state["last_match_count"] = 0
        save_json(STATE_FILE, state)
        return

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

    fetch_limit = config["lookback_candles"] + config["mavol_period"] + 10

    # {interval: [(base, triangle_window), ...]}
    triangle_by_interval = {}
    # {interval: [(base, direction, window, pct, close), ...]}
    breakout_by_interval = {}

    for interval in intervals:
        interval_ms = INTERVAL_MS[interval]

        triangle_matches = []
        breakout_matches = []
        for symbol, base_currency in candidates:
            try:
                klines = get_klines(session, symbol, interval, fetch_limit)
            except Exception as e:
                print(f"[警告] 取得 {symbol} {interval} K 線失敗:{e}")
                continue
            finally:
                time.sleep(config["request_sleep_sec"])

            result = evaluate_symbol(klines, config, interval_ms, now_ms)
            if result["triangle_window"] is not None:
                triangle_matches.append((base_currency, result["triangle_window"]))

            direction, window, pct, close_price = result["breakout"]
            if direction is not None:
                breakout_matches.append((base_currency, direction, window, pct, close_price))

        triangle_by_interval[interval] = triangle_matches
        breakout_by_interval[interval] = breakout_matches
        print(f"[{interval}] 三角收斂:{len(triangle_matches)} 個,盤整突破/跌破:{len(breakout_matches)} 個")

    total_matches = sum(len(v) for v in triangle_by_interval.values()) + \
        sum(len(v) for v in breakout_by_interval.values())

    # 記錄這次已經處理過的各週期K線邊界,避免下次重複觸發同一根
    for interval in intervals:
        state[STATE_BOUNDARY_KEYS[interval]] = latest_boundary_times[interval]
    state["last_run_utc"] = datetime.now(timezone.utc).isoformat()
    state["last_run_intervals"] = intervals
    state["last_match_count"] = total_matches
    save_json(STATE_FILE, state)

    if total_matches == 0:
        print("本次偵測的週期都沒有符合型態的幣種,本次不發送通知。")
        return

    now_taipei_str = run_start_taipei.strftime("%Y-%m-%d %H:%M")
    triangle_ratio_pct = int(config["triangle_convergence_ratio"] * 100)
    max_consolidation_pct = config["max_consolidation_ratio"] * 100
    breakout_vol_multiplier = config["breakout_vol_multiplier"]
    mavol_period = config["mavol_period"]
    lookback_candles = config["lookback_candles"]

    lines = [
        f"📐 Pionex 型態訊號快訊 ({now_taipei_str} UTC+8)",
        f"偵測項目(1H/4H/日線,每次從最新收盤往前抓{lookback_candles}根K線,自適應找型態範圍)",
        f"1.三角收斂(高點遞減/低點遞增,波動收窄至前段的{triangle_ratio_pct}%以下)",
        f"2.盤整突破/跌破(盤整區間<=平均價{max_consolidation_pct:g}%,"
        f"帶量({breakout_vol_multiplier}倍MAVOL{mavol_period})衝出區間後,"
        f"連續2根收盤+最低/最高價都站穩區間外才算數)",
    ]

    for interval in intervals:
        interval_ms = INTERVAL_MS[interval]
        triangle_matches = triangle_by_interval.get(interval, [])
        breakout_matches = breakout_by_interval.get(interval, [])
        if not triangle_matches and not breakout_matches:
            continue  # 這個週期沒有任何符合的型態,整段不顯示

        label = INTERVAL_LABELS.get(interval, {}).get("full", interval)
        lines.append("=============================")
        lines.append(f"(當前偵測 {label})")

        if triangle_matches:
            parts = []
            for base_currency, window in triangle_matches:
                window_label = format_window_label(window, interval_ms)
                parts.append(f"{base_currency.lower()}({window_label})")
            lines.append(f"三角收斂:{','.join(parts)}")

        for base_currency, direction, window, pct, close_price in breakout_matches:
            base_lower = base_currency.lower()
            window_label = format_window_label(window, interval_ms)
            action_label = "突破" if direction == "breakout" else "跌破"
            action_name = "盤整突破" if direction == "breakout" else "盤整跌破"
            lines.append(
                f"{action_name}:{base_lower}({window_label}) "
                f"{action_label}區間{pct:.2f}%(現價{format_price(close_price)})"
            )

    message = "\n".join(lines)
    print(message)
    send_telegram_message(message)


if __name__ == "__main__":
    main()
