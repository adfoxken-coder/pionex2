"""
Pionex 合約(PERP)型態訊號監控 + Telegram 通知
=============================================

跟「五條件監控」(fetch_and_alert.py)完全獨立的第二支程式,用另一個 Telegram
機器人(TELEGRAM_BOT_TOKEN_2 / TELEGRAM_CHAT_ID_2)發送通知。資產排除邏輯跟
第一支程式一樣。

分兩階段運作:

【第一階段:4H / 1D,找出候選標的】
每 5 分鐘執行一次,分別問 Pionex「4H / 1D 各自最新收盤的那一根,是不是比
上次記錄的更新」,只有真的有新一根收盤,才針對該週期重新掃描所有候選幣種。
掃描時用自適應窗口找出:
  - 三角收斂(找到就用該窗口「最近三分之一段」的高低點當作追蹤區間)
  - 盤整狀態(不論有沒有突破,只要現在正處於盤整,就用該盤整區間)
只要偵測到其中一種,就把這個幣種加入「追蹤名單」(記錄區間高低點、來源週期、
型態種類、加入時間),存進 pattern_state.json,留到下一步驟去驗證。

【第二階段:1H,驗證追蹤名單裡的標的是不是真的突破/跌破】
只要 1H 有新一根收盤(同樣用「是不是比上次記錄的更新」判斷,不用猜時鐘),
就只針對追蹤名單裡的幣種抓 1H K線(不會掃全部候選幣種,比較輕量),用下面
規則驗證:
  - 第一根(真正衝出區間的那一根)要帶量:成交量 > breakout_vol_multiplier
    倍的 MAVOL
  - 接下來連續兩根不需要帶量,但收盤價、最低價(突破時)/最高價(跌破時)
    都要維持在區間外,只要中途有一根跌回/漲回區間內就不算數
  - 三根都站穩,才算「真正突破/跌破」,發送 Telegram 通知(會附註累計失敗
    過幾次),並把該標的從追蹤名單移除
  - 如果曾經衝出區間、但後來又折返回區間內(突破/跌破失敗),不會發通知,
    但會把失敗次數 +1,並重新從這個時間點開始追蹤 watchlist_max_age_hours
    (預設 120 小時,即 5 天),繼續等待下一次真正突破/跌破
  - 追蹤太久還沒被驗證出結果的標的,超過 watchlist_max_age_hours 都沒有
    任何突破嘗試或折返,會自動從追蹤名單移除,避免名單越滾越大

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

# 第一階段(找候選標的)只用這兩個週期
SCAN_INTERVALS = ["4H", "1D"]
# 第二階段(驗證真正突破/跌破)固定用 1 小時
CONFIRM_INTERVAL = "60M"
# 這三個週期都要各自做「是否有新收盤」的判斷
DUE_CHECK_INTERVALS = ["4H", "1D", "60M"]

DEFAULT_CONFIG = {
    "min_24h_amount_usdt": 20000,        # 共用:24 小時成交金額(USDT)門檻
    "mavol_period": 5,                   # 1H 確認突破/跌破用的 MAVOL 期數
    "triangle_convergence_ratio": 0.5,   # 三角收斂:後半段波動範圍需收窄到前半段的比例(越小越嚴格)
    "triangle_min_window": 16,           # 三角收斂最少要幾根K線才算數
    "triangle_min_range_pct": 0.01,      # 三角收斂:整段平均波動至少要佔平均價的比例,避免死盤誤判
    "triangle_min_touches": 3,           # 三角收斂:至少一邊趨勢線要摸到幾個樞紐點
    "triangle_touch_tolerance_pct": 0.015,  # 樞紐點與趨勢線的容許誤差,佔平均價的比例
    "triangle_pivot_span": 1,            # 判斷樞紐高/低點時,左右各比較幾根K線
    "max_consolidation_ratio": 0.03,     # 盤整區間:高低價差需 <= 平均收盤價的比例
    "breakout_vol_multiplier": 1.5,      # 1H 確認突破/跌破:成交量需超過 MAVOL 的倍數
    "lookback_candles": 200,             # 4H/1D 每次往前抓的K線根數上限
    "min_pattern_window": 6,             # 盤整狀態最少要幾根K線才算數
    "watchlist_max_age_hours": 120,      # 追蹤名單裡的標的,超過這個時數還沒驗證出結果就自動移除
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
        "SNDKX", "USOX", "BRENTOIL",
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

PATTERN_TYPE_NAMES = {
    "triangle": "三角收斂",
    "consolidation": "盤整",
}

REFERENCE_SYMBOL = "BTC_USDT_PERP"  # 用來偵測「各週期K線是否有新的一根收盤」的參考幣種

DAY_MS = 24 * 60 * 60 * 1000
HOUR_MS = 60 * 60 * 1000

STATE_BOUNDARY_KEYS = {
    "60M": "last_60m_boundary_ms",
    "4H": "last_4h_boundary_ms",
    "1D": "last_1d_boundary_ms",
}


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


def find_consolidation_range(closed, min_window, max_window, max_consolidation_ratio):
    """
    從這批K線的最尾端開始,往前(往舊的方向)擴張窗口,找出能維持「盤整」
    條件(高低價差 <= max_consolidation_ratio * 平均收盤價)的最大窗口。
    這裡不預留任何「確認K線」,單純判斷「現在是不是正處於盤整」。

    回傳 {"window": int, "high": float, "low": float},若連最小窗口都不算
    盤整,回傳 None。
    """
    available = len(closed)
    if available < min_window:
        return None
    max_window = min(max_window, available)

    def ratio_ok(high, low, close_sum, window):
        avg_close = close_sum / window
        return avg_close > 0 and (high - low) <= max_consolidation_ratio * avg_close

    window = min_window
    period = closed[-window:]
    high = max(float(k["high"]) for k in period)
    low = min(float(k["low"]) for k in period)
    close_sum = sum(float(k["close"]) for k in period)

    if not ratio_ok(high, low, close_sum, window):
        return None  # 連最小窗口都不算盤整,放棄

    best = {"window": window, "high": high, "low": low}

    # 持續往舊的方向多納入一根,只要條件還能維持就繼續擴張
    while window < max_window:
        older_candle = closed[-(window + 1)]
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

    1. 前後兩段比較:高點遞減、低點遞增、波動收窄。
    2. 最小波動門檻:整段資料本身要有一定波動幅度才算數,避免死盤誤判。
    3. 三段式單調收斂:拆成前/中/後三段,波動範圍需連續遞減(留 10% 容錯)。
    4. 趨勢線觸碰驗證:找出樞紐高/低點,至少一邊的趨勢線要摸到
       min_touches(預設 3)個以上的樞紐點,確保是真的畫得出來的三角形。
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

    avg_price = statistics.mean(float(k["close"]) for k in segment)
    if avg_price <= 0 or first_avg_range < min_range_pct * avg_price:
        return False

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


def get_triangle_apex_range(closed, window):
    """
    給定已經確認符合三角收斂的窗口大小,回傳這個三角形「最尖端」(最近三分之
    一段)的高低點,當作後續要驗證真正突破/跌破用的區間。
    """
    segment = closed[-window:]
    third = window // 3
    seg3 = segment[2 * third:]
    high = max(float(k["high"]) for k in seg3)
    low = min(float(k["low"]) for k in seg3)
    return high, low


def get_triangle_touched_sides(closed, window, min_touches, touch_tolerance_pct, pivot_span):
    """
    給定已經確認符合三角收斂的窗口大小,回傳「哪一邊的趨勢線摸到 >= min_touches
    個樞紐點」:(touched_low, touched_high),分別對應下方支撐線、上方壓力線。
    後續驗證真正突破/跌破時,方向要跟摸到的那一邊一致才算數(例如要突破,
    上方壓力線本身要摸到足夠的點;要跌破,下方支撐線本身要摸到足夠的點)。
    """
    segment = closed[-window:]
    avg_price = statistics.mean(float(k["close"]) for k in segment)
    tolerance_abs = touch_tolerance_pct * avg_price

    low_pivots = find_pivots(segment, "low", pivot_span)
    high_pivots = find_pivots(segment, "high", pivot_span)

    low_line = linear_regression(low_pivots)
    high_line = linear_regression(high_pivots)

    touches_low = count_trendline_touches(low_pivots, low_line, tolerance_abs)
    touches_high = count_trendline_touches(high_pivots, high_line, tolerance_abs)

    return touches_low >= min_touches, touches_high >= min_touches


def check_breakout_confirmation(closed, range_high, range_low, mavol_period, vol_multiplier):
    """
    驗證「最新連續三根K線」是不是真的確認站穩在指定區間(range_low,
    range_high)之外:
      - 第一根(真正衝出區間的那一根)要帶量:成交量 > vol_multiplier 倍的
        MAVOL
      - 接下來兩根不需要帶量,但收盤價、最低價(突破時)/最高價(跌破時)
        都要維持在區間外,只要中途有一根跌回/漲回區間內就不算數

    回傳 (方向, 幅度%, 最新收盤價),方向為 "breakout" / "breakdown" / None。
    幅度%與最新收盤價都是用最後一根(第三根)的收盤價計算。
    """
    if len(closed) < 3 + mavol_period:
        return None, None, None

    breakout_candle = closed[-3]   # 第一根:真正衝出區間、需要帶量
    confirm1 = closed[-2]          # 第二根:延續確認,不需帶量
    confirm2 = closed[-1]          # 第三根:再次確認,不需帶量
    history = closed[:-3]

    mavol_candles = history[-mavol_period:]
    mavol = statistics.mean(float(k["volume"]) for k in mavol_candles)
    if mavol <= 0:
        return None, None, None

    b_close = float(breakout_candle["close"])
    b_low = float(breakout_candle["low"])
    b_high = float(breakout_candle["high"])
    b_volume = float(breakout_candle["volume"])
    c1_close = float(confirm1["close"])
    c1_low = float(confirm1["low"])
    c1_high = float(confirm1["high"])
    c2_close = float(confirm2["close"])
    c2_low = float(confirm2["low"])
    c2_high = float(confirm2["high"])

    vol_ok = b_volume > vol_multiplier * mavol
    if not vol_ok:
        return None, None, None

    breakout_ok = (
        b_close > range_high and b_low > range_high
        and c1_close > range_high and c1_low > range_high
        and c2_close > range_high and c2_low > range_high
    )
    if breakout_ok:
        pct = (c2_close - range_high) / range_high * 100
        return "breakout", pct, c2_close

    breakdown_ok = (
        b_close < range_low and b_high < range_low
        and c1_close < range_low and c1_high < range_low
        and c2_close < range_low and c2_high < range_low
    )
    if breakdown_ok:
        pct = (range_low - c2_close) / range_low * 100
        return "breakdown", pct, c2_close

    return None, None, None


def detect_failed_reversal(closed, range_high, range_low):
    """
    判斷「最新這根K線」是不是剛好從區間外折返回區間內:上一根收盤價還在
    區間外,最新這根收盤價卻已經回到區間內。用來標記一次「突破嘗試失敗」。
    只看收盤價、只比較最新兩根,確保同一次折返只會被偵測到一次。
    """
    if len(closed) < 2:
        return False
    prev_close = float(closed[-2]["close"])
    latest_close = float(closed[-1]["close"])
    prev_outside = prev_close > range_high or prev_close < range_low
    latest_inside = range_low <= latest_close <= range_high
    return prev_outside and latest_inside


def get_pattern_matches_for_interval(session, interval, candidates, config, now_ms):
    """
    針對指定週期(4H 或 1D),掃描所有候選幣種,找出目前正處於「三角收斂」或
    「盤整狀態」的標的(不管有沒有突破),回傳 list of dict,每筆記錄型態種類
    與追蹤區間。
    """
    interval_ms = INTERVAL_MS[interval]
    fetch_limit = config["lookback_candles"] + 10
    matches = []

    for symbol, base_currency in candidates:
        try:
            klines = get_klines(session, symbol, interval, fetch_limit)
        except Exception as e:
            print(f"[警告] 取得 {symbol} {interval} K 線失敗:{e}")
            continue
        finally:
            time.sleep(config["request_sleep_sec"])

        closed = get_closed_klines(klines, interval_ms, now_ms)
        if not closed:
            continue

        triangle_window = find_max_triangle_window(
            closed, config["triangle_min_window"], config["lookback_candles"],
            config["triangle_convergence_ratio"], config["triangle_min_range_pct"],
            config["triangle_min_touches"], config["triangle_touch_tolerance_pct"],
            config["triangle_pivot_span"],
        )
        if triangle_window is not None:
            high, low = get_triangle_apex_range(closed, triangle_window)
            touched_low, touched_high = get_triangle_touched_sides(
                closed, triangle_window, config["triangle_min_touches"],
                config["triangle_touch_tolerance_pct"], config["triangle_pivot_span"],
            )
            matches.append({
                "symbol": symbol, "base": base_currency, "pattern_type": "triangle",
                "window": triangle_window, "range_high": high, "range_low": low,
                "touched_low": touched_low, "touched_high": touched_high,
            })

        consolidation = find_consolidation_range(
            closed, config["min_pattern_window"], config["lookback_candles"],
            config["max_consolidation_ratio"],
        )
        if consolidation is not None:
            matches.append({
                "symbol": symbol, "base": base_currency, "pattern_type": "consolidation",
                "window": consolidation["window"],
                "range_high": consolidation["high"], "range_low": consolidation["low"],
            })

    return matches


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
    watchlist = state.get("watchlist", {})

    session = requests.Session()

    # 分別問 4H / 1D / 1H 各自「最新收盤那一根」是不是比上次記錄的更新
    due_flags = {}
    latest_boundary_times = {}
    for interval in DUE_CHECK_INTERVALS:
        latest_time = get_latest_closed_candle_time(session, REFERENCE_SYMBOL, interval, now_ms)
        latest_boundary_times[interval] = latest_time
        boundary_key = STATE_BOUNDARY_KEYS[interval]
        prev_time = state.get(boundary_key)
        due = latest_time is not None and (prev_time is None or latest_time > prev_time)
        due_flags[interval] = due

    print(
        f"本次執行時間點:{run_start_taipei.strftime('%Y-%m-%d %H:%M:%S')} UTC+8,"
        f"4H新收盤:{due_flags['4H']},1D新收盤:{due_flags['1D']},1H新收盤:{due_flags['60M']}"
    )

    candidates = None  # 只在真的需要時(4H 或 1D 有新收盤)才去抓候選清單,節省 API

    def ensure_candidates():
        nonlocal candidates
        if candidates is not None:
            return candidates
        symbols_map = get_perp_symbols()
        tickers = get_perp_tickers()
        crypto_only = {
            symbol: info
            for symbol, info in symbols_map.items()
            if not is_excluded_asset(info, config)
        }
        excluded_count = len(symbols_map) - len(crypto_only)
        print(f"排除非加密貨幣資產(美股代幣/貴金屬等)數量:{excluded_count} / {len(symbols_map)}")

        result = []
        for symbol, info in crypto_only.items():
            ticker = tickers.get(symbol)
            if not ticker:
                continue
            try:
                amount_24h = float(ticker.get("amount", 0))
            except (TypeError, ValueError):
                continue
            if amount_24h > config["min_24h_amount_usdt"]:
                result.append((symbol, info["base"]))
        print(f"通過 24 小時成交金額篩選的幣種數量:{len(result)} / {len(crypto_only)}")
        candidates = result
        return candidates

    # ---- 第一階段:4H / 1D 掃描,更新追蹤名單 ----
    for interval in SCAN_INTERVALS:
        if not due_flags[interval]:
            continue
        cand = ensure_candidates()
        matches = get_pattern_matches_for_interval(session, interval, cand, config, now_ms)
        for m in matches:
            key = f"{m['symbol']}|{interval}|{m['pattern_type']}"
            existing = watchlist.get(key)
            if existing:
                # 型態持續存在(還沒突破也還沒失敗折返):更新最新的區間範圍,
                # 但保留已經累積的失敗次數跟原本的追蹤起始時間,不要因為每次
                # 重新掃到同一個型態就重置計時
                watchlist[key] = {
                    **existing,
                    "window": m["window"],
                    "range_high": m["range_high"],
                    "range_low": m["range_low"],
                    "touched_low": m.get("touched_low"),
                    "touched_high": m.get("touched_high"),
                }
            else:
                watchlist[key] = {
                    "symbol": m["symbol"],
                    "base": m["base"],
                    "source_interval": interval,
                    "pattern_type": m["pattern_type"],
                    "window": m["window"],
                    "range_high": m["range_high"],
                    "range_low": m["range_low"],
                    "touched_low": m.get("touched_low"),
                    "touched_high": m.get("touched_high"),
                    "added_ms": now_ms,
                    "fail_count": 0,
                }
        print(f"[{interval}] 找到 {len(matches)} 個型態,更新追蹤名單(目前共 {len(watchlist)} 個)")

    # ---- 清掉追蹤太久還沒驗證出結果的標的 ----
    max_age_ms = config["watchlist_max_age_hours"] * 3600 * 1000
    expired_keys = [k for k, v in watchlist.items() if now_ms - v.get("added_ms", now_ms) > max_age_ms]
    for k in expired_keys:
        del watchlist[k]
    if expired_keys:
        print(f"移除 {len(expired_keys)} 個追蹤過久仍未驗證出結果的標的")

    # ---- 第二階段:1H 驗證追蹤名單裡的標的是否真正突破/跌破 ----
    confirmed_events = []
    if due_flags["60M"] and watchlist:
        interval_ms_1h = INTERVAL_MS[CONFIRM_INTERVAL]
        fetch_limit_1h = config["mavol_period"] + 12
        resolved_keys = []

        for key, entry in list(watchlist.items()):
            try:
                klines = get_klines(session, entry["symbol"], CONFIRM_INTERVAL, fetch_limit_1h)
            except Exception as e:
                print(f"[警告] 取得 {entry['symbol']} 1H K線失敗:{e}")
                continue
            finally:
                time.sleep(config["request_sleep_sec"])

            closed_1h = get_closed_klines(klines, interval_ms_1h, now_ms)
            direction, pct, close_price = check_breakout_confirmation(
                closed_1h, entry["range_high"], entry["range_low"],
                config["mavol_period"], config["breakout_vol_multiplier"],
            )

            if direction is not None and entry["pattern_type"] == "triangle":
                # 三角收斂:突破/跌破的方向要跟「摸到 >=3 個點的那一邊趨勢線」
                # 一致,才算真正確認。例如要算突破,壓力線(上方)本身就要
                # 摸到足夠的點;要算跌破,支撐線(下方)本身要摸到足夠的點。
                side_ok = (
                    (direction == "breakout" and entry.get("touched_high"))
                    or (direction == "breakdown" and entry.get("touched_low"))
                )
                if not side_ok:
                    action_label = "突破" if direction == "breakout" else "跌破"
                    print(
                        f"[1H驗證] {entry['base']} 雖然出現{action_label}訊號,"
                        f"但摸到足夠點數的趨勢線不是{action_label}的那一邊,不算數,"
                        f"視為一次失敗,重新追蹤 {config['watchlist_max_age_hours']} 小時"
                    )
                    entry["fail_count"] = entry.get("fail_count", 0) + 1
                    entry["added_ms"] = now_ms
                    watchlist[key] = entry
                    continue  # 已經計為一次失敗,這輪不用再檢查折返

            if direction is not None:
                confirmed_events.append({
                    "base": entry["base"],
                    "direction": direction,
                    "pct": pct,
                    "close": close_price,
                    "source_interval": entry["source_interval"],
                    "pattern_type": entry["pattern_type"],
                    "window": entry["window"],
                    "fail_count": entry.get("fail_count", 0),
                })
                resolved_keys.append(key)
                continue

            # 沒有確認突破/跌破:檢查是不是「曾經衝出區間、但又折返回區間內」
            # 的失敗嘗試,如果是,失敗次數+1,並重新從現在開始追蹤5天
            if detect_failed_reversal(closed_1h, entry["range_high"], entry["range_low"]):
                entry["fail_count"] = entry.get("fail_count", 0) + 1
                entry["added_ms"] = now_ms
                watchlist[key] = entry
                print(f"[1H驗證] {entry['base']} 曾嘗試衝出區間但折返失敗,"
                      f"累計失敗 {entry['fail_count']} 次,重新追蹤 {config['watchlist_max_age_hours']} 小時")

        for k in resolved_keys:
            del watchlist[k]

        print(f"[1H驗證] 本次確認真正突破/跌破:{len(confirmed_events)} 個,追蹤名單剩餘 {len(watchlist)} 個")

    # ---- 存檔 ----
    for interval in DUE_CHECK_INTERVALS:
        if due_flags[interval]:
            state[STATE_BOUNDARY_KEYS[interval]] = latest_boundary_times[interval]
    state["watchlist"] = watchlist
    state["last_run_utc"] = datetime.now(timezone.utc).isoformat()
    state["last_confirmed_count"] = len(confirmed_events)
    save_json(STATE_FILE, state)

    # 真正突破或跌破,都要推播通知
    if not confirmed_events:
        print("本次沒有追蹤名單標的確認真正突破/跌破,不發送通知。")
        return

    now_taipei_str = run_start_taipei.strftime("%Y-%m-%d %H:%M")
    lines = [
        f"📐 Pionex 型態確認突破快訊 ({now_taipei_str} UTC+8)",
        "流程:4H/日線先抓出三角收斂或盤整中的標的,",
        "1H連續三根K線(第一根需帶量衝出區間,後兩根不用帶量但要站穩)才算真正突破/跌破。",
    ]

    for e in confirmed_events:
        interval_ms_src = INTERVAL_MS[e["source_interval"]]
        window_label = format_window_label(e["window"], interval_ms_src)
        pattern_name = PATTERN_TYPE_NAMES.get(e["pattern_type"], e["pattern_type"])
        source_label = INTERVAL_LABELS.get(e["source_interval"], {}).get("short", e["source_interval"])
        action = "突破" if e["direction"] == "breakout" else "跌破"
        fail_note = f"(已{action}失敗{e['fail_count']}次)" if e.get("fail_count", 0) > 0 else ""
        lines.append(
            f"{e['base'].lower()}:{pattern_name}({source_label},{window_label}) "
            f"1H確認{action} {e['pct']:.2f}%(現價{format_price(e['close'])}){fail_note}"
        )

    message = "\n".join(lines)
    print(message)
    send_telegram_message(message)


if __name__ == "__main__":
    main()
