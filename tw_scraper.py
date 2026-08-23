# -*- coding: utf-8 -*-
"""
台股財報爬蟲核心：MOPS 法說會／公告快易查 M31 財報日曆、
證交所／櫃買損益表與行情、FinMind 與 Yahoo 共識預估備援。
"""
import io
import json
import os
import random
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta, timezone

import numpy as np
import pandas as pd
import requests
import urllib3
import yfinance as yf
from requests.exceptions import SSLError

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_FILE = os.path.join(BASE_DIR, "earnings_data.json")
LOG_FILE = os.path.join(BASE_DIR, "DailyLog.txt")

KEEP_DAYS = 30
PAST_REPORT_DAYS = 20
MAX_WORKERS = 5
GAP_LOOKAHEAD_DAYS = 10
GAP_LOOKBACK_BARS = 3
GAP_THRESHOLD_PCT = 2.0

MOPS_CONFERENCE_URL = "https://mopsov.twse.com.tw/mops/web/ajax_t100sb02_1"
MOPS_EZSEARCH_URL = "https://mopsov.twse.com.tw/mops/web/ezsearch_query"
MOPS_INCOME_URL = "https://mopsov.twse.com.tw/mops/web/ajax_t163sb04"
FINMIND_URL = "https://api.finmindtrade.com/api/v4/data"
TWSE_NEWS_URL = "https://openapi.twse.com.tw/v1/opendata/t187ap04_L"
TPEX_NEWS_URL = "https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap04_O"
TWSE_QUOTE_URL = "https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL"
TWSE_RATIO_URL = "https://openapi.twse.com.tw/v1/exchangeReport/BWIBBU_ALL"
TPEX_QUOTE_URL = "https://www.tpex.org.tw/openapi/v1/tpex_mainboard_quotes"
TPEX_RATIO_URL = "https://www.tpex.org.tw/openapi/v1/tpex_mainboard_peratio_analysis"
TWSE_COMPANY_URL = "https://openapi.twse.com.tw/v1/opendata/t187ap03_L"
TPEX_COMPANY_URL = "https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap03_O"
YAHOO_CONSENSUS_LOOKBACK_DAYS = 7
TWSE_INCOME_URLS = (
    "https://openapi.twse.com.tw/v1/opendata/t187ap06_L_ci",
    "https://openapi.twse.com.tw/v1/opendata/t187ap06_L_basi",
    "https://openapi.twse.com.tw/v1/opendata/t187ap06_L_ins",
    "https://openapi.twse.com.tw/v1/opendata/t187ap06_L_fh",
    "https://openapi.twse.com.tw/v1/opendata/t187ap06_L_bd",
    "https://openapi.twse.com.tw/v1/opendata/t187ap06_L_mim",
)
TPEX_INCOME_URLS = (
    "https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap06_O_ci",
    "https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap06_O_basi",
    "https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap06_O_ins",
    "https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap06_O_fh",
    "https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap06_O_bd",
    "https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap06_O_mim",
)
EZSEARCH_PAGE_LIMIT = 1000
EZSEARCH_CHUNK_DAYS = 7
M31_LOOKBACK_EXTRA_DAYS = 14

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

ROC_DATE_RE = re.compile(
    r"(?P<y>1[0-9]{2})[./年\-](?P<m>\d{1,2})[./月\-](?P<d>\d{1,2})"
)
ISO_DATE_RE = re.compile(
    r"(?P<y>20\d{2})[./\-](?P<m>\d{1,2})[./\-](?P<d>\d{1,2})"
)
COMPACT_ROC_RE = re.compile(r"^(?P<y>1[0-9]{2})(?P<m>\d{2})(?P<d>\d{2})$")
TIME_RE = re.compile(r"(?P<h>\d{1,2})\s*[:：時]\s*(?P<min>\d{2})")
EPS_LINE_RE = re.compile(
    r"基本每股盈餘(?:\s*\(損失\))?[^\d\-]{0,24}(?P<eps>-?\d+(?:\.\d+)?)"
)

FIN_TITLE_KEYS = (
    "財務報告",
    "財務報表",
    "合併財報",
    "合併財務",
    "自結財務",
    "自結盈餘",
    "自結數",
    "第1季財務",
    "第2季財務",
    "第3季財務",
    "第4季財務",
    "第一季財務",
    "第二季財務",
    "第三季財務",
    "第四季財務",
    "年度財務",
)
BOARD_TITLE_KEYS = ("董事會預計召開", "預計召開董事會", "召開董事會")
CONF_TITLE_KEYS = ("法人說明會", "法說會", "投資人說明會")
SCHEDULE_TITLE_KEYS = ("預計召開", "召開日期", "預計於", "將於", "訂於")
PUBLISHED_TITLE_KEYS = ("通過", "決議通過", "自結", "提報董事會")

REVENUE_ROW_NAMES = (
    "Total Revenue",
    "Operating Revenue",
    "Revenue",
    "TotalRevenue",
    "OperatingRevenue",
)
GROSS_PROFIT_ROW_NAMES = ("Gross Profit", "GrossProfit", "Gross Profit Combined")
COGS_ROW_NAMES = (
    "Cost Of Revenue",
    "Cost of Revenue",
    "Reconciled Cost Of Revenue",
    "CostOfRevenue",
)
NII_ROW_NAMES = ("Net Interest Income", "NetInterestIncome")


def parse_num(val):
    """
    將欄位值轉成浮點數。
    @param {*} val - 原始值
    @returns {number|null} 解析結果；缺值則為 None
    """
    if val is None:
        return None
    if isinstance(val, (int, float)):
        if pd.isna(val):
            return None
        return round(float(val), 2)
    text = str(val).strip().replace(",", "").replace("%", "").replace("+", "")
    if text in ("", "-", "—", "–", "--", "nan", "None", "N/A", "null"):
        return None
    try:
        return round(float(text), 2)
    except (TypeError, ValueError):
        return None


def is_missing_num(val):
    """
    判斷數值欄位是否缺值。
    @param {*} val - 欄位值
    @returns {boolean} 是否缺值
    """
    return parse_num(val) is None


def is_blank(val):
    """
    判斷顯示欄位是否為空白或缺值（含已格式化的市值字串）。
    @param {*} val - 欄位值
    @returns {boolean} 是否空白
    """
    if val is None:
        return True
    text = str(val).strip()
    return text in ("", "-", "nan", "None", "N/A", "null", "—", "–", "--")


def parse_shares(val):
    """
    解析發行股數（不四捨五入到兩位）。
    @param {*} val - 原始股數
    @returns {number|null} 股數
    """
    num = _to_finite_float(val)
    if num is None:
        text = str(val or "").strip().replace(",", "")
        if not text or text in ("-", "nan", "None"):
            return None
        try:
            num = float(text)
        except (TypeError, ValueError):
            return None
    if num <= 0 or num != num or num in (float("inf"), float("-inf")):
        return None
    return num


def parse_par_value(val):
    """
    從「新台幣 10.0000元」等字串取出面額，預設 10。
    @param {*} val - 面額欄位
    @returns {number} 每股面額
    """
    match = re.search(r"(\d+(?:\.\d+)?)", str(val or ""))
    if not match:
        return 10.0
    num = _to_finite_float(match.group(1))
    return num if num and num > 0 else 10.0


def _to_finite_float(val):
    """
    將值轉成有限浮點數。
    @param {*} val - 原始值
    @returns {number|null} 轉換結果
    """
    if val is None:
        return None
    if isinstance(val, (pd.Series, pd.DataFrame)):
        try:
            cleaned = val.dropna()
            if cleaned.empty:
                return None
            val = cleaned.iloc[0]
        except Exception:
            return None
    try:
        if pd.isna(val):
            return None
    except Exception:
        pass
    try:
        num = float(val)
    except (TypeError, ValueError):
        return None
    if num != num or num in (float("inf"), float("-inf")):
        return None
    return num


def pick(row, *names):
    """
    從 dict 依多組鍵名取值（含去空白鍵名）。
    @param {object} row - 資料列
    @returns {*} 第一個非空值
    """
    if not isinstance(row, dict):
        return None
    stripped = {str(key).strip(): value for key, value in row.items()}
    for name in names:
        if name in row and row[name] not in (None, ""):
            return row[name]
        key = str(name).strip()
        if key in stripped and stripped[key] not in (None, ""):
            return stripped[key]
    return None


def roc_year(gregorian_year):
    """
    西元年轉民國年。
    @param {number} gregorian_year
    @returns {number} 民國年
    """
    return int(gregorian_year) - 1911


def parse_roc_date(text):
    """
    解析民國或西元日期字串，若為區間則取起始日。
    @param {*} text - 例如 115/08/17、115/08/11 至 115/08/12
    @returns {datetime.date|null} 日期
    """
    if text is None or (isinstance(text, float) and pd.isna(text)):
        return None
    raw = str(text).strip()
    if not raw or raw in ("-", "nan", "None"):
        return None
    raw = raw.replace("至", " ").replace("~", " ").replace("－", " ")
    token = raw.split()[0]
    compact = COMPACT_ROC_RE.match(token.replace("/", "").replace("-", ""))
    if compact and len(token.replace("/", "").replace("-", "")) == 7:
        year = int(compact.group("y")) + 1911
        try:
            return date(year, int(compact.group("m")), int(compact.group("d")))
        except ValueError:
            return None
    match = ROC_DATE_RE.search(raw)
    if match:
        year = int(match.group("y")) + 1911
        try:
            return date(year, int(match.group("m")), int(match.group("d")))
        except ValueError:
            return None
    match = ISO_DATE_RE.search(raw)
    if match:
        try:
            return date(int(match.group("y")), int(match.group("m")), int(match.group("d")))
        except ValueError:
            return None
    try:
        return datetime.strptime(token[:10], "%Y-%m-%d").date()
    except ValueError:
        return None


def parse_clock(text):
    """
    解析法說會時間為 HH:MM。
    @param {*} text - 原始時間
    @returns {string|null} 24 小時制時間
    """
    if text is None or (isinstance(text, float) and pd.isna(text)):
        return None
    match = TIME_RE.search(str(text))
    if not match:
        return None
    hour = int(match.group("h"))
    minute = int(match.group("min"))
    if hour > 23 or minute > 59:
        return None
    return f"{hour:02d}:{minute:02d}"


def fill_if_missing(data, key, value):
    """
    僅在目標欄位缺值時寫入新值。
    @param {object} data
    @param {string} key
    @param {*} value
    @returns {boolean} 是否寫入
    """
    if value in (None, "", "-", "nan"):
        return False
    if key in ("name", "market", "event_type", "date_source", "time", "clock", "market_cap", "yahoo_symbol"):
        current = data.get(key)
        if is_blank(current) or current == "不確定":
            data[key] = value
            return True
        return False
    if is_missing_num(data.get(key)):
        data[key] = value
        return True
    return False


def is_scheduled_title(text):
    """
    判斷主旨是否為預計召開／召開日期預告。
    @param {string} text
    @returns {boolean}
    """
    blob = str(text or "")
    if any(key in blob for key in PUBLISHED_TITLE_KEYS) and "預計" not in blob:
        return False
    return any(key in blob for key in SCHEDULE_TITLE_KEYS)


def is_published_title(text):
    """
    判斷主旨是否為財報已通過／自結公告。
    @param {string} text
    @returns {boolean}
    """
    blob = str(text or "")
    return any(key in blob for key in PUBLISHED_TITLE_KEYS) and "預計" not in blob


def chinese_quarter(token):
    """
    將中文或數字季別轉成 1–4。
    @param {string} token
    @returns {number|null}
    """
    mapping = {"1": 1, "2": 2, "3": 3, "4": 4, "一": 1, "二": 2, "三": 3, "四": 4}
    return mapping.get(str(token or "").strip())


def parse_report_quarter(text, fallback_date=None):
    """
    從主旨判斷財報所屬西元年與季別。
    「115年度第二季」視為第 2 季，不可誤判成年報。
    @param {string} text
    @param {datetime.date|null} fallback_date
    @returns {tuple|null} (西元年, 季)
    """
    blob = str(text or "").replace("\r", " ").replace("\n", " ")
    season = None
    q_match = re.search(r"第\s*([1234一二三四])\s*季|Q\s*([1234])", blob, re.IGNORECASE)
    if q_match:
        season = chinese_quarter(q_match.group(1) or q_match.group(2))
    year = None
    y_match = re.search(r"(?:民國)?(1[0-9]{2}|20\d{2})\s*(?:年度|年)", blob)
    if y_match:
        year = int(y_match.group(1))
        if year < 1911:
            year += 1911
    if season is None and re.search(r"年報|全年財務|年度財務", blob) and "季" not in blob:
        season = 4
    if year is None and fallback_date is not None and season is not None:
        year = fallback_date.year
        if season == 4 and fallback_date.month in (1, 2, 3):
            year = fallback_date.year - 1
    if year is None or season is None:
        return None
    return (year, season)


def infer_report_quarter(event_date, title=""):
    """
    依主旨或公告月份推估財報季別。
    @param {datetime.date|null} event_date
    @param {string} title
    @returns {tuple|null} (西元年, 季)
    """
    parsed = parse_report_quarter(title, event_date)
    if parsed:
        return parsed
    if event_date is None:
        return None
    month = event_date.month
    if month in (1, 2, 3):
        return (event_date.year - 1, 4)
    if month in (4, 5, 6):
        return (event_date.year, 1)
    if month in (7, 8, 9):
        return (event_date.year, 2)
    return (event_date.year, 3)


def ytd_to_quarterly(current_ytd, prev_ytd, season):
    """
    將 MOPS 累計 EPS／營收轉成單季數字。
    @param {number|null} current_ytd
    @param {number|null} prev_ytd
    @param {number} season
    @returns {number|null}
    """
    current_ytd = parse_num(current_ytd)
    if current_ytd is None:
        return None
    if int(season) == 1:
        return current_ytd
    prev_ytd = parse_num(prev_ytd)
    if prev_ytd is None:
        return None
    return round(current_ytd - prev_ytd, 2)


def iter_date_chunks(start_day, end_day, chunk_days=EZSEARCH_CHUNK_DAYS):
    """
    將日期區間切成多段，避免 ezsearch 1000 筆上限截斷。
    @param {datetime.date} start_day
    @param {datetime.date} end_day
    @param {number} chunk_days
    @returns {Array<tuple>}
    """
    chunks = []
    cursor = start_day
    while cursor <= end_day:
        chunk_end = min(cursor + timedelta(days=chunk_days - 1), end_day)
        chunks.append((cursor, chunk_end))
        cursor = chunk_end + timedelta(days=1)
    return chunks


def classify_session(clock, fallback="不確定"):
    """
    依時間判斷盤前／盤中／盤後。
    @param {string|null} clock - HH:MM
    @param {string} fallback - 無法判斷時的預設
    @returns {string} 時段標籤
    """
    if not clock:
        return fallback
    hour, minute = (int(x) for x in clock.split(":"))
    minutes = hour * 60 + minute
    if minutes < 9 * 60:
        return "盤前"
    if minutes >= 13 * 60 + 30:
        return "盤後"
    return "盤中"


def format_market_cap(val):
    """
    將市值格式化為 T/B/M。
    @param {*} val - 數值或字串
    @returns {string} 顯示字串
    """
    num = parse_num(val) if not isinstance(val, (int, float)) else _to_finite_float(val)
    if num is None:
        return "-" if val in (None, "", "nan") else str(val)
    abs_num = abs(num)
    if abs_num >= 1e12:
        return f"{num / 1e12:.2f}T"
    if abs_num >= 1e9:
        return f"{num / 1e9:.2f}B"
    if abs_num >= 1e6:
        return f"{num / 1e6:.2f}M"
    return str(round(num, 2))


def record_key(item):
    """
    以股票代碼與日期組成唯一鍵。
    @param {object} item - 單筆紀錄
    @returns {tuple} (symbol, date)
    """
    return (str(item.get("symbol") or "").strip(), str(item.get("date") or "").strip())


def yahoo_symbol_for(code, market):
    """
    依上市／上櫃組成 Yahoo 代號。
    @param {string} code - 四位數代號
    @param {string} market - 上市或上櫃
    @returns {string} Yahoo 代號
    """
    suffix = ".TWO" if market == "上櫃" else ".TW"
    return f"{code}{suffix}"


def yahoo_symbol_candidates(code, market=None):
    """
    產生 Yahoo 代號候選（正確市場優先，失敗時改試另一個後綴）。
    @param {string} code - 四位數代號
    @param {string|null} market - 上市或上櫃
    @returns {Array<string>}
    """
    primary = yahoo_symbol_for(code, market or "上市")
    alternate = f"{code}.TWO" if primary.endswith(".TW") else f"{code}.TW"
    ordered = [primary]
    if alternate not in ordered:
        ordered.append(alternate)
    return ordered


def _series_cell(series, col):
    """
    取出損益表某一列、某一期的數值。
    @param {pandas.Series|null} series
    @param {*} col
    @returns {number|null}
    """
    if series is None:
        return None
    try:
        return _to_finite_float(series[col])
    except Exception:
        return None


def _find_statement_row(df, names):
    """
    以多組欄位名稱找出損益表列。
    @param {pandas.DataFrame} df
    @param {Array<string>} names
    @returns {pandas.Series|null}
    """
    if df is None or getattr(df, "empty", True):
        return None
    index_map = {str(idx).strip().lower(): idx for idx in df.index}
    for name in names:
        key = name.strip().lower()
        if key in index_map:
            return df.loc[index_map[key]]
    return None


def _margin_pct(numer, denom):
    """
    計算百分比毛利率。
    @param {number|null} numer
    @param {number|null} denom
    @returns {number|null}
    """
    numer = _to_finite_float(numer)
    denom = _to_finite_float(denom)
    if numer is None or denom is None or denom == 0:
        return None
    return round(numer / denom * 100, 2)


def _margin_from_info(info):
    """
    從 yfinance info 取 TTM 毛利率。
    @param {object|null} info
    @returns {number|null}
    """
    if not info:
        return None
    gm = _to_finite_float(info.get("grossMargins"))
    gp = _to_finite_float(info.get("grossProfits"))
    rev = _to_finite_float(info.get("totalRevenue"))
    gp_equals_rev = (
        gp is not None and rev is not None and rev != 0 and abs(gp - rev) / abs(rev) < 1e-6
    )
    if gp is not None and rev is not None and rev > 0 and not gp_equals_rev:
        computed = _margin_pct(gp, rev)
        if computed is not None:
            return computed
    if gm is not None:
        bank_placeholder = gm == 0 and (rev is None or gp is None or gp_equals_rev)
        if not bank_placeholder:
            return round(gm * 100, 2)
    return None


def _margin_from_statement(df):
    """
    從損益表計算毛利率。
    @param {pandas.DataFrame|null} df
    @returns {number|null}
    """
    if df is None or getattr(df, "empty", True):
        return None
    rev_row = _find_statement_row(df, REVENUE_ROW_NAMES)
    if rev_row is None:
        return None
    gp_row = _find_statement_row(df, GROSS_PROFIT_ROW_NAMES)
    cogs_row = _find_statement_row(df, COGS_ROW_NAMES)
    nii_row = _find_statement_row(df, NII_ROW_NAMES)
    for col in df.columns:
        rev = _series_cell(rev_row, col)
        if rev is None or rev == 0:
            continue
        gp = _series_cell(gp_row, col)
        if gp is not None:
            return _margin_pct(gp, rev)
        cogs = _series_cell(cogs_row, col)
        if cogs is not None:
            return _margin_pct(rev - cogs, rev)
    for col in df.columns:
        rev = _series_cell(rev_row, col)
        nii = _series_cell(nii_row, col)
        if rev is None or rev == 0 or nii is None:
            continue
        return _margin_pct(nii, rev)
    return None


def get_gross_margin_pct(stock, info=None):
    """
    取得最新毛利率（%）。
    @param {yfinance.Ticker} stock
    @param {object|null} info
    @returns {number|string}
    """
    try:
        if info is None:
            try:
                info = stock.info or {}
            except Exception:
                info = {}
        val = _margin_from_info(info)
        if val is not None:
            return val
        for attr in ("income_stmt", "quarterly_income_stmt", "financials", "quarterly_financials"):
            try:
                df = getattr(stock, attr)
            except Exception:
                continue
            val = _margin_from_statement(df)
            if val is not None:
                return val
        op = _to_finite_float((info or {}).get("operatingMargins"))
        if op is not None and op != 0:
            return round(op * 100, 2)
        return "-"
    except Exception:
        return "-"


def _extract_price_series(df, symbol, field):
    """
    從 yfinance download 結果取出價位序列。
    @param {pandas.DataFrame} df
    @param {string} symbol
    @param {string} field
    @returns {pandas.Series|null}
    """
    if df is None or df.empty:
        return None
    try:
        if isinstance(df.columns, pd.MultiIndex):
            level0 = list(df.columns.get_level_values(0))
            level1 = list(df.columns.get_level_values(1))
            if symbol in level0:
                sub = df[symbol]
                if field in sub.columns:
                    return sub[field]
            if field in level0 and symbol in level1:
                return df[field][symbol]
            return None
        if field in df.columns:
            return df[field]
    except Exception:
        return None
    return None


def _gaps_from_open_close(opens, closes):
    """
    以 ((今開 - 昨收) / 昨收) * 100 計算近幾個交易日跳空。
    @param {pandas.Series|null} opens
    @param {pandas.Series|null} closes
    @returns {string} 例如「8/3_3%」；無跳空則為 "-"
    """
    if opens is None or closes is None:
        return "-"
    try:
        frame = pd.concat({"Open": opens, "Close": closes}, axis=1).dropna()
    except Exception:
        return "-"
    if len(frame) < 2:
        return "-"
    frame = frame.tail(GAP_LOOKBACK_BARS + 1)
    labels = []
    for i in range(1, len(frame)):
        prev_close = float(frame["Close"].iloc[i - 1])
        curr_open = float(frame["Open"].iloc[i])
        if prev_close == 0:
            continue
        pct = (curr_open - prev_close) / prev_close * 100
        if abs(pct) >= GAP_THRESHOLD_PCT:
            idx = frame.index[i]
            bar_date = idx.date() if hasattr(idx, "date") else pd.Timestamp(idx).date()
            labels.append(f"{bar_date.month}/{bar_date.day}_{int(round(pct))}%")
    return " ".join(labels) if labels else "-"


def fetch_gap_map(symbols):
    """
    批次抓取各股近 3 日跳空（今開對昨收）。
    @param {Array<string>} symbols - Yahoo 代號
    @returns {object} 代號對應跳空字串
    """
    gap_map = {s: "-" for s in symbols}
    if not symbols:
        return gap_map
    chunk_size = 30
    for i in range(0, len(symbols), chunk_size):
        chunk = symbols[i:i + chunk_size]
        try:
            df = yf.download(
                tickers=chunk,
                period="10d",
                interval="1d",
                group_by="ticker",
                auto_adjust=False,
                threads=True,
                progress=False,
            )
            for symbol in chunk:
                gap_map[symbol] = _gaps_from_open_close(
                    _extract_price_series(df, symbol, "Open"),
                    _extract_price_series(df, symbol, "Close"),
                )
        except Exception as exc:
            print(f"⚠️ 批次抓取跳空失敗，改為逐檔補抓: {exc}")
            for symbol in chunk:
                try:
                    hist = yf.Ticker(symbol).history(period="10d", auto_adjust=False)
                    if hist is None or hist.empty:
                        gap_map[symbol] = "-"
                        continue
                    opens = hist["Open"] if "Open" in hist.columns else None
                    closes = hist["Close"] if "Close" in hist.columns else None
                    gap_map[symbol] = _gaps_from_open_close(opens, closes)
                except Exception:
                    gap_map[symbol] = "-"
        time.sleep(0.3)
    return gap_map


def update_gap_fields(records):
    """
    為今日至後 10 日的各股寫入跳空欄。
    @param {Array<object>} records
    @returns {number} 變更筆數
    """
    today = datetime.now().date()
    end_date = today + timedelta(days=GAP_LOOKAHEAD_DAYS)
    targets = []
    for item in records:
        try:
            item_date = datetime.strptime(item["date"], "%Y-%m-%d").date()
        except Exception:
            continue
        if today <= item_date <= end_date:
            targets.append(item)
    if not targets:
        return 0
    symbols = list(dict.fromkeys(
        item.get("yahoo_symbol") or yahoo_symbol_for(item["symbol"], item.get("market"))
        for item in targets
    ))
    print(f"📈 開始抓取今日至後 {GAP_LOOKAHEAD_DAYS} 日共 {len(symbols)} 檔近 {GAP_LOOKBACK_BARS} 日跳空")
    gap_map = fetch_gap_map(symbols)
    changed = 0
    hit = 0
    for item in targets:
        ysym = item.get("yahoo_symbol") or yahoo_symbol_for(item["symbol"], item.get("market"))
        new_gap = gap_map.get(ysym, "-")
        if new_gap != "-":
            hit += 1
        if item.get("gap") != new_gap:
            item["gap"] = new_gap
            changed += 1
    print(f"   🔍 發現跳空 {hit} 檔，更新 {changed} 筆")
    return changed


def merge_reported_fields(existing, incoming):
    """
    只合併 Reported EPS 與 Surprise (%)。
    @param {object} existing
    @param {object} incoming
    @returns {boolean} 是否有更新
    """
    changed = False
    for key in ("eps_reported", "surprise_pct"):
        new_val = incoming.get(key)
        if new_val not in (None, "-", "", "nan"):
            if existing.get(key) != new_val:
                existing[key] = new_val
                changed = True
        elif key not in existing:
            existing[key] = "-"
            changed = True
    return changed


def merge_event_type(old_type, new_type):
    """
    合併同一天的事件類型。
    @param {string} old_type
    @param {string} new_type
    @returns {string}
    """
    parts = []
    for text in (old_type, new_type):
        if not text:
            continue
        for piece in str(text).split("+"):
            piece = piece.strip()
            if piece and piece not in parts:
                parts.append(piece)
    return "+".join(parts) if parts else "財報"


class TwStockScraper:
    """台股財報日曆與個股預估爬蟲。"""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7",
            "Origin": "https://mopsov.twse.com.tw",
            "Referer": "https://mopsc.twse.com.tw/mops/web/ezsearch",
        })
        self.market_map = {}
        self.name_map = {}
        self.quote_map = {}
        self.ratio_map = {}
        self.shares_map = {}
        self.statement_map = {}
        self.yahoo_consensus_cache = {}
        self.finmind_token = os.environ.get("FINMIND_TOKEN") or os.environ.get("FINMIND_API_TOKEN") or ""

    def log(self, msg):
        """
        輸出帶時間戳的訊息。
        @param {string} msg
        """
        text = f"[{datetime.now().strftime('%H:%M:%S')}] {msg}"
        encoding = getattr(sys.stdout, "encoding", None) or "utf-8"
        try:
            print(text)
        except UnicodeEncodeError:
            print(text.encode(encoding, "replace").decode(encoding, "replace"))

    def warmup_mops(self):
        """先打開公開資訊觀測站頁面，取得工作階段。"""
        for url in (
            "https://mopsov.twse.com.tw/mops/web/t100sb02_1",
            "https://mopsc.twse.com.tw/mops/web/ezsearch",
        ):
            try:
                self.session.get(url, timeout=20)
            except Exception:
                pass

    def _sleep_polite(self, low=0.5, high=1.1):
        """
        對 MOPS 請求之間稍作間隔。
        @param {number} low
        @param {number} high
        """
        time.sleep(random.uniform(low, high))

    def json_get(self, url):
        """
        GET JSON 公開資料；櫃買 SSL 失敗時改不驗證憑證重試。
        @param {string} url
        @returns {Array|object|null}
        """
        last_exc = None
        for verify in (True, False):
            try:
                resp = self.session.get(url, timeout=30, verify=verify)
                resp.raise_for_status()
                return resp.json()
            except SSLError as exc:
                last_exc = exc
                continue
            except Exception as exc:
                self.log(f"⚠️ GET 失敗 {url}: {exc}")
                return None
        self.log(f"⚠️ GET 失敗 {url}: {last_exc}")
        return None

    def _post_json(self, url, payload, retries=3):
        """
        POST 並解析 UTF-8 BOM JSON，失敗時重試。
        @param {string} url
        @param {object} payload
        @param {number} retries
        @returns {object|null}
        """
        last_exc = None
        for attempt in range(retries):
            try:
                resp = self.session.post(url, data=payload, timeout=45)
                raw = resp.content.decode("utf-8-sig", errors="replace").strip()
                if not raw or raw[:1] == "<":
                    last_exc = RuntimeError(f"非 JSON 回應 len={len(raw)}")
                    time.sleep(1.2 * (attempt + 1))
                    continue
                return json.loads(raw)
            except Exception as exc:
                last_exc = exc
                time.sleep(1.2 * (attempt + 1))
        self.log(f"⚠️ POST JSON 失敗 {url}: {last_exc}")
        return None

    def _post_html(self, url, payload, retries=3):
        """
        POST 並取得 HTML 文字。
        @param {string} url
        @param {object} payload
        @param {number} retries
        @returns {string}
        """
        last_exc = None
        for attempt in range(retries):
            try:
                resp = self.session.post(url, data=payload, timeout=60)
                resp.encoding = "utf-8"
                if resp.status_code >= 500 or not resp.text:
                    last_exc = RuntimeError(f"HTTP {resp.status_code}")
                    time.sleep(1.2 * (attempt + 1))
                    continue
                return resp.text
            except Exception as exc:
                last_exc = exc
                time.sleep(1.2 * (attempt + 1))
        self.log(f"⚠️ POST HTML 失敗 {url}: {last_exc}")
        return ""

    def fetch_market_maps(self):
        """載入上市／上櫃名稱、收盤價、本益比與股價淨值比。"""
        twse_quotes = self.json_get(TWSE_QUOTE_URL) or []
        tpex_quotes = self.json_get(TPEX_QUOTE_URL) or []
        twse_ratio = self.json_get(TWSE_RATIO_URL) or []
        tpex_ratio = self.json_get(TPEX_RATIO_URL) or []

        for row in twse_quotes:
            code = str(pick(row, "Code", "證券代號") or "").strip()
            if not re.fullmatch(r"\d{4}", code):
                continue
            self.market_map[code] = "上市"
            name = pick(row, "Name", "證券名稱")
            if name:
                self.name_map[code] = str(name).strip()
            price = parse_num(pick(row, "ClosingPrice", "收盤價"))
            if price is not None:
                self.quote_map[code] = price

        for row in tpex_quotes:
            code = str(pick(row, "SecuritiesCompanyCode", "代號", "Code") or "").strip()
            if not re.fullmatch(r"\d{4}", code):
                continue
            self.market_map[code] = "上櫃"
            name = pick(row, "CompanyName", "名稱", "Name")
            if name:
                self.name_map[code] = str(name).strip()
            price = parse_num(pick(
                row,
                "Close",
                "ClosingPrice",
                "收盤",
                "收盤價",
                "LatestTradePrice",
            ))
            if price is not None:
                self.quote_map[code] = price

        for row in twse_ratio:
            code = str(pick(row, "Code", "證券代號") or "").strip()
            if not re.fullmatch(r"\d{4}", code):
                continue
            self.market_map.setdefault(code, "上市")
            name = pick(row, "Name", "證券名稱", "公司名稱")
            if name:
                self.name_map.setdefault(code, str(name).strip())
            self.ratio_map[code] = {
                "pe": parse_num(pick(row, "PEratio", "本益比")),
                "pb": parse_num(pick(row, "PBratio", "股價淨值比")),
            }

        for row in tpex_ratio:
            code = str(pick(row, "SecuritiesCompanyCode", "代號", "Code") or "").strip()
            if not re.fullmatch(r"\d{4}", code):
                continue
            self.market_map.setdefault(code, "上櫃")
            name = pick(row, "CompanyName", "名稱", "Name")
            if name:
                self.name_map.setdefault(code, str(name).strip())
            self.ratio_map[code] = {
                "pe": parse_num(pick(row, "PriceEarningRatio", "本益比", "PEratio")),
                "pb": parse_num(pick(row, "PriceBookRatio", "股價淨值比", "PBratio")),
            }

        self.fetch_share_maps()
        self.log(
            f"📊 行情對照：上市櫃 {len(self.market_map)} 檔，"
            f"收盤 {len(self.quote_map)}，本益/淨值 {len(self.ratio_map)}，"
            f"發行股數 {len(self.shares_map)}"
        )

    def _shares_from_company_row(self, row):
        """
        從公司基本資料列取出流通普通股股數。
        @param {object} row
        @returns {number|null} 股數
        """
        if not isinstance(row, dict):
            return None
        shares = parse_shares(pick(
            row,
            "已發行普通股數或TDR原股發行股數",
            "已發行普通股數或TDR原發行股數",
            "IssueShares",
        ))
        if shares:
            return shares
        for key, value in row.items():
            name = str(key)
            if "已發行普通股" in name or name == "IssueShares":
                shares = parse_shares(value)
                if shares:
                    return shares
        capital = parse_shares(pick(row, "實收資本額", "Paidin.Capital.NTDollars"))
        par = parse_par_value(pick(row, "普通股每股面額", "ParValueOfCommonStock"))
        if capital and par:
            return capital / par
        return None

    def fetch_share_maps(self):
        """載入上市／上櫃已發行普通股數，供市值＝股數×收盤價。"""
        sources = (
            (TWSE_COMPANY_URL, "上市", ("公司代號", "Code"), ("公司簡稱", "公司名稱", "Name")),
            (TPEX_COMPANY_URL, "上櫃", ("SecuritiesCompanyCode", "公司代號", "Code"), ("CompanyAbbreviation", "CompanyName", "公司簡稱", "公司名稱")),
        )
        for url, market, code_keys, name_keys in sources:
            rows = self.json_get(url) or []
            hit = 0
            if not isinstance(rows, list):
                continue
            for row in rows:
                code = str(pick(row, *code_keys) or "").strip()
                if not re.fullmatch(r"\d{4}", code):
                    continue
                shares = self._shares_from_company_row(row)
                if not shares:
                    continue
                self.shares_map[code] = shares
                self.market_map.setdefault(code, market)
                name = pick(row, *name_keys)
                if name:
                    self.name_map.setdefault(code, str(name).strip())
                hit += 1
            self.log(f"📦 {market}發行股數 {hit} 檔")

    def market_cap_for(self, code, price=None):
        """
        以官方股數×收盤價計算市值顯示字串。
        @param {string} code
        @param {number|null} price
        @returns {string|null}
        """
        shares = self.shares_map.get(code)
        if not shares:
            return None
        px = parse_num(price)
        if px is None:
            px = parse_num(self.quote_map.get(code))
        if px is None:
            return None
        return format_market_cap(shares * float(px))

    def apply_official_market_data(self, records):
        """
        用證交所／櫃買對照表校正市場別、Yahoo 代號、收盤價與市值。
        @param {Array<object>} records
        @returns {number} 寫入市值的筆數
        """
        filled = 0
        for item in records:
            code = str(item.get("symbol") or "").strip()
            if not re.fullmatch(r"\d{4}", code):
                continue
            if code in self.market_map:
                item["market"] = self.market_map[code]
                item["yahoo_symbol"] = yahoo_symbol_for(code, item["market"])
            elif is_blank(item.get("yahoo_symbol")):
                item["yahoo_symbol"] = yahoo_symbol_for(code, item.get("market") or "上市")
            if code in self.name_map and is_blank(item.get("name")):
                item["name"] = self.name_map[code]
            if code in self.quote_map:
                item["price"] = self.quote_map[code]
            cap = self.market_cap_for(code, item.get("price"))
            if cap:
                item["market_cap"] = cap
                filled += 1
        self.log(f"💰 官方股數×收盤價補上市值 {filled} 筆")
        return filled

    def _flatten_columns(self, df):
        """
        將 MultiIndex 欄名攤平成單一中文欄名。
        @param {pandas.DataFrame} df
        @returns {pandas.DataFrame}
        """
        columns = []
        for col in df.columns:
            if isinstance(col, tuple):
                parts = []
                for item in col:
                    text = str(item).strip()
                    if text and text != "nan" and text not in parts:
                        parts.append(text)
                columns.append(parts[0] if parts else "")
            else:
                columns.append(str(col).strip())
        df = df.copy()
        df.columns = columns
        return df

    def _col(self, df, *keywords):
        """
        依關鍵字找出欄位名稱。
        @param {pandas.DataFrame} df
        @returns {string|null}
        """
        for col in df.columns:
            name = str(col)
            if all(key in name for key in keywords):
                return col
        return None

    def fetch_conferences_month(self, roc_y, month, typek):
        """
        抓取單月法說會一覽。
        @param {number} roc_y - 民國年
        @param {number} month - 月
        @param {string} typek - sii 或 otc
        @returns {Array<object>}
        """
        market = "上市" if typek == "sii" else "上櫃"
        payload = {
            "encodeURIComponent": "1",
            "step": "1",
            "firstin": "1",
            "off": "1",
            "TYPEK": typek,
            "year": str(roc_y),
            "month": f"{int(month):02d}",
            "co_id": "",
        }
        try:
            resp = self.session.post(MOPS_CONFERENCE_URL, data=payload, timeout=45)
            resp.encoding = "utf-8"
            if "<table" not in resp.text.lower():
                return []
            dfs = pd.read_html(io.StringIO(resp.text))
        except Exception as exc:
            self.log(f"⚠️ 法說會 {market} {roc_y}/{month:02d} 失敗: {exc}")
            return []

        events = []
        for df in dfs:
            if df is None or df.empty or df.shape[1] < 3:
                continue
            df = self._flatten_columns(df)
            code_col = self._col(df, "公司代號") or self._col(df, "代號")
            name_col = self._col(df, "公司名稱") or self._col(df, "名稱")
            date_col = self._col(df, "日期")
            time_col = self._col(df, "時間")
            if not code_col or not date_col:
                continue
            for _, row in df.iterrows():
                code = str(row.get(code_col, "")).strip()
                if not re.fullmatch(r"\d{4}", code):
                    continue
                event_date = parse_roc_date(row.get(date_col))
                if event_date is None:
                    continue
                name = str(row.get(name_col, "")).strip() if name_col else ""
                clock = parse_clock(row.get(time_col) if time_col else None)
                self.market_map[code] = market
                if name:
                    self.name_map[code] = name
                events.append({
                    "date": event_date,
                    "symbol": code,
                    "name": name or self.name_map.get(code, ""),
                    "market": market,
                    "time": classify_session(clock),
                    "clock": clock,
                    "event_type": "法說會",
                    "date_source": "MOPS法說會",
                    "date_certainty": "確定",
                })
        self.log(f"📅 法說會 {market} {roc_y}/{month:02d}：{len(events)} 場")
        return events

    def fetch_conferences(self, months):
        """
        抓取多個民國年月的上市＋上櫃法說會。
        @param {Array<tuple>} months - [(民國年, 月), ...]
        @returns {Array<object>}
        """
        self.warmup_mops()
        events = []
        for roc_y, month in months:
            for typek in ("sii", "otc"):
                events.extend(self.fetch_conferences_month(roc_y, month, typek))
                self._sleep_polite()
        return events

    def fetch_ezsearch_chunk(self, pro_item, typek, start_day, end_day):
        """
        查詢公告快易查單一市場、單一公告代碼的日期區間。
        來源：MOPS ezsearch_query（GitHub Casualtek/Cyberwatch 同款）。
        @param {string} pro_item - 例如 M31、M12
        @param {string} typek - sii 或 otc
        @param {datetime.date} start_day
        @param {datetime.date} end_day
        @returns {Array<object>}
        """
        payload = {
            "step": "00",
            "RADIO_CM": "1",
            "TYPEK": typek,
            "PRO_ITEM": pro_item,
            "SDATE": start_day.strftime("%Y%m%d"),
            "EDATE": end_day.strftime("%Y%m%d"),
            "lang": "zh",
        }
        data = self._post_json(MOPS_EZSEARCH_URL, payload)
        if not data or data.get("status") != "success":
            return []
        rows = data.get("data") or []
        if len(rows) >= EZSEARCH_PAGE_LIMIT and (end_day - start_day).days > 0:
            mid = start_day + timedelta(days=(end_day - start_day).days // 2)
            self.log(
                f"⚠️ ezsearch {pro_item} {typek} {start_day}~{end_day} 達 {EZSEARCH_PAGE_LIMIT} 筆，改切半查"
            )
            left = self.fetch_ezsearch_chunk(pro_item, typek, start_day, mid)
            self._sleep_polite(0.7, 1.3)
            right = self.fetch_ezsearch_chunk(pro_item, typek, mid + timedelta(days=1), end_day)
            return left + right
        return rows

    def fetch_ezsearch_rows(self, pro_item, start_day, end_day):
        """
        分段抓取上市＋上櫃公告快易查。
        @param {string} pro_item
        @param {datetime.date} start_day
        @param {datetime.date} end_day
        @returns {Array<object>}
        """
        rows = []
        for typek in ("sii", "otc"):
            for chunk_start, chunk_end in iter_date_chunks(start_day, end_day):
                part = self.fetch_ezsearch_chunk(pro_item, typek, chunk_start, chunk_end)
                rows.extend(part)
                self.log(
                    f"🔎 快易查 {pro_item} {'上市' if typek == 'sii' else '上櫃'} "
                    f"{chunk_start}~{chunk_end}：{len(part)} 筆"
                )
                self._sleep_polite(0.6, 1.2)
        return rows

    def _event_from_ezsearch_row(self, row, default_type):
        """
        將公告快易查一列轉成日曆事件。
        @param {object} row
        @param {string} default_type
        @returns {object|null}
        """
        code = str(row.get("COMPANY_ID") or "").strip()
        if not re.fullmatch(r"\d{4}", code):
            return None
        title = str(row.get("SUBJECT") or "").replace("\r", "\n")
        announced = parse_roc_date(row.get("CDATE"))
        clock = parse_clock(row.get("CTIME"))
        typek = str(row.get("TYPEK") or "").lower()
        guessed_market = "上櫃" if typek == "otc" else "上市"
        market = self.market_map.get(code) or guessed_market
        name = str(row.get("COMPANY_NAME") or "").strip()
        event_type = default_type
        if any(key in title for key in CONF_TITLE_KEYS):
            event_type = "法說會"
        event_date = announced
        certainty = "確定"
        if is_scheduled_title(title):
            guessed = self._future_date_from_text(title, announced)
            if guessed:
                event_date = guessed
            else:
                certainty = "預告"
        elif is_published_title(title):
            event_date = announced
        if event_date is None:
            return None
        self.market_map.setdefault(code, market)
        if name:
            self.name_map[code] = name
        return {
            "date": event_date,
            "symbol": code,
            "name": name or self.name_map.get(code, ""),
            "market": market,
            "time": classify_session(
                clock,
                "盤後" if event_type == "財報" and is_published_title(title) else "不確定",
            ),
            "clock": clock,
            "event_type": event_type,
            "date_source": "MOPS快易查",
            "date_certainty": certainty,
            "title": title,
            "report_quarter": infer_report_quarter(event_date, title),
        }

    def fetch_m31_events(self, start_day, end_day):
        """
        抓取董事會決議財務報告／自結（M31），含預計召開日與已公告日。
        @param {datetime.date} start_day - 公告日起
        @param {datetime.date} end_day - 公告日迄
        @returns {Array<object>}
        """
        rows = self.fetch_ezsearch_rows("M31", start_day, end_day)
        events = []
        for row in rows:
            event = self._event_from_ezsearch_row(row, "財報")
            if event:
                events.append(event)
        self.log(f"📰 M31 財報相關事件 {len(events)} 筆（原始 {len(rows)}）")
        return events

    def _classify_news(self, title, body):
        """
        判斷重大訊息是否為財報／董事會／法說會。
        @param {string} title
        @param {string} body
        @returns {string|null} 事件類型
        """
        blob = f"{title}\n{body}"
        if any(key in blob for key in FIN_TITLE_KEYS):
            return "財報"
        if any(key in blob for key in CONF_TITLE_KEYS):
            return "法說會"
        if any(key in blob for key in BOARD_TITLE_KEYS):
            return "董事會"
        return None

    def _future_date_from_text(self, text, announced):
        """
        從公告內文抽出董事會預計召開日（優先取公告日之後）。
        @param {string} text
        @param {datetime.date|null} announced
        @returns {datetime.date|null}
        """
        candidates = []
        for match in ROC_DATE_RE.finditer(text or ""):
            try:
                parsed = date(
                    int(match.group("y")) + 1911,
                    int(match.group("m")),
                    int(match.group("d")),
                )
                candidates.append(parsed)
            except ValueError:
                continue
        if not candidates:
            return None
        if announced:
            later = [d for d in candidates if d >= announced]
            if later:
                return min(later)
        return candidates[0]

    def fetch_material_news_events(self):
        """
        從證交所／櫃買每日重大訊息抽出財報相關事件。
        @returns {Array<object>}
        """
        events = []
        sources = (
            (TWSE_NEWS_URL, "上市", ("公司代號", "Code"), ("公司名稱", "CompanyName", "Name")),
            (TPEX_NEWS_URL, "上櫃", ("SecuritiesCompanyCode", "公司代號", "Code"), ("CompanyName", "公司名稱", "Name")),
        )
        for url, market, code_keys, name_keys in sources:
            rows = self.json_get(url) or []
            hit = 0
            for row in rows:
                title = str(pick(row, "主旨", "主旨 ") or "")
                body = str(pick(row, "說明", "內容") or "")
                event_type = self._classify_news(title, body)
                if not event_type:
                    continue
                code = str(pick(row, *code_keys) or "").strip()
                if not re.fullmatch(r"\d{4}", code):
                    continue
                announced = parse_roc_date(pick(row, "發言日期", "Date"))
                fact_date = parse_roc_date(pick(row, "事實發生日"))
                blob = f"{title}\n{body}"
                event_date = announced or fact_date
                if is_scheduled_title(blob) or event_type == "董事會":
                    guessed = self._future_date_from_text(blob, announced)
                    if guessed:
                        event_date = guessed
                if event_date is None:
                    continue
                name = str(pick(row, *name_keys) or "").strip()
                self.market_map[code] = market
                if name:
                    self.name_map[code] = name
                clock = None
                speak_time = str(pick(row, "發言時間") or "")
                if speak_time.isdigit() and len(speak_time) >= 5:
                    clock = f"{int(speak_time[:2]):02d}:{int(speak_time[2:4]):02d}"
                eps_from_news = None
                match = EPS_LINE_RE.search(body.replace("：", ":"))
                if match and event_type == "財報":
                    eps_from_news = parse_num(match.group("eps"))
                events.append({
                    "date": event_date,
                    "symbol": code,
                    "name": name or self.name_map.get(code, ""),
                    "market": market,
                    "time": classify_session(clock, "盤後" if event_type == "財報" else "不確定"),
                    "clock": clock,
                    "event_type": event_type,
                    "date_source": "MOPS重大訊息",
                    "date_certainty": "確定",
                    "eps_from_news": eps_from_news,
                    "title": title,
                    "report_quarter": infer_report_quarter(event_date, title + "\n" + body),
                })
                hit += 1
            self.log(f"📰 {market}重大訊息財報相關 {hit} 筆（原始 {len(rows)}）")
        return events

    def _statement_from_row(self, row):
        """
        從損益表列取出代號、累計 EPS、營收與毛利。
        @param {object} row
        @returns {tuple|null} (code, payload)
        """
        if not isinstance(row, dict):
            row = dict(row)
        compact = {str(key).replace(" ", ""): value for key, value in row.items()}
        code = str(
            compact.get("公司代號")
            or compact.get("SecuritiesCompanyCode")
            or pick(row, "公司代號", "公司 代號", "SecuritiesCompanyCode", "Code")
            or ""
        ).strip()
        code = re.sub(r"\s+", "", code)
        if not re.fullmatch(r"\d{4}", code):
            return None
        eps = parse_num(pick(row, "基本每股盈餘（元）", "基本每股盈餘(元)", "基本每股盈餘"))
        revenue = parse_num(pick(row, "營業收入", "利息淨收益", "淨收益"))
        gross = parse_num(pick(
            row,
            "營業毛利（毛損）淨額",
            "營業毛利(毛損)淨額",
            "營業毛利（毛損）",
            "營業毛利(毛損)",
        ))
        name = str(pick(row, "公司名稱", "CompanyName", "Name") or "").strip()
        return code, {
            "eps_ytd": eps,
            "revenue": revenue,
            "gross": gross,
            "name": name,
        }

    def fetch_income_season_mops(self, gregorian_year, season, typek):
        """
        從 MOPS 綜合損益彙總表抓單季累計數（GitHub 常見 ajax_t163sb04 寫法）。
        @param {number} gregorian_year
        @param {number} season
        @param {string} typek
        @returns {object} 代號對應累計財報
        """
        payload = {
            "encodeURIComponent": "1",
            "step": "1",
            "firstin": "1",
            "off": "1",
            "TYPEK": typek,
            "year": str(roc_year(gregorian_year)),
            "season": f"{int(season):02d}",
        }
        html = self._post_html(MOPS_INCOME_URL, payload)
        result = {}
        if not html or "<table" not in html.lower():
            return result
        try:
            dfs = pd.read_html(io.StringIO(html))
        except Exception as exc:
            self.log(f"⚠️ 損益表 {typek} {gregorian_year}Q{season} 解析失敗: {exc}")
            return result
        for df in dfs:
            if df is None or df.empty or df.shape[1] < 3:
                continue
            df = self._flatten_columns(df)
            for _, row in df.iterrows():
                parsed = self._statement_from_row(row)
                if not parsed:
                    continue
                code, payload_row = parsed
                if payload_row.get("eps_ytd") is None and payload_row.get("revenue") is None:
                    continue
                result[code] = payload_row
                if payload_row.get("name"):
                    self.name_map.setdefault(code, payload_row["name"])
        self.log(f"📒 MOPS 損益 { '上市' if typek == 'sii' else '上櫃' } {gregorian_year}Q{season}：{len(result)} 檔")
        return result

    def fetch_income_season_openapi(self):
        """
        備援：證交所／櫃買 OpenAPI 最新一季綜合損益表。
        @returns {tuple} (season_key, map)
        """
        result = {}
        year = None
        season = None
        for url in TWSE_INCOME_URLS + TPEX_INCOME_URLS:
            rows = self.json_get(url) or []
            if not isinstance(rows, list):
                continue
            for row in rows:
                parsed = self._statement_from_row(row)
                if not parsed:
                    continue
                code, payload_row = parsed
                result[code] = payload_row
                if year is None:
                    raw_year = parse_num(pick(row, "年度", "Year"))
                    raw_season = parse_num(pick(row, "季別", "Season"))
                    if raw_year is not None:
                        year = int(raw_year) + 1911 if raw_year < 1911 else int(raw_year)
                    if raw_season is not None:
                        season = int(raw_season)
        if year and season:
            self.log(f"📒 OpenAPI 最新損益 {year}Q{season}：{len(result)} 檔")
            return (year, season), result
        self.log(f"📒 OpenAPI 最新損益：{len(result)} 檔（季別不明）")
        return None, result

    def load_statement_map(self, today):
        """
        載入本季、上季、去年同季累計損益，供單季 EPS／毛利率換算。
        @param {datetime.date} today
        """
        current_year, current_season = infer_report_quarter(today, "")
        seasons = []
        prev_year, prev_season = (
            (current_year, current_season - 1) if current_season > 1 else (current_year - 1, 4)
        )
        year_ago_prev = (
            (current_year - 1, current_season - 1)
            if current_season > 1
            else (current_year - 2, 4)
        )
        for year, season in (
            (current_year, current_season),
            (prev_year, prev_season),
            (current_year - 1, current_season),
            year_ago_prev,
            (current_year - 1, 4),
        ):
            key = (year, season)
            if key not in seasons:
                seasons.append(key)
        for year, season in seasons:
            combined = {}
            for typek in ("sii", "otc"):
                part = self.fetch_income_season_mops(year, season, typek)
                combined.update(part)
                self._sleep_polite(0.4, 0.9)
            if combined:
                self.statement_map[(year, season)] = combined
        latest_key, openapi_map = self.fetch_income_season_openapi()
        if openapi_map:
            if latest_key and latest_key not in self.statement_map:
                self.statement_map[latest_key] = openapi_map
            elif latest_key:
                merged = dict(self.statement_map[latest_key])
                for code, payload_row in openapi_map.items():
                    if code not in merged:
                        merged[code] = payload_row
                self.statement_map[latest_key] = merged
        self.log(f"📒 已載入損益季別 {sorted(self.statement_map.keys())}")

    def _statement_for(self, code, year, season):
        """
        取出某公司某季累計損益。
        @param {string} code
        @param {number} year
        @param {number} season
        @returns {object|null}
        """
        return (self.statement_map.get((year, season)) or {}).get(code)

    def _prev_season(self, year, season):
        """
        回傳上一季的西元年與季。
        @param {number} year
        @param {number} season
        @returns {tuple}
        """
        if int(season) <= 1:
            return (year - 1, 4)
        return (year, int(season) - 1)

    def quarterly_metrics(self, code, year, season):
        """
        計算單季 EPS 與毛利率。
        @param {string} code
        @param {number} year
        @param {number} season
        @returns {object}
        """
        current = self._statement_for(code, year, season)
        if not current:
            return {}
        prev = self._statement_for(code, *self._prev_season(year, season)) or {}
        eps = ytd_to_quarterly(current.get("eps_ytd"), prev.get("eps_ytd"), season)
        rev = ytd_to_quarterly(current.get("revenue"), prev.get("revenue"), season)
        gp = ytd_to_quarterly(current.get("gross"), prev.get("gross"), season)
        margin = _margin_pct(gp, rev)
        if margin is None and current.get("gross") is not None and current.get("revenue"):
            margin = _margin_pct(current.get("gross"), current.get("revenue"))
        return {
            "eps": eps if eps is not None else current.get("eps_ytd") if int(season) == 1 else None,
            "gross_margin": margin,
            "name": current.get("name"),
        }

    def fetch_finmind_eps(self, code, year, season):
        """
        備援：FinMind 單檔季 EPS（可選 FINMIND_TOKEN）。
        @param {string} code
        @param {number} year
        @param {number} season
        @returns {number|null}
        """
        quarter_end = {1: f"{year}-03-31", 2: f"{year}-06-30", 3: f"{year}-09-30", 4: f"{year}-12-31"}[int(season)]
        params = {
            "dataset": "TaiwanStockFinancialStatements",
            "data_id": code,
            "start_date": f"{year - 1}-01-01",
            "end_date": quarter_end,
        }
        if self.finmind_token:
            params["token"] = self.finmind_token
        try:
            resp = self.session.get(FINMIND_URL, params=params, timeout=30)
            payload = resp.json()
            rows = payload.get("data") or []
            eps_rows = [
                row for row in rows
                if row.get("type") == "EPS" and str(row.get("date")) == quarter_end
            ]
            if not eps_rows:
                return None
            return parse_num(eps_rows[-1].get("value"))
        except Exception as exc:
            self.log(f"⚠️ FinMind {code} 失敗: {exc}")
            return None

    def apply_mops_fundamentals(self, records, use_finmind=True):
        """
        用官方損益表補 EPS、去年／上季、毛利率與 YoY／QoQ。
        @param {Array<object>} records
        @param {boolean} use_finmind
        @returns {number} 補到 EPS 的筆數
        """
        filled = 0
        finmind_budget = 25
        for item in records:
            code = item.get("symbol")
            if not code:
                continue
            try:
                item_date = datetime.strptime(item["date"], "%Y-%m-%d").date()
            except Exception:
                continue
            title = item.get("title") or ""
            event_type = str(item.get("event_type") or "")
            quarter = infer_report_quarter(item_date, title)
            if not quarter and item.get("report_quarter"):
                stored = item.get("report_quarter")
                if isinstance(stored, (list, tuple)) and len(stored) == 2:
                    quarter = (int(stored[0]), int(stored[1]))
            if not quarter:
                continue
            year, season = quarter
            item["report_quarter"] = [year, season]
            metrics = self.quarterly_metrics(code, year, season)
            prev_metrics = self.quarterly_metrics(code, *self._prev_season(year, season))
            year_ago = self.quarterly_metrics(code, year - 1, season)
            if use_finmind and finmind_budget > 0 and not metrics.get("eps"):
                fm_eps = self.fetch_finmind_eps(code, year, season)
                finmind_budget -= 1
                if fm_eps is not None:
                    metrics["eps"] = fm_eps
            if "財報" in event_type:
                if fill_if_missing(item, "eps_reported", metrics.get("eps")):
                    filled += 1
                fill_if_missing(item, "eps_last_q", prev_metrics.get("eps"))
            else:
                latest_eps = metrics.get("eps")
                if latest_eps is None:
                    latest_eps = prev_metrics.get("eps")
                fill_if_missing(item, "eps_last_q", latest_eps)
            fill_if_missing(item, "eps_last_y", year_ago.get("eps"))
            latest_margin = metrics.get("gross_margin")
            if latest_margin is None:
                latest_margin = prev_metrics.get("gross_margin")
            fill_if_missing(item, "gross_margin", latest_margin)
            if metrics.get("name"):
                fill_if_missing(item, "name", metrics["name"])
            base = item.get("eps_est") if not is_missing_num(item.get("eps_est")) else item.get("eps_reported")
            last_q = item.get("eps_last_q")
            last_y = item.get("eps_last_y")
            if is_missing_num(base) and not is_missing_num(last_q):
                base = last_q
            if not is_missing_num(base) and not is_missing_num(last_q) and float(last_q) != 0 and base != last_q:
                item["qoq"] = round((float(base) - float(last_q)) / abs(float(last_q)) * 100, 1)
            if not is_missing_num(base) and not is_missing_num(last_y) and float(last_y) != 0:
                item["yoy"] = round((float(base) - float(last_y)) / abs(float(last_y)) * 100, 1)
            if (
                is_missing_num(item.get("surprise_pct"))
                and not is_missing_num(item.get("eps_reported"))
                and not is_missing_num(item.get("eps_est"))
                and float(item.get("eps_est")) != 0
            ):
                item["surprise_pct"] = round(
                    (float(item["eps_reported"]) - float(item["eps_est"])) / abs(float(item["eps_est"])) * 100,
                    2,
                )
        self.log(f"🧮 MOPS/FinMind 補上 Reported EPS {filled} 筆")
        return filled

    def _to_taipei_date(self, val):
        """
        將 Yahoo 財報時間轉成台北日期。
        @param {*} val
        @returns {datetime.date|null}
        """
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return None
        try:
            ts = pd.Timestamp(val)
            if ts.tzinfo is not None:
                ts = ts.tz_convert("Asia/Taipei")
            return ts.date()
        except Exception:
            return None

    def get_historical_eps_from_financials(self, stock, target_date_past):
        """
        從季損益表補歷史 EPS。
        @param {yfinance.Ticker} stock
        @param {datetime.date} target_date_past
        @returns {number|null}
        """
        try:
            fin = stock.quarterly_financials
            if fin is None or fin.empty:
                return None
            eps_row = None
            if "Basic EPS" in fin.index:
                eps_row = fin.loc["Basic EPS"]
            elif "Diluted EPS" in fin.index:
                eps_row = fin.loc["Diluted EPS"]
            if eps_row is None:
                return None
            best_date = None
            min_diff = 999
            for col_date in fin.columns:
                if isinstance(col_date, str):
                    try:
                        d = datetime.strptime(col_date, "%Y-%m-%d").date()
                    except ValueError:
                        continue
                else:
                    d = col_date.date()
                diff = abs((d - target_date_past).days)
                if diff < 45 and diff < min_diff:
                    min_diff = diff
                    best_date = col_date
            if best_date is None:
                return None
            val = eps_row[best_date]
            if pd.isna(val):
                return None
            return float(val)
        except Exception:
            return None

    def enrich_lite(self, event):
        """
        只用證交所／櫃買對照表組出一筆紀錄，不呼叫 Yahoo。
        @param {object} event - 日曆事件
        @returns {object} 基本紀錄
        """
        code = event["symbol"]
        market = event.get("market") or self.market_map.get(code, "上市")
        target_date = event["date"] if isinstance(event["date"], date) else parse_roc_date(event["date"])
        name = event.get("name") or self.name_map.get(code, "")
        ratio = self.ratio_map.get(code) or {}
        price = self.quote_map.get(code)
        pe = ratio.get("pe") if ratio else None
        pb = ratio.get("pb") if ratio else None
        official_cap = self.market_cap_for(code, price)
        return {
            "date": str(target_date),
            "symbol": code,
            "yahoo_symbol": yahoo_symbol_for(code, market),
            "name": name,
            "market": market,
            "time": event.get("time") or "不確定",
            "event_type": event.get("event_type") or "財報",
            "date_source": event.get("date_source") or "MOPS",
            "date_certainty": event.get("date_certainty") or "確定",
            "market_cap": official_cap if official_cap else "-",
            "pe": pe if pe is not None else "-",
            "pb": pb if pb is not None else "-",
            "eps_est": "-",
            "eps_reported": event.get("eps_from_news") if event.get("eps_from_news") is not None else "-",
            "surprise_pct": "-",
            "eps_last_q": "-",
            "eps_last_y": "-",
            "qoq": "-",
            "yoy": "-",
            "price": price if price is not None else "-",
            "gap": "-",
            "gross_margin": "-",
            "analyst_count": "-",
            "eps_est_high": "-",
            "eps_est_low": "-",
            "title": event.get("title") or "",
            "report_quarter": event.get("report_quarter"),
        }

    def _series_get(self, row, *names):
        """
        從 Series／dict 取出第一個有效欄位。
        @param {*} row
        @returns {*}
        """
        if row is None:
            return None
        for name in names:
            try:
                if hasattr(row, "index") and name in row.index:
                    val = row[name]
                elif isinstance(row, dict) and name in row:
                    val = row[name]
                else:
                    continue
            except Exception:
                continue
            if val is None or (isinstance(val, float) and pd.isna(val)):
                continue
            return val
        return None

    def _estimate_from_row(self, row):
        """
        從 Yahoo 預估列取出共識 EPS。
        @param {*} row
        @returns {object}
        """
        avg = parse_num(self._series_get(row, "avg", "EPS Estimate", "earningsAverage"))
        high = parse_num(self._series_get(row, "high", "Earnings High"))
        low = parse_num(self._series_get(row, "low", "Earnings Low"))
        analysts = parse_num(self._series_get(row, "numberOfAnalysts"))
        year_ago = parse_num(self._series_get(row, "yearAgoEps"))
        payload = {}
        if avg is not None:
            payload["eps_est"] = avg
        if high is not None:
            payload["eps_est_high"] = high
        if low is not None:
            payload["eps_est_low"] = low
        if analysts is not None:
            payload["analyst_count"] = int(analysts)
        if year_ago is not None:
            payload["eps_last_y"] = year_ago
        return payload

    def _calendar_eps(self, calendar):
        """
        從 Yahoo calendar 取出本季共識預估與公告日。
        @param {*} calendar
        @returns {object}
        """
        payload = {}
        if calendar is None:
            return payload
        if isinstance(calendar, pd.DataFrame):
            if calendar.empty:
                return payload
            calendar = calendar.iloc[:, 0] if calendar.shape[1] else calendar
        if isinstance(calendar, pd.Series):
            calendar = calendar.to_dict()
        if not isinstance(calendar, dict):
            return payload
        avg = parse_num(calendar.get("Earnings Average") or calendar.get("earningsAverage"))
        if avg is not None:
            payload["eps_est"] = avg
            payload["eps_est_high"] = parse_num(calendar.get("Earnings High"))
            payload["eps_est_low"] = parse_num(calendar.get("Earnings Low"))
        earn_date = calendar.get("Earnings Date")
        if isinstance(earn_date, (list, tuple)) and earn_date:
            earn_date = earn_date[0]
        parsed = None
        if earn_date is not None:
            try:
                parsed = pd.Timestamp(earn_date).date()
            except Exception:
                parsed = None
        if parsed is not None:
            payload["earnings_date"] = parsed
            quarter = infer_report_quarter(parsed, "")
            if quarter:
                payload["zero_q"] = quarter
        return payload

    def _fast_info_map(self, stock):
        """
        讀取 yfinance fast_info（比 info 輕、較少被限流）。
        yfinance 1.x 的 dict 鍵為 camelCase，屬性則為 snake_case。
        @param {yfinance.Ticker} stock
        @returns {object}
        """
        result = {}
        try:
            info = stock.fast_info
        except Exception:
            return result
        data = {}
        try:
            data = dict(info)
        except Exception:
            data = {}

        def grab(*keys):
            for key in keys:
                val = None
                try:
                    val = data.get(key)
                except Exception:
                    val = None
                if val is None:
                    try:
                        val = getattr(info, key, None)
                    except Exception:
                        val = None
                if val is not None:
                    return val
            return None

        result["market_cap"] = _to_finite_float(grab("marketCap", "market_cap"))
        result["price"] = parse_num(grab("lastPrice", "last_price", "previousClose", "previous_close"))
        result["shares"] = parse_shares(grab("shares"))
        result["exchange"] = grab("exchange")
        return result

    def _load_earnings_estimate(self, stock):
        """
        讀取 Yahoo 本季／下季共識預估表。
        @param {yfinance.Ticker} stock
        @returns {pandas.DataFrame|null}
        """
        try:
            if hasattr(stock, "get_earnings_estimate"):
                est = stock.get_earnings_estimate()
            else:
                est = stock.earnings_estimate
        except Exception:
            return None
        if est is None or getattr(est, "empty", True):
            return None
        return est

    def _load_earnings_dates(self, stock):
        """
        讀取 Yahoo 歷史／即將公布的 EPS Estimate。
        @param {yfinance.Ticker} stock
        @returns {pandas.DataFrame|null}
        """
        try:
            if hasattr(stock, "get_earnings_dates"):
                df = stock.get_earnings_dates(limit=16)
            else:
                df = stock.earnings_dates
        except Exception:
            return None
        if df is None or getattr(df, "empty", True):
            return None
        return df.sort_index(ascending=False)

    def fetch_yahoo_consensus(self, code, market=None, deep=True):
        """
        抓單一股票的 Yahoo 共識預估與備援市值（結果會快取）。
        @param {string} code
        @param {string|null} market
        @param {boolean} deep - 是否再抓 earnings_dates／calendar
        @returns {object}
        """
        cache_key = (str(code), "deep" if deep else "light")
        if cache_key in self.yahoo_consensus_cache:
            return self.yahoo_consensus_cache[cache_key]
        payload = {
            "yahoo_symbol": None,
            "market_cap_raw": None,
            "price": None,
            "eps_est_0q": None,
            "eps_est_high": None,
            "eps_est_low": None,
            "analyst_count": None,
            "eps_last_y": None,
            "zero_q": None,
            "by_quarter": {},
            "past_reported": [],
        }
        last_exc = None
        for ysym in yahoo_symbol_candidates(code, market):
            try:
                stock = yf.Ticker(ysym)
                fast = self._fast_info_map(stock)
                est = self._load_earnings_estimate(stock)
                has_quote = fast.get("market_cap") is not None or fast.get("price") is not None
                has_est = est is not None
                if not has_quote and not has_est:
                    continue
                payload["yahoo_symbol"] = ysym
                payload["market_cap_raw"] = fast.get("market_cap")
                payload["price"] = fast.get("price")
                if payload["market_cap_raw"] is None and fast.get("shares") and fast.get("price"):
                    payload["market_cap_raw"] = fast["shares"] * float(fast["price"])

                if est is not None:
                    row = est.loc["0q"] if "0q" in est.index else est.iloc[0]
                    zero = self._estimate_from_row(row)
                    payload["eps_est_0q"] = zero.get("eps_est")
                    payload["eps_est_high"] = zero.get("eps_est_high")
                    payload["eps_est_low"] = zero.get("eps_est_low")
                    payload["analyst_count"] = zero.get("analyst_count")
                    payload["eps_last_y"] = zero.get("eps_last_y")

                if deep:
                    try:
                        cal_payload = self._calendar_eps(stock.calendar)
                    except Exception:
                        cal_payload = {}
                    if payload["eps_est_0q"] is None:
                        payload["eps_est_0q"] = cal_payload.get("eps_est")
                        payload["eps_est_high"] = payload["eps_est_high"] or cal_payload.get("eps_est_high")
                        payload["eps_est_low"] = payload["eps_est_low"] or cal_payload.get("eps_est_low")
                    payload["zero_q"] = cal_payload.get("zero_q")
                    earning_df = self._load_earnings_dates(stock)
                    if earning_df is not None:
                        surprise_col = next(
                            (c for c in earning_df.columns if "surprise" in str(c).lower()),
                            None,
                        )
                        for k in range(len(earning_df)):
                            e_date = self._to_taipei_date(earning_df.index[k])
                            if e_date is None:
                                continue
                            quarter = infer_report_quarter(e_date, "")
                            row_est = parse_num(earning_df["EPS Estimate"].iloc[k]) if "EPS Estimate" in earning_df.columns else None
                            row_rep = parse_num(earning_df["Reported EPS"].iloc[k]) if "Reported EPS" in earning_df.columns else None
                            row_sur = parse_num(earning_df[surprise_col].iloc[k]) if surprise_col else None
                            if row_rep is not None:
                                payload["past_reported"].append(row_rep)
                            if quarter and (row_est is not None or row_rep is not None):
                                qmap = payload["by_quarter"].setdefault(quarter, {})
                                if row_est is not None:
                                    qmap.setdefault("eps_est", row_est)
                                if row_rep is not None:
                                    qmap.setdefault("eps_reported", row_rep)
                                if row_sur is not None:
                                    qmap.setdefault("surprise_pct", row_sur)
                        if payload["zero_q"] is None:
                            for k in range(len(earning_df)):
                                e_date = self._to_taipei_date(earning_df.index[k])
                                if e_date is None:
                                    continue
                                if e_date >= datetime.now().date() - timedelta(days=3):
                                    payload["zero_q"] = infer_report_quarter(e_date, "")
                                    break
                if payload["zero_q"] and payload["eps_est_0q"] is not None:
                    qmap = payload["by_quarter"].setdefault(payload["zero_q"], {})
                    qmap.setdefault("eps_est", payload["eps_est_0q"])
                    if payload.get("eps_est_high") is not None:
                        qmap.setdefault("eps_est_high", payload["eps_est_high"])
                    if payload.get("eps_est_low") is not None:
                        qmap.setdefault("eps_est_low", payload["eps_est_low"])
                    if payload.get("analyst_count") is not None:
                        qmap.setdefault("analyst_count", payload["analyst_count"])
                break
            except Exception as exc:
                last_exc = exc
                continue
        if last_exc and payload["yahoo_symbol"] is None:
            self.log(f"   ⚠️ [{code}] Yahoo 共識失敗: {last_exc}")
        self.yahoo_consensus_cache[cache_key] = payload
        if deep:
            self.yahoo_consensus_cache[(str(code), "light")] = payload
        return payload

    def _item_report_quarter(self, item):
        """
        取出紀錄的財報年／季。
        @param {object} item
        @returns {tuple|null}
        """
        stored = item.get("report_quarter")
        if isinstance(stored, (list, tuple)) and len(stored) == 2:
            try:
                return (int(stored[0]), int(stored[1]))
            except (TypeError, ValueError):
                pass
        try:
            event_date = datetime.strptime(item.get("date") or "", "%Y-%m-%d").date()
        except Exception:
            return None
        return infer_report_quarter(event_date, item.get("title") or "")

    def _apply_consensus_to_item(self, item, payload, today):
        """
        把 Yahoo 共識寫入單筆紀錄：優先對齊財報季，避免誤用下一季 0q。
        @param {object} item
        @param {object} payload
        @param {datetime.date} today
        @returns {boolean} 是否寫入預估
        """
        changed = False
        if payload.get("yahoo_symbol"):
            item["yahoo_symbol"] = payload["yahoo_symbol"]
        if is_blank(item.get("market_cap")) and payload.get("market_cap_raw"):
            item["market_cap"] = format_market_cap(payload["market_cap_raw"])
            changed = True
        fill_if_missing(item, "price", payload.get("price"))
        fill_if_missing(item, "eps_last_y", payload.get("eps_last_y"))
        past = payload.get("past_reported") or []
        if is_missing_num(item.get("eps_last_q")) and past:
            item["eps_last_q"] = past[0]
            changed = True
        if is_missing_num(item.get("eps_last_y")) and len(past) >= 4:
            item["eps_last_y"] = past[3]
            changed = True

        quarter = self._item_report_quarter(item)
        matched = payload.get("by_quarter", {}).get(quarter) if quarter else None
        if matched and matched.get("eps_est") is not None:
            item["eps_est"] = matched.get("eps_est")
            changed = True
            fill_if_missing(item, "eps_est_high", matched.get("eps_est_high") or payload.get("eps_est_high"))
            fill_if_missing(item, "eps_est_low", matched.get("eps_est_low") or payload.get("eps_est_low"))
            fill_if_missing(item, "analyst_count", matched.get("analyst_count") or payload.get("analyst_count"))
            fill_if_missing(item, "eps_reported", matched.get("eps_reported"))
            fill_if_missing(item, "surprise_pct", matched.get("surprise_pct"))
        elif is_missing_num(item.get("eps_est")):
            try:
                event_date = datetime.strptime(item.get("date") or "", "%Y-%m-%d").date()
            except Exception:
                event_date = None
            upcoming = event_date is None or event_date >= today - timedelta(days=YAHOO_CONSENSUS_LOOKBACK_DAYS)
            same_zero_q = quarter and payload.get("zero_q") and quarter == payload.get("zero_q")
            if upcoming or same_zero_q:
                if fill_if_missing(item, "eps_est", payload.get("eps_est_0q")):
                    changed = True
                fill_if_missing(item, "eps_est_high", payload.get("eps_est_high"))
                fill_if_missing(item, "eps_est_low", payload.get("eps_est_low"))
                fill_if_missing(item, "analyst_count", payload.get("analyst_count"))
        self._refresh_derived_eps_fields(item)
        return changed or not is_missing_num(item.get("eps_est"))

    def _refresh_derived_eps_fields(self, data):
        """
        依預估／真實 EPS 重算 QoQ、YoY、Surprise。
        @param {object} data
        """
        base = data.get("eps_est") if not is_missing_num(data.get("eps_est")) else data.get("eps_reported")
        last_q = data.get("eps_last_q")
        last_y = data.get("eps_last_y")
        if not is_missing_num(base) and not is_missing_num(last_q) and float(last_q) != 0:
            data["qoq"] = round((float(base) - float(last_q)) / abs(float(last_q)) * 100, 1)
        if not is_missing_num(base) and not is_missing_num(last_y) and float(last_y) != 0:
            data["yoy"] = round((float(base) - float(last_y)) / abs(float(last_y)) * 100, 1)
        if (
            is_missing_num(data.get("surprise_pct"))
            and not is_missing_num(data.get("eps_reported"))
            and not is_missing_num(data.get("eps_est"))
            and float(data.get("eps_est")) != 0
        ):
            data["surprise_pct"] = round(
                (float(data["eps_reported"]) - float(data["eps_est"])) / abs(float(data["eps_est"])) * 100,
                2,
            )
        if (
            is_blank(data.get("pe"))
            and not is_missing_num(data.get("price"))
            and not is_missing_num(data.get("eps_est"))
            and float(data.get("eps_est")) > 0
        ):
            data["pe"] = round(float(data["price"]) / float(data["eps_est"]), 2)

    def fill_yahoo_consensus(self, records, deep=True, apply_to=None):
        """
        對缺預估 EPS 的股票批次抓 Yahoo 共識，同一代號只打一次 API。
        @param {Array<object>} records - 用來決定要抓哪些代號
        @param {boolean} deep
        @param {Array<object>|null} apply_to - 寫回目標，預設為 records
        @returns {number} 補到預估的筆數
        """
        today = datetime.now().date()
        targets = apply_to if apply_to is not None else records
        need_codes = set()
        samples = {}
        for item in records:
            code = str(item.get("symbol") or "").strip()
            if not re.fullmatch(r"\d{4}", code):
                continue
            samples.setdefault(code, item)
        jobs = list(samples.values())
        if not jobs:
            return 0
        self.log(f"📥 Yahoo 共識預估 {len(jobs)} 檔（deep={deep}）")
        payload_map = {}

        def _one(item):
            code = item["symbol"]
            return code, self.fetch_yahoo_consensus(code, item.get("market"), deep=deep)

        filled_symbols = 0
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [executor.submit(_one, item) for item in jobs]
            done = 0
            for future in as_completed(futures):
                done += 1
                try:
                    code, payload = future.result()
                    payload_map[code] = payload
                    if payload.get("eps_est_0q") is not None or payload.get("by_quarter"):
                        filled_symbols += 1
                except Exception as exc:
                    self.log(f"   ⚠️ Yahoo 共識執行失敗: {exc}")
                if done % 25 == 0 or done == len(jobs):
                    self.log(f"   … Yahoo 進度 {done}/{len(jobs)}")

        applied = 0
        for item in targets:
            code = str(item.get("symbol") or "").strip()
            payload = payload_map.get(code) or self.yahoo_consensus_cache.get((code, "deep" if deep else "light"))
            if not payload:
                continue
            before = item.get("eps_est")
            self._apply_consensus_to_item(item, payload, today)
            if is_missing_num(before) and not is_missing_num(item.get("eps_est")):
                applied += 1
        self.log(f"   ✅ Yahoo 有共識 {filled_symbols} 檔，寫入預估 {applied} 筆")
        return applied

    def enrich_with_yahoo(self, event, data=None):
        """
        以 Yahoo Finance 補 EPS 預估與備援市值；已有官方數字則不覆蓋。
        @param {object} event - 日曆事件
        @param {object|null} data - 可傳入已補過 MOPS 的紀錄
        @returns {object} 完整紀錄
        """
        data = data or self.enrich_lite(event)
        payload = self.fetch_yahoo_consensus(data["symbol"], data.get("market"), deep=True)
        today = datetime.now().date()
        self._apply_consensus_to_item(data, payload, today)
        if is_missing_num(data.get("gross_margin")):
            try:
                ysym = payload.get("yahoo_symbol") or data.get("yahoo_symbol")
                stock = yf.Ticker(ysym)
                gm = get_gross_margin_pct(stock, None)
                if gm not in (None, "-"):
                    data["gross_margin"] = gm
            except Exception:
                pass
        return data

    def fetch_reported_from_ticker(self, symbol, yahoo_symbol, target_date):
        """
        以 earnings_dates 對齊公布日，取出 Reported EPS / Surprise。
        @param {string} symbol
        @param {string} yahoo_symbol
        @param {datetime.date|string} target_date
        @returns {object|null}
        """
        try:
            if isinstance(target_date, str):
                target_date = datetime.strptime(target_date, "%Y-%m-%d").date()
            stock = yf.Ticker(yahoo_symbol)
            earning_df = stock.earnings_dates
            if earning_df is None or earning_df.empty:
                return None
            earning_df = earning_df.sort_index(ascending=False)
            found_idx = -1
            for k in range(len(earning_df)):
                e_date = self._to_taipei_date(earning_df.index[k])
                if e_date is None:
                    continue
                if abs((e_date - target_date).days) <= 7:
                    found_idx = k
                    break
            if found_idx == -1:
                return None
            eps_reported = parse_num(earning_df["Reported EPS"].iloc[found_idx])
            surprise_pct = None
            surprise_col = next(
                (c for c in earning_df.columns if "surprise" in str(c).lower()),
                None,
            )
            if surprise_col is not None:
                surprise_pct = parse_num(earning_df[surprise_col].iloc[found_idx])
            if surprise_pct is None and eps_reported is not None:
                eps_est = parse_num(earning_df["EPS Estimate"].iloc[found_idx])
                if eps_est not in (None, 0):
                    surprise_pct = round((eps_reported - float(eps_est)) / abs(float(eps_est)) * 100, 2)
            if eps_reported is None and surprise_pct is None:
                return None
            return {
                "date": str(target_date),
                "symbol": symbol,
                "eps_reported": eps_reported if eps_reported is not None else "-",
                "surprise_pct": surprise_pct if surprise_pct is not None else "-",
            }
        except Exception:
            return None

    def fill_missing_reported_from_tickers(self, missing_items):
        """
        對仍缺 Reported/Surprise 的紀錄以 Yahoo 補抓。
        @param {Array<object>} missing_items
        @returns {Array<object>}
        """
        if not missing_items:
            return []
        print(f"🔎 以 Yahoo earnings_dates 補抓 {len(missing_items)} 檔")
        results = []

        def _one(item):
            ysym = item.get("yahoo_symbol") or yahoo_symbol_for(
                item["symbol"], item.get("market") or self.market_map.get(item["symbol"], "上市")
            )
            return self.fetch_reported_from_ticker(item["symbol"], ysym, item["date"])

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [executor.submit(_one, item) for item in missing_items]
            for future in as_completed(futures):
                try:
                    row = future.result()
                    if row:
                        results.append(row)
                except Exception:
                    pass
        print(f"   ✅ Yahoo 補到 {len(results)} 筆 EPS/Surprise")
        return results


def _collect_missing_reported_items(records):
    """
    收集過去 PAST_REPORT_DAYS 內缺 Reported/Surprise 的紀錄。
    @param {Array<object>} records
    @returns {Array<object>}
    """
    today = datetime.now().date()
    start_date = today - timedelta(days=PAST_REPORT_DAYS)
    items = []
    seen = set()
    for item in records:
        symbol = item.get("symbol")
        date_str = item.get("date")
        if not symbol or not date_str:
            continue
        try:
            item_date = datetime.strptime(date_str, "%Y-%m-%d").date()
        except Exception:
            continue
        if item_date < start_date or item_date > today:
            continue
        event_type = str(item.get("event_type") or "")
        has_estimate = not is_missing_num(item.get("eps_est"))
        if "財報" not in event_type and not has_estimate:
            continue
        if is_missing_num(item.get("eps_reported")) or is_missing_num(item.get("surprise_pct")):
            key = (symbol, date_str)
            if key not in seen:
                seen.add(key)
                items.append(item)
    return items


def _sort_yoy(item):
    """
    排序用年增率，缺值視為極小。
    @param {object} item
    @returns {number}
    """
    val = parse_num(item.get("yoy"))
    return val if val is not None else -9999.0


def load_existing_records():
    """
    讀取既有 earnings_data.json。
    @returns {Array<object>}
    """
    if not os.path.exists(DATA_FILE):
        return []
    try:
        with open(DATA_FILE, "r", encoding="utf-8") as handle:
            content = json.load(handle)
        if isinstance(content, list):
            return content
        if isinstance(content, dict) and "data" in content:
            return content["data"]
    except Exception as exc:
        print(f"⚠️ 讀取舊檔失敗 ({exc})，將建立新檔案。")
    return []


def merge_calendar_events(events):
    """
    合併同一代碼、同一日的多個事件來源。
    @param {Array<object>} events
    @returns {Array<object>}
    """
    merged = {}
    for event in events:
        event_date = event["date"] if isinstance(event["date"], date) else parse_roc_date(event["date"])
        if event_date is None:
            continue
        key = (event["symbol"], str(event_date))
        if key not in merged:
            copied = dict(event)
            copied["date"] = event_date
            merged[key] = copied
            continue
        old = merged[key]
        old["event_type"] = merge_event_type(old.get("event_type"), event.get("event_type"))
        sources = [old.get("date_source"), event.get("date_source")]
        old["date_source"] = "+".join(dict.fromkeys(s for s in sources if s))
        if old.get("date_certainty") != "確定" and event.get("date_certainty") == "確定":
            old["date_certainty"] = "確定"
        if event.get("name") and not old.get("name"):
            old["name"] = event["name"]
        if event.get("eps_from_news") is not None and old.get("eps_from_news") is None:
            old["eps_from_news"] = event["eps_from_news"]
        if event.get("title") and not old.get("title"):
            old["title"] = event["title"]
        if event.get("report_quarter") and not old.get("report_quarter"):
            old["report_quarter"] = event["report_quarter"]
        if old.get("time") in (None, "不確定") and event.get("time") not in (None, "不確定"):
            old["time"] = event["time"]
    return list(merged.values())


def save_data(new_data_list, reported_updates=None):
    """
    合併、清理並寫入 earnings_data.json 與 DailyLog.txt。
    @param {Array<object>} new_data_list
    @param {Array<object>|null} reported_updates
    """
    print("💾 正在準備存檔...")
    existing_list = load_existing_records()
    data_map = {}
    for item in existing_list:
        key = record_key(item)
        if key[0] and key[1]:
            data_map[key] = item

    overwrite_count = 0
    new_entry_count = 0
    keep_keys = (
        "eps_reported",
        "surprise_pct",
        "gross_margin",
        "name",
        "market",
        "event_type",
        "eps_est",
        "eps_last_q",
        "eps_last_y",
        "market_cap",
        "analyst_count",
        "yoy",
        "qoq",
    )

    for item in new_data_list:
        key = record_key(item)
        if not key[0] or not key[1]:
            continue
        if key in data_map:
            old = data_map[key]
            if is_blank(item.get("market_cap")) and not is_blank(old.get("market_cap")):
                item["market_cap"] = old["market_cap"]
            for keep_key in keep_keys:
                if keep_key == "market_cap":
                    continue
                if is_missing_num(item.get(keep_key)) and not is_missing_num(old.get(keep_key)):
                    item[keep_key] = old[keep_key]
                elif keep_key in ("name", "market", "event_type"):
                    if not item.get(keep_key) and old.get(keep_key):
                        item[keep_key] = old[keep_key]
                    elif keep_key == "event_type" and old.get(keep_key):
                        item[keep_key] = merge_event_type(old.get(keep_key), item.get(keep_key))
            if item.get("gap") in (None, "-", "") and old.get("gap") not in (None, "-", ""):
                item["gap"] = old["gap"]
            data_map[key] = item
            overwrite_count += 1
        else:
            data_map[key] = item
            new_entry_count += 1

    reported_update_count = 0
    past_new_count = 0
    for item in (reported_updates or []):
        key = record_key(item)
        if not key[0] or not key[1]:
            continue
        if key not in data_map:
            if "eps_est" in item or "event_type" in item:
                data_map[key] = item
                past_new_count += 1
            continue
        if merge_reported_fields(data_map[key], item):
            reported_update_count += 1

    today = datetime.now().date()
    cutoff_date = today - timedelta(days=KEEP_DAYS)
    final_list = []
    removed_count = 0
    for item in data_map.values():
        try:
            item_date = datetime.strptime(item["date"], "%Y-%m-%d").date()
            if item_date >= cutoff_date:
                final_list.append(item)
            else:
                removed_count += 1
        except Exception:
            final_list.append(item)

    gap_update_count = update_gap_fields(final_list)
    final_list = sorted(final_list, key=lambda x: (x.get("date") or "", -_sort_yoy(x)))

    with open(DATA_FILE, "w", encoding="utf-8") as handle:
        json.dump(final_list, handle, ensure_ascii=False, indent=4)

    tw_time = datetime.now(timezone.utc) + timedelta(hours=8)
    current_time_str = tw_time.strftime("%Y-%m-%d %H:%M:%S")
    log_summary = (
        f"[{current_time_str}] 執行完成報告 (台灣時間):\n"
        f"  - 新增資料: {new_entry_count} 筆\n"
        f"  - 往前20日新增: {past_new_count} 筆\n"
        f"  - 更新資料: {overwrite_count} 筆\n"
        f"  - 補抓 EPS/Surprise: {reported_update_count} 筆\n"
        f"  - 更新跳空: {gap_update_count} 筆\n"
        f"  - 清除過期: {removed_count} 筆 (早於 {cutoff_date})\n"
        f"  - 總資料數: {len(final_list)} 筆\n"
        f"------------------------------------\n"
    )
    print(log_summary)
    try:
        lines = []
        if os.path.exists(LOG_FILE):
            with open(LOG_FILE, "r", encoding="utf-8") as handle:
                lines = handle.readlines()
        lines.insert(0, log_summary)
        if len(lines) > 2000:
            print(f"⚠️ 日誌檔超過 2000 行 ({len(lines)} 行)，正在刪除最舊(末端)的 1000 行...")
            lines = lines[:-1000]
        with open(LOG_FILE, "w", encoding="utf-8") as handle:
            handle.writelines(lines)
        print(f"✅ 執行結果已寫入 (最新在首行): {LOG_FILE}")
    except Exception as exc:
        print(f"⚠️ 寫入日誌檔失敗: {exc}")


def months_for_window(start_day, end_day):
    """
    列出涵蓋日期區間的民國年月。
    @param {datetime.date} start_day
    @param {datetime.date} end_day
    @returns {Array<tuple>} [(民國年, 月), ...]
    """
    months = []
    cursor = date(start_day.year, start_day.month, 1)
    last = date(end_day.year, end_day.month, 1)
    while cursor <= last:
        months.append((roc_year(cursor.year), cursor.month))
        if cursor.month == 12:
            cursor = date(cursor.year + 1, 1, 1)
        else:
            cursor = date(cursor.year, cursor.month + 1, 1)
    return months


def run_scrape(start_offset, search_days):
    """
    執行指定天數區間的台股財報爬蟲。
    @param {number} start_offset - 從今日起算的起始偏移
    @param {number} search_days - 往後搜尋天數
    """
    if "yfinance" not in sys.modules:
        print("缺少必要套件")
        sys.exit(1)

    scraper = TwStockScraper()
    today = datetime.now().date()
    window_start = today + timedelta(days=start_offset)
    window_end = window_start + timedelta(days=search_days - 1)
    past_start = today - timedelta(days=PAST_REPORT_DAYS)

    print("🚀 開始執行台股財報爬蟲")
    print(f"   - 起始搜尋日期: {window_start} (今日+{start_offset}天)")
    print(f"   - 搜尋天數: {search_days} 天（至 {window_end}）")
    print(f"   - 清除截止日期: {today - timedelta(days=KEEP_DAYS)}（保留近 {KEEP_DAYS} 日）")

    scraper.fetch_market_maps()
    scraper.load_statement_map(today)
    months = months_for_window(min(past_start, window_start), max(today, window_end))
    conference_events = scraper.fetch_conferences(months)
    announce_start = past_start - timedelta(days=M31_LOOKBACK_EXTRA_DAYS)
    m31_events = scraper.fetch_m31_events(announce_start, today)
    news_events = scraper.fetch_material_news_events()
    merged = merge_calendar_events(conference_events + m31_events + news_events)

    window_events = []
    past_events = []
    for event in merged:
        event_date = event["date"]
        if window_start <= event_date <= window_end:
            window_events.append(event)
        elif past_start <= event_date <= today:
            past_events.append(event)

    print(
        f"🎯 本區間事件 {len(window_events)} 筆；近 {PAST_REPORT_DAYS} 日 {len(past_events)} 筆"
        f"（法說會 {sum(1 for e in merged if '法說會' in str(e.get('event_type')))}，"
        f"財報 {sum(1 for e in merged if '財報' in str(e.get('event_type')))}）"
    )

    all_new_results = [scraper.enrich_lite(event) for event in window_events]
    scraper.apply_official_market_data(all_new_results)
    scraper.apply_mops_fundamentals(all_new_results, use_finmind=True)
    scraper.fill_yahoo_consensus(all_new_results, deep=True)

    for data in all_new_results:
        print(
            f"   [{data['symbol']}] ✅ {data.get('name') or ''} {data.get('event_type')}"
            f" (EPS:{data.get('eps_reported')} YoY:{data.get('yoy')})"
        )

    existing = load_existing_records()
    scraper.apply_official_market_data(existing)
    if existing:
        scraper.apply_mops_fundamentals(existing, use_finmind=False)
    nearby_existing = []
    for item in existing:
        try:
            item_date = datetime.strptime(item.get("date") or "", "%Y-%m-%d").date()
        except Exception:
            continue
        if item_date >= today - timedelta(days=YAHOO_CONSENSUS_LOOKBACK_DAYS) and (
            is_missing_num(item.get("eps_est")) or is_blank(item.get("market_cap"))
        ):
            nearby_existing.append(item)
    if nearby_existing:
        scraper.fill_yahoo_consensus(nearby_existing, deep=True, apply_to=existing)
    existing_keys = {record_key(item) for item in existing}
    past_new = []
    for event in past_events:
        key = (event["symbol"], str(event["date"]))
        if key in existing_keys:
            continue
        past_new.append(scraper.enrich_lite(event))
    if past_new:
        scraper.apply_official_market_data(past_new)
        scraper.apply_mops_fundamentals(past_new, use_finmind=False)
        scraper.fill_yahoo_consensus(past_new, deep=True)
        print(f"   近窗新增 {len(past_new)} 筆（MOPS 損益表＋Yahoo 共識）")

    missing_items = _collect_missing_reported_items(existing + all_new_results + past_new)
    still_missing = [
        item for item in missing_items
        if "財報" in str(item.get("event_type") or "") and is_missing_num(item.get("eps_reported"))
    ]
    reported_updates = []
    if still_missing:
        reported_updates = scraper.fill_missing_reported_from_tickers(still_missing[:40])
    reported_updates.extend(past_new)
    save_data(existing + all_new_results, reported_updates)
