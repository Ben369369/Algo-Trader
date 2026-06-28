"""
Sector rotation strategy — rank all 11 SPDR sector ETFs by 3-month momentum,
hold the top N. Rebalance whenever the ranking changes materially.

Academic basis: sectors trend for months due to economic cycles, making momentum
a much cleaner signal than on individual stocks (Moskowitz & Grinblatt, 1999).
"""
import sqlite3
import pandas as pd
import numpy as np
from config.settings import Config

SECTOR_ETFS = ["XLK", "XLF", "XLE", "XLV", "XLI", "XLY", "XLP", "XLB", "XLRE", "XLC", "XLU"]

SECTOR_NAMES = {
    "XLK":  "Technology",
    "XLF":  "Financials",
    "XLE":  "Energy",
    "XLV":  "Healthcare",
    "XLI":  "Industrials",
    "XLY":  "Consumer Discretionary",
    "XLP":  "Consumer Staples",
    "XLB":  "Materials",
    "XLRE": "Real Estate",
    "XLC":  "Communication",
    "XLU":  "Utilities",
}

# Momentum lookback windows (trading days)
LOOKBACK_3M  = 63
LOOKBACK_6M  = 126
LOOKBACK_12M = 252
SKIP_DAYS    = 21   # skip last month to avoid reversal contamination

TOP_N = 3           # number of sectors to hold simultaneously


def _load_etf(symbol, db_path):
    with sqlite3.connect(db_path) as conn:
        df = pd.read_sql_query(
            "SELECT timestamp, close FROM ohlcv WHERE symbol=? ORDER BY timestamp",
            conn, params=(symbol,)
        )
    if df.empty:
        return None
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    return df.set_index("timestamp")["close"]


def rank_sectors(db_path=None):
    """
    Return a DataFrame ranking all 11 sector ETFs by composite momentum score.
    Composite = average of 3-month and 6-month momentum (skip last month each).
    """
    db_path = db_path or Config.DB_PATH
    rows = []
    for sym in SECTOR_ETFS:
        prices = _load_etf(sym, db_path)
        if prices is None or len(prices) < LOOKBACK_12M + SKIP_DAYS:
            continue
        close = prices.iloc[-1]
        mom_3m  = prices.iloc[-1] / prices.iloc[-(LOOKBACK_3M  + SKIP_DAYS)] - 1
        mom_6m  = prices.iloc[-1] / prices.iloc[-(LOOKBACK_6M  + SKIP_DAYS)] - 1
        mom_12m = prices.iloc[-1] / prices.iloc[-(LOOKBACK_12M + SKIP_DAYS)] - 1
        # Equal-weight composite of 3m and 6m (both skip last month)
        composite = (mom_3m + mom_6m) / 2
        rows.append({
            "symbol":    sym,
            "sector":    SECTOR_NAMES[sym],
            "price":     round(float(close), 2),
            "mom_3m":    round(float(mom_3m)  * 100, 2),
            "mom_6m":    round(float(mom_6m)  * 100, 2),
            "mom_12m":   round(float(mom_12m) * 100, 2),
            "composite": round(float(composite) * 100, 2),
        })
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows).sort_values("composite", ascending=False).reset_index(drop=True)
    df.index += 1
    return df


def top_sectors(n=TOP_N, db_path=None):
    """Return the top N sector ETF symbols by composite momentum."""
    ranked = rank_sectors(db_path)
    if ranked.empty:
        return []
    return ranked.head(n)["symbol"].tolist()
