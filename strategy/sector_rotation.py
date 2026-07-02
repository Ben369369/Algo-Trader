"""
Sector rotation strategy — rank all 11 SPDR sector ETFs by 3-month momentum,
hold the top N. Rebalance whenever the ranking changes materially.

Academic basis: sectors trend for months due to economic cycles, making momentum
a much cleaner signal than on individual stocks (Moskowitz & Grinblatt, 1999).
"""
import json
import sqlite3
import datetime
import pandas as pd
import numpy as np
from pathlib import Path
from config.settings import Config
from utils.logger import logger

SECTOR_ETFS = Config.SECTOR_ETF_SYMBOLS

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


# ---------------------------------------------------------------------------
# Live execution
# ---------------------------------------------------------------------------

class SectorRotationExecutor:
    """
    Monthly rebalancer for sector ETF rotation.
    Shares positions_state.json with TradeExecutor so trailing stops
    are managed consistently. Sector ETF entries are flagged with
    is_sector_etf=True so check_exits skips breakdown and time exits.
    """
    ALLOCATION_PCT  = 0.15   # 15% of portfolio per sector ETF
    TOP_N           = 3
    REBALANCE_DAYS  = 25     # rebalance ~monthly

    _STATE_FILE = Path(__file__).parent.parent / "data" / "positions_state.json"

    def __init__(self):
        from utils.broker import BrokerConnection
        self.broker = BrokerConnection()
        self._state = self._load_state()

    def _load_state(self):
        if self._STATE_FILE.exists():
            try:
                with open(self._STATE_FILE) as f:
                    return json.load(f)
            except Exception:
                return {}
        return {}

    def _save_state(self):
        with open(self._STATE_FILE, "w") as f:
            json.dump(self._state, f, indent=2)

    def _is_rebalance_due(self):
        last_str = self._state.get("__sector_rebalance__")
        if not last_str:
            return True
        last = datetime.date.fromisoformat(last_str)
        return (datetime.date.today() - last).days >= self.REBALANCE_DAYS

    def rebalance(self):
        if not self._is_rebalance_due():
            last = datetime.date.fromisoformat(self._state["__sector_rebalance__"])
            days_left = self.REBALANCE_DAYS - (datetime.date.today() - last).days
            logger.info(f"Sector rotation: Next rebalance in ~{days_left} days.")
            return []

        logger.info("Sector rotation: Monthly rebalance triggered.")

        positions  = self.broker.get_positions()
        held_etfs  = {p["symbol"]: p for p in positions if p["symbol"] in SECTOR_ETFS}

        ranked = rank_sectors()
        if ranked.empty:
            logger.warning("Sector rotation: No ranking data — skipping rebalance.")
            return []

        # Absolute-momentum filter: only hold sectors that are actually rising.
        # In a broad decline the sleeve rotates to cash instead of the
        # least-bad sector (Antonacci-style dual momentum).
        if Config.SECTOR_ABS_FILTER:
            ranked = ranked[ranked["composite"] > 0]
            if ranked.empty:
                logger.info("Sector rotation: No sector has positive momentum — staying in cash.")

        targets = ranked.head(self.TOP_N)["symbol"].tolist()
        logger.info(f"Sector rotation: Top {self.TOP_N} sectors = {targets}")

        acc            = self.broker.get_account()
        portfolio_val  = float(acc["portfolio_value"])
        cash           = float(acc["cash"])
        orders         = []

        # Drawdown circuit breaker — same policy as stock entries. Exits from
        # dropped sectors still happen below; only NEW entries are halted.
        peak = self._state.get("__portfolio_peak__", portfolio_val)
        drawdown = (peak - portfolio_val) / peak if peak > 0 else 0.0
        breaker_active = drawdown > Config.MAX_DRAWDOWN_LIMIT
        if breaker_active:
            logger.warning(
                f"Sector rotation: Portfolio drawdown {drawdown:.1%} exceeds "
                f"{Config.MAX_DRAWDOWN_LIMIT:.0%} — exits only, no new entries."
            )

        # --- Exit ETFs that dropped out of the top N ---
        for sym, pos in held_etfs.items():
            if sym not in targets:
                qty = abs(int(float(pos["qty"])))
                self.broker.cancel_orders_for_symbol(sym)
                order = self.broker.place_market_order(sym, qty, "sell",
                                                       note="sector rotation exit")
                if order:
                    self._state.pop(sym, None)
                    orders.append({"symbol": sym, "side": "sell", "qty": qty})
                    logger.info(f"Sector rotation: EXIT {sym} ({qty} shares)")

        self._save_state()

        # --- Enter ETFs newly in the top N ---
        alloc = portfolio_val * self.ALLOCATION_PCT
        for sym in targets:
            if breaker_active:
                break
            if sym in held_etfs:
                logger.info(f"Sector rotation: Holding {sym} — no change.")
                continue
            price = self.broker.get_latest_price(sym)
            if not price:
                logger.warning(f"Sector rotation: Could not price {sym} — skipping.")
                continue
            shares = int(alloc / price)
            if shares <= 0 or shares * price > cash:
                logger.warning(f"Sector rotation: Insufficient cash for {sym}.")
                continue
            order = self.broker.place_market_order(sym, shares, "buy",
                                                   note="sector rotation entry")
            if order:
                cash -= shares * price
                self._state[sym] = {
                    "entry_date":      str(datetime.date.today()),
                    "entry_price":     price,
                    "high_water_mark": price,
                    "is_sector_etf":   True,
                    "strategy":        "sector",
                }
                orders.append({"symbol": sym, "side": "buy", "qty": shares})
                logger.info(f"Sector rotation: ENTER {sym} ({shares} shares @ ${price:.2f})")

        self._state["__sector_rebalance__"] = str(datetime.date.today())
        self._save_state()
        logger.info(f"Sector rotation: Rebalance done — {len(orders)} order(s).")
        return orders
