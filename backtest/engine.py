import math
import sqlite3
import pandas as pd
import numpy as np
from config.settings import Config
from strategy.signals import SignalDetector

STOP_LOSS_PCT   = 0.05
TAKE_PROFIT_PCT = 0.10
RISK_PCT        = 0.02
MAX_POSITION_PCT = 0.10


def _score_row(rsi, zscore, macd_hist, price):
    """Replicate TradeScorer weights without importing the live scanner class."""
    rsi_score    = abs(rsi - 50) / 50
    zscore_score = min(abs(zscore), 3) / 3
    macd_score   = min(abs(macd_hist) / price * 100, 1) if price > 0 else 0
    return round(rsi_score * 0.35 + zscore_score * 0.40 + macd_score * 0.25, 4)


def _size_shares(portfolio_value, price):
    """Replicate PositionSizer.calculate without importing (avoids logger dep)."""
    if price <= 0:
        return 0
    max_risk_dollars   = portfolio_value * RISK_PCT
    risk_per_share     = price * STOP_LOSS_PCT
    shares             = max_risk_dollars / risk_per_share
    max_shares_by_size = (portfolio_value * MAX_POSITION_PCT) / price
    return math.floor(min(shares, max_shares_by_size))


def _load_symbol(symbol, db_path):
    """Load all OHLCV rows for one symbol from SQLite, sorted by date."""
    with sqlite3.connect(db_path) as conn:
        df = pd.read_sql_query(
            "SELECT timestamp, open, high, low, close, volume "
            "FROM ohlcv WHERE symbol=? ORDER BY timestamp",
            conn, params=(symbol,)
        )
    if df.empty:
        return None
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df = df.set_index("timestamp")
    return df


class BacktestEngine:

    def __init__(self, initial_capital=100_000.0, db_path=None):
        self.initial_capital = initial_capital
        self.db_path = db_path or Config.DB_PATH

    # ------------------------------------------------------------------
    # Main entry point
    # ------------------------------------------------------------------
    def run(self, symbols=None):
        """
        Run a full multi-symbol backtest.

        Returns
        -------
        equity_df : pd.DataFrame  — daily portfolio value (index = date)
        trades_df : pd.DataFrame  — one row per closed trade
        """
        symbols = symbols or Config.symbols()

        # 1. Load & compute signals for every symbol
        all_signals = {}
        all_ohlcv   = {}
        for sym in symbols:
            df = _load_symbol(sym, self.db_path)
            if df is None or len(df) < 210:
                continue
            sig = SignalDetector.detect(df)
            all_signals[sym] = sig
            all_ohlcv[sym]   = df

        if not all_signals:
            raise RuntimeError("No symbol data found in the database.")

        # 2. Determine common trading calendar
        all_dates = sorted(set.union(*[set(s.index) for s in all_signals.values()]))

        # 3. Simulation state
        cash        = self.initial_capital
        positions   = {}   # sym -> {shares, entry_price, stop, target}
        pending     = None  # (sym, entry_price_est) — queued for next open
        equity_rows = []
        trade_log   = []

        for today in all_dates:
            # --- Step 1: Execute pending entry at today's open ---
            if pending is not None:
                sym, _ = pending
                if sym not in all_ohlcv or today not in all_ohlcv[sym].index:
                    pending = None
                elif sym not in positions:   # only one position per symbol
                    open_px = all_ohlcv[sym].loc[today, "open"]
                    shares  = _size_shares(cash, open_px)
                    cost    = shares * open_px
                    if shares > 0 and cost <= cash:
                        cash -= cost
                        positions[sym] = {
                            "shares":      shares,
                            "entry_price": open_px,
                            "entry_date":  today,
                            "stop":        round(open_px * (1 - STOP_LOSS_PCT), 4),
                            "target":      round(open_px * (1 + TAKE_PROFIT_PCT), 4),
                        }
                pending = None

            # --- Step 2: Check exits for held positions ---
            to_exit = []
            for sym, pos in positions.items():
                if sym not in all_ohlcv or today not in all_ohlcv[sym].index:
                    continue
                row    = all_ohlcv[sym].loc[today]
                stop   = pos["stop"]
                target = pos["target"]
                reason = None
                exit_px = None

                if row["low"] <= stop:
                    # Gap-down protection: exit at min(open, stop)
                    exit_px = min(row["open"], stop)
                    reason  = "stop_loss"
                elif row["high"] >= target:
                    exit_px = target
                    reason  = "take_profit"
                elif sym in all_signals and today in all_signals[sym].index:
                    if all_signals[sym].loc[today, "sell"]:
                        exit_px = row["open"]
                        reason  = "sell_signal"

                if reason:
                    to_exit.append((sym, exit_px, reason))

            for sym, exit_px, reason in to_exit:
                pos     = positions.pop(sym)
                shares  = pos["shares"]
                proceeds = shares * exit_px
                cash    += proceeds
                pnl     = proceeds - shares * pos["entry_price"]
                trade_log.append({
                    "symbol":      sym,
                    "entry_date":  pos["entry_date"],
                    "exit_date":   today,
                    "entry_price": pos["entry_price"],
                    "exit_price":  exit_px,
                    "shares":      shares,
                    "pnl":         round(pnl, 2),
                    "pnl_pct":     round(pnl / (shares * pos["entry_price"]) * 100, 2),
                    "exit_reason": reason,
                })

            # --- Step 3: Mark portfolio to market (close prices) ---
            portfolio_value = cash
            for sym, pos in positions.items():
                if sym in all_ohlcv and today in all_ohlcv[sym].index:
                    close_px = all_ohlcv[sym].loc[today, "close"]
                    portfolio_value += pos["shares"] * close_px

            equity_rows.append({"date": today, "equity": portfolio_value})

            # --- Step 4: Find best buy candidate → queue for tomorrow ---
            if pending is None:
                best_sym   = None
                best_score = -1
                for sym, sig in all_signals.items():
                    if today not in sig.index:
                        continue
                    if sym in positions:
                        continue
                    row = sig.loc[today]
                    if not row["buy"]:
                        continue
                    score = _score_row(
                        rsi=row["rsi"],
                        zscore=row["zscore"],
                        macd_hist=row["macd_hist"],
                        price=row["close"],
                    )
                    if score > best_score:
                        best_score = score
                        best_sym   = sym
                if best_sym:
                    pending = (best_sym, all_signals[best_sym].loc[today, "close"])

        equity_df = pd.DataFrame(equity_rows).set_index("date")
        trades_df = pd.DataFrame(trade_log) if trade_log else pd.DataFrame(
            columns=["symbol", "entry_date", "exit_date", "entry_price",
                     "exit_price", "shares", "pnl", "pnl_pct", "exit_reason"]
        )
        return equity_df, trades_df

    # ------------------------------------------------------------------
    # SPY benchmark
    # ------------------------------------------------------------------
    def spy_benchmark(self, equity_df):
        """
        Buy as many whole SPY shares as possible at the strategy's start date,
        hold through the strategy's end date.

        Returns an equity_df aligned to the same date range.
        """
        start_date = equity_df.index[0]
        end_date   = equity_df.index[-1]

        spy = _load_symbol("SPY", self.db_path)
        if spy is None:
            return None

        spy = spy[(spy.index >= start_date) & (spy.index <= end_date)]
        if spy.empty:
            return None

        buy_price = spy.iloc[0]["open"]
        shares    = math.floor(self.initial_capital / buy_price)
        cash_left = self.initial_capital - shares * buy_price

        spy_equity = pd.DataFrame(
            {"equity": cash_left + shares * spy["close"].values},
            index=spy.index,
        )
        return spy_equity
