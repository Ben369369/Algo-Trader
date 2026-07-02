"""
backtest/engine.py — event-driven daily backtest that mirrors the live system.

Simulates the same pipeline main.py runs every morning:
  1. Regime detection on SPY (SMA50/SMA200/RSI) decides which strategy may
     enter each day: TRENDING_UP -> momentum, RANGING -> mean reversion,
     TRENDING_DOWN -> no new entries.
  2. Candidates are ranked with the SAME TradeScorer used live.
  3. Position sizing on total equity: risk MAX_RISK_PER_TRADE at an
     ATR_STOP_MULT x ATR stop, capped at MAX_POSITION_SIZE of equity.
  4. Sector concentration guard (MAX_POSITIONS_PER_SECTOR).
  5. Portfolio drawdown circuit breaker halts new entries.
  6. Exit ladder per position, matching TradeExecutor.check_exits:
     stop (ATR bracket / breakdown floor), take-profit, time exit,
     trailing stop, signal exit.
  7. Optional sector rotation sleeve: top-N SPDR ETFs by 3m+6m skip-month
     momentum, ~monthly rebalance, ALLOCATION_PCT of equity each,
     7% trailing stop, no fixed stop/target (mirrors SectorRotationExecutor).

Timing convention (matches live): signals are computed on day T-1 closing
data and executed at day T's open — exactly like the 9:30 live run, which
scans on data through yesterday and sends orders minutes after the open.

Costs: per-share commission with a minimum, plus slippage both ways.

Live features NOT simulated (no reliable historical data):
  - Earnings blackout filter (yfinance only exposes upcoming dates)
  - Tiger Capital research boosts (scorers called with use_tiger=False)
"""
import math
import sqlite3
import numpy as np
import pandas as pd
from config.settings import Config
from strategy.indicators import Indicators
from strategy.signals import SignalDetector
from strategy.momentum_signals import MomentumSignalDetector
from strategy.scorer import TradeScorer
from strategy.sector_rotation import (
    SECTOR_ETFS, LOOKBACK_3M, LOOKBACK_6M, LOOKBACK_12M, SKIP_DAYS,
)

# Transaction cost model
COMMISSION_PER_SHARE = 0.005   # $0.005/share
MIN_COMMISSION       = 1.00    # minimum $1 per order
SLIPPAGE_PCT         = 0.0005  # 0.05% market impact each way

MAX_ENTRIES_PER_DAY = 5        # live: executor.execute_best(ranked, max_entries=5)

# Sector sleeve constants (mirror SectorRotationExecutor)
SECTOR_ALLOC_PCT   = 0.15
SECTOR_TOP_N       = 3
SECTOR_REBAL_DAYS  = 25
SECTOR_MIN_BARS    = LOOKBACK_12M + SKIP_DAYS

# Index/benchmark symbols never traded as stocks
_NON_TRADE = {"SPY", "QQQ", "IWM", "DIA", *SECTOR_ETFS}


def _commission(shares):
    return max(shares * COMMISSION_PER_SHARE, MIN_COMMISSION)


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
    return df.set_index("timestamp")


def _default_stock_params():
    """Per-strategy exit parameters. Defaults = the live system (Config)."""
    return {
        "momentum": {
            "take_profit_pct": Config.MOM_TAKE_PROFIT_PCT,
            "max_hold_days":   Config.MOM_MAX_HOLD_DAYS,
            "trail_stop_pct":  Config.MOM_TRAIL_STOP_PCT,
            "breakdown_pct":   None,   # retired: ATR stop is the only hard stop
        },
        "mean_reversion": {
            "take_profit_pct": Config.MR_TAKE_PROFIT_PCT,
            "max_hold_days":   Config.MR_MAX_HOLD_DAYS,
            "trail_stop_pct":  Config.MR_TRAIL_STOP_PCT,
            "breakdown_pct":   None,
        },
    }


class BacktestEngine:

    def __init__(self, initial_capital=100_000.0, db_path=None,
                 strategies=("momentum", "mean_reversion"),
                 include_sector_sleeve=False,
                 sector_abs_filter=None,  # None -> Config.SECTOR_ABS_FILTER (live)
                 sector_alloc_pct=SECTOR_ALLOC_PCT,
                 sector_top_n=SECTOR_TOP_N,
                 sector_trail_pct=None,   # None -> Config.SECTOR_TRAIL_PCT (live); 0 disables
                 breaker_covers_sectors=True,
                 stock_params=None,
                 momentum_signal_kwargs=None,
                 mr_signal_kwargs=None,
                 regime_map=None):
        """
        strategies             : which stock strategies participate. The regime
                                 still decides which one may enter on a given
                                 day; listing only one simulates that sleeve
                                 alone (its regime gate still applies).
        include_sector_sleeve  : add the sector ETF rotation sleeve.
        sector_abs_filter      : require composite momentum > 0 to hold a
                                 sector (absolute-momentum cash filter).
        breaker_covers_sectors : drawdown circuit breaker also halts sector
                                 ETF entries (live currently does NOT).
        stock_params           : per-strategy overrides merged over
                                 _default_stock_params().
        momentum_signal_kwargs : extra kwargs for MomentumSignalDetector.detect
                                 (e.g. {"sell_rule": "sma50"}).
        """
        self.initial_capital = initial_capital
        self.db_path         = db_path or Config.DB_PATH
        self.strategies      = tuple(strategies)
        self.include_sectors = include_sector_sleeve
        self.sector_abs_filter      = (Config.SECTOR_ABS_FILTER if sector_abs_filter is None
                                       else sector_abs_filter)
        self.sector_alloc_pct       = sector_alloc_pct
        self.sector_top_n           = sector_top_n
        self.sector_trail_pct       = (Config.SECTOR_TRAIL_PCT if sector_trail_pct is None
                                       else sector_trail_pct)
        self.breaker_covers_sectors = breaker_covers_sectors
        self.momentum_signal_kwargs = momentum_signal_kwargs or {}
        self.mr_signal_kwargs       = mr_signal_kwargs or {}
        # Which strategy may ENTER in each regime. Default = live main.py.
        # TRENDING_DOWN is never mapped: no new entries in a death cross.
        self.regime_map = regime_map or {"TRENDING_UP": "momentum",
                                         "RANGING":     "mean_reversion"}

        self.stock_params = _default_stock_params()
        if stock_params:
            for strat, overrides in stock_params.items():
                self.stock_params.setdefault(strat, {}).update(overrides)

    # ------------------------------------------------------------------
    # Regime series (vectorised RegimeDetector.detect)
    # ------------------------------------------------------------------
    @staticmethod
    def _regime_series(spy_df):
        close  = spy_df["close"]
        sma50  = Indicators.sma(close, 50)
        sma200 = Indicators.sma(close, 200)
        rsi    = Indicators.rsi(close)

        vals = np.select(
            [
                sma50 < sma200,                                    # death cross
                close < sma50 * 0.97,                              # pullback in uptrend
                (close >= sma50) & (sma50 > sma200) & (rsi > 45),  # healthy uptrend
            ],
            ["TRENDING_DOWN", "RANGING", "TRENDING_UP"],
            default="RANGING",
        )
        out = pd.Series(vals, index=spy_df.index)
        out[sma200.isna()] = "RANGING"   # live default with insufficient data
        return out

    # ------------------------------------------------------------------
    # Position sizing (mirrors PositionSizer.calculate on total equity)
    # ------------------------------------------------------------------
    @staticmethod
    def _size_shares(equity, price, atr):
        if price <= 0:
            return 0
        risk_dollars = equity * Config.MAX_RISK_PER_TRADE
        if atr and atr > 0:
            risk_per_share = atr * Config.ATR_STOP_MULT
        else:
            risk_per_share = price * 0.06
        if risk_per_share <= 0:
            return 0
        shares = risk_dollars / risk_per_share
        shares = min(shares, equity * Config.MAX_POSITION_SIZE / price)
        return math.floor(shares)

    # ------------------------------------------------------------------
    # Sector sleeve helpers
    # ------------------------------------------------------------------
    def _rank_sectors_asof(self, today):
        """Composite 3m+6m skip-month momentum using data strictly before today."""
        rows = []
        for sym, close in self._etf_close.items():
            sub = close[close.index < today]
            if len(sub) < SECTOR_MIN_BARS:
                continue
            mom_3m = sub.iloc[-1] / sub.iloc[-(LOOKBACK_3M + SKIP_DAYS)] - 1
            mom_6m = sub.iloc[-1] / sub.iloc[-(LOOKBACK_6M + SKIP_DAYS)] - 1
            rows.append((sym, (mom_3m + mom_6m) / 2))
        rows.sort(key=lambda r: r[1], reverse=True)
        return rows

    # ------------------------------------------------------------------
    # Main entry point
    # ------------------------------------------------------------------
    def run(self, symbols=None):
        """
        Returns
        -------
        equity_df : pd.DataFrame — daily portfolio value (index = date)
        trades_df : pd.DataFrame — one row per closed trade (incl. strategy)
        """
        symbols = symbols or [s for s in Config.symbols() if s not in _NON_TRADE]

        # --- SPY: regime + relative strength ---
        spy_df = _load_symbol("SPY", self.db_path)
        if spy_df is not None and len(spy_df) >= 210:
            regime_series = self._regime_series(spy_df)
            spy_ret20     = spy_df["close"].pct_change(20)
        else:
            regime_series = None
            spy_ret20     = None

        # --- Load stock data & compute signals per strategy ---
        data, sig = {}, {s: {} for s in self.strategies}
        for sym in symbols:
            df = _load_symbol(sym, self.db_path)
            if df is None or len(df) < 210:   # live scanner threshold
                continue
            data[sym] = df
            if "momentum" in self.strategies:
                sig["momentum"][sym] = MomentumSignalDetector.detect(
                    df, spy_ret20, **self.momentum_signal_kwargs)
            if "mean_reversion" in self.strategies:
                sig["mean_reversion"][sym] = SignalDetector.detect(
                    df, **self.mr_signal_kwargs)

        if not data:
            raise RuntimeError("No symbol data found in the database.")

        # --- Sector sleeve data ---
        self._etf_close = {}
        etf_data = {}
        if self.include_sectors:
            for sym in SECTOR_ETFS:
                df = _load_symbol(sym, self.db_path)
                if df is not None:
                    etf_data[sym] = df
                    self._etf_close[sym] = df["close"]

        # --- Trading calendar: union of signal dates (post-warmup) ---
        date_sets = [set(f.index) for frames in sig.values() for f in frames.values()]
        if date_sets:
            all_dates = sorted(set.union(*date_sets))
        elif self.include_sectors and self._etf_close:
            # Sector-sleeve-only run: start when the first ETF becomes rankable
            starts = [c.index[SECTOR_MIN_BARS] for c in self._etf_close.values()
                      if len(c) > SECTOR_MIN_BARS]
            if not starts:
                raise RuntimeError("Not enough sector ETF history to rank.")
            first = min(starts)
            all_dates = sorted({d for c in self._etf_close.values()
                                for d in c.index if d >= first})
        else:
            raise RuntimeError("No signals produced — check strategies/data.")

        if regime_series is not None:
            regime = regime_series.reindex(all_dates, method="ffill").fillna("RANGING")
        else:
            regime = pd.Series("RANGING", index=all_dates)

        # --- Simulation state ---
        cash        = self.initial_capital
        equity_prev = self.initial_capital   # yesterday's close equity (sizing/breaker)
        peak        = self.initial_capital
        positions   = {}   # sym -> dict
        pending     = []   # [(sym, atr, strategy), ...] ranked, queued yesterday
        last_rebal  = None
        last_close  = {}   # sym -> last seen close (marking gaps)
        equity_rows = []
        trade_log   = []

        strat_for_regime = self.regime_map

        def close_position(sym, exit_px, reason, today):
            pos      = positions.pop(sym)
            shares   = pos["shares"]
            adj_exit = round(exit_px * (1 - SLIPPAGE_PCT), 4)
            proceeds = shares * adj_exit - _commission(shares)
            pnl      = proceeds - shares * pos["entry_price"]
            trade_log.append({
                "symbol":      sym,
                "strategy":    pos["strategy"],
                "entry_date":  pos["entry_date"],
                "exit_date":   today,
                "entry_price": pos["entry_price"],
                "exit_price":  adj_exit,
                "shares":      shares,
                "pnl":         round(pnl, 2),
                "pnl_pct":     round(pnl / (shares * pos["entry_price"]) * 100, 2),
                "exit_reason": reason,
            })
            return proceeds

        for today in all_dates:
            breaker_active = (peak - equity_prev) / peak > Config.MAX_DRAWDOWN_LIMIT

            # ---- Phase A: open-priced exits (known at the open) ----
            for sym in list(positions):
                pos = positions[sym]
                src = etf_data if pos["is_etf"] else data
                if sym not in src or today not in src[sym].index:
                    continue
                open_px = float(src[sym].loc[today, "open"])

                if pos["is_etf"]:
                    continue   # ETF exits: rebalance (below) + trailing (phase C)

                params = self.stock_params[pos["strategy"]]

                # Time exit
                days_held = (today - pos["entry_date"]).days
                if days_held >= params["max_hold_days"]:
                    cash += close_position(sym, open_px, "time_exit", today)
                    continue

                # Signal exit — most recent signal on/after entry, before today
                frame = sig[pos["strategy"]].get(sym)
                if frame is not None:
                    i = frame.index.searchsorted(today)
                    if i > 0:
                        d = frame.index[i - 1]
                        if d >= pos["entry_date"] and bool(frame.loc[d, "sell"]):
                            cash += close_position(sym, open_px, "sell_signal", today)

            # ---- Phase A2: sector sleeve rebalance (at the open) ----
            if self.include_sectors:
                rebal_due = last_rebal is None or (today - last_rebal).days >= SECTOR_REBAL_DAYS
                if rebal_due:
                    ranked_etfs = self._rank_sectors_asof(today)
                    if ranked_etfs:
                        last_rebal = today
                        if self.sector_abs_filter:
                            ranked_etfs = [r for r in ranked_etfs if r[1] > 0]
                        targets = [s for s, _ in ranked_etfs[:self.sector_top_n]]

                        # Exit ETFs that dropped out of the top N
                        for sym in [s for s in positions if positions[s]["is_etf"]]:
                            if sym not in targets:
                                if today in etf_data[sym].index:
                                    open_px = float(etf_data[sym].loc[today, "open"])
                                    cash += close_position(sym, open_px, "rebalance", today)

                        # Enter new ETFs (skipped if breaker covers sectors)
                        if not (breaker_active and self.breaker_covers_sectors):
                            alloc = equity_prev * self.sector_alloc_pct
                            for sym in targets:
                                if sym in positions or today not in etf_data[sym].index:
                                    continue
                                open_px = float(etf_data[sym].loc[today, "open"])
                                entry_px = round(open_px * (1 + SLIPPAGE_PCT), 4)
                                shares = int(alloc / entry_px)
                                cost = shares * entry_px + _commission(shares)
                                if shares <= 0 or cost > cash:
                                    continue   # live skips, doesn't downsize
                                cash -= cost
                                positions[sym] = {
                                    "shares":      shares,
                                    "entry_price": entry_px,
                                    "entry_date":  today,
                                    "stop":        None,
                                    "target":      None,
                                    "hwm":         entry_px,
                                    "strategy":    "sector",
                                    "is_etf":      True,
                                }

            # ---- Phase A3: stock entries queued yesterday (at the open) ----
            if pending and not breaker_active:
                held_sectors = {}
                for sym, pos in positions.items():
                    sec = Config.SECTOR_MAP.get(sym, "Other")
                    held_sectors[sec] = held_sectors.get(sec, 0) + 1

                placed = 0
                for sym, atr, strat in pending:
                    if placed >= MAX_ENTRIES_PER_DAY:
                        break
                    if sym in positions:
                        continue
                    sec = Config.SECTOR_MAP.get(sym, "Other")
                    if held_sectors.get(sec, 0) >= Config.MAX_POSITIONS_PER_SECTOR:
                        continue
                    if sym not in data or today not in data[sym].index:
                        continue
                    open_px  = float(data[sym].loc[today, "open"])
                    entry_px = round(open_px * (1 + SLIPPAGE_PCT), 4)
                    shares   = self._size_shares(equity_prev, entry_px, atr)
                    cost     = shares * entry_px + _commission(shares)
                    if shares <= 0 or cost > cash:
                        continue
                    cash -= cost

                    params = self.stock_params[strat]
                    if atr and atr > 0:
                        stop = entry_px - atr * Config.ATR_STOP_MULT
                    else:
                        stop = entry_px * (1 - 0.06)
                    # Live breakdown exit (fixed % below entry) acts as a
                    # floor on how far the stop can sit. None disables it.
                    if params.get("breakdown_pct"):
                        stop = max(stop, entry_px * (1 - params["breakdown_pct"]))

                    positions[sym] = {
                        "shares":      shares,
                        "entry_price": entry_px,
                        "entry_date":  today,
                        "stop":        round(stop, 4),
                        "target":      round(entry_px * (1 + params["take_profit_pct"]), 4),
                        "hwm":         entry_px,
                        "strategy":    strat,
                        "is_etf":      False,
                    }
                    held_sectors[sec] = held_sectors.get(sec, 0) + 1
                    placed += 1
            pending = []

            # ---- Phase B: intraday exits (stop / target), stop first ----
            for sym in list(positions):
                pos = positions[sym]
                if pos["is_etf"]:
                    continue
                if sym not in data or today not in data[sym].index:
                    continue
                row = data[sym].loc[today]
                if pos["stop"] is not None and row["low"] <= pos["stop"]:
                    exit_px = min(float(row["open"]), pos["stop"])   # gap-down protection
                    cash += close_position(sym, exit_px, "stop_loss", today)
                elif pos["target"] is not None and row["high"] >= pos["target"]:
                    exit_px = max(float(row["open"]), pos["target"])
                    cash += close_position(sym, exit_px, "take_profit", today)

            # ---- Phase C: trailing stops (vs prior high-water mark) ----
            for sym in list(positions):
                pos = positions[sym]
                src = etf_data if pos["is_etf"] else data
                if sym not in src or today not in src[sym].index:
                    continue
                row = src[sym].loc[today]
                trail_pct = (self.sector_trail_pct if pos["is_etf"]
                             else self.stock_params[pos["strategy"]]["trail_stop_pct"])
                if trail_pct and trail_pct > 0:
                    trail_px = pos["hwm"] * (1 - trail_pct)
                    # only relevant if tighter than the hard stop
                    if (pos["stop"] is None or trail_px > pos["stop"]) and row["low"] <= trail_px:
                        exit_px = min(float(row["open"]), trail_px)
                        cash += close_position(sym, exit_px, "trailing_stop", today)
                        continue
                if row["high"] > pos["hwm"]:
                    pos["hwm"] = float(row["high"])

            # ---- Phase D: mark to market at the close ----
            equity = cash
            for sym, pos in positions.items():
                src = etf_data if pos["is_etf"] else data
                if sym in src and today in src[sym].index:
                    last_close[sym] = float(src[sym].loc[today, "close"])
                equity += pos["shares"] * last_close.get(sym, pos["entry_price"])
            equity_rows.append({"date": today, "equity": equity})
            peak        = max(peak, equity)
            equity_prev = equity

            # ---- Phase E: build tomorrow's entry queue from today's signals ----
            todays_regime = regime.loc[today]
            strat = strat_for_regime.get(todays_regime)
            if strat in self.strategies:
                rows = []
                for sym, frame in sig[strat].items():
                    if today not in frame.index:
                        continue
                    r = frame.loc[today]
                    if strat == "momentum":
                        rows.append({
                            "symbol":       sym,
                            "price":        float(r["close"]),
                            "rsi":          float(r["rsi"]),
                            "macd_hist":    float(r["macd_hist"]),
                            "atr":          float(r["atr"]),
                            "volume_ratio": float(r["volume_ratio"]),
                            "rel_strength": float(r["rel_strength"]),
                            "bb_position":  float(r["bb_position"]),
                            "buy_signal":   bool(r["buy"]),
                            "sell_signal":  bool(r["sell"]),
                        })
                    else:
                        rows.append({
                            "symbol":       sym,
                            "price":        float(r["close"]),
                            "rsi":          float(r["rsi"]),
                            "zscore":       float(r["zscore"]),
                            "macd_hist":    float(r["macd_hist"]),
                            "atr":          float(r["atr"]),
                            "volume_ratio": float(r["volume_ratio"]),
                            "buy_signal":   bool(r["buy"]),
                            "sell_signal":  bool(r["sell"]),
                        })
                if rows:
                    scan_df = pd.DataFrame(rows)
                    if strat == "momentum":
                        ranked = TradeScorer.score_momentum(scan_df, use_tiger=False)
                    else:
                        ranked = TradeScorer.score(scan_df, use_tiger=False)
                    buys = ranked[ranked["direction"] == "BUY"]
                    pending = [(r["symbol"], float(r["atr"]), strat)
                               for _, r in buys.iterrows()]

        equity_df = pd.DataFrame(equity_rows).set_index("date")
        trades_df = pd.DataFrame(trade_log) if trade_log else pd.DataFrame(
            columns=["symbol", "strategy", "entry_date", "exit_date", "entry_price",
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

        return pd.DataFrame(
            {"equity": cash_left + shares * spy["close"].values},
            index=spy.index,
        )
