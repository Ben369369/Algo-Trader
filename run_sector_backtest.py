"""
run_sector_backtest.py — Sector rotation backtest

Strategy:
  - Each month, rank the 11 SPDR sector ETFs by composite 3m+6m momentum
  - Buy the top N sectors (default 3), equal-weighted
  - Rebalance when the top-N set changes or at month-end
  - Cash when SPY is below its 200-day SMA (bear market defense)

Usage:
    python run_sector_backtest.py
    python run_sector_backtest.py --top 2
    python run_sector_backtest.py --capital 50000
"""
import argparse
import sqlite3
import numpy as np
import pandas as pd
from config.settings import Config
from strategy.sector_rotation import SECTOR_ETFS, SECTOR_NAMES, LOOKBACK_3M, LOOKBACK_6M, SKIP_DAYS
from backtest.metrics import compute_metrics

INITIAL_CAPITAL = 100_000.0
COMMISSION_PCT   = 0.0005   # 0.05% per trade (ETF commission is near-zero)
SLIPPAGE_PCT     = 0.0005


def _load(symbol, db_path):
    with sqlite3.connect(db_path) as conn:
        df = pd.read_sql_query(
            "SELECT timestamp, open, high, low, close, volume FROM ohlcv "
            "WHERE symbol=? ORDER BY timestamp",
            conn, params=(symbol,)
        )
    if df.empty:
        return None
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    return df.set_index("timestamp")


def _momentum_score(prices, today_idx):
    """Composite of 3m and 6m momentum, skipping last month."""
    try:
        p_now   = prices.iloc[today_idx]
        p_3m    = prices.iloc[today_idx - (LOOKBACK_3M  + SKIP_DAYS)]
        p_6m    = prices.iloc[today_idx - (LOOKBACK_6M  + SKIP_DAYS)]
        mom_3m  = p_now / p_3m - 1
        mom_6m  = p_now / p_6m - 1
        return (mom_3m + mom_6m) / 2
    except IndexError:
        return None


def run(top_n=3, initial_capital=INITIAL_CAPITAL, db_path=None):
    db_path = db_path or Config.DB_PATH

    # Load all sector ETF price data
    etf_data = {}
    for sym in SECTOR_ETFS:
        df = _load(sym, db_path)
        if df is not None and len(df) >= LOOKBACK_6M + SKIP_DAYS + 5:
            etf_data[sym] = df

    if not etf_data:
        raise RuntimeError("No sector ETF data found. Run download first.")

    # Load SPY for bear-market filter (cash when SPY < SMA200)
    spy_df = _load("SPY", db_path)
    if spy_df is not None:
        spy_sma200 = spy_df["close"].rolling(200).mean()
        spy_above  = (spy_df["close"] > spy_sma200).reindex(spy_df.index)
    else:
        spy_above = None

    # Build common trading calendar
    all_dates = sorted(set.union(*[set(df.index) for df in etf_data.values()]))

    # --- Simulation ---
    cash       = initial_capital
    holdings   = {}          # sym -> {shares, entry_price}
    equity_rows = []
    trade_log   = []

    last_rebalance_month = None

    for i, today in enumerate(all_dates):
        # ---- SPY bear-market defense: go to cash ----
        spy_ok = True
        if spy_above is not None and today in spy_above.index:
            spy_ok = bool(spy_above.loc[today])

        # ---- Monthly rebalance trigger ----
        this_month = (today.year, today.month)
        do_rebalance = (this_month != last_rebalance_month)

        if do_rebalance and spy_ok:
            last_rebalance_month = this_month

            # Rank sectors by composite momentum
            scores = {}
            for sym, df in etf_data.items():
                if today not in df.index:
                    continue
                idx = df.index.get_loc(today)
                score = _momentum_score(df["close"], idx)
                if score is not None:
                    scores[sym] = score

            if scores:
                ranked = sorted(scores.items(), key=lambda x: x[1], reverse=True)
                target = [sym for sym, _ in ranked[:top_n]]
            else:
                target = list(holdings.keys())

            # Exit positions not in new target
            for sym in list(holdings.keys()):
                if sym not in target:
                    pos = holdings.pop(sym)
                    if today not in etf_data[sym].index:
                        continue
                    exit_px = float(etf_data[sym].loc[today, "open"]) * (1 - SLIPPAGE_PCT)
                    proceeds = pos["shares"] * exit_px * (1 - COMMISSION_PCT)
                    pnl = proceeds - pos["shares"] * pos["entry_price"]
                    cash += proceeds
                    trade_log.append({
                        "symbol":      sym,
                        "entry_date":  pos["entry_date"],
                        "exit_date":   today,
                        "entry_price": pos["entry_price"],
                        "exit_price":  round(exit_px, 4),
                        "shares":      pos["shares"],
                        "pnl":         round(pnl, 2),
                        "pnl_pct":     round(pnl / (pos["shares"] * pos["entry_price"]) * 100, 2),
                        "exit_reason": "rebalance",
                    })

            # Enter new positions with equal allocation
            new_entries = [sym for sym in target if sym not in holdings]
            if new_entries:
                alloc_per = cash / (len(new_entries) + len(holdings))
                for sym in new_entries:
                    if today not in etf_data[sym].index:
                        continue
                    entry_px = float(etf_data[sym].loc[today, "open"]) * (1 + SLIPPAGE_PCT)
                    shares = int(alloc_per / entry_px)
                    if shares <= 0:
                        continue
                    cost = shares * entry_px * (1 + COMMISSION_PCT)
                    if cost > cash:
                        shares = int(cash / (entry_px * (1 + COMMISSION_PCT)))
                    if shares <= 0:
                        continue
                    cost = shares * entry_px * (1 + COMMISSION_PCT)
                    cash -= cost
                    holdings[sym] = {
                        "shares":      shares,
                        "entry_price": round(entry_px, 4),
                        "entry_date":  today,
                    }

        elif not spy_ok:
            # Bear market: exit everything to cash
            if holdings:
                last_rebalance_month = None
            for sym in list(holdings.keys()):
                pos = holdings.pop(sym)
                if today not in etf_data[sym].index:
                    continue
                exit_px = float(etf_data[sym].loc[today, "open"]) * (1 - SLIPPAGE_PCT)
                proceeds = pos["shares"] * exit_px * (1 - COMMISSION_PCT)
                pnl = proceeds - pos["shares"] * pos["entry_price"]
                cash += proceeds
                trade_log.append({
                    "symbol":      sym,
                    "entry_date":  pos["entry_date"],
                    "exit_date":   today,
                    "entry_price": pos["entry_price"],
                    "exit_price":  round(exit_px, 4),
                    "shares":      pos["shares"],
                    "pnl":         round(pnl, 2),
                    "pnl_pct":     round(pnl / (pos["shares"] * pos["entry_price"]) * 100, 2),
                    "exit_reason": "bear_market",
                })

        # Mark to market
        portfolio_value = cash
        for sym, pos in holdings.items():
            if today in etf_data[sym].index:
                portfolio_value += pos["shares"] * float(etf_data[sym].loc[today, "close"])
        equity_rows.append({"date": today, "equity": portfolio_value})

    equity_df = pd.DataFrame(equity_rows).set_index("date")
    trades_df = pd.DataFrame(trade_log) if trade_log else pd.DataFrame(
        columns=["symbol", "entry_date", "exit_date", "entry_price",
                 "exit_price", "shares", "pnl", "pnl_pct", "exit_reason"]
    )
    return equity_df, trades_df


def _spy_benchmark(equity_df, db_path=None):
    db_path = db_path or Config.DB_PATH
    spy = _load("SPY", db_path)
    if spy is None:
        return None
    start = equity_df.index[0]
    end   = equity_df.index[-1]
    spy   = spy[(spy.index >= start) & (spy.index <= end)]
    if spy.empty:
        return None
    shares = int(INITIAL_CAPITAL / float(spy["close"].iloc[0]))
    equity = spy["close"] * shares
    return pd.DataFrame({"equity": equity})


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--top",     type=int,   default=3)
    parser.add_argument("--capital", type=float, default=INITIAL_CAPITAL)
    args = parser.parse_args()

    print(f"\nRunning sector rotation backtest (top {args.top} sectors)...")
    equity_df, trades_df = run(top_n=args.top, initial_capital=args.capital)

    if equity_df.empty:
        print("ERROR: No equity data generated.")
        return

    m = compute_metrics(equity_df, trades_df, args.capital)

    print(f"\n{'='*50}")
    print(f"  SECTOR ROTATION (top {args.top})")
    print(f"{'='*50}")
    print(f"  Period        {m['start_date']}  ->  {m['end_date']}  ({m['years']:.1f} yrs)")
    print(f"  Total Return  {m['total_return']:>8.2f}%")
    print(f"  CAGR          {m['cagr']:>8.2f}%")
    print(f"  Max Drawdown  {m['max_drawdown']:>8.2f}%")
    print(f"  Sharpe Ratio  {m['sharpe_ratio']:>8.3f}")
    print(f"  # Trades      {m['num_trades']:>8}")
    print(f"  Win Rate      {m['win_rate']:>8.2f}%")
    print(f"  Avg Win       {m['avg_win_pct']:>8.2f}%")
    print(f"  Avg Loss      {m['avg_loss_pct']:>8.2f}%")

    # SPY benchmark
    spy_eq = _spy_benchmark(equity_df)
    if spy_eq is not None:
        spy_m = compute_metrics(spy_eq, pd.DataFrame(columns=["pnl", "pnl_pct", "exit_reason"]), args.capital)
        print(f"\n{'='*50}")
        print(f"  SPY BENCHMARK")
        print(f"{'='*50}")
        print(f"  Total Return  {spy_m['total_return']:>8.2f}%")
        print(f"  CAGR          {spy_m['cagr']:>8.2f}%")
        print(f"  Max Drawdown  {spy_m['max_drawdown']:>8.2f}%")
        print(f"  Sharpe Ratio  {spy_m['sharpe_ratio']:>8.3f}")
        alpha = m['cagr'] - spy_m['cagr']
        print(f"\n  Alpha vs SPY  {alpha:>+8.2f}%")

    # Top trades
    if not trades_df.empty:
        print(f"\n{'='*50}")
        print(f"  TOP 10 TRADES")
        print(f"{'='*50}")
        top = trades_df.nlargest(10, "pnl")[
            ["symbol", "entry_date", "exit_date", "pnl", "pnl_pct", "exit_reason"]
        ]
        print(top.to_string(index=False))

    print()


if __name__ == "__main__":
    main()
