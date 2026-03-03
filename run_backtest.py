"""
run_backtest.py — Phase 3 backtesting CLI

Usage:
    python run_backtest.py [--capital 100000]

Does NOT require Alpaca API keys (reads from local SQLite only).
"""
import sys
import argparse
import pandas as pd
from backtest.engine  import BacktestEngine
from backtest.metrics import compute_metrics

INITIAL_CAPITAL = 100_000.0


def _fmt_metrics(m, label):
    lines = [
        f"  {'Period':25s} {m['start_date']}  ->  {m['end_date']}  ({m['years']:.1f} yrs)",
        f"  {'Total Return':25s} {m['total_return']:>8.2f}%",
        f"  {'CAGR':25s} {m['cagr']:>8.2f}%",
        f"  {'Max Drawdown':25s} {m['max_drawdown']:>8.2f}%",
        f"  {'Sharpe Ratio':25s} {m['sharpe_ratio']:>8.3f}",
        f"  {'# Trades':25s} {m['num_trades']:>8}",
        f"  {'Win Rate':25s} {m['win_rate']:>8.2f}%",
        f"  {'Avg Win':25s} {m['avg_win_pct']:>8.2f}%",
        f"  {'Avg Loss':25s} {m['avg_loss_pct']:>8.2f}%",
    ]
    if m["exit_breakdown"]:
        for reason, cnt in m["exit_breakdown"].items():
            lines.append(f"  {'  ' + reason:25s} {cnt:>8}")
    header = f"\n{'='*45}\n  {label}\n{'='*45}"
    return header + "\n" + "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description="Run Phase 3 backtest")
    parser.add_argument("--capital", type=float, default=INITIAL_CAPITAL,
                        help="Starting capital (default: 100000)")
    args = parser.parse_args()

    capital = args.capital
    print(f"\nRunning backtest with ${capital:,.0f} starting capital …")

    engine = BacktestEngine(initial_capital=capital)

    # --- Strategy run ---
    equity_df, trades_df = engine.run()

    if equity_df.empty:
        print("ERROR: No equity data generated. Check that the database has data.")
        sys.exit(1)

    strat_metrics = compute_metrics(equity_df, trades_df, capital)

    # --- SPY benchmark ---
    spy_equity = engine.spy_benchmark(equity_df)
    if spy_equity is not None and not spy_equity.empty:
        spy_trades = pd.DataFrame(columns=["pnl", "pnl_pct", "exit_reason"])
        spy_metrics = compute_metrics(spy_equity, spy_trades, capital)
    else:
        spy_metrics = None

    # --- Print strategy metrics ---
    print(_fmt_metrics(strat_metrics, "STRATEGY METRICS"))

    # --- Print SPY benchmark ---
    if spy_metrics:
        print(_fmt_metrics(spy_metrics, "SPY BUY-AND-HOLD BENCHMARK"))
        alpha = strat_metrics["cagr"] - spy_metrics["cagr"]
        print(f"\n  Alpha (Strategy CAGR - SPY CAGR): {alpha:+.2f}%")
    else:
        print("\n  SPY benchmark unavailable (no SPY data in database).")

    # --- Top 10 trades by absolute P&L ---
    if not trades_df.empty:
        top10 = (trades_df
                 .assign(abs_pnl=trades_df["pnl"].abs())
                 .sort_values("abs_pnl", ascending=False)
                 .head(10)
                 .drop(columns=["abs_pnl"]))
        print(f"\n{'='*45}")
        print("  TOP 10 TRADES BY ABSOLUTE P&L")
        print(f"{'='*45}")
        pd.set_option("display.max_columns", None)
        pd.set_option("display.width", 120)
        cols = ["symbol", "entry_date", "exit_date", "entry_price",
                "exit_price", "shares", "pnl", "pnl_pct", "exit_reason"]
        print(top10[cols].to_string(index=False))
    else:
        print("\n  No completed trades in the backtest period.")

    print()


if __name__ == "__main__":
    main()
