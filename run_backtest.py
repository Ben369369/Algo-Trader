"""
run_backtest.py — Backtesting CLI (live-mirror engine)

The engine simulates the SAME pipeline the live bot runs: SPY regime
detection decides which strategy may enter each day, candidates are ranked
with the live TradeScorer, sizing/stops/exits match TradeExecutor, and the
sector rotation sleeve rebalances ~monthly.

Usage:
    python run_backtest.py                          # full composite (default)
    python run_backtest.py --strategy momentum      # momentum sleeve only
    python run_backtest.py --strategy mean_reversion
    python run_backtest.py --strategy sectors       # sector sleeve only
    python run_backtest.py --strategy all           # every sleeve + composite
    python run_backtest.py --db data/market_data_10y.db   # long-history DB
    python run_backtest.py --capital 50000

No Alpaca API keys required.

Not simulated (no historical data): earnings blackout filter, Tiger boosts.
"""
import sys
import argparse
import pandas as pd
from pathlib import Path
from backtest.engine import BacktestEngine
from backtest.metrics import compute_metrics
from config.settings import Config

INITIAL_CAPITAL = 100_000.0

# Sleeve configurations for the engine
CASES = {
    "composite": {
        "label":  "COMPOSITE (momentum + mean reversion + sectors)",
        "kwargs": dict(strategies=("momentum", "mean_reversion"),
                       include_sector_sleeve=True),
    },
    "momentum": {
        "label":  "MOMENTUM sleeve",
        "kwargs": dict(strategies=("momentum",)),
    },
    "mean_reversion": {
        "label":  "MEAN REVERSION sleeve",
        "kwargs": dict(strategies=("mean_reversion",)),
    },
    "sectors": {
        "label":  "SECTOR ROTATION sleeve",
        "kwargs": dict(strategies=(), include_sector_sleeve=True),
    },
}


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
    header = f"\n{'='*52}\n  {label}\n{'='*52}"
    return header + "\n" + "\n".join(lines)


def _print_strategy_pnl(trades_df):
    if trades_df.empty or "strategy" not in trades_df.columns:
        return
    print("\n  P&L by strategy:")
    for strat, g in trades_df.groupby("strategy"):
        wins = (g["pnl"] > 0).mean() * 100
        print(f"    {strat:<16} n={len(g):>4}  pnl=${g['pnl'].sum():>10,.2f}  win={wins:.1f}%")


def _fmt_comparison(results):
    keys = [
        ("CAGR (%)",         "cagr"),
        ("Total Return (%)", "total_return"),
        ("Max Drawdown (%)", "max_drawdown"),
        ("Sharpe Ratio",     "sharpe_ratio"),
        ("# Trades",         "num_trades"),
        ("Win Rate (%)",     "win_rate"),
    ]
    col_w, row_lbl = 16, 22
    names = [r["label_short"] for r in results]

    lines = [f"\n{'='*60}", "  SIDE-BY-SIDE COMPARISON", f"{'='*60}",
             f"  {'Metric':<{row_lbl}}" + "".join(f"{n:>{col_w}}" for n in names),
             "  " + "-" * (row_lbl + col_w * len(names))]

    for display, key in keys:
        vals = []
        for r in results:
            v = r["metrics"][key]
            vals.append(f"{v:>{col_w}.2f}" if isinstance(v, float) else f"{v:>{col_w}}")
        lines.append(f"  {display:<{row_lbl}}" + "".join(vals))

    spy = next((r for r in results if r["label_short"] == "SPY"), None)
    if spy:
        lines.append("  " + "-" * (row_lbl + col_w * len(results)))
        for r in results:
            if r["label_short"] == "SPY":
                continue
            alpha = r["metrics"]["cagr"] - spy["metrics"]["cagr"]
            lines.append(f"  {'Alpha vs SPY (' + r['label_short'] + ')':<{row_lbl}}{alpha:>+{col_w}.2f}%")
    return "\n".join(lines)


def _print_top_trades(trades_df, label, n=10):
    if trades_df.empty:
        print(f"\n  No completed trades for {label}.")
        return
    top = (trades_df
           .assign(abs_pnl=trades_df["pnl"].abs())
           .sort_values("abs_pnl", ascending=False)
           .head(n)
           .drop(columns=["abs_pnl"]))
    print(f"\n{'='*52}")
    print(f"  TOP {n} TRADES — {label}")
    print(f"{'='*52}")
    pd.set_option("display.max_columns", None)
    pd.set_option("display.width", 140)
    cols = ["symbol", "strategy", "entry_date", "exit_date", "entry_price",
            "exit_price", "shares", "pnl", "pnl_pct", "exit_reason"]
    print(top[[c for c in cols if c in top.columns]].to_string(index=False))


def main():
    parser = argparse.ArgumentParser(description="Backtesting CLI (live-mirror engine)")
    parser.add_argument("--capital",  type=float, default=INITIAL_CAPITAL)
    parser.add_argument("--strategy", choices=[*CASES.keys(), "all"],
                        default="composite")
    parser.add_argument("--db", type=str, default=None,
                        help="Path to an alternate OHLCV SQLite DB "
                             "(e.g. data/market_data_10y.db)")
    args = parser.parse_args()

    db_path = Path(args.db) if args.db else Config.DB_PATH
    run_names = list(CASES.keys()) if args.strategy == "all" else [args.strategy]

    print(f"\nRunning live-mirror backtest with ${args.capital:,.0f} starting capital ...")
    print(f"Data: {db_path}")

    all_results = []
    for name in run_names:
        cfg = CASES[name]
        print(f"  Computing {cfg['label']} ...")
        engine = BacktestEngine(initial_capital=args.capital, db_path=db_path,
                                **cfg["kwargs"])
        equity_df, trades_df = engine.run()
        if equity_df.empty:
            print(f"  ERROR: No data for {cfg['label']}. Skipping.")
            continue
        metrics = compute_metrics(equity_df, trades_df, args.capital)
        all_results.append({
            "label":       cfg["label"],
            "label_short": name,
            "metrics":     metrics,
            "equity_df":   equity_df,
            "trades_df":   trades_df,
            "engine":      engine,
        })

    if not all_results:
        print("ERROR: No results generated.")
        sys.exit(1)

    # SPY benchmark aligned to the first case's date range
    spy_equity = all_results[0]["engine"].spy_benchmark(all_results[0]["equity_df"])
    if spy_equity is not None and not spy_equity.empty:
        spy_trades = pd.DataFrame(columns=["pnl", "pnl_pct", "exit_reason"])
        all_results.append({
            "label":       "SPY BENCHMARK (buy & hold)",
            "label_short": "SPY",
            "metrics":     compute_metrics(spy_equity, spy_trades, args.capital),
            "equity_df":   spy_equity,
            "trades_df":   spy_trades,
            "engine":      None,
        })
    else:
        print("  SPY benchmark unavailable (no SPY data in database).")

    for r in all_results:
        print(_fmt_metrics(r["metrics"], r["label"]))
        _print_strategy_pnl(r["trades_df"])

    if len(all_results) > 1:
        print(_fmt_comparison(all_results))

    for r in all_results:
        if r["label_short"] != "SPY":
            _print_top_trades(r["trades_df"], r["label"])

    print()


if __name__ == "__main__":
    main()
