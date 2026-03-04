"""
run_backtest.py — Backtesting CLI

Usage:
    python run_backtest.py                        # mean reversion vs SPY
    python run_backtest.py --strategy momentum    # momentum vs mean reversion vs SPY
    python run_backtest.py --capital 50000        # custom starting capital

Does NOT require Alpaca API keys (reads from local SQLite only).
"""
import sys
import argparse
import pandas as pd
from backtest.engine  import BacktestEngine
from backtest.metrics import compute_metrics
from strategy.signals          import SignalDetector
from strategy.momentum_signals import MomentumSignalDetector

INITIAL_CAPITAL = 100_000.0

# Strategy configurations
STRATEGIES = {
    "mean_reversion": {
        "label":          "MEAN REVERSION",
        "detector":       SignalDetector,
        "stop_loss_pct":  0.06,
        "take_profit_pct": 0.10,
    },
    "momentum": {
        "label":          "MOMENTUM",
        "detector":       MomentumSignalDetector,
        "stop_loss_pct":  0.07,
        "take_profit_pct": 0.15,
    },
}


def _run_strategy(name, capital):
    cfg    = STRATEGIES[name]
    engine = BacktestEngine(
        initial_capital  = capital,
        stop_loss_pct    = cfg["stop_loss_pct"],
        take_profit_pct  = cfg["take_profit_pct"],
    )
    equity_df, trades_df = engine.run(signal_detector_cls=cfg["detector"])
    return engine, equity_df, trades_df


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


def _fmt_comparison(results):
    """Print a side-by-side comparison table for multiple strategies + SPY."""
    keys = [
        ("CAGR (%)",        "cagr"),
        ("Total Return (%)", "total_return"),
        ("Max Drawdown (%)", "max_drawdown"),
        ("Sharpe Ratio",     "sharpe_ratio"),
        ("# Trades",         "num_trades"),
        ("Win Rate (%)",     "win_rate"),
        ("Avg Win (%)",      "avg_win_pct"),
        ("Avg Loss (%)",     "avg_loss_pct"),
    ]
    col_w   = 16
    row_lbl = 22
    names   = [r["label"] for r in results]

    header_row = f"  {'Metric':<{row_lbl}}" + "".join(f"{n:>{col_w}}" for n in names)
    sep        = "  " + "-" * (row_lbl + col_w * len(names))

    lines = [f"\n{'='*60}", "  SIDE-BY-SIDE COMPARISON", f"{'='*60}",
             header_row, sep]

    for display, key in keys:
        vals = []
        for r in results:
            v = r["metrics"][key]
            vals.append(f"{v:>{col_w}.2f}" if isinstance(v, float) else f"{v:>{col_w}}")
        lines.append(f"  {display:<{row_lbl}}" + "".join(vals))

    # Alpha rows vs SPY
    spy_cagr = next(r["metrics"]["cagr"] for r in results if r["label"] == "SPY")
    lines.append(sep)
    for r in results:
        if r["label"] == "SPY":
            continue
        alpha = r["metrics"]["cagr"] - spy_cagr
        lines.append(f"  {'Alpha vs SPY (' + r['label'] + ')':<{row_lbl}}{alpha:>+{col_w}.2f}%")

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
    print(f"\n{'='*45}")
    print(f"  TOP {n} TRADES — {label}")
    print(f"{'='*45}")
    pd.set_option("display.max_columns", None)
    pd.set_option("display.width", 130)
    cols = ["symbol", "entry_date", "exit_date", "entry_price",
            "exit_price", "shares", "pnl", "pnl_pct", "exit_reason"]
    print(top[cols].to_string(index=False))


def main():
    parser = argparse.ArgumentParser(description="Backtesting CLI")
    parser.add_argument("--capital",  type=float, default=INITIAL_CAPITAL)
    parser.add_argument("--strategy", choices=["mean_reversion", "momentum"],
                        default="mean_reversion",
                        help="Strategy to run. 'momentum' also shows mean reversion for comparison.")
    args = parser.parse_args()

    capital = args.capital
    print(f"\nRunning backtest with ${capital:,.0f} starting capital ...")

    # Determine which strategies to run
    run_names = (["momentum", "mean_reversion"]
                 if args.strategy == "momentum"
                 else ["mean_reversion"])

    all_results = []

    for name in run_names:
        label = STRATEGIES[name]["label"]
        print(f"  Computing {label} ...")
        engine, equity_df, trades_df = _run_strategy(name, capital)

        if equity_df.empty:
            print(f"  ERROR: No data for {label}. Skipping.")
            continue

        metrics = compute_metrics(equity_df, trades_df, capital)
        all_results.append({
            "label":     label,
            "metrics":   metrics,
            "equity_df": equity_df,
            "trades_df": trades_df,
            "engine":    engine,
        })

    if not all_results:
        print("ERROR: No results generated.")
        sys.exit(1)

    # SPY benchmark — aligned to first strategy's date range
    ref_equity = all_results[0]["equity_df"]
    spy_equity = all_results[0]["engine"].spy_benchmark(ref_equity)
    spy_trades = pd.DataFrame(columns=["pnl", "pnl_pct", "exit_reason"])
    if spy_equity is not None and not spy_equity.empty:
        spy_metrics = compute_metrics(spy_equity, spy_trades, capital)
        all_results.append({
            "label":     "SPY",
            "metrics":   spy_metrics,
            "equity_df": spy_equity,
            "trades_df": spy_trades,
            "engine":    None,
        })
    else:
        print("  SPY benchmark unavailable (no SPY data in database).")

    # Print full detail blocks
    for r in all_results:
        print(_fmt_metrics(r["metrics"], r["label"]))

    # Comparison table (only meaningful when multiple strategies shown)
    if len(all_results) > 1:
        print(_fmt_comparison(all_results))

    # Top trades per strategy
    for r in all_results:
        if r["label"] != "SPY":
            _print_top_trades(r["trades_df"], r["label"])

    print()


if __name__ == "__main__":
    main()
