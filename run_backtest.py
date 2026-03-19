"""
run_backtest.py — Backtesting CLI

Usage:
    python run_backtest.py                        # mean reversion vs SPY
    python run_backtest.py --strategy momentum    # momentum vs mean reversion vs SPY
    python run_backtest.py --capital 50000        # custom starting capital
    python run_backtest.py --filters              # enable earnings + sentiment filters
                                                  # (requires Alpaca API keys in .env)

Does NOT require Alpaca API keys unless --filters is used.
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
        "label":           "MEAN REVERSION",
        "detector":        SignalDetector,
        "stop_loss_pct":   0.06,
        "take_profit_pct": 0.10,
        "trail_stop_pct":  0.0,
    },
    "momentum": {
        "label":           "MOMENTUM",
        "detector":        MomentumSignalDetector,
        "stop_loss_pct":   0.07,
        "take_profit_pct": 0.20,   # wider target — trailing stop handles downside
        "trail_stop_pct":  0.07,   # 7% trailing stop locks in gains
    },
}


def _run_strategy(name, capital, news_filter=None,
                  use_earnings_filter=False, use_sentiment_filter=False):
    cfg    = STRATEGIES[name]
    engine = BacktestEngine(
        initial_capital      = capital,
        stop_loss_pct        = cfg["stop_loss_pct"],
        take_profit_pct      = cfg["take_profit_pct"],
        trail_stop_pct       = cfg["trail_stop_pct"],
        news_filter          = news_filter,
        use_earnings_filter  = use_earnings_filter,
        use_sentiment_filter = use_sentiment_filter,
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


def _print_filter_comparison(m_base, m_filt, label):
    """Print a before/after table showing the impact of news filters."""
    keys = [
        ("CAGR (%)",         "cagr"),
        ("Total Return (%)", "total_return"),
        ("Max Drawdown (%)", "max_drawdown"),
        ("Sharpe Ratio",     "sharpe_ratio"),
        ("# Trades",         "num_trades"),
        ("Win Rate (%)",     "win_rate"),
        ("Avg Win (%)",      "avg_win_pct"),
        ("Avg Loss (%)",     "avg_loss_pct"),
    ]
    col_w = 16
    row_w = 22
    print(f"\n{'='*56}")
    print(f"  {label} — FILTER IMPACT")
    print(f"{'='*56}")
    print(f"  {'Metric':<{row_w}} {'No Filters':>{col_w}} {'With Filters':>{col_w}} {'Delta':>{col_w}}")
    print("  " + "-" * (row_w + col_w * 3))
    for display, key in keys:
        b = m_base[key]
        f = m_filt[key]
        if isinstance(b, float):
            delta = f - b
            print(f"  {display:<{row_w}} {b:>{col_w}.2f} {f:>{col_w}.2f} {delta:>+{col_w}.2f}")
        else:
            delta = f - b
            print(f"  {display:<{row_w}} {b:>{col_w}} {f:>{col_w}} {delta:>+{col_w}}")


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
    parser.add_argument("--filters", action="store_true",
                        help="Enable earnings + sentiment news filters (requires Alpaca API keys).")
    args = parser.parse_args()

    capital = args.capital

    # Set up news filter if requested
    news_filter = None
    use_earnings  = False
    use_sentiment = False
    if args.filters:
        from strategy.news_filter import NewsFilter
        news_filter   = NewsFilter()
        use_earnings  = True
        use_sentiment = True
        print(f"\nRunning backtest WITH news filters (earnings + sentiment) ...")
    else:
        print(f"\nRunning backtest with ${capital:,.0f} starting capital ...")

    # Determine which strategies to run
    run_names = (["momentum", "mean_reversion"]
                 if args.strategy == "momentum"
                 else ["mean_reversion"])

    all_results = []

    for name in run_names:
        label = STRATEGIES[name]["label"]
        if args.filters:
            # Run once WITHOUT filters, once WITH — for direct comparison
            print(f"\n  Computing {label} (no filters) ...")
            engine_base, eq_base, tr_base = _run_strategy(name, capital)
            m_base = compute_metrics(eq_base, tr_base, capital)

            print(f"  Computing {label} (with news filters) ...")
            engine_filt, eq_filt, tr_filt = _run_strategy(
                name, capital,
                news_filter=news_filter,
                use_earnings_filter=use_earnings,
                use_sentiment_filter=use_sentiment,
            )
            m_filt = compute_metrics(eq_filt, tr_filt, capital)

            # Print side-by-side for this strategy
            _print_filter_comparison(m_base, m_filt, label)
            _print_top_trades(tr_filt, f"{label} + FILTERS")

            # Keep the filtered version in all_results for SPY benchmark
            all_results.append({
                "label":     label + " + FILTERS",
                "metrics":   m_filt,
                "equity_df": eq_filt,
                "trades_df": tr_filt,
                "engine":    engine_filt,
            })
            continue

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

    # When --filters was used we already printed per-strategy comparisons in the loop.
    # Just show final SPY benchmark context.
    if args.filters:
        spy_r = next((r for r in all_results if r["label"] == "SPY"), None)
        if spy_r:
            print(_fmt_metrics(spy_r["metrics"], "SPY BENCHMARK"))
        print()
        return

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
