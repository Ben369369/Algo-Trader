import numpy as np
import pandas as pd


def compute_metrics(equity_df, trades_df, initial_capital):
    """
    Compute standard performance metrics from an equity curve and trade log.

    Parameters
    ----------
    equity_df       : pd.DataFrame with DatetimeIndex and 'equity' column
    trades_df       : pd.DataFrame with columns incl. 'pnl', 'pnl_pct', 'exit_reason'
    initial_capital : float

    Returns
    -------
    dict of named metrics
    """
    equity = equity_df["equity"]
    start  = equity.index[0]
    end    = equity.index[-1]
    years  = (end - start).days / 365.25

    total_return = (equity.iloc[-1] / initial_capital - 1) * 100
    cagr         = ((equity.iloc[-1] / initial_capital) ** (1 / max(years, 1e-9)) - 1) * 100

    # Drawdown
    peak         = equity.cummax()
    drawdown     = (equity - peak) / peak
    max_drawdown = drawdown.min() * 100

    # Sharpe — daily returns, annualised
    daily_returns = equity.pct_change().dropna()
    if daily_returns.std() > 0:
        sharpe = (daily_returns.mean() / daily_returns.std()) * np.sqrt(252)
    else:
        sharpe = 0.0

    num_trades = len(trades_df)
    if num_trades > 0:
        wins     = trades_df[trades_df["pnl"] > 0]
        losses   = trades_df[trades_df["pnl"] <= 0]
        win_rate = len(wins) / num_trades * 100
        avg_win  = wins["pnl_pct"].mean()  if not wins.empty   else 0.0
        avg_loss = losses["pnl_pct"].mean() if not losses.empty else 0.0
        exit_breakdown = trades_df["exit_reason"].value_counts().to_dict()
    else:
        win_rate = avg_win = avg_loss = 0.0
        exit_breakdown = {}

    return {
        "total_return":    round(total_return, 2),
        "cagr":            round(cagr, 2),
        "max_drawdown":    round(max_drawdown, 2),
        "sharpe_ratio":    round(sharpe, 3),
        "num_trades":      num_trades,
        "win_rate":        round(win_rate, 2),
        "avg_win_pct":     round(avg_win,  2) if avg_win  else 0.0,
        "avg_loss_pct":    round(avg_loss, 2) if avg_loss else 0.0,
        "exit_breakdown":  exit_breakdown,
        "start_date":      str(start.date()),
        "end_date":        str(end.date()),
        "years":           round(years, 2),
    }
