import pandas as pd
import numpy as np

class TradeScorer:

    @staticmethod
    def score(scan_df):
        df = scan_df.copy()

        # RSI score — higher when RSI is far from 50 in either direction
        df["rsi_score"] = abs(df["rsi"] - 50) / 50

        # Zscore score — higher when price is more extreme
        df["zscore_score"] = df["zscore"].abs().clip(0, 3) / 3

        # MACD score — normalize histogram by stock price for cross-stock comparability
        # (a $1 MACD hist on a $10 stock is much more significant than on a $500 stock)
        df["macd_score"] = (df["macd_hist"].abs() / df["price"] * 100).clip(0, 1)

        # Volume score — higher when above-average volume confirms the signal
        # volume_ratio = today's volume / 20-day avg; clip at 3x to prevent outliers dominating
        df["volume_score"] = (df["volume_ratio"].fillna(1.0) - 1.0).clip(0, 2) / 2

        # Combined score — weighted average
        df["score"] = (
            df["rsi_score"]    * 0.30 +
            df["zscore_score"] * 0.35 +
            df["macd_score"]   * 0.20 +
            df["volume_score"] * 0.15
        ).round(4)

        # Tag direction
        df["direction"] = "NEUTRAL"
        df.loc[df["buy_signal"]  == True, "direction"] = "BUY"
        df.loc[df["sell_signal"] == True, "direction"] = "SELL"

        # Sort by score descending
        df = df.sort_values("score", ascending=False).reset_index(drop=True)
        df.index += 1  # Start ranking at 1

        return df[[
            "symbol", "price", "direction",
            "score", "rsi", "zscore", "macd_hist", "atr", "volume_ratio"
        ]]
