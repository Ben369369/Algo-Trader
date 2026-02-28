import pandas as pd
import numpy as np

class TradeScorer:

    @staticmethod
    def score(scan_df):
        df = scan_df.copy()

        # Score each component 0 to 1
        # RSI score — higher when RSI is far from 50 in either direction
        df["rsi_score"] = abs(df["rsi"] - 50) / 50

        # Zscore score — higher when price is more extreme
        df["zscore_score"] = df["zscore"].abs().clip(0, 3) / 3

        # Bollinger score — higher when price is near the edges of the bands
        df["bb_score"] = abs(df["bb_position"] - 0.5) * 2

        # Combined score — weighted average
        df["score"] = (
            df["rsi_score"]    * 0.35 +
            df["zscore_score"] * 0.40 +
            df["bb_score"]     * 0.25
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
            "score", "rsi", "zscore", "bb_position"
        ]]