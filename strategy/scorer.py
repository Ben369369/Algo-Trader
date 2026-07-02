import pandas as pd
import numpy as np
from strategy.tiger_parser import load_tiger_boosts

class TradeScorer:

    @staticmethod
    def score(scan_df, use_tiger=True):
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
        )

        # Tiger Capital research boost — applied after base score, clipped to [0, 1]
        tiger_boosts = load_tiger_boosts() if use_tiger else {}
        if tiger_boosts:
            df["tiger_boost"] = df["symbol"].map(tiger_boosts).fillna(0.0)
            df["score"] = (df["score"] + df["tiger_boost"]).clip(0, 1)
        df["score"] = df["score"].round(4)

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

    @staticmethod
    def score_momentum(scan_df, use_tiger=True):
        """
        Score momentum candidates. Unlike mean-reversion, higher RSI is better here
        (we want strength, not oversold conditions).
        """
        df = scan_df.copy()

        # RSI score: sweet spot is 55-65, peaks at 60, falls off toward 50 and 70
        df["rsi_score"] = (1 - abs(df["rsi"] - 60) / 20).clip(0, 1)

        # Relative strength: how much the stock is outperforming SPY (capped at 15%)
        df["rs_score"] = df["rel_strength"].clip(0, 0.15) / 0.15

        # MACD score: normalized by price for cross-stock comparability
        df["macd_score"] = (df["macd_hist"].abs() / df["price"] * 100).clip(0, 1)

        # Volume score: above-average volume confirms the breakout
        df["volume_score"] = (df["volume_ratio"].fillna(1.0) - 1.0).clip(0, 2) / 2

        # Bollinger position: for momentum, price near upper band is positive
        df["bb_score"] = df["bb_position"].clip(0, 1)

        df["score"] = (
            df["rsi_score"]    * 0.25 +
            df["rs_score"]     * 0.30 +
            df["macd_score"]   * 0.20 +
            df["volume_score"] * 0.15 +
            df["bb_score"]     * 0.10
        )

        tiger_boosts = load_tiger_boosts() if use_tiger else {}
        if tiger_boosts:
            df["tiger_boost"] = df["symbol"].map(tiger_boosts).fillna(0.0)
            df["score"] = (df["score"] + df["tiger_boost"]).clip(0, 1)
        df["score"] = df["score"].round(4)

        df["direction"] = "NEUTRAL"
        df.loc[df["buy_signal"]  == True, "direction"] = "BUY"
        df.loc[df["sell_signal"] == True, "direction"] = "SELL"

        df = df.sort_values("score", ascending=False).reset_index(drop=True)
        df.index += 1

        return df[[
            "symbol", "price", "direction",
            "score", "rsi", "macd_hist", "atr", "volume_ratio", "rel_strength"
        ]]
