import pandas as pd
from strategy.indicators import Indicators


class MomentumSignalDetector:

    @staticmethod
    def detect(df, spy_return_20d=None, sell_rule="sma50"):
        """
        Momentum signals: buy stocks breaking to new highs with strong trend,
        above-average volume, and positive relative strength vs SPY.

        spy_return_20d: SPY's 20-day return for relative strength comparison.
                        Accepts a float (live: latest value) or a date-indexed
                        pd.Series (backtest: aligned per-day). If None, uses the
                        stock's absolute 20-day return instead.
        sell_rule:      "sma50"     — close below SMA50 (default: let winners
                                      run; trailing/ATR stops handle downside)
                        "sma20_rsi" — RSI > 75 or close < SMA20 * 0.99
                                      (legacy tight exit, kept for A/B)
        """
        signals = pd.DataFrame(index=df.index)
        close = df["close"]

        signals["close"]     = close
        signals["rsi"]       = Indicators.rsi(close)
        signals["atr"]       = Indicators.atr(df)

        sma20  = Indicators.sma(close, 20)
        sma50  = Indicators.sma(close, 50)
        sma200 = Indicators.sma(close, 200)

        _, _, hist = Indicators.macd(close)
        signals["macd_hist"]   = hist
        signals["macd_rising"] = hist > hist.shift(1)

        upper, _, lower = Indicators.bollinger_bands(close)
        signals["bb_position"] = (close - lower) / (upper - lower)

        # Volume confirmation
        vol_ma = df["volume"].rolling(20).mean()
        signals["volume_ratio"] = df["volume"] / vol_ma

        # 20-day high breakout: price within 2% of its 20-day rolling high
        rolling_high_20 = close.rolling(20).max()
        signals["near_high_20"] = close >= rolling_high_20 * 0.95

        # Relative strength vs SPY over 20 days
        stock_return_20d = close.pct_change(20)
        if isinstance(spy_return_20d, pd.Series):
            signals["rel_strength"] = stock_return_20d - spy_return_20d.reindex(df.index)
        elif spy_return_20d is not None:
            signals["rel_strength"] = stock_return_20d - spy_return_20d
        else:
            signals["rel_strength"] = stock_return_20d

        # Cross-sectional 12-1 momentum: return from ~12 months ago to ~1 month ago.
        # Skip the most recent month to avoid short-term reversal contamination.
        # fillna(0) so dropna() doesn't wipe rows when fewer than 252 bars are fetched.
        signals["mom_12_1"] = (close.shift(21) / close.shift(252) - 1).fillna(0.0)

        # Trend alignment
        above_sma50  = close > sma50
        golden_cross = sma50 > sma200

        # Buy: breakout near 20d high, RSI in the momentum sweet spot (50-75),
        # MACD positive and rising, price in uptrend, and stock outperforming
        # SPY over the last 20 days. Volume is soft-weighted in the scorer
        # rather than used as a hard gate here.
        signals["buy"] = (
            (signals["rsi"] >= 50) &
            (signals["rsi"] <= 75) &
            signals["near_high_20"] &
            signals["macd_rising"] &
            above_sma50 &
            golden_cross &
            (signals["rel_strength"] > 0)
        )

        # Sell rule (see docstring). "sma50" holds through normal pullbacks;
        # "sma20_rsi" is the legacy tight exit kept for A/B comparison.
        if sell_rule == "sma50":
            signals["sell"] = close < sma50
        else:
            signals["sell"] = (
                (signals["rsi"] > 75) |
                (close < sma20 * 0.99)
            )

        return signals.dropna()
