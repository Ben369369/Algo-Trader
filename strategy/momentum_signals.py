import pandas as pd
from strategy.indicators import Indicators


class MomentumSignalDetector:

    @staticmethod
    def detect(df, spy_return_20d=None):
        """
        Momentum signals: buy stocks breaking to new highs with strong trend,
        above-average volume, and positive relative strength vs SPY.

        spy_return_20d: SPY's 20-day return (float) for relative strength comparison.
                        If None, uses the stock's absolute 20-day return instead.
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
        signals["near_high_20"] = close >= rolling_high_20 * 0.98

        # Relative strength vs SPY over 20 days
        stock_return_20d = close.pct_change(20)
        if spy_return_20d is not None:
            signals["rel_strength"] = stock_return_20d - spy_return_20d
        else:
            signals["rel_strength"] = stock_return_20d

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
            (hist > 0) &
            signals["macd_rising"] &
            above_sma50 &
            golden_cross &
            (signals["rel_strength"] > 0)
        )

        # Sell: RSI overextended, or price breaks below SMA20 (momentum lost)
        signals["sell"] = (
            (signals["rsi"] > 75) |
            (close < sma20 * 0.99)
        )

        return signals.dropna()
