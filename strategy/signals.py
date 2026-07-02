import pandas as pd
import numpy as np
from strategy.indicators import Indicators

class SignalDetector:

    @staticmethod
    def detect(df, variant="v2"):
        """
        variant:
          "v2"     — short-horizon reversal (default): simpler entry (RSI +
                     zscore + trend), exit when price reverts to its 20-day
                     mean (zscore >= 0) or RSI recovers above 55. Designed for
                     2-10 day holds per the short-term reversal literature.
          "legacy" — six ANDed entry conditions (RSI, zscore, BB, MACD, trend,
                     volume) with an overbought-based sell. Very few signals;
                     kept for A/B comparison.
        """
        signals = pd.DataFrame(index=df.index)
        signals['close'] = df['close']
        signals['rsi'] = Indicators.rsi(df['close'])
        signals['zscore'] = Indicators.zscore(df['close'])

        macd, signal_line, hist = Indicators.macd(df['close'])
        signals['macd_hist'] = hist
        # Rising/falling momentum: today's histogram vs yesterday's
        signals['macd_rising']  = hist > hist.shift(1)
        signals['macd_falling'] = hist < hist.shift(1)

        upper, mid, lower = Indicators.bollinger_bands(df['close'])
        signals['bb_position'] = (df['close'] - lower) / (upper - lower)

        # ATR — used for dynamic stop sizing downstream
        signals['atr'] = Indicators.atr(df)

        # Volume confirmation — buy signals are more reliable on above-average volume
        vol_ma = df['volume'].rolling(20).mean()
        signals['volume_ratio'] = df['volume'] / vol_ma
        high_volume = signals['volume_ratio'] > 1.1  # at least 10% above 20-day avg

        # Trend filters for mean reversion:
        # - Price within 5% below SMA200: allows dip-buying near long-term support
        # - SMA50 still above SMA200 (golden cross): medium-term trend not broken
        #   (price CAN be below SMA50 — that's the pullback we're trading)
        sma50  = Indicators.sma(df['close'], 50)
        sma200 = Indicators.sma(df['close'], 200)
        trend_up = (df['close'] > sma200 * 0.95) & (sma50 > sma200)

        if variant == "v2":
            # Buy: oversold and stretched below the 20-day mean, inside an
            # intact long-term uptrend. BB/volume/MACD gates dropped — BB and
            # zscore measure the same stretch, and IEX volume is unreliable.
            signals['buy'] = (
                (signals['rsi'] < 35) &
                (signals['zscore'] < -1.0) &
                trend_up
            )
            # Sell: reversion complete — price back at/above its 20-day mean,
            # or RSI recovered. This is the profit-taking exit; the edge decays
            # within days, so there is no reason to hold for an overbought print.
            signals['sell'] = (
                (signals['zscore'] >= 0) |
                (signals['rsi'] > 55)
            )
        else:
            # Buy: oversold + below mean + MACD momentum turning up + trend intact
            # + volume confirmation: above-average volume validates the move
            signals['buy'] = (
                (signals['rsi'] < 38) &
                (signals['zscore'] < -1.0) &
                (signals['bb_position'] < 0.3) &
                signals['macd_rising'] &
                trend_up &
                high_volume
            )

            # Sell: overbought + above mean + MACD momentum rolling over
            signals['sell'] = (
                (signals['rsi'] > 65) &
                (signals['zscore'] > 1.2) &
                signals['macd_falling']
            )

        return signals.dropna()
