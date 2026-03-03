import pandas as pd
import numpy as np
from strategy.indicators import Indicators

class SignalDetector:

    @staticmethod
    def detect(df):
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

        # Trend filter: price above 200-day SMA = uptrend (only buy in uptrends)
        sma200 = Indicators.sma(df['close'], 200)
        trend_up = df['close'] > sma200

        # Buy: oversold + below mean + MACD momentum turning up + in uptrend
        # Uses "rising histogram" instead of "histogram > 0" — catches the turn earlier
        signals['buy'] = (
            (signals['rsi'] < 35) &
            (signals['zscore'] < -1.2) &
            signals['macd_rising'] &
            trend_up
        )

        # Sell: overbought + above mean + MACD momentum rolling over
        signals['sell'] = (
            (signals['rsi'] > 65) &
            (signals['zscore'] > 1.2) &
            signals['macd_falling']
        )

        return signals.dropna()
