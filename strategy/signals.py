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

        macd, signal, hist = Indicators.macd(df['close'])
        signals['macd_hist'] = hist

        upper, mid, lower = Indicators.bollinger_bands(df['close'])
        signals['bb_position'] = (df['close'] - lower) / (upper - lower)

        # Buy signal: RSI oversold + price near bottom of bands + negative zscore
        signals['buy'] = (
            (signals['rsi'] < 40) &
            (signals['bb_position'] < 0.2) &
            (signals['zscore'] < -1.0)
        )

        # Sell signal: RSI overbought + price near top of bands + positive zscore
        signals['sell'] = (
            (signals['rsi'] > 60) &
            (signals['bb_position'] > 0.8) &
            (signals['zscore'] > 1.0)
        )

        return signals.dropna()