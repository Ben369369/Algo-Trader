import pandas as pd
import numpy as np
from strategy.indicators import Indicators


class MomentumSignalDetector:

    @staticmethod
    def detect(df):
        signals = pd.DataFrame(index=df.index)
        signals['close'] = df['close']
        signals['rsi'] = Indicators.rsi(df['close'])
        signals['zscore'] = Indicators.zscore(df['close'])

        macd, signal_line, hist = Indicators.macd(df['close'])
        signals['macd_hist']    = hist
        signals['macd_rising']  = hist > hist.shift(1)
        signals['macd_falling'] = hist < hist.shift(1)

        sma50  = Indicators.sma(df['close'], 50)
        sma200 = Indicators.sma(df['close'], 200)

        # Breakout confirmation: today's close is at or above the 20-day rolling high
        high20 = df['close'].rolling(20).max()

        # Buy: price above both trend MAs, RSI in momentum zone (not yet overbought),
        # and confirmed by a 20-day breakout
        signals['buy'] = (
            (df['close'] > sma50) &
            (df['close'] > sma200) &
            (signals['rsi'] >= 50) &
            (signals['rsi'] <= 65) &
            (df['close'] >= high20)
        )

        # Sell: RSI enters overbought territory OR price breaks back below 50-day MA
        signals['sell'] = (
            (signals['rsi'] > 75) |
            (df['close'] < sma50)
        )

        return signals.dropna()
