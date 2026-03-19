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

        # ATR — used for dynamic stop sizing downstream
        signals['atr'] = Indicators.atr(df)

        # Volume confirmation — buy signals are more reliable on above-average volume
        vol_ma = df['volume'].rolling(20).mean()
        signals['volume_ratio'] = df['volume'] / vol_ma
        high_volume = signals['volume_ratio'] > 1.1  # at least 10% above 20-day avg

        # Trend filters for mean reversion:
        # - Price above 200-day SMA: long-term uptrend intact
        # - SMA50 still above SMA200 (golden cross): medium-term trend not broken
        #   (price CAN be below SMA50 — that's the pullback we're trading)
        sma50  = Indicators.sma(df['close'], 50)
        sma200 = Indicators.sma(df['close'], 200)
        trend_up = (df['close'] > sma200) & (sma50 > sma200)

        # Buy: oversold + below mean + MACD momentum turning up + in long-term uptrend
        # + bounce confirmation: today's close is above yesterday's (reversal started)
        # + volume confirmation: above-average volume validates the move
        bounce = df['close'] > df['close'].shift(1)

        signals['buy'] = (
            (signals['rsi'] < 38) &
            (signals['zscore'] < -1.0) &
            (signals['bb_position'] < 0.3) &
            signals['macd_rising'] &
            trend_up &
            bounce &
            high_volume
        )

        # Sell: overbought + above mean + MACD momentum rolling over
        signals['sell'] = (
            (signals['rsi'] > 65) &
            (signals['zscore'] > 1.2) &
            signals['macd_falling']
        )

        return signals.dropna()
