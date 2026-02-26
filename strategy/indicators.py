import pandas as pd
import numpy as np

class Indicators:

    @staticmethod
    def rsi(close, period=14):
        delta = close.diff()
        gain = delta.clip(lower=0)
        loss = -delta.clip(upper=0)
        avg_gain = gain.rolling(period).mean()
        avg_loss = loss.rolling(period).mean()
        rs = avg_gain / avg_loss
        return 100 - (100 / (1 + rs))

    @staticmethod
    def macd(close, fast=12, slow=26, signal=9):
        ema_fast = close.ewm(span=fast).mean()
        ema_slow = close.ewm(span=slow).mean()
        macd_line = ema_fast - ema_slow
        signal_line = macd_line.ewm(span=signal).mean()
        histogram = macd_line - signal_line
        return macd_line, signal_line, histogram

    @staticmethod
    def bollinger_bands(close, period=20, std=2):
        sma = close.rolling(period).mean()
        stddev = close.rolling(period).std()
        upper = sma + (std * stddev)
        lower = sma - (std * stddev)
        return upper, sma, lower

    @staticmethod
    def zscore(close, period=20):
        mean = close.rolling(period).mean()
        std = close.rolling(period).std()
        return (close - mean) / std