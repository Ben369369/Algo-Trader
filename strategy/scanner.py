import pandas as pd
import numpy as np
from data.pipeline import DataPipeline
from strategy.signals import SignalDetector
from utils.logger import logger

class MarketScanner:

    def __init__(self):
        self.pipeline = DataPipeline()

    def scan_symbol(self, symbol):
        try:
            df = self.pipeline.get_latest_bars(symbol, n=100)
            if df.empty or len(df) < 30:
                logger.warning(f"{symbol}: Not enough data")
                return None
            signals = SignalDetector.detect(df)
            latest = signals.iloc[-1]
            return {
                "symbol":       symbol,
                "price":        round(latest["close"], 2),
                "rsi":          round(latest["rsi"], 2),
                "zscore":       round(latest["zscore"], 3),
                "bb_position":  round(latest["bb_position"], 3),
                "buy_signal":   bool(latest["buy"]),
                "sell_signal":  bool(latest["sell"]),
            }
        except Exception as e:
            logger.error(f"{symbol}: Scan failed — {e}")
            return None

    def scan_all(self):
        from config.settings import Config
        symbols = Config.symbols()
        logger.info(f"Scanning {len(symbols)} symbols...")
        results = []
        for symbol in symbols:
            result = self.scan_symbol(symbol)
            if result:
                results.append(result)
        return pd.DataFrame(results)