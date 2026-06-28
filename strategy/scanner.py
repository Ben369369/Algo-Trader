import pandas as pd
import numpy as np
from data.pipeline import DataPipeline
from strategy.signals import SignalDetector
from strategy.momentum_signals import MomentumSignalDetector
from strategy.regime import RegimeDetector
from utils.earnings import is_near_earnings
from utils.logger import logger

class MarketScanner:

    def __init__(self):
        self.pipeline = DataPipeline()

    def scan_symbol(self, symbol):
        try:
            from config.settings import Config
            if Config.USE_EARNINGS_FILTER and is_near_earnings(
                symbol, Config.EARNINGS_DAYS_BEFORE, Config.EARNINGS_DAYS_AFTER
            ):
                return None
            df = self.pipeline.get_latest_bars(symbol, n=250)
            if df.empty or len(df) < 210:
                logger.warning(f"{symbol}: Not enough data for 200-day trend filter")
                return None
            signals = SignalDetector.detect(df)
            latest = signals.iloc[-1]
            return {
                "symbol":       symbol,
                "price":        round(latest["close"], 2),
                "rsi":          round(latest["rsi"], 2),
                "zscore":       round(latest["zscore"], 3),
                "bb_position":  round(latest["bb_position"], 3),
                "macd_hist":    round(latest["macd_hist"], 4),
                "atr":          round(float(latest["atr"]), 4),
                "volume_ratio": round(float(latest["volume_ratio"]), 3),
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

    def detect_regime(self):
        """Detect current market regime from SPY data."""
        self.pipeline.download_symbol("SPY")
        spy_df = self.pipeline.get_latest_bars("SPY", n=250)
        return RegimeDetector.detect(spy_df)

    def scan_symbol_momentum(self, symbol, spy_return_20d=None):
        try:
            from config.settings import Config
            if Config.USE_EARNINGS_FILTER and is_near_earnings(
                symbol, Config.EARNINGS_DAYS_BEFORE, Config.EARNINGS_DAYS_AFTER
            ):
                return None
            df = self.pipeline.get_latest_bars(symbol, n=250)
            if df.empty or len(df) < 210:
                logger.warning(f"{symbol}: Not enough data for momentum scan")
                return None
            signals = MomentumSignalDetector.detect(df, spy_return_20d)
            latest = signals.iloc[-1]
            return {
                "symbol":       symbol,
                "price":        round(latest["close"], 2),
                "rsi":          round(latest["rsi"], 2),
                "macd_hist":    round(latest["macd_hist"], 4),
                "atr":          round(float(latest["atr"]), 4),
                "volume_ratio": round(float(latest["volume_ratio"]), 3),
                "rel_strength": round(float(latest["rel_strength"]), 4),
                "bb_position":  round(float(latest["bb_position"]), 3),
                "near_high_20": bool(latest["near_high_20"]),
                "mom_12_1":     round(float(latest["mom_12_1"]) if pd.notna(latest["mom_12_1"]) else 0.0, 4),
                "buy_signal":   bool(latest["buy"]),
                "sell_signal":  bool(latest["sell"]),
            }
        except Exception as e:
            logger.error(f"{symbol}: Momentum scan failed -- {e}")
            return None

    def scan_all_momentum(self):
        """
        Run momentum scan on all symbols.
        Returns (results_df, regime_string).
        """
        from config.settings import Config
        self.pipeline.download_symbol("SPY")
        spy_df = self.pipeline.get_latest_bars("SPY", n=250)
        regime = RegimeDetector.detect(spy_df)

        spy_return_20d = None
        if not spy_df.empty and len(spy_df) >= 21:
            spy_return_20d = (spy_df["close"].iloc[-1] / spy_df["close"].iloc[-21]) - 1

        symbols = [s for s in Config.symbols() if s not in ("SPY", "QQQ", "IWM", "DIA")]
        logger.info(f"Momentum scan: {len(symbols)} symbols | Regime: {regime}")
        results = []
        for symbol in symbols:
            result = self.scan_symbol_momentum(symbol, spy_return_20d)
            if result:
                results.append(result)
        return pd.DataFrame(results), regime
