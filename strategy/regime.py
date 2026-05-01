from strategy.indicators import Indicators
from utils.logger import logger


class RegimeDetector:

    @staticmethod
    def detect(spy_df):
        """
        Classify the current market regime using SPY data.

        Returns one of:
          'TRENDING_UP'   -- SPY above SMA50, SMA50 above SMA200, RSI > 45
                            Favours momentum strategy (buy breakouts)
          'RANGING'       -- SPY pulled back below SMA50 within an uptrend
                            Favours mean-reversion strategy (buy dips)
          'TRENDING_DOWN' -- Death cross: SMA50 below SMA200
                            Avoid new entries; hold cash or exit only
        """
        if len(spy_df) < 210:
            logger.warning("RegimeDetector: Not enough SPY data -- defaulting to RANGING")
            return "RANGING"

        close  = spy_df["close"]
        price  = close.iloc[-1]
        sma50  = Indicators.sma(close, 50).iloc[-1]
        sma200 = Indicators.sma(close, 200).iloc[-1]
        rsi    = Indicators.rsi(close).iloc[-1]

        if sma50 < sma200:
            regime = "TRENDING_DOWN"
        elif price < sma50 * 0.97:
            # Price >3% below SMA50 -- pullback inside uptrend, mean-reversion territory
            regime = "RANGING"
        elif price >= sma50 and sma50 > sma200 and rsi > 45:
            regime = "TRENDING_UP"
        else:
            regime = "RANGING"

        logger.info(
            f"Market regime: {regime} | "
            f"SPY ${price:.2f} | SMA50 ${sma50:.2f} | SMA200 ${sma200:.2f} | RSI {rsi:.1f}"
        )
        return regime
