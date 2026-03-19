import math
from utils.logger import logger

class PositionSizer:

    @staticmethod
    def calculate(portfolio_value, price, risk_pct=0.02, stop_loss_pct=0.06,
                  atr=None, atr_multiplier=2.0):
        """
        Calculate how many shares to buy.
        Never risks more than risk_pct of portfolio on one trade.

        If atr is provided, stop distance = atr * atr_multiplier (adapts to volatility).
        Otherwise falls back to a fixed stop_loss_pct below entry.
        """
        if price <= 0:
            return 0

        max_risk_dollars = portfolio_value * risk_pct

        if atr and atr > 0:
            # ATR-based stop: adapts to the stock's actual volatility
            risk_per_share = atr * atr_multiplier
            stop_method = f"ATR-based ({atr_multiplier}x ATR=${atr:.2f})"
        else:
            # Fixed percentage fallback
            risk_per_share = price * stop_loss_pct
            stop_method = f"fixed {stop_loss_pct:.0%}"

        if risk_per_share <= 0:
            return 0

        shares = max_risk_dollars / risk_per_share

        # Never let one position exceed 10% of portfolio
        max_shares_by_size = (portfolio_value * 0.10) / price
        shares = min(shares, max_shares_by_size)
        shares = math.floor(shares)

        logger.info(
            f"Position size: {shares} shares @ ${price:.2f} "
            f"| Stop: {stop_method} "
            f"| Risk: ${shares * risk_per_share:.2f} "
            f"| Value: ${shares * price:.2f}"
        )

        return shares

    @staticmethod
    def stop_price(entry_price, atr=None, atr_multiplier=2.0, stop_loss_pct=0.06):
        """Compute the initial stop-loss price for an entry."""
        if atr and atr > 0:
            return round(entry_price - atr * atr_multiplier, 2)
        return round(entry_price * (1 - stop_loss_pct), 2)
