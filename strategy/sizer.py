import math
from utils.logger import logger

class PositionSizer:

    @staticmethod
    def calculate(portfolio_value, price, risk_pct=0.02, stop_loss_pct=0.05):
        """
        Calculate how many shares to buy.
        Never risks more than risk_pct of portfolio on one trade.
        Uses a stop loss of stop_loss_pct below entry price.
        """
        if price <= 0:
            return 0

        # Maximum dollar amount we are willing to lose on this trade
        max_risk_dollars = portfolio_value * risk_pct

        # Dollar risk per share based on stop loss distance
        risk_per_share = price * stop_loss_pct

        # Number of shares
        shares = max_risk_dollars / risk_per_share

        # Never let one position exceed 10% of portfolio
        max_shares_by_size = (portfolio_value * 0.10) / price

        shares = min(shares, max_shares_by_size)

        # Round down to whole shares
        shares = math.floor(shares)

        logger.info(
            f"Position size: {shares} shares @ ${price:.2f} "
            f"| Risk: ${shares * risk_per_share:.2f} "
            f"| Value: ${shares * price:.2f}"
        )

        return shares