from utils.logger import logger
from utils.broker import BrokerConnection
from strategy.sizer import PositionSizer

class TradeExecutor:

    def __init__(self):
        self.broker = BrokerConnection()

    def execute_best(self, ranked_df):
        """
        Takes the ranked leaderboard, finds the best
        actionable trade and executes it.
        """
        # Filter to only confirmed buy or sell signals
        actionable = ranked_df[ranked_df["direction"].isin(["BUY", "SELL"])]

        if actionable.empty:
            logger.info("No actionable signals right now — holding cash.")
            return None

        # Take the highest scored opportunity
        best = actionable.iloc[0]
        symbol    = best["symbol"]
        direction = best["direction"]
        price     = best["price"]
        score     = best["score"]

        logger.info(f"Best trade: {direction} {symbol} @ ${price} | Score: {score}")

        # Get account info for position sizing
        account  = self.broker.get_account()
        portfolio = account["portfolio_value"]

        # Check if we already hold this stock
        positions = self.broker.get_positions()
        held = [p["symbol"] for p in positions]

        if direction == "BUY" and symbol in held:
            logger.info(f"Already holding {symbol} — skipping buy.")
            return None

        if direction == "SELL" and symbol not in held:
            logger.info(f"Not holding {symbol} — nothing to sell.")
            return None

        # Calculate position size
        shares = PositionSizer.calculate(portfolio, price)

        if shares <= 0:
            logger.warning(f"Position size calculated as 0 — skipping.")
            return None

        # Place the order
        order = self.broker.place_market_order(
            symbol=symbol,
            qty=shares,
            side=direction.lower(),
            note=f"Signal score: {score}"
        )

        if order:
            logger.info(f"Order placed successfully: {order}")
        return order

    def check_exits(self, ranked_df):
        """
        Check if any held positions now have a sell signal
        and exit them automatically.
        """
        positions = self.broker.get_positions()
        if not positions:
            return

        for position in positions:
            symbol = position["symbol"]
            match = ranked_df[ranked_df["symbol"] == symbol]

            if match.empty:
                continue

            direction = match.iloc[0]["direction"]
            pnl_pct   = position["unrealized_plpc"] * 100

            # Exit if sell signal triggered
            if direction == "SELL":
                logger.info(f"Exit signal on {symbol} — P&L: {pnl_pct:.2f}%")
                self.broker.place_market_order(
                    symbol=symbol,
                    qty=abs(position["qty"]),
                    side="sell",
                    note="Exit signal triggered"
                )

            # Emergency exit if down more than 5%
            elif pnl_pct < -5.0:
                logger.warning(f"Stop loss hit on {symbol} — P&L: {pnl_pct:.2f}%")
                self.broker.place_market_order(
                    symbol=symbol,
                    qty=abs(position["qty"]),
                    side="sell",
                    note="Stop loss triggered"
                )