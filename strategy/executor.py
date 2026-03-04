from utils.logger import logger
from utils.broker import BrokerConnection
from strategy.sizer import PositionSizer

STOP_LOSS_PCT    = 0.06   # 6% below entry
TAKE_PROFIT_PCT  = 0.10   # 10% above entry

class TradeExecutor:

    def __init__(self):
        self.broker = BrokerConnection()

    def execute_best(self, ranked_df):
        """
        Takes the ranked leaderboard, finds the best
        actionable trade and executes it as a bracket order
        (market entry + broker-managed stop-loss + take-profit).
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
        account   = self.broker.get_account()
        portfolio = account["portfolio_value"]

        # Check if we already hold this stock
        positions = self.broker.get_positions()
        held_map  = {p["symbol"]: p for p in positions}

        if direction == "BUY" and symbol in held_map:
            logger.info(f"Already holding {symbol} — skipping buy.")
            return None

        if direction == "SELL" and symbol not in held_map:
            logger.info(f"Not holding {symbol} — nothing to sell.")
            return None

        # Use live price for accurate bracket levels (falls back to last close)
        live_price = self.broker.get_latest_price(symbol)
        if live_price:
            logger.info(f"{symbol}: Using live price ${live_price:.2f} (last close was ${price:.2f})")
            price = live_price

        if direction == "BUY":
            # Calculate position size based on live price
            shares = PositionSizer.calculate(portfolio, price)
            if shares <= 0:
                logger.warning(f"Position size calculated as 0 — skipping.")
                return None
            # Bracket order: broker enforces stop-loss and take-profit automatically
            stop_price        = round(price * (1 - STOP_LOSS_PCT), 2)
            take_profit_price = round(price * (1 + TAKE_PROFIT_PCT), 2)
            order = self.broker.place_bracket_order(
                symbol=symbol,
                qty=shares,
                side="buy",
                stop_price=stop_price,
                take_profit_price=take_profit_price,
            )
        else:
            # SELL = closing an existing long position — use actual held qty
            sell_qty = int(abs(held_map[symbol]["qty"]))
            if sell_qty <= 0:
                logger.warning(f"{symbol}: Held qty is 0 — skipping sell.")
                return None
            self.broker.cancel_orders_for_symbol(symbol)
            order = self.broker.place_market_order(
                symbol=symbol,
                qty=sell_qty,
                side="sell",
                note=f"Signal score: {score}",
            )

        if order:
            logger.info(f"Order placed successfully: {order}")
        return order

    def check_exits(self, ranked_df):
        """
        Check if any held positions now have a sell signal and exit them.
        Hard stop-loss and take-profit are handled by the broker's bracket OCO orders.
        This method only handles signal-based exits.
        """
        positions = self.broker.get_positions()
        if not positions:
            return

        for position in positions:
            symbol = position["symbol"]
            match  = ranked_df[ranked_df["symbol"] == symbol]

            if match.empty:
                continue

            direction = match.iloc[0]["direction"]
            pnl_pct   = position["unrealized_plpc"] * 100

            if direction == "SELL":
                logger.info(f"Exit signal on {symbol} — P&L: {pnl_pct:.2f}%")
                # Cancel bracket child orders first, then place clean market exit
                self.broker.cancel_orders_for_symbol(symbol)
                self.broker.place_market_order(
                    symbol=symbol,
                    qty=abs(position["qty"]),
                    side="sell",
                    note="Exit signal triggered",
                )
