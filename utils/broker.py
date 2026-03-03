import alpaca_trade_api as tradeapi
from config.settings import Config
from utils.logger import logger

class BrokerConnection:
    def __init__(self):
        self.api = tradeapi.REST(
            key_id=Config.ALPACA_API_KEY,
            secret_key=Config.ALPACA_SECRET_KEY,
            base_url=Config.alpaca_base_url(),
            api_version="v2",
        )
        self._verify_connection()

    def _verify_connection(self):
        try:
            account = self.api.get_account()
            logger.info(f"Connected to Alpaca [{Config.ALPACA_MODE.upper()}]")
            logger.info(f"Portfolio value : ${float(account.portfolio_value):,.2f}")
            logger.info(f"Cash            : ${float(account.cash):,.2f}")
            logger.info(f"Buying power    : ${float(account.buying_power):,.2f}")
        except Exception as e:
            logger.error(f"Failed to connect: {e}")
            raise

    def get_account(self):
        acc = self.api.get_account()
        return {
            "status":          acc.status,
            "portfolio_value": float(acc.portfolio_value),
            "cash":            float(acc.cash),
            "buying_power":    float(acc.buying_power),
            "pnl_today":       float(acc.equity) - float(acc.last_equity),
        }

    def get_positions(self):
        return [
            {
                "symbol":          p.symbol,
                "qty":             float(p.qty),
                "unrealized_pl":   float(p.unrealized_pl),
                "unrealized_plpc": float(p.unrealized_plpc),
            }
            for p in self.api.list_positions()
        ]

    def place_market_order(self, symbol, qty, side, note=""):
        if side not in ("buy", "sell"):
            logger.error(f"Invalid side: {side}")
            return None
        if qty <= 0:
            logger.error(f"Invalid quantity: {qty}")
            return None
        try:
            logger.info(f"Placing {side.upper()} order: {qty} shares of {symbol} | {note}")
            order = self.api.submit_order(
                symbol=symbol,
                qty=qty,
                side=side,
                type="market",
                time_in_force="day",
            )
            logger.info(f"Order placed: {order.id} | {symbol} | {side} {qty}")
            return {
                "id":     order.id,
                "symbol": order.symbol,
                "side":   order.side,
                "qty":    float(order.qty),
                "status": order.status,
            }
        except Exception as e:
            logger.error(f"Order failed for {symbol}: {e}")
            return None

    def place_bracket_order(self, symbol, qty, side, stop_price, take_profit_price):
        """
        Place a market order with broker-level stop-loss and take-profit OCO legs.
        The broker manages both exits automatically — no soft stop needed.
        """
        if side not in ("buy", "sell"):
            logger.error(f"Invalid side: {side}")
            return None
        if qty <= 0:
            logger.error(f"Invalid quantity: {qty}")
            return None
        try:
            logger.info(
                f"Placing bracket {side.upper()}: {qty} shares of {symbol} | "
                f"Stop: ${stop_price:.2f} | Target: ${take_profit_price:.2f}"
            )
            order = self.api.submit_order(
                symbol=symbol,
                qty=qty,
                side=side,
                type="market",
                time_in_force="day",
                order_class="bracket",
                stop_loss={"stop_price": round(stop_price, 2)},
                take_profit={"limit_price": round(take_profit_price, 2)},
            )
            logger.info(f"Bracket order placed: {order.id} | {symbol} | {side} {qty}")
            return {
                "id":     order.id,
                "symbol": order.symbol,
                "side":   order.side,
                "qty":    float(order.qty),
                "status": order.status,
            }
        except Exception as e:
            logger.error(f"Bracket order failed for {symbol}: {e}")
            return None

    def cancel_orders_for_symbol(self, symbol):
        """Cancel all open orders for a symbol (e.g. bracket child orders before a manual exit)."""
        try:
            orders = self.api.list_orders(status="open")
            for order in orders:
                if order.symbol == symbol:
                    self.api.cancel_order(order.id)
                    logger.info(f"Cancelled order {order.id} for {symbol}")
        except Exception as e:
            logger.error(f"Failed to cancel orders for {symbol}: {e}")

    def get_latest_price(self, symbol):
        """Return the most recent trade price for a symbol, or None on failure."""
        try:
            trade = self.api.get_latest_trade(symbol)
            return float(trade.price)
        except Exception as e:
            logger.warning(f"{symbol}: Could not fetch live price — {e}")
            return None

    def is_market_open(self):
        return self.api.get_clock().is_open

    def next_market_open(self):
        return str(self.api.get_clock().next_open)