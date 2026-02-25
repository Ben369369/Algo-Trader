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
            logger.info(f"Connected to Alpaca [PAPER]")
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
        return [{"symbol": p.symbol, "qty": float(p.qty),
                 "unrealized_pl": float(p.unrealized_pl),
                 "unrealized_plpc": float(p.unrealized_plpc)}
                for p in self.api.list_positions()]

    def is_market_open(self):
        return self.api.get_clock().is_open

    def next_market_open(self):
        return str(self.api.get_clock().next_open)
