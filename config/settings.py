import os
from dotenv import load_dotenv
from pathlib import Path

load_dotenv(Path(__file__).parent.parent / ".env")

class Config:
    ALPACA_API_KEY    = os.getenv("ALPACA_API_KEY", "")
    ALPACA_SECRET_KEY = os.getenv("ALPACA_SECRET_KEY", "")
    ALPACA_MODE       = os.getenv("ALPACA_MODE", "paper")
    timeframe         = os.getenv("TIMEFRAME", "1Day")
    LOOKBACK_YEARS    = int(os.getenv("LOOKBACK_YEARS", "3"))
    MAX_RISK_PER_TRADE  = float(os.getenv("MAX_RISK_PER_TRADE", "0.02"))
    MAX_DRAWDOWN_LIMIT  = float(os.getenv("MAX_DRAWDOWN_LIMIT", "0.10"))
    MAX_POSITION_SIZE   = float(os.getenv("MAX_POSITION_SIZE", "0.10"))
    PROJECT_ROOT = Path(__file__).parent.parent
    DATA_DIR     = Path(__file__).parent.parent / "data"
    LOGS_DIR     = Path(__file__).parent.parent / "logs"
    DB_PATH      = Path(__file__).parent.parent / "data" / "market_data.db"
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
    LOG_FILE  = os.getenv("LOG_FILE", "logs/tradingbot.log")

    @classmethod
    def alpaca_base_url(cls):
        return "https://paper-api.alpaca.markets" if cls.ALPACA_MODE == "paper" else "https://api.alpaca.markets"

    @classmethod
    def alpaca_data_url(cls):
        return "https://data.alpaca.markets"

    @classmethod
    def symbols(cls):
        raw = os.getenv("SYMBOLS", "AAPL,MSFT,GOOGL,AMZN,TSLA,NVDA,META,JPM,V,SPY")
        return [s.strip() for s in raw.split(",") if s.strip()]

    @classmethod
    def validate(cls):
        errors = []
        if not cls.ALPACA_API_KEY or cls.ALPACA_API_KEY == "your_api_key_here":
            errors.append("ALPACA_API_KEY is not set in your .env file")
        if not cls.ALPACA_SECRET_KEY or cls.ALPACA_SECRET_KEY == "your_secret_key_here":
            errors.append("ALPACA_SECRET_KEY is not set in your .env file")
        if errors:
            for e in errors:
                print(f"  ERROR: {e}")
            return False
        print(f"  Config loaded — Mode: {cls.ALPACA_MODE.upper()} | Symbols: {len(cls.symbols())} | Timeframe: {cls.timeframe}")
        return True

Config.DATA_DIR.mkdir(exist_ok=True)
Config.LOGS_DIR.mkdir(exist_ok=True)
