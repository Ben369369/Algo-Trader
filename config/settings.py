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
    USE_EARNINGS_FILTER   = os.getenv("USE_EARNINGS_FILTER",  "true").lower() == "true"
    EARNINGS_DAYS_BEFORE  = int(os.getenv("EARNINGS_DAYS_BEFORE", "5"))
    EARNINGS_DAYS_AFTER   = int(os.getenv("EARNINGS_DAYS_AFTER",  "2"))
    USE_SENTIMENT_FILTER  = os.getenv("USE_SENTIMENT_FILTER", "false").lower() == "true"
    MAX_POSITIONS_PER_SECTOR = int(os.getenv("MAX_POSITIONS_PER_SECTOR", "2"))

    # Sector groupings — used by executor to prevent correlated entries
    SECTOR_MAP = {
        # Tech
        "AAPL": "Tech",  "MSFT": "Tech",  "GOOGL": "Tech", "AMZN": "Tech",
        "NVDA": "Tech",  "META": "Tech",  "AMD":   "Tech", "AVGO": "Tech",
        "INTC": "Tech",  "NOW":  "Tech",  "CRWD":  "Tech", "CRM":  "Tech",
        "ORCL": "Tech",  "QCOM": "Tech",  "AMAT":  "Tech",
        # Consumer
        "TSLA": "Consumer", "MCD":  "Consumer", "KO":   "Consumer",
        "WMT":  "Consumer", "PG":   "Consumer", "COST": "Consumer",
        "NKE":  "Consumer", "SBUX": "Consumer", "TGT":  "Consumer", "LULU": "Consumer",
        # Finance
        "JPM": "Finance", "BAC": "Finance", "GS":  "Finance", "V":   "Finance",
        "WFC": "Finance", "MS":  "Finance", "AXP": "Finance", "C":   "Finance",
        "BLK": "Finance", "CME": "Finance",
        # Healthcare
        "JNJ":  "Healthcare", "PFE":  "Healthcare", "UNH":  "Healthcare",
        "MRK":  "Healthcare", "ABBV": "Healthcare", "TMO":  "Healthcare",
        "ISRG": "Healthcare", "DHR":  "Healthcare",
        # Industrial
        "CAT": "Industrial", "BA":  "Industrial", "HON": "Industrial",
        "GE":  "Industrial", "LMT": "Industrial", "RTX": "Industrial",
        "DE":  "Industrial", "ETN": "Industrial",
        # Energy
        "CVX": "Energy", "XOM": "Energy", "SLB": "Energy",
        "COP": "Energy", "OXY": "Energy",
        # Materials
        "LIN": "Materials", "APD": "Materials", "FCX": "Materials", "NEM": "Materials",
        # REIT
        "AMT": "REIT", "PLD": "REIT", "EQIX": "REIT",
    }
    ATR_PERIOD           = int(os.getenv("ATR_PERIOD", "14"))
    ATR_STOP_MULT        = float(os.getenv("ATR_STOP_MULT", "2.0"))   # stop = ATR_STOP_MULT x ATR below entry

    # Per-strategy exit parameters — values validated by A/B backtest on the
    # 2020-2026 window (see backtest/engine.py, run_backtest.py):
    #   momentum: momentum edge needs room — wide trail, long max hold
    #   mean reversion: short-horizon edge — exit at the mean within days
    MOM_TAKE_PROFIT_PCT  = float(os.getenv("MOM_TAKE_PROFIT_PCT", "0.20"))
    MOM_MAX_HOLD_DAYS    = int(os.getenv("MOM_MAX_HOLD_DAYS", "45"))
    MOM_TRAIL_STOP_PCT   = float(os.getenv("MOM_TRAIL_STOP_PCT", "0.10"))
    MR_TAKE_PROFIT_PCT   = float(os.getenv("MR_TAKE_PROFIT_PCT", "0.10"))
    MR_MAX_HOLD_DAYS     = int(os.getenv("MR_MAX_HOLD_DAYS", "10"))
    MR_TRAIL_STOP_PCT    = float(os.getenv("MR_TRAIL_STOP_PCT", "0.0"))    # 0 = disabled

    # Sector rotation sleeve
    SECTOR_TRAIL_PCT     = float(os.getenv("SECTOR_TRAIL_PCT", "0.0"))     # 0 = rebalance-only exits
    SECTOR_ABS_FILTER    = os.getenv("SECTOR_ABS_FILTER", "true").lower() == "true"

    # Legacy fallbacks for positions_state.json entries without a strategy tag
    TRAIL_STOP_PCT       = float(os.getenv("TRAIL_STOP_PCT", "0.10"))
    MAX_HOLD_DAYS        = int(os.getenv("MAX_HOLD_DAYS", "45"))

    @classmethod
    def alpaca_base_url(cls):
        return "https://paper-api.alpaca.markets" if cls.ALPACA_MODE == "paper" else "https://api.alpaca.markets"

    @classmethod
    def alpaca_data_url(cls):
        return "https://data.alpaca.markets"

    # Sector SPDR ETFs traded by the rotation sleeve (also kept fresh by the
    # data pipeline). Defined here so data/ and strategy/ share one list.
    SECTOR_ETF_SYMBOLS = ["XLK", "XLF", "XLE", "XLV", "XLI", "XLY",
                          "XLP", "XLB", "XLRE", "XLC", "XLU"]

    @classmethod
    def symbols(cls):
        raw = os.getenv("SYMBOLS", (
            # Tech (15)
            "AAPL,MSFT,GOOGL,AMZN,NVDA,META,AMD,AVGO,INTC,NOW,CRWD,CRM,ORCL,QCOM,AMAT,"
            # Consumer (10)
            "TSLA,MCD,KO,WMT,PG,COST,NKE,SBUX,TGT,LULU,"
            # Finance (10)
            "JPM,BAC,GS,V,WFC,MS,AXP,C,BLK,CME,"
            # Healthcare (8)
            "JNJ,PFE,UNH,MRK,ABBV,TMO,ISRG,DHR,"
            # Industrial (8)
            "CAT,BA,HON,GE,LMT,RTX,DE,ETN,"
            # Energy (5)
            "CVX,XOM,SLB,COP,OXY,"
            # Materials (4)
            "LIN,APD,FCX,NEM,"
            # REIT (3)
            "AMT,PLD,EQIX"
        ))
        return [s.strip() for s in raw.split(",") if s.strip()]

    @classmethod
    def data_symbols(cls):
        """Everything the pipeline must keep fresh: trading universe + SPY
        (regime & relative strength) + sector ETFs (rotation sleeve)."""
        seen, out = set(), []
        for s in cls.symbols() + ["SPY"] + cls.SECTOR_ETF_SYMBOLS:
            if s not in seen:
                seen.add(s)
                out.append(s)
        return out

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
