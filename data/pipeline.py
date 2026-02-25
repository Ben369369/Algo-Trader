import sqlite3
import pandas as pd
from datetime import datetime, timedelta
from tqdm import tqdm
import alpaca_trade_api as tradeapi
from alpaca_trade_api.rest import TimeFrame
from config.settings import Config
from utils.logger import logger

def init_database():
    with sqlite3.connect(Config.DB_PATH) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS ohlcv (
                symbol TEXT NOT NULL, timestamp TEXT NOT NULL,
                open REAL, high REAL, low REAL, close REAL, volume REAL,
                PRIMARY KEY (symbol, timestamp))""")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_symbol_time ON ohlcv (symbol, timestamp)")
        conn.commit()
    logger.info(f"Database ready at: {Config.DB_PATH}")

class DataPipeline:
    def __init__(self):
        self.api = tradeapi.REST(
            key_id=Config.ALPACA_API_KEY,
            secret_key=Config.ALPACA_SECRET_KEY,
            base_url=Config.alpaca_base_url(),
            api_version="v2",
        )
        init_database()

    def download_symbol(self, symbol):
        end_date   = datetime.now()
        start_date = end_date - timedelta(days=365 * Config.LOOKBACK_YEARS)
        try:
            bars = self.api.get_bars(symbol, TimeFrame.Day,
                start=start_date.strftime("%Y-%m-%d"),
                end=end_date.strftime("%Y-%m-%d"),
                adjustment="all", feed="iex").df
            if bars.empty:
                logger.warning(f"{symbol}: No data returned")
                return 0
            bars = bars.reset_index()
            bars["symbol"] = symbol
            bars["timestamp"] = bars["timestamp"].astype(str)
            return self._store_bars(symbol, bars)
        except Exception as e:
            logger.error(f"{symbol}: Download failed — {e}")
            return 0

    def download_all(self):
        results = {}
        for symbol in tqdm(Config.symbols(), desc="Downloading"):
            results[symbol] = self.download_symbol(symbol)
        return results

    def _store_bars(self, symbol, bars):
        rows = bars[["symbol", "timestamp", "open", "high", "low", "close", "volume"]].values.tolist()
        with sqlite3.connect(Config.DB_PATH) as conn:
            conn.executemany(
                "INSERT OR REPLACE INTO ohlcv (symbol, timestamp, open, high, low, close, volume) VALUES (?,?,?,?,?,?,?)",
                rows
            )
            conn.commit()
        logger.info(f"{symbol}: Stored {len(rows)} rows")
        return len(rows)

    def validate_data(self):
        records = []
        with sqlite3.connect(Config.DB_PATH) as conn:
            for symbol in Config.symbols():
                row = conn.execute(
                    "SELECT COUNT(*), MIN(timestamp), MAX(timestamp) FROM ohlcv WHERE symbol=?",
                    (symbol,)
                ).fetchone()
                count, start, end = row
                records.append({
                    "symbol": symbol,
                    "status": "OK" if count > 0 else "MISSING",
                    "rows": count,
                    "start": start,
                    "end": end,
                })
        return pd.DataFrame(records)

    def get_latest_bars(self, symbol, n=5):
        with sqlite3.connect(Config.DB_PATH) as conn:
            df = pd.read_sql_query(
                "SELECT * FROM ohlcv WHERE symbol=? ORDER BY timestamp DESC LIMIT ?",
                conn, params=(symbol, n)
            )
        return df.sort_values("timestamp").reset_index(drop=True)
