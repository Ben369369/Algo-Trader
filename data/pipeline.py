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

    def _get_stored_range(self, symbol):
        """Return (min_timestamp, max_timestamp) stored for this symbol, or None."""
        with sqlite3.connect(Config.DB_PATH) as conn:
            row = conn.execute(
                "SELECT MIN(timestamp), MAX(timestamp) FROM ohlcv WHERE symbol=?",
                (symbol,)
            ).fetchone()
        return row if row and row[0] else None

    def _fetch_range(self, symbol, start_date, end_date):
        """Fetch and store daily bars for [start_date, end_date]. Returns row count."""
        try:
            bars = self.api.get_bars(symbol, TimeFrame.Day,
                start=start_date.strftime("%Y-%m-%d"),
                end=end_date.strftime("%Y-%m-%d"),
                adjustment="all", feed="iex").df
            if bars.empty:
                logger.info(f"{symbol}: No new bars available yet.")
                return 0
            bars = bars.reset_index()
            bars["symbol"] = symbol
            bars["timestamp"] = bars["timestamp"].astype(str)
            return self._store_bars(symbol, bars)
        except Exception as e:
            logger.error(f"{symbol}: Download failed — {e}")
            return 0

    def download_symbol(self, symbol, force_full=False):
        """
        Keep a symbol's daily bars current.
          - New symbol: full fetch over LOOKBACK_YEARS.
          - Existing:  incremental fetch forward, plus a backfill if the stored
            history starts later than the LOOKBACK_YEARS window.
          - force_full: wipe and refetch everything. Use periodically — split/
            dividend adjustments are as-of fetch time, so stitched ranges drift
            slightly at the seams until refreshed.
        """
        end_date     = datetime.now()
        target_start = end_date - timedelta(days=365 * Config.LOOKBACK_YEARS)

        if force_full:
            with sqlite3.connect(Config.DB_PATH) as conn:
                conn.execute("DELETE FROM ohlcv WHERE symbol=?", (symbol,))
                conn.commit()
            logger.info(f"{symbol}: Full refetch from {target_start.date()}")
            return self._fetch_range(symbol, target_start, end_date)

        stored = self._get_stored_range(symbol)
        if not stored:
            logger.info(f"{symbol}: Full historical fetch from {target_start.date()}")
            return self._fetch_range(symbol, target_start, end_date)

        first_stored, last_stored = stored
        total = 0

        # Backfill older history if the configured window reaches further back
        first_dt = datetime.fromisoformat(first_stored[:10])
        if first_dt - timedelta(days=7) > target_start:
            logger.info(f"{symbol}: Backfilling {target_start.date()} -> {first_dt.date()}")
            total += self._fetch_range(symbol, target_start, first_dt - timedelta(days=1))

        # Incremental fetch forward
        start_date = datetime.fromisoformat(last_stored[:10]) + timedelta(days=1)
        if start_date.date() <= end_date.date():
            total += self._fetch_range(symbol, start_date, end_date)
        else:
            logger.info(f"{symbol}: Already up to date — skipping download.")
        return total

    def download_all(self, force_full=False):
        """Refresh the full data set: trading universe + SPY + sector ETFs."""
        results = {}
        for symbol in tqdm(Config.data_symbols(), desc="Downloading"):
            results[symbol] = self.download_symbol(symbol, force_full=force_full)
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
