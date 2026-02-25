import sys
from config.settings import Config
from utils.logger import logger
from utils.broker import BrokerConnection
from data.pipeline import DataPipeline

def run():
    print("\n" + "="*55)
    print("   ALGORITHMIC TRADING BOT v0.1 - PHASE 1 SETUP")
    print("="*55)

    print("\n[ STEP 1 ] Validating config...")
    if not Config.validate():
        print("\n  Fix errors above, then re-run.")
        sys.exit(1)

    print("\n[ STEP 2 ] Connecting to Alpaca...")
    broker = BrokerConnection()
    acc = broker.get_account()
    print(f"  Portfolio : {acc['portfolio_value']}")
    print(f"  Cash      : {acc['cash']}")
    print(f"  Status    : {acc['status'].upper()}")
    print(f"  Market    : {'OPEN' if broker.is_market_open() else 'CLOSED - next open: ' + broker.next_market_open()}")

    print("\n[ STEP 3 ] Downloading historical data...")
    pipeline = DataPipeline()
    results = pipeline.download_all()
    print(f"  Total rows downloaded: {sum(results.values()):,}")

    print("\n[ STEP 4 ] Validating data...")
    report = pipeline.validate_data()
    for _, row in report.iterrows():
        icon = "OK" if row["status"] == "OK" else "MISSING"
        print(f"  {row['symbol']:<6} {icon:<8} {int(row['rows'] or 0):>6} rows  {row['start'] or 'N/A'} to {row['end'] or 'N/A'}")

    print("\n[ STEP 5 ] Sample data (AAPL last 5 days)...")
    df = pipeline.get_latest_bars("AAPL", n=5)
    print(df[["open","high","low","close","volume"]].to_string())

    print("\n" + "="*55)
    print("  PHASE 1 COMPLETE! Ready for Phase 2.")
    print("="*55 + "\n")

if __name__ == "__main__":
    run()
