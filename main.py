import sys
import time
import datetime
import schedule
import pytz
from config.settings import Config
from utils.logger import logger
from utils.broker import BrokerConnection
from data.pipeline import DataPipeline
from strategy.scanner import MarketScanner
from strategy.scorer import TradeScorer
from strategy.executor import TradeExecutor

def run():
    print("\n" + "="*60)
    print("   ALGORITHMIC TRADING BOT v0.3 - FULL AUTO")
    print("="*60)

    print("\n[ STEP 1 ] Validating config...")
    if not Config.validate():
        sys.exit(1)

    print("\n[ STEP 2 ] Connecting to Alpaca...")
    broker = BrokerConnection()
    acc = broker.get_account()
    print(f"  Portfolio : ${acc['portfolio_value']:,.2f}")
    print(f"  Cash      : ${acc['cash']:,.2f}")
    print(f"  Market    : {'OPEN' if broker.is_market_open() else 'CLOSED'}")

    print("\n[ STEP 3 ] Refreshing market data...")
    pipeline = DataPipeline()
    pipeline.download_all()

    print("\n[ STEP 4 ] Scanning all symbols...")
    scanner = MarketScanner()
    scan_results = scanner.scan_all()

    print("\n[ STEP 5 ] Scoring and ranking...")
    ranked = TradeScorer.score(scan_results)

    print("\n" + "="*60)
    print("   TRADE LEADERBOARD")
    print("="*60)
    print(f"\n  {'RANK':<6} {'SYMBOL':<8} {'PRICE':>8} {'DIRECTION':<10} {'SCORE':>7} {'RSI':>7} {'ZSCORE':>8} {'MACD_H':>8}")
    print(f"  {'----':<6} {'------':<8} {'-----':>8} {'---------':<10} {'-----':>7} {'---':>7} {'------':>8} {'------':>8}")

    for rank, row in ranked.iterrows():
        direction = row['direction']
        print(
            f"  {rank:<6} "
            f"{row['symbol']:<8} "
            f"${row['price']:>7.2f} "
            f"{direction:<10} "
            f"{row['score']:>7.4f} "
            f"{row['rsi']:>7.2f} "
            f"{row['zscore']:>8.3f} "
            f"{row['macd_hist']:>8.4f}"
        )

    print("\n[ STEP 6 ] Checking exit conditions on open positions...")
    executor = TradeExecutor()
    executor.check_exits(ranked)

    print("\n[ STEP 7 ] Executing best trade opportunity...")
    if broker.is_market_open():
        order = executor.execute_best(ranked)
        if order:
            print(f"\n  ORDER PLACED: {order['side'].upper()} {order['qty']} shares of {order['symbol']}")
        else:
            print("\n  No trade executed — no actionable signals or already in position.")
    else:
        print("\n  Market is closed — no orders placed.")
        print("  Run this again during market hours to execute trades.")

    print("\n[ STEP 8 ] Portfolio summary...")
    positions = broker.get_positions()
    if positions:
        print(f"\n  Open Positions:")
        print(f"  {'SYMBOL':<8} {'SHARES':>8} {'P&L':>12} {'P&L %':>8}")
        print(f"  {'------':<8} {'------':>8} {'---':>12} {'-----':>8}")
        for p in positions:
            print(
                f"  {p['symbol']:<8} "
                f"{p['qty']:>8.2f} "
                f"${p['unrealized_pl']:>+11.2f} "
                f"{p['unrealized_plpc']*100:>+7.2f}%"
            )
    else:
        print("\n  No open positions.")

    print("\n" + "="*60)
    print("  PHASE 2 COMPLETE — Bot is fully operational.")
    print("="*60 + "\n")

def scheduled_run():
    est = pytz.timezone("America/New_York")
    now = datetime.datetime.now(est)
    if now.weekday() >= 5:  # Saturday=5, Sunday=6
        print(f"  Skipping — today is {now.strftime('%A')}.")
        return
    run()


if __name__ == "__main__":
    # Run once immediately on startup
    scheduled_run()

    # Schedule every weekday at 9:30am EST
    schedule.every().monday.at("09:30").do(scheduled_run)
    schedule.every().tuesday.at("09:30").do(scheduled_run)
    schedule.every().wednesday.at("09:30").do(scheduled_run)
    schedule.every().thursday.at("09:30").do(scheduled_run)
    schedule.every().friday.at("09:30").do(scheduled_run)

    est = pytz.timezone("America/New_York")
    while True:
        schedule.run_pending()
        next_run = schedule.next_run()
        if next_run:
            next_run_est = next_run.astimezone(est) if next_run.tzinfo else next_run
            print(f"  Waiting... next run at {next_run_est.strftime('%Y-%m-%d %H:%M %Z')}", end="\r")
        time.sleep(60)