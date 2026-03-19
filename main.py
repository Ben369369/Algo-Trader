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


def run(execute_entries=True):
    print("\n" + "="*60)
    label = "FULL RUN" if execute_entries else "EXIT CHECK"
    print(f"   ALGORITHMIC TRADING BOT v0.4 -- {label}")
    print("="*60)

    print("\n[ STEP 1 ] Validating config...")
    if not Config.validate():
        sys.exit(1)

    print("\n[ STEP 2 ] Connecting to Alpaca...")
    broker = BrokerConnection()
    acc = broker.get_account()
    print(f"  Portfolio : ${acc['portfolio_value']:,.2f}")
    print(f"  Cash      : ${acc['cash']:,.2f}")
    print(f"  P&L Today : ${acc['pnl_today']:+,.2f}")
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
    print(f"\n  {'RANK':<6} {'SYMBOL':<8} {'PRICE':>8} {'DIR':<8} {'SCORE':>7} {'RSI':>7} {'ZSCORE':>8} {'VOL_R':>7}")
    print(f"  {'----':<6} {'------':<8} {'-----':>8} {'---':<8} {'-----':>7} {'---':>7} {'------':>8} {'-----':>7}")

    for rank, row in ranked.iterrows():
        print(
            f"  {rank:<6} "
            f"{row['symbol']:<8} "
            f"${row['price']:>7.2f} "
            f"{row['direction']:<8} "
            f"{row['score']:>7.4f} "
            f"{row['rsi']:>7.2f} "
            f"{row['zscore']:>8.3f} "
            f"{row['volume_ratio']:>7.2f}x"
        )

    print("\n[ STEP 6 ] Checking exit conditions on open positions...")
    executor = TradeExecutor()
    executor.check_exits(ranked)

    if execute_entries:
        print("\n[ STEP 7 ] Executing best trade opportunity...")
        if broker.is_market_open():
            order = executor.execute_best(ranked)
            if order:
                print(f"\n  ORDER PLACED: {order['side'].upper()} {order['qty']} shares of {order['symbol']}")
            else:
                print("\n  No trade executed -- no actionable signals or circuit breaker active.")
        else:
            print("\n  Market is closed -- no orders placed.")

    print("\n[ STEP 8 ] Portfolio summary...")
    positions = broker.get_positions()
    if positions:
        print(f"\n  Open Positions:")
        print(f"  {'SYMBOL':<8} {'SHARES':>8} {'ENTRY':>9} {'P&L':>12} {'P&L %':>8}")
        print(f"  {'------':<8} {'------':>8} {'-----':>9} {'---':>12} {'-----':>8}")
        for p in positions:
            print(
                f"  {p['symbol']:<8} "
                f"{p['qty']:>8.2f} "
                f"${p['avg_entry_price']:>8.2f} "
                f"${p['unrealized_pl']:>+11.2f} "
                f"{p['unrealized_plpc']*100:>+7.2f}%"
            )
    else:
        print("\n  No open positions.")

    print("\n" + "="*60 + "\n")


def _is_weekday():
    est = pytz.timezone("America/New_York")
    return datetime.datetime.now(est).weekday() < 5


def scheduled_run():
    if _is_weekday():
        run(execute_entries=True)


def scheduled_exit_check():
    if _is_weekday():
        run(execute_entries=False)


def _add_daily_job(time_str, func):
    schedule.every().monday.at(time_str).do(func)
    schedule.every().tuesday.at(time_str).do(func)
    schedule.every().wednesday.at(time_str).do(func)
    schedule.every().thursday.at(time_str).do(func)
    schedule.every().friday.at(time_str).do(func)


if __name__ == "__main__":
    # Run once immediately on startup
    scheduled_run()

    # 9:30am EST -- full run: data refresh + scan + exits + new entries
    _add_daily_job("09:30", scheduled_run)

    # 12:00pm EST -- mid-session: catch deteriorating signals before afternoon
    _add_daily_job("12:00", scheduled_exit_check)

    # 3:30pm EST -- pre-close: exit weak positions before end of day
    _add_daily_job("15:30", scheduled_exit_check)

    est = pytz.timezone("America/New_York")
    while True:
        schedule.run_pending()
        next_run = schedule.next_run()
        if next_run:
            next_run_est = next_run.astimezone(est) if next_run.tzinfo else next_run
            print(f"  Waiting... next run at {next_run_est.strftime('%Y-%m-%d %H:%M %Z')}", end="\r")
        time.sleep(60)
