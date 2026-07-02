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
from strategy.sector_rotation import SectorRotationExecutor, rank_sectors

EASTERN = pytz.timezone("America/New_York")


def _print_leaderboard(ranked, kind):
    print("\n" + "="*60)
    print("   TRADE LEADERBOARD")
    print("="*60)
    if ranked is None or ranked.empty:
        print("\n  No scan results.")
        return

    if kind == "momentum":
        cols = ("RANK", "SYMBOL", "PRICE", "DIR", "SCORE", "RSI", "REL_STR", "VOL_R")
        last_col = "rel_strength"
    else:
        cols = ("RANK", "SYMBOL", "PRICE", "DIR", "SCORE", "RSI", "ZSCORE", "VOL_R")
        last_col = "zscore"

    print(f"\n  {cols[0]:<6} {cols[1]:<8} {cols[2]:>8} {cols[3]:<8} {cols[4]:>7} {cols[5]:>7} {cols[6]:>8} {cols[7]:>7}")
    print(f"  {'----':<6} {'------':<8} {'-----':>8} {'---':<8} {'-----':>7} {'---':>7} {'-------':>8} {'-----':>7}")
    for rank, row in ranked.iterrows():
        print(
            f"  {rank:<6} "
            f"{row['symbol']:<8} "
            f"${row['price']:>7.2f} "
            f"{row['direction']:<8} "
            f"{row['score']:>7.4f} "
            f"{row['rsi']:>7.2f} "
            f"{row[last_col]:>8.3f} "
            f"{row['volume_ratio']:>7.2f}x"
        )


def run(execute_entries=True):
    print("\n" + "="*60)
    label = "FULL RUN" if execute_entries else "EXIT CHECK"
    print(f"   ALGORITHMIC TRADING BOT v0.5 -- {label}")
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
    pipeline.download_all()   # trading universe + SPY + sector ETFs

    print("\n[ STEP 4 ] Detecting market regime and scanning symbols...")
    scanner = MarketScanner()

    # Both scans run every time so exits can always be evaluated with the
    # rules of the strategy that ENTERED each position, regardless of the
    # regime today. Entries only use the regime-appropriate scan.
    mom_results, regime = scanner.scan_all_momentum()
    mr_results = scanner.scan_all()
    print(f"  Regime: {regime}")

    ranked_mom = TradeScorer.score_momentum(mom_results) if not mom_results.empty else mom_results
    ranked_mr  = TradeScorer.score(mr_results) if not mr_results.empty else mr_results

    print("\n[ STEP 5 ] Scoring and ranking...")
    if regime == "TRENDING_UP":
        print("  Strategy: MOMENTUM (buying breakouts)")
        entry_strategy, entry_ranked = "momentum", ranked_mom
        _print_leaderboard(ranked_mom, "momentum")
    elif regime == "RANGING":
        print("  Strategy: MEAN-REVERSION (buying dips)")
        entry_strategy, entry_ranked = "mean_reversion", ranked_mr
        _print_leaderboard(ranked_mr, "mean_reversion")
    else:
        print("  Strategy: CASH ONLY (death cross -- no new entries)")
        entry_strategy, entry_ranked = None, None
        _print_leaderboard(ranked_mr, "mean_reversion")

    print("\n[ STEP 6 ] Checking exit conditions on open positions...")
    executor = TradeExecutor()
    executor.check_exits({"momentum": ranked_mom, "mean_reversion": ranked_mr})

    if execute_entries:
        max_entries = 5
        print(f"\n[ STEP 7 ] Executing top trade opportunities (up to {max_entries})...")
        if entry_strategy is None:
            print("\n  Death cross detected -- no new entries, exits only.")
        elif broker.is_market_open():
            orders = executor.execute_best(entry_ranked, max_entries=max_entries,
                                           strategy=entry_strategy)
            if orders:
                for order in orders:
                    print(f"\n  ORDER PLACED: {order['side'].upper()} {order['qty']} shares of {order['symbol']}")
            else:
                print("\n  No trade executed -- no actionable signals or circuit breaker active.")
        else:
            print("\n  Market is closed -- no orders placed.")

    print("\n[ STEP 7.5 ] Sector rotation (monthly rebalance)...")
    sector_ranked = rank_sectors()
    if not sector_ranked.empty:
        print(f"\n  {'RANK':<6} {'ETF':<6} {'SECTOR':<26} {'3M':>7} {'6M':>7} {'SCORE':>8}")
        print(f"  {'----':<6} {'---':<6} {'------':<26} {'---':>7} {'---':>7} {'-----':>8}")
        for rank, row in sector_ranked.iterrows():
            marker = " <-- TOP" if rank <= 3 and row["composite"] > 0 else ""
            print(f"  {rank:<6} {row['symbol']:<6} {row['sector']:<26} {row['mom_3m']:>6.1f}% {row['mom_6m']:>6.1f}% {row['composite']:>7.1f}%{marker}")
    if execute_entries and broker.is_market_open():
        sector_exec = SectorRotationExecutor()
        sector_orders = sector_exec.rebalance()
        if sector_orders:
            for o in sector_orders:
                print(f"\n  SECTOR ORDER: {o['side'].upper()} {o['qty']} shares of {o['symbol']}")
        else:
            print("\n  No sector rotation changes needed.")
    elif execute_entries and not broker.is_market_open():
        print("\n  Market closed -- sector rotation skipped.")

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
    return datetime.datetime.now(EASTERN).weekday() < 5


def scheduled_run():
    if _is_weekday():
        run(execute_entries=True)


def scheduled_exit_check():
    if _is_weekday():
        run(execute_entries=False)


def _et_to_local(hhmm):
    """
    Convert an Eastern-time HH:MM to the machine's local HH:MM.
    The `schedule` library works in local time, so on a UTC server a naive
    "09:30" would fire at 4:30/5:30am ET. Note: computed once at startup —
    restart the bot after a DST change to re-align.
    """
    h, m = map(int, hhmm.split(":"))
    now_et = datetime.datetime.now(EASTERN)
    target_et = now_et.replace(hour=h, minute=m, second=0, microsecond=0)
    return target_et.astimezone().strftime("%H:%M")


def _add_daily_job(et_time_str, func):
    local_time = _et_to_local(et_time_str)
    for day in (schedule.every().monday, schedule.every().tuesday,
                schedule.every().wednesday, schedule.every().thursday,
                schedule.every().friday):
        day.at(local_time).do(func)


if __name__ == "__main__":
    # Run once immediately on startup
    scheduled_run()

    # 9:30am ET -- full run: data refresh + scan + exits + new entries
    _add_daily_job("09:30", scheduled_run)

    # 12:00pm ET -- mid-session: catch deteriorating signals before afternoon
    _add_daily_job("12:00", scheduled_exit_check)

    # 3:30pm ET -- pre-close: exit weak positions before end of day
    _add_daily_job("15:30", scheduled_exit_check)

    while True:
        schedule.run_pending()
        next_run = schedule.next_run()
        if next_run:
            next_run_est = next_run.astimezone(EASTERN) if next_run.tzinfo else next_run
            print(f"  Waiting... next run at {next_run_est.strftime('%Y-%m-%d %H:%M %Z')}", end="\r")
        time.sleep(60)
