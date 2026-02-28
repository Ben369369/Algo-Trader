import sys
from config.settings import Config
from utils.logger import logger
from utils.broker import BrokerConnection
from data.pipeline import DataPipeline
from strategy.scanner import MarketScanner
from strategy.scorer import TradeScorer

def run():
    print("\n" + "="*60)
    print("   ALGORITHMIC TRADING BOT v0.2 - MARKET SCANNER")
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

    print("\n[ STEP 3 ] Scanning all symbols...")
    scanner = MarketScanner()
    scan_results = scanner.scan_all()

    print("\n[ STEP 4 ] Scoring and ranking opportunities...")
    ranked = TradeScorer.score(scan_results)

    print("\n" + "="*60)
    print("   TODAY'S TRADE LEADERBOARD")
    print("="*60)
    print(f"\n  {'RANK':<6} {'SYMBOL':<8} {'PRICE':>8} {'DIRECTION':<10} {'SCORE':>7} {'RSI':>7} {'ZSCORE':>8}")
    print(f"  {'----':<6} {'------':<8} {'-----':>8} {'---------':<10} {'-----':>7} {'---':>7} {'------':>8}")

    for rank, row in ranked.iterrows():
        direction = row['direction']
        arrow = "BUY  " if direction == "BUY" else "SELL " if direction == "SELL" else "     "
        print(
            f"  {rank:<6} "
            f"{row['symbol']:<8} "
            f"${row['price']:>7.2f} "
            f"{arrow:<10} "
            f"{row['score']:>7.4f} "
            f"{row['rsi']:>7.2f} "
            f"{row['zscore']:>8.3f}"
        )

    best = ranked.iloc[0]
    print(f"\n  BEST OPPORTUNITY RIGHT NOW:")
    print(f"  {best['symbol']} — Score: {best['score']} | Direction: {best['direction']} | RSI: {best['rsi']} | Price: ${best['price']}")

    print("\n" + "="*60)
    print("  Scan complete. Run again anytime to refresh.")
    print("="*60 + "\n")

if __name__ == "__main__":
    run()