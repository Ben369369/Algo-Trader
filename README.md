# Algo-Trader
Automated stock trading bot built in Python. Connects to Alpaca's brokerage API to detect mean-reversion signals using statistical models, then executes trades automatically with full risk management. Features a full data pipeline, SQLite database, backtesting engine, and paper trading mode.

## How It Works
AlgoTrader scans 27 US equities three times a day (9:30am, 12pm, 3:30pm EST) and buys the top 3 oversold setups it finds. Every decision is based on a combination of technical signals — RSI, Z-score, Bollinger Bands, MACD, and volume — rather than human emotion.

The strategy is **mean reversion within a trend**: it looks for stocks that have pulled back significantly from their historical average while the broader trend is still intact, then enters expecting a recovery.

## Signal Conditions (Buy)
- RSI < 38 (oversold)
- Z-score < -1.0 (price well below its rolling mean)
- Bollinger Band position < 30% (near lower band)
- MACD histogram turning upward (momentum shifting)
- Price within 5% of 200-day SMA (trend still intact)
- Above-average volume (confirms the move)

## Risk Management
- 2% of portfolio risked per trade
- ATR-based stop losses (adapts to each stock's volatility)
- 10% take-profit target
- 7% trailing stop below high-water mark
- 10% max position size
- 10% portfolio drawdown circuit breaker (halts all new entries)
- 30-day max hold period (time-based exit)
- Breakdown exit if price drops >4% below entry

## Project Structure
```
main.py               — Live trading loop (runs 3x daily)
run_backtest.py       — Backtest CLI (no API keys needed)
config/settings.py    — All configuration and env vars
data/pipeline.py      — Alpaca API + SQLite OHLCV store
strategy/
  signals.py          — Buy/sell signal detection
  scanner.py          — Scans all symbols
  scorer.py           — Ranks signals by quality score
  sizer.py            — ATR-based position sizing
  executor.py         — Places bracket orders (top 3 signals)
backtest/
  engine.py           — Pure-pandas backtest simulation
  metrics.py          — Sharpe, drawdown, win rate, vs SPY
```

## Setup
1. Clone the repo and create a virtual environment
2. `pip install -r requirements.txt`
3. Copy `.env.example` to `.env` and add your Alpaca API keys
4. `python main.py` to run live (paper mode by default)
5. `python run_backtest.py` to backtest without API keys

## Phases Complete
- Phase 1: Data pipeline, broker connection, SQLite DB
- Phase 2: Indicators, signal detection, scanner, scorer, executor
- Phase 3: Backtesting engine (pure pandas, vs SPY benchmark)
- Phase 4: Multi-entry execution, relaxed dip-buying filter, 3x daily scans
