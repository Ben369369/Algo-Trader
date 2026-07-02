# Algo-Trader
Automated stock trading bot built in Python. Connects to Alpaca's brokerage API and runs a regime-switching multi-strategy portfolio — sector ETF rotation, breakout momentum, and short-horizon mean reversion — with full risk management, a SQLite data pipeline, a live-mirror backtesting engine, and paper trading mode.

## How It Works
The bot scans 63 US large caps plus the 11 SPDR sector ETFs three times a day (9:30am, 12pm, 3:30pm ET). A market regime detector on SPY decides which strategy may open new positions:

| SPY regime | Condition | Entry strategy |
|---|---|---|
| TRENDING_UP | price > SMA50 > SMA200, RSI > 45 | Momentum (buy breakouts) |
| RANGING | pullback below SMA50 inside an uptrend | Mean reversion (buy dips) |
| TRENDING_DOWN | death cross (SMA50 < SMA200) | Cash only — exits keep working |

Every position is tagged with the strategy that opened it and is exited by that strategy's rules, no matter how the regime shifts later.

## Strategies

**Sector rotation** — rank the 11 sector ETFs by composite 3m+6m momentum (skip-month), hold the top 3 at 15% of equity each, rebalance ~monthly. An absolute-momentum filter rotates to cash instead of holding falling sectors (dual momentum).

**Momentum** — buy stocks within 5% of their 20-day high, in a golden-cross uptrend, RSI 50–75, outperforming SPY over 20 days. Exit on close below SMA50, a 10% trailing stop, a 2×ATR hard stop, +20% target, or 45-day time-out. Winners get room to run.

**Mean reversion (v2)** — buy stocks with RSI < 35 and z-score < -1 inside an intact long-term uptrend; exit when price reverts to its 20-day mean (z-score ≥ 0 or RSI > 55), at +10%, at the 2×ATR stop, or after 10 days. Short holds per the short-term reversal literature.

## Risk Management
- 2% of portfolio risked per trade, ATR-based stop distances
- 10% max position size, max 2 positions per sector
- Broker-level bracket orders (stop + target) plus soft-stop safety nets
- 10% portfolio drawdown circuit breaker halts all new entries (stocks and sector ETFs)
- Per-strategy holding-period limits (45d momentum / 10d mean reversion)

## Backtesting
`backtest/engine.py` is a live-mirror simulation: same regime switching, same scorers, same sizing, same exit ladder, same sector sleeve, T-1 signal → T open execution, commissions + slippage. Validated on 2020–2026 (including the 2022 bear): composite ≈ 14% CAGR, Sharpe 1.13, max drawdown -12.3% vs SPY 16.1% / 0.97 / -24.5%.

```
python run_backtest.py                            # full composite (default)
python run_backtest.py --strategy momentum        # single sleeve
python run_backtest.py --strategy all             # every sleeve + composite
python run_backtest.py --db data/market_data_10y.db   # 6-year history DB
```

## Project Structure
```
main.py                    — Live trading loop (runs 3x daily, ET-aware scheduling)
run_backtest.py            — Backtest CLI (no API keys needed)
config/settings.py         — All configuration, per-strategy exit parameters
data/pipeline.py           — Alpaca API + SQLite OHLCV store (incremental + backfill)
strategy/
  regime.py                — SPY regime detection
  signals.py               — Mean-reversion signals
  momentum_signals.py      — Momentum/breakout signals
  sector_rotation.py       — Sector ETF rotation sleeve
  scanner.py               — Scans all symbols
  scorer.py                — Ranks candidates by quality score
  sizer.py                 — ATR-based position sizing
  executor.py              — Bracket orders + per-strategy exit ladder
backtest/
  engine.py                — Live-mirror backtest engine
  metrics.py               — Sharpe, drawdown, win rate, vs SPY
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
- Phase 5: Live-mirror backtest engine + validated strategy overhaul
  (per-strategy exits, mean-reversion v2, sector dual momentum, regime-consistent exits)
