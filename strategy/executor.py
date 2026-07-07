import json
import datetime
from pathlib import Path
from utils.logger import logger
from utils.broker import BrokerConnection
from strategy.sizer import PositionSizer
from config.settings import Config

STATE_FILE = Path(__file__).parent.parent / "data" / "positions_state.json"


def _strategy_params(strategy):
    """Per-strategy exit parameters (validated by backtest A/B)."""
    if strategy == "mean_reversion":
        return {
            "take_profit_pct": Config.MR_TAKE_PROFIT_PCT,
            "max_hold_days":   Config.MR_MAX_HOLD_DAYS,
            "trail_stop_pct":  Config.MR_TRAIL_STOP_PCT,
        }
    if strategy == "momentum":
        return {
            "take_profit_pct": Config.MOM_TAKE_PROFIT_PCT,
            "max_hold_days":   Config.MOM_MAX_HOLD_DAYS,
            "trail_stop_pct":  Config.MOM_TRAIL_STOP_PCT,
        }
    # Legacy/untagged positions fall back to global settings
    return {
        "take_profit_pct": Config.MOM_TAKE_PROFIT_PCT,
        "max_hold_days":   Config.MAX_HOLD_DAYS,
        "trail_stop_pct":  Config.TRAIL_STOP_PCT,
    }


class TradeExecutor:

    def __init__(self):
        self.broker = BrokerConnection()
        self._state = self._load_state()
        self._reconcile_state()

    # ------------------------------------------------------------------
    # State file — tracks entry metadata for stops, targets & time exits
    # ------------------------------------------------------------------

    def _reconcile_state(self):
        """Remove state entries for positions Alpaca no longer holds (e.g. bracket stop auto-executed)."""
        held = {p["symbol"] for p in self.broker.get_positions()}
        stale = [s for s in self._state if not s.startswith("__") and s not in held]
        if stale:
            for sym in stale:
                logger.info(f"{sym}: Removing stale state — bracket order closed this position.")
            for sym in stale:
                self._state.pop(sym)
            self._save_state()

    def _load_state(self):
        if STATE_FILE.exists():
            try:
                with open(STATE_FILE) as f:
                    return json.load(f)
            except Exception:
                return {}
        return {}

    def _save_state(self):
        try:
            with open(STATE_FILE, "w") as f:
                json.dump(self._state, f, indent=2)
        except Exception as e:
            logger.error(f"Failed to save positions state: {e}")

    # ------------------------------------------------------------------
    # Entry execution
    # ------------------------------------------------------------------

    def execute_best(self, ranked_df, max_entries=5, strategy="momentum"):
        """
        Finds the top N highest-scored actionable BUY signals and executes them.
        Uses ATR-based stops; enforces a portfolio drawdown circuit breaker.
        `strategy` tags the resulting positions so exits use the right rules.
        Returns list of placed orders.
        """
        account   = self.broker.get_account()
        portfolio = account["portfolio_value"]
        cash      = float(account["cash"])

        # --- Drawdown circuit breaker ---
        peak = self._state.get("__portfolio_peak__", portfolio)
        if portfolio > peak:
            peak = portfolio
            self._state["__portfolio_peak__"] = peak
            self._save_state()

        drawdown = (peak - portfolio) / peak
        if drawdown > Config.MAX_DRAWDOWN_LIMIT:
            logger.warning(
                f"Portfolio drawdown {drawdown:.1%} exceeds limit "
                f"{Config.MAX_DRAWDOWN_LIMIT:.0%} — halting new entries."
            )
            return []

        # --- Cash floor guard ---
        if cash < Config.MIN_CASH_BUFFER:
            logger.warning(
                f"Cash ${cash:,.2f} is below the minimum buffer "
                f"${Config.MIN_CASH_BUFFER:,.0f} — halting new entries to avoid margin."
            )
            return []

        # Filter to confirmed buy signals only
        actionable = ranked_df[ranked_df["direction"] == "BUY"]
        if actionable.empty:
            logger.info("No actionable BUY signals — holding cash.")
            return []

        positions    = self.broker.get_positions()
        held_symbols = {p["symbol"] for p in positions}

        # Count how many open positions exist per sector
        sector_map = Config.SECTOR_MAP
        held_sectors: dict[str, int] = {}
        for p in positions:
            sector = sector_map.get(p["symbol"], "Other")
            held_sectors[sector] = held_sectors.get(sector, 0) + 1

        params = _strategy_params(strategy)
        orders = []

        for _, candidate in actionable.iterrows():
            if len(orders) >= max_entries:
                break

            symbol = candidate["symbol"]
            price  = candidate["price"]
            score  = candidate["score"]
            atr    = candidate.get("atr", None)

            if symbol in held_symbols:
                logger.info(f"Already holding {symbol} — skipping.")
                continue

            # Sector concentration guard
            sector = sector_map.get(symbol, "Other")
            if held_sectors.get(sector, 0) >= Config.MAX_POSITIONS_PER_SECTOR:
                logger.info(
                    f"{symbol}: Sector '{sector}' already has "
                    f"{held_sectors[sector]} position(s) — skipping to avoid concentration."
                )
                continue

            logger.info(f"Candidate: BUY {symbol} @ ${price} | Score: {score} | ATR: {atr} | {strategy}")

            # Use live price for accurate bracket levels
            live_price = self.broker.get_latest_price(symbol)
            if live_price:
                logger.info(f"{symbol}: Using live price ${live_price:.2f} (last close was ${price:.2f})")
                price = live_price

            shares = PositionSizer.calculate(
                portfolio, price,
                risk_pct=Config.MAX_RISK_PER_TRADE,
                atr=atr,
                atr_multiplier=Config.ATR_STOP_MULT,
            )
            if shares <= 0:
                logger.warning(f"{symbol}: Position size calculated as 0 — skipping.")
                continue

            trade_cost = shares * price
            if trade_cost > cash - Config.MIN_CASH_BUFFER:
                logger.warning(
                    f"{symbol}: Trade cost ${trade_cost:,.0f} would push cash below "
                    f"buffer — skipping."
                )
                continue

            stop   = PositionSizer.stop_price(price, atr=atr, atr_multiplier=Config.ATR_STOP_MULT)
            target = round(price * (1 + params["take_profit_pct"]), 2)

            order = self.broker.place_bracket_order(
                symbol=symbol,
                qty=shares,
                side="buy",
                stop_price=stop,
                take_profit_price=target,
            )

            if order:
                self._state[symbol] = {
                    "entry_date":      str(datetime.date.today()),
                    "entry_price":     price,
                    "stop_price":      stop,
                    "target_price":    target,
                    "high_water_mark": price,
                    "strategy":        strategy,
                }
                self._save_state()
                logger.info(f"State saved for {symbol}: entry=${price:.2f} stop=${stop:.2f} target=${target:.2f} [{strategy}]")
                cash -= trade_cost
                held_symbols.add(symbol)
                held_sectors[sector] = held_sectors.get(sector, 0) + 1
                orders.append(order)

        if not orders:
            logger.info("No orders placed.")

        return orders

    # ------------------------------------------------------------------
    # Exit management
    # ------------------------------------------------------------------

    def check_exits(self, ranked_by_strategy):
        """
        Check all open positions for exit conditions in priority order:
          1. Soft stop-loss    — price below the ATR-based stop (safety net if
                                 the broker bracket is gone)
          2. Soft take-profit  — price >= target_price (same safety net)
          3. Time-based exit   — held > per-strategy max hold days
          4. Trailing stop     — price fell trail% below high-water mark
                                 (momentum only; 0 disables)
          5. Signal-based exit — SELL signal from the scanner that ENTERED the
                                 position (regime flips don't change exit rules)

        ranked_by_strategy: dict {"momentum": df, "mean_reversion": df}.
        A bare DataFrame is also accepted and used for all stock positions.
        Sector ETF positions (is_sector_etf) are exited by the monthly
        rebalance; only an optional SECTOR_TRAIL_PCT trailing stop applies.
        """
        if not isinstance(ranked_by_strategy, dict):
            ranked_by_strategy = {
                "momentum":       ranked_by_strategy,
                "mean_reversion": ranked_by_strategy,
            }

        positions = self.broker.get_positions()
        if not positions:
            return

        today = datetime.date.today()

        for position in positions:
            symbol     = position["symbol"]
            live_price = self.broker.get_latest_price(symbol)
            state      = self._state.get(symbol, {})

            # Sync state from broker when state file is missing or stale
            if not state:
                entry_price = position.get("avg_entry_price", 0)
                state = {
                    "entry_date":      str(today),
                    "entry_price":     entry_price,
                    "stop_price":      round(entry_price * 0.94, 2),
                    "target_price":    round(entry_price * (1 + Config.MOM_TAKE_PROFIT_PCT), 2),
                    "high_water_mark": entry_price,
                    "strategy":        "momentum",
                }
                self._state[symbol] = state
                self._save_state()
                logger.info(f"{symbol}: Initialised missing state from broker position.")

            # Update high-water mark
            if live_price and live_price > state.get("high_water_mark", 0):
                state["high_water_mark"] = live_price
                self._state[symbol] = state
                self._save_state()

            is_etf   = state.get("is_sector_etf", False)
            strategy = state.get("strategy", "momentum")
            params   = _strategy_params(strategy)

            if is_etf:
                # Sector ETFs: optional trailing stop only; rebalance handles the rest
                trail_pct = Config.SECTOR_TRAIL_PCT
                hwm = state.get("high_water_mark", 0)
                if trail_pct > 0 and hwm > 0 and live_price:
                    trail_stop = round(hwm * (1 - trail_pct), 2)
                    if live_price < trail_stop:
                        logger.info(
                            f"{symbol}: Sector trailing stop hit — "
                            f"price ${live_price:.2f} < stop ${trail_stop:.2f} (HWM ${hwm:.2f})"
                        )
                        self._exit_position(symbol, position, f"sector trailing stop ${trail_stop:.2f}")
                continue

            # 1. Soft stop-loss — the ATR-based stop from entry, enforced here in
            #    case the broker bracket leg is missing. Replaces the old fixed
            #    4% breakdown exit, which fired long before ATR stops on
            #    volatile names and broke the risk-sizing assumptions.
            stop_price = state.get("stop_price", 0)
            if live_price and stop_price and live_price < stop_price:
                logger.warning(
                    f"{symbol}: Soft stop hit — "
                    f"price ${live_price:.2f} < stop ${stop_price:.2f}"
                )
                self._exit_position(symbol, position, f"soft stop ${stop_price:.2f}")
                continue

            # 2. Soft take-profit — fires if broker bracket order is missing
            target_price = state.get("target_price", 0)
            if live_price and target_price and live_price >= target_price:
                pnl_pct = position["unrealized_plpc"] * 100
                logger.info(
                    f"{symbol}: Soft take-profit hit — "
                    f"price ${live_price:.2f} >= target ${target_price:.2f} | P&L: {pnl_pct:.2f}%"
                )
                self._exit_position(symbol, position, f"soft take-profit ${target_price:.2f}")
                continue

            # 3. Time-based exit — per-strategy holding period
            entry_date_str = state.get("entry_date", "")
            if entry_date_str:
                try:
                    days_held = (today - datetime.date.fromisoformat(entry_date_str)).days
                    if days_held >= params["max_hold_days"]:
                        logger.info(f"{symbol}: Time-based exit after {days_held} days [{strategy}].")
                        self._exit_position(symbol, position, f"time exit ({days_held}d)")
                        continue
                except ValueError:
                    pass

            # 4. Trailing stop — per-strategy; 0 disables
            trail_pct = params["trail_stop_pct"]
            hwm = state.get("high_water_mark", 0)
            if trail_pct > 0 and hwm > 0 and live_price:
                trail_stop = round(hwm * (1 - trail_pct), 2)
                if live_price < trail_stop:
                    logger.info(
                        f"{symbol}: Trailing stop hit — "
                        f"price ${live_price:.2f} < stop ${trail_stop:.2f} (HWM ${hwm:.2f})"
                    )
                    self._exit_position(symbol, position, f"trailing stop ${trail_stop:.2f}")
                    continue

            # 5. Signal-based exit — from the strategy that entered the position
            ranked_df = ranked_by_strategy.get(strategy)
            if ranked_df is None or ranked_df.empty:
                continue
            match = ranked_df[ranked_df["symbol"] == symbol]
            if match.empty:
                continue

            if match.iloc[0]["direction"] == "SELL":
                pnl_pct = position["unrealized_plpc"] * 100
                logger.info(f"Exit signal on {symbol} [{strategy}] — P&L: {pnl_pct:.2f}%")
                self._exit_position(symbol, position, f"{strategy} sell signal")

    def _exit_position(self, symbol, position, reason):
        """Cancel bracket legs and place a clean market exit."""
        self.broker.cancel_orders_for_symbol(symbol)
        order = self.broker.place_market_order(
            symbol=symbol,
            qty=abs(int(position["qty"])),
            side="sell",
            note=reason,
        )
        if order:
            self._state.pop(symbol, None)
            self._save_state()
        else:
            logger.warning(
                f"{symbol}: Sell order failed — keeping state so next run retries."
            )
