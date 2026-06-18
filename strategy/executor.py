import json
import datetime
from pathlib import Path
from utils.logger import logger
from utils.broker import BrokerConnection
from strategy.sizer import PositionSizer
from config.settings import Config

TAKE_PROFIT_PCT = 0.10   # 10% above entry
STATE_FILE      = Path(__file__).parent.parent / "data" / "positions_state.json"


class TradeExecutor:

    def __init__(self):
        self.broker = BrokerConnection()
        self._state = self._load_state()
        self._reconcile_state()

    # ------------------------------------------------------------------
    # State file — tracks entry metadata for trailing stops & time exits
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

    def execute_best(self, ranked_df, max_entries=3):
        """
        Finds the top N highest-scored actionable BUY signals and executes them.
        Uses ATR-based stops; enforces a portfolio drawdown circuit breaker.
        Returns list of placed orders.
        """
        account   = self.broker.get_account()
        portfolio = account["portfolio_value"]

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

            logger.info(f"Candidate: BUY {symbol} @ ${price} | Score: {score} | ATR: {atr}")

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

            stop   = PositionSizer.stop_price(price, atr=atr, atr_multiplier=Config.ATR_STOP_MULT)
            target = round(price * (1 + TAKE_PROFIT_PCT), 2)

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
                }
                self._save_state()
                logger.info(f"State saved for {symbol}: entry=${price:.2f} stop=${stop:.2f} target=${target:.2f}")
                held_symbols.add(symbol)
                held_sectors[sector] = held_sectors.get(sector, 0) + 1
                orders.append(order)

        if not orders:
            logger.info("No orders placed.")

        return orders

    # ------------------------------------------------------------------
    # Exit management
    # ------------------------------------------------------------------

    def check_exits(self, ranked_df):
        """
        Check all open positions for exit conditions in priority order:
          0. Breakdown exit    — price dropped >BREAKDOWN_PCT below entry (fast crash)
          1. Soft take-profit  — price >= target_price (fallback if broker bracket is gone)
          2. Time-based exit   — held > MAX_HOLD_DAYS calendar days
          3. Trailing stop     — price fell Config.TRAIL_STOP_PCT below high-water mark
          4. Signal-based exit — SELL signal from scanner
        Hard stop-loss and take-profit are also enforced by the broker's bracket OCO orders
        when present. The soft take-profit here acts as a safety net if those orders disappear.
        """
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
                    "target_price":    round(entry_price * (1 + TAKE_PROFIT_PCT), 2),
                    "high_water_mark": entry_price,
                }
                self._state[symbol] = state
                self._save_state()
                logger.info(f"{symbol}: Initialised missing state from broker position.")

            # Update high-water mark
            if live_price and live_price > state.get("high_water_mark", 0):
                state["high_water_mark"] = live_price
                self._state[symbol] = state
                self._save_state()

            # 0. Breakdown exit — price fell >4% below entry (gap down, news, fast crash)
            #    Fires before the broker hard stop to get a cleaner fill
            entry_price = state.get("entry_price", 0)
            if live_price and entry_price and live_price < entry_price * (1 - Config.BREAKDOWN_PCT):
                logger.warning(
                    f"{symbol}: Breakdown exit — "
                    f"price ${live_price:.2f} is >{Config.BREAKDOWN_PCT:.0%} below entry ${entry_price:.2f}"
                )
                self._exit_position(symbol, position, f"breakdown exit (${live_price:.2f} vs entry ${entry_price:.2f})")
                continue

            # 1. Soft take-profit — fires if broker bracket order is missing
            target_price = state.get("target_price", 0)
            if live_price and target_price and live_price >= target_price:
                pnl_pct = position["unrealized_plpc"] * 100
                logger.info(
                    f"{symbol}: Soft take-profit hit — "
                    f"price ${live_price:.2f} >= target ${target_price:.2f} | P&L: {pnl_pct:.2f}%"
                )
                self._exit_position(symbol, position, f"soft take-profit ${target_price:.2f}")
                continue

            # 2. Time-based exit
            entry_date_str = state.get("entry_date", "")
            if entry_date_str:
                try:
                    days_held = (today - datetime.date.fromisoformat(entry_date_str)).days
                    if days_held >= Config.MAX_HOLD_DAYS:
                        logger.info(f"{symbol}: Time-based exit after {days_held} days.")
                        self._exit_position(symbol, position, f"time exit ({days_held}d)")
                        continue
                except ValueError:
                    pass

            # 3. Trailing stop
            hwm = state.get("high_water_mark", 0)
            if hwm > 0 and live_price:
                trail_stop = round(hwm * (1 - Config.TRAIL_STOP_PCT), 2)
                if live_price < trail_stop:
                    logger.info(
                        f"{symbol}: Trailing stop hit — "
                        f"price ${live_price:.2f} < stop ${trail_stop:.2f} (HWM ${hwm:.2f})"
                    )
                    self._exit_position(symbol, position, f"trailing stop ${trail_stop:.2f}")
                    continue

            # 4. Signal-based exit
            match = ranked_df[ranked_df["symbol"] == symbol]
            if match.empty:
                continue

            if match.iloc[0]["direction"] == "SELL":
                pnl_pct = position["unrealized_plpc"] * 100
                logger.info(f"Exit signal on {symbol} — P&L: {pnl_pct:.2f}%")
                self._exit_position(symbol, position, "sell signal")

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
