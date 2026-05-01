import datetime
from utils.logger import logger

# Session-level cache: symbol -> (date_fetched, earnings_date_or_None)
_cache: dict = {}


def is_near_earnings(symbol: str, days_before: int = 5, days_after: int = 2) -> bool:
    """
    Return True if symbol has a known earnings event within the blackout window.
    Window: [today - days_after, today + days_before].
    Fails open (returns False) on any fetch error so it never silently blocks trades.
    """
    today = datetime.date.today()

    if symbol in _cache:
        cached_on, earnings_date = _cache[symbol]
        if cached_on == today:
            return _in_window(earnings_date, today, days_before, days_after)

    earnings_date = _fetch_nearest_earnings(symbol, today)
    _cache[symbol] = (today, earnings_date)
    near = _in_window(earnings_date, today, days_before, days_after)
    if near:
        logger.info(f"{symbol}: Earnings on {earnings_date} — within blackout window, skipping.")
    return near


def _fetch_nearest_earnings(symbol: str, today: datetime.date):
    try:
        import yfinance as yf
        cal = yf.Ticker(symbol).calendar
        if cal is None or (hasattr(cal, "empty") and cal.empty):
            return None
        # calendar columns are Timestamps representing upcoming earnings dates
        dates = list(cal.columns)
        if not dates:
            return None
        nearest = min(dates, key=lambda d: abs((d.date() - today).days))
        return nearest.date()
    except Exception as e:
        logger.debug(f"{symbol}: Earnings fetch failed ({e})")
        return None


def _in_window(earnings_date, today: datetime.date, days_before: int, days_after: int) -> bool:
    if earnings_date is None:
        return False
    delta = (earnings_date - today).days
    return -days_after <= delta <= days_before
