"""
strategy/news_filter.py — News-aware trade filters

Provides two filters backed by Alpaca's free news API:
  1. EarningsFilter  — skip trades if earnings headlines appear within N days
  2. SentimentFilter — skip trades if recent headlines are predominantly negative

For live use:  call has_earnings_soon() / is_sentiment_ok() with no cache arg.
For backtest:  call preload(symbol, start, end) once per symbol, then pass the
               returned list as `cache=` to avoid repeated API calls.

Both methods fail OPEN on API errors (don't block trades on connectivity issues).
"""

import re
import datetime
import requests
from config.settings import Config

# ---------------------------------------------------------------------------
# Keyword sets
# ---------------------------------------------------------------------------
POSITIVE_WORDS = {
    "beat", "beats", "record", "surge", "soar", "rally", "gain", "gains",
    "upgrade", "upgraded", "outperform", "strong", "growth", "profit",
    "raised", "raise", "bullish", "exceeded", "above", "better",
    "optimistic", "momentum", "breakthrough", "boost", "boosted",
    "higher", "rebound", "record-high", "expand", "expansion",
}
NEGATIVE_WORDS = {
    "miss", "misses", "missed", "cut", "cuts", "downgrade", "downgraded",
    "loss", "losses", "decline", "slump", "fall", "fell", "sink", "crash",
    "weak", "underperform", "bearish", "sell", "negative", "below",
    "disappointing", "warning", "layoff", "lawsuit", "fraud", "risk",
    "concern", "worry", "volatile", "probe", "investigation", "fine",
    "charges", "plunge", "plunged", "recall", "default", "bankrupt",
}
EARNINGS_KEYWORDS = {"earnings", "eps", "quarterly", "results", "guidance"}


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------
def _headers():
    return {
        "APCA-API-KEY-ID":     Config.ALPACA_API_KEY,
        "APCA-API-SECRET-KEY": Config.ALPACA_SECRET_KEY,
    }


def _to_rfc3339(d):
    """Convert a date string or date object to RFC 3339 UTC format required by Alpaca."""
    if isinstance(d, str):
        d = datetime.date.fromisoformat(d[:10])
    if hasattr(d, "date"):
        d = d.date()
    return f"{d.isoformat()}T00:00:00Z"


_MAX_PER_PAGE = 50   # Alpaca free-tier hard limit


def _fetch_news(symbol, start=None, end=None, limit=50):
    """
    GET Alpaca /v1beta1/news, paginating automatically up to `limit` total articles.
    `limit` may exceed 50 — the function handles multiple pages transparently.
    """
    url = f"{Config.alpaca_data_url()}/v1beta1/news"

    # Build base params (no sort — default asc is fine; we sort ourselves in preload)
    params: dict = {"symbols": symbol}
    if start:
        params["start"] = _to_rfc3339(start)
    if end:
        end_date = (datetime.date.fromisoformat(end[:10])
                    if isinstance(end, str)
                    else (end.date() if hasattr(end, "date") else end))
        params["end"] = _to_rfc3339(min(end_date, datetime.date.today()))

    collected = []
    remaining = limit
    page_token = None

    while remaining > 0:
        page_params = {**params, "limit": min(remaining, _MAX_PER_PAGE)}
        if page_token:
            page_params["page_token"] = page_token

        resp = requests.get(url, headers=_headers(), params=page_params, timeout=10)
        resp.raise_for_status()
        data = resp.json()

        batch = data.get("news", [])
        collected.extend(batch)
        remaining -= len(batch)

        page_token = data.get("next_page_token")
        if not page_token or not batch:
            break

    return collected


def _score_headlines(headlines):
    """
    Return 'negative', 'positive', or 'neutral' by majority word-hit vote.
    """
    pos = neg = 0
    for h in headlines:
        words = set(re.findall(r"\b\w+\b", h.lower()))
        pos  += len(words & POSITIVE_WORDS)
        neg  += len(words & NEGATIVE_WORDS)
    if neg > pos:
        return "negative"
    if pos > neg:
        return "positive"
    return "neutral"


def _to_date(d):
    """Coerce datetime / date / string to datetime.date."""
    if d is None:
        return datetime.date.today()
    if hasattr(d, "date"):
        return d.date()
    if isinstance(d, str):
        return datetime.date.fromisoformat(d[:10])
    return d


# ---------------------------------------------------------------------------
# NewsFilter
# ---------------------------------------------------------------------------
class NewsFilter:
    """
    News-aware filter with optional pre-loaded cache for fast backtest use.
    """

    # --- Cache pre-loading (backtest) --------------------------------------

    def preload(self, symbol, start_date, end_date):
        """
        Fetch up to 500 articles for `symbol` in [start_date, end_date].
        Returns a list sorted ascending by date:
            [{"date": datetime.date, "headline": str}, ...]

        Call once per symbol before running the backtest loop.
        Returns [] on any API error so backtest can continue without filters.
        """
        try:
            raw = _fetch_news(
                symbol,
                start=_to_date(start_date).isoformat(),
                end=_to_date(end_date).isoformat(),
                limit=500,
            )
        except Exception as exc:
            print(f"  [news_filter] preload failed for {symbol}: {exc}")
            return []

        result = []
        for a in raw:
            raw_ts = a.get("created_at") or a.get("updated_at") or ""
            try:
                dt = datetime.date.fromisoformat(raw_ts[:10])
            except Exception:
                continue
            headline = a.get("headline") or a.get("title") or ""
            result.append({"date": dt, "headline": headline})

        result.sort(key=lambda x: x["date"])
        return result

    # --- Live / backtest filter methods ------------------------------------

    def has_earnings_soon(self, symbol, ref_date=None, days=5, cache=None):
        """
        Returns True if earnings-related headlines appear within `days` of ref_date.
        - Live mode (cache=None): makes a single API call.
        - Backtest mode: pass the list returned by preload().
        - Returns False on API error (fail open — don't block the trade).
        """
        ref  = _to_date(ref_date)
        end  = ref + datetime.timedelta(days=days)

        if cache is not None:
            window_headlines = [
                a["headline"] for a in cache
                if ref <= a["date"] <= end
            ]
        else:
            try:
                raw = _fetch_news(symbol, start=ref.isoformat(),
                                  end=end.isoformat(), limit=20)
                window_headlines = [
                    a.get("headline") or a.get("title") or "" for a in raw
                ]
            except Exception:
                return False  # fail open

        for headline in window_headlines:
            words = set(re.findall(r"\b\w+\b", headline.lower()))
            if words & EARNINGS_KEYWORDS:
                return True
        return False

    def is_sentiment_ok(self, symbol, ref_date=None, n=5, cache=None):
        """
        Returns True if the last `n` headlines (up to 14 days back) are NOT
        predominantly negative.
        - Live mode (cache=None): makes a single API call.
        - Backtest mode: pass the list returned by preload().
        - Returns True on API error or no news (fail open).
        """
        ref        = _to_date(ref_date)
        lookback   = ref - datetime.timedelta(days=14)

        if cache is not None:
            recent = [
                a["headline"] for a in cache
                if lookback <= a["date"] <= ref
            ][-n:]
        else:
            try:
                raw = _fetch_news(symbol, start=lookback.isoformat(),
                                  end=ref.isoformat(), limit=n)
                recent = [a.get("headline") or a.get("title") or "" for a in raw]
            except Exception:
                return True  # fail open

        if not recent:
            return True  # no news = neutral, allow the trade

        return _score_headlines(recent) != "negative"
