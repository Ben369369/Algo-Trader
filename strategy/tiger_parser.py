import json
from datetime import date
from pathlib import Path

SIGNALS_PATH = Path(__file__).parent.parent / "data" / "tiger_signals.json"


def load_tiger_boosts() -> dict:
    """
    Returns a dict of {symbol: boost_value} for today's Tiger Capital signals.
    Returns empty dict if file missing or signals have expired.
    """
    if not SIGNALS_PATH.exists():
        return {}

    with open(SIGNALS_PATH) as f:
        data = json.load(f)

    expiry = date.fromisoformat(data.get("expiry_date", "1970-01-01"))
    if date.today() > expiry:
        return {}

    return {
        symbol: entry["boost"]
        for symbol, entry in data.get("signals", {}).items()
    }


def get_regime_bias() -> str:
    """Returns 'risk_off', 'risk_on', or 'neutral'."""
    if not SIGNALS_PATH.exists():
        return "neutral"

    with open(SIGNALS_PATH) as f:
        data = json.load(f)

    expiry = date.fromisoformat(data.get("expiry_date", "1970-01-01"))
    if date.today() > expiry:
        return "neutral"

    return data.get("regime_bias", "neutral")
