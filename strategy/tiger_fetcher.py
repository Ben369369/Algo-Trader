"""
Reads today's Tiger Capital email from Google Sheet,
calls Claude to extract trading signals, and writes tiger_signals.json.
Run daily before market open via cron.
"""
import json
import os
from datetime import date, datetime, timedelta
from pathlib import Path

import anthropic
import gspread
from dotenv import load_dotenv
from google.oauth2.service_account import Credentials

load_dotenv(Path(__file__).parent.parent / ".env")

SIGNALS_PATH = Path(__file__).parent.parent / "data" / "tiger_signals.json"
CREDS_PATH   = Path(__file__).parent.parent / "config" / "google_creds.json"
SHEET_NAME   = "TigerCapitalSignals"
TAB_NAME     = "RawEmails"

SYMBOLS = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "NVDA", "META",
    "JPM", "V", "AMD", "AVGO", "BA", "BAC", "CAT", "CVX",
    "GS", "HON", "INTC", "JNJ", "KO", "MCD", "PFE", "PG",
    "UNH", "WFC", "WMT", "XOM"
]

CLAUDE_PROMPT = """You are a trading signal extractor for an equity trading bot.

Given the following market research email, extract actionable signals for these specific US equity symbols only:
{symbols}

Rules:
- Only include symbols with a CLEAR directional signal from the email. Omit neutral/unclear ones.
- "bullish" = email content supports buying this stock (sector tailwind, mentioned positively, rate regime favors it, etc.)
- "bearish" = email content is negative for this stock (sector headwind, mentioned negatively, regime hurts it, etc.)
- conviction: "high" = explicitly called out or strongly implied, "medium" = sector-level implication, "low" = weak/indirect signal
- boost values: high=0.15, medium=0.10, low=0.05 (negative for bearish)
- regime_bias: "risk_on" if overall market tone is bullish, "risk_off" if bearish/cautious, "neutral" if mixed

Return ONLY valid JSON, no explanation, no markdown:
{{
  "regime_bias": "risk_on|risk_off|neutral",
  "signals": {{
    "SYMBOL": {{"direction": "bullish|bearish", "conviction": "high|medium|low", "boost": 0.15, "reason": "one line"}},
    ...
  }}
}}

Email content:
{email_body}"""


def fetch_latest_email():
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets.readonly",
        "https://www.googleapis.com/auth/drive.readonly"
    ]
    creds = Credentials.from_service_account_file(str(CREDS_PATH), scopes=scopes)
    gc    = gspread.authorize(creds)

    sheet  = gc.open(SHEET_NAME).worksheet(TAB_NAME)
    # Use get_all_values() to get raw strings unaffected by Sheet date formatting
    all_values = sheet.get_all_values()
    if len(all_values) < 2:
        return None

    headers = all_values[0]
    rows = [dict(zip(headers, row)) for row in all_values[1:]]

    print(f"[tiger_fetcher] Headers: {headers}")
    print(f"[tiger_fetcher] Total data rows: {len(rows)}")

    # Auto-detect which column actually holds the "pending" status
    # (older sheet setups have "pdf_text" before "processed", shifting values)
    processed_key = "processed"
    for key in ["processed", "pdf_text"]:
        if key in headers and any(r.get(key, "").strip() == "pending" for r in rows):
            processed_key = key
            break
    print(f"[tiger_fetcher] Status column: '{processed_key}'")

    cutoff = date.today() - timedelta(days=7)

    # Find the most recent pending row within the last 7 days
    for row in reversed(rows):
        if row.get(processed_key, "").strip() != "pending":
            continue
        raw_date = row.get("date", "").strip()
        # Normalize date — handle "2026-06-14", "6/14/2026", "Jun 14, 2026", etc.
        for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%b %d, %Y", "%B %d, %Y"):
            try:
                row_date = datetime.strptime(raw_date, fmt).date()
                if row_date >= cutoff:
                    return row
                break
            except ValueError:
                continue

    return None


def extract_signals(email_body: str) -> dict:
    client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])

    msg = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=1024,
        messages=[{
            "role": "user",
            "content": CLAUDE_PROMPT.format(
                symbols=", ".join(SYMBOLS),
                email_body=email_body[:15000]
            )
        }]
    )

    raw = msg.content[0].text.strip()
    # Strip markdown code fences if Claude added them
    if raw.startswith("```"):
        raw = raw.split("```")[1]
        if raw.startswith("json"):
            raw = raw[4:]
    return json.loads(raw.strip())


def run():
    print(f"[tiger_fetcher] Running for {date.today()}")

    row = fetch_latest_email()
    if not row:
        print("[tiger_fetcher] No pending email found for today/yesterday.")
        return

    print(f"[tiger_fetcher] Parsing: {row.get('subject', '')[:60]}")
    signals = extract_signals(row["body"])

    output = {
        "issue_date":  date.today().isoformat(),
        "expiry_date": (date.today() + timedelta(days=1)).isoformat(),
        "regime_bias": signals.get("regime_bias", "neutral"),
        "signals":     signals.get("signals", {})
    }

    SIGNALS_PATH.write_text(json.dumps(output, indent=2))
    print(f"[tiger_fetcher] Wrote {len(output['signals'])} signals to tiger_signals.json")
    print(f"[tiger_fetcher] Regime: {output['regime_bias']}")
    for sym, sig in output["signals"].items():
        print(f"  {sym}: {sig['direction']} ({sig['conviction']}) boost={sig['boost']}")


if __name__ == "__main__":
    run()
