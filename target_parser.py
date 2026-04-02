"""
Target Price Parser
===================
Extracts target price, asset, and direction from Polymarket
PRICE_TARGET market questions.

Examples:
  "Will BTC be above $66,000 on March 31?"   → BTC, 66000, ABOVE
  "Will Bitcoin dip to $60,000 on April 5?"  → BTC, 60000, BELOW
  "Will ETH reach $2,100 by April 2?"        → ETH, 2100,  ABOVE
  "Will BTC stay below $70k this week?"      → BTC, 70000, BELOW
  "Will the price of ETH be above $2,000?"   → ETH, 2000,  ABOVE
  "Will SOL exceed $200 by end of March?"    → SOL, 200,   ABOVE
  "BTC above 80k end of March?"              → BTC, 80000, ABOVE
"""

import re

# Asset detection patterns
ASSET_PATTERNS = {
    "BTC": [
        "bitcoin", "btc", "btc/usd", "xbt",
    ],
    "ETH": [
        "ethereum", "eth", "eth/usd", "ether",
    ],
    "SOL": [
        "solana", "sol", "sol/usd",
    ],
    "BNB": [
        "binance coin", "bnb",
    ],
    "XRP": [
        "xrp", "ripple",
    ],
}

# Direction keywords
ABOVE_KEYWORDS = [
    "above", "over", "exceed", "higher than", "greater than",
    "break", "surpass", "reach", "hit", "touch",
    "more than", ">",
]

BELOW_KEYWORDS = [
    "below", "under", "dip to", "drop to", "fall to",
    "less than", "lower than", "<", "down to",
    "stay below", "remain below",
]


def parse_price_value(raw: str) -> float | None:
    """
    Convert price string to float.
    Handles: $66,000  $66k  $2.1k  66000  2100  80K
    """
    if not raw:
        return None

    raw = raw.strip().replace(",", "").replace("$", "").strip()

    # Handle k/K suffix (e.g. "66k", "2.1k", "80K")
    if raw.lower().endswith("k"):
        try:
            return float(raw[:-1]) * 1000
        except ValueError:
            return None

    # Handle m/M suffix (e.g. "1.5m")
    if raw.lower().endswith("m"):
        try:
            return float(raw[:-1]) * 1_000_000
        except ValueError:
            return None

    try:
        return float(raw)
    except ValueError:
        return None


def detect_asset(question: str) -> str | None:
    """Detect which crypto asset the market is about."""
    q = question.lower()
    for asset, patterns in ASSET_PATTERNS.items():
        for pattern in patterns:
            # Use word boundaries for short tickers
            if len(pattern) <= 3:
                if re.search(rf'\b{re.escape(pattern)}\b', q):
                    return asset
            else:
                if pattern in q:
                    return asset
    return None


def detect_direction(question: str) -> str | None:
    """
    Detect whether YES resolves if price is ABOVE or BELOW target.
    Returns 'ABOVE' or 'BELOW'.
    """
    q = question.lower()

    # Check below keywords first (more specific)
    for kw in BELOW_KEYWORDS:
        if kw in q:
            return "BELOW"

    for kw in ABOVE_KEYWORDS:
        if kw in q:
            return "ABOVE"

    return None


def extract_target_price(question: str) -> float | None:
    """
    Extract the dollar target from a market question.
    Returns float or None if not found.
    """
    q = question

    # Pattern 1: $66,000 or $66000
    match = re.search(r'\$\s*([\d,]+(?:\.\d+)?)\s*(?:k|K|m|M)?', q)
    if match:
        raw = match.group(0).replace(" ", "")
        val = parse_price_value(raw)
        if val and val > 0:
            return val

    # Pattern 2: number followed by k/K (e.g. "66k", "2.1k")
    match = re.search(r'\b(\d+(?:\.\d+)?)\s*[kK]\b', q)
    if match:
        val = parse_price_value(match.group(0))
        if val and val > 0:
            return val

    # Pattern 3: plain large number that looks like a price
    # e.g. "above 66000" or "reach 2100"
    # Must be preceded by a price-related keyword
    price_context = re.search(
        r'(?:above|below|reach|exceed|dip to|drop to|at|over|under)\s+'
        r'(\d{3,}(?:,\d{3})*(?:\.\d+)?)',
        q, re.IGNORECASE
    )
    if price_context:
        val = parse_price_value(price_context.group(1))
        if val and val > 0:
            return val

    return None


def parse_market(question: str) -> dict | None:
    """
    Full parse of a market question.
    Returns dict with asset, target_price, direction
    or None if not a parseable price target market.

    Example return:
    {
        "asset": "BTC",
        "target_price": 66000.0,
        "direction": "ABOVE",   # YES wins if BTC > 66000
        "question": "Will BTC be above $66,000 on March 31?"
    }
    """
    asset = detect_asset(question)
    if not asset:
        return None

    target = extract_target_price(question)
    if not target:
        return None

    direction = detect_direction(question)
    if not direction:
        # Default to ABOVE for ambiguous cases
        direction = "ABOVE"

    # Sanity check: prices should be in reasonable ranges
    price_ranges = {
        "BTC": (1_000, 500_000),
        "ETH": (50,    50_000),
        "SOL": (1,     10_000),
        "BNB": (10,    10_000),
        "XRP": (0.01,  100),
    }
    lo, hi = price_ranges.get(asset, (0, float("inf")))
    if not (lo <= target <= hi):
        return None

    return {
        "asset":        asset,
        "target_price": target,
        "direction":    direction,
        "question":     question,
    }


def calculate_proximity(current_price: float, target_price: float,
                         direction: str) -> dict:
    """
    Calculate how close the current price is to the target.

    Returns:
        proximity_pct:  how far away (always positive %)
        approaching:    True if price is moving toward target
        already_past:   True if price already past target
        gap_pct:        signed gap (positive = approaching from correct side)
    """
    if target_price == 0:
        return {"proximity_pct": 999, "approaching": False,
                "already_past": False, "gap_pct": 999}

    gap_pct = round((current_price - target_price) / target_price * 100, 2)

    if direction == "ABOVE":
        # YES wins if current > target
        # approaching = current is below but getting close
        already_past  = current_price > target_price
        approaching   = not already_past
        proximity_pct = abs(gap_pct)  # how far below target

    else:  # BELOW
        # YES wins if current < target
        # approaching = current is above but dropping
        already_past  = current_price < target_price
        approaching   = not already_past
        proximity_pct = abs(gap_pct)  # how far above target

    return {
        "proximity_pct": round(proximity_pct, 2),
        "approaching":   approaching,
        "already_past":  already_past,
        "gap_pct":       gap_pct,
    }


# ─────────────────────────────────────────────────────────────
# Tests
# ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    test_cases = [
        ("Will BTC be above $66,000 on March 31?",          "BTC", 66000,  "ABOVE"),
        ("Will Bitcoin dip to $60,000 on April 5?",         "BTC", 60000,  "BELOW"),
        ("Will ETH reach $2,100 by April 2?",               "ETH", 2100,   "ABOVE"),
        ("Will BTC stay below $70k this week?",             "BTC", 70000,  "BELOW"),
        ("Will the price of ETH be above $2,000 on Apr 3?", "ETH", 2000,   "ABOVE"),
        ("Will SOL exceed $200 by end of March?",           "SOL", 200,    "ABOVE"),
        ("BTC above 80k end of March?",                     "BTC", 80000,  "ABOVE"),
        ("Will BTC/USD exceed $75,000 by April?",           "BTC", 75000,  "ABOVE"),
        ("Will Ethereum drop below $1,800 this week?",      "ETH", 1800,   "BELOW"),
        ("Will XRP reach $3 by April?",                     "XRP", 3,      "ABOVE"),
        # Should NOT parse (no price target)
        ("Will Bitcoin dominate crypto in April?",           None,  None,   None),
        ("Will Trump win the election?",                     None,  None,   None),
    ]

    print("Target Parser Tests")
    print("=" * 65)
    all_pass = True
    for q, exp_asset, exp_price, exp_dir in test_cases:
        result = parse_market(q)
        if exp_asset is None:
            ok = result is None
        else:
            ok = (result is not None and
                  result["asset"] == exp_asset and
                  result["target_price"] == exp_price and
                  result["direction"] == exp_dir)
        if not ok:
            all_pass = False
        status = "✅" if ok else "❌"
        if result:
            print(f"{status} {result['asset']:3} ${result['target_price']:>10,.0f} "
                  f"{result['direction']:<6} | {q[:55]}")
        else:
            print(f"{status} None                     | {q[:55]}")

    print()
    print("✅ All passed" if all_pass else "❌ Some failed")

    # Proximity test
    print("\nProximity Tests")
    print("=" * 65)
    prox_tests = [
        ("BTC above $66k", 66000, "ABOVE", 64000,  "approaching, 3.0% away"),
        ("BTC above $66k", 66000, "ABOVE", 66500,  "already past target"),
        ("BTC above $66k", 66000, "ABOVE", 60000,  "too far, 9.1% away"),
        ("ETH below $1800", 1800, "BELOW", 1850,   "approaching, 2.8% away"),
        ("ETH below $1800", 1800, "BELOW", 1750,   "already past target"),
    ]
    for label, target, direction, current, expected in prox_tests:
        p = calculate_proximity(current, target, direction)
        print(f"  {label:<20} current=${current:,} → "
              f"proximity={p['proximity_pct']:.1f}% "
              f"approaching={p['approaching']} "
              f"past={p['already_past']}")