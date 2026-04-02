"""
Proximity Bot — Price Target Spike Exploiter
=============================================
Scans Polymarket for BTC/ETH/SOL PRICE_TARGET markets
where the underlying asset is approaching the target price.

Strategy:
  Enter BEFORE the spike when asset is 2-5% from target.
  Market is priced at 25-35c (pre-spike) giving much better
  entry than the main bot which enters during the spike at 55-70c.

  Entry: 25-35c when asset 2-5% from target
  Spike: market moves to 70-90c as asset approaches target
  Exit:  tight trailing stop (-6% from peak) to catch the spike

Start command: python proximity_bot.py

Deploys to Railway as a separate service sharing the same DB.
Uses existing Telegram credentials for alerts.
"""

import asyncio
import aiohttp
import asyncpg
import json
import logging
import os
import re
from datetime import datetime

from target_parser  import parse_market, calculate_proximity
from proximity_db   import (
    init_proximity_db, log_proximity_alert,
    get_open_proximity_positions, close_proximity_position,
    update_proximity_peak, get_proximity_stats,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────────

DATABASE_URL     = os.environ.get("DATABASE_URL")
TELEGRAM_TOKEN   = os.environ.get("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")

SCAN_INTERVAL    = 60       # seconds between full scans
MONITOR_INTERVAL = 15       # seconds between position checks

# Proximity thresholds
MIN_PROXIMITY    = 0.5      # % — already past target, different trade
MAX_PROXIMITY    = 5.0      # % — too far away, skip
SWEET_SPOT_MIN   = 1.0      # % — ideal entry zone
SWEET_SPOT_MAX   = 4.0      # % — ideal entry zone

# Position management
TAKE_PROFIT_PCT  = 40.0     # % gain to take profit
STOP_LOSS_PCT    = -20.0    # % loss to stop out
TRAILING_ACTIVATE = 8.0     # % gain before trailing stop activates
TRAILING_PCT      = 6.0     # % trail from peak (tight — spike exits fast)

# Market filters
MIN_VOLUME       = 500      # minimum $ volume
MAX_DAYS_TO_RES  = 14       # skip markets resolving too far out
MIN_MARKET_PRICE = 0.15     # minimum YES price (allow pre-spike entries)
MAX_MARKET_PRICE = 0.85     # skip near-resolved markets

GAMMA_BASE = "https://gamma-api.polymarket.com/markets"
BINANCE_BASE = "https://api.binance.com/api/v3"

# Binance symbol mapping
BINANCE_SYMBOLS = {
    "BTC": "BTCUSDT",
    "ETH": "ETHUSDT",
    "SOL": "SOLUSDT",
    "BNB": "BNBUSDT",
    "XRP": "XRPUSDT",
}


def now():
    return datetime.utcnow()


# ─────────────────────────────────────────────────────────────
# Telegram
# ─────────────────────────────────────────────────────────────

async def send_telegram(message: str):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        log.warning("Telegram not configured")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text":    message,
        "parse_mode": "HTML",
    }
    try:
        async with aiohttp.ClientSession() as s:
            async with s.post(url, json=payload,
                              timeout=aiohttp.ClientTimeout(total=8)) as r:
                if r.status != 200:
                    log.warning("Telegram status %d", r.status)
    except Exception as e:
        log.warning("Telegram error: %s", e)


# ─────────────────────────────────────────────────────────────
# Binance prices
# ─────────────────────────────────────────────────────────────

_price_cache: dict = {}
_price_cache_ts: dict = {}
PRICE_CACHE_TTL = 30  # seconds


async def get_asset_price(session: aiohttp.ClientSession,
                           asset: str) -> float | None:
    """Get current price for an asset from Binance."""
    symbol = BINANCE_SYMBOLS.get(asset)
    if not symbol:
        return None

    # Cache check
    cached_at = _price_cache_ts.get(asset, 0)
    if (now().timestamp() - cached_at) < PRICE_CACHE_TTL:
        return _price_cache.get(asset)

    try:
        url = f"{BINANCE_BASE}/ticker/price?symbol={symbol}"
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=8)) as r:
            if r.status == 200:
                data = await r.json()
                price = float(data["price"])
                _price_cache[asset] = price
                _price_cache_ts[asset] = now().timestamp()
                return price
    except Exception as e:
        log.warning("Binance price error for %s: %s", asset, e)
    return _price_cache.get(asset)  # return stale if available


# ─────────────────────────────────────────────────────────────
# Market scanning
# ─────────────────────────────────────────────────────────────

async def fetch_price_target_markets(session: aiohttp.ClientSession) -> list:
    """Fetch open PRICE_TARGET markets from Gamma API."""
    markets = []
    offset  = 0
    limit   = 100

    while True:
        params = {
            "active":    "true",
            "closed":    "false",
            "limit":     limit,
            "offset":    offset,
            "order":     "volumeNum",
            "ascending": "false",
        }
        try:
            async with session.get(
                GAMMA_BASE, params=params,
                timeout=aiohttp.ClientTimeout(total=15)
            ) as r:
                if r.status != 200:
                    break
                page = await r.json()
                if not page:
                    break

                # Filter to likely price target markets
                for m in page:
                    q = m.get("question", "")
                    # Quick pre-filter before full parse
                    if any(kw in q.lower() for kw in
                           ["$", "above", "below", "reach", "dip", "exceed",
                            "price of btc", "price of eth", "price of sol",
                            "bitcoin", "ethereum", "solana"]):
                        markets.append(m)

                if len(page) < limit:
                    break
                offset += limit

                # Cap at 500 markets — only need BTC/ETH/SOL targets
                if offset >= 500:
                    break

        except Exception as e:
            log.warning("Gamma fetch error: %s", e)
            break

    log.info("Fetched %d candidate markets", len(markets))
    return markets


def parse_market_data(market: dict) -> dict | None:
    """
    Parse a raw Gamma API market dict.
    Returns enriched dict or None if not a valid price target.
    """
    question = market.get("question", "")
    parsed = parse_market(question)
    if not parsed:
        return None

    # Get YES price
    outcomes = market.get("outcomePrices", "[]")
    if isinstance(outcomes, str):
        try:
            outcomes = json.loads(outcomes)
        except Exception:
            return None
    if not outcomes:
        return None

    try:
        yes_price = float(outcomes[0])
    except (ValueError, IndexError):
        return None

    # Price filters
    if yes_price < MIN_MARKET_PRICE or yes_price > MAX_MARKET_PRICE:
        return None

    # Volume filter
    volume = float(market.get("volumeNum", 0) or 0)
    if volume < MIN_VOLUME:
        return None

    # Days to resolution
    days_to_res = None
    end_date = market.get("endDate")
    if end_date:
        try:
            from datetime import timezone
            end_dt = datetime.fromisoformat(
                str(end_date).replace("Z", "+00:00")
            ).replace(tzinfo=None)
            days_to_res = (end_dt - now()).total_seconds() / 86400
            if days_to_res < 0 or days_to_res > MAX_DAYS_TO_RES:
                return None
        except Exception:
            pass

    return {
        "market_id":      str(market.get("id", "")),
        "question":       question,
        "asset":          parsed["asset"],
        "target_price":   parsed["target_price"],
        "direction":      parsed["direction"],
        "yes_price":      yes_price,
        "volume":         volume,
        "days_to_res":    days_to_res,
    }


# ─────────────────────────────────────────────────────────────
# Alert formatting
# ─────────────────────────────────────────────────────────────

def format_proximity_alert(market: dict, proximity: dict,
                            current_price: float, alert_id: int) -> str:
    asset      = market["asset"]
    target     = market["target_price"]
    direction  = market["direction"]
    yes_price  = market["yes_price"]
    prox       = proximity["proximity_pct"]
    days       = market.get("days_to_res")

    # Entry/exit levels
    entry_pct  = yes_price
    tp_price   = round(min(entry_pct * (1 + TAKE_PROFIT_PCT/100), 0.99), 2)
    sl_price   = round(max(entry_pct * (1 + STOP_LOSS_PCT/100), 0.01), 2)

    days_str = f"{days:.1f}d" if days else "unknown"
    dir_str  = ">" if direction == "ABOVE" else "<"
    asset_emoji = {"BTC": "₿", "ETH": "Ξ", "SOL": "◎"}.get(asset, "💎")

    sweetspot = SWEET_SPOT_MIN <= prox <= SWEET_SPOT_MAX
    urgency = "🔥" if prox < 2.0 else ("⚡" if sweetspot else "💡")

    msg = (
        f"{urgency} <b>Proximity Alert!</b> {asset_emoji}\n\n"
        f"<b>Market:</b> {market['question'][:100]}\n"
        f"<b>Target:</b> {dir_str} ${target:,.0f}\n"
        f"<b>Current {asset}:</b> ${current_price:,.0f}\n"
        f"<b>Distance:</b> {prox:.1f}% away from target\n"
        f"<b>Market Price:</b> {round(yes_price*100)}¢ YES\n"
        f"<b>Volume:</b> ${market['volume']:,.0f}\n"
        f"<b>Resolves in:</b> {days_str}\n\n"
        f"<b>Position Management:</b>\n"
        f"  Take Profit: {round(tp_price*100)}¢ (+{TAKE_PROFIT_PCT:.0f}%)\n"
        f"  Stop Loss:   {round(sl_price*100)}¢ ({STOP_LOSS_PCT:.0f}%)\n"
        f"  Trail Stop:  {TRAILING_PCT:.0f}% from peak "
        f"(activates at +{TRAILING_ACTIVATE:.0f}%)\n\n"
        f"<b>Strategy:</b> Pre-spike entry — asset {prox:.1f}% from target\n"
        f"Expected spike: {round(yes_price*100)}¢ → 75-90¢ if {asset} "
        f"{'crosses' if direction == 'ABOVE' else 'drops below'} "
        f"${target:,.0f}\n\n"
        f"#proximity_bot alert #{alert_id}"
    )
    return msg


def format_exit_alert(pos: dict, exit_price: float,
                       exit_reason: str, current_asset_price: float) -> str:
    entry    = pos["entry_price"]
    ret_pct  = round((exit_price - entry) / entry * 100, 1) if entry else 0
    peak     = pos["peak_price"] or entry
    peak_pct = round((peak - entry) / entry * 100, 1) if entry else 0

    emoji = "✅" if ret_pct > 0 else "❌"
    asset = pos["asset"]

    msg = (
        f"{emoji} <b>Proximity Exit</b>\n\n"
        f"<b>Market:</b> {pos['question'][:80]}\n"
        f"<b>Exit Reason:</b> {exit_reason}\n"
        f"<b>Return:</b> {ret_pct:+.1f}%\n"
        f"<b>Peak Return:</b> {peak_pct:+.1f}%\n"
        f"<b>Entry:</b> {round(entry*100)}¢ → "
        f"<b>Exit:</b> {round(exit_price*100)}¢\n"
        f"<b>Current {asset}:</b> ${current_asset_price:,.0f}\n"
        f"<b>Target:</b> ${pos['target_price']:,.0f} "
        f"({'ABOVE' if pos['direction'] == 'ABOVE' else 'BELOW'})\n"
    )
    return msg


# ─────────────────────────────────────────────────────────────
# Position monitoring
# ─────────────────────────────────────────────────────────────

async def monitor_positions(pool, session):
    """Check all open positions for TP/SL/trailing stop."""
    async with pool.acquire() as conn:
        positions = await get_open_proximity_positions(conn)

    if not positions:
        return

    for pos in positions:
        try:
            market_id = pos["market_id"]
            entry     = pos["entry_price"]
            asset     = pos["asset"]

            # Get current market price
            url = f"{GAMMA_BASE}?id={market_id}"
            async with session.get(
                url, timeout=aiohttp.ClientTimeout(total=10)
            ) as r:
                if r.status != 200:
                    continue
                markets = await r.json()
                if not markets:
                    continue
                market = markets[0]

            outcomes = market.get("outcomePrices", "[]")
            if isinstance(outcomes, str):
                outcomes = json.loads(outcomes)
            if not outcomes:
                continue
            current_price = float(outcomes[0])

            return_pct = round((current_price - entry) / entry * 100, 2)
            new_peak   = max(pos["peak_price"] or entry, current_price)

            # Get current asset price for exit alerts
            current_asset_price = await get_asset_price(session, asset) or 0

            # Update peak
            async with pool.acquire() as conn:
                await update_proximity_peak(conn, pos["id"], current_price)

            peak_return = round((new_peak - entry) / entry * 100, 2)

            exit_reason = None
            outcome     = None

            # Check resolution
            if market.get("closed") or market.get("resolved"):
                exit_reason = "RESOLVED"
                if current_price >= 0.99:
                    outcome = "FULL_WIN"
                elif current_price <= 0.01:
                    outcome = "LOSS"
                else:
                    outcome = "PARTIAL_WIN" if return_pct > 0 else "LOSS"

            # Take profit
            elif return_pct >= TAKE_PROFIT_PCT:
                exit_reason = "TAKE_PROFIT"
                outcome     = "PARTIAL_WIN"

            # Stop loss
            elif return_pct <= STOP_LOSS_PCT:
                exit_reason = "STOP_LOSS"
                outcome     = "LOSS"

            # Trailing stop — tight for spike catching
            elif peak_return >= TRAILING_ACTIVATE:
                trailing_level = peak_return - TRAILING_PCT
                if return_pct <= trailing_level:
                    exit_reason = "TRAILING_STOP"
                    outcome = "PARTIAL_WIN" if return_pct > 0 else "LOSS"
                    log.info(
                        "Trailing stop: peak=%.1f%% current=%.1f%% "
                        "stop_level=%.1f%%",
                        peak_return, return_pct, trailing_level
                    )

            if exit_reason:
                async with pool.acquire() as conn:
                    await close_proximity_position(
                        conn, pos["id"], current_price,
                        exit_reason, outcome
                    )

                exit_msg = format_exit_alert(
                    dict(pos), current_price, exit_reason,
                    current_asset_price
                )
                await send_telegram(exit_msg)
                log.info("Position closed: %s | %s | %.1f%%",
                         pos["question"][:50], exit_reason, return_pct)

        except Exception as e:
            log.error("Monitor error for market %s: %s",
                      pos.get("market_id"), e)


# ─────────────────────────────────────────────────────────────
# Main scan loop
# ─────────────────────────────────────────────────────────────

async def scan_once(pool, session, alerted_markets: set):
    """Run one scan cycle — find new proximity opportunities."""
    markets = await fetch_price_target_markets(session)
    new_alerts = 0

    for raw_market in markets:
        parsed = parse_market_data(raw_market)
        if not parsed:
            continue
        log.info("  Candidate: %s | %s $%,.0f %s | %.0fc | prox=checking",
                 parsed["asset"], parsed["direction"],
                 parsed["target_price"], parsed["question"][:40],
                 parsed["yes_price"]*100)

        market_id = parsed["market_id"]
        if market_id in alerted_markets:
            continue

        asset        = parsed["asset"]
        target_price = parsed["target_price"]
        direction    = parsed["direction"]

        # Get current asset price
        current_price = await get_asset_price(session, asset)
        if not current_price:
            log.warning("  Could not get %s price from Binance", asset)
            continue
        log.info("    Binance %s: $%,.0f | target: $%,.0f | proximity: %.1f%%",
                 asset, current_price, target_price,
                 abs(current_price - target_price) / target_price * 100)

        # Calculate proximity
        prox = calculate_proximity(current_price, target_price, direction)

        # Skip if already past target (different trade) or too far
        if prox["already_past"]:
            log.info("    → SKIP: already past target (current=${:,.0f} target=${:,.0f})".format(
                current_price, target_price))
            continue
        if prox["proximity_pct"] > MAX_PROXIMITY:
            log.info("    → SKIP: too far (%.1f%% > %.1f%% max)",
                     prox["proximity_pct"], MAX_PROXIMITY)
            continue
        if prox["proximity_pct"] < MIN_PROXIMITY:
            log.info("    → SKIP: already at target (%.1f%% < %.1f%% min)",
                     prox["proximity_pct"], MIN_PROXIMITY)
            continue

        # Log and alert
        log.info(
            "PROXIMITY: %s target=$%,.0f %s | current=$%,.0f | %.1f%% away | "
            "market=%.0fc | %s",
            asset, target_price, direction, current_price,
            prox["proximity_pct"], parsed["yes_price"] * 100,
            parsed["question"][:50]
        )

        async with pool.acquire() as conn:
            alert_id = await log_proximity_alert(
                conn,
                market_id    = market_id,
                question     = parsed["question"],
                asset        = asset,
                target_price = target_price,
                direction    = direction,
                entry_price  = parsed["yes_price"],
                proximity_pct = prox["proximity_pct"],
                days_to_resolution = parsed.get("days_to_res"),
                notes        = f"proximity={prox['proximity_pct']:.1f}% "
                               f"asset_price={current_price:,.0f}",
            )

        if alert_id:
            alerted_markets.add(market_id)
            new_alerts += 1

            msg = format_proximity_alert(parsed, prox,
                                          current_price, alert_id)
            await send_telegram(msg)

    if new_alerts:
        log.info("Scan complete: %d new proximity alerts", new_alerts)
    else:
        log.info("Scan complete: no new proximity opportunities found")


async def send_daily_summary(pool):
    """Send daily performance summary."""
    async with pool.acquire() as conn:
        stats = await get_proximity_stats(conn)
        total = await conn.fetchval(
            "SELECT COUNT(*) FROM proximity_alerts WHERE exit_reason IS NOT NULL"
        )
        open_pos = await conn.fetchval(
            "SELECT COUNT(*) FROM proximity_alerts WHERE exit_reason IS NULL"
        )
        wins = await conn.fetchval(
            "SELECT COUNT(*) FROM proximity_alerts "
            "WHERE exit_reason IS NOT NULL AND profitable = TRUE"
        )

    wr = round((wins or 0) / total * 100, 1) if total else 0

    lines = [
        "📊 <b>Proximity Bot Daily Summary</b>\n",
        f"Open positions: {open_pos}",
        f"Total resolved: {total}",
        f"Win rate: {wr}% ({wins}W / {total - (wins or 0)}L)",
        "",
        "<b>By Asset:</b>",
    ]
    for row in stats:
        lines.append(
            f"  {row['asset']} {row['direction']}: "
            f"{row['avg_exit']:+.1f}% avg | {row['total']} trades"
        )

    await send_telegram("\n".join(lines))


# ─────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────

async def main():
    if not DATABASE_URL:
        log.error("DATABASE_URL not set")
        return

    log.info("=" * 55)
    log.info("Proximity Bot v1 Starting")
    log.info("Proximity window: %.1f%% - %.1f%%",
             MIN_PROXIMITY, MAX_PROXIMITY)
    log.info("Sweet spot: %.1f%% - %.1f%%",
             SWEET_SPOT_MIN, SWEET_SPOT_MAX)
    log.info("TP: +%.0f%%  SL: %.0f%%  Trail: -%.0f%% from peak",
             TAKE_PROFIT_PCT, STOP_LOSS_PCT, TRAILING_PCT)
    log.info("=" * 55)

    pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=5)

    async with pool.acquire() as conn:
        await init_proximity_db(conn)

    await send_telegram(
        "🎯 <b>Proximity Bot Started</b>\n\n"
        f"Scanning for BTC/ETH/SOL price target markets\n"
        f"Entry zone: {MIN_PROXIMITY:.1f}–{MAX_PROXIMITY:.1f}% from target\n"
        f"TP: +{TAKE_PROFIT_PCT:.0f}% | SL: {STOP_LOSS_PCT:.0f}% | "
        f"Trail: -{TRAILING_PCT:.0f}% from peak"
    )

    alerted_markets: set = set()
    scan_count      = 0
    last_summary    = now()

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "Accept":     "application/json",
    }

    async with aiohttp.ClientSession(headers=headers) as session:
        while True:
            try:
                # Full scan every SCAN_INTERVAL
                if scan_count % (SCAN_INTERVAL // MONITOR_INTERVAL) == 0:
                    await scan_once(pool, session, alerted_markets)
                    scan_count = 0

                # Monitor open positions every MONITOR_INTERVAL
                await monitor_positions(pool, session)

                # Daily summary
                hours_since = (now() - last_summary).total_seconds() / 3600
                if hours_since >= 24:
                    await send_daily_summary(pool)
                    last_summary = now()

                scan_count += 1
                await asyncio.sleep(MONITOR_INTERVAL)

            except KeyboardInterrupt:
                log.info("Interrupted — shutting down")
                break
            except Exception as e:
                log.error("Scan error: %s", e)
                await asyncio.sleep(30)

    await pool.close()
    log.info("Proximity bot stopped")


if __name__ == "__main__":
    asyncio.run(main())