"""
Proximity Bot — Database Module
================================
Manages the proximity_alerts table independently from
the main bot's alerts table. Clean data separation.
"""

import logging
from datetime import datetime

log = logging.getLogger(__name__)

BOT_VERSION = "proximity_v1"


def now():
    return datetime.utcnow()


async def init_proximity_db(conn):
    """Create proximity_alerts table if not exists."""
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS proximity_alerts (
            id               SERIAL PRIMARY KEY,
            market_id        TEXT NOT NULL,
            question         TEXT NOT NULL,
            asset            TEXT NOT NULL,
            target_price     FLOAT NOT NULL,
            direction        TEXT NOT NULL,
            entry_price      FLOAT NOT NULL,
            proximity_pct    FLOAT NOT NULL,
            market_price_at_alert FLOAT,
            alerted_at       TIMESTAMP NOT NULL,
            peak_price       FLOAT,
            peak_return_pct  FLOAT,
            exit_price       FLOAT,
            exit_return_pct  FLOAT,
            exit_reason      TEXT,
            outcome          TEXT,
            profitable       BOOLEAN,
            hold_duration_hours FLOAT,
            days_to_resolution  FLOAT,
            resolved_at      TIMESTAMP,
            bot_version      TEXT DEFAULT 'proximity_v1',
            notes            TEXT
        )
    """)

    # Safe migrations
    for col, typedef in [
        ("peak_price",            "FLOAT"),
        ("peak_return_pct",       "FLOAT"),
        ("exit_price",            "FLOAT"),
        ("exit_return_pct",       "FLOAT"),
        ("exit_reason",           "TEXT"),
        ("outcome",               "TEXT"),
        ("profitable",            "BOOLEAN"),
        ("hold_duration_hours",   "FLOAT"),
        ("resolved_at",           "TIMESTAMP"),
        ("bot_version",           "TEXT"),
        ("notes",                 "TEXT"),
        ("market_price_at_alert", "FLOAT"),
    ]:
        await conn.execute(
            f"ALTER TABLE proximity_alerts "
            f"ADD COLUMN IF NOT EXISTS {col} {typedef}"
        )

    log.info("proximity_alerts table ready")


async def log_proximity_alert(conn, market_id, question, asset,
                               target_price, direction, entry_price,
                               proximity_pct, days_to_resolution=None,
                               notes=None):
    """Log a new proximity alert."""
    # Check if already alerted for this market
    existing = await conn.fetchval(
        "SELECT id FROM proximity_alerts WHERE market_id=$1 "
        "AND exit_reason IS NULL",
        market_id
    )
    if existing:
        return None  # Already have open position on this market

    alert_id = await conn.fetchval("""
        INSERT INTO proximity_alerts (
            market_id, question, asset, target_price, direction,
            entry_price, proximity_pct, market_price_at_alert,
            alerted_at, peak_price, days_to_resolution,
            bot_version, notes
        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
        RETURNING id
    """,
        market_id, question, asset, target_price, direction,
        entry_price, proximity_pct, entry_price,
        now(), entry_price, days_to_resolution,
        BOT_VERSION, notes
    )

    total = await conn.fetchval("SELECT COUNT(*) FROM proximity_alerts")
    log.info("Proximity alert logged #%d (total: %d)", alert_id, total)
    return alert_id


async def get_open_proximity_positions(conn):
    """Get all open proximity positions for monitoring."""
    return await conn.fetch("""
        SELECT * FROM proximity_alerts
        WHERE exit_reason IS NULL
          AND alerted_at < NOW() - INTERVAL '5 minutes'
        ORDER BY alerted_at ASC
    """)


async def close_proximity_position(conn, alert_id, exit_price,
                                    exit_reason, outcome):
    """Close a proximity position with exit details."""
    pos = await conn.fetchrow(
        "SELECT * FROM proximity_alerts WHERE id=$1", alert_id
    )
    if not pos:
        return

    entry        = pos["entry_price"]
    exit_ret     = round((exit_price - entry) / entry * 100, 2) if entry else 0
    peak         = pos["peak_price"] or entry
    peak_ret     = round((peak - entry) / entry * 100, 2) if entry else 0
    profitable   = exit_ret > 0
    hold_hours   = round(
        (now() - pos["alerted_at"]).total_seconds() / 3600, 1
    )

    await conn.execute("""
        UPDATE proximity_alerts
        SET exit_price       = $1,
            exit_return_pct  = $2,
            exit_reason      = $3,
            outcome          = $4,
            profitable       = $5,
            peak_return_pct  = $6,
            hold_duration_hours = $7,
            resolved_at      = $8
        WHERE id = $9
    """,
        exit_price, exit_ret, exit_reason, outcome,
        profitable, peak_ret, hold_hours, now(), alert_id
    )
    log.info("Proximity position closed: #%d | %s | exit=%.1f%% | %s",
             alert_id, exit_reason, exit_ret, outcome)


async def update_proximity_peak(conn, alert_id, current_price):
    """Update peak price if current price is higher."""
    await conn.execute("""
        UPDATE proximity_alerts
        SET peak_price = GREATEST(COALESCE(peak_price, entry_price), $1)
        WHERE id = $2
    """, current_price, alert_id)


async def get_proximity_stats(conn):
    """Get performance summary for proximity bot."""
    rows = await conn.fetch("""
        SELECT
            COUNT(*) as total,
            SUM(CASE WHEN profitable THEN 1 ELSE 0 END) as wins,
            ROUND(AVG(exit_return_pct)::numeric, 1) as avg_exit,
            ROUND(AVG(CASE WHEN profitable THEN exit_return_pct END)::numeric, 1) as avg_win,
            ROUND(AVG(CASE WHEN NOT profitable THEN exit_return_pct END)::numeric, 1) as avg_loss,
            ROUND(AVG(proximity_pct)::numeric, 1) as avg_proximity_at_entry,
            asset,
            direction
        FROM proximity_alerts
        WHERE exit_reason IS NOT NULL
        GROUP BY asset, direction
        ORDER BY total DESC
    """)
    return rows