#!/usr/bin/env python3
"""
proxy_db.py — Async SQLite database layer for Proxy IP Checker.

Schema (table ``proxies``):
  ip           TEXT  – proxy IP address
  port         INT   – proxy port
  proxy_type   TEXT  – http / https / socks4 / socks4a / socks5 / socks5h
  status       TEXT  – "online" or "fallen"
  country_code TEXT  – 2-letter ISO country code
  country      TEXT  – full country name
  city         TEXT  – city name
  isp          TEXT  – ISP / org name
  anonymity    TEXT  – Elite / Anonymous / Unknown
  response_ms  REAL  – last response time in milliseconds
  first_seen   TEXT  – ISO-8601 UTC datetime of first check
  last_checked TEXT  – ISO-8601 UTC datetime of most recent check
  last_online  TEXT  – ISO-8601 UTC datetime of last successful check

Primary key: (ip, port, proxy_type)
"""

import asyncio
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

# ---------------------------------------------------------------------------
# Zero-setup: auto-install aiosqlite if missing
# ---------------------------------------------------------------------------

try:
    import aiosqlite
    _AIOSQLITE_AVAILABLE = True
except ImportError:
    try:
        subprocess.check_call(
            [sys.executable, "-m", "pip", "install", "--quiet", "aiosqlite>=0.19.0"]
        )
        import aiosqlite  # noqa: F811
        _AIOSQLITE_AVAILABLE = True
    except Exception:
        _AIOSQLITE_AVAILABLE = False

DB_PATH = Path("proxy_lists") / "proxies.db"

# ---------------------------------------------------------------------------
# Shared persistent connection (eliminates "database is locked" under load)
#
# All async writes go through _db_conn serialised by _db_write_lock.
# WAL journal mode allows concurrent reads from other connections (e.g. the
# web server) without blocking or being blocked by these writes.
# ---------------------------------------------------------------------------

_db_conn: Optional[Any] = None       # aiosqlite.Connection, set by init_db()
_db_write_lock: Optional[asyncio.Lock] = None  # created lazily in init_db()

_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS proxies (
    ip           TEXT    NOT NULL,
    port         INTEGER NOT NULL,
    proxy_type   TEXT    NOT NULL,
    status       TEXT    NOT NULL DEFAULT 'online',
    country_code TEXT    NOT NULL DEFAULT '',
    country      TEXT    NOT NULL DEFAULT '',
    city         TEXT    NOT NULL DEFAULT '',
    isp          TEXT    NOT NULL DEFAULT '',
    anonymity    TEXT    NOT NULL DEFAULT '',
    response_ms  REAL    NOT NULL DEFAULT 0,
    first_seen   TEXT    NOT NULL,
    last_checked TEXT    NOT NULL,
    last_online  TEXT    NOT NULL DEFAULT '',
    PRIMARY KEY (ip, port, proxy_type)
);
"""

_CREATE_IDX_STATUS = (
    "CREATE INDEX IF NOT EXISTS idx_status ON proxies(status);"
)
_CREATE_IDX_TYPE = (
    "CREATE INDEX IF NOT EXISTS idx_type ON proxies(proxy_type);"
)

def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _check_dep() -> None:
    if not _AIOSQLITE_AVAILABLE:
        raise RuntimeError(
            "aiosqlite is required for database support. "
            "Install it with:  pip install aiosqlite"
        )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

async def init_db(path: Path = DB_PATH) -> None:
    """Create the database file and tables, and open the shared connection."""
    global _db_conn, _db_write_lock
    _check_dep()
    path.parent.mkdir(parents=True, exist_ok=True)

    # Create the lock once per event-loop lifetime.
    if _db_write_lock is None:
        _db_write_lock = asyncio.Lock()

    # Open a single persistent connection used for all writes.
    if _db_conn is None:
        _db_conn = await aiosqlite.connect(str(path))
        # WAL mode: readers never block writers, writers never block readers.
        await _db_conn.execute("PRAGMA journal_mode=WAL")
        # Wait up to 30 s before giving up on a locked write (safety net).
        await _db_conn.execute("PRAGMA busy_timeout=30000")
        await _db_conn.execute(_CREATE_TABLE)
        await _db_conn.execute(_CREATE_IDX_STATUS)
        await _db_conn.execute(_CREATE_IDX_TYPE)
        await _db_conn.commit()


async def close_db() -> None:
    """Close the shared persistent connection gracefully."""
    global _db_conn
    if _db_conn is not None:
        try:
            await _db_conn.close()
        except Exception:
            pass
        _db_conn = None


async def upsert_proxy(
    ip: str,
    port: int,
    proxy_type: str,
    status: str,                 # "online" or "fallen"
    country_code: str = "",
    country: str = "",
    city: str = "",
    isp: str = "",
    anonymity: str = "",
    response_ms: float = 0.0,
    path: Path = DB_PATH,        # kept for API compatibility; ignored when shared conn is open
) -> None:
    """
    Insert a new proxy row or update the existing one.

    All writes are serialised through the shared connection opened by
    init_db() to prevent "database is locked" errors under heavy concurrency.

    - ``first_seen`` is only written on INSERT; it is never updated.
    - ``last_online`` is updated only when ``status == "online"``.
    """
    _check_dep()
    if _db_conn is None or _db_write_lock is None:
        return  # init_db() was not called; silently skip
    now = _now()
    last_online = now if status == "online" else ""

    async with _db_write_lock:
        await _db_conn.execute(
            """
            INSERT INTO proxies
                (ip, port, proxy_type, status, country_code, country, city,
                 isp, anonymity, response_ms, first_seen, last_checked, last_online)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(ip, port, proxy_type) DO UPDATE SET
                status       = excluded.status,
                country_code = CASE WHEN excluded.country_code != ''
                                    THEN excluded.country_code
                                    ELSE country_code END,
                country      = CASE WHEN excluded.country != ''
                                    THEN excluded.country
                                    ELSE country END,
                city         = CASE WHEN excluded.city != ''
                                    THEN excluded.city
                                    ELSE city END,
                isp          = CASE WHEN excluded.isp != ''
                                    THEN excluded.isp
                                    ELSE isp END,
                anonymity    = CASE WHEN excluded.anonymity != ''
                                    THEN excluded.anonymity
                                    ELSE anonymity END,
                response_ms  = CASE WHEN excluded.response_ms > 0
                                    THEN excluded.response_ms
                                    ELSE response_ms END,
                last_checked = excluded.last_checked,
                last_online  = CASE WHEN excluded.status = 'online'
                                    THEN excluded.last_checked
                                    ELSE last_online END
            """,
            (
                ip, port, proxy_type, status,
                country_code, country, city, isp, anonymity, response_ms,
                now, now, last_online,
            ),
        )
        await _db_conn.commit()


async def get_all_proxies(
    status: Optional[str] = None,
    proxy_type: Optional[str] = None,
    limit: int = 5000,
    path: Path = DB_PATH,
) -> List[Dict[str, Any]]:
    """
    Return proxy rows as a list of dicts.
    Optionally filter by *status* ("online" / "fallen") and/or *proxy_type*.
    Results are ordered: online first, then by last_checked DESC.
    """
    _check_dep()
    if not path.exists():
        return []

    conditions: List[str] = []
    params: List[Any] = []
    if status:
        conditions.append("status = ?")
        params.append(status)
    if proxy_type:
        conditions.append("proxy_type = ?")
        params.append(proxy_type)

    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
    params.append(limit)

    async with aiosqlite.connect(path) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            f"""
            SELECT ip, port, proxy_type, status, country_code, country,
                   city, isp, anonymity, response_ms,
                   first_seen, last_checked, last_online
            FROM proxies
            {where}
            ORDER BY
                CASE status WHEN 'online' THEN 0 ELSE 1 END,
                last_checked DESC
            LIMIT ?
            """,
            params,
        )
        rows = await cursor.fetchall()
    return [dict(r) for r in rows]


def get_all_proxies_sync(
    status: Optional[str] = None,
    proxy_type: Optional[str] = None,
    limit: int = 5000,
    path: Path = DB_PATH,
) -> List[Dict[str, Any]]:
    """Synchronous wrapper for use in Flask request handlers."""
    import sqlite3

    if not path.exists():
        return []

    conditions: List[str] = []
    params: List[Any] = []
    if status:
        conditions.append("status = ?")
        params.append(status)
    if proxy_type:
        conditions.append("proxy_type = ?")
        params.append(proxy_type)

    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
    params.append(limit)

    con = sqlite3.connect(path)
    con.row_factory = sqlite3.Row
    try:
        cur = con.execute(
            f"""
            SELECT ip, port, proxy_type, status, country_code, country,
                   city, isp, anonymity, response_ms,
                   first_seen, last_checked, last_online
            FROM proxies
            {where}
            ORDER BY
                CASE status WHEN 'online' THEN 0 ELSE 1 END,
                last_checked DESC
            LIMIT ?
            """,
            params,
        )
        return [dict(r) for r in cur.fetchall()]
    finally:
        con.close()


def get_stats_sync(path: Path = DB_PATH) -> Dict[str, Any]:
    """
    Return aggregate statistics.

    Returns a dict with keys:
      total, online, fallen, success_rate,
      by_type: {type: {total, online, fallen, success_rate}}
    """
    import sqlite3

    empty: Dict[str, Any] = {
        "total": 0, "online": 0, "fallen": 0, "success_rate": 0.0,
        "by_type": {},
    }
    if not path.exists():
        return empty

    con = sqlite3.connect(path)
    try:
        cur = con.execute(
            """
            SELECT proxy_type, status, COUNT(*) AS cnt
            FROM proxies
            GROUP BY proxy_type, status
            """
        )
        rows = cur.fetchall()
    finally:
        con.close()

    by_type: Dict[str, Dict[str, Any]] = {}
    total_online = 0
    total_fallen = 0

    for ptype, status, cnt in rows:
        if ptype not in by_type:
            by_type[ptype] = {"total": 0, "online": 0, "fallen": 0, "success_rate": 0.0}
        by_type[ptype]["total"] += cnt
        by_type[ptype][status]   = cnt
        if status == "online":
            total_online += cnt
        else:
            total_fallen += cnt

    # Compute per-type success rate
    for ptype, d in by_type.items():
        t = d["total"]
        d["success_rate"] = round(d["online"] / t * 100, 1) if t > 0 else 0.0

    total = total_online + total_fallen
    success_rate = round(total_online / total * 100, 1) if total > 0 else 0.0

    return {
        "total":        total,
        "online":       total_online,
        "fallen":       total_fallen,
        "success_rate": success_rate,
        "by_type":      by_type,
    }
