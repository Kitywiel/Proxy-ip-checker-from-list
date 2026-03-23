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
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

try:
    import aiosqlite
    _AIOSQLITE_AVAILABLE = True
except ImportError:
    _AIOSQLITE_AVAILABLE = False

DB_PATH = Path("proxy_lists") / "proxies.db"

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

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

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
    """Create the database file and tables if they do not exist."""
    _check_dep()
    path.parent.mkdir(parents=True, exist_ok=True)
    async with aiosqlite.connect(path) as db:
        await db.execute(_CREATE_TABLE)
        await db.execute(_CREATE_IDX_STATUS)
        await db.execute(_CREATE_IDX_TYPE)
        await db.commit()


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
    path: Path = DB_PATH,
) -> None:
    """
    Insert a new proxy row or update the existing one.

    - ``first_seen`` is only written on INSERT; it is never updated.
    - ``last_online`` is updated only when ``status == "online"``.
    """
    _check_dep()
    now = _now()
    last_online = now if status == "online" else ""

    async with aiosqlite.connect(path) as db:
        # Try INSERT first (preserves first_seen).
        await db.execute(
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
        await db.commit()


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
      total, online, fallen,
      by_type: {type: {total, online, fallen}}
    """
    import sqlite3

    empty: Dict[str, Any] = {
        "total": 0, "online": 0, "fallen": 0, "by_type": {},
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

    by_type: Dict[str, Dict[str, int]] = {}
    total_online = 0
    total_fallen = 0

    for ptype, status, cnt in rows:
        if ptype not in by_type:
            by_type[ptype] = {"total": 0, "online": 0, "fallen": 0}
        by_type[ptype]["total"] += cnt
        by_type[ptype][status] = cnt
        if status == "online":
            total_online += cnt
        else:
            total_fallen += cnt

    return {
        "total": total_online + total_fallen,
        "online": total_online,
        "fallen": total_fallen,
        "by_type": by_type,
    }
