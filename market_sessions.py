"""
IST derivative session helpers for the stream worker.

Mirrors ``hedgium_backend.utils.market_sessions`` (no Django dependency).

NFO/BFO: 9:15 AM–3:30 PM IST.
MCX: 9:00 AM–11:30 PM IST.
"""

from __future__ import annotations

from datetime import datetime, time
from zoneinfo import ZoneInfo

IST = ZoneInfo("Asia/Kolkata")

NFO_BFO_SESSION_START = time(9, 15)
NFO_BFO_SESSION_END = time(15, 30)
MCX_SESSION_START = time(9, 0)
MCX_SESSION_END = time(23, 30)

ALL_DERIVATIVE_EXCHANGES = frozenset({"NFO", "BFO", "MCX"})
MCX_ONLY_EXCHANGES = frozenset({"MCX"})


def derivative_exchanges_open(now: datetime | None = None) -> frozenset[str]:
    """
    Return derivative exchanges currently in session (IST, Mon–Fri).

    - 09:15–15:30 → NFO, BFO, MCX
    - 09:00–09:15 and 15:30–23:30 → MCX only
    - otherwise → empty
    """
    now = now or datetime.now(IST)
    if now.weekday() >= 5:
        return frozenset()

    current = now.time()
    if NFO_BFO_SESSION_START <= current <= NFO_BFO_SESSION_END:
        return ALL_DERIVATIVE_EXCHANGES
    if MCX_SESSION_START <= current <= MCX_SESSION_END:
        return MCX_ONLY_EXCHANGES
    return frozenset()


def position_in_derivative_session(pos: dict, active_exchanges: frozenset[str]) -> bool:
    if not active_exchanges:
        return False
    exchange = (pos.get("exchange") or "NFO").strip().upper()
    return exchange in active_exchanges
