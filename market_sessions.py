"""
IST derivative session helpers for the stream worker.

Prefers ``GET /internal/stream/session`` from the backend (holidays + evening-only).
Falls back to local Mon–Fri clock rules when the API is unreachable.

NFO/BFO: 9:15 AM–3:45 PM IST.
MCX/NCO: 9:00 AM–11:30 PM IST (17:30 start on evening-only days — backend only).
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta, time as dt_time
from zoneinfo import ZoneInfo

IST = ZoneInfo("Asia/Kolkata")

NFO_BFO_SESSION_START = dt_time(9, 15)
NFO_BFO_SESSION_END = dt_time(15, 45)
MCX_SESSION_START = dt_time(9, 0)
MCX_SESSION_END = dt_time(23, 30)
MCX_EVENING_SESSION_START = dt_time(17, 30)

ALL_DERIVATIVE_EXCHANGES = frozenset({"NFO", "BFO", "MCX", "NCO"})
MCX_ONLY_EXCHANGES = frozenset({"MCX"})
COMMODITY_SESSION_EXCHANGES = frozenset({"MCX", "NCO"})
EQUITY_SESSION_EXCHANGES = frozenset({"NFO", "BFO"})

_SESSION_BOUNDARY_TIMES: tuple[dt_time, ...] = (
    dt_time(0, 0),
    MCX_SESSION_START,
    NFO_BFO_SESSION_START,
    dt_time(15, 46),
    MCX_EVENING_SESSION_START,
    dt_time(23, 31),
)

logger = logging.getLogger(__name__)

# Cache of backend session snapshot.
_cached_open: frozenset[str] | None = None
_cached_boundary: datetime | None = None
_cached_at_mono: float = 0.0
_CACHE_TTL_S = 30.0


def _local_derivative_exchanges_open(now: datetime | None = None) -> frozenset[str]:
    """Mon–Fri clock only (no holiday table). Used when backend is unreachable."""
    now = now or datetime.now(IST)
    if now.tzinfo is None:
        now = now.replace(tzinfo=IST)
    else:
        now = now.astimezone(IST)
    if now.weekday() >= 5:
        return frozenset()

    current = now.time()
    if NFO_BFO_SESSION_START <= current <= NFO_BFO_SESSION_END:
        return ALL_DERIVATIVE_EXCHANGES
    if MCX_SESSION_START <= current <= MCX_SESSION_END:
        return COMMODITY_SESSION_EXCHANGES
    return frozenset()


def _local_next_session_boundary(now: datetime | None = None) -> datetime:
    now = now or datetime.now(IST)
    if now.tzinfo is None:
        now = now.replace(tzinfo=IST)
    else:
        now = now.astimezone(IST)

    current = _local_derivative_exchanges_open(now)
    for day_offset in range(0, 8):
        day = now.date() + timedelta(days=day_offset)
        for bt in _SESSION_BOUNDARY_TIMES:
            dt = datetime.combine(day, bt, tzinfo=IST)
            if dt <= now:
                continue
            if _local_derivative_exchanges_open(dt) != current:
                return dt
    return now + timedelta(hours=6)


def set_cached_session(
    open_exchanges: frozenset[str] | list[str] | None,
    *,
    next_boundary_at: datetime | str | None = None,
) -> None:
    """Update the in-process session cache (called after stream/config or /session)."""
    global _cached_open, _cached_boundary, _cached_at_mono
    if open_exchanges is None:
        return
    _cached_open = frozenset(str(x).strip().upper() for x in open_exchanges if x)
    _cached_at_mono = time.monotonic()
    if next_boundary_at is None:
        return
    if isinstance(next_boundary_at, datetime):
        _cached_boundary = next_boundary_at
        return
    raw = str(next_boundary_at).strip()
    if not raw:
        return
    try:
        _cached_boundary = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        logger.debug("market_sessions: bad next_boundary_at=%r", raw)


def refresh_session_from_backend() -> frozenset[str]:
    """Fetch ``/internal/stream/session`` and refresh the cache."""
    from client import backend_api

    data = backend_api.get_stream_session()
    if data.get("_status", 200) >= 400 or "error" in data:
        raise RuntimeError(data.get("error") or data.get("detail") or "session fetch failed")

    open_exchanges = data.get("active_derivative_exchanges") or []
    set_cached_session(
        open_exchanges,
        next_boundary_at=data.get("next_boundary_at"),
    )
    assert _cached_open is not None
    return _cached_open


def derivative_exchanges_open(now: datetime | None = None) -> frozenset[str]:
    """
    Exchanges currently in session.

    Uses a short-lived backend cache when available; otherwise local clock fallback.
    ``now`` is only used for the local fallback (backend snapshot is “current”).
    """
    global _cached_open, _cached_at_mono
    age = time.monotonic() - _cached_at_mono
    if _cached_open is not None and age < _CACHE_TTL_S:
        return _cached_open

    try:
        return refresh_session_from_backend()
    except Exception as exc:
        logger.debug("market_sessions: backend session unavailable (%s); local fallback", exc)
        return _local_derivative_exchanges_open(now)


def next_session_boundary(now: datetime | None = None) -> datetime:
    """Next time the open-exchange set may change (backend cache or local)."""
    age = time.monotonic() - _cached_at_mono
    if _cached_boundary is not None and age < _CACHE_TTL_S:
        boundary = _cached_boundary
        if boundary.tzinfo is None:
            boundary = boundary.replace(tzinfo=IST)
        now_ist = now or datetime.now(IST)
        if now_ist.tzinfo is None:
            now_ist = now_ist.replace(tzinfo=IST)
        else:
            now_ist = now_ist.astimezone(IST)
        if boundary > now_ist:
            return boundary.astimezone(IST)

    try:
        refresh_session_from_backend()
        if _cached_boundary is not None:
            boundary = _cached_boundary
            if boundary.tzinfo is None:
                boundary = boundary.replace(tzinfo=IST)
            return boundary.astimezone(IST)
    except Exception as exc:
        logger.debug("market_sessions: boundary from backend failed (%s)", exc)

    return _local_next_session_boundary(now)


def seconds_until_next_boundary(
    now: datetime | None = None,
    *,
    max_sleep_s: float = 300.0,
    min_sleep_s: float = 5.0,
) -> float:
    """Sleep duration until the next session boundary, clamped for the idle loop."""
    now_ist = now or datetime.now(IST)
    if now_ist.tzinfo is None:
        now_ist = now_ist.replace(tzinfo=IST)
    else:
        now_ist = now_ist.astimezone(IST)
    boundary = next_session_boundary(now_ist)
    delay = (boundary - now_ist).total_seconds()
    if delay < min_sleep_s:
        return min_sleep_s
    return min(delay, max_sleep_s)


def position_in_derivative_session(pos: dict, active_exchanges: frozenset[str]) -> bool:
    if not active_exchanges:
        return False
    exchange = (pos.get("exchange") or "NFO").strip().upper()
    return exchange in active_exchanges
