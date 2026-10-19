"""Unit tests for worker market_sessions (local fallback + cache)."""

from __future__ import annotations

from datetime import datetime, timedelta
from unittest.mock import patch

from market_sessions import (
    ALL_DERIVATIVE_EXCHANGES,
    COMMODITY_SESSION_EXCHANGES,
    IST,
    _local_derivative_exchanges_open,
    _local_next_session_boundary,
    derivative_exchanges_open,
    next_session_boundary,
    position_in_derivative_session,
    seconds_until_next_boundary,
    set_cached_session,
)


def _ist(hour: int, minute: int, *, weekday: int = 0) -> datetime:
    # 2026-07-13 is a Monday
    base = datetime(2026, 7, 13, hour, minute, tzinfo=IST)
    return base + timedelta(days=weekday)


class TestLocalDerivativeExchangesOpen:
    def test_before_open_empty(self):
        assert _local_derivative_exchanges_open(_ist(8, 59)) == frozenset()

    def test_mcx_pre_nfo(self):
        assert (
            _local_derivative_exchanges_open(_ist(9, 0)) == COMMODITY_SESSION_EXCHANGES
        )

    def test_nfo_window_all(self):
        assert (
            _local_derivative_exchanges_open(_ist(10, 0)) == ALL_DERIVATIVE_EXCHANGES
        )

    def test_at_nfo_close_still_all(self):
        assert (
            _local_derivative_exchanges_open(_ist(15, 45)) == ALL_DERIVATIVE_EXCHANGES
        )

    def test_after_nfo_mcx_only(self):
        assert (
            _local_derivative_exchanges_open(_ist(15, 46)) == COMMODITY_SESSION_EXCHANGES
        )

    def test_weekend_empty(self):
        assert _local_derivative_exchanges_open(_ist(12, 0, weekday=5)) == frozenset()


class TestLocalNextBoundary:
    def test_before_0900(self):
        b = _local_next_session_boundary(_ist(8, 30))
        assert (b.hour, b.minute) == (9, 0)

    def test_during_mcx_only(self):
        b = _local_next_session_boundary(_ist(9, 5))
        assert (b.hour, b.minute) == (9, 15)

    def test_before_nfo_close(self):
        b = _local_next_session_boundary(_ist(15, 40))
        assert (b.hour, b.minute) == (15, 46)


class TestCachedSession:
    def test_cache_preferred_over_local(self):
        set_cached_session(["MCX", "NCO"])
        with patch(
            "market_sessions.refresh_session_from_backend",
            side_effect=AssertionError("should not call backend"),
        ):
            # Force fresh cache age by re-setting
            set_cached_session(["MCX"])
            assert derivative_exchanges_open(_ist(10, 0)) == frozenset({"MCX"})

    def test_fallback_when_backend_fails(self):
        set_cached_session([])  # empty + stale path
        # Expire cache
        import market_sessions as ms

        ms._cached_at_mono = 0.0
        ms._cached_open = None
        with patch(
            "market_sessions.refresh_session_from_backend",
            side_effect=RuntimeError("down"),
        ):
            assert derivative_exchanges_open(_ist(10, 0)) == ALL_DERIVATIVE_EXCHANGES

    def test_boundary_from_cache(self):
        boundary = _ist(9, 15)
        set_cached_session(["MCX", "NCO"], next_boundary_at=boundary.isoformat())
        assert next_session_boundary(_ist(9, 5)) == boundary

    def test_seconds_until_clamped(self):
        set_cached_session(
            ["MCX"],
            next_boundary_at=(_ist(9, 5) + timedelta(hours=2)).isoformat(),
        )
        delay = seconds_until_next_boundary(
            _ist(9, 5), max_sleep_s=60.0, min_sleep_s=5.0
        )
        assert delay == 60.0


class TestPositionFilter:
    def test_filters_by_exchange(self):
        open_ex = frozenset({"MCX", "NCO"})
        assert position_in_derivative_session({"exchange": "MCX"}, open_ex)
        assert not position_in_derivative_session({"exchange": "NFO"}, open_ex)
        assert not position_in_derivative_session({"exchange": "NFO"}, frozenset())
