"""Tests for futures underlier resolution and quote selection."""

from datetime import date
from unittest.mock import MagicMock, patch

from adjustments.futures_underlier import (
    FUTURES_RISK_FREE_RATE,
    future_price_from_quote,
    resolve_future,
)
from adjustments.option_chain_store import OptionChainStore, _bs_greeks, _implied_vol


def test_future_price_liquid_uses_ltp():
    price, source = future_price_from_quote(bid=100.0, ask=101.0, ltp=100.5)
    assert source == "ltp"
    assert price == 100.5


def test_future_price_outside_book_uses_mid():
    price, source = future_price_from_quote(bid=100.0, ask=101.0, ltp=99.0)
    assert source == "mid"
    assert price == 100.5


def test_future_price_missing_ltp_uses_mid():
    price, source = future_price_from_quote(bid=100.0, ask=102.0, ltp=0.0)
    assert source == "mid"
    assert price == 101.0


def test_future_price_ltp_fallback_without_book():
    price, source = future_price_from_quote(bid=0.0, ask=0.0, ltp=250.0)
    assert source == "ltp_fallback"
    assert price == 250.0


def test_future_price_none_when_empty():
    price, source = future_price_from_quote(0, 0, 0)
    assert price is None
    assert source == "none"


def test_resolve_future_match_kinds():
    futs = [
        {"name": "NIFTY", "instrument_token": 1, "tradingsymbol": "NIFTY26JULFUT",
         "expiry": date(2026, 7, 30)},
        {"name": "NIFTY", "instrument_token": 2, "tradingsymbol": "NIFTY26AUGFUT",
         "expiry": date(2026, 8, 27)},
        {"name": "NIFTY", "instrument_token": 3, "tradingsymbol": "NIFTY26JUNFUT",
         "expiry": date(2026, 6, 25)},
    ]
    with patch("adjustments.futures_underlier._cached_futs", return_value=futs):
        exact = resolve_future("NIFTY", date(2026, 7, 30))
        assert exact is not None
        assert exact["match_kind"] == "exact"
        assert exact["instrument_token"] == 1

        nxt = resolve_future("NIFTY", date(2026, 7, 22))
        assert nxt is not None
        assert nxt["match_kind"] == "next"
        assert nxt["instrument_token"] == 1  # nearest on/after weekly expiry

        # Option expiry after all listed futures → fallback to latest
        fb = resolve_future("NIFTY", date(2026, 12, 1))
        assert fb is not None
        assert fb["match_kind"] == "fallback"
        assert fb["instrument_token"] == 2


def test_shared_iv_ce_pe_delta_sum_near_one():
    """With shared IV and r=0, |Δ_CE| + |Δ_PE| ≈ 1."""
    F = 24500.0
    K = 24500.0
    t = 7 / 365.0
    r = FUTURES_RISK_FREE_RATE
    # Synthetic ATM mids with put-call parity-ish prices at r=0
    ce_mid = 180.0
    pe_mid = 180.0
    iv_c = _implied_vol("c", ce_mid, F, K, t, r)
    iv_p = _implied_vol("p", pe_mid, F, K, t, r)
    assert iv_c and iv_p
    shared = (iv_c + iv_p) / 2.0
    g_c = _bs_greeks("c", F, K, t, r, shared)
    g_p = _bs_greeks("p", F, K, t, r, shared)
    assert g_c and g_p
    abs_sum = abs(g_c["delta"]) + abs(g_p["delta"])
    assert abs(abs_sum - 1.0) < 1e-6


def test_option_chain_store_update_greeks_uses_futures_no_dividend():
    store = OptionChainStore()
    store.load([
        {
            "zerodha_instrument_token": 101,
            "underlying_symbol": "NIFTY",
            "strike": 24500,
            "option_type": "CE",
            "expiry": "2026-07-30",
            "lot_size": 65,
            "zerodha_tradingsymbol": "NIFTY26JUL24500CE",
            "greeks_calculated_by": "AUTO",
        },
        {
            "zerodha_instrument_token": 102,
            "underlying_symbol": "NIFTY",
            "strike": 24500,
            "option_type": "PE",
            "expiry": "2026-07-30",
            "lot_size": 65,
            "zerodha_tradingsymbol": "NIFTY26JUL24500PE",
            "greeks_calculated_by": "AUTO",
        },
    ])

    fut_meta = {
        "name": "NIFTY",
        "instrument_token": 999,
        "tradingsymbol": "NIFTY26JULFUT",
        "expiry": date(2026, 7, 30),
        "match_kind": "exact",
    }

    def fake_tick(_r, tok):
        if int(tok) == 101:
            return {"bid_price": 180, "ask_price": 182, "last_price": 181}
        if int(tok) == 102:
            return {"bid_price": 178, "ask_price": 180, "last_price": 179}
        return None

    with patch("adjustments.option_chain_store.refresh_nfo_futures", return_value=1):
        with patch(
            "adjustments.option_chain_store.get_future_price_for_option",
            return_value=(24500.0, fut_meta, "ltp"),
        ):
            with patch(
                "adjustments.option_chain_store.fetch_tick_by_token",
                side_effect=fake_tick,
            ):
                with patch(
                    "adjustments.option_chain_store.get_future_price",
                    return_value=(24500.0, "ltp"),
                ):
                    updated = store.update_greeks(MagicMock(), {"api_key": "k", "access_token": "t"})

    assert updated == 2
    ce = store.get_chain_by_token(101)
    pe = store.get_chain_by_token(102)
    assert ce["delta"] is not None and pe["delta"] is not None
    assert abs(abs(ce["delta"]) + abs(pe["delta"]) - 1.0) < 0.02
    assert ce["computed_at_spot"] == 24500.0
