"""Tests for futures underlier resolution and quote selection."""

from datetime import date
from unittest.mock import MagicMock, patch

from adjustments.futures_underlier import (
    FUTURES_RISK_FREE_RATE,
    SPOT_INTEREST_RATE,
    future_price_from_quote,
    get_future_price_for_option,
    refresh_nfo_futures,
    resolve_future,
    spot_with_interest,
    synthetic_weekly_future_price,
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


def test_future_price_spot_last_resort():
    price, source = future_price_from_quote(bid=0.0, ask=0.0, ltp=0.0, spot=24480.0)
    assert source == "spot"
    assert price == 24480.0


def test_future_price_prefers_mid_over_spot():
    price, source = future_price_from_quote(bid=100.0, ask=102.0, ltp=0.0, spot=24480.0)
    assert source == "mid"
    assert price == 101.0


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


def test_resolve_future_sensex_uses_bfo_monthly():
    futs = [
        {"name": "SENSEX", "instrument_token": 9, "tradingsymbol": "SENSEX26SEPFUT",
         "expiry": date(2026, 9, 24), "exchange": "BFO"},
        {"name": "NIFTY", "instrument_token": 1, "tradingsymbol": "NIFTY26SEPFUT",
         "expiry": date(2026, 9, 24), "exchange": "NFO"},
    ]
    with patch("adjustments.futures_underlier._cached_futs", return_value=futs):
        weekly = resolve_future("SENSEX", date(2026, 9, 3))
        assert weekly is not None
        assert weekly["match_kind"] == "next"
        assert weekly["instrument_token"] == 9
        monthly = resolve_future("SENSEX", date(2026, 9, 24))
        assert monthly is not None
        assert monthly["match_kind"] == "exact"


def test_synthetic_weekly_future_price_example():
    # Monthly 200 above spot, 20 days out; weekly 5 days out → +50
    price = synthetic_weekly_future_price(24500.0, 24700.0, 5, 20)
    assert price == 24550.0


def test_synthetic_weekly_future_price_guards():
    assert synthetic_weekly_future_price(0.0, 24700.0, 5, 20) is None
    assert synthetic_weekly_future_price(24500.0, None, 5, 20) is None
    assert synthetic_weekly_future_price(24500.0, 0.0, 5, 20) is None
    # Monthly expiry day → listed monthly
    assert synthetic_weekly_future_price(24500.0, 24700.0, 0, 0) == 24700.0
    # Weekly expiry day → spot
    assert synthetic_weekly_future_price(24500.0, 24700.0, 0, 20) == 24500.0
    # Weekly beyond covering monthly → cap at monthly
    assert synthetic_weekly_future_price(24500.0, 24700.0, 25, 20) == 24700.0


def test_get_future_price_for_option_monthly_exact_not_synthetic():
    futs = [
        {"name": "NIFTY", "instrument_token": 2, "tradingsymbol": "NIFTY26AUGFUT",
         "expiry": date(2026, 8, 27)},
    ]
    with patch("adjustments.futures_underlier._cached_futs", return_value=futs):
        with patch("adjustments.futures_underlier._cash_spot_from_redis", return_value=24500.0):
            with patch(
                "adjustments.futures_underlier.get_future_price",
                return_value=(24700.0, "ltp"),
            ):
                price, fut, source = get_future_price_for_option(
                    MagicMock(), None, "NIFTY", date(2026, 8, 27),
                )
    assert source == "ltp"
    assert price == 24700.0
    assert fut is not None
    assert fut["match_kind"] == "exact"
    assert fut["instrument_token"] == 2


def test_get_future_price_for_option_weekly_uses_synthetic():
    futs = [
        {"name": "NIFTY", "instrument_token": 2, "tradingsymbol": "NIFTY26AUGFUT",
         "expiry": date(2026, 8, 21)},
    ]
    with patch("adjustments.futures_underlier._cached_futs", return_value=futs):
        with patch("adjustments.futures_underlier._cash_spot_from_redis", return_value=24500.0):
            with patch(
                "adjustments.futures_underlier.get_future_price",
                return_value=(24700.0, "ltp"),
            ):
                with patch(
                    "adjustments.futures_underlier._ist_today",
                    return_value=date(2026, 8, 1),
                ):
                    price, fut, source = get_future_price_for_option(
                        MagicMock(), None, "NIFTY", date(2026, 8, 6),
                    )
    assert source == "synthetic"
    assert price == 24550.0
    assert fut is not None
    assert fut["match_kind"] == "next"
    assert fut["instrument_token"] == 2
    assert fut["expiry"] == date(2026, 8, 21)


def test_get_future_price_for_option_weekly_between_two_futures():
    futs = [
        {"name": "NIFTY", "instrument_token": 1, "tradingsymbol": "NIFTY26AUGFUT",
         "expiry": date(2026, 8, 27)},
        {"name": "NIFTY", "instrument_token": 2, "tradingsymbol": "NIFTY26SEPFUT",
         "expiry": date(2026, 9, 24)},
    ]

    def fake_fut_price(_r, _creds, tok, spot=0.0):
        if int(tok) == 1:
            return 24700.0, "ltp"
        if int(tok) == 2:
            return 24900.0, "ltp"
        return None, "none"

    with patch("adjustments.futures_underlier._cached_futs", return_value=futs):
        with patch("adjustments.futures_underlier._cash_spot_from_redis", return_value=24500.0):
            with patch(
                "adjustments.futures_underlier.get_future_price",
                side_effect=fake_fut_price,
            ):
                with patch(
                    "adjustments.futures_underlier._ist_today",
                    return_value=date(2026, 8, 26),
                ):
                    price, fut, source = get_future_price_for_option(
                        MagicMock(), None, "NIFTY", date(2026, 9, 3),
                    )
    # F1=24700 (27 Aug), F2=24900 (24 Sep); weekly 3 Sep → 7/28 of the 200 spread
    assert source == "synthetic"
    assert fut is not None
    assert fut["instrument_token"] == 2
    assert price == 24700.0 + 200.0 * 7 / 28


def test_get_future_price_for_option_weekly_fallback_without_spot():
    futs = [
        {"name": "NIFTY", "instrument_token": 2, "tradingsymbol": "NIFTY26AUGFUT",
         "expiry": date(2026, 8, 21)},
    ]
    with patch("adjustments.futures_underlier._cached_futs", return_value=futs):
        with patch("adjustments.futures_underlier._cash_spot_from_redis", return_value=0.0):
            with patch(
                "adjustments.futures_underlier.get_future_price",
                return_value=(24700.0, "ltp"),
            ):
                price, fut, source = get_future_price_for_option(
                    MagicMock(), None, "NIFTY", date(2026, 8, 6),
                )
    assert source == "ltp"
    assert price == 24700.0
    assert fut is not None
    assert fut["match_kind"] == "next"


def test_spot_with_interest_formula():
    # 8 days at 6.5%
    price = spot_with_interest(77576.63, 8)
    assert price == 77576.63 * (1.0 + SPOT_INTEREST_RATE * 8 / 365.0)
    assert spot_with_interest(77576.63, 0) == 77576.63
    assert spot_with_interest(0.0, 8) is None


def test_get_future_price_for_option_spot_adds_interest():
    with patch("adjustments.futures_underlier._cached_futs", return_value=[]):
        with patch(
            "adjustments.futures_underlier._cash_spot_from_redis",
            return_value=77576.63,
        ):
            with patch(
                "adjustments.futures_underlier._ist_today",
                return_value=date(2026, 8, 26),
            ):
                price, fut, source = get_future_price_for_option(
                    MagicMock(), None, "SENSEX", date(2026, 9, 3),
                )
    assert fut is None
    assert source == "spot_interest"
    assert price == 77576.63 * (1.0 + SPOT_INTEREST_RATE * 8 / 365.0)


def test_get_future_price_for_option_listed_quote_spot_adds_interest():
    futs = [
        {"name": "NIFTY", "instrument_token": 2, "tradingsymbol": "NIFTY26AUGFUT",
         "expiry": date(2026, 8, 27)},
    ]
    with patch("adjustments.futures_underlier._cached_futs", return_value=futs):
        with patch("adjustments.futures_underlier._cash_spot_from_redis", return_value=24500.0):
            with patch(
                "adjustments.futures_underlier.get_future_price",
                return_value=(24500.0, "spot"),
            ):
                with patch(
                    "adjustments.futures_underlier._ist_today",
                    return_value=date(2026, 8, 20),
                ):
                    price, fut, source = get_future_price_for_option(
                        MagicMock(), None, "NIFTY", date(2026, 8, 27),
                    )
    assert source == "spot_interest"
    assert fut is not None
    assert price == 24500.0 * (1.0 + SPOT_INTEREST_RATE * 7 / 365.0)


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


def test_refresh_nfo_futures_loads_bfo_and_nfo():
    nfo_rows = [{
        "instrument_type": "FUT",
        "name": "NIFTY",
        "instrument_token": 1,
        "tradingsymbol": "NIFTY26SEPFUT",
        "expiry": date(2026, 9, 24),
    }]
    bfo_rows = [{
        "instrument_type": "FUT",
        "name": "SENSEX",
        "instrument_token": 9,
        "tradingsymbol": "SENSEX26SEPFUT",
        "expiry": date(2026, 9, 24),
    }]

    class FakeKite:
        def __init__(self, api_key):
            pass

        def set_access_token(self, token):
            pass

        def instruments(self, exchange):
            if exchange == "NFO":
                return nfo_rows
            if exchange == "BFO":
                return bfo_rows
            return []

    import adjustments.futures_underlier as fu

    with patch("kiteconnect.KiteConnect", FakeKite):
        try:
            n = refresh_nfo_futures("k", "t", force=True)
            assert n == 2
            sensex = resolve_future("SENSEX", date(2026, 9, 3))
            assert sensex is not None
            assert sensex["instrument_token"] == 9
            assert sensex["match_kind"] == "next"
            nifty = resolve_future("NIFTY", date(2026, 9, 24))
            assert nifty is not None
            assert nifty["match_kind"] == "exact"
        finally:
            with fu._lock:
                fu._nfo_futs = []
                fu._nfo_loaded_at = 0.0


def test_get_future_price_for_option_sensex_weekly_uses_synthetic():
    futs = [
        {"name": "SENSEX", "instrument_token": 9, "tradingsymbol": "SENSEX26SEPFUT",
         "expiry": date(2026, 9, 24), "exchange": "BFO"},
    ]
    with patch("adjustments.futures_underlier._cached_futs", return_value=futs):
        with patch("adjustments.futures_underlier._cash_spot_from_redis", return_value=77500.0):
            with patch(
                "adjustments.futures_underlier.get_future_price",
                return_value=(77700.0, "ltp"),
            ):
                with patch(
                    "adjustments.futures_underlier._ist_today",
                    return_value=date(2026, 8, 26),
                ):
                    price, fut, source = get_future_price_for_option(
                        MagicMock(), None, "SENSEX", date(2026, 9, 3),
                    )
    # 200 premium over 29 days, weekly 8 days → +200 * 8/29
    assert source == "synthetic"
    assert fut is not None
    assert fut["instrument_token"] == 9
    assert abs(price - (77500.0 + 200.0 * 8 / 29)) < 1e-6
