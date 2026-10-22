"""Closed sessions must not fall back to the database position book."""

from __future__ import annotations

from unittest.mock import patch

from adjustments.positions_manager import LivePositionsManager


def _nfo_builder() -> dict:
    return {
        "builder_id": 1,
        "master_profile_id": 10,
        "legs": [{"symbol": "NIFTY"}],
        "positions": [
            {
                "exchange": "NFO",
                "underlying_symbol": "NIFTY",
                "instrument": "NIFTY26OCT25000CE",
                "quantity": 65,
            }
        ],
    }


def test_closed_nfo_drops_db_book_when_broker_book_is_empty():
    pm = LivePositionsManager()
    original = _nfo_builder()
    with patch(
        "adjustments.positions_manager.derivative_exchanges_open",
        return_value=frozenset({"MCX", "NCO"}),
    ):
        out = pm.map_to_builders([original])

    assert out[0]["positions"] == []
    assert original["positions"]  # caller dict is not mutated


def test_closed_nfo_drops_db_book_when_only_mcx_is_live():
    pm = LivePositionsManager()
    pm._positions = [
        {
            "exchange": "MCX",
            "underlying_symbol": "CRUDEOIL",
            "tradingsymbol": "CRUDEOIL26OCTFUT",
            "profile_id": 10,
            "product": "NRML",
            "instrument_token": 1,
            "quantity": 1,
        }
    ]
    with patch(
        "adjustments.positions_manager.derivative_exchanges_open",
        return_value=frozenset({"MCX", "NCO"}),
    ):
        out = pm.map_to_builders([_nfo_builder()])

    assert out[0]["positions"] == []


def test_open_nfo_keeps_db_book_when_broker_has_no_match():
    pm = LivePositionsManager()
    with patch(
        "adjustments.positions_manager.derivative_exchanges_open",
        return_value=frozenset({"NFO", "BFO", "MCX", "NCO"}),
    ):
        out = pm.map_to_builders([_nfo_builder()])

    assert out[0]["positions"][0]["exchange"] == "NFO"
    assert out[0]["positions"][0]["quantity"] == 65
