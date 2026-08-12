from datetime import date
from unittest.mock import MagicMock, patch

from adjustments.greeks import compute_greeks_for_builder, get_greeks_for_position


def test_get_greeks_for_position_short_quantity_signs():
    r = MagicMock()
    with patch("adjustments.greeks.fetch_tick_by_token", return_value={"bid_price": 10, "ask_price": 12, "last_price": 11}):
        with patch("adjustments.greeks._stored_greeks_fresh", return_value=True):
            with patch("adjustments.greeks._implied_vol", return_value=0.2):
                greeks = get_greeks_for_position(
                    r,
                    zerodha_instrument_token=123,
                    underlying_spot=100.0,
                    strike=100.0,
                    option_type="CE",
                    expiry=date(2026, 3, 27),
                    quantity=-50,
                    instrument_label="TEST",
                    stored_chain={
                        "delta": 0.5,
                        "gamma": 0.01,
                        "theta": -1.0,
                        "vega": 2.0,
                        "computed_at_spot": 100.0,
                        "last_greeks_at": "2026-03-21T10:00:00",
                    },
                )
    assert greeks is not None
    assert greeks["net_delta"] < 0
    assert greeks["net_gamma"] < 0


def test_post_adjustment_trigger_includes_position_greeks():
    from client import backend_api

    captured = {}

    def fake_request(method, path, **kwargs):
        captured["json"] = kwargs.get("json")
        return {"status": "ok"}

    with patch.object(backend_api, "_request", side_effect=fake_request):
        backend_api.post_adjustment_trigger(
            builder_id=1,
            strategy_id=2,
            net_delta_by_underlying={"NIFTY": 1.0},
            spot_by_underlying={"NIFTY": 24500.0},
            future_by_underlying={"NIFTY": 24550.0},
            future_expiry_by_underlying={"NIFTY": "2026-03-26"},
            book_positions=[],
            net_greeks={"net_delta": 1.0},
            position_greeks=[
                {
                    "position_id": 10,
                    "instrument": "NIFTY26MAR24000CE",
                    "net_delta": 1.0,
                    "calculated_at": "2026-03-21T10:00:00+00:00",
                }
            ],
            master_trade_cycle_id=99,
        )

    body = captured["json"]
    assert body["position_greeks"][0]["position_id"] == 10
    assert body["master_trade_cycle_id"] == 99
    assert body["future_by_underlying"]["NIFTY"] == 24550.0
    assert body["future_expiry_by_underlying"]["NIFTY"] == "2026-03-26"


def test_compute_greeks_for_builder_enriches_per_leg_metadata():
    r = MagicMock()
    builder_data = {
        "builder_id": 1,
        "strategy_id": 2,
        "master_profile_id": 11,
        "master_trade_cycle_id": 22,
        "positions": [
            {
                "position_id": 5,
                "zerodha_instrument_token": 123,
                "underlying_symbol": "NIFTY",
                "strike": 24000,
                "option_type": "CE",
                "expiry": "2026-03-27",
                "quantity": 50,
                "exchange": "NFO",
                "lot_size": 1,
                "instrument": "NIFTY26MAR24000CE",
            }
        ],
        "legs": [{"token": 256265, "symbol": "NIFTY"}],
        "dividend_by_underlying": {},
    }
    fake_greeks = {
        "instrument": "NIFTY26MAR24000CE",
        "zerodha_instrument_token": 123,
        "delta": 0.5,
        "gamma": 0.01,
        "theta": -1.0,
        "vega": 2.0,
        "net_delta": 25.0,
        "net_gamma": 0.5,
        "net_theta": -50.0,
        "net_vega": 100.0,
        "greeks_source": "gamma_adj",
        "bid": 1,
        "ask": 2,
        "ltp": 1.5,
        "mid": 1.5,
        "iv": 0.0,
    }
    with patch("adjustments.greeks.get_future_price_for_option", return_value=(24500.0, {"match_kind": "exact"}, "ltp")):
        with patch("adjustments.greeks.get_underlying_spot", return_value=24480.0):
            with patch("adjustments.greeks.get_greeks_for_position", return_value=fake_greeks):
                with patch(
                    "adjustments.greeks.resolve_near_month_future",
                    return_value={
                        "name": "NIFTY",
                        "instrument_token": 999,
                        "tradingsymbol": "NIFTY26MARFUT",
                        "expiry": date(2026, 3, 26),
                        "match_kind": "near_month",
                    },
                ):
                    with patch(
                        "adjustments.greeks.get_future_price",
                        return_value=(24550.0, "ltp"),
                    ):
                        snap = compute_greeks_for_builder(r, builder_data)

    assert snap is not None
    leg = snap["per_leg"][0]
    assert leg["position_id"] == 5
    assert leg["instrument"] == "NIFTY26MAR24000CE"
    assert leg["exchange"] == "NFO"
    assert leg["spot"] == 24500.0  # futures underlier used for greek math
    assert snap["spot_by_underlying"]["NIFTY"] == 24480.0  # cash/index for greek_spot_by_underlying
    assert snap["future_by_underlying"]["NIFTY"] == 24550.0
    assert snap["future_expiry_by_underlying"]["NIFTY"] == "2026-03-26"
    assert leg["calculated_at"]
