"""NCO websocket prices must match Kite REST rupee quotes."""

from stream.ws_stream import correct_nco_tick_prices


def test_nco_tick_prices_scaled_down_by_100():
    # token low byte 12 is the NCO segment; kiteconnect divided by 100 not 10000
    tick = correct_nco_tick_prices({
        "instrument_token": 33538828,
        "last_price": 889600.0,
        "average_traded_price": 900690.0,
        "bid_price": 886700.0,
        "ask_price": 887600.0,
        "change": -0.1795,
        "ohlc": {"open": 904700.0, "high": 908200.0, "low": 889600.0, "close": 891200.0},
        "depth": {
            "buy": [{"quantity": 1, "price": 886700.0, "orders": 1}],
            "sell": [{"quantity": 5, "price": 887600.0, "orders": 1}],
        },
    })
    assert tick["last_price"] == 8896.0
    assert tick["average_traded_price"] == 9006.9
    assert tick["bid_price"] == 8867.0
    assert tick["ask_price"] == 8876.0
    assert tick["change"] == -0.1795
    assert tick["ohlc"]["close"] == 8912.0
    assert tick["depth"]["buy"][0]["price"] == 8867.0
    assert tick["depth"]["sell"][0]["price"] == 8876.0


def test_nfo_tick_prices_unchanged():
    tick = correct_nco_tick_prices({
        "instrument_token": (1000 << 8) | 2,  # NFO segment
        "last_price": 24500.0,
        "bid_price": 24499.0,
        "ask_price": 24501.0,
    })
    assert tick["last_price"] == 24500.0
    assert tick["bid_price"] == 24499.0
    assert tick["ask_price"] == 24501.0
