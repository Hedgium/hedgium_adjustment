"""
Fetch streaming token config from the backend internal API and seed initial LTPs
via Kite REST (same as ``token_builder.py`` in the Django backend but without DB access).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

import config as cfg
from client import backend_api

logger = logging.getLogger(__name__)


@dataclass
class StreamTokenSet:
    option_tokens: list[int]
    equity_tokens: list[int]
    zerodha_symbol_by_token: dict[int, str]
    ltp_by_underlying_token: dict[int, float]
    underlying_token_by_symbol: dict[str, int]


def fetch_ltps_from_kite(
    api_key: str,
    access_token: str,
    tokens: list[int],
) -> dict[int, float]:
    """
    Fetch last-traded prices for a list of instrument tokens via Kite REST
    ``/quote/ohlc``.  Returns ``{instrument_token: last_price}`` for each
    token that has a price; silently omits tokens that the API doesn't return.

    Used both as the LTP seed at startup and as the runtime fallback when a
    token's tick is not yet present in Redis.
    """
    if not tokens:
        return {}
    try:
        import requests as _requests
        from urllib.parse import quote as _quote

        headers = {
            "X-Kite-Version": "3",
            "Authorization": f"token {api_key}:{access_token}",
        }
        tokens_str = "&i=".join(_quote(str(t), safe="") for t in tokens)
        url = f"https://api.kite.trade/quote/ohlc?i={tokens_str}"
        resp = _requests.get(url, headers=headers, timeout=10)
        data = resp.json()
        out: dict[int, float] = {}
        for key, val in (data.get("data") or {}).items():
            lp = val.get("last_price")
            inst_tok = val.get("instrument_token")
            if lp is not None and inst_tok is not None:
                out[int(inst_tok)] = float(lp)
        return out
    except Exception as exc:
        logger.warning("token_fetcher: LTP fetch failed: %s", exc)
        return {}


def _fetch_initial_ltps(
    api_key: str,
    access_token: str,
    underlying_tokens: list[int],
) -> dict[int, float]:
    """Seed underlying LTPs at startup — thin wrapper around fetch_ltps_from_kite."""
    return fetch_ltps_from_kite(api_key, access_token, underlying_tokens)


def collect_stream_tokens(credentials: dict) -> StreamTokenSet:
    """
    Build the subscription token set by calling the backend config API.

    Raises ``ValueError`` if the backend reports no tokens.
    """
    data = backend_api.get_stream_config(mode=cfg.STREAM_MODE)

    if "error" in data:
        raise ValueError(data["error"])

    option_chains = data.get("option_chains") or []
    underlying_tokens: list[int] = data.get("underlying_tokens") or []
    underlying_token_by_symbol: dict[str, int] = {
        str(k): int(v)
        for k, v in (data.get("underlying_token_by_symbol") or {}).items()
    }

    if not underlying_tokens:
        raise ValueError(
            "No BuilderLeg.token values for active StrategyBuilders — set token on legs"
        )

    option_tokens: list[int] = []
    zerodha_symbol_by_token: dict[int, str] = {}

    for row in option_chains:
        tok = row.get("zerodha_instrument_token")
        sym = row.get("zerodha_tradingsymbol") or ""
        if tok is None:
            continue
        t = int(tok)
        option_tokens.append(t)
        if sym:
            zerodha_symbol_by_token[t] = sym.upper()

    option_tokens = sorted(set(option_tokens))
    equity_tokens = sorted(set(underlying_tokens))

    ltp_by_underlying_token = _fetch_initial_ltps(
        credentials["api_key"],
        credentials["access_token"],
        equity_tokens,
    )
    if not ltp_by_underlying_token:
        raise ValueError("Could not fetch underlying LTP from Kite for any token")

    logger.info(
        "token_fetcher: underlying_tokens=%s underlying_symbols=%s "
        "option_tokens=%s equity_tokens=%s",
        equity_tokens,
        sorted(underlying_token_by_symbol.keys()),
        len(option_tokens),
        len(equity_tokens),
    )

    return StreamTokenSet(
        option_tokens=option_tokens,
        equity_tokens=equity_tokens,
        zerodha_symbol_by_token=zerodha_symbol_by_token,
        ltp_by_underlying_token=ltp_by_underlying_token,
        underlying_token_by_symbol=underlying_token_by_symbol,
    )
