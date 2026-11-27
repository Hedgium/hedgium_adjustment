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


def fetch_quotes_from_kite(
    api_key: str,
    access_token: str,
    tokens: list[int],
) -> dict[int, dict]:
    """
    Fetch full quotes (LTP + best bid/ask) via Kite REST ``/quote``.

    Returns ``{instrument_token: {last_price, bid_price, ask_price}}``.
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
        url = f"https://api.kite.trade/quote?i={tokens_str}"
        resp = _requests.get(url, headers=headers, timeout=15)
        data = resp.json()
        out: dict[int, dict] = {}
        for _key, val in (data.get("data") or {}).items():
            inst_tok = val.get("instrument_token")
            if inst_tok is None:
                continue
            depth = val.get("depth") or {}
            buy = (depth.get("buy") or [{}])
            sell = (depth.get("sell") or [{}])
            bid = 0.0
            ask = 0.0
            if buy and isinstance(buy[0], dict):
                bid = float(buy[0].get("price") or 0)
            if sell and isinstance(sell[0], dict):
                ask = float(sell[0].get("price") or 0)
            # Some payloads also expose top-of-book on ohlc/last_price only.
            out[int(inst_tok)] = {
                "last_price": float(val.get("last_price") or 0),
                "bid_price": bid,
                "ask_price": ask,
            }
        return out
    except Exception as exc:
        logger.warning("token_fetcher: quote fetch failed: %s", exc)
        return {}


def classify_kite_token_response(status_code: int, body: object) -> str:
    """
    Classify a Kite REST response for the market-profile access token.

    ``ok`` — token accepted. ``rejected`` — token missing/expired (Kite
    ``TokenException``, HTTP 401/403). ``unknown`` — network or server
    failure; callers must not treat that as an expired token.
    """
    if status_code == 200:
        return "ok"
    if status_code in (401, 403):
        return "rejected"
    text = ""
    if isinstance(body, dict):
        text = f"{body.get('error_type') or ''} {body.get('message') or ''}"
    elif body:
        text = str(body)
    lowered = text.lower()
    if "tokenexception" in lowered or "incorrect `api_key`" in lowered or "incorrect api_key" in lowered:
        return "rejected"
    return "unknown"


def kite_token_status(api_key: str, access_token: str) -> str:
    """
    Probe ``GET /user/profile``.

    Returns ``ok``, ``rejected``, or ``unknown`` (see
    ``classify_kite_token_response``). Does not raise.
    """
    if not api_key or not access_token:
        return "rejected"
    try:
        import requests as _requests

        resp = _requests.get(
            "https://api.kite.trade/user/profile",
            headers={
                "X-Kite-Version": "3",
                "Authorization": f"token {api_key}:{access_token}",
            },
            timeout=10,
        )
    except Exception as exc:
        logger.warning("token_fetcher: broker token check failed: %s", exc)
        return "unknown"
    try:
        body = resp.json()
    except Exception:
        body = {}
    status = classify_kite_token_response(resp.status_code, body)
    if status == "rejected":
        logger.warning(
            "token_fetcher: broker access token rejected by Kite (http %s)",
            resp.status_code,
        )
    return status


def should_probe_broker_token(
    access_token: str,
    *,
    rejected_access_token: str | None,
    last_probe_at: float | None,
    now: float,
    recheck_s: float = 300.0,
) -> bool:
    """
    Probe Kite immediately for a new token.

    After a rejection, re-check the same token at ``recheck_s`` so a transient
    403 can recover, without calling Kite on every backend poll.
    """
    if not access_token:
        return False
    if access_token != (rejected_access_token or ""):
        return True
    if last_probe_at is None:
        return True
    return (now - last_probe_at) >= recheck_s


def decide_credential_action(
    creds: dict | None,
    *,
    rejected_access_token: str | None,
    token_status: str,
) -> str:
    """
    ``ready`` — Kite accepted this access token.

    ``ready_unverified`` — probe failed open (network); use it unless it is
    the token Kite already rejected.

    ``wait`` — missing, or expired. Caller keeps polling until auto-login
    stores a different token.
    """
    api_key = str((creds or {}).get("api_key") or "")
    token = str((creds or {}).get("access_token") or "")
    if not api_key or not token:
        return "wait"
    if token_status == "ok":
        return "ready"
    if token_status == "rejected":
        return "wait"
    if rejected_access_token and token == rejected_access_token:
        return "wait"
    return "ready_unverified"


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

    Empty token sets are valid (no active builders on open exchanges / holiday /
    commodity-only window with no MCX strategies). Callers idle until the next
    session boundary.
    """
    from market_sessions import set_cached_session

    data = backend_api.get_stream_config(mode=cfg.STREAM_MODE)

    if "error" in data:
        raise ValueError(data["error"])

    # Keep worker session cache in sync with backend (holidays / evening-only).
    set_cached_session(
        data.get("active_derivative_exchanges") or [],
        next_boundary_at=data.get("next_boundary_at"),
    )

    option_chains = data.get("option_chains") or []
    underlying_tokens: list[int] = data.get("underlying_tokens") or []
    underlying_token_by_symbol: dict[str, int] = {
        str(k): int(v)
        for k, v in (data.get("underlying_token_by_symbol") or {}).items()
    }

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
    equity_tokens = sorted(set(int(t) for t in underlying_tokens))

    if not equity_tokens and not option_tokens:
        logger.info(
            "token_fetcher: no tokens for current session "
            "(exchanges=%s) — idle until next boundary",
            data.get("active_derivative_exchanges") or [],
        )
        return StreamTokenSet(
            option_tokens=[],
            equity_tokens=[],
            zerodha_symbol_by_token={},
            ltp_by_underlying_token={},
            underlying_token_by_symbol={},
        )

    if not equity_tokens:
        raise ValueError(
            "No BuilderLeg.token values for active StrategyBuilders — set token on legs"
        )

    ltp_by_underlying_token = _fetch_initial_ltps(
        credentials["api_key"],
        credentials["access_token"],
        equity_tokens,
    )
    if not ltp_by_underlying_token:
        raise ValueError("Could not fetch underlying LTP from Kite for any token")

    logger.info(
        "token_fetcher: underlying_tokens=%s underlying_symbols=%s "
        "option_tokens=%s equity_tokens=%s exchanges=%s",
        equity_tokens,
        sorted(underlying_token_by_symbol.keys()),
        len(option_tokens),
        len(equity_tokens),
        data.get("active_derivative_exchanges") or [],
    )

    return StreamTokenSet(
        option_tokens=option_tokens,
        equity_tokens=equity_tokens,
        zerodha_symbol_by_token=zerodha_symbol_by_token,
        ltp_by_underlying_token=ltp_by_underlying_token,
        underlying_token_by_symbol=underlying_token_by_symbol,
    )
