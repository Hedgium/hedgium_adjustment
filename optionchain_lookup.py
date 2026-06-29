"""
Enrich raw broker positions with OptionChain metadata.

The worker's ``OptionChainStore`` is keyed by ``zerodha_instrument_token``.
This module builds a secondary index keyed by every known trading-symbol
variant so that cross-broker positions (Shoonya, KotakNeo) can be looked up
by ``tradingsymbol``.

Usage::

    from optionchain_lookup import enrich_positions_with_option_chain
    enriched = enrich_positions_with_option_chain(raw_positions, store)
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from adjustments.option_chain_store import OptionChainStore

logger = logging.getLogger(__name__)


def enrich_positions_with_option_chain(
    positions: list[dict],
    store: "OptionChainStore",
) -> list[dict]:
    """
    Attach OptionChain metadata to each raw broker position.

    Lookup is attempted against all three broker tradingsymbol variants stored
    in the OptionChainStore (zerodha, shoonya, kotakneo) so that positions from
    any broker type are correctly enriched.  Without this, non-Zerodha positions
    return ``underlying_symbol=None`` and are invisible to Greek computation.

    Adds the following keys to each position (``None`` if not found):
    - ``instrument_token``  — always the **Zerodha** token for Redis tick lookup
    - ``underlying_symbol``
    - ``strike``
    - ``option_type``
    - ``expiry``  (ISO date string)
    - ``lot_size``
    - ``zerodha_tradingsymbol``
    """
    # Build a unified tradingsymbol → chain-row index covering every broker format.
    ts_index: dict[str, dict] = {}
    for row in store.get_all_rows():
        for field in ("zerodha_tradingsymbol", "shoonya_tradingsymbol", "kotakneo_tradingsymbol"):
            ts = (row.get(field) or "").strip().upper()
            if ts and ts not in ts_index:
                ts_index[ts] = row

    enriched: list[dict] = []
    for pos in positions:
        ts_key = (pos.get("tradingsymbol") or "").strip().upper()
        chain = ts_index.get(ts_key)

        p = dict(pos)
        if chain:
            expiry = chain.get("expiry")
            # Always use the Zerodha token so Redis tick lookups work regardless
            # of which broker placed the position.
            p["instrument_token"] = chain.get("zerodha_instrument_token")
            p["underlying_symbol"] = (chain.get("underlying_symbol") or "").upper() or None
            p["strike"] = float(chain["strike"]) if chain.get("strike") is not None else None
            p["option_type"] = chain.get("option_type")
            p["expiry"] = expiry.isoformat() if hasattr(expiry, "isoformat") else expiry
            p["lot_size"] = int(chain.get("lot_size") or 1)
            p["zerodha_tradingsymbol"] = chain.get("zerodha_tradingsymbol") or ""
        else:
            logger.warning(
                "optionchain_lookup: no OptionChain match for tradingsymbol=%r "
                "(position will be excluded from Greek computation)",
                ts_key,
            )
            p.setdefault("instrument_token", None)
            p.setdefault("underlying_symbol", None)
            p.setdefault("strike", None)
            p.setdefault("option_type", None)
            p.setdefault("expiry", None)
            p.setdefault("lot_size", 1)
            p.setdefault("zerodha_tradingsymbol", "")

        enriched.append(p)

    return enriched
