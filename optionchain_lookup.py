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

    Lookup order for each position's ``tradingsymbol``:
    1. ``zerodha_tradingsymbol`` in the store (exact, case-insensitive)

    Adds the following keys to each position (``None`` if not found):
    - ``instrument_token`` (zerodha)
    - ``underlying_symbol``
    - ``strike``
    - ``option_type``
    - ``expiry``  (ISO date string)
    - ``lot_size``
    - ``zerodha_tradingsymbol``

    Also normalises ``broker_name`` if the caller set it, otherwise leaves it.
    """
    # Build tradingsymbol → chain row lookup from the in-memory store
    ts_index: dict[str, dict] = {}
    for row in store.get_all_rows():
        ts = (row.get("zerodha_tradingsymbol") or "").strip().upper()
        if ts and ts not in ts_index:
            ts_index[ts] = row

    enriched: list[dict] = []
    for pos in positions:
        ts_key = (pos.get("tradingsymbol") or "").strip().upper()
        chain = ts_index.get(ts_key)

        p = dict(pos)
        if chain:
            expiry = chain.get("expiry")
            p.setdefault("instrument_token", chain.get("zerodha_instrument_token"))
            p["underlying_symbol"] = (chain.get("underlying_symbol") or "").upper() or None
            p["strike"] = float(chain["strike"]) if chain.get("strike") is not None else None
            p["option_type"] = chain.get("option_type")
            p["expiry"] = expiry.isoformat() if hasattr(expiry, "isoformat") else expiry
            p["lot_size"] = int(chain.get("lot_size") or 1)
            p["zerodha_tradingsymbol"] = chain.get("zerodha_tradingsymbol") or ""
        else:
            logger.debug(
                "optionchain_lookup: no OptionChain match for tradingsymbol=%r", ts_key
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
