"""
Live broker positions manager for the stream worker.

Periodically fetches live positions from the backend (which calls the broker
API for master trade-cycle profiles), maps them to active builders by
underlying symbol, and exposes the instrument tokens so that the WebSocket
subscription set can be updated dynamically.
"""

from __future__ import annotations

import copy
import logging
import threading
from datetime import date
from typing import Optional

from client import backend_api

logger = logging.getLogger(__name__)


class LivePositionsManager:
    """
    Manages live broker positions and their mapping to strategy builders.

    Thread-safety: all mutable state is protected by ``_lock``.  The refresh
    loop in ``main.py`` calls ``refresh()`` from its own thread; ``map_to_builders``
    is called from the ``AdjustmentRunner`` thread.

    Usage::

        pm = LivePositionsManager()
        changed = pm.refresh()          # fetch from backend
        builders_live = pm.map_to_builders(builders_raw)
        new_tokens = pm.get_all_tokens()
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._positions: list[dict] = []
        self._tokens: set[int] = set()
        self._last_signature: str = ""

    # ── public ────────────────────────────────────────────────────────────────

    def refresh(self) -> bool:
        """
        Fetch live positions from the backend and update internal state.

        Returns ``True`` if the set of instrument tokens changed (caller should
        then update WebSocket subscriptions), ``False`` otherwise.
        """
        try:
            resp = backend_api.get_live_positions()
        except Exception as exc:
            logger.warning("LivePositionsManager.refresh: API error: %s", exc)
            return False

        raw_positions: list[dict] = resp.get("positions") or []
        profiles_fetched: int = resp.get("profiles_fetched", 0)

        if not raw_positions:
            logger.debug(
                "LivePositionsManager.refresh: no live positions returned "
                "(profiles_fetched=%s)", profiles_fetched,
            )
        else:
            logger.info(
                "LivePositionsManager.refresh: %s live positions across %s profile(s)",
                len(raw_positions), profiles_fetched,
            )

        new_tokens: set[int] = set()
        for pos in raw_positions:
            tok = pos.get("instrument_token")
            if tok is not None:
                try:
                    new_tokens.add(int(tok))
                except (TypeError, ValueError):
                    pass

        # Signature: sorted token list as string — cheap change detector
        new_sig = ",".join(str(t) for t in sorted(new_tokens))
        changed = new_sig != self._last_signature

        with self._lock:
            self._positions = raw_positions
            self._tokens = new_tokens
            self._last_signature = new_sig

        if changed:
            logger.info(
                "LivePositionsManager.refresh: token set changed → %s tokens",
                len(new_tokens),
            )

        return changed

    def get_all_tokens(self) -> set[int]:
        """Return the set of all instrument tokens from live positions."""
        with self._lock:
            return set(self._tokens)

    def get_positions(self) -> list[dict]:
        """Return a snapshot of the current live positions list."""
        with self._lock:
            return copy.deepcopy(self._positions)

    def map_to_builders(self, builders: list[dict]) -> list[dict]:
        """
        Replace each builder's ``positions`` list with live broker positions
        that match the builder's underlying symbols (derived from its legs).

        Falls back to the original book positions if no live positions are
        found for a builder's underlyings (so Greek computation still runs).

        Returns a new list; the original ``builders`` dicts are not mutated.
        """
        with self._lock:
            live_positions = list(self._positions)

        if not live_positions:
            return builders

        # Group live positions by underlying_symbol
        live_by_underlying: dict[str, list[dict]] = {}
        for pos in live_positions:
            u = (pos.get("underlying_symbol") or "").strip().upper()
            if not u:
                continue
            live_by_underlying.setdefault(u, []).append(pos)

        result: list[dict] = []
        for builder in builders:
            b = dict(builder)

            # Collect underlyings from legs
            underlyings: set[str] = set()
            for leg in b.get("legs") or []:
                sym = (leg.get("symbol") or "").strip().upper()
                if sym:
                    underlyings.add(sym)

            # Gather live positions that belong to these underlyings
            matched: list[dict] = []
            for u in underlyings:
                matched.extend(live_by_underlying.get(u) or [])

            if matched:
                # Normalize live positions into the same shape as book positions
                b["positions"] = [_normalize_live_position(pos) for pos in matched]
                logger.debug(
                    "LivePositionsManager.map_to_builders: builder_id=%s → "
                    "%s live positions (underlyings=%s)",
                    b.get("builder_id"), len(matched), sorted(underlyings),
                )
            else:
                logger.debug(
                    "LivePositionsManager.map_to_builders: builder_id=%s — "
                    "no live match for underlyings=%s, keeping book positions",
                    b.get("builder_id"), sorted(underlyings),
                )

            result.append(b)

        return result


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

def _normalize_live_position(pos: dict) -> dict:
    """
    Convert a live-broker position dict (from /internal/positions/live) to the
    shape expected by ``adjustments/greeks.py::compute_greeks_for_builder``.

    The backend endpoint already resolves ``instrument_token`` to the Zerodha
    token for all broker types (Zerodha, Shoonya, KotakNeo), so we can use it
    directly for tick lookups in Redis.

    The builder data from ``adjustments_builders`` uses::

        {position_id, instrument, quantity, exchange,
         zerodha_instrument_token, underlying_symbol, strike,
         option_type, expiry (ISO str), lot_size}
    """
    expiry_raw = pos.get("expiry")
    qty = pos.get("quantity") or 0
    try:
        qty_int = int(float(qty))
    except (TypeError, ValueError):
        qty_int = 0
    return {
        "position_id": None,
        "instrument": pos.get("tradingsymbol") or "",
        "quantity": qty_int,
        "exchange": (pos.get("exchange") or "NFO").strip().upper() or "NFO",
        "zerodha_instrument_token": pos.get("instrument_token"),
        "underlying_symbol": (pos.get("underlying_symbol") or "").strip().upper() or None,
        "strike": pos.get("strike"),
        "option_type": pos.get("option_type"),
        "expiry": expiry_raw,
        "lot_size": int(pos.get("lot_size") or 1),
        "broker_name": pos.get("broker_name") or "",
    }
