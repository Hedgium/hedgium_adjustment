"""
Adjustment polling loop for hedgium_stream_worker.

Periodically:
  1. Fetches active builder configs from the backend API.
  2. Replaces book positions with live broker positions via LivePositionsManager
     (if a positions_manager is provided).
  3. Reads live LTPs / ticks from shared Redis (written by ws_stream).
  4. Computes per-position Greeks and aggregates net delta per underlying.
  5. Sends the snapshot to the backend ``/internal/adjustments/trigger`` endpoint
     which handles delta-band checking, proposal generation, and pushing.

Does NOT need Django or the ORM — all data comes through the HTTP + Redis layer.
"""

from __future__ import annotations

import logging
import json
import threading
import time
from typing import Optional

import config as cfg
from client import backend_api
from adjustments.greeks import compute_greeks_for_builder

logger = logging.getLogger(__name__)


class AdjustmentRunner:
    """
    Background thread that runs the adjustment loop.

    Usage::

        r = redis.from_url(cfg.REDIS_URL)
        runner = AdjustmentRunner(r, positions_manager=pm)
        runner.start()
        ...
        runner.stop()
        runner.join()
    """

    def __init__(self, redis_client, positions_manager=None, option_chain_store=None):
        self._r = redis_client
        self._positions_manager = positions_manager
        self._option_chain_store = option_chain_store
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        # strategy_id -> canonical position signature from previous cycle
        self._last_positions_sig_by_strategy: dict[int, str] = {}

    @staticmethod
    def _positions_signature(book_positions: list[dict]) -> str:
        """
        Canonical signature of live positions used to detect external changes.
        """
        rows: list[tuple] = []
        for p in book_positions or []:
            rows.append(
                (
                    str(p.get("underlying_symbol") or "").upper(),
                    str(p.get("option_type") or "").upper(),
                    str(p.get("expiry") or ""),
                    float(p.get("strike") or 0.0),
                    int(p.get("quantity") or 0),
                    str(p.get("exchange") or ""),
                    int(p.get("instrument_token") or 0),
                )
            )
        rows.sort()
        return json.dumps(rows, separators=(",", ":"))

    def start(self):
        self._thread = threading.Thread(
            target=self._loop,
            name="adjustment-runner",
            daemon=True,
        )
        self._thread.start()
        logger.info("AdjustmentRunner started (interval=%.0fs)", cfg.ADJUSTMENTS_INTERVAL_S)

    def stop(self):
        self._stop_event.set()

    def join(self, timeout: float = 30.0):
        if self._thread:
            self._thread.join(timeout=timeout)

    def is_alive(self) -> bool:
        return bool(self._thread and self._thread.is_alive())

    # ──────────────────────────────────────────────────────────────────────────

    def _loop(self):
        while not self._stop_event.wait(cfg.ADJUSTMENTS_INTERVAL_S):
            try:
                self._run_once()
            except Exception:
                logger.exception("AdjustmentRunner: unhandled error in run_once")

    def _run_once(self):
        # Guard: only run if mystream is marked as running in Redis.
        running_flag = self._r.get(cfg.REDIS_RUNNING_KEY)
        if running_flag != "1":
            logger.debug("AdjustmentRunner: mystream not running in Redis — skipping")
            return

        try:
            resp = backend_api.get_adjustment_builders()
        except Exception as exc:
            logger.warning("AdjustmentRunner: could not fetch builders: %s", exc)
            return

        builders = resp.get("builders") or []
        if not builders:
            logger.debug("AdjustmentRunner: no active builders")
            return

        # Always fetch fresh live positions before computing Greeks / triggering.
        # Provide the master_profile_id from each builder so positions are fetched
        # directly from the broker (not the stale DB positions table).
        if self._positions_manager is not None:
            profile_ids = [
                b["master_profile_id"]
                for b in builders
                if b.get("master_profile_id")
            ]
            if profile_ids:
                self._positions_manager.set_profile_ids(profile_ids)
            self._positions_manager.refresh()
            builders = self._positions_manager.map_to_builders(builders)

        logger.info("AdjustmentRunner: processing %s builder(s)", len(builders))

        for builder_data in builders:
            builder_id = builder_data.get("builder_id")
            strategy_id = builder_data.get("strategy_id")
            builder_name = builder_data.get("builder_name", "")

            try:
                snap = compute_greeks_for_builder(
                    self._r,
                    builder_data,
                    option_chain_store=self._option_chain_store,
                )
            except Exception:
                logger.exception(
                    "AdjustmentRunner: greek computation failed builder_id=%s", builder_id
                )
                continue

            if not snap:
                logger.debug(
                    "AdjustmentRunner: builder_id=%s skipped (no greek snapshot)",
                    builder_id,
                )
                continue

            logger.info(
                "AdjustmentRunner: builder_id=%s strategy_id=%s net_delta_by_u=%s spots=%s",
                builder_id,
                strategy_id,
                snap["net_delta_by_underlying"],
                {u: round(v, 2) for u, v in snap["spot_by_underlying"].items()},
            )

            try:
                sid = int(strategy_id or 0)
                if sid > 0:
                    current_sig = self._positions_signature(snap.get("book_positions") or [])
                    prev_sig = self._last_positions_sig_by_strategy.get(sid)
                    self._last_positions_sig_by_strategy[sid] = current_sig
                    # Ignore first-seen snapshot per strategy.
                    if prev_sig is not None and prev_sig != current_sig:
                        try:
                            spot_res = backend_api.post_strategy_spot_snapshot(
                                strategy_id=sid,
                                spot_by_underlying=snap.get("spot_by_underlying") or {},
                                reason="positions_changed_external",
                            )
                            logger.info(
                                "AdjustmentRunner: spot snapshot updated strategy_id=%s res=%s",
                                sid,
                                spot_res,
                            )
                        except Exception as exc:
                            logger.warning(
                                "AdjustmentRunner: spot snapshot update failed strategy_id=%s: %s",
                                sid,
                                exc,
                            )

                result = backend_api.post_adjustment_trigger(
                    builder_id=int(builder_id),
                    strategy_id=int(strategy_id),
                    net_delta_by_underlying=snap["net_delta_by_underlying"],
                    spot_by_underlying=snap["spot_by_underlying"],
                    book_positions=snap["book_positions"],
                    net_greeks=snap.get("net_greeks"),
                )
                status = result.get("status", "unknown")
                if status == "pushed":
                    logger.info(
                        "AdjustmentRunner: pushed adjustment builder_id=%s strategy_id=%s push=%s",
                        builder_id, strategy_id, result.get("push"),
                    )
                elif status in ("skipped", "ok"):
                    logger.debug(
                        "AdjustmentRunner: builder_id=%s status=%s reason=%s",
                        builder_id, status, result.get("reason", ""),
                    )
                elif status == "error":
                    logger.error(
                        "AdjustmentRunner: builder_id=%s trigger error: %s",
                        builder_id, result.get("message"),
                    )
                else:
                    logger.warning(
                        "AdjustmentRunner: builder_id=%s unexpected trigger response (status=%s http=%s): %s",
                        builder_id, status, result.get("_status", "?"), result,
                    )
            except Exception as exc:
                logger.warning(
                    "AdjustmentRunner: trigger API call failed builder_id=%s: %s",
                    builder_id, exc,
                )
