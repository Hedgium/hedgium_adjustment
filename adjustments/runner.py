"""
Adjustment polling loop for hedgium_stream_worker.

Periodically:
  1. Fetches active builder configs + open positions from the backend API.
  2. Reads live LTPs / ticks from shared Redis (written by ws_stream).
  3. Computes per-position Greeks and aggregates net delta per underlying.
  4. Sends the snapshot to the backend ``/internal/adjustments/trigger`` endpoint
     which handles delta-band checking, proposal generation, and pushing.

Does NOT need Django or the ORM — all data comes through the HTTP + Redis layer.
"""

from __future__ import annotations

import logging
import threading
import time

import config as cfg
from client import backend_api
from adjustments.greeks import compute_greeks_for_builder

logger = logging.getLogger(__name__)


class AdjustmentRunner:
    """
    Background thread that runs the adjustment loop.

    Usage::

        r = redis.from_url(cfg.REDIS_URL)
        runner = AdjustmentRunner(r)
        runner.start()
        ...
        runner.stop()
        runner.join()
    """

    def __init__(self, redis_client):
        self._r = redis_client
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self):
        self._thread = threading.Thread(
            target=self._loop,
            name="adjustment-runner",
            daemon=False,
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

        logger.info("AdjustmentRunner: processing %s builder(s)", len(builders))

        for builder_data in builders:
            builder_id = builder_data.get("builder_id")
            strategy_id = builder_data.get("strategy_id")
            builder_name = builder_data.get("builder_name", "")

            try:
                snap = compute_greeks_for_builder(self._r, builder_data)
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
                result = backend_api.post_adjustment_trigger(
                    builder_id=int(builder_id),
                    strategy_id=int(strategy_id),
                    net_delta_by_underlying=snap["net_delta_by_underlying"],
                    spot_by_underlying=snap["spot_by_underlying"],
                    book_positions=snap["book_positions"],
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
            except Exception as exc:
                logger.warning(
                    "AdjustmentRunner: trigger API call failed builder_id=%s: %s",
                    builder_id, exc,
                )
