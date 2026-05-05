"""
hedgium_stream_worker — entry point.

Connects to the Kite WebSocket, writes ticks to shared Redis, and runs the
adjustment polling loop — all without Django / ORM.

Usage::

    python main.py [--flush] [--no-adjustments]

Env vars (see config.py / .env.example):
    BACKEND_API_URL, INTERNAL_SERVICE_TOKEN, REDIS_URL, WORKER_STREAM_MODE, …

The worker shares the same Redis instance as the Django backend; all Redis key
names are identical to those written by the Django ``run_mystream`` management
command so backend readers (greeks service, stream status page, etc.) continue
to work unchanged.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import queue
import signal
import sys
import threading
import time
from datetime import datetime, timezone

# config.py uses python-decouple which auto-reads .env from CWD.

import config as cfg

# ── Logging setup ────────────────────────────────────────────────────────────
_LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, _LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("hedgium_stream_worker")

# ── Deferred imports (after env/logging are ready) ───────────────────────────
import redis as _redis

from client import backend_api
from stream.redis_writer import (
    clear_stream_keys,
    write_ltps,
    write_meta,
    write_symbol_index,
    write_ticks_batch,
    write_underlying_symbol_token_map,
)
from stream.token_fetcher import collect_stream_tokens
from stream.ws_stream import run_multi_connection_stream
from adjustments.runner import AdjustmentRunner


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

def _get_redis() -> _redis.Redis:
    return _redis.from_url(cfg.REDIS_URL, decode_responses=True)


def _wait_for_credentials() -> dict:
    """Block until the backend returns valid broker credentials."""
    while True:
        try:
            creds = backend_api.get_credentials()
            if "error" in creds:
                raise ValueError(creds["error"])
            if not creds.get("api_key") or not creds.get("access_token"):
                raise ValueError("api_key or access_token missing in credentials response")
            return creds
        except Exception as exc:
            logger.warning(
                "worker: waiting for credentials (%s); retry in %.0fs…", exc, cfg.WAIT_POLL_S
            )
            time.sleep(cfg.WAIT_POLL_S)


def _wait_for_tokens(credentials: dict):
    """Block until at least one instrument token is available."""
    while True:
        try:
            token_set = collect_stream_tokens(credentials)
            tokens = sorted(set(token_set.option_tokens + token_set.equity_tokens))
            if not tokens:
                raise ValueError("No tokens — check active builders and OptionChain rows")
            return token_set, tokens
        except Exception as exc:
            logger.warning(
                "worker: waiting for tokens (%s); retry in %.0fs…", exc, cfg.WAIT_POLL_S
            )
            time.sleep(cfg.WAIT_POLL_S)


# ──────────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────────

def run(*, flush: bool = False, run_adjustments: bool = True) -> None:
    logger.info("hedgium_stream_worker starting (mode=%s)", cfg.STREAM_MODE)

    r = _get_redis()

    if flush:
        clear_stream_keys(r)
        logger.info("worker: flushed mystream:* Redis keys")

    _stop_requested = threading.Event()

    def _handle_signal(sig, frame):
        logger.info("worker: signal %s — stopping…", sig)
        _stop_requested.set()

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    # ── outer restart loop ────────────────────────────────────────────────────
    while not _stop_requested.is_set():

        # 1. Credentials
        credentials = _wait_for_credentials()
        logger.info("worker: credentials OK (api_key=%s…)", credentials["api_key"][:6])

        # 2. Token set
        token_set, all_tokens = _wait_for_tokens(credentials)

        option_tokens = sorted(set(token_set.option_tokens))
        underlying_symbols = sorted(token_set.underlying_token_by_symbol.keys())

        # 3. Tick queue + workers
        tick_q: queue.Queue = queue.Queue()
        stop_greeks_event = threading.Event()
        last_atm_sync_t = [0.0]

        def _persist_active() -> bool:
            return r.get(cfg.REDIS_PERSIST_ENABLED_KEY) != "0"

        def _periodic_greeks():
            logger.debug("worker: periodic greeks thread started")
            if stop_greeks_event.wait(min(3.0, cfg.GREEKS_FULL_INTERVAL_S)):
                return
            while not stop_greeks_event.wait(cfg.GREEKS_FULL_INTERVAL_S):
                if not option_tokens or not _persist_active():
                    continue
                try:
                    greeks_payload = _collect_greeks_for_bulk_upsert(r, option_tokens)
                    if greeks_payload:
                        stats = backend_api.post_greeks_bulk_upsert(greeks_payload)
                        logger.info(
                            "worker: periodic greeks upsert tokens=%s stats=%s",
                            len(option_tokens), stats,
                        )
                except Exception:
                    logger.exception("worker: periodic greeks error")

        def _tick_worker():
            while True:
                batch = tick_q.get()
                try:
                    if batch is None:
                        break
                    if not _persist_active():
                        continue
                    now = time.monotonic()
                    if (
                        cfg.SYNC_ATM_ENABLED
                        and now - last_atm_sync_t[0] >= cfg.SYNC_ATM_MIN_INTERVAL_S
                    ):
                        last_atm_sync_t[0] = now
                        try:
                            backend_api.post_atm_sync(underlying_symbols)
                        except Exception:
                            logger.exception("worker: atm-sync call failed")
                except Exception:
                    logger.exception("worker: tick worker error")
                finally:
                    tick_q.task_done()

        def on_ticks(ticks: list):
            try:
                write_ticks_batch(r, ticks)
                tick_q.put(list(ticks))
            except Exception:
                logger.exception("worker: Redis write failed")

        # 4. Start WebSocket streams
        try:
            streams = run_multi_connection_stream(
                api_key=credentials["api_key"],
                access_token=credentials["access_token"],
                all_tokens=all_tokens,
                on_ticks=on_ticks,
            )
        except ValueError as exc:
            logger.error("worker: WebSocket config error: %s", exc)
            time.sleep(cfg.WAIT_POLL_S)
            continue
        except Exception:
            logger.exception("worker: WebSocket setup failed")
            time.sleep(cfg.WAIT_POLL_S)
            continue

        # 5. Publish Redis state (same keys as Django run_mystream)
        r.delete(cfg.REDIS_STOP_KEY)
        r.set(cfg.REDIS_RUNNING_KEY, "1")
        r.set(cfg.REDIS_PID_KEY, str(os.getpid()))

        write_ltps(r, token_set.ltp_by_underlying_token)
        write_underlying_symbol_token_map(r, token_set.underlying_token_by_symbol)
        write_symbol_index(r, token_set.zerodha_symbol_by_token)

        now_iso = datetime.now(timezone.utc).isoformat()
        write_meta(r, {
            "started_at": now_iso,
            "last_heartbeat": now_iso,
            "mode": cfg.STREAM_MODE,
            "pid": os.getpid(),
            "token_count": len(all_tokens),
            "option_token_count": len(option_tokens),
            "equity_token_count": len(token_set.equity_tokens),
            "underlying_ltp_tokens": sorted(token_set.ltp_by_underlying_token.keys()),
            "underlying_leg_tokens": sorted(token_set.equity_tokens),
            "worker": "hedgium_stream_worker",
        })

        # 6. Start worker threads
        greeks_thread = threading.Thread(
            target=_periodic_greeks, name="worker-greeks", daemon=False
        )
        tick_thread = threading.Thread(
            target=_tick_worker, name="worker-ticks", daemon=True
        )
        greeks_thread.start()
        tick_thread.start()

        # 7. Start adjustment runner
        adj_runner: AdjustmentRunner | None = None
        if run_adjustments:
            adj_runner = AdjustmentRunner(r)
            adj_runner.start()

        logger.info(
            "worker running: %s WS connections, %s tokens (mode=%s)",
            len(streams), len(all_tokens), cfg.STREAM_MODE,
        )
        print(
            f"hedgium_stream_worker: {len(streams)} WS, {len(all_tokens)} tokens, "
            f"mode={cfg.STREAM_MODE}. Ctrl+C to stop.",
            flush=True,
        )

        # 8. Session watch loop
        should_restart = False
        last_reload_t = 0.0
        try:
            while not _stop_requested.is_set():
                time.sleep(2)

                if r.get(cfg.REDIS_STOP_KEY) == "1":
                    logger.info("worker: stop flag set — shutting down")
                    break

                dead = [s for s in streams if not s.is_alive()]
                if dead:
                    logger.error(
                        "worker: WebSocket thread(s) died (%s) — restarting",
                        [s.name for s in dead],
                    )
                    should_restart = True
                    break

                now_m = time.monotonic()
                if now_m - last_reload_t >= cfg.RELOAD_CHECK_INTERVAL_S:
                    last_reload_t = now_m
                    try:
                        latest = collect_stream_tokens(credentials)
                        latest_tokens = sorted(set(latest.option_tokens + latest.equity_tokens))
                        if latest_tokens != all_tokens:
                            logger.info(
                                "worker: token set changed (%s→%s) — reloading",
                                len(all_tokens), len(latest_tokens),
                            )
                            should_restart = True
                            break
                    except Exception:
                        logger.exception("worker: reload check failed; keeping session")

                # Heartbeat
                try:
                    m = json.loads(r.get(cfg.REDIS_META_KEY) or "{}")
                    m["last_heartbeat"] = datetime.now(timezone.utc).isoformat()
                    r.set(cfg.REDIS_META_KEY, json.dumps(m))
                except Exception:
                    pass

        finally:
            # Teardown
            stop_greeks_event.set()
            for s in streams:
                s.stop()

            if adj_runner:
                adj_runner.stop()

            # Drain tick queue
            drained = 0
            while True:
                try:
                    tick_q.get_nowait()
                    tick_q.task_done()
                    drained += 1
                except queue.Empty:
                    break
            if drained:
                logger.info("worker: drained %s queued tick batches", drained)

            tick_q.put(None)
            tick_thread.join(timeout=60.0)
            greeks_thread.join(timeout=5.0)
            if adj_runner:
                adj_runner.join(timeout=30.0)

            # Clear Redis running flags
            try:
                m = json.loads(r.get(cfg.REDIS_META_KEY) or "{}")
                m["stopped_at"] = datetime.now(timezone.utc).isoformat()
                r.set(cfg.REDIS_META_KEY, json.dumps(m))
            except Exception:
                pass
            r.set(cfg.REDIS_RUNNING_KEY, "0")
            r.delete(cfg.REDIS_PID_KEY)
            r.delete(cfg.REDIS_STOP_KEY)

        if should_restart and not _stop_requested.is_set():
            logger.info("worker: restarting in %.0fs…", cfg.DEAD_RESTART_S)
            time.sleep(cfg.DEAD_RESTART_S)
            continue

        break  # clean exit

    logger.info("hedgium_stream_worker stopped")


# ──────────────────────────────────────────────────────────────────────────────
# Periodic Greeks helper (reads ticks from Redis, returns bulk-upsert payload)
# ──────────────────────────────────────────────────────────────────────────────

def _collect_greeks_for_bulk_upsert(r, option_tokens: list[int]) -> list[dict]:
    """
    For each option token, read the current tick from Redis and return a
    list of Greek dicts ready for the bulk-upsert endpoint.

    This is a lightweight version — it does NOT do a full BS calculation
    here; the backend's ``greeks/bulk-upsert`` endpoint already stores
    mid-price based Greeks.  The heavy BS computation runs inside the Django
    backend's ``persist_auto_greeks_for_tokens`` — triggered here via the ATM
    sync call in the tick worker.

    For full Greeks upsert from the worker side, this would use
    ``adjustments.greeks.get_greeks_for_position``.  Left as a placeholder
    so the architecture is clear; the ATM-sync call in the tick worker is the
    primary periodic mechanism.
    """
    # Placeholder: actual bulk Greek computation would iterate tokens,
    # read ticks, compute BS and return [{zerodha_instrument_token, iv, delta, …}].
    # For now we return empty so the backend persists Greeks via its own
    # periodic greeks thread (triggered by the ATM sync path).
    return []


# ──────────────────────────────────────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="hedgium_stream_worker")
    parser.add_argument(
        "--flush",
        action="store_true",
        help="Delete mystream:* Redis keys before starting.",
    )
    parser.add_argument(
        "--no-adjustments",
        action="store_true",
        help="Disable the adjustment polling loop (streaming only).",
    )
    args = parser.parse_args()
    run(flush=args.flush, run_adjustments=not args.no_adjustments)


if __name__ == "__main__":
    main()
