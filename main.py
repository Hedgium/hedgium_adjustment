"""
hedgium_stream_worker — entry point.

Connects to the Kite WebSocket, writes ticks to shared Redis, and runs the
full live pipeline:

  - Option chain store (in-memory, refreshed from backend every
    GREEKS_UPDATE_INTERVAL_S seconds; picks up MANUAL/AUTO source changes
    automatically without restarting)
  - Greek persist thread (bulk-upserts Greeks to backend DB every
    GREEKS_PERSIST_INTERVAL_S seconds)
  - Live positions thread (fetches broker positions every
    POSITIONS_REFRESH_INTERVAL_S seconds, maps to builders, triggers dynamic
    WebSocket subscription updates for new/removed tokens)
  - Adjustment runner (delta-band checks using live positions, every
    ADJUSTMENTS_INTERVAL_S seconds)

Usage::

    python main.py [--flush] [--no-adjustments]

Env vars (see config.py / .env.example):
    BACKEND_API_URL, INTERNAL_SERVICE_TOKEN, REDIS_URL, WORKER_STREAM_MODE, …
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

import config as cfg

# ── Logging setup ────────────────────────────────────────────────────────────
_LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, _LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("hedgium_stream_worker")

# ── Deferred imports ─────────────────────────────────────────────────────────
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
from stream.ws_stream import KiteQuoteStreamer, run_multi_connection_stream
from adjustments.runner import AdjustmentRunner
from adjustments.option_chain_store import OptionChainStore
from adjustments.positions_manager import LivePositionsManager


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
                raise ValueError("api_key or access_token missing")
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


def _load_option_chains(
    option_chain_store: OptionChainStore,
    underlying_symbols: list[str] | None = None,
) -> None:
    """
    Fetch full OptionChain rows from backend and load into the store.

    Tries ``mode=auto`` first (positions-filtered).  If that returns zero rows
    (e.g. no open positions yet), falls back to ``mode=full`` so the store is
    always seeded and Greeks can be computed for the entire subscribed universe.
    """
    try:
        resp = backend_api.get_option_chains(underlying_symbols or None, mode="auto")
        chains = resp.get("option_chains") or []
        if not chains:
            logger.info(
                "worker: option-chains auto mode returned 0 rows — falling back to mode=full"
            )
            resp = backend_api.get_option_chains(underlying_symbols or None, mode="full")
            chains = resp.get("option_chains") or []
        option_chain_store.load(
            chains,
            dividend_by_underlying=resp.get("dividend_by_underlying") or {},
        )
        logger.info(
            "worker: option chain store loaded %s rows (mode=%s)",
            len(chains), resp.get("mode", "?"),
        )
    except Exception:
        logger.exception("worker: failed to load option chains from backend")


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

    # Shared state objects (persist across inner restarts)
    option_chain_store = OptionChainStore()
    positions_manager = LivePositionsManager(option_chain_store=option_chain_store)

    # ── outer restart loop ────────────────────────────────────────────────────
    while not _stop_requested.is_set():

        # 1. Credentials
        credentials = _wait_for_credentials()
        logger.info("worker: credentials OK (api_key=%s…)", credentials["api_key"][:6])

        # 2. Token set
        token_set, all_tokens = _wait_for_tokens(credentials)

        option_tokens = sorted(set(token_set.option_tokens))
        underlying_symbols = sorted(token_set.underlying_token_by_symbol.keys())

        # 3. Load option chains into memory store
        _load_option_chains(option_chain_store, underlying_symbols)

        # 4. Tick queue + persist-active guard
        tick_q: queue.Queue = queue.Queue()
        stop_threads_event = threading.Event()
        last_atm_sync_t = [0.0]

        # Current subscribed token set (mutable list shared with positions thread)
        current_tokens: list[int] = list(all_tokens)
        current_tokens_lock = threading.Lock()

        def _persist_active() -> bool:
            return cfg.GREEKS_PERSIST_ENABLED

        # Signals the persist thread to flush immediately after the first
        # successful Greek update rather than waiting the full persist interval.
        _first_greeks_ready = threading.Event()

        # ── Greek update thread ───────────────────────────────────────────────
        def _greek_update_thread():
            logger.info("worker: Greek update thread started (interval=%.0fs)", cfg.GREEKS_UPDATE_INTERVAL_S)
            while not stop_threads_event.wait(cfg.GREEKS_UPDATE_INTERVAL_S):
                # Re-fetch option chain metadata every cycle so that backend
                # changes (e.g. switching source to MANUAL, strike updates) are
                # picked up automatically without restarting the worker.
                try:
                    _load_option_chains(option_chain_store)
                except Exception:
                    logger.exception("worker: option chain reload error")

                store_size = option_chain_store.size()
                if not store_size:
                    logger.info("worker: Greek update — store empty (chains not loaded yet), skipping")
                    continue
                try:
                    updated = option_chain_store.update_greeks(r, credentials)
                    logger.info(
                        "worker: Greek update done — updated=%s/%s",
                        updated, store_size,
                    )
                    if updated > 0:
                        _first_greeks_ready.set()
                except Exception:
                    logger.exception("worker: Greek update error")

        def _do_persist():
            """Run one persist cycle — call the bulk-upsert API if payload is non-empty."""
            if not _persist_active():
                logger.debug("worker: Greek persist disabled (Redis flag), skipping")
                return
            payload = option_chain_store.get_greeks_payload()
            if payload:
                stats = backend_api.post_greeks_bulk_upsert(payload)
                logger.info(
                    "worker: Greek persist upsert — tokens=%s stats=%s",
                    len(payload), stats,
                )
            else:
                logger.info(
                    "worker: Greek persist — payload empty (store=%s rows, none with computed Greeks yet)",
                    option_chain_store.size(),
                )

        # ── Greek persist thread ──────────────────────────────────────────────
        def _greek_persist_thread():
            logger.info("worker: Greek persist thread started (interval=%.0fs)", cfg.GREEKS_PERSIST_INTERVAL_S)
            # Wait for the first Greeks to be computed (at most GREEKS_PERSIST_INTERVAL_S)
            # so we persist as soon as data is available rather than at a fixed delay.
            _first_greeks_ready.wait(timeout=cfg.GREEKS_PERSIST_INTERVAL_S)
            if stop_threads_event.is_set():
                return
            try:
                _do_persist()
            except Exception:
                logger.exception("worker: Greek persist error (initial flush)")
            # Then run on the regular cadence
            while not stop_threads_event.wait(cfg.GREEKS_PERSIST_INTERVAL_S):
                try:
                    _do_persist()
                except Exception:
                    logger.exception("worker: Greek persist error")

        # ── Live positions thread ─────────────────────────────────────────────
        def _positions_thread():
            logger.debug(
                "worker: live positions thread started (interval=%.0fs)",
                cfg.POSITIONS_REFRESH_INTERVAL_S,
            )
            if stop_threads_event.wait(5.0):
                return
            while not stop_threads_event.wait(cfg.POSITIONS_REFRESH_INTERVAL_S):
                try:
                    # Sync profile IDs from latest builder list so positions are
                    # fetched directly from the broker (not the stale DB table).
                    try:
                        builders_resp = backend_api.get_adjustment_builders()
                        profile_ids = list(dict.fromkeys(
                            b["master_profile_id"]
                            for b in (builders_resp.get("builders") or [])
                            if b.get("master_profile_id")
                        ))
                        if profile_ids:
                            positions_manager.set_profile_ids(profile_ids)
                    except Exception:
                        logger.debug("worker: could not refresh profile IDs for positions")

                    positions_manager.refresh()

                    new_live_tokens = positions_manager.get_all_tokens()
                    with current_tokens_lock:
                        old_set = set(current_tokens)

                    add_tokens = sorted(new_live_tokens - old_set)
                    # Do not remove option tokens that are part of the original
                    # stream config — only add new ones from live positions.
                    if not add_tokens:
                        continue

                    logger.info(
                        "worker: live positions changed → %s new tokens; "
                        "reloading option chains and subscribing",
                        len(add_tokens),
                    )

                    # Reload option chains immediately so new position tokens
                    # get Greek baselines before the next adjustment cycle.
                    try:
                        _load_option_chains(option_chain_store)
                    except Exception:
                        logger.exception("worker: option chain reload after new positions failed")

                    # Distribute new tokens across streams
                    total_per_stream = cfg.MAX_INSTRUMENTS_PER_WS
                    add_queue = list(add_tokens)
                    for s in streams:
                        if not add_queue:
                            break
                        spare = total_per_stream - len(s.tokens)
                        if spare <= 0:
                            continue
                        batch = add_queue[:spare]
                        add_queue = add_queue[spare:]
                        s.update_subscriptions(add_tokens=batch, remove_tokens=[])

                    with current_tokens_lock:
                        current_tokens.extend(add_tokens)

                    # Seed LTPs for new underlying tokens via Kite REST if they are
                    # equity tokens (instrument type heuristic: token < 5_000_000)
                    new_equity = [t for t in add_tokens if t < 5_000_000]
                    if new_equity:
                        from stream.token_fetcher import fetch_ltps_from_kite
                        ltp_map = fetch_ltps_from_kite(
                            credentials["api_key"], credentials["access_token"], new_equity
                        )
                        if ltp_map:
                            write_ltps(r, ltp_map)

                except Exception:
                    logger.exception("worker: live positions thread error")

        # ── Tick worker ───────────────────────────────────────────────────────
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

        # 5. Start WebSocket streams
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

        # 6. Publish Redis state
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
            "option_chain_store_size": option_chain_store.size(),
            "worker": "hedgium_stream_worker",
        })

        # 6b. Bootstrap Greeks from live ticks before adjustments run
        if option_chain_store.size():
            try:
                boot_updated = option_chain_store.update_greeks(r, credentials)
                logger.info(
                    "worker: Greek bootstrap — updated=%s/%s",
                    boot_updated, option_chain_store.size(),
                )
            except Exception:
                logger.exception("worker: Greek bootstrap failed")
        if option_chain_store.has_fresh_greeks():
            _first_greeks_ready.set()
            logger.info("worker: Greek bootstrap ready — adjustments may run")
        else:
            logger.warning(
                "worker: Greek bootstrap incomplete — adjustments blocked until "
                "first successful Greek update"
            )

        # 7. Start worker threads
        thread_greek_update = threading.Thread(
            target=_greek_update_thread, name="worker-greeks-update", daemon=True
        )
        thread_greek_persist = threading.Thread(
            target=_greek_persist_thread, name="worker-greeks-persist", daemon=True
        )
        thread_positions = threading.Thread(
            target=_positions_thread, name="worker-positions", daemon=True
        )
        tick_thread = threading.Thread(
            target=_tick_worker, name="worker-ticks", daemon=True
        )

        thread_greek_update.start()
        thread_greek_persist.start()
        thread_positions.start()
        tick_thread.start()

        # 8. Adjustment runner
        adj_runner: AdjustmentRunner | None = None
        if run_adjustments:
            adj_runner = AdjustmentRunner(
                r,
                positions_manager=positions_manager,
                option_chain_store=option_chain_store,
                greeks_ready=_first_greeks_ready,
            )
            adj_runner.start()

        logger.info(
            "worker running: %s WS connections, %s tokens, "
            "chain_store=%s, mode=%s",
            len(streams), len(all_tokens),
            option_chain_store.size(), cfg.STREAM_MODE,
        )
        print(
            f"hedgium_stream_worker: {len(streams)} WS, {len(all_tokens)} tokens, "
            f"chain_store={option_chain_store.size()}, mode={cfg.STREAM_MODE}. Ctrl+C to stop.",
            flush=True,
        )

        # 9. Session watch loop
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
                    m["option_chain_store_size"] = option_chain_store.size()
                    r.set(cfg.REDIS_META_KEY, json.dumps(m))
                except Exception:
                    pass

        finally:
            stop_threads_event.set()
            _first_greeks_ready.set()   # unblock persist thread if still in initial wait
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
            thread_greek_update.join(timeout=5.0)
            thread_greek_persist.join(timeout=5.0)
            thread_positions.join(timeout=5.0)
            if adj_runner:
                adj_runner.join(timeout=30.0)

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
