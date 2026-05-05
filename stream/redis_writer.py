"""
Write ticks / metadata to Redis.

Key schema is identical to ``optionchain/mystream/redis_store.py`` in the
Django backend so that all existing backend readers (greeks, ATM sync, etc.)
continue to work without changes.
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

import config as cfg

_IST = ZoneInfo("Asia/Kolkata")


def _now_india() -> str:
    return datetime.now(_IST).strftime("%Y-%m-%d %H:%M:%S")


def write_meta(r, payload: dict[str, Any]) -> None:
    r.set(cfg.REDIS_META_KEY, json.dumps(payload))


def write_ltps(r, ltp_by_underlying_token: dict[int, float]) -> None:
    if not ltp_by_underlying_token:
        return
    pipe = r.pipeline()
    for tok, v in ltp_by_underlying_token.items():
        pipe.hset(cfg.REDIS_HASH_LTP, str(int(tok)), str(v))
    pipe.execute()


def write_underlying_symbol_token_map(r, underlying_token_by_symbol: dict[str, int]) -> None:
    if not underlying_token_by_symbol:
        return
    pipe = r.pipeline()
    for sym, tok in underlying_token_by_symbol.items():
        k = (sym or "").strip().upper()
        if k:
            pipe.hset(cfg.REDIS_HASH_UNDERLYING_SYMBOL_TOKEN, k, str(int(tok)))
    pipe.execute()


def write_symbol_index(r, zerodha_symbol_by_token: dict[int, str]) -> None:
    if not zerodha_symbol_by_token:
        return
    pipe = r.pipeline()
    for tok, sym in zerodha_symbol_by_token.items():
        pipe.hset(cfg.REDIS_HASH_SYMBOL_TO_TOKEN, sym, str(int(tok)))
    pipe.execute()


def write_ticks_batch(r, ticks: list[dict[str, Any]]) -> None:
    if not ticks:
        return
    pipe = r.pipeline()
    received_at = _now_india()
    for t in ticks:
        tok = t.get("instrument_token")
        if tok is None:
            continue
        t["received_at_india"] = received_at
        pipe.hset(cfg.REDIS_TICKS_HASH, str(int(tok)), json.dumps(t, default=str))
    pipe.execute()


def clear_stream_keys(r) -> None:
    r.delete(
        cfg.REDIS_TICKS_HASH,
        cfg.REDIS_HASH_SYMBOL_TO_TOKEN,
        cfg.REDIS_META_KEY,
        cfg.REDIS_HASH_LTP,
        cfg.REDIS_HASH_UNDERLYING_SYMBOL_TOKEN,
        cfg.REDIS_STOP_KEY,
        cfg.REDIS_PID_KEY,
        cfg.REDIS_RUNNING_KEY,
    )


def fetch_tick_by_token(r, instrument_token: int) -> dict | None:
    raw = r.hget(cfg.REDIS_TICKS_HASH, str(int(instrument_token)))
    if raw:
        try:
            return json.loads(raw)
        except (TypeError, json.JSONDecodeError):
            pass
    fb = cfg.REDIS_TICKS_HASH_FALLBACK.strip()
    if fb and fb != cfg.REDIS_TICKS_HASH:
        try:
            raw2 = r.hget(fb, str(int(instrument_token)))
            if raw2:
                return json.loads(raw2)
        except Exception:
            pass
    return None


def fetch_ltps(r) -> dict[int, float]:
    h = r.hgetall(cfg.REDIS_HASH_LTP) or {}
    out: dict[int, float] = {}
    for k, v in h.items():
        try:
            out[int(k)] = float(v)
        except (TypeError, ValueError):
            continue
    return out


def resolve_underlying_zerodha_token(r, underlying_symbol: str) -> int | None:
    u = (underlying_symbol or "").strip().upper()
    if not u:
        return None
    raw = r.hget(cfg.REDIS_HASH_UNDERLYING_SYMBOL_TOKEN, u)
    if raw is None:
        return None
    try:
        return int(raw)
    except (TypeError, ValueError):
        return None
