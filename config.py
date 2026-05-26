"""
Configuration for hedgium_stream_worker.

Reads from a .env file in the CWD (via python-decouple) or real environment variables.
Copy .env.example to .env and fill in the blanks.
"""

from __future__ import annotations

from decouple import config as _cfg, UndefinedValueError

def _env(key: str, default: str = "") -> str:
    try:
        return (_cfg(key, default=default) or "").strip()
    except UndefinedValueError:
        return default.strip()


def _env_float(key: str, default: float) -> float:
    try:
        return float(_cfg(key, default=str(default), cast=str) or str(default))
    except (UndefinedValueError, ValueError, TypeError):
        return default


def _env_int(key: str, default: int) -> int:
    try:
        return int(_cfg(key, default=str(default), cast=str) or str(default))
    except (UndefinedValueError, ValueError, TypeError):
        return default


# ── Backend API ───────────────────────────────────────────────────────────────
# URL of the Django backend (no trailing slash).
BACKEND_API_URL: str = _env("BACKEND_API_URL", "http://localhost:8000/api")

# Shared secret — set INTERNAL_SERVICE_TOKEN in both backend and worker .env.
SERVICE_TOKEN: str = _env("INTERNAL_SERVICE_TOKEN", "")

# HTTP timeout for backend API calls (seconds).
BACKEND_HTTP_TIMEOUT: float = _env_float("WORKER_BACKEND_HTTP_TIMEOUT", 15.0)

# Retry: max attempts + base delay (exponential backoff) for transient errors.
BACKEND_RETRY_ATTEMPTS: int = _env_int("WORKER_BACKEND_RETRY_ATTEMPTS", 3)
BACKEND_RETRY_BASE_DELAY_S: float = _env_float("WORKER_BACKEND_RETRY_BASE_DELAY_S", 1.0)

# ── Redis ─────────────────────────────────────────────────────────────────────
REDIS_URL: str = _env("REDIS_URL", "redis://localhost:6379/0")

# ── Streaming ─────────────────────────────────────────────────────────────────
# Stream mode sent to backend config endpoint: "full" or "auto".
STREAM_MODE: str = _env("WORKER_STREAM_MODE", "auto")

# Seconds between retries while waiting for backend config / tokens.
WAIT_POLL_S: float = _env_float("MYSTREAM_WAIT_POLL_S", 10.0)

# Seconds before restarting after a dead WebSocket thread.
DEAD_RESTART_S: float = _env_float("MYSTREAM_DEAD_RESTART_S", 5.0)

# Seconds between token-set reload checks.
RELOAD_CHECK_INTERVAL_S: float = _env_float("MYSTREAM_RELOAD_CHECK_INTERVAL_S", 180.0)

# Seconds between full periodic Greeks persist sweeps (min 5).
GREEKS_FULL_INTERVAL_S: float = max(5.0, _env_float("MYSTREAM_GREEKS_FULL_INTERVAL_S", 180.0))

# Whether to run ATM sync on the heartbeat loop.
SYNC_ATM_ENABLED: bool = _env("MYSTREAM_SYNC_ATM_ENABLED", "1").lower() not in ("0", "false", "no", "off")

# Minimum seconds between ATM sync calls.
SYNC_ATM_MIN_INTERVAL_S: float = _env_float("MYSTREAM_SYNC_ATM_MIN_INTERVAL_S", 180.0)

# Kite WebSocket settings.
WS_PING_TIMEOUT: float = _env_float("MYSTREAM_WS_PING_TIMEOUT", 25.0)
WS_PING_INTERVAL: float = _env_float("MYSTREAM_WS_PING_INTERVAL", 30.0)
WS_RECONNECT_BASE_DELAY_S: float = _env_float("MYSTREAM_WS_RECONNECT_BASE_S", 1.0)
WS_RECONNECT_MAX_DELAY_S: float = _env_float("MYSTREAM_WS_RECONNECT_MAX_S", 60.0)
MAX_INSTRUMENTS_PER_WS: int = 3000
MAX_WS_CONNECTIONS: int = 3

# ── Adjustments ───────────────────────────────────────────────────────────────
# Seconds between each adjustment polling cycle.
ADJUSTMENTS_INTERVAL_S: float = _env_float("WORKER_ADJUSTMENTS_INTERVAL_S", 60.0)

# Seconds between in-memory Greek refresh (reads Redis ticks, computes BS Greeks).
GREEKS_UPDATE_INTERVAL_S: float = max(30.0, _env_float("WORKER_GREEKS_UPDATE_INTERVAL_S", 90.0))

# Seconds between persisting computed Greeks to the backend DB (bulk upsert).
GREEKS_PERSIST_INTERVAL_S: float = max(60.0, _env_float("WORKER_GREEKS_PERSIST_INTERVAL_S", 300.0))

# Seconds between live broker position refresh.
POSITIONS_REFRESH_INTERVAL_S: float = max(15.0, _env_float("WORKER_POSITIONS_REFRESH_INTERVAL_S", 60.0))

# Greeks pipeline verbose trace.
GREEKS_PIPELINE_TRACE: bool = _env("GREEKS_PIPELINE_TRACE", "0").lower() not in ("0", "false", "no", "off")

# Age (seconds) beyond which in-memory Greeks are considered stale for the
# adjustment check.  If the last update_greeks() run was older than this,
# get_greeks_for_position() falls back to fresh Black-Scholes.
# Should be at least WORKER_GREEKS_UPDATE_INTERVAL_S + some buffer.
GREEKS_STALE_THRESHOLD_S: float = _env_float("WORKER_GREEKS_STALE_THRESHOLD_S", 300.0)

# Max seconds the adjustment runner waits for the first Greek bootstrap at startup.
GREEKS_BOOTSTRAP_TIMEOUT_S: float = max(
    120.0, _env_float("WORKER_GREEKS_BOOTSTRAP_TIMEOUT_S", 180.0)
)

# ── Redis key schema (must match backend optionchain/mystream/constants.py) ───
REDIS_TICKS_HASH: str = _env("MYSTREAM_TICKS_HASH", "mystream:ticks")
REDIS_TICKS_HASH_FALLBACK: str = _env("MYSTREAM_TICKS_HASH_FALLBACK", "")
REDIS_HASH_SYMBOL_TO_TOKEN: str = "mystream:symbol_to_token"
REDIS_META_KEY: str = "mystream:meta"
REDIS_HASH_LTP: str = "mystream:ltp"
REDIS_HASH_UNDERLYING_SYMBOL_TOKEN: str = "mystream:underlying_symbol_token"
REDIS_RUNNING_KEY: str = "mystream:running"
REDIS_PID_KEY: str = "mystream:pid"
REDIS_STOP_KEY: str = "mystream:stop"

# When false, the Greek persist thread skips bulk-upsert calls.
# Set GREEKS_PERSIST_ENABLED=false in .env to disable without restarting.
GREEKS_PERSIST_ENABLED: bool = _env("GREEKS_PERSIST_ENABLED", "true").lower() not in ("0", "false", "no", "off")
