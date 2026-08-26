# hedgium_stream_worker

Standalone Kite WebSocket worker that streams live ticks to Redis and computes option Greeks for adjustments.

## What it does

1. Subscribes to option + equity + **futures** tokens over Kite WS  
2. Writes ticks to Redis (`mystream:*`) for the Django backend to read  
3. Recomputes Greeks on an interval and bulk-upserts them to the backend  
4. Runs delta-band adjustment checks using live broker positions  

## Run

```bash
cd hedgium_stream_worker
cp .env.example .env   # if present; otherwise copy from existing .env
# fill BACKEND_API_URL, INTERNAL_SERVICE_TOKEN, REDIS_URL

pip install -r requirements.txt
python main.py
```

Flags:

- `--flush` — clear `mystream:*` Redis keys before start  
- `--no-adjustments` — stream + Greeks only (skip adjustment runner)

Auth for internal APIs: `Authorization: Bearer <INTERNAL_SERVICE_TOKEN>` (same token as the backend).

## Greeks model (futures underlier)

Greeks use the **NFO/BFO futures price** `F` for `(underlying, option_expiry)`, not cash/index spot.

| Rule | Detail |
|------|--------|
| Contract | Exact FUT expiry match → listed monthly. Else nearest FUT with `expiry >= option expiry` (covering monthly) → else nearest overall |
| Quote | Monthly: liquid LTP if `bid ≤ ltp ≤ ask`; else bid/ask mid; else LTP. Weekly before near-month FUT: `F = spot + (F_near − spot) × days_weekly / days_near`. Weekly between near and away FUT: `F = F_near + (F_away − F_near) × days_from_near_to_weekly / days_near_to_away`. If F would be cash/index **spot**: `F = spot × (1 + 0.065 × days / 365)` |
| Dividend | Not applied (carry is already in `F`) |
| Rate | Black–Scholes with `S = F`, `r = 0` |
| Shared IV | Same strike CE+PE share one IV (mean of both) so `\|Δ_CE\| + \|Δ_PE\| ≈ 1` |
| Baseline | AUTO: `computed_at_spot` = futures `F`. MANUAL: gamma-adj vs cash spot / `manual_delta_spot` |
| Strategy spot | `greek_spot_by_underlying` stores **cash/index LTP**, not futures |

Key modules:

- `adjustments/futures_underlier.py` — FUT resolve + quote pick  
- `adjustments/option_chain_store.py` — periodic IV / Greeks update + persist payload  
- `adjustments/greeks.py` — per-position Greeks for adjustments  

## Layout

```
main.py                      # entry point, WS + threads
config.py                    # env (strips inline # comments)
client/backend_api.py        # internal HTTP API
stream/                      # WS, Redis, token fetch
adjustments/                 # Greeks, futures, positions, runner
tests/                       # pytest
```

## Important env vars

| Variable | Purpose |
|----------|---------|
| `BACKEND_API_URL` | Backend API base (e.g. `https://…/api`) |
| `INTERNAL_SERVICE_TOKEN` | Shared service secret |
| `REDIS_URL` | Shared Redis with backend |
| `WORKER_STREAM_MODE` | `auto` or `full` |
| `WORKER_GREEKS_UPDATE_INTERVAL_S` | Recompute Greeks interval |
| `WORKER_GREEKS_PERSIST_INTERVAL_S` | Bulk upsert interval |
| `WORKER_ADJUSTMENTS_INTERVAL_S` | Adjustment poll interval |
| `WORKER_GREEKS_DELTA_SUM_TOLERANCE` | Warn if `\|Δ_CE\|+\|Δ_PE\|` drifts from 1 (default `0.02`) |
| `GREEKS_PERSIST_ENABLED` | `true` / `false` |

Do not put inline comments on the same line as values unless they start with ` #` (config strips that). Prefer comments on their own lines.

## Tests

```bash
pytest tests/ -q
```

## Notes

- Futures tokens are merged into the WS subscription set so `F` usually comes from live Redis ticks; Kite full quote is the fallback.  
- Reload checks compare the full set (options + equity + futures) so adding futures does not falsely trigger a restart loop.  
- On the backend, set `WORKER_STREAM_DISABLED=1` and `WORKER_ADJUSTMENTS_DISABLED=1` if this worker owns streaming/adjustments.
