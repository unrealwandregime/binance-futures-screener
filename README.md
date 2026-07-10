# Binance Futures Screener

[Live app](https://binance-futures-screener-a39v.onrender.com) |
[GitHub repository](https://github.com/unrealwandregime/binance-futures-screener)

A standalone realtime screener for Binance USD-M perpetual futures. It gives traders, analysts, and market surveillance reviewers a fast way to scan the futures universe for unusual movement, volume, open interest, funding, volatility, and short-term trade activity.

This project is intentionally independent. It does not depend on Dash, Plotly, Pandas, CCXT, PostgreSQL, another dashboard repo, or private exchange credentials.

## What It Does

- Fetches public Binance Futures data server-side.
- Serves a dark-mode browser screener that refreshes every second.
- Tracks price, 5-minute change, 1-hour change, 24-hour change, volume, open interest, funding, volatility, and 5-minute trade count.
- Uses a shared backend cache so every visitor is not hitting Binance from their own browser.
- Hydrates heavier per-symbol metrics in rolling batches so the app stays responsive on small hosting plans.
- Falls back to browser-side Binance public-data hydration for deep fields when cloud-provider REST egress is blocked.
- Shows UTC timestamps, cache age, hydration progress, and feed status in the UI.
- Opens sorted by highest signal first, so the strongest anomaly candidates are at the top immediately.
- Runs on Render Free, Docker, or any small Python web host.

## Live Demo

```text
https://binance-futures-screener-a39v.onrender.com
```

The hosted app uses public Binance market-data endpoints only. There is no account connection and no trading functionality.

## Architecture

```mermaid
flowchart LR
  A["Binance Futures WebSocket"] --> C["Python backend cache"]
  B["Binance Futures REST fallback"] --> C
  P["Optional deep REST proxy on clean IP"] --> C
  C --> D["/api/screener"]
  D --> E["Dark-mode frontend"]
```

The backend prefers Binance WebSocket ticker streams for fresh quote data through the current USD-M futures market route, `wss://fstream.binance.com/market/ws`. If the stream is not warm after a short grace period, it can fall back to Binance REST. Heavier metrics such as open interest, 1-hour volume, volatility, and trade count are fetched separately in controlled batches.

For production, the public screener can keep running on Render while deep metrics are fetched through a separate REST proxy on a cleaner outbound IP. That proxy is optional. If `SCREENER_DEEP_PROXY_BASE` is not set, the app behaves exactly like the normal single-service version.

The browser UI also includes a throttled direct-fill fallback for deep metrics. If Binance blocks a cloud host's REST egress, the backend still streams live quote data from Binance WebSocket and the visitor's browser fills the REST-only fields for top rows from public Binance endpoints. This keeps the public demo useful without requiring private exchange credentials or a paid proxy.

## Tech Stack

- Python
- Flask
- Requests
- websocket-client
- Vanilla HTML, CSS, and JavaScript
- Gunicorn
- Render / Docker deployment

## Local Run

```powershell
python -m venv .venv
.\.venv\Scripts\activate
pip install -r requirements.txt
python server.py
```

Open:

```text
http://127.0.0.1:8050/
```

Health check:

```text
http://127.0.0.1:8050/api/healthz
```

API:

```text
http://127.0.0.1:8050/api/screener
```

## Signal Score

The `Sig` column is a 0-100 anomaly score calculated by this app. Binance does not provide this number.

The score is designed for triage, not prediction. A high score means a symbol deserves attention because several market conditions are unusual at the same time. A low score means the symbol is behaving closer to normal market noise based on the available public data.

Inputs:

- absolute 5-minute price move
- absolute 1-hour price move
- absolute 24-hour price move
- 15-minute high-low volatility
- absolute 1-hour open-interest change
- absolute funding rate
- 24-hour notional volume bonus

Score bands:

| Score | Meaning |
| --- | --- |
| 0-39 | Normal market noise |
| 40-69 | Worth monitoring |
| 70-84 | Strong anomaly |
| 85-100 | Priority review |

## Why Some Cells Say `queued`

Binance publishes quote-level fields for all futures markets quickly. Deep metrics are heavier because they require extra requests per symbol.

To avoid hammering Binance and getting the backend IP rate-limited, the service hydrates those deeper fields in batches. Right after a deploy or restart, lower-volume rows may briefly show `waiting`. If Binance temporarily pauses REST access for the host, the UI labels the deep source as paused and those REST-only cells show `--` while WebSocket-backed quote, 24-hour volume, 1-day change, and funding fields continue updating live.

## API Contract

`GET /api/screener`

Example response shape:

```json
{
  "status": "live",
  "exchange": "binance",
  "venue": "Binance Futures",
  "source": "binance_ws",
  "generatedAt": "2026-06-26T13:30:00Z",
  "deepGeneratedAt": "2026-06-26T13:29:42Z",
  "cacheAgeMs": 180,
  "streamAgeMs": 420,
  "quoteRefreshMs": 1000,
  "deepRefreshMs": 180000,
  "deepStatus": "hydrated",
  "restPaused": false,
  "deepRestPaused": false,
  "deepProxyEnabled": false,
  "deepSource": "binance_rest",
  "deepRetryAt": null,
  "deepRetryInMs": null,
  "deepHydratedCount": 420,
  "deepQueuedCount": 0,
  "deepTotalRows": 420,
  "baseRefreshing": false,
  "deepRefreshing": false,
  "lastError": null,
  "rows": []
}
```

## Environment Variables

| Variable | Default | Purpose |
| --- | --- | --- |
| `SCREENER_ENABLE_WS` | `1` | Enables Binance WebSocket streams |
| `BINANCE_WS_BASE` | `wss://fstream.binance.com/market/ws` | Binance USD-M futures market WebSocket base |
| `SCREENER_WS_WARMUP_SECONDS` | `15` | Grace period before REST fallback is allowed during startup |
| `SCREENER_REST_QUOTE_TTL_SECONDS` | `10` | Minimum pause between REST fallback quote refreshes |
| `SCREENER_DEEP_CACHE_TTL_SECONDS` | `180` | Deep metric cache lifetime |
| `SCREENER_DEEP_BATCH_INTERVAL_SECONDS` | `2` | Minimum pause between hydration batches |
| `SCREENER_DEEP_BATCH_SIZE` | `20` | Symbols hydrated per backend batch |
| `SCREENER_DEEP_WORKERS` | `3` | Concurrent deep metric workers |
| `SCREENER_DEEP_PROXY_BASE` | unset | Optional base URL for a separate deep-metrics proxy |
| `SCREENER_DEEP_PROXY_TOKEN` | unset | Bearer token sent to the deep proxy when configured |
| `SCREENER_DEEP_PROXY_TIMEOUT_SECONDS` | `8` | Timeout for deep proxy calls |
| `SCREENER_ALLOWED_ORIGINS` | `*` locally | CORS allowlist for browser clients |

Production clamps the numeric settings to reasonable ranges so a bad environment value cannot accidentally overload the host or Binance.

## Production Deep-Metrics Proxy

The clean production setup is two services:

1. Public screener on Render: serves the UI, keeps the Binance WebSocket connection warm, and exposes `/api/screener`.
2. Private deep proxy on a clean VPS or backend IP: makes the heavier Binance REST calls for 5-minute change, 1-hour volume, open interest, volatility, and trade count.

Do not run the proxy on the same Render free service and expect it to fix Render's IP reputation. The point is to move the REST-heavy requests to a separate outbound IP with calmer rate limits.

The proxy entrypoint is `deep_proxy.py`.

Local proxy run:

```powershell
$env:DEEP_PROXY_REQUIRE_TOKEN="0"
python deep_proxy.py
```

Proxy health check:

```text
http://127.0.0.1:8060/api/healthz
```

Proxy deep metric example:

```text
http://127.0.0.1:8060/api/deep?symbol=BTCUSDT&price=60000
```

Production proxy command:

```bash
gunicorn deep_proxy:server --bind 0.0.0.0:$PORT --workers 1 --threads 4 --timeout 120
```

Vercel proxy deployment:

This repository also includes `pyproject.toml` and `vercel.json` so Vercel loads `deep_proxy:app` directly as a Python Flask backend. Use the same proxy environment variables in the Vercel project settings.

Recommended proxy environment:

| Variable | Default | Purpose |
| --- | --- | --- |
| `DEEP_PROXY_TOKEN` | unset | Shared bearer token required by the screener |
| `DEEP_PROXY_REQUIRE_TOKEN` | `1` | Keeps the proxy closed unless a token is configured |
| `DEEP_PROXY_CACHE_TTL_SECONDS` | `180` | Per-symbol deep metrics cache |
| `DEEP_PROXY_MIN_UPSTREAM_INTERVAL_SECONDS` | `0.35` | Minimum spacing between Binance REST calls |
| `DEEP_PROXY_REQUEST_TIMEOUT_SECONDS` | `6` | Binance request timeout |

Then set these on the public screener service:

```text
SCREENER_DEEP_PROXY_BASE=https://your-deep-proxy.example.com
SCREENER_DEEP_PROXY_TOKEN=the-same-token-from-the-proxy
```

The proxy should be protected with the bearer token at minimum. For a stronger setup, also firewall it so only the screener backend can call it.


## Deployment

### Render

Create a Render Blueprint from this repository. `render.yaml` defines a free web service named:

```text
binance-futures-screener
```

The current Render app is:

```text
https://binance-futures-screener-a39v.onrender.com
```

### Docker

```bash
docker build -t binance-futures-screener .
docker run --rm -p 8050:7860 binance-futures-screener
```

## Security Posture

This is a read-only public market-data app.

It does not use or store:

- passwords
- private API keys
- exchange account credentials
- database credentials
- seed phrases
- wallet keys
- user sessions

The backend also avoids returning raw upstream error details that could expose infrastructure information, applies basic browser security headers, and restricts CORS in the production Render configuration.

If you fork this project, keep it read-only unless you have a strong reason to do otherwise. Do not add trading permissions, private exchange keys, wallet secrets, or real `.env` files to the repository.

## Limitations

Public futures data is useful for realtime scanning, but it does not prove trader intent or account-level misconduct. Binance may also rate-limit or block hosting-provider IPs. The backend cache centralizes that risk and makes it easier to manage than browser-direct exchange calls.
