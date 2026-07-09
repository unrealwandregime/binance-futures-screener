from __future__ import annotations

import json
import os
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests
from flask import Flask, jsonify, make_response, request, send_from_directory

try:
    import websocket
except ImportError:  # pragma: no cover
    websocket = None


IP_ADDRESS_RE = re.compile(r"\b(?:\d{1,3}\.){3}\d{1,3}\b")
SECRETISH_RE = re.compile(r"(?i)\b(api[_-]?key|secret|token|password|signature)=([^&\s]+)")


def env_float(name: str, default: float, *, minimum: float, maximum: float) -> float:
    try:
        value = float(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        value = default
    return max(minimum, min(maximum, value))


def env_int(name: str, default: int, *, minimum: int, maximum: int) -> int:
    try:
        value = int(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        value = default
    return max(minimum, min(maximum, value))


def public_error(message: str | None) -> str | None:
    if not message:
        return None
    text = IP_ADDRESS_RE.sub("[redacted-ip]", str(message))
    text = SECRETISH_RE.sub(r"\1=[redacted]", text)
    return text[:220]


BINANCE_FAPI = os.getenv("BINANCE_FAPI_BASE", "https://fapi.binance.com").rstrip("/")
BINANCE_WS = os.getenv("BINANCE_WS_BASE", "wss://fstream.binance.com/market/ws").rstrip("/")
REQUEST_TIMEOUT_SECONDS = env_float("SCREENER_REQUEST_TIMEOUT_SECONDS", 6, minimum=1, maximum=15)
REST_QUOTE_TTL_SECONDS = env_float("SCREENER_REST_QUOTE_TTL_SECONDS", 10, minimum=1, maximum=60)
DEEP_CACHE_TTL_SECONDS = env_float("SCREENER_DEEP_CACHE_TTL_SECONDS", 180, minimum=30, maximum=900)
DEEP_BATCH_INTERVAL_SECONDS = env_float("SCREENER_DEEP_BATCH_INTERVAL_SECONDS", 2, minimum=0.5, maximum=30)
STREAM_STALE_SECONDS = env_float("SCREENER_STREAM_STALE_SECONDS", 12, minimum=3, maximum=120)
WS_WARMUP_SECONDS = env_float("SCREENER_WS_WARMUP_SECONDS", 15, minimum=2, maximum=60)
DEEP_BATCH_SIZE = env_int("SCREENER_DEEP_BATCH_SIZE", 20, minimum=1, maximum=150)
DEEP_WORKERS = env_int("SCREENER_DEEP_WORKERS", 3, minimum=1, maximum=12)
MAX_ROWS = env_int("SCREENER_MAX_ROWS", 420, minimum=50, maximum=600)
ENABLE_WS = os.getenv("SCREENER_ENABLE_WS", "1").lower() in {"1", "true", "yes"}
PUBLIC_DIR = Path(__file__).resolve().parent / "public"
DEEP_METRIC_FIELDS = ("chg5m", "chg1h", "vol1h", "oiUsd", "oiChg1h", "volatility15m", "trades5m")
ROW_FIELD_DEFAULTS = {
    "price": None,
    "chg5m": None,
    "chg1h": None,
    "chg1d": None,
    "vol1h": None,
    "quoteVolume24h": None,
    "oiUsd": None,
    "oiChg1h": None,
    "fundingRatePct": None,
    "volatility15m": None,
    "trades5m": None,
}


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_utc(value: datetime | None = None) -> str:
    return (value or utc_now()).isoformat().replace("+00:00", "Z")


def num(value: Any, default: float | None = 0.0) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return default
    return parsed if parsed == parsed else default


def pct_change(current: float | None, previous: float | None) -> float | None:
    if current is None or previous in {None, 0}:
        return None
    return ((current / previous) - 1) * 100


def sum_numbers(values: list[float | None]) -> float:
    return sum(value for value in values if isinstance(value, (int, float)))


def compute_signal_score(row: dict[str, Any]) -> int:
    chg5m = abs(num(row.get("chg5m")) or 0)
    chg1h = abs(num(row.get("chg1h")) or 0)
    chg1d = abs(num(row.get("chg1d")) or 0)
    volatility = abs(num(row.get("volatility15m")) or 0)
    oi_change = abs(num(row.get("oiChg1h")) or 0)
    funding = abs(num(row.get("fundingRatePct")) or 0)

    score = 0.0
    score += min(chg5m * 16, 24)
    score += min(chg1h * 8, 22)
    score += min(chg1d * 1.2, 12)
    score += min(volatility * 10, 18)
    score += min(oi_change * 6, 18)
    score += min(funding * 500, 16)
    if (num(row.get("quoteVolume24h")) or 0) > 1_000_000_000:
        score += 5
    if (num(row.get("quoteVolume24h")) or 0) > 5_000_000_000:
        score += 5
    return int(max(0, min(100, round(score))))


class BinanceScreenerCache:
    def __init__(self) -> None:
        self.session = requests.Session()
        self.session.headers.update({
            "accept": "application/json",
            "user-agent": "binance-futures-screener/1.0",
        })
        self.lock = threading.RLock()
        self.ticker_by_symbol: dict[str, dict[str, Any]] = {}
        self.mark_by_symbol: dict[str, dict[str, Any]] = {}
        self.deep_cache: dict[str, dict[str, Any]] = {}
        self.rows: list[dict[str, Any]] = []
        self.generated_at: datetime | None = None
        self.deep_generated_at: datetime | None = None
        self.last_stream_message_at: float | None = None
        self.last_rest_refresh_at = 0.0
        self.last_error: str | None = None
        self.base_refreshing = False
        self.deep_refreshing = False
        self.streams_started = False
        self.streams_started_at: float | None = None
        self.rest_backoff_until = 0.0
        self.last_deep_batch_started_at = 0.0

    def ensure_streams(self) -> None:
        if not ENABLE_WS or websocket is None:
            return
        with self.lock:
            if self.streams_started:
                return
            self.streams_started = True
            self.streams_started_at = time.monotonic()

        self._start_stream("ticker", f"{BINANCE_WS}/!ticker@arr", self._handle_ticker_payload)
        self._start_stream("mark", f"{BINANCE_WS}/!markPrice@arr@1s", self._handle_mark_payload)

    def _start_stream(self, name: str, url: str, handler) -> None:
        thread = threading.Thread(target=self._run_stream, args=(name, url, handler), daemon=True)
        thread.start()

    def _run_stream(self, name: str, url: str, handler) -> None:
        while True:
            try:
                socket = websocket.WebSocketApp(
                    url,
                    on_message=lambda _ws, message: handler(json.loads(message)),
                    on_error=lambda _ws, error: self._record_error(f"{name} stream: {error}"),
                )
                socket.run_forever(ping_interval=20, ping_timeout=10)
                self._record_error(f"{name} stream disconnected")
            except Exception as exc:  # pragma: no cover - depends on live network
                self._record_error(f"{name} stream failed: {exc}")
            time.sleep(2.5)

    def _record_error(self, message: str) -> None:
        with self.lock:
            self.last_error = public_error(message)

    def _handle_ticker_payload(self, payload: Any) -> None:
        if isinstance(payload, dict) and isinstance(payload.get("data"), list):
            payload = payload["data"]
        rows = payload if isinstance(payload, list) else [payload]
        with self.lock:
            for row in rows:
                symbol = str(row.get("s", ""))
                if symbol.endswith("USDT") and "_" not in symbol and (num(row.get("c")) or 0) > 0:
                    self.ticker_by_symbol[symbol] = row
            self.last_stream_message_at = time.monotonic()
            self.last_error = None

    def _handle_mark_payload(self, payload: Any) -> None:
        if isinstance(payload, dict) and isinstance(payload.get("data"), list):
            payload = payload["data"]
        rows = payload if isinstance(payload, list) else [payload]
        with self.lock:
            for row in rows:
                symbol = str(row.get("s", ""))
                if symbol.endswith("USDT") and "_" not in symbol:
                    self.mark_by_symbol[symbol] = row
            self.last_stream_message_at = time.monotonic()
            self.last_error = None

    def snapshot(self) -> dict[str, Any]:
        self.ensure_streams()

        stream_rows = self._rows_from_stream()
        if stream_rows:
            rows = self._merge_cached_metrics(stream_rows)
            with self.lock:
                self.rows = rows
                self.generated_at = utc_now()
            self._maybe_start_deep_refresh(stream_rows)
            return self._payload(rows, source="binance_ws")

        if self._waiting_for_stream_warmup():
            return self._payload([], source="binance_ws_warming")

        should_block = not self._has_rows()
        if self._rest_refresh_due():
            self._refresh_rest(blocking=should_block)

        with self.lock:
            rows = list(self.rows)
        if rows:
            self._maybe_start_deep_refresh(rows)
        return self._payload(rows, source="binance_rest_cache")

    def _has_rows(self) -> bool:
        with self.lock:
            return bool(self.rows)

    def _stream_age_seconds(self) -> float | None:
        with self.lock:
            if self.last_stream_message_at is None:
                return None
            return max(0.0, time.monotonic() - self.last_stream_message_at)

    def _waiting_for_stream_warmup(self) -> bool:
        if not ENABLE_WS:
            return False
        with self.lock:
            if self.last_stream_message_at is not None:
                return False
            if self.streams_started_at is None:
                return False
            return time.monotonic() - self.streams_started_at < WS_WARMUP_SECONDS

    def _rows_from_stream(self) -> list[dict[str, Any]]:
        with self.lock:
            if not self.ticker_by_symbol:
                return []
            tickers = list(self.ticker_by_symbol.values())
            marks = dict(self.mark_by_symbol)

        rows = []
        for row in tickers:
            symbol = row.get("s")
            mark = marks.get(symbol, {})
            price = num(row.get("c"))
            if not symbol or not price:
                continue
            rows.append({
                "exchange": "binance",
                "venue": "Binance Futures",
                "symbol": symbol,
                "base": str(symbol).removesuffix("USDT"),
                "price": price,
                "markPrice": num(mark.get("p"), price),
                "chg1d": num(row.get("P"), None),
                "quoteVolume24h": num(row.get("q"), 0),
                "volume24hBase": num(row.get("v"), 0),
                "fundingRatePct": (num(mark.get("r"), 0) or 0) * 100 if mark else None,
                "nextFundingTime": num(mark.get("T"), None),
            })
        return sorted(rows, key=lambda item: item.get("quoteVolume24h") or 0, reverse=True)

    def _rest_refresh_due(self) -> bool:
        with self.lock:
            if self.base_refreshing:
                return False
            return time.monotonic() - self.last_rest_refresh_at >= REST_QUOTE_TTL_SECONDS

    def _refresh_rest(self, blocking: bool) -> None:
        with self.lock:
            if self.base_refreshing:
                return
            self.base_refreshing = True

        if blocking:
            self._refresh_rest_worker()
            return

        threading.Thread(target=self._refresh_rest_worker, daemon=True).start()

    def _refresh_rest_worker(self) -> None:
        try:
            rows = self._fetch_rest_base_rows()
            merged = self._merge_cached_metrics(rows)
            with self.lock:
                self.rows = merged
                self.generated_at = utc_now()
                self.last_rest_refresh_at = time.monotonic()
                self.last_error = None
            self._maybe_start_deep_refresh(rows)
        except Exception as exc:
            self._record_error(str(exc))
            with self.lock:
                self.last_rest_refresh_at = time.monotonic()
        finally:
            with self.lock:
                self.base_refreshing = False

    def _fetch_rest_base_rows(self) -> list[dict[str, Any]]:
        if time.monotonic() < self.rest_backoff_until:
            raise RuntimeError("Binance REST is paused after a rate-limit response; waiting for WebSocket data")

        tickers = self._get("/fapi/v1/ticker/24hr")
        premiums = self._get("/fapi/v1/premiumIndex")
        premium_by_symbol = {
            row.get("symbol"): row
            for row in premiums
            if isinstance(row, dict) and row.get("symbol")
        } if isinstance(premiums, list) else {}

        rows = []
        for row in tickers if isinstance(tickers, list) else []:
            symbol = str(row.get("symbol", ""))
            price = num(row.get("lastPrice"))
            if not symbol.endswith("USDT") or "_" in symbol or not price:
                continue
            premium = premium_by_symbol.get(symbol, {})
            rows.append({
                "exchange": "binance",
                "venue": "Binance Futures",
                "symbol": symbol,
                "base": symbol.removesuffix("USDT"),
                "price": price,
                "markPrice": num(premium.get("markPrice"), price),
                "chg1d": num(row.get("priceChangePercent"), None),
                "quoteVolume24h": num(row.get("quoteVolume"), 0),
                "volume24hBase": num(row.get("volume"), 0),
                "fundingRatePct": (num(premium.get("lastFundingRate"), 0) or 0) * 100 if premium else None,
                "nextFundingTime": num(premium.get("nextFundingTime"), None),
            })
        return sorted(rows, key=lambda item: item.get("quoteVolume24h") or 0, reverse=True)

    def _get(self, path: str, params: dict[str, Any] | None = None) -> Any:
        if time.monotonic() < self.rest_backoff_until:
            raise RuntimeError("Binance REST is paused after a rate-limit response; waiting for WebSocket data")

        response = self.session.get(
            f"{BINANCE_FAPI}{path}",
            params=params,
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
        if response.status_code in {418, 429}:
            self._set_rest_backoff(response.text, default_seconds=300 if response.status_code == 418 else 60)
        if not response.ok:
            message = response.text[:180].replace("\n", " ")
            raise RuntimeError(f"Binance {path} returned {response.status_code}: {message}")
        return response.json()

    def _set_rest_backoff(self, body: str, default_seconds: int) -> None:
        wait_seconds = default_seconds
        match = re.search(r"banned until (\d{13})", body or "")
        if match:
            ban_until_epoch = int(match.group(1)) / 1000
            wait_seconds = max(default_seconds, int(ban_until_epoch - time.time()) + 30)
        self.rest_backoff_until = time.monotonic() + wait_seconds

    def _maybe_start_deep_refresh(self, base_rows: list[dict[str, Any]]) -> None:
        now = time.monotonic()
        with self.lock:
            if self.deep_refreshing:
                return
            if now < self.rest_backoff_until:
                return
            if now - self.last_deep_batch_started_at < DEEP_BATCH_INTERVAL_SECONDS:
                return
            rows = self._select_deep_rows_locked(base_rows, now)
            if not rows:
                return
            self.last_deep_batch_started_at = now
            self.deep_refreshing = True

        threading.Thread(target=self._refresh_deep_worker, args=(rows,), daemon=True).start()

    def _select_deep_rows_locked(self, base_rows: list[dict[str, Any]], now: float) -> list[dict[str, Any]]:
        uncached = []
        stale = []
        for row in base_rows:
            symbol = str(row.get("symbol", ""))
            if not symbol:
                continue
            cached = self.deep_cache.get(symbol)
            if not cached:
                uncached.append(row)
            elif now - float(cached.get("ts", 0.0)) >= DEEP_CACHE_TTL_SECONDS:
                stale.append(row)
        return (uncached + stale)[:DEEP_BATCH_SIZE]

    def _refresh_deep_worker(self, rows: list[dict[str, Any]]) -> None:
        try:
            with ThreadPoolExecutor(max_workers=max(1, DEEP_WORKERS)) as executor:
                futures = {executor.submit(self._fetch_deep_metrics, row): row for row in rows}
                for future in as_completed(futures):
                    row = futures[future]
                    try:
                        metrics = future.result()
                    except Exception as exc:
                        self._record_error(f"{row.get('symbol')} deep metrics: {exc}")
                        continue
                    if not any(key in metrics for key in DEEP_METRIC_FIELDS):
                        self._record_error(f"{row.get('symbol')} deep metrics are still warming")
                        continue
                    with self.lock:
                        self.deep_cache[str(row["symbol"])] = {
                            "ts": time.monotonic(),
                            "data": metrics,
                        }
            with self.lock:
                self.deep_generated_at = utc_now()
                self.rows = self._merge_cached_metrics(self.rows)
        finally:
            with self.lock:
                self.deep_refreshing = False

    def _fetch_deep_metrics(self, row: dict[str, Any]) -> dict[str, Any]:
        symbol = row["symbol"]
        metrics: dict[str, Any] = {}

        try:
            klines = self._get("/fapi/v1/klines", {"symbol": symbol, "interval": "1m", "limit": 65})
            metrics.update(metrics_from_binance_klines(klines))
        except Exception as exc:
            self._record_error(f"{symbol} kline metrics unavailable: {exc}")

        try:
            oi = self._get("/fapi/v1/openInterest", {"symbol": symbol})
            open_interest = num(oi.get("openInterest"), None) if isinstance(oi, dict) else None
            if open_interest and row.get("price"):
                metrics["oiUsd"] = open_interest * float(row["price"])
        except Exception as exc:
            self._record_error(f"{symbol} open interest unavailable: {exc}")

        try:
            history = self._get(
                "/futures/data/openInterestHist",
                {"symbol": symbol, "period": "5m", "limit": 13},
            )
            metrics.update(metrics_from_open_interest_history(history if isinstance(history, list) else []))
        except Exception as exc:
            self._record_error(f"{symbol} open interest history unavailable: {exc}")

        return metrics

    def _merge_cached_metrics(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        with self.lock:
            deep_cache = dict(self.deep_cache)

        merged_rows = []
        now = time.monotonic()
        for row in rows:
            merged = dict(row)
            cached = deep_cache.get(str(row.get("symbol")))
            if cached:
                merged.update(cached.get("data") or {})
            for key, fallback in ROW_FIELD_DEFAULTS.items():
                merged.setdefault(key, fallback)
            merged["deepHydrated"] = any(key in merged and merged[key] is not None for key in DEEP_METRIC_FIELDS)
            merged["deepStale"] = bool(cached and now - float(cached.get("ts", 0.0)) >= DEEP_CACHE_TTL_SECONDS)
            merged["score"] = compute_signal_score(merged)
            merged_rows.append(merged)
        return sorted(
            merged_rows,
            key=lambda item: (
                item.get("score") or 0,
                item.get("quoteVolume24h") or 0,
                abs(num(item.get("chg1d")) or 0),
                str(item.get("symbol") or ""),
            ),
            reverse=True,
        )

    def _payload(self, rows: list[dict[str, Any]], source: str) -> dict[str, Any]:
        with self.lock:
            generated_at = self.generated_at
            deep_generated_at = self.deep_generated_at
            last_error = self.last_error
            deep_refreshing = self.deep_refreshing
            base_refreshing = self.base_refreshing
            deep_cache = dict(self.deep_cache)

        stream_age = self._stream_age_seconds()
        cache_age = (utc_now() - generated_at).total_seconds() if generated_at else None
        visible_rows = rows[:MAX_ROWS]
        deep_hydrated_count = sum(1 for row in visible_rows if str(row.get("symbol")) in deep_cache)
        if rows and (stream_age is None or stream_age <= STREAM_STALE_SECONDS):
            status = "live"
        elif rows:
            status = "stale"
        elif last_error:
            status = "error"
        else:
            status = "warming"

        return {
            "status": status,
            "exchange": "binance",
            "venue": "Binance Futures",
            "source": source,
            "generatedAt": iso_utc(generated_at) if generated_at else None,
            "deepGeneratedAt": iso_utc(deep_generated_at) if deep_generated_at else None,
            "cacheAgeMs": round(cache_age * 1000) if cache_age is not None else None,
            "streamAgeMs": round(stream_age * 1000) if stream_age is not None else None,
            "quoteRefreshMs": 1000,
            "deepRefreshMs": round(DEEP_CACHE_TTL_SECONDS * 1000),
            "deepBatchSize": DEEP_BATCH_SIZE,
            "deepHydratedCount": deep_hydrated_count,
            "deepQueuedCount": max(0, len(visible_rows) - deep_hydrated_count),
            "deepTotalRows": len(visible_rows),
            "baseRefreshing": base_refreshing,
            "deepRefreshing": deep_refreshing,
            "lastError": public_error(last_error),
            "rows": visible_rows,
        }


def metrics_from_binance_klines(rows: list[Any]) -> dict[str, Any]:
    candles = []
    for row in rows:
        try:
            candles.append({
                "open": num(row[1], None),
                "high": num(row[2], None),
                "low": num(row[3], None),
                "close": num(row[4], None),
                "quoteVolume": num(row[7], 0),
                "trades": num(row[8], 0),
            })
        except (IndexError, TypeError):
            continue
    candles = [row for row in candles if row.get("close")]
    return metrics_from_candles(candles)


def metrics_from_candles(candles: list[dict[str, Any]]) -> dict[str, Any]:
    if not candles:
        return {}
    last = candles[-1]

    def ago(minutes: int) -> dict[str, Any]:
        return candles[max(0, len(candles) - 1 - minutes)]

    last5 = candles[-5:]
    last15 = candles[-15:]
    last60 = candles[-60:]
    high15 = max(row.get("high") or 0 for row in last15)
    low15 = min(row.get("low") or 0 for row in last15)
    close = last.get("close")

    return {
        "chg5m": pct_change(close, ago(5).get("close")),
        "chg1h": pct_change(close, ago(60).get("close")),
        "vol1h": sum_numbers([row.get("quoteVolume") for row in last60]),
        "volatility15m": ((high15 - low15) / close) * 100 if close else None,
        "trades5m": sum_numbers([row.get("trades") for row in last5]),
    }


def metrics_from_open_interest_history(history: list[dict[str, Any]]) -> dict[str, Any]:
    rows = []
    for row in history:
        value = num(row.get("sumOpenInterestValue"), None)
        contracts = num(row.get("sumOpenInterest"), None)
        if value or contracts:
            rows.append({"value": value, "contracts": contracts})
    if len(rows) < 2:
        return {}

    first = rows[0]
    last = rows[-1]
    metric = "value" if first.get("value") and last.get("value") else "contracts"
    output = {"oiChg1h": pct_change(last.get(metric), first.get(metric))}
    if last.get("value"):
        output["oiUsd"] = last["value"]
    return output


cache = BinanceScreenerCache()
app = Flask(__name__, static_folder=None)
server = app


@app.after_request
def add_cors_headers(response):
    origin = request.headers.get("Origin")
    allowed = os.getenv("SCREENER_ALLOWED_ORIGINS", "*")
    allowed_origins = {item.strip() for item in allowed.split(",") if item.strip()}
    if allowed == "*" or "*" in allowed_origins:
        response.headers["Access-Control-Allow-Origin"] = origin or "*"
    elif origin in allowed_origins:
        response.headers["Access-Control-Allow-Origin"] = origin
    response.headers["Vary"] = "Origin"
    response.headers["Access-Control-Allow-Methods"] = "GET, OPTIONS"
    response.headers["Access-Control-Allow-Headers"] = "Content-Type"
    response.headers["Cache-Control"] = "no-store"
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["Referrer-Policy"] = "no-referrer"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["Permissions-Policy"] = "camera=(), microphone=(), geolocation=()"
    response.headers["Content-Security-Policy"] = (
        "default-src 'self'; "
        "script-src 'self' https://unpkg.com; "
        "style-src 'self'; "
        "img-src 'self' data:; "
        "connect-src 'self' https://binance-futures-screener-a39v.onrender.com http://127.0.0.1:8050 http://localhost:8050; "
        "font-src 'self'; "
        "base-uri 'self'; "
        "form-action 'none'; "
        "frame-ancestors 'none'"
    )
    return response


@app.route("/api/screener", methods=["GET", "OPTIONS"])
def api_screener():
    if request.method == "OPTIONS":
        return make_response("", 204)
    return jsonify(cache.snapshot())


@app.get("/api/healthz")
@app.get("/healthz")
def healthz():
    return jsonify({"status": "ok", "service": "binance-futures-screener", "time": iso_utc()})


@app.get("/")
def root():
    return send_from_directory(PUBLIC_DIR, "index.html")


@app.get("/<path:filename>")
def public_static(filename: str):
    return send_from_directory(PUBLIC_DIR, filename)


if __name__ == "__main__":
    host = os.getenv("SCREENER_API_HOST", os.getenv("DASH_HOST", "127.0.0.1"))
    port = int(os.getenv("SCREENER_API_PORT", os.getenv("PORT", "8050")))
    debug = os.getenv("SCREENER_API_DEBUG", "0").lower() in {"1", "true", "yes"}
    app.run(host=host, port=port, debug=debug, threaded=True)
