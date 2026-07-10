from __future__ import annotations

import os
import re
import threading
import time
from datetime import datetime, timezone
from typing import Any

import requests
from flask import Flask, jsonify, make_response, request


BINANCE_FAPI = os.getenv("BINANCE_FAPI_BASE", "https://fapi.binance.com").rstrip("/")
TOKEN = os.getenv("DEEP_PROXY_TOKEN", "")
REQUIRE_TOKEN = os.getenv("DEEP_PROXY_REQUIRE_TOKEN", "1").lower() in {"1", "true", "yes"}
USER_AGENT = os.getenv("DEEP_PROXY_USER_AGENT", "binance-futures-deep-proxy/1.0")
DEEP_METRIC_FIELDS = ("chg5m", "chg1h", "vol1h", "oiUsd", "oiChg1h", "volatility15m", "trades5m")


def env_float(name: str, default: float, *, minimum: float, maximum: float) -> float:
    try:
        value = float(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        value = default
    return max(minimum, min(maximum, value))


def env_int(name: str, default: int, *, minimum: int, maximum: int) -> int:
    try:
        value = int(float(os.getenv(name, str(default))))
    except (TypeError, ValueError):
        value = default
    return max(minimum, min(maximum, value))


REQUEST_TIMEOUT_SECONDS = env_float("DEEP_PROXY_REQUEST_TIMEOUT_SECONDS", 6, minimum=1, maximum=20)
MIN_UPSTREAM_INTERVAL_SECONDS = env_float("DEEP_PROXY_MIN_UPSTREAM_INTERVAL_SECONDS", 0.35, minimum=0.05, maximum=10)
CACHE_TTL_SECONDS = env_int("DEEP_PROXY_CACHE_TTL_SECONDS", 180, minimum=30, maximum=3600)


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


class DeepMetricsProxy:
    def __init__(self) -> None:
        self.session = requests.Session()
        self.session.headers.update({"accept": "application/json", "user-agent": USER_AGENT})
        self.lock = threading.RLock()
        self.cache: dict[str, dict[str, Any]] = {}
        self.last_upstream_at = 0.0
        self.upstream_backoff_until = 0.0
        self.upstream_backoff_until_epoch: float | None = None
        self.last_error: str | None = None

    def get_metrics(self, symbol: str, price: float | None) -> tuple[dict[str, Any], int, dict[str, str]]:
        symbol = self._normalize_symbol(symbol)
        if not symbol:
            return {"error": "invalid symbol"}, 400, {}

        now = time.monotonic()
        with self.lock:
            cached = self.cache.get(symbol)
            if cached and now - float(cached.get("ts", 0.0)) < CACHE_TTL_SECONDS:
                return self._payload(symbol, cached["metrics"], cached=True), 200, {}
            if now < self.upstream_backoff_until:
                retry_after = max(1, round(self.upstream_backoff_until - now))
                return self._paused_payload(retry_after), 503, {"Retry-After": str(retry_after)}

        try:
            metrics = self._fetch_from_binance(symbol, price)
        except Exception as exc:
            with self.lock:
                self.last_error = str(exc)[:220]
            return {"error": "upstream unavailable", "detail": str(exc)[:180]}, 502, {}

        with self.lock:
            self.cache[symbol] = {"ts": time.monotonic(), "metrics": metrics}
            self.last_error = None
        return self._payload(symbol, metrics, cached=False), 200, {}

    def _normalize_symbol(self, symbol: str) -> str:
        value = str(symbol or "").upper().strip()
        if not re.fullmatch(r"[A-Z0-9]{2,30}USDT", value):
            return ""
        return value

    def _fetch_from_binance(self, symbol: str, price: float | None) -> dict[str, Any]:
        klines = self._get("/fapi/v1/klines", {"symbol": symbol, "interval": "1m", "limit": 65})
        metrics = metrics_from_binance_klines(klines if isinstance(klines, list) else [])

        oi = self._get("/fapi/v1/openInterest", {"symbol": symbol})
        open_interest = num(oi.get("openInterest"), None) if isinstance(oi, dict) else None
        if open_interest and price:
            metrics["oiUsd"] = open_interest * price

        history = self._get("/futures/data/openInterestHist", {"symbol": symbol, "period": "5m", "limit": 13})
        metrics.update(metrics_from_open_interest_history(history if isinstance(history, list) else []))
        return {key: metrics.get(key) for key in DEEP_METRIC_FIELDS if metrics.get(key) is not None}

    def _get(self, path: str, params: dict[str, Any]) -> Any:
        self._wait_for_slot()
        response = self.session.get(
            f"{BINANCE_FAPI}{path}",
            params=params,
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
        if response.status_code in {418, 429}:
            self._set_upstream_backoff(response.text, default_seconds=300 if response.status_code == 418 else 60)
        if not response.ok:
            message = response.text[:180].replace("\n", " ")
            raise RuntimeError(f"Binance {path} returned {response.status_code}: {message}")
        return response.json()

    def _wait_for_slot(self) -> None:
        with self.lock:
            now = time.monotonic()
            if now < self.upstream_backoff_until:
                retry_after = max(1, round(self.upstream_backoff_until - now))
                raise RuntimeError(f"Binance REST paused, retry after {retry_after}s")
            wait_seconds = max(0.0, MIN_UPSTREAM_INTERVAL_SECONDS - (now - self.last_upstream_at))
            if wait_seconds:
                time.sleep(wait_seconds)
            self.last_upstream_at = time.monotonic()

    def _set_upstream_backoff(self, body: str, default_seconds: int) -> None:
        wait_seconds = default_seconds
        match = re.search(r"banned until (\d{13})", body or "")
        if match:
            ban_until_epoch = int(match.group(1)) / 1000
            wait_seconds = max(default_seconds, int(ban_until_epoch - time.time()) + 30)
        with self.lock:
            self.upstream_backoff_until = time.monotonic() + wait_seconds
            self.upstream_backoff_until_epoch = time.time() + wait_seconds

    def _payload(self, symbol: str, metrics: dict[str, Any], *, cached: bool) -> dict[str, Any]:
        return {
            "status": "ok",
            "symbol": symbol,
            "source": "binance_rest",
            "cached": cached,
            "generatedAt": iso_utc(),
            "metrics": metrics,
        }

    def _paused_payload(self, retry_after: int) -> dict[str, Any]:
        retry_at = time.time() + retry_after
        return {
            "status": "paused",
            "source": "binance_rest",
            "retryAfterSeconds": retry_after,
            "retryAt": iso_utc(datetime.fromtimestamp(retry_at, timezone.utc)),
            "error": "Binance REST is paused for this proxy IP",
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


proxy = DeepMetricsProxy()
app = Flask(__name__)
server = app


@app.after_request
def add_headers(response):
    response.headers["Cache-Control"] = "no-store"
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["Referrer-Policy"] = "no-referrer"
    response.headers["Permissions-Policy"] = "camera=(), microphone=(), geolocation=()"
    return response


def authorized() -> bool:
    if not REQUIRE_TOKEN:
        return True
    if not TOKEN:
        return False
    return request.headers.get("Authorization") == f"Bearer {TOKEN}"


@app.get("/api/deep")
def api_deep():
    if not authorized():
        return make_response(jsonify({"error": "proxy token required"}), 401)
    price = num(request.args.get("price"), None)
    payload, status, headers = proxy.get_metrics(request.args.get("symbol", ""), price)
    response = make_response(jsonify(payload), status)
    for key, value in headers.items():
        response.headers[key] = value
    return response


@app.get("/api/healthz")
@app.get("/healthz")
def healthz():
    return jsonify({
        "status": "ok",
        "service": "binance-futures-deep-proxy",
        "time": iso_utc(),
        "tokenRequired": REQUIRE_TOKEN,
        "tokenConfigured": bool(TOKEN),
        "cacheSize": len(proxy.cache),
    })


if __name__ == "__main__":
    host = os.getenv("DEEP_PROXY_HOST", "127.0.0.1")
    port = int(os.getenv("DEEP_PROXY_PORT", os.getenv("PORT", "8060")))
    debug = os.getenv("DEEP_PROXY_DEBUG", "0").lower() in {"1", "true", "yes"}
    app.run(host=host, port=port, debug=debug, threaded=True)
