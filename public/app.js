const DEFAULT_REMOTE_API_BASE = "https://binance-futures-screener-a39v.onrender.com";

const CONFIG = {
  quoteRefreshMs: 1_000,
  requestTimeoutMs: 8_000,
  maxRenderedRows: 240,
  browserDeepEnabled: true,
  browserDeepMaxSymbols: 90,
  browserDeepCacheTtlMs: 180_000,
  browserDeepConcurrency: 2,
};

const BINANCE_BROWSER_BASES = [
  "https://www.binance.com",
  "https://fapi.binance.com",
];

const EXCHANGE_LABELS = {
  binance: "Binance Futures",
};

const COLUMNS = [
  { key: "symbol", label: "Symbol", title: "Perpetual futures market" },
  { key: "price", label: "Price", title: "Latest mark or traded price" },
  { key: "chg5m", label: "Chg % (5m)", title: "Close-to-close move over roughly five minutes" },
  { key: "chg1h", label: "Chg % (1h)", title: "One-hour close-to-close move" },
  { key: "chg1d", label: "Chg % (1d)", title: "Twenty-four hour percentage move" },
  { key: "vol1h", label: "Vol (1h)", title: "Approximate one-hour notional volume" },
  { key: "quoteVolume24h", label: "Vol (1d)", title: "Twenty-four hour notional volume" },
  { key: "oiUsd", label: "OI $", title: "Open interest converted to notional value" },
  { key: "oiChg1h", label: "OI Chg % (1h)", title: "Approximate one-hour open interest change" },
  { key: "fundingRatePct", label: "Funding", title: "Current funding rate as a percentage" },
  { key: "volatility15m", label: "Vlt (15m)", title: "High-low range over the last 15 minutes" },
  { key: "trades5m", label: "Trd (5m)", title: "Trade count over the last five minutes" },
  { key: "score", label: "Sig", title: "0-100 anomaly signal score" },
];

const DEEP_METRIC_KEYS = new Set(["chg5m", "chg1h", "vol1h", "oiUsd", "oiChg1h", "volatility15m", "trades5m"]);

const state = {
  exchange: "binance",
  preset: "all",
  search: "",
  sortKey: "score",
  sortDir: "desc",
  rows: [],
  previousPrices: new Map(),
  loading: false,
  requestToken: 0,
  quoteTimer: null,
  clockTimer: null,
  lastQuoteRefresh: null,
  payload: null,
  apiBase: resolveApiBase(),
  browserDeepCache: new Map(),
  browserDeepQueued: new Set(),
  browserDeepInflight: new Set(),
  browserDeepQueue: [],
  browserDeepLastError: null,
  browserDeepUpdatedAt: null,
};

const els = {};

document.addEventListener("DOMContentLoaded", () => {
  bindElements();
  renderTableHead();
  bindEvents();
  updateClock();
  state.clockTimer = window.setInterval(updateClock, 1_000);
  state.quoteTimer = window.setInterval(() => refreshData(false), CONFIG.quoteRefreshMs);
  refreshData(true);
  window.screenerState = state;
  window.lucide?.createIcons();
});

function bindElements() {
  els.clock = document.getElementById("utc-clock");
  els.status = document.getElementById("status-pill");
  els.search = document.getElementById("search-input");
  els.refreshButton = document.getElementById("refresh-button");
  els.tableHead = document.getElementById("table-head");
  els.tableBody = document.getElementById("table-body");
  els.tableTitle = document.getElementById("table-title");
  els.lastRefresh = document.getElementById("last-refresh");
  els.metricSymbols = document.getElementById("metric-symbols");
  els.metricVolume = document.getElementById("metric-volume");
  els.metricMove = document.getElementById("metric-move");
  els.metricSignals = document.getElementById("metric-signals");
}

function bindEvents() {
  document.querySelectorAll("[data-preset]").forEach((button) => {
    button.addEventListener("click", () => {
      state.preset = button.dataset.preset || "all";
      updateActiveButtons("[data-preset]", state.preset);
      renderAll();
    });
  });

  els.search.addEventListener("input", () => {
    state.search = els.search.value.trim().toUpperCase();
    renderAll();
  });

  els.refreshButton.addEventListener("click", () => refreshData(true));

  els.tableHead.addEventListener("click", (event) => {
    const button = event.target.closest("[data-sort]");
    if (!button) return;
    const key = button.dataset.sort;
    if (state.sortKey === key) {
      state.sortDir = state.sortDir === "asc" ? "desc" : "asc";
    } else {
      state.sortKey = key === "symbol" ? "symbol" : key;
      state.sortDir = key === "symbol" ? "asc" : "desc";
    }
    renderAll();
  });
}

function updateActiveButtons(selector, activeValue) {
  document.querySelectorAll(selector).forEach((button) => {
    const isActive = button.dataset.exchange === activeValue || button.dataset.preset === activeValue;
    button.classList.toggle("is-active", isActive);
    if (button.hasAttribute("aria-selected")) {
      button.setAttribute("aria-selected", String(isActive));
    }
  });
}

async function refreshData(manual) {
  if (state.loading && !manual) return;

  const token = ++state.requestToken;
  state.loading = true;
  els.refreshButton.classList.add("is-spinning");
  if (manual || !state.rows.length) {
    setStatus("loading", "Refreshing backend Binance feed...");
  }

  try {
    const payload = await fetchScreenerPayload();
    if (token !== state.requestToken) return;

    state.payload = payload;
    state.lastQuoteRefresh = payload.generatedAt ? Date.parse(payload.generatedAt) : Date.now();
    state.rows = normalizeRows(payload.rows || []);
    scheduleBrowserDeepHydration();
    renderAll();
    updateStatusFromPayload(payload);
  } catch (error) {
    console.error(error);
    if (token === state.requestToken) {
      setStatus("error", `Backend feed error: ${error.message || "request failed"}`);
      renderError(error);
    }
  } finally {
    if (token === state.requestToken) {
      state.loading = false;
      els.refreshButton.classList.remove("is-spinning");
    }
  }
}

async function fetchScreenerPayload() {
  const payload = await fetchJson(apiUrl("/api/screener"));
  if (!payload || !Array.isArray(payload.rows)) {
    throw new Error("Screener API returned an invalid payload");
  }
  return payload;
}

function normalizeRows(rows) {
  pruneBrowserDeepCache();
  return rows.map((row) => {
    const merged = mergeBrowserDeepMetrics(row);
    return {
      ...merged,
      score: computeSignalScore(merged),
    };
  });
}

function updateStatusFromPayload(payload) {
  const source = payload.source === "binance_ws" ? "WebSocket cache" : "REST cache";
  if (payload.status === "live") {
    const sourceNote = payload.deepProxyEnabled ? " | deep via proxy" : "";
    const pausedNote = payload.deepStatus === "paused" ? ` | ${deepSourceLabel(payload)} paused${deepRetryLabel(payload)}` : "";
    const browserNote = shouldUseBrowserDeepFallback(payload) ? " | browser deep fill" : "";
    setStatus("live", `Binance live via ${source}${sourceNote}${browserNote}${pausedNote} | UI 1s | UTC`);
    return;
  }
  if (payload.status === "stale") {
    setStatus("loading", `Showing stale backend cache | retrying Binance feed`);
    return;
  }
  if (payload.status === "warming") {
    setStatus("loading", "Backend warming Binance feed...");
    return;
  }
  setStatus("error", `Backend feed error: ${payload.lastError || "no live rows"}`);
}

function renderAll() {
  const filtered = sortRows(filterRows(state.rows));
  updateTableMeta(filtered);
  renderMetrics(filtered);
  renderTable(filtered.slice(0, CONFIG.maxRenderedRows));
}

function filterRows(rows) {
  let output = rows;

  if (state.search) {
    output = output.filter((row) => row.symbol.includes(state.search) || row.base?.includes(state.search));
  }

  if (state.preset === "volume") {
    output = [...output]
      .sort((a, b) => (b.vol1h || b.quoteVolume24h || 0) - (a.vol1h || a.quoteVolume24h || 0))
      .slice(0, 80);
  } else if (state.preset === "oi") {
    output = output.filter((row) => Math.abs(num(row.oiChg1h)) >= 1);
  } else if (state.preset === "movers") {
    output = output.filter((row) => Math.abs(num(row.chg5m)) >= 0.35 || Math.abs(num(row.chg1h)) >= 1.2 || Math.abs(num(row.chg1d)) >= 6);
  } else if (state.preset === "funding") {
    output = output.filter((row) => Math.abs(num(row.fundingRatePct)) >= 0.025);
  }

  return output;
}

function sortRows(rows) {
  const sorted = [...rows];
  const dir = state.sortDir === "asc" ? 1 : -1;

  sorted.sort((a, b) => {
    if (state.sortKey === "symbol") {
      return a.symbol.localeCompare(b.symbol) * dir;
    }
    const aValue = sortableValue(a[state.sortKey], state.sortDir);
    const bValue = sortableValue(b[state.sortKey], state.sortDir);
    return (aValue - bValue) * dir;
  });

  return sorted;
}

function sortableValue(value, direction) {
  if (isFiniteNumber(value)) return Number(value);
  return direction === "asc" ? Number.POSITIVE_INFINITY : Number.NEGATIVE_INFINITY;
}

function renderTableHead() {
  els.tableHead.innerHTML = COLUMNS.map((column) => {
    const classes = [
      state.sortKey === column.key ? "is-sorted" : "",
      state.sortKey === column.key && state.sortDir === "asc" ? "is-asc" : "",
    ].filter(Boolean).join(" ");
    return `<th><button type="button" class="${classes}" data-sort="${column.key}" title="${escapeHtml(column.title)}">${escapeHtml(column.label)}</button></th>`;
  }).join("");
}

function renderTable(rows) {
  renderTableHead();

  if (!state.rows.length && state.loading) {
    renderLoadingState();
    return;
  }

  if (!rows.length) {
    const message = state.payload?.lastError
      ? `No symbols available yet. Backend says: ${escapeHtml(state.payload.lastError)}`
      : "No symbols match the current filter.";
    els.tableBody.innerHTML = `<tr><td colspan="${COLUMNS.length}"><div class="empty-state">${message}</div></td></tr>`;
    return;
  }

  const html = rows.map((row) => {
    const oldPrice = state.previousPrices.get(row.symbol);
    const isUpdated = oldPrice && row.price && Math.abs((row.price - oldPrice) / oldPrice) > 0.000005;
    return `<tr class="${isUpdated ? "is-updated" : ""}">
      ${COLUMNS.map((column) => `<td>${renderCell(row, column.key)}</td>`).join("")}
    </tr>`;
  }).join("");

  els.tableBody.innerHTML = html;
  state.previousPrices = new Map(state.rows.map((row) => [row.symbol, row.price]));
}

function renderCell(row, key) {
  if (key === "symbol") {
    return `<div class="symbol-cell"><strong>${escapeHtml(row.symbol)}</strong><span>${escapeHtml(row.venue || "Binance Futures")}</span></div>`;
  }
  if (DEEP_METRIC_KEYS.has(key) && !row.deepHydrated) return deepMetricPlaceholder(row, key);
  if (key === "price") return formatPrice(row.price);
  if (key === "chg5m" || key === "chg1h" || key === "chg1d" || key === "oiChg1h") return pctCell(row[key], 2);
  if (key === "vol1h" || key === "quoteVolume24h" || key === "oiUsd") return formatUsd(row[key]);
  if (key === "fundingRatePct") return fundingCell(row[key]);
  if (key === "volatility15m") return pctCell(row[key], 2, false);
  if (key === "trades5m") return formatInteger(row[key]);
  if (key === "score") return scoreBadge(row.score);
  return unavailableValue();
}

function renderMetrics(rows) {
  const sourceCount = state.rows.length;
  els.metricSymbols.textContent = sourceCount ? `${rows.length}/${sourceCount}` : "--";

  const topVolume = rows.reduce((best, row) => row.quoteVolume24h > (best?.quoteVolume24h || 0) ? row : best, null);
  els.metricVolume.textContent = topVolume ? `${topVolume.symbol} ${formatUsdText(topVolume.quoteVolume24h)}` : "--";

  const topMover = rows.reduce((best, row) => {
    const move = Math.abs(num(row.chg1h || row.chg1d));
    const bestMove = Math.abs(num(best?.chg1h || best?.chg1d));
    return move > bestMove ? row : best;
  }, null);
  els.metricMove.textContent = topMover ? `${topMover.symbol} ${signedText(topMover.chg1h ?? topMover.chg1d, 2)}%` : "--";

  const highSignals = rows.filter((row) => row.score >= 70).length;
  els.metricSignals.textContent = sourceCount ? String(highSignals) : "--";
}

function updateTableMeta(rows) {
  els.tableTitle.textContent = EXCHANGE_LABELS[state.exchange];
  const deepStatus = state.payload?.deepStatus;
  const browserCount = [...state.browserDeepCache.values()]
    .filter((entry) => Date.now() - entry.ts < CONFIG.browserDeepCacheTtlMs)
    .length;
  const browserText = shouldUseBrowserDeepFallback(state.payload) && browserCount
    ? ` | browser-filled ${browserCount}`
    : "";
  const deepText = deepStatus === "paused"
    ? ` | ${deepSourceLabel(state.payload)} metrics paused${deepRetryLabel(state.payload)}`
    : deepStatus === "hydrating"
      ? ` | ${deepSourceLabel(state.payload)} metrics ${state.payload.deepHydratedCount}/${state.payload.deepTotalRows}`
      : "";
  els.lastRefresh.textContent = state.lastQuoteRefresh
    ? `Last backend quote ${utcTime(state.lastQuoteRefresh)}${deepText}${browserText}`
    : "Waiting for backend data";
}

function shouldUseBrowserDeepFallback(payload) {
  if (!CONFIG.browserDeepEnabled || !payload) return false;
  return payload.deepStatus === "paused" ||
    payload.deepRestPaused ||
    payload.deepHydratedCount < Math.min(payload.deepTotalRows || 0, CONFIG.browserDeepMaxSymbols);
}

function pruneBrowserDeepCache() {
  const now = Date.now();
  for (const [symbol, entry] of state.browserDeepCache.entries()) {
    if (now - entry.ts > CONFIG.browserDeepCacheTtlMs * 2) {
      state.browserDeepCache.delete(symbol);
    }
  }
}

function mergeBrowserDeepMetrics(row) {
  const cached = state.browserDeepCache.get(row.symbol);
  if (!cached || Date.now() - cached.ts > CONFIG.browserDeepCacheTtlMs) return row;
  const merged = { ...row, ...cached.metrics };
  merged.deepHydrated = row.deepHydrated || Object.keys(cached.metrics).some((key) => DEEP_METRIC_KEYS.has(key));
  merged.deepFilledBy = row.deepHydrated ? row.deepFilledBy : "browser";
  return merged;
}

function scheduleBrowserDeepHydration() {
  if (!shouldUseBrowserDeepFallback(state.payload)) return;
  const now = Date.now();
  const targets = sortRows([...state.rows])
    .filter((row) => row.symbol && row.price)
    .slice(0, CONFIG.browserDeepMaxSymbols);

  for (const row of targets) {
    const cached = state.browserDeepCache.get(row.symbol);
    const fresh = cached && now - cached.ts < CONFIG.browserDeepCacheTtlMs;
    if (row.deepHydrated || fresh || state.browserDeepQueued.has(row.symbol) || state.browserDeepInflight.has(row.symbol)) {
      continue;
    }
    state.browserDeepQueued.add(row.symbol);
    state.browserDeepQueue.push({ symbol: row.symbol, price: row.price });
  }

  pumpBrowserDeepQueue();
}

function pumpBrowserDeepQueue() {
  if (!CONFIG.browserDeepEnabled) return;
  while (state.browserDeepInflight.size < CONFIG.browserDeepConcurrency && state.browserDeepQueue.length) {
    const item = state.browserDeepQueue.shift();
    state.browserDeepQueued.delete(item.symbol);
    state.browserDeepInflight.add(item.symbol);
    hydrateBrowserDeepSymbol(item)
      .catch((error) => {
        state.browserDeepLastError = error.message || String(error);
      })
      .finally(() => {
        state.browserDeepInflight.delete(item.symbol);
        window.setTimeout(pumpBrowserDeepQueue, 350);
      });
  }
}

async function hydrateBrowserDeepSymbol({ symbol, price }) {
  const metrics = await fetchBrowserDeepMetrics(symbol, price);
  if (!Object.keys(metrics).length) return;

  state.browserDeepCache.set(symbol, { ts: Date.now(), metrics });
  state.browserDeepUpdatedAt = Date.now();
  state.rows = state.rows.map((row) => row.symbol === symbol
    ? { ...mergeBrowserDeepMetrics(row), score: computeSignalScore(mergeBrowserDeepMetrics(row)) }
    : row);
  renderAll();
}

async function fetchBrowserDeepMetrics(symbol, price) {
  const metrics = {};

  const klines = await fetchBinanceBrowserJson("/fapi/v1/klines", {
    symbol,
    interval: "1m",
    limit: 65,
  });
  Object.assign(metrics, metricsFromBinanceKlines(Array.isArray(klines) ? klines : []));

  try {
    const oi = await fetchBinanceBrowserJson("/fapi/v1/openInterest", { symbol });
    const contracts = nullableNumber(oi?.openInterest);
    if (contracts && price) metrics.oiUsd = contracts * Number(price);
  } catch (error) {
    state.browserDeepLastError = error.message || String(error);
  }

  try {
    const history = await fetchBinanceBrowserJson("/futures/data/openInterestHist", {
      symbol,
      period: "5m",
      limit: 13,
    });
    Object.assign(metrics, metricsFromOpenInterestHistory(Array.isArray(history) ? history : []));
  } catch (error) {
    state.browserDeepLastError = error.message || String(error);
  }

  return Object.fromEntries(
    Object.entries(metrics).filter(([key, value]) => DEEP_METRIC_KEYS.has(key) && isFiniteNumber(value))
  );
}

async function fetchBinanceBrowserJson(path, params) {
  const query = new URLSearchParams();
  Object.entries(params || {}).forEach(([key, value]) => {
    if (value !== undefined && value !== null) query.set(key, value);
  });

  let lastError = null;
  for (const base of BINANCE_BROWSER_BASES) {
    try {
      return await fetchJson(`${base}${path}?${query.toString()}`, { timeoutMs: 7_000 });
    } catch (error) {
      lastError = error;
    }
  }
  throw lastError || new Error("Binance browser fetch failed");
}

function metricsFromBinanceKlines(rows) {
  const candles = [];
  for (const row of rows) {
    if (!Array.isArray(row) || row.length < 9) continue;
    const candle = {
      high: nullableNumber(row[2]),
      low: nullableNumber(row[3]),
      close: nullableNumber(row[4]),
      quoteVolume: nullableNumber(row[7]) || 0,
      trades: nullableNumber(row[8]) || 0,
    };
    if (candle.close) candles.push(candle);
  }
  if (!candles.length) return {};

  const last = candles[candles.length - 1];
  const ago = (minutes) => candles[Math.max(0, candles.length - 1 - minutes)];
  const last5 = candles.slice(-5);
  const last15 = candles.slice(-15);
  const last60 = candles.slice(-60);
  const high15 = Math.max(...last15.map((row) => row.high || 0));
  const low15 = Math.min(...last15.map((row) => row.low || 0));

  return {
    chg5m: pctChange(last.close, ago(5).close),
    chg1h: pctChange(last.close, ago(60).close),
    vol1h: sumNumbers(last60.map((row) => row.quoteVolume)),
    volatility15m: last.close ? ((high15 - low15) / last.close) * 100 : null,
    trades5m: sumNumbers(last5.map((row) => row.trades)),
  };
}

function metricsFromOpenInterestHistory(history) {
  const rows = history
    .map((row) => ({
      value: nullableNumber(row?.sumOpenInterestValue),
      contracts: nullableNumber(row?.sumOpenInterest),
    }))
    .filter((row) => row.value || row.contracts);
  if (rows.length < 2) return {};

  const first = rows[0];
  const last = rows[rows.length - 1];
  const metric = first.value && last.value ? "value" : "contracts";
  const output = { oiChg1h: pctChange(last[metric], first[metric]) };
  if (last.value) output.oiUsd = last.value;
  return output;
}

function sumNumbers(values) {
  return values.reduce((total, value) => Number.isFinite(Number(value)) ? total + Number(value) : total, 0);
}

function renderLoadingState() {
  els.tableTitle.textContent = EXCHANGE_LABELS[state.exchange];
  els.tableBody.innerHTML = `<tr><td colspan="${COLUMNS.length}"><div class="loading-state"><span></span>Loading backend-cached Binance Futures markets...</div></td></tr>`;
  els.metricSymbols.textContent = "--";
  els.metricVolume.textContent = "--";
  els.metricMove.textContent = "--";
  els.metricSignals.textContent = "--";
}

function renderError(error) {
  if (state.rows.length) return;
  const message = error.message || "request failed";
  els.tableBody.innerHTML = `<tr><td colspan="${COLUMNS.length}"><div class="empty-state">Backend feed unavailable: ${escapeHtml(message)}</div></td></tr>`;
}

function computeSignalScore(row) {
  const chg5m = Math.abs(num(row.chg5m));
  const chg1h = Math.abs(num(row.chg1h));
  const chg1d = Math.abs(num(row.chg1d));
  const volatility = Math.abs(num(row.volatility15m));
  const oiChange = Math.abs(num(row.oiChg1h));
  const funding = Math.abs(num(row.fundingRatePct));

  let score = 0;
  score += Math.min(chg5m * 16, 24);
  score += Math.min(chg1h * 8, 22);
  score += Math.min(chg1d * 1.2, 12);
  score += Math.min(volatility * 10, 18);
  score += Math.min(oiChange * 6, 18);
  score += Math.min(funding * 500, 16);
  if (row.quoteVolume24h > 1_000_000_000) score += 5;
  if (row.quoteVolume24h > 5_000_000_000) score += 5;

  return Math.max(0, Math.min(100, Math.round(score)));
}

function setStatus(type, text) {
  els.status.className = `status-pill ${type}`;
  els.status.textContent = text;
}

function updateClock() {
  els.clock.textContent = `${new Date().toISOString().slice(11, 19)} UTC`;
}

async function fetchJson(url, options = {}) {
  const controller = new AbortController();
  const timeout = window.setTimeout(() => controller.abort(), options.timeoutMs || CONFIG.requestTimeoutMs);
  try {
    const response = await fetch(url, {
      ...options,
      signal: controller.signal,
      cache: "no-store",
    });
    if (!response.ok) throw new Error(`${response.status} ${response.statusText}`);
    return await response.json();
  } finally {
    window.clearTimeout(timeout);
  }
}

function resolveApiBase() {
  const params = new URLSearchParams(window.location.search);
  const queryBase = params.get("api");
  if (queryBase) return normalizeApiBase(queryBase);

  const configured = window.SCREENER_API_BASE;
  if (configured) return normalizeApiBase(configured);

  if (window.location.hostname.endsWith("github.io")) {
    return DEFAULT_REMOTE_API_BASE;
  }

  if (window.location.protocol === "file:" || ["8080", "8088", "5500"].includes(window.location.port)) {
    return "http://127.0.0.1:8050";
  }

  return "";
}

function normalizeApiBase(value) {
  return String(value || "").replace(/\/+$/, "");
}

function apiUrl(path) {
  return `${state.apiBase}${path}`;
}

function pctChange(current, previous) {
  if (!isFiniteNumber(current) || !isFiniteNumber(previous) || previous === 0) return null;
  return ((current / previous) - 1) * 100;
}

function num(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : 0;
}

function nullableNumber(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function isFiniteNumber(value) {
  return value !== null && value !== "" && Number.isFinite(Number(value));
}

function formatPrice(value) {
  if (!isFiniteNumber(value) || Number(value) <= 0) return unavailableValue();
  const number = Number(value);
  if (number >= 1_000) return number.toLocaleString(undefined, { maximumFractionDigits: 2 });
  if (number >= 1) return number.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 4 });
  if (number >= 0.01) return number.toLocaleString(undefined, { minimumFractionDigits: 4, maximumFractionDigits: 6 });
  return number.toLocaleString(undefined, { minimumFractionDigits: 6, maximumFractionDigits: 8 });
}

function formatUsd(value) {
  if (!isFiniteNumber(value) || Number(value) < 0) return unavailableValue();
  return formatUsdText(value);
}

function formatUsdText(value) {
  const number = Number(value);
  if (!Number.isFinite(number) || number < 0) return "n/a";
  if (number === 0) return "$0";
  if (number >= 1_000_000_000) return `$${(number / 1_000_000_000).toFixed(2)}B`;
  if (number >= 1_000_000) return `$${(number / 1_000_000).toFixed(2)}M`;
  if (number >= 1_000) return `$${(number / 1_000).toFixed(1)}K`;
  return `$${number.toFixed(0)}`;
}

function formatInteger(value) {
  if (!isFiniteNumber(value) || Number(value) < 0) return unavailableValue();
  return Number(value).toLocaleString(undefined, { maximumFractionDigits: 0 });
}

function pctCell(value, digits = 2, signed = true) {
  if (!isFiniteNumber(value)) return unavailableValue();
  const number = Number(value);
  const prefix = signed && number > 0 ? "+" : "";
  return `<span class="${numberClass(number)}">${prefix}${number.toFixed(digits)}%</span>`;
}

function fundingCell(value) {
  if (!isFiniteNumber(value)) return unavailableValue();
  const number = Number(value);
  const hot = Math.abs(number) >= 0.025;
  const className = hot ? "funding-hot" : numberClass(number);
  const prefix = number > 0 ? "+" : "";
  return `<span class="${className}">${prefix}${number.toFixed(4)}%</span>`;
}

function scoreBadge(score) {
  const value = Number(score) || 0;
  let className = "";
  if (value >= 85) className = "critical";
  else if (value >= 70) className = "high";
  else if (value >= 40) className = "medium";
  return `<span class="score-badge ${className}" title="Signal score ${value}/100">${value}</span>`;
}

function numberClass(value) {
  if (Number(value) > 0) return "positive";
  if (Number(value) < 0) return "negative";
  return "neutral";
}

function signedText(value, digits) {
  if (!isFiniteNumber(value)) return "n/a";
  const number = Number(value);
  return `${number > 0 ? "+" : ""}${number.toFixed(digits)}`;
}

function utcTime(value) {
  return `${new Date(value).toISOString().slice(11, 19)} UTC`;
}

function unavailableValue() {
  return `<span class="unavailable-value" title="Binance does not currently provide this value for the symbol">n/a</span>`;
}

function deepMetricPlaceholder(row, key) {
  if (state.browserDeepInflight.has(row.symbol)) {
    return `<span class="pending-value" title="Browser is filling ${escapeHtml(deepMetricLabel(key))} directly from Binance public data">syncing</span>`;
  }
  if (state.browserDeepQueued.has(row.symbol)) {
    return `<span class="pending-value" title="Queued for browser-side Binance public data fill">queued</span>`;
  }
  if (state.payload?.deepStatus === "paused" || state.payload?.deepRestPaused) {
    const source = deepSourceLabel(state.payload);
    return `<span class="pending-value" title="${escapeHtml(deepMetricLabel(key))} is being filled in the browser because ${escapeHtml(source)} is paused${escapeHtml(deepRetryLabel(state.payload))}.">syncing</span>`;
  }
  return `<span class="pending-value" title="Backend is hydrating ${escapeHtml(deepMetricLabel(key))} in controlled batches">waiting</span>`;
}

function deepSourceLabel(payload) {
  return payload?.deepSource === "proxy" ? "deep proxy" : "Binance REST";
}

function deepRetryLabel(payload) {
  const ms = Number(payload?.deepRetryInMs);
  if (!Number.isFinite(ms) || ms <= 0) return "";
  const seconds = Math.ceil(ms / 1000);
  if (seconds < 90) return `, retry in ${seconds}s`;
  return `, retry in ${Math.ceil(seconds / 60)}m`;
}

function deepMetricLabel(key) {
  const column = COLUMNS.find((item) => item.key === key);
  return column ? column.label : key;
}

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}
