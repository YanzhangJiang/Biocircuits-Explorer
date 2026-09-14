// Biocircuits Explorer — API Communication & Utility Functions

import { API, ensureDebugClientId, state, nodeRegistry } from './state.js';

let activeApiRequests = 0;
let statusRevision = 0;
let currentStatusClass = null;
let readyResetTimer = null;

// Request timeouts. Synchronous compute endpoints can legitimately run for
// minutes behind the two-slot work gate, so give them a wide default.
// AbortSignal.any needs Safari 17.4+ / Chrome 116+ / Firefox 124+ / Node 20.3+.
const SYNC_REQUEST_TIMEOUT_MS = 5 * 60 * 1000;

function withTimeout(signal, timeoutMs) {
  const timeoutSignal = AbortSignal.timeout(timeoutMs);
  return signal ? AbortSignal.any([signal, timeoutSignal]) : timeoutSignal;
}

function apiHeaders() {
  return {
    'Content-Type': 'application/json',
    'X-Biocircuits-Explorer-Debug-Client': ensureDebugClientId(),
    'X-ROP-Debug-Client': ensureDebugClientId(),
  };
}

// ===== HTML Escaping =====
export function escapeHtml(text) {
  return String(text ?? '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

// ===== API Helpers =====
function normalizeApiEndpoint(endpoint) {
  let normalized = String(endpoint || '').trim().replace(/^\/+/, '');
  const versionMatch = normalized.match(/^(?:api\/)?v(\d+)(?:\/|$)/);
  if (versionMatch && versionMatch[1] !== '1') {
    throw new Error(`Unsupported API version v${versionMatch[1]}.`);
  }
  normalized = normalized
    .replace(/^api\/v1(?:\/|$)/, '')
    .replace(/^api\//, '')
    .replace(/^v1(?:\/|$)/, '');
  if (!normalized) throw new Error('API endpoint must not be empty.');
  return normalized;
}

function canonicalApiUrl(endpoint) {
  return `${API}/api/v1/${normalizeApiEndpoint(endpoint)}`;
}

function responseErrorMessage(json, fallback) {
  if (typeof json?.error === 'string' && json.error.trim()) return json.error;
  if (typeof json?.error?.message === 'string' && json.error.message.trim()) {
    return json.error.message;
  }
  return fallback;
}

function statusPredicatePasses(statusIsCurrent) {
  if (typeof statusIsCurrent !== 'function') return true;
  try { return !!statusIsCurrent(); }
  catch { return false; }
}

function settleApiActivity(
  requestStatusRevision,
  statusIsCurrent,
  { errorMessage = null, doneText = 'Done' } = {},
) {
  activeApiRequests = Math.max(0, activeApiRequests - 1);
  const mayCommit = statusPredicatePasses(statusIsCurrent);
  const mayReconcile = statusRevision === requestStatusRevision || currentStatusClass === 'working';
  if (errorMessage && mayCommit) {
    setStatus('error', errorMessage);
    return;
  }
  if (!mayCommit && !mayReconcile) return;
  if (activeApiRequests > 0) {
    setStatus('working', `Computing... (${activeApiRequests})`);
  } else {
    setStatus('done', doneText);
  }
}

function contextMatchesSession(ctx, sessionId) {
  return ctx && String(ctx.sessionId || ctx.session_id || '') === String(sessionId || '');
}

function findModelContextForSession(sessionId) {
  for (const info of Object.values(nodeRegistry)) {
    const ctx = info?.data?.modelContext;
    if (contextMatchesSession(ctx, sessionId)) return ctx;
  }
  if (contextMatchesSession(state.model, sessionId)) return state.model;
  return null;
}

export function enrichModelRequestPayload(data, { recover = false } = {}) {
  if (!data || typeof data !== 'object' || Array.isArray(data)) return data;
  if (!data.session_id || data.network || (data.network_ir_hash && !recover)) return data;
  const ctx = findModelContextForSession(data.session_id);
  if (!ctx) return data;

  const networkIrHash = ctx.networkIrHash || ctx.network_ir_hash || null;
  const networkIr = ctx.networkIr || ctx.network_ir || null;
  if (data.network_ir_hash && data.network_ir_hash !== networkIrHash) return data;
  if (!networkIrHash && !networkIr) return data;

  const enriched = { ...data };
  if (networkIrHash) enriched.network_ir_hash = networkIrHash;
  if (networkIr && (recover || !networkIrHash)) enriched.network = networkIr;
  return enriched;
}

async function requestModelJson(endpoint, data, signal, statusIsCurrent) {
  const payload = enrichModelRequestPayload(data || {});
  // Capture recovery input now: a later workspace edit must not change the
  // network used by an already-running request.
  const recoveryPayload = enrichModelRequestPayload(payload, { recover: true });
  const recoveryBody = recoveryPayload.network ? JSON.stringify(recoveryPayload) : null;
  const requestSignal = withTimeout(signal, SYNC_REQUEST_TIMEOUT_MS);
  const send = body => fetch(canonicalApiUrl(endpoint), {
    method: 'POST',
    headers: apiHeaders(),
    body,
    signal: requestSignal,
  });
  let resp = await send(JSON.stringify(payload));
  let json = await readApiJson(resp);
  if (resp.status === 409 && json?.need_network === true &&
      !payload.network && recoveryBody && statusPredicatePasses(statusIsCurrent)) {
    resp = await send(recoveryBody);
    json = await readApiJson(resp);
  }
  const message = responseErrorMessage(
    json, resp.ok === false ? `Backend request failed (${resp.status})` : null,
  );
  if (resp.ok === false || message) {
    const error = new Error(message || `Backend request failed (${resp.status})`);
    error.status = resp.status;
    if (json?.need_network) error.needNetwork = true;
    throw error;
  }
  return json;
}

async function readApiJson(resp) {
  const contentType = resp.headers.get('content-type');
  if (!contentType || !contentType.includes('application/json')) {
    throw new Error('Backend server not responding');
  }
  return resp.json();
}

export async function apiSilent(endpoint, data, { signal = null } = {}) {
  return requestModelJson(endpoint, data, signal);
}

export async function api(endpoint, data, { statusIsCurrent = null, signal = null } = {}) {
  activeApiRequests += 1;
  setStatus('working', activeApiRequests > 1 ? `Computing... (${activeApiRequests})` : 'Computing...');
  const requestStatusRevision = statusRevision;
  try {
    const json = await requestModelJson(endpoint, data, signal, statusIsCurrent);
    settleApiActivity(requestStatusRevision, statusIsCurrent);
    return json;
  } catch (e) {
    settleApiActivity(requestStatusRevision, statusIsCurrent, { errorMessage: e.message });
    throw e;
  }
}

// Fixed-topology ROP shape optimization is a versioned evidence contract with
// additional response-shape checks beyond the shared canonical v1 helper.
export async function optimizeRopShape(request, { statusIsCurrent = null, signal = null } = {}) {
  if (!request || typeof request !== 'object' || Array.isArray(request)) {
    throw new Error('ROP shape optimization request must be an object.');
  }
  activeApiRequests += 1;
  setStatus('working', activeApiRequests > 1
    ? `Optimizing ROP shape... (${activeApiRequests})`
    : 'Optimizing ROP shape...');
  const requestStatusRevision = statusRevision;
  try {
    const resp = await fetch(`${API}/api/v1/rop_shape_optimize`, {
      method: 'POST',
      headers: apiHeaders(),
      body: JSON.stringify(request),
      signal: withTimeout(signal, SYNC_REQUEST_TIMEOUT_MS),
    });
    const contentType = resp.headers.get('content-type');
    if (!contentType || !contentType.includes('application/json')) {
      throw new Error('Backend server did not return the ROP shape optimization contract.');
    }
    const json = await resp.json();
    if (!json || typeof json !== 'object' || Array.isArray(json)) {
      throw new Error('Backend returned an invalid ROP shape optimization payload.');
    }
    const errorMessage = typeof json.error === 'string' ? json.error : json.error?.message;
    if (!resp.ok || errorMessage) {
      const apiError = new Error(errorMessage || `ROP shape optimization failed (${resp.status})`);
      apiError.status = resp.status;
      throw apiError;
    }
    settleApiActivity(requestStatusRevision, statusIsCurrent);
    return json;
  } catch (error) {
    settleApiActivity(requestStatusRevision, statusIsCurrent, { errorMessage: error.message });
    throw error;
  }
}

// ===== Status Badge =====
export function setStatus(cls, text) {
  currentStatusClass = cls;
  const badge = document.getElementById('status-badge');
  if (!badge) return;

  statusRevision += 1;
  const currentRevision = statusRevision;
  if (readyResetTimer) {
    clearTimeout(readyResetTimer);
    readyResetTimer = null;
  }
  badge.className = `badge ${cls}`;
  badge.textContent = text;
  if (cls === 'done') {
    readyResetTimer = setTimeout(() => {
      if (activeApiRequests !== 0 || statusRevision !== currentRevision) return;
      badge.className = 'badge';
      badge.textContent = 'Ready';
      readyResetTimer = null;
    }, 3000);
  }
}

// ===== Toast Notifications =====
export function showToast(message, duration = 2500) {
  const container = document.getElementById('toast-container');
  const toast = document.createElement('div');
  toast.className = 'toast';
  toast.textContent = message;
  container.appendChild(toast);
  requestAnimationFrame(() => toast.classList.add('show'));
  setTimeout(() => {
    toast.classList.remove('show');
    setTimeout(() => toast.remove(), 300);
  }, duration);
}

// Render backend- or provider-controlled error text without asking the HTML
// parser to interpret it. API errors may include a server-supplied `error`
// field, so callers must not interpolate Error.message into innerHTML.
export function renderNodeError(container, error) {
  if (!container) return null;
  const errorElement = document.createElement('div');
  errorElement.className = 'node-error';
  errorElement.textContent = error?.message || String(error ?? 'Unknown error');
  container.replaceChildren(errorElement);
  return errorElement;
}

// ===== Parsing Utilities =====
export function splitCommaList(value) {
  return String(value || '')
    .split(',')
    .map(item => item.trim())
    .filter(Boolean);
}

export function parseOptionalInteger(value) {
  const text = String(value ?? '').trim();
  if (!text) return null;
  const parsed = parseInt(text, 10);
  return Number.isFinite(parsed) ? parsed : null;
}

export function parseOptionalFloat(value) {
  const text = String(value ?? '').trim();
  if (!text) return null;
  const parsed = parseFloat(text);
  return Number.isFinite(parsed) ? parsed : null;
}

export function parseOptionalJson(value, fallback, label) {
  const text = String(value ?? '').trim();
  if (!text) return fallback;
  try {
    return JSON.parse(text);
  } catch {
    throw new Error(`${label} must be valid JSON.`);
  }
}

export function normalizePredicateArray(value, label) {
  if (Array.isArray(value)) return value;
  if (value && typeof value === 'object') return [value];
  throw new Error(`${label} must be a JSON object or array.`);
}

// ===== Select Sync =====
export function syncSelectOptions(selectEl, values, preferredValue = null, fallbackIndex = 0) {
  if (!selectEl) return;
  const orderedValues = Array.isArray(values) ? values.filter(v => v != null && v !== '') : [];
  const pendingValue = selectEl.dataset.pendingValue || null;
  const explicitPreferredValue = preferredValue != null && preferredValue !== '' ? preferredValue : null;
  const liveValue = selectEl.value || null;
  const previousValue = explicitPreferredValue ?? pendingValue ?? liveValue;
  selectEl.innerHTML = '';
  orderedValues.forEach(value => selectEl.add(new Option(value, value)));
  if (!orderedValues.length) return;
  if (previousValue && orderedValues.includes(previousValue)) {
    selectEl.value = previousValue;
    delete selectEl.dataset.pendingValue;
    return;
  }
  const safeIndex = Math.min(Math.max(fallbackIndex, 0), orderedValues.length - 1);
  selectEl.value = orderedValues[safeIndex];
  delete selectEl.dataset.pendingValue;
}

// ===== Unified Error Handler =====
export function handleNodeError(error, nodeId, operation) {
  const msg = error?.message || String(error);
  showToast(`${operation}: ${msg}`);
  console.error(`[${nodeId || 'global'}] ${operation}:`, error);
  // Also try to clear loading state if nodeId is provided
  try {
    const loadingEl = nodeId ? document.querySelector(`#${nodeId} .node-loading`) : null;
    if (loadingEl) loadingEl.style.display = 'none';
  } catch {}
}

// ===== Serialization =====
export function cloneSerializable(value) {
  if (value == null) return value;
  return JSON.parse(JSON.stringify(value));
}
