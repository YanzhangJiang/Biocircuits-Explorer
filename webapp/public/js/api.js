// Biocircuits Explorer — API Communication & Utility Functions

import { API, ensureDebugClientId } from './state.js';

let activeApiRequests = 0;
let statusRevision = 0;
let readyResetTimer = null;

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
export async function apiSilent(endpoint, data) {
  const resp = await fetch(`${API}/api/${endpoint}`, {
    method: 'POST',
    headers: apiHeaders(),
    body: JSON.stringify(data || {}),
  });
  const contentType = resp.headers.get('content-type');
  if (!contentType || !contentType.includes('application/json')) {
    throw new Error('Backend server not responding');
  }
  const json = await resp.json();
  if (json.error) throw new Error(json.error);
  return json;
}

export async function api(endpoint, data) {
  activeApiRequests += 1;
  setStatus('working', activeApiRequests > 1 ? `Computing... (${activeApiRequests})` : 'Computing...');
  try {
    const resp = await fetch(`${API}/api/${endpoint}`, {
      method: 'POST',
      headers: apiHeaders(),
      body: JSON.stringify(data),
    });

    const contentType = resp.headers.get('content-type');
    if (!contentType || !contentType.includes('application/json')) {
      throw new Error('Backend server not responding. Please ensure Julia server is running.');
    }

    const json = await resp.json();
    if (json.error) throw new Error(json.error);
    activeApiRequests = Math.max(0, activeApiRequests - 1);
    if (activeApiRequests > 0) {
      setStatus('working', `Computing... (${activeApiRequests})`);
    } else {
      setStatus('done', 'Done');
    }
    return json;
  } catch (e) {
    activeApiRequests = Math.max(0, activeApiRequests - 1);
    setStatus('error', e.message);
    throw e;
  }
}

// ===== Status Badge =====
export function setStatus(cls, text) {
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
  } catch (error) {
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
  } catch (_) {}
}

// ===== Serialization =====
export function cloneSerializable(value) {
  if (value == null) return value;
  return JSON.parse(JSON.stringify(value));
}
