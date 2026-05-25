// Biocircuits Explorer — API Communication & Utility Functions

import { API, CLOUD_API, ensureDebugClientId } from './state.js';
import { isCloudComputeEnabled } from './cloud-compute.js';
import { fetchAuthConfig, getIdToken, signIn } from './auth.js';

let activeApiRequests = 0;
let statusRevision = 0;
let readyResetTimer = null;
const CLOUD_JOB_ENDPOINTS = new Set(['build_atlas', 'run_inverse_design']);
const JOB_TERMINAL_STATUSES = new Set(['succeeded', 'failed', 'cancelled']);

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
    if (json.error) {
      // The backend asks for the NetworkIR to be resent when it can no longer
      // resolve a model from session_id/hash alone (e.g. after a restart).
      const apiError = new Error(json.error);
      if (json.need_network) apiError.needNetwork = true;
      apiError.status = resp.status;
      throw apiError;
    }
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

async function jobApi(path, { method = 'GET', data = null } = {}) {
  const headers = data == null
    ? {
        'X-Biocircuits-Explorer-Debug-Client': ensureDebugClientId(),
        'X-ROP-Debug-Client': ensureDebugClientId(),
      }
    : apiHeaders();
  // Auth header: when the deployment has Cognito on, the backend requires
  // Authorization: Bearer <ID token>. The token is silently refreshed if it
  // is within 5 minutes of expiry.
  const config = await fetchAuthConfig().catch(() => null);
  if (config && config.enabled) {
    const token = await getIdToken();
    if (token) headers['Authorization'] = `Bearer ${token}`;
  }
  const resp = await fetch(path, {
    method,
    headers,
    body: data == null ? undefined : JSON.stringify(data),
  });
  const contentType = resp.headers.get('content-type');
  if (!contentType || !contentType.includes('application/json')) {
    throw new Error(`Backend server not responding (${resp.status})`);
  }
  const json = await resp.json();
  if (!resp.ok || json.error) {
    if (resp.status === 401 || resp.status === 403) {
      throw new Error(json.error || 'Sign in required');
    }
    if (resp.status === 429) {
      throw new Error(json.error || 'Daily quota exceeded — try again tomorrow.');
    }
    throw new Error(json.error || `Server error (${resp.status})`);
  }
  return json;
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// All /api/jobs/* endpoints route to CLOUD_API: this is the SaaS path.
// In the browser CLOUD_API == '' (same-origin) so nothing changes; in the
// macOS WebView CLOUD_API points at the EC2 broker so cloud submissions
// hit the production backend instead of the local Julia process.
export async function submitJob(kind, spec, execution = { mode: 'local_async' }) {
  return jobApi(`${CLOUD_API}/api/jobs`, {
    method: 'POST',
    data: { kind, spec, execution },
  });
}

export async function getJob(jobId) {
  return jobApi(`${CLOUD_API}/api/jobs/${encodeURIComponent(jobId)}`, { method: 'POST', data: {} });
}

export async function getJobResult(jobId) {
  return jobApi(`${CLOUD_API}/api/jobs/${encodeURIComponent(jobId)}/result`, { method: 'POST', data: {} });
}

export async function getJobResultUrl(jobId) {
  return jobApi(`${CLOUD_API}/api/jobs/${encodeURIComponent(jobId)}/result-url`, { method: 'POST', data: {} });
}

export async function cancelJob(jobId) {
  return jobApi(`${CLOUD_API}/api/jobs/${encodeURIComponent(jobId)}/cancel`, { method: 'POST', data: {} });
}

export async function runCloudJob(kind, spec) {
  activeApiRequests += 1;
  setStatus('working', activeApiRequests > 1 ? `Submitting cloud job... (${activeApiRequests})` : 'Submitting cloud job...');
  try {
    const config = await fetchAuthConfig().catch(() => null);
    if (config && config.enabled) {
      const token = await getIdToken();
      if (!token) {
        activeApiRequests = Math.max(0, activeApiRequests - 1);
        setStatus('done', 'Ready');
        await signIn();   // redirects; control will not return.
        return;
      }
    }
    let job = await submitJob(kind, spec, { mode: 'aws_batch' });
    const jobId = job.job_id;
    if (!jobId) throw new Error('Backend did not return a job id.');

    while (!JOB_TERMINAL_STATUSES.has(String(job.status || '').toLowerCase())) {
      const status = String(job.status || 'queued');
      setStatus('working', status === 'running' ? 'Cloud job running...' : 'Cloud job queued...');
      await sleep(1500);
      job = await getJob(jobId);
    }

    if (job.status !== 'succeeded') {
      throw new Error(job.error || `Cloud job ${job.status}`);
    }

    const payload = await getJobResult(jobId);
    activeApiRequests = Math.max(0, activeApiRequests - 1);
    if (activeApiRequests > 0) {
      setStatus('working', `Computing... (${activeApiRequests})`);
    } else {
      setStatus('done', 'Done');
    }
    return payload.result;
  } catch (e) {
    activeApiRequests = Math.max(0, activeApiRequests - 1);
    setStatus('error', e.message);
    throw e;
  }
}

export async function computeApi(endpoint, data) {
  if (isCloudComputeEnabled() && CLOUD_JOB_ENDPOINTS.has(endpoint)) {
    return runCloudJob(endpoint, data);
  }
  return api(endpoint, data);
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
