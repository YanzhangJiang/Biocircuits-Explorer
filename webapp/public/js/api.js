// Biocircuits Explorer — API Communication & Utility Functions

import { API, CLOUD_API, ensureDebugClientId, state, nodeRegistry } from './state.js';
import { isCloudComputeEnabled } from './cloud-compute.js';
import {
  fetchAuthConfig,
  getIdToken,
  isCancellationError,
  signIn,
  withRequestTimeout,
} from './auth.js';

let activeApiRequests = 0;
let statusRevision = 0;
let currentStatusClass = null;
let readyResetTimer = null;
const CLOUD_JOB_ENDPOINTS = new Set(['build_atlas', 'run_inverse_design']);
const JOB_TERMINAL_STATUSES = new Set(['succeeded', 'failed', 'cancelled']);
const JOB_KNOWN_STATUSES = new Set([
  'queued', 'running', 'cancel_requested', ...JOB_TERMINAL_STATUSES,
]);
const CLOUD_JOB_MAX_CONSECUTIVE_POLL_RETRIES = 2;
const CLOUD_RESULT_MAX_ATTEMPTS = 2;
const RETRYABLE_HTTP_STATUSES = new Set([408, 425, 429, 500, 502, 503, 504]);

// Request timeouts. Synchronous compute endpoints can legitimately run for
// minutes behind the two-slot work gate, so give them a wide default; job
// queue/poll operations are quick round trips and get a short one; the
// pre-signed S3 result download may move a large artifact.
const SYNC_REQUEST_TIMEOUT_MS = 5 * 60 * 1000;
const JOB_REQUEST_TIMEOUT_MS = 60 * 1000;
const RESULT_DOWNLOAD_TIMEOUT_MS = 10 * 60 * 1000;

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

function normalizedJobStatus(job) {
  const status = String(job?.status || '').toLowerCase();
  if (!JOB_KNOWN_STATUSES.has(status)) {
    throw new Error(`Backend returned an unknown job status: ${status || '(missing)'}`);
  }
  return status;
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

export function enrichModelRequestPayload(data) {
  if (!data || typeof data !== 'object' || Array.isArray(data)) return data;
  if (!data.session_id || data.network || data.network_ir_hash) return data;
  const ctx = findModelContextForSession(data.session_id);
  if (!ctx) return data;

  const networkIrHash = ctx.networkIrHash || ctx.network_ir_hash || null;
  const networkIr = ctx.networkIr || ctx.network_ir || null;
  if (!networkIrHash && !networkIr) return data;

  const enriched = { ...data };
  if (networkIrHash) enriched.network_ir_hash = networkIrHash;
  if (networkIr) enriched.network = networkIr;
  return enriched;
}

export async function apiSilent(endpoint, data, { signal = null } = {}) {
  const payload = enrichModelRequestPayload(data || {});
  return withRequestTimeout(signal, SYNC_REQUEST_TIMEOUT_MS, async requestSignal => {
    const resp = await fetch(canonicalApiUrl(endpoint), {
      method: 'POST',
      headers: apiHeaders(),
      body: JSON.stringify(payload),
      signal: requestSignal,
    });
    const contentType = resp.headers.get('content-type');
    if (!contentType || !contentType.includes('application/json')) {
      throw new Error('Backend server not responding');
    }
    const json = await resp.json();
    const errorMessage = responseErrorMessage(
      json,
      resp.ok === false ? `Backend request failed (${resp.status})` : null,
    );
    if (resp.ok === false || errorMessage) {
      throw new Error(errorMessage || `Backend request failed (${resp.status})`);
    }
    return json;
  });
}

export async function api(endpoint, data, { statusIsCurrent = null, signal = null } = {}) {
  activeApiRequests += 1;
  setStatus('working', activeApiRequests > 1 ? `Computing... (${activeApiRequests})` : 'Computing...');
  const requestStatusRevision = statusRevision;
  try {
    const payload = enrichModelRequestPayload(data || {});
    const json = await withRequestTimeout(signal, SYNC_REQUEST_TIMEOUT_MS, async requestSignal => {
      const resp = await fetch(canonicalApiUrl(endpoint), {
        method: 'POST',
        headers: apiHeaders(),
        body: JSON.stringify(payload),
        signal: requestSignal,
      });

      const contentType = resp.headers.get('content-type');
      if (!contentType || !contentType.includes('application/json')) {
        throw new Error('Backend server not responding. Please ensure Julia server is running.');
      }

      const responseJson = await resp.json();
      const errorMessage = responseErrorMessage(
        responseJson,
        resp.ok === false ? `Backend request failed (${resp.status})` : null,
      );
      if (resp.ok === false || errorMessage) {
        // The backend asks for the NetworkIR to be resent when it can no longer
        // resolve a model from session_id/hash alone (e.g. after a restart).
        const apiError = new Error(errorMessage || `Backend request failed (${resp.status})`);
        if (responseJson.need_network) apiError.needNetwork = true;
        apiError.status = resp.status;
        throw apiError;
      }
      return responseJson;
    });
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
    const json = await withRequestTimeout(signal, SYNC_REQUEST_TIMEOUT_MS, async requestSignal => {
      const resp = await fetch(`${API}/api/v1/rop_shape_optimize`, {
        method: 'POST',
        headers: apiHeaders(),
        body: JSON.stringify(request),
        signal: requestSignal,
      });
      const contentType = resp.headers.get('content-type');
      if (!contentType || !contentType.includes('application/json')) {
        throw new Error('Backend server did not return the ROP shape optimization contract.');
      }
      const responseJson = await resp.json();
      if (!responseJson || typeof responseJson !== 'object' || Array.isArray(responseJson)) {
        throw new Error('Backend returned an invalid ROP shape optimization payload.');
      }
      const errorMessage = typeof responseJson.error === 'string'
        ? responseJson.error
        : responseJson.error?.message;
      if (!resp.ok || errorMessage) {
        const apiError = new Error(errorMessage || `ROP shape optimization failed (${resp.status})`);
        apiError.status = resp.status;
        throw apiError;
      }
      return responseJson;
    });
    settleApiActivity(requestStatusRevision, statusIsCurrent);
    return json;
  } catch (error) {
    settleApiActivity(requestStatusRevision, statusIsCurrent, { errorMessage: error.message });
    throw error;
  }
}

async function jobApi(
  path,
  { method = 'GET', data = null, signal = null, timeoutMs = JOB_REQUEST_TIMEOUT_MS } = {},
) {
  return withRequestTimeout(signal, timeoutMs, async requestSignal => {
    const headers = data == null
      ? {
          'X-Biocircuits-Explorer-Debug-Client': ensureDebugClientId(),
          'X-ROP-Debug-Client': ensureDebugClientId(),
        }
      : apiHeaders();
    // Auth header: when the deployment has Cognito on, the backend requires
    // Authorization: Bearer <ID token>. The token is silently refreshed if it
    // is within 5 minutes of expiry. The same lifetime covers config, refresh,
    // broker headers, and the JSON response body.
    const config = await fetchAuthConfig({ signal: requestSignal, timeoutMs });
    if (config?.enabled) {
      const token = await getIdToken({ signal: requestSignal, timeoutMs });
      if (token) headers['Authorization'] = `Bearer ${token}`;
    }
    const resp = await fetch(path, {
      method,
      headers,
      body: data == null ? undefined : JSON.stringify(data),
      signal: requestSignal,
    });
    const contentType = resp.headers.get('content-type');
    if (!contentType || !contentType.includes('application/json')) {
      const error = new Error(`Backend server not responding (${resp.status})`);
      error.status = resp.status;
      error.retryable = RETRYABLE_HTTP_STATUSES.has(Number(resp.status));
      throw error;
    }
    const json = await resp.json();
    if (!resp.ok || json?.error) {
      let fallback = `Server error (${resp.status})`;
      if (resp.status === 401 || resp.status === 403) {
        fallback = 'Sign in required';
      } else if (resp.status === 429) {
        fallback = 'Daily quota exceeded — try again tomorrow.';
      }
      const error = new Error(responseErrorMessage(json, fallback));
      error.status = resp.status;
      error.code = json?.code || json?.error?.code || null;
      error.retryable = json?.retryable ?? json?.error?.retryable ?? null;
      throw error;
    }
    return json;
  });
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function retryDelayMs(attempt) {
  return Math.min(2000, 250 * (2 ** Math.max(0, attempt - 1)));
}

function errorIsRetryable(error) {
  if (error?.retryable != null) return error.retryable === true;
  // A deadline may be retried within the operation's explicit budget. Caller
  // cancellation is ownership loss and must never start another request.
  if (error?.name === 'TimeoutError') return true;
  if (error?.name === 'AbortError') return false;
  if (error instanceof TypeError) return true;
  return RETRYABLE_HTTP_STATUSES.has(Number(error?.status));
}

function responseIsRetryable(response) {
  const status = Number(response?.status);
  // A pre-signed object URL can report an expired signature as 401/403. Refresh
  // the URL once; the broker APIs themselves retain their normal auth semantics.
  return status === 401 || status === 403 || RETRYABLE_HTTP_STATUSES.has(status);
}

function signalAbortReason(signal) {
  if (signal?.reason) return signal.reason;
  if (typeof DOMException === 'function') return new DOMException('The request was aborted.', 'AbortError');
  const error = new Error('The request was aborted.');
  error.name = 'AbortError';
  return error;
}

async function waitForDelay(wait, delayMs, signal) {
  if (!signal) return wait(delayMs);
  if (signal.aborted) throw signalAbortReason(signal);
  let rejectAbort;
  const aborted = new Promise((resolve, reject) => { rejectAbort = reject; });
  const onAbort = () => rejectAbort(signalAbortReason(signal));
  signal.addEventListener('abort', onAbort, { once: true });
  try {
    const waitPromise = Promise.resolve(wait(delayMs));
    return await Promise.race([waitPromise, aborted]);
  } finally {
    signal.removeEventListener('abort', onAbort);
  }
}

async function downloadCloudResult(resultUrl, { signal, timeoutMs }) {
  return withRequestTimeout(signal, timeoutMs, async requestSignal => {
    const response = await fetch(resultUrl, { method: 'GET', signal: requestSignal });
    if (!response.ok) {
      const error = new Error(`Cloud result download failed (${response.status}).`);
      error.status = response.status;
      error.retryable = responseIsRetryable(response);
      throw error;
    }
    const contentType = String(response.headers?.get?.('content-type') || '')
      .split(';', 1)[0]
      .trim()
      .toLowerCase();
    if (contentType !== 'application/json') {
      throw new Error('Cloud result download did not return application/json.');
    }
    try {
      return await response.json();
    } catch (error) {
      // A response-body deadline/cancellation is a transport outcome. Preserve
      // it so timeout policy can fetch a fresh pre-signed URL and an explicit
      // caller abort can stop immediately. Only parse failures are invalid JSON.
      if (isCancellationError(error)) throw error;
      throw new Error('Cloud result download returned invalid JSON.', { cause: error });
    }
  });
}

// All /api/v1/jobs/* endpoints route to CLOUD_API: this is the SaaS path.
// In the browser CLOUD_API == '' (same-origin) so nothing changes; in the
// macOS WebView CLOUD_API points at the EC2 broker so cloud submissions
// hit the production backend instead of the local Julia process.
export async function submitJob(
  kind,
  spec,
  execution = { mode: 'local_async' },
  { signal = null, timeoutMs = JOB_REQUEST_TIMEOUT_MS } = {},
) {
  return jobApi(`${CLOUD_API}/api/v1/jobs`, {
    method: 'POST',
    data: { kind, spec, execution },
    signal,
    timeoutMs,
  });
}

export async function getJob(jobId, { signal = null, timeoutMs = JOB_REQUEST_TIMEOUT_MS } = {}) {
  return jobApi(`${CLOUD_API}/api/v1/jobs/${encodeURIComponent(jobId)}`, {
    method: 'POST', data: {}, signal, timeoutMs,
  });
}

export async function getJobResultUrl(
  jobId,
  { signal = null, timeoutMs = JOB_REQUEST_TIMEOUT_MS } = {},
) {
  return jobApi(`${CLOUD_API}/api/v1/jobs/${encodeURIComponent(jobId)}/result-url`, {
    method: 'POST', data: {}, signal, timeoutMs,
  });
}

export async function cancelJob(jobId, { signal = null, timeoutMs = JOB_REQUEST_TIMEOUT_MS } = {}) {
  return jobApi(`${CLOUD_API}/api/v1/jobs/${encodeURIComponent(jobId)}/cancel`, {
    method: 'POST', data: {}, signal, timeoutMs,
  });
}

export async function runCloudJob(
  kind,
  spec,
  {
    statusIsCurrent = null,
    sleepFn = sleep,
    signal = null,
    jobRequestTimeoutMs = JOB_REQUEST_TIMEOUT_MS,
    resultDownloadTimeoutMs = RESULT_DOWNLOAD_TIMEOUT_MS,
  } = {},
) {
  activeApiRequests += 1;
  setStatus('working', activeApiRequests > 1 ? `Submitting cloud job... (${activeApiRequests})` : 'Submitting cloud job...');
  const requestStatusRevision = statusRevision;
  const requestIsCurrent = () => statusPredicatePasses(statusIsCurrent);
  const wait = typeof sleepFn === 'function' ? sleepFn : sleep;
  let activitySettled = false;
  let jobId = null;
  let status = null;
  let cancellationAttempted = false;
  let cancellationSucceeded = false;
  let cancellationError = null;
  const settle = (options = {}) => {
    if (activitySettled) return;
    activitySettled = true;
    settleApiActivity(requestStatusRevision, statusIsCurrent, options);
  };
  const bestEffortCancelNonterminalJob = async () => {
    if (!jobId || cancellationAttempted || JOB_TERMINAL_STATUSES.has(status)) return false;
    cancellationAttempted = true;
    try {
      const cancellation = await cancelJob(jobId, { timeoutMs: jobRequestTimeoutMs });
      cancellationSucceeded = true;
      const cancellationStatus = String(cancellation?.status || '').toLowerCase();
      if (JOB_KNOWN_STATUSES.has(cancellationStatus)) status = cancellationStatus;
      return true;
    } catch (error) {
      cancellationError = error;
      // The obsolete owner has already settled its UI activity. Cancellation is
      // best-effort and must never report into, or reject through, a newer owner.
      return false;
    }
  };
  const attachRecoverableJobIdentity = (error) => {
    if (!jobId) return error;
    const recovery = {
      jobId,
      status,
      cancellationAttempted,
      cancellationSucceeded,
      recoverable: !JOB_TERMINAL_STATUSES.has(status),
    };
    if (cancellationError) recovery.cancellationError = cancellationError.message;
    let reported = error;
    if (!reported || typeof reported !== 'object' || !Object.isExtensible(reported)) {
      const message = error?.message || String(error ?? 'Cloud job failed.');
      reported = new Error(message);
      reported.name = typeof error?.name === 'string' ? error.name : 'Error';
      if (error && typeof error === 'object') {
        reported.cause = error;
        for (const key of ['status', 'code', 'retryable']) {
          if (error[key] !== undefined) reported[key] = error[key];
        }
      }
    }
    try {
      reported.jobId = jobId;
      reported.jobStatus = status;
      reported.recoverableJob = recovery.recoverable;
      reported.jobRecovery = recovery;
    } catch {
      // A hostile/custom throwable may reject property assignment despite
      // reporting itself extensible. Fall back to a normal mutable Error.
      const fallback = new Error(reported?.message || 'Cloud job failed.');
      fallback.name = typeof reported?.name === 'string' ? reported.name : 'Error';
      fallback.cause = reported;
      fallback.jobId = jobId;
      fallback.jobStatus = status;
      fallback.recoverableJob = recovery.recoverable;
      fallback.jobRecovery = recovery;
      reported = fallback;
    }
    return reported;
  };
  const retireIfStale = async () => {
    if (requestIsCurrent()) return false;
    // Settle first so a slow cancellation endpoint cannot leave the global
    // activity counter owned by a request that is already obsolete.
    settle();
    await bestEffortCancelNonterminalJob();
    return true;
  };
  try {
    const config = await fetchAuthConfig({ signal, timeoutMs: jobRequestTimeoutMs });
    if (await retireIfStale()) return null;
    if (config?.enabled) {
      const token = await getIdToken({ signal, timeoutMs: jobRequestTimeoutMs });
      if (await retireIfStale()) return null;
      if (!token) {
        await signIn({ signal });   // redirects; control will not return.
        settle({ doneText: 'Ready' });
        return;
      }
    }
    let job = await submitJob(kind, spec, { mode: 'aws_batch' }, {
      signal,
      timeoutMs: jobRequestTimeoutMs,
    });
    jobId = job.job_id;
    if (!jobId) throw new Error('Backend did not return a job id.');
    status = normalizedJobStatus(job);
    if (await retireIfStale()) return null;

    let pollAttempt = 0;
    let consecutivePollFailures = 0;
    while (!JOB_TERMINAL_STATUSES.has(status)) {
      if (await retireIfStale()) return null;
      const statusText = status === 'running'
        ? 'Cloud job running...'
        : status === 'cancel_requested'
          ? 'Cloud job cancelling...'
          : 'Cloud job queued...';
      setStatus('working', statusText);
      const pollDelay = Math.min(5000, 1500 + pollAttempt * 250);
      pollAttempt += 1;
      await waitForDelay(wait, pollDelay, signal);
      if (await retireIfStale()) return null;

      while (true) {
        try {
          job = await getJob(jobId, { signal, timeoutMs: jobRequestTimeoutMs });
          status = normalizedJobStatus(job);
          consecutivePollFailures = 0;
          break;
        } catch (error) {
          if (await retireIfStale()) return null;
          if (!errorIsRetryable(error) ||
              consecutivePollFailures >= CLOUD_JOB_MAX_CONSECUTIVE_POLL_RETRIES) {
            throw error;
          }
          consecutivePollFailures += 1;
          setStatus(
            'working',
            `Cloud job status temporarily unavailable — retrying (${consecutivePollFailures}/${CLOUD_JOB_MAX_CONSECUTIVE_POLL_RETRIES})...`,
          );
          await waitForDelay(wait, retryDelayMs(consecutivePollFailures), signal);
          if (await retireIfStale()) return null;
        }
      }
      if (await retireIfStale()) return null;
    }

    if (status !== 'succeeded') {
      throw new Error(job.error || `Cloud job ${status}`);
    }

    for (let resultAttempt = 1; resultAttempt <= CLOUD_RESULT_MAX_ATTEMPTS; resultAttempt += 1) {
      let resultLocation;
      try {
        resultLocation = await getJobResultUrl(jobId, {
          signal,
          timeoutMs: jobRequestTimeoutMs,
        });
      } catch (error) {
        if (await retireIfStale()) return null;
        if (resultAttempt < CLOUD_RESULT_MAX_ATTEMPTS && errorIsRetryable(error)) {
          await waitForDelay(wait, retryDelayMs(resultAttempt), signal);
          if (await retireIfStale()) return null;
          continue;
        }
        throw error;
      }
      if (await retireIfStale()) return null;

      const resultUrl = typeof resultLocation?.result_url === 'string'
        ? resultLocation.result_url.trim()
        : '';
      if (!resultUrl) {
        throw new Error('Backend did not return a cloud result URL.');
      }

      // The pre-signed object URL already contains its S3 authorization. Keep
      // this a plain GET: broker bearer/debug headers and request bodies must not
      // cross into the artifact request. A transient failure refreshes the
      // pre-signed URL once; it never falls back to the broker result body.
      try {
        const payload = await downloadCloudResult(resultUrl, {
          signal,
          timeoutMs: resultDownloadTimeoutMs,
        });
        if (await retireIfStale()) return null;
        settle();
        return payload;
      } catch (error) {
        if (await retireIfStale()) return null;
        if (resultAttempt < CLOUD_RESULT_MAX_ATTEMPTS && errorIsRetryable(error)) {
          await waitForDelay(wait, retryDelayMs(resultAttempt), signal);
          if (await retireIfStale()) return null;
          continue;
        }
        throw error;
      }
    }
    throw new Error('Cloud result download retry limit exceeded.');
  } catch (e) {
    if (await retireIfStale()) return null;
    if (jobId && !JOB_TERMINAL_STATUSES.has(status)) {
      await bestEffortCancelNonterminalJob();
    }
    const reportedError = attachRecoverableJobIdentity(e);
    settle({ errorMessage: reportedError?.message || String(reportedError) });
    throw reportedError;
  }
}

export async function computeApi(endpoint, data, options = {}) {
  const normalizedEndpoint = normalizeApiEndpoint(endpoint);
  if (isCloudComputeEnabled() && CLOUD_JOB_ENDPOINTS.has(normalizedEndpoint)) {
    return runCloudJob(normalizedEndpoint, data, options);
  }
  return api(normalizedEndpoint, data, options);
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
