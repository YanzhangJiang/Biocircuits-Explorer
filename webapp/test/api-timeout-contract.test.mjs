// Request-lifetime regressions for local and cloud compute. These use
// node:test directly so every async body is owned by the runner; a pending or
// rejected promise can no longer be counted as a synchronous pass.
import assert from 'node:assert/strict';
import { afterEach, test } from 'node:test';

const sessionValues = new Map();
global.window = {
  matchMedia: () => null,
  crypto: { randomUUID: () => 'debug-test' },
  sessionStorage: {
    getItem: key => sessionValues.get(key) ?? null,
    setItem: (key, value) => sessionValues.set(key, String(value)),
    removeItem: key => sessionValues.delete(key),
  },
};
global.document = {
  getElementById: () => null,
  documentElement: {
    dataset: {},
    style: { setProperty: () => {} },
  },
};

const originalFetch = globalThis.fetch;
const originalDOMException = globalThis.DOMException;
const { cloudComputeState } = await import('../public/js/state.js');
const { fetchAuthConfig, getIdToken } = await import('../public/js/auth.js');
const {
  api,
  apiSilent,
  computeApi,
  getJob,
  optimizeRopShape,
  submitJob,
} = await import('../public/js/api.js');

const AUTH_PREFIX = 'biocircuits-explorer.auth.';

afterEach(() => {
  globalThis.fetch = originalFetch;
  globalThis.DOMException = originalDOMException;
  cloudComputeState.enabled = false;
});

function jsonResponse(json, { status = 200, ok = status >= 200 && status < 300 } = {}) {
  return {
    ok,
    status,
    headers: { get: () => 'application/json' },
    json: async () => json,
  };
}

function rejectWhenAborted(signal) {
  return new Promise((resolve, reject) => {
    const rejectAbort = () => reject(signal.reason || Object.assign(new Error('aborted'), {
      name: 'AbortError',
    }));
    if (signal.aborted) rejectAbort();
    else signal.addEventListener('abort', rejectAbort, { once: true });
  });
}

test('node:test awaits asynchronous contract bodies', async () => {
  let completed = false;
  await new Promise(resolve => setTimeout(() => {
    completed = true;
    resolve();
  }, 1));
  assert.equal(completed, true);
});

test('a pre-aborted cloud request performs no auth or job request', async () => {
  cloudComputeState.enabled = true;
  let fetchCalls = 0;
  globalThis.fetch = async () => {
    fetchCalls += 1;
    throw new Error('pre-aborted cloud request must not fetch');
  };
  const controller = new AbortController();
  controller.abort();

  await assert.rejects(
    computeApi('build_atlas', {}, { signal: controller.signal }),
    error => error?.name === 'AbortError',
  );
  assert.equal(fetchCalls, 0);
});

test('auth config hangs only until the cloud request deadline', async () => {
  cloudComputeState.enabled = true;
  const calls = [];
  globalThis.fetch = async (url, options) => {
    calls.push(url);
    assert.equal(url, '/api/v1/auth/config');
    assert.ok(options.signal instanceof AbortSignal);
    return rejectWhenAborted(options.signal);
  };

  await assert.rejects(
    computeApi('build_atlas', {}, { jobRequestTimeoutMs: 10 }),
    error => error?.name === 'TimeoutError',
  );
  assert.deepEqual(calls, ['/api/v1/auth/config']);
});

test('orphaned auth config cannot publish or abort another single-flight waiter', async () => {
  const abandonedController = new AbortController();
  const firstController = new AbortController();
  const secondController = new AbortController();
  let finishAbandoned;
  let finishConfig;
  let fetchCalls = 0;
  const configReady = new Promise(resolve => { finishConfig = resolve; });
  globalThis.fetch = async url => {
    assert.equal(url, '/api/v1/auth/config');
    fetchCalls += 1;
    if (fetchCalls === 1) {
      // Deliberately ignore AbortSignal to model a non-cooperative fetch/body
      // implementation.  This stale response must never populate the cache.
      return new Promise(resolve => { finishAbandoned = resolve; });
    }
    return configReady;
  };

  const abandoned = fetchAuthConfig({ signal: abandonedController.signal, timeoutMs: 1000 });
  abandonedController.abort();
  await assert.rejects(abandoned, error => error?.name === 'AbortError');

  const first = fetchAuthConfig({ signal: firstController.signal, timeoutMs: 1000 });
  const second = fetchAuthConfig({ signal: secondController.signal, timeoutMs: 1000 });
  firstController.abort();
  await assert.rejects(first, error => error?.name === 'AbortError');
  finishAbandoned(jsonResponse({
    enabled: false,
    cognito_domain: 'stale.example.test',
    cognito_app_client_id: 'stale-client',
  }));
  await new Promise(resolve => setImmediate(resolve));
  let thirdSettled = false;
  const third = fetchAuthConfig({ timeoutMs: 1000 });
  third.then(() => { thirdSettled = true; }, () => { thirdSettled = true; });
  await Promise.resolve();
  assert.equal(thirdSettled, false);
  assert.equal(fetchCalls, 2);
  finishConfig(jsonResponse({
    enabled: true,
    cognito_domain: 'auth.example.test',
    cognito_app_client_id: 'client-id',
  }));
  assert.equal((await second).enabled, true);
  assert.equal((await third).cognito_domain, 'auth.example.test');
  assert.equal(secondController.signal.aborted, false);
  assert.equal(fetchCalls, 2);

  const alreadyAborted = new AbortController();
  alreadyAborted.abort();
  await assert.rejects(
    fetchAuthConfig({ signal: alreadyAborted.signal }),
    error => error?.name === 'AbortError',
  );
});

test('token refresh shares the caller signal and cloud deadline', async () => {
  cloudComputeState.enabled = true;
  sessionValues.set(`${AUTH_PREFIX}id_token`, 'expired-token');
  sessionValues.set(`${AUTH_PREFIX}refresh_token`, 'refresh-token');
  sessionValues.set(`${AUTH_PREFIX}expires_at`, '0');
  const calls = [];
  globalThis.fetch = async (url, options) => {
    calls.push(url);
    if (url === '/api/v1/auth/config') {
      return jsonResponse({
        enabled: true,
        cognito_domain: 'auth.example.test',
        cognito_app_client_id: 'client-id',
      });
    }
    assert.equal(url, 'https://auth.example.test/oauth2/token');
    assert.ok(options.signal instanceof AbortSignal);
    return rejectWhenAborted(options.signal);
  };

  await assert.rejects(
    computeApi('build_atlas', {}, { jobRequestTimeoutMs: 10 }),
    error => error?.name === 'TimeoutError',
  );
  assert.deepEqual(calls, [
    'https://auth.example.test/oauth2/token',
  ]);

  // The successful config is cached for the remaining tests. Give subsequent
  // job calls a fresh in-memory token so they do not depend on this timeout.
  sessionValues.set(`${AUTH_PREFIX}id_token`, 'fresh-token');
  sessionValues.set(`${AUTH_PREFIX}expires_at`, String(Date.now() + 60 * 60 * 1000));
});

test('orphaned token refresh cannot persist or abort another single-flight waiter', async () => {
  sessionValues.set(`${AUTH_PREFIX}id_token`, 'expired-token');
  sessionValues.set(`${AUTH_PREFIX}refresh_token`, 'refresh-token');
  sessionValues.set(`${AUTH_PREFIX}expires_at`, '0');
  const abandonedController = new AbortController();
  const firstController = new AbortController();
  const secondController = new AbortController();
  let finishAbandoned;
  let finishRefresh;
  let markAbandonedStarted;
  let markRefreshStarted;
  const abandonedStarted = new Promise(resolve => { markAbandonedStarted = resolve; });
  const refreshStarted = new Promise(resolve => { markRefreshStarted = resolve; });
  let refreshCalls = 0;
  globalThis.fetch = async url => {
    assert.equal(url, 'https://auth.example.test/oauth2/token');
    refreshCalls += 1;
    if (refreshCalls === 1) {
      markAbandonedStarted();
      return new Promise(resolve => { finishAbandoned = resolve; });
    }
    markRefreshStarted();
    return new Promise(resolve => { finishRefresh = resolve; });
  };

  const abandoned = getIdToken({ signal: abandonedController.signal, timeoutMs: 1000 });
  await abandonedStarted;
  abandonedController.abort();
  await assert.rejects(abandoned, error => error?.name === 'AbortError');

  const first = getIdToken({ signal: firstController.signal, timeoutMs: 1000 });
  const second = getIdToken({ signal: secondController.signal, timeoutMs: 1000 });
  await refreshStarted;
  firstController.abort();
  await assert.rejects(first, error => error?.name === 'AbortError');
  finishAbandoned(jsonResponse({ id_token: 'stale-token', expires_in: 3600 }));
  await new Promise(resolve => setImmediate(resolve));
  assert.equal(sessionValues.get(`${AUTH_PREFIX}id_token`), 'expired-token');
  let thirdSettled = false;
  const third = getIdToken({ timeoutMs: 1000 });
  third.then(() => { thirdSettled = true; }, () => { thirdSettled = true; });
  await Promise.resolve();
  assert.equal(thirdSettled, false);
  assert.equal(refreshCalls, 2);
  finishRefresh(jsonResponse({ id_token: 'shared-fresh-token', expires_in: 3600 }));
  assert.equal(await second, 'shared-fresh-token');
  assert.equal(await third, 'shared-fresh-token');
  assert.equal(secondController.signal.aborted, false);
  assert.equal(refreshCalls, 2);
  assert.equal(sessionValues.get(`${AUTH_PREFIX}id_token`), 'shared-fresh-token');
  sessionValues.set(`${AUTH_PREFIX}id_token`, 'fresh-token');
  sessionValues.set(`${AUTH_PREFIX}expires_at`, String(Date.now() + 60 * 60 * 1000));
});

test('local API helpers bound headers and response bodies', async () => {
  for (const invoke of [
    () => api('build_model', {}),
    () => apiSilent('build_model', {}),
    () => optimizeRopShape({ network: {} }),
  ]) {
    let capturedSignal = null;
    globalThis.fetch = async (url, options) => {
      capturedSignal = options.signal;
      return jsonResponse({ ok: true, rop_shape: { status: 'ok' } });
    };
    await invoke();
    assert.ok(capturedSignal instanceof AbortSignal);
    assert.equal(capturedSignal.aborted, false);
  }
});

test('job submit and poll carry a bounded signal through auth and JSON', async () => {
  const captured = [];
  globalThis.fetch = async (url, options) => {
    captured.push({ url, signal: options.signal, authorization: options.headers.Authorization });
    if (url === '/api/v1/jobs') return jsonResponse({ job_id: 'j-1', status: 'queued' });
    if (url === '/api/v1/jobs/j-1') return jsonResponse({ job_id: 'j-1', status: 'succeeded' });
    throw new Error(`Unexpected URL ${url}`);
  };

  await submitJob('build_atlas', {});
  await getJob('j-1');
  assert.deepEqual(captured.map(call => call.url), ['/api/v1/jobs', '/api/v1/jobs/j-1']);
  for (const call of captured) {
    assert.ok(call.signal instanceof AbortSignal);
    assert.equal(call.authorization, 'Bearer fresh-token');
  }
});

test('caller abort rejects a pending local request', async () => {
  const controller = new AbortController();
  globalThis.fetch = async (url, options) => rejectWhenAborted(options.signal);
  const pending = api('build_model', {}, { signal: controller.signal });
  controller.abort();
  await assert.rejects(pending, error => error?.name === 'AbortError');
});

test('caller abort stops cloud polling without retry and requests cancellation', async () => {
  cloudComputeState.enabled = true;
  const controller = new AbortController();
  let pollCalls = 0;
  let cancelCalls = 0;
  globalThis.fetch = async (url, options) => {
    if (url === '/api/v1/jobs') return jsonResponse({ job_id: 'abort-poll', status: 'queued' });
    if (url === '/api/v1/jobs/abort-poll') {
      pollCalls += 1;
      setTimeout(() => controller.abort(), 0);
      return rejectWhenAborted(options.signal);
    }
    if (url === '/api/v1/jobs/abort-poll/cancel') {
      cancelCalls += 1;
      return jsonResponse({ job_id: 'abort-poll', status: 'cancel_requested' });
    }
    throw new Error(`Unexpected URL ${url}`);
  };

  let error;
  try {
    await computeApi('build_atlas', {}, {
      signal: controller.signal,
      sleepFn: async () => {},
    });
  } catch (caught) {
    error = caught;
  }
  assert.equal(error?.name, 'AbortError');
  assert.equal(error?.jobId, 'abort-poll');
  assert.equal(pollCalls, 1);
  assert.equal(cancelCalls, 1);
});

test('response-body timeout refreshes the pre-signed URL and is not invalid JSON', async () => {
  cloudComputeState.enabled = true;
  let resultUrlCalls = 0;
  const directUrls = [];
  globalThis.fetch = async (url, options) => {
    if (url === '/api/v1/jobs') return jsonResponse({ job_id: 'body-timeout', status: 'succeeded' });
    if (url === '/api/v1/jobs/body-timeout/result-url') {
      resultUrlCalls += 1;
      return jsonResponse({ result_url: `https://artifacts.example/body-${resultUrlCalls}.json` });
    }
    if (url.startsWith('https://artifacts.example/body-')) {
      directUrls.push(url);
      if (directUrls.length === 1) {
        return {
          ...jsonResponse(null),
          json: async () => rejectWhenAborted(options.signal),
        };
      }
      return jsonResponse({ recovered: true });
    }
    throw new Error(`Unexpected URL ${url}`);
  };

  const result = await computeApi('build_atlas', {}, {
    resultDownloadTimeoutMs: 10,
    sleepFn: async () => {},
  });
  assert.deepEqual(result, { recovered: true });
  assert.equal(resultUrlCalls, 2);
  assert.deepEqual(directUrls, [
    'https://artifacts.example/body-1.json',
    'https://artifacts.example/body-2.json',
  ]);
});

test('exhausted polling cancels or returns a recoverable job identity', async () => {
  cloudComputeState.enabled = true;
  let pollCalls = 0;
  let cancelCalls = 0;
  globalThis.fetch = async (url) => {
    if (url === '/api/v1/jobs') return jsonResponse({ job_id: 'poll-orphan', status: 'queued' });
    if (url === '/api/v1/jobs/poll-orphan') {
      pollCalls += 1;
      return jsonResponse(
        { error: { message: 'poll unavailable', retryable: true } },
        { status: 503, ok: false },
      );
    }
    if (url === '/api/v1/jobs/poll-orphan/cancel') {
      cancelCalls += 1;
      return jsonResponse(
        { error: { message: 'cancel unavailable', retryable: true } },
        { status: 503, ok: false },
      );
    }
    throw new Error(`Unexpected URL ${url}`);
  };

  let error;
  try {
    await computeApi('build_atlas', {}, { sleepFn: async () => {} });
  } catch (caught) {
    error = caught;
  }
  assert.match(error?.message || '', /poll unavailable/);
  assert.equal(pollCalls, 3);
  assert.equal(cancelCalls, 1);
  assert.equal(error?.jobId, 'poll-orphan');
  assert.equal(error?.recoverableJob, true);
  assert.deepEqual(error?.jobRecovery, {
    jobId: 'poll-orphan',
    status: 'queued',
    cancellationAttempted: true,
    cancellationSucceeded: false,
    recoverable: true,
    cancellationError: 'cancel unavailable',
  });
});

test('non-extensible WebKit timeout still carries recoverable job identity', async () => {
  cloudComputeState.enabled = true;
  class LockedDOMException extends Error {
    constructor(message, name) {
      super(message);
      Object.defineProperty(this, 'name', { value: name, configurable: true });
      Object.preventExtensions(this);
    }
  }
  globalThis.DOMException = LockedDOMException;
  let pollCalls = 0;
  let cancelCalls = 0;
  globalThis.fetch = async (url, options) => {
    if (url === '/api/v1/jobs') {
      return jsonResponse({ job_id: 'locked-timeout', status: 'queued' });
    }
    if (url === '/api/v1/jobs/locked-timeout') {
      pollCalls += 1;
      return rejectWhenAborted(options.signal);
    }
    if (url === '/api/v1/jobs/locked-timeout/cancel') {
      cancelCalls += 1;
      return jsonResponse({ job_id: 'locked-timeout', status: 'cancel_requested' });
    }
    throw new Error(`Unexpected URL ${url}`);
  };

  let error;
  try {
    await computeApi('build_atlas', {}, {
      jobRequestTimeoutMs: 5,
      sleepFn: async () => {},
    });
  } catch (caught) {
    error = caught;
  }
  assert.equal(error?.name, 'TimeoutError');
  assert.equal(error?.jobId, 'locked-timeout');
  assert.equal(error?.jobRecovery?.cancellationSucceeded, true);
  assert.equal(error?.cause instanceof LockedDOMException, true);
  assert.equal(Object.isExtensible(error?.cause), false);
  assert.equal(pollCalls, 3);
  assert.equal(cancelCalls, 1);
});

test('request composition works without AbortSignal.any or AbortSignal.timeout', async () => {
  const anyDescriptor = Object.getOwnPropertyDescriptor(AbortSignal, 'any');
  const timeoutDescriptor = Object.getOwnPropertyDescriptor(AbortSignal, 'timeout');
  Object.defineProperty(AbortSignal, 'any', { configurable: true, value: undefined });
  Object.defineProperty(AbortSignal, 'timeout', { configurable: true, value: undefined });
  try {
    let capturedSignal = null;
    globalThis.fetch = async (url, options) => {
      capturedSignal = options.signal;
      return jsonResponse({ fallback: true });
    };
    assert.deepEqual(await apiSilent('version', {}), { fallback: true });
    assert.ok(capturedSignal instanceof AbortSignal);

    const controller = new AbortController();
    globalThis.fetch = async (url, options) => rejectWhenAborted(options.signal);
    const pending = apiSilent('version', {}, { signal: controller.signal });
    controller.abort();
    await assert.rejects(pending, error => error?.name === 'AbortError');
  } finally {
    if (anyDescriptor) Object.defineProperty(AbortSignal, 'any', anyDescriptor);
    else delete AbortSignal.any;
    if (timeoutDescriptor) Object.defineProperty(AbortSignal, 'timeout', timeoutDescriptor);
    else delete AbortSignal.timeout;
  }
});
