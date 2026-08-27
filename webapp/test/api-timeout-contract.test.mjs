// Contract: every canonical API request in api.js carries a bounded timeout
// signal. A caller-supplied signal is combined with the default timeout, and an
// abort rejects the pending request instead of leaving the status badge stuck
// on "Computing..." forever.
import assert from 'node:assert/strict';

global.window = {
  matchMedia: () => null,
  crypto: { randomUUID: () => 'debug-test' },
  sessionStorage: { getItem: () => null, setItem: () => {} },
};
global.document = {
  getElementById: () => null,
  documentElement: {
    dataset: {},
    style: { setProperty: () => {} },
  },
};

const { api, apiSilent, optimizeRopShape, submitJob, getJob } = await import('../public/js/api.js');

let passed = 0;
function test(name, fn) {
  fn();
  passed += 1;
  console.log(`  ok - ${name}`);
}

function jsonResponse(json, { status = 200, ok = status >= 200 && status < 300 } = {}) {
  return {
    ok,
    status,
    headers: { get: () => 'application/json' },
    json: async () => json,
  };
}

// Captures the signal of the next fetch and answers with a valid payload.
function captureSignal(response = {}) {
  let captured = null;
  globalThis.fetch = async (url, options) => {
    captured = options.signal;
    return jsonResponse({ ok: true, ...response });
  };
  return () => captured;
}

test('api attaches a default timeout signal', async () => {
  const captured = captureSignal({});
  await api('build_model', {});
  assert.ok(captured() instanceof AbortSignal);
  assert.equal(captured().aborted, false);
});

test('apiSilent attaches a default timeout signal', async () => {
  const captured = captureSignal({});
  await apiSilent('build_model', {});
  assert.ok(captured() instanceof AbortSignal);
});

test('optimizeRopShape attaches a default timeout signal', async () => {
  const captured = captureSignal({ rop_shape: { status: 'ok' } });
  await optimizeRopShape({ network: {} });
  assert.ok(captured() instanceof AbortSignal);
});

test('job endpoints attach a timeout signal', async () => {
  const captured = captureSignal({ job_id: 'j-1', status: 'queued' });
  await submitJob('build_atlas', {});
  assert.ok(captured() instanceof AbortSignal);
  const capturedPoll = captureSignal({ job_id: 'j-1', status: 'succeeded' });
  await getJob('j-1');
  assert.ok(capturedPoll() instanceof AbortSignal);
});

test('a caller abort rejects the pending request', async () => {
  const controller = new AbortController();
  globalThis.fetch = async (url, options) => new Promise((resolve, reject) => {
    options.signal.addEventListener('abort', () => {
      const error = new Error('aborted');
      error.name = 'AbortError';
      reject(error);
    }, { once: true });
  });
  const pending = api('build_model', {}, { signal: controller.signal });
  controller.abort();
  await assert.rejects(pending, (error) => error.name === 'AbortError');
});

console.log(`api timeout contract: ${passed}/${passed} passed`);
