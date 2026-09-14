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

const { state, nodeRegistry } = await import('../public/js/state.js');
const {
  api,
  apiSilent,
  enrichModelRequestPayload,
  optimizeRopShape,
  setStatus,
} = await import('../public/js/api.js');

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

test('model requests use the cached hash and retain NetworkIR for recovery', () => {
  Object.keys(nodeRegistry).forEach((key) => delete nodeRegistry[key]);
  state.model = null;
  nodeRegistry.builder = {
    data: {
      modelContext: {
        sessionId: 'stale-session',
        networkIrHash: 'hash-1',
        networkIr: { reactions: [{ formula: 'A + B <-> AB', kd: 1 }] },
      },
    },
  };

  const original = { session_id: 'stale-session', output_exprs: ['A'] };
  const enriched = enrichModelRequestPayload(original);

  assert.notEqual(enriched, original);
  assert.equal(enriched.session_id, 'stale-session');
  assert.equal(enriched.network_ir_hash, 'hash-1');
  assert.equal(enriched.network, undefined);
  assert.deepEqual(enrichModelRequestPayload(enriched, { recover: true }).network,
    { reactions: [{ formula: 'A + B <-> AB', kd: 1 }] });
  assert.equal(original.network, undefined, 'must not mutate caller payload');
});

test('model requests keep explicit network payloads untouched', () => {
  const payload = { session_id: 'stale-session', network: { label: 'explicit' } };
  assert.equal(enrichModelRequestPayload(payload), payload);
});

test('recovery never substitutes a different explicit model identity', () => {
  const payload = { session_id: 'stale-session', network_ir_hash: 'other-model' };
  assert.equal(enrichModelRequestPayload(payload, { recover: true }), payload);
});

test('recovery requires a matching identity in the saved model context', () => {
  const ctx = nodeRegistry.builder.data.modelContext;
  const hash = ctx.networkIrHash;
  delete ctx.networkIrHash;
  const payload = { session_id: 'stale-session', network_ir_hash: 'hash-1' };
  assert.equal(enrichModelRequestPayload(payload, { recover: true }), payload);
  ctx.networkIrHash = hash;
});

for (const call of [api, apiSilent]) {
  const previousFetch = globalThis.fetch;
  const requests = [];
  try {
    globalThis.fetch = async (_url, options) => {
      requests.push({ body: JSON.parse(options.body), signal: options.signal });
      if (requests.length === 1) {
        nodeRegistry.builder.data.modelContext.networkIr = { label: 'edited-during-request' };
        return jsonResponse({ error: 'Model evicted', need_network: true }, { status: 409 });
      }
      return jsonResponse({ recovered: true });
    };
    nodeRegistry.builder.data.modelContext.networkIr = { label: 'original-network' };
    assert.deepEqual(await call('parameter_scan_1d', { session_id: 'stale-session' }),
      { recovered: true });
    assert.equal(requests.length, 2);
    assert.equal(requests[0].body.network, undefined);
    assert.deepEqual(requests[1].body.network, { label: 'original-network' });
    assert.equal(requests[0].signal, requests[1].signal, 'recovery shares the original deadline');

    let attempts = 0;
    globalThis.fetch = async () => {
      attempts += 1;
      return jsonResponse({ error: 'Still missing', need_network: true }, { status: 409 });
    };
    await assert.rejects(() => call('parameter_scan_1d', { session_id: 'stale-session' }), /Still missing/);
    assert.equal(attempts, 2, 'recovery is attempted only once');
    attempts = 0;
    globalThis.fetch = async () => {
      attempts += 1;
      return jsonResponse({ error: 'Capacity full' }, { status: 429 });
    };
    await assert.rejects(() => call('parameter_scan_1d', { session_id: 'stale-session' }), /Capacity full/);
    assert.equal(attempts, 1, 'ordinary failures do not rebuild models');
    passed += 1;
  } finally {
    globalThis.fetch = previousFetch;
  }
}

{
  const priorFetch = globalThis.fetch;
  const calledUrls = [];
  try {
    globalThis.fetch = async (url) => {
      calledUrls.push(url);
      return {
        ok: true,
        status: 200,
        headers: { get: () => 'application/json; charset=utf-8' },
        json: async () => ({ status: 'ok' }),
      };
    };
    await apiSilent('version', {});
    await api('build_model', {});
    await apiSilent('v1/import/sbml', {});
    await apiSilent('/api/v1/version', {});
    assert.deepEqual(calledUrls, [
      '/api/v1/version',
      '/api/v1/build_model',
      '/api/v1/import/sbml',
      '/api/v1/version',
    ]);
    await assert.rejects(() => apiSilent('', {}), /endpoint must not be empty/);
    await assert.rejects(() => apiSilent('/api/v10/version', {}), /Unsupported API version v10/);
    await assert.rejects(() => apiSilent('v2/version', {}), /Unsupported API version v2/);
    passed += 1;
    console.log('  ok - shared browser API helpers use canonical v1 routes');
  } finally {
    globalThis.fetch = priorFetch;
  }
}

{
  const priorFetch = globalThis.fetch;
  const priorGetElementById = globalThis.document.getElementById;
  const badge = { className: '', textContent: '' };
  let rejectOldFetch;
  let oldRequestIsCurrent = true;
  try {
    globalThis.document.getElementById = id => id === 'status-badge' ? badge : null;
    globalThis.fetch = async () => new Promise((_, reject) => {
      rejectOldFetch = reject;
    });
    const request = {
      schema_version: 'bne-rop-shape-optimize-request/v1.0.0',
      edit_intent: { kind: 'broaden_both_ears' },
    };
    const oldRequest = optimizeRopShape(request, {
      statusIsCurrent: () => oldRequestIsCurrent,
    });
    await Promise.resolve();
    oldRequestIsCurrent = false;
    setStatus('done', 'Newer request done');
    rejectOldFetch(new Error('obsolete failure'));
    await assert.rejects(() => oldRequest, /obsolete failure/);
    assert.equal(badge.textContent, 'Newer request done');
    assert.doesNotMatch(badge.className, /error/);
    setStatus('working', 'cleanup');
    passed += 1;
    console.log('  ok - stale specialized API failures cannot overwrite newer status');
  } finally {
    globalThis.fetch = priorFetch;
    globalThis.document.getElementById = priorGetElementById;
  }
}

{
  const priorFetch = globalThis.fetch;
  try {
    globalThis.fetch = async () => ({
      ok: false,
      status: 503,
      headers: { get: () => 'application/json' },
      json: async () => ({ status: 'warming' }),
    });
    await assert.rejects(() => apiSilent('version', {}), /Backend request failed \(503\)/);
    await assert.rejects(() => api('build_model', {}), /Backend request failed \(503\)/);
    passed += 1;
    console.log('  ok - HTTP failures cannot pass as successful JSON payloads');
  } finally {
    globalThis.fetch = priorFetch;
  }
}

{
  const priorFetch = globalThis.fetch;
  const request = {
    schema_version: 'bne-rop-shape-optimize-request/v1.0.0',
    edit_intent: { kind: 'broaden_both_ears' },
  };
  let calledUrl = null;
  try {
    globalThis.fetch = async (url, options) => {
      calledUrl = url;
      assert.equal(options.method, 'POST');
      assert.deepEqual(JSON.parse(options.body), request);
      return {
        ok: true,
        status: 200,
        headers: { get: () => 'application/json; charset=utf-8' },
        json: async () => ({ schema_version: 'bne-rop-shape-optimization/v1.0.0' }),
      };
    };
    const result = await optimizeRopShape(request);
    assert.equal(calledUrl, '/api/v1/rop_shape_optimize');
    assert.notEqual(calledUrl, '/api/rop_shape_optimize');
    assert.equal(result.schema_version, 'bne-rop-shape-optimization/v1.0.0');
    await assert.rejects(() => optimizeRopShape(null), /request must be an object/);
    globalThis.fetch = async () => ({
      ok: true,
      status: 200,
      headers: { get: () => 'application/json' },
      json: async () => null,
    });
    await assert.rejects(() => optimizeRopShape(request), /invalid ROP shape optimization payload/);
    passed += 1;
    console.log('  ok - ROP shape client uses only the canonical versioned route');
  } finally {
    globalThis.fetch = priorFetch;
  }
}

console.log(`\nAll ${passed} model request tests passed.`);
