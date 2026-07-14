import assert from 'node:assert/strict';

const elements = new Map();
const makeClassList = () => {
  const values = new Set();
  return {
    add: value => values.add(value),
    remove: value => values.delete(value),
    contains: value => values.has(value),
  };
};
const input = value => ({ value: String(value), classList: makeClassList() });
const content = () => ({
  dataset: {},
  innerHTML: '',
  insertAdjacentHTML() {},
  querySelector: () => null,
  querySelectorAll: () => [],
});

const toastContainer = { appendChild() {} };
global.window = {
  matchMedia: () => null,
  crypto: { randomUUID: () => 'placer-lifecycle-test' },
  sessionStorage: { getItem: () => null, setItem() {} },
  localStorage: { getItem: () => null, setItem() {} },
  setTimeout,
  clearTimeout,
  dispatchEvent() {},
};
global.document = {
  getElementById: id => id === 'toast-container' ? toastContainer : (elements.get(id) || null),
  createElement: () => ({
    className: '',
    classList: makeClassList(),
    style: {},
    textContent: '',
    remove() {},
  }),
  documentElement: { dataset: {}, style: { setProperty() {} } },
};
global.requestAnimationFrame = callback => callback();
global.CustomEvent = class CustomEvent {
  constructor(type, options = {}) {
    this.type = type;
    this.detail = options.detail;
  }
};
global.alert = () => {};

const {
  advanceWorkspaceRuntimeEpoch,
  connections,
  nodeRegistry,
} = await import('../public/js/state.js');
await import('../public/js/node-types/index.js');
const {
  executePlacerResult,
  inspectPlacerExecution,
  readCurrentPlacerResult,
  restorePlacerResultView,
} = await import('../public/js/node-types/placer.js');
const {
  invalidateAtlasExecutionsDownstreamOf,
  invalidatePlacerExecution,
} = await import('../public/js/execution-lifecycle.js');
const { serializeNodeBySchema } = await import('../public/js/node-schema.js');

let passed = 0;
async function test(name, fn) {
  await fn();
  passed += 1;
  console.log(`  ok - ${name}`);
}

function resetGraph() {
  Object.keys(nodeRegistry).forEach(key => delete nodeRegistry[key]);
  connections.splice(0, connections.length);
  elements.clear();

  nodeRegistry.source = { type: 'reaction-network', data: {} };
  nodeRegistry.builder = {
    type: 'model-builder',
    data: {
      built: true,
      modelContext: {
        sessionId: 'runtime-session',
        networkIrHash: 'a'.repeat(64),
        networkIr: { ir_schema_version: 'bne-ir/v1.0.0', label: 'test-network' },
        inputFingerprint: 'model-input-v1',
        sourceNodeId: 'source',
        model: {
          q_sym: ['A_tot'],
          K_sym: ['Kd1'],
          x_sym: ['A'],
          network_ir_hash: 'a'.repeat(64),
        },
      },
    },
  };
  nodeRegistry.params = { type: 'placer-params', data: {} };
  nodeRegistry.result = { type: 'placer-result', data: {} };
  connections.push(
    { fromNode: 'source', fromPort: 'reactions', toNode: 'builder', toPort: 'reactions' },
    { fromNode: 'builder', fromPort: 'model', toNode: 'params', toPort: 'model' },
    { fromNode: 'params', fromPort: 'params', toNode: 'result', toPort: 'params' },
  );

  elements.set('params-input', input('A_tot'));
  elements.set('params-output', input('A'));
  elements.set('params-target', input('1'));
  elements.set('params-kdlo', input('-3'));
  elements.set('params-kdhi', input('3'));
  elements.set('result', { classList: makeClassList() });
  elements.set('result-content', content());
  elements.set('result-menu', content());
}

function response(marker) {
  return {
    kd: [marker],
    totals: { A_tot: 1 },
    predicted_RO: marker,
    measured_RO: marker,
    pass: true,
  };
}

function fetchResponse(payload) {
  return {
    ok: true,
    status: 200,
    headers: { get: () => 'application/json' },
    json: async () => payload,
  };
}

async function flushMicrotasks(count = 6) {
  for (let idx = 0; idx < count; idx += 1) await Promise.resolve();
}

await test('rapid replacement runs are latest-wins and obsolete finally keeps replacement loading', async () => {
  resetGraph();
  const priorFetch = globalThis.fetch;
  const pending = [];
  globalThis.fetch = () => new Promise(resolve => pending.push(resolve));
  try {
    const first = executePlacerResult('result');
    await flushMicrotasks();
    const second = executePlacerResult('result');
    await flushMicrotasks();
    assert.equal(pending.length, 2);
    pending[0](fetchResponse(response(1)));
    assert.equal(await first, false);
    assert.equal(elements.get('result').classList.contains('loading'), true);
    assert.equal(readCurrentPlacerResult('result'), null);

    pending[1](fetchResponse(response(2)));
    assert.equal(await second, true);
    assert.equal(readCurrentPlacerResult('result').kd[0], 2);
    assert.equal(inspectPlacerExecution('result').endpoint, '/api/v1/place_parameters');
    assert.equal(elements.get('result').classList.contains('loading'), false);
    assert.doesNotMatch(JSON.stringify(nodeRegistry.result.data.placerResult), /session_?id/i);
    const serialized = serializeNodeBySchema('result', 'placer-result');
    assert.equal(serialized.placerResult.kd[0], 2);
    assert.doesNotMatch(JSON.stringify(serialized), /session_?id/i);
    assert.equal(inspectPlacerExecution('result').sessionId, 'runtime-session');
  } finally {
    globalThis.fetch = priorFetch;
  }
});

await test('config invalidation retires a pending response before the debounced config catches up', async () => {
  resetGraph();
  const priorFetch = globalThis.fetch;
  let resolveFetch;
  globalThis.fetch = () => new Promise(resolve => { resolveFetch = resolve; });
  try {
    const run = executePlacerResult('result');
    await flushMicrotasks();
    elements.get('params-target').value = '3';
    assert.deepEqual(
      invalidateAtlasExecutionsDownstreamOf('params', 'placer-config-input-changed'),
      ['result'],
    );
    resolveFetch(fetchResponse(response(1)));
    assert.equal(await run, false);
    assert.equal(inspectPlacerExecution('result').freshness, 'invalidated');
    assert.equal(nodeRegistry.result.data.placerResult, undefined);
  } finally {
    globalThis.fetch = priorFetch;
  }
});

await test('an invalidated delayed plot callback cannot publish', async () => {
  resetGraph();
  const priorFetch = globalThis.fetch;
  const priorPlotly = globalThis.Plotly;
  let plotCalls = 0;
  globalThis.Plotly = { newPlot: () => { plotCalls += 1; } };
  const payload = {
    ...response(1),
    dose_response_curve: {
      param_symbol: 'A_tot',
      param_values: [-1, 1],
      output_exprs: ['A'],
      output_traj: [[-1], [1]],
      valid: [true, true],
      partial: false,
    },
  };
  globalThis.fetch = async () => fetchResponse(payload);
  try {
    assert.equal(await executePlacerResult('result'), true);
    invalidatePlacerExecution('result', 'plot-input-invalidated');
    await new Promise(resolve => setTimeout(resolve, 80));
    assert.equal(plotCalls, 0);
    assert.equal(inspectPlacerExecution('result').freshness, 'invalidated');
  } finally {
    globalThis.fetch = priorFetch;
    globalThis.Plotly = priorPlotly;
  }
});

await test('owner replacement and workspace-epoch drift reject delayed placement responses', async () => {
  for (const drift of ['owner', 'epoch']) {
    resetGraph();
    const priorFetch = globalThis.fetch;
    let resolveFetch;
    globalThis.fetch = () => new Promise(resolve => { resolveFetch = resolve; });
    try {
      const oldOwner = nodeRegistry.result;
      const run = executePlacerResult('result');
      await flushMicrotasks();
      if (drift === 'owner') nodeRegistry.result = { type: 'placer-result', data: {} };
      else advanceWorkspaceRuntimeEpoch();
      resolveFetch(fetchResponse(response(4)));
      assert.equal(await run, false);
      assert.equal(nodeRegistry.result.data.placerResult, undefined);
      if (drift === 'owner') assert.notEqual(nodeRegistry.result, oldOwner);
    } finally {
      globalThis.fetch = priorFetch;
    }
  }
});

await test('restored Placer output is historical, strips sessions, and cannot be consumed as current', async () => {
  resetGraph();
  restorePlacerResultView('result', {
    placerResult: {
      endpoint: '/api/v1/placer_curve',
      action: 'curve',
      sessionId: 'saved-session-must-die',
      config: { input_sym: 'A_tot', output_sym: 'A' },
      kd: [1],
      totals: { A_tot: 1 },
      kdBounds: [-3, 3],
      response: { nested: { session_id: 'also-must-die' } },
    },
    lifecycle: {
      state: 'current',
      freshness: 'current',
      evidence: { evidence_grade: 'current-computation' },
    },
  });
  const runtime = inspectPlacerExecution('result');
  assert.equal(runtime.state, 'historical');
  assert.equal(runtime.freshness, 'historical');
  assert.equal(runtime.sessionId, null);
  assert.equal(readCurrentPlacerResult('result'), null);
  assert.doesNotMatch(JSON.stringify(nodeRegistry.result.data.placerResult), /session_?id/i);
  assert.match(elements.get('result-content').innerHTML, /Historical saved Parameter Placer result/);
});

console.log(`\nAll ${passed} Parameter Placer execution lifecycle tests passed.`);
