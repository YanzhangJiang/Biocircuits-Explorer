import assert from 'node:assert/strict';

const elements = new Map();
const requests = [];
const plots = [];
const timers = new Map();
let nextTimerId = 1;

class FakeClassList {
  constructor() { this.values = new Set(); }
  add(...values) { values.forEach(value => this.values.add(value)); }
  remove(...values) { values.forEach(value => this.values.delete(value)); }
  contains(value) { return this.values.has(value); }
}

class FakeElement {
  constructor(tagName = 'div', { type = '', value = '' } = {}) {
    this.tagName = String(tagName).toUpperCase();
    this.type = type;
    this.value = String(value);
    this.checked = false;
    this.innerHTML = '';
    this.textContent = '';
    this.style = {};
    this.dataset = {};
    this.classList = new FakeClassList();
    this.controls = [];
    this.listeners = new Map();
    this.options = [];
  }
  addEventListener(type, callback) {
    if (!this.listeners.has(type)) this.listeners.set(type, []);
    this.listeners.get(type).push(callback);
  }
  dispatch(type) {
    for (const callback of this.listeners.get(type) || []) callback({ target: this });
  }
  querySelectorAll(selector) {
    return selector === '.auto-update' ? this.controls : [];
  }
  querySelector() { return null; }
  add(option) {
    this.options.push(option);
    if (!this.value) this.value = option.value;
  }
  appendChild() {}
  remove() {}
}

function fakeSetTimeout(callback, delay = 0) {
  const id = nextTimerId++;
  timers.set(id, { callback, delay });
  return id;
}

function fakeClearTimeout(id) {
  timers.delete(id);
}

function runTimersThrough(maxDelay) {
  while (true) {
    const ready = [...timers.entries()]
      .filter(([, timer]) => timer.delay <= maxDelay)
      .sort((left, right) => left[0] - right[0]);
    if (!ready.length) return;
    for (const [id, timer] of ready) {
      if (!timers.delete(id)) continue;
      timer.callback();
    }
  }
}

globalThis.setTimeout = fakeSetTimeout;
globalThis.clearTimeout = fakeClearTimeout;
globalThis.window = {
  matchMedia: () => null,
  addEventListener() {},
  dispatchEvent() {},
  location: { protocol: 'http:', hostname: '127.0.0.1', port: '8000' },
  sessionStorage: { getItem: () => null, setItem() {} },
  localStorage: { getItem: () => null, setItem() {} },
  crypto: { randomUUID: () => 'derived-result-lifecycle-test' },
  setTimeout: fakeSetTimeout,
  clearTimeout: fakeClearTimeout,
  BiocircuitsExplorerWorkspaceShell: { notifyWorkspaceChanged: () => true },
};
globalThis.document = {
  readyState: 'loading',
  documentElement: { dataset: {}, style: { setProperty() {} } },
  getElementById(id) { return elements.get(id) || null; },
  createElement(tagName) { return new FakeElement(tagName); },
  addEventListener() {},
  querySelectorAll() { return []; },
  querySelector() { return null; },
};
globalThis.HTMLSelectElement = class HTMLSelectElement {};
globalThis.Option = class Option {
  constructor(text, value) { this.text = text; this.value = value; }
};
globalThis.ResizeObserver = class ResizeObserver { observe() {} disconnect() {} };
globalThis.getComputedStyle = () => ({ getPropertyValue: () => '' });
globalThis.requestAnimationFrame = callback => callback();
globalThis.cancelAnimationFrame = () => {};
globalThis.CustomEvent = class CustomEvent {};
globalThis.alert = () => {};
globalThis.Plotly = {
  newPlot(plotId) { plots.push(plotId); },
  purge() {},
  Plots: { resize() {} },
};
globalThis.fetch = (url, options) => new Promise(resolve => {
  requests.push({ url, options, resolve });
});

const siso = await import('../public/js/siso.js');
const ropCloud = await import('../public/js/rop-cloud.js');
const scan = await import('../public/js/scan.js');
const regimeGraph = await import('../public/js/regime-graph.js');
const {
  advanceWorkspaceRuntimeEpoch,
  nodeRegistry,
  setConnections,
} = await import('../public/js/state.js');
const { setupAutoUpdate } = await import('../public/js/nodes.js');

let passed = 0;
async function test(name, callback) {
  await callback();
  passed += 1;
  console.log(`  ok - ${name}`);
}

function addElement(id, tagName = 'div', options = {}) {
  const element = new FakeElement(tagName, options);
  elements.set(id, element);
  return element;
}

function addNode(id, type) {
  const root = addElement(id);
  const content = addElement(`${id}-content`);
  nodeRegistry[id] = { type, data: {}, el: root };
  return { root, content };
}

function resetHarness() {
  Object.keys(nodeRegistry).forEach(key => delete nodeRegistry[key]);
  elements.clear();
  requests.length = 0;
  plots.length = 0;
  timers.clear();
  addElement('status-badge');
  addElement('toast-container');
}

function installModelChain(resultType = 'fret-result', configType = 'fret-params') {
  addNode('builder', 'model-builder');
  nodeRegistry.builder.data = {
    built: true,
    modelContext: {
      sessionId: 'session-current',
      networkIrHash: 'network-hash',
      networkIr: { ir_schema_version: 'bne-ir/v1.0.0', reactions: [] },
      inputFingerprint: 'model-input-a',
      builtForRevision: 0,
      qK_syms: ['q_A', 'q_B'],
      model: { q_sym: ['q_A', 'q_B'], K_sym: [], x_sym: ['A', 'B'] },
    },
  };
  nodeRegistry.builder._modelInputRevision = 0;
  const { root } = addNode('config', configType);
  addNode('result', resultType);
  const grid = addElement('config-grid', 'input', { type: 'number', value: '80' });
  const min = addElement('config-min', 'input', { type: 'number', value: '-6' });
  const max = addElement('config-max', 'input', { type: 'number', value: '6' });
  grid.className = min.className = max.className = 'auto-update';
  root.controls = [grid, min, max];
  setConnections([
    { fromNode: 'builder', fromPort: 'model', toNode: 'config', toPort: 'model' },
    { fromNode: 'config', fromPort: 'params', toNode: 'result', toPort: 'params' },
  ]);
  return { grid, root };
}

function installSISOChain() {
  installModelChain('siso-result', 'siso-params');
  const values = {
    'siso-select': 'q_A',
    'target-x': 'A',
    'path-scope': 'feasible',
    'min-volume': '0',
    'keep-singular': '',
    'keep-nonasym': '',
    min: '-6',
    max: '6',
  };
  for (const [suffix, value] of Object.entries(values)) {
    const tagName = ['siso-select', 'target-x', 'path-scope'].includes(suffix) ? 'select' : 'input';
    const element = addElement(`config-${suffix}`, tagName, { value });
    if (suffix === 'keep-singular') element.checked = true;
  }
}

function fretResponse(marker) {
  return {
    marker,
    q_sym: ['q_A', 'q_B'],
    logq1: [-1, 1],
    logq2: [-1, 1],
    fret: [[1, 2], [3, 4]],
    bounds: [[0, 0], [1, 1]],
    validity_grid: [[true, true], [true, true]],
    partial: false,
  };
}

function respond(request, payload, status = 200) {
  request.resolve({
    ok: status >= 200 && status < 300,
    status,
    headers: { get: () => 'application/json; charset=utf-8' },
    json: async () => payload,
  });
}

async function waitForRequests(count) {
  for (let attempt = 0; attempt < 30 && requests.length < count; attempt += 1) {
    await Promise.resolve();
  }
  assert.equal(requests.length, count);
}

await test('all priority owners expose the unified lifecycle inspection and invalidation surface', () => {
  assert.equal(typeof siso.inspectSISOResultLifecycle, 'function');
  assert.equal(typeof siso.inspectQKPolyResultLifecycle, 'function');
  assert.equal(typeof siso.invalidateSISOResultsDownstreamOf, 'function');
  assert.equal(typeof ropCloud.inspectROPCloudResultLifecycle, 'function');
  assert.equal(typeof ropCloud.inspectFRETResultLifecycle, 'function');
  assert.equal(typeof ropCloud.invalidateDerivedResultsDownstreamOf, 'function');
  assert.equal(typeof scan.inspectROPPolyResultLifecycle, 'function');
  assert.equal(typeof scan.invalidateROPPolyResultsDownstreamOf, 'function');
  assert.equal(typeof regimeGraph.inspectRegimeGraphLifecycle, 'function');
  assert.equal(typeof regimeGraph.invalidateRegimeGraphResult, 'function');
});

await test('SISO may finish current with no selected path and advertises a missing output', async () => {
  resetHarness();
  installSISOChain();
  const run = siso.computeSISOResult('result');
  await waitForRequests(1);
  assert.equal(requests[0].url, '/api/v1/behavior_families');
  respond(requests[0], {
    change_qK: 'q_A',
    observe_x: 'A',
    exact_families: [],
    paths: [],
    included_paths: 0,
    excluded_paths: 0,
    exclusion_counts: {},
  });
  const outcome = await run;
  assert.equal(outcome.status, 'succeeded');
  assert.equal(outcome.outputs.result, 'missing');
  assert.equal(outcome.code, 'siso_path_not_selected');
  assert.equal(siso.inspectSISOResultLifecycle('result').freshness, 'current');
});

await test('Cloud, Poly, and Regime execute through their exact bound endpoints', async () => {
  resetHarness();
  installModelChain('rop-cloud-result', 'rop-cloud-params');
  addElement('config-sampling-mode', 'select', { value: 'qk' });
  addElement('config-samples', 'input', { type: 'number', value: '100' });
  addElement('config-span', 'input', { type: 'number', value: '4' });
  addElement('config-logx-min', 'input', { type: 'number', value: '-6' });
  addElement('config-logx-max', 'input', { type: 'number', value: '6' });
  addElement('config-target-species', 'select', { value: 'A' });
  let run = ropCloud.executeROPCloudResult('result');
  await waitForRequests(1);
  assert.equal(requests[0].url, '/api/v1/rop_cloud');
  respond(requests[0], {
    marker: 'cloud', q_sym: ['q_A', 'q_B'], d: 2,
    reaction_orders: [[1, -1]], fret_values: [1], valid: [true], partial: false,
  });
  assert.equal(await run, true);
  assert.equal(ropCloud.inspectROPCloudResultLifecycle('result').state, 'current');

  resetHarness();
  installModelChain('rop-poly-result', 'rop-poly-params');
  addElement('config-dimension', 'select', { value: '2' });
  addElement('config-add-inner-points', 'input').checked = true;
  addElement('config-npoints', 'input', { type: 'number', value: '50' });
  addElement('config-singular-extends', 'input', { type: 'number', value: '2' });
  addElement('config-x1', 'select', { value: 'A' });
  addElement('config-qk1', 'select', { value: 'q_A' });
  addElement('config-x2', 'select', { value: 'B' });
  addElement('config-qk2', 'select', { value: 'q_B' });
  run = scan.executeROPPolyResult('result');
  await waitForRequests(1);
  assert.equal(requests[0].url, '/api/v1/rop_polyhedron');
  respond(requests[0], {
    marker: 'poly', dimension: 2,
    pairs: [{ x_symbol: 'A', qk_symbol: 'q_A' }, { x_symbol: 'B', qk_symbol: 'q_B' }],
    vertices: [[0, 0], [1, 0], [0, 1]], rays: [], inner_points: [], is_bounded: true,
  });
  assert.equal(await run, true);
  assert.equal(scan.inspectROPPolyResultLifecycle('result').state, 'current');

  resetHarness();
  installModelChain('regime-graph', 'fret-params');
  setConnections([{ fromNode: 'builder', fromPort: 'model', toNode: 'result', toPort: 'model' }]);
  addElement('result-graph-mode', 'select', { value: 'qk' });
  addElement('result-change-qk', 'select', { value: 'q_A' });
  addElement('result-change-qk-row');
  run = regimeGraph.executeRegimeGraph('result');
  await waitForRequests(1);
  assert.equal(requests[0].url, '/api/v1/build_graph');
  respond(requests[0], { marker: 'graph', nodes: [], edges: [], graph_label: 'test' });
  assert.equal(await run, true);
  assert.equal(regimeGraph.inspectRegimeGraphLifecycle('result').state, 'current');
});

await test('rapid FRET edits are latest-wins and obsolete finally cannot clear newer loading', async () => {
  resetHarness();
  const { grid } = installModelChain();
  const first = ropCloud.executeFRETResult('result');
  await waitForRequests(1);
  grid.value = '120';
  const second = ropCloud.executeFRETResult('result');
  await waitForRequests(2);
  assert.equal(requests[0].url, '/api/v1/fret_heatmap');
  assert.equal(elements.get('result').classList.contains('loading'), true);

  respond(requests[0], fretResponse('old'));
  assert.equal(await first, false);
  assert.equal(elements.get('result').classList.contains('loading'), true);
  assert.equal(nodeRegistry.result.data.fretHeatmapData, undefined);

  respond(requests[1], fretResponse('new'));
  assert.equal(await second, true);
  assert.equal(nodeRegistry.result.data.fretHeatmapData.marker, 'new');
  assert.equal(elements.get('result').classList.contains('loading'), false);
  const lifecycle = ropCloud.inspectFRETResultLifecycle('result');
  assert.equal(lifecycle.state, 'current');
  assert.equal(lifecycle.endpoint, '/api/v1/fret_heatmap');
  assert.equal(lifecycle.evidence.partial, false);
});

await test('owner replacement, workspace epoch drift, and silent fingerprint drift reject publication', async () => {
  resetHarness();
  const { grid } = installModelChain();

  const ownerRun = ropCloud.executeFRETResult('result');
  await waitForRequests(1);
  const oldOwner = nodeRegistry.result;
  addNode('result', 'fret-result');
  respond(requests[0], fretResponse('old-owner'));
  assert.equal(await ownerRun, false);
  assert.notEqual(nodeRegistry.result, oldOwner);
  assert.equal(nodeRegistry.result.data.fretHeatmapData, undefined);

  const epochRun = ropCloud.executeFRETResult('result');
  await waitForRequests(2);
  advanceWorkspaceRuntimeEpoch();
  respond(requests[1], fretResponse('old-epoch'));
  assert.equal(await epochRun, false);
  assert.equal(nodeRegistry.result.data.fretHeatmapData, undefined);

  const fingerprintRun = ropCloud.executeFRETResult('result');
  await waitForRequests(3);
  grid.value = '200';
  respond(requests[2], fretResponse('old-config'));
  assert.equal(await fingerprintRun, false);
  assert.equal(nodeRegistry.result.data.fretHeatmapData, undefined);
  assert.equal(ropCloud.inspectFRETResultLifecycle('result').state, 'invalidated');
});

await test('config input invalidates synchronously and cancels a delayed plot before debounce', async () => {
  resetHarness();
  const { grid } = installModelChain();
  const run = ropCloud.executeFRETResult('result');
  await waitForRequests(1);
  respond(requests[0], fretResponse('current'));
  assert.equal(await run, true);
  assert.equal([...timers.values()].filter(timer => timer.delay === 50).length, 1,
    'fresh result schedules one delayed plot');

  ropCloud.installDerivedResultInvalidation('config');
  setupAutoUpdate('config', 'fret-params');
  grid.value = '160';
  grid.dispatch('input');
  assert.equal(nodeRegistry.result.data.fretHeatmapData, undefined);
  assert.equal(ropCloud.inspectFRETResultLifecycle('result').state, 'invalidated');
  assert.equal(plots.length, 0);
  runTimersThrough(50);
  assert.equal(plots.length, 0, 'invalidated delayed callback cannot plot');
  assert.ok([...timers.values()].some(timer => timer.delay === 500),
    'the serialization debounce is still pending after synchronous invalidation');
});

await test('a restored historical SISO selection is blocked from qK until SISO reruns', async () => {
  resetHarness();
  addNode('siso', 'siso-result');
  addNode('qk', 'qk-poly-result');
  nodeRegistry.siso.data = {
    behaviorData: {
      change_qK: 'q_A', observe_x: 'A', paths: [{
        path_idx: 1, feasible: true, included: true, vertex_indices: [], perms: [],
      }], exact_families: [],
    },
    selectedPath: { path_idx: 1, change_qK: 'q_A', observe_x: 'A' },
    lifecycle: { state: 'historical', freshness: 'historical', evidence: null },
  };
  setConnections([{ fromNode: 'siso', fromPort: 'result', toNode: 'qk', toPort: 'result' }]);

  const outcome = await siso.executeQKPolyResult('qk');
  assert.equal(outcome.status, 'blocked');
  assert.equal(outcome.code, 'upstream_output_historical');
  assert.equal(requests.length, 0);
  assert.equal(siso.inspectSISOResultLifecycle('siso').freshness, 'historical');
  assert.equal(siso.getConnectedSISOSelection('qk').selection, null);
});

await test('Cloud, FRET, Poly, and Regime restored results enter historical lifecycle state', () => {
  resetHarness();
  addNode('cloud', 'rop-cloud-result');
  addNode('fret', 'fret-result');
  addNode('poly', 'rop-poly-result');
  addNode('graph', 'regime-graph');
  const historical = { state: 'historical', freshness: 'historical', evidence: { grade: 'saved' } };
  nodeRegistry.cloud.data = { ropCloudData: { marker: 'saved-cloud' }, lifecycle: historical };
  nodeRegistry.fret.data = { fretHeatmapData: { marker: 'saved-fret' }, lifecycle: historical };
  nodeRegistry.poly.data = { ropPlotData: { marker: 'saved-poly' }, lifecycle: historical };
  nodeRegistry.graph.data = {
    graphData: { marker: 'saved-graph' },
    config: { graphMode: 'qk', changeQK: '', viewMode: '3d' },
    lifecycle: historical,
  };
  setConnections([]);

  const checks = [
    [ropCloud.inspectROPCloudResultLifecycle('cloud'), '/api/v1/rop_cloud'],
    [ropCloud.inspectFRETResultLifecycle('fret'), '/api/v1/fret_heatmap'],
    [scan.inspectROPPolyResultLifecycle('poly'), '/api/v1/rop_polyhedron'],
    [regimeGraph.inspectRegimeGraphLifecycle('graph'), '/api/v1/build_graph'],
  ];
  for (const [runtime, endpoint] of checks) {
    assert.equal(runtime.state, 'historical');
    assert.equal(runtime.freshness, 'historical');
    assert.equal(runtime.endpoint, endpoint);
    assert.deepEqual(runtime.evidence, { grade: 'saved' });
  }
});

console.log(`\nAll ${passed} derived-result lifecycle contract tests passed.`);
