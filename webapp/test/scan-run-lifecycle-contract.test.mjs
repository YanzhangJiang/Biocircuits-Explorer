import assert from 'node:assert/strict';

const elements = new Map();
const requests = [];
const plots = [];
const alerts = [];
const timers = new Map();
let nextTimerId = 1;

class FakeClassList {
  constructor() { this.values = new Set(); }
  add(...values) { values.forEach(value => this.values.add(value)); }
  remove(...values) { values.forEach(value => this.values.delete(value)); }
  contains(value) { return this.values.has(value); }
  toggle(value, force) {
    const enabled = force == null ? !this.values.has(value) : !!force;
    if (enabled) this.values.add(value);
    else this.values.delete(value);
    return enabled;
  }
}

class FakeElement {
  constructor(tagName = 'div') {
    this.tagName = String(tagName).toUpperCase();
    this.type = '';
    this.value = '';
    this.checked = false;
    this.style = {};
    this.dataset = {};
    this.className = '';
    this.classList = new FakeClassList();
    this.innerHTML = '';
    this.textContent = '';
    this.children = [];
    this.controls = [];
    this.listeners = new Map();
    this.parentNode = null;
    this.options = [];
    this._id = '';
  }

  set id(value) {
    if (this._id) elements.delete(this._id);
    this._id = String(value);
    if (this._id) elements.set(this._id, this);
  }
  get id() { return this._id; }

  appendChild(child) {
    child.parentNode = this;
    this.children.push(child);
    return child;
  }
  insertBefore(child) { return this.appendChild(child); }
  remove() { if (this._id) elements.delete(this._id); }
  querySelector() { return null; }
  querySelectorAll(selector) {
    if (selector.includes('input') || selector.includes('select') || selector === '.auto-update') {
      return this.controls;
    }
    return [];
  }
  addEventListener(type, callback) {
    if (!this.listeners.has(type)) this.listeners.set(type, []);
    this.listeners.get(type).push(callback);
  }
  dispatch(type) {
    for (const callback of this.listeners.get(type) || []) callback({ target: this });
  }
  setAttribute() {}
  removeAttribute() {}
}

class FakeSelectElement extends FakeElement {
  constructor() { super('select'); }
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

global.Element = FakeElement;
global.HTMLSelectElement = FakeSelectElement;
global.ResizeObserver = class ResizeObserver { observe() {} disconnect() {} };
global.setTimeout = fakeSetTimeout;
global.clearTimeout = fakeClearTimeout;
global.requestAnimationFrame = callback => { callback(); return 0; };
global.cancelAnimationFrame = () => {};
global.CustomEvent = class CustomEvent {};
global.Event = class Event { constructor(type) { this.type = type; } };
global.getComputedStyle = () => ({ getPropertyValue: () => '' });
global.alert = message => alerts.push(message);
global.Plotly = {
  newPlot: (plotId, traces) => plots.push({ plotId, traces }),
  Plots: { resize: () => {} },
};

global.window = {
  matchMedia: () => null,
  crypto: { randomUUID: () => 'scan-lifecycle-test' },
  sessionStorage: { getItem: () => null, setItem: () => {} },
  localStorage: { getItem: () => null, setItem: () => {}, removeItem: () => {} },
  addEventListener: () => {},
  dispatchEvent: () => {},
  setTimeout: fakeSetTimeout,
  clearTimeout: fakeClearTimeout,
  BiocircuitsExplorerWorkspaceShell: { notifyWorkspaceChanged: () => true },
};

const documentElement = new FakeElement('html');
documentElement.style.setProperty = () => {};
global.document = {
  getElementById: id => elements.get(id) || null,
  createElement: tagName => new FakeElement(tagName),
  createElementNS: (_namespace, tagName) => new FakeElement(tagName),
  querySelector: () => null,
  querySelectorAll: () => [],
  addEventListener: () => {},
  documentElement,
  head: new FakeElement('head'),
  body: new FakeElement('body'),
};

global.fetch = (url, options) => new Promise((resolve) => {
  requests.push({ url, options, resolve });
});

const stateModule = await import('../public/js/state.js');
const {
  advanceWorkspaceRuntimeEpoch,
  getWorkspaceRuntimeEpoch,
  nodeRegistry,
  setConnections,
  plotResizeObservers,
  wiringState,
} = stateModule;
const scanModule = await import('../public/js/scan.js');
const {
  runParameterScan1D, executeScan1DResult,
  runParameterScan2D, executeScan2DResult,
  setupLegacyScanInputInvalidation,
} = scanModule;
const {
  beginScanExecution,
  inspectScanExecution,
  invalidateScanExecution,
  readCurrentScanResult,
} = await import('../public/js/execution-lifecycle.js');
const {
  invalidateModelBuilder, replaceConnectionsWithModelInvalidation,
} = await import('../public/js/model-lifecycle.js');
const { setupAutoUpdate } = await import('../public/js/nodes.js');
const { restoreCachedNodeRuntime } = await import('../public/js/workspace.js');

const RUN_MODES = [
  { name: 'legacy 1D', dimension: 1, paired: false, type: 'parameter-scan-1d', run: runParameterScan1D },
  { name: 'paired 1D', dimension: 1, paired: true, type: 'scan-1d-result', configType: 'scan-1d-params', run: executeScan1DResult },
  { name: 'legacy 2D', dimension: 2, paired: false, type: 'parameter-scan-2d', run: runParameterScan2D },
  { name: 'paired 2D', dimension: 2, paired: true, type: 'scan-2d-result', configType: 'scan-2d-params', run: executeScan2DResult },
];

let passed = 0;
async function test(name, callback) {
  await callback();
  passed += 1;
  console.log(`  ok - ${name}`);
}

function addElement(id, { tagName = 'div', type = '', value = '' } = {}) {
  const element = tagName === 'select' ? new FakeSelectElement() : new FakeElement(tagName);
  element.id = id;
  element.type = type;
  element.value = String(value);
  return element;
}

function addNode(id, type) {
  const root = addElement(id);
  const content = addElement(`${id}-content`);
  nodeRegistry[id] = { type, el: root, data: {} };
  return { root, content };
}

function addScanInputs(id, dimension, variant = 'A') {
  const controls = [];
  const add = (suffix, options) => {
    const element = addElement(`${id}-${suffix}`, options);
    controls.push(element);
    return element;
  };
  if (dimension === 1) {
    add('param', { tagName: 'select', value: variant === 'A' ? 'q_A' : 'q_B' });
    add('min', { tagName: 'input', type: 'number', value: '-3' });
    add('max', { tagName: 'input', type: 'number', value: '3' });
    add('points', { tagName: 'input', type: 'number', value: '20' });
    add('expr', { tagName: 'input', type: 'text', value: variant === 'A' ? 'A' : 'B' });
  } else {
    add('param1', { tagName: 'select', value: 'q_A' });
    add('param2', { tagName: 'select', value: variant === 'A' ? 'q_B' : 'q_C' });
    add('min1', { tagName: 'input', type: 'number', value: '-3' });
    add('max1', { tagName: 'input', type: 'number', value: '3' });
    add('min2', { tagName: 'input', type: 'number', value: '-2' });
    add('max2', { tagName: 'input', type: 'number', value: '2' });
    add('points', { tagName: 'input', type: 'number', value: '12' });
    add('grid', { tagName: 'input', type: 'number', value: '12' });
    add('expr', { tagName: 'input', type: 'text', value: variant === 'A' ? 'A' : 'C' });
  }
  nodeRegistry[id].el.controls = controls;
  return controls;
}

function setVariant(id, dimension, variant) {
  if (dimension === 1) {
    elements.get(`${id}-param`).value = variant === 'A' ? 'q_A' : 'q_B';
    elements.get(`${id}-expr`).value = variant === 'A' ? 'A' : 'B';
  } else {
    elements.get(`${id}-param2`).value = variant === 'A' ? 'q_B' : 'q_C';
    elements.get(`${id}-expr`).value = variant === 'A' ? 'A' : 'C';
  }
}

function resetHarness(mode) {
  for (const id of Object.keys(nodeRegistry)) delete nodeRegistry[id];
  elements.clear();
  requests.length = 0;
  plots.length = 0;
  alerts.length = 0;
  timers.clear();
  plotResizeObservers.clear();
  wiringState.isWiring = false;
  wiringState.connSnapshotBefore = null;

  addElement('status-badge');
  addElement('toast-container');
  addNode('builder', 'model-builder');
  nodeRegistry.builder.data = {
    built: true,
    modelContext: {
      sessionId: 'session-current',
      networkIrHash: 'network-hash',
      inputFingerprint: 'model-input-a',
      builtForRevision: 0,
      qK_syms: ['q_A', 'q_B', 'q_C'],
      model: {
        q_sym: ['q_A', 'q_B', 'q_C'], K_sym: [], x_sym: ['A', 'B', 'C'],
      },
    },
  };
  nodeRegistry.builder._modelInputRevision = 0;

  const resultId = 'result';
  addNode(resultId, mode.type);
  if (mode.paired) {
    addNode('config', mode.configType);
    addScanInputs('config', mode.dimension);
    setConnections([
      { fromNode: 'builder', fromPort: 'model', toNode: 'config', toPort: 'model' },
      { fromNode: 'config', fromPort: 'params', toNode: resultId, toPort: 'params' },
    ]);
  } else {
    addScanInputs(resultId, mode.dimension);
    setConnections([
      { fromNode: 'builder', fromPort: 'model', toNode: resultId, toPort: 'model' },
    ]);
  }
  return {
    resultId,
    inputNodeId: mode.paired ? 'config' : resultId,
  };
}

function scanResponse(dimension, marker) {
  if (dimension === 1) {
    return {
      marker,
      param_symbol: marker === 'new' ? 'q_B' : 'q_A',
      param_values: [-1, 1],
      output_exprs: [marker === 'new' ? 'B' : 'A'],
      output_traj: [[1], [2]],
      valid: [true, true],
    };
  }
  return {
    marker,
    param1_symbol: 'q_A',
    param2_symbol: marker === 'new' ? 'q_C' : 'q_B',
    param1_values: [-1, 1],
    param2_values: [-1, 1],
    output_expr: marker === 'new' ? 'C' : 'A',
    output_grid: [[1, 2], [3, 4]],
    valid: [[true, true], [true, true]],
  };
}

function modelBuildResponse() {
  return {
    session_id: 'session-rebuilt',
    network_ir_hash: 'network-hash',
    network_ir: { version: 'bne-network-ir/v1.0.0' },
    artifact: { kind: 'model' },
    n: 3,
    d: 2,
    r: 1,
    x_sym: ['A', 'B', 'AB'],
    q_sym: ['q_A', 'q_B'],
    K_sym: ['K_1'],
  };
}

function respond(request, json, status = 200) {
  request.resolve({
    status,
    ok: status >= 200 && status < 300,
    headers: { get: () => 'application/json; charset=utf-8' },
    json: async () => json,
  });
}

async function flushMicrotasks(count = 8) {
  for (let index = 0; index < count; index += 1) await Promise.resolve();
}

for (const mode of RUN_MODES) {
  for (const oldOutcome of ['success', 'failure']) {
    await test(`${mode.name} is latest-wins when the old request finishes with ${oldOutcome}`, async () => {
      const { resultId, inputNodeId } = resetHarness(mode);
      const oldRun = mode.run(resultId);
      await flushMicrotasks();
      assert.equal(requests.length, 1);

      setVariant(inputNodeId, mode.dimension, 'B');
      const newRun = mode.run(resultId);
      await flushMicrotasks();
      assert.equal(requests.length, 2);

      respond(requests[1], scanResponse(mode.dimension, 'new'));
      assert.equal(await newRun, true);
      const resultKey = mode.dimension === 1 ? 'scan1DResult' : 'scan2DResult';
      const metaKey = mode.dimension === 1 ? 'scan1DResultMeta' : 'scan2DResultMeta';
      assert.equal(nodeRegistry[resultId].data[resultKey].marker, 'new');
      assert.equal(nodeRegistry[resultId].data[metaKey].historical, false);
      assert.equal(nodeRegistry[resultId].data[metaKey].model.networkIrHash, 'network-hash');
      assert.match(nodeRegistry[resultId].data[metaKey].requestFingerprint, /network-hash/);
      const runtime = inspectScanExecution(resultId);
      assert.equal(runtime.state, 'current');
      assert.equal(runtime.freshness, 'current');
      assert.equal(runtime.owner, nodeRegistry[resultId]);
      assert.equal(runtime.workspaceEpoch, getWorkspaceRuntimeEpoch());
      assert.equal(runtime.endpoint,
        mode.dimension === 1 ? '/api/v1/parameter_scan_1d' : '/api/v1/parameter_scan_2d');
      assert.equal(runtime.inputFingerprint, nodeRegistry[resultId].data[metaKey].requestFingerprint);
      assert.equal(readCurrentScanResult(resultId).result.marker, 'new');

      if (oldOutcome === 'success') {
        respond(requests[0], scanResponse(mode.dimension, 'old'));
      } else {
        respond(requests[0], { error: 'obsolete scan failed' }, 500);
      }
      assert.equal(await oldRun, false);
      assert.equal(nodeRegistry[resultId].data[resultKey].marker, 'new');
      assert.equal(elements.get(resultId).classList.contains('loading'), false);
      assert.equal(elements.get(`${resultId}-content`).innerHTML.includes('node-error'), false);

      runTimersThrough(50);
      assert.equal(plots.length, 1, 'only the latest delayed plot may render');
    });
  }
}

for (const mode of RUN_MODES) {
  await test(`${mode.name} invalid retry retires the pending request before validation`, async () => {
    const { resultId, inputNodeId } = resetHarness(mode);
    const oldRun = mode.run(resultId);
    await flushMicrotasks();
    assert.equal(requests.length, 1);

    elements.get(`${inputNodeId}-expr`).value = '';
    assert.equal(await mode.run(resultId), false);
    assert.equal(requests.length, 1, 'invalid retry must not issue HTTP');
    assert.equal(elements.get(resultId).classList.contains('loading'), false);

    respond(requests[0], scanResponse(mode.dimension, 'old'));
    assert.equal(await oldRun, false);
    const resultKey = mode.dimension === 1 ? 'scan1DResult' : 'scan2DResult';
    assert.equal(nodeRegistry[resultId].data[resultKey], undefined);
    assert.match(elements.get(`${resultId}-content`).innerHTML, /did not run/);
    assert.equal(inspectScanExecution(resultId).state, 'blocked');
  });
}

await test('workspace epoch drift invalidates a pending scan before response publication', async () => {
  const mode = RUN_MODES[0];
  const { resultId } = resetHarness(mode);
  const run = mode.run(resultId);
  await flushMicrotasks();
  assert.equal(requests.length, 1);
  advanceWorkspaceRuntimeEpoch();
  respond(requests[0], scanResponse(1, 'old-epoch'));
  assert.equal(await run, false);
  assert.equal(nodeRegistry[resultId].data.scan1DResult, undefined);
  assert.equal(inspectScanExecution(resultId).state, 'invalidated');
});

await test('config input invalidates a paired result immediately, before debounced persistence', async () => {
  const mode = RUN_MODES[1];
  const { resultId } = resetHarness(mode);
  setupAutoUpdate('config', 'scan-1d-params');
  nodeRegistry[resultId].data.scan1DResult = scanResponse(1, 'old');
  nodeRegistry[resultId].data.scan1DResultMeta = { historical: false };
  beginScanExecution(resultId, 'pending-scan');
  elements.get(resultId).classList.add('loading');

  const numericInput = elements.get('config-min');
  numericInput.value = '-4';
  numericInput.dispatch('input');

  assert.equal(nodeRegistry[resultId].data.scan1DResult, undefined);
  assert.equal(elements.get(resultId).classList.contains('loading'), false);
  assert.match(elements.get(`${resultId}-content`).innerHTML, /Inputs changed/);
  assert.ok([...timers.values()].some(timer => timer.delay === 500), 'config persistence remains debounced');
});

await test('a pending config debounce cannot retire the run that already synchronized that value', async () => {
  const mode = RUN_MODES[1];
  const { resultId } = resetHarness(mode);
  setupAutoUpdate('config', 'scan-1d-params');
  const input = elements.get('config-min');
  input.value = '-4';
  input.dispatch('input');

  const run = mode.run(resultId);
  await flushMicrotasks();
  respond(requests[0], scanResponse(1, 'new'));
  assert.equal(await run, true);
  runTimersThrough(500);

  assert.equal(nodeRegistry[resultId].data.scan1DResult.marker, 'new');
  assert.equal(plots.length, 1);
});

await test('legacy input invalidates its own saved result immediately', async () => {
  const mode = RUN_MODES[0];
  const { resultId } = resetHarness(mode);
  setupLegacyScanInputInvalidation(resultId);
  nodeRegistry[resultId].data.scan1DResult = scanResponse(1, 'old');
  elements.get('result-min').dispatch('input');
  assert.equal(nodeRegistry[resultId].data.scan1DResult, undefined);
  assert.match(elements.get('result-content').innerHTML, /Inputs changed/);
});

await test('model invalidation clears a two-hop paired scan result', async () => {
  const mode = RUN_MODES[1];
  const { resultId } = resetHarness(mode);
  nodeRegistry[resultId].data.scan1DResult = scanResponse(1, 'old');
  beginScanExecution(resultId, 'pending-scan');
  elements.get(resultId).classList.add('loading');

  invalidateModelBuilder('builder', 'reaction-edited');
  assert.equal(nodeRegistry[resultId].data.scan1DResult, undefined);
  assert.equal(elements.get(resultId).classList.contains('loading'), false);
});

await test('connection signatures ignore reorder/unrelated edits but invalidate a model or params rewire', async () => {
  const mode = RUN_MODES[1];
  const { resultId } = resetHarness(mode);
  addNode('unrelated-a', 'reaction-network');
  addNode('unrelated-b', 'model-builder');
  addNode('config-2', 'scan-1d-params');
  addScanInputs('config-2', 1, 'B');
  const base = [
    ...stateModule.connections,
    { fromNode: 'unrelated-a', fromPort: 'reactions', toNode: 'unrelated-b', toPort: 'reactions' },
  ];
  setConnections(base);
  nodeRegistry[resultId].data.scan1DResult = scanResponse(1, 'old');

  replaceConnectionsWithModelInvalidation([...base].reverse(), 'reordered');
  assert.ok(nodeRegistry[resultId].data.scan1DResult, 'connection order is not a semantic change');

  const rewired = stateModule.connections.map(connection =>
    connection.toNode === resultId && connection.toPort === 'params'
      ? { ...connection, fromNode: 'config-2' }
      : connection);
  replaceConnectionsWithModelInvalidation(rewired, 'params-rewired');
  assert.equal(nodeRegistry[resultId].data.scan1DResult, undefined);
});

await test('a response during a wire drag uses the committed graph when the gesture is a no-op', async () => {
  const mode = RUN_MODES[1];
  const { resultId } = resetHarness(mode);
  const committed = stateModule.connections.map(connection => ({ ...connection }));
  const run = mode.run(resultId);
  await flushMicrotasks();

  wiringState.connSnapshotBefore = committed;
  wiringState.isWiring = true;
  setConnections(committed.filter(connection =>
    !(connection.toNode === resultId && connection.toPort === 'params')));
  respond(requests[0], scanResponse(1, 'new'));
  assert.equal(await run, true);
  assert.equal(nodeRegistry[resultId].data.scan1DResult.marker, 'new');

  setConnections(committed);
  wiringState.isWiring = false;
  wiringState.connSnapshotBefore = null;
  runTimersThrough(50);
  assert.equal(plots.length, 1);
});

await test('model-session rehydration also uses the committed graph during a no-op wire drag', async () => {
  const mode = RUN_MODES[1];
  const { resultId } = resetHarness(mode);
  addNode('source', 'reaction-network');
  const ruleInput = { value: 'A + B <-> AB' };
  const kdInput = { value: '1' };
  const reactionRow = {
    querySelector: selector => selector === '.reaction-input' ? ruleInput : kdInput,
  };
  const reactionList = addElement('source-reactions-list');
  reactionList.querySelectorAll = () => [reactionRow];
  addElement('builder-model-info');
  addElement('builder-model-info-text');

  const expectedFingerprint = JSON.stringify(['source', ['A + B <-> AB'], [1]]);
  nodeRegistry.builder.data = {
    built: false,
    modelContext: {
      ...nodeRegistry.builder.data.modelContext,
      sessionId: null,
      inputFingerprint: expectedFingerprint,
    },
  };
  const committed = [
    { fromNode: 'source', fromPort: 'reactions', toNode: 'builder', toPort: 'reactions' },
    ...stateModule.connections,
  ];
  setConnections(committed);

  const run = mode.run(resultId);
  await flushMicrotasks();
  assert.equal(requests.length, 1);
  assert.match(requests[0].url, /build_model/);

  wiringState.connSnapshotBefore = committed;
  wiringState.isWiring = true;
  setConnections(committed.filter(connection =>
    !(connection.fromNode === 'builder' && connection.toNode === 'config')));
  respond(requests[0], modelBuildResponse());
  await flushMicrotasks(16);
  assert.equal(requests.length, 2, 'scan request should follow the rebuilt session during the drag');
  assert.match(requests[1].url, /parameter_scan_1d/);

  respond(requests[1], scanResponse(1, 'new'));
  assert.equal(await run, true);
  assert.equal(nodeRegistry[resultId].data.scan1DResult.marker, 'new');

  setConnections(committed);
  wiringState.isWiring = false;
  wiringState.connSnapshotBefore = null;
});

await test('model rebuild input capture uses the committed reaction edge during a no-op drag', async () => {
  const mode = RUN_MODES[1];
  const { resultId } = resetHarness(mode);
  addNode('source', 'reaction-network');
  const reactionRow = {
    querySelector: selector => selector === '.reaction-input'
      ? { value: 'A + B <-> AB' }
      : { value: '1' },
  };
  const reactionList = addElement('source-reactions-list');
  reactionList.querySelectorAll = () => [reactionRow];
  addElement('builder-model-info');
  addElement('builder-model-info-text');

  const expectedFingerprint = JSON.stringify(['source', ['A + B <-> AB'], [1]]);
  nodeRegistry.builder.data = {
    built: false,
    modelContext: {
      ...nodeRegistry.builder.data.modelContext,
      sessionId: null,
      inputFingerprint: expectedFingerprint,
    },
  };
  const committed = [
    { fromNode: 'source', fromPort: 'reactions', toNode: 'builder', toPort: 'reactions' },
    ...stateModule.connections,
  ];
  setConnections(committed);

  const run = mode.run(resultId);
  await flushMicrotasks();
  assert.equal(requests.length, 1);
  assert.match(requests[0].url, /build_model/);

  wiringState.connSnapshotBefore = committed;
  wiringState.isWiring = true;
  setConnections(committed.filter(connection =>
    !(connection.fromNode === 'source' && connection.toNode === 'builder')));
  respond(requests[0], modelBuildResponse());
  await flushMicrotasks(16);
  assert.equal(requests.length, 2);
  assert.match(requests[1].url, /parameter_scan_1d/);

  respond(requests[1], scanResponse(1, 'new'));
  assert.equal(await run, true);
  assert.equal(nodeRegistry[resultId].data.scan1DResult.marker, 'new');

  setConnections(committed);
  wiringState.isWiring = false;
  wiringState.connSnapshotBefore = null;
});

await test('invalidation after response but before the delayed callback prevents plotting', async () => {
  const mode = RUN_MODES[0];
  const { resultId } = resetHarness(mode);
  const run = mode.run(resultId);
  await flushMicrotasks();
  respond(requests[0], scanResponse(1, 'new'));
  assert.equal(await run, true);
  assert.equal(plots.length, 0);

  invalidateScanExecution(resultId, 'input-changed-before-plot');
  runTimersThrough(50);
  assert.equal(plots.length, 0);
  assert.equal(nodeRegistry[resultId].data.scan1DResult, undefined);
});

await test('unannounced config or model drift rejects an otherwise successful response', async () => {
  for (const drift of ['config', 'model']) {
    const mode = RUN_MODES[0];
    const { resultId } = resetHarness(mode);
    const run = mode.run(resultId);
    await flushMicrotasks();
    if (drift === 'config') {
      setVariant(resultId, 1, 'B');
    } else {
      nodeRegistry.builder.data.modelContext.inputFingerprint = 'model-input-b';
    }

    respond(requests[0], scanResponse(1, 'old'));
    assert.equal(await run, false);
    assert.equal(nodeRegistry[resultId].data.scan1DResult, undefined);
    assert.match(elements.get(`${resultId}-content`).innerHTML, /Inputs changed/);
  }
});

await test('an obsolete error after unannounced dependency drift stays silent', async () => {
  for (const drift of ['config', 'model']) {
    const mode = RUN_MODES[0];
    const { resultId } = resetHarness(mode);
    const run = mode.run(resultId);
    await flushMicrotasks();
    if (drift === 'config') {
      setVariant(resultId, 1, 'B');
    } else {
      nodeRegistry.builder.data.modelContext.inputFingerprint = 'model-input-b';
    }

    respond(requests[0], { error: 'obsolete scan error' }, 500);
    assert.equal(await run, false);
    assert.equal(elements.get(`${resultId}-content`).innerHTML.includes('obsolete scan error'), false);
    assert.match(elements.get(`${resultId}-content`).innerHTML, /Inputs changed/);
  }
});

await test('the delayed plot rechecks dependencies even without an input event', async () => {
  const mode = RUN_MODES[0];
  const { resultId } = resetHarness(mode);
  const run = mode.run(resultId);
  await flushMicrotasks();
  respond(requests[0], scanResponse(1, 'new'));
  assert.equal(await run, true);

  setVariant(resultId, 1, 'B');
  runTimersThrough(50);
  assert.equal(plots.length, 0);
  assert.equal(nodeRegistry[resultId].data.scan1DResult, undefined);
});

await test('scan invalidation disconnects the old plot resize observer', async () => {
  const mode = RUN_MODES[0];
  const { resultId } = resetHarness(mode);
  let disconnected = false;
  plotResizeObservers.set(resultId, { disconnect: () => { disconnected = true; } });
  nodeRegistry[resultId].data.scan1DResult = scanResponse(1, 'old');

  invalidateScanExecution(resultId, 'input-changed');
  assert.equal(disconnected, true);
  assert.equal(plotResizeObservers.has(resultId), false);
});

await test('a deleted and recreated node with the same id rejects the old owner response', async () => {
  const mode = RUN_MODES[0];
  const { resultId } = resetHarness(mode);
  const oldRun = mode.run(resultId);
  await flushMicrotasks();
  const oldOwner = nodeRegistry[resultId];

  nodeRegistry[resultId] = { type: mode.type, el: elements.get(resultId), data: {} };
  assert.notEqual(nodeRegistry[resultId], oldOwner);
  setVariant(resultId, 1, 'B');
  const newRun = mode.run(resultId);
  await flushMicrotasks();
  respond(requests[1], scanResponse(1, 'new'));
  assert.equal(await newRun, true);

  respond(requests[0], scanResponse(1, 'old'));
  assert.equal(await oldRun, false);
  assert.equal(nodeRegistry[resultId].data.scan1DResult.marker, 'new');
});

await test('restored scan data is historical and never receives a fresh runtime ticket', async () => {
  const mode = RUN_MODES[1];
  const { resultId } = resetHarness(mode);
  restoreCachedNodeRuntime(resultId, mode.type, {
    scan1DResult: scanResponse(1, 'saved'),
    scan1DResultMeta: {
      contract: 'bne-scan-execution/v1',
      request: {
        param_symbol: 'q_A',
        session_id: 'stale-backend-session',
        nested: { sessionId: 'also-stale' },
      },
      model: { networkIrHash: 'saved-hash' },
      historical: false,
    },
  });

  assert.equal(nodeRegistry[resultId].data.scan1DResultMeta.historical, true);
  assert.equal(elements.get(`${resultId}-content`).dataset.resultState, 'historical');
  assert.match(elements.get(`${resultId}-content`).innerHTML, /Historical saved result/);
  assert.equal(nodeRegistry[resultId]._latestScanExecutionToken, undefined);
  assert.equal(nodeRegistry[resultId].data.scan1DResultMeta.request.session_id, undefined);
  assert.equal(nodeRegistry[resultId].data.scan1DResultMeta.request.nested.sessionId, undefined);
  const runtime = inspectScanExecution(resultId);
  assert.equal(runtime.state, 'historical');
  assert.equal(runtime.freshness, 'historical');
  assert.equal(runtime.owner, nodeRegistry[resultId]);
  assert.equal(runtime.workspaceEpoch, getWorkspaceRuntimeEpoch());
  assert.equal(runtime.endpoint, '/api/v1/parameter_scan_1d');
  assert.equal(runtime.sessionId, null);
});

await test('invalidating a restored historical result cancels its owner-guarded plot timer', async () => {
  const mode = RUN_MODES[1];
  const { resultId } = resetHarness(mode);
  restoreCachedNodeRuntime(resultId, mode.type, {
    scan1DResult: scanResponse(1, 'saved'),
  });
  assert.ok(nodeRegistry[resultId]._scanPlotTimer != null);

  invalidateScanExecution(resultId, 'config-edited-after-restore');
  runTimersThrough(50);
  assert.equal(plots.length, 0);
  assert.equal(nodeRegistry[resultId]._scanPlotTimer, undefined);
  assert.match(elements.get(`${resultId}-content`).innerHTML, /Inputs changed/);
});

console.log(`\nAll ${passed} scan run lifecycle contract tests passed.`);
