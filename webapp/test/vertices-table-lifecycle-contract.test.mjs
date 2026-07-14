import assert from 'node:assert/strict';

const elements = new Map();
const requests = [];

class FakeClassList {
  constructor() { this.values = new Set(); }
  add(...values) { values.forEach(value => this.values.add(value)); }
  remove(...values) { values.forEach(value => this.values.delete(value)); }
  contains(value) { return this.values.has(value); }
}

class FakeElement {
  constructor() {
    this.classList = new FakeClassList();
    this.className = '';
    this.dataset = {};
    this.innerHTML = '';
    this.textContent = '';
    this.style = {};
  }
  addEventListener() {}
  querySelector() { return null; }
  querySelectorAll() { return []; }
  replaceChildren(child) {
    this.innerHTML = `<div class="${child?.className || ''}">${child?.textContent || ''}</div>`;
  }
}

globalThis.window = {
  matchMedia: () => null,
  addEventListener() {},
  dispatchEvent() {},
  sessionStorage: { getItem: () => null, setItem() {} },
  localStorage: { getItem: () => null, setItem() {} },
  crypto: { randomUUID: () => 'vertices-lifecycle-test' },
};
globalThis.document = {
  readyState: 'loading',
  documentElement: { style: { setProperty() {} } },
  getElementById(id) { return elements.get(id) || null; },
  createElement() { return new FakeElement(); },
  addEventListener() {},
  querySelector() { return null; },
  querySelectorAll() { return []; },
};
globalThis.getComputedStyle = () => ({ getPropertyValue: () => '' });
globalThis.ResizeObserver = class ResizeObserver { observe() {} disconnect() {} };
globalThis.requestAnimationFrame = callback => callback();
globalThis.cancelAnimationFrame = () => {};
globalThis.CustomEvent = class CustomEvent {};
globalThis.Plotly = { newPlot() {}, Plots: { resize() {} } };
globalThis.fetch = (url, options) => new Promise(resolve => {
  requests.push({ url, options, resolve });
});

const {
  advanceWorkspaceRuntimeEpoch,
  getWorkspaceRuntimeEpoch,
  nodeRegistry,
  setConnections,
} = await import('../public/js/state.js');
// Load the normal node registry root first so this focused test follows the
// production ESM cycle (nodes -> node-types/index -> result).
await import('../public/js/nodes.js');
const resultModule = await import('../public/js/node-types/result.js');
const {
  executeVerticesTable,
  inspectVerticesTableExecution,
  readCurrentVerticesTableResult,
} = resultModule;

let passed = 0;
async function test(name, callback) {
  await callback();
  passed += 1;
  console.log(`  ok - ${name}`);
}

function addElement(id) {
  const element = new FakeElement();
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
      model: { q_sym: ['q_A'], K_sym: [], x_sym: ['A'] },
    },
  };
  nodeRegistry.builder._modelInputRevision = 0;
  addNode('result', 'vertices-table');
  setConnections([
    { fromNode: 'builder', fromPort: 'model', toNode: 'result', toPort: 'model' },
  ]);
}

function verticesResponse(label, idx) {
  return {
    marker: label,
    vertices: [{
      idx,
      perm: [idx],
      species: [label],
      asymptotic: true,
      singular: false,
      nullity: 0,
    }],
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
  for (let attempt = 0; attempt < 40 && requests.length < count; attempt += 1) {
    await Promise.resolve();
  }
  assert.equal(requests.length, count);
}

for (const oldOutcome of ['success', 'failure']) {
  await test(`Vertices Table is latest-wins when the old request finishes with ${oldOutcome}`, async () => {
    resetHarness();
    const oldRun = executeVerticesTable('result');
    await waitForRequests(1);
    const newRun = executeVerticesTable('result');
    await waitForRequests(2);
    assert.equal(requests[0].url, '/api/v1/find_vertices');
    assert.equal(requests[1].url, '/api/v1/find_vertices');
    assert.equal(elements.get('result').classList.contains('loading'), true);

    respond(requests[1], verticesResponse('New', 2));
    assert.equal(await newRun, true);
    assert.match(elements.get('result-content').innerHTML, /New/);
    const runtime = inspectVerticesTableExecution('result');
    assert.equal(runtime.state, 'current');
    assert.equal(runtime.freshness, 'current');
    assert.equal(runtime.owner, nodeRegistry.result);
    assert.equal(runtime.workspaceEpoch, getWorkspaceRuntimeEpoch());
    assert.equal(runtime.endpoint, '/api/v1/find_vertices');
    assert.match(runtime.inputFingerprint, /network-hash/);
    assert.equal(readCurrentVerticesTableResult('result').marker, 'New');

    if (oldOutcome === 'success') {
      respond(requests[0], verticesResponse('Old', 1));
    } else {
      respond(requests[0], { error: 'obsolete vertices failed' }, 500);
    }
    assert.equal(await oldRun, false);
    assert.match(elements.get('result-content').innerHTML, /New/);
    assert.doesNotMatch(elements.get('result-content').innerHTML, /Old|obsolete vertices failed/);
    assert.equal(elements.get('result').classList.contains('loading'), false);
  });
}

await test('an obsolete Vertices finally cannot clear the newer loading owner', async () => {
  resetHarness();
  const oldRun = executeVerticesTable('result');
  await waitForRequests(1);
  const newRun = executeVerticesTable('result');
  await waitForRequests(2);

  respond(requests[0], verticesResponse('Old first', 1));
  assert.equal(await oldRun, false);
  assert.equal(elements.get('result').classList.contains('loading'), true);
  assert.doesNotMatch(elements.get('result-content').innerHTML, /Old first/);

  respond(requests[1], verticesResponse('New last', 2));
  assert.equal(await newRun, true);
  assert.equal(elements.get('result').classList.contains('loading'), false);
  assert.match(elements.get('result-content').innerHTML, /New last/);
});

await test('model input revision drift during preflight prevents the Vertices request', async () => {
  resetHarness();
  const run = executeVerticesTable('result');
  nodeRegistry.builder._modelInputRevision = 1;
  assert.equal(await run, false);
  assert.equal(requests.length, 0);
  assert.equal(inspectVerticesTableExecution('result').state, 'invalidated');
  assert.equal(elements.get('result').classList.contains('loading'), false);
});

await test('the current Vertices failure enters failed state and renders its error', async () => {
  resetHarness();
  const run = executeVerticesTable('result');
  await waitForRequests(1);
  respond(requests[0], { error: 'current vertices failed' }, 500);
  assert.equal(await run, false);
  assert.equal(inspectVerticesTableExecution('result').state, 'failed');
  assert.equal(readCurrentVerticesTableResult('result'), null);
  assert.match(elements.get('result-content').innerHTML, /current vertices failed/);
  assert.equal(elements.get('result').classList.contains('loading'), false);
});

await test('owner replacement, workspace epoch drift, and model drift reject Vertices publication', async () => {
  resetHarness();
  let run = executeVerticesTable('result');
  await waitForRequests(1);
  const oldOwner = nodeRegistry.result;
  addNode('result', 'vertices-table');
  respond(requests[0], verticesResponse('Old owner', 1));
  assert.equal(await run, false);
  assert.notEqual(nodeRegistry.result, oldOwner);
  assert.doesNotMatch(elements.get('result-content').innerHTML, /Old owner/);

  run = executeVerticesTable('result');
  await waitForRequests(2);
  advanceWorkspaceRuntimeEpoch();
  respond(requests[1], verticesResponse('Old epoch', 2));
  assert.equal(await run, false);
  assert.doesNotMatch(elements.get('result-content').innerHTML, /Old epoch/);
  assert.equal(inspectVerticesTableExecution('result').state, 'invalidated');

  run = executeVerticesTable('result');
  await waitForRequests(3);
  nodeRegistry.builder.data.modelContext.inputFingerprint = 'model-input-b';
  respond(requests[2], verticesResponse('Old model', 3));
  assert.equal(await run, false);
  assert.doesNotMatch(elements.get('result-content').innerHTML, /Old model/);
  assert.equal(inspectVerticesTableExecution('result').state, 'invalidated');
});

console.log(`\nAll ${passed} Vertices Table lifecycle contract tests passed.`);
