import assert from 'node:assert/strict';

class FakeClassList {
  constructor() { this.values = new Set(); }
  add(...values) { values.forEach(value => this.values.add(value)); }
  remove(...values) { values.forEach(value => this.values.delete(value)); }
  contains(value) { return this.values.has(value); }
  toggle(value, force) {
    const enabled = force == null ? !this.values.has(value) : Boolean(force);
    if (enabled) this.values.add(value); else this.values.delete(value);
    return enabled;
  }
}

const elements = new Map();
const requests = [];
const jobs = new Map();
const jobRequests = [];
const timers = new Map();
let timerId = 0;
const schedule = (callback, delay) => { timers.set(++timerId, { callback, delay }); return timerId; };
const cancel = id => timers.delete(id);

function fakeElement(overrides = {}) {
  const listeners = new Map();
  return {
    className: '', textContent: '', innerHTML: '', value: '', tagName: 'DIV', type: '',
    dataset: {}, style: {}, classList: new FakeClassList(),
    appendChild() {}, remove() {}, querySelectorAll() { return []; }, querySelector() { return null; },
    addEventListener(type, listener) {
      if (!listeners.has(type)) listeners.set(type, []);
      listeners.get(type).push(listener);
    },
    dispatchEvent(event) { for (const listener of listeners.get(event.type) || []) listener(event); return true; },
    ...overrides,
  };
}

globalThis.window = {
  matchMedia: () => null, addEventListener() {},
  location: { protocol: 'http:', hostname: '127.0.0.1', port: '8000' },
  sessionStorage: { getItem: () => null, setItem() {} },
  localStorage: { getItem: () => null, setItem() {} },
  crypto: { randomUUID: () => 'inverse-lifecycle-test' }, setTimeout: schedule, clearTimeout: cancel,
};
globalThis.document = {
  readyState: 'loading', documentElement: { dataset: {}, style: { setProperty() {} } },
  getElementById(id) { return elements.get(id) || null; },
  createElement() { return fakeElement(); }, addEventListener() {}, querySelectorAll() { return []; },
};
globalThis.requestAnimationFrame = callback => callback();
globalThis.alert = () => {};
globalThis.CustomEvent = class CustomEvent {};
globalThis.setTimeout = schedule;
globalThis.clearTimeout = cancel;
function jsonResponse(payload) {
  return { ok: true, status: 200, headers: { get: () => 'application/json' }, json: async () => payload };
}
// Submission is deliberately held even after invalidation: an accepted job's
// exact id must still be obtained and cancelled, with no stale publication.
globalThis.fetch = (url, options) => {
  if (url === '/api/v1/design_network') return new Promise(resolve => requests.push({ url, options, resolve }));
  const match = url.match(/^\/api\/v1\/jobs\/([^/]+)(?:\/(result|cancel))?$/);
  assert.ok(match, `Unexpected route ${url}`);
  const [, id, action] = match, job = jobs.get(id);
  assert.ok(job, `Unknown job ${id}`);
  jobRequests.push({ id, action, options });
  if (action === 'cancel') return Promise.resolve(jsonResponse({ job_id: id, status: 'cancelled' }));
  if (action === 'result') {
    const envelope = { job: { job_id: id, status: 'succeeded' }, result: job.payload };
    if (job.holdResult) return new Promise(resolve => { job.resolveResult = () => resolve(jsonResponse(envelope)); });
    return Promise.resolve(jsonResponse(envelope));
  }
  return Promise.resolve(jsonResponse({ job_id: id, status: 'succeeded' }));
};

await import('../public/js/node-types/index.js');
const {
  INVERSE_DESIGN_TYPES, applyInverseDesignPreset, executeGradientDesign, executeDesignedNetwork,
  cancelGradientDesign,
  handleInverseDesignConnectionsChanged, readCurrentGradientResult, readCurrentDesignedNetwork,
  restoreInverseDesignResultView, restoreDesignedNetworkView, restoreInverseDesignTargetView,
} = await import('../public/js/node-types/inverse-design.js');
const { INVERSE_DESIGN_DEFAULTS } = await import('../public/js/inverse-design-core.js');
const { DEFAULT_TARGET } = await import('../public/js/design-target-adapters.js');
const { advanceWorkspaceRuntimeEpoch, nodeRegistry, setConnections } = await import('../public/js/state.js');
const { getReactionsFromNode } = await import('../public/js/model.js');
const { setNodeAttr } = await import('../public/js/nodes.js');
const { registerPerformers, undoStack, undo, redo } = await import('../public/js/commands.js');
registerPerformers({ setAttr: setNodeAttr });

const DEFAULT_GRAPH = [
  { fromNode: 'target', fromPort: 'inverse-design-request', toNode: 'gradient', toPort: 'inverse-design-request' },
  { fromNode: 'gradient', fromPort: 'inverse-design-result', toNode: 'network', toPort: 'inverse-design-result' },
];

function createNode(id, type, controls = {}) {
  const children = Object.entries(controls).map(([suffix, options]) => {
    const control = fakeElement({ id: `${id}-${suffix}`, value: String(options.value), type: options.type || 'number', tagName: options.tagName || 'INPUT' });
    elements.set(`${id}-${suffix}`, control);
    return control;
  });
  elements.set(id, fakeElement({ querySelectorAll: selector => selector === '.auto-update' ? children : [] }));
  elements.set(`${id}-content`, fakeElement());
  nodeRegistry[id] = { type, data: {} };
  INVERSE_DESIGN_TYPES[type].onInit(id);
}

function createGradient(id = 'gradient') {
  const defaults = INVERSE_DESIGN_DEFAULTS.optimization;
  createNode(id, 'gradient-design', {
    'learning-rate': { value: defaults.learning_rate }, epochs: { value: defaults.epochs },
    restarts: { value: defaults.restarts }, 'prune-rounds': { value: defaults.prune_rounds },
    'prune-fraction': { value: defaults.prune_fraction }, 'prune-tolerance': { value: defaults.prune_tolerance },
    'max-rmse': { value: defaults.max_rmse }, seed: { value: defaults.seed },
    'optimize-totals': { value: true, tagName: 'SELECT', type: 'select-one' },
  });
}

function resetHarness() {
  Object.keys(nodeRegistry).forEach(key => delete nodeRegistry[key]);
  elements.clear(); requests.length = 0; timers.clear(); jobs.clear(); jobRequests.length = 0;
  undoStack.clear();
  setConnections(structuredClone(DEFAULT_GRAPH));
  elements.set('status-badge', fakeElement());
  elements.set('toast-container', fakeElement());
  createNode('target', 'inverse-design-target', {
    description: { value: DEFAULT_TARGET.description, tagName: 'TEXTAREA', type: 'textarea' },
    'target-mode': { value: 'curve', tagName: 'SELECT', type: 'select-one' },
    'target-json': { value: JSON.stringify(DEFAULT_TARGET), tagName: 'TEXTAREA', type: 'textarea' },
    'reference-state': { value: '', type: 'hidden' }, 'drawing-json': { value: '', type: 'hidden' },
    'target-points': { value: 24 }, 'image-resolution': { value: 12 },
    'image-invert': { value: false, tagName: 'SELECT', type: 'select-one' },
    'aux-monomers': { value: 2 }, 'max-complex-size': { value: 3 }, 'max-reactions': { value: 48 },
    'allow-homomers': { value: true, tagName: 'SELECT', type: 'select-one' },
    'chemistry-json': { value: '{}', tagName: 'TEXTAREA', type: 'textarea' },
  });
  createGradient();
  createNode('network', 'designed-network');
}

function response(pending, marker) {
  const input = JSON.parse(pending.options.body);
  const rules = ['X + A <-> AX'], kd = [0.48];
  const targets = input.target.samples.map(sample => sample.outputs.slice());
  return {
    marker, status: 'ok', target: input.target, target_met: true,
    initial_reaction_count: 3, final_reaction_count: 1,
    pruning_history: [{ before: 3, after: 1, accepted: true, rmse: 0, reason: 'Refit accepted.' }],
    optimization_history: [{ phase: 'initial', step: 0, loss: 0.1 }, { phase: 'refit', step: 1, loss: 0 }],
    algorithm: { name: 'implicit_equilibrium_gradient_and_occupancy_pruning' }, warnings: [],
    selected_network: {
      status: 'ok', rules: rules.slice(), kd: kd.slice(), totals: { A: 1.1 }, outputs: input.target.outputs,
      network_ir: { ir_schema_version: 'bne-ir/v1.0.0', reactions: [{ formula: rules[0], kd: kd[0] }] },
      network_ir_hash: 'lifecycle-selected-network', predictions: structuredClone(targets), targets: structuredClone(targets),
      fit_loss: 0, rmse: 0, per_output_rmse: input.target.outputs.map(() => 0),
      physical_audit: { cold_replay: true, max_log10_mass_residual: 1e-12,
        max_stepwise_log10_mass_action_residual: 1e-12, training_samples: input.target.samples.length,
        validation_samples: input.target.validation_samples?.length ?? 0 },
      prediction_basis: 'selected_network', evidence_tier: 'sampled_equilibrium_replay',
    },
  };
}

function respond(pending, marker, { status = 'succeeded', holdResult = false } = {}) {
  const payload = response(pending, marker);
  const id = `lifecycle-job-${requests.indexOf(pending)}`;
  jobs.set(id, { payload, holdResult });
  pending.resolve(jsonResponse({ job_id: id, status }));
  return jobs.get(id);
}

async function waitForRequests(count) {
  for (let attempt = 0; attempt < 30 && requests.length < count; attempt += 1) await Promise.resolve();
  assert.equal(requests.length, count);
}

async function completeFit(id = 'gradient', marker = 'current') {
  const index = requests.length;
  const run = executeGradientDesign(id);
  await waitForRequests(index + 1);
  respond(requests[index], marker);
  assert.equal((await run).status, 'succeeded');
}

let passed = 0;
async function test(name, callback) {
  resetHarness();
  await callback();
  passed += 1;
  console.log(`  ok - ${name}`);
}

await test('numerical target input immediately retires fit and network before config debounce', async () => {
  await completeFit();
  assert.equal(executeDesignedNetwork('network').status, 'succeeded');
  assert.deepEqual(readCurrentDesignedNetwork('network').reactions, ['X + A <-> AX']);
  assert.deepEqual(readCurrentDesignedNetwork('network').totals, { A: 1.1 });
  const control = elements.get('target-target-points');
  const saved = nodeRegistry.target.data.config.inverseTargetPoints;
  control.value = '25';
  control.dispatchEvent(new Event('input'));
  assert.equal(nodeRegistry.target.data.config.inverseTargetPoints, saved, 'serialized input has not passed the debounce');
  assert.equal(timers.get(control._autoUpdateTimer)?.delay, 500);
  assert.equal(nodeRegistry.target.data.lifecycle.state, 'invalidated');
  assert.equal(nodeRegistry.gradient.data.lifecycle.state, 'invalidated');
  assert.equal(nodeRegistry.network.data.lifecycle.state, 'invalidated');
  assert.equal(nodeRegistry.gradient.data.inverseDesignResult, undefined);
  assert.equal(nodeRegistry.network.data.designedNetwork, undefined);
  assert.equal(readCurrentGradientResult('gradient'), null);
  assert.equal(readCurrentDesignedNetwork('network'), null);
  assert.deepEqual(getReactionsFromNode('network'), { reactions: [], kds: [] });
});

await test('latest fit wins and obsolete response/finally cannot clear newer loading', async () => {
  const first = executeGradientDesign('gradient');
  await waitForRequests(1);
  const firstOwner = nodeRegistry.gradient._inverseDesignAbort;
  elements.get('gradient-prune-tolerance').value = '0.03';
  const second = executeGradientDesign('gradient');
  await waitForRequests(2);
  assert.equal(requests[0].url, '/api/v1/design_network');
  assert.equal(firstOwner.signal.aborted, true);
  assert.equal(elements.get('gradient').classList.contains('loading'), true);
  respond(requests[0], 'old', { status: 'queued' });
  assert.equal((await first).status, 'stale');
  assert.equal(nodeRegistry.gradient.data.inverseDesignResult, undefined);
  assert.equal(elements.get('gradient').classList.contains('loading'), true);
  assert.equal(jobRequests.filter(request => request.id === 'lifecycle-job-0' && request.action === 'cancel').length, 1,
    'a stale accepted job must be cancelled by the exact returned id');
  respond(requests[1], 'new');
  assert.equal((await second).status, 'succeeded');
  assert.equal(nodeRegistry.gradient.data.inverseDesignResult.marker, 'new');
  assert.equal(elements.get('gradient').classList.contains('loading'), false);
  assert.equal(readCurrentGradientResult('gradient').result.marker, 'new');
});

await test('saved evidence stays historical until a fresh run and network extraction', async () => {
  await completeFit();
  assert.equal(executeDesignedNetwork('network').status, 'succeeded');
  const gradientSaved = structuredClone(nodeRegistry.gradient.data);
  const networkSaved = structuredClone(nodeRegistry.network.data);
  nodeRegistry.gradient = { type: 'gradient-design', data: gradientSaved };
  nodeRegistry.network = { type: 'designed-network', data: networkSaved };
  assert.equal(restoreInverseDesignResultView('gradient'), true);
  assert.equal(restoreDesignedNetworkView('network'), true);
  assert.match(elements.get('network-content').innerHTML, /Historical result/);
  assert.equal(nodeRegistry.gradient.data.lifecycle.freshness, 'historical');
  assert.equal(nodeRegistry.network.data.lifecycle.freshness, 'historical');
  assert.equal(readCurrentGradientResult('gradient'), null);
  assert.equal(readCurrentDesignedNetwork('network'), null);
  assert.deepEqual(getReactionsFromNode('network'), { reactions: [], kds: [] });
  assert.equal(executeDesignedNetwork('network').status, 'blocked');
  await completeFit('gradient', 'refreshed');
  assert.equal(executeDesignedNetwork('network').status, 'succeeded');
  const network = readCurrentDesignedNetwork('network');
  assert.deepEqual(network.reactions, ['X + A <-> AX']);
  assert.deepEqual(network.kds, [0.48]);
  assert.deepEqual(network.outputs, gradientSaved.inverseDesignRequest.target.outputs);
  network.kds[0] = 99;
  network.totals.A = 99;
  network.outputs[0].offset = 99;
  assert.equal(readCurrentDesignedNetwork('network').kds[0], 0.48, 'downstream callers receive a copy');
  assert.equal(getReactionsFromNode('network').totals.A, 1.1);
  assert.equal(getReactionsFromNode('network').outputs[0].offset, 0);
});

await test('disconnect immediately denies reuse and graph notification removes stored outputs', async () => {
  await completeFit();
  assert.equal(executeDesignedNetwork('network').status, 'succeeded');
  const before = structuredClone(DEFAULT_GRAPH), after = [structuredClone(DEFAULT_GRAPH[1])];
  setConnections(after);
  assert.equal(readCurrentGradientResult('gradient'), null, 'fingerprint sees the disconnected target without a UI event');
  assert.equal(readCurrentDesignedNetwork('network'), null);
  handleInverseDesignConnectionsChanged(before, after);
  assert.equal(nodeRegistry.gradient.data.lifecycle.state, 'invalidated');
  assert.equal(nodeRegistry.network.data.lifecycle.state, 'invalidated');
  assert.equal(nodeRegistry.network.data.designedNetwork, undefined);
  assert.equal(executeDesignedNetwork('network').status, 'blocked');
  assert.equal((await executeGradientDesign('gradient')).status, 'blocked');
  assert.equal(requests.length, 1, 'disconnected gradient must not send HTTP');
});

await test('workspace epoch changes deny current artifacts and discard delayed responses', async () => {
  await completeFit();
  assert.equal(executeDesignedNetwork('network').status, 'succeeded');
  advanceWorkspaceRuntimeEpoch();
  assert.equal(readCurrentGradientResult('gradient'), null);
  assert.equal(readCurrentDesignedNetwork('network'), null);
  const delayed = executeGradientDesign('gradient');
  await waitForRequests(2);
  advanceWorkspaceRuntimeEpoch();
  respond(requests[1], 'previous-workspace');
  assert.equal((await delayed).status, 'stale');
  assert.equal(nodeRegistry.gradient.data.inverseDesignResult, undefined);
  assert.equal(readCurrentGradientResult('gradient'), null);
  assert.equal(nodeRegistry.gradient.data.lifecycle.state, 'invalidated');
  assert.equal(elements.get('gradient').classList.contains('loading'), false);
});

await test('programmatic control drift retires a stale response even without an input event', async () => {
  const run = executeGradientDesign('gradient');
  await waitForRequests(1);
  elements.get('gradient-learning-rate').value = '0.04';
  respond(requests[0], 'previous-settings');
  assert.equal((await run).status, 'stale');
  assert.equal(nodeRegistry.gradient.data.lifecycle.state, 'invalidated');
  assert.equal(nodeRegistry.gradient.data.inverseDesignResult, undefined);
  assert.equal(readCurrentGradientResult('gradient'), null);
  assert.equal(elements.get('gradient').classList.contains('loading'), false);
  assert.doesNotMatch(elements.get('gradient-content').innerHTML, /Constructing a legal network/);
});

await test('invalid optimizer input blocks before HTTP and retires earlier output', async () => {
  await completeFit();
  assert.equal(executeDesignedNetwork('network').status, 'succeeded');
  for (const invalid of ['', '250garbage', 'true', 'Infinity', '2001']) {
    elements.get('gradient-epochs').value = invalid;
    const outcome = await executeGradientDesign('gradient');
    assert.equal(outcome.status, 'blocked');
    assert.equal(requests.length, 1);
    assert.equal(nodeRegistry.gradient.data.lifecycle.state, 'blocked');
    assert.equal(nodeRegistry.gradient.data.inverseDesignResult, undefined);
    assert.equal(readCurrentDesignedNetwork('network'), null);
    assert.equal(elements.get('gradient').classList.contains('loading'), false);
  }
});

await test('one target can feed two gradient branches without retiring the first branch', async () => {
  createGradient('gradient-b');
  setConnections([...structuredClone(DEFAULT_GRAPH), {
    fromNode: 'target', fromPort: 'inverse-design-request', toNode: 'gradient-b', toPort: 'inverse-design-request',
  }]);
  await completeFit('gradient', 'branch-a');
  assert.equal(executeDesignedNetwork('network').status, 'succeeded');
  const branchA = readCurrentGradientResult('gradient');
  const second = executeGradientDesign('gradient-b');
  await waitForRequests(2);
  assert.equal(readCurrentGradientResult('gradient'), branchA);
  assert.equal(readCurrentDesignedNetwork('network').kds[0], 0.48);
  respond(requests[1], 'branch-b');
  assert.equal((await second).status, 'succeeded');
  assert.equal(readCurrentGradientResult('gradient').result.marker, 'branch-a');
  assert.equal(readCurrentGradientResult('gradient-b').result.marker, 'branch-b');
  assert.equal(readCurrentDesignedNetwork('network').kds[0], 0.48);
});

await test('generating a target example is one undoable numerical target edit', async () => {
  await completeFit();
  assert.equal(executeDesignedNetwork('network').status, 'succeeded');
  const priorTarget = structuredClone(DEFAULT_TARGET);
  priorTarget.samples[0].outputs[0] = -1;
  const prior = JSON.stringify(priorTarget);
  elements.get('target-target-json').value = prior;
  assert.equal(applyInverseDesignPreset('target'), true);
  assert.equal(undoStack.depth, 1);
  const generated = elements.get('target-target-json').value;
  assert.deepEqual(JSON.parse(generated), DEFAULT_TARGET);
  assert.equal(readCurrentDesignedNetwork('network'), null);
  undo();
  assert.equal(undoStack.depth, 0);
  assert.equal(elements.get('target-target-json').value, prior);
  assert.equal(readCurrentDesignedNetwork('network'), null, 'undoing target generation does not revive old fit evidence');
  redo();
  assert.equal(undoStack.depth, 1);
  assert.equal(elements.get('target-target-json').value, generated);
  assert.equal(nodeRegistry.target.data.config.inverseTargetJSON, generated);
});

await test('a completed job result arriving in a later workspace cannot publish evidence', async () => {
  const running = executeGradientDesign('gradient');
  await waitForRequests(1);
  const job = respond(requests[0], 'late-result', { holdResult: true });
  for (let i = 0; i < 60 && !job.resolveResult; i++) await Promise.resolve();
  assert.equal(typeof job.resolveResult, 'function');
  advanceWorkspaceRuntimeEpoch();
  job.resolveResult();
  assert.equal((await running).status, 'stale');
  assert.equal(readCurrentGradientResult('gradient'), null);
  assert.equal(nodeRegistry.gradient.data.inverseDesignResult, undefined);
  assert.equal(jobRequests.some(request => request.action === 'cancel'), false,
    'a terminal job needs no cancellation');
});

await test('a queued local job is polled before publishing its own completed result', async () => {
  const running = executeGradientDesign('gradient');
  await waitForRequests(1);
  respond(requests[0], 'polled-job', { status: 'queued' });
  let pollTimer;
  for (let i = 0; i < 60 && !pollTimer; i++) {
    await Promise.resolve();
    pollTimer = [...timers.entries()].find(([, timer]) => timer.delay === 350);
  }
  assert.ok(pollTimer, 'queued local jobs schedule a polling wait');
  assert.equal(readCurrentGradientResult('gradient'), null);
  timers.delete(pollTimer[0]); pollTimer[1].callback();
  assert.equal((await running).status, 'succeeded');
  assert.deepEqual(jobRequests.map(request => request.action), [undefined, 'result']);
  assert.equal(readCurrentGradientResult('gradient').result.marker, 'polled-job');
});

await test('Stop cancels a late accepted job once without clearing a newer run or reviving its network', async () => {
  await completeFit();
  assert.equal(executeDesignedNetwork('network').status, 'succeeded');
  const stopped = executeGradientDesign('gradient');
  await waitForRequests(2);
  assert.equal(cancelGradientDesign('gradient'), true);
  assert.equal(cancelGradientDesign('gradient'), false, 'Stop is idempotent after retiring its controller');
  assert.equal(readCurrentDesignedNetwork('network'), null);
  assert.match(elements.get('gradient-content').innerHTML, /Design stopped/);
  const newer = executeGradientDesign('gradient');
  await waitForRequests(3);
  respond(requests[1], 'stopped-job', { status: 'queued' });
  assert.equal((await stopped).status, 'cancelled');
  assert.equal(jobRequests.filter(request => request.id === 'lifecycle-job-1' && request.action === 'cancel').length, 1);
  assert.equal(jobRequests.some(request => request.id === 'lifecycle-job-1' && request.action === 'result'), false);
  assert.equal(elements.get('gradient').classList.contains('loading'), true);
  assert.equal(readCurrentGradientResult('gradient'), null);
  assert.equal(readCurrentDesignedNetwork('network'), null);
  respond(requests[2], 'newer-run');
  assert.equal((await newer).status, 'succeeded');
  assert.equal(readCurrentGradientResult('gradient').result.marker, 'newer-run');
  assert.equal(cancelGradientDesign('gradient'), false, 'completed evidence is not an active job');
});

await test('Stop interrupts a known queued job during polling without fetching its result', async () => {
  const running = executeGradientDesign('gradient');
  await waitForRequests(1);
  respond(requests[0], 'queued-stop', { status: 'queued' });
  for (let i = 0; i < 60 && ![...timers.values()].some(timer => timer.delay === 350); i++) await Promise.resolve();
  assert.ok([...timers.values()].some(timer => timer.delay === 350));
  assert.equal(cancelGradientDesign('gradient'), true);
  assert.equal((await running).status, 'cancelled');
  assert.deepEqual(jobRequests.map(request => [request.id, request.action]), [['lifecycle-job-0', 'cancel']]);
  assert.equal(readCurrentGradientResult('gradient'), null);
  assert.equal(nodeRegistry.gradient.data.inverseDesignResult, undefined);
  assert.equal(elements.get('gradient').classList.contains('loading'), false);
  assert.match(elements.get('gradient-content').innerHTML, /Design stopped/);
});

await test('restored candidate-library targets cannot silently run the new default target', async () => {
  const legacy = { inverseDesignRequest: {
    reactions: ['A + B <-> AB'], output_exprs: ['AB'],
    samples: [{ totals: { tA: 1, tB: 1 }, target: [0.5] }],
  } };
  assert.equal(restoreInverseDesignTargetView('target', legacy), true);
  assert.match(elements.get('target-content').innerHTML, /Earlier candidate-library workflow/);
  assert.equal((await executeGradientDesign('gradient')).status, 'blocked');
  assert.equal(requests.length, 0);
  assert.equal(readCurrentGradientResult('gradient'), null);
  // An explicit edit is a deliberate new target, not an automatic migration.
  elements.get('target-target-json').dispatchEvent(new Event('input'));
  await completeFit('gradient', 'explicitly-reviewed');
  assert.equal(readCurrentGradientResult('gradient').result.marker, 'explicitly-reviewed');
});

await test('running during pattern decode or Agent compilation does not cancel authoring and reuse an older numerical target', async () => {
  for (const key of ['_designReferenceLoading', '_designTargetAbort']) {
    const pending = new AbortController();
    nodeRegistry.target[key] = pending;
    const result = await executeGradientDesign('gradient');
    assert.equal(result.status, 'blocked');
    assert.equal(requests.length, 0);
    assert.equal(nodeRegistry.target[key], pending, 'attempting a run must not erase the pending authoring guard');
    assert.equal(pending.signal.aborted, false);
    delete nodeRegistry.target[key];
  }
  const reference = elements.get('target-reference-state');
  reference.value = JSON.stringify({ id: 'local-pattern', name: 'pattern.png', pending: true });
  reference.dispatchEvent(new Event('input'));
  assert.equal((await executeGradientDesign('gradient')).status, 'blocked');
  assert.equal(requests.length, 0);
  assert.match(elements.get('gradient-content').innerHTML, /Trace the uploaded pattern/);
});

console.log(`\nAll ${passed} inverse design lifecycle integration tests passed.`);
