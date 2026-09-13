import assert from 'node:assert/strict';

const elements = new Map(), requests = [];
class Element {
  constructor() {
    this.style = {}; this.dataset = {}; this.value = ''; this.textContent = '';
    this.children = []; this.listeners = new Map(); this.className = ''; this.tagName = 'DIV';
    this.classList = { add() {}, remove() {}, toggle() {}, contains: () => false };
  }
  set id(value) { this._id = value; elements.set(value, this); }
  get id() { return this._id; }
  append(...children) { children.forEach(child => this.appendChild(child)); }
  appendChild(child) { this.children.push(child); child.parentNode = this; }
  replaceChildren() { this.children = []; }
  querySelectorAll() { return []; }
  querySelector() { return null; }
  remove() {}
  addEventListener(type, callback) {
    const callbacks = this.listeners.get(type) || [];
    this.listeners.set(type, [...callbacks, callback]);
  }
  dispatchEvent(event) { for (const callback of this.listeners.get(event.type) || []) callback(event); }
}
const element = (id, value = '') => {
  const el = new Element(); el.id = id; el.value = String(value); return el;
};
globalThis.setTimeout = () => 0;
globalThis.clearTimeout = () => {};
globalThis.requestAnimationFrame = callback => callback();
globalThis.CustomEvent = class CustomEvent {};
globalThis.alert = () => {};
globalThis.Plotly = { newPlot() {}, react() {}, purge() {}, Plots: { resize() {} } };
globalThis.ResizeObserver = class ResizeObserver { observe() {} disconnect() {} };
globalThis.window = {
  matchMedia: () => null, addEventListener() {}, dispatchEvent() {},
  crypto: { randomUUID: () => 'design-parameters-test' },
  location: { hostname: '127.0.0.1', protocol: 'http:', port: '8000' },
  sessionStorage: { getItem: () => null, setItem() {} },
  localStorage: { getItem: () => null, setItem() {} },
  setTimeout: globalThis.setTimeout, clearTimeout: globalThis.clearTimeout,
};
globalThis.document = {
  readyState: 'loading', addEventListener() {}, getElementById: id => elements.get(id) || null,
  querySelector: () => null, querySelectorAll: () => [], createElement: () => new Element(),
  documentElement: { dataset: {}, style: { setProperty() {} } },
};
globalThis.fetch = (url, options) => new Promise(resolve => requests.push({ url, options, resolve }));

const stateModule = await import('../public/js/state.js');
const { nodeRegistry, setConnections } = stateModule;
await import('../public/js/node-types/index.js');
const { executeGradientDesign, executeDesignedNetwork } = await import('../public/js/node-types/inverse-design.js');
const { buildModel, getReactionsFromNode } = await import('../public/js/model.js');
const { prepareFixedParameterControls } = await import('../public/js/node-types/scan.js');
const { modelParameterDefaults, scanConfigWithModelParameters } = await import('../public/js/model-parameters.js');
const { getNodeSerialData, serializeState, restoreCachedNodeRuntime } = await import('../public/js/workspace.js');
const { getModelContextFromBuilder } = await import('../public/js/nodes.js');
const { executeScan1DResult } = await import('../public/js/scan.js');

const target = {
  schema_version: 'bne-design-target/v1.0.0', description: 'Fit a response', source: 'data',
  inputs: [{ name: 'X', min: 0.1, max: 10, scale: 'log' }],
  outputs: [{ name: 'response', species: 'A', transform: 'log10', offset: -0.3, optimize_offset: false }],
  samples: [{ inputs: [0.1], outputs: [0.2], weight: 1 }, { inputs: [10], outputs: [0.4], weight: 1 }],
};
function node(id, type, fields = {}) {
  nodeRegistry[id] = { type, data: {} }; element(id); element(`${id}-content`);
  Object.entries(fields).forEach(([suffix, value]) => element(`${id}-${suffix}`, value));
}
function setup() {
  Object.keys(nodeRegistry).forEach(key => delete nodeRegistry[key]); elements.clear(); requests.length = 0;
  element('toast-container'); element('status-badge');
  node('target', 'inverse-design-target', {
    description: target.description, 'target-mode': 'data', 'target-json': JSON.stringify(target),
    'reference-state': '', 'drawing-json': '',
    'target-points': 2, 'image-resolution': 12, 'image-invert': false,
    'aux-monomers': 1, 'max-complex-size': 2, 'max-reactions': 8, 'allow-homomers': true,
    'chemistry-json': '{}',
  });
  node('gradient', 'gradient-design', {
    epochs: 10, 'learning-rate': 0.03, restarts: 1, 'prune-rounds': 1, 'prune-fraction': 0.2,
    'prune-tolerance': 0.02, 'max-rmse': 0.05, 'optimize-totals': true, seed: 1,
  });
  node('network', 'designed-network'); node('builder', 'model-builder');
  node('params', 'scan-1d-params', { param: 'tX', min: -1, max: 1, points: 10, expr: 'A' });
  element('params-fixed-parameters'); element('params-design-readout');
  setConnections([
    { fromNode: 'target', fromPort: 'inverse-design-request', toNode: 'gradient', toPort: 'inverse-design-request' },
    { fromNode: 'gradient', fromPort: 'inverse-design-result', toNode: 'network', toPort: 'inverse-design-result' },
    { fromNode: 'network', fromPort: 'reactions', toNode: 'builder', toPort: 'reactions' },
    { fromNode: 'builder', fromPort: 'model', toNode: 'params', toPort: 'model' },
  ]);
}
async function nextRequest(index) {
  for (let i = 0; i < 100 && !requests[index]; i++) await Promise.resolve();
  assert.ok(requests[index], `Expected request ${index}`); return requests[index];
}
function respond(request, data) {
  request.resolve({ ok: true, status: 200, headers: { get: () => 'application/json' }, json: async () => data });
}
async function publishDesign() {
  const index = requests.length;
  const running = executeGradientDesign('gradient');
  const pending = await nextRequest(index);
  const request = JSON.parse(pending.options.body);
  const selected = {
    status: 'ok', rules: ['X + A <-> AX'], kd: [0.02], totals: { A: 10 }, outputs: request.target.outputs,
    predictions: [[0.2], [0.4]], targets: [[0.2], [0.4]], rmse: 0, fit_loss: 0,
    per_output_rmse: [0], physical_audit: {
      cold_replay: true, max_log10_mass_residual: 1e-12, max_stepwise_log10_mass_action_residual: 1e-12,
      training_samples: 2, validation_samples: 0,
    },
    evidence_tier: 'sampled_equilibrium_replay', prediction_basis: 'selected_network',
    network_ir: { ir_schema_version: 'bne-ir/v1.0.0', reactions: [{ formula: 'X + A <-> AX', kd: 0.02 }] },
  };
  respond(pending, {
    status: 'ok', target: request.target, target_met: true, selected_network: selected,
    initial_reaction_count: 3, final_reaction_count: 1, pruning_history: [
      { before: 3, after: 1, accepted: true, rmse: 0, reason: 'refit accepted' },
    ], optimization_history: [], algorithm: {}, warnings: [],
  });
  assert.equal((await running).status, 'succeeded');
  assert.equal(executeDesignedNetwork('network').status, 'succeeded');
}
const built = {
  session_id: 'fitted-session', network_ir_hash: 'fitted-network', n: 3, d: 2, r: 1,
  free_species: ['A', 'X'], q_sym: ['tA', 'tX'], K_sym: ['Kd1'], x_sym: ['A', 'X', 'AX'], kd: [0.02],
};

setup();
await publishDesign();
let running = buildModel('builder', { triggerDownstream: false });
let pending = await nextRequest(1);
assert.deepEqual(JSON.parse(pending.options.body), {
  network: getReactionsFromNode('network').network_ir, build_mode: 'design_equilibrium',
});
respond(pending, built);
assert.equal(await running, true);
let context = getModelContextFromBuilder('builder');
assert.deepEqual(context.totals, { A: 10 });
assert.deepEqual(context.outputs, target.outputs);
assert.deepEqual(context.parameterDefaults, { tA: 10, tX: 1, Kd1: 0.02 });
prepareFixedParameterControls('params');
assert.equal(elements.get('params-fixed-tA').value, '10');
assert.equal(elements.get('params-fixed-Kd1').value, '0.02');
assert.equal(elements.get('params-fixed-tX').disabled, true);
assert.match(elements.get('params-design-readout').textContent, /log10.*offset -0.3/);
assert.deepEqual(scanConfigWithModelParameters(getNodeSerialData('params', 'scan-1d-params'), context).fixed_qK,
  [1, 0, Math.log10(0.02)]);
console.log('  ok - replayed design passes fitted totals, Kd and readouts through Model Builder into scan controls');

node('scan', 'scan-1d-result');
setConnections([...stateModule.connections,
  { fromNode: 'params', fromPort: 'params', toNode: 'scan', toPort: 'params' },
]);
running = executeScan1DResult('scan'); pending = await nextRequest(2);
assert.match(pending.url, /parameter_scan_1d$/);
assert.deepEqual(JSON.parse(pending.options.body).fixed_qK, [1, 0, Math.log10(0.02)]);
assert.equal(Object.hasOwn(JSON.parse(pending.options.body), 'fixedParameterOverrides'), false);
respond(pending, {
  param_symbol: 'tX', param_values: [-1, 1], output_exprs: ['A'], output_traj: [[0.2], [0.4]],
  valid: [true, true], partial: false, regimes: [],
});
assert.equal(await running, true);
assert.deepEqual(nodeRegistry.scan.data.scan1DResultMeta.request.fixed_qK, [1, 0, Math.log10(0.02)]);
console.log('  ok - the downstream scan HTTP request and persisted provenance use the fitted physical parameters');

const totalControl = elements.get('params-fixed-tA');
totalControl.value = '25'; totalControl.dispatchEvent({ type: 'input' });
assert.deepEqual(nodeRegistry.params.data.fixedParameterOverrides, { tA: 25 });
const serialized = getNodeSerialData('params', 'scan-1d-params');
assert.deepEqual(serialized.fixedParameterOverrides, { tA: 25 });
assert.deepEqual(scanConfigWithModelParameters(serialized, context).fixed_qK,
  [Math.log10(25), 0, Math.log10(0.02)]);
const otherContext = { model: { ...built, kd: [0.7] }, totals: { A: 50 } };
otherContext.parameterDefaults = modelParameterDefaults(otherContext.model, otherContext.totals);
assert.deepEqual(scanConfigWithModelParameters(serialized, otherContext).fixed_qK,
  [Math.log10(25), 0, Math.log10(0.7)]);
assert.deepEqual(serialized.fixedParameterOverrides, { tA: 25 });
assert.deepEqual(scanConfigWithModelParameters({ fixed_qK: [2, 3, 4], fixedParameterOverrides: { tA: 25 } }, context),
  { fixed_qK: [2, 3, 4] });
assert.throws(() => scanConfigWithModelParameters({ fixedParameterOverrides: { tA: null } }, context), /Fixed tA/);
assert.deepEqual(scanConfigWithModelParameters({ output_exprs: ['A'] }, { model: built }), { output_exprs: ['A'] });
console.log('  ok - explicit physical and log-qK overrides survive rebuilds and invalid values fail closed');

const saved = serializeState().nodes.find(entry => entry.id === 'builder').data;
assert.deepEqual(saved.modelContext.totals, { A: 10 });
assert.deepEqual(saved.modelContext.outputs, target.outputs);
restoreCachedNodeRuntime('builder', 'model-builder', saved);
assert.equal(getModelContextFromBuilder('builder'), null);
assert.deepEqual(nodeRegistry.builder.data.modelContext.totals, { A: 10 });
console.log('  ok - persisted fitted metadata remains historical until a live rebuild');

for (const field of ['totals', 'outputs']) {
  setup(); await publishDesign();
  running = buildModel('builder', { triggerDownstream: false }); pending = await nextRequest(1);
  if (field === 'totals') nodeRegistry.network.data.designedNetwork.totals.A = 100;
  else nodeRegistry.network.data.designedNetwork.outputs[0].offset = 0.5;
  respond(pending, built);
  assert.equal(await running, false, `${field} drift must retire the pending model`);
  assert.equal(getModelContextFromBuilder('builder'), null);
}
console.log('  ok - concentration or readout drift rejects an obsolete model response');

console.log('\nAll 5 designed model parameter integration tests passed.');
