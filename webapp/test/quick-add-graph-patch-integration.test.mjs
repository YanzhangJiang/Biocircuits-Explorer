import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';

import { planDesignSpecExportWorkflow } from '../public/js/graph-patch.js';

const elements = new Map();

class FakeElement {
  constructor(tagName = 'div') {
    this.tagName = String(tagName).toUpperCase();
    this.style = {};
    this.dataset = {};
    this.children = [];
    this.parentElement = null;
    this.isConnected = true;
    this.className = '';
    this.innerHTML = '';
    this.textContent = '';
    this.offsetWidth = 280;
    this.offsetHeight = 220;
    this.attributes = new Map();
    this.classList = {
      add() {},
      remove() {},
      toggle() {},
      contains() { return false; },
    };
    this._id = '';
  }

  set id(value) {
    if (this._id) elements.delete(this._id);
    this._id = String(value);
    if (this._id) elements.set(this._id, this);
  }

  get id() { return this._id; }

  appendChild(child) {
    child.parentElement = this;
    child.isConnected = true;
    this.children.push(child);
    return child;
  }

  append(...children) { children.forEach(child => this.appendChild(child)); }

  replaceChildren(...children) {
    this.children = [];
    this.append(...children);
  }

  remove() {
    this.isConnected = false;
    if (this._id) elements.delete(this._id);
    if (this.parentElement) {
      this.parentElement.children = this.parentElement.children.filter(child => child !== this);
    }
  }

  querySelector() { return null; }
  querySelectorAll() { return []; }
  closest() { return null; }
  contains() { return false; }
  addEventListener() {}
  dispatchEvent() { return true; }
  setAttribute(name, value) { this.attributes.set(name, String(value)); }
  getAttribute(name) { return this.attributes.get(name) || null; }
  removeAttribute(name) { this.attributes.delete(name); }
  getBoundingClientRect() {
    return { left: 0, top: 0, width: this.offsetWidth, height: this.offsetHeight };
  }
}

const scheduled = [];
function fakeSetTimeout(callback, delay = 0) {
  const handle = { callback, delay, cancelled: false };
  scheduled.push(handle);
  return handle;
}
function fakeClearTimeout(handle) {
  if (handle) handle.cancelled = true;
}

globalThis.Element = FakeElement;
globalThis.HTMLSelectElement = class HTMLSelectElement extends FakeElement {};
globalThis.ResizeObserver = class ResizeObserver {
  observe() {}
  disconnect() {}
};
globalThis.requestAnimationFrame = callback => {
  callback();
  return 1;
};
globalThis.cancelAnimationFrame = () => {};
globalThis.setTimeout = fakeSetTimeout;
globalThis.clearTimeout = fakeClearTimeout;
globalThis.CustomEvent = class CustomEvent {
  constructor(type, options = {}) { this.type = type; this.detail = options.detail; }
};

globalThis.window = {
  matchMedia: () => null,
  crypto: { randomUUID: () => 'quick-add-test' },
  location: { protocol: 'http:', hostname: '127.0.0.1', port: '8000' },
  sessionStorage: { getItem: () => null, setItem: () => {} },
  localStorage: { getItem: () => null, setItem: () => {} },
  addEventListener() {},
  dispatchEvent() {},
  setTimeout: fakeSetTimeout,
  clearTimeout: fakeClearTimeout,
  requestAnimationFrame: globalThis.requestAnimationFrame,
};

globalThis.document = {
  readyState: 'loading',
  createElement: tagName => new FakeElement(tagName),
  createElementNS: (_namespace, tagName) => new FakeElement(tagName),
  getElementById: id => elements.get(id) || null,
  querySelector: () => null,
  querySelectorAll: () => [],
  addEventListener() {},
  documentElement: { dataset: {}, style: { setProperty() {} } },
  head: new FakeElement('head'),
};

for (const id of [
  'canvas',
  'add-node-menu',
  'legacy-nodes-menu',
  'theme-mode-menu',
  'toast-container',
]) {
  const element = new FakeElement('div');
  element.id = id;
}

const stateModule = await import('../public/js/state.js');
const commandModule = await import('../public/js/commands.js');
const selectionModule = await import('../public/js/selection.js');
const { NODE_TYPES } = await import('../public/js/node-types/index.js');
const nodesModule = await import('../public/js/nodes.js');
const agentNodeModule = await import('../public/js/agent-node.js');
const designTargetModule = await import('../public/js/node-types/design-target.js');

const LEGACY_RESTORE_ONLY = [
  'siso-analysis',
  'rop-cloud',
  'fret-heatmap',
  'parameter-scan-1d',
  'parameter-scan-2d',
  'rop-polyhedron',
];

function resetHarness() {
  for (const id of Object.keys(stateModule.nodeRegistry)) {
    nodesModule.removeNode(id);
  }
  stateModule.setConnections([]);
  stateModule.setNodeIdCounter(0);
  selectionModule.setSelection([]);
  commandModule.undoStack.clear();
  scheduled.length = 0;
}

function topology() {
  return {
    nodeIds: Object.keys(stateModule.nodeRegistry),
    connections: stateModule.connections.map(connection => ({ ...connection })),
    nodeIdCounter: stateModule.nodeIdCounter,
  };
}

test('restore-only legacy nodes fail closed outside explicit v1 restore or migration', () => {
  resetHarness();
  const originalError = console.error;
  console.error = () => {};
  try {
    for (const nodeType of LEGACY_RESTORE_ONLY) {
      assert.equal(nodesModule.createNode(nodeType, 0, 0), null, nodeType);
    }
  } finally {
    console.error = originalError;
  }
  assert.deepEqual(topology(), { nodeIds: [], connections: [], nodeIdCounter: 0 });

  assert.equal(
    nodesModule.createNode('siso-analysis', 10, 20, {
      id: 'node-9',
      creationMode: 'workspace-restore',
    }),
    'node-9',
  );
  assert.equal(stateModule.nodeRegistry['node-9'].type, 'siso-analysis');
  assert.equal(stateModule.nodeIdCounter, 9);
});

test('production Quick Add is one Undo item and Redo preserves every planned ID', () => {
  resetHarness();
  const result = nodesModule.addQuickAddChain('siso-analysis');

  assert.equal(result.ok, true);
  assert.equal(commandModule.undoStack.depth, 1);
  assert.equal(Object.keys(stateModule.nodeRegistry).length, 4);
  assert.equal(stateModule.connections.length, 3);
  const applied = topology();
  const firstEpochTimers = scheduled.filter(handle => handle.delay === 100);
  assert.equal(firstEpochTimers.length, 2);

  commandModule.undo();
  assert.deepEqual(topology(), { nodeIds: [], connections: [], nodeIdCounter: 0 });
  assert.equal(commandModule.undoStack.depth, 0);
  assert.equal(commandModule.undoStack.canRedo, true);
  assert.equal(firstEpochTimers.every(handle => handle.cancelled), true);
  firstEpochTimers.forEach(handle => handle.callback());
  assert.deepEqual(topology(), { nodeIds: [], connections: [], nodeIdCounter: 0 });

  commandModule.redo();
  assert.deepEqual(topology(), applied);
  assert.equal(commandModule.undoStack.depth, 1);
  commandModule.undo();
});

test('every production Quick Add workflow passes typed full-graph validation atomically', () => {
  const workflowTypes = [
    'siso-analysis',
    'rop-cloud',
    'fret-heatmap',
    'parameter-scan-1d',
    'parameter-scan-2d',
    'rop-polyhedron',
    'parameter-placer',
    'atlas-preview',
    'atlas-search',
    'atlas-workflow',
    'atlas-inverse-design',
  ];

  for (const workflowType of workflowTypes) {
    resetHarness();
    const result = nodesModule.addQuickAddChain(workflowType);
    assert.equal(result.ok, true, workflowType);
    assert.equal(commandModule.undoStack.depth, 1, `${workflowType} history depth`);
    for (const spec of result.patch.nodes) {
      assert.equal(NODE_TYPES[spec.type].availability, 'active', `${workflowType}: ${spec.type}`);
    }
    commandModule.undo();
    assert.deepEqual(
      topology(),
      { nodeIds: [], connections: [], nodeIdCounter: 0 },
      `${workflowType} undo`,
    );
  }
});

test('ambiguous sources require selection while explicit isolation creates a fresh source', () => {
  resetHarness();
  assert.equal(nodesModule.createNode('reaction-network', 0, 0), 'node-1');
  assert.equal(nodesModule.createNode('sbml-import', 350, 0), 'node-2');
  const before = topology();

  const ambiguous = nodesModule.addQuickAddChain('parameter-scan-1d');
  assert.equal(ambiguous.ok, false);
  assert.equal(ambiguous.diagnostic.code, 'manual-source-selection-required');
  assert.deepEqual(topology(), before);
  assert.equal(commandModule.undoStack.depth, 0);

  selectionModule.setSelection(['node-2']);
  const selected = nodesModule.addQuickAddChain('parameter-scan-1d');
  assert.equal(selected.ok, true);
  assert.equal(selected.patch.metadata.sourceNodeId, 'node-2');
  assert.equal(selected.patch.metadata.sourceWasCreated, false);
  commandModule.undo();

  selectionModule.setSelection([]);
  const isolated = nodesModule.addQuickAddChain('parameter-scan-1d', {
    createIsolatedSource: true,
  });
  assert.equal(isolated.ok, true);
  assert.equal(isolated.patch.metadata.isolatedSourceCreated, true);
  assert.notEqual(isolated.patch.metadata.sourceNodeId, 'node-1');
  assert.notEqual(isolated.patch.metadata.sourceNodeId, 'node-2');
  commandModule.undo();
});

test('full-graph validation failure preserves graph, ID counter, and Undo depth', () => {
  resetHarness();
  nodesModule.createNode('reaction-network', 0, 0);
  stateModule.setConnections([{
    fromNode: 'node-1',
    fromPort: 'reactions',
    toNode: 'missing-builder',
    toPort: 'reactions',
  }]);
  const before = topology();

  const originalError = console.error;
  console.error = () => {};
  let result;
  try {
    result = nodesModule.addQuickAddChain('siso-analysis');
  } finally {
    console.error = originalError;
  }

  assert.equal(result.ok, false);
  assert.equal(result.diagnostic.code, 'connection-endpoint-missing');
  assert.deepEqual(topology(), before);
  assert.equal(commandModule.undoStack.depth, 0);
  assert.equal(commandModule.undoStack.canRedo, false);
});

test('a real partial DOM commit rolls back nodes, connections, history, and shell snapshot', () => {
  resetHarness();
  nodesModule.createNode('reaction-network', 15, 25);
  const before = topology();
  const shellSnapshot = JSON.stringify({ marker: 'before-quick-add' });
  stateModule.setLastWorkspaceShellSnapshot(shellSnapshot);
  let afterChangeCalls = 0;
  commandModule.registerPerformers({
    afterChange() { afterChangeCalls += 1; },
  });

  const originalInit = NODE_TYPES['siso-result'].onInit;
  NODE_TYPES['siso-result'].onInit = () => {
    throw new Error('synthetic Quick Add node initialization failure');
  };
  const originalError = console.error;
  console.error = () => {};
  let result;
  try {
    result = nodesModule.addQuickAddChain('siso-analysis');
  } finally {
    console.error = originalError;
    if (originalInit) NODE_TYPES['siso-result'].onInit = originalInit;
    else delete NODE_TYPES['siso-result'].onInit;
    commandModule.registerPerformers({ afterChange: null });
  }

  assert.equal(result.ok, false);
  assert.match(result.diagnostic.message, /synthetic Quick Add node initialization failure/);
  assert.deepEqual(topology(), before);
  assert.equal(commandModule.undoStack.depth, 0);
  assert.equal(commandModule.undoStack.canRedo, false);
  assert.equal(afterChangeCalls, 0);
  assert.equal(stateModule.lastWorkspaceShellSnapshot, shellSnapshot);
});

test('Design Build & Tune graph is one atomic command with cancellable deferred work', () => {
  resetHarness();
  nodesModule.createNode('design-target', 20, 30);
  const before = topology();
  const btn = new FakeElement('button');
  btn.textContent = 'Build & tune →';
  btn.disabled = false;

  const result = designTargetModule.spawnBuildAndTuneGraph('node-1', {
    inputSymbol: 'S_tot',
    outputSymbol: 'C_ES',
    btn,
  });
  assert.equal(result.ok, true);
  assert.equal(btn.disabled, true);
  assert.equal(btn.textContent, 'Building…');
  assert.equal(commandModule.undoStack.depth, 1);
  assert.deepEqual(Object.values(stateModule.nodeRegistry).map(info => info.type), [
    'design-target', 'model-builder', 'placer-params', 'placer-result',
  ]);
  assert.equal(stateModule.connections.length, 3);
  const ids = result.patch.nodes.map(spec => spec.id);
  const deferred = scheduled.filter(handle => handle.delay === 100);
  assert.equal(deferred.length, 1);

  commandModule.undo();
  assert.deepEqual(topology(), before);
  assert.equal(btn.disabled, false);
  assert.equal(btn.textContent, 'Build & tune →');
  assert.equal(deferred[0].cancelled, true);
  deferred[0].callback();
  assert.deepEqual(topology(), before);

  commandModule.redo();
  assert.deepEqual(result.patch.nodes.map(spec => spec.id), ids);
  commandModule.undo();
});

test('Design Build & Tune rolls back a real partial node commit without adding Undo history', () => {
  resetHarness();
  nodesModule.createNode('design-target', 20, 30);
  const before = topology();
  const originalInit = NODE_TYPES['placer-result'].onInit;
  NODE_TYPES['placer-result'].onInit = () => {
    throw new Error('synthetic Build & Tune node initialization failure');
  };
  const originalError = console.error;
  console.error = () => {};

  let result;
  try {
    result = designTargetModule.spawnBuildAndTuneGraph('node-1', {
      inputSymbol: 'S_tot',
      outputSymbol: 'C_ES',
    });
  } finally {
    console.error = originalError;
    if (originalInit) NODE_TYPES['placer-result'].onInit = originalInit;
    else delete NODE_TYPES['placer-result'].onInit;
  }

  assert.equal(result.ok, false);
  assert.match(result.diagnostic.message, /synthetic Build & Tune node initialization failure/);
  assert.deepEqual(topology(), before);
  assert.equal(commandModule.undoStack.depth, 0);
  assert.equal(commandModule.undoStack.canRedo, false);
  assert.equal(scheduled.some(handle => handle.delay === 100), false);
});

test('Agent Design Spec export uses one production GraphPatch with stable Undo/Redo IDs', () => {
  resetHarness();
  const spec = {
    schema_version: 'bne-designability/v1.0.0',
    target: { legacy_target: { target_kind: 'sign', target: ['+', '-'] } },
    constraints: {},
  };
  const plan = planDesignSpecExportWorkflow({
    graph: nodesModule.captureEditorGraphPlanningGraph(),
    nextNodeOrdinal: stateModule.nodeIdCounter + 1,
    anchor: { x: 90, y: 100 },
    spec,
  });
  assert.equal(plan.ok, true);

  const command = nodesModule.createEditorGraphPatchCommand(plan, {
    initializeNode(nodeSpec, { nodeId }) {
      if (nodeSpec.initialization?.kind !== 'design-spec-config') return;
      const info = stateModule.nodeRegistry[nodeId];
      info.data = info.data || {};
      info.data.config = { designabilitySpec: structuredClone(nodeSpec.initialization.spec) };
    },
  });
  commandModule.dispatch(command);

  assert.equal(commandModule.undoStack.depth, 1);
  assert.deepEqual(Object.values(stateModule.nodeRegistry).map(info => info.type), [
    'design-spec-config', 'design-target',
  ]);
  assert.deepEqual(stateModule.connections, [{
    fromNode: plan.patch.metadata.specNodeId,
    fromPort: 'designability-spec',
    toNode: plan.patch.metadata.designTargetNodeId,
    toPort: 'designability-spec',
  }]);
  assert.deepEqual(
    stateModule.nodeRegistry[plan.patch.metadata.specNodeId].data.config.designabilitySpec,
    spec,
  );
  const applied = topology();
  const ids = [...applied.nodeIds];

  commandModule.undo();
  assert.deepEqual(topology(), { nodeIds: [], connections: [], nodeIdCounter: 0 });
  commandModule.redo();
  assert.deepEqual(topology(), applied);
  assert.deepEqual(Object.keys(stateModule.nodeRegistry), ids);
  commandModule.undo();

  const mainSource = readFileSync(new URL('../public/js/main.js', import.meta.url), 'utf8');
  const exportStart = mainSource.indexOf('function exportDesignSpecToWorkspace');
  const exportEnd = mainSource.indexOf('window.exportDesignSpecToWorkspace', exportStart);
  assert.notEqual(exportStart, -1);
  assert.notEqual(exportEnd, -1);
  const productionExport = mainSource.slice(exportStart, exportEnd);
  assert.match(productionExport, /planDesignSpecExportWorkflow\s*\(/);
  assert.match(productionExport, /createEditorGraphPatchCommand\s*\(/);
  assert.match(productionExport, /dispatch\s*\(command\)/);
  assert.doesNotMatch(productionExport, /\bcreateNode\s*\(/);
  assert.doesNotMatch(productionExport, /\baddConnection\s*\(/);
});

test('Agent Design Spec export initialization failure restores topology, counter, history, and snapshot', () => {
  resetHarness();
  nodesModule.createNode('markdown-note', 15, 25);
  const before = topology();
  const shellSnapshot = JSON.stringify({ marker: 'before-design-spec-export' });
  stateModule.setLastWorkspaceShellSnapshot(shellSnapshot);
  let afterChangeCalls = 0;
  commandModule.registerPerformers({
    afterChange() { afterChangeCalls += 1; },
  });
  const plan = planDesignSpecExportWorkflow({
    graph: nodesModule.captureEditorGraphPlanningGraph(),
    nextNodeOrdinal: stateModule.nodeIdCounter + 1,
    spec: {
      schema_version: 'bne-designability/v1.0.0',
      target: {},
      constraints: {},
    },
  });
  const command = nodesModule.createEditorGraphPatchCommand(plan, {
    initializeNode(nodeSpec) {
      if (nodeSpec.initialization?.kind === 'design-spec-config') {
        throw new Error('synthetic Design Spec initialization failure');
      }
    },
  });

  assert.throws(() => commandModule.dispatch(command), /synthetic Design Spec initialization failure/);
  commandModule.registerPerformers({ afterChange: null });
  assert.deepEqual(topology(), before);
  assert.equal(commandModule.undoStack.depth, 0);
  assert.equal(commandModule.undoStack.canRedo, false);
  assert.equal(afterChangeCalls, 0);
  assert.equal(stateModule.lastWorkspaceShellSnapshot, shellSnapshot);
});

test('Agent autoSpawn is atomic, initializes only active split nodes, and leaves no failed residue', () => {
  resetHarness();
  const agentElement = new FakeElement('div');
  agentElement.id = 'agent-source';
  agentElement.style.left = '10px';
  agentElement.style.top = '20px';
  document.getElementById('canvas').appendChild(agentElement);
  stateModule.nodeRegistry['agent-source'] = { type: 'ai-import', el: agentElement, data: {} };
  const result = {
    networks: [{
      name: 'network A',
      reactions: [{ rule: 'E + S <-> C_ES', kd: 0.001 }],
      recommended_analyses: [
        { name: 'siso-analysis' },
        { name: 'parameter-scan-1d', scan_param: 'S_tot', output_expression: 'C_ES' },
      ],
    }],
    warnings: [],
  };

  const spawned = agentNodeModule.autoSpawn('agent-source', result);
  assert.equal(spawned.ok, true);
  assert.equal(commandModule.undoStack.depth, 1);
  assert.equal(
    Object.values(stateModule.nodeRegistry).some(info => info.type === 'siso-analysis'),
    false,
  );
  const applied = topology();
  const ids = spawned.patch.nodes.map(spec => spec.id);
  const deferred = scheduled.filter(handle => handle.delay === 100);
  assert.equal(deferred.length, 3);

  commandModule.undo();
  assert.deepEqual(topology(), {
    nodeIds: ['agent-source'],
    connections: [],
    nodeIdCounter: 0,
  });
  assert.equal(deferred.every(handle => handle.cancelled), true);
  commandModule.redo();
  assert.deepEqual(topology(), applied);
  assert.deepEqual(spawned.patch.nodes.map(spec => spec.id), ids);
  commandModule.undo();

  const invalidBefore = topology();
  const invalid = agentNodeModule.autoSpawn('agent-source', {
    networks: [{
      name: 'bad',
      reactions: [{ rule: 'E + S <-> C_ES', kd: 0.001 }],
      recommended_analyses: [{ name: 'restore-only-or-unknown' }],
    }],
    warnings: [],
  });
  assert.equal(invalid.ok, false);
  assert.deepEqual(topology(), invalidBefore);
  assert.equal(commandModule.undoStack.depth, 0);
});

test('Agent autoSpawn rolls back a real partial node commit without adding Undo history', () => {
  resetHarness();
  const agentElement = new FakeElement('div');
  agentElement.id = 'agent-source';
  agentElement.style.left = '10px';
  agentElement.style.top = '20px';
  document.getElementById('canvas').appendChild(agentElement);
  stateModule.nodeRegistry['agent-source'] = { type: 'ai-import', el: agentElement, data: {} };
  const before = topology();
  const originalInit = NODE_TYPES['scan-1d-result'].onInit;
  NODE_TYPES['scan-1d-result'].onInit = () => {
    throw new Error('synthetic Agent node initialization failure');
  };
  const originalError = console.error;
  console.error = () => {};

  let result;
  try {
    result = agentNodeModule.autoSpawn('agent-source', {
      networks: [{
        name: 'network A',
        reactions: [{ rule: 'E + S <-> C_ES', kd: 0.001 }],
        recommended_analyses: [{
          name: 'parameter-scan-1d',
          scan_param: 'S_tot',
          output_expression: 'C_ES',
        }],
      }],
      warnings: [],
    });
  } finally {
    console.error = originalError;
    if (originalInit) NODE_TYPES['scan-1d-result'].onInit = originalInit;
    else delete NODE_TYPES['scan-1d-result'].onInit;
  }

  assert.equal(result.ok, false);
  assert.match(result.diagnostic.message, /synthetic Agent node initialization failure/);
  assert.deepEqual(topology(), before);
  assert.equal(commandModule.undoStack.depth, 0);
  assert.equal(commandModule.undoStack.canRedo, false);
  assert.equal(scheduled.some(handle => handle.delay === 100), false);
});
