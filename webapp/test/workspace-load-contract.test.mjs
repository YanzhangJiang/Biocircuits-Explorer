import assert from 'node:assert/strict';

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
    this.offsetWidth = 100;
    this.offsetHeight = 60;
    this.classList = {
      add: () => {},
      remove: () => {},
      toggle: () => {},
      contains: () => false,
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

  remove() {
    this.isConnected = false;
    if (this._id) elements.delete(this._id);
    if (this.parentElement) {
      this.parentElement.children = this.parentElement.children.filter(child => child !== this);
    }
  }

  querySelector() { return null; }
  querySelectorAll() { return []; }
  addEventListener() {}
  setAttribute() {}
  removeAttribute() {}
}

global.Element = FakeElement;
global.HTMLSelectElement = class HTMLSelectElement extends FakeElement {};
global.ResizeObserver = class ResizeObserver {
  observe() {}
  disconnect() {}
};
global.requestAnimationFrame = callback => setTimeout(callback, 0);
global.cancelAnimationFrame = timer => clearTimeout(timer);
global.CustomEvent = class CustomEvent {
  constructor(type, options = {}) { this.type = type; this.detail = options.detail; }
};

global.window = {
  matchMedia: () => null,
  crypto: { randomUUID: () => 'workspace-load-test' },
  sessionStorage: { getItem: () => null, setItem: () => {} },
  addEventListener: () => {},
  dispatchEvent: () => {},
  setTimeout,
  clearTimeout,
};

global.document = {
  createElement: tagName => new FakeElement(tagName),
  createElementNS: (_namespace, tagName) => new FakeElement(tagName),
  getElementById: id => elements.get(id) || null,
  querySelector: () => null,
  querySelectorAll: () => [],
  documentElement: {
    dataset: {},
    style: { setProperty: () => {} },
  },
};

const canvas = new FakeElement('div');
canvas.id = 'canvas';

const stateModule = await import('../public/js/state.js');
const {
  state, nodeRegistry, setConnections, canvasState,
  setScale, MIN_SCALE, MAX_SCALE, MAX_CANVAS_PAN,
} = stateModule;
const { undoStack } = await import('../public/js/commands.js');
const { getSelection, setSelection } = await import('../public/js/selection.js');
const { NODE_TYPES } = await import('../public/js/node-types/index.js');
const { gridLineScreenPositions } = await import('../public/js/canvas.js');
const {
  applyState, initWorkspaceShell, serializeState, validateWorkspaceDocument,
} = await import('../public/js/workspace.js');

let passed = 0;
function test(name, fn) {
  fn();
  passed += 1;
  console.log(`  ok - ${name}`);
}

function resetHarness() {
  for (const id of Object.keys(nodeRegistry)) delete nodeRegistry[id];
  for (const [id, element] of [...elements]) {
    if (id !== 'canvas') element.remove();
  }
  canvas.children = [];
  setConnections([]);
  canvasState.panX = 0;
  canvasState.panY = 0;
  setScale(1);
  state.sessionId = null;
  state.model = null;
  state.qK_syms = [];
  setSelection([]);
  undoStack.clear();
}

function installExistingWorkspace() {
  const element = new FakeElement('div');
  element.id = 'existing';
  element.style.left = '12px';
  element.style.top = '34px';
  canvas.appendChild(element);
  const info = { type: 'markdown-note', el: element, data: { markdown: 'keep me' } };
  nodeRegistry.existing = info;
  setConnections([{ fromNode: 'existing', fromPort: 'result', toNode: 'existing', toPort: 'result' }]);
  canvasState.panX = 17;
  canvasState.panY = -9;
  setScale(1.25);
  state.sessionId = 'live-session';
  state.model = { n: 2 };
  state.qK_syms = ['A_tot'];
  setSelection(['existing']);
  undoStack.record({ label: 'existing history entry', apply() {}, revert() {} });
  return { element, info };
}

test('workspace canvas scale must be a finite in-range number', () => {
  for (const badScale of [-2, 0, MIN_SCALE / 2, MAX_SCALE + 0.1, '1', NaN, Infinity, -Infinity]) {
    assert.throws(
      () => validateWorkspaceDocument({ version: 1, canvas: { scale: badScale }, nodes: [] }),
      /canvas\.scale/,
      `expected scale ${String(badScale)} to be rejected`,
    );
  }

  for (const validScale of [MIN_SCALE, 1, MAX_SCALE]) {
    assert.equal(
      validateWorkspaceDocument({ version: 1, canvas: { scale: validScale }, nodes: [] }).canvas.scale,
      validScale,
    );
  }
});

test('workspace identity and node records fail closed before restoration', () => {
  assert.throws(
    () => validateWorkspaceDocument({ version: '999', nodes: [] }),
    /version must be an integer/,
  );
  assert.throws(
    () => validateWorkspaceDocument({
      version: 1,
      nodes: [{ id: 'same', type: 'markdown-note' }, { id: 'same', type: 'markdown-note' }],
    }),
    /duplicates workspace node/,
  );
  assert.throws(
    () => validateWorkspaceDocument({
      version: 1,
      nodes: [{ id: 'unknown', type: 'node-type-from-the-future' }],
    }),
    /not a supported node type/,
  );
  assert.throws(
    () => validateWorkspaceDocument({
      version: 1,
      canvas: { panX: '12' },
      nodes: [],
    }),
    /canvas\.panX/,
  );
  assert.throws(
    () => validateWorkspaceDocument({
      version: 1,
      canvas: { panX: MAX_CANVAS_PAN + 1 },
      nodes: [],
    }),
    /canvas pan/,
  );

  const historicalZeroSize = validateWorkspaceDocument({
    version: 1,
    nodes: [{ id: 'hidden', type: 'markdown-note', width: 0, height: 0 }],
  });
  assert.equal('width' in historicalZeroSize.nodes[0], false);
  assert.equal('height' in historicalZeroSize.nodes[0], false);
});

test('screen-space grid positions stay finite and bounded for extreme pan values', () => {
  for (const pan of [0, -25, Number.MAX_VALUE, -Number.MAX_VALUE]) {
    const positions = gridLineScreenPositions(1000, pan, 12);
    assert.ok(positions.length <= Math.ceil(1000 / 12) + 2);
    assert.ok(positions.every(position => Number.isFinite(position) && position >= 0 && position <= 1000));
  }
  assert.deepEqual(gridLineScreenPositions(1000, Infinity, 12), []);
  assert.deepEqual(gridLineScreenPositions(Infinity, 0, 12), []);
});

test('hidden nodes serialize their inline or declared dimensions instead of zero', () => {
  resetHarness();
  const element = new FakeElement('div');
  element.id = 'hidden-note';
  element.offsetWidth = 0;
  element.offsetHeight = 0;
  element.style.left = '1px';
  element.style.top = '2px';
  element.style.width = '420px';
  element.style.height = '210px';
  canvas.appendChild(element);
  nodeRegistry['hidden-note'] = {
    type: 'markdown-note',
    el: element,
    data: { markdown: 'hidden view' },
  };

  const serialized = serializeState();
  assert.equal(serialized.nodes[0].width, 420);
  assert.equal(serialized.nodes[0].height, 210);

  element.style.width = '';
  element.style.height = '';
  const defaulted = serializeState();
  assert.equal(defaulted.nodes[0].width, NODE_TYPES['markdown-note'].defaultWidth);
  assert.equal(defaulted.nodes[0].height, NODE_TYPES['markdown-note'].defaultHeight);
});

test('setScale rejects invalid values independently of workspace validation', () => {
  setScale(1);
  for (const badScale of [-1, 0, MIN_SCALE / 2, MAX_SCALE + 1, NaN, Infinity]) {
    assert.throws(() => setScale(badScale), /scale/i);
    assert.equal(stateModule.scale, 1, 'failed setter must retain the prior scale');
  }
});

test('malformed nodes are rejected before any live workspace state changes', () => {
  resetHarness();
  const { element, info } = installExistingWorkspace();
  const priorConnections = stateModule.connections.map(connection => ({ ...connection }));
  const priorModel = state.model;
  const priorUndoDepth = undoStack.depth;

  assert.throws(
    () => applyState({ version: 1, nodes: [null], connections: [] }),
    /nodes\[0\]/,
  );

  assert.equal(nodeRegistry.existing, info);
  assert.equal(element.isConnected, true);
  assert.deepEqual(stateModule.connections, priorConnections);
  assert.equal(canvasState.panX, 17);
  assert.equal(canvasState.panY, -9);
  assert.equal(stateModule.scale, 1.25);
  assert.equal(state.sessionId, 'live-session');
  assert.equal(state.model, priorModel);
  assert.deepEqual(state.qK_syms, ['A_tot']);
  assert.deepEqual(getSelection(), ['existing']);
  assert.equal(undoStack.depth, priorUndoDepth);
});

test('a restore hook failure removes staged nodes and preserves the old workspace', () => {
  resetHarness();
  const { element, info } = installExistingWorkspace();
  const counterBefore = stateModule.nodeIdCounter;
  const priorConnections = stateModule.connections.map(connection => ({ ...connection }));
  const priorModel = state.model;

  assert.throws(
    () => applyState({
      version: 1,
      canvas: { panX: 100, panY: 200, scale: 2 },
      nodes: [
        { id: 'saved-1', type: 'rop-poly-params', x: 1, y: 2,
          data: { pairs: { not: 'an array' } } },
      ],
      connections: [],
    }),
    /forEach/,
  );

  assert.equal(nodeRegistry.existing, info);
  assert.equal(element.isConnected, true);
  assert.deepEqual(Object.keys(nodeRegistry), ['existing']);
  assert.ok(stateModule.nodeIdCounter > counterBefore, 'failed staging IDs must never be reused');
  assert.deepEqual(stateModule.connections, priorConnections);
  assert.equal(canvasState.panX, 17);
  assert.equal(canvasState.panY, -9);
  assert.equal(stateModule.scale, 1.25);
  assert.equal(state.sessionId, 'live-session');
  assert.equal(state.model, priorModel);
  assert.deepEqual(state.qK_syms, ['A_tot']);
  assert.deepEqual(getSelection(), ['existing']);
  assert.equal(undoStack.depth, 1);
});

test('a failed restore preserves both undo and redo history', () => {
  resetHarness();
  installExistingWorkspace();
  undoStack.undo();
  assert.equal(undoStack.canRedo, true);

  assert.throws(
    () => applyState({ version: 1, nodes: [null], connections: [] }),
    /nodes\[0\]/,
  );
  assert.equal(undoStack.canRedo, true);
  assert.equal(undoStack.depth, 0);
});

test('a valid workspace replaces the old workspace only after staging succeeds', () => {
  resetHarness();
  const { element } = installExistingWorkspace();

  applyState({
    version: 1,
    canvas: { panX: 8, panY: 9, scale: 1.5 },
    nodes: [
      { id: 'saved-note', type: 'markdown-note', x: 10, y: 20, width: 320, height: 180,
        data: { markdown: 'restored' } },
    ],
    connections: [],
  });

  assert.equal(element.isConnected, false);
  assert.equal(nodeRegistry.existing, undefined);
  assert.equal(Object.keys(nodeRegistry).length, 1);
  const [newId] = Object.keys(nodeRegistry);
  assert.equal(nodeRegistry[newId].type, 'markdown-note');
  assert.equal(nodeRegistry[newId].data.markdown, 'restored');
  assert.equal(document.getElementById(newId).style.visibility, '');
  assert.equal(canvasState.panX, 8);
  assert.equal(canvasState.panY, 9);
  assert.equal(stateModule.scale, 1.5);
  assert.equal(state.sessionId, null);
  assert.equal(state.model, null);
  assert.deepEqual(state.qK_syms, []);
  assert.deepEqual(getSelection(), []);
  assert.equal(undoStack.depth, 0);
});

test('successful staging remaps valid connections exactly once', () => {
  resetHarness();
  installExistingWorkspace();
  NODE_TYPES['workspace-test-source'] = {
    category: 'input', headerClass: 'header-input', title: 'Source',
    inputs: [], outputs: [{ port: 'reactions', label: 'Reactions' }], createBody: () => '',
  };
  NODE_TYPES['workspace-test-sink'] = {
    category: 'process', headerClass: 'header-process', title: 'Sink',
    inputs: [{ port: 'reactions', label: 'Reactions' }], outputs: [], createBody: () => '',
  };

  try {
    applyState({
      version: 1,
      nodes: [
        { id: 'old-source', type: 'workspace-test-source', x: 0, y: 0 },
        { id: 'old-sink', type: 'workspace-test-sink', x: 100, y: 0 },
      ],
      connections: [
        { fromNode: 'old-source', fromPort: 'reactions', toNode: 'old-sink', toPort: 'reactions' },
      ],
    });
  } finally {
    delete NODE_TYPES['workspace-test-source'];
    delete NODE_TYPES['workspace-test-sink'];
  }

  assert.equal(stateModule.connections.length, 1);
  const [connection] = stateModule.connections;
  assert.ok(nodeRegistry[connection.fromNode]);
  assert.ok(nodeRegistry[connection.toNode]);
  assert.notEqual(connection.fromNode, 'old-source');
  assert.notEqual(connection.toNode, 'old-sink');
});

test('the public shell entry rejects a bad document without replacing the current workspace', () => {
  const [currentId] = Object.keys(nodeRegistry);
  const currentInfo = nodeRegistry[currentId];
  initWorkspaceShell();

  assert.throws(
    () => window.BiocircuitsExplorerWorkspaceShell.applyWorkspaceFromJSONString(JSON.stringify({
      version: 1,
      canvas: { scale: -2 },
      nodes: [],
      connections: [],
    })),
    /canvas\.scale/,
  );
  assert.equal(nodeRegistry[currentId], currentInfo);
});

test('a committed public-shell apply survives follow-up snapshot serialization failure', () => {
  resetHarness();
  installExistingWorkspace();
  initWorkspaceShell();

  const shell = window.BiocircuitsExplorerWorkspaceShell;
  const originalSerializeWorkspace = shell.serializeWorkspace;
  const originalWarn = console.warn;
  const warnings = [];
  const jsonString = JSON.stringify({
    version: 1,
    canvas: { panX: 88, panY: -44, scale: 1.5 },
    nodes: [],
    connections: [],
  });

  shell.serializeWorkspace = () => {
    throw new Error('synthetic post-apply serialization failure');
  };
  console.warn = (...arguments_) => warnings.push(arguments_.map(String).join(' '));
  try {
    assert.equal(shell.applyWorkspaceFromJSONString(jsonString), true);
  } finally {
    shell.serializeWorkspace = originalSerializeWorkspace;
    console.warn = originalWarn;
  }

  assert.deepEqual(Object.keys(nodeRegistry), []);
  assert.equal(canvasState.panX, 88);
  assert.equal(canvasState.panY, -44);
  assert.equal(stateModule.scale, 1.5);
  assert.equal(stateModule.lastWorkspaceShellSnapshot, jsonString);
  assert.ok(warnings.some(message => message.includes('snapshot serialization failed')));
});

resetHarness();
console.log(`\nAll ${passed} workspace load contract tests passed.`);
