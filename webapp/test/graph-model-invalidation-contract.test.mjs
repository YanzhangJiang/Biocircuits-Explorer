import assert from 'node:assert/strict';

global.window = {
  matchMedia: () => null,
  crypto: { randomUUID: () => 'graph-model-test' },
  sessionStorage: { getItem: () => null, setItem: () => {} },
  addEventListener: () => {},
  dispatchEvent: () => {},
  setTimeout,
  clearTimeout,
};
global.document = {
  getElementById: () => null,
  querySelector: () => null,
  querySelectorAll: () => [],
  documentElement: { dataset: {}, style: { setProperty: () => {} } },
};
global.CustomEvent = class CustomEvent {};

const stateModule = await import('../public/js/state.js');
const { state, nodeRegistry, setConnections } = stateModule;
const {
  addConnection, finalizeInteractiveConnectionChange, removeConnection, replaceConnections,
} = await import('../public/js/connections.js');
const {
  getModelContextFromBuilder, markReactionSourceDirty, removeNode,
} = await import('../public/js/nodes.js');
const { addReactionRow } = await import('../public/js/model.js');
const { applyImportedSbmlReactions } = await import('../public/js/sbml-io.js');

let passed = 0;
function test(name, fn) {
  fn();
  passed += 1;
  console.log(`  ok - ${name}`);
}

function reset() {
  for (const id of Object.keys(nodeRegistry)) delete nodeRegistry[id];
  setConnections([]);
  state.sessionId = null;
  state.model = null;
  state.qK_syms = [];
}

function addNode(id, type) {
  nodeRegistry[id] = { type, data: {} };
  return nodeRegistry[id];
}

function seedBuiltModel(builderId = 'builder', sessionId = 'session-a') {
  const model = { session_id: sessionId, q_sym: ['A_tot'], K_sym: ['Kd_1'] };
  const info = addNode(builderId, 'model-builder');
  info.data = {
    built: true,
    modelContext: {
      sessionId,
      networkIrHash: 'hash-a',
      model,
      qK_syms: ['A_tot', 'Kd_1'],
    },
  };
  info._buildToken = Symbol('pending-old-build');
  state.sessionId = sessionId;
  state.model = model;
  state.qK_syms = ['A_tot', 'Kd_1'];
  return info;
}

const reactionEdge = (source, builder = 'builder') => ({
  fromNode: source,
  fromPort: 'reactions',
  toNode: builder,
  toPort: 'reactions',
});

test('disconnecting a reaction input retires the builder context immediately', () => {
  reset();
  addNode('source-a', 'reaction-network');
  const builder = seedBuiltModel();
  const edge = reactionEdge('source-a');
  setConnections([edge]);

  removeConnection(edge);

  assert.equal(getModelContextFromBuilder('builder'), null);
  assert.equal(builder.data.built, false);
  assert.equal(builder.data.modelContext.sessionId, null);
  assert.equal(builder._buildToken, undefined);
  assert.ok(builder._modelInputRevision > 0);
  assert.equal(state.sessionId, null);
  assert.equal(state.model, null);
  assert.deepEqual(state.qK_syms, []);
});

test('invalidating one builder does not clear another builder from global compatibility state', () => {
  reset();
  addNode('source-a', 'reaction-network');
  addNode('source-b', 'reaction-network');
  const builderA = seedBuiltModel('builder-a', 'session-a');
  const builderB = seedBuiltModel('builder-b', 'session-b');
  const modelB = builderB.data.modelContext.model;
  const edgeA = reactionEdge('source-a', 'builder-a');
  const edgeB = reactionEdge('source-b', 'builder-b');
  setConnections([edgeA, edgeB]);

  removeConnection(edgeA);

  assert.equal(builderA.data.built, false);
  assert.equal(builderB.data.built, true);
  assert.equal(state.sessionId, 'session-b');
  assert.equal(state.model, modelB);
  assert.deepEqual(state.qK_syms, ['A_tot', 'Kd_1']);
});

test('rewiring the reaction input retires the old model before the new build', () => {
  reset();
  addNode('source-a', 'reaction-network');
  addNode('source-b', 'reaction-network');
  const builder = seedBuiltModel();
  const oldEdge = reactionEdge('source-a');
  setConnections([oldEdge]);

  const replaced = addConnection(reactionEdge('source-b'));

  assert.deepEqual(replaced, oldEdge);
  assert.equal(builder.data.built, false);
  assert.equal(builder.data.modelContext.sessionId, null);
  assert.deepEqual(stateModule.connections, [reactionEdge('source-b')]);
});

test('whole-graph replacement invalidates on real input changes but not unrelated edges', () => {
  reset();
  addNode('source-a', 'reaction-network');
  addNode('source-b', 'reaction-network');
  addNode('other-a', 'markdown-note');
  addNode('other-b', 'markdown-note');
  const builder = seedBuiltModel();
  const input = reactionEdge('source-a');
  setConnections([input]);

  replaceConnections([input, {
    fromNode: 'other-a', fromPort: 'result', toNode: 'other-b', toPort: 'result',
  }]);
  assert.equal(builder.data.built, true, 'unrelated graph edits must retain the model');
  assert.equal(builder._modelInputRevision ?? 0, 0);

  replaceConnections([reactionEdge('source-b')]);
  assert.equal(builder.data.built, false);
  assert.ok(builder._modelInputRevision > 0);
});

test('dragging a reaction wire back to its original socket is a semantic no-op', () => {
  reset();
  addNode('source-a', 'reaction-network');
  const builder = seedBuiltModel();
  const before = [
    reactionEdge('source-a'),
    { fromNode: 'other-a', fromPort: 'result', toNode: 'other-b', toPort: 'result' },
  ];
  setConnections(before);

  // Match the interactive drag path: detach transiently, then return to the
  // exact original graph before mouseup finalization.
  setConnections([]);
  setConnections([...before].reverse().map(connection => ({ ...connection })));
  const changed = finalizeInteractiveConnectionChange(before);

  assert.equal(changed, false);
  assert.equal(builder.data.built, true);
  assert.equal(builder.data.modelContext.sessionId, 'session-a');
  assert.equal(builder._modelInputRevision ?? 0, 0);
});

test('undo/redo-style connection replacement retires whichever input is no longer current', () => {
  reset();
  addNode('source-a', 'reaction-network');
  addNode('source-b', 'reaction-network');
  let builder = seedBuiltModel();
  const edgeA = reactionEdge('source-a');
  const edgeB = reactionEdge('source-b');
  setConnections([edgeA]);

  replaceConnections([edgeB]);
  assert.equal(builder.data.built, false);

  builder = seedBuiltModel('builder', 'session-b');
  replaceConnections([edgeA]);
  assert.equal(builder.data.built, false);
  assert.equal(builder.data.modelContext.sessionId, null);
});

test('deleting a reaction source invalidates surviving downstream builders', () => {
  reset();
  addNode('source-a', 'reaction-network');
  const builder = seedBuiltModel();
  setConnections([reactionEdge('source-a')]);

  removeNode('source-a');

  assert.equal(nodeRegistry['source-a'], undefined);
  assert.equal(builder.data.built, false);
  assert.equal(builder.data.modelContext.sessionId, null);
  assert.deepEqual(stateModule.connections, []);
});

test('reaction source edits mark the model dirty synchronously', () => {
  reset();
  addNode('source-a', 'reaction-network');
  const builder = seedBuiltModel();
  setConnections([reactionEdge('source-a')]);

  const affected = markReactionSourceDirty('source-a');

  assert.deepEqual(affected, ['builder']);
  assert.equal(builder.data.built, false);
  assert.equal(builder.data.modelContext.sessionId, null);
  assert.equal(getModelContextFromBuilder('builder'), null);
});

test('the reaction editor invalidates on the input event, before its rebuild debounce', () => {
  reset();
  addNode('source-a', 'reaction-network');
  const builder = seedBuiltModel();
  setConnections([reactionEdge('source-a')]);

  const list = {
    children: [],
    appendChild(child) { this.children.push(child); },
  };
  const originalGetElementById = document.getElementById;
  const originalCreateElement = document.createElement;
  document.getElementById = id => id === 'source-a-reactions-list' ? list : null;
  document.createElement = tagName => ({
    tagName: String(tagName).toUpperCase(),
    children: [],
    listeners: {},
    append(...children) { this.children.push(...children); },
    addEventListener(type, callback) {
      this.listeners[type] = this.listeners[type] || [];
      this.listeners[type].push(callback);
    },
    remove() {},
  });

  try {
    addReactionRow('source-a', 'A + B <-> AB', 1);
    const row = list.children[0];
    const reactionInput = row.children[0];
    assert.equal(builder.data.built, true);
    reactionInput.listeners.input[0]();
    assert.equal(builder.data.built, false, 'dirty state must be synchronous');
    assert.equal(builder.data.modelContext.sessionId, null);
  } finally {
    document.getElementById = originalGetElementById;
    document.createElement = originalCreateElement;
  }
});

test('applying imported SBML reactions retires an already-built downstream model', () => {
  reset();
  addNode('sbml-source', 'sbml-import');
  const builder = seedBuiltModel();
  setConnections([reactionEdge('sbml-source')]);

  const list = {
    innerHTML: 'old rows',
    children: [],
    appendChild(child) { this.children.push(child); },
  };
  const originalGetElementById = document.getElementById;
  const originalCreateElement = document.createElement;
  document.getElementById = id => id === 'sbml-source-reactions-list' ? list : null;
  document.createElement = tagName => ({
    tagName: String(tagName).toUpperCase(),
    children: [],
    listeners: {},
    append(...children) { this.children.push(...children); },
    addEventListener(type, callback) {
      this.listeners[type] = this.listeners[type] || [];
      this.listeners[type].push(callback);
    },
    remove() {},
  });

  try {
    applyImportedSbmlReactions('sbml-source', [
      { formula: 'A + B <-> AB', kd: 2 },
    ]);
    assert.equal(builder.data.built, false);
    assert.equal(builder.data.modelContext.sessionId, null);
    assert.equal(list.innerHTML, '');
    assert.equal(list.children.length, 1);
  } finally {
    document.getElementById = originalGetElementById;
    document.createElement = originalCreateElement;
  }
});

reset();
console.log(`\nAll ${passed} graph/model invalidation contract tests passed.`);
