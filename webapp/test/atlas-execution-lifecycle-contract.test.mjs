import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';

const toastContainer = { appendChild() {} };
global.window = {
  matchMedia: () => null,
  crypto: { randomUUID: () => 'atlas-lifecycle-test' },
  sessionStorage: { getItem: () => null, setItem() {} },
  localStorage: { getItem: () => null, setItem() {} },
  setTimeout,
  clearTimeout,
  dispatchEvent() {},
};
global.document = {
  getElementById: id => id === 'toast-container' ? toastContainer : null,
  createElement: () => ({
    className: '',
    textContent: '',
    classList: { add() {}, remove() {} },
    remove() {},
  }),
  documentElement: {
    dataset: {},
    style: { setProperty() {} },
  },
};
global.requestAnimationFrame = callback => callback();
global.CustomEvent = class CustomEvent {
  constructor(type, options = {}) {
    this.type = type;
    this.detail = options.detail;
  }
};

const { nodeRegistry, connections, wiringState } = await import('../public/js/state.js');
const {
  beginAtlasNodeExecution,
  executeAtlasBuilder,
  getConnectedAtlasData,
  isCurrentAtlasNodeExecution,
} = await import('../public/js/atlas.js');
const {
  invalidateAtlasExecutionsDownstreamOf,
  invalidateAtlasExecutionsForConnectionChange,
} = await import('../public/js/execution-lifecycle.js');
const { NODE_TYPES } = await import('../public/js/node-types/index.js');
const { runConnectedWorkspace } = await import('../public/js/nodes.js');

let passed = 0;
async function test(name, fn) {
  await fn();
  passed += 1;
  console.log(`  ok - ${name}`);
}

await test('Atlas execution tickets are latest-wins and bind node object identity', async () => {
  Object.keys(nodeRegistry).forEach(key => delete nodeRegistry[key]);
  nodeRegistry.atlas = { type: 'atlas-builder', data: {} };

  const first = beginAtlasNodeExecution('atlas', 'build');
  assert.equal(isCurrentAtlasNodeExecution('atlas', first), true);

  const second = beginAtlasNodeExecution('atlas', 'build');
  assert.equal(isCurrentAtlasNodeExecution('atlas', first), false);
  assert.equal(isCurrentAtlasNodeExecution('atlas', second), true);

  nodeRegistry.atlas = { type: 'atlas-builder', data: {} };
  assert.equal(
    isCurrentAtlasNodeExecution('atlas', second),
    false,
    'delete/recreate with the same id must reject the old owner',
  );
});

await test('new and invalidated Atlas executions retire stored upstream and downstream results', async () => {
  Object.keys(nodeRegistry).forEach(key => delete nodeRegistry[key]);
  connections.splice(0, connections.length);
  nodeRegistry.spec = { type: 'atlas-spec', data: {} };
  nodeRegistry.builder = {
    type: 'atlas-builder',
    data: { atlasData: { old: true }, lastSpec: { old: true }, sqlitePath: '/old' },
  };
  nodeRegistry.query = {
    type: 'atlas-query-result',
    data: { queryData: { old: true }, lastQuery: { old: true } },
  };
  nodeRegistry.inverse = {
    type: 'atlas-inverse-result',
    data: { inverseDesignData: { old: true }, lastInverseRequest: { old: true } },
  };
  connections.push(
    { fromNode: 'spec', fromPort: 'atlas-spec', toNode: 'builder', toPort: 'atlas-spec' },
    { fromNode: 'builder', fromPort: 'atlas', toNode: 'query', toPort: 'atlas' },
    { fromNode: 'spec', fromPort: 'atlas-spec', toNode: 'inverse', toPort: 'atlas-spec' },
  );

  beginAtlasNodeExecution('builder', 'atlas-builder');
  assert.equal(nodeRegistry.builder.data.atlasData, undefined);
  assert.equal(nodeRegistry.builder.data.lastSpec, undefined);
  assert.equal(nodeRegistry.builder.data.sqlitePath, undefined);
  assert.equal(nodeRegistry.query.data.queryData, undefined, 'a new builder run retires downstream queries');
  assert.deepEqual(nodeRegistry.inverse.data.inverseDesignData, { old: true });

  const invalidated = invalidateAtlasExecutionsDownstreamOf('spec', 'spec-changed');
  assert.deepEqual(new Set(invalidated), new Set(['builder', 'query', 'inverse']));
  assert.equal(nodeRegistry.inverse.data.inverseDesignData, undefined);
  assert.equal(nodeRegistry.inverse.data.lastInverseRequest, undefined);
});

await test('Atlas connection invalidation follows semantic upstream changes only', async () => {
  Object.keys(nodeRegistry).forEach(key => delete nodeRegistry[key]);
  connections.splice(0, connections.length);
  nodeRegistry.specA = { type: 'atlas-spec', data: {} };
  nodeRegistry.specB = { type: 'atlas-spec', data: {} };
  nodeRegistry.builder = { type: 'atlas-builder', data: { atlasData: { old: true } } };
  nodeRegistry.query = { type: 'atlas-query-result', data: { queryData: { old: true } } };
  const before = [
    { fromNode: 'specA', fromPort: 'atlas-spec', toNode: 'builder', toPort: 'atlas-spec' },
    { fromNode: 'builder', fromPort: 'atlas', toNode: 'query', toPort: 'atlas' },
  ];
  const after = [
    { fromNode: 'specB', fromPort: 'atlas-spec', toNode: 'builder', toPort: 'atlas-spec' },
    { fromNode: 'builder', fromPort: 'atlas', toNode: 'query', toPort: 'atlas' },
  ];

  assert.deepEqual(invalidateAtlasExecutionsForConnectionChange(before, before), []);
  assert.deepEqual(
    new Set(invalidateAtlasExecutionsForConnectionChange(before, after)),
    new Set(['builder', 'query']),
  );
  assert.equal(nodeRegistry.builder.data.atlasData, undefined);
  assert.equal(nodeRegistry.query.data.queryData, undefined);
});

await test('Atlas reads the committed graph during an interactive no-op rewire', async () => {
  Object.keys(nodeRegistry).forEach(key => delete nodeRegistry[key]);
  connections.splice(0, connections.length);
  nodeRegistry.builder = { type: 'atlas-builder', data: { atlasData: { current: true } } };
  nodeRegistry.query = { type: 'atlas-query-result', data: {} };
  const committed = {
    fromNode: 'builder', fromPort: 'atlas', toNode: 'query', toPort: 'atlas',
  };
  wiringState.isWiring = true;
  wiringState.connSnapshotBefore = [committed];
  try {
    assert.deepEqual(getConnectedAtlasData('query'), { current: true });
  } finally {
    wiringState.isWiring = false;
    wiringState.connSnapshotBefore = null;
  }
});

await test('real Atlas build rejects fingerprint drift and never republishes stale data', async () => {
  Object.keys(nodeRegistry).forEach(key => delete nodeRegistry[key]);
  connections.splice(0, connections.length);
  nodeRegistry.spec = { type: 'atlas-spec', data: {} };
  nodeRegistry.builder = { type: 'atlas-builder', data: { atlasData: { old: true } } };
  connections.push({
    fromNode: 'spec', fromPort: 'atlas-spec', toNode: 'builder', toPort: 'atlas-spec',
  });

  const priorFetch = globalThis.fetch;
  const priorGetElementById = globalThis.document.getElementById;
  const profileInput = { value: 'binding_small_v0' };
  const builderElement = { classList: { add() {}, remove() {} } };
  const contentElement = {
    dataset: {},
    innerHTML: '',
    querySelectorAll: () => [],
  };
  let resolveFetch;
  try {
    globalThis.document.getElementById = id => {
      if (id === 'toast-container') return toastContainer;
      if (id === 'builder') return builderElement;
      if (id === 'builder-content') return contentElement;
      if (id === 'spec-profile-name') return profileInput;
      return null;
    };
    globalThis.fetch = () => new Promise(resolve => { resolveFetch = resolve; });

    const build = executeAtlasBuilder('builder', {
      triggerDownstream: false,
      throwOnFailure: true,
    });
    assert.equal(nodeRegistry.builder.data.atlasData, undefined, 'the old result retires before fetch');
    profileInput.value = 'changed-profile';
    resolveFetch({
      ok: true,
      status: 200,
      headers: { get: () => 'application/json' },
      json: async () => ({ network_entries: [] }),
    });
    await assert.rejects(() => build, /inputs changed while it was running/);
    assert.equal(nodeRegistry.builder.data.atlasData, undefined);
  } finally {
    globalThis.fetch = priorFetch;
    globalThis.document.getElementById = priorGetElementById;
  }
});

await test('an old Atlas finally cannot clear the loading state of its replacement', async () => {
  Object.keys(nodeRegistry).forEach(key => delete nodeRegistry[key]);
  connections.splice(0, connections.length);
  nodeRegistry.spec = { type: 'atlas-spec', data: {} };
  nodeRegistry.builder = { type: 'atlas-builder', data: {} };
  connections.push({
    fromNode: 'spec', fromPort: 'atlas-spec', toNode: 'builder', toPort: 'atlas-spec',
  });

  const priorFetch = globalThis.fetch;
  const priorGetElementById = globalThis.document.getElementById;
  const loadingClasses = new Set();
  const builderElement = {
    classList: {
      add: value => loadingClasses.add(value),
      remove: value => loadingClasses.delete(value),
    },
  };
  const contentElement = { dataset: {}, innerHTML: '', querySelectorAll: () => [] };
  const pendingFetches = [];
  try {
    globalThis.document.getElementById = id => {
      if (id === 'toast-container') return toastContainer;
      if (id === 'builder') return builderElement;
      if (id === 'builder-content') return contentElement;
      return null;
    };
    globalThis.fetch = () => new Promise(resolve => pendingFetches.push(resolve));

    const first = executeAtlasBuilder('builder', {
      triggerDownstream: false,
      throwOnFailure: true,
    });
    const second = executeAtlasBuilder('builder', {
      triggerDownstream: false,
      throwOnFailure: true,
    });
    assert.equal(pendingFetches.length, 2);
    pendingFetches[0]({
      ok: true,
      status: 200,
      headers: { get: () => 'application/json' },
      json: async () => ({ network_entries: [] }),
    });
    await assert.rejects(() => first, /inputs changed while it was running/);
    assert.equal(loadingClasses.has('loading'), true);

    pendingFetches[1]({
      ok: true,
      status: 200,
      headers: { get: () => 'application/json' },
      json: async () => ({ network_entries: [] }),
    });
    await second;
    assert.equal(loadingClasses.has('loading'), false);
  } finally {
    globalThis.fetch = priorFetch;
    globalThis.document.getElementById = priorGetElementById;
  }
});

await test('Run Connected invokes every Atlas node once and suppresses builder auto-trigger', async () => {
  Object.keys(nodeRegistry).forEach(key => delete nodeRegistry[key]);
  connections.splice(0, connections.length);
  nodeRegistry.builder = { type: 'atlas-builder', data: {} };
  nodeRegistry.query = { type: 'atlas-query-result', data: {} };
  connections.push({
    fromNode: 'builder',
    fromPort: 'atlas',
    toNode: 'query',
    toPort: 'atlas',
  });

  const originalBuilderExecute = NODE_TYPES['atlas-builder'].execute;
  const originalQueryExecute = NODE_TYPES['atlas-query-result'].execute;
  const calls = [];
  try {
    NODE_TYPES['atlas-builder'].execute = async (nodeId, options) => {
      calls.push({ nodeId, options });
    };
    NODE_TYPES['atlas-query-result'].execute = async (nodeId, options) => {
      calls.push({ nodeId, options });
    };

    const priorSetTimeout = global.setTimeout;
    global.setTimeout = callback => {
      callback();
      return 0;
    };
    try {
      await runConnectedWorkspace();
    } finally {
      global.setTimeout = priorSetTimeout;
    }

    assert.deepEqual(calls.map(call => call.nodeId), ['builder', 'query']);
    assert.equal(calls[0].options.triggerDownstream, false);
    assert.equal(calls[0].options.throwOnFailure, true);
    assert.equal(calls[1].options.throwOnFailure, true);
  } finally {
    NODE_TYPES['atlas-builder'].execute = originalBuilderExecute;
    NODE_TYPES['atlas-query-result'].execute = originalQueryExecute;
    Object.keys(nodeRegistry).forEach(key => delete nodeRegistry[key]);
    connections.splice(0, connections.length);
  }

  const processSource = readFileSync(
    new URL('../public/js/node-types/process.js', import.meta.url),
    'utf8',
  );
  assert.match(
    processSource,
    /async execute\(nodeId, options = \{\}\)[\s\S]*executeAtlasBuilder\(nodeId, options\)/,
    'the Atlas Builder node adapter must forward Run Connected options',
  );
});

console.log(`\nAll ${passed} Atlas execution lifecycle tests passed.`);
