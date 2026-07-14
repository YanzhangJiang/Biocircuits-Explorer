import assert from 'node:assert/strict';

const elements = new Map();
const toastMessages = [];
const requests = [];

class FakeClassList {
  constructor() { this.values = new Set(); }
  add(value) { this.values.add(value); }
  remove(value) { this.values.delete(value); }
  contains(value) { return this.values.has(value); }
}

function fakeElement() {
  return {
    className: '',
    textContent: '',
    style: {},
    classList: new FakeClassList(),
    appendChild(child) {
      if (child?.textContent) toastMessages.push(child.textContent);
    },
    remove() {},
  };
}

let currentRule = 'A + B <-> AB';
let currentKd = 1;
const reactionRow = {
  querySelector(selector) {
    if (selector === '.reaction-input') return { value: currentRule };
    if (selector === '.kd-input') return { value: String(currentKd) };
    return null;
  },
};
const reactionList = { querySelectorAll: () => [reactionRow] };
const builderElement = fakeElement();
const modelInfo = fakeElement();
const modelInfoText = fakeElement();
const toastContainer = fakeElement();
const statusBadge = fakeElement();

elements.set('source-reactions-list', reactionList);
elements.set('builder', builderElement);
elements.set('builder-model-info', modelInfo);
elements.set('builder-model-info-text', modelInfoText);
elements.set('toast-container', toastContainer);
elements.set('status-badge', statusBadge);

global.setTimeout = () => 0;
global.clearTimeout = () => {};
global.requestAnimationFrame = callback => { callback(); return 0; };
global.window = {
  matchMedia: () => null,
  crypto: { randomUUID: () => 'model-build-test' },
  sessionStorage: { getItem: () => null, setItem: () => {} },
  addEventListener: () => {},
  dispatchEvent: () => {},
  setTimeout: global.setTimeout,
  clearTimeout: global.clearTimeout,
};
global.document = {
  getElementById: id => elements.get(id) || null,
  querySelector: () => null,
  querySelectorAll: () => [],
  createElement: () => fakeElement(),
  documentElement: { dataset: {}, style: { setProperty: () => {} } },
};
global.CustomEvent = class CustomEvent {};

global.fetch = (url, options) => new Promise((resolve) => {
  requests.push({ url, options, resolve });
});

const stateModule = await import('../public/js/state.js');
const { advanceWorkspaceRuntimeEpoch, state, nodeRegistry, setConnections } = stateModule;
const { buildModel } = await import('../public/js/model.js');
const {
  ensureModelSession, getModelContextFromBuilder, markReactionSourceDirty,
} = await import('../public/js/nodes.js');
const {
  MODEL_BUILD_ENDPOINT,
  beginModelBuild,
  commitModelBuild,
  inspectModelBuildLifecycle,
  modelBuildContext,
  releaseModelBuild,
} = await import('../public/js/model-lifecycle.js');
const { restoreCachedNodeRuntime, serializeState } = await import('../public/js/workspace.js');

let passed = 0;
async function test(name, fn) {
  await fn();
  passed += 1;
  console.log(`  ok - ${name}`);
}

function modelResponse(sessionId, marker) {
  return {
    session_id: sessionId,
    network_ir_hash: `hash-${marker}`,
    network_ir: { label: marker },
    artifact: { marker },
    n: 2,
    d: 1,
    r: 1,
    x_sym: ['A', 'B', 'AB'],
    q_sym: ['A_tot'],
    K_sym: ['Kd_1'],
  };
}

function respond(request, json, status = 200) {
  request.resolve({
    status,
    headers: { get: () => 'application/json; charset=utf-8' },
    json: async () => json,
  });
}

function containsSessionIdentifier(value, visited = new Set()) {
  if (!value || typeof value !== 'object') return false;
  if (visited.has(value)) return false;
  visited.add(value);
  if (!Array.isArray(value) && Object.keys(value).some(key => /^session_?id$/i.test(key))) {
    return true;
  }
  return Object.values(value).some(entry => containsSessionIdentifier(entry, visited));
}

function resetHarness({ withViewer = false } = {}) {
  for (const id of Object.keys(nodeRegistry)) delete nodeRegistry[id];
  requests.length = 0;
  toastMessages.length = 0;
  currentRule = 'A + B <-> AB';
  currentKd = 1;
  builderElement.classList.values.clear();
  modelInfo.style = {};
  modelInfoText.textContent = '';
  statusBadge.className = '';
  statusBadge.textContent = '';
  state.sessionId = null;
  state.model = null;
  state.qK_syms = [];
  nodeRegistry.source = { type: 'reaction-network', data: {} };
  nodeRegistry.builder = { type: 'model-builder', data: {} };
  const graph = [{
    fromNode: 'source', fromPort: 'reactions', toNode: 'builder', toPort: 'reactions',
  }];
  if (withViewer) {
    nodeRegistry.viewer = { type: 'scan-1d-params', data: {} };
    graph.push({ fromNode: 'builder', fromPort: 'model', toNode: 'viewer', toPort: 'model' });
  }
  setConnections(graph);
}

await test('an invalid new build attempt retires an older pending response', async () => {
  resetHarness();
  const oldBuild = buildModel('builder');
  assert.equal(requests.length, 1);

  // Simulate a programmatic input mutation that did not emit an editor event.
  // Starting a new build must still own/retire the previous ticket even though
  // this new input fails validation before issuing HTTP.
  currentRule = '';
  assert.equal(await buildModel('builder'), false);
  assert.equal(requests.length, 1);

  respond(requests[0], modelResponse('stale-session', 'stale'));
  assert.equal(await oldBuild, false);
  assert.equal(nodeRegistry.builder.data.modelContext, undefined);
  assert.equal(state.sessionId, null);
});

await test('an old success cannot clear loading or overwrite a newer build', async () => {
  resetHarness();
  const oldBuild = buildModel('builder');
  currentRule = 'A + C <-> AC';
  markReactionSourceDirty('source');
  const newBuild = buildModel('builder');
  assert.equal(requests.length, 2);
  assert.equal(builderElement.classList.contains('loading'), true);

  respond(requests[0], modelResponse('old-session', 'old'));
  assert.equal(await oldBuild, false);
  assert.equal(builderElement.classList.contains('loading'), true, 'old finally must not clear new loading');
  assert.equal(nodeRegistry.builder.data.modelContext, undefined);

  respond(requests[1], modelResponse('new-session', 'new'));
  assert.equal(await newBuild, true);
  assert.equal(builderElement.classList.contains('loading'), false);
  assert.equal(nodeRegistry.builder.data.modelContext.sessionId, 'new-session');
  assert.equal(nodeRegistry.builder.data.modelContext.model.network_ir.label, 'new');
});

await test('an old failure cannot report a node error or clear a newer build', async () => {
  resetHarness();
  const oldBuild = buildModel('builder');
  currentRule = 'A + C <-> AC';
  markReactionSourceDirty('source');
  const newBuild = buildModel('builder');

  respond(requests[0], { error: 'old request failed' }, 500);
  assert.equal(await oldBuild, false);
  assert.equal(builderElement.classList.contains('loading'), true);
  assert.equal(toastMessages.some(message => message.includes('old request failed')), false);
  assert.equal(statusBadge.textContent.includes('old request failed'), false);
  assert.match(statusBadge.className, /working/);

  respond(requests[1], modelResponse('new-session', 'new'));
  assert.equal(await newBuild, true);
});

await test('an old completion cannot erase the current build error status', async () => {
  for (const oldOutcome of ['success', 'failure']) {
    resetHarness();
    const oldBuild = buildModel('builder');
    currentRule = 'A + C <-> AC';
    markReactionSourceDirty('source');
    const currentBuild = buildModel('builder');

    respond(requests[1], { error: 'current request failed' }, 500);
    assert.equal(await currentBuild, false);
    assert.match(statusBadge.className, /error/);
    assert.equal(statusBadge.textContent, 'current request failed');

    if (oldOutcome === 'success') {
      respond(requests[0], modelResponse('obsolete-session', 'obsolete'));
    } else {
      respond(requests[0], { error: 'obsolete request failed' }, 500);
    }
    assert.equal(await oldBuild, false);
    assert.match(statusBadge.className, /error/);
    assert.equal(statusBadge.textContent, 'current request failed');
  }
});

await test('an old completion closes the working count after the current build succeeds', async () => {
  for (const oldOutcome of ['success', 'failure']) {
    resetHarness();
    const oldBuild = buildModel('builder');
    currentRule = 'A + C <-> AC';
    markReactionSourceDirty('source');
    const currentBuild = buildModel('builder');

    respond(requests[1], modelResponse('current-session', 'current'));
    assert.equal(await currentBuild, true);
    assert.match(statusBadge.className, /working/);
    assert.equal(statusBadge.textContent, 'Computing... (1)');

    if (oldOutcome === 'success') {
      respond(requests[0], modelResponse('obsolete-session', 'obsolete'));
    } else {
      respond(requests[0], { error: 'obsolete request failed' }, 500);
    }
    assert.equal(await oldBuild, false);
    assert.match(statusBadge.className, /done/);
    assert.equal(statusBadge.textContent, 'Done');
  }
});

await test('invalidating a pending build clears loading even without a replacement build', async () => {
  for (const outcome of ['success', 'failure']) {
    resetHarness();
    const pending = buildModel('builder');
    assert.equal(builderElement.classList.contains('loading'), true);

    currentRule = '';
    markReactionSourceDirty('source');
    assert.equal(builderElement.classList.contains('loading'), false);

    if (outcome === 'success') {
      respond(requests[0], modelResponse('obsolete-session', 'obsolete'));
    } else {
      respond(requests[0], { error: 'obsolete failure' }, 500);
    }
    assert.equal(await pending, false);
    assert.equal(builderElement.classList.contains('loading'), false);
    assert.equal(nodeRegistry.builder.data.modelContext, undefined);
    assert.equal(toastMessages.some(message => message.includes('obsolete failure')), false);
  }
});

await test('a fingerprint mismatch retires last-good context before invalid-input validation', async () => {
  resetHarness();
  const lastGood = modelResponse('last-good-session', 'last-good');
  const fingerprint = JSON.stringify(['source', ['A + B <-> AB'], [1]]);
  nodeRegistry.builder.data = {
    built: true,
    modelContext: {
      sessionId: 'last-good-session',
      model: lastGood,
      qK_syms: ['A_tot', 'Kd_1'],
      builtForRevision: 0,
      inputFingerprint: fingerprint,
      sourceNodeId: 'source',
    },
  };
  state.sessionId = 'last-good-session';
  state.model = lastGood;
  state.qK_syms = ['A_tot', 'Kd_1'];

  currentRule = '';
  assert.equal(await buildModel('builder'), false);
  assert.equal(nodeRegistry.builder.data.built, false);
  assert.equal(nodeRegistry.builder.data.modelContext.sessionId, null);
  assert.ok(nodeRegistry.builder._modelInputRevision > 0);
  assert.equal(state.sessionId, null);
  assert.equal(state.model, null);
});

await test('an unannounced fingerprint change during build retires last-good context', async () => {
  resetHarness();
  const lastGood = modelResponse('last-good-session', 'last-good');
  nodeRegistry.builder.data = {
    built: true,
    modelContext: {
      sessionId: 'last-good-session',
      model: lastGood,
      qK_syms: ['A_tot', 'Kd_1'],
      builtForRevision: 0,
      inputFingerprint: JSON.stringify(['source', ['A + B <-> AB'], [1]]),
      sourceNodeId: 'source',
    },
  };
  state.sessionId = 'last-good-session';
  state.model = lastGood;
  state.qK_syms = ['A_tot', 'Kd_1'];

  const pending = buildModel('builder');
  currentRule = 'A + C <-> AC'; // deliberately bypass markReactionSourceDirty
  respond(requests[0], modelResponse('obsolete-session', 'obsolete'));

  assert.equal(await pending, false);
  assert.equal(nodeRegistry.builder.data.built, false);
  assert.equal(nodeRegistry.builder.data.modelContext.sessionId, null);
  assert.equal(state.sessionId, null);
  assert.equal(builderElement.classList.contains('loading'), false);
});

await test('an obsolete build error after unannounced input drift retires last-good silently', async () => {
  resetHarness();
  const lastGood = modelResponse('last-good-session', 'last-good');
  nodeRegistry.builder.data = {
    built: true,
    modelContext: {
      sessionId: 'last-good-session',
      model: lastGood,
      qK_syms: ['A_tot', 'Kd_1'],
      builtForRevision: 0,
      inputFingerprint: JSON.stringify(['source', ['A + B <-> AB'], [1]]),
      sourceNodeId: 'source',
    },
  };
  state.sessionId = 'last-good-session';
  state.model = lastGood;
  state.qK_syms = ['A_tot', 'Kd_1'];

  const pending = buildModel('builder', { throwOnFailure: true });
  currentRule = 'A + C <-> AC'; // deliberately bypass markReactionSourceDirty
  respond(requests[0], { error: 'obsolete build error' }, 500);

  assert.equal(await pending, false, 'obsolete errors must not escape through throwOnFailure');
  assert.equal(nodeRegistry.builder.data.built, false);
  assert.equal(nodeRegistry.builder.data.modelContext.sessionId, null);
  assert.equal(state.sessionId, null);
  assert.equal(builderElement.classList.contains('loading'), false);
  assert.equal(toastMessages.some(message => message.includes('obsolete build error')), false);
  assert.equal(statusBadge.textContent.includes('obsolete build error'), false);
});

await test('ensureModelSession starts a new single-flight build for a new input revision', async () => {
  resetHarness({ withViewer: true });
  const oldEnsure = ensureModelSession('viewer');
  assert.equal(requests.length, 1);

  currentRule = 'A + C <-> AC';
  markReactionSourceDirty('source');
  const newEnsure = ensureModelSession('viewer');
  assert.equal(requests.length, 2, 'new revision must not reuse the old build promise');

  let oldOutcome = 'pending';
  const trackedOldEnsure = oldEnsure.then(
    value => { oldOutcome = value; return value; },
    error => { oldOutcome = error; return error; },
  );
  respond(requests[0], modelResponse('old-session', 'old'));
  await Promise.resolve();
  await Promise.resolve();
  assert.equal(oldOutcome, 'pending', 'old caller should hand off to the current revision build');

  respond(requests[1], modelResponse('new-session', 'new'));
  assert.equal(await newEnsure, 'new-session');
  assert.equal(await trackedOldEnsure, 'new-session');
  assert.equal(nodeRegistry.builder.data.modelContext.sessionId, 'new-session');
});

await test('Model Builder shared lifecycle is current only after the exact owner context commits', async () => {
  resetHarness();
  const pending = buildModel('builder');
  assert.deepEqual(nodeRegistry.builder.data.lifecycle, {
    state: 'running', freshness: 'empty', evidence: null,
  });
  respond(requests[0], modelResponse('current-session', 'current'));
  assert.equal(await pending, true);

  const runtime = inspectModelBuildLifecycle('builder');
  assert.equal(runtime.state, 'current');
  assert.equal(runtime.freshness, 'current');
  assert.equal(runtime.endpoint, MODEL_BUILD_ENDPOINT);
  assert.equal(runtime.owner, nodeRegistry.builder);
  assert.equal(runtime.result, nodeRegistry.builder.data.modelContext);
  assert.equal(getModelContextFromBuilder('builder'), runtime.result);
  assert.deepEqual(nodeRegistry.builder.data.lifecycle, {
    state: 'current',
    freshness: 'current',
    evidence: {
      source_endpoint: MODEL_BUILD_ENDPOINT,
      network_ir_hash: 'hash-current',
    },
  });
});

await test('workspace epoch and endpoint drift invalidate a Model Builder ticket', async () => {
  resetHarness();
  const pending = buildModel('builder');
  advanceWorkspaceRuntimeEpoch();
  respond(requests[0], modelResponse('obsolete-session', 'obsolete'));
  assert.equal(await pending, false);
  assert.equal(inspectModelBuildLifecycle('builder').state, 'invalidated');
  assert.equal(nodeRegistry.builder.data.lifecycle.freshness, 'invalidated');
  assert.equal(getModelContextFromBuilder('builder'), null);

  resetHarness();
  const beginContext = modelBuildContext('builder', 'fingerprint-a');
  const ticket = beginModelBuild('builder', beginContext);
  const endpointDrift = { ...beginContext, endpoint: '/api/v1/not-build-model' };
  assert.equal(commitModelBuild('builder', ticket, endpointDrift, {
    modelContext: { sessionId: 'must-not-commit', model: {} },
  }), false);
  assert.equal(inspectModelBuildLifecycle('builder').state, 'invalidated');
  assert.equal(getModelContextFromBuilder('builder'), null);
  assert.equal(releaseModelBuild('builder', ticket), true);
});

await test('restored Model Builder output is historical, session-free, and unavailable downstream', async () => {
  resetHarness();
  const savedModel = modelResponse('nested-backend-session', 'saved');
  savedModel.nested = {
    sessionId: 'nested-camel-session',
    deeper: [{ session_id: 'nested-snake-session', value: 1 }],
  };
  restoreCachedNodeRuntime('builder', 'model-builder', {
    modelContext: {
      sessionId: 'outer-session',
      model: savedModel,
      qK_syms: ['A_tot', 'Kd_1'],
      inputFingerprint: 'saved-fingerprint',
      nested: { session_id: 'another-session' },
    },
    lifecycle: {
      state: 'current',
      freshness: 'current',
      evidence: { source_endpoint: MODEL_BUILD_ENDPOINT },
    },
  });

  assert.equal(nodeRegistry.builder.data.built, false);
  assert.equal(containsSessionIdentifier(nodeRegistry.builder.data.modelContext), false);
  assert.equal(nodeRegistry.builder.data.lifecycle.state, 'historical');
  assert.equal(nodeRegistry.builder.data.lifecycle.freshness, 'historical');
  assert.equal(inspectModelBuildLifecycle('builder').sessionId, null);
  assert.equal(getModelContextFromBuilder('builder'), null);
});

await test('Workspace serialization recursively strips Model Builder session identifiers', async () => {
  resetHarness();
  const pending = buildModel('builder');
  const response = modelResponse('live-session', 'serialized');
  response.deep = { sessionId: 'deep-session', list: [{ session_id: 'deeper-session' }] };
  respond(requests[0], response);
  assert.equal(await pending, true);
  nodeRegistry.builder.data.modelContext.provenance = {
    nested: { sessionId: 'provenance-session', session_id: 'provenance-snake' },
  };

  const document = serializeState();
  const savedBuilder = document.nodes.find(node => node.id === 'builder');
  assert.ok(savedBuilder);
  assert.equal(savedBuilder.data.lifecycle.state, 'current');
  assert.equal(containsSessionIdentifier(savedBuilder.data), false);
  assert.equal(savedBuilder.data.modelContext.sessionId, undefined);
  assert.equal(savedBuilder.data.modelContext.model.session_id, undefined);
});

console.log(`\nAll ${passed} model build lifecycle contract tests passed.`);
