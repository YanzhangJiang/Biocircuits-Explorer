import assert from 'node:assert/strict';

class FakeClassList {
  constructor() { this.values = new Set(); }
  add(...values) { values.forEach(value => this.values.add(value)); }
  remove(...values) { values.forEach(value => this.values.delete(value)); }
  contains(value) { return this.values.has(value); }
  toggle(value, force) {
    const enabled = force == null ? !this.values.has(value) : Boolean(force);
    if (enabled) this.values.add(value);
    else this.values.delete(value);
    return enabled;
  }
}

const elements = new Map();
const requests = [];
const alerts = [];

function fakeElement(overrides = {}) {
  return {
    className: '',
    textContent: '',
    innerHTML: '',
    value: '',
    dataset: {},
    style: {},
    classList: new FakeClassList(),
    appendChild() {},
    remove() {},
    querySelectorAll() { return []; },
    querySelector() { return null; },
    addEventListener() {},
    ...overrides,
  };
}

globalThis.window = {
  matchMedia: () => null,
  addEventListener() {},
  location: { protocol: 'http:', hostname: '127.0.0.1', port: '8000' },
  sessionStorage: { getItem: () => null, setItem() {} },
  localStorage: { getItem: () => null, setItem() {} },
  crypto: { randomUUID: () => 'design-lifecycle-test' },
  setTimeout,
  clearTimeout,
};
globalThis.document = {
  readyState: 'loading',
  documentElement: { dataset: {}, style: { setProperty() {} } },
  getElementById(id) { return elements.get(id) || null; },
  createElement() { return fakeElement(); },
  addEventListener() {},
  querySelectorAll() { return []; },
};
globalThis.requestAnimationFrame = callback => callback();
globalThis.alert = message => alerts.push(String(message));
globalThis.CustomEvent = class CustomEvent {};
globalThis.fetch = (url, options) => new Promise(resolve => {
  requests.push({ url, options, resolve });
});

await import('../public/js/node-types/index.js');
const {
  inspectDesignTargetLifecycles,
  invalidateDesignTargetInputs,
  runDesignSearch,
  selectNetwork,
} = await import('../public/js/node-types/design-target.js');
const {
  advanceWorkspaceRuntimeEpoch,
  getWorkspaceRuntimeEpoch,
  nodeRegistry,
  setConnections,
} = await import('../public/js/state.js');
const { getReactionsFromNode } = await import('../public/js/model.js');

let passed = 0;
async function test(name, callback) {
  await callback();
  passed += 1;
  console.log(`  ok - ${name}`);
}

function resetHarness() {
  Object.keys(nodeRegistry).forEach(key => delete nodeRegistry[key]);
  setConnections([]);
  elements.clear();
  requests.length = 0;
  alerts.length = 0;

  elements.set('status-badge', fakeElement());
  elements.set('toast-container', fakeElement());
  elements.set('design', fakeElement());
  elements.set('design-kind', fakeElement({ value: 'sign' }));
  elements.set('design-target', fakeElement({ value: '+-+' }));
  elements.set('design-content', fakeElement());
  nodeRegistry.design = { type: 'design-target', data: {} };
}

function response(marker, overrides = {}) {
  return {
    marker,
    designable: false,
    constraint_audit: [],
    verified_recommendations: [],
    screened_candidates: [],
    minimal_certificates: [],
    ...overrides,
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

await test('workspace runtime epoch is monotonic and independent of document version', () => {
  const before = getWorkspaceRuntimeEpoch();
  assert.equal(advanceWorkspaceRuntimeEpoch(), before + 1);
  assert.equal(getWorkspaceRuntimeEpoch(), before + 1);
});

await test('restored Design Target lifecycle snapshots bridge to historical runtime owners', () => {
  resetHarness();
  nodeRegistry.design.data = {
    config: {
      selectedCandidateKey: 'saved::tA::C_A_B',
      resolvedDefinition: { raw_rules: ['A + B <-> C_A_B'] },
    },
    lifecycle: {
      state: 'historical',
      freshness: 'historical',
      evidence: {
        evidence_grade: 'screened_proxy',
        sessionId: 'must-not-restore',
      },
      sessionId: 'must-not-restore-at-top-level',
    },
    selectionLifecycle: {
      state: 'historical',
      freshness: 'historical',
      evidence: {
        certificate_grade: 'proxy-only',
        nested: { session_id: 'must-not-restore-nested' },
      },
      session_id: 'must-not-restore-at-top-level',
    },
  };

  let lifecycles = inspectDesignTargetLifecycles('design');
  for (const runtime of [lifecycles.screen, lifecycles.selection]) {
    assert.equal(runtime.state, 'historical');
    assert.equal(runtime.freshness, 'historical');
    assert.equal(runtime.owner, nodeRegistry.design);
    assert.equal(runtime.workspaceEpoch, getWorkspaceRuntimeEpoch());
    assert.equal(runtime.result, null, 'saved result payload is not restored into runtime ownership');
    assert.equal(runtime.sessionId, null);
    assert.match(runtime.inputFingerprint, /design-target-historical-context/);
  }
  assert.deepEqual(lifecycles.screen.evidence, { evidence_grade: 'screened_proxy' });
  assert.deepEqual(lifecycles.selection.evidence, {
    certificate_grade: 'proxy-only',
    nested: {},
  });
  assert.equal(Object.hasOwn(nodeRegistry.design.data.lifecycle, 'sessionId'), false);
  assert.equal(Object.hasOwn(nodeRegistry.design.data.selectionLifecycle, 'session_id'), false);
  assert.deepEqual(
    getReactionsFromNode('design'),
    { reactions: [], kds: [] },
    'historical selection cannot feed a downstream model build',
  );

  invalidateDesignTargetInputs('design', 'restored-target-edited');
  lifecycles = inspectDesignTargetLifecycles('design');
  assert.equal(lifecycles.screen.state, 'invalidated');
  assert.equal(lifecycles.screen.freshness, 'invalidated');
  assert.equal(lifecycles.selection.state, 'invalidated');
  assert.equal(lifecycles.selection.freshness, 'invalidated');
  assert.equal(nodeRegistry.design.data.config.resolvedDefinition, undefined);
  assert.deepEqual(getReactionsFromNode('design'), { reactions: [], kds: [] });
});

await test('latest Design Search response wins and obsolete finally cannot clear loading', async () => {
  resetHarness();
  const first = runDesignSearch('design');
  await waitForRequests(1);
  elements.get('design-target').value = '-+-';
  const second = runDesignSearch('design');
  await waitForRequests(2);
  assert.equal(elements.get('design').classList.contains('loading'), true);
  assert.equal(requests[0].url, '/api/v1/design_screen');

  respond(requests[0], response('old'));
  const firstOutcome = await first;
  assert.equal(firstOutcome.status, 'stale');
  assert.equal(elements.get('design').classList.contains('loading'), true);
  assert.equal(nodeRegistry.design.data.designSearchResponse, undefined);

  respond(requests[1], response('new'));
  const secondOutcome = await second;
  assert.equal(secondOutcome.status, 'succeeded');
  assert.equal(nodeRegistry.design.data.designSearchResponse.marker, 'new');
  assert.equal(elements.get('design').classList.contains('loading'), false);
  assert.deepEqual(nodeRegistry.design.data.lifecycle, {
    state: 'current', freshness: 'current',
    evidence: { source_endpoint: '/api/v1/design_screen', truncated: false },
  });
});

await test('delete and recreate with the same node id rejects the old owner response', async () => {
  resetHarness();
  const oldOwner = nodeRegistry.design;
  const oldRun = runDesignSearch('design');
  await waitForRequests(1);

  nodeRegistry.design = { type: 'design-target', data: {} };
  elements.get('design-target').value = '++-';
  const newRun = runDesignSearch('design');
  await waitForRequests(2);
  assert.notEqual(nodeRegistry.design, oldOwner);

  respond(requests[0], response('old-owner'));
  assert.equal((await oldRun).status, 'stale');
  assert.equal(elements.get('design').classList.contains('loading'), true);
  assert.equal(nodeRegistry.design.data.designSearchResponse, undefined);

  respond(requests[1], response('new-owner'));
  assert.equal((await newRun).status, 'succeeded');
  assert.equal(nodeRegistry.design.data.designSearchResponse.marker, 'new-owner');
});

await test('workspace epoch drift rejects a delayed Design Search response', async () => {
  resetHarness();
  const run = runDesignSearch('design');
  await waitForRequests(1);
  advanceWorkspaceRuntimeEpoch();
  respond(requests[0], response('prior-workspace'));

  assert.equal((await run).status, 'stale');
  assert.equal(nodeRegistry.design.data.designSearchResponse, undefined);
  assert.equal(nodeRegistry.design.data.lifecycle.state, 'invalidated');
  assert.equal(elements.get('design').classList.contains('loading'), false);
});

await test('invalid retry retires screen and selected outputs before validation or HTTP', async () => {
  resetHarness();
  const current = runDesignSearch('design');
  await waitForRequests(1);
  respond(requests[0], response('current'));
  await current;
  nodeRegistry.design.data.config = {
    selectedCandidateKey: 'old::tA::C_A_B',
    resolvedDefinition: { raw_rules: ['A + B <-> C_A_B'] },
  };

  elements.get('design-target').value = '';
  const outcome = await runDesignSearch('design');
  assert.equal(outcome.status, 'blocked');
  assert.equal(requests.length, 1, 'invalid retry must not reach the backend');
  assert.equal(nodeRegistry.design.data.designSearchResponse, undefined);
  assert.equal(nodeRegistry.design.data.config.resolvedDefinition, undefined);
  assert.equal(nodeRegistry.design.data.lifecycle.state, 'blocked');
  assert.ok(alerts.length > 0);
});

await test('screen freshness and manually selected network freshness remain separate', async () => {
  resetHarness();
  const card = {
    nid: '[1]+[2]<->[1,2]',
    inp: 'tA',
    out: 'C_A_B',
    pass: false,
    screen_status: 'screened_proxy',
    evidence_grade: 'proxy_only',
    certificate_grade: 'proxy-only',
  };
  const run = runDesignSearch('design');
  await waitForRequests(1);
  respond(requests[0], response('proxy-screen', {
    designable: true,
    screened_candidates: [card],
  }));
  await run;

  let lifecycles = inspectDesignTargetLifecycles('design');
  assert.equal(lifecycles.screen.freshness, 'current');
  assert.notEqual(lifecycles.selection?.freshness, 'current');

  assert.deepEqual(selectNetwork('design', card.nid, card.inp, card.out), [
    'A + B <-> C_A_B',
  ]);
  lifecycles = inspectDesignTargetLifecycles('design');
  assert.equal(lifecycles.screen.freshness, 'current');
  assert.equal(lifecycles.selection.freshness, 'current');
  assert.equal(lifecycles.selection.evidence.evidence_grade, 'proxy_only');
  assert.equal(lifecycles.selection.evidence.certificate_grade, 'proxy-only');
  assert.deepEqual(getReactionsFromNode('design').reactions, ['A + B <-> C_A_B']);

  nodeRegistry.design.data.selectionLifecycle = {
    ...nodeRegistry.design.data.selectionLifecycle,
    state: 'historical',
    freshness: 'historical',
  };
  assert.deepEqual(
    getReactionsFromNode('design'),
    { reactions: [], kds: [] },
    'a restored selected candidate must not rebuild downstream models',
  );

  invalidateDesignTargetInputs('design', 'target-edited');
  lifecycles = inspectDesignTargetLifecycles('design');
  assert.equal(lifecycles.screen.freshness, 'invalidated');
  assert.equal(lifecycles.selection.freshness, 'invalidated');
  assert.equal(nodeRegistry.design.data.config.resolvedDefinition, undefined);
});

console.log(`\nAll ${passed} Design Target execution lifecycle contract tests passed.`);
