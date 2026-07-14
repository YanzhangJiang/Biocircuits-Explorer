import assert from 'node:assert/strict';

import {
  DERIVED_RESULT_STATES,
  RESULT_FRESHNESS,
  begin,
  block,
  commit,
  createExecutionLifecycle,
  fail,
  inspectExecutionLifecycle,
  invalidate,
  isCurrent,
  readCurrentResult,
  readReusableArtifact,
  release,
  restoreHistorical,
  serializeExecutionLifecycle,
} from '../public/js/execution-lifecycle-core.js';

let passed = 0;
function test(name, callback) {
  callback();
  passed += 1;
  console.log(`  ok - ${name}`);
}

function executionContext(owner, overrides = {}) {
  return {
    owner,
    workspaceEpoch: 'workspace-7',
    inputFingerprint: 'sha256:input-a',
    endpoint: '/api/v1/parameter_scan_1d',
    ...overrides,
  };
}

test('the core exposes exactly the seven lifecycle states and four freshness states', () => {
  assert.deepEqual([...DERIVED_RESULT_STATES].sort(), [
    'blocked', 'current', 'empty', 'failed', 'historical', 'invalidated', 'running',
  ]);
  assert.deepEqual([...RESULT_FRESHNESS].sort(), [
    'current', 'empty', 'historical', 'invalidated',
  ]);

  const lifecycle = createExecutionLifecycle();
  assert.deepEqual(serializeExecutionLifecycle(lifecycle), {
    state: 'empty',
    freshness: 'empty',
    evidence: null,
  });
});

test('begin creates a non-persistable ticket bound to owner identity and all execution inputs', () => {
  const lifecycle = createExecutionLifecycle();
  const owner = { id: 'node-1' };
  const context = executionContext(owner);
  const ticket = begin(lifecycle, context);

  assert.equal(ticket.owner, owner);
  assert.equal(ticket.revision, 1);
  assert.equal(ticket.workspaceEpoch, context.workspaceEpoch);
  assert.equal(ticket.inputFingerprint, context.inputFingerprint);
  assert.equal(ticket.endpoint, context.endpoint);
  assert.equal(Object.isFrozen(ticket), true);
  assert.throws(() => JSON.stringify(ticket), /runtime ticket cannot be serialized/);
  assert.equal(isCurrent(lifecycle, ticket, context), true);

  const runtime = inspectExecutionLifecycle(lifecycle);
  assert.equal(runtime.state, 'running');
  assert.equal(runtime.loading, true);
  assert.equal(runtime.owner, owner);
  assert.equal(runtime.currentTicket, ticket);

  const persisted = serializeExecutionLifecycle(lifecycle);
  assert.deepEqual(persisted, { state: 'running', freshness: 'empty', evidence: null });
  assert.equal('owner' in persisted, false);
  assert.equal('ticket' in persisted, false);
  assert.equal('sessionId' in persisted, false);
});

test('a replacement run wins and an obsolete finally cannot clear its loading or owner', () => {
  const lifecycle = createExecutionLifecycle();
  const owner = { id: 'node-1' };
  const first = begin(lifecycle, executionContext(owner));
  const secondContext = executionContext(owner, { inputFingerprint: 'sha256:input-b' });
  const second = begin(lifecycle, secondContext);

  assert.equal(commit(lifecycle, first, {
    context: executionContext(owner),
    result: { marker: 'old' },
    evidence: { evidence_grade: 'current-computation' },
  }), false);
  assert.equal(release(lifecycle, first), false, 'an obsolete finally is a no-op');

  const runtime = inspectExecutionLifecycle(lifecycle);
  assert.equal(runtime.loading, true);
  assert.equal(runtime.currentTicket, second);
  assert.equal(runtime.owner, owner);
  assert.equal(runtime.state, 'running');
});

test('delayed completion fails closed on owner, epoch, fingerprint, or endpoint drift', () => {
  const driftCases = [
    context => ({ ...context, owner: { id: context.owner.id } }),
    context => ({ ...context, workspaceEpoch: 'workspace-8' }),
    context => ({ ...context, inputFingerprint: 'sha256:input-b' }),
    context => ({ ...context, endpoint: '/api/v1/parameter_scan_2d' }),
  ];

  for (const drift of driftCases) {
    const lifecycle = createExecutionLifecycle();
    const context = executionContext({ id: 'node-1' });
    const ticket = begin(lifecycle, context);
    assert.equal(commit(lifecycle, ticket, {
      context: drift(context),
      result: { marker: 'must-not-publish' },
      evidence: { evidence_grade: 'current-computation' },
      sessionId: 'must-not-publish',
    }), false);

    const runtime = inspectExecutionLifecycle(lifecycle);
    assert.equal(runtime.state, 'invalidated');
    assert.equal(runtime.freshness, 'invalidated');
    assert.equal(runtime.loading, false);
    assert.equal(runtime.result, null);
    assert.equal(runtime.sessionId, null);
    assert.equal(runtime.currentTicket, null);
  }
});

test('commit, fail, block, and invalidate implement guarded terminal transitions', () => {
  const owner = { id: 'node-1' };
  const context = executionContext(owner);

  const committed = createExecutionLifecycle();
  const commitTicket = begin(committed, context);
  assert.equal(commit(committed, commitTicket, {
    context,
    result: { marker: 'fresh' },
    evidence: { evidence_grade: 'screened_proxy' },
    sessionId: 'live-session',
  }), true);
  assert.deepEqual(serializeExecutionLifecycle(committed), {
    state: 'current',
    freshness: 'current',
    evidence: { evidence_grade: 'screened_proxy' },
  });
  assert.deepEqual(readCurrentResult(committed), { marker: 'fresh' });
  assert.equal(inspectExecutionLifecycle(committed).sessionId, 'live-session');

  const failed = createExecutionLifecycle();
  const failTicket = begin(failed, context);
  assert.equal(fail(failed, failTicket, {
    context,
    error: new Error('solver did not converge'),
    result: { valid: [true, false], partial: true },
    evidence: { evidence_grade: 'partial-numerical-evidence' },
  }), true);
  assert.deepEqual(serializeExecutionLifecycle(failed), {
    state: 'failed',
    freshness: 'current',
    evidence: { evidence_grade: 'partial-numerical-evidence' },
  });
  assert.equal(readCurrentResult(failed), null, 'a failed diagnostic is not a reusable current result');
  assert.deepEqual(inspectExecutionLifecycle(failed).result, {
    valid: [true, false], partial: true,
  });

  const blocked = createExecutionLifecycle();
  const blockTicket = begin(blocked, context);
  assert.equal(block(blocked, blockTicket, { context, reason: 'missing model input' }), true);
  assert.deepEqual(serializeExecutionLifecycle(blocked), {
    state: 'blocked', freshness: 'empty', evidence: null,
  });

  assert.equal(invalidate(committed, {
    owner,
    workspaceEpoch: context.workspaceEpoch,
    reason: 'upstream input changed',
  }), true);
  assert.deepEqual(serializeExecutionLifecycle(committed), {
    state: 'invalidated',
    freshness: 'invalidated',
    evidence: { evidence_grade: 'screened_proxy' },
  });
  assert.equal(readCurrentResult(committed), null);
  assert.equal(inspectExecutionLifecycle(committed).sessionId, null);
});

test('freshness and scientific evidence are independent axes', () => {
  const lifecycle = createExecutionLifecycle();
  const owner = { id: 'node-1' };
  const context = executionContext(owner);
  const ticket = begin(lifecycle, context);
  commit(lifecycle, ticket, {
    context,
    result: { source: 'retrieval' },
    evidence: { evidence_grade: 'screened_proxy', certificate_grade: 'proxy-only' },
  });

  assert.deepEqual(serializeExecutionLifecycle(lifecycle), {
    state: 'current',
    freshness: 'current',
    evidence: { evidence_grade: 'screened_proxy', certificate_grade: 'proxy-only' },
  }, 'a freshly obtained proxy must not be upgraded to verified evidence');

  const savedEvidence = {
    evidence_grade: 'exact-window-siso-rop-path',
    certificate_grade: 'enumeration-relative',
  };
  restoreHistorical(lifecycle, {
    context: executionContext(owner, { workspaceEpoch: 'workspace-9' }),
    result: { marker: 'saved' },
    evidence: savedEvidence,
  });
  assert.deepEqual(serializeExecutionLifecycle(lifecycle), {
    state: 'historical',
    freshness: 'historical',
    evidence: savedEvidence,
  }, 'restore changes freshness without rewriting the saved evidence grade');
});

test('restore strips every saved session identifier and never publishes historical data as current', () => {
  const lifecycle = createExecutionLifecycle();
  const owner = { id: 'node-1' };
  const saved = {
    sessionId: 'camel-session',
    model: {
      session_id: 'snake-session',
      payload: [{ sessionId: 'nested-session', value: 3 }],
    },
  };

  assert.equal(restoreHistorical(lifecycle, {
    context: executionContext(owner),
    result: saved,
    evidence: { evidence_grade: 'current-computation' },
    sessionId: 'top-level-runtime-session',
  }), true);

  const runtime = inspectExecutionLifecycle(lifecycle);
  assert.equal(runtime.state, 'historical');
  assert.equal(runtime.freshness, 'historical');
  assert.equal(runtime.sessionId, null);
  assert.deepEqual(runtime.result, { model: { payload: [{ value: 3 }] } });
  assert.equal(readCurrentResult(lifecycle), null);
  assert.equal(readReusableArtifact(lifecycle), null);
  assert.equal(saved.sessionId, 'camel-session', 'restore must not mutate the serialized source');
  assert.equal(saved.model.session_id, 'snake-session');
});

test('only an allow-listed immutable artifact with a verified SHA-256 hash is reusable', () => {
  const validHash = 'a'.repeat(64);
  const calls = [];
  const lifecycle = createExecutionLifecycle({
    immutableArtifactKinds: ['network-ir'],
    verifyArtifactHash({ kind, expectedHash, value }) {
      calls.push({ kind, expectedHash, value });
      return kind === 'network-ir' && expectedHash === validHash && value.marker === 'frozen';
    },
  });
  const owner = { id: 'node-1' };
  const context = executionContext(owner);
  const artifactValue = { marker: 'frozen', nested: { value: 1 } };

  restoreHistorical(lifecycle, {
    context,
    result: { marker: 'historical-result' },
    evidence: { evidence_grade: 'current-computation' },
    artifact: { kind: 'network-ir', hash: validHash, value: artifactValue },
  });

  assert.equal(calls.length, 1);
  assert.deepEqual(readReusableArtifact(lifecycle), artifactValue);
  assert.equal(Object.isFrozen(readReusableArtifact(lifecycle)), true);
  assert.equal(Object.isFrozen(readReusableArtifact(lifecycle).nested), true);
  assert.equal(inspectExecutionLifecycle(lifecycle).artifactReuse.reusable, true);
  assert.equal(serializeExecutionLifecycle(lifecycle).freshness, 'historical');

  artifactValue.nested.value = 99;
  assert.equal(readReusableArtifact(lifecycle).nested.value, 1, 'verified bytes are snapshotted');
});

test('artifact reuse fails closed for a missing allow-list, bad hash, failed verifier, or session data', () => {
  const validHash = 'b'.repeat(64);
  const cases = [
    {
      options: { immutableArtifactKinds: [], verifyArtifactHash: () => true },
      artifact: { kind: 'network-ir', hash: validHash, value: { marker: 'saved' } },
      reason: 'kind-not-allowlisted',
    },
    {
      options: { immutableArtifactKinds: ['network-ir'], verifyArtifactHash: () => true },
      artifact: { kind: 'network-ir', hash: 'not-a-sha256', value: { marker: 'saved' } },
      reason: 'invalid-hash',
    },
    {
      options: { immutableArtifactKinds: ['network-ir'], verifyArtifactHash: () => false },
      artifact: { kind: 'network-ir', hash: validHash, value: { marker: 'saved' } },
      reason: 'hash-verification-failed',
    },
    {
      options: { immutableArtifactKinds: ['network-ir'] },
      artifact: { kind: 'network-ir', hash: validHash, value: { marker: 'saved' } },
      reason: 'hash-verifier-unavailable',
    },
    {
      options: { immutableArtifactKinds: ['network-ir'], verifyArtifactHash: () => true },
      artifact: {
        kind: 'network-ir', hash: validHash, value: { marker: 'saved', session_id: 'ephemeral' },
      },
      reason: 'contains-session-id',
    },
  ];

  for (const entry of cases) {
    const lifecycle = createExecutionLifecycle(entry.options);
    restoreHistorical(lifecycle, {
      context: executionContext({ id: 'node-1' }),
      result: { marker: 'saved' },
      evidence: { evidence_grade: 'current-computation' },
      artifact: entry.artifact,
    });
    assert.equal(readReusableArtifact(lifecycle), null);
    assert.equal(inspectExecutionLifecycle(lifecycle).artifactReuse.reason, entry.reason);
  }
});

test('malformed contexts and unguarded completion calls fail closed', () => {
  const lifecycle = createExecutionLifecycle();
  const owner = { id: 'node-1' };
  assert.throws(
    () => begin(lifecycle, executionContext(owner, { inputFingerprint: '' })),
    /inputFingerprint/,
  );
  const ticket = begin(lifecycle, executionContext(owner));
  assert.throws(
    () => commit(lifecycle, ticket, { result: { marker: 'unguarded' } }),
    /current context/,
  );
  assert.equal(inspectExecutionLifecycle(lifecycle).state, 'running');
});

console.log(`\nAll ${passed} execution lifecycle core contract tests passed.`);
