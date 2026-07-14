import assert from 'node:assert/strict';
import test from 'node:test';

import { UndoStack } from '../public/js/commands.js';
import {
  GraphPatchCommand,
  planAgentAutoSpawnWorkflow,
  planDesignBuildAndTuneWorkflow,
  planDesignSpecExportWorkflow,
  planQuickAddWorkflow,
} from '../public/js/graph-patch.js';

const clone = value => structuredClone(value);

function deepFreeze(value) {
  if (!value || typeof value !== 'object' || Object.isFrozen(value)) return value;
  Object.freeze(value);
  for (const child of Object.values(value)) deepFreeze(child);
  return value;
}

function graph(nodes = [], connections = [], extra = {}) {
  return { ...extra, nodes, connections };
}

function node(id, type, x = 0, y = 0, width = 280, height = 220) {
  return { id, type, x, y, width, height, data: {} };
}

function connection(fromNode, fromPort, toNode, toPort) {
  return { fromNode, fromPort, toNode, toPort };
}

function makeAdapter(initialSnapshot, options = {}) {
  let live = clone(initialSnapshot);
  let commitCount = 0;
  let restoreCount = 0;
  let discardCount = 0;
  let stageNodeCount = 0;
  let stageConnectionCount = 0;

  const adapter = {
    captureSnapshot() {
      if (options.captureError) throw options.captureError;
      return clone(live);
    },
    stageNode(spec, context) {
      stageNodeCount += 1;
      options.onStageNode?.(spec, context, stageNodeCount);
      return { kind: 'staged-node', spec: clone(spec) };
    },
    stageConnection(spec, context) {
      stageConnectionCount += 1;
      options.onStageConnection?.(spec, context, stageConnectionCount);
      return { kind: 'staged-connection', spec: clone(spec) };
    },
    commit(transaction, context) {
      commitCount += 1;
      options.onCommit?.(transaction, context, commitCount);
      live = clone(transaction.nextSnapshot);
      if (options.commitError) throw options.commitError;
    },
    restoreSnapshot(snapshot, context) {
      restoreCount += 1;
      live = clone(snapshot);
      if (options.onRestore) options.onRestore(snapshot, context, restoreCount);
    },
    discardStage(transaction, context) {
      discardCount += 1;
      options.onDiscard?.(transaction, context, discardCount);
    },
  };

  return {
    adapter,
    snapshot: () => clone(live),
    replaceSnapshot: snapshot => { live = clone(snapshot); },
    counts: () => ({
      commit: commitCount,
      restore: restoreCount,
      discard: discardCount,
      stageNode: stageNodeCount,
      stageConnection: stageConnectionCount,
    }),
  };
}

test('Quick Add planning is pure, deterministic, and allocates explicit stable node IDs', () => {
  const before = deepFreeze(graph([
    node('node-4', 'markdown-note', 20, 20),
  ], [], { workspaceRevision: 7 }));

  const options = deepFreeze({
    chainType: 'siso-analysis',
    graph: before,
    nextNodeOrdinal: 11,
    anchor: { x: 80, y: 150 },
  });
  const first = planQuickAddWorkflow(options);
  const second = planQuickAddWorkflow(options);

  assert.deepEqual(first, second);
  assert.equal(first.ok, true);
  assert.deepEqual(first.patch.nodes.map(item => item.id), [
    'node-11', 'node-12', 'node-13', 'node-14',
  ]);
  assert.deepEqual(first.patch.nodes.map(item => item.type), [
    'reaction-network', 'model-builder', 'siso-params', 'siso-result',
  ]);
  assert.deepEqual(first.patch.connections, [
    connection('node-11', 'reactions', 'node-12', 'reactions'),
    connection('node-12', 'model', 'node-13', 'model'),
    connection('node-13', 'params', 'node-14', 'params'),
  ]);
  assert.deepEqual(before, graph([
    node('node-4', 'markdown-note', 20, 20),
  ], [], { workspaceRevision: 7 }));
});

test('Quick Add reuses a sole SBML source and its connected builder', () => {
  const before = graph([
    node('sbml-source', 'sbml-import', 30, 40),
    node('existing-builder', 'model-builder', 380, 40),
  ], [
    connection('sbml-source', 'reactions', 'existing-builder', 'reactions'),
  ]);

  const result = planQuickAddWorkflow({
    chainType: 'parameter-scan-1d',
    graph: before,
    nextNodeOrdinal: 20,
  });

  assert.equal(result.ok, true);
  assert.equal(result.patch.metadata.sourceNodeId, 'sbml-source');
  assert.equal(result.patch.metadata.modelBuilderNodeId, 'existing-builder');
  assert.deepEqual(result.patch.metadata.reusedNodeIds, ['sbml-source', 'existing-builder']);
  assert.deepEqual(result.patch.nodes.map(item => [item.id, item.type]), [
    ['node-20', 'scan-1d-params'],
    ['node-21', 'scan-1d-result'],
  ]);
  assert.deepEqual(result.patch.connections, [
    connection('existing-builder', 'model', 'node-20', 'model'),
    connection('node-20', 'params', 'node-21', 'params'),
  ]);
});

test('multiple compatible sources fail closed with a manual-selection diagnostic', () => {
  const before = deepFreeze(graph([
    node('rn-a', 'reaction-network'),
    node('sbml-b', 'sbml-import'),
    node('note', 'markdown-note'),
  ]));

  const ambiguous = planQuickAddWorkflow({
    chainType: 'rop-cloud',
    graph: before,
    nextNodeOrdinal: 30,
  });

  assert.equal(ambiguous.ok, false);
  assert.equal(ambiguous.patch, null);
  assert.equal(ambiguous.diagnostic.kind, 'manual-selection');
  assert.equal(ambiguous.diagnostic.code, 'manual-source-selection-required');
  assert.equal(ambiguous.diagnostic.reason, 'multiple-compatible-sources');
  assert.deepEqual(ambiguous.diagnostic.candidateNodeIds, ['rn-a', 'sbml-b']);

  const selected = planQuickAddWorkflow({
    chainType: 'rop-cloud',
    graph: before,
    nextNodeOrdinal: 30,
    selectedSourceId: 'sbml-b',
  });
  assert.equal(selected.ok, true);
  assert.equal(selected.patch.metadata.sourceNodeId, 'sbml-b');
  assert.equal(selected.patch.nodes.some(item => item.type === 'reaction-network'), false);
  assert.equal(
    selected.patch.connections.some(item =>
      item.fromNode === 'sbml-b' && item.toPort === 'reactions'),
    true,
  );
});

test('multiple compatible sources may only be bypassed by an explicit isolated source', () => {
  const before = deepFreeze(graph([
    node('rn-a', 'reaction-network', 10, 20),
    node('sbml-b', 'sbml-import', 400, 20),
  ]));

  const isolated = planQuickAddWorkflow({
    chainType: 'siso-analysis',
    graph: before,
    nextNodeOrdinal: 70,
    createIsolatedSource: true,
  });

  assert.equal(isolated.ok, true);
  assert.equal(isolated.patch.metadata.sourceNodeId, 'node-70');
  assert.equal(isolated.patch.metadata.sourceWasCreated, true);
  assert.equal(isolated.patch.metadata.isolatedSourceCreated, true);
  assert.deepEqual(isolated.patch.metadata.reusedNodeIds, []);
  assert.deepEqual(isolated.patch.nodes.slice(0, 2).map(item => [item.id, item.type]), [
    ['node-70', 'reaction-network'],
    ['node-71', 'model-builder'],
  ]);
  assert.equal(
    isolated.patch.connections.some(item => item.fromNode === 'rn-a' || item.fromNode === 'sbml-b'),
    false,
  );

  const conflicting = planQuickAddWorkflow({
    chainType: 'siso-analysis',
    graph: before,
    nextNodeOrdinal: 70,
    createIsolatedSource: true,
    selectedSourceId: 'rn-a',
  });
  assert.equal(conflicting.ok, false);
  assert.equal(conflicting.diagnostic.code, 'conflicting-source-selection');
});

test('atlas Quick Add workflows also return complete explicit graph patches', () => {
  const result = planQuickAddWorkflow({
    chainType: 'atlas-search',
    graph: graph(),
    nextNodeOrdinal: 5,
  });

  assert.equal(result.ok, true);
  assert.deepEqual(result.patch.nodes.map(item => [item.id, item.type]), [
    ['node-5', 'atlas-spec'],
    ['node-6', 'atlas-builder'],
    ['node-7', 'atlas-query-config'],
    ['node-8', 'atlas-query-result'],
  ]);
  assert.deepEqual(result.patch.connections, [
    connection('node-5', 'atlas-spec', 'node-6', 'atlas-spec'),
    connection('node-6', 'atlas', 'node-8', 'atlas'),
    connection('node-7', 'atlas-query', 'node-8', 'atlas-query'),
  ]);
});

test('Design Build & Tune planning is pure and emits one explicit active-node patch', () => {
  const before = deepFreeze(graph([
    node('design', 'design-target', 100, 80, 460, 500),
    node('note', 'markdown-note', 700, 80, 200, 100),
  ]));
  const options = deepFreeze({
    graph: before,
    nextNodeOrdinal: 20,
    designNodeId: 'design',
  });

  const first = planDesignBuildAndTuneWorkflow(options);
  const second = planDesignBuildAndTuneWorkflow(options);
  assert.deepEqual(first, second);
  assert.equal(first.ok, true);
  assert.deepEqual(first.patch.nodes.map(spec => [spec.id, spec.type]), [
    ['node-20', 'model-builder'],
    ['node-21', 'placer-params'],
    ['node-22', 'placer-result'],
  ]);
  assert.deepEqual(first.patch.connections, [
    connection('design', 'reactions', 'node-20', 'reactions'),
    connection('node-20', 'model', 'node-21', 'model'),
    connection('node-21', 'params', 'node-22', 'params'),
  ]);
  assert.deepEqual(before, graph([
    node('design', 'design-target', 100, 80, 460, 500),
    node('note', 'markdown-note', 700, 80, 200, 100),
  ]));
});

test('Design Spec export planning is pure, stable, and emits the strict active-node chain', () => {
  const spec = deepFreeze({
    schema_version: 'bne-designability/v1.0.0',
    target: { legacy_target: { target_kind: 'sign', target: ['+', '-'] } },
    constraints: {},
  });
  const before = deepFreeze(graph([
    node('note', 'markdown-note', 20, 500, 200, 100),
  ]));
  const options = deepFreeze({
    graph: before,
    nextNodeOrdinal: 40,
    anchor: { x: 90, y: 100 },
    spec,
  });

  const first = planDesignSpecExportWorkflow(options);
  const second = planDesignSpecExportWorkflow(options);

  assert.deepEqual(first, second);
  assert.equal(first.ok, true);
  assert.deepEqual(first.patch.nodes.map(nodeSpec => [
    nodeSpec.id, nodeSpec.type, nodeSpec.x, nodeSpec.y,
  ]), [
    ['node-40', 'design-spec-config', 90, 100],
    ['node-41', 'design-target', 590, 100],
  ]);
  assert.deepEqual(first.patch.nodes[0].initialization, {
    kind: 'design-spec-config',
    spec,
  });
  assert.deepEqual(first.patch.connections, [
    connection('node-40', 'designability-spec', 'node-41', 'designability-spec'),
  ]);
  assert.deepEqual(first.patch.metadata, {
    workflowType: 'design-spec-export',
    createdNodeIds: ['node-40', 'node-41'],
    specNodeId: 'node-40',
    designTargetNodeId: 'node-41',
    nextNodeOrdinal: 42,
  });
  assert.deepEqual(before, graph([
    node('note', 'markdown-note', 20, 500, 200, 100),
  ]));

  const unsupported = planDesignSpecExportWorkflow({
    graph: before,
    spec: { schema_version: 'bne-designability/v9.0.0' },
  });
  assert.equal(unsupported.ok, false);
  assert.equal(unsupported.patch, null);
  assert.equal(unsupported.diagnostic.code, 'unsupported-design-spec-version');
});

test('Quick Add layout uses the rendered viewer minimum width', () => {
  const result = planQuickAddWorkflow({
    chainType: 'siso-analysis',
    graph: graph(),
    nextNodeOrdinal: 1,
    anchor: { x: 80, y: 150 },
  });
  assert.equal(result.ok, true);
  const params = result.patch.nodes.find(item => item.type === 'siso-params');
  const output = result.patch.nodes.find(item => item.type === 'siso-result');
  assert.ok(params);
  assert.ok(output);
  assert.equal(
    output.x - (params.x + 380),
    60,
    'the planner must reserve CSS .node.viewer min-width plus the declared gap',
  );
});

test('Agent auto-spawn planning validates the whole response and never emits legacy node types', () => {
  const result = {
    networks: [{
      name: 'network A',
      reactions: [
        { rule: 'E + S <-> C_ES', kd: 0.001 },
        { rule: 'E + P <-> C_EP', kd: 0.002 },
      ],
      recommended_analyses: [
        { name: 'model-summary' },
        { name: 'siso-analysis' },
        { name: 'rop-cloud', output_expression: 'C_ES' },
      ],
    }],
  };
  const before = deepFreeze(graph([
    node('agent', 'ai-import', 10, 20, 340, 600),
  ]));
  const plan = planAgentAutoSpawnWorkflow({
    graph: before,
    nextNodeOrdinal: 30,
    agentNodeId: 'agent',
    result: deepFreeze(result),
  });

  assert.equal(plan.ok, true);
  assert.deepEqual(plan.patch.nodes.map(spec => spec.type), [
    'reaction-network',
    'model-builder',
    'model-summary',
    'siso-params',
    'siso-result',
    'rop-cloud-params',
    'rop-cloud-result',
  ]);
  assert.equal(plan.patch.nodes.some(spec => spec.type === 'siso-analysis'), false);
  assert.deepEqual(plan.patch.nodes[0].initialization, {
    kind: 'reaction-network-rules',
    reactions: result.networks[0].reactions,
  });
  assert.equal(plan.patch.metadata.modelBuilderNodeIds.length, 1);
  assert.equal(plan.patch.effects.filter(effect => effect.kind === 'node-auto-build-if-needed').length, 1);
  assert.equal(plan.patch.effects.filter(effect => effect.kind === 'node-execute-if-ready').length, 2);
  assert.deepEqual(before, graph([node('agent', 'ai-import', 10, 20, 340, 600)]));

  const invalid = planAgentAutoSpawnWorkflow({
    graph: before,
    nextNodeOrdinal: 30,
    agentNodeId: 'agent',
    result: {
      networks: [{
        name: 'bad',
        reactions: [{ rule: 'E + S <-> C_ES', kd: 0.001 }],
        recommended_analyses: [{ name: 'unknown-analysis' }],
      }],
    },
  });
  assert.equal(invalid.ok, false);
  assert.equal(invalid.patch, null);
  assert.equal(invalid.diagnostic.code, 'unsupported-agent-analysis');
});

test('GraphPatchCommand validates the projected graph, stages every addition, then commits once', () => {
  const events = [];
  const initial = graph([node('source', 'reaction-network')]);
  const patch = {
    nodes: [
      node('builder', 'model-builder', 300, 0),
      node('params', 'siso-params', 600, 0),
    ],
    connections: [
      connection('source', 'reactions', 'builder', 'reactions'),
      connection('builder', 'model', 'params', 'model'),
    ],
  };
  const harness = makeAdapter(initial, {
    onStageNode(spec) { events.push(`stage-node:${spec.id}`); },
    onStageConnection(spec) { events.push(`stage-connection:${spec.toNode}`); },
    onCommit() { events.push('commit'); },
  });
  const command = new GraphPatchCommand({
    patch,
    adapter: harness.adapter,
    validators: {
      snapshot() { events.push('validate-snapshot'); },
      node(spec, { isNew }) { events.push(`validate-node:${spec.id}:${isNew}`); },
      connection(spec, { isNew }) { events.push(`validate-connection:${spec.toNode}:${isNew}`); },
      graph() { events.push('validate-graph'); },
    },
  });

  command.apply();

  assert.equal(command.applied, true);
  assert.deepEqual(harness.counts(), {
    commit: 1, restore: 0, discard: 0, stageNode: 2, stageConnection: 2,
  });
  assert.deepEqual(harness.snapshot(), graph([
    node('source', 'reaction-network'),
    node('builder', 'model-builder', 300, 0),
    node('params', 'siso-params', 600, 0),
  ], patch.connections));
  assert.equal(events.at(-1), 'commit');
  assert.ok(events.indexOf('validate-graph') < events.indexOf('stage-node:builder'));
  assert.ok(events.indexOf('stage-connection:params') < events.indexOf('commit'));
  assert.ok(events.includes('validate-node:source:false'));
  assert.ok(events.includes('validate-node:builder:true'));
  assert.ok(events.includes('validate-connection:builder:true'));
});

test('node, connection, staging, and commit failures preserve the exact pre-apply snapshot', () => {
  const initial = graph([
    node('source', 'reaction-network'),
  ], [], { shellSnapshot: 'before' });
  const patch = {
    nodes: [node('builder', 'model-builder')],
    connections: [connection('source', 'reactions', 'builder', 'reactions')],
  };

  const nodeFailure = makeAdapter(initial);
  assert.throws(() => new GraphPatchCommand({
    patch,
    adapter: nodeFailure.adapter,
    validators: { node: spec => spec.id === 'builder' ? false : true },
  }).apply(), /node validator/i);
  assert.deepEqual(nodeFailure.snapshot(), initial);
  assert.equal(nodeFailure.counts().commit, 0);
  assert.equal(nodeFailure.counts().restore, 0);

  const connectionFailure = makeAdapter(initial);
  assert.throws(() => new GraphPatchCommand({
    patch,
    adapter: connectionFailure.adapter,
    validators: { connection: () => ({ ok: false, message: 'port mismatch' }) },
  }).apply(), /port mismatch/);
  assert.deepEqual(connectionFailure.snapshot(), initial);
  assert.equal(connectionFailure.counts().commit, 0);
  assert.equal(connectionFailure.counts().restore, 0);

  const stageFailure = makeAdapter(initial, {
    onStageConnection() { throw new Error('synthetic staging failure'); },
  });
  assert.throws(() => new GraphPatchCommand({
    patch,
    adapter: stageFailure.adapter,
  }).apply(), /synthetic staging failure/);
  assert.deepEqual(stageFailure.snapshot(), initial);
  assert.equal(stageFailure.counts().commit, 0);
  assert.equal(stageFailure.counts().discard, 1);
  assert.equal(stageFailure.counts().restore, 0);

  const commitFailure = makeAdapter(initial, {
    commitError: new Error('synthetic partial commit failure'),
  });
  const failedCommand = new GraphPatchCommand({ patch, adapter: commitFailure.adapter });
  assert.throws(() => failedCommand.apply(), /synthetic partial commit failure/);
  assert.deepEqual(commitFailure.snapshot(), initial);
  assert.equal(commitFailure.counts().commit, 1);
  assert.equal(commitFailure.counts().restore, 1);
  assert.equal(failedCommand.applied, false);

  const validatorMutation = makeAdapter(initial);
  assert.throws(() => new GraphPatchCommand({
    patch,
    adapter: validatorMutation.adapter,
    validators: {
      node(spec) {
        if (spec.id !== 'builder') return true;
        validatorMutation.replaceSnapshot(graph([], [], { shellSnapshot: 'corrupted' }));
        throw new Error('validator failed after accidental mutation');
      },
    },
  }).apply(), /validator failed after accidental mutation/);
  assert.deepEqual(validatorMutation.snapshot(), initial);
});

test('a failed revert rolls forward to its exact pre-revert snapshot and remains retryable', () => {
  const initial = graph([node('source', 'reaction-network')]);
  let failNextRestore = true;
  const harness = makeAdapter(initial, {
    onRestore(_snapshot, _context, restoreCount) {
      if (failNextRestore && restoreCount === 1) {
        throw new Error('synthetic revert failure after mutation');
      }
    },
  });
  const command = new GraphPatchCommand({
    patch: { nodes: [node('added', 'model-builder')], connections: [] },
    adapter: harness.adapter,
  });
  command.apply();
  const appliedSnapshot = harness.snapshot();

  assert.throws(() => command.revert(), /synthetic revert failure/);
  assert.deepEqual(harness.snapshot(), appliedSnapshot);
  assert.equal(command.applied, true);

  failNextRestore = false;
  command.revert();
  assert.deepEqual(harness.snapshot(), initial);
  assert.equal(command.applied, false);
});

test('a silent partial revert is detected and rolled forward before reporting failure', () => {
  const initial = graph([node('source', 'reaction-network')]);
  let live = clone(initial);
  let restoreCount = 0;
  const adapter = {
    captureSnapshot: () => clone(live),
    stageNode: spec => clone(spec),
    stageConnection: spec => clone(spec),
    commit(transaction) { live = clone(transaction.nextSnapshot); },
    restoreSnapshot(snapshot) {
      restoreCount += 1;
      live = restoreCount === 1 ? graph([], [], { partial: true }) : clone(snapshot);
    },
  };
  const command = new GraphPatchCommand({
    patch: { nodes: [node('added', 'model-builder')], connections: [] },
    adapter,
  });
  command.apply();
  const appliedSnapshot = clone(live);

  assert.throws(() => command.revert(), /restore.*snapshot|snapshot.*restore/i);
  assert.deepEqual(live, appliedSnapshot);
  assert.equal(command.applied, true);
});

test('the command owns an immutable copy of explicit IDs across its lifetime', () => {
  const harness = makeAdapter(graph());
  const sourcePatch = {
    nodes: [node('stable-id', 'reaction-network')],
    connections: [],
  };
  const command = new GraphPatchCommand({ patch: sourcePatch, adapter: harness.adapter });

  sourcePatch.nodes[0].id = 'caller-mutated';
  assert.equal(command.patch.nodes[0].id, 'stable-id');
  assert.throws(() => {
    command.patch.nodes[0].id = 'directly-mutated';
  }, TypeError);

  command.apply();
  command.revert();
  command.apply();
  assert.equal(harness.snapshot().nodes[0].id, 'stable-id');
});

test('one Undo removes the whole patch, Redo keeps IDs, and stale delayed callbacks cannot run', () => {
  const initial = graph([node('source', 'reaction-network')]);
  const plan = planQuickAddWorkflow({
    chainType: 'siso-analysis',
    graph: initial,
    nextNodeOrdinal: 50,
  });
  assert.equal(plan.ok, true);

  const scheduled = [];
  const scheduler = {
    setTimeout(callback, delay) {
      const handle = { callback, delay, cancelled: false };
      scheduled.push(handle);
      return handle;
    },
    clearTimeout(handle) { handle.cancelled = true; },
  };
  const effects = [];
  const cancellations = [];
  const harness = makeAdapter(initial);
  const command = new GraphPatchCommand({
    patch: plan.patch,
    adapter: harness.adapter,
    scheduler,
    deferredTasks: [{
      id: 'auto-build',
      delay: 100,
      run(context) {
        effects.push({ epoch: context.epoch, aborted: context.signal.aborted });
      },
    }],
    onEpochCancel(details) {
      cancellations.push(details.reason);
    },
  });
  const stack = new UndoStack();

  command.apply();
  stack.record(command);
  const createdIds = harness.snapshot().nodes
    .filter(item => item.id !== 'source')
    .map(item => item.id);
  const firstEpoch = command.epoch;
  assert.equal(stack.depth, 1);
  assert.equal(scheduled.length, 1);

  stack.undo();
  assert.deepEqual(harness.snapshot(), initial);
  assert.equal(command.epoch, null);
  assert.equal(scheduled[0].cancelled, true);
  assert.deepEqual(cancellations, ['graph-patch-reverted']);
  scheduled[0].callback(); // Simulate a timer that escaped clearTimeout.
  assert.deepEqual(effects, []);

  stack.redo();
  const redoneIds = harness.snapshot().nodes
    .filter(item => item.id !== 'source')
    .map(item => item.id);
  assert.deepEqual(redoneIds, createdIds);
  assert.notEqual(command.epoch, firstEpoch);
  assert.equal(scheduled.length, 2);

  scheduled[0].callback();
  assert.deepEqual(effects, []);
  scheduled[1].callback();
  assert.deepEqual(effects, [{ epoch: command.epoch, aborted: false }]);
});
