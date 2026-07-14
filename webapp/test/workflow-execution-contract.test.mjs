import assert from 'node:assert/strict';

import {
  blockedOutcome,
  failedOutcome,
  succeededOutcome,
} from '../public/js/execution-outcome.js';
import {
  connectedComponentNodeIds,
  planWorkflowExecution,
  runWorkflowPlan,
} from '../public/js/workflow-execution.js';

let passed = 0;
async function test(name, fn) {
  await fn();
  passed += 1;
  console.log(`  ok - ${name}`);
}

await test('selected component scope excludes independent connected branches', () => {
  const edges = [
    { fromNode: 'a', toNode: 'b' },
    { fromNode: 'x', toNode: 'y' },
  ];
  assert.deepEqual(connectedComponentNodeIds('b', edges), ['a', 'b']);
  assert.deepEqual(connectedComponentNodeIds('x', edges), ['x', 'y']);
});

await test('cycle planning fails before any node can execute', async () => {
  let calls = 0;
  const registry = { a: { type: 'compute' }, b: { type: 'compute' } };
  const nodeTypes = {
    compute: {
      execution: { mode: 'execute' },
      async execute(nodeId) { calls += 1; return succeededOutcome(nodeId); },
    },
  };
  const plan = planWorkflowExecution({
    nodeIds: ['a', 'b'],
    connections: [{ fromNode: 'a', toNode: 'b' }, { fromNode: 'b', toNode: 'a' }],
    registry,
  });
  assert.equal(plan.ok, false);
  assert.equal(plan.code, 'dependency_cycle');
  const report = await runWorkflowPlan(plan, { registry, nodeTypes });
  assert.equal(report.status, 'blocked');
  assert.equal(calls, 0);
  assert.equal(report.summary.executed, 0);
});

await test('failed and blocked nodes skip descendants while an independent branch continues serially', async () => {
  const calls = [];
  let active = 0;
  let maxActive = 0;
  const registry = {
    sourceA: { type: 'source' },
    fail: { type: 'fail' },
    skipped: { type: 'ok' },
    sourceB: { type: 'source' },
    independent: { type: 'ok' },
  };
  const execute = async (nodeId, result) => {
    active += 1;
    maxActive = Math.max(maxActive, active);
    calls.push(nodeId);
    await Promise.resolve();
    active -= 1;
    return result;
  };
  const nodeTypes = {
    source: { execution: { mode: 'none' } },
    fail: {
      execution: { mode: 'execute' },
      execute: nodeId => execute(nodeId, failedOutcome(nodeId, { code: 'boom' })),
    },
    ok: {
      execution: { mode: 'execute' },
      execute: nodeId => execute(nodeId, succeededOutcome(nodeId)),
    },
  };
  const plan = planWorkflowExecution({
    nodeIds: Object.keys(registry),
    connections: [
      { fromNode: 'sourceA', toNode: 'fail' },
      { fromNode: 'fail', toNode: 'skipped' },
      { fromNode: 'sourceB', toNode: 'independent' },
    ],
    registry,
  });
  const report = await runWorkflowPlan(plan, { registry, nodeTypes });
  assert.equal(report.status, 'failed');
  assert.deepEqual(calls, ['fail', 'independent']);
  assert.equal(maxActive, 1);
  assert.equal(report.outcomes.skipped.status, 'blocked');
  assert.equal(report.summary.executed, 1);
  assert.equal(report.summary.failed, 1);
  assert.equal(report.summary.blocked, 1);
});

await test('a structured blocked result propagates without becoming success', async () => {
  let childCalls = 0;
  const registry = { gate: { type: 'gate' }, child: { type: 'child' } };
  const nodeTypes = {
    gate: {
      execution: { mode: 'execute' },
      execute: nodeId => blockedOutcome(nodeId, { code: 'manual_selection_required' }),
    },
    child: {
      execution: { mode: 'execute' },
      execute: nodeId => { childCalls += 1; return succeededOutcome(nodeId); },
    },
  };
  const plan = planWorkflowExecution({
    nodeIds: ['gate', 'child'],
    connections: [{ fromNode: 'gate', toNode: 'child' }],
    registry,
  });
  const report = await runWorkflowPlan(plan, { registry, nodeTypes });
  assert.equal(report.status, 'blocked');
  assert.equal(report.outcomes.gate.status, 'blocked');
  assert.equal(report.outcomes.child.status, 'blocked');
  assert.equal(childCalls, 0);
});

await test('undefined execution results become contract failures and block descendants', async () => {
  let childCalls = 0;
  const registry = { bad: { type: 'bad' }, child: { type: 'child' } };
  const nodeTypes = {
    bad: { execution: { mode: 'execute' }, execute: async () => undefined },
    child: {
      execution: { mode: 'execute' },
      execute: nodeId => { childCalls += 1; return succeededOutcome(nodeId); },
    },
  };
  const plan = planWorkflowExecution({
    nodeIds: ['bad', 'child'],
    connections: [{ fromNode: 'bad', toNode: 'child' }],
    registry,
  });
  const report = await runWorkflowPlan(plan, { registry, nodeTypes });
  assert.equal(report.outcomes.bad.status, 'failed');
  assert.equal(report.outcomes.bad.code, 'execution_contract_violation');
  assert.equal(report.outcomes.child.status, 'blocked');
  assert.equal(childCalls, 0);
});

console.log(`\nAll ${passed} workflow execution contract tests passed.`);
