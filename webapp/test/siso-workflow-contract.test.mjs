import assert from 'node:assert/strict';

globalThis.window = {
  matchMedia: () => null,
  addEventListener() {},
  location: { protocol: 'http:', hostname: '127.0.0.1', port: '8000' },
  sessionStorage: { getItem() { return null; }, setItem() {} },
};
globalThis.document = {
  readyState: 'loading',
  documentElement: { dataset: {}, style: { setProperty() {} } },
  getElementById() { return null; },
  addEventListener() {},
  querySelectorAll() { return []; },
};
globalThis.HTMLSelectElement = class HTMLSelectElement {};

const { NODE_TYPES } = await import('../public/js/node-types/index.js');
const { succeededOutcome } = await import('../public/js/execution-outcome.js');
const {
  planWorkflowExecution,
  runWorkflowPlan,
} = await import('../public/js/workflow-execution.js');

let passed = 0;
async function test(name, callback) {
  await callback();
  passed += 1;
  console.log(`  ok - ${name}`);
}

function makePlan() {
  const registry = {
    siso: { type: 'siso-result' },
    qk: { type: 'qk-poly-result' },
  };
  const connections = [{
    fromNode: 'siso',
    fromPort: 'result',
    toNode: 'qk',
    toPort: 'result',
  }];
  return {
    registry,
    plan: planWorkflowExecution({ nodeIds: Object.keys(registry), connections, registry }),
  };
}

await test('SISO emits the exact PathResult artifact consumed by qK', () => {
  const sisoOutput = NODE_TYPES['siso-result'].outputs.find(port => port.port === 'result');
  const qkInput = NODE_TYPES['qk-poly-result'].inputs.find(port => port.port === 'result');
  assert.equal(sisoOutput.type, 'PathResult');
  assert.equal(qkInput.type, 'PathResult');
  assert.equal(NODE_TYPES['siso-result'].execution.mode, 'execute');
  assert.equal(NODE_TYPES['qk-poly-result'].execution.mode, 'execute');
});

await test('a completed SISO run without a selected path blocks qK', async () => {
  const { registry, plan } = makePlan();
  let qkCalls = 0;
  const nodeTypes = {
    ...NODE_TYPES,
    'siso-result': {
      ...NODE_TYPES['siso-result'],
      execute: nodeId => succeededOutcome(nodeId, {
        outputs: { result: 'missing' },
        code: 'siso_path_not_selected',
      }),
    },
    'qk-poly-result': {
      ...NODE_TYPES['qk-poly-result'],
      execute: nodeId => {
        qkCalls += 1;
        return succeededOutcome(nodeId);
      },
    },
  };
  const report = await runWorkflowPlan(plan, { registry, nodeTypes });
  assert.equal(report.status, 'blocked');
  assert.equal(report.outcomes.siso.status, 'succeeded');
  assert.equal(report.outcomes.siso.outputs.result, 'missing');
  assert.equal(report.outcomes.qk.status, 'blocked');
  assert.equal(report.outcomes.qk.code, 'upstream_output_missing');
  assert.equal(qkCalls, 0);
});

await test('a current selected SISO path allows qK to execute', async () => {
  const { registry, plan } = makePlan();
  let qkCalls = 0;
  const nodeTypes = {
    ...NODE_TYPES,
    'siso-result': {
      ...NODE_TYPES['siso-result'],
      execute: nodeId => succeededOutcome(nodeId, { outputs: { result: 'present' } }),
    },
    'qk-poly-result': {
      ...NODE_TYPES['qk-poly-result'],
      execute: nodeId => {
        qkCalls += 1;
        return succeededOutcome(nodeId);
      },
    },
  };
  const report = await runWorkflowPlan(plan, { registry, nodeTypes });
  assert.equal(report.status, 'succeeded');
  assert.equal(report.summary.executed, 2);
  assert.equal(qkCalls, 1);
});

await test('a restored historical SISO path never flows into qK', async () => {
  const { registry, plan } = makePlan();
  let qkCalls = 0;
  const nodeTypes = {
    ...NODE_TYPES,
    'siso-result': {
      ...NODE_TYPES['siso-result'],
      execute: nodeId => succeededOutcome(nodeId, {
        work: 'reused',
        outputs: { result: 'historical' },
      }),
    },
    'qk-poly-result': {
      ...NODE_TYPES['qk-poly-result'],
      execute: nodeId => {
        qkCalls += 1;
        return succeededOutcome(nodeId);
      },
    },
  };
  const report = await runWorkflowPlan(plan, { registry, nodeTypes });
  assert.equal(report.status, 'blocked');
  assert.equal(report.outcomes.qk.code, 'upstream_output_historical');
  assert.equal(report.summary.reused, 1);
  assert.equal(qkCalls, 0);
});

console.log(`\nAll ${passed} SISO workflow contract tests passed.`);
