import assert from 'node:assert/strict';

import {
  EXECUTION_OUTCOME_CONTRACT,
  EXECUTION_STATUSES,
  assertExecutionOutcome,
  blockedOutcome,
  cancelledOutcome,
  failedOutcome,
  staleOutcome,
  succeededOutcome,
} from '../public/js/execution-outcome.js';

let passed = 0;
function test(name, fn) {
  fn();
  passed += 1;
  console.log(`  ok - ${name}`);
}

test('ExecutionOutcome exposes exactly the five workflow states', () => {
  assert.deepEqual([...EXECUTION_STATUSES].sort(), [
    'blocked', 'cancelled', 'failed', 'stale', 'succeeded',
  ]);
  for (const outcome of [
    succeededOutcome('n1', { outputs: { result: 'present' } }),
    blockedOutcome('n1', { code: 'missing_input' }),
    failedOutcome('n1', { code: 'request_failed' }),
    cancelledOutcome('n1'),
    staleOutcome('n1'),
  ]) {
    assert.equal(outcome.contract, EXECUTION_OUTCOME_CONTRACT);
    assert.equal(assertExecutionOutcome(outcome, { nodeId: 'n1' }), outcome);
    assert.equal(Object.isFrozen(outcome), true);
  }
});

test('work and output availability are orthogonal to status', () => {
  const missing = succeededOutcome('siso', {
    work: 'executed',
    outputs: { result: 'missing' },
    code: 'siso_path_not_selected',
  });
  const reused = succeededOutcome('builder', {
    work: 'reused',
    outputs: { model: 'present' },
  });
  assert.equal(missing.status, 'succeeded');
  assert.equal(missing.outputs.result, 'missing');
  assert.equal(reused.work, 'reused');
});

test('undefined, null, false and malformed outcomes fail closed', () => {
  for (const value of [undefined, null, false, true, {}, { status: 'succeeded' }]) {
    assert.throws(
      () => assertExecutionOutcome(value, { nodeId: 'n1', nodeType: 'example' }),
      /ExecutionOutcome contract/,
    );
  }
  assert.throws(
    () => succeededOutcome('n1', { outputs: { result: 'fresh-ish' } }),
    /output availability/,
  );
});

console.log(`\nAll ${passed} execution outcome contract tests passed.`);
