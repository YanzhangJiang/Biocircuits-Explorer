import assert from 'node:assert/strict';
import { test } from 'node:test';
import { createLocalDesignJobRunner } from '../public/js/local-design-job-core.js';

function fixture(overrides = {}) {
  const calls = [];
  const services = {
    submit: async () => ({ job_id: 'owned', status: 'queued' }),
    inspect: async id => { calls.push(['inspect', id]); return { job_id: id, status: 'succeeded' }; },
    result: async id => { calls.push(['result', id]); return { status: 'ok', selected_network: { kd: [2] } }; },
    cancel: async id => { calls.push(['cancel', id]); },
    wait: async () => {},
    ...overrides,
  };
  return { run: createLocalDesignJobRunner(services), calls };
}

test('polls the accepted local job and reads only its successful result', async () => {
  const { run, calls } = fixture();
  assert.equal((await run({})).selected_network.kd[0], 2);
  assert.deepEqual(calls, [['inspect', 'owned'], ['result', 'owned']]);
});

test('ownership lost during submission cancels the exact accepted job', async () => {
  let current = true;
  const { run, calls } = fixture({ submit: async () => {
    current = false;
    return { job_id: 'owned', status: 'running' };
  } });
  await assert.rejects(run({}, { statusIsCurrent: () => current }), { name: 'AbortError' });
  assert.deepEqual(calls, [['cancel', 'owned']]);
});

test('abort during polling cancels once and cannot read a result', async () => {
  const controller = new AbortController();
  const { run, calls } = fixture({ wait: async () => controller.abort() });
  await assert.rejects(run({}, { signal: controller.signal }), { name: 'AbortError' });
  assert.deepEqual(calls, [['cancel', 'owned']]);
});

test('stale terminal results are discarded without cancelling completed work', async () => {
  let current = true;
  const { run, calls } = fixture({ result: async () => { current = false; return { status: 'ok' }; } });
  await assert.rejects(run({}, { statusIsCurrent: () => current }), { name: 'AbortError' });
  assert.deepEqual(calls, [['inspect', 'owned']]);
});

test('transient polling errors have a bounded retry count', async () => {
  let count = 0;
  const { run, calls } = fixture({ inspect: async () => { count++; throw new TypeError('offline'); } });
  await assert.rejects(run({}), /offline/);
  assert.equal(count, 3);
  assert.deepEqual(calls, [['cancel', 'owned']]);
});

test('unknown identity and status fail closed', async () => {
  for (const job of [{ job_id: 'other', status: 'succeeded' }, { job_id: 'owned', status: 'magic' }]) {
    const { run, calls } = fixture({ inspect: async () => job });
    await assert.rejects(run({}), /identity|Unknown/);
    assert.deepEqual(calls, [['cancel', 'owned']]);
  }
});
