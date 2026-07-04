import assert from 'node:assert/strict';

global.window = {
  matchMedia: () => null,
  crypto: { randomUUID: () => 'debug-test' },
  sessionStorage: { getItem: () => null, setItem: () => {} },
};
global.document = {
  getElementById: () => null,
  documentElement: {
    dataset: {},
    style: { setProperty: () => {} },
  },
};

const { state, nodeRegistry } = await import('../public/js/state.js');
const { enrichModelRequestPayload } = await import('../public/js/api.js');

let passed = 0;
function test(name, fn) {
  fn();
  passed += 1;
  console.log(`  ok - ${name}`);
}

test('model requests include NetworkIR recovery data for matching node context', () => {
  Object.keys(nodeRegistry).forEach((key) => delete nodeRegistry[key]);
  state.model = null;
  nodeRegistry.builder = {
    data: {
      modelContext: {
        sessionId: 'stale-session',
        networkIrHash: 'hash-1',
        networkIr: { reactions: [{ formula: 'A + B <-> AB', kd: 1 }] },
      },
    },
  };

  const original = { session_id: 'stale-session', output_exprs: ['A'] };
  const enriched = enrichModelRequestPayload(original);

  assert.notEqual(enriched, original);
  assert.equal(enriched.session_id, 'stale-session');
  assert.equal(enriched.network_ir_hash, 'hash-1');
  assert.deepEqual(enriched.network, { reactions: [{ formula: 'A + B <-> AB', kd: 1 }] });
  assert.equal(original.network, undefined, 'must not mutate caller payload');
});

test('model requests keep explicit network payloads untouched', () => {
  const payload = { session_id: 'stale-session', network: { label: 'explicit' } };
  assert.equal(enrichModelRequestPayload(payload), payload);
});

console.log(`\nAll ${passed} model request tests passed.`);
