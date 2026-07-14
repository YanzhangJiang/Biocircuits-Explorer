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
const { NODE_CONTRACTS, NODE_ROLES } = await import('../public/js/node-contracts.js');

const LEGACY_RESTORE_ONLY = new Set([
  'siso-analysis',
  'rop-cloud',
  'fret-heatmap',
  'parameter-scan-1d',
  'parameter-scan-2d',
  'rop-polyhedron',
]);

let passed = 0;
function test(name, fn) {
  fn();
  passed += 1;
  console.log(`  ok - ${name}`);
}

test('the node contract inventory is exhaustive and uses the five architecture roles', () => {
  assert.equal(Object.keys(NODE_TYPES).length, 40);
  assert.deepEqual(Object.keys(NODE_CONTRACTS).sort(), Object.keys(NODE_TYPES).sort());
  assert.deepEqual([...NODE_ROLES].sort(), ['compute', 'config', 'manual-gate', 'source', 'viewer']);
  for (const [nodeType, definition] of Object.entries(NODE_TYPES)) {
    assert.ok(NODE_ROLES.has(definition.role), `${nodeType} role`);
    assert.ok(['active', 'restore-only'].includes(definition.availability), `${nodeType} availability`);
    assert.ok(definition.execution, `${nodeType} execution descriptor`);
  }
});

test('exactly six merged legacy nodes are restore-only and never scheduler-runnable', () => {
  const actual = Object.entries(NODE_TYPES)
    .filter(([, definition]) => definition.availability === 'restore-only')
    .map(([nodeType]) => nodeType);
  assert.deepEqual(new Set(actual), LEGACY_RESTORE_ONLY);
  for (const nodeType of actual) {
    assert.equal(NODE_TYPES[nodeType].execution.mode, 'restore-only');
  }
});

test('prepare and execute operations are separate and match their descriptors', () => {
  for (const [nodeType, definition] of Object.entries(NODE_TYPES)) {
    const mode = definition.execution.mode;
    if (mode === 'prepare') {
      assert.equal(typeof definition.prepare, 'function', `${nodeType}.prepare`);
      assert.equal(definition.execute, undefined, `${nodeType} must not expose prepare as execute`);
    }
    if (mode === 'execute' || mode === 'manual') {
      assert.equal(typeof definition.execute, 'function', `${nodeType}.execute`);
    }
  }
});

test('every active Run button has an execution operation and SISO is a real compute node', () => {
  for (const [nodeType, definition] of Object.entries(NODE_TYPES)) {
    const body = definition.createBody?.('inventory-node') || '';
    const hasRunButton = /class="[^"]*\bbtn-run\b/.test(body);
    if (hasRunButton && definition.availability === 'active') {
      assert.ok(
        ['prepare', 'execute', 'manual'].includes(definition.execution.mode),
        `${nodeType} Run button needs an execution operation`,
      );
    }
  }
  assert.equal(NODE_TYPES['siso-result'].role, 'compute');
  assert.equal(NODE_TYPES['siso-result'].execution.mode, 'execute');
  assert.equal(typeof NODE_TYPES['siso-result'].execute, 'function');
});

console.log(`\nAll ${passed} execution descriptor inventory tests passed.`);
