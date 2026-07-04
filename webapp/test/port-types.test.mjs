// Pure unit tests for the typed port graph. No DOM — port-types.js is
// dependency-free. Run: node webapp/test/port-types.test.mjs

import assert from 'node:assert/strict';
import {
  PORT_TYPES, portTypeOf, portTypesCompatible, portsCompatible,
} from '../public/js/port-types.js';

let passed = 0;
function test(name, fn) {
  fn();
  passed += 1;
  console.log(`  ok - ${name}`);
}

// The full set of port ids declared across node types (keep in sync with the
// node-type definitions; this test fails loudly if one loses its type mapping).
const PORT_IDS = [
  'reactions', 'model', 'params', 'result',
  'atlas-spec', 'atlas', 'atlas-query', 'atlas-network',
  'designability-spec',
];

test('every known port id maps to a declared type (no raw-id fallback)', () => {
  for (const id of PORT_IDS) {
    const t = portTypeOf(id);
    assert.notEqual(t, id, `port '${id}' should map to a type, not fall back to its id`);
    assert.ok(Object.values(PORT_TYPES).includes(t), `'${id}' → '${t}' must be a PORT_TYPES value`);
  }
});

test('unknown port id falls back to its own id and is only self-compatible', () => {
  assert.equal(portTypeOf('mystery'), 'mystery');
  assert.ok(portsCompatible('mystery', 'mystery'));
  assert.ok(!portsCompatible('mystery', 'model'));
});

test('a declared per-port type overrides the central map', () => {
  assert.equal(portTypeOf('model', 'CustomType'), 'CustomType');
});

test('type compatibility is reflexive and rejects mismatches', () => {
  assert.ok(portTypesCompatible(PORT_TYPES.NetworkIR, PORT_TYPES.NetworkIR));
  assert.ok(!portTypesCompatible(PORT_TYPES.NetworkIR, PORT_TYPES.ModelArtifact));
});

test('behavior-preserving: typed compatibility matches old port-id equality', () => {
  // The old rule was `fromPort === toPort`. With a bijective id→type map, the
  // typed rule must accept/reject exactly the same pairs — no silent widening.
  for (const a of PORT_IDS) {
    for (const b of PORT_IDS) {
      assert.equal(portsCompatible(a, b), a === b, `compat(${a}, ${b})`);
    }
  }
});

console.log(`\nAll ${passed} port-type tests passed.`);
