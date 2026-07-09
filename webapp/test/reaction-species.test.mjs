import assert from 'node:assert/strict';

globalThis.window = {
  matchMedia: () => null,
  crypto: { randomUUID: () => 'reaction-species-test' },
  sessionStorage: { getItem: () => null, setItem: () => {} },
  addEventListener() {},
  location: { protocol: 'http:', hostname: '127.0.0.1', port: '8000' },
};
globalThis.document = {
  readyState: 'loading',
  documentElement: { dataset: {}, style: { setProperty() {} } },
  getElementById: () => null,
  addEventListener() {},
  querySelectorAll: () => [],
};

const {
  parseSpeciesFromReactionSide,
  inferSpeciesOrderFromReactions,
} = await import('../public/js/rop-cloud.js');

let passed = 0;
function test(name, fn) {
  fn();
  passed += 1;
  console.log(`  ok - ${name}`);
}

test('reaction species parser accepts the full SBML SId leading grammar', () => {
  assert.deepEqual(parseSpeciesFromReactionSide('2 _free_A + B_2'), ['_free_A', 'B_2']);
  assert.deepEqual(parseSpeciesFromReactionSide('2 9bad + has-dash'), []);
});

test('ROP species inference preserves leading-underscore SBML identifiers', () => {
  const inferred = inferSpeciesOrderFromReactions(['_free_A + B <-> _complex_AB']);
  assert.deepEqual(inferred.species, ['B', '_free_A', '_complex_AB']);
  assert.deepEqual(inferred.productSpecies, ['_complex_AB']);
});

console.log(`\nAll ${passed} reaction species tests passed.`);
