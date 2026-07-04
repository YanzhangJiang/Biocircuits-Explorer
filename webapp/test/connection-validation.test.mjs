import assert from 'node:assert/strict';
import { isRestoredConnectionValid } from '../public/js/connection-validation.js';

let passed = 0;
function test(name, fn) {
  fn();
  passed += 1;
  console.log(`  ok - ${name}`);
}

const nodeTypes = {
  source: { outputs: [{ port: 'reactions', label: 'Reactions' }], inputs: [] },
  builder: { inputs: [{ port: 'reactions', label: 'Reactions' }], outputs: [{ port: 'model', label: 'Model' }] },
  viewer: { inputs: [{ port: 'model', label: 'Model' }], outputs: [] },
};

test('restored connections must use declared output and input ports', () => {
  assert.equal(isRestoredConnectionValid(
    { fromPort: 'reactions', toPort: 'reactions' },
    'source',
    'builder',
    nodeTypes,
  ), true);
  assert.equal(isRestoredConnectionValid(
    { fromPort: 'model', toPort: 'reactions' },
    'source',
    'builder',
    nodeTypes,
  ), false);
  assert.equal(isRestoredConnectionValid(
    { fromPort: 'reactions', toPort: 'params' },
    'source',
    'builder',
    nodeTypes,
  ), false);
});

test('restored connections must keep compatible port artifact types', () => {
  assert.equal(isRestoredConnectionValid(
    { fromPort: 'reactions', toPort: 'model' },
    'source',
    'viewer',
    nodeTypes,
  ), false);
});

console.log(`\nAll ${passed} connection validation tests passed.`);
