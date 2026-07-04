import assert from 'node:assert/strict';
import { shouldDispatchActionForEvent } from '../public/js/action-events.js';

let passed = 0;
function test(name, fn) {
  fn();
  passed += 1;
  console.log(`  ok - ${name}`);
}

function el(tagName, type = '') {
  return { tagName, type };
}

test('button data-actions dispatch on click', () => {
  assert.equal(shouldDispatchActionForEvent('click', el('BUTTON')), true);
});

test('select data-actions dispatch on change but not click', () => {
  assert.equal(shouldDispatchActionForEvent('click', el('SELECT')), false);
  assert.equal(shouldDispatchActionForEvent('change', el('SELECT')), true);
});

test('text inputs dispatch on change but not click', () => {
  assert.equal(shouldDispatchActionForEvent('click', el('INPUT', 'text')), false);
  assert.equal(shouldDispatchActionForEvent('change', el('INPUT', 'text')), true);
});

console.log(`\nAll ${passed} action event tests passed.`);
