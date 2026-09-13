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
const {
  FALLBACK_MIN_NODE_SIZE,
  NODE_MIN_SIZES,
  clampNodeSize,
  nodeMinSize,
} = await import('../public/js/node-sizes.js');

let passed = 0;
function test(name, fn) {
  fn();
  passed += 1;
  console.log(`  ok - ${name}`);
}

test('the min-size inventory is exhaustive and matches NODE_TYPES exactly', () => {
  assert.equal(Object.keys(NODE_TYPES).length, 43);
  assert.deepEqual(Object.keys(NODE_MIN_SIZES).sort(), Object.keys(NODE_TYPES).sort());
});

test('every minimum respects the global floor and its declared default size', () => {
  for (const [nodeType, min] of Object.entries(NODE_MIN_SIZES)) {
    assert.ok(min.width >= 240, `${nodeType} min width below 240`);
    assert.ok(min.height >= 100, `${nodeType} min height below 100`);
    const definition = NODE_TYPES[nodeType];
    if (definition.defaultWidth != null) {
      assert.ok(
        min.width <= definition.defaultWidth,
        `${nodeType} min width ${min.width} exceeds defaultWidth ${definition.defaultWidth}`,
      );
    }
    if (definition.defaultHeight != null) {
      assert.ok(
        min.height <= definition.defaultHeight,
        `${nodeType} min height ${min.height} exceeds defaultHeight ${definition.defaultHeight}`,
      );
    }
  }
});

test('nodeMinSize returns the table entry for known types and the fallback otherwise', () => {
  assert.equal(nodeMinSize('markdown-note'), NODE_MIN_SIZES['markdown-note']);
  assert.equal(nodeMinSize('siso-result'), NODE_MIN_SIZES['siso-result']);
  assert.equal(nodeMinSize('nonexistent-type'), FALLBACK_MIN_NODE_SIZE);
  assert.equal(nodeMinSize(undefined), FALLBACK_MIN_NODE_SIZE);
});

test('clampNodeSize clamps below-min inputs up and passes above-min inputs through', () => {
  assert.deepEqual(clampNodeSize('markdown-note', 100, 50), { width: 280, height: 220 });
  assert.deepEqual(clampNodeSize('markdown-note', 500, 400), { width: 500, height: 400 });
  assert.deepEqual(clampNodeSize('markdown-note', 280, 220), { width: 280, height: 220 });
});

test('clampNodeSize yields the minimum for non-finite inputs', () => {
  assert.deepEqual(clampNodeSize('markdown-note', NaN, undefined), { width: 280, height: 220 });
  assert.deepEqual(clampNodeSize('markdown-note', Infinity, 400), { width: 280, height: 400 });
  assert.deepEqual(clampNodeSize('nonexistent-type', NaN, NaN), {
    width: FALLBACK_MIN_NODE_SIZE.width,
    height: FALLBACK_MIN_NODE_SIZE.height,
  });
});

console.log(`\nAll ${passed} node size inventory tests passed.`);
