// Pure geometry unit tests (marquee selection + alignment math). No DOM.
// Run: node webapp/test/geometry.test.mjs

import assert from 'node:assert/strict';
import { normalizeRect, rectsIntersect, nodeIdsInRect } from '../public/js/geometry.js';
import { alignOffsets, distributeOffsets } from '../public/js/align.js';
import { internalConnections, remapSnapshots } from '../public/js/clipboard-util.js';

let passed = 0;
function test(name, fn) { fn(); passed += 1; console.log(`  ok - ${name}`); }

test('normalizeRect handles drags in any direction', () => {
  assert.deepEqual(normalizeRect(10, 10, 30, 40), { x: 10, y: 10, w: 20, h: 30 });
  assert.deepEqual(normalizeRect(30, 40, 10, 10), { x: 10, y: 10, w: 20, h: 30 });
  assert.deepEqual(normalizeRect(5, 50, 25, 20), { x: 5, y: 20, w: 20, h: 30 });
});

test('rectsIntersect: overlap yes, gap no, touching edge no', () => {
  const a = { x: 0, y: 0, w: 10, h: 10 };
  assert.equal(rectsIntersect(a, { x: 5, y: 5, w: 10, h: 10 }), true);
  assert.equal(rectsIntersect(a, { x: 20, y: 0, w: 5, h: 5 }), false);
  assert.equal(rectsIntersect(a, { x: 10, y: 0, w: 5, h: 5 }), false); // edge touch
});

test('nodeIdsInRect returns only intersecting node ids', () => {
  const bounds = [
    { id: 'n1', x: 0, y: 0, w: 50, h: 30 },
    { id: 'n2', x: 100, y: 100, w: 50, h: 30 },
    { id: 'n3', x: 40, y: 20, w: 50, h: 30 },
  ];
  const hits = nodeIdsInRect({ x: 10, y: 10, w: 60, h: 60 }, bounds);
  assert.deepEqual(hits.sort(), ['n1', 'n3']);
});

// ─── Alignment math ───

const NODES = [
  { id: 'a', x: 0,   y: 0,   w: 100, h: 40 },
  { id: 'b', x: 200, y: 50,  w: 60,  h: 40 },
  { id: 'c', x: 120, y: 120, w: 80,  h: 40 },
];

test('alignOffsets left: all share the min x', () => {
  const offs = alignOffsets(NODES, 'left');
  // min x = 0, so a:0, b:-200, c:-120 on x; y unchanged
  assert.equal(offs.a.dx, 0);
  assert.equal(offs.b.dx, -200);
  assert.equal(offs.c.dx, -120);
  assert.equal(offs.a.dy, 0);
});

test('alignOffsets right: right edges align to the max right edge', () => {
  // right edges: a=100, b=260, c=200 → target 260
  const offs = alignOffsets(NODES, 'right');
  assert.equal(offs.a.dx, 160); // 260-100
  assert.equal(offs.b.dx, 0);
  assert.equal(offs.c.dx, 60);  // 260-200
});

test('alignOffsets top: all share the min y', () => {
  const offs = alignOffsets(NODES, 'top');
  assert.equal(offs.a.dy, 0);
  assert.equal(offs.b.dy, -50);
  assert.equal(offs.c.dy, -120);
});

test('alignOffsets center-h: centers share the bounding-box center x', () => {
  // bbox x: min=0, maxRight=260 → center = 130
  const offs = alignOffsets(NODES, 'center-h');
  const center = (0 + 260) / 2;
  assert.equal(offs.a.dx, center - 50);   // 80
  assert.equal(offs.b.dx, center - 230);  // -100
  assert.equal(offs.c.dx, center - 160);  // -30
  assert.equal(offs.a.dy, 0);
});

test('distributeOffsets h: equal gaps between sorted nodes, ends fixed', () => {
  // Sort by center-x: a(50), c(160), b(230). Ends a,b stay; c is evenly spaced.
  const offs = distributeOffsets(NODES, 'h');
  // Ends must not move.
  assert.equal(offs.a.dx, 0);
  assert.equal(offs.b.dx, 0);
  // Total width of all three = 100+80+60 = 240. Span from a.left(0) to
  // b.right(260) = 260. Free space = 260-240 = 20, one gap each side of the
  // middle node → gap = 20/2 = 10. a ends at x=0..100, gap 10 → c.left=110.
  // c currently at 120, so dx = -10.
  assert.equal(offs.c.dx, -10);
  assert.equal(offs.a.dy, 0);
});

test('alignOffsets: fewer than 2 nodes → no offsets', () => {
  assert.deepEqual(alignOffsets([NODES[0]], 'left'), {});
  assert.deepEqual(alignOffsets([], 'left'), {});
});

// ─── Clipboard remapping (paste / duplicate) ───

test('internalConnections keeps only wires within the copied set', () => {
  const snaps = [
    { id: 'a', connections: [
      { fromNode: 'a', fromPort: 'p', toNode: 'b', toPort: 'p' },     // internal
      { fromNode: 'ext', fromPort: 'p', toNode: 'a', toPort: 'q' },   // external
    ] },
    { id: 'b', connections: [
      { fromNode: 'a', fromPort: 'p', toNode: 'b', toPort: 'p' },     // dup of internal
    ] },
  ];
  const conns = internalConnections(snaps);
  assert.equal(conns.length, 1);
  assert.equal(conns[0].fromNode, 'a');
  assert.equal(conns[0].toNode, 'b');
});

test('remapSnapshots: new ids, offset position, remapped internal wires', () => {
  const source = [
    { id: 'a', type: 'x', x: 10, y: 20, width: 100, height: 40,
      data: { foo: 1 },
      connections: [{ fromNode: 'a', fromPort: 'p', toNode: 'b', toPort: 'p' },
                    { fromNode: 'ext', fromPort: 'p', toNode: 'a', toPort: 'q' }] },
    { id: 'b', type: 'y', x: 200, y: 20, width: 60, height: 40,
      data: { bar: 2 },
      connections: [{ fromNode: 'a', fromPort: 'p', toNode: 'b', toPort: 'p' }] },
  ];
  const idMap = { a: 'node-50', b: 'node-51' };
  const out = remapSnapshots(source, idMap, 40);

  assert.equal(out[0].id, 'node-50');
  assert.equal(out[1].id, 'node-51');
  assert.equal(out[0].x, 50);   // 10 + 40
  assert.equal(out[0].y, 60);   // 20 + 40
  assert.deepEqual(out[0].data, { foo: 1 });

  // Internal wire remapped to new ids; external wire dropped.
  const aWires = out[0].connections;
  assert.equal(aWires.length, 1);
  assert.equal(aWires[0].fromNode, 'node-50');
  assert.equal(aWires[0].toNode, 'node-51');
  // b also lists the same internal wire (restoreNode dedups live).
  assert.equal(out[1].connections.length, 1);
});

console.log(`\nAll ${passed} geometry/alignment tests passed.`);
