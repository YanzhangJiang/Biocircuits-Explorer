// Pure unit tests for the command/undo skeleton. No DOM required — the
// command layer talks to the editor through injected "performers", so we
// stub them with an in-memory model and assert the history behavior.
//
// Run: node webapp/test/commands.test.mjs
// (Node 18+; commands.js is browser-targeted but has no top-level browser
//  globals — window is only touched inside initUndoKeyboard, never called
//  here.)

import assert from 'node:assert/strict';
import {
  UndoStack, registerPerformers, dispatch, record, undo, redo, undoStack,
  CreateNodeCommand, MoveNodeCommand, RemoveNodeCommand,
  ConnectCommand, DisconnectCommand, SetConnectionsCommand,
  ChangeAttrCommand, CompoundCommand,
} from '../public/js/commands.js';

let passed = 0;
function test(name, fn) {
  fn();
  passed += 1;
  console.log(`  ok - ${name}`);
}

// ─── In-memory stub editor ───
// Models just enough of the real editor (nodes, connections, attrs) for the
// command performers to operate on, with no DOM.
function makeStubEditor() {
  const nodes = new Map();      // id -> { type, x, y, attrs }
  let conns = [];               // [{fromNode,fromPort,toNode,toPort}]
  let counter = 0;
  let changeCount = 0;

  const connKey = (c) => `${c.fromNode}:${c.fromPort}->${c.toNode}:${c.toPort}`;

  return {
    nodes,
    get conns() { return conns; },
    get changeCount() { return changeCount; },
    performers: {
      createNode(type, x, y, opts = {}) {
        const id = opts.id || `node-${++counter}`;
        nodes.set(id, { type, x, y, attrs: {} });
        return id;
      },
      removeNode(id) {
        nodes.delete(id);
        conns = conns.filter((c) => c.fromNode !== id && c.toNode !== id);
      },
      moveNode(id, x, y) {
        const n = nodes.get(id);
        if (n) { n.x = x; n.y = y; }
      },
      snapshotNode(id) {
        const n = nodes.get(id);
        if (!n) return null;
        return {
          id, type: n.type, x: n.x, y: n.y, width: 100, height: 50,
          data: { ...n.attrs },
          connections: conns
            .filter((c) => c.fromNode === id || c.toNode === id)
            .map((c) => ({ ...c })),
        };
      },
      restoreNode(snap) {
        if (!snap) return null;
        nodes.set(snap.id, { type: snap.type, x: snap.x, y: snap.y, attrs: { ...snap.data } });
        for (const c of (snap.connections || [])) {
          if (!conns.some((x) => connKey(x) === connKey(c))) conns.push({ ...c });
        }
        return snap.id;
      },
      addConnection(conn) {
        let replaced = null;
        const existing = conns.find((c) => c.toNode === conn.toNode && c.toPort === conn.toPort);
        if (existing) { replaced = { ...existing }; conns = conns.filter((c) => c !== existing); }
        conns.push({ ...conn });
        return replaced;
      },
      removeConnection(conn) {
        conns = conns.filter((c) => connKey(c) !== connKey(conn));
      },
      replaceConnections(arr) {
        conns = (arr || []).map((c) => ({ ...c }));
      },
      setAttr(_nodeId, key, value) {
        // key is "<nodeId>::<attr>" in tests so we can route to a node.
        const [nid, attr] = key.split('::');
        const n = nodes.get(nid);
        if (n) n.attrs[attr] = value;
      },
      afterChange() { changeCount += 1; },
    },
  };
}

// ─── UndoStack unit tests (no performers needed) ───

test('UndoStack: record/undo/redo cycle with a fake command', () => {
  const stack = new UndoStack(10);
  let state = 0;
  const cmd = {
    kind: 'set',
    apply() { state = 5; },
    revert() { state = 0; },
  };
  cmd.apply();
  stack.record(cmd);
  assert.equal(state, 5);
  assert.equal(stack.canUndo, true);
  assert.equal(stack.canRedo, false);

  stack.undo();
  assert.equal(state, 0);
  assert.equal(stack.canUndo, false);
  assert.equal(stack.canRedo, true);

  stack.redo();
  assert.equal(state, 5);
});

test('UndoStack: a new record clears the redo stack', () => {
  const stack = new UndoStack(10);
  const mk = (n) => ({ kind: 'noop', apply() {}, revert() {}, n });
  stack.record(mk(1));
  stack.record(mk(2));
  stack.undo();
  assert.equal(stack.canRedo, true);
  stack.record(mk(3));         // overwrites the redo future
  assert.equal(stack.canRedo, false);
  assert.equal(stack.depth, 2); // [1, 3]
});

test('UndoStack: capacity drops the oldest entry', () => {
  const stack = new UndoStack(3);
  for (let i = 0; i < 5; i += 1) {
    stack.record({ kind: 'noop', apply() {}, revert() {} });
  }
  assert.equal(stack.depth, 3);
});

test('UndoStack: onChange fires on record/undo/redo', () => {
  const stack = new UndoStack(10);
  let fires = 0;
  const off = stack.onChange(() => { fires += 1; });
  const cmd = { kind: 'noop', apply() {}, revert() {} };
  stack.record(cmd);          // +1
  stack.undo();               // +1
  stack.redo();               // +1
  off();
  stack.record({ kind: 'noop', apply() {}, revert() {} }); // not counted
  assert.equal(fires, 3);
});

test('UndoStack: a failed undo preserves both history stacks', () => {
  const stack = new UndoStack(10);
  let attempts = 0;
  const command = {
    kind: 'fallible-undo',
    apply() {},
    revert() {
      attempts += 1;
      throw new Error('synthetic undo failure');
    },
  };
  stack.record(command);

  assert.throws(() => stack.undo(), /synthetic undo failure/);
  assert.equal(attempts, 1);
  assert.equal(stack.depth, 1);
  assert.equal(stack.canUndo, true);
  assert.equal(stack.canRedo, false);
});

test('UndoStack: a failed redo remains retryable without changing undo depth', () => {
  const stack = new UndoStack(10);
  let failRedo = false;
  const command = {
    kind: 'fallible-redo',
    apply() {
      if (failRedo) throw new Error('synthetic redo failure');
    },
    revert() {},
  };
  command.apply();
  stack.record(command);
  stack.undo();
  failRedo = true;

  assert.throws(() => stack.redo(), /synthetic redo failure/);
  assert.equal(stack.depth, 0);
  assert.equal(stack.canUndo, false);
  assert.equal(stack.canRedo, true);

  failRedo = false;
  assert.equal(stack.redo(), command);
  assert.equal(stack.depth, 1);
});

// ─── End-to-end through the shared dispatch API ───

test('dispatch(CreateNodeCommand): creates, undo removes, redo recreates', () => {
  const editor = makeStubEditor();
  registerPerformers(editor.performers);
  undoStack.clear();

  const cmd = dispatch(new CreateNodeCommand({ nodeType: 'reaction-network', x: 80, y: 150 }));
  assert.ok(cmd.createdId);
  assert.equal(editor.nodes.size, 1);
  assert.equal(editor.changeCount, 1);    // afterChange ran

  undo();
  assert.equal(editor.nodes.size, 0);
  assert.equal(editor.changeCount, 2);

  redo();
  assert.equal(editor.nodes.size, 1);
  assert.equal(editor.changeCount, 3);
});

test('CreateNodeCommand: redo preserves id so later redo commands still apply', () => {
  const editor = makeStubEditor();
  registerPerformers(editor.performers);
  undoStack.clear();

  const create = dispatch(new CreateNodeCommand({ nodeType: 'reaction-network', x: 80, y: 150 }));
  const id = create.createdId;
  editor.performers.moveNode(id, 180, 250);
  record(new MoveNodeCommand({ nodeId: id, fromX: 80, fromY: 150, toX: 180, toY: 250 }));

  undo(); // move
  undo(); // create
  assert.equal(editor.nodes.has(id), false);

  redo(); // create
  assert.equal(create.createdId, id);
  assert.equal(editor.nodes.has(id), true);

  redo(); // move still targets the original id
  assert.deepEqual([editor.nodes.get(id).x, editor.nodes.get(id).y], [180, 250]);
});

test('record(MoveNodeCommand): undo restores the original position', () => {
  const editor = makeStubEditor();
  registerPerformers(editor.performers);
  undoStack.clear();

  const id = editor.performers.createNode('model-builder', 0, 0);
  // Simulate a live drag that already moved the node to (200, 120).
  editor.performers.moveNode(id, 200, 120);
  record(new MoveNodeCommand({ nodeId: id, fromX: 0, fromY: 0, toX: 200, toY: 120 }));

  assert.deepEqual([editor.nodes.get(id).x, editor.nodes.get(id).y], [200, 120]);
  undo();
  assert.deepEqual([editor.nodes.get(id).x, editor.nodes.get(id).y], [0, 0]);
  redo();
  assert.deepEqual([editor.nodes.get(id).x, editor.nodes.get(id).y], [200, 120]);
});

test('MoveNodeCommand: rapid moves of the same node coalesce into one step', () => {
  const editor = makeStubEditor();
  registerPerformers(editor.performers);
  undoStack.clear();

  const id = editor.performers.createNode('viewer', 0, 0);

  // Three moves within the merge window — should collapse to one undo step
  // whose "from" is the very first origin.
  const m1 = new MoveNodeCommand({ nodeId: id, fromX: 0, fromY: 0, toX: 10, toY: 10 });
  const m2 = new MoveNodeCommand({ nodeId: id, fromX: 10, fromY: 10, toX: 20, toY: 20 });
  const m3 = new MoveNodeCommand({ nodeId: id, fromX: 20, fromY: 20, toX: 30, toY: 30 });
  // Force same time window.
  m2.timestamp = m1.timestamp;
  m3.timestamp = m1.timestamp;
  editor.performers.moveNode(id, 30, 30);
  record(m1); record(m2); record(m3);

  assert.equal(undoStack.depth, 1, 'three rapid moves should be one undo step');
  undo();
  assert.deepEqual([editor.nodes.get(id).x, editor.nodes.get(id).y], [0, 0]);
});

test('MoveNodeCommand: moves of different nodes do NOT merge', () => {
  const editor = makeStubEditor();
  registerPerformers(editor.performers);
  undoStack.clear();

  const a = editor.performers.createNode('viewer', 0, 0);
  const b = editor.performers.createNode('viewer', 0, 0);
  record(new MoveNodeCommand({ nodeId: a, fromX: 0, fromY: 0, toX: 5, toY: 5 }));
  record(new MoveNodeCommand({ nodeId: b, fromX: 0, fromY: 0, toX: 9, toY: 9 }));
  assert.equal(undoStack.depth, 2);
});

test('RemoveNodeCommand: undo restores the node and its wires', () => {
  const editor = makeStubEditor();
  registerPerformers(editor.performers);
  undoStack.clear();

  const a = editor.performers.createNode('reaction-network', 0, 0);
  const b = editor.performers.createNode('model-builder', 300, 0);
  editor.performers.addConnection({ fromNode: a, fromPort: 'reactions', toNode: b, toPort: 'reactions' });
  assert.equal(editor.conns.length, 1);

  dispatch(new RemoveNodeCommand({ nodeId: b }));
  assert.equal(editor.nodes.has(b), false);
  assert.equal(editor.conns.length, 0);   // incident wire dropped with node

  undo();
  assert.equal(editor.nodes.has(b), true);
  assert.equal(editor.conns.length, 1);   // wire restored
  assert.equal(editor.nodes.get(b).x, 300);

  redo();
  assert.equal(editor.nodes.has(b), false);
  assert.equal(editor.conns.length, 0);
});

test('ConnectCommand: undo removes the new wire and restores the evicted one', () => {
  const editor = makeStubEditor();
  registerPerformers(editor.performers);
  undoStack.clear();

  const a = editor.performers.createNode('a', 0, 0);
  const b = editor.performers.createNode('b', 0, 0);
  const c = editor.performers.createNode('c', 0, 0);
  // Pre-existing wire into b's "model" input.
  editor.performers.addConnection({ fromNode: a, fromPort: 'model', toNode: b, toPort: 'model' });

  // Connecting c->b on the same input should evict a->b.
  dispatch(new ConnectCommand({ fromNode: c, fromPort: 'model', toNode: b, toPort: 'model' }));
  assert.equal(editor.conns.length, 1);
  assert.equal(editor.conns[0].fromNode, c);

  undo();   // remove c->b, restore a->b
  assert.equal(editor.conns.length, 1);
  assert.equal(editor.conns[0].fromNode, a);
});

test('DisconnectCommand: undo re-adds the wire', () => {
  const editor = makeStubEditor();
  registerPerformers(editor.performers);
  undoStack.clear();

  const a = editor.performers.createNode('a', 0, 0);
  const b = editor.performers.createNode('b', 0, 0);
  const conn = { fromNode: a, fromPort: 'model', toNode: b, toPort: 'model' };
  editor.performers.addConnection(conn);

  dispatch(new DisconnectCommand(conn));
  assert.equal(editor.conns.length, 0);
  undo();
  assert.equal(editor.conns.length, 1);
});

test('SetConnectionsCommand: swaps whole set, reversible', () => {
  const editor = makeStubEditor();
  registerPerformers(editor.performers);
  undoStack.clear();

  const before = [{ fromNode: 'n1', fromPort: 'p', toNode: 'n2', toPort: 'p' }];
  const after = [{ fromNode: 'n3', fromPort: 'p', toNode: 'n4', toPort: 'p' }];
  editor.performers.replaceConnections(before);

  // The gesture already produced `after`; record (not dispatch) it.
  editor.performers.replaceConnections(after);
  record(new SetConnectionsCommand({ before, after }));

  assert.equal(editor.conns[0].fromNode, 'n3');
  undo();
  assert.equal(editor.conns[0].fromNode, 'n1');
  redo();
  assert.equal(editor.conns[0].fromNode, 'n3');
});

test('ChangeAttrCommand: undo restores prior value; bursts merge', () => {
  const editor = makeStubEditor();
  registerPerformers(editor.performers);
  undoStack.clear();

  const id = editor.performers.createNode('scan', 0, 0);
  const key = `${id}::param_min`;
  editor.nodes.get(id).attrs.param_min = '-6';

  // Two quick edits in the same field — should collapse to one undo step
  // that returns to the original value.
  const c1 = new ChangeAttrCommand({ nodeId: id, key, before: '-6', after: '-5' });
  const c2 = new ChangeAttrCommand({ nodeId: id, key, before: '-5', after: '-4' });
  c2.timestamp = c1.timestamp;     // force within merge window
  editor.performers.setAttr(id, key, '-5'); record(c1);
  editor.performers.setAttr(id, key, '-4'); record(c2);

  assert.equal(undoStack.depth, 1);
  assert.equal(editor.nodes.get(id).attrs.param_min, '-4');
  undo();
  assert.equal(editor.nodes.get(id).attrs.param_min, '-6');
});

test('CompoundCommand: one undo step, revert runs in reverse order', () => {
  const editor = makeStubEditor();
  registerPerformers(editor.performers);
  undoStack.clear();

  const order = [];
  const mk = (n) => ({
    kind: 'probe',
    apply() { order.push(`a${n}`); },
    revert() { order.push(`r${n}`); },
  });
  const compound = new CompoundCommand([mk(1), mk(2), mk(3)], 'Batch');
  dispatch(compound);
  assert.deepEqual(order, ['a1', 'a2', 'a3']);
  undo();
  assert.deepEqual(order, ['a1', 'a2', 'a3', 'r3', 'r2', 'r1']);
  assert.equal(undoStack.depth, 0);
});

test('CompoundCommand: batch delete of two connected nodes round-trips', () => {
  const editor = makeStubEditor();
  registerPerformers(editor.performers);
  undoStack.clear();

  const a = editor.performers.createNode('a', 0, 0);
  const b = editor.performers.createNode('b', 0, 0);
  editor.performers.addConnection({ fromNode: a, fromPort: 'p', toNode: b, toPort: 'p' });

  // Build remove commands, then run as a compound (delete both at once).
  dispatch(new CompoundCommand([
    new RemoveNodeCommand({ nodeId: a }),
    new RemoveNodeCommand({ nodeId: b }),
  ], 'Delete 2 nodes'));
  assert.equal(editor.nodes.size, 0);
  assert.equal(editor.conns.length, 0);

  undo();
  assert.equal(editor.nodes.size, 2);
  assert.equal(editor.conns.length, 1);   // wire restored exactly once
});

console.log(`\nAll ${passed} command-model tests passed.`);
