// Biocircuits Explorer — Command model + Undo/Redo (skeleton).
//
// This is the foundation for editor history. The design goal is to make
// *every* mutating editor action expressible as a Command with apply() and
// revert(), so a single Undo stack can walk the document backward and
// forward. This file ships the machinery plus two concrete commands
// (create-node, move-node) wired end-to-end as a proof of pattern. The
// remaining atomic operations — remove-node, change-attr, connect,
// disconnect — are intentionally left for the next stage; see MIGRATION
// at the bottom.
//
// Why dependency injection (performers) instead of importing nodes.js
// directly: commands.js must not import the DOM/state modules, because
// those modules will want to dispatch commands, which would create an
// import cycle. Injecting the concrete mutators at boot keeps this module
// free of cycles and unit-testable with stub performers (no DOM needed).

// ─── Performer registry (dependency injection) ───
// nodes.js / connections.js register the real DOM+state mutators here at
// boot. Commands call through these handles. Keeping the surface explicit
// (rather than a free-form object) documents exactly what the command
// layer depends on.
const performers = {
  createNode: null,     // (nodeType, x, y) => nodeId
  removeNode: null,     // (nodeId) => void
  moveNode: null,       // (nodeId, x, y) => void
  snapshotNode: null,   // (nodeId) => snapshot {id,type,x,y,width,height,data,connections}
  restoreNode: null,    // (snapshot) => nodeId  (recreates with original id)
  addConnection: null,  // (conn) => replacedConn|null  (one-input-one-wire)
  removeConnection: null,// (conn) => void
  replaceConnections: null,// (connArray) => void  (swap whole connection set)
  setAttr: null,        // (nodeId, key, value) => void
  afterChange: null,    // () => void  — commit workspace snapshot / sync shell
};

export function registerPerformers(map) {
  Object.assign(performers, map);
}

function requirePerformer(name) {
  const fn = performers[name];
  if (typeof fn !== 'function') {
    throw new Error(`[commands] performer "${name}" is not registered`);
  }
  return fn;
}

function afterChange() {
  const fn = performers.afterChange;
  if (typeof fn !== 'function') return;
  try {
    fn();
  } catch (e) {
    // A failing change-notifier must not corrupt the undo stack.
    console.error('[commands] afterChange hook failed', e);
  }
}

// ─── Undo stack ───
export class UndoStack {
  constructor(capacity = 100) {
    this.capacity = capacity;
    this._undo = [];
    this._redo = [];
    this._listeners = new Set();
  }

  // Subscribe to stack changes (for an Edit menu / toolbar enabled-state).
  // Returns an unsubscribe function.
  onChange(fn) {
    this._listeners.add(fn);
    return () => this._listeners.delete(fn);
  }

  _emit() {
    for (const fn of this._listeners) {
      try {
        fn(this);
      } catch (e) {
        console.error('[commands] onChange listener failed', e);
      }
    }
  }

  get canUndo() { return this._undo.length > 0; }
  get canRedo() { return this._redo.length > 0; }
  get depth() { return this._undo.length; }

  // Record an already-applied command. If the top of the stack can absorb
  // it (tryMerge), the two collapse into a single undo step. Any new entry
  // invalidates the redo stack — you can't redo into a future that a fresh
  // edit has overwritten.
  record(command) {
    const top = this._undo[this._undo.length - 1];
    if (top && typeof top.tryMerge === 'function' && top.tryMerge(command)) {
      this._redo.length = 0;
      this._emit();
      return;
    }
    this._undo.push(command);
    if (this._undo.length > this.capacity) {
      // Drop the oldest entry; that history is no longer reachable.
      this._undo.shift();
    }
    this._redo.length = 0;
    this._emit();
  }

  undo() {
    const cmd = this._undo.pop();
    if (!cmd) return null;
    cmd.revert();
    this._redo.push(cmd);
    this._emit();
    return cmd;
  }

  redo() {
    const cmd = this._redo.pop();
    if (!cmd) return null;
    cmd.apply();
    this._undo.push(cmd);
    this._emit();
    return cmd;
  }

  clear() {
    this._undo.length = 0;
    this._redo.length = 0;
    this._emit();
  }
}

// The single shared editor history.
export const undoStack = new UndoStack(100);

// ─── Public dispatch API ───

// First-time execution: the command owns the mutation, so we apply() then
// record. Use for programmatic actions (menu "Add node", paste, etc.).
export function dispatch(command) {
  command.apply();
  undoStack.record(command);
  afterChange();
  return command;
}

// Record a command whose mutation the caller already performed via direct
// manipulation (e.g. a live drag that updated the DOM as the mouse moved).
// apply() is not called now but will be on redo, so it must be idempotent
// with the already-applied end state.
export function record(command) {
  undoStack.record(command);
  afterChange();
  return command;
}

export function undo() {
  const cmd = undoStack.undo();
  if (cmd) afterChange();
  return cmd;
}

export function redo() {
  const cmd = undoStack.redo();
  if (cmd) afterChange();
  return cmd;
}

// ─── Concrete commands ───

export class CreateNodeCommand {
  constructor({ nodeType, x, y }) {
    this.kind = 'create-node';
    this.nodeType = nodeType;
    this.x = x;
    this.y = y;
    this.createdId = null;
    this.label = `Add ${nodeType}`;
  }

  apply() {
    // On redo this recreates the node. The id may differ from the original
    // (createNode allocates a fresh id), which is acceptable for the
    // skeleton: redo-after-undo-of-create is rare, and connections to the
    // node were already torn down by the matching revert(). Stage 2's
    // remove-node command will use serialize/restore to preserve ids.
    this.createdId = requirePerformer('createNode')(this.nodeType, this.x, this.y);
    return this.createdId;
  }

  revert() {
    if (this.createdId) {
      requirePerformer('removeNode')(this.createdId);
    }
  }
}

// Time window within which successive moves of the same node coalesce into
// one undo step. A drag emits one move at mouseup, but arrow-key nudges (a
// future feature) would otherwise each be their own step.
const MOVE_MERGE_WINDOW_MS = 600;

export class MoveNodeCommand {
  constructor({ nodeId, fromX, fromY, toX, toY }) {
    this.kind = 'move-node';
    this.nodeId = nodeId;
    this.fromX = fromX;
    this.fromY = fromY;
    this.toX = toX;
    this.toY = toY;
    this.timestamp = Date.now();
    this.label = 'Move node';
  }

  apply() { requirePerformer('moveNode')(this.nodeId, this.toX, this.toY); }
  revert() { requirePerformer('moveNode')(this.nodeId, this.fromX, this.fromY); }

  // Absorb a later move of the same node if it arrives within the merge
  // window. The starting position (fromX/fromY) is kept; only the
  // destination advances, so one Undo returns the node to where the whole
  // gesture began.
  tryMerge(next) {
    if (next.kind !== this.kind) return false;
    if (next.nodeId !== this.nodeId) return false;
    if (next.timestamp - this.timestamp > MOVE_MERGE_WINDOW_MS) return false;
    this.toX = next.toX;
    this.toY = next.toY;
    this.timestamp = next.timestamp;
    return true;
  }
}

// Remove a node and restore it (with its original id + incident wires) on
// undo. snapshotNode captures everything needed; restoreNode rebuilds it.
// Preserving the id is what lets other commands already on the stack keep
// referring to this node across an undo/redo cycle.
export class RemoveNodeCommand {
  constructor({ nodeId }) {
    this.kind = 'remove-node';
    this.nodeId = nodeId;
    this.snapshot = null;
    this.label = 'Delete node';
  }

  apply() {
    // Snapshot before removing. removeNode also drops incident connections,
    // so the snapshot (taken first) is the only record of those wires.
    this.snapshot = requirePerformer('snapshotNode')(this.nodeId);
    requirePerformer('removeNode')(this.nodeId);
  }

  revert() {
    if (this.snapshot) requirePerformer('restoreNode')(this.snapshot);
  }
}

// Add a wire. Because an input socket holds at most one wire, connecting may
// evict an existing wire; we capture it so undo restores the prior state
// exactly (remove ours, re-add theirs).
export class ConnectCommand {
  constructor({ fromNode, fromPort, toNode, toPort }) {
    this.kind = 'connect';
    this.conn = { fromNode, fromPort, toNode, toPort };
    this.replaced = null;
    this.label = 'Connect';
  }

  apply() {
    this.replaced = requirePerformer('addConnection')(this.conn) || null;
  }

  revert() {
    requirePerformer('removeConnection')(this.conn);
    if (this.replaced) requirePerformer('addConnection')(this.replaced);
  }
}

// Swap the entire connection set from `before` to `after`. This is the
// workhorse for interactive wiring, where a single gesture (drag a wire,
// detach a wire, or rewire an input) can add and remove edges at once and
// is awkward to decompose into individual Connect/Disconnect steps. Capture
// the array before the gesture and after it; one command captures the net
// diff. Connect/Disconnect below remain for clean programmatic single-edge
// changes (quick-add chains, paste).
export class SetConnectionsCommand {
  constructor({ before, after, label = 'Rewire' }) {
    this.kind = 'set-connections';
    // Defensive clones so later mutations of the live array can't corrupt
    // the captured history.
    this.before = (before || []).map((c) => ({ ...c }));
    this.after = (after || []).map((c) => ({ ...c }));
    this.label = label;
  }

  apply() { requirePerformer('replaceConnections')(this.after); }
  revert() { requirePerformer('replaceConnections')(this.before); }
}

// Remove a specific wire; undo re-adds it.
export class DisconnectCommand {
  constructor({ fromNode, fromPort, toNode, toPort }) {
    this.kind = 'disconnect';
    this.conn = { fromNode, fromPort, toNode, toPort };
    this.label = 'Disconnect';
  }

  apply() { requirePerformer('removeConnection')(this.conn); }
  revert() { requirePerformer('addConnection')(this.conn); }
}

const ATTR_MERGE_WINDOW_MS = 800;

// Change one node attribute (a config field value). Mergeable per
// (nodeId, key) within a short window so a burst of keystrokes collapses
// into a single undo step instead of one-per-character.
export class ChangeAttrCommand {
  constructor({ nodeId, key, before, after }) {
    this.kind = 'change-attr';
    this.nodeId = nodeId;
    this.key = key;
    this.before = before;
    this.after = after;
    this.timestamp = Date.now();
    this.label = `Edit ${key}`;
  }

  apply() { requirePerformer('setAttr')(this.nodeId, this.key, this.after); }
  revert() { requirePerformer('setAttr')(this.nodeId, this.key, this.before); }

  tryMerge(next) {
    if (next.kind !== this.kind) return false;
    if (next.nodeId !== this.nodeId || next.key !== this.key) return false;
    if (next.timestamp - this.timestamp > ATTR_MERGE_WINDOW_MS) return false;
    this.after = next.after;
    this.timestamp = next.timestamp;
    return true;
  }
}

// Restore a set of node snapshots (each carrying its own id + incident
// wires) as one undo step. Backs paste and duplicate: build snapshots with
// fresh ids and remapped internal connections, then dispatch this. apply()
// creates them; revert() removes them.
export class RestoreNodesCommand {
  constructor({ snapshots, label = 'Paste' }) {
    this.kind = 'restore-nodes';
    this.snapshots = snapshots || [];
    this.label = label;
  }

  apply() {
    for (const snap of this.snapshots) requirePerformer('restoreNode')(snap);
  }

  revert() {
    for (const snap of this.snapshots) requirePerformer('removeNode')(snap.id);
  }
}

// Group several commands into one undo step (batch delete, batch move,
// alignment, paste-many). apply() runs forward order; revert() runs reverse
// order so dependencies (e.g. nodes before their wires) unwind correctly.
export class CompoundCommand {
  constructor(commands, label = 'Batch edit') {
    this.kind = 'compound';
    this.commands = commands.filter(Boolean);
    this.label = label;
  }

  get isEmpty() { return this.commands.length === 0; }

  apply() {
    for (const cmd of this.commands) cmd.apply();
  }

  revert() {
    for (let i = this.commands.length - 1; i >= 0; i -= 1) {
      this.commands[i].revert();
    }
  }
}

// ─── Keyboard shortcuts ───

// Wire Ctrl/Cmd+Z (undo), Ctrl/Cmd+Shift+Z and Ctrl+Y (redo). Skipped when
// focus is in a text field so the browser's native text undo still works
// there. Call once at boot.
export function initUndoKeyboard(target = window) {
  target.addEventListener('keydown', (e) => {
    const t = e.target;
    if (t && (t.isContentEditable ||
              (t.tagName && /^(INPUT|TEXTAREA|SELECT)$/.test(t.tagName)))) {
      return;
    }
    if (!(e.metaKey || e.ctrlKey)) return;
    const key = (e.key || '').toLowerCase();
    if (key === 'z' && !e.shiftKey) {
      e.preventDefault();
      undo();
    } else if ((key === 'z' && e.shiftKey) || key === 'y') {
      e.preventDefault();
      redo();
    }
  });
}

// ─── History coverage ───
// Covered by commands (undoable): create node, move node (incl. batch via
// CompoundCommand), delete node (+ incident wires), connect, disconnect,
// and config-field edits (ChangeAttrCommand). Each call site routes through
// dispatch() (command owns the mutation) or record() (caller already
// mutated, e.g. a live drag). Operations not yet covered — node resize,
// canvas pan/zoom — are intentionally outside history; they don't change
// document content, only the view.
