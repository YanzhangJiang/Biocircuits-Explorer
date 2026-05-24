// Biocircuits Explorer — Node selection, marquee geometry, batch operations.
//
// Selection state is a Set of node ids with a `.selected` CSS class mirror.
// The geometry helpers (normalizeRect / rectsIntersect / nodeIdsInRect) are
// pure so they can be unit-tested without a DOM. Batch delete/move route
// through the command layer (CompoundCommand) so a multi-node edit is one
// undo step.

import { nodeRegistry } from './state.js';
import {
  dispatch, record, CompoundCommand, RemoveNodeCommand, MoveNodeCommand,
} from './commands.js';
// Pure geometry lives in its own (import-free) module so it stays unit-
// testable; re-exported here so callers keep importing from selection.js.
import { normalizeRect, rectsIntersect, nodeIdsInRect } from './geometry.js';
import { alignOffsets, distributeOffsets } from './align.js';

export { normalizeRect, rectsIntersect, nodeIdsInRect };

// ─── Selection model ───

const _selected = new Set();

export function getSelection() { return [..._selected]; }
export function selectionSize() { return _selected.size; }
export function isSelected(id) { return _selected.has(id); }

// Prune ids whose node no longer exists, then reconcile the .selected class
// across all node elements. Called after any selection mutation.
export function syncSelectionVisuals() {
  for (const id of [..._selected]) {
    if (!document.getElementById(id)) _selected.delete(id);
  }
  document.querySelectorAll('.node.selected').forEach((el) => {
    if (!_selected.has(el.id)) el.classList.remove('selected');
  });
  _selected.forEach((id) => document.getElementById(id)?.classList.add('selected'));
  notifySelectionChange();
}

export function clearSelection() {
  if (_selected.size === 0) return;
  _selected.clear();
  syncSelectionVisuals();
}

export function setSelection(ids) {
  _selected.clear();
  (ids || []).forEach((id) => _selected.add(id));
  syncSelectionVisuals();
}

export function selectOnly(id) { setSelection(id ? [id] : []); }
export function addToSelection(id) { if (id) { _selected.add(id); syncSelectionVisuals(); } }

export function toggleSelection(id) {
  if (!id) return;
  if (_selected.has(id)) _selected.delete(id); else _selected.add(id);
  syncSelectionVisuals();
}

export function selectAll() { setSelection(Object.keys(nodeRegistry)); }

// Listeners (alignment toolbar visibility, etc.).
const _listeners = new Set();
export function onSelectionChange(fn) { _listeners.add(fn); return () => _listeners.delete(fn); }
function notifySelectionChange() {
  for (const fn of _listeners) {
    try { fn(getSelection()); } catch (e) { console.error('[selection] listener failed', e); }
  }
}

// ─── World-space bounds (DOM) ───

// Node CSS px equal world units because nodes live inside the scaled
// #canvas, so left/top + offsetWidth/Height are already world coordinates.
export function collectNodeWorldBounds() {
  const bounds = [];
  for (const id of Object.keys(nodeRegistry)) {
    const el = document.getElementById(id);
    if (!el) continue;
    bounds.push({
      id,
      x: parseFloat(el.style.left) || 0,
      y: parseFloat(el.style.top) || 0,
      w: el.offsetWidth,
      h: el.offsetHeight,
    });
  }
  return bounds;
}

// ─── Batch operations (undoable) ───

export function deleteSelection() {
  const ids = getSelection().filter((id) => document.getElementById(id));
  if (ids.length === 0) return;
  const cmds = ids.map((id) => new RemoveNodeCommand({ nodeId: id }));
  dispatch(new CompoundCommand(cmds, `Delete ${cmds.length} node(s)`));
  clearSelection();
}

// Capture {id -> {x,y}} for the current selection at gesture start so a
// group drag can be recorded as one move command afterward.
export function captureSelectionPositions() {
  const starts = new Map();
  for (const id of _selected) {
    const el = document.getElementById(id);
    if (el) starts.set(id, { x: parseFloat(el.style.left) || 0, y: parseFloat(el.style.top) || 0 });
  }
  return starts;
}

// After a live group drag, record the net move(s) as a single undo step.
// The DOM already holds the final positions, so these are recorded (not
// dispatched/re-applied).
export function recordGroupMove(starts) {
  const cmds = [];
  for (const [id, start] of starts) {
    const el = document.getElementById(id);
    if (!el) continue;
    const toX = parseFloat(el.style.left) || 0;
    const toY = parseFloat(el.style.top) || 0;
    if (Math.abs(toX - start.x) > 0.5 || Math.abs(toY - start.y) > 0.5) {
      cmds.push(new MoveNodeCommand({ nodeId: id, fromX: start.x, fromY: start.y, toX, toY }));
    }
  }
  if (cmds.length === 1) record(cmds[0]);
  else if (cmds.length > 1) record(new CompoundCommand(cmds, `Move ${cmds.length} nodes`));
}

// Apply an alignment/distribution to the current selection as one undo step.
// mode: left|right|top|bottom|center-h|center-v|distribute-h|distribute-v.
// Computes target offsets from current bounds, then dispatches a fresh
// CompoundCommand of MoveNodeCommands (the nodes have not moved yet).
export function applyAlignment(mode) {
  const bounds = collectNodeWorldBounds().filter((b) => _selected.has(b.id));
  if (bounds.length < 2) return;
  const offsets =
    mode === 'distribute-h' ? distributeOffsets(bounds, 'h') :
    mode === 'distribute-v' ? distributeOffsets(bounds, 'v') :
    alignOffsets(bounds, mode);

  const cmds = [];
  for (const b of bounds) {
    const off = offsets[b.id];
    if (!off) continue;
    if (Math.abs(off.dx) < 0.5 && Math.abs(off.dy) < 0.5) continue;
    cmds.push(new MoveNodeCommand({
      nodeId: b.id, fromX: b.x, fromY: b.y, toX: b.x + off.dx, toY: b.y + off.dy,
    }));
  }
  if (cmds.length > 0) dispatch(new CompoundCommand(cmds, `Align ${mode}`));
}
