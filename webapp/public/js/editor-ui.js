// Biocircuits Explorer — Editor chrome: alignment toolbar, keyboard
// shortcuts, and the shortcut cheatsheet overlay. All DOM here is created
// programmatically so no index-node.html changes are required.

import {
  getSelection, selectionSize, selectAll, clearSelection, deleteSelection,
  applyAlignment, onSelectionChange,
} from './selection.js';
import { copySelection, pasteClipboard, duplicateSelection } from './workspace.js';
import { showToast } from './api.js';

// ─── Alignment toolbar ───

const ALIGN_GROUPS = [
  [
    { mode: 'left',     label: 'L',  title: 'Align left' },
    { mode: 'center-h', label: 'C',  title: 'Align horizontal centers' },
    { mode: 'right',    label: 'R',  title: 'Align right' },
  ],
  [
    { mode: 'top',      label: 'T',  title: 'Align top' },
    { mode: 'center-v', label: 'M',  title: 'Align vertical centers' },
    { mode: 'bottom',   label: 'B',  title: 'Align bottom' },
  ],
  [
    { mode: 'distribute-h', label: '↔', title: 'Distribute horizontally (3+ nodes)' },
    { mode: 'distribute-v', label: '↕', title: 'Distribute vertically (3+ nodes)' },
  ],
];

let alignToolbarEl = null;

function buildAlignToolbar() {
  const bar = document.createElement('div');
  bar.id = 'align-toolbar';
  bar.className = 'align-toolbar hidden';
  bar.setAttribute('role', 'toolbar');
  bar.setAttribute('aria-label', 'Align selected nodes');

  ALIGN_GROUPS.forEach((group, i) => {
    if (i > 0) {
      const sep = document.createElement('span');
      sep.className = 'align-sep';
      bar.appendChild(sep);
    }
    for (const btn of group) {
      const b = document.createElement('button');
      b.type = 'button';
      b.className = 'align-btn';
      b.textContent = btn.label;
      b.title = btn.title;
      b.setAttribute('aria-label', btn.title);
      b.addEventListener('click', () => applyAlignment(btn.mode));
      bar.appendChild(b);
    }
  });

  document.body.appendChild(bar);
  return bar;
}

function updateAlignToolbarVisibility() {
  if (!alignToolbarEl) return;
  alignToolbarEl.classList.toggle('hidden', selectionSize() < 2);
}

// ─── Cheatsheet overlay ───

const SHORTCUTS = [
  ['Space + drag', 'Pan the canvas'],
  ['Middle / right-drag, trackpad', 'Pan the canvas'],
  ['Scroll, Ctrl/⌘ + scroll', 'Pan / zoom'],
  ['Drag on empty canvas', 'Marquee select'],
  ['Shift + click node', 'Add / remove from selection'],
  ['Drag a selected node', 'Move the whole selection'],
  ['Ctrl/⌘ + Z', 'Undo'],
  ['Ctrl/⌘ + Shift + Z, Ctrl + Y', 'Redo'],
  ['Delete / Backspace', 'Delete selected nodes'],
  ['Ctrl/⌘ + A', 'Select all'],
  ['Ctrl/⌘ + C / V', 'Copy / paste nodes'],
  ['Ctrl/⌘ + D', 'Duplicate selection'],
  ['Esc', 'Clear selection / close this'],
  ['?', 'Toggle this cheatsheet'],
];

let cheatsheetEl = null;

function buildCheatsheet() {
  const overlay = document.createElement('div');
  overlay.id = 'shortcut-cheatsheet';
  overlay.className = 'cheatsheet-overlay hidden';
  overlay.addEventListener('mousedown', (e) => {
    if (e.target === overlay) closeCheatsheet();
  });

  const panel = document.createElement('div');
  panel.className = 'cheatsheet-panel';

  const h = document.createElement('h3');
  h.textContent = 'Keyboard & mouse shortcuts';
  panel.appendChild(h);

  const table = document.createElement('table');
  for (const [keys, desc] of SHORTCUTS) {
    const tr = document.createElement('tr');
    const kc = document.createElement('td');
    kc.className = 'cheatsheet-keys';
    kc.textContent = keys;
    const dc = document.createElement('td');
    dc.textContent = desc;
    tr.append(kc, dc);
    table.appendChild(tr);
  }
  panel.appendChild(table);

  const hint = document.createElement('p');
  hint.className = 'cheatsheet-hint';
  hint.textContent = 'Press ? or Esc to close';
  panel.appendChild(hint);

  overlay.appendChild(panel);
  document.body.appendChild(overlay);
  return overlay;
}

function cheatsheetOpen() { return cheatsheetEl && !cheatsheetEl.classList.contains('hidden'); }
function openCheatsheet() { cheatsheetEl?.classList.remove('hidden'); }
function closeCheatsheet() { cheatsheetEl?.classList.add('hidden'); }
function toggleCheatsheet() { if (cheatsheetOpen()) closeCheatsheet(); else openCheatsheet(); }

// ─── Keyboard shortcuts ───

function isEditingField(t) {
  return t && (t.isContentEditable ||
    (t.tagName && /^(INPUT|TEXTAREA|SELECT)$/.test(t.tagName)));
}

export function initEditorShortcuts(target = window) {
  target.addEventListener('keydown', (e) => {
    const t = e.target;
    const inField = isEditingField(t);
    const mod = e.metaKey || e.ctrlKey;
    const key = (e.key || '').toLowerCase();

    // '?' opens the cheatsheet (not while typing); Esc closes it or clears
    // the selection.
    if (!inField && key === '?') { e.preventDefault(); toggleCheatsheet(); return; }
    if (key === 'escape') {
      if (cheatsheetOpen()) { e.preventDefault(); closeCheatsheet(); return; }
      if (!inField) clearSelection();
      return;
    }

    if (inField) return; // never hijack text editing

    if (key === 'delete' || key === 'backspace') {
      // Always prevent default here so Backspace can't trigger browser
      // back-navigation when the canvas has focus.
      e.preventDefault();
      if (selectionSize() > 0) deleteSelection();
      return;
    }
    if (mod && key === 'a') { e.preventDefault(); selectAll(); return; }
    if (mod && key === 'c') {
      if (selectionSize() > 0) { e.preventDefault(); const n = copySelection(); showToast(`Copied ${n} node(s)`); }
      return;
    }
    if (mod && key === 'v') { e.preventDefault(); pasteClipboard(); return; }
    if (mod && key === 'd') {
      if (selectionSize() > 0) { e.preventDefault(); duplicateSelection(); }
      return;
    }
  });
}

// ─── Init ───

export function initEditorUI() {
  alignToolbarEl = buildAlignToolbar();
  cheatsheetEl = buildCheatsheet();
  onSelectionChange(updateAlignToolbarVisibility);
  updateAlignToolbarVisibility();
  initEditorShortcuts();
}
