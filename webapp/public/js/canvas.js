// Biocircuits Explorer — Canvas Panning, Zooming & Node Interaction Events
import { canvasState, dragState, resizeState, wiringState, scale, setScale, MIN_SCALE, MAX_SCALE, MAX_CANVAS_PAN, ZOOM_SENSITIVITY } from './state.js';
import { updateConnections, scheduleUpdateConnections, getSocketCenter, bezierPath } from './connections.js';
import { record, MoveNodeCommand } from './commands.js';
import {
  normalizeRect, nodeIdsInRect, collectNodeWorldBounds,
  clearSelection, setSelection, addToSelection, selectOnly, toggleSelection,
  isSelected, captureSelectionPositions, recordGroupMove,
} from './selection.js';

// Marquee (rubber-band) selection state. Lives here because it is pure view
// interaction, like the connection-redraw RAF throttle above.
const marqueeState = {
  active: false, additive: false, startX: 0, startY: 0, el: null,
};

// Whether the spacebar is held: while it is, a left-drag pans the canvas
// (Figma/Photoshop convention) instead of starting a marquee or dragging a
// node. This keeps plain left-drag free for selection while still giving an
// easy, discoverable pan gesture.
let spaceHeld = false;

// Module-level DOM refs, set by initCanvasEvents()
let editor = null;
let canvas = null;
let svgLayer = null;
let gridBg = null;
let editorResizeObserver = null;
let themeChangeListenerInstalled = false;
const GRID_SPACING = 50;
const MIN_GRID_PIXEL_STEP = 12;

// Compute a bounded set of screen-space grid coordinates. Using the pan phase
// avoids incrementing enormous world-grid indices, which can stop changing at
// IEEE-754 magnitudes and otherwise create a non-terminating loop.
export function gridLineScreenPositions(viewportSize, pan, pixelStep) {
  if (!Number.isFinite(viewportSize) || viewportSize < 0 ||
      !Number.isFinite(pan) || !Number.isFinite(pixelStep) || pixelStep < MIN_GRID_PIXEL_STEP) {
    return [];
  }
  const phase = ((pan % pixelStep) + pixelStep) % pixelStep;
  const maxLines = Math.ceil(viewportSize / MIN_GRID_PIXEL_STEP) + 2;
  const positions = [];
  for (let index = 0; index < maxLines; index += 1) {
    const position = phase + index * pixelStep;
    if (position > viewportSize) break;
    positions.push(position);
  }
  return positions;
}

function setBoundedCanvasPan(panX, panY) {
  const bounded = value => Number.isFinite(value)
    ? Math.max(-MAX_CANVAS_PAN, Math.min(MAX_CANVAS_PAN, value))
    : 0;
  canvasState.panX = bounded(panX);
  canvasState.panY = bounded(panY);
}

function getVisibleWorldBounds() {
  const viewportWidth = Math.max(editor.clientWidth, 1);
  const viewportHeight = Math.max(editor.clientHeight, 1);
  const visibleLeft = -canvasState.panX / scale;
  const visibleTop = -canvasState.panY / scale;
  const visibleRight = visibleLeft + viewportWidth / scale;
  const visibleBottom = visibleTop + viewportHeight / scale;

  return {
    viewportWidth,
    viewportHeight,
    visibleLeft,
    visibleTop,
    visibleRight,
    visibleBottom,
  };
}

function renderGrid() {
  if (!editor || !gridBg) return;
  // setScale() enforces this invariant. Keep a local guard as a final defense
  // because a non-positive scale makes the adaptive-grid loop non-terminating.
  if (!Number.isFinite(scale) || scale < MIN_SCALE || scale > MAX_SCALE) return;

  const width = Math.max(editor.clientWidth, 1);
  const height = Math.max(editor.clientHeight, 1);
  const dpr = Math.max(window.devicePixelRatio || 1, 1);
  const pixelWidth = Math.max(1, Math.round(width * dpr));
  const pixelHeight = Math.max(1, Math.round(height * dpr));

  if (gridBg.width !== pixelWidth) gridBg.width = pixelWidth;
  if (gridBg.height !== pixelHeight) gridBg.height = pixelHeight;
  gridBg.style.width = `${width}px`;
  gridBg.style.height = `${height}px`;

  const ctx = gridBg.getContext('2d');
  if (!ctx) return;

  ctx.setTransform(1, 0, 0, 1, 0, 0);
  ctx.clearRect(0, 0, pixelWidth, pixelHeight);

  const gridColor = getComputedStyle(document.documentElement).getPropertyValue('--grid-color').trim() || '#2b2b2b';
  let displayedGridStep = GRID_SPACING;
  while (displayedGridStep * scale < MIN_GRID_PIXEL_STEP) {
    displayedGridStep *= 2;
  }
  const pixelStep = displayedGridStep * scale;

  ctx.beginPath();
  ctx.strokeStyle = gridColor;
  ctx.lineWidth = 1;

  for (const screenX of gridLineScreenPositions(width, canvasState.panX, pixelStep)) {
    const x = Math.round(screenX * dpr) + 0.5;
    ctx.moveTo(x, 0);
    ctx.lineTo(x, pixelHeight);
  }

  for (const screenY of gridLineScreenPositions(height, canvasState.panY, pixelStep)) {
    const y = Math.round(screenY * dpr) + 0.5;
    ctx.moveTo(0, y);
    ctx.lineTo(pixelWidth, y);
  }

  ctx.stroke();
}

// svgLayer is pinned at editor inset:0 (sibling of #grid-bg, NOT inside #canvas),
// so its CSS geometry never changes during pan/zoom. Only the viewBox is updated
// to track the visible world rectangle. This avoids Safari/Chrome paint ghosts
// that would otherwise appear when the SVG element itself is resized/moved
// while its parent has a CSS transform.
function updateSvgViewBox() {
  if (!editor || !svgLayer) return;
  const { viewportWidth, viewportHeight, visibleLeft, visibleTop } = getVisibleWorldBounds();
  const visibleW = viewportWidth / scale;
  const visibleH = viewportHeight / scale;
  svgLayer.setAttribute('viewBox', `${visibleLeft} ${visibleTop} ${visibleW} ${visibleH}`);
}

function syncViewportLayers() {
  updateSvgViewBox();
  renderGrid();
}

function getEditorRect() {
  return editor?.getBoundingClientRect() || null;
}

function clientToWorld(clientX, clientY) {
  const rect = getEditorRect();
  if (!rect) return { x: 0, y: 0 };
  return {
    x: (clientX - rect.left - canvasState.panX) / scale,
    y: (clientY - rect.top - canvasState.panY) / scale,
  };
}

export function applyViewportTransform() {
  if (!canvas || !svgLayer || !gridBg) return;

  // Only #canvas is CSS-transformed. #svg-layer and #grid-bg are siblings of #canvas
  // (not inside it), pinned to the editor at inset:0; they track pan/zoom through
  // viewBox (SVG) / explicit redraw (canvas) instead of inheriting a transform.
  syncViewportLayers();
  canvas.style.transform = `translate(${canvasState.panX}px, ${canvasState.panY}px) scale(${scale})`;
}

export function findScrollableAncestor(target, stopAt = editor) {
  let el = target instanceof Element ? target : null;
  while (el && el !== stopAt) {
    const style = window.getComputedStyle(el);
    const canScrollY = ['auto', 'scroll'].includes(style.overflowY) && el.scrollHeight > el.clientHeight + 1;
    const canScrollX = ['auto', 'scroll'].includes(style.overflowX) && el.scrollWidth > el.clientWidth + 1;
    if (canScrollY || canScrollX) return el;
    el = el.parentElement;
  }
  return null;
}

export function findWheelScrollableAncestor(target, deltaX, deltaY, stopAt = editor) {
  let el = target instanceof Element ? target : null;
  while (el && el !== stopAt) {
    const style = window.getComputedStyle(el);
    const canScrollY = ['auto', 'scroll'].includes(style.overflowY) && el.scrollHeight > el.clientHeight + 1;
    const canScrollX = ['auto', 'scroll'].includes(style.overflowX) && el.scrollWidth > el.clientWidth + 1;
    const canConsumeY = canScrollY && (
      (deltaY < 0 && el.scrollTop > 0) ||
      (deltaY > 0 && el.scrollTop + el.clientHeight < el.scrollHeight - 1)
    );
    const canConsumeX = canScrollX && (
      (deltaX < 0 && el.scrollLeft > 0) ||
      (deltaX > 0 && el.scrollLeft + el.clientWidth < el.scrollWidth - 1)
    );

    if (canConsumeY || canConsumeX) return el;
    el = el.parentElement;
  }
  return null;
}

export function isInteractivePlotTarget(target) {
  return target instanceof Element && !!target.closest('.plot-container, .js-plotly-plot, .plotly, .modebar');
}

export function normalizeWheelDelta(delta, deltaMode) {
  if (deltaMode === 1) {
    return delta * 16;
  }
  if (deltaMode === 2) {
    return delta * Math.max(editor.clientHeight, 800);
  }
  return delta;
}

export function computeZoomFactor(e) {
  const normalizedDelta = normalizeWheelDelta(e.deltaY, e.deltaMode);
  const clampedDelta = Math.max(-240, Math.min(240, normalizedDelta));
  return Math.exp(-clampedDelta * ZOOM_SENSITIVITY);
}

export function resetView() {
  setBoundedCanvasPan(0, 0);
  setScale(1.0);
  applyViewportTransform();
  updateConnections();
}

export function initCanvasEvents() {
  editor = document.getElementById('editor');
  canvas = document.getElementById('canvas');
  svgLayer = document.getElementById('svg-layer');
  gridBg = document.getElementById('grid-bg');

  if (!themeChangeListenerInstalled) {
    window.addEventListener('biocircuits-explorer:theme-changed', () => renderGrid());
    window.addEventListener('rop:theme-changed', () => renderGrid());
    themeChangeListenerInstalled = true;
  }

  if (editorResizeObserver) editorResizeObserver.disconnect();
  if (window.ResizeObserver && editor) {
    editorResizeObserver = new ResizeObserver(() => {
      applyViewportTransform();
      scheduleUpdateConnections();
    });
    editorResizeObserver.observe(editor);
  }

  applyViewportTransform();

  // Pan with: middle/right mouse anywhere, or Space + left-drag anywhere.
  // Plain left-drag on empty canvas starts a marquee selection instead.
  editor.addEventListener('mousedown', (e) => {
    if (e.button === 1 || e.button === 2 || (e.button === 0 && spaceHeld)) {
      canvasState.isPanning = true;
      canvasState.startPanX = e.clientX - canvasState.panX;
      canvasState.startPanY = e.clientY - canvasState.panY;
      if (editor) editor.style.cursor = 'grabbing';
      e.preventDefault();
    } else if (e.button === 0 && (e.target === editor || e.target === canvas || e.target === svgLayer)) {
      // Left-drag on empty canvas = marquee selection (CAD convention).
      // Panning is still available via Space+drag, trackpad/wheel, middle/right-drag.
      const w = clientToWorld(e.clientX, e.clientY);
      marqueeState.active = true;
      marqueeState.additive = e.shiftKey;
      marqueeState.startX = w.x;
      marqueeState.startY = w.y;
      if (!marqueeState.additive) clearSelection();
      marqueeState.el = document.createElement('div');
      marqueeState.el.className = 'marquee';
      marqueeState.el.style.left = `${w.x}px`;
      marqueeState.el.style.top = `${w.y}px`;
      marqueeState.el.style.width = '0px';
      marqueeState.el.style.height = '0px';
      canvas.appendChild(marqueeState.el);
      e.preventDefault();
    }
  });

  // Wheel / trackpad panning and zooming
  editor.addEventListener('wheel', (e) => {
    if (isInteractivePlotTarget(e.target)) {
      return;
    }

    if (!(e.ctrlKey || e.metaKey) && findWheelScrollableAncestor(e.target, e.deltaX, e.deltaY)) {
      return;
    }

    e.preventDefault();

    if (e.ctrlKey || e.metaKey) {
      // Zoom mode
      const oldScale = scale;
      const zoomFactor = computeZoomFactor(e);
      setScale(Math.max(MIN_SCALE, Math.min(MAX_SCALE, scale * zoomFactor)));

      // Calculate mouse position relative to editor
      const rect = editor.getBoundingClientRect();
      const mouseX = e.clientX - rect.left;
      const mouseY = e.clientY - rect.top;

      // Adjust pan to keep mouse position fixed
      const scaleDiff = scale / oldScale;
      setBoundedCanvasPan(
        mouseX - (mouseX - canvasState.panX) * scaleDiff,
        mouseY - (mouseY - canvasState.panY) * scaleDiff,
      );
    } else {
      // Pan mode
      setBoundedCanvasPan(canvasState.panX - e.deltaX, canvasState.panY - e.deltaY);
    }

    applyViewportTransform();
    updateConnections();
  }, { passive: false });

  window.addEventListener('mousemove', (e) => {
    if (canvasState.isPanning) {
      setBoundedCanvasPan(e.clientX - canvasState.startPanX, e.clientY - canvasState.startPanY);
      applyViewportTransform();
      scheduleUpdateConnections();
    }
    if (marqueeState.active && marqueeState.el) {
      const w = clientToWorld(e.clientX, e.clientY);
      const rect = normalizeRect(marqueeState.startX, marqueeState.startY, w.x, w.y);
      marqueeState.el.style.left = `${rect.x}px`;
      marqueeState.el.style.top = `${rect.y}px`;
      marqueeState.el.style.width = `${rect.w}px`;
      marqueeState.el.style.height = `${rect.h}px`;
    }
    if (dragState.isDraggingNode && dragState.draggedNode) {
      const worldPoint = clientToWorld(e.clientX, e.clientY);
      const newLeft = worldPoint.x - dragState.nodeOffsetX;
      const newTop = worldPoint.y - dragState.nodeOffsetY;
      const starts = dragState.groupStarts;
      if (starts && starts.size > 0) {
        // Move the whole selection by the same delta as the dragged node.
        const dx = newLeft - dragState.dragStartLeft;
        const dy = newTop - dragState.dragStartTop;
        for (const [id, start] of starts) {
          const el = document.getElementById(id);
          if (!el) continue;
          el.style.left = `${start.x + dx}px`;
          el.style.top = `${start.y + dy}px`;
        }
      } else {
        dragState.draggedNode.style.left = `${newLeft}px`;
        dragState.draggedNode.style.top = `${newTop}px`;
      }
      scheduleUpdateConnections();
    }
    if (resizeState.isResizing && resizeState.resizeNode) {
      const dw = (e.clientX - resizeState.resizeStartX) / scale;
      const dh = (e.clientY - resizeState.resizeStartY) / scale;
      resizeState.resizeNode.style.width = Math.max(240, resizeState.resizeStartW + dw) + 'px';
      resizeState.resizeNode.style.height = Math.max(100, resizeState.resizeStartH + dh) + 'px';
      const plotEl = resizeState.resizeNode.querySelector('.plot-container');
      if (plotEl) Plotly.Plots.resize(plotEl);
      scheduleUpdateConnections();
    }
    if (wiringState.isWiring && wiringState.tempWire && wiringState.wireStartSocket) {
      const pointer = clientToWorld(e.clientX, e.clientY);
      const sr = getSocketCenter(wiringState.wireStartSocket);
      if (wiringState.wireStartIsOutput) {
        wiringState.tempWire.setAttribute('d', bezierPath(sr.x, sr.y, pointer.x, pointer.y));
      } else {
        wiringState.tempWire.setAttribute('d', bezierPath(pointer.x, pointer.y, sr.x, sr.y));
      }
    }
  });

  window.addEventListener('mouseup', (e) => {
    if (canvasState.isPanning) {
      canvasState.isPanning = false;
      if (editor) editor.style.cursor = spaceHeld ? 'grab' : '';
    }
    if (marqueeState.active) {
      const w = clientToWorld(e.clientX, e.clientY);
      const rect = normalizeRect(marqueeState.startX, marqueeState.startY, w.x, w.y);
      if (marqueeState.el) { marqueeState.el.remove(); marqueeState.el = null; }
      // Only act if the marquee has real area; a click with no drag just
      // clears the selection (already done on mousedown unless additive).
      if (rect.w > 3 || rect.h > 3) {
        const hits = nodeIdsInRect(rect, collectNodeWorldBounds());
        if (marqueeState.additive) hits.forEach(addToSelection);
        else setSelection(hits);
      }
      marqueeState.active = false;
    }
    if (dragState.isDraggingNode) {
      // Nodes were moved live during mousemove, so the DOM holds the final
      // positions. Record the whole gesture as one undo step (group move if
      // a multi-selection was dragged, else a single move).
      const starts = dragState.groupStarts;
      if (starts && starts.size > 0) {
        recordGroupMove(starts);
      } else {
        const node = dragState.draggedNode;
        if (node) {
          const endLeft = parseFloat(node.style.left) || 0;
          const endTop = parseFloat(node.style.top) || 0;
          const moved = Math.abs(endLeft - dragState.dragStartLeft) > 0.5 ||
                        Math.abs(endTop - dragState.dragStartTop) > 0.5;
          if (moved) {
            record(new MoveNodeCommand({
              nodeId: node.id,
              fromX: dragState.dragStartLeft,
              fromY: dragState.dragStartTop,
              toX: endLeft,
              toY: endTop,
            }));
          }
        }
      }
      dragState.isDraggingNode = false;
      dragState.draggedNode = null;
      dragState.groupStarts = null;
    }
    if (resizeState.isResizing) { resizeState.isResizing = false; resizeState.resizeNode = null; }
    if (wiringState.isWiring) {
      if (wiringState.tempWire) { wiringState.tempWire.remove(); wiringState.tempWire = null; }
      wiringState.isWiring = false;
      wiringState.wireStartSocket = null;
    }
  });

  editor.addEventListener('contextmenu', (e) => e.preventDefault());

  // ===== Node Dragging (via headers) =====
  document.addEventListener('mousedown', (e) => {
    // While Space is held the gesture is a canvas pan, not a node drag.
    if (spaceHeld) return;
    const header = e.target.closest('.node-header');
    if (!header || e.button !== 0) return;
    const node = header.closest('.node');

    // Selection rules: Shift toggles this node; a plain click on an
    // unselected node selects it alone; clicking an already-selected node
    // keeps the group so the whole selection can be dragged together.
    if (e.shiftKey) {
      toggleSelection(node.id);
      if (!isSelected(node.id)) { e.preventDefault(); return; } // deselected → no drag
    } else if (!isSelected(node.id)) {
      selectOnly(node.id);
    }

    dragState.isDraggingNode = true;
    dragState.draggedNode = node;
    const nodeLeft = parseFloat(node.style.left || 0);
    const nodeTop = parseFloat(node.style.top || 0);
    // Capture the gesture's origin so mouseup can record one undoable move.
    dragState.dragStartLeft = nodeLeft;
    dragState.dragStartTop = nodeTop;
    // Snapshot all selected nodes' positions for a group move.
    dragState.groupStarts = captureSelectionPositions();
    const worldPoint = clientToWorld(e.clientX, e.clientY);
    dragState.nodeOffsetX = worldPoint.x - nodeLeft;
    dragState.nodeOffsetY = worldPoint.y - nodeTop;
    node.style.zIndex = 20;
    document.querySelectorAll('.node').forEach(n => { if (n !== node) n.style.zIndex = 10; });
    e.preventDefault();
  });

  // ===== Node Resizing =====
  document.addEventListener('mousedown', (e) => {
    const handle = e.target.closest('.node-resize');
    if (!handle || e.button !== 0) return;
    const node = handle.closest('.node');
    resizeState.isResizing = true;
    resizeState.resizeNode = node;
    resizeState.resizeStartX = e.clientX;
    resizeState.resizeStartY = e.clientY;
    resizeState.resizeStartW = node.offsetWidth;
    resizeState.resizeStartH = node.offsetHeight;
    e.preventDefault();
    e.stopPropagation();
  });

  // ===== Space-to-pan =====
  // Hold Space to switch left-drag from marquee/node-drag to canvas pan.
  // Ignored while typing in a field or focused on a control so Space keeps
  // its normal meaning there.
  window.addEventListener('keydown', (e) => {
    if (e.code !== 'Space' && e.key !== ' ') return;
    const t = e.target;
    if (t && (t.isContentEditable ||
              (t.tagName && /^(INPUT|TEXTAREA|SELECT|BUTTON|OPTION)$/.test(t.tagName)))) {
      return;
    }
    if (!spaceHeld) {
      spaceHeld = true;
      if (editor && !canvasState.isPanning) editor.style.cursor = 'grab';
    }
    e.preventDefault();   // stop Space from scrolling the page
  });
  window.addEventListener('keyup', (e) => {
    if (e.code !== 'Space' && e.key !== ' ') return;
    spaceHeld = false;
    if (editor && !canvasState.isPanning) editor.style.cursor = '';
  });
}
