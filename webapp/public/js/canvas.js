// Biocircuits Explorer — Canvas Panning, Zooming & Node Interaction Events
import { canvasState, dragState, resizeState, wiringState, scale, setScale, MIN_SCALE, MAX_SCALE, ZOOM_SENSITIVITY } from './state.js';
import { updateConnections, scheduleUpdateConnections, getSocketCenter, bezierPath } from './connections.js';

// Module-level DOM refs, set by initCanvasEvents()
let editor = null;
let canvas = null;
let svgLayer = null;
let gridBg = null;
let editorResizeObserver = null;
let themeChangeListenerInstalled = false;
const GRID_SPACING = 50;
const SVG_OVERSCAN_VIEWPORTS = 1;
const MIN_GRID_PIXEL_STEP = 12;
const svgCoverage = {
  ready: false,
  left: 0,
  top: 0,
  right: 0,
  bottom: 0,
};

function snapGridStart(value) {
  return Math.floor(value / GRID_SPACING) * GRID_SPACING;
}

function snapGridEnd(value) {
  return Math.ceil(value / GRID_SPACING) * GRID_SPACING;
}

function resetSvgCoverage() {
  svgCoverage.ready = false;
}

function getVisibleWorldBounds(overscanViewports = 0) {
  const viewportWidth = Math.max(editor.clientWidth, 1);
  const viewportHeight = Math.max(editor.clientHeight, 1);
  const visibleLeft = -canvasState.panX / scale;
  const visibleTop = -canvasState.panY / scale;
  const visibleRight = visibleLeft + viewportWidth / scale;
  const visibleBottom = visibleTop + viewportHeight / scale;
  const overscanWorld = Math.max(viewportWidth, viewportHeight) * overscanViewports / scale;

  return {
    viewportWidth,
    viewportHeight,
    visibleLeft,
    visibleTop,
    visibleRight,
    visibleBottom,
    left: visibleLeft - overscanWorld,
    top: visibleTop - overscanWorld,
    right: visibleRight + overscanWorld,
    bottom: visibleBottom + overscanWorld,
  };
}

function renderGrid() {
  if (!editor || !gridBg) return;

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
  const worldBounds = getVisibleWorldBounds(0);
  let displayedGridStep = GRID_SPACING;
  while (displayedGridStep * scale < MIN_GRID_PIXEL_STEP) {
    displayedGridStep *= 2;
  }

  const startCol = Math.floor(worldBounds.visibleLeft / displayedGridStep) - 1;
  const endCol = Math.ceil(worldBounds.visibleRight / displayedGridStep) + 1;
  const startRow = Math.floor(worldBounds.visibleTop / displayedGridStep) - 1;
  const endRow = Math.ceil(worldBounds.visibleBottom / displayedGridStep) + 1;

  ctx.beginPath();
  ctx.strokeStyle = gridColor;
  ctx.lineWidth = 1;

  for (let col = startCol; col <= endCol; col += 1) {
    const screenX = col * displayedGridStep * scale + canvasState.panX;
    const x = Math.round(screenX * dpr) + 0.5;
    ctx.moveTo(x, 0);
    ctx.lineTo(x, pixelHeight);
  }

  for (let row = startRow; row <= endRow; row += 1) {
    const screenY = row * displayedGridStep * scale + canvasState.panY;
    const y = Math.round(screenY * dpr) + 0.5;
    ctx.moveTo(0, y);
    ctx.lineTo(pixelWidth, y);
  }

  ctx.stroke();
}

function ensureSvgCoverage(force = false) {
  if (!editor || !svgLayer) return;

  const worldBounds = getVisibleWorldBounds(SVG_OVERSCAN_VIEWPORTS);
  const neededLeft = worldBounds.left;
  const neededTop = worldBounds.top;
  const neededRight = worldBounds.right;
  const neededBottom = worldBounds.bottom;

  const alreadyCovered = svgCoverage.ready
    && neededLeft >= svgCoverage.left
    && neededTop >= svgCoverage.top
    && neededRight <= svgCoverage.right
    && neededBottom <= svgCoverage.bottom;

  if (!force && alreadyCovered) return;

  const left = snapGridStart(neededLeft);
  const top = snapGridStart(neededTop);
  const right = Math.max(snapGridEnd(neededRight), left + GRID_SPACING * 2);
  const bottom = Math.max(snapGridEnd(neededBottom), top + GRID_SPACING * 2);
  const width = right - left;
  const height = bottom - top;

  svgLayer.style.left = `${left}px`;
  svgLayer.style.top = `${top}px`;
  svgLayer.style.width = `${width}px`;
  svgLayer.style.height = `${height}px`;
  svgLayer.setAttribute('viewBox', `${left} ${top} ${width} ${height}`);

  svgCoverage.ready = true;
  svgCoverage.left = left;
  svgCoverage.top = top;
  svgCoverage.right = right;
  svgCoverage.bottom = bottom;
}

function syncViewportLayers(force = false) {
  ensureSvgCoverage(force);
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

  // Keep a single transform source for world-space layers while svgLayer uses a world-space viewBox.
  syncViewportLayers();
  const viewportTransform = `translate(${canvasState.panX}px, ${canvasState.panY}px) scale(${scale})`;
  canvas.style.transform = viewportTransform;
  svgLayer.style.transform = 'none';
  gridBg.style.transform = 'none';
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
  canvasState.panX = 0;
  canvasState.panY = 0;
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
      resetSvgCoverage();
      applyViewportTransform();
      scheduleUpdateConnections();
    });
    editorResizeObserver.observe(editor);
  }

  resetSvgCoverage();
  applyViewportTransform();

  // Canvas panning (middle/right mouse button, or left-click on blank area)
  editor.addEventListener('mousedown', (e) => {
    if (e.button === 1 || e.button === 2) {
      canvasState.isPanning = true;
      canvasState.startPanX = e.clientX - canvasState.panX;
      canvasState.startPanY = e.clientY - canvasState.panY;
      e.preventDefault();
    } else if (e.button === 0 && (e.target === editor || e.target === canvas || e.target === svgLayer)) {
      canvasState.isPanning = true;
      canvasState.startPanX = e.clientX - canvasState.panX;
      canvasState.startPanY = e.clientY - canvasState.panY;
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
      canvasState.panX = mouseX - (mouseX - canvasState.panX) * scaleDiff;
      canvasState.panY = mouseY - (mouseY - canvasState.panY) * scaleDiff;
    } else {
      // Pan mode
      canvasState.panX -= e.deltaX;
      canvasState.panY -= e.deltaY;
    }

    applyViewportTransform();
    updateConnections();
  }, { passive: false });

  window.addEventListener('mousemove', (e) => {
    if (canvasState.isPanning) {
      canvasState.panX = e.clientX - canvasState.startPanX;
      canvasState.panY = e.clientY - canvasState.startPanY;
      applyViewportTransform();
      scheduleUpdateConnections();
    }
    if (dragState.isDraggingNode && dragState.draggedNode) {
      const worldPoint = clientToWorld(e.clientX, e.clientY);
      dragState.draggedNode.style.left = `${worldPoint.x - dragState.nodeOffsetX}px`;
      dragState.draggedNode.style.top = `${worldPoint.y - dragState.nodeOffsetY}px`;
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
    if (canvasState.isPanning) canvasState.isPanning = false;
    if (dragState.isDraggingNode) { dragState.isDraggingNode = false; dragState.draggedNode = null; }
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
    const header = e.target.closest('.node-header');
    if (!header || e.button !== 0) return;
    const node = header.closest('.node');
    dragState.isDraggingNode = true;
    dragState.draggedNode = node;
    const nodeLeft = parseFloat(node.style.left || 0);
    const nodeTop = parseFloat(node.style.top || 0);
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
}
