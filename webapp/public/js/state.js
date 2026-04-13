// Biocircuits Explorer — Shared State & Constants
// All mutable state is exported as objects (properties visible across modules)
// or via setter functions for primitive let bindings.

export const API = '';
export const DEBUG_CLIENT_STORAGE_KEY = 'biocircuits-explorer.debug-client-id';
export const LEGACY_DEBUG_CLIENT_STORAGE_KEY = 'rop-explorer.debug-client-id';

export const state = {
  sessionId: null,
  model: null,
  qK_syms: [],
};

export const debugConsoleState = {
  open: false,
  entries: [],
  lastSeq: 0,
  pollTimer: null,
  fetchInFlight: false,
  unseenPriority: false,
};

export const WORKSPACE_DOCUMENT_VERSION = 1;
export const WORKSPACE_SHELL_CONTRACT_VERSION = 1;
export const THEME_MODE_STORAGE_KEY = 'biocircuits-explorer.theme-mode';
export const LEGACY_THEME_MODE_STORAGE_KEY = 'rop-explorer.theme-mode';
export const LIGHT_THEME_STYLESHEET_ID = 'biocircuits-explorer-light-theme-stylesheet';
export const colorSchemeMediaQuery = window.matchMedia ? window.matchMedia('(prefers-color-scheme: light)') : null;

// Workspace shell mutable state
export let workspaceShellHost = null;
export function setWorkspaceShellHost(h) { workspaceShellHost = h; }
export let workspaceShellReady = false;
export function setWorkspaceShellReady(r) { workspaceShellReady = r; }
export let workspaceShellSyncTimer = null;
export function setWorkspaceShellSyncTimer(t) { workspaceShellSyncTimer = t; }
export let lastWorkspaceShellSnapshot = '';
export function setLastWorkspaceShellSnapshot(s) { lastWorkspaceShellSnapshot = s; }

export const themeState = {
  mode: 'auto',
  effective: 'dark',
};

let debugClientId = null;

function createDebugClientId() {
  if (window.crypto?.randomUUID) {
    return window.crypto.randomUUID();
  }
  return `debug-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 10)}`;
}

export function ensureDebugClientId() {
  if (debugClientId) return debugClientId;

  try {
    const stored = window.sessionStorage.getItem(DEBUG_CLIENT_STORAGE_KEY)
      || window.sessionStorage.getItem(LEGACY_DEBUG_CLIENT_STORAGE_KEY);
    if (stored) {
      debugClientId = stored;
      window.sessionStorage.setItem(DEBUG_CLIENT_STORAGE_KEY, debugClientId);
      return debugClientId;
    }
  } catch (_) {}

  debugClientId = createDebugClientId();

  try {
    window.sessionStorage.setItem(DEBUG_CLIENT_STORAGE_KEY, debugClientId);
  } catch (_) {}

  return debugClientId;
}

// ===== Node Registry =====
export let nodeIdCounter = 0;
export function nextNodeId() { return ++nodeIdCounter; }
export function setNodeIdCounter(val) { nodeIdCounter = val; }
export const nodeRegistry = {};
export let connections = [];
export function setConnections(c) { connections = c; }

// ===== Canvas State =====
export const canvasState = {
  panX: 0, panY: 0, isPanning: false, startPanX: 0, startPanY: 0,
};
export let scale = 1.0;
export function setScale(s) { scale = s; }
export const MIN_SCALE = 0.005;
export const MAX_SCALE = 3.0;
export const ZOOM_SENSITIVITY = 0.0048;

export const dragState = {
  isDraggingNode: false, draggedNode: null, nodeOffsetX: 0, nodeOffsetY: 0,
};
export const wiringState = {
  isWiring: false, wireStartSocket: null, wireStartIsOutput: true, tempWire: null,
};
export const resizeState = {
  isResizing: false, resizeNode: null, resizeStartX: 0, resizeStartY: 0, resizeStartW: 0, resizeStartH: 0,
};

// ===== Port Types & Validation =====
export const PORT_TYPES = {
  reactions: 'reactions',
  model: 'model',
  params: 'params',
  result: 'result',
  'atlas-spec': 'atlas-spec',
  atlas: 'atlas',
  'atlas-query': 'atlas-query',
};

export const PORT_COLOR_GROUPS = {
  reactions: 'reactions',
  model: 'model',
  params: 'params',
  result: 'result',
  'atlas-spec': 'params',
  atlas: 'model',
  'atlas-query': 'params',
};

export function getPortColor(port) {
  const group = PORT_COLOR_GROUPS[port] || port;
  const style = getComputedStyle(document.documentElement);
  return style.getPropertyValue(`--port-${group}`)?.trim() || '#888';
}

// ===== Observer Registries =====
export const plotResizeObservers = new Map();
export const nodeResizeObservers = new Map();
export const plotInteractionGuards = new WeakSet();

// ===== Atlas Constants =====
export const ATLAS_ROLE_OPTIONS = ['', 'source', 'sink', 'interior', 'branch', 'merge'];
export const ATLAS_ORDER_OPTIONS = ['', '-1', '0', '+1', '+2', '-Inf', '+Inf'];
export const ATLAS_SINGULAR_OPTIONS = ['', 'regular', 'singular'];

// ===== SISO Colors =====
export const SISO_FAMILY_COLORS = ['#ff8c42', '#2ec4b6', '#f94144', '#577590', '#f9c74f', '#8d99ae', '#90be6d', '#c77dff', '#4cc9f0', '#fb6f92'];

// ===== State Accessors =====

export function getNodeData(nodeId) {
  return nodeRegistry[nodeId]?.data || {};
}

export function getNodeInfo(nodeId) {
  return nodeRegistry[nodeId] || null;
}

export function ensureNodeData(nodeId) {
  const info = nodeRegistry[nodeId];
  if (!info) return {};
  if (!info.data) info.data = {};
  return info.data;
}

export function getNodeType(nodeId) {
  return nodeRegistry[nodeId]?.type || null;
}
