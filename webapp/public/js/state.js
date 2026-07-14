// Biocircuits Explorer — Shared State & Constants
// All mutable state is exported as objects (properties visible across modules)
// or via setter functions for primitive let bindings.

// Same-origin local backend. Browser SPA, the macOS WebView, and any local
// dev server all hit '' (same-origin) for local-only endpoints.
export const API = '';

// Cloud broker base — used only for endpoints that must reach the SaaS
// backend (Cloud Compute job submission + Cognito-driven auth). Same-origin
// by default; the macOS Swift app injects an absolute URL like
// 'https://app.yourdomain.com' via window.BIOCIRCUITS_EXPLORER_CLOUD_API so
// its WebView talks to the EC2 broker for cloud features while keeping
// instant local responses for the rest.
export const CLOUD_API = (typeof window !== 'undefined' && window.BIOCIRCUITS_EXPLORER_CLOUD_API)
  ? String(window.BIOCIRCUITS_EXPLORER_CLOUD_API).replace(/\/+$/, '')
  : '';
export const DEBUG_CLIENT_STORAGE_KEY = 'biocircuits-explorer.debug-client-id';
export const LEGACY_DEBUG_CLIENT_STORAGE_KEY = 'rop-explorer.debug-client-id';
export const CLOUD_COMPUTE_STORAGE_KEY = 'biocircuits-explorer.cloud-compute-enabled';

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
  versionFetchInFlight: false,
  buildInfo: null,
  unseenPriority: false,
};

export const cloudComputeState = {
  enabled: false,
};

export const WORKSPACE_DOCUMENT_VERSION = 2;
export const WORKSPACE_SHELL_CONTRACT_VERSION = 2;

// Runtime-only generation. It is deliberately independent of the persisted
// workspace document version: replacing/restoring a workspace advances this
// epoch so delayed async work from the previous graph cannot publish into the
// newly installed node registry.
let workspaceRuntimeEpoch = 0;
export function getWorkspaceRuntimeEpoch() { return workspaceRuntimeEpoch; }
export function advanceWorkspaceRuntimeEpoch() {
  if (!Number.isSafeInteger(workspaceRuntimeEpoch) ||
      workspaceRuntimeEpoch >= Number.MAX_SAFE_INTEGER) {
    throw new RangeError('Workspace runtime epoch exhausted');
  }
  workspaceRuntimeEpoch += 1;
  return workspaceRuntimeEpoch;
}

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
  } catch {}

  debugClientId = createDebugClientId();

  try {
    window.sessionStorage.setItem(DEBUG_CLIENT_STORAGE_KEY, debugClientId);
  } catch {}

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
export const MIN_SCALE = 0.005;
export const MAX_SCALE = 3.0;
export const MAX_CANVAS_PAN = 1_000_000_000;
export let scale = 1.0;
export function setScale(s) {
  if (!Number.isFinite(s) || s < MIN_SCALE || s > MAX_SCALE) {
    throw new RangeError(`Canvas scale must be a finite number between ${MIN_SCALE} and ${MAX_SCALE}`);
  }
  scale = s;
}
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

// ===== Port presentation =====
// Artifact types and compatibility are owned by port-types.js. This table is
// presentation-only: multiple strict artifact types may share one color.
export const PORT_COLOR_GROUPS = {
  reactions: 'reactions',
  model: 'model',
  params: 'params',
  result: 'result',
  'atlas-spec': 'params',
  atlas: 'model',
  'atlas-query': 'params',
  'atlas-network': 'params',
  'rop-shape-reference': 'params',
  'rop-shape-request': 'params',
  'rop-shape-result': 'result',
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
