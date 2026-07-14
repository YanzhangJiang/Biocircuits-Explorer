// Biocircuits Explorer — Workspace Serialization, Shell Bridge & Save/Load
import {
  state, nodeRegistry, connections, setConnections, canvasState, scale, setScale,
  workspaceShellHost, setWorkspaceShellHost, workspaceShellReady, setWorkspaceShellReady,
  workspaceShellSyncTimer, setWorkspaceShellSyncTimer,
  lastWorkspaceShellSnapshot, setLastWorkspaceShellSnapshot,
  WORKSPACE_DOCUMENT_VERSION, WORKSPACE_SHELL_CONTRACT_VERSION, themeState,
  ensureNodeData, nextNodeId, MIN_SCALE, MAX_SCALE, MAX_CANVAS_PAN,
} from './state.js';
import { dispatch, RestoreNodesCommand, undoStack } from './commands.js';
import { getSelection, setSelection } from './selection.js';
import { remapSnapshots } from './clipboard-util.js';
import { showToast, cloneSerializable, escapeHtml, syncSelectOptions } from './api.js';
import { applyThemeMode } from './theme.js';
import { isCloudComputeEnabled, setCloudComputeEnabled as setCloudComputePreference } from './cloud-compute.js';
import { applyViewportTransform } from './canvas.js';
import { updateConnections } from './connections.js';
import { normalizeRestoredConnection, validateNodeConnection } from './connection-validation.js';
import { replaceConnectionsWithModelInvalidation } from './model-lifecycle.js';
import { scheduleHistoricalScanPlot } from './execution-lifecycle.js';

// Circular-dep imports (safe: only accessed inside function bodies at call time)
import { createNode, removeNode, triggerAutoModelBuild, triggerAllAutoModelBuilds, setupPlotResize, setupPlotInteractionGuard, setupAutoUpdate, setupAutoModelBuild } from './nodes.js';
import { addReactionRow, getReactionsFromNode } from './model.js';
import { NODE_TYPES } from './node-types/index.js';
import { updateROPCloudMode, refreshROPCloudTargetOptions, renderROPCloudOutput, plotROPCloud } from './rop-cloud.js';
import { updateRegimeGraphMode, plotRegimeGraph } from './regime-graph.js';
import { plotTrajectory, plotHeatmap } from './plotting.js';
import { plotParameterScan1D, plotParameterScan2D, plotROPPolyhedron, renderROPPolyhedronOutput, updateScan1DConfig, updateScan2DConfig, updateROPPolyConfig, updateROPPolyDimension } from './scan.js';
import { plotQKPolyhedron, renderQKPolyhedronResult, renderBehaviorFamiliesResult, normalizeSISOConfig } from './siso.js';
import { renderAtlasBuilderResult, renderAtlasQueryResult, renderAtlasInverseDesignResult, hydrateAtlasResultContent, readAtlasNetworkDefinitionState, renderAtlasNetworkDefinitionPreview, readAtlasSpecEditorState, readAtlasQueryEditorState, refreshAtlasQueryDesigner, restoreAtlasQueryBuilderState, collectAtlasRegimeRows, collectAtlasTransitionRows, readAtlasQueryBuilderState, clearAtlasBuilderRows, addAtlasBuilderRow } from './atlas.js';
import { runAllConnectedWorkspace, runConnectedWorkspace } from './nodes.js';
import { serializeNodeBySchema, restoreNodeBySchema, NODE_SCHEMAS } from './node-schema.js';

// ===== Shell Metadata & Validation =====

export function workspaceShellMetadata() {
  return {
    contractVersion: WORKSPACE_SHELL_CONTRACT_VERSION,
    workspaceVersion: WORKSPACE_DOCUMENT_VERSION,
    schemaVersion: WORKSPACE_DOCUMENT_VERSION,
  };
}

export function validateWorkspaceDocument(data) {
  if (!data || typeof data !== 'object' || Array.isArray(data)) {
    throw new Error('Workspace document must be an object');
  }

  const version = data.version == null ? WORKSPACE_DOCUMENT_VERSION : data.version;
  if (!Number.isInteger(version)) {
    throw new Error('Workspace version must be an integer');
  }
  if (version < 1) {
    throw new Error(`Unsupported workspace version: ${version}`);
  }
  if (version > WORKSPACE_DOCUMENT_VERSION) {
    throw new Error(`Workspace version ${version} is newer than this app supports (${WORKSPACE_DOCUMENT_VERSION})`);
  }
  if (!Array.isArray(data.nodes)) {
    throw new Error('Workspace document is missing a nodes array');
  }
  if (data.connections != null && !Array.isArray(data.connections)) {
    throw new Error('Workspace document has an invalid connections array');
  }

  const sourceCanvas = data.canvas == null ? {} : data.canvas;
  if (!sourceCanvas || typeof sourceCanvas !== 'object' || Array.isArray(sourceCanvas)) {
    throw new Error('Workspace document has an invalid canvas object');
  }

  const finiteNumber = (value, path, fallback) => {
    if (value == null) return fallback;
    if (typeof value !== 'number' || !Number.isFinite(value)) {
      throw new Error(`${path} must be a finite number`);
    }
    return value;
  };

  const canvas = {
    ...sourceCanvas,
    panX: finiteNumber(sourceCanvas.panX, 'canvas.panX', 0),
    panY: finiteNumber(sourceCanvas.panY, 'canvas.panY', 0),
    scale: finiteNumber(sourceCanvas.scale, 'canvas.scale', 1),
  };
  if (canvas.scale < MIN_SCALE || canvas.scale > MAX_SCALE) {
    throw new Error(`canvas.scale must be between ${MIN_SCALE} and ${MAX_SCALE}`);
  }
  if (Math.abs(canvas.panX) > MAX_CANVAS_PAN || Math.abs(canvas.panY) > MAX_CANVAS_PAN) {
    throw new Error(`canvas pan must be between -${MAX_CANVAS_PAN} and ${MAX_CANVAS_PAN}`);
  }

  const seenNodeIds = new Set();
  const nodes = data.nodes.map((saved, index) => {
    const path = `nodes[${index}]`;
    if (!saved || typeof saved !== 'object' || Array.isArray(saved)) {
      throw new Error(`${path} must be an object`);
    }
    if (typeof saved.id !== 'string' || !saved.id.trim()) {
      throw new Error(`${path}.id must be a non-empty string`);
    }
    if (seenNodeIds.has(saved.id)) {
      throw new Error(`${path}.id duplicates workspace node ${saved.id}`);
    }
    seenNodeIds.add(saved.id);

    if (typeof saved.type !== 'string' || !Object.hasOwn(NODE_TYPES, saved.type)) {
      throw new Error(`${path}.type is not a supported node type: ${String(saved.type)}`);
    }
    if (saved.data != null && (typeof saved.data !== 'object' || Array.isArray(saved.data))) {
      throw new Error(`${path}.data must be an object`);
    }

    const normalized = {
      ...saved,
      x: finiteNumber(saved.x, `${path}.x`, 0),
      y: finiteNumber(saved.y, `${path}.y`, 0),
      data: saved.data || {},
    };
    for (const dimension of ['width', 'height']) {
      if (saved[dimension] == null) continue;
      const value = finiteNumber(saved[dimension], `${path}.${dimension}`, null);
      if (value < 0) throw new Error(`${path}.${dimension} must not be negative`);
      if (value === 0) delete normalized[dimension];
      else normalized[dimension] = value;
    }
    return normalized;
  });

  return {
    ...data,
    version,
    canvas,
    nodes,
    connections: Array.isArray(data.connections) ? data.connections : [],
  };
}

// ===== Shell Sync Queue =====

export function queueWorkspaceShellSync(reason = 'unknown') {
  clearTimeout(workspaceShellSyncTimer);
  setWorkspaceShellSyncTimer(window.setTimeout(() => {
    (window.BiocircuitsExplorerWorkspaceShell || window.ROPWorkspaceShell)?.notifyWorkspaceChanged(reason);
  }, 250));
}

export function commitWorkspaceSnapshot(reason = 'unknown') {
  clearTimeout(workspaceShellSyncTimer);
  setWorkspaceShellSyncTimer(null);
  return (window.BiocircuitsExplorerWorkspaceShell || window.ROPWorkspaceShell)?.notifyWorkspaceChanged(reason) ?? false;
}

// ===== BiocircuitsExplorerWorkspaceShell Initializer =====

export function initWorkspaceShell() {
  const workspaceShell = {
    ...workspaceShellMetadata(),

    registerHost(host) {
      setWorkspaceShellHost(host || null);

      if (workspaceShellReady) {
        workspaceShellHost?.shellDidBecomeReady?.(workspaceShellMetadata());
      }

      return workspaceShellMetadata();
    },

    unregisterHost() {
      setWorkspaceShellHost(null);
    },

    markReady() {
      if (workspaceShellReady) return;

      setWorkspaceShellReady(true);
      workspaceShellHost?.shellDidBecomeReady?.(workspaceShellMetadata());
      const detail = workspaceShellMetadata();
      window.dispatchEvent(new CustomEvent('biocircuits-explorer:workspace-shell-ready', { detail }));
      window.dispatchEvent(new CustomEvent('rop:workspace-shell-ready', { detail }));
    },

    serializeWorkspace() {
      return JSON.stringify(serializeState());
    },

    notifyWorkspaceChanged(reason = 'unknown') {
      const jsonString = this.serializeWorkspace();
      if (!jsonString || jsonString === lastWorkspaceShellSnapshot) {
        return false;
      }

      setLastWorkspaceShellSnapshot(jsonString);
      workspaceShellHost?.workspaceDidChange?.(jsonString, {
        reason,
        ...workspaceShellMetadata(),
      });
      const detail = {
        reason,
        jsonString,
        ...workspaceShellMetadata(),
      };
      window.dispatchEvent(new CustomEvent('biocircuits-explorer:workspace-changed', { detail }));
      window.dispatchEvent(new CustomEvent('rop:workspace-changed', { detail }));
      return true;
    },

    applyWorkspaceFromJSONString(jsonString) {
      const data = validateWorkspaceDocument(JSON.parse(jsonString));

      applyState(data);
      // Applying the staged document is the commit point. Serialization is a
      // follow-up snapshot optimization and must not turn a committed workspace
      // into an apparent apply failure for native hosts (which would leave the
      // visible document and its project identity pointing at different files).
      let appliedSnapshot = jsonString;
      try {
        appliedSnapshot = this.serializeWorkspace() || jsonString;
      } catch (error) {
        console.warn('[workspace] Workspace applied, but snapshot serialization failed', error);
      }
      setLastWorkspaceShellSnapshot(appliedSnapshot);
      return true;
    },

    saveWorkspace() {
      if (workspaceShellHost?.saveWorkspaceJSONString) {
        const jsonString = this.serializeWorkspace();
        setLastWorkspaceShellSnapshot(jsonString);
        workspaceShellHost.saveWorkspaceJSONString(jsonString);
        showToast('Saved to the current JSON project');
        return true;
      }

      return defaultSaveState();
    },

    loadWorkspace() {
      if (workspaceShellHost?.requestCurrentWorkspace) {
        workspaceShellHost.requestCurrentWorkspace();
        showToast('Reloaded from the selected JSON project');
        return true;
      }

      return defaultLoadState();
    },

    setThemeMode(mode, effectiveThemeOverride = null) {
      void applyThemeMode(mode, { effectiveThemeOverride });
      return true;
    },

    getThemeMode() {
      return themeState.mode;
    },

    setCloudComputeEnabled(enabled) {
      setCloudComputePreference(!!enabled);
      return true;
    },

    getCloudComputeEnabled() {
      return isCloudComputeEnabled();
    },

    async runConnectedWorkspace() {
      return runConnectedWorkspace();
    },

    async runAllConnectedWorkspace() {
      return runAllConnectedWorkspace();
    },
  };
  window.BiocircuitsExplorerWorkspaceShell = workspaceShell;
  window.ROPWorkspaceShell = workspaceShell;
}

// ===== Node Serial Data =====

export function getNodeSerialData(nodeId, type) {
  // Schema-based serialization handles most node types declaratively
  if (NODE_SCHEMAS[type]) {
    return serializeNodeBySchema(nodeId, type);
  }
  // Custom serialization for types that need special logic
  switch (type) {
    case 'sbml-import':       // same reactions-list structure as reaction-network
    case 'reaction-network': {
      const { reactions, kds } = getReactionsFromNode(nodeId);
      return { reactions: reactions.map((rule, i) => ({ rule, kd: kds[i] })) };
    }
    case 'network-id-definition':
      return readAtlasNetworkDefinitionState(nodeId);
    case 'atlas-spec':
      return readAtlasSpecEditorState(nodeId);
    case 'atlas-query-config':
      return readAtlasQueryEditorState(nodeId);
    case 'design-target': {
      // Persist the selected candidate network (config.resolvedDefinition.raw_rules +
      // selectedNid) so a save/reload doesn't drop it and downstream model-builders
      // keep their reaction source. The search panel is transient (out of scope).
      const info = nodeRegistry[nodeId];
      const cfg = info?.data?.config;
      return cfg ? { config: cloneSerializable(cfg) } : {};
    }
    default:
      return {};
  }
}

// ===== Serialize / Deserialize =====

function serializedNodeDimension(el, type, dimension) {
  const measured = dimension === 'width' ? el.offsetWidth : el.offsetHeight;
  if (Number.isFinite(measured) && measured > 0) return measured;

  const inline = Number.parseFloat(el.style?.[dimension]);
  if (Number.isFinite(inline) && inline > 0) return inline;

  const fallback = dimension === 'width'
    ? NODE_TYPES[type]?.defaultWidth
    : NODE_TYPES[type]?.defaultHeight;
  return Number.isFinite(fallback) && fallback > 0 ? fallback : undefined;
}

export function serializeState() {
  const nodes = [];
  for (const [id, info] of Object.entries(nodeRegistry)) {
    const el = document.getElementById(id);
    if (!el) continue;
    nodes.push({
      id,
      type: info.type,
      x: parseFloat(el.style.left) || 0,
      y: parseFloat(el.style.top) || 0,
      width: serializedNodeDimension(el, info.type, 'width'),
      height: serializedNodeDimension(el, info.type, 'height'),
      data: getNodeSerialData(id, info.type),
    });
  }
  return {
    version: WORKSPACE_DOCUMENT_VERSION,
    timestamp: new Date().toISOString(),
    canvas: { panX: canvasState.panX, panY: canvasState.panY, scale },
    nodes,
    connections: connections.map(c => ({ ...c })),
    // The Design-Agent conversation rides with the document → one project = one
    // workspace + one conversation. (undefined when the agent surface isn't loaded.)
    designAgent: (typeof window !== 'undefined' && window.getDesignAgentConversation)
      ? window.getDesignAgentConversation() : undefined,
  };
}

// ===== Single-node snapshot / restore (undo of delete, copy/paste) =====

// Capture everything needed to recreate one node exactly: its type,
// geometry, serialized data, and the wires touching it. Used by
// RemoveNodeCommand (so delete is undoable) and by clipboard copy.
export function snapshotNode(nodeId) {
  const info = nodeRegistry[nodeId];
  const el = document.getElementById(nodeId);
  if (!info || !el) return null;
  return {
    id: nodeId,
    type: info.type,
    x: parseFloat(el.style.left) || 0,
    y: parseFloat(el.style.top) || 0,
    width: serializedNodeDimension(el, info.type, 'width'),
    height: serializedNodeDimension(el, info.type, 'height'),
    data: getNodeSerialData(nodeId, info.type),
    connections: connections
      .filter(c => c.fromNode === nodeId || c.toNode === nodeId)
      .map(c => ({ ...c })),
  };
}

// Recreate a node from a snapshot, reusing its original id so other undo
// entries and wires stay valid. Incident connections are re-added (skipping
// any duplicate). Connections whose other endpoint is missing are harmless:
// updateConnections() only draws wires whose both sockets exist.
export function restoreNode(snapshot) {
  if (!snapshot) return null;
  const newId = createNode(snapshot.type, snapshot.x, snapshot.y, { id: snapshot.id });
  if (!newId) return null;
  const el = document.getElementById(newId);
  if (el && snapshot.width) el.style.width = `${snapshot.width}px`;
  if (el && snapshot.height) el.style.height = `${snapshot.height}px`;
  restoreNodeData(newId, snapshot.type, snapshot.data || {});
  const nextConnections = connections.map(conn => ({ ...conn }));
  for (const conn of (snapshot.connections || [])) {
    const dup = nextConnections.some(c =>
      c.fromNode === conn.fromNode && c.fromPort === conn.fromPort &&
      c.toNode === conn.toNode && c.toPort === conn.toPort);
    if (dup) continue;
    // Restore/paste/Undo use the same endpoint resolver as interactive wiring.
    // A sibling pasted node may not exist yet; its duplicate snapshot will
    // retry the internal wire after both endpoints have been recreated.
    if (!nodeRegistry[conn.fromNode] || !nodeRegistry[conn.toNode]) continue;
    const validation = validateNodeConnection(conn, nodeRegistry, NODE_TYPES);
    if (!validation.ok) {
      console.warn(`[workspace] Dropped invalid restored connection: ${validation.message}`);
      continue;
    }
    nextConnections.push({ ...conn });
  }
  replaceConnectionsWithModelInvalidation(nextConnections, 'node-restored');
  updateConnections();
  return newId;
}

// ===== Clipboard (copy / paste / duplicate) =====

const PASTE_OFFSET = 40;
let _clipboard = null;   // { snapshots: [...] }

// Recreate `sourceSnapshots` with fresh ids, offset position, and remapped
// internal wires, as one undoable step; the pasted nodes become the
// selection. The pure remap lives in clipboard-util.js.
function pasteSnapshots(sourceSnapshots, offset) {
  if (!sourceSnapshots || sourceSnapshots.length === 0) return;
  const idMap = {};
  for (const s of sourceSnapshots) idMap[s.id] = `node-${nextNodeId()}`;
  const newSnaps = remapSnapshots(sourceSnapshots, idMap, offset);
  dispatch(new RestoreNodesCommand({ snapshots: newSnaps, label: `Paste ${newSnaps.length} node(s)` }));
  setSelection(newSnaps.map((s) => s.id));
}

export function copySelection() {
  const ids = getSelection().filter((id) => document.getElementById(id));
  if (ids.length === 0) return 0;
  _clipboard = { snapshots: ids.map(snapshotNode).filter(Boolean) };
  return _clipboard.snapshots.length;
}

export function pasteClipboard(offset = PASTE_OFFSET) {
  if (_clipboard) pasteSnapshots(_clipboard.snapshots, offset);
}

export function duplicateSelection(offset = PASTE_OFFSET) {
  const ids = getSelection().filter((id) => document.getElementById(id));
  if (ids.length === 0) return;
  pasteSnapshots(ids.map(snapshotNode).filter(Boolean), offset);
}

export function defaultSaveState() {
  const data = serializeState();
  const blob = new Blob([JSON.stringify(data, null, 2)], { type: 'application/json' });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = `biocircuits-explorer-workspace-${Date.now()}.json`;
  a.click();
  URL.revokeObjectURL(url);
  showToast('Workspace saved');
  setLastWorkspaceShellSnapshot((window.BiocircuitsExplorerWorkspaceShell || window.ROPWorkspaceShell).serializeWorkspace());
  return true;
}

export function saveState() {
  return (window.BiocircuitsExplorerWorkspaceShell || window.ROPWorkspaceShell).saveWorkspace();
}

export function defaultLoadState() {
  const input = document.createElement('input');
  input.type = 'file';
  input.accept = '.json';
  input.onchange = (e) => {
    const file = e.target.files[0];
    if (!file) return;
    const reader = new FileReader();
    reader.onload = (ev) => {
      try {
        const data = validateWorkspaceDocument(JSON.parse(ev.target.result));
        applyState(data);
        setLastWorkspaceShellSnapshot((window.BiocircuitsExplorerWorkspaceShell || window.ROPWorkspaceShell).serializeWorkspace());
        showToast('Workspace loaded');
      } catch (err) {
        showToast('Failed to load: ' + err.message);
      }
    };
    reader.readAsText(file);
  };
  input.click();
  return true;
}

export function loadState() {
  return (window.BiocircuitsExplorerWorkspaceShell || window.ROPWorkspaceShell).loadWorkspace();
}

function removeStagedNodes(stagedIds) {
  for (const id of [...stagedIds].reverse()) {
    try {
      if (nodeRegistry[id]) removeNode(id);
      else document.getElementById(id)?.remove();
    } catch (error) {
      console.warn(`[workspace] Failed to clean staged node ${id}`, error);
    }
  }
}

// Build the replacement nodes while the current workspace remains live. The
// staged elements stay hidden and have no restored wires until every create and
// restore hook succeeds. This turns hook failures into a no-op for user data.
function stageWorkspaceRestore(data, existingNodeIds) {
  const reservedIds = new Set(existingNodeIds);
  const stagedIds = [];
  const stagedNodes = [];
  const idMap = {}; // oldId -> newId
  const savedTypeById = {};
  const restoredConnections = [];
  let droppedConnections = 0;

  try {
    for (const saved of data.nodes) {
      savedTypeById[saved.id] = saved.type;

      let stagedId;
      do stagedId = `node-${nextNodeId()}`;
      while (reservedIds.has(stagedId));
      reservedIds.add(stagedId);
      stagedIds.push(stagedId);

      const newId = createNode(saved.type, saved.x, saved.y, { id: stagedId });
      if (!newId) throw new Error(`Failed to create node ${saved.id} (${saved.type})`);
      idMap[saved.id] = newId;
      stagedNodes.push({ id: newId, type: saved.type, data: saved.data });

      const el = document.getElementById(newId);
      if (!el) throw new Error(`Restored node ${saved.id} did not create a DOM element`);
      el.style.visibility = 'hidden';
      if (saved.width != null) el.style.width = `${saved.width}px`;
      if (saved.height != null) el.style.height = `${saved.height}px`;

      restoreNodeData(newId, saved.type, saved.data, { deferEffects: true });
    }

    for (const conn of data.connections) {
      const restored = normalizeRestoredConnection(conn, idMap, savedTypeById, NODE_TYPES);
      if (restored) restoredConnections.push(restored);
      else droppedConnections += 1;
    }
  } catch (error) {
    removeStagedNodes(stagedIds);
    // Keep allocated IDs consumed: a node onInit hook may have queued work
    // before throwing, and reusing its ID could target an unrelated later node.
    throw error;
  }

  return { stagedIds, stagedNodes, restoredConnections, droppedConnections };
}

function runPostRestoreHook(label, callback) {
  try {
    callback();
  } catch (error) {
    console.warn(`[workspace] ${label} failed after the workspace was restored`, error);
  }
}

export function applyState(data) {
  data = validateWorkspaceDocument(data);
  const existingNodeIds = Object.keys(nodeRegistry);
  const { stagedIds, stagedNodes, restoredConnections, droppedConnections } =
    stageWorkspaceRestore(data, existingNodeIds);

  // Staging succeeded: the remaining operations commit the prepared document.
  for (const id of existingNodeIds) removeNode(id);
  setConnections(restoredConnections);
  document.getElementById('svg-layer')?.querySelectorAll('.wire').forEach(w => w.remove());

  state.sessionId = null;
  state.model = null;
  state.qK_syms = [];

  canvasState.panX = data.canvas.panX;
  canvasState.panY = data.canvas.panY;
  setScale(data.canvas.scale);
  applyViewportTransform();

  for (const id of stagedIds) {
    const el = document.getElementById(id);
    if (el) el.style.visibility = '';
  }

  undoStack.clear();
  setSelection([]);

  for (const staged of stagedNodes) {
    runPostRestoreHook(`runtime restore for ${staged.id}`, () => {
      restoreCachedNodeRuntime(staged.id, staged.type, staged.data);
    });
  }

  if (droppedConnections > 0) {
    console.warn(`[workspace] Dropped ${droppedConnections} invalid connection(s) while loading workspace`);
    showToast(`Dropped ${droppedConnections} invalid connection${droppedConnections === 1 ? '' : 's'} while loading`);
  }

  if (typeof window !== 'undefined' && window.setDesignAgentConversation) {
    runPostRestoreHook('Design Agent conversation restore', () => {
      window.setDesignAgentConversation(data.designAgent || null);
    });
  }

  runPostRestoreHook('connection redraw', updateConnections);
  runPostRestoreHook('automatic model rebuild', triggerAllAutoModelBuilds);

  for (const [id, info] of Object.entries(nodeRegistry)) {
    if (info.type !== 'rop-cloud' && info.type !== 'rop-cloud-params') continue;
    runPostRestoreHook(`ROP cloud refresh for ${id}`, () => {
      updateROPCloudMode(id);
      const savedTarget = info.data?.targetSpecies;
      const sel = document.getElementById(`${id}-target-species`);
      if (sel && savedTarget && Array.from(sel.options).some(o => o.value === savedTarget)) {
        sel.value = savedTarget;
      }
    });
  }

  return true;
}

// ===== Restore Node Data =====

export function restoreNodeData(nodeId, type, data, { deferEffects = false } = {}) {
  // Schema-based restore handles most node types declaratively
  if (NODE_SCHEMAS[type]) {
    // Schema hooks parse and apply persisted fields, so they are part of the
    // staging contract and must succeed before the old workspace is removed.
    restoreNodeBySchema(nodeId, type, data);
    if (!deferEffects) restoreCachedNodeRuntime(nodeId, type, data);
    return;
  }
  // Custom restore for types that need special logic
  switch (type) {
    case 'sbml-import':       // restored exactly like reaction-network
    case 'reaction-network': {
      const list = document.getElementById(`${nodeId}-reactions-list`);
      if (list) list.innerHTML = '';
      if (data.reactions && data.reactions.length > 0) {
        data.reactions.forEach(r => addReactionRow(nodeId, r.rule, r.kd));
      }
      break;
    }
    case 'network-id-definition': {
      const el = document.getElementById(`${nodeId}-network-id`);
      if (el && data.networkId != null) el.value = data.networkId;
      if (nodeRegistry[nodeId]) {
        nodeRegistry[nodeId].data = nodeRegistry[nodeId].data || {};
        nodeRegistry[nodeId].data.config = readAtlasNetworkDefinitionState(nodeId);
      }
      renderAtlasNetworkDefinitionPreview(nodeId);
      break;
    }
    case 'design-target': {
      // Restore the selected candidate network and re-emit it so a wired model-builder
      // rebuilds. Guarded: only re-trigger when a network was actually selected, so
      // we don't fire an empty rebuild on a fresh node.
      if (data.config && nodeRegistry[nodeId]) {
        nodeRegistry[nodeId].data = nodeRegistry[nodeId].data || {};
        nodeRegistry[nodeId].data.config = data.config;
        if (!deferEffects && data.config?.resolvedDefinition?.raw_rules) triggerAutoModelBuild(nodeId);
      }
      break;
    }
    case 'atlas-spec': {
      // Atlas spec has many fields — restore via DOM element IDs
      const fieldMap = {
        sourceLabel: 'source-label', libraryLabel: 'library-label',
        sqlitePath: 'sqlite-path', profileName: 'profile-name',
        pathScope: 'path-scope', enumerationMode: 'enum-mode',
        baseSpeciesCountsText: 'base-species-counts',
        explicitNetworksText: 'explicit-networks',
      };
      const intFields = {
        maxBaseSpecies: 'max-base-species', maxReactions: 'max-reactions',
        maxSupport: 'max-support', minEnumerationReactions: 'min-enum-reactions',
        maxEnumerationReactions: 'max-enum-reactions', enumerationLimit: 'enum-limit',
      };
      const floatFields = { minVolumeMean: 'min-volume', logqkMin: 'logqk-min', logqkMax: 'logqk-max' };
      const boolFields = {
        persistSqlite: 'persist-sqlite', skipExisting: 'skip-existing',
        keepSingular: 'keep-singular', keepNonasymptotic: 'keep-nonasym',
        includePathRecords: 'include-path-records', enableEnumeration: 'enable-enumeration',
      };
      for (const [key, suffix] of Object.entries(fieldMap)) {
        const el = document.getElementById(`${nodeId}-${suffix}`);
        if (el && data[key] != null) el.value = data[key];
      }
      for (const [key, suffix] of Object.entries(intFields)) {
        const el = document.getElementById(`${nodeId}-${suffix}`);
        if (el && data[key] != null) el.value = data[key];
      }
      for (const [key, suffix] of Object.entries(floatFields)) {
        const el = document.getElementById(`${nodeId}-${suffix}`);
        if (el && data[key] != null) el.value = data[key];
      }
      for (const [key, suffix] of Object.entries(boolFields)) {
        const el = document.getElementById(`${nodeId}-${suffix}`);
        if (el && data[key] != null) el.checked = data[key];
      }
      if (data.config && nodeRegistry[nodeId]) nodeRegistry[nodeId].data.config = data.config;
      break;
    }
    case 'atlas-query-config': {
      // Atlas query config has many fields
      const fieldMap = {
        sqlitePath: 'query-sqlite-path', goalIoText: 'goal-io',
        goalMotifText: 'goal-motif', goalExactText: 'goal-exact',
        goalWitnessText: 'goal-witness', goalTransitionsText: 'goal-transitions',
        goalForbidRegimesText: 'goal-forbid-regimes',
        motifLabelsText: 'motif-labels', motifMatchMode: 'motif-match-mode',
        exactLabelsText: 'exact-labels', exactMatchMode: 'exact-match-mode',
        inputSymbolsText: 'input-symbols', outputSymbolsText: 'output-symbols',
        rankingMode: 'ranking-mode', inverseSourceLabel: 'inverse-source-label',
        requiredRegimesText: 'required-regimes', forbiddenRegimesText: 'forbidden-regimes',
        requiredTransitionsText: 'required-transitions', forbiddenTransitionsText: 'forbidden-transitions',
        requiredPathSequencesText: 'required-path-sequences',
      };
      const intFields = {
        minRobustPathCount: 'min-robust-path-count',
        maxBaseSpecies: 'query-max-base-species', maxReactions: 'query-max-reactions',
        maxSupport: 'query-max-support', maxSupportMass: 'query-max-support-mass',
        maxWitnessPathLength: 'max-witness-path-length', limit: 'query-limit',
        refinementTopK: 'refinement-top-k', refinementTrials: 'refinement-trials',
        refinementNPoints: 'refinement-n-points',
      };
      const floatFields = { goalMinVolumeMean: 'goal-min-volume', minWitnessVolumeMean: 'min-witness-volume-mean' };
      const boolFields = {
        preferPersistedAtlas: 'prefer-persisted-atlas',
        goalRobust: 'goal-robust', goalFeasible: 'goal-feasible',
        requireRobust: 'require-robust',
        forbidSingularOnWitness: 'forbid-singular-on-witness',
        requireWitnessFeasible: 'require-witness-feasible',
        requireWitnessRobust: 'require-witness-robust',
        collapseByNetwork: 'collapse-by-network', paretoOnly: 'pareto-only',
        inverseSkipExisting: 'inverse-skip-existing',
        inverseBuildLibraryIfMissing: 'inverse-build-library-if-missing',
        allowDuplicateAtlas: 'allow-duplicate-atlas',
        refinementEnabled: 'refinement-enabled',
        refinementIncludeTraces: 'refinement-include-traces',
        refinementRerank: 'refinement-rerank',
      };
      for (const [key, suffix] of Object.entries(fieldMap)) {
        const el = document.getElementById(`${nodeId}-${suffix}`);
        if (el && data[key] != null) el.value = data[key];
      }
      for (const [key, suffix] of Object.entries(intFields)) {
        const el = document.getElementById(`${nodeId}-${suffix}`);
        if (el && data[key] != null) el.value = data[key];
      }
      for (const [key, suffix] of Object.entries(floatFields)) {
        const el = document.getElementById(`${nodeId}-${suffix}`);
        if (el && data[key] != null) el.value = data[key];
      }
      for (const [key, suffix] of Object.entries(boolFields)) {
        const el = document.getElementById(`${nodeId}-${suffix}`);
        if (el && data[key] != null) el.checked = data[key];
      }
      restoreAtlasQueryBuilderState(nodeId, data);
      refreshAtlasQueryDesigner(nodeId);
      if (data.config && nodeRegistry[nodeId]) nodeRegistry[nodeId].data.config = data.config;
      break;
    }
  }

  if (!deferEffects) restoreCachedNodeRuntime(nodeId, type, data);
}

// ===== Restore Cached Runtime State =====

export function restoreCachedNodeRuntime(nodeId, type, data) {
  const info = nodeRegistry[nodeId];
  if (!info) return;
  const nd = ensureNodeData(nodeId);

  switch (type) {
    case 'model-builder': {
      if (!data.modelContext) break;
      const cachedModel = data.modelContext.model || null;
      const cachedQKSymbols = Array.isArray(data.modelContext.qK_syms) && data.modelContext.qK_syms.length
        ? data.modelContext.qK_syms
        : cachedModel ? [...(cachedModel.q_sym || []), ...(cachedModel.K_sym || [])] : [];
      info.data.built = false;
      info.data.modelContext = cachedModel ? {
        ...data.modelContext,
        sessionId: null,
        model: cachedModel,
        qK_syms: cachedQKSymbols,
      } : null;
      const infoEl = document.getElementById(`${nodeId}-model-info`);
      const infoText = document.getElementById(`${nodeId}-model-info-text`);
      if (infoEl && infoText && cachedModel) {
        infoEl.style.display = '';
        infoText.textContent = `n=${cachedModel.n}, d=${cachedModel.d}, r=${cachedModel.r}\nSpecies: ${cachedModel.x_sym.join(', ')}\nTotals: ${cachedModel.q_sym.join(', ')}\nConstants: ${cachedModel.K_sym.join(', ')}\n\nReloaded workspace: run Model Builder to refresh the backend session.`;
      }
      break;
    }
    case 'regime-graph': {
      if (!data.graphData) break;
      info.data.graphData = data.graphData;
      const contentEl = document.getElementById(`${nodeId}-content`);
      if (!contentEl) break;
      contentEl.innerHTML = `<div class="plot-container" id="${nodeId}-plot"></div>`;
      setTimeout(() => {
        plotRegimeGraph(data.graphData, `${nodeId}-plot`, { viewMode: info.data.config?.viewMode || '3d' });
        setupPlotResize(nodeId, `${nodeId}-plot`);
      }, 50);
      break;
    }
    case 'siso-result': {
      info.data.selectedPath = data.selectedPath || null;
      info.data.behaviorData = data.behaviorData || null;
      info.data.trajectoryData = data.trajectoryData || null;
      const contentEl = document.getElementById(`${nodeId}-content`);
      if (!contentEl || !data.behaviorData) break;
      const changeQK = data.behaviorData.change_qK || data.selectedPath?.change_qK || '';
      contentEl.innerHTML = renderBehaviorFamiliesResult(nodeId, changeQK, data.behaviorData);
      if (data.selectedPath?.path_idx != null) {
        contentEl.querySelectorAll('.path-item, .path-chip').forEach(item => {
          const currentIdx = parseInt(item.dataset.pathIdx || item.dataset.idx, 10);
          item.classList.toggle('selected', currentIdx === data.selectedPath.path_idx);
        });
      }
      if (data.trajectoryData && document.getElementById(`${nodeId}-traj-plot`)) {
        const plotEl = document.getElementById(`${nodeId}-traj-plot`);
        if (plotEl) plotEl.style.display = '';
        plotTrajectory(data.trajectoryData, `${nodeId}-traj-plot`);
      }
      break;
    }
    case 'qk-poly-result': {
      if (!data.polyhedronPayload || !data.selection) break;
      info.data.selection = data.selection;
      info.data.polyhedronPayload = data.polyhedronPayload;
      const contentEl = document.getElementById(`${nodeId}-content`);
      if (!contentEl) break;
      const rendered = renderQKPolyhedronResult(nodeId, data.selection, data.polyhedronPayload);
      contentEl.innerHTML = rendered.html;
      if (rendered.canPlot) {
        setTimeout(() => {
          plotQKPolyhedron(data.polyhedronPayload.polyhedra?.[0], data.polyhedronPayload.qk_symbols || [], `${nodeId}-plot`);
          setupPlotResize(nodeId, `${nodeId}-plot`);
        }, 50);
      }
      break;
    }
    case 'rop-cloud':
    case 'rop-cloud-result': {
      if (!data.ropCloudData) break;
      info.data.ropCloudData = data.ropCloudData;
      info.data.ropCloudPreset = data.ropCloudPreset || 'robust';
      info.data.ropCloudRanges = data.ropCloudRanges || null;
      const contentEl = document.getElementById(`${nodeId}-content`);
      if (!contentEl) break;
      renderROPCloudOutput(nodeId, contentEl, data.ropCloudData);
      break;
    }
    case 'fret-heatmap':
    case 'fret-result': {
      if (!data.fretHeatmapData) break;
      info.data.fretHeatmapData = data.fretHeatmapData;
      const contentEl = document.getElementById(`${nodeId}-content`);
      if (!contentEl) break;
      contentEl.innerHTML = `<div class="plot-container" id="${nodeId}-plot"></div>`;
      setTimeout(() => {
        plotHeatmap(data.fretHeatmapData, `${nodeId}-plot`);
        setupPlotResize(nodeId, `${nodeId}-plot`);
      }, 50);
      break;
    }
    case 'parameter-scan-1d':
    case 'scan-1d-result': {
      if (!data.scan1DResult) break;
      info.data.scan1DResult = data.scan1DResult;
      info.data.scan1DResultMeta = {
        contract: 'bne-scan-execution/v1',
        ...(data.scan1DResultMeta || {}),
        historical: true,
      };
      const contentEl = document.getElementById(`${nodeId}-content`);
      if (!contentEl) break;
      contentEl.dataset.resultState = 'historical';
      contentEl.innerHTML = `
        <div class="scan-result-status text-dim">Historical saved result — rerun to verify it against the current model and inputs.</div>
        <div class="plot-container" id="${nodeId}-plot"></div>
      `;
      scheduleHistoricalScanPlot(nodeId, () => {
        plotParameterScan1D(data.scan1DResult, `${nodeId}-plot`);
        setupPlotResize(nodeId, `${nodeId}-plot`);
      });
      break;
    }
    case 'parameter-scan-2d':
    case 'scan-2d-result': {
      if (!data.scan2DResult) break;
      info.data.scan2DResult = data.scan2DResult;
      info.data.scan2DResultMeta = {
        contract: 'bne-scan-execution/v1',
        ...(data.scan2DResultMeta || {}),
        historical: true,
      };
      const contentEl = document.getElementById(`${nodeId}-content`);
      if (!contentEl) break;
      contentEl.dataset.resultState = 'historical';
      contentEl.innerHTML = `
        <div class="scan-result-status text-dim">Historical saved result — rerun to verify it against the current model and inputs.</div>
        <div class="plot-container" id="${nodeId}-plot"></div>
      `;
      scheduleHistoricalScanPlot(nodeId, () => {
        plotParameterScan2D(data.scan2DResult, `${nodeId}-plot`);
        setupPlotResize(nodeId, `${nodeId}-plot`);
      });
      break;
    }
    case 'rop-polyhedron':
    case 'rop-poly-result': {
      if (!data.ropPlotData) break;
      info.data.ropPlotData = data.ropPlotData;
      info.data.fitInnerPoints = !!data.fitInnerPoints;
      const contentEl = document.getElementById(`${nodeId}-content`);
      if (!contentEl) break;
      renderROPPolyhedronOutput(nodeId, contentEl, data.ropPlotData, data.config || {});
      const fitEl = document.getElementById(`${nodeId}-fit-inner-points`);
      if (fitEl) {
        fitEl.checked = !!data.fitInnerPoints;
        info.data.fitInnerPoints = !!data.fitInnerPoints;
      }
      break;
    }
    case 'atlas-builder': {
      if (!data.atlasData) break;
      info.data.atlasData = data.atlasData;
      info.data.lastSpec = data.lastSpec || null;
      info.data.sqlitePath = data.sqlitePath || '';
      const contentEl = document.getElementById(`${nodeId}-content`);
      if (contentEl) contentEl.innerHTML = renderAtlasBuilderResult(data.atlasData);
      hydrateAtlasResultContent(nodeId, data.atlasData);
      break;
    }
    case 'atlas-query-result': {
      if (!data.queryData) break;
      info.data.queryData = data.queryData;
      info.data.lastQuery = data.lastQuery || null;
      const contentEl = document.getElementById(`${nodeId}-content`);
      if (contentEl) contentEl.innerHTML = renderAtlasQueryResult(data.queryData);
      hydrateAtlasResultContent(nodeId, data.queryData);
      break;
    }
    case 'atlas-inverse-result': {
      if (!data.inverseDesignData) break;
      info.data.inverseDesignData = data.inverseDesignData;
      info.data.lastInverseRequest = data.lastInverseRequest || null;
      const contentEl = document.getElementById(`${nodeId}-content`);
      if (contentEl) contentEl.innerHTML = renderAtlasInverseDesignResult(data.inverseDesignData);
      hydrateAtlasResultContent(nodeId, data.inverseDesignData);
      break;
    }
    default:
      break;
  }
}

// ===== Workspace Shell Observers =====

export function installWorkspaceShellObservers() {
  const queueSync = (reason) => queueWorkspaceShellSync(reason);

  ['input', 'change', 'keyup', 'mouseup'].forEach((eventName) => {
    document.addEventListener(eventName, () => queueSync(eventName), true);
  });

  document.addEventListener('click', () => {
    window.requestAnimationFrame(() => queueSync('click'));
  }, true);

  window.setInterval(() => queueSync('poll'), 1500);
}
