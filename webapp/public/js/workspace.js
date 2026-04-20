// Biocircuits Explorer — Workspace Serialization, Shell Bridge & Save/Load
import {
  state, nodeRegistry, connections, setConnections, canvasState, scale, setScale,
  workspaceShellHost, setWorkspaceShellHost, workspaceShellReady, setWorkspaceShellReady,
  workspaceShellSyncTimer, setWorkspaceShellSyncTimer,
  lastWorkspaceShellSnapshot, setLastWorkspaceShellSnapshot,
  WORKSPACE_DOCUMENT_VERSION, WORKSPACE_SHELL_CONTRACT_VERSION, themeState,
  ensureNodeData,
} from './state.js';
import { showToast, cloneSerializable, escapeHtml, syncSelectOptions } from './api.js';
import { applyThemeMode } from './theme.js';
import { applyViewportTransform } from './canvas.js';
import { updateConnections } from './connections.js';

// Circular-dep imports (safe: only accessed inside function bodies at call time)
import { createNode, removeNode, triggerAllAutoModelBuilds, setupPlotResize, setupPlotInteractionGuard, setupAutoUpdate, setupAutoModelBuild } from './nodes.js';
import { addReactionRow, getReactionsFromNode } from './model.js';
import { NODE_TYPES } from './node-types/index.js';
import { updateROPCloudMode, refreshROPCloudTargetOptions, renderROPCloudOutput, plotROPCloud } from './rop-cloud.js';
import { updateRegimeGraphMode, plotRegimeGraph } from './regime-graph.js';
import { plotTrajectory, plotHeatmap } from './plotting.js';
import { plotParameterScan1D, plotParameterScan2D, plotROPPolyhedron, renderROPPolyhedronOutput, updateScan1DConfig, updateScan2DConfig, updateROPPolyConfig, updateROPPolyDimension } from './scan.js';
import { plotQKPolyhedron, renderQKPolyhedronResult, renderBehaviorFamiliesResult, normalizeSISOConfig } from './siso.js';
import { renderAtlasBuilderResult, renderAtlasQueryResult, renderAtlasInverseDesignResult, hydrateAtlasResultContent, readAtlasSpecEditorState, readAtlasQueryEditorState, refreshAtlasQueryDesigner, restoreAtlasQueryBuilderState, collectAtlasRegimeRows, collectAtlasTransitionRows, readAtlasQueryBuilderState, clearAtlasBuilderRows, addAtlasBuilderRow } from './atlas.js';
import { runConnectedWorkspace } from './nodes.js';
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

  const version = Number.isInteger(data.version) ? data.version : WORKSPACE_DOCUMENT_VERSION;
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

  return {
    ...data,
    version,
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
      setLastWorkspaceShellSnapshot(this.serializeWorkspace());
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

    runConnectedWorkspace() {
      void runConnectedWorkspace();
      return true;
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
    case 'reaction-network': {
      const { reactions, kds } = getReactionsFromNode(nodeId);
      return { reactions: reactions.map((rule, i) => ({ rule, kd: kds[i] })) };
    }
    case 'atlas-spec':
      return readAtlasSpecEditorState(nodeId);
    case 'atlas-query-config':
      return readAtlasQueryEditorState(nodeId);
    default:
      return {};
  }
}

// ===== Serialize / Deserialize =====

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
      width: el.offsetWidth,
      height: el.offsetHeight,
      data: getNodeSerialData(id, info.type),
    });
  }
  return {
    version: WORKSPACE_DOCUMENT_VERSION,
    timestamp: new Date().toISOString(),
    canvas: { panX: canvasState.panX, panY: canvasState.panY, scale },
    nodes,
    connections: connections.map(c => ({ ...c })),
  };
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

export function applyState(data) {
  data = validateWorkspaceDocument(data);

  // 1. Clear existing canvas
  for (const id of Object.keys(nodeRegistry)) {
    removeNode(id);
  }
  connections.length = 0;
  document.getElementById('svg-layer')?.querySelectorAll('.wire').forEach(w => w.remove());

  // Reset model state (user will need to rebuild)
  state.sessionId = null;
  state.model = null;
  state.qK_syms = [];

  // 2. Restore canvas position
  if (data.canvas) {
    canvasState.panX = data.canvas.panX || 0;
    canvasState.panY = data.canvas.panY || 0;
    setScale(data.canvas.scale || 1.0);
    applyViewportTransform();
  }

  // 3. Create nodes and build ID mapping
  const idMap = {}; // oldId -> newId
  for (const saved of data.nodes) {
    const newId = createNode(saved.type, saved.x, saved.y);
    if (!newId) continue;
    idMap[saved.id] = newId;

    // Restore width
    const el = document.getElementById(newId);
    if (el && saved.width) el.style.width = `${saved.width}px`;
    if (el && saved.height) el.style.height = `${saved.height}px`;

    // Restore node-specific data
    restoreNodeData(newId, saved.type, saved.data || {});
  }

  // 4. Restore connections (with remapped IDs)
  if (data.connections) {
    for (const conn of data.connections) {
      const fromNode = idMap[conn.fromNode];
      const toNode = idMap[conn.toNode];
      if (fromNode && toNode) {
        connections.push({
          fromNode,
          fromPort: conn.fromPort,
          toNode,
          toPort: conn.toPort,
        });
      }
    }
  }

  // 5. Update wires
  updateConnections();

  // 6. Re-run any model-builders now that their reactions connections exist again.
  triggerAllAutoModelBuilds();

  // 7. Refresh ROP cloud target options now that connections are restored
  for (const [id, info] of Object.entries(nodeRegistry)) {
    if (info.type !== 'rop-cloud' && info.type !== 'rop-cloud-params') continue;
    updateROPCloudMode(id);
    const savedTarget = info.data?.targetSpecies;
    const sel = document.getElementById(`${id}-target-species`);
    if (sel && savedTarget && Array.from(sel.options).some(o => o.value === savedTarget)) {
      sel.value = savedTarget;
    }
  }
}

// ===== Restore Node Data =====

export function restoreNodeData(nodeId, type, data) {
  // Schema-based restore handles most node types declaratively
  if (NODE_SCHEMAS[type]) {
    restoreNodeBySchema(nodeId, type, data);
    restoreCachedNodeRuntime(nodeId, type, data);
    return;
  }
  // Custom restore for types that need special logic
  switch (type) {
    case 'reaction-network': {
      const list = document.getElementById(`${nodeId}-reactions-list`);
      if (list) list.innerHTML = '';
      if (data.reactions && data.reactions.length > 0) {
        data.reactions.forEach(r => addReactionRow(nodeId, r.rule, r.kd));
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
      const floatFields = { minVolumeMean: 'min-volume' };
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

  restoreCachedNodeRuntime(nodeId, type, data);
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
      const contentEl = document.getElementById(`${nodeId}-content`);
      if (!contentEl) break;
      contentEl.innerHTML = `<div class="plot-container" id="${nodeId}-plot"></div>`;
      setTimeout(() => {
        plotParameterScan1D(data.scan1DResult, `${nodeId}-plot`);
        setupPlotResize(nodeId, `${nodeId}-plot`);
      }, 50);
      break;
    }
    case 'parameter-scan-2d':
    case 'scan-2d-result': {
      if (!data.scan2DResult) break;
      info.data.scan2DResult = data.scan2DResult;
      const contentEl = document.getElementById(`${nodeId}-content`);
      if (!contentEl) break;
      contentEl.innerHTML = `<div class="plot-container" id="${nodeId}-plot"></div>`;
      setTimeout(() => {
        plotParameterScan2D(data.scan2DResult, `${nodeId}-plot`);
        setupPlotResize(nodeId, `${nodeId}-plot`);
      }, 50);
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
