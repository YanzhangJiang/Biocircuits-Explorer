// Biocircuits Explorer — Node Edition Frontend (ES Module Entry Point)
// This is the main entry point loaded by index-node.html as <script type="module">.
// It imports all modules, exposes necessary globals for the Swift bridge,
// sets up event delegation, and runs the initialization sequence.

// ===== Module Imports =====
import { showToast } from './api.js';
import { nodeRegistry } from './state.js';
import { shouldDispatchActionForEvent } from './action-events.js';
import { initAuthUiEvents } from './auth-ui.js';
import { initCloudComputeToggleEvents, setCloudComputeEnabled, toggleCloudComputeEnabled } from './cloud-compute.js';
import { applyThemeMode, installThemeChangeObserver } from './theme.js';
import { initCanvasEvents, resetView } from './canvas.js';
import {
  initSocketEvents, updateConnections,
  addConnection, removeConnection, replaceConnections,
} from './connections.js';
import { toggleDebugConsole, initDebugConsoleEvents } from './debug-console.js';
import { NODE_TYPES, switchNoteTab } from './node-types/index.js';
import {
  addNodeFromMenu, addQuickAddChain, removeNode, runConnectedWorkspace,
  initNodeMenuEvents, setupPlotResize, setupPlotInteractionGuard,
  createNode, moveNode, deleteNodeWithHistory, setNodeAttr, initAttrHistory,
} from './nodes.js';
import { registerPerformers, initUndoKeyboard } from './commands.js';
import { initEditorUI } from './editor-ui.js';
import { buildModel, addReactionRow, triggerDownstreamNodes, getReactionsFromNode } from './model.js';
import {
  initWorkspaceShell, installWorkspaceShellObservers,
  saveState, loadState, commitWorkspaceSnapshot,
  snapshotNode, restoreNode,
} from './workspace.js';
import {
  computeSISOResult, recomputeSISO, recomputeROPCloud, recomputeHeatmap,
  plotSISOPath, selectSISOPath, toggleSISOPathCondition, executeQKPolyResult, updateSISOPlotMode, refreshSISOPlot,
  expandSISOPaths,
} from './siso.js';
import {
  executeROPCloudResult, updateROPCloudMode, refreshROPCloudPlot,
  applyROPCloudFOVPreset, updateFRETConfig, executeFRETResult,
} from './rop-cloud.js';
import { updateRegimeGraphMode, executeRegimeGraph } from './regime-graph.js';
import {
  executeScan1DResult, executeScan2DResult, runParameterScan1D, runParameterScan2D,
  insertSpecies1D, insertSpecies2D, updateROPPolyDimension,
  refreshROPPolyhedronPlot, executeROPPolyResult, runROPPolyhedron,
} from './scan.js';
import { executeAtlasBuilder, executeAtlasQueryResult, executeAtlasInverseDesignResult, addAtlasBuilderRow } from './atlas.js';
import { executePlacerResult, loadPlacerMenu, realizePlacerProgram } from './node-types/placer.js';
import {
  executeRopShapeResult,
  prepareRopShapeRequest,
  updateRopShapeIntentVisibility,
} from './node-types/rop-shape.js';
import {
  runDesignSearch,
  onDesignTargetKindChange,
  onDesignSpecKindChange,
  validateDesignSpecConfig,
} from './node-types/design-target.js';
import { importSbml, exportSbml, loadSbmlFile } from './sbml-io.js';
import { initAgentView, setNodeView } from './agent-view.js';
import './llm-settings.js';   // temporary self-mounting LLM-key panel (OpenAI/Anthropic)

// ===== Event Delegation Dispatcher =====
const ACTION_HANDLERS = {
  // Notes
  switchNoteTab: (el) => switchNoteTab(el.dataset.node, el.dataset.tab),
  // Reactions
  addReactionRow: (el) => addReactionRow(el.dataset.node),
  buildModel: (el) => buildModel(el.dataset.node),
  removeReactionRow: (el) => el.closest('.reaction-row')?.remove(),
  // SISO
  computeSISOResult: (el) => computeSISOResult(el.dataset.node),
  recomputeSISO: (el) => recomputeSISO(el.dataset.node),
  selectSISOPath: (el) => selectSISOPath(el),
  plotSISOPath: (el) => plotSISOPath(el.dataset.node, el.dataset.qk, parseInt(el.dataset.idx), el),
  toggleSISOPathCondition: (el) => toggleSISOPathCondition(el),
  toggleSISOPaths: (el) => expandSISOPaths(el),
  updateSISOPlotMode: (el) => updateSISOPlotMode(el.dataset.node, el.value),
  refreshSISOPlot: (el) => refreshSISOPlot(el.dataset.node),
  executeQKPolyResult: (el) => executeQKPolyResult(el.dataset.node),
  // ROP Cloud
  recomputeROPCloud: (el) => recomputeROPCloud(el.dataset.node),
  recomputeHeatmap: (el) => recomputeHeatmap(el.dataset.node),
  executeROPCloudResult: (el) => executeROPCloudResult(el.dataset.node),
  executeFRETResult: (el) => executeFRETResult(el.dataset.node),
  updateROPCloudMode: (el) => updateROPCloudMode(el.dataset.node),
  refreshROPCloudPlot: (el) => refreshROPCloudPlot(el.dataset.node),
  applyROPCloudFOVPreset: (el) => applyROPCloudFOVPreset(el.dataset.node, el.dataset.preset),
  // Scans
  runParameterScan1D: (el) => runParameterScan1D(el.dataset.node),
  runParameterScan2D: (el) => runParameterScan2D(el.dataset.node),
  executeScan1DResult: (el) => executeScan1DResult(el.dataset.node),
  executeScan2DResult: (el) => executeScan2DResult(el.dataset.node),
  insertSpecies1D: (el) => insertSpecies1D(el.dataset.node),
  insertSpecies2D: (el) => insertSpecies2D(el.dataset.node),
  // Parameter Placer (non-conversational)
  executePlacerResult: (el) => executePlacerResult(el.dataset.node),
  loadPlacerMenu: (el) => loadPlacerMenu(el.dataset.node),
  realizePlacerProgram: (el) => realizePlacerProgram(el.dataset.node),
  runDesignSearch: (el) => runDesignSearch(el.dataset.node),
  designTargetKindChange: (el) => onDesignTargetKindChange(el.dataset.node),
  designSpecKindChange: (el) => onDesignSpecKindChange(el.dataset.node),
  validateDesignSpecConfig: (el) => validateDesignSpecConfig(el.dataset.node),
  updateRopShapeIntentVisibility: (el) => updateRopShapeIntentVisibility(el.dataset.node),
  prepareRopShapeRequest: (el) => prepareRopShapeRequest(el.dataset.node),
  executeRopShapeResult: (el) => executeRopShapeResult(el.dataset.node),
  // ROP Polyhedron
  runROPPolyhedron: (el) => runROPPolyhedron(el.dataset.node),
  executeROPPolyResult: (el) => executeROPPolyResult(el.dataset.node),
  updateROPPolyDimension: (el) => updateROPPolyDimension(el.dataset.node),
  refreshROPPolyhedronPlot: (el) => refreshROPPolyhedronPlot(el.dataset.node),
  // Regime graph
  updateRegimeGraphMode: (el) => updateRegimeGraphMode(el.dataset.node),
  // Atlas
  executeAtlasBuilder: (el) => executeAtlasBuilder(el.dataset.node),
  executeAtlasQueryResult: (el) => executeAtlasQueryResult(el.dataset.node),
  executeAtlasInverseDesignResult: (el) => executeAtlasInverseDesignResult(el.dataset.node),
  addAtlasBuilderRow: (el) => addAtlasBuilderRow(el.dataset.node, el.dataset.container, el.dataset.kind),
  // SBML import/export
  importSbml: (el) => importSbml(el.dataset.node),
  exportSbml: (el) => exportSbml(el.dataset.node),
  loadSbmlFile: (el) => loadSbmlFile(el),
  // Node management (× button) — routed through history so it's undoable.
  removeNode: (el) => deleteNodeWithHistory(el.dataset.node),
  // Toolbar (index-node.html)
  saveState: () => saveState(),
  loadState: () => loadState(),
  resetView: () => resetView(),
};

function dispatchActionEvent(e) {
  const target = e.target.closest('[data-action]');
  if (!target) return;
  if (!shouldDispatchActionForEvent(e.type, target)) return;
  const handler = ACTION_HANDLERS[target.dataset.action];
  if (handler) handler(target);
}

document.addEventListener('click', dispatchActionEvent);
document.addEventListener('change', dispatchActionEvent);

// ===== Expose globals for Swift bridge (evaluateJavaScript calls) =====
window.addNodeFromMenu = addNodeFromMenu;
window.addQuickAddChain = addQuickAddChain;
window.resetView = resetView;
window.toggleDebugConsole = toggleDebugConsole;
window.toggleCloudComputeEnabled = toggleCloudComputeEnabled;
window.setCloudComputeEnabled = setCloudComputeEnabled;
window.showToast = showToast;
window.runConnectedWorkspace = runConnectedWorkspace;
window.saveState = saveState;
window.loadState = loadState;
window.setNodeView = setNodeView;

let headerOffsetObserver = null;

function installHeaderOffsetObserver() {
  const header = document.getElementById('header');
  if (!header) return;
  const update = () => {
    const height = Math.ceil(header.getBoundingClientRect().height || 50);
    document.documentElement.style.setProperty('--app-header-offset', `${height}px`);
  };
  update();
  if (typeof ResizeObserver !== 'undefined' && !headerOffsetObserver) {
    headerOffsetObserver = new ResizeObserver(update);
    headerOffsetObserver.observe(header);
  }
  window.addEventListener('resize', update, { passive: true });
}

// Design Agent → Workspace: drop an engine-verified candidate (its reactions + per-reaction Kd)
// into a fresh Reaction-Network node and switch to the Workspace, ready to wire up a viewer.
function exportNetworkToWorkspace(reactions, kd) {
  const rxns = Array.isArray(reactions) ? reactions.filter(Boolean) : [];
  if (!rxns.length) { window.showToast?.('No reactions to export'); return null; }
  setNodeView('workspace');
  const id = createNode('reaction-network', 90, 100);
  if (!id) { window.showToast?.('Could not create the Workspace node'); return null; }
  const list = document.getElementById(`${id}-reactions-list`);
  if (list) list.innerHTML = '';          // drop the default seed rows from onInit
  rxns.forEach((r, i) => addReactionRow(id, String(r), (Array.isArray(kd) && kd[i] != null) ? kd[i] : 1));
  triggerDownstreamNodes?.(id);
  window.showToast?.(`Added ${rxns.length}-reaction network to the Workspace`);
  return id;
}
window.exportNetworkToWorkspace = exportNetworkToWorkspace;

function populateDesignSpecConfigNode(nodeId, spec) {
  const target = spec?.target?.legacy_target || null;
  const behavior = spec?.target?.behavior_spec || null;
  const kindEl = document.getElementById(`${nodeId}-spec-kind`);
  const targetEl = document.getElementById(`${nodeId}-spec-target`);
  if (kindEl && target?.target_kind) kindEl.value = String(target.target_kind);
  if (kindEl && behavior) kindEl.value = 'behavior_spec';
  if (targetEl && target?.target != null) {
    targetEl.value = Array.isArray(target.target) ? target.target.join(', ') : String(target.target);
  }
  if (targetEl && behavior?.program) {
    targetEl.value = behavior.program
      .filter(step => step && step.kind === 'reaction_order')
      .map(step => step.value)
      .filter(value => value != null)
      .join(', ');
  }
  const inputEl = document.getElementById(`${nodeId}-spec-input`);
  const outputEl = document.getElementById(`${nodeId}-spec-output`);
  if (inputEl && behavior?.input) inputEl.value = String(behavior.input);
  if (outputEl && behavior?.output) outputEl.value = String(behavior.output);
  const window = behavior?.input_window || spec?.target?.input_window || null;
  const inputWindow = Array.isArray(window?.input_log10) ? window.input_log10 : null;
  const winLoEl = document.getElementById(`${nodeId}-spec-window-lo`);
  const winHiEl = document.getElementById(`${nodeId}-spec-window-hi`);
  if (inputWindow && winLoEl) winLoEl.value = String(inputWindow[0]);
  if (inputWindow && winHiEl) winHiEl.value = String(inputWindow[1]);
  const outputFeature = spec?.target?.output_feature || null;
  const outFeatureEl = document.getElementById(`${nodeId}-spec-output-feature`);
  const outValueEl = document.getElementById(`${nodeId}-spec-output-value`);
  if (outFeatureEl && outputFeature?.feature) outFeatureEl.value = String(outputFeature.feature);
  if (outValueEl && outputFeature?.value != null) outValueEl.value = String(outputFeature.value);
  const shapeEl = document.getElementById(`${nodeId}-spec-shape`);
  if (shapeEl && spec?.target?.shape?.class) shapeEl.value = String(spec.target.shape.class);
  const dynEl = document.getElementById(`${nodeId}-spec-dynamic-range`);
  if (dynEl && spec?.constraints?.dynamic_range?.min_fold_change != null) {
    dynEl.value = String(spec.constraints.dynamic_range.min_fold_change);
  }
  const spacingEl = document.getElementById(`${nodeId}-spec-transition-spacing`);
  if (spacingEl && spec?.constraints?.transitions?.min_spacing_decades != null) {
    spacingEl.value = String(spec.constraints.transitions.min_spacing_decades);
  }
  const jsonEl = document.getElementById(`${nodeId}-spec-json`);
  if (jsonEl) jsonEl.value = JSON.stringify(spec, null, 2);
  const info = NODE_TYPES['design-spec-config'];
  if (nodeRegistry[nodeId]) {
    nodeRegistry[nodeId].data = nodeRegistry[nodeId].data || {};
    nodeRegistry[nodeId].data.config = {
      ...(nodeRegistry[nodeId].data.config || {}),
      designabilitySpec: spec,
    };
  }
  return !!info;
}

function exportDesignSpecToWorkspace(spec) {
  if (!spec || typeof spec !== 'object') {
    window.showToast?.('No DesignabilitySpec to export');
    return null;
  }
  if (spec.schema_version !== 'bne-designability/v1.0.0') {
    window.showToast?.('Unsupported DesignabilitySpec version');
    return null;
  }
  setNodeView('workspace');
  const specId = createNode('design-spec-config', 90, 100);
  const targetId = createNode('design-target', 590, 100);
  if (!specId || !targetId) {
    window.showToast?.('Could not create the Design Spec workflow');
    return null;
  }
  populateDesignSpecConfigNode(specId, spec);
  addConnection({
    fromNode: specId,
    fromPort: 'designability-spec',
    toNode: targetId,
    toPort: 'designability-spec',
  });
  updateConnections();
  commitWorkspaceSnapshot('agent-design-spec-export');
  window.showToast?.('Added Design Spec Config and Design Target to the Workspace');
  return { specNodeId: specId, designTargetNodeId: targetId };
}
window.exportDesignSpecToWorkspace = exportDesignSpecToWorkspace;

async function boot() {
  initWorkspaceShell();
  // Inject the concrete node mutators into the command layer, then enable
  // Ctrl/Cmd+Z (undo) and Ctrl/Cmd+Shift+Z / Ctrl+Y (redo). Done early so
  // history is live before any user interaction.
  registerPerformers({
    createNode,
    removeNode,
    moveNode,
    snapshotNode,
    restoreNode,
    addConnection,
    removeConnection,
    replaceConnections,
    setAttr: setNodeAttr,
    afterChange: () => commitWorkspaceSnapshot('command'),
  });
  initUndoKeyboard();
  initAttrHistory();
  initEditorUI();   // alignment toolbar + selection shortcuts + cheatsheet
  installHeaderOffsetObserver();
  initCloudComputeToggleEvents();
  initAuthUiEvents();
  initCanvasEvents();
  initSocketEvents();
  initDebugConsoleEvents();
  initNodeMenuEvents();
  initAgentView();   // Design Agent surface + Workspace ⇄ Design Agent switch
  installWorkspaceShellObservers();
  await installThemeChangeObserver();
  (window.BiocircuitsExplorerWorkspaceShell || window.ROPWorkspaceShell)?.markReady?.();
}

void boot();
