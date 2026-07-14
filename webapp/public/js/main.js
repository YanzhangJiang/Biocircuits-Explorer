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
import { installThemeChangeObserver } from './theme.js';
import { initCanvasEvents, resetView } from './canvas.js';
import {
  initSocketEvents, updateConnections,
  addConnection, removeConnection, replaceConnections,
} from './connections.js';
import { toggleDebugConsole, initDebugConsoleEvents } from './debug-console.js';
import { NODE_TYPES, switchNoteTab } from './node-types/index.js';
import {
  addNodeFromMenu, addQuickAddChain, removeNode, runConnectedWorkspace, runAllConnectedWorkspace,
  initNodeMenuEvents,
  createNode, moveNode, deleteNodeWithHistory, setNodeAttr, initAttrHistory,
} from './nodes.js';
import { registerPerformers, initUndoKeyboard } from './commands.js';
import { initEditorUI } from './editor-ui.js';
import { addReactionRow, triggerDownstreamNodes } from './model.js';
import {
  initWorkspaceShell, installWorkspaceShellObservers,
  saveState, loadState, commitWorkspaceSnapshot,
  snapshotNode, restoreNode,
} from './workspace.js';
import {
  recomputeSISO, recomputeROPCloud, recomputeHeatmap,
  plotSISOPath, selectSISOPath, toggleSISOPathCondition, updateSISOPlotMode, refreshSISOPlot,
  expandSISOPaths,
} from './siso.js';
import {
  updateROPCloudMode, refreshROPCloudPlot, applyROPCloudFOVPreset,
} from './rop-cloud.js';
import { updateRegimeGraphMode } from './regime-graph.js';
import {
  runParameterScan1D, runParameterScan2D,
  insertSpecies1D, insertSpecies2D, updateROPPolyDimension,
  refreshROPPolyhedronPlot, runROPPolyhedron,
} from './scan.js';
import { addAtlasBuilderRow } from './atlas.js';
import { executePlacerResult, loadPlacerMenu } from './node-types/placer.js';
import {
  updateRopShapeIntentVisibility,
} from './node-types/rop-shape.js';
import {
  onDesignTargetKindChange,
  onDesignSpecKindChange,
} from './node-types/design-target.js';
import { importSbml, loadSbmlFile } from './sbml-io.js';
import { initAgentView, setNodeView } from './agent-view.js';
import './llm-settings.js';   // temporary self-mounting LLM-key panel (OpenAI/Anthropic)

// ===== Event Delegation Dispatcher =====
const ACTION_HANDLERS = {
  // Notes
  switchNoteTab: (el) => switchNoteTab(el.dataset.node, el.dataset.tab),
  // Reactions
  addReactionRow: (el) => addReactionRow(el.dataset.node),
  buildModel: (el) => runNodeOperation(el),
  removeReactionRow: (el) => el.closest('.reaction-row')?.remove(),
  // SISO
  computeSISOResult: (el) => runNodeOperation(el),
  recomputeSISO: (el) => recomputeSISO(el.dataset.node),
  selectSISOPath: (el) => selectSISOPath(el),
  plotSISOPath: (el) => plotSISOPath(el.dataset.node, el.dataset.qk, parseInt(el.dataset.idx), el),
  toggleSISOPathCondition: (el) => toggleSISOPathCondition(el),
  toggleSISOPaths: (el) => expandSISOPaths(el),
  updateSISOPlotMode: (el) => updateSISOPlotMode(el.dataset.node, el.value),
  refreshSISOPlot: (el) => refreshSISOPlot(el.dataset.node),
  executeQKPolyResult: (el) => runNodeOperation(el),
  // ROP Cloud
  recomputeROPCloud: (el) => recomputeROPCloud(el.dataset.node),
  recomputeHeatmap: (el) => recomputeHeatmap(el.dataset.node),
  executeROPCloudResult: (el) => runNodeOperation(el),
  executeFRETResult: (el) => runNodeOperation(el),
  updateROPCloudMode: (el) => updateROPCloudMode(el.dataset.node),
  refreshROPCloudPlot: (el) => refreshROPCloudPlot(el.dataset.node),
  applyROPCloudFOVPreset: (el) => applyROPCloudFOVPreset(el.dataset.node, el.dataset.preset),
  // Scans
  runParameterScan1D: (el) => runParameterScan1D(el.dataset.node),
  runParameterScan2D: (el) => runParameterScan2D(el.dataset.node),
  executeScan1DResult: (el) => runNodeOperation(el),
  executeScan2DResult: (el) => runNodeOperation(el),
  insertSpecies1D: (el) => insertSpecies1D(el.dataset.node),
  insertSpecies2D: (el) => insertSpecies2D(el.dataset.node),
  // Parameter Placer (non-conversational)
  executePlacerResult: (el) => executePlacerResult(el.dataset.node),
  loadPlacerMenu: (el) => loadPlacerMenu(el.dataset.node),
  realizePlacerProgram: (el) => runNodeOperation(el),
  runDesignSearch: (el) => runNodeOperation(el),
  designTargetKindChange: (el) => onDesignTargetKindChange(el.dataset.node),
  designSpecKindChange: (el) => onDesignSpecKindChange(el.dataset.node),
  validateDesignSpecConfig: (el) => runNodeOperation(el, 'prepare'),
  updateRopShapeIntentVisibility: (el) => updateRopShapeIntentVisibility(el.dataset.node),
  prepareRopShapeRequest: (el) => runNodeOperation(el, 'prepare'),
  executeRopShapeResult: (el) => runNodeOperation(el),
  // ROP Polyhedron
  runROPPolyhedron: (el) => runROPPolyhedron(el.dataset.node),
  executeROPPolyResult: (el) => runNodeOperation(el),
  updateROPPolyDimension: (el) => updateROPPolyDimension(el.dataset.node),
  refreshROPPolyhedronPlot: (el) => refreshROPPolyhedronPlot(el.dataset.node),
  // Regime graph
  updateRegimeGraphMode: (el) => updateRegimeGraphMode(el.dataset.node),
  // Atlas
  executeAtlasBuilder: (el) => runNodeOperation(el),
  executeAtlasQueryResult: (el) => runNodeOperation(el),
  executeAtlasInverseDesignResult: (el) => runNodeOperation(el),
  addAtlasBuilderRow: (el) => addAtlasBuilderRow(el.dataset.node, el.dataset.container, el.dataset.kind),
  // SBML import/export
  importSbml: (el) => importSbml(el.dataset.node),
  exportSbml: (el) => runNodeOperation(el),
  loadSbmlFile: (el) => loadSbmlFile(el),
  // Node management (× button) — routed through history so it's undoable.
  removeNode: (el) => deleteNodeWithHistory(el.dataset.node),
  // Toolbar (index-node.html)
  saveState: () => saveState(),
  loadState: () => loadState(),
  resetView: () => resetView(),
};

async function runNodeOperation(el, phase = 'execute') {
  const nodeId = el?.dataset?.node;
  const info = nodeRegistry[nodeId];
  const definition = NODE_TYPES[info?.type];
  const operation = definition?.[phase];
  if (typeof operation !== 'function') {
    showToast(definition?.availability === 'restore-only'
      ? `${definition.title} is restore-only; use Quick Add to migrate it`
      : `${definition?.title || info?.type || 'Node'} has no ${phase} operation`);
    return null;
  }
  const outcome = await operation(nodeId, { triggerDownstream: false });
  if (outcome?.status && outcome.status !== 'succeeded') {
    showToast(outcome.message || `${definition.title}: ${outcome.status}`);
  }
  return outcome;
}

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
window.runAllConnectedWorkspace = runAllConnectedWorkspace;
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
