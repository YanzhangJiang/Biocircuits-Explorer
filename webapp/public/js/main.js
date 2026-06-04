// Biocircuits Explorer — Node Edition Frontend (ES Module Entry Point)
// This is the main entry point loaded by index-node.html as <script type="module">.
// It imports all modules, exposes necessary globals for the Swift bridge,
// sets up event delegation, and runs the initialization sequence.

// ===== Module Imports =====
import { showToast } from './api.js';
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

document.addEventListener('click', (e) => {
  const target = e.target.closest('[data-action]');
  if (!target) return;
  const handler = ACTION_HANDLERS[target.dataset.action];
  if (handler) handler(target);
});

document.addEventListener('change', (e) => {
  const target = e.target.closest('[data-action]');
  if (!target) return;
  const handler = ACTION_HANDLERS[target.dataset.action];
  if (handler) handler(target);
});

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
  await installThemeChangeObserver();
  initCloudComputeToggleEvents();
  initAuthUiEvents();
  initCanvasEvents();
  initSocketEvents();
  initDebugConsoleEvents();
  initNodeMenuEvents();
  initAgentView();   // Design Agent surface + Workspace ⇄ Design Agent switch
  installWorkspaceShellObservers();
  (window.BiocircuitsExplorerWorkspaceShell || window.ROPWorkspaceShell)?.markReady?.();
}

void boot();
