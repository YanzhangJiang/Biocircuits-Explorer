// Biocircuits Explorer — Node Edition Frontend (ES Module Entry Point)
// This is the main entry point loaded by index-node.html as <script type="module">.
// It imports all modules, exposes necessary globals for the Swift bridge,
// sets up event delegation, and runs the initialization sequence.

// ===== Module Imports =====
import { showToast } from './api.js';
import { applyThemeMode, installThemeChangeObserver } from './theme.js';
import { initCanvasEvents, resetView } from './canvas.js';
import { initSocketEvents, updateConnections } from './connections.js';
import { toggleDebugConsole, initDebugConsoleEvents } from './debug-console.js';
import { NODE_TYPES, switchNoteTab } from './node-types/index.js';
import {
  addNodeFromMenu, addQuickAddChain, removeNode, runConnectedWorkspace,
  initNodeMenuEvents, setupPlotResize, setupPlotInteractionGuard,
} from './nodes.js';
import { buildModel, addReactionRow, triggerDownstreamNodes, getReactionsFromNode } from './model.js';
import {
  initWorkspaceShell, installWorkspaceShellObservers,
  saveState, loadState, commitWorkspaceSnapshot,
} from './workspace.js';
import {
  computeSISOResult, recomputeSISO, recomputeROPCloud, recomputeHeatmap,
  plotSISOPath, selectSISOPath, executeQKPolyResult,
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
  // Node management
  removeNode: (el) => removeNode(el.dataset.node),
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
window.showToast = showToast;
window.runConnectedWorkspace = runConnectedWorkspace;
window.saveState = saveState;
window.loadState = loadState;

async function boot() {
  initWorkspaceShell();
  await installThemeChangeObserver();
  initCanvasEvents();
  initSocketEvents();
  initDebugConsoleEvents();
  initNodeMenuEvents();
  installWorkspaceShellObservers();
  window.BiocircuitsExplorerWorkspaceShell.markReady();
}

void boot();
