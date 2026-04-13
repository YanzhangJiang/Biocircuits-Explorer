import { api, syncSelectOptions } from '../api.js';
import { setNodeLoading, getModelForNode, getSessionIdForNode, setupAutoUpdate, triggerConfigUpdate } from '../nodes.js';
import { executeScan1DResult, executeScan2DResult, runParameterScan1D, runParameterScan2D, insertSpecies1D, insertSpecies2D } from '../scan.js';
import { commitWorkspaceSnapshot } from '../workspace.js';
import { plotHeatmap } from '../plotting.js';
import { setupPlotResize } from '../nodes.js';
import { nodeRegistry, ensureNodeData } from '../state.js';

export const SCAN_TYPES = {
  'scan-1d-params': {
    category: 'parameter',
    headerClass: 'header-parameter',
    title: 'Scan 1D Config',
    inputs: [{ port: 'model', label: 'Model' }],
    outputs: [{ port: 'params', label: 'Config' }],
    defaultWidth: 320,
    createBody(nodeId) {
      return `
        <div class="param-row">
          <label>Scan parameter:</label>
          <select id="${nodeId}-param" class="auto-update"></select>
        </div>
        <div class="param-row">
          <label>Range min:</label>
          <input type="number" id="${nodeId}-min" value="-6" step="0.5" class="auto-update">
        </div>
        <div class="param-row">
          <label>Range max:</label>
          <input type="number" id="${nodeId}-max" value="6" step="0.5" class="auto-update">
        </div>
        <div class="param-row">
          <label>Points:</label>
          <input type="number" id="${nodeId}-points" value="200" min="10" max="1000" class="auto-update">
        </div>
        <div class="param-row">
          <label>Output expression:</label>
          <div style="display:flex;gap:4px;">
            <input type="text" id="${nodeId}-expr" placeholder="e.g., C_ES or 2*C_ES+E" style="flex:1;" class="auto-update">
            <select id="${nodeId}-species-helper" data-action="insertSpecies1D" data-node="${nodeId}" style="width:80px;">
              <option value="">Insert...</option>
            </select>
          </div>
        </div>
      `;
    },
    onInit(nodeId) {
      setupAutoUpdate(nodeId, 'scan-1d-params');
    },
    async execute(nodeId) {
      const model = getModelForNode(nodeId);
      if (!model) return;

      const paramSelect = document.getElementById(`${nodeId}-param`);
      const speciesHelper = document.getElementById(`${nodeId}-species-helper`);
      const qkSymbols = [...model.q_sym, ...model.K_sym];
      syncSelectOptions(paramSelect, qkSymbols);
      syncSelectOptions(speciesHelper, [''].concat(model.x_sym), '', 0);
      triggerConfigUpdate(nodeId, 'scan-1d-params');
    },
  },
  'scan-2d-params': {
    category: 'parameter',
    headerClass: 'header-parameter',
    title: 'Scan 2D Config',
    inputs: [{ port: 'model', label: 'Model' }],
    outputs: [{ port: 'params', label: 'Config' }],
    defaultWidth: 320,
    createBody(nodeId) {
      return `
        <div class="param-row">
          <label>X-axis parameter:</label>
          <select id="${nodeId}-param1" class="auto-update"></select>
        </div>
        <div class="param-row">
          <label>X range min:</label>
          <input type="number" id="${nodeId}-min1" value="-6" step="0.5" class="auto-update">
        </div>
        <div class="param-row">
          <label>X range max:</label>
          <input type="number" id="${nodeId}-max1" value="6" step="0.5" class="auto-update">
        </div>
        <div class="param-row">
          <label>Y-axis parameter:</label>
          <select id="${nodeId}-param2" class="auto-update"></select>
        </div>
        <div class="param-row">
          <label>Y range min:</label>
          <input type="number" id="${nodeId}-min2" value="-6" step="0.5" class="auto-update">
        </div>
        <div class="param-row">
          <label>Y range max:</label>
          <input type="number" id="${nodeId}-max2" value="6" step="0.5" class="auto-update">
        </div>
        <div class="param-row">
          <label>Grid points:</label>
          <input type="number" id="${nodeId}-points" value="50" min="10" max="200" class="auto-update">
        </div>
        <div class="param-row">
          <label>Output expression:</label>
          <div style="display:flex;gap:4px;">
            <input type="text" id="${nodeId}-expr" placeholder="e.g., C_ES or 2*C_ES+E" style="flex:1;" class="auto-update">
            <select id="${nodeId}-species-helper" data-action="insertSpecies2D" data-node="${nodeId}" style="width:80px;">
              <option value="">Insert...</option>
            </select>
          </div>
        </div>
      `;
    },
    onInit(nodeId) {
      setupAutoUpdate(nodeId, 'scan-2d-params');
    },
    async execute(nodeId) {
      const model = getModelForNode(nodeId);
      if (!model) return;
      const param1Select = document.getElementById(`${nodeId}-param1`);
      const param2Select = document.getElementById(`${nodeId}-param2`);
      const speciesHelper = document.getElementById(`${nodeId}-species-helper`);
      const qkSymbols = [...model.q_sym, ...model.K_sym];
      syncSelectOptions(param1Select, qkSymbols, param1Select?.value, 0);
      syncSelectOptions(param2Select, qkSymbols, param2Select?.value, 1);
      syncSelectOptions(speciesHelper, [''].concat(model.x_sym), '', 0);
      triggerConfigUpdate(nodeId, 'scan-2d-params');
    },
  },
  'scan-1d-result': {
    category: 'result',
    headerClass: 'header-result',
    title: '1D Scan Result',
    inputs: [{ port: 'params', label: 'Config' }],
    outputs: [],
    defaultWidth: 420,
    createBody(nodeId) {
      return `
        <button class="btn btn-run" data-action="executeScan1DResult" data-node="${nodeId}">Run</button>
        <div class="viewer-content" id="${nodeId}-content">
          <span class="text-dim">Connect to Scan 1D Config and click Run.</span>
        </div>
      `;
    },
    async execute(nodeId) {
      await executeScan1DResult(nodeId);
    },
  },
  'scan-2d-result': {
    category: 'result',
    headerClass: 'header-result',
    title: '2D Scan Result',
    inputs: [{ port: 'params', label: 'Config' }],
    outputs: [],
    defaultWidth: 600,
    createBody(nodeId) {
      return `
        <button class="btn btn-run" data-action="executeScan2DResult" data-node="${nodeId}">Run</button>
        <div class="viewer-content" id="${nodeId}-content">
          <span class="text-dim">Connect to Scan 2D Config and click Run.</span>
        </div>
      `;
    },
    async execute(nodeId) {
      await executeScan2DResult(nodeId);
    },
  },
  'parameter-scan-1d': {
    category: 'viewer',
    headerClass: 'header-viewer',
    title: 'Parameter Scan (1D)',
    inputs: [{ port: 'model', label: 'Model' }],
    outputs: [],
    defaultWidth: 420,
    createBody(nodeId) {
      return `
        <div class="param-row">
          <label>Scan parameter:</label>
          <select id="${nodeId}-param"></select>
        </div>
        <div class="param-row">
          <label>Range min:</label>
          <input type="number" id="${nodeId}-min" value="-6" step="0.5">
        </div>
        <div class="param-row">
          <label>Range max:</label>
          <input type="number" id="${nodeId}-max" value="6" step="0.5">
        </div>
        <div class="param-row">
          <label>Points:</label>
          <input type="number" id="${nodeId}-points" value="200" min="10" max="1000">
        </div>
        <div class="param-row">
          <label>Output expression:</label>
          <div style="display:flex;gap:4px;">
            <input type="text" id="${nodeId}-expr" placeholder="e.g., C_ES or 2*C_ES+E" style="flex:1;">
            <select id="${nodeId}-species-helper" data-action="insertSpecies1D" data-node="${nodeId}" style="width:80px;">
              <option value="">Insert...</option>
            </select>
          </div>
        </div>
        <button class="btn btn-run" data-action="runParameterScan1D" data-node="${nodeId}">Run</button>
        <div class="viewer-content" id="${nodeId}-content">
          <span class="text-dim">Connect to model and configure scan.</span>
        </div>
      `;
    },
    async execute(nodeId) {
      const model = getModelForNode(nodeId);
      if (!model) return;

      const paramSelect = document.getElementById(`${nodeId}-param`);
      const speciesHelper = document.getElementById(`${nodeId}-species-helper`);
      const qkSymbols = [...model.q_sym, ...model.K_sym];
      syncSelectOptions(paramSelect, qkSymbols);
      syncSelectOptions(speciesHelper, [''].concat(model.x_sym), '', 0);
    },
  },
  'parameter-scan-2d': {
    category: 'viewer',
    headerClass: 'header-viewer',
    title: 'Parameter Scan (2D)',
    inputs: [{ port: 'model', label: 'Model' }],
    outputs: [],
    defaultWidth: 420,
    createBody(nodeId) {
      return `
        <div class="param-row">
          <label>X-axis parameter:</label>
          <select id="${nodeId}-param1"></select>
        </div>
        <div class="param-row">
          <label>X range:</label>
          <input type="number" id="${nodeId}-min1" value="-6" step="0.5" style="width:60px">
          to
          <input type="number" id="${nodeId}-max1" value="6" step="0.5" style="width:60px">
        </div>
        <div class="param-row">
          <label>Y-axis parameter:</label>
          <select id="${nodeId}-param2"></select>
        </div>
        <div class="param-row">
          <label>Y range:</label>
          <input type="number" id="${nodeId}-min2" value="-6" step="0.5" style="width:60px">
          to
          <input type="number" id="${nodeId}-max2" value="6" step="0.5" style="width:60px">
        </div>
        <div class="param-row">
          <label>Grid size:</label>
          <input type="number" id="${nodeId}-grid" value="80" min="20" max="200">
        </div>
        <div class="param-row">
          <label>Output expression:</label>
          <div style="display:flex;gap:4px;">
            <input type="text" id="${nodeId}-expr" placeholder="e.g., C_ES or 2*C_ES+E" style="flex:1;">
            <select id="${nodeId}-species-helper" data-action="insertSpecies2D" data-node="${nodeId}" style="width:80px;">
              <option value="">Insert...</option>
            </select>
          </div>
        </div>
        <button class="btn btn-run" data-action="runParameterScan2D" data-node="${nodeId}">Run</button>
        <div class="viewer-content" id="${nodeId}-content">
          <span class="text-dim">Connect to model and configure scan.</span>
        </div>
      `;
    },
    async execute(nodeId) {
      const model = getModelForNode(nodeId);
      if (!model) return;

      const param1Select = document.getElementById(`${nodeId}-param1`);
      const param2Select = document.getElementById(`${nodeId}-param2`);
      const speciesHelper = document.getElementById(`${nodeId}-species-helper`);
      const qkSymbols = [...model.q_sym, ...model.K_sym];
      syncSelectOptions(param1Select, qkSymbols, param1Select?.value, 0);
      syncSelectOptions(param2Select, qkSymbols, param2Select?.value, 1);
      syncSelectOptions(speciesHelper, [''].concat(model.x_sym), '', 0);
    },
  },
};
