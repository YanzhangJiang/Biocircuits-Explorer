import { syncSelectOptions } from '../api.js';
import { getNodeData } from '../state.js';
import { getModelForNode, setupAutoUpdate, triggerConfigUpdate } from '../nodes.js';
import {
  executeROPPolyResult,
  installROPPolyConfigInvalidation,
  updateROPPolyDimension,
} from '../scan.js';

function ropPolyAxesBody(nodeId, autoUpdate) {
  const auto = autoUpdate ? ' class="auto-update"' : '';
  return `
        <div class="param-row">
          <label>View:</label>
          <select id="${nodeId}-dimension"${auto} data-action="updateROPPolyDimension" data-node="${nodeId}">
            <option value="2">2D</option>
            <option value="3">3D</option>
          </select>
        </div>
        <div class="param-row">
          <label>Axis 1 x:</label>
          <select id="${nodeId}-x1"${auto}></select>
        </div>
        <div class="param-row">
          <label>Axis 1 qK:</label>
          <select id="${nodeId}-qk1"${auto}></select>
        </div>
        <div class="param-row">
          <label>Axis 2 x:</label>
          <select id="${nodeId}-x2"${auto}></select>
        </div>
        <div class="param-row">
          <label>Axis 2 qK:</label>
          <select id="${nodeId}-qk2"${auto}></select>
        </div>
        <div class="param-row" id="${nodeId}-axis3-x-row" style="display:none;">
          <label>Axis 3 x:</label>
          <select id="${nodeId}-x3"${auto}></select>
        </div>
        <div class="param-row" id="${nodeId}-axis3-qk-row" style="display:none;">
          <label>Axis 3 qK:</label>
          <select id="${nodeId}-qk3"${auto}></select>
        </div>
        <div class="param-row">
          <label><input type="checkbox" id="${nodeId}-add-inner-points" checked${auto}> Add inner points</label>
        </div>
        <div class="param-row">
          <label>Inner samples:</label>
          <input type="number" id="${nodeId}-npoints" value="5000" min="0" max="20000" step="500"${auto}>
        </div>
        <div class="param-row">
          <label>Ray extend:</label>
          <input type="number" id="${nodeId}-singular-extends" value="2" min="0.1" max="20" step="0.1"${auto}>
        </div>`;
}

export const ROP_POLY_TYPES = {
  'rop-poly-params': {
    category: 'parameter',
    headerClass: 'header-parameter',
    title: 'ROP Polyhedron Config',
    inputs: [{ port: 'model', type: 'ModelArtifact', label: 'Model' }],
    outputs: [{ port: 'params', type: 'ROPPolyhedronConfig', label: 'Config' }],
    defaultWidth: 320,
    createBody(nodeId) {
      return `${ropPolyAxesBody(nodeId, true)}
      `;
    },
    onInit(nodeId) {
      installROPPolyConfigInvalidation(nodeId);
      setupAutoUpdate(nodeId, 'rop-poly-params');
    },
    async prepare(nodeId) {
      const model = getModelForNode(nodeId);
      if (!model) return;
      const savedConfig = getNodeData(nodeId).config || {};
      const xSelects = ['x1', 'x2', 'x3'].map(id => document.getElementById(`${nodeId}-${id}`)).filter(Boolean);
      const qkSelects = ['qk1', 'qk2', 'qk3'].map(id => document.getElementById(`${nodeId}-${id}`)).filter(Boolean);
      const qkSymbols = [...model.q_sym, ...model.K_sym];
      const defaultX = model.x_sym[0] || '';
      const defaultQK = qkSymbols[0] || '';

      xSelects.forEach((sel, idx) => {
        const preferred = savedConfig.pairs?.[idx]?.x_symbol || sel.value;
        syncSelectOptions(sel, model.x_sym, preferred, idx);
        if (!sel.value) sel.value = model.x_sym[idx] || defaultX;
      });

      qkSelects.forEach((sel, idx) => {
        const preferred = savedConfig.pairs?.[idx]?.qk_symbol || sel.value;
        syncSelectOptions(sel, qkSymbols, preferred, idx);
        if (!sel.value) sel.value = qkSymbols[idx] || defaultQK;
      });

      if (savedConfig.dimension != null) {
        const dimensionEl = document.getElementById(`${nodeId}-dimension`);
        if (dimensionEl) dimensionEl.value = savedConfig.dimension;
      }
      (savedConfig.pairs || []).forEach((pair, idx) => {
        const xEl = xSelects[idx];
        const qkEl = qkSelects[idx];
        if (xEl && pair.x_symbol && Array.from(xEl.options).some(opt => opt.value === pair.x_symbol)) xEl.value = pair.x_symbol;
        if (qkEl && pair.qk_symbol && Array.from(qkEl.options).some(opt => opt.value === pair.qk_symbol)) qkEl.value = pair.qk_symbol;
      });

      updateROPPolyDimension(nodeId);
      triggerConfigUpdate(nodeId, 'rop-poly-params');
    },
  },
  'rop-poly-result': {
    category: 'result',
    headerClass: 'header-result',
    title: 'ROP Polyhedron Result',
    inputs: [{ port: 'params', type: 'ROPPolyhedronConfig', label: 'Config' }],
    outputs: [],
    defaultWidth: 600,
    createBody(nodeId) {
      return `
        <button class="btn btn-run" data-action="executeROPPolyResult" data-node="${nodeId}">Run</button>
        <div class="viewer-content" id="${nodeId}-content">
          <span class="text-dim">Connect to ROP Polyhedron Config and click Run.</span>
        </div>
      `;
    },
    async execute(nodeId) {
      return executeROPPolyResult(nodeId);
    },
  },
  'rop-polyhedron': {
    category: 'viewer',
    headerClass: 'header-viewer',
    title: 'ROP Polyhedron',
    inputs: [{ port: 'model', type: 'ModelArtifact', label: 'Model' }],
    outputs: [],
    defaultWidth: 420,
    createBody(nodeId) {
      return `${ropPolyAxesBody(nodeId, false)}
        <button class="btn btn-run" data-action="runROPPolyhedron" data-node="${nodeId}">Run</button>
        <div class="viewer-content" id="${nodeId}-content">
          <span class="text-dim">Connect to model and configure.</span>
        </div>
      `;
    },
  },
};
