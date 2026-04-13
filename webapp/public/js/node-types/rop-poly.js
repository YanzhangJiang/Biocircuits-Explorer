import { api, syncSelectOptions } from '../api.js';
import { getNodeData } from '../state.js';
import { setNodeLoading, getModelForNode, setupAutoUpdate, triggerConfigUpdate } from '../nodes.js';
import { executeROPPolyResult, runROPPolyhedron, updateROPPolyDimension } from '../scan.js';

export const ROP_POLY_TYPES = {
  'rop-poly-params': {
    category: 'parameter',
    headerClass: 'header-parameter',
    title: 'ROP Polyhedron Config',
    inputs: [{ port: 'model', label: 'Model' }],
    outputs: [{ port: 'params', label: 'Config' }],
    defaultWidth: 320,
    createBody(nodeId) {
      return `
        <div class="param-row">
          <label>View:</label>
          <select id="${nodeId}-dimension" class="auto-update" data-action="updateROPPolyDimension" data-node="${nodeId}">
            <option value="2">2D</option>
            <option value="3">3D</option>
          </select>
        </div>
        <div class="param-row">
          <label>Axis 1 x:</label>
          <select id="${nodeId}-x1" class="auto-update"></select>
        </div>
        <div class="param-row">
          <label>Axis 1 qK:</label>
          <select id="${nodeId}-qk1" class="auto-update"></select>
        </div>
        <div class="param-row">
          <label>Axis 2 x:</label>
          <select id="${nodeId}-x2" class="auto-update"></select>
        </div>
        <div class="param-row">
          <label>Axis 2 qK:</label>
          <select id="${nodeId}-qk2" class="auto-update"></select>
        </div>
        <div class="param-row" id="${nodeId}-axis3-x-row" style="display:none;">
          <label>Axis 3 x:</label>
          <select id="${nodeId}-x3" class="auto-update"></select>
        </div>
        <div class="param-row" id="${nodeId}-axis3-qk-row" style="display:none;">
          <label>Axis 3 qK:</label>
          <select id="${nodeId}-qk3" class="auto-update"></select>
        </div>
        <div class="param-row">
          <label><input type="checkbox" id="${nodeId}-add-inner-points" checked class="auto-update"> Add inner points</label>
        </div>
        <div class="param-row">
          <label>Inner samples:</label>
          <input type="number" id="${nodeId}-npoints" value="5000" min="0" max="100000" step="500" class="auto-update">
        </div>
        <div class="param-row">
          <label>Ray extend:</label>
          <input type="number" id="${nodeId}-singular-extends" value="2" min="0.1" max="20" step="0.1" class="auto-update">
        </div>
      `;
    },
    onInit(nodeId) {
      setupAutoUpdate(nodeId, 'rop-poly-params');
    },
    async execute(nodeId) {
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
    inputs: [{ port: 'params', label: 'Config' }],
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
      await executeROPPolyResult(nodeId);
    },
  },
  'rop-polyhedron': {
    category: 'viewer',
    headerClass: 'header-viewer',
    title: 'ROP Polyhedron',
    inputs: [{ port: 'model', label: 'Model' }],
    outputs: [],
    defaultWidth: 420,
    createBody(nodeId) {
      return `
        <div class="param-row">
          <label>View:</label>
          <select id="${nodeId}-dimension" data-action="updateROPPolyDimension" data-node="${nodeId}">
            <option value="2">2D</option>
            <option value="3">3D</option>
          </select>
        </div>
        <div class="param-row">
          <label>Axis 1 x:</label>
          <select id="${nodeId}-x1"></select>
        </div>
        <div class="param-row">
          <label>Axis 1 qK:</label>
          <select id="${nodeId}-qk1"></select>
        </div>
        <div class="param-row">
          <label>Axis 2 x:</label>
          <select id="${nodeId}-x2"></select>
        </div>
        <div class="param-row">
          <label>Axis 2 qK:</label>
          <select id="${nodeId}-qk2"></select>
        </div>
        <div class="param-row" id="${nodeId}-axis3-x-row" style="display:none;">
          <label>Axis 3 x:</label>
          <select id="${nodeId}-x3"></select>
        </div>
        <div class="param-row" id="${nodeId}-axis3-qk-row" style="display:none;">
          <label>Axis 3 qK:</label>
          <select id="${nodeId}-qk3"></select>
        </div>
        <div class="param-row">
          <label><input type="checkbox" id="${nodeId}-add-inner-points" checked> Add inner points</label>
        </div>
        <div class="param-row">
          <label>Inner samples:</label>
          <input type="number" id="${nodeId}-npoints" value="5000" min="0" max="100000" step="500">
        </div>
        <div class="param-row">
          <label>Ray extend:</label>
          <input type="number" id="${nodeId}-singular-extends" value="2" min="0.1" max="20" step="0.1">
        </div>
        <button class="btn btn-run" data-action="runROPPolyhedron" data-node="${nodeId}">Run</button>
        <div class="viewer-content" id="${nodeId}-content">
          <span class="text-dim">Connect to model and configure.</span>
        </div>
      `;
    },
    async execute(nodeId) {
      const model = getModelForNode(nodeId);
      if (!model) return;
      const nd = getNodeData(nodeId);
      const savedConfig = nd.config || nd;
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
      const addInnerEl = document.getElementById(`${nodeId}-add-inner-points`);
      const npointsEl = document.getElementById(`${nodeId}-npoints`);
      const singularExtendsEl = document.getElementById(`${nodeId}-singular-extends`);
      if (addInnerEl && savedConfig.add_inner_points != null) addInnerEl.checked = savedConfig.add_inner_points;
      if (npointsEl && savedConfig.npoints != null) npointsEl.value = savedConfig.npoints;
      if (singularExtendsEl && savedConfig.singular_extends != null) singularExtendsEl.value = savedConfig.singular_extends;
      updateROPPolyDimension(nodeId);
    },
  },
};
