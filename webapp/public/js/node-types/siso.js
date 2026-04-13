import { nodeRegistry } from '../state.js';
import { api } from '../api.js';
import { setNodeLoading, getModelForNode, getQKSymbolsForNode, getModelContextForNode, hasModelContextForNode, setupAutoUpdate } from '../nodes.js';
import { computeSISOResult, recomputeSISO, executeQKPolyResult } from '../siso.js';

export const SISO_TYPES = {
  'siso-params': {
    category: 'parameter',
    headerClass: 'header-parameter',
    title: 'SISO Config',
    inputs: [{ port: 'model', label: 'Model' }],
    outputs: [{ port: 'params', label: 'Params' }],
    defaultWidth: 320,
    createBody(nodeId) {
      return `
        <div class="param-row">
          <label>Change qK:</label>
          <select id="${nodeId}-siso-select" class="auto-update"></select>
        </div>
        <div class="param-row">
          <label>Target x:</label>
          <select id="${nodeId}-target-x" class="auto-update"></select>
        </div>
        <div class="param-row">
          <label>Path scope:</label>
          <select id="${nodeId}-path-scope" class="auto-update">
            <option value="feasible">feasible</option>
            <option value="all">all graph paths</option>
            <option value="robust">robust</option>
          </select>
        </div>
        <div class="param-row">
          <label>Min volume:</label>
          <input type="number" id="${nodeId}-min-volume" value="0" min="0" step="0.01" class="auto-update">
        </div>
        <div class="param-row">
          <label>Keep singular:</label>
          <input type="checkbox" id="${nodeId}-keep-singular" checked class="auto-update">
        </div>
        <div class="param-row">
          <label>Keep non-asym:</label>
          <input type="checkbox" id="${nodeId}-keep-nonasym" class="auto-update">
        </div>
        <div class="param-row">
          <label>Min (log10):</label>
          <input type="number" id="${nodeId}-min" value="-6" min="-20" max="20" step="0.5" class="auto-update">
        </div>
        <div class="param-row">
          <label>Max (log10):</label>
          <input type="number" id="${nodeId}-max" value="6" min="-20" max="20" step="0.5" class="auto-update">
        </div>
      `;
    },
    onInit(nodeId) {
      setupAutoUpdate(nodeId, 'siso-params');
    },
    async execute(nodeId) {
      const model = getModelForNode(nodeId);
      const qKSymbols = getQKSymbolsForNode(nodeId);
      const sel = document.getElementById(`${nodeId}-siso-select`);
      if (sel && qKSymbols.length > 0) {
        const curVal = sel.value;
        sel.innerHTML = '';
        qKSymbols.forEach(s => {
          const opt = document.createElement('option');
          opt.value = s; opt.textContent = s;
          sel.appendChild(opt);
        });
        if (curVal && qKSymbols.includes(curVal)) sel.value = curVal;
        else sel.value = qKSymbols[0];
      }

      const targetSel = document.getElementById(`${nodeId}-target-x`);
      if (targetSel && model?.x_sym?.length > 0) {
        const curVal = targetSel.value;
        targetSel.innerHTML = '';
        model.x_sym.forEach(s => {
          const opt = document.createElement('option');
          opt.value = s; opt.textContent = s;
          targetSel.appendChild(opt);
        });
        if (curVal && model.x_sym.includes(curVal)) targetSel.value = curVal;
        else targetSel.value = model.x_sym[0];
      }

      // Store config in node data
      const info = nodeRegistry[nodeId];
      if (info) {
        info.data = info.data || {};
        info.data.config = {
          change_qK: sel ? sel.value : qKSymbols[0],
          observe_x: targetSel ? targetSel.value : model?.x_sym?.[0],
          path_scope: document.getElementById(`${nodeId}-path-scope`)?.value || 'feasible',
          min_volume_mean: parseFloat(document.getElementById(`${nodeId}-min-volume`)?.value || '0'),
          keep_singular: document.getElementById(`${nodeId}-keep-singular`)?.checked ?? true,
          keep_nonasymptotic: document.getElementById(`${nodeId}-keep-nonasym`)?.checked ?? false,
          min: parseFloat(document.getElementById(`${nodeId}-min`)?.value || '-6'),
          max: parseFloat(document.getElementById(`${nodeId}-max`)?.value || '6')
        };
      }
    },
  },
  'siso-result': {
    category: 'result',
    headerClass: 'header-result',
    title: 'SISO Behaviors',
    inputs: [{ port: 'params', label: 'Params' }],
    outputs: [{ port: 'result', label: 'Path' }],
    defaultWidth: 420,
    createBody(nodeId) {
      return `
        <button class="btn btn-run" data-action="computeSISOResult" data-node="${nodeId}">Run</button>
        <div class="viewer-content siso-result-viewer" id="${nodeId}-content"><span class="text-dim">Click Run to enumerate behavior families</span></div>
      `;
    },
  },
  'qk-poly-result': {
    category: 'result',
    headerClass: 'header-result',
    title: 'qK-space Polyhedron',
    inputs: [{ port: 'result', label: 'Path' }],
    outputs: [],
    defaultWidth: 420,
    createBody(nodeId) {
      return `
        <button class="btn btn-run" data-action="executeQKPolyResult" data-node="${nodeId}">Run</button>
        <div class="viewer-content" id="${nodeId}-content"><span class="text-dim">Connect to a SISO Behaviors node, select a path, and click Run.</span></div>
      `;
    },
    async execute(nodeId) {
      await executeQKPolyResult(nodeId);
    },
  },
  'siso-analysis': {
    category: 'viewer',
    headerClass: 'header-viewer',
    title: 'SISO Analysis',
    inputs: [{ port: 'model', label: 'Model' }],
    outputs: [],
    defaultWidth: 420,
    createBody(nodeId) {
      return `
        <div class="param-row">
          <label>Change qK:</label>
          <select id="${nodeId}-siso-select"></select>
        </div>
        <button class="btn btn-run" data-action="recomputeSISO" data-node="${nodeId}">Run</button>
        <div class="viewer-content" id="${nodeId}-content"><span class="text-dim">Waiting for model...</span></div>
      `;
    },
    async execute(nodeId) {
      const modelContext = getModelContextForNode(nodeId);
      const qKSymbols = modelContext?.qK_syms || [];

      // Populate the select
      const sel = document.getElementById(`${nodeId}-siso-select`);
      if (sel && qKSymbols.length > 0) {
        const curVal = sel.value;
        sel.innerHTML = '';
        qKSymbols.forEach(s => {
          const opt = document.createElement('option');
          opt.value = s; opt.textContent = s;
          sel.appendChild(opt);
        });
        if (curVal && qKSymbols.includes(curVal)) sel.value = curVal;
      }
      const changeQK = sel ? sel.value : qKSymbols[0];
      if (!changeQK) return;

      const contentEl = document.getElementById(`${nodeId}-content`);
      setNodeLoading(nodeId, true);
      try {
        if (!modelContext?.sessionId) throw new Error('Build the connected model first');
        const data = await api('siso_paths', { session_id: modelContext.sessionId, change_qK: changeQK });
        let html = `<div style="margin-bottom:8px;"><strong>${data.n_paths}</strong> paths, <strong>${data.sources.length}</strong> sources, <strong>${data.sinks.length}</strong> sinks</div>`;
        html += '<div class="path-list">';
        data.paths.forEach(p => {
          const permStr = p.perms.map(pr => `[${pr.join(',')}]`).join(' → ');
          html += `<div class="path-item" data-idx="${p.idx}" data-qk="${changeQK}" data-node="${nodeId}" data-action="selectSISOPath">#${p.idx}: ${permStr}</div>`;
        });
        html += '</div>';
        html += `<div class="plot-container" id="${nodeId}-traj-plot" style="display:none;"></div>`;
        contentEl.innerHTML = html;
      } catch (e) {
        contentEl.innerHTML = `<div class="node-error">${e.message}</div>`;
      }
      setNodeLoading(nodeId, false);
    },
  },
};
