import { connections, nodeRegistry, ensureNodeData } from '../state.js';
import { api } from '../api.js';
import { setNodeLoading, getModelForNode, getSessionIdForNode, setupAutoUpdate } from '../nodes.js';
import { getReactionsFromNode } from '../model.js';
import { executeROPCloudResult, updateROPCloudMode, refreshROPCloudTargetOptions, renderROPCloudOutput, executeFRETResult } from '../rop-cloud.js';
import { recomputeROPCloud, recomputeHeatmap } from '../siso.js';
import { commitWorkspaceSnapshot } from '../workspace.js';
import { plotHeatmap } from '../plotting.js';
import { setupPlotResize } from '../nodes.js';

export const ROP_CLOUD_TYPES = {
  'rop-cloud-params': {
    category: 'parameter',
    headerClass: 'header-parameter',
    title: 'ROP Cloud Config',
    inputs: [{ port: 'reactions', label: 'Reactions' }, { port: 'model', label: 'Model' }],
    outputs: [{ port: 'params', label: 'Config' }],
    defaultWidth: 320,
    createBody(nodeId) {
      return `
        <div class="param-row">
          <label>Sampling mode:</label>
          <select id="${nodeId}-sampling-mode" data-action="updateROPCloudMode" data-node="${nodeId}" class="auto-update">
            <option value="x_space">x-space closed-form</option>
            <option value="qk">qK sampling (legacy)</option>
          </select>
        </div>
        <div class="param-row">
          <label>Samples:</label>
          <input type="number" id="${nodeId}-samples" value="10000" min="100" max="100000" step="1000" class="auto-update">
        </div>
        <div id="${nodeId}-xspace-params">
          <div class="param-row">
            <label>Target:</label>
            <select id="${nodeId}-target-species" class="auto-update"></select>
          </div>
          <div class="param-row">
            <label>log10(x) min:</label>
            <input type="number" id="${nodeId}-logx-min" value="-6" min="-20" max="20" step="0.5" class="auto-update">
          </div>
          <div class="param-row">
            <label>log10(x) max:</label>
            <input type="number" id="${nodeId}-logx-max" value="6" min="-20" max="20" step="0.5" class="auto-update">
          </div>
        </div>
        <div id="${nodeId}-qk-params" style="display:none;">
          <div class="param-row">
            <label>Span:</label>
            <input type="number" id="${nodeId}-span" value="6" min="1" max="20" class="auto-update">
          </div>
        </div>
      `;
    },
    onInit(nodeId) {
      updateROPCloudMode(nodeId);
      setupAutoUpdate(nodeId, 'rop-cloud-params');
    },
    async execute(nodeId) {
      updateROPCloudMode(nodeId);
      const mode = document.getElementById(`${nodeId}-sampling-mode`)?.value || 'x_space';
      if (mode === 'x_space') {
        const rxConn = connections.find(c => c.toNode === nodeId && c.toPort === 'reactions');
        if (rxConn) {
          const { reactions } = getReactionsFromNode(rxConn.fromNode);
          if (reactions.length > 0) {
            refreshROPCloudTargetOptions(nodeId, reactions);
          }
        }
      }
    },
  },
  'rop-cloud-result': {
    category: 'result',
    headerClass: 'header-result',
    title: 'ROP Cloud Result',
    inputs: [{ port: 'params', label: 'Config' }],
    outputs: [],
    defaultWidth: 600,
    createBody(nodeId) {
      return `
        <button class="btn btn-run" data-action="executeROPCloudResult" data-node="${nodeId}">Run</button>
        <div class="viewer-content" id="${nodeId}-content">
          <span class="text-dim">Connect to ROP Cloud Config and click Run.</span>
        </div>
      `;
    },
    async execute(nodeId) {
      await executeROPCloudResult(nodeId);
    },
  },
  'rop-cloud': {
    category: 'viewer',
    headerClass: 'header-viewer',
    title: 'ROP Point Cloud',
    inputs: [{ port: 'reactions', label: 'Reactions' }, { port: 'model', label: 'Model' }],
    outputs: [],
    defaultWidth: 420,
    createBody(nodeId) {
      return `
        <div class="param-row">
          <label>Mode:</label>
          <select id="${nodeId}-sampling-mode" data-action="updateROPCloudMode" data-node="${nodeId}">
            <option value="x_space">x-space closed-form</option>
            <option value="qk">qK sampling (legacy)</option>
          </select>
        </div>
        <div class="param-row">
          <label>Samples:</label>
          <input type="number" id="${nodeId}-samples" value="10000" min="100" max="100000" step="1000">
        </div>
        <div id="${nodeId}-xspace-params">
          <div class="param-row">
            <label>Target:</label>
            <select id="${nodeId}-target-species"></select>
          </div>
          <div class="param-row">
            <label>log10(x) min:</label>
            <input type="number" id="${nodeId}-logx-min" value="-6" min="-20" max="20" step="0.5">
          </div>
          <div class="param-row">
            <label>log10(x) max:</label>
            <input type="number" id="${nodeId}-logx-max" value="6" min="-20" max="20" step="0.5">
          </div>
        </div>
        <div id="${nodeId}-qk-params" style="display:none;">
          <div class="param-row">
            <label>Span:</label>
            <input type="number" id="${nodeId}-span" value="6" min="1" max="20">
          </div>
        </div>
        <button class="btn btn-run" data-action="recomputeROPCloud" data-node="${nodeId}">Run</button>
        <div class="viewer-content" id="${nodeId}-content"><span class="text-dim">Waiting for input...</span></div>
      `;
    },
    onInit(nodeId) {
      updateROPCloudMode(nodeId);
    },
    async execute(nodeId) {
      const nSamples = parseInt(document.getElementById(`${nodeId}-samples`)?.value || '10000');
      const contentEl = document.getElementById(`${nodeId}-content`);
      const mode = document.getElementById(`${nodeId}-sampling-mode`)?.value || 'x_space';
      updateROPCloudMode(nodeId);
      setNodeLoading(nodeId, true);
      try {
        let data;
        if (mode === 'qk') {
          const sessionId = getSessionIdForNode(nodeId);
          if (!sessionId) throw new Error('Build the connected model first, or switch to x-space mode');
          const modelConn = connections.find(c => c.toNode === nodeId && c.toPort === 'model');
          if (!modelConn) throw new Error('qK mode requires Model input connection');
          const span = parseInt(document.getElementById(`${nodeId}-span`)?.value || '6');
          data = await api('rop_cloud', {
            sampling_mode: 'qk',
            session_id: sessionId,
            n_samples: nSamples,
            span: span,
          });
        } else {
          const rxConn = connections.find(c => c.toNode === nodeId && c.toPort === 'reactions');
          if (!rxConn) throw new Error('x-space mode requires Reactions input connection');
          const { reactions } = getReactionsFromNode(rxConn.fromNode);
          if (!reactions.length) throw new Error('Add at least one reaction in the connected Reaction Network');
          refreshROPCloudTargetOptions(nodeId, reactions);
          const targetSpecies = document.getElementById(`${nodeId}-target-species`)?.value || '';
          const logxMin = parseFloat(document.getElementById(`${nodeId}-logx-min`)?.value || '-6');
          const logxMax = parseFloat(document.getElementById(`${nodeId}-logx-max`)?.value || '6');
          data = await api('rop_cloud', {
            sampling_mode: 'x_space',
            reactions: reactions,
            n_samples: nSamples,
            logx_min: logxMin,
            logx_max: logxMax,
            target_species: targetSpecies,
          });
        }
        renderROPCloudOutput(nodeId, contentEl, data);
      } catch (e) {
        contentEl.innerHTML = `<div class="node-error">${e.message}</div>`;
      }
      setNodeLoading(nodeId, false);
    },
  },
  'fret-params': {
    category: 'parameter',
    headerClass: 'header-parameter',
    title: 'FRET Config',
    inputs: [{ port: 'model', label: 'Model' }],
    outputs: [{ port: 'params', label: 'Config' }],
    defaultWidth: 320,
    createBody(nodeId) {
      return `
        <div class="param-row">
          <label>Grid size:</label>
          <input type="number" id="${nodeId}-grid" value="80" min="20" max="300" class="auto-update">
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
      setupAutoUpdate(nodeId, 'fret-params');
    },
    async execute(nodeId) {
      if (!getModelForNode(nodeId)) return;
      // Store config in node data
      const info = nodeRegistry[nodeId];
      if (info) {
        info.data = info.data || {};
        info.data.config = {
          grid: parseInt(document.getElementById(`${nodeId}-grid`)?.value || '80'),
          min: parseFloat(document.getElementById(`${nodeId}-min`)?.value || '-6'),
          max: parseFloat(document.getElementById(`${nodeId}-max`)?.value || '6')
        };
      }
    },
  },
  'fret-result': {
    category: 'result',
    headerClass: 'header-result',
    title: 'FRET Result',
    inputs: [{ port: 'params', label: 'Config' }],
    outputs: [],
    defaultWidth: 600,
    createBody(nodeId) {
      return `
        <button class="btn btn-run" data-action="executeFRETResult" data-node="${nodeId}">Run</button>
        <div class="viewer-content" id="${nodeId}-content">
          <span class="text-dim">Connect to FRET Config and click Run.</span>
        </div>
      `;
    },
    async execute(nodeId) {
      await executeFRETResult(nodeId);
    },
  },
  'fret-heatmap': {
    category: 'viewer',
    headerClass: 'header-viewer',
    title: 'FRET Heatmap',
    inputs: [{ port: 'model', label: 'Model' }],
    outputs: [],
    defaultWidth: 420,
    createBody(nodeId) {
      return `
        <div class="param-row">
          <label>Grid size:</label>
          <input type="number" id="${nodeId}-grid" value="80" min="20" max="300">
        </div>
        <button class="btn btn-run" data-action="recomputeHeatmap" data-node="${nodeId}">Run</button>
        <div class="viewer-content" id="${nodeId}-content"><span class="text-dim">Waiting for model (d=2 only)...</span></div>
      `;
    },
    async execute(nodeId) {
      const nGrid = parseInt(document.getElementById(`${nodeId}-grid`)?.value || '80');
      const contentEl = document.getElementById(`${nodeId}-content`);
      setNodeLoading(nodeId, true);
      try {
        const sessionId = getSessionIdForNode(nodeId);
        if (!sessionId) throw new Error('Build the connected model first');
        const data = await api('fret_heatmap', {
          session_id: sessionId,
          n_grid: nGrid,
        });
        if (nodeRegistry[nodeId]) {
          ensureNodeData(nodeId).fretHeatmapData = data;
        }
        contentEl.innerHTML = `<div class="plot-container" id="${nodeId}-plot"></div>`;
        commitWorkspaceSnapshot('fret-heatmap');
        setTimeout(() => {
          plotHeatmap(data, `${nodeId}-plot`);
          setupPlotResize(nodeId, `${nodeId}-plot`);
        }, 50);
      } catch (e) {
        contentEl.innerHTML = `<div class="node-error">${e.message}</div>`;
      }
      setNodeLoading(nodeId, false);
    },
  },
};
