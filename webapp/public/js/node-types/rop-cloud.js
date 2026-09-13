import { connections, nodeRegistry } from '../state.js';
import { getModelForNode, setupAutoUpdate } from '../nodes.js';
import { getReactionsFromNode } from '../model.js';
import {
  executeROPCloudResult,
  updateROPCloudMode,
  refreshROPCloudTargetOptions,
  executeFRETResult,
  installDerivedResultInvalidation,
  runROPCloud,
  runFRETHeatmap,
} from '../rop-cloud.js';

function ropCloudSamplingBody(nodeId, { modeLabel, autoUpdate }) {
  const auto = autoUpdate ? ' class="auto-update"' : '';
  return `
        <div class="param-row">
          <label>${modeLabel}</label>
          <select id="${nodeId}-sampling-mode" data-action="updateROPCloudMode" data-node="${nodeId}"${auto}>
            <option value="x_space">x-space closed-form</option>
            <option value="qk">qK sampling (legacy)</option>
          </select>
        </div>
        <div class="param-row">
          <label>Samples:</label>
          <input type="number" id="${nodeId}-samples" value="10000" min="100" max="20000" step="1000"${auto}>
        </div>
        <div id="${nodeId}-xspace-params">
          <div class="param-row">
            <label>Target:</label>
            <select id="${nodeId}-target-species"${auto}></select>
          </div>
          <div class="param-row">
            <label>log10(x) min:</label>
            <input type="number" id="${nodeId}-logx-min" value="-6" min="-20" max="20" step="0.5"${auto}>
          </div>
          <div class="param-row">
            <label>log10(x) max:</label>
            <input type="number" id="${nodeId}-logx-max" value="6" min="-20" max="20" step="0.5"${auto}>
          </div>
        </div>
        <div id="${nodeId}-qk-params" style="display:none;">
          <div class="param-row">
            <label>Span:</label>
            <input type="number" id="${nodeId}-span" value="6" min="1" max="20"${auto}>
          </div>
        </div>`;
}

export const ROP_CLOUD_TYPES = {
  'rop-cloud-params': {
    category: 'parameter',
    headerClass: 'header-parameter',
    title: 'ROP Cloud Config',
    inputs: [{ port: 'reactions', type: 'NetworkIR', label: 'Reactions' }, { port: 'model', type: 'ModelArtifact', label: 'Model' }],
    outputs: [{ port: 'params', type: 'ROPCloudConfig', label: 'Config' }],
    defaultWidth: 320,
    createBody(nodeId) {
      return `${ropCloudSamplingBody(nodeId, { modeLabel: 'Sampling mode:', autoUpdate: true })}
      `;
    },
    onInit(nodeId) {
      updateROPCloudMode(nodeId);
      installDerivedResultInvalidation(nodeId);
      setupAutoUpdate(nodeId, 'rop-cloud-params');
    },
    async prepare(nodeId) {
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
    inputs: [{ port: 'params', type: 'ROPCloudConfig', label: 'Config' }],
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
      return executeROPCloudResult(nodeId);
    },
  },
  'rop-cloud': {
    category: 'viewer',
    headerClass: 'header-viewer',
    title: 'ROP Point Cloud',
    inputs: [{ port: 'reactions', type: 'NetworkIR', label: 'Reactions' }, { port: 'model', type: 'ModelArtifact', label: 'Model' }],
    outputs: [],
    defaultWidth: 420,
    createBody(nodeId) {
      return `${ropCloudSamplingBody(nodeId, { modeLabel: 'Mode:', autoUpdate: false })}
        <button class="btn btn-run" data-action="recomputeROPCloud" data-node="${nodeId}">Run</button>
        <div class="viewer-content" id="${nodeId}-content"><span class="text-dim">Waiting for input...</span></div>
      `;
    },
    onInit(nodeId) {
      updateROPCloudMode(nodeId);
    },
  },
  'fret-params': {
    category: 'parameter',
    headerClass: 'header-parameter',
    title: 'FRET Config',
    inputs: [{ port: 'model', type: 'ModelArtifact', label: 'Model' }],
    outputs: [{ port: 'params', type: 'FRETConfig', label: 'Config' }],
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
      installDerivedResultInvalidation(nodeId);
      setupAutoUpdate(nodeId, 'fret-params');
    },
    async prepare(nodeId) {
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
    inputs: [{ port: 'params', type: 'FRETConfig', label: 'Config' }],
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
      return executeFRETResult(nodeId);
    },
  },
  'fret-heatmap': {
    category: 'viewer',
    headerClass: 'header-viewer',
    title: 'FRET Heatmap',
    inputs: [{ port: 'model', type: 'ModelArtifact', label: 'Model' }],
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
  },
};

export function recomputeROPCloud(nodeId) {
  runROPCloud(nodeId);
}

export function recomputeHeatmap(nodeId) {
  runFRETHeatmap(nodeId);
}
