import { api } from '../api.js';
import { setNodeLoading, getModelForNode, getSessionIdForNode, setupAutoUpdate, hasModelContextForNode } from '../nodes.js';
import { executeRegimeGraph, updateRegimeGraphMode } from '../regime-graph.js';

export const RESULT_TYPES = {
  'model-summary': {
    category: 'result',
    headerClass: 'header-result',
    title: 'Model Summary',
    inputs: [{ port: 'model', label: 'Model' }],
    outputs: [],
    defaultWidth: 300,
    createBody(nodeId) {
      return `<div class="viewer-content" id="${nodeId}-content"><span class="text-dim">Connect to a Model Builder to see summary.</span></div>`;
    },
    async execute(nodeId) {
      const contentEl = document.getElementById(`${nodeId}-content`);
      const m = getModelForNode(nodeId);
      if (!m) { contentEl.innerHTML = '<span class="text-dim">No model built yet.</span>'; return; }
      contentEl.innerHTML = `
        <table>
          <tr><th>Property</th><th>Value</th></tr>
          <tr><td>Species (n)</td><td>${m.n}</td></tr>
          <tr><td>Totals (d)</td><td>${m.d}</td></tr>
          <tr><td>Reactions (r)</td><td>${m.r}</td></tr>
          <tr><td>Species</td><td>${m.x_sym.join(', ')}</td></tr>
          <tr><td>Totals</td><td>${m.q_sym.join(', ')}</td></tr>
          <tr><td>Constants</td><td>${m.K_sym.join(', ')}</td></tr>
        </table>
        <div style="margin-top:8px;"><strong>N matrix:</strong></div>
        <pre style="font-size:10px;color:#aaa;margin:4px 0;">${m.N.map(r => r.map(v => String(v).padStart(3)).join(' ')).join('\n')}</pre>
        <div><strong>L matrix:</strong></div>
        <pre style="font-size:10px;color:#aaa;margin:4px 0;">${m.L.map(r => r.map(v => String(v).padStart(3)).join(' ')).join('\n')}</pre>
      `;
    },
  },
  'vertices-table': {
    category: 'result',
    headerClass: 'header-result',
    title: 'Vertices Table',
    inputs: [{ port: 'model', label: 'Model' }],
    outputs: [],
    defaultWidth: 380,
    createBody(nodeId) {
      return `<div class="viewer-content" id="${nodeId}-content"><span class="text-dim">Waiting for model...</span></div>`;
    },
    async execute(nodeId) {
      const contentEl = document.getElementById(`${nodeId}-content`);
      setNodeLoading(nodeId, true);
      try {
        const sessionId = getSessionIdForNode(nodeId);
        if (!sessionId) throw new Error('Build the connected model first');
        const data = await api('find_vertices', { session_id: sessionId });
        let html = '<table><thead><tr><th>#</th><th>Perm</th><th>Species</th><th>Type</th><th>Nullity</th></tr></thead><tbody>';
        data.vertices.forEach(v => {
          const typeTag = v.asymptotic
            ? '<span class="tag tag-asym">Asymp</span>'
            : '<span class="tag tag-nonasym">Non-A</span>';
          const singTag = v.singular
            ? ' <span class="tag tag-singular">Sing</span>'
            : ' <span class="tag tag-invertible">Inv</span>';
          const speciesStr = v.species ? v.species.join(', ') : '';
          html += `<tr><td>${v.idx}</td><td>[${v.perm.join(',')}]</td><td style="font-family:monospace;font-size:10px;">${speciesStr}</td><td>${typeTag}${singTag}</td><td>${v.nullity}</td></tr>`;
        });
        html += '</tbody></table>';
        contentEl.innerHTML = html;
      } catch (e) {
        contentEl.innerHTML = `<div class="node-error">${e.message}</div>`;
      }
      setNodeLoading(nodeId, false);
    },
  },
  'regime-graph': {
    category: 'result',
    headerClass: 'header-result',
    title: 'Regime Graph',
    inputs: [{ port: 'model', label: 'Model' }],
    outputs: [],
    defaultWidth: 840,
    defaultHeight: 840,
    createBody(nodeId) {
      return `
        <div class="param-row">
          <label>Graph:</label>
          <select id="${nodeId}-graph-mode" class="auto-update" data-action="updateRegimeGraphMode" data-node="${nodeId}">
            <option value="qk">qK-neighbor</option>
            <option value="siso">SISO</option>
          </select>
        </div>
        <div class="param-row" id="${nodeId}-change-qk-row" style="display:none;">
          <label>Change qK:</label>
          <select id="${nodeId}-change-qk" class="auto-update"></select>
        </div>
        <div class="viewer-content" id="${nodeId}-content"><span class="text-dim">Waiting for model...</span></div>
      `;
    },
    onInit(nodeId) {
      setupAutoUpdate(nodeId, 'regime-graph');
      const node = document.getElementById(nodeId);
      if (node) {
        node.querySelectorAll('.auto-update').forEach(input => {
          input.addEventListener('change', () => {
            if (hasModelContextForNode(nodeId)) executeRegimeGraph(nodeId);
          });
        });
      }
      updateRegimeGraphMode(nodeId);
    },
    async execute(nodeId) {
      await executeRegimeGraph(nodeId);
    },
  },
};
