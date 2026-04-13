// Biocircuits Explorer — Node Edition Frontend (v3 — Dynamic Canvas)
const API = '';

// ===== State =====
const state = {
  sessionId: null,
  model: null,
  qK_syms: [],
};

const debugConsoleState = {
  open: false,
  entries: [],
  lastSeq: 0,
  pollTimer: null,
  fetchInFlight: false,
  unseenPriority: false,
};

const WORKSPACE_DOCUMENT_VERSION = 1;
const WORKSPACE_SHELL_CONTRACT_VERSION = 1;
const THEME_MODE_STORAGE_KEY = 'biocircuits-explorer.theme-mode';
const LIGHT_THEME_STYLESHEET_ID = 'biocircuits-explorer-light-theme-stylesheet';
const colorSchemeMediaQuery = window.matchMedia ? window.matchMedia('(prefers-color-scheme: light)') : null;

let workspaceShellHost = null;
let workspaceShellReady = false;
let workspaceShellSyncTimer = null;
let lastWorkspaceShellSnapshot = '';
const themeState = {
  mode: 'auto',
  effective: 'dark',
};

function workspaceShellMetadata() {
  return {
    contractVersion: WORKSPACE_SHELL_CONTRACT_VERSION,
    workspaceVersion: WORKSPACE_DOCUMENT_VERSION,
    schemaVersion: WORKSPACE_DOCUMENT_VERSION,
  };
}

function validateWorkspaceDocument(data) {
  if (!data || typeof data !== 'object' || Array.isArray(data)) {
    throw new Error('Workspace document must be an object');
  }

  const version = Number.isInteger(data.version) ? data.version : WORKSPACE_DOCUMENT_VERSION;
  if (version < 1) {
    throw new Error(`Unsupported workspace version: ${version}`);
  }
  if (version > WORKSPACE_DOCUMENT_VERSION) {
    throw new Error(`Workspace version ${version} is newer than this app supports (${WORKSPACE_DOCUMENT_VERSION})`);
  }
  if (!Array.isArray(data.nodes)) {
    throw new Error('Workspace document is missing a nodes array');
  }
  if (data.connections != null && !Array.isArray(data.connections)) {
    throw new Error('Workspace document has an invalid connections array');
  }

  return {
    ...data,
    version,
    connections: Array.isArray(data.connections) ? data.connections : [],
  };
}

function queueWorkspaceShellSync(reason = 'unknown') {
  clearTimeout(workspaceShellSyncTimer);
  workspaceShellSyncTimer = window.setTimeout(() => {
    window.BiocircuitsExplorerWorkspaceShell?.notifyWorkspaceChanged(reason);
  }, 250);
}

function commitWorkspaceSnapshot(reason = 'unknown') {
  clearTimeout(workspaceShellSyncTimer);
  return window.BiocircuitsExplorerWorkspaceShell?.notifyWorkspaceChanged(reason) ?? false;
}

window.BiocircuitsExplorerWorkspaceShell = {
  ...workspaceShellMetadata(),

  registerHost(host) {
    workspaceShellHost = host || null;

    if (workspaceShellReady) {
      workspaceShellHost?.shellDidBecomeReady?.(workspaceShellMetadata());
    }

    return workspaceShellMetadata();
  },

  unregisterHost() {
    workspaceShellHost = null;
  },

  markReady() {
    if (workspaceShellReady) return;

    workspaceShellReady = true;
    workspaceShellHost?.shellDidBecomeReady?.(workspaceShellMetadata());
    window.dispatchEvent(new CustomEvent('biocircuits-explorer:workspace-shell-ready', {
      detail: workspaceShellMetadata(),
    }));
  },

  serializeWorkspace() {
    return JSON.stringify(serializeState());
  },

  notifyWorkspaceChanged(reason = 'unknown') {
    const jsonString = this.serializeWorkspace();
    if (!jsonString || jsonString === lastWorkspaceShellSnapshot) {
      return false;
    }

    lastWorkspaceShellSnapshot = jsonString;
    workspaceShellHost?.workspaceDidChange?.(jsonString, {
      reason,
      ...workspaceShellMetadata(),
    });
    window.dispatchEvent(new CustomEvent('biocircuits-explorer:workspace-changed', {
      detail: {
        reason,
        jsonString,
        ...workspaceShellMetadata(),
      },
    }));
    return true;
  },

  applyWorkspaceFromJSONString(jsonString) {
    const data = validateWorkspaceDocument(JSON.parse(jsonString));

    applyState(data);
    lastWorkspaceShellSnapshot = this.serializeWorkspace();
    return true;
  },

  saveWorkspace() {
    if (workspaceShellHost?.saveWorkspaceJSONString) {
      const jsonString = this.serializeWorkspace();
      lastWorkspaceShellSnapshot = jsonString;
      workspaceShellHost.saveWorkspaceJSONString(jsonString);
      showToast('Saved to the current JSON project');
      return true;
    }

    return defaultSaveState();
  },

  loadWorkspace() {
    if (workspaceShellHost?.requestCurrentWorkspace) {
      workspaceShellHost.requestCurrentWorkspace();
      showToast('Reloaded from the selected JSON project');
      return true;
    }

    return defaultLoadState();
  },

  setThemeMode(mode) {
    void applyThemeMode(mode);
    return true;
  },

  getThemeMode() {
    return themeState.mode;
  },

  runConnectedWorkspace() {
    void runConnectedWorkspace();
    return true;
  },
};

// ===== Node Registry =====
let nodeIdCounter = 0;
const nodeRegistry = {}; // id → { type, el, data }
let connections = [];     // { fromNode, fromPort, toNode, toPort }

function getIncomingConnections(nodeId) {
  return connections.filter(conn => conn.toNode === nodeId);
}

function findUpstreamNode(nodeId, predicate, visited = new Set()) {
  if (!nodeId || visited.has(nodeId)) return null;
  visited.add(nodeId);

  if (predicate(nodeId)) {
    return nodeId;
  }

  for (const conn of getIncomingConnections(nodeId)) {
    const found = findUpstreamNode(conn.fromNode, predicate, visited);
    if (found) return found;
  }

  return null;
}

function findUpstreamNodeByType(nodeId, type) {
  return findUpstreamNode(nodeId, candidateId => nodeRegistry[candidateId]?.type === type);
}

function getModelContextFromBuilder(modelBuilderNodeId) {
  return nodeRegistry[modelBuilderNodeId]?.data?.modelContext || null;
}

function getModelContextForNode(nodeId) {
  if (!nodeId) return null;
  const modelBuilderNodeId = findUpstreamNodeByType(nodeId, 'model-builder');
  if (!modelBuilderNodeId) return null;
  return getModelContextFromBuilder(modelBuilderNodeId);
}

function getModelForNode(nodeId) {
  return getModelContextForNode(nodeId)?.model || null;
}

function getSessionIdForNode(nodeId) {
  return getModelContextForNode(nodeId)?.sessionId || null;
}

function getQKSymbolsForNode(nodeId) {
  return getModelContextForNode(nodeId)?.qK_syms || [];
}

function hasModelContextForNode(nodeId) {
  return !!getModelContextForNode(nodeId);
}

// ===== Port Types & Validation =====
const PORT_TYPES = {
  reactions: 'reactions',
  model: 'model',
  params: 'params',
  result: 'result',
  'atlas-spec': 'atlas-spec',
  atlas: 'atlas',
  'atlas-query': 'atlas-query',
};

const PORT_COLOR_GROUPS = {
  reactions: 'reactions',
  model: 'model',
  params: 'params',
  result: 'result',
  'atlas-spec': 'params',
  atlas: 'model',
  'atlas-query': 'params',
};

function getPortColor(port) {
  const group = PORT_COLOR_GROUPS[port];
  if (!group) return '#888';
  return getComputedStyle(document.documentElement)
    .getPropertyValue(`--port-${group}`)
    .trim() || '#888';
}

// ===== NODE_TYPES Registry =====
const NODE_TYPES = {
  'markdown-note': {
    category: 'note',
    headerClass: 'header-note',
    title: 'Markdown Note',
    inputs: [],
    outputs: [],
    defaultWidth: 400,
    defaultHeight: 300,
    createBody(nodeId) {
      return `
        <div class="note-tabs">
          <button class="note-tab active" data-tab="edit" onclick="switchNoteTab('${nodeId}', 'edit')">Edit</button>
          <button class="note-tab" data-tab="preview" onclick="switchNoteTab('${nodeId}', 'preview')">Preview</button>
        </div>
        <div class="note-edit-area" id="${nodeId}-edit-area">
          <textarea id="${nodeId}-markdown" class="markdown-editor" placeholder="Write your markdown notes here...

# Example
- Bullet point
- **Bold text**
- *Italic text*
- [Link](https://example.com)
"></textarea>
        </div>
        <div class="note-preview-area" id="${nodeId}-preview-area" style="display:none;">
          <div id="${nodeId}-preview" class="markdown-preview"></div>
        </div>
      `;
    },
    onInit(nodeId) {
      const textarea = document.getElementById(`${nodeId}-markdown`);
      if (textarea) {
        // Auto-save on input
        textarea.addEventListener('input', () => {
          const info = nodeRegistry[nodeId];
          if (info) {
            info.data = info.data || {};
            info.data.markdown = textarea.value;
            // Update preview if in preview mode
            const previewArea = document.getElementById(`${nodeId}-preview-area`);
            if (previewArea && previewArea.style.display !== 'none') {
              renderMarkdown(nodeId);
            }
          }
        });
      }
    },
  },
  'reaction-network': {
    category: 'input',
    headerClass: 'header-input',
    title: 'Reaction Network',
    inputs: [],
    outputs: [{ port: 'reactions', label: 'Reactions' }],
    defaultWidth: 280,
    createBody(nodeId) {
      return `
        <div class="reaction-header">
          <span class="reaction-header-label">Reaction</span>
          <span class="reaction-header-label reaction-header-kd">Kd (opt)</span>
          <span class="reaction-header-spacer"></span>
        </div>
        <div id="${nodeId}-reactions-list"></div>
        <button class="btn btn-small" onclick="addReactionRow('${nodeId}')">+ Add Reaction</button>
      `;
    },
    onInit(nodeId) {
      addReactionRow(nodeId, 'E + S <-> C_ES', 1e-3);
      addReactionRow(nodeId, 'E + P <-> C_EP', 1e-3);
    },
  },
  'model-builder': {
    category: 'process',
    headerClass: 'header-process',
    title: 'Model Builder',
    inputs: [{ port: 'reactions', label: 'Reactions' }],
    outputs: [{ port: 'model', label: 'Model' }],
    defaultWidth: 260,
    createBody(nodeId) {
      return `
        <div class="node-info" id="${nodeId}-model-info" style="display:none;">
          <pre id="${nodeId}-model-info-text"></pre>
        </div>
        <button class="btn btn-primary" onclick="buildModel('${nodeId}')">Build Model</button>
      `;
    },
    onInit(nodeId) {
      setupAutoModelBuild(nodeId);
    },
    async execute(nodeId) {
      await buildModel(nodeId, { triggerDownstream: false });
    },
  },
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
          <select id="${nodeId}-graph-mode" class="auto-update" onchange="updateRegimeGraphMode('${nodeId}')">
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
        <button class="btn btn-small" onclick="computeSISOResult('${nodeId}')">Run</button>
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
        <button class="btn btn-small" onclick="executeQKPolyResult('${nodeId}')">Compute</button>
        <div class="viewer-content" id="${nodeId}-content"><span class="text-dim">Connect to a SISO Behaviors node and select a path.</span></div>
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
        <button class="btn btn-small" onclick="recomputeSISO('${nodeId}')">Recompute</button>
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
          html += `<div class="path-item" data-idx="${p.idx}" data-qk="${changeQK}" data-node="${nodeId}" onclick="selectSISOPath(this)">#${p.idx}: ${permStr}</div>`;
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
          <select id="${nodeId}-sampling-mode" onchange="updateROPCloudMode('${nodeId}')">
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
        <button class="btn btn-small" onclick="recomputeROPCloud('${nodeId}')">Recompute</button>
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
        <button class="btn btn-small" onclick="recomputeHeatmap('${nodeId}')">Recompute</button>
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
          nodeRegistry[nodeId].data = nodeRegistry[nodeId].data || {};
          nodeRegistry[nodeId].data.fretHeatmapData = data;
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
            <select id="${nodeId}-species-helper" onchange="insertSpecies1D('${nodeId}')" style="width:80px;">
              <option value="">Insert...</option>
            </select>
          </div>
        </div>
        <button class="btn btn-primary" onclick="runParameterScan1D('${nodeId}')">Run Scan</button>
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
            <select id="${nodeId}-species-helper" onchange="insertSpecies2D('${nodeId}')" style="width:80px;">
              <option value="">Insert...</option>
            </select>
          </div>
        </div>
        <button class="btn btn-primary" onclick="runParameterScan2D('${nodeId}')">Run Scan</button>
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
          <select id="${nodeId}-dimension" onchange="updateROPPolyDimension('${nodeId}')">
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
        <button class="btn btn-primary" onclick="runROPPolyhedron('${nodeId}')">Compute Polyhedron</button>
        <div class="viewer-content" id="${nodeId}-content">
          <span class="text-dim">Connect to model and configure.</span>
        </div>
      `;
    },
    async execute(nodeId) {
      const model = getModelForNode(nodeId);
      if (!model) return;
      const savedConfig = nodeRegistry[nodeId]?.data?.config || nodeRegistry[nodeId]?.data || {};
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
            <select id="${nodeId}-species-helper" onchange="insertSpecies1D('${nodeId}')" style="width:80px;">
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
  'scan-1d-result': {
    category: 'result',
    headerClass: 'header-result',
    title: '1D Scan Result',
    inputs: [{ port: 'params', label: 'Config' }],
    outputs: [],
    defaultWidth: 420,
    createBody(nodeId) {
      return `
        <button class="btn btn-primary" onclick="executeScan1DResult('${nodeId}')">Run Scan</button>
        <div class="viewer-content" id="${nodeId}-content">
          <span class="text-dim">Connect to Scan 1D Config and click Run.</span>
        </div>
      `;
    },
    async execute(nodeId) {
      await executeScan1DResult(nodeId);
    },
  },
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
          <select id="${nodeId}-sampling-mode" onchange="updateROPCloudMode('${nodeId}')" class="auto-update">
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
        <button class="btn btn-primary" onclick="executeROPCloudResult('${nodeId}')">Run</button>
        <div class="viewer-content" id="${nodeId}-content">
          <span class="text-dim">Connect to ROP Cloud Config and click Run.</span>
        </div>
      `;
    },
    async execute(nodeId) {
      await executeROPCloudResult(nodeId);
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
        <button class="btn btn-primary" onclick="executeFRETResult('${nodeId}')">Run</button>
        <div class="viewer-content" id="${nodeId}-content">
          <span class="text-dim">Connect to FRET Config and click Run.</span>
        </div>
      `;
    },
    async execute(nodeId) {
      await executeFRETResult(nodeId);
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
            <select id="${nodeId}-species-helper" onchange="insertSpecies2D('${nodeId}')" style="width:80px;">
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
  'scan-2d-result': {
    category: 'result',
    headerClass: 'header-result',
    title: '2D Scan Result',
    inputs: [{ port: 'params', label: 'Config' }],
    outputs: [],
    defaultWidth: 600,
    createBody(nodeId) {
      return `
        <button class="btn btn-primary" onclick="executeScan2DResult('${nodeId}')">Run Scan</button>
        <div class="viewer-content" id="${nodeId}-content">
          <span class="text-dim">Connect to Scan 2D Config and click Run.</span>
        </div>
      `;
    },
    async execute(nodeId) {
      await executeScan2DResult(nodeId);
    },
  },
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
          <select id="${nodeId}-dimension" class="auto-update" onchange="updateROPPolyDimension('${nodeId}')">
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
      const savedConfig = nodeRegistry[nodeId]?.data?.config || {};
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
        <button class="btn btn-primary" onclick="executeROPPolyResult('${nodeId}')">Compute</button>
        <div class="viewer-content" id="${nodeId}-content">
          <span class="text-dim">Connect to ROP Polyhedron Config and click Compute.</span>
        </div>
      `;
    },
    async execute(nodeId) {
      await executeROPPolyResult(nodeId);
    },
  },
  'atlas-spec': {
    category: 'parameter',
    headerClass: 'header-parameter',
    title: 'Atlas Spec',
    inputs: [],
    outputs: [{ port: 'atlas-spec', label: 'Spec' }],
    defaultWidth: 420,
    defaultHeight: 620,
    createBody(nodeId) {
      return `
        <div class="tab-nav">
          <button class="tab-btn active" data-tab="basic">Basic</button>
          <button class="tab-btn" data-tab="behavior">Behavior</button>
          <button class="tab-btn" data-tab="enumeration">Enumeration</button>
          <button class="tab-btn" data-tab="explicit">Explicit</button>
        </div>

        <div class="tab-content active" data-tab="basic">
          <div class="atlas-config-section">
            <div class="atlas-section-title">Persistence</div>
            <div class="param-row">
              <label>Source label:</label>
              <input type="text" id="${nodeId}-source-label" value="" class="auto-update" placeholder="atlas_run_001">
            </div>
            <div class="param-row">
              <label>Library label:</label>
              <input type="text" id="${nodeId}-library-label" value="" class="auto-update" placeholder="optional">
            </div>
            <div class="param-row">
              <label>SQLite path:</label>
            </div>
            <textarea
              id="${nodeId}-sqlite-path"
              class="auto-update atlas-textarea atlas-textarea-compact atlas-textarea-singleline"
              placeholder="/absolute/path/to/atlas.sqlite"
            ></textarea>
            <div class="param-row">
              <label>Skip existing:</label>
              <input type="checkbox" id="${nodeId}-skip-existing" checked class="auto-update">
            </div>
            <div class="param-row">
              <label>Persist to SQLite:</label>
              <input type="checkbox" id="${nodeId}-persist-sqlite" class="auto-update">
            </div>
          </div>
          <div class="atlas-config-section">
            <div class="atlas-section-title">Search Profile</div>
            <div class="param-row">
              <label>Profile name:</label>
              <input type="text" id="${nodeId}-profile-name" value="binding_small_v0" class="auto-update">
            </div>
            <div class="param-row">
              <label>Max base species:</label>
              <input type="number" id="${nodeId}-max-base-species" value="4" min="1" max="12" class="auto-update">
            </div>
            <div class="param-row">
              <label>Max reactions:</label>
              <input type="number" id="${nodeId}-max-reactions" value="5" min="1" max="24" class="auto-update">
            </div>
            <div class="param-row">
              <label>Max support:</label>
              <input type="number" id="${nodeId}-max-support" value="3" min="1" max="12" class="auto-update">
            </div>
          </div>
        </div>

        <div class="tab-content" data-tab="behavior">
          <div class="atlas-config-section">
            <div class="atlas-section-title">Behavior Config</div>
            <div class="param-row">
              <label>Path scope:</label>
              <select id="${nodeId}-path-scope" class="auto-update">
                <option value="robust">robust</option>
                <option value="feasible">feasible</option>
                <option value="all">all graph paths</option>
              </select>
            </div>
            <div class="param-row">
              <label>Min volume:</label>
              <input type="number" id="${nodeId}-min-volume" value="0.01" min="0" step="0.01" class="auto-update">
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
              <label>Path records:</label>
              <input type="checkbox" id="${nodeId}-include-path-records" checked class="auto-update">
            </div>
          </div>
        </div>

        <div class="tab-content" data-tab="enumeration">
          <div class="atlas-config-section">
            <div class="atlas-section-title">Enumeration</div>
            <div class="param-row">
              <label>Enable:</label>
              <input type="checkbox" id="${nodeId}-enable-enumeration" checked class="auto-update">
            </div>
            <div class="param-row">
              <label>Mode:</label>
              <select id="${nodeId}-enum-mode" class="auto-update">
                <option value="pairwise_binding">pairwise_binding</option>
              </select>
            </div>
            <div class="param-row">
              <label>Base species counts:</label>
              <input type="text" id="${nodeId}-base-species-counts" value="2,3" class="auto-update" placeholder="2,3">
            </div>
            <div class="param-row">
              <label>Min reactions:</label>
              <input type="number" id="${nodeId}-min-enum-reactions" value="1" min="1" max="12" class="auto-update">
            </div>
            <div class="param-row">
              <label>Max reactions:</label>
              <input type="number" id="${nodeId}-max-enum-reactions" value="2" min="1" max="12" class="auto-update">
            </div>
            <div class="param-row">
              <label>Limit:</label>
              <input type="number" id="${nodeId}-enum-limit" value="0" min="0" step="1" class="auto-update">
            </div>
          </div>
        </div>

        <div class="tab-content" data-tab="explicit">
          <div class="atlas-config-section">
            <div class="atlas-section-title">Explicit Networks (JSON)</div>
            <textarea
              id="${nodeId}-explicit-networks"
              class="auto-update atlas-textarea"
              placeholder='[
  {
    "label": "monomer_dimer",
    "reactions": ["A + B <-> AB"],
    "input_symbols": ["tA"],
    "output_symbols": ["AB"]
  }
]'
            ></textarea>
          </div>
        </div>
      `;
    },
    onInit(nodeId) {
      setupAutoUpdate(nodeId, 'atlas-spec');
      setupTabNavigation(nodeId);
    },
  },
  'atlas-builder': {
    category: 'process',
    headerClass: 'header-process',
    title: 'Atlas Builder',
    inputs: [{ port: 'atlas-spec', label: 'Spec' }],
    outputs: [{ port: 'atlas', label: 'Atlas' }],
    defaultWidth: 460,
    defaultHeight: 480,
    createBody(nodeId) {
      return `
        <button class="btn btn-primary" onclick="executeAtlasBuilder('${nodeId}')">Build Atlas</button>
        <div class="viewer-content" id="${nodeId}-content">
          <span class="text-dim">Connect an Atlas Spec node and run the builder.</span>
        </div>
      `;
    },
    async execute(nodeId) {
      await executeAtlasBuilder(nodeId);
    },
  },
  'atlas-query-config': {
    category: 'parameter',
    headerClass: 'header-parameter',
    title: 'Atlas Query Config',
    inputs: [],
    outputs: [{ port: 'atlas-query', label: 'Query' }],
    defaultWidth: 400,
    defaultHeight: 620,
    createBody(nodeId) {
      return `
        <div class="tab-nav">
          <button class="tab-btn active" data-tab="basic">Basic</button>
          <button class="tab-btn" data-tab="behavior">Behavior</button>
          <button class="tab-btn" data-tab="structure">Structure</button>
          <button class="tab-btn" data-tab="advanced">Advanced</button>
        </div>

        <div class="tab-content active" data-tab="basic">
          <div class="atlas-config-section">
            <div class="atlas-section-title">Atlas Source</div>
            <div class="param-row">
              <label>Prefer persisted atlas:</label>
              <input type="checkbox" id="${nodeId}-prefer-persisted-atlas" checked class="auto-update">
            </div>
            <div class="param-row">
              <label>SQLite override:</label>
            </div>
            <textarea
              id="${nodeId}-query-sqlite-path"
              class="auto-update atlas-textarea atlas-textarea-compact atlas-textarea-singleline"
              placeholder="/absolute/path/to/atlas.sqlite"
            ></textarea>
          </div>
          <div class="atlas-config-section">
            <div class="atlas-section-title">Goal Query</div>
            <div class="param-row">
              <label>IO pair:</label>
              <input type="text" id="${nodeId}-goal-io" class="auto-update" placeholder="tA -> AB">
            </div>
            <div class="param-row">
              <label>Target motif:</label>
              <input type="text" id="${nodeId}-goal-motif" class="auto-update" placeholder="activation_with_saturation">
            </div>
            <div class="param-row">
              <label>Target exact:</label>
              <input type="text" id="${nodeId}-goal-exact" class="auto-update" placeholder="optional">
            </div>
            <div class="param-row">
              <label>Witness path:</label>
              <input type="text" id="${nodeId}-goal-witness" class="auto-update" placeholder="source:0 -> +1 -> sink:+1">
            </div>
            <div class="param-row">
              <label>Must transitions:</label>
              <input type="text" id="${nodeId}-goal-transitions" class="auto-update" placeholder="0->+1,+1->0">
            </div>
            <div class="param-row">
              <label>Forbid regimes:</label>
              <input type="text" id="${nodeId}-goal-forbid-regimes" class="auto-update" placeholder="singular">
            </div>
            <div class="param-row">
              <label>Require robust:</label>
              <input type="checkbox" id="${nodeId}-goal-robust" class="auto-update">
            </div>
            <div class="param-row">
              <label>Require feasible:</label>
              <input type="checkbox" id="${nodeId}-goal-feasible" class="auto-update">
            </div>
            <div class="param-row">
              <label>Min witness volume:</label>
              <input type="number" id="${nodeId}-goal-min-volume" value="" step="0.001" min="0" class="auto-update" placeholder="optional">
            </div>
          </div>
          <div class="atlas-config-section">
            <div class="atlas-section-title">Ranking</div>
            <div class="param-row">
              <label>Ranking mode:</label>
              <select id="${nodeId}-ranking-mode" class="auto-update">
                <option value="minimal_first">minimal_first</option>
                <option value="robustness_first">robustness_first</option>
              </select>
            </div>
            <div class="param-row">
              <label>Collapse by network:</label>
              <input type="checkbox" id="${nodeId}-collapse-by-network" checked class="auto-update">
            </div>
            <div class="param-row">
              <label>Pareto only:</label>
              <input type="checkbox" id="${nodeId}-pareto-only" class="auto-update">
            </div>
            <div class="param-row">
              <label>Limit:</label>
              <input type="number" id="${nodeId}-query-limit" value="20" min="1" step="1" class="auto-update">
            </div>
          </div>
        </div>

        <div class="tab-content" data-tab="behavior">
          <div class="atlas-config-section">
            <div class="atlas-section-title">Behavior Filters</div>
            <div class="param-row">
              <label>Motif labels:</label>
              <input type="text" id="${nodeId}-motif-labels" class="auto-update" placeholder="activation_with_saturation,biphasic_peak">
            </div>
            <div class="param-row">
              <label>Motif mode:</label>
              <select id="${nodeId}-motif-match-mode" class="auto-update">
                <option value="any">any</option>
                <option value="all">all</option>
              </select>
            </div>
            <div class="param-row">
              <label>Exact labels:</label>
              <input type="text" id="${nodeId}-exact-labels" class="auto-update" placeholder="up-up-down">
            </div>
            <div class="param-row">
              <label>Exact mode:</label>
              <select id="${nodeId}-exact-match-mode" class="auto-update">
                <option value="any">any</option>
                <option value="all">all</option>
              </select>
            </div>
          </div>
          <div class="atlas-config-section">
            <div class="atlas-section-title">IO + Robustness</div>
            <div class="param-row">
              <label>Inputs:</label>
              <input type="text" id="${nodeId}-input-symbols" class="auto-update" placeholder="tA,tB">
            </div>
            <div class="param-row">
              <label>Outputs:</label>
              <input type="text" id="${nodeId}-output-symbols" class="auto-update" placeholder="AB,ABC">
            </div>
            <div class="param-row">
              <label>Require robust:</label>
              <input type="checkbox" id="${nodeId}-require-robust" class="auto-update">
            </div>
            <div class="param-row">
              <label>Min robust paths:</label>
              <input type="number" id="${nodeId}-min-robust-path-count" value="0" min="0" step="1" class="auto-update">
            </div>
          </div>
        </div>

        <div class="tab-content" data-tab="structure">
          <div class="atlas-config-section">
            <div class="atlas-section-title">Structural Bounds</div>
            <div class="param-row">
              <label>Max base species:</label>
              <input type="number" id="${nodeId}-query-max-base-species" value="" min="1" step="1" class="auto-update" placeholder="optional">
            </div>
            <div class="param-row">
              <label>Max reactions:</label>
              <input type="number" id="${nodeId}-query-max-reactions" value="" min="1" step="1" class="auto-update" placeholder="optional">
            </div>
            <div class="param-row">
              <label>Max support:</label>
              <input type="number" id="${nodeId}-query-max-support" value="" min="1" step="1" class="auto-update" placeholder="optional">
            </div>
            <div class="param-row">
              <label>Max support mass:</label>
              <input type="number" id="${nodeId}-query-max-support-mass" value="" min="0" step="1" class="auto-update" placeholder="optional">
            </div>
          </div>
          <div class="atlas-config-section">
            <div class="atlas-section-title">Graph Spec</div>
            <div class="param-row">
              <label>Required regimes:</label>
            </div>
            <textarea
              id="${nodeId}-required-regimes"
              class="auto-update atlas-textarea atlas-textarea-compact"
              placeholder='[
  {"role": "source", "output_order_token": "+1"},
  {"role": "sink", "output_order_token": "0"}
]'
            ></textarea>
            <div class="param-row">
              <label>Forbidden regimes:</label>
            </div>
            <textarea
              id="${nodeId}-forbidden-regimes"
              class="auto-update atlas-textarea atlas-textarea-compact"
              placeholder='[
  {"singular": true}
]'
            ></textarea>
            <div class="param-row">
              <label>Required transitions:</label>
            </div>
            <textarea
              id="${nodeId}-required-transitions"
              class="auto-update atlas-textarea atlas-textarea-compact"
              placeholder='[
  {"transition_token": "+1->0"}
]'
            ></textarea>
            <div class="param-row">
              <label>Forbidden transitions:</label>
            </div>
            <textarea
              id="${nodeId}-forbidden-transitions"
              class="auto-update atlas-textarea atlas-textarea-compact"
              placeholder='[
  {"transition_token": "+1->-1"}
]'
            ></textarea>
          </div>
          <div class="atlas-config-section">
            <div class="atlas-section-title">Path Spec</div>
            <div class="param-row">
              <label>Required sequences:</label>
            </div>
            <textarea
              id="${nodeId}-required-path-sequences"
              class="auto-update atlas-textarea atlas-textarea-compact"
              placeholder='[
  [
    {"role": "source", "output_order_token": "+1"},
    {"role": "sink", "output_order_token": "0"}
  ]
]'
            ></textarea>
            <div class="param-row">
              <label>Forbid singular:</label>
              <input type="checkbox" id="${nodeId}-forbid-singular-on-witness" class="auto-update">
            </div>
            <div class="param-row">
              <label>Max path length:</label>
              <input type="number" id="${nodeId}-max-witness-path-length" value="" min="1" step="1" class="auto-update" placeholder="optional">
            </div>
          </div>
          <div class="atlas-config-section">
            <div class="atlas-section-title">Polytope Spec</div>
            <div class="param-row">
              <label>Require feasible:</label>
              <input type="checkbox" id="${nodeId}-require-witness-feasible" class="auto-update">
            </div>
            <div class="param-row">
              <label>Require robust:</label>
              <input type="checkbox" id="${nodeId}-require-witness-robust" class="auto-update">
            </div>
            <div class="param-row">
              <label>Min volume mean:</label>
              <input type="number" id="${nodeId}-min-witness-volume-mean" value="" step="0.001" min="0" class="auto-update" placeholder="optional">
            </div>
          </div>
        </div>

        <div class="tab-content" data-tab="advanced">
          <div class="atlas-config-section">
            <div class="atlas-section-title">Condition Builder</div>
            <div class="atlas-builder-group">
              <div class="atlas-builder-head">
                <span>Required regimes</span>
                <button type="button" class="btn btn-small" onclick="addAtlasBuilderRow('${nodeId}', 'builder-required-regimes', 'regime')">+ Add</button>
              </div>
              <div id="${nodeId}-builder-required-regimes" class="atlas-builder-list"></div>
            </div>
            <div class="atlas-builder-group">
              <div class="atlas-builder-head">
                <span>Forbidden regimes</span>
                <button type="button" class="btn btn-small" onclick="addAtlasBuilderRow('${nodeId}', 'builder-forbidden-regimes', 'regime')">+ Add</button>
              </div>
              <div id="${nodeId}-builder-forbidden-regimes" class="atlas-builder-list"></div>
            </div>
            <div class="atlas-builder-group">
              <div class="atlas-builder-head">
                <span>Required transitions</span>
                <button type="button" class="btn btn-small" onclick="addAtlasBuilderRow('${nodeId}', 'builder-required-transitions', 'transition')">+ Add</button>
              </div>
              <div id="${nodeId}-builder-required-transitions" class="atlas-builder-list"></div>
            </div>
            <div class="atlas-builder-group">
              <div class="atlas-builder-head">
                <span>Witness stages</span>
                <button type="button" class="btn btn-small" onclick="addAtlasBuilderRow('${nodeId}', 'builder-witness-sequence', 'regime')">+ Add</button>
              </div>
              <div id="${nodeId}-builder-witness-sequence" class="atlas-builder-list"></div>
            </div>
          </div>
          <div class="atlas-config-section">
            <div class="atlas-section-title">Preview</div>
            <div id="${nodeId}-behavior-sketch"></div>
            <pre id="${nodeId}-query-preview" class="atlas-query-preview"></pre>
          </div>
        </div>
      `;
    },
    onInit(nodeId) {
      setupAutoUpdate(nodeId, 'atlas-query-config');
      setupTabNavigation(nodeId);
      refreshAtlasQueryDesigner(nodeId);
    },
  },
  'atlas-query-result': {
    category: 'result',
    headerClass: 'header-result',
    title: 'Atlas Query Result',
    inputs: [{ port: 'atlas', label: 'Atlas' }, { port: 'atlas-query', label: 'Query' }],
    outputs: [],
    defaultWidth: 640,
    defaultHeight: 540,
    createBody(nodeId) {
      return `
        <button class="btn btn-primary" onclick="executeAtlasQueryResult('${nodeId}')">Run Query</button>
        <div class="viewer-content" id="${nodeId}-content">
          <span class="text-dim">Connect an Atlas Builder and an Atlas Query Config node, or provide a SQLite atlas path in the query config.</span>
        </div>
      `;
    },
    async execute(nodeId) {
      await executeAtlasQueryResult(nodeId);
    },
  },
};

// Required predecessor chain for each node type
const PREREQ_CHAIN = {
  'model-builder': ['reaction-network'],
  'model-summary': ['reaction-network', 'model-builder'],
  'vertices-table': ['reaction-network', 'model-builder'],
  'regime-graph': ['reaction-network', 'model-builder'],
  'siso-analysis': ['reaction-network', 'model-builder'],
  'siso-params': ['reaction-network', 'model-builder'],
  'siso-result': ['siso-params'],
  'qk-poly-result': ['siso-result'],
  'rop-cloud': ['reaction-network'],
  'fret-heatmap': ['reaction-network', 'model-builder'],
  'parameter-scan-1d': ['reaction-network', 'model-builder'],
  'parameter-scan-2d': ['reaction-network', 'model-builder'],
  'rop-polyhedron': ['model-builder'],
  'scan-1d-params': ['reaction-network', 'model-builder'],
  'scan-1d-result': ['scan-1d-params'],
  'rop-cloud-params': ['reaction-network'],
  'rop-cloud-result': ['rop-cloud-params'],
  'fret-params': ['reaction-network', 'model-builder'],
  'fret-result': ['fret-params'],
  'scan-2d-params': ['reaction-network', 'model-builder'],
  'scan-2d-result': ['scan-2d-params'],
  'rop-poly-params': ['model-builder'],
  'rop-poly-result': ['rop-poly-params'],
  'atlas-spec': [],
  'atlas-builder': ['atlas-spec'],
  'atlas-query-config': [],
  'atlas-query-result': ['atlas-builder', 'atlas-query-config'],
};

// ===== Canvas Interaction =====
const editor = document.getElementById('editor');
const canvas = document.getElementById('canvas');
const svgLayer = document.getElementById('svg-layer');

let panX = 0, panY = 0, isPanning = false, startPanX = 0, startPanY = 0;
let scale = 1.0;
const MIN_SCALE = 0.1;
const MAX_SCALE = 3.0;
const ZOOM_SENSITIVITY = 0.0048;
let isDraggingNode = false, draggedNode = null, nodeOffsetX = 0, nodeOffsetY = 0;
let isWiring = false, wireStartSocket = null, wireStartIsOutput = true, tempWire = null;
let isResizing = false, resizeNode = null, resizeStartX = 0, resizeStartY = 0, resizeStartW = 0, resizeStartH = 0;

function applyViewportTransform() {
  // Keep a single transform source (canvas). svgLayer stays in canvas-local coordinates.
  canvas.style.transform = `translate(${panX}px, ${panY}px) scale(${scale})`;
  svgLayer.style.transform = 'none';
  editor.style.backgroundPosition = `${panX}px ${panY}px`;
  editor.style.backgroundSize = `${50 * scale}px ${50 * scale}px`;
}

function findScrollableAncestor(target, stopAt = editor) {
  let el = target instanceof Element ? target : null;
  while (el && el !== stopAt) {
    const style = window.getComputedStyle(el);
    const canScrollY = ['auto', 'scroll'].includes(style.overflowY) && el.scrollHeight > el.clientHeight + 1;
    const canScrollX = ['auto', 'scroll'].includes(style.overflowX) && el.scrollWidth > el.clientWidth + 1;
    if (canScrollY || canScrollX) return el;
    el = el.parentElement;
  }
  return null;
}

function findWheelScrollableAncestor(target, deltaX, deltaY, stopAt = editor) {
  let el = target instanceof Element ? target : null;
  while (el && el !== stopAt) {
    if (el.classList?.contains('node-body')) {
      el = el.parentElement;
      continue;
    }

    const style = window.getComputedStyle(el);
    const canScrollY = ['auto', 'scroll'].includes(style.overflowY) && el.scrollHeight > el.clientHeight + 1;
    const canScrollX = ['auto', 'scroll'].includes(style.overflowX) && el.scrollWidth > el.clientWidth + 1;
    const canConsumeY = canScrollY && (
      (deltaY < 0 && el.scrollTop > 0) ||
      (deltaY > 0 && el.scrollTop + el.clientHeight < el.scrollHeight - 1)
    );
    const canConsumeX = canScrollX && (
      (deltaX < 0 && el.scrollLeft > 0) ||
      (deltaX > 0 && el.scrollLeft + el.clientWidth < el.scrollWidth - 1)
    );

    if (canConsumeY || canConsumeX) return el;
    el = el.parentElement;
  }
  return null;
}

function isInteractivePlotTarget(target) {
  return target instanceof Element && !!target.closest('.plot-container, .js-plotly-plot, .plotly, .modebar');
}

function normalizeWheelDelta(delta, deltaMode) {
  if (deltaMode === 1) {
    return delta * 16;
  }
  if (deltaMode === 2) {
    return delta * Math.max(editor.clientHeight, 800);
  }
  return delta;
}

function computeZoomFactor(e) {
  const normalizedDelta = normalizeWheelDelta(e.deltaY, e.deltaMode);
  const clampedDelta = Math.max(-240, Math.min(240, normalizedDelta));
  return Math.exp(-clampedDelta * ZOOM_SENSITIVITY);
}

// Canvas panning (middle/right mouse button, or left-click on blank area)
editor.addEventListener('mousedown', (e) => {
  if (e.button === 1 || e.button === 2) {
    isPanning = true;
    startPanX = e.clientX - panX;
    startPanY = e.clientY - panY;
    e.preventDefault();
  } else if (e.button === 0 && (e.target === editor || e.target === canvas || e.target === svgLayer)) {
    isPanning = true;
    startPanX = e.clientX - panX;
    startPanY = e.clientY - panY;
    e.preventDefault();
  }
});

// Wheel / trackpad panning and zooming
editor.addEventListener('wheel', (e) => {
  if (isInteractivePlotTarget(e.target)) {
    return;
  }

  if (!(e.ctrlKey || e.metaKey) && findWheelScrollableAncestor(e.target, e.deltaX, e.deltaY)) {
    return;
  }

  e.preventDefault();

  if (e.ctrlKey || e.metaKey) {
    // Zoom mode
    const oldScale = scale;
    const zoomFactor = computeZoomFactor(e);
    scale = Math.max(MIN_SCALE, Math.min(MAX_SCALE, scale * zoomFactor));

    // Calculate mouse position relative to editor
    const rect = editor.getBoundingClientRect();
    const mouseX = e.clientX - rect.left;
    const mouseY = e.clientY - rect.top;

    // Adjust pan to keep mouse position fixed
    const scaleDiff = scale / oldScale;
    panX = mouseX - (mouseX - panX) * scaleDiff;
    panY = mouseY - (mouseY - panY) * scaleDiff;
  } else {
    // Pan mode
    panX -= e.deltaX;
    panY -= e.deltaY;
  }

  applyViewportTransform();
  updateConnections();
}, { passive: false });

window.addEventListener('mousemove', (e) => {
  if (isPanning) {
    panX = e.clientX - startPanX;
    panY = e.clientY - startPanY;
    applyViewportTransform();
    updateConnections();
  }
  if (isDraggingNode && draggedNode) {
    const canvasRect = canvas.getBoundingClientRect();
    draggedNode.style.left = `${(e.clientX - canvasRect.left) / scale - nodeOffsetX}px`;
    draggedNode.style.top = `${(e.clientY - canvasRect.top) / scale - nodeOffsetY}px`;
    updateConnections();
  }
  if (isResizing && resizeNode) {
    const dw = (e.clientX - resizeStartX) / scale;
    const dh = (e.clientY - resizeStartY) / scale;
    resizeNode.style.width = Math.max(240, resizeStartW + dw) + 'px';
    resizeNode.style.height = Math.max(100, resizeStartH + dh) + 'px';
    const plotEl = resizeNode.querySelector('.plot-container');
    if (plotEl) Plotly.Plots.resize(plotEl);
    updateConnections();
  }
  if (isWiring && tempWire && wireStartSocket) {
    const canvasRect = canvas.getBoundingClientRect();
    const mx = (e.clientX - canvasRect.left) / scale;
    const my = (e.clientY - canvasRect.top) / scale;
    const sr = getSocketCenter(wireStartSocket);
    if (wireStartIsOutput) {
      tempWire.setAttribute('d', bezierPath(sr.x, sr.y, mx, my));
    } else {
      tempWire.setAttribute('d', bezierPath(mx, my, sr.x, sr.y));
    }
  }
});

window.addEventListener('mouseup', (e) => {
  if (isPanning) isPanning = false;
  if (isDraggingNode) { isDraggingNode = false; draggedNode = null; }
  if (isResizing) { isResizing = false; resizeNode = null; }
  if (isWiring) {
    if (tempWire) { tempWire.remove(); tempWire = null; }
    isWiring = false;
    wireStartSocket = null;
  }
});

editor.addEventListener('contextmenu', (e) => e.preventDefault());

// ===== Node Dragging (via headers) =====
document.addEventListener('mousedown', (e) => {
  const header = e.target.closest('.node-header');
  if (!header || e.button !== 0) return;
  const node = header.closest('.node');
  isDraggingNode = true;
  draggedNode = node;
  const canvasRect = canvas.getBoundingClientRect();
  const nodeLeft = parseFloat(node.style.left || 0);
  const nodeTop = parseFloat(node.style.top || 0);
  nodeOffsetX = (e.clientX - canvasRect.left) / scale - nodeLeft;
  nodeOffsetY = (e.clientY - canvasRect.top) / scale - nodeTop;
  node.style.zIndex = 20;
  document.querySelectorAll('.node').forEach(n => { if (n !== node) n.style.zIndex = 10; });
  e.preventDefault();
});

// ===== Node Resizing =====
document.addEventListener('mousedown', (e) => {
  const handle = e.target.closest('.node-resize');
  if (!handle || e.button !== 0) return;
  const node = handle.closest('.node');
  isResizing = true;
  resizeNode = node;
  resizeStartX = e.clientX;
  resizeStartY = e.clientY;
  resizeStartW = node.offsetWidth;
  resizeStartH = node.offsetHeight;
  e.preventDefault();
  e.stopPropagation();
});

// ===== Socket Wiring =====
document.addEventListener('mousedown', (e) => {
  const socket = e.target.closest('.socket');
  if (!socket || e.button !== 0) return;

  if (socket.classList.contains('output')) {
    // Start wiring from output
    isWiring = true;
    wireStartSocket = socket;
    wireStartIsOutput = true;
    const sr = getSocketCenter(socket);
    tempWire = document.createElementNS('http://www.w3.org/2000/svg', 'path');
    tempWire.classList.add('wire', 'active');
    tempWire.style.stroke = getPortColor(socket.dataset.port);
    tempWire.setAttribute('d', bezierPath(sr.x, sr.y, sr.x, sr.y));
    svgLayer.appendChild(tempWire);
    e.preventDefault();
    e.stopPropagation();
  } else if (socket.classList.contains('input')) {
    const nodeId = socket.dataset.node;
    const port = socket.dataset.port;
    const existing = connections.find(c => c.toNode === nodeId && c.toPort === port);

    if (existing) {
      // Disconnect existing wire and start re-dragging from the output end
      connections = connections.filter(c => c !== existing);
      updateConnections();
      // Start wiring from the original output socket
      const fromSocket = document.querySelector(`#${existing.fromNode} .socket.output[data-port="${existing.fromPort}"]`);
      if (fromSocket) {
        isWiring = true;
        wireStartSocket = fromSocket;
        wireStartIsOutput = true;
        const sr = getSocketCenter(fromSocket);
        tempWire = document.createElementNS('http://www.w3.org/2000/svg', 'path');
        tempWire.classList.add('wire', 'active');
        tempWire.style.stroke = getPortColor(fromSocket.dataset.port);
        tempWire.setAttribute('d', bezierPath(sr.x, sr.y, sr.x, sr.y));
        svgLayer.appendChild(tempWire);
      }
    } else {
      // No existing connection, start wiring from input
      isWiring = true;
      wireStartSocket = socket;
      wireStartIsOutput = false;
      const sr = getSocketCenter(socket);
      tempWire = document.createElementNS('http://www.w3.org/2000/svg', 'path');
      tempWire.classList.add('wire', 'active');
      tempWire.style.stroke = getPortColor(socket.dataset.port);
      tempWire.setAttribute('d', bezierPath(sr.x, sr.y, sr.x, sr.y));
      svgLayer.appendChild(tempWire);
    }
    e.preventDefault();
    e.stopPropagation();
  }
});

document.addEventListener('mouseup', (e) => {
  if (!isWiring || !wireStartSocket) return;
  const socket = e.target.closest('.socket');
  if (socket && socket !== wireStartSocket) {
    let fromSocket, toSocket;
    if (wireStartIsOutput && socket.classList.contains('input')) {
      fromSocket = wireStartSocket;
      toSocket = socket;
    } else if (!wireStartIsOutput && socket.classList.contains('output')) {
      fromSocket = socket;
      toSocket = wireStartSocket;
    }
    if (fromSocket && toSocket) {
      const fromPort = fromSocket.dataset.port;
      const toPort = toSocket.dataset.port;
      // Validate port type compatibility
      if (fromPort === toPort) {
        const fromNode = fromSocket.dataset.node;
        const toNode = toSocket.dataset.node;
        // No self-connections
        if (fromNode !== toNode) {
          // Remove existing connection to this input (one input = one wire)
          connections = connections.filter(c => !(c.toNode === toNode && c.toPort === toPort));
          connections.push({ fromNode, fromPort, toNode, toPort });
          updateConnections();

          // Auto-populate config nodes when connected
          const toNodeInfo = nodeRegistry[toNode];
          if (toNodeInfo && toNodeInfo.type) {
            const typeDef = NODE_TYPES[toNodeInfo.type];
            if (typeDef && typeDef.execute) {
              // Execute the node to populate dropdowns/options
              // Check if we have the necessary data before executing
              const shouldExecute =
                (toPort === 'model' && hasModelContextForNode(toNode)) || // Has model data
                (toPort === 'reactions') || // Has reactions data
                (toPort === 'params'); // Params connection

              if (shouldExecute) {
                setTimeout(() => {
                  typeDef.execute(toNode).catch(e => {
                    console.error(`Failed to auto-populate ${toNode}:`, e);
                  });
                }, 100);
              }
            }
          }
        }
      } else {
        showToast(`Port mismatch: ${fromPort} ≠ ${toPort}`);
      }
    }
  }
});

// ===== Connection Drawing =====
function getSocketCenter(socket) {
  const rect = socket.getBoundingClientRect();
  const canvasRect = canvas.getBoundingClientRect();
  return {
    x: (rect.left + rect.width / 2 - canvasRect.left) / scale,
    y: (rect.top + rect.height / 2 - canvasRect.top) / scale,
  };
}

function bezierPath(x1, y1, x2, y2) {
  const dx = Math.abs(x2 - x1) * 0.5;
  return `M ${x1} ${y1} C ${x1 + dx} ${y1}, ${x2 - dx} ${y2}, ${x2} ${y2}`;
}

function updateConnections() {
  // Store transmitting state before removing wires
  const transmittingWires = new Set();
  svgLayer.querySelectorAll('.wire.connected.transmitting').forEach(w => {
    const id = w.getAttribute('id');
    if (id) transmittingWires.add(id);
  });

  svgLayer.querySelectorAll('.wire.connected').forEach(w => w.remove());
  // Reset all socket connected state
  document.querySelectorAll('.socket.connected').forEach(s => s.classList.remove('connected'));
  connections.forEach(conn => {
    const fromSocket = document.querySelector(`#${conn.fromNode} .socket.output[data-port="${conn.fromPort}"]`);
    const toSocket = document.querySelector(`#${conn.toNode} .socket.input[data-port="${conn.toPort}"]`);
    if (!fromSocket || !toSocket) return;
    fromSocket.classList.add('connected');
    toSocket.classList.add('connected');
    const from = getSocketCenter(fromSocket);
    const to = getSocketCenter(toSocket);
    const path = document.createElementNS('http://www.w3.org/2000/svg', 'path');
    const wireId = `wire-${conn.fromNode}-${conn.toNode}`;
    path.classList.add('wire', 'connected');
    path.setAttribute('id', wireId);
    path.setAttribute('d', bezierPath(from.x, from.y, to.x, to.y));
    path.setAttribute('data-port-type', conn.fromPort);
    path.style.stroke = getPortColor(conn.fromPort);

    // Restore transmitting state if it was active
    if (transmittingWires.has(wireId)) {
      path.classList.add('transmitting');
    }

    svgLayer.appendChild(path);
  });
}

// ===== Node Factory =====
function createNode(nodeType, x, y) {
  const typeDef = NODE_TYPES[nodeType];
  if (!typeDef) { console.error('Unknown node type:', nodeType); return null; }

  nodeIdCounter++;
  const nodeId = `node-${nodeIdCounter}`;

  const node = document.createElement('div');
  const isLargeNode = ['viewer', 'result', 'parameter'].includes(typeDef.category);
  node.className = `node${isLargeNode ? ' viewer' : ''}`;
  node.id = nodeId;
  node.dataset.type = typeDef.category;
  node.dataset.nodeType = nodeType;
  node.style.left = `${x}px`;
  node.style.top = `${y}px`;
  if (typeDef.defaultWidth) node.style.width = `${typeDef.defaultWidth}px`;
  if (typeDef.defaultHeight) node.style.height = `${typeDef.defaultHeight}px`;

  // Header
  const header = document.createElement('div');
  header.className = `node-header ${typeDef.headerClass}`;
  header.innerHTML = `
    <span>${typeDef.title}</span>
    <button class="btn-close" onclick="removeNode('${nodeId}')">&times;</button>
  `;
  node.appendChild(header);

  // Body
  const body = document.createElement('div');
  body.className = 'node-body';

  // Input sockets
  typeDef.inputs.forEach(inp => {
    body.innerHTML += `
      <div class="socket-row left">
        <div class="socket input" data-node="${nodeId}" data-port="${inp.port}"></div>
        <span class="socket-label">${inp.label}</span>
      </div>
    `;
  });

  // Custom body content
  if (typeDef.createBody) {
    body.innerHTML += typeDef.createBody(nodeId);
  }

  if (body.querySelector('.tab-nav')) {
    body.classList.add('node-body-tabbed');
  }

  // Output sockets
  typeDef.outputs.forEach(out => {
    body.innerHTML += `
      <div class="socket-row right">
        <span class="socket-label">${out.label}</span>
        <div class="socket output" data-node="${nodeId}" data-port="${out.port}"></div>
      </div>
    `;
  });

  node.appendChild(body);

  // Resize handle
  const resize = document.createElement('div');
  resize.className = 'node-resize';
  node.appendChild(resize);

  canvas.appendChild(node);

  nodeRegistry[nodeId] = { type: nodeType, el: node, data: {} };
  setupNodeResizeObserver(nodeId, node);

  // Run init hook
  if (typeDef.onInit) typeDef.onInit(nodeId);

  return nodeId;
}

function removeNode(nodeId) {
  const el = document.getElementById(nodeId);
  if (el) el.remove();
  connections = connections.filter(c => c.fromNode !== nodeId && c.toNode !== nodeId);
  delete nodeRegistry[nodeId];
  cleanupNodeResizeObserver(nodeId);
  cleanupPlotResize(nodeId);
  updateConnections();
}

// ===== Node Loading State =====
function setNodeLoading(nodeId, loading) {
  const el = document.getElementById(nodeId);
  if (!el) return;
  if (loading) {
    el.classList.add('loading');
    // Mark all input wires as transmitting
    const inputConns = connections.filter(c => c.toNode === nodeId);
    inputConns.forEach(conn => {
      const wireId = `wire-${conn.fromNode}-${conn.toNode}`;
      const wire = document.getElementById(wireId);
      if (wire) wire.classList.add('transmitting');
    });
  } else {
    el.classList.remove('loading');
    // Remove transmitting state from all input wires
    const inputConns = connections.filter(c => c.toNode === nodeId);
    inputConns.forEach(conn => {
      const wireId = `wire-${conn.fromNode}-${conn.toNode}`;
      const wire = document.getElementById(wireId);
      if (wire) wire.classList.remove('transmitting');
    });
  }
}

// ===== Auto-Chain Generation =====

// Find an existing chain ending with a model-builder that has a model output
function findExistingModelBuilder() {
  for (const [id, info] of Object.entries(nodeRegistry)) {
    if (info.type === 'model-builder') {
      // Check if this model-builder is connected to a reaction-network
      const conn = connections.find(c => c.toNode === id && c.toPort === 'reactions');
      if (conn && nodeRegistry[conn.fromNode]?.type === 'reaction-network') {
        return { modelBuilderId: id, reactionNetworkId: conn.fromNode };
      }
    }
  }
  return null;
}

function findExistingReactionNetwork() {
  for (const [id, info] of Object.entries(nodeRegistry)) {
    if (info.type === 'reaction-network') return id;
  }
  return null;
}

function getNodePosition(nodeId) {
  const el = document.getElementById(nodeId);
  if (!el) return { x: 100, y: 150 };
  return { x: parseFloat(el.style.left) || 0, y: parseFloat(el.style.top) || 0 };
}

function getNodeSize(nodeId) {
  const el = document.getElementById(nodeId);
  if (!el) return { w: 260, h: 200 };
  return { w: el.offsetWidth, h: el.offsetHeight };
}

// Count how many viewers are already attached to a model-builder
function countDownstreamViewers(modelBuilderId) {
  return connections.filter(c => c.fromNode === modelBuilderId && c.fromPort === 'model').length;
}

// Simple collision detection — shift node down if overlapping
function resolveOverlap(x, y, width, height, excludeNodeId) {
  let maxAttempts = 20;
  let curY = y;
  while (maxAttempts-- > 0) {
    let overlaps = false;
    for (const [id, info] of Object.entries(nodeRegistry)) {
      if (id === excludeNodeId) continue;
      const pos = getNodePosition(id);
      const size = getNodeSize(id);
      if (x < pos.x + size.w && x + width > pos.x &&
          curY < pos.y + size.h && curY + height > pos.y) {
        curY = pos.y + size.h + 30;
        overlaps = true;
        break;
      }
    }
    if (!overlaps) break;
  }
  return curY;
}

function addNodeFromMenu(nodeType) {
  closeDropdown();

  // Simple strategy: just create the node at a reasonable position
  const typeDef = NODE_TYPES[nodeType];
  if (!typeDef) return;

  // Find a good position based on existing nodes
  let x = 80;
  let y = 150;

  // If there are existing nodes, place new node to the right
  const existingNodes = Object.keys(nodeRegistry);
  if (existingNodes.length > 0) {
    let maxX = 0;
    for (const id of existingNodes) {
      const pos = getNodePosition(id);
      const size = getNodeSize(id);
      if (pos.x + size.w > maxX) {
        maxX = pos.x + size.w;
      }
    }
    x = maxX + 60;
  }

  const width = typeDef.defaultWidth || 280;
  y = resolveOverlap(x, y, width, 300, null);

  createNode(nodeType, x, y);
}

function addResultNode(nodeType) {
  // This function is no longer used - kept for compatibility
  // All nodes are now added via addNodeFromMenu
  addNodeFromMenu(nodeType);
}

// ===== Quick Add Chain Generation =====
function addQuickAddChain(chainType) {
  closeDropdown();

  if (chainType === 'atlas-workflow') {
    const specX = 80;
    const specY = resolveOverlap(specX, 150, 420, 620, null);
    const specId = createNode('atlas-spec', specX, specY);
    const builderX = specX + 480;
    const builderY = resolveOverlap(builderX, specY, 460, 480, null);
    const builderId = createNode('atlas-builder', builderX, builderY);
    const queryX = builderX + 520;
    const queryY = resolveOverlap(queryX, specY, 400, 560, null);
    const queryId = createNode('atlas-query-config', queryX, queryY);
    const resultX = queryX + 460;
    const resultY = resolveOverlap(resultX, queryY, 640, 540, null);
    const resultId = createNode('atlas-query-result', resultX, resultY);

    connections.push({ fromNode: specId, fromPort: 'atlas-spec', toNode: builderId, toPort: 'atlas-spec' });
    connections.push({ fromNode: builderId, fromPort: 'atlas', toNode: resultId, toPort: 'atlas' });
    connections.push({ fromNode: queryId, fromPort: 'atlas-query', toNode: resultId, toPort: 'atlas-query' });
    updateConnections();
    return;
  }

  // Map legacy node types to their new chain equivalents
  const chainMap = {
    'siso-analysis': { params: 'siso-params', result: 'siso-result' },
    'rop-cloud': { params: 'rop-cloud-params', result: 'rop-cloud-result' },
    'fret-heatmap': { params: 'fret-params', result: 'fret-result' },
    'parameter-scan-1d': { params: 'scan-1d-params', result: 'scan-1d-result' },
    'parameter-scan-2d': { params: 'scan-2d-params', result: 'scan-2d-result' },
    'rop-polyhedron': { params: 'rop-poly-params', result: 'rop-poly-result' },
  };

  const chain = chainMap[chainType];
  if (!chain) {
    console.error('Unknown quick add chain type:', chainType);
    return;
  }

  // Check for existing nodes and reuse them
  let rnId = findExistingReactionNetwork();
  let mbId = null;
  let createdModelBuilder = false;

  if (!rnId) {
    // No reaction network exists, create one
    rnId = createNode('reaction-network', 80, 150);
  }

  // Check for existing model-builder connected to this reaction network
  const existing = findExistingModelBuilder();
  if (existing && existing.reactionNetworkId === rnId) {
    mbId = existing.modelBuilderId;
  } else {
    // Create model-builder and connect to reaction network
    const rnPos = getNodePosition(rnId);
    const rnSize = getNodeSize(rnId);
    const mbX = rnPos.x + rnSize.w + 60;
    const mbY = resolveOverlap(mbX, rnPos.y, 260, 200, null);
    mbId = createNode('model-builder', mbX, mbY);
    createdModelBuilder = true;
    connections.push({ fromNode: rnId, fromPort: 'reactions', toNode: mbId, toPort: 'reactions' });
  }

  // Create params and result nodes
  const mbPos = getNodePosition(mbId);
  const mbSize = getNodeSize(mbId);
  const paramsX = mbPos.x + mbSize.w + 60;
  const nDownstream = countDownstreamViewers(mbId);
  const paramsY = resolveOverlap(paramsX, mbPos.y + nDownstream * 50, 320, 300, null);
  const paramsId = createNode(chain.params, paramsX, paramsY);

  const paramsSize = getNodeSize(paramsId);
  const resultX = paramsX + paramsSize.w + 60;
  const resultY = resolveOverlap(resultX, paramsY, 420, 300, null);
  const resultId = createNode(chain.result, resultX, resultY);

  // Connect them
  connections.push({ fromNode: mbId, fromPort: 'model', toNode: paramsId, toPort: 'model' });
  connections.push({ fromNode: paramsId, fromPort: 'params', toNode: resultId, toPort: 'params' });

  // Special case: ROP cloud params also needs reactions connection
  if (chain.params === 'rop-cloud-params') {
    connections.push({ fromNode: rnId, fromPort: 'reactions', toNode: paramsId, toPort: 'reactions' });
  }

  updateConnections();

  const modelBuilderInfo = nodeRegistry[mbId];
  if ((createdModelBuilder || !getModelContextFromBuilder(mbId)) && modelBuilderInfo?._autoBuildCheck) {
    setTimeout(() => {
      modelBuilderInfo._autoBuildCheck();
    }, 100);
  }

  // Auto-populate the params node if model data is available
  const paramsTypeDef = NODE_TYPES[chain.params];
  if (paramsTypeDef && paramsTypeDef.execute) {
    // Check if we have model data or reactions data
    const hasModelData = hasModelContextForNode(paramsId);
    const hasReactionsData = chain.params === 'rop-cloud-params'; // ROP cloud uses reactions

    if (hasModelData || hasReactionsData) {
      setTimeout(() => {
        paramsTypeDef.execute(paramsId).catch(e => {
          console.error(`Failed to auto-populate ${paramsId}:`, e);
        });
      }, 100);
    }
  }
}

// ===== Toolbar / Dropdown =====
const addNodeBtn = document.getElementById('add-node-btn');
const addNodeMenu = document.getElementById('add-node-menu');
const legacyNodesBtn = document.getElementById('legacy-nodes-btn');
const legacyNodesMenu = document.getElementById('legacy-nodes-menu');
const runConnectedBtn = document.getElementById('run-connected-btn');
const themeModeBtn = document.getElementById('theme-mode-btn');
const themeModeMenu = document.getElementById('theme-mode-menu');
const debugConsoleBtn = document.getElementById('debug-console-btn');
const debugConsolePanel = document.getElementById('debug-console');
const debugConsoleBody = document.getElementById('debug-console-body');
const debugConsoleCounter = document.getElementById('debug-console-counter');
const debugConsoleIndicator = document.getElementById('debug-console-indicator');
const debugConsoleCloseBtn = document.getElementById('debug-console-close');
const debugConsoleRefreshBtn = document.getElementById('debug-console-refresh');

addNodeBtn.addEventListener('click', (e) => {
  e.stopPropagation();
  addNodeMenu.classList.toggle('open');
  legacyNodesMenu.classList.remove('open');
  themeModeMenu?.classList.remove('open');
});

legacyNodesBtn.addEventListener('click', (e) => {
  e.stopPropagation();
  legacyNodesMenu.classList.toggle('open');
  addNodeMenu.classList.remove('open');
  themeModeMenu?.classList.remove('open');
});

themeModeBtn?.addEventListener('click', (e) => {
  e.stopPropagation();
  themeModeMenu?.classList.toggle('open');
  addNodeMenu.classList.remove('open');
  legacyNodesMenu.classList.remove('open');
});

runConnectedBtn?.addEventListener('click', (e) => {
  e.stopPropagation();
  void runConnectedWorkspace();
});

document.addEventListener('click', (e) => {
  if (!addNodeMenu.contains(e.target) && e.target !== addNodeBtn) {
    addNodeMenu.classList.remove('open');
  }
  if (!legacyNodesMenu.contains(e.target) && e.target !== legacyNodesBtn) {
    legacyNodesMenu.classList.remove('open');
  }
  if (themeModeMenu && !themeModeMenu.contains(e.target) && e.target !== themeModeBtn) {
    themeModeMenu.classList.remove('open');
  }
});

debugConsoleBtn?.addEventListener('click', (e) => {
  e.stopPropagation();
  toggleDebugConsole();
});

debugConsoleCloseBtn?.addEventListener('click', () => closeDebugConsole());
debugConsoleRefreshBtn?.addEventListener('click', () => refreshDebugConsole(true));

document.addEventListener('keydown', (e) => {
  if (e.key === 'Escape' && debugConsoleState.open) {
    closeDebugConsole();
  }
});

addNodeMenu.querySelectorAll('.menu-item').forEach(item => {
  item.addEventListener('click', () => {
    addNodeFromMenu(item.dataset.type);
  });
});

legacyNodesMenu.querySelectorAll('.menu-item').forEach(item => {
  item.addEventListener('click', () => {
    addQuickAddChain(item.dataset.type);
  });
});

themeModeMenu?.querySelectorAll('.menu-item').forEach(item => {
  item.addEventListener('click', () => {
    void applyThemeMode(item.dataset.themeMode || 'auto');
    themeModeMenu.classList.remove('open');
  });
});

function closeDropdown() {
  addNodeMenu.classList.remove('open');
  legacyNodesMenu.classList.remove('open');
  themeModeMenu?.classList.remove('open');
}

function normalizeThemeMode(mode) {
  return ['auto', 'light', 'dark'].includes(mode) ? mode : 'auto';
}

function resolveEffectiveTheme(mode = themeState.mode) {
  if (mode === 'light' || mode === 'dark') return mode;
  return colorSchemeMediaQuery?.matches ? 'light' : 'dark';
}

function getLightThemeStylesheetURL() {
  return 'style-node-light.css';
}

function ensureLightThemeStylesheet(enabled) {
  const existing = document.getElementById(LIGHT_THEME_STYLESHEET_ID);
  if (!enabled) {
    existing?.remove();
    return Promise.resolve();
  }

  if (existing) {
    return Promise.resolve();
  }

  return new Promise((resolve) => {
    const link = document.createElement('link');
    link.id = LIGHT_THEME_STYLESHEET_ID;
    link.rel = 'stylesheet';
    link.href = getLightThemeStylesheetURL();
    link.addEventListener('load', () => resolve(), { once: true });
    link.addEventListener('error', () => resolve(), { once: true });
    document.head.appendChild(link);
  });
}

function syncThemeMenuUI() {
  const label = themeState.mode === 'auto'
    ? `Appearance: System (${themeState.effective === 'light' ? 'Light' : 'Dark'})`
    : `Appearance: ${themeState.mode[0].toUpperCase()}${themeState.mode.slice(1)}`;
  if (themeModeBtn) {
    themeModeBtn.title = label;
    themeModeBtn.setAttribute('aria-label', label);
  }
  themeModeMenu?.querySelectorAll('.menu-item').forEach(item => {
    item.classList.toggle('is-selected', item.dataset.themeMode === themeState.mode);
  });
}

async function applyThemeMode(mode, options = {}) {
  const persist = options.persist !== false;
  const refreshPlots = options.refreshPlots !== false;
  const normalized = normalizeThemeMode(mode);
  const effective = resolveEffectiveTheme(normalized);

  themeState.mode = normalized;
  themeState.effective = effective;

  document.documentElement.dataset.themeMode = normalized;
  document.documentElement.dataset.effectiveTheme = effective;
  document.documentElement.style.colorScheme = effective;

  await ensureLightThemeStylesheet(effective === 'light');
  syncThemeMenuUI();

  if (persist) {
    try {
      window.localStorage.setItem(THEME_MODE_STORAGE_KEY, normalized);
    } catch (_) {}
  }

  if (refreshPlots) {
    window.requestAnimationFrame(() => refreshThemeAwarePlots());
  }
}

function storedThemeMode() {
  try {
    return normalizeThemeMode(window.localStorage.getItem(THEME_MODE_STORAGE_KEY));
  } catch (_) {
    return 'auto';
  }
}

function escapeHtml(text) {
  return String(text ?? '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

async function apiSilent(endpoint, data) {
  const resp = await fetch(`${API}/api/${endpoint}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(data || {}),
  });
  const contentType = resp.headers.get('content-type');
  if (!contentType || !contentType.includes('application/json')) {
    throw new Error('Backend server not responding');
  }
  const json = await resp.json();
  if (json.error) throw new Error(json.error);
  return json;
}

function debugConsoleShouldStickToBottom() {
  if (!debugConsoleBody) return true;
  const threshold = 32;
  return debugConsoleBody.scrollHeight - debugConsoleBody.scrollTop - debugConsoleBody.clientHeight <= threshold;
}

function renderDebugEntry(entry) {
  const level = String(entry.level || 'INFO').toUpperCase();
  const scope = [entry.module, entry.file ? `${entry.file}${entry.line ? `:${entry.line}` : ''}` : null]
    .filter(Boolean)
    .join(' · ');
  const lines = [];
  const header = [entry.timestamp || '', level, scope].filter(Boolean).join('  ');
  if (header) lines.push(header);
  if (entry.message) lines.push(String(entry.message));
  if (entry.details) lines.push(String(entry.details));
  return `<pre class="debug-terminal-line level-${level.toLowerCase()}" data-seq="${entry.seq}">${escapeHtml(lines.join('\n'))}</pre>`;
}

function updateDebugConsoleIndicator() {
  if (!debugConsoleIndicator) return;
  debugConsoleIndicator.style.display = debugConsoleState.unseenPriority ? '' : 'none';
}

function renderDebugConsole(fullRender = true, newEntries = []) {
  if (!debugConsoleBody) return;
  const shouldStick = debugConsoleShouldStickToBottom();
  if (!debugConsoleState.entries.length) {
    debugConsoleBody.innerHTML = '<div class="debug-console-empty">No backend logs captured yet.</div>';
  } else if (fullRender) {
    debugConsoleBody.innerHTML = debugConsoleState.entries.map(renderDebugEntry).join('');
  } else if (newEntries.length) {
    const emptyEl = debugConsoleBody.querySelector('.debug-console-empty');
    if (emptyEl) emptyEl.remove();
    debugConsoleBody.insertAdjacentHTML('beforeend', newEntries.map(renderDebugEntry).join(''));
  }

  if (debugConsoleCounter) {
    const count = debugConsoleState.entries.length;
    debugConsoleCounter.textContent = `${count} line${count === 1 ? '' : 's'}`;
  }

  if (shouldStick || fullRender) {
    debugConsoleBody.scrollTop = debugConsoleBody.scrollHeight;
  }
}

async function refreshDebugConsole(forceFull = false) {
  if (debugConsoleState.fetchInFlight) return;
  debugConsoleState.fetchInFlight = true;

  try {
    const data = await apiSilent('debug_logs', {
      after_seq: forceFull ? 0 : debugConsoleState.lastSeq,
      limit: forceFull ? 400 : 200,
    });

    const entries = Array.isArray(data.entries) ? data.entries : [];
    if (forceFull) {
      debugConsoleState.entries = entries.slice(-800);
      const lastEntry = debugConsoleState.entries.length ? debugConsoleState.entries[debugConsoleState.entries.length - 1] : null;
      debugConsoleState.lastSeq = data.next_seq || (lastEntry ? lastEntry.seq : 0);
      renderDebugConsole(true);
    } else if (entries.length) {
      debugConsoleState.entries.push(...entries);
      if (debugConsoleState.entries.length > 800) {
        debugConsoleState.entries = debugConsoleState.entries.slice(-800);
      }
      debugConsoleState.lastSeq = data.next_seq || debugConsoleState.lastSeq;
      renderDebugConsole(false, entries);
      if (!debugConsoleState.open) {
        debugConsoleState.unseenPriority ||= entries.some(entry => ['WARN', 'ERROR'].includes(String(entry.level || '').toUpperCase()));
        updateDebugConsoleIndicator();
      }
    }
  } catch (e) {
    if (forceFull && debugConsoleBody) {
      debugConsoleBody.innerHTML = `<div class="debug-console-empty">${escapeHtml(e.message)}</div>`;
    }
  } finally {
    debugConsoleState.fetchInFlight = false;
  }
}

function startDebugConsolePolling() {
  if (debugConsoleState.pollTimer) return;
  debugConsoleState.pollTimer = setInterval(() => {
    refreshDebugConsole(false);
  }, 1500);
}

function stopDebugConsolePolling() {
  if (!debugConsoleState.pollTimer) return;
  clearInterval(debugConsoleState.pollTimer);
  debugConsoleState.pollTimer = null;
}

function openDebugConsole() {
  debugConsoleState.open = true;
  debugConsoleState.unseenPriority = false;
  document.body.classList.add('debug-console-open');
  debugConsolePanel?.classList.add('open');
  debugConsolePanel?.setAttribute('aria-hidden', 'false');
  debugConsoleBtn?.classList.add('active');
  updateDebugConsoleIndicator();
  refreshDebugConsole(debugConsoleState.entries.length === 0 ? true : false);
  startDebugConsolePolling();
}

function closeDebugConsole() {
  debugConsoleState.open = false;
  document.body.classList.remove('debug-console-open');
  debugConsolePanel?.classList.remove('open');
  debugConsolePanel?.setAttribute('aria-hidden', 'true');
  debugConsoleBtn?.classList.remove('active');
  stopDebugConsolePolling();
}

function toggleDebugConsole() {
  if (debugConsoleState.open) closeDebugConsole();
  else openDebugConsole();
}

// ===== Toast Notifications =====
function showToast(message, duration = 2500) {
  const container = document.getElementById('toast-container');
  const toast = document.createElement('div');
  toast.className = 'toast';
  toast.textContent = message;
  container.appendChild(toast);
  requestAnimationFrame(() => toast.classList.add('show'));
  setTimeout(() => {
    toast.classList.remove('show');
    setTimeout(() => toast.remove(), 300);
  }, duration);
}

// ===== API Helpers =====
async function api(endpoint, data) {
  setStatus('working', 'Computing...');
  try {
    const resp = await fetch(`${API}/api/${endpoint}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(data),
    });

    // Check if response is JSON
    const contentType = resp.headers.get('content-type');
    if (!contentType || !contentType.includes('application/json')) {
      throw new Error('Backend server not responding. Please ensure Julia server is running.');
    }

    const json = await resp.json();
    if (json.error) throw new Error(json.error);
    setStatus('done', 'Done');
    return json;
  } catch (e) {
    setStatus('error', e.message);
    throw e;
  }
}

function setStatus(cls, text) {
  const badge = document.getElementById('status-badge');
  badge.className = `badge ${cls}`;
  badge.textContent = text;
  if (cls === 'done') setTimeout(() => {
    badge.className = 'badge';
    badge.textContent = 'Ready';
  }, 3000);
}

// ===== Reaction Editor =====
function getReactionsFromNode(nodeId) {
  const list = document.getElementById(`${nodeId}-reactions-list`);
  if (!list) return { reactions: [], kds: [] };
  const rows = list.querySelectorAll('.reaction-row');
  const reactions = [];
  const kds = [];
  rows.forEach(row => {
    const rule = row.querySelector('.reaction-input').value.trim();
    const kd = parseFloat(row.querySelector('.kd-input').value);
    if (rule) {
      reactions.push(rule);
      kds.push(Number.isFinite(kd) ? kd : null);
    }
  });
  return { reactions, kds };
}

function splitCommaList(value) {
  return String(value || '')
    .split(',')
    .map(item => item.trim())
    .filter(Boolean);
}

function parseOptionalInteger(value) {
  const text = String(value ?? '').trim();
  if (!text) return null;
  const parsed = parseInt(text, 10);
  return Number.isFinite(parsed) ? parsed : null;
}

function parseOptionalFloat(value) {
  const text = String(value ?? '').trim();
  if (!text) return null;
  const parsed = parseFloat(text);
  return Number.isFinite(parsed) ? parsed : null;
}

function parseOptionalJson(value, fallback, label) {
  const text = String(value ?? '').trim();
  if (!text) return fallback;
  try {
    return JSON.parse(text);
  } catch (error) {
    throw new Error(`${label} must be valid JSON.`);
  }
}

function normalizePredicateArray(value, label) {
  if (Array.isArray(value)) return value;
  if (value && typeof value === 'object') return [value];
  throw new Error(`${label} must be a JSON object or array.`);
}

function normalizePredicateSequenceArray(value, label) {
  if (!Array.isArray(value)) {
    throw new Error(`${label} must be a JSON array.`);
  }
  if (!value.length) return [];
  if (Array.isArray(value[0])) return value;
  if (value[0] && typeof value[0] === 'object') return [value];
  throw new Error(`${label} must be an array of predicate arrays.`);
}

function parseAtlasExplicitNetworks(text) {
  const trimmed = String(text || '').trim();
  if (!trimmed) return [];

  const parsed = JSON.parse(trimmed);
  if (Array.isArray(parsed)) return parsed;
  if (parsed && Array.isArray(parsed.networks)) return parsed.networks;
  throw new Error('Explicit networks must be a JSON array or an object with a `networks` array.');
}

const ATLAS_ROLE_OPTIONS = ['', 'source', 'sink', 'interior', 'branch', 'merge'];
const ATLAS_ORDER_OPTIONS = ['', '-1', '0', '+1', '+2', '-Inf', '+Inf'];
const ATLAS_SINGULAR_OPTIONS = ['', 'regular', 'singular'];

function atlasOptionHtml(value, selectedValue, label = value || 'any') {
  const selected = String(value) === String(selectedValue ?? '') ? ' selected' : '';
  return `<option value="${escapeHtml(value)}"${selected}>${escapeHtml(label || 'any')}</option>`;
}

function atlasRoleSelectHtml(selectedValue = '') {
  return `
    <select class="atlas-builder-input atlas-role">
      ${ATLAS_ROLE_OPTIONS.map(value => atlasOptionHtml(value, selectedValue, value || 'any role')).join('')}
    </select>
  `;
}

function atlasOrderSelectHtml(selectedValue = '') {
  return `
    <select class="atlas-builder-input atlas-order">
      ${ATLAS_ORDER_OPTIONS.map(value => atlasOptionHtml(value, selectedValue, value || 'any order')).join('')}
    </select>
  `;
}

function atlasSingularSelectHtml(selectedValue = '') {
  return `
    <select class="atlas-builder-input atlas-singular">
      ${ATLAS_SINGULAR_OPTIONS.map(value => atlasOptionHtml(value, selectedValue, value || 'any singularity')).join('')}
    </select>
  `;
}

function atlasBuilderRowHtml(kind, value = {}) {
  if (kind === 'transition') {
    return `
      <div class="atlas-builder-row" data-builder-kind="transition">
        <div class="atlas-builder-pair">
          ${atlasRoleSelectHtml(value.from?.role || '')}
          ${atlasOrderSelectHtml(value.from?.output_order_token || '')}
        </div>
        <span class="atlas-builder-arrow">→</span>
        <div class="atlas-builder-pair">
          ${atlasRoleSelectHtml(value.to?.role || '')}
          ${atlasOrderSelectHtml(value.to?.output_order_token || '')}
        </div>
        <button type="button" class="btn btn-small atlas-builder-remove" title="Remove condition">×</button>
      </div>
    `;
  }

  return `
    <div class="atlas-builder-row" data-builder-kind="${escapeHtml(kind)}">
      ${atlasRoleSelectHtml(value.role || '')}
      ${atlasOrderSelectHtml(value.output_order_token || '')}
      ${atlasSingularSelectHtml(
        value.singular === true ? 'singular' :
        value.singular === false ? 'regular' : ''
      )}
      <button type="button" class="btn btn-small atlas-builder-remove" title="Remove condition">×</button>
    </div>
  `;
}

function bindAtlasBuilderRowEvents(nodeId, row) {
  row.querySelectorAll('input, select').forEach(input => {
    const eventType = input.tagName === 'SELECT' ? 'change' : 'input';
    input.addEventListener(eventType, () => triggerConfigUpdate(nodeId, 'atlas-query-config'));
  });
  row.querySelector('.atlas-builder-remove')?.addEventListener('click', () => {
    row.remove();
    triggerConfigUpdate(nodeId, 'atlas-query-config');
  });
}

function addAtlasBuilderRow(nodeId, containerKey, kind, value = {}) {
  const container = document.getElementById(`${nodeId}-${containerKey}`);
  if (!container) return;
  const wrapper = document.createElement('div');
  wrapper.innerHTML = atlasBuilderRowHtml(kind, value);
  const row = wrapper.firstElementChild;
  if (!row) return;
  bindAtlasBuilderRowEvents(nodeId, row);
  container.appendChild(row);
  triggerConfigUpdate(nodeId, 'atlas-query-config');
}

function clearAtlasBuilderRows(nodeId, containerKey) {
  const container = document.getElementById(`${nodeId}-${containerKey}`);
  if (container) container.innerHTML = '';
}

function collectAtlasRegimeRows(nodeId, containerKey) {
  const container = document.getElementById(`${nodeId}-${containerKey}`);
  if (!container) return [];
  const rows = [];
  container.querySelectorAll('.atlas-builder-row').forEach(row => {
    const role = row.querySelector('.atlas-role')?.value || '';
    const outputOrder = row.querySelector('.atlas-order')?.value || '';
    const singular = row.querySelector('.atlas-singular')?.value || '';
    const predicate = {};
    if (role) predicate.role = role;
    if (outputOrder) predicate.output_order_token = outputOrder;
    if (singular === 'singular') predicate.singular = true;
    if (singular === 'regular') predicate.singular = false;
    if (Object.keys(predicate).length) rows.push(predicate);
  });
  return rows;
}

function collectAtlasTransitionRows(nodeId, containerKey) {
  const container = document.getElementById(`${nodeId}-${containerKey}`);
  if (!container) return [];
  const rows = [];
  container.querySelectorAll('.atlas-builder-row').forEach(row => {
    const fromRole = row.querySelectorAll('.atlas-role')[0]?.value || '';
    const fromOrder = row.querySelectorAll('.atlas-order')[0]?.value || '';
    const toRole = row.querySelectorAll('.atlas-role')[1]?.value || '';
    const toOrder = row.querySelectorAll('.atlas-order')[1]?.value || '';
    const from = {};
    const to = {};
    if (fromRole) from.role = fromRole;
    if (fromOrder) from.output_order_token = fromOrder;
    if (toRole) to.role = toRole;
    if (toOrder) to.output_order_token = toOrder;
    if (!Object.keys(from).length || !Object.keys(to).length) return;
    const predicate = { from, to };
    if (from.output_order_token && to.output_order_token) {
      predicate.transition_token = `${from.output_order_token}->${to.output_order_token}`;
    }
    rows.push(predicate);
  });
  return rows;
}

function readAtlasQueryBuilderState(nodeId) {
  return {
    builderRequiredRegimes: collectAtlasRegimeRows(nodeId, 'builder-required-regimes'),
    builderForbiddenRegimes: collectAtlasRegimeRows(nodeId, 'builder-forbidden-regimes'),
    builderRequiredTransitions: collectAtlasTransitionRows(nodeId, 'builder-required-transitions'),
    builderWitnessSequence: collectAtlasRegimeRows(nodeId, 'builder-witness-sequence'),
  };
}

function restoreAtlasQueryBuilderState(nodeId, data = {}) {
  clearAtlasBuilderRows(nodeId, 'builder-required-regimes');
  clearAtlasBuilderRows(nodeId, 'builder-forbidden-regimes');
  clearAtlasBuilderRows(nodeId, 'builder-required-transitions');
  clearAtlasBuilderRows(nodeId, 'builder-witness-sequence');
  (data.builderRequiredRegimes || []).forEach(item => addAtlasBuilderRow(nodeId, 'builder-required-regimes', 'regime', item));
  (data.builderForbiddenRegimes || []).forEach(item => addAtlasBuilderRow(nodeId, 'builder-forbidden-regimes', 'regime', item));
  (data.builderRequiredTransitions || []).forEach(item => addAtlasBuilderRow(nodeId, 'builder-required-transitions', 'transition', item));
  (data.builderWitnessSequence || []).forEach(item => addAtlasBuilderRow(nodeId, 'builder-witness-sequence', 'regime', item));
}

function atlasSketchSeriesFromMotif(label) {
  const key = String(label || '').toLowerCase();
  if (!key) return null;
  const presets = {
    activation_with_saturation: [0.12, 0.2, 0.52, 0.82, 0.88],
    thresholded_activation: [0.12, 0.12, 0.18, 0.74, 0.92],
    monotone_activation: [0.12, 0.28, 0.46, 0.68, 0.9],
    thresholded_repression: [0.9, 0.88, 0.8, 0.34, 0.1],
    repression_with_floor: [0.92, 0.72, 0.4, 0.18, 0.12],
    monotone_repression: [0.92, 0.74, 0.52, 0.28, 0.1],
    biphasic_peak: [0.12, 0.32, 0.9, 0.46, 0.16],
    biphasic_valley: [0.88, 0.62, 0.12, 0.54, 0.84],
    flat: [0.5, 0.5, 0.5, 0.5, 0.5],
  };
  return presets[key] || null;
}

function atlasSketchSeriesFromWitness(sequence) {
  if (!Array.isArray(sequence) || !sequence.length) return null;
  const mapToken = (token) => {
    const text = String(token || '').trim();
    if (!text) return 0.5;
    if (text === '+Inf') return 0.92;
    if (text === '-Inf') return 0.08;
    const num = Number(text);
    if (Number.isFinite(num)) {
      const clamped = Math.max(-2, Math.min(2, num));
      return 0.5 - clamped * 0.18;
    }
    return 0.5;
  };
  return sequence.map(item => mapToken(item.output_order_token || item));
}

function renderAtlasBehaviorSketch(payload) {
  const query = payload?.query || {};
  const goal = query.goal || {};
  const motifLabel = Array.isArray(goal.motif) ? goal.motif[0] : (Array.isArray(query.motif_labels) ? query.motif_labels[0] : goal.motif);
  let series = atlasSketchSeriesFromMotif(motifLabel);
  let caption = motifLabel ? `motif: ${motifLabel}` : 'behavior sketch';

  if (!series) {
    const witnessSeq = Array.isArray(query.required_path_sequences) && query.required_path_sequences.length
      ? query.required_path_sequences[0]
      : [];
    series = atlasSketchSeriesFromWitness(witnessSeq);
    if (series) caption = 'witness path sketch';
  }

  if (!series || !series.length) {
    return `
      <div class="atlas-sketch-empty">
        <span class="text-dim">Add a goal motif or witness stages to preview a qualitative behavior sketch.</span>
      </div>
    `;
  }

  const width = 260;
  const height = 108;
  const left = 18;
  const top = 12;
  const plotWidth = width - left * 2;
  const plotHeight = 64;
  const points = series.map((value, idx) => {
    const x = left + (plotWidth * idx) / Math.max(series.length - 1, 1);
    const y = top + (1 - value) * plotHeight;
    return [x, y];
  });
  const pathD = points.map(([x, y], idx) => `${idx === 0 ? 'M' : 'L'}${x.toFixed(1)},${y.toFixed(1)}`).join(' ');
  const pointDots = points.map(([x, y]) => `<circle cx="${x.toFixed(1)}" cy="${y.toFixed(1)}" r="3.2"></circle>`).join('');

  return `
    <div class="atlas-sketch-card">
      <svg class="atlas-sketch-svg" viewBox="0 0 ${width} ${height}" preserveAspectRatio="none">
        <line x1="${left}" y1="${top + plotHeight}" x2="${width - left}" y2="${top + plotHeight}" class="atlas-sketch-axis"></line>
        <line x1="${left}" y1="${top}" x2="${left}" y2="${top + plotHeight}" class="atlas-sketch-axis"></line>
        <path d="${pathD}" class="atlas-sketch-line"></path>
        ${pointDots}
      </svg>
      <div class="atlas-sketch-caption">${escapeHtml(caption)}</div>
    </div>
  `;
}

function refreshAtlasQueryDesigner(nodeId) {
  const previewEl = document.getElementById(`${nodeId}-query-preview`);
  const sketchEl = document.getElementById(`${nodeId}-behavior-sketch`);
  if (!previewEl && !sketchEl) return;
  try {
    const payload = atlasQueryPayloadFromState(readAtlasQueryEditorState(nodeId));
    if (previewEl) previewEl.textContent = JSON.stringify(payload.query, null, 2);
    if (sketchEl) sketchEl.innerHTML = renderAtlasBehaviorSketch(payload);
  } catch (error) {
    if (previewEl) previewEl.textContent = error.message;
    if (sketchEl) sketchEl.innerHTML = '<div class="atlas-sketch-empty"><span class="text-dim">Fix the query fields to preview the behavior sketch.</span></div>';
  }
}

function readAtlasSpecEditorState(nodeId) {
  return {
    sourceLabel: document.getElementById(`${nodeId}-source-label`)?.value || '',
    libraryLabel: document.getElementById(`${nodeId}-library-label`)?.value || '',
    sqlitePath: document.getElementById(`${nodeId}-sqlite-path`)?.value || '',
    persistSqlite: document.getElementById(`${nodeId}-persist-sqlite`)?.checked ?? false,
    skipExisting: document.getElementById(`${nodeId}-skip-existing`)?.checked ?? true,
    profileName: document.getElementById(`${nodeId}-profile-name`)?.value || 'binding_small_v0',
    maxBaseSpecies: parseInt(document.getElementById(`${nodeId}-max-base-species`)?.value || '4', 10),
    maxReactions: parseInt(document.getElementById(`${nodeId}-max-reactions`)?.value || '5', 10),
    maxSupport: parseInt(document.getElementById(`${nodeId}-max-support`)?.value || '3', 10),
    pathScope: document.getElementById(`${nodeId}-path-scope`)?.value || 'robust',
    minVolumeMean: parseFloat(document.getElementById(`${nodeId}-min-volume`)?.value || '0.01'),
    keepSingular: document.getElementById(`${nodeId}-keep-singular`)?.checked ?? true,
    keepNonasymptotic: document.getElementById(`${nodeId}-keep-nonasym`)?.checked ?? false,
    includePathRecords: document.getElementById(`${nodeId}-include-path-records`)?.checked ?? true,
    enableEnumeration: document.getElementById(`${nodeId}-enable-enumeration`)?.checked ?? true,
    enumerationMode: document.getElementById(`${nodeId}-enum-mode`)?.value || 'pairwise_binding',
    baseSpeciesCountsText: document.getElementById(`${nodeId}-base-species-counts`)?.value || '2,3',
    minEnumerationReactions: parseInt(document.getElementById(`${nodeId}-min-enum-reactions`)?.value || '1', 10),
    maxEnumerationReactions: parseInt(document.getElementById(`${nodeId}-max-enum-reactions`)?.value || '2', 10),
    enumerationLimit: parseInt(document.getElementById(`${nodeId}-enum-limit`)?.value || '0', 10),
    explicitNetworksText: document.getElementById(`${nodeId}-explicit-networks`)?.value || '',
  };
}

function atlasSpecPayloadFromState(rawState) {
  const state = { ...rawState };
  const explicitNetworks = parseAtlasExplicitNetworks(state.explicitNetworksText);
  const baseSpeciesCounts = splitCommaList(state.baseSpeciesCountsText)
    .map(item => parseInt(item, 10))
    .filter(Number.isFinite);

  const spec = {
    search_profile: {
      name: state.profileName || 'binding_small_v0',
      max_base_species: state.maxBaseSpecies,
      max_reactions: state.maxReactions,
      max_support: state.maxSupport,
    },
    behavior_config: {
      path_scope: state.pathScope,
      min_volume_mean: state.minVolumeMean,
      keep_singular: state.keepSingular,
      keep_nonasymptotic: state.keepNonasymptotic,
      include_path_records: state.includePathRecords,
    },
  };

  const sourceLabel = String(state.sourceLabel || '').trim();
  const libraryLabel = String(state.libraryLabel || '').trim();
  const sqlitePath = String(state.sqlitePath || '').trim();
  if (sourceLabel) spec.source_label = sourceLabel;
  if (libraryLabel) spec.library_label = libraryLabel;
  if (sqlitePath) spec.sqlite_path = sqlitePath;
  spec.skip_existing = !!state.skipExisting;
  if (sqlitePath) spec.persist_sqlite = !!state.persistSqlite;

  if (explicitNetworks.length) {
    spec.networks = explicitNetworks;
  }

  if (state.enableEnumeration) {
    spec.enumeration = {
      mode: state.enumerationMode || 'pairwise_binding',
      base_species_counts: baseSpeciesCounts.length ? baseSpeciesCounts : [2, 3],
      min_reactions: Math.min(state.minEnumerationReactions, state.maxEnumerationReactions),
      max_reactions: Math.max(state.minEnumerationReactions, state.maxEnumerationReactions),
      limit: Math.max(0, state.enumerationLimit || 0),
    };
  }

  if (!spec.networks && !spec.enumeration) {
    throw new Error('Atlas spec must include explicit networks or enable enumeration.');
  }

  return { serial: state, spec };
}

function getConnectedAtlasSpec(nodeId) {
  const conn = connections.find(c => c.toNode === nodeId && c.toPort === 'atlas-spec');
  if (!conn) return null;
  const sourceNodeId = conn.fromNode;
  return atlasSpecPayloadFromState(getNodeSerialData(sourceNodeId, 'atlas-spec'));
}

function readAtlasQueryEditorState(nodeId) {
  return {
    sqlitePath: document.getElementById(`${nodeId}-query-sqlite-path`)?.value || '',
    preferPersistedAtlas: document.getElementById(`${nodeId}-prefer-persisted-atlas`)?.checked ?? true,
    goalIoText: document.getElementById(`${nodeId}-goal-io`)?.value || '',
    goalMotifText: document.getElementById(`${nodeId}-goal-motif`)?.value || '',
    goalExactText: document.getElementById(`${nodeId}-goal-exact`)?.value || '',
    goalWitnessText: document.getElementById(`${nodeId}-goal-witness`)?.value || '',
    goalTransitionsText: document.getElementById(`${nodeId}-goal-transitions`)?.value || '',
    goalForbidRegimesText: document.getElementById(`${nodeId}-goal-forbid-regimes`)?.value || '',
    goalRobust: document.getElementById(`${nodeId}-goal-robust`)?.checked ?? false,
    goalFeasible: document.getElementById(`${nodeId}-goal-feasible`)?.checked ?? false,
    goalMinVolumeMean: parseOptionalFloat(document.getElementById(`${nodeId}-goal-min-volume`)?.value),
    motifLabelsText: document.getElementById(`${nodeId}-motif-labels`)?.value || '',
    motifMatchMode: document.getElementById(`${nodeId}-motif-match-mode`)?.value || 'any',
    exactLabelsText: document.getElementById(`${nodeId}-exact-labels`)?.value || '',
    exactMatchMode: document.getElementById(`${nodeId}-exact-match-mode`)?.value || 'any',
    inputSymbolsText: document.getElementById(`${nodeId}-input-symbols`)?.value || '',
    outputSymbolsText: document.getElementById(`${nodeId}-output-symbols`)?.value || '',
    requireRobust: document.getElementById(`${nodeId}-require-robust`)?.checked ?? false,
    minRobustPathCount: parseInt(document.getElementById(`${nodeId}-min-robust-path-count`)?.value || '0', 10),
    maxBaseSpecies: parseOptionalInteger(document.getElementById(`${nodeId}-query-max-base-species`)?.value),
    maxReactions: parseOptionalInteger(document.getElementById(`${nodeId}-query-max-reactions`)?.value),
    maxSupport: parseOptionalInteger(document.getElementById(`${nodeId}-query-max-support`)?.value),
    maxSupportMass: parseOptionalInteger(document.getElementById(`${nodeId}-query-max-support-mass`)?.value),
    requiredRegimesText: document.getElementById(`${nodeId}-required-regimes`)?.value || '',
    forbiddenRegimesText: document.getElementById(`${nodeId}-forbidden-regimes`)?.value || '',
    requiredTransitionsText: document.getElementById(`${nodeId}-required-transitions`)?.value || '',
    forbiddenTransitionsText: document.getElementById(`${nodeId}-forbidden-transitions`)?.value || '',
    requiredPathSequencesText: document.getElementById(`${nodeId}-required-path-sequences`)?.value || '',
    forbidSingularOnWitness: document.getElementById(`${nodeId}-forbid-singular-on-witness`)?.checked ?? false,
    maxWitnessPathLength: parseOptionalInteger(document.getElementById(`${nodeId}-max-witness-path-length`)?.value),
    requireWitnessFeasible: document.getElementById(`${nodeId}-require-witness-feasible`)?.checked ?? false,
    requireWitnessRobust: document.getElementById(`${nodeId}-require-witness-robust`)?.checked ?? false,
    minWitnessVolumeMean: parseOptionalFloat(document.getElementById(`${nodeId}-min-witness-volume-mean`)?.value),
    rankingMode: document.getElementById(`${nodeId}-ranking-mode`)?.value || 'minimal_first',
    collapseByNetwork: document.getElementById(`${nodeId}-collapse-by-network`)?.checked ?? true,
    paretoOnly: document.getElementById(`${nodeId}-pareto-only`)?.checked ?? false,
    limit: parseInt(document.getElementById(`${nodeId}-query-limit`)?.value || '20', 10),
    ...readAtlasQueryBuilderState(nodeId),
  };
}

function atlasQueryPayloadFromState(rawState) {
  const state = { ...rawState };
  const dedupeObjects = (items) => {
    const seen = new Set();
    return (items || []).filter(item => {
      const key = JSON.stringify(item);
      if (seen.has(key)) return false;
      seen.add(key);
      return true;
    });
  };
  const requiredRegimes = dedupeObjects([
    ...normalizePredicateArray(parseOptionalJson(state.requiredRegimesText, [], 'Required regimes'), 'Required regimes'),
    ...(Array.isArray(state.builderRequiredRegimes) ? state.builderRequiredRegimes : []),
  ]);
  const forbiddenRegimes = dedupeObjects([
    ...normalizePredicateArray(parseOptionalJson(state.forbiddenRegimesText, [], 'Forbidden regimes'), 'Forbidden regimes'),
    ...(Array.isArray(state.builderForbiddenRegimes) ? state.builderForbiddenRegimes : []),
  ]);
  const requiredTransitions = dedupeObjects([
    ...normalizePredicateArray(parseOptionalJson(state.requiredTransitionsText, [], 'Required transitions'), 'Required transitions'),
    ...(Array.isArray(state.builderRequiredTransitions) ? state.builderRequiredTransitions : []),
  ]);
  const forbiddenTransitions = normalizePredicateArray(parseOptionalJson(state.forbiddenTransitionsText, [], 'Forbidden transitions'), 'Forbidden transitions');
  const requiredPathSequences = normalizePredicateSequenceArray(parseOptionalJson(state.requiredPathSequencesText, [], 'Required path sequences'), 'Required path sequences');
  if (Array.isArray(state.builderWitnessSequence) && state.builderWitnessSequence.length) {
    requiredPathSequences.push(state.builderWitnessSequence);
  }

  const query = {
    motif_labels: splitCommaList(state.motifLabelsText),
    motif_match_mode: state.motifMatchMode || 'any',
    exact_labels: splitCommaList(state.exactLabelsText),
    exact_match_mode: state.exactMatchMode || 'any',
    input_symbols: splitCommaList(state.inputSymbolsText),
    output_symbols: splitCommaList(state.outputSymbolsText),
    require_robust: !!state.requireRobust,
    min_robust_path_count: Math.max(0, state.minRobustPathCount || 0),
    ranking_mode: state.rankingMode || 'minimal_first',
    collapse_by_network: !!state.collapseByNetwork,
    pareto_only: !!state.paretoOnly,
    limit: Math.max(1, state.limit || 20),
    required_regimes: requiredRegimes,
    forbidden_regimes: forbiddenRegimes,
    required_transitions: requiredTransitions,
    forbidden_transitions: forbiddenTransitions,
    required_path_sequences: requiredPathSequences,
    forbid_singular_on_witness: !!state.forbidSingularOnWitness,
    require_witness_feasible: !!state.requireWitnessFeasible,
    require_witness_robust: !!state.requireWitnessRobust,
  };

  if (state.maxBaseSpecies != null) query.max_base_species = state.maxBaseSpecies;
  if (state.maxReactions != null) query.max_reactions = state.maxReactions;
  if (state.maxSupport != null) query.max_support = state.maxSupport;
  if (state.maxSupportMass != null) query.max_support_mass = state.maxSupportMass;
  if (state.maxWitnessPathLength != null) query.max_witness_path_length = state.maxWitnessPathLength;
  if (state.minWitnessVolumeMean != null) query.min_witness_volume_mean = state.minWitnessVolumeMean;

  query.graph_spec = {
    required_regimes: requiredRegimes,
    forbidden_regimes: forbiddenRegimes,
    required_transitions: requiredTransitions,
    forbidden_transitions: forbiddenTransitions,
  };
  query.path_spec = {
    required_path_sequences: requiredPathSequences,
    forbid_singular_on_witness: !!state.forbidSingularOnWitness,
  };
  query.polytope_spec = {
    require_feasible: !!state.requireWitnessFeasible,
    require_robust: !!state.requireWitnessRobust,
  };
  if (state.maxWitnessPathLength != null) query.path_spec.max_path_length = state.maxWitnessPathLength;
  if (state.minWitnessVolumeMean != null) query.polytope_spec.min_volume_mean = state.minWitnessVolumeMean;

  const goal = {};
  const goalIo = String(state.goalIoText || '').trim();
  const goalMotif = String(state.goalMotifText || '').trim();
  const goalExact = String(state.goalExactText || '').trim();
  const goalWitness = String(state.goalWitnessText || '').trim();
  const goalTransitions = splitCommaList(state.goalTransitionsText);
  const goalForbidRegimes = splitCommaList(state.goalForbidRegimesText);
  if (goalIo) goal.io = goalIo;
  if (goalMotif) goal.motif = splitCommaList(goalMotif);
  if (goalExact) goal.exact = splitCommaList(goalExact);
  if (goalWitness) goal.witness = goalWitness;
  if (goalTransitions.length) goal.must_transitions = goalTransitions;
  if (goalForbidRegimes.length) goal.forbid_regimes = goalForbidRegimes;
  if (state.goalRobust) goal.robust = true;
  if (state.goalFeasible) goal.feasible = true;
  if (state.goalMinVolumeMean != null) goal.min_volume = state.goalMinVolumeMean;
  if (Object.keys(goal).length) query.goal = goal;

  return {
    serial: state,
    query,
    sqlitePath: String(state.sqlitePath || '').trim(),
    preferPersistedAtlas: !!state.preferPersistedAtlas,
  };
}

function getConnectedAtlasQuery(nodeId) {
  const conn = connections.find(c => c.toNode === nodeId && c.toPort === 'atlas-query');
  if (!conn) return null;
  const sourceNodeId = conn.fromNode;
  return atlasQueryPayloadFromState(getNodeSerialData(sourceNodeId, 'atlas-query-config'));
}

function getConnectedAtlasData(nodeId) {
  const conn = connections.find(c => c.toNode === nodeId && c.toPort === 'atlas');
  if (!conn) return null;
  return nodeRegistry[conn.fromNode]?.data?.atlasData || null;
}

function formatAtlasStatusTag(status) {
  const label = String(status || 'unknown');
  let cls = 'tag-atlas-neutral';
  if (label === 'ok') cls = 'tag-atlas-ok';
  else if (label === 'failed') cls = 'tag-atlas-failed';
  else if (label === 'excluded_by_search_profile') cls = 'tag-atlas-excluded';
  return `<span class="tag ${cls}">${escapeHtml(label)}</span>`;
}

function renderAtlasLabelRefs(labels) {
  const unique = Array.from(new Set((labels || []).filter(Boolean)));
  if (!unique.length) return '<span class="text-dim">none</span>';
  return unique.map(label => `<span class="family-ref">${escapeHtml(label)}</span>`).join('');
}

function renderTokenRefs(tokens) {
  const items = Array.isArray(tokens) ? tokens.filter(Boolean) : [];
  if (!items.length) return '<span class="text-dim">none</span>';
  return items.map(token => `<span class="family-ref">${escapeHtml(token)}</span>`).join('');
}

function renderWitnessPathSummary(path) {
  if (!path) return '<span class="text-dim">none</span>';
  const orderTokens = renderTokenRefs(path.output_order_tokens || []);
  const transitionTokens = renderTokenRefs(path.transition_tokens || []);
  const volumeMean = path.volume?.mean;
  return `
    <div class="family-meta">
      <span class="family-metric">path ${path.path_idx ?? '-'}</span>
      <span class="family-metric">${path.robust ? 'robust' : 'non-robust'}</span>
      <span class="family-metric">feasible ${path.feasible ? 'yes' : 'no'}</span>
      <span class="family-metric">vol ${Number.isFinite(volumeMean) ? Number(volumeMean).toFixed(3) : 'n/a'}</span>
    </div>
    <div>
      <div class="family-kicker">Regime Tokens</div>
      <div class="siso-wrap-cell">${orderTokens}</div>
    </div>
    <div>
      <div class="family-kicker">Transition Tokens</div>
      <div class="siso-wrap-cell">${transitionTokens}</div>
    </div>
  `;
}

function renderAtlasRules(rules) {
  const list = Array.isArray(rules) ? rules : [];
  if (!list.length) return '<span class="text-dim">No reactions recorded.</span>';
  return `<div class="atlas-rule-list">${list.map(rule => `<code>${escapeHtml(rule)}</code>`).join('')}</div>`;
}

function renderAtlasBuilderResult(data) {
  const entries = Array.isArray(data.network_entries) ? data.network_entries : [];
  const enumeration = data.enumeration || null;
  const sqliteSummary = data.sqlite_library_summary || null;
  const previewEntries = entries.slice(0, 12);

  let html = `
    <section class="siso-section">
      <div class="siso-section-head">
        <div class="siso-section-title">Atlas Summary</div>
        <div class="text-dim">${data.generated_at || ''}</div>
      </div>
      <div class="siso-summary-grid">
        <div class="siso-stat-card"><div class="siso-stat-label">Input Networks</div><div class="siso-stat-value">${data.input_network_count ?? 0}</div></div>
        <div class="siso-stat-card"><div class="siso-stat-label">Unique Networks</div><div class="siso-stat-value">${data.unique_network_count ?? 0}</div></div>
        <div class="siso-stat-card"><div class="siso-stat-label">Successful</div><div class="siso-stat-value">${data.successful_network_count ?? 0}</div></div>
        <div class="siso-stat-card"><div class="siso-stat-label">Deduplicated</div><div class="siso-stat-value">${data.deduplicated_network_count ?? 0}</div></div>
      </div>
      <div class="siso-summary-line">
        ${data.pruned_against_library ? '<span class="summary-chip">reused library</span>' : ''}
        ${data.pruned_against_sqlite ? '<span class="summary-chip">reused sqlite</span>' : ''}
        ${data.sqlite_persisted ? '<span class="summary-chip">persisted sqlite</span>' : ''}
        <span class="summary-chip">skipped slices ${data.skipped_existing_slice_count ?? 0}</span>
        <span class="summary-chip">skipped networks ${data.skipped_existing_network_count ?? 0}</span>
      </div>
    </section>
  `;

  if (data.sqlite_path || sqliteSummary) {
    html += `
      <section class="siso-section">
        <div class="siso-section-head">
          <div class="siso-section-title">SQLite Store</div>
          <div class="text-dim">${data.sqlite_persisted ? 'updated' : (data.pruned_against_sqlite ? 'read-only reuse' : 'not used')}</div>
        </div>
        <div class="atlas-inline-card">
          <div><strong>Path</strong></div>
          <div class="text-dim atlas-path-inline">${escapeHtml(data.sqlite_path || 'n/a')}</div>
        </div>
        ${sqliteSummary ? `
          <div class="siso-summary-grid">
            <div class="siso-stat-card"><div class="siso-stat-label">Atlases</div><div class="siso-stat-value">${sqliteSummary.atlas_count ?? 0}</div></div>
            <div class="siso-stat-card"><div class="siso-stat-label">Networks</div><div class="siso-stat-value">${sqliteSummary.unique_network_count ?? 0}</div></div>
            <div class="siso-stat-card"><div class="siso-stat-label">Slices</div><div class="siso-stat-value">${sqliteSummary.behavior_slice_count ?? 0}</div></div>
            <div class="siso-stat-card"><div class="siso-stat-label">Buckets</div><div class="siso-stat-value">${sqliteSummary.family_bucket_count ?? 0}</div></div>
          </div>
        ` : ''}
      </section>
    `;
  }

  if (enumeration) {
    html += `
      <section class="siso-section">
        <div class="siso-section-head">
          <div class="siso-section-title">Enumeration</div>
          <div class="text-dim">${enumeration.truncated ? 'truncated' : 'complete'}</div>
        </div>
        <div class="siso-summary-line">
          <span class="summary-chip">generated ${enumeration.generated_network_count ?? 0}</span>
          <span class="summary-chip">mode ${escapeHtml(enumeration.enumeration_spec?.mode || 'unknown')}</span>
          <span class="summary-chip">base counts ${(enumeration.enumeration_spec?.base_species_counts || []).join(', ') || 'n/a'}</span>
        </div>
      </section>
    `;
  }

  html += `
    <section class="siso-section">
      <div class="siso-section-head">
        <div class="siso-section-title">Networks</div>
        <div class="text-dim">showing ${previewEntries.length} of ${entries.length}</div>
      </div>
      <div class="siso-table-wrap scroll-panel">
        <table class="siso-family-table">
          <thead>
            <tr>
              <th>Label</th>
              <th>Status</th>
              <th>d</th>
              <th>r</th>
              <th>Support</th>
              <th>Motifs</th>
            </tr>
          </thead>
          <tbody>
            ${previewEntries.map(entry => `
              <tr>
                <td class="siso-wrap-cell">${escapeHtml(entry.source_label || entry.network_id || 'network')}</td>
                <td>${formatAtlasStatusTag(entry.analysis_status)}</td>
                <td>${entry.base_species_count ?? '-'}</td>
                <td>${entry.reaction_count ?? '-'}</td>
                <td>${entry.max_support ?? '-'}</td>
                <td class="siso-wrap-cell">${renderAtlasLabelRefs(entry.motif_union || [])}</td>
              </tr>
            `).join('')}
          </tbody>
        </table>
      </div>
    </section>
  `;

  if (data.duplicate_inputs?.length) {
    html += `
      <section class="siso-section">
        <div class="siso-section-head">
          <div class="siso-section-title">Deduplicated Inputs</div>
          <div class="text-dim">${data.duplicate_inputs.length}</div>
        </div>
        <div class="atlas-dedup-list">
          ${data.duplicate_inputs.slice(0, 8).map(item => `
            <div class="atlas-inline-card">
              <div><strong>${escapeHtml(item.source_label || 'duplicate')}</strong></div>
              <div class="text-dim">canonical ${escapeHtml(item.duplicate_of_network_id || '')}</div>
            </div>
          `).join('')}
        </div>
      </section>
    `;
  }

  return html;
}

function renderAtlasQueryResult(data) {
  const results = Array.isArray(data.results) ? data.results : [];
  const resultUnit = data.result_unit || 'slice';
  const query = data.query || {};
  const querySource = data.query_source || 'atlas';
  const sqlitePath = data.sqlite_path || '';
  const goal = query.goal || {};
  const hasGraphSpec = (query.required_regimes?.length || 0) > 0 || (query.forbidden_regimes?.length || 0) > 0 ||
    (query.required_transitions?.length || 0) > 0 || (query.forbidden_transitions?.length || 0) > 0;
  const hasPathSpec = (query.required_path_sequences?.length || 0) > 0 || query.forbid_singular_on_witness || query.max_witness_path_length != null;
  const hasPolytopeSpec = query.require_witness_feasible || query.require_witness_robust || query.min_witness_volume_mean != null;
  const hasGoal = Object.keys(goal).length > 0;

  let html = `
    <section class="siso-section">
      <div class="siso-section-head">
        <div class="siso-section-title">Query Summary</div>
        <div class="text-dim">${data.result_count ?? 0} matches</div>
      </div>
      <div class="siso-summary-line">
        <span class="summary-chip">unit ${escapeHtml(resultUnit)}</span>
        <span class="summary-chip">ranking ${escapeHtml(query.ranking_mode || 'minimal_first')}</span>
        <span class="summary-chip">limit ${query.limit ?? '-'}</span>
        <span class="summary-chip">source ${escapeHtml(querySource)}</span>
        ${hasGoal ? '<span class="summary-chip">goal dsl</span>' : ''}
        ${query.pareto_only ? '<span class="summary-chip">pareto only</span>' : ''}
        ${hasGraphSpec ? '<span class="summary-chip">graph spec</span>' : ''}
        ${hasPathSpec ? '<span class="summary-chip">path spec</span>' : ''}
        ${hasPolytopeSpec ? '<span class="summary-chip">polytope spec</span>' : ''}
      </div>
      ${sqlitePath ? `<div class="text-dim atlas-path-inline">${escapeHtml(sqlitePath)}</div>` : ''}
      ${hasGoal ? `<div class="text-dim">goal: ${escapeHtml(JSON.stringify(goal))}</div>` : ''}
    </section>
  `;

  if (!results.length) {
    html += '<div class="text-dim">No atlas entries matched the current query.</div>';
    return html;
  }

  html += `
    <section class="siso-section">
      <div class="siso-section-head">
        <div class="siso-section-title">Top Matches</div>
        <div class="text-dim">ranked</div>
      </div>
      <div class="family-grid">
        ${results.map((result, idx) => {
          const accent = getFamilyColor(idx + 1, 1);
          const motifLabels = resultUnit === 'network'
            ? (result.motif_union || [])
            : (result.matched_motif_buckets || []).map(bucket => bucket.family_label);
          const exactLabels = resultUnit === 'network'
            ? (result.exact_union || [])
            : (result.matched_exact_buckets || []).map(bucket => bucket.family_label);
          const witnessPath = resultUnit === 'network' ? result.best_witness_path : result.best_witness_path;
          const title = resultUnit === 'network'
            ? escapeHtml(result.source_label || result.network_id || `network_${idx + 1}`)
            : `${escapeHtml(result.source_label || result.network_id || `slice_${idx + 1}`)} <span class="text-dim">${escapeHtml(result.input_symbol || '')} -> ${escapeHtml(result.output_symbol || '')}</span>`;
          return `
            <div class="family-card" style="--family-accent:${accent}; --family-soft:${hexToRgba(accent, 0.16)};">
              <div class="family-card-header">
                <div>
                  <div class="family-kicker">Rank ${result.rank ?? idx + 1}</div>
                  <div class="family-title">${title}</div>
                  <div class="family-subtitle">${escapeHtml(resultUnit === 'network' ? `${result.matching_slice_count || 1} matching slices` : `${result.slice_id || 'slice'} in ${result.network_id || 'network'}`)}</div>
                </div>
              </div>
              <div class="family-meta">
                <span class="family-metric">d ${result.base_species_count ?? '-'}</span>
                <span class="family-metric">r ${result.reaction_count ?? '-'}</span>
                <span class="family-metric">support ${result.max_support ?? '-'}</span>
                <span class="family-metric">mass ${result.support_mass ?? '-'}</span>
                <span class="family-metric">robust ${Number(result.robustness_score || 0).toFixed(2)}</span>
              </div>
              <div class="family-meta">
                <span class="family-metric">regimes ${result.matched_regime_count ?? 0}</span>
                <span class="family-metric">transitions ${result.matched_transition_count ?? 0}</span>
                <span class="family-metric">witness ${result.witness_path_count ?? 0}</span>
              </div>
              <div>
                <div class="family-kicker">Motifs</div>
                <div class="siso-wrap-cell">${renderAtlasLabelRefs(motifLabels)}</div>
              </div>
              <div>
                <div class="family-kicker">Exact Families</div>
                <div class="siso-wrap-cell">${renderAtlasLabelRefs(exactLabels)}</div>
              </div>
              <div>
                <div class="family-kicker">Rules</div>
                ${renderAtlasRules(result.raw_rules || [])}
              </div>
              <div>
                <div class="family-kicker">Best Witness Path</div>
                ${renderWitnessPathSummary(witnessPath)}
              </div>
            </div>
          `;
        }).join('')}
      </div>
    </section>
  `;

  return html;
}

async function executeAtlasBuilder(nodeId) {
  const contentEl = document.getElementById(`${nodeId}-content`);
  let payload;
  try {
    payload = getConnectedAtlasSpec(nodeId);
  } catch (e) {
    if (contentEl) contentEl.innerHTML = `<div class="node-error">${escapeHtml(e.message)}</div>`;
    return;
  }

  if (!payload) {
    showToast('Connect an Atlas Spec node first');
    return;
  }

  setNodeLoading(nodeId, true);
  try {
    const data = await api('build_atlas', payload.spec);
    const info = nodeRegistry[nodeId];
    if (info) {
      info.data = info.data || {};
      info.data.atlasData = data;
      info.data.lastSpec = payload.serial;
      info.data.sqlitePath = data.sqlite_path || payload.spec.sqlite_path || '';
    }
    if (contentEl) contentEl.innerHTML = renderAtlasBuilderResult(data);
    commitWorkspaceSnapshot('atlas-built');
    triggerDownstreamNodes(nodeId, 'atlas');
  } catch (e) {
    if (contentEl) contentEl.innerHTML = `<div class="node-error">${escapeHtml(e.message)}</div>`;
  } finally {
    setNodeLoading(nodeId, false);
  }
}

async function executeAtlasQueryResult(nodeId) {
  const contentEl = document.getElementById(`${nodeId}-content`);
  const atlas = getConnectedAtlasData(nodeId);

  let queryPayload;
  try {
    queryPayload = getConnectedAtlasQuery(nodeId);
  } catch (e) {
    if (contentEl) contentEl.innerHTML = `<div class="node-error">${escapeHtml(e.message)}</div>`;
    return;
  }

  if (!queryPayload) {
    showToast('Connect an Atlas Query Config node first');
    return;
  }

  const configuredSqlitePath = queryPayload.sqlitePath;
  const persistedAtlasSqlitePath = queryPayload.preferPersistedAtlas && atlas?.sqlite_persisted ? atlas.sqlite_path : '';
  const sqlitePath = configuredSqlitePath || persistedAtlasSqlitePath;

  if (!atlas && !sqlitePath) {
    showToast('Build an atlas first, or provide a SQLite path in Atlas Query Config');
    return;
  }

  setNodeLoading(nodeId, true);
  try {
    const request = sqlitePath
      ? { sqlite_path: sqlitePath, query: queryPayload.query }
      : { atlas, query: queryPayload.query };
    const data = await api('query_atlas', request);
    const renderData = {
      ...data,
      query_source: sqlitePath ? 'sqlite' : 'atlas',
      sqlite_path: sqlitePath || '',
    };
    const info = nodeRegistry[nodeId];
    if (info) {
      info.data = info.data || {};
      info.data.queryData = renderData;
      info.data.lastQuery = queryPayload.serial;
    }
    if (contentEl) contentEl.innerHTML = renderAtlasQueryResult(renderData);
    commitWorkspaceSnapshot('atlas-query');
  } catch (e) {
    if (contentEl) contentEl.innerHTML = `<div class="node-error">${escapeHtml(e.message)}</div>`;
  } finally {
    setNodeLoading(nodeId, false);
  }
}

function addReactionRow(nodeId, rule = '', kd = 1e-3) {
  const list = document.getElementById(`${nodeId}-reactions-list`);
  if (!list) return;
  const row = document.createElement('div');
  row.className = 'reaction-row';
  row.innerHTML = `
    <input type="text" class="reaction-input" value="${rule}" placeholder="A + B <-> C">
    <input type="number" class="kd-input" value="${kd == null ? '' : kd}" step="any" min="1e-12" placeholder="optional">
    <button class="btn-remove" title="Remove">&times;</button>
  `;

  const removeBtn = row.querySelector('.btn-remove');
  removeBtn.onclick = () => {
    row.remove();
    triggerAutoModelBuild(nodeId);
  };

  // Add event listeners for auto-build
  const reactionInput = row.querySelector('.reaction-input');
  const kdInput = row.querySelector('.kd-input');

  [reactionInput, kdInput].forEach(input => {
    input.addEventListener('input', () => {
      clearTimeout(input._autoTimer);
      input._autoTimer = setTimeout(() => {
        triggerAutoModelBuild(nodeId);
      }, 1000);
    });
  });

  list.appendChild(row);
}

// ===== Build Model =====
async function buildModel(modelBuilderNodeId, options = {}) {
  const shouldTriggerDownstream = options.triggerDownstream !== false;
  // Find connected reaction-network
  const conn = connections.find(c => c.toNode === modelBuilderNodeId && c.toPort === 'reactions');
  if (!conn) {
    showToast('Model Builder has no Reaction Network connected');
    return;
  }
  const rnNodeId = conn.fromNode;
  const { reactions, kds } = getReactionsFromNode(rnNodeId);
  if (reactions.length === 0) { showToast('Add at least one reaction'); return; }
  if (kds.some(kd => kd == null || kd <= 0)) { showToast('Model Builder requires Kd for every reaction (> 0)'); return; }

  setNodeLoading(modelBuilderNodeId, true);
  try {
    const data = await api('build_model', { reactions, kd: kds });
    const modelContext = {
      sessionId: data.session_id,
      model: data,
      qK_syms: [...data.q_sym, ...data.K_sym],
    };
    state.sessionId = data.session_id;
    state.model = data;
    state.qK_syms = modelContext.qK_syms;

    // Update model info display
    const infoEl = document.getElementById(`${modelBuilderNodeId}-model-info`);
    const infoText = document.getElementById(`${modelBuilderNodeId}-model-info-text`);
    if (infoEl && infoText) {
      const info = `n=${data.n}, d=${data.d}, r=${data.r}\nSpecies: ${data.x_sym.join(', ')}\nTotals: ${data.q_sym.join(', ')}\nConstants: ${data.K_sym.join(', ')}`;
      infoEl.style.display = '';
      infoText.textContent = info;
    }

    // Store model builder node reference
    nodeRegistry[modelBuilderNodeId].data.built = true;
    nodeRegistry[modelBuilderNodeId].data.modelContext = modelContext;

    showToast('Model built successfully');
    commitWorkspaceSnapshot('model-built');

    // Trigger all downstream viewers
    if (shouldTriggerDownstream) {
      onModelBuilt(modelBuilderNodeId);
    }
  } catch (e) {
    console.error('Build model failed:', e);
  }
  setNodeLoading(modelBuilderNodeId, false);
}

// ===== Downstream Viewer Auto-Execution =====
function triggerDownstreamNodes(fromNodeId, fromPort) {
  const downstream = connections.filter(c => c.fromNode === fromNodeId && c.fromPort === fromPort);
  for (const conn of downstream) {
    const viewerInfo = nodeRegistry[conn.toNode];
    if (!viewerInfo) continue;
    const typeDef = NODE_TYPES[viewerInfo.type];
    if (typeDef && typeDef.execute) {
      // Mark input wires as transmitting
      const wireId = `wire-${conn.fromNode}-${conn.toNode}`;
      const wire = document.getElementById(wireId);
      if (wire) wire.classList.add('transmitting');

      // Execute asynchronously (don't await — run in parallel)
      typeDef.execute(conn.toNode).catch(e => {
        console.error(`Node ${conn.toNode} (${viewerInfo.type}) failed:`, e);
      }).finally(() => {
        // Remove transmitting state immediately after execution completes
        if (wire) wire.classList.remove('transmitting');
      });
    }
  }
}

async function onModelBuilt(modelBuilderNodeId) {
  triggerDownstreamNodes(modelBuilderNodeId, 'model');
}

function syncSelectOptions(selectEl, values, preferredValue = null, fallbackIndex = 0) {
  if (!selectEl) return;
  const orderedValues = Array.isArray(values) ? values.filter(v => v != null && v !== '') : [];
  const previousValue = preferredValue ?? selectEl.value;
  selectEl.innerHTML = '';
  orderedValues.forEach(value => selectEl.add(new Option(value, value)));
  if (!orderedValues.length) return;
  if (previousValue && orderedValues.includes(previousValue)) {
    selectEl.value = previousValue;
    return;
  }
  const safeIndex = Math.min(Math.max(fallbackIndex, 0), orderedValues.length - 1);
  selectEl.value = orderedValues[safeIndex];
}

// ===== Markdown Note Functions =====
function switchNoteTab(nodeId, tab) {
  const editArea = document.getElementById(`${nodeId}-edit-area`);
  const previewArea = document.getElementById(`${nodeId}-preview-area`);
  const node = document.getElementById(nodeId);

  if (!editArea || !previewArea || !node) return;

  // Update tab buttons
  node.querySelectorAll('.note-tab').forEach(btn => {
    btn.classList.toggle('active', btn.dataset.tab === tab);
  });

  if (tab === 'edit') {
    editArea.style.display = '';
    previewArea.style.display = 'none';
  } else {
    editArea.style.display = 'none';
    previewArea.style.display = '';
    renderMarkdown(nodeId);
  }
}

function renderMarkdown(nodeId) {
  const textarea = document.getElementById(`${nodeId}-markdown`);
  const preview = document.getElementById(`${nodeId}-preview`);

  if (!textarea || !preview) return;

  const markdown = textarea.value;
  preview.innerHTML = simpleMarkdownToHTML(markdown);
}

function simpleMarkdownToHTML(markdown) {
  if (!markdown) return '<p class="text-dim">No content yet.</p>';

  let html = markdown;

  // Escape HTML
  html = html.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');

  // Headers
  html = html.replace(/^### (.*$)/gim, '<h3>$1</h3>');
  html = html.replace(/^## (.*$)/gim, '<h2>$1</h2>');
  html = html.replace(/^# (.*$)/gim, '<h1>$1</h1>');

  // Bold
  html = html.replace(/\*\*(.+?)\*\*/g, '<strong>$1</strong>');
  html = html.replace(/__(.+?)__/g, '<strong>$1</strong>');

  // Italic
  html = html.replace(/\*(.+?)\*/g, '<em>$1</em>');
  html = html.replace(/_(.+?)_/g, '<em>$1</em>');

  // Code inline
  html = html.replace(/`(.+?)`/g, '<code>$1</code>');

  // Links
  html = html.replace(/\[([^\]]+)\]\(([^)]+)\)/g, '<a href="$2" target="_blank">$1</a>');

  // Lists
  html = html.replace(/^\* (.+)$/gim, '<li>$1</li>');
  html = html.replace(/^- (.+)$/gim, '<li>$1</li>');
  html = html.replace(/(<li>.*<\/li>)/s, '<ul>$1</ul>');

  // Line breaks
  html = html.replace(/\n\n/g, '</p><p>');
  html = html.replace(/\n/g, '<br>');

  // Wrap in paragraphs
  if (!html.startsWith('<h') && !html.startsWith('<ul>')) {
    html = '<p>' + html + '</p>';
  }

  return html;
}

function updateRegimeGraphMode(nodeId) {
  const modeEl = document.getElementById(`${nodeId}-graph-mode`);
  const changeRow = document.getElementById(`${nodeId}-change-qk-row`);
  if (!modeEl || !changeRow) return;
  changeRow.style.display = modeEl.value === 'siso' ? '' : 'none';
}

async function executeRegimeGraph(nodeId) {
  const contentEl = document.getElementById(`${nodeId}-content`);
  if (!contentEl) return;

  const modelContext = getModelContextForNode(nodeId);
  const qKSymbols = modelContext?.qK_syms || [];
  const modeEl = document.getElementById(`${nodeId}-graph-mode`);
  const changeEl = document.getElementById(`${nodeId}-change-qk`);
  const info = nodeRegistry[nodeId];
  const config = info?.data?.config || {};

  syncSelectOptions(changeEl, qKSymbols, config.changeQK || changeEl?.value, 0);
  updateRegimeGraphMode(nodeId);

  const graphMode = modeEl?.value || 'qk';
  const changeQK = changeEl?.value || qKSymbols[0] || '';
  const viewMode = '3d';

  info.data = info.data || {};
  info.data.config = { graphMode, changeQK, viewMode };

  setNodeLoading(nodeId, true);
  try {
    if (!modelContext?.sessionId) throw new Error('Build the connected model first');
    if (graphMode === 'siso' && !changeQK) throw new Error('Select a qK coordinate for SISO graph');

    const payload = {
      session_id: modelContext.sessionId,
      graph_mode: graphMode,
    };
    if (graphMode === 'siso') payload.change_qK = changeQK;

    const data = await api('build_graph', payload);
    if (nodeRegistry[nodeId]) {
      nodeRegistry[nodeId].data = nodeRegistry[nodeId].data || {};
      nodeRegistry[nodeId].data.graphData = data;
    }
    contentEl.innerHTML = `<div class="plot-container" id="${nodeId}-plot"></div>`;
    commitWorkspaceSnapshot('regime-graph');
    setTimeout(() => {
      plotRegimeGraph(data, `${nodeId}-plot`, { viewMode });
      setupPlotResize(nodeId, `${nodeId}-plot`);
    }, 50);
  } catch (e) {
    contentEl.innerHTML = `<div class="node-error">${e.message}</div>`;
  }
  setNodeLoading(nodeId, false);
}

// ===== Recompute Functions =====
function recomputeSISO(nodeId) {
  const typeDef = NODE_TYPES['siso-analysis'];
  if (typeDef.execute) typeDef.execute(nodeId);
}

function formatVolumeSummary(vol) {
  if (!vol || vol.mean == null) return 'n/a';
  const mean = Number(vol.mean);
  const std = Number(vol.std ?? Math.sqrt(vol.var ?? 0));
  if (!Number.isFinite(mean)) return 'n/a';
  if (!Number.isFinite(std)) return mean.toExponential(2);
  return `${mean.toExponential(2)} ± ${std.toExponential(1)}`;
}

function renderExclusionCounts(exclusionCounts) {
  const entries = Object.entries(exclusionCounts || {});
  if (!entries.length) return '';
  const items = entries.map(([reason, count]) => `<span class="tag tag-nonasym">${reason}: ${count}</span>`).join(' ');
  return `<div class="siso-inline-tags"><strong>Excluded paths</strong>: ${items}</div>`;
}

const SISO_FAMILY_COLORS = ['#ff8c42', '#2ec4b6', '#f94144', '#577590', '#f9c74f', '#8d99ae', '#90be6d', '#c77dff', '#4cc9f0', '#fb6f92'];

function hexToRgba(hex, alpha) {
  const clean = (hex || '#888888').replace('#', '');
  const value = clean.length === 3
    ? clean.split('').map(ch => ch + ch).join('')
    : clean;
  const intVal = parseInt(value, 16);
  const r = (intVal >> 16) & 255;
  const g = (intVal >> 8) & 255;
  const b = intVal & 255;
  return `rgba(${r}, ${g}, ${b}, ${alpha})`;
}

function prefersLightTheme() {
  return themeState.effective === 'light';
}

function getPlotTheme() {
  if (prefersLightTheme()) {
    return {
      paperBg: '#f4f8fc',
      plotBg: '#ffffff',
      sceneBg: '#f4f8fc',
      fontColor: '#5f7184',
      titleColor: '#223242',
      gridColor: '#d8e2ec',
      zeroLineColor: '#c9d5e1',
      legendBg: 'rgba(255,255,255,0.72)',
      legendBorderColor: '#d7e1eb',
      annotationBg: 'rgba(248,250,253,0.92)',
      annotationBorderColor: '#cad7e4',
      edgeLineColor: '#8b9caf',
      edgeLine3DColor: '#90a2b5',
      edgeArrowColor: '#72859a',
      edgeConeColor: '#7d90a4',
      edgeLabelColor: '#55687c',
      edgeHoverMarkerColor: 'rgba(124, 142, 164, 0.12)',
      subtleTextColor: '#738498',
      contourLineColor: '#1f2b38',
      nodeOutlineColor: '#1d2733',
      nodeTextColor: '#ffffff',
    };
  }

  return {
    paperBg: '#111',
    plotBg: '#1a1a1a',
    sceneBg: '#111',
    fontColor: '#888',
    titleColor: '#888',
    gridColor: '#333',
    zeroLineColor: '#444',
    legendBg: 'rgba(0,0,0,0)',
    legendBorderColor: 'rgba(0,0,0,0)',
    annotationBg: 'rgba(0,0,0,0.38)',
    annotationBorderColor: '#5e6a78',
    edgeLineColor: '#55606f',
    edgeLine3DColor: '#617082',
    edgeArrowColor: '#7b8794',
    edgeConeColor: '#8fa0b4',
    edgeLabelColor: '#c9d3dc',
    edgeHoverMarkerColor: 'rgba(201, 211, 220, 0.02)',
    subtleTextColor: '#7c8a97',
    contourLineColor: '#fff',
    nodeOutlineColor: '#1d2733',
    nodeTextColor: '#ffffff',
  };
}

function themeAxisTitle(title, color) {
  if (title == null) return title;
  if (typeof title === 'string') {
    return { text: title, font: { color } };
  }
  return {
    ...title,
    font: {
      ...(title.font || {}),
      color,
    },
  };
}

function applyPlotAxisTheme(axis, theme) {
  if (!axis) return axis;
  const themed = {
    ...axis,
    tickfont: {
      ...(axis.tickfont || {}),
      color: theme.fontColor,
    },
    title: themeAxisTitle(axis.title, theme.fontColor),
  };

  if (axis.showgrid !== false || axis.gridcolor !== undefined) {
    themed.gridcolor = theme.gridColor;
  }
  if (axis.zeroline !== false || axis.zerolinecolor !== undefined) {
    themed.zerolinecolor = theme.zeroLineColor;
  }
  if (axis.showline || axis.linecolor !== undefined) {
    themed.linecolor = theme.zeroLineColor;
  }

  return themed;
}

function applyPlotSceneAxisTheme(axis, theme) {
  if (!axis) return axis;
  const themed = {
    ...axis,
    color: theme.fontColor,
    title: themeAxisTitle(axis.title, theme.fontColor),
  };

  if (axis.showgrid !== false || axis.gridcolor !== undefined) {
    themed.gridcolor = theme.gridColor;
  }
  if (axis.zeroline !== false || axis.zerolinecolor !== undefined) {
    themed.zerolinecolor = theme.zeroLineColor;
  }
  if (axis.showbackground !== false || axis.backgroundcolor !== undefined) {
    themed.backgroundcolor = theme.sceneBg;
  }

  return themed;
}

function applyPlotLayoutTheme(layout) {
  const theme = getPlotTheme();
  const themed = {
    ...layout,
    paper_bgcolor: theme.paperBg,
    plot_bgcolor: theme.plotBg,
    font: {
      ...(layout.font || {}),
      color: theme.fontColor,
    },
  };

  if (layout.title) {
    themed.title = {
      ...layout.title,
      font: {
        ...(layout.title.font || {}),
        color: theme.titleColor,
      },
    };
  }

  if (layout.legend) {
    themed.legend = {
      ...layout.legend,
      bgcolor: theme.legendBg,
      bordercolor: theme.legendBorderColor,
      font: {
        ...(layout.legend.font || {}),
        color: theme.fontColor,
      },
    };
  }

  if (layout.xaxis) themed.xaxis = applyPlotAxisTheme(layout.xaxis, theme);
  if (layout.yaxis) themed.yaxis = applyPlotAxisTheme(layout.yaxis, theme);

  if (layout.scene) {
    themed.scene = {
      ...layout.scene,
      bgcolor: theme.sceneBg,
      xaxis: applyPlotSceneAxisTheme(layout.scene.xaxis, theme),
      yaxis: applyPlotSceneAxisTheme(layout.scene.yaxis, theme),
      zaxis: applyPlotSceneAxisTheme(layout.scene.zaxis, theme),
    };
  }

  return themed;
}

function themedColorbar(title) {
  const theme = getPlotTheme();
  return {
    title,
    titlefont: { color: theme.fontColor, size: 9 },
    tickfont: { color: theme.fontColor, size: 8 },
  };
}

function getFamilyColor(index, offset = 0) {
  const safeIndex = Math.max(1, Number(index) || 1);
  return SISO_FAMILY_COLORS[(safeIndex - 1 + offset) % SISO_FAMILY_COLORS.length];
}

function buildPathFamilyMaps(data) {
  const exactFamilyByPath = new Map();

  (data.exact_families || []).forEach(family => {
    (family.path_indices || []).forEach(pathIdx => exactFamilyByPath.set(pathIdx, family.family_idx));
  });

  return { exactFamilyByPath };
}

function buildSISOSelection(nodeId, changeQK, pathIdx) {
  const nodeData = nodeRegistry[nodeId]?.data;
  const behaviorData = nodeData?.behaviorData;
  if (!behaviorData) return null;

  const path = (behaviorData.paths || []).find(p => p.path_idx === pathIdx);
  if (!path) return null;

  const { exactFamilyByPath } = buildPathFamilyMaps(behaviorData);
  return {
    path_idx: pathIdx,
    change_qK: behaviorData.change_qK || changeQK,
    observe_x: behaviorData.observe_x,
    exact_family_idx: exactFamilyByPath.get(pathIdx) || null,
    exact_label: path.exact_label,
    feasible: path.feasible,
    included: path.included,
    vertex_indices: path.vertex_indices,
    perms: path.perms,
  };
}

function setSISOSelection(nodeId, changeQK, pathIdx) {
  const nodeData = nodeRegistry[nodeId]?.data;
  if (!nodeData) return null;
  const selection = buildSISOSelection(nodeId, changeQK, pathIdx);
  if (!selection) return null;
  nodeData.selectedPath = selection;
  commitWorkspaceSnapshot('siso-selection');
  triggerDownstreamNodes(nodeId, 'result');
  return selection;
}

function clearSISOSelection(nodeId, notify = true) {
  const nodeData = nodeRegistry[nodeId]?.data;
  if (!nodeData) return;
  nodeData.selectedPath = null;
  commitWorkspaceSnapshot('siso-selection-cleared');
  if (notify) triggerDownstreamNodes(nodeId, 'result');
}

function renderPathChips(nodeId, changeQK, pathIndices, accent) {
  return (pathIndices || []).map(pathIdx => `
    <button
      type="button"
      class="path-chip"
      data-path-idx="${pathIdx}"
      style="--path-chip-accent:${accent}; --path-chip-soft:${hexToRgba(accent, 0.16)};"
      onclick="plotSISOPath('${nodeId}', '${changeQK}', ${pathIdx}, this)"
    >#${pathIdx}</button>
  `).join('');
}

function renderFamilyTable(nodeId, changeQK, families) {
  if (!families.length) return '';

  const rows = families.map(family => {
    const accent = getFamilyColor(family.family_idx, 0);
    const badgeStyle = `--badge-accent:${accent}; --badge-soft:${hexToRgba(accent, 0.18)};`;
    const profile = family.exact_label;

    return `
      <tr>
        <td><span class="family-badge family-badge-exact" style="${badgeStyle}">E${family.family_idx}</span></td>
        <td class="siso-profile-cell">${profile}</td>
        <td>
          <div class="family-path-chips">
            ${renderPathChips(nodeId, changeQK, family.path_indices, accent)}
          </div>
        </td>
        <td>${formatVolumeSummary(family.total_volume)}</td>
      </tr>
    `;
  }).join('');

  const headerRow = '<tr><th>#</th><th>RO profile</th><th>Paths</th><th>Volume</th></tr>';

  return `
    <section class="siso-section">
      <div class="siso-section-head">
        <div class="siso-section-title">Exact Families</div>
        <div class="text-dim">${families.length} families</div>
      </div>
      <div class="siso-table-wrap scroll-panel">
        <table class="siso-family-table">
          <thead>${headerRow}</thead>
          <tbody>${rows}</tbody>
        </table>
      </div>
    </section>
  `;
}

function renderBehaviorFamiliesResult(nodeId, changeQK, data) {
  const { exactFamilyByPath } = buildPathFamilyMaps(data);
  const feasiblePaths = (data.paths || [])
    .filter(path => path.feasible)
    .sort((a, b) => {
      const exactA = exactFamilyByPath.get(a.path_idx) || Number.MAX_SAFE_INTEGER;
      const exactB = exactFamilyByPath.get(b.path_idx) || Number.MAX_SAFE_INTEGER;
      return exactA - exactB || a.path_idx - b.path_idx;
    });
  const includedFeasiblePaths = feasiblePaths.filter(path => path.included);

  let html = '';

  html += renderFamilyTable(nodeId, changeQK, data.exact_families || []);

  html += `
    <section class="siso-section">
      <div class="siso-section-head">
        <div class="siso-section-title">Feasible Paths</div>
        <div class="text-dim">Sorted by exact family</div>
      </div>
      <div class="path-list siso-feasible-list scroll-panel">
  `;

  feasiblePaths.forEach(path => {
    const permStr = path.perms.map(pr => `[${pr.join(',')}]`).join(' → ');
    const exactFamilyIdx = exactFamilyByPath.get(path.path_idx);
    const exactAccent = getFamilyColor(exactFamilyIdx || path.path_idx, 0);
    const includeTag = path.included
      ? '<span class="tag tag-asym">Included</span>'
      : `<span class="tag tag-nonasym">${path.exclusion_reason || 'Excluded'}</span>`;

    html += `
      <div
        class="path-item siso-path-item ${path.included ? 'is-included' : 'is-excluded'}"
        data-idx="${path.path_idx}"
        data-path-idx="${path.path_idx}"
        data-qk="${changeQK}"
        data-node="${nodeId}"
        style="--exact-accent:${exactAccent}; --exact-soft:${hexToRgba(exactAccent, 0.14)};"
        onclick="selectSISOPath(this)"
      >
        <div class="siso-path-head">
          <div class="siso-path-title">Path #${path.path_idx}</div>
          <button type="button" class="btn btn-small siso-inline-btn" onclick="event.stopPropagation(); plotSISOPath('${nodeId}', '${changeQK}', ${path.path_idx});">Plot</button>
        </div>
        <div class="siso-path-badges">
          ${exactFamilyIdx ? `<span class="family-badge family-badge-exact" style="--badge-accent:${exactAccent}; --badge-soft:${hexToRgba(exactAccent, 0.18)};">Exact ${exactFamilyIdx}</span>` : ''}
          ${includeTag}
        </div>
        <div class="siso-path-detail">${permStr}</div>
        <div class="siso-path-meta">
          <span>RO ${path.exact_label}</span>
          <span>Vol ${formatVolumeSummary(path.volume)}</span>
        </div>
      </div>
    `;
  });

  html += `
      </div>
    </section>
  `;

  const showPlot = includedFeasiblePaths.length > 0 ? '' : 'display:none;';
  html += `<div class="plot-container" id="${nodeId}-traj-plot" style="${showPlot}"></div>`;
  return html;
}

function normalizeSISOConfig(rawConfig) {
  if (!rawConfig) return null;
  const nested = rawConfig.config || {};
  return {
    change_qK: rawConfig.change_qK ?? rawConfig.changeQK ?? nested.change_qK ?? nested.changeQK ?? '',
    observe_x: rawConfig.observe_x ?? rawConfig.observeX ?? nested.observe_x ?? nested.observeX ?? '',
    path_scope: rawConfig.path_scope ?? rawConfig.pathScope ?? nested.path_scope ?? nested.pathScope ?? 'feasible',
    min_volume_mean: rawConfig.min_volume_mean ?? rawConfig.minVolumeMean ?? nested.min_volume_mean ?? nested.minVolumeMean ?? 0,
    keep_singular: rawConfig.keep_singular ?? rawConfig.keepSingular ?? nested.keep_singular ?? nested.keepSingular ?? true,
    keep_nonasymptotic: rawConfig.keep_nonasymptotic ?? rawConfig.keepNonasymptotic ?? nested.keep_nonasymptotic ?? nested.keepNonasymptotic ?? false,
    min: rawConfig.min ?? nested.min ?? -6,
    max: rawConfig.max ?? nested.max ?? 6,
  };
}

function getConnectedSISOConfig(resultNodeId) {
  const paramsConn = connections.find(c => c.toNode === resultNodeId && c.toPort === 'params');
  if (!paramsConn) return null;
  const paramsNodeId = paramsConn.fromNode;
  const liveConfig = normalizeSISOConfig(getNodeSerialData(paramsNodeId, 'siso-params'));
  if (liveConfig) {
    const paramsInfo = nodeRegistry[paramsNodeId];
    if (paramsInfo) {
      paramsInfo.data = paramsInfo.data || {};
      paramsInfo.data.config = liveConfig;
    }
    return liveConfig;
  }
  return normalizeSISOConfig(nodeRegistry[paramsNodeId]?.data?.config);
}

async function computeSISOResult(nodeId) {
  // Find the connected params node
  const paramsConn = connections.find(c => c.toNode === nodeId && c.toPort === 'params');
  if (!paramsConn) {
    showToast('Connect a SISO Config node first');
    return;
  }

  const paramsNode = nodeRegistry[paramsConn.fromNode];
  const config = getConnectedSISOConfig(nodeId);
  if (!paramsNode || !config) {
    showToast('SISO Config node has no configuration');
    return;
  }

  setNodeLoading(nodeId, true);
  const contentEl = document.getElementById(`${nodeId}-content`);
  const previousSelectedPath = nodeRegistry[nodeId]?.data?.selectedPath?.path_idx || null;

  try {
    const sessionId = getSessionIdForNode(nodeId);
    if (!sessionId) throw new Error('Build the connected model first');
    if (nodeRegistry[nodeId]) {
      nodeRegistry[nodeId].data = nodeRegistry[nodeId].data || {};
      nodeRegistry[nodeId].data.sisoTrajectoryRequestId = (nodeRegistry[nodeId].data.sisoTrajectoryRequestId || 0) + 1;
    }
    const data = await api('behavior_families', {
      session_id: sessionId,
      change_qK: config.change_qK,
      observe_x: config.observe_x,
      path_scope: config.path_scope,
      min_volume_mean: config.min_volume_mean,
      keep_singular: config.keep_singular,
      keep_nonasymptotic: config.keep_nonasymptotic,
      deduplicate: true,
      compute_volume: true,
    });
    if (nodeRegistry[nodeId]) {
      nodeRegistry[nodeId].data = nodeRegistry[nodeId].data || {};
      nodeRegistry[nodeId].data.behaviorData = data;
    }
    contentEl.innerHTML = renderBehaviorFamiliesResult(nodeId, config.change_qK, data);
    commitWorkspaceSnapshot('siso-behavior');
    const pathStillExists = previousSelectedPath && (data.paths || []).some(path => path.path_idx === previousSelectedPath);
    if (pathStillExists) {
      await plotSISOPath(nodeId, config.change_qK, previousSelectedPath);
    } else {
      clearSISOSelection(nodeId, previousSelectedPath !== null);
    }
  } catch (e) {
    contentEl.innerHTML = `<div class="node-error">${e.message}</div>`;
  } finally {
    setNodeLoading(nodeId, false);
  }
}

function recomputeROPCloud(nodeId) {
  const typeDef = NODE_TYPES['rop-cloud'];
  if (typeDef.execute) typeDef.execute(nodeId);
}

function recomputeHeatmap(nodeId) {
  const typeDef = NODE_TYPES['fret-heatmap'];
  if (typeDef.execute) typeDef.execute(nodeId);
}

function cloneSerializable(value) {
  if (value == null) return value;
  return JSON.parse(JSON.stringify(value));
}

function parseSpeciesFromReactionSide(side) {
  const species = [];
  side.split('+').forEach(term => {
    const t = term.trim();
    if (!t) return;
    const m = t.match(/^([0-9]+)?\s*([A-Za-z][A-Za-z0-9_]*)$/);
    if (m) species.push(m[2]);
  });
  return species;
}

function inferSpeciesOrderFromReactions(reactions) {
  const allSet = new Set();
  const productSet = new Set();
  reactions.forEach(rule => {
    const m = rule.match(/<->|<=>|↔/);
    if (!m) return;
    const parts = rule.split(m[0]);
    if (parts.length !== 2) return;
    const left = parseSpeciesFromReactionSide(parts[0]);
    const right = parseSpeciesFromReactionSide(parts[1]);
    left.forEach(s => allSet.add(s));
    right.forEach(s => {
      allSet.add(s);
      productSet.add(s);
    });
  });

  const allSpecies = Array.from(allSet).sort();
  const productSpecies = Array.from(productSet).sort();
  const freeSpecies = allSpecies.filter(s => !productSet.has(s));
  const orderedSpecies = [...freeSpecies, ...productSpecies];
  return { species: orderedSpecies, productSpecies };
}

function refreshROPCloudTargetOptions(nodeId, reactions = null) {
  const sel = document.getElementById(`${nodeId}-target-species`);
  if (!sel) return;

  if (!reactions) {
    const rxConn = connections.find(c => c.toNode === nodeId && c.toPort === 'reactions');
    if (rxConn) reactions = getReactionsFromNode(rxConn.fromNode).reactions;
  }
  reactions = reactions || [];

  const { species, productSpecies } = inferSpeciesOrderFromReactions(reactions);
  const preferred = productSpecies.length ? productSpecies : species;
  const orderedTargets = [...preferred, ...species.filter(s => !preferred.includes(s))];

  const prev = sel.value;
  sel.innerHTML = '';
  if (!orderedTargets.length) {
    const opt = document.createElement('option');
    opt.value = '';
    opt.textContent = '(target)';
    sel.appendChild(opt);
    return;
  }

  orderedTargets.forEach(sym => {
    const opt = document.createElement('option');
    opt.value = sym;
    opt.textContent = sym;
    sel.appendChild(opt);
  });

  if (prev && orderedTargets.includes(prev)) {
    sel.value = prev;
  }
}

function updateROPCloudMode(nodeId) {
  const mode = document.getElementById(`${nodeId}-sampling-mode`)?.value || 'x_space';
  const xParams = document.getElementById(`${nodeId}-xspace-params`);
  const qkParams = document.getElementById(`${nodeId}-qk-params`);
  if (xParams) xParams.style.display = mode === 'x_space' ? '' : 'none';
  if (qkParams) qkParams.style.display = mode === 'qk' ? '' : 'none';
  if (mode === 'x_space') refreshROPCloudTargetOptions(nodeId);
}

// ===== SISO Path Selection =====
async function plotSISOPath(nodeId, changeQK, pathIdx, selectedEl = null) {
  const config = getConnectedSISOConfig(nodeId);
  const sessionId = getSessionIdForNode(nodeId);
  if (!sessionId) return;
  const nodeData = nodeRegistry[nodeId]?.data;
  const requestId = (nodeData?.sisoTrajectoryRequestId || 0) + 1;
  if (nodeData) {
    nodeData.sisoTrajectoryRequestId = requestId;
  }
  const contentEl = document.getElementById(`${nodeId}-content`);
  if (contentEl) {
    contentEl.querySelectorAll('.path-item, .path-chip').forEach(p => {
      const currentIdx = parseInt(p.dataset.pathIdx || p.dataset.idx, 10);
      p.classList.toggle('selected', currentIdx === pathIdx);
    });
  }
  setSISOSelection(nodeId, changeQK, pathIdx);
  if (contentEl && selectedEl) {
    const listItem = contentEl.querySelector(`.path-item[data-path-idx="${pathIdx}"]`);
    if (listItem && listItem !== selectedEl) {
      listItem.scrollIntoView({ block: 'nearest', behavior: 'smooth' });
    }
  }
  try {
    const data = await api('siso_trajectory', {
      session_id: sessionId,
      change_qK: changeQK,
      path_idx: pathIdx,
      start: config?.min ?? -6,
      stop: config?.max ?? 6,
    });
    if (nodeRegistry[nodeId]?.data?.sisoTrajectoryRequestId !== requestId) return;
    if (nodeRegistry[nodeId]) {
      nodeRegistry[nodeId].data = nodeRegistry[nodeId].data || {};
      nodeRegistry[nodeId].data.trajectoryData = data;
    }
    const plotEl = document.getElementById(`${nodeId}-traj-plot`);
    if (plotEl) {
      plotEl.style.display = '';
      plotTrajectory(data, `${nodeId}-traj-plot`);
    }
    commitWorkspaceSnapshot('siso-trajectory');
  } catch (e) {
    console.error('Trajectory failed:', e);
  }
}

async function selectSISOPath(el) {
  const pathIdx = parseInt(el.dataset.idx);
  const changeQK = el.dataset.qk;
  const nodeId = el.dataset.node;
  await plotSISOPath(nodeId, changeQK, pathIdx, el);
}

function getConnectedSISOSelection(nodeId) {
  const conn = connections.find(c => c.toNode === nodeId && c.toPort === 'result');
  if (!conn) return null;
  const sourceInfo = nodeRegistry[conn.fromNode];
  if (!sourceInfo) return null;
  return {
    sourceNodeId: conn.fromNode,
    sourceInfo,
    selection: sourceInfo.data?.selectedPath || null,
  };
}

function formatPolyNumber(value) {
  const num = Number(value);
  if (!Number.isFinite(num)) return String(value);
  if (Math.abs(num) < 1e-9) return '0';
  if (Math.abs(num - Math.round(num)) < 1e-9) return String(Math.round(num));
  return num.toFixed(3).replace(/\.?0+$/, '');
}

function formatPolyConstraint(row, rhs, symbols, isEquality = false) {
  const terms = [];
  row.forEach((coeff, idx) => {
    const num = Number(coeff);
    if (!Number.isFinite(num) || Math.abs(num) < 1e-9) return;
    const absCoeff = Math.abs(num);
    const coeffStr = Math.abs(absCoeff - 1) < 1e-9 ? '' : `${formatPolyNumber(absCoeff)}*`;
    const sign = num < 0 ? '-' : (terms.length ? '+' : '');
    terms.push(`${sign}${coeffStr}${symbols[idx]}`);
  });
  const lhs = terms.length ? terms.join(' ') : '0';
  return `${lhs} ${isEquality ? '=' : '≤'} ${formatPolyNumber(rhs)}`;
}

function renderPolyCoordinateTable(rows, symbols, kind, linealitySet = new Set()) {
  if (!rows || !rows.length) return '';
  const header = symbols.map(sym => `<th>${sym}</th>`).join('');
  const label = kind === 'rays' ? 'R' : 'V';
  const extraHeader = kind === 'rays' ? '<th>Type</th>' : '';
  const body = rows.map((row, idx) => {
    const cells = row.map(value => `<td class="siso-profile-cell">${formatPolyNumber(value)}</td>`).join('');
    const extraCell = kind === 'rays'
      ? `<td>${linealitySet.has(idx + 1) ? '<span class="tag tag-nonasym">lineality</span>' : '<span class="tag tag-asym">ray</span>'}</td>`
      : '';
    return `<tr><td>${label}${idx + 1}</td>${cells}${extraCell}</tr>`;
  }).join('');
  return `
    <div class="siso-table-wrap scroll-panel">
      <table class="siso-family-table">
        <thead><tr><th>#</th>${header}${extraHeader}</tr></thead>
        <tbody>${body}</tbody>
      </table>
    </div>
  `;
}

function convexHull2D(points) {
  if (points.length <= 1) return points.slice();
  const sorted = points
    .map((point, index) => ({ ...point, index }))
    .sort((a, b) => a.x - b.x || a.y - b.y || a.index - b.index);
  const cross = (o, a, b) => (a.x - o.x) * (b.y - o.y) - (a.y - o.y) * (b.x - o.x);
  const lower = [];
  sorted.forEach(point => {
    while (lower.length >= 2 && cross(lower[lower.length - 2], lower[lower.length - 1], point) <= 0) lower.pop();
    lower.push(point);
  });
  const upper = [];
  for (let i = sorted.length - 1; i >= 0; i--) {
    const point = sorted[i];
    while (upper.length >= 2 && cross(upper[upper.length - 2], upper[upper.length - 1], point) <= 0) upper.pop();
    upper.push(point);
  }
  lower.pop();
  upper.pop();
  return lower.concat(upper);
}

function plotQKPolyhedron(polyData, qkSymbols, plotId) {
  if (!polyData || polyData.dimension !== 2 || !polyData.is_bounded || !(polyData.vertices || []).length) return;

  const plotTheme = getPlotTheme();
  const points = polyData.vertices.map(vertex => ({ x: Number(vertex[0]), y: Number(vertex[1]) }))
    .filter(point => Number.isFinite(point.x) && Number.isFinite(point.y));
  if (!points.length) return;

  const hull = convexHull2D(points);
  const traces = [];
  if (hull.length >= 2) {
    const closed = hull.concat(hull[0]);
    traces.push({
      x: closed.map(point => point.x),
      y: closed.map(point => point.y),
      mode: 'lines',
      type: 'scatter',
      fill: 'toself',
      fillcolor: 'rgba(108, 140, 255, 0.14)',
      line: { color: '#6c8cff', width: 2 },
      name: 'Boundary',
      hoverinfo: 'skip',
    });
  }
  traces.push({
    x: points.map(point => point.x),
    y: points.map(point => point.y),
    mode: 'markers',
    type: 'scatter',
    marker: { color: '#ff922b', size: 8 },
    name: 'Vertices',
    hovertemplate: `${qkSymbols[0]}=%{x:.3f}<br>${qkSymbols[1]}=%{y:.3f}<extra></extra>`,
  });

  const layout = {
    autosize: true,
    margin: { t: 40, b: 60, l: 70, r: 20 },
    title: { text: 'qK-space Polyhedron', font: { color: plotTheme.titleColor, size: 11 }, y: 0.98, yanchor: 'top' },
    xaxis: { title: qkSymbols[0] },
    yaxis: { title: qkSymbols[1] },
    showlegend: true,
  };

  Plotly.newPlot(plotId, traces, applyPlotLayoutTheme(layout), { responsive: true, displayModeBar: false, scrollZoom: true });
}

function renderQKPolyhedronResult(nodeId, selection, payload) {
  const poly = payload.polyhedra?.[0];
  if (!poly) {
    return { html: '<div class="node-error">No polyhedron data returned for the selected path.</div>', canPlot: false };
  }

  const qkSymbols = payload.qk_symbols || [];
  const linearConstraints = new Set(poly.linear_constraints || []);
  const rayLineality = new Set(poly.ray_lineality || []);
  const vertices = poly.vertices || [];
  const rays = poly.rays || [];
  const canPlot = poly.dimension === 2 && poly.is_bounded && vertices.length > 0;

  const constraintRows = (poly.A || []).map((row, idx) => `
    <tr>
      <td>C${idx + 1}</td>
      <td class="siso-profile-cell">${formatPolyConstraint(row, poly.b?.[idx], qkSymbols, linearConstraints.has(idx + 1))}</td>
    </tr>
  `).join('');

  let html = `
    <div class="siso-summary-line">
      <span class="summary-chip"><strong>Path #${selection.path_idx}</strong></span>
      ${selection.exact_family_idx ? `<span class="family-badge family-badge-exact">E${selection.exact_family_idx}</span>` : ''}
      <span class="summary-chip">${selection.observe_x}</span>
      <span class="summary-chip">${payload.change_qK} scanned</span>
    </div>
    <div class="siso-scope-note">
      Fixed coordinates: ${qkSymbols.length ? qkSymbols.join(', ') : 'n/a'}<br>
      Dimension ${poly.dimension}, constraints ${poly.n_constraints ?? (poly.A || []).length}, vertices ${poly.n_vertices ?? vertices.length}, rays ${poly.n_rays ?? rays.length}.
    </div>
  `;

  if (!canPlot) {
    html += `
      <div class="text-dim">
        Direct geometric plotting is only shown for bounded 2D path polyhedra. This selected path is ${poly.dimension}D${poly.is_bounded ? '' : ' and unbounded'}.
      </div>
    `;
  } else {
    html += `<div class="plot-container" id="${nodeId}-plot"></div>`;
  }

  html += `
    <section class="siso-section">
      <div class="siso-section-head">
        <div class="siso-section-title">H-Representation</div>
        <div class="text-dim">${(poly.A || []).length} rows</div>
      </div>
      <div class="siso-table-wrap scroll-panel">
        <table class="siso-family-table">
          <thead><tr><th>#</th><th>Constraint</th></tr></thead>
          <tbody>${constraintRows || '<tr><td colspan="2" class="text-dim">No constraints</td></tr>'}</tbody>
        </table>
      </div>
    </section>
  `;

  if (vertices.length) {
    html += `
      <section class="siso-section">
        <div class="siso-section-head">
          <div class="siso-section-title">Vertices</div>
          <div class="text-dim">${vertices.length} points</div>
        </div>
        ${renderPolyCoordinateTable(vertices, qkSymbols, 'vertices')}
      </section>
    `;
  }

  if (rays.length) {
    html += `
      <section class="siso-section">
        <div class="siso-section-head">
          <div class="siso-section-title">Rays</div>
          <div class="text-dim">${rays.length} directions</div>
        </div>
        ${renderPolyCoordinateTable(rays, qkSymbols, 'rays', rayLineality)}
      </section>
    `;
  }

  return { html, canPlot };
}

async function executeQKPolyResult(nodeId) {
  const contentEl = document.getElementById(`${nodeId}-content`);
  const source = getConnectedSISOSelection(nodeId);
  if (!source) {
    if (contentEl) contentEl.innerHTML = '<span class="text-dim">Connect to a SISO Behaviors node first.</span>';
    return;
  }

  const selection = source.selection;
  if (!selection) {
    if (contentEl) contentEl.innerHTML = '<span class="text-dim">Select a path in the upstream SISO Behaviors node.</span>';
    return;
  }

  setNodeLoading(nodeId, true);
  try {
    const sessionId = getSessionIdForNode(nodeId);
    if (!sessionId) throw new Error('Build the connected model first');
    const payload = await api('siso_polyhedra', {
      session_id: sessionId,
      change_qK: selection.change_qK,
      path_indices: [selection.path_idx],
    });
    if (nodeRegistry[nodeId]) {
      nodeRegistry[nodeId].data = nodeRegistry[nodeId].data || {};
      nodeRegistry[nodeId].data.selection = selection;
      nodeRegistry[nodeId].data.polyhedronPayload = payload;
    }
    const rendered = renderQKPolyhedronResult(nodeId, selection, payload);
    contentEl.innerHTML = rendered.html;
    commitWorkspaceSnapshot('qk-polyhedron');
    if (rendered.canPlot) {
      setTimeout(() => {
        plotQKPolyhedron(payload.polyhedra?.[0], payload.qk_symbols || [], `${nodeId}-plot`);
        setupPlotResize(nodeId, `${nodeId}-plot`);
      }, 50);
    }
  } catch (e) {
    contentEl.innerHTML = `<div class="node-error">${e.message}</div>`;
  } finally {
    setNodeLoading(nodeId, false);
  }
}

// ===== Plotly Renderers =====

function getRegimeGraphNodeColor(node) {
  if (node.singular) return '#ff6b6b';
  if (!node.asymptotic) return '#ffd43b';
  return '#51cf66';
}

function parseRegimeGraphNodeSpecies(label) {
  if (!label) return [];
  return String(label)
    .replace(/^\s*\[/, '')
    .replace(/\]\s*$/, '')
    .split(',')
    .map(token => token.trim().replace(/^:+/, ''))
    .filter(Boolean);
}

function formatRegimeGraphPermMapping(node) {
  const species = parseRegimeGraphNodeSpecies(node.label);
  if (!Array.isArray(node.perm) || !node.perm.length || species.length !== node.perm.length) {
    return node.label || 'n/a';
  }
  return node.perm.map((idx, i) => `${idx}→${species[i]}`).join(', ');
}

function getRegimeGraph3DMarkerSize(baseSize, nodeCount) {
  const n = Math.max(1, Number(nodeCount) || 1);
  const countFactor = Math.max(0.95, Math.min(2.05, 1.9 - 0.016 * (n - 1)));
  const size = (baseSize * 0.58 + 5.5) * countFactor;
  return Math.max(13, Math.min(34, size));
}

function getRegimeGraph3DTextSize(nodeCount) {
  const n = Math.max(1, Number(nodeCount) || 1);
  return Math.max(10, Math.min(15, 15 - 0.11 * (n - 1)));
}

function getRegimeGraph3DCamera(nodeCount) {
  const n = Math.max(1, Number(nodeCount) || 1);
  const eyeScale = Math.max(0.88, Math.min(1.18, 0.92 + 0.012 * (n - 1)));
  return {
    center: { x: 0, y: 0, z: 0 },
    eye: { x: eyeScale * 1.04, y: eyeScale * 1.06, z: eyeScale * 0.8 },
    up: { x: 0, y: 0, z: 1 },
  };
}

function buildCircularGraphPositions(nodes, dimensions = 2) {
  const positions = {};
  const n = Math.max(nodes.length, 1);
  const radius = 2 + Math.sqrt(n) * 0.5;
  nodes.forEach((node, idx) => {
    const angle = (2 * Math.PI * idx) / n;
    const base = {
      x: radius * Math.cos(angle),
      y: radius * Math.sin(angle),
      z: 0,
    };
    if (dimensions === 3) {
      base.z = 1.1 * Math.sin(angle * 1.7 + idx * 0.4) + ((idx % 5) - 2) * 0.35;
    }
    positions[node.id] = base;
  });
  return positions;
}

function relaxGraphLayout(nodes, edges, positions, dimensions = 2, iterations = 180) {
  const hasZ = dimensions === 3;
  for (let iter = 0; iter < iterations; iter++) {
    const forces = {};
    nodes.forEach(node => {
      forces[node.id] = { x: 0, y: 0, z: 0 };
    });

    for (let i = 0; i < nodes.length; i++) {
      for (let j = i + 1; j < nodes.length; j++) {
        const a = nodes[i].id;
        const b = nodes[j].id;
        let dx = positions[a].x - positions[b].x;
        let dy = positions[a].y - positions[b].y;
        let dz = hasZ ? positions[a].z - positions[b].z : 0;
        let dist = Math.sqrt(dx * dx + dy * dy + dz * dz) + 0.02;
        let repulsion = (hasZ ? 3.4 : 3.0) / (dist * dist);
        forces[a].x += repulsion * dx / dist;
        forces[a].y += repulsion * dy / dist;
        forces[b].x -= repulsion * dx / dist;
        forces[b].y -= repulsion * dy / dist;
        if (hasZ) {
          forces[a].z += repulsion * dz / dist;
          forces[b].z -= repulsion * dz / dist;
        }
      }
    }

    edges.forEach(edge => {
      const source = positions[edge.source];
      const target = positions[edge.target];
      if (!source || !target) return;
      let dx = target.x - source.x;
      let dy = target.y - source.y;
      let dz = hasZ ? target.z - source.z : 0;
      let dist = Math.sqrt(dx * dx + dy * dy + dz * dz) + 0.02;
      let attraction = dist * (hasZ ? 0.08 : 0.1);
      forces[edge.source].x += attraction * dx / dist;
      forces[edge.source].y += attraction * dy / dist;
      forces[edge.target].x -= attraction * dx / dist;
      forces[edge.target].y -= attraction * dy / dist;
      if (hasZ) {
        forces[edge.source].z += attraction * dz / dist;
        forces[edge.target].z -= attraction * dz / dist;
      }
    });

    nodes.forEach(node => {
      const centerPull = hasZ ? 0.02 : 0.015;
      forces[node.id].x -= positions[node.id].x * centerPull;
      forces[node.id].y -= positions[node.id].y * centerPull;
      if (hasZ) forces[node.id].z -= positions[node.id].z * centerPull;
    });

    const cooling = (hasZ ? 0.18 : 0.16) * (1 - 0.65 * (iter / Math.max(iterations, 1)));
    nodes.forEach(node => {
      positions[node.id].x += forces[node.id].x * cooling;
      positions[node.id].y += forces[node.id].y * cooling;
      if (hasZ) positions[node.id].z += forces[node.id].z * cooling;
    });
  }
}

function normalizeGraphLayout(nodes, positions, dimensions = 2) {
  if (!nodes.length) return;
  const axes = dimensions === 3 ? ['x', 'y', 'z'] : ['x', 'y'];
  axes.forEach(axis => {
    const values = nodes.map(node => Number(positions[node.id]?.[axis]) || 0);
    const mean = values.reduce((sum, value) => sum + value, 0) / Math.max(values.length, 1);
    nodes.forEach(node => {
      positions[node.id][axis] -= mean;
    });
  });

  let maxAbs = 0;
  nodes.forEach(node => {
    maxAbs = Math.max(maxAbs, Math.abs(positions[node.id].x), Math.abs(positions[node.id].y));
    if (dimensions === 3) maxAbs = Math.max(maxAbs, Math.abs(positions[node.id].z));
  });
  const scale = maxAbs > 0 ? 5.2 / maxAbs : 1;
  nodes.forEach(node => {
    positions[node.id].x *= scale;
    positions[node.id].y *= scale;
    if (dimensions === 3) positions[node.id].z *= scale;
  });
}

function computeGraphLayout(nodes, edges, viewMode = '2d') {
  const dimensions = viewMode === '3d' ? 3 : 2;
  const hasBackendPositions = nodes.every(node => Number.isFinite(node.x) && Number.isFinite(node.y));
  const positions = hasBackendPositions ? {} : buildCircularGraphPositions(nodes, dimensions);

  if (hasBackendPositions) {
    nodes.forEach((node, idx) => {
      positions[node.id] = {
        x: Number(node.x),
        y: Number(node.y),
        z: dimensions === 3
          ? ((idx % 5) - 2) * 0.45 + (node.singular ? 0.55 : 0) + (!node.asymptotic ? -0.25 : 0.15)
          : 0,
      };
    });
  }

  if (dimensions === 3 || !hasBackendPositions) {
    relaxGraphLayout(nodes, edges, positions, dimensions, dimensions === 3 ? 220 : 200);
  }
  normalizeGraphLayout(nodes, positions, dimensions);
  return positions;
}

function plotRegimeGraph(data, plotId, options = {}) {
  const { nodes = [], edges = [], graph_label, change_qK } = data;
  const viewMode = options.viewMode === '2d' ? '2d' : '3d';
  const is3D = viewMode === '3d';
  const plotTheme = getPlotTheme();
  const positions = computeGraphLayout(nodes, edges, viewMode);

  // Node size scaling: use volume-based sizes from backend with improved scaling
  const rawSizes = nodes.map(node => Number(node.size)).filter(size => Number.isFinite(size));
  const minRawSize = rawSizes.length ? Math.min(...rawSizes) : 44;
  const maxRawSize = rawSizes.length ? Math.max(...rawSizes) : 44;
  const rawSpan = Math.max(maxRawSize - minRawSize, 1e-9);
  const scaledNodeSize = new Map();
  nodes.forEach(node => {
    const raw = Number(node.size);
    if (!Number.isFinite(raw)) {
      scaledNodeSize.set(node.id, 30);
      return;
    }
    // Use logarithmic scaling for better visual distribution when volume varies greatly
    const normalized = rawSpan < 1e-6 ? 0.5 : (raw - minRawSize) / rawSpan;
    const logScaled = rawSpan > 100
      ? Math.log10(1 + normalized * 9) // log scale for large ranges
      : normalized; // linear scale for small ranges
    const scaled = 18 + 40 * logScaled;
    scaledNodeSize.set(node.id, Math.max(12, Math.min(60, scaled)));
  });

  const traces = [];
  const nodeById = new Map(nodes.map(node => [node.id, node]));
  const arrowAnnotations = [];
  const edgeLabelAnnotations = [];
  const edgeMidX = [];
  const edgeMidY = [];
  const edgeMidZ = [];
  const edgeHoverText = [];
  const labeledEdgeMidX = [];
  const labeledEdgeMidY = [];
  const labeledEdgeMidZ = [];
  const labeledEdgeText = [];
  const coneX = [];
  const coneY = [];
  const coneZ = [];
  const coneU = [];
  const coneV = [];
  const coneW = [];

  // Pre-compute edge distances for adaptive arrow sizing
  const edgeDistances = edges.map(edge => {
    const sourcePos = positions[edge.source];
    const targetPos = positions[edge.target];
    if (!sourcePos || !targetPos) return 0;
    const dx = targetPos.x - sourcePos.x;
    const dy = targetPos.y - sourcePos.y;
    const dz = is3D ? targetPos.z - sourcePos.z : 0;
    return Math.sqrt(dx * dx + dy * dy + dz * dz);
  }).filter(d => d > 0);

  const avgDist = edgeDistances.length > 0
    ? edgeDistances.reduce((a, b) => a + b, 0) / edgeDistances.length
    : 1.0;
  const minDist = edgeDistances.length > 0 ? Math.min(...edgeDistances) : 0.5;
  const maxDist = edgeDistances.length > 0 ? Math.max(...edgeDistances) : 2.0;

  edges.forEach((edge, edgeIdx) => {
    const sourceNode = nodeById.get(edge.source);
    const targetNode = nodeById.get(edge.target);
    const sourcePos = positions[edge.source];
    const targetPos = positions[edge.target];
    if (!sourceNode || !targetNode || !sourcePos || !targetPos) return;

    const dx = targetPos.x - sourcePos.x;
    const dy = targetPos.y - sourcePos.y;
    const dz = is3D ? targetPos.z - sourcePos.z : 0;
    const dist = Math.sqrt(dx * dx + dy * dy + dz * dz) + 1e-9;
    const ux = dx / dist;
    const uy = dy / dist;
    const uz = is3D ? dz / dist : 0;
    const sourcePad = (scaledNodeSize.get(edge.source) || 30) * 0.016;
    const targetPad = (scaledNodeSize.get(edge.target) || 30) * 0.02;
    const x0 = sourcePos.x + ux * sourcePad;
    const y0 = sourcePos.y + uy * sourcePad;
    const z0 = sourcePos.z + uz * sourcePad;
    const x1 = targetPos.x - ux * targetPad;
    const y1 = targetPos.y - uy * targetPad;
    const z1 = targetPos.z - uz * targetPad;
    const directionText = (edge.label || '').trim();
    const edgeInfoText = `Edge: #${edge.source} → #${edge.target}<br>Direction: ${directionText || 'n/a'}<br>From: [${sourceNode.perm.join(',')}]<br>To: [${targetNode.perm.join(',')}]`;

    if (is3D) {
      traces.push({
        x: [x0, x1],
        y: [y0, y1],
        z: [z0, z1],
        mode: 'lines',
        type: 'scatter3d',
        line: { color: plotTheme.edgeLine3DColor, width: 7 },
        hoverinfo: 'skip',
        showlegend: false,
      });

      // Adaptive arrow length based on edge distance distribution
      const relDist = (dist - minDist) / Math.max(maxDist - minDist, 1e-9);
      const baseArrowLength = 0.08 + 0.12 * Math.sqrt(relDist);
      const arrowLength = Math.min(dist * 0.35, Math.max(0.06, baseArrowLength));
      coneX.push(x1);
      coneY.push(y1);
      coneZ.push(z1);
      coneU.push(ux * arrowLength);
      coneV.push(uy * arrowLength);
      coneW.push(uz * arrowLength);
    } else {
      traces.push({
        x: [x0, x1],
        y: [y0, y1],
        mode: 'lines',
        type: 'scatter',
        line: { color: plotTheme.edgeLineColor, width: 1.5 },
        hoverinfo: 'skip',
        showlegend: false,
      });

      // Adaptive arrow length for 2D based on edge distance distribution
      const relDist = (dist - minDist) / Math.max(maxDist - minDist, 1e-9);
      const baseArrowLength = 0.06 + 0.10 * Math.sqrt(relDist);
      const arrowLength = Math.min(dist * 0.30, Math.max(0.04, baseArrowLength));
      arrowAnnotations.push({
        x: x1,
        y: y1,
        ax: x1 - ux * arrowLength,
        ay: y1 - uy * arrowLength,
        xref: 'x',
        yref: 'y',
        axref: 'x',
        ayref: 'y',
        showarrow: true,
        text: '',
        arrowhead: 2,
        arrowsize: 1.1,
        arrowwidth: 1.4,
        arrowcolor: plotTheme.edgeArrowColor,
        opacity: 0.95,
      });
    }

    const midX = (x0 + x1) / 2;
    const midY = (y0 + y1) / 2;
    const midZ = (z0 + z1) / 2;
    edgeMidX.push(midX);
    edgeMidY.push(midY);
    if (is3D) edgeMidZ.push(midZ);
    edgeHoverText.push(edgeInfoText);

    if (directionText) {
      labeledEdgeMidX.push(midX);
      labeledEdgeMidY.push(midY);
      if (is3D) labeledEdgeMidZ.push(midZ);
      labeledEdgeText.push(directionText);
    }

    if (!is3D && directionText) {
      const normalX = -uy;
      const normalY = ux;
      const offsetMag = 0.18 + (edgeIdx % 3) * 0.06;
      edgeLabelAnnotations.push({
        x: midX + normalX * offsetMag,
        y: midY + normalY * offsetMag,
        xref: 'x',
        yref: 'y',
        text: directionText,
        showarrow: false,
        font: { size: 10, color: plotTheme.edgeLabelColor },
        bgcolor: plotTheme.annotationBg,
        bordercolor: plotTheme.annotationBorderColor,
        borderwidth: 1,
        borderpad: 2,
        opacity: 0.98,
      });
    }
  });

  if (edges.length) {
    if (is3D) {
      traces.push({
        type: 'cone',
        x: coneX,
        y: coneY,
        z: coneZ,
        u: coneU,
        v: coneV,
        w: coneW,
        anchor: 'tip',
        sizemode: 'absolute',
        sizeref: 0.18,
        colorscale: [[0, plotTheme.edgeConeColor], [1, plotTheme.edgeConeColor]],
        showscale: false,
        hoverinfo: 'skip',
      });
      if (labeledEdgeText.length) {
        traces.push({
          x: labeledEdgeMidX,
          y: labeledEdgeMidY,
          z: labeledEdgeMidZ,
          mode: 'text',
          type: 'scatter3d',
          text: labeledEdgeText,
          textposition: 'top center',
          textfont: { size: getRegimeGraph3DTextSize(nodes.length), color: plotTheme.edgeLabelColor },
          hoverinfo: 'skip',
          showlegend: false,
        });
      }
      traces.push({
        x: edgeMidX,
        y: edgeMidY,
        z: edgeMidZ,
        mode: 'markers',
        type: 'scatter3d',
        marker: { size: 7, color: plotTheme.edgeHoverMarkerColor, line: { width: 0 } },
        hovertext: edgeHoverText,
        hoverinfo: 'text',
        showlegend: false,
      });
    } else {
      traces.push({
        x: edgeMidX,
        y: edgeMidY,
        mode: 'markers',
        type: 'scatter',
        marker: { size: 10, color: plotTheme.edgeHoverMarkerColor, line: { width: 0 } },
        hovertext: edgeHoverText,
        hoverinfo: 'text',
        showlegend: false,
      });
    }
  }

  const nodeColors = nodes.map(getRegimeGraphNodeColor);
  const nodeHoverText = nodes.map(node => {
    const volumeStr = node.volume !== null && node.volume !== undefined
      ? `volume=${Number(node.volume).toExponential(3)}`
      : 'volume=n/a';
    return `#${node.id}<br>Dominant species: ${node.label || 'n/a'}<br>Perm mapping: ${formatRegimeGraphPermMapping(node)}<br>${node.asymptotic ? 'Asymptotic' : 'Non-Asymp'} ${node.singular ? 'Singular' : 'Invertible'}<br>nullity=${node.nullity}<br>${volumeStr}`;
  });

  if (is3D) {
    traces.push({
      x: nodes.map(node => positions[node.id].x),
      y: nodes.map(node => positions[node.id].y),
      z: nodes.map(node => positions[node.id].z),
      mode: 'markers+text',
      type: 'scatter3d',
      marker: {
        size: nodes.map(node => getRegimeGraph3DMarkerSize(scaledNodeSize.get(node.id) || 30, nodes.length)),
        color: nodeColors,
        line: { width: 1.2, color: plotTheme.nodeOutlineColor },
      },
      text: nodes.map(node => `#${node.id}`),
      textposition: 'middle center',
      textfont: { size: getRegimeGraph3DTextSize(nodes.length), color: plotTheme.nodeTextColor },
      hovertext: nodeHoverText,
      hoverinfo: 'text',
      showlegend: false,
    });
  } else {
    traces.push({
      x: nodes.map(node => positions[node.id].x),
      y: nodes.map(node => positions[node.id].y),
      mode: 'markers+text',
      type: 'scatter',
      marker: {
        size: nodes.map(node => scaledNodeSize.get(node.id) || 30),
        color: nodeColors,
        line: { width: 1.2, color: plotTheme.nodeOutlineColor },
      },
      text: nodes.map(node => `#${node.id}`),
      textposition: 'middle center',
      textfont: { size: 9, color: plotTheme.nodeTextColor },
      hovertext: nodeHoverText,
      hoverinfo: 'text',
      showlegend: false,
    });

    traces.push({
      x: nodes.map(node => positions[node.id].x),
      y: nodes.map(node => positions[node.id].y - (scaledNodeSize.get(node.id) || 30) * 0.02 - 0.14),
      text: nodes.map(node => node.label || ''),
      mode: 'text',
      type: 'scatter',
      textposition: 'middle center',
      textfont: { size: 9, color: plotTheme.subtleTextColor },
      hoverinfo: 'skip',
      showlegend: false,
    });
  }

  const titleText = `${graph_label || 'Regime Graph'}${change_qK ? ` (${change_qK})` : ''}<br><span style="font-size:11px;color:${plotTheme.subtleTextColor};">${viewMode.toUpperCase()} view · ${nodes.length} vertices, ${edges.length} edges</span>`;
  const layout = {
    autosize: true,
    showlegend: false,
    margin: { t: 40, b: 20, l: 20, r: 20 },
    dragmode: is3D ? 'orbit' : 'pan',
    title: {
      text: titleText,
      font: { color: plotTheme.titleColor, size: 13 },
      y: 0.98,
      yanchor: 'top',
    },
  };

  if (is3D) {
    layout.scene = {
      xaxis: { visible: false, showbackground: false, showgrid: false, zeroline: false },
      yaxis: { visible: false, showbackground: false, showgrid: false, zeroline: false },
      zaxis: { visible: false, showbackground: false, showgrid: false, zeroline: false },
      bgcolor: plotTheme.sceneBg,
      aspectmode: 'data',
      camera: getRegimeGraph3DCamera(nodes.length),
    };
  } else {
    layout.xaxis = { visible: false };
    layout.yaxis = { visible: false, scaleanchor: 'x' };
    layout.annotations = arrowAnnotations.concat(edgeLabelAnnotations);
  }

  Plotly.newPlot(plotId, traces, applyPlotLayoutTheme(layout), { responsive: true, displayModeBar: false, scrollZoom: true });
}

function plotTrajectory(data, plotId) {
  const { change_values, logx, regimes, x_sym, change_sym } = data;
  const nSpecies = x_sym.length;
  const nPoints = change_values.length;
  const plotTheme = getPlotTheme();

  const uniqueRegimes = [...new Set(regimes)];
  const palette = ['#6c8cff', '#51cf66', '#ff6b6b', '#ffd43b', '#4ecdc4', '#e599f7', '#ff922b', '#74c0fc', '#f06595', '#a9e34b'];
  const regimeColor = {};
  uniqueRegimes.forEach((r, i) => { regimeColor[r] = palette[i % palette.length]; });

  const speciesPalette = ['#7da2ff', '#4fd67a', '#ffd24d', '#ff7a7a', '#63d4d6', '#d48cff', '#ffb347', '#86caff', '#ff82b2', '#b8f06b'];
  const regimeSegments = [];
  let segStart = 0;
  for (let i = 1; i <= nPoints; i++) {
    if (i === nPoints || regimes[i] !== regimes[segStart]) {
      regimeSegments.push({
        regime: regimes[segStart],
        startIdx: segStart,
        endIdx: i - 1,
        x0: change_values[segStart],
        x1: change_values[i - 1],
      });
      segStart = i;
    }
  }

  const traces = [];
  for (let s = 0; s < nSpecies; s++) {
    const traceX = [];
    const traceY = [];
    const traceRegimes = [];
    regimeSegments.forEach((segment, idx) => {
      for (let i = segment.startIdx; i <= segment.endIdx; i++) {
        traceX.push(change_values[i]);
        traceY.push(logx[i][s]);
        traceRegimes.push(segment.regime);
      }
      if (idx < regimeSegments.length - 1) {
        traceX.push(null);
        traceY.push(null);
        traceRegimes.push(null);
      }
    });

    traces.push({
      x: traceX,
      y: traceY,
      customdata: traceRegimes,
      mode: 'lines',
      type: 'scatter',
      line: { color: speciesPalette[s % speciesPalette.length], width: 2.5 },
      name: x_sym[s],
      hovertemplate: `${x_sym[s]}<br>log ${change_sym}=%{x:.3g}<br>log(x)=%{y:.3g}<br>rgm %{customdata}<extra></extra>`,
    });
  }

  const totalRange = Math.max((change_values[nPoints - 1] ?? 0) - (change_values[0] ?? 0), 1e-9);
  const shapes = regimeSegments.map(segment => ({
    type: 'rect',
    xref: 'x',
    yref: 'paper',
    x0: segment.x0,
    x1: segment.x1,
    y0: 0,
    y1: 1,
    fillcolor: hexToRgba(regimeColor[segment.regime], 0.14),
    line: { width: 0 },
    layer: 'below',
  }));
  const annotations = regimeSegments.map(segment => {
    const widthRatio = Math.abs(segment.x1 - segment.x0) / totalRange;
    return {
      x: (segment.x0 + segment.x1) / 2,
      y: 0.985,
      xref: 'x',
      yref: 'paper',
      text: `rgm ${segment.regime}`,
      showarrow: false,
      textangle: widthRatio < 0.06 ? -90 : 0,
      font: { size: 10, color: regimeColor[segment.regime] },
      bgcolor: plotTheme.annotationBg,
      bordercolor: hexToRgba(regimeColor[segment.regime], 0.4),
      borderwidth: 1,
      borderpad: 2,
      opacity: widthRatio < 0.025 ? 0.75 : 1,
    };
  });

  const layout = {
    showlegend: true,
    margin: { t: 40, b: 60, l: 70, r: 20 },
    title: { text: `Changing ${change_sym}`, font: { color: plotTheme.titleColor, size: 11 }, y: 0.98, yanchor: 'top' },
    xaxis: { title: `log ${change_sym}` },
    yaxis: { title: 'log(x)' },
    legend: { font: { color: plotTheme.fontColor, size: 9 } },
    shapes,
    annotations,
  };

  Plotly.newPlot(plotId, traces, applyPlotLayoutTheme(layout), { responsive: true, displayModeBar: false, scrollZoom: true });
  const plotEl = document.getElementById(plotId);
  if (plotEl) setupPlotInteractionGuard(plotEl);
}

function getROPCloudPlotAxes(data) {
  const { reaction_orders = [], q_sym = [], d = 0 } = data;
  const plottedDims = d === 3 ? 3 : 2;
  const labels = [];
  for (let i = 0; i < plottedDims; i++) {
    labels.push(`\u2202log/\u2202log ${q_sym[i] || `q${i + 1}`}`);
  }
  return {
    plottedDims,
    labels,
    x: reaction_orders.map(row => row[0]).filter(Number.isFinite),
    y: reaction_orders.map(row => row[1]).filter(Number.isFinite),
    z: plottedDims === 3 ? reaction_orders.map(row => row[2]).filter(Number.isFinite) : [],
  };
}

function quantileSorted(sorted, q) {
  if (!sorted.length) return null;
  const pos = Math.max(0, Math.min(sorted.length - 1, (sorted.length - 1) * q));
  const lower = Math.floor(pos);
  const upper = Math.ceil(pos);
  if (lower === upper) return sorted[lower];
  const weight = pos - lower;
  return sorted[lower] * (1 - weight) + sorted[upper] * weight;
}

function computeROPCloudAxisRange(values, preset = 'robust') {
  const finite = (values || []).map(Number).filter(Number.isFinite).sort((a, b) => a - b);
  if (!finite.length) return [null, null];
  const minValue = finite[0];
  const maxValue = finite[finite.length - 1];

  let lo = minValue;
  let hi = maxValue;
  if (preset === 'robust' && finite.length > 4) {
    lo = quantileSorted(finite, 0.01);
    hi = quantileSorted(finite, 0.99);
  }

  if (!(Number.isFinite(lo) && Number.isFinite(hi))) return [null, null];
  if (Math.abs(hi - lo) < 1e-9) {
    const pad = Math.max(1, Math.abs(lo) * 0.08);
    return [lo - pad, hi + pad];
  }
  const pad = (hi - lo) * 0.06;
  return [lo - pad, hi + pad];
}

function getROPCloudPresetRanges(data, preset = 'robust') {
  const axes = getROPCloudPlotAxes(data);
  const ranges = [
    computeROPCloudAxisRange(axes.x, preset),
    computeROPCloudAxisRange(axes.y, preset),
  ];
  if (axes.plottedDims === 3) {
    ranges.push(computeROPCloudAxisRange(axes.z, preset));
  }
  return ranges;
}

function syncROPCloudFOVInputs(nodeId, ranges = []) {
  ranges.forEach((range, idx) => {
    const axis = idx + 1;
    const minEl = document.getElementById(`${nodeId}-fov-${axis}-min`);
    const maxEl = document.getElementById(`${nodeId}-fov-${axis}-max`);
    if (minEl && Number.isFinite(range?.[0])) minEl.value = range[0].toFixed(2);
    if (maxEl && Number.isFinite(range?.[1])) maxEl.value = range[1].toFixed(2);
  });
}

function readROPCloudFOVRanges(nodeId, plottedDims) {
  const ranges = [];
  for (let i = 1; i <= plottedDims; i++) {
    const minVal = parseFloat(document.getElementById(`${nodeId}-fov-${i}-min`)?.value ?? '');
    const maxVal = parseFloat(document.getElementById(`${nodeId}-fov-${i}-max`)?.value ?? '');
    if (Number.isFinite(minVal) && Number.isFinite(maxVal) && minVal < maxVal) {
      ranges.push([minVal, maxVal]);
    } else {
      ranges.push(null);
    }
  }
  return ranges;
}

function renderROPCloudOutput(nodeId, contentEl, data) {
  const axes = getROPCloudPlotAxes(data);
  const existingData = nodeRegistry[nodeId]?.data || {};
  const currentPreset = existingData.ropCloudPreset || 'robust';
  const presetRanges = getROPCloudPresetRanges(data, currentPreset);
  const savedRanges = Array.isArray(existingData.ropCloudRanges) ? existingData.ropCloudRanges : null;
  const initialRanges = savedRanges && savedRanges.length === axes.plottedDims ? savedRanges : presetRanges;

  nodeRegistry[nodeId].data = nodeRegistry[nodeId].data || {};
  nodeRegistry[nodeId].data.ropCloudData = data;
  nodeRegistry[nodeId].data.ropCloudPreset = currentPreset;
  nodeRegistry[nodeId].data.ropCloudRanges = initialRanges;

  const rangeRows = axes.labels.map((label, idx) => `
    <div class="cloud-fov-row">
      <span class="cloud-fov-axis">${label}</span>
      <input type="number" step="0.1" id="${nodeId}-fov-${idx + 1}-min" onchange="refreshROPCloudPlot('${nodeId}')">
      <span class="cloud-fov-sep">to</span>
      <input type="number" step="0.1" id="${nodeId}-fov-${idx + 1}-max" onchange="refreshROPCloudPlot('${nodeId}')">
    </div>
  `).join('');

  contentEl.innerHTML = `
    <div class="siso-summary-line">
      <button type="button" class="btn btn-small" onclick="applyROPCloudFOVPreset('${nodeId}', 'robust')">Robust</button>
      <button type="button" class="btn btn-small" onclick="applyROPCloudFOVPreset('${nodeId}', 'full')">Full</button>
      <span class="summary-chip">Field of view</span>
    </div>
    <div class="cloud-fov-panel">
      ${rangeRows}
    </div>
    <div class="plot-container" id="${nodeId}-plot"></div>
  `;

  syncROPCloudFOVInputs(nodeId, initialRanges);
  commitWorkspaceSnapshot('rop-cloud');
  setTimeout(() => {
    refreshROPCloudPlot(nodeId);
    setupPlotResize(nodeId, `${nodeId}-plot`);
  }, 50);
}

function applyROPCloudFOVPreset(nodeId, preset) {
  const data = nodeRegistry[nodeId]?.data?.ropCloudData;
  if (!data) return;
  const ranges = getROPCloudPresetRanges(data, preset);
  nodeRegistry[nodeId].data.ropCloudPreset = preset;
  nodeRegistry[nodeId].data.ropCloudRanges = ranges;
  syncROPCloudFOVInputs(nodeId, ranges);
  refreshROPCloudPlot(nodeId);
}

function refreshROPCloudPlot(nodeId) {
  const nodeData = nodeRegistry[nodeId]?.data;
  const data = nodeData?.ropCloudData;
  if (!data) return;
  const plottedDims = getROPCloudPlotAxes(data).plottedDims;
  const ranges = readROPCloudFOVRanges(nodeId, plottedDims);
  nodeData.ropCloudRanges = ranges;
  plotROPCloud(data, `${nodeId}-plot`, { ranges });
  commitWorkspaceSnapshot('rop-cloud-fov');
}

function plotROPCloud(data, plotId, options = {}) {
  const { reaction_orders, fret_values, q_sym, d } = data;
  const plotTheme = getPlotTheme();
  const baseLayout = {
    autosize: true,
    margin: { t: 40, b: 60, l: 70, r: 20 },
  };
  const ranges = Array.isArray(options.ranges) ? options.ranges : [];

  if (d === 2) {
    const x = reaction_orders.map(r => r[0]);
    const y = reaction_orders.map(r => r[1]);
    const traces = [{
      x, y, mode: 'markers', type: 'scatter',
      marker: {
        size: 2, color: fret_values.map(v => Math.log10(v + 1e-30)),
        colorscale: 'Viridis', showscale: true,
        colorbar: themedColorbar('log(FRET)'),
      },
    }];
    const layout = {
      ...baseLayout,
      title: { text: 'ROP Cloud (2D)', font: { color: plotTheme.titleColor, size: 11 }, y: 0.98, yanchor: 'top' },
      xaxis: { title: `\u2202log/\u2202log ${q_sym[0]}`, range: ranges[0] || undefined },
      yaxis: { title: `\u2202log/\u2202log ${q_sym[1]}`, range: ranges[1] || undefined },
    };
    Plotly.newPlot(plotId, traces, applyPlotLayoutTheme(layout), { responsive: true, displayModeBar: false, scrollZoom: true });
  } else if (d === 3) {
    const x = reaction_orders.map(r => r[0]);
    const y = reaction_orders.map(r => r[1]);
    const z = reaction_orders.map(r => r[2]);
    const traces = [{
      x, y, z, mode: 'markers', type: 'scatter3d',
      marker: {
        size: 2, color: fret_values.map(v => Math.log10(v + 1e-30)),
        colorscale: 'Viridis', showscale: true,
        colorbar: themedColorbar('log(FRET)'),
      },
    }];
    const layout = {
      ...baseLayout,
      title: { text: 'ROP Cloud (3D)', font: { color: plotTheme.titleColor, size: 11 }, y: 0.98, yanchor: 'top' },
      scene: {
        xaxis: { title: `\u2202log/\u2202log ${q_sym[0]}`, range: ranges[0] || undefined },
        yaxis: { title: `\u2202log/\u2202log ${q_sym[1]}`, range: ranges[1] || undefined },
        zaxis: { title: `\u2202log/\u2202log ${q_sym[2]}`, range: ranges[2] || undefined },
      },
    };
    Plotly.newPlot(plotId, traces, applyPlotLayoutTheme(layout), { responsive: true, displayModeBar: false, scrollZoom: true });
  } else {
    const x = reaction_orders.map(r => r[0]);
    const y = reaction_orders.map(r => r[1]);
    const traces = [{
      x, y, mode: 'markers', type: 'scatter',
      marker: { size: 2, color: '#6c8cff', opacity: 0.3 },
    }];
    const layout = {
      ...baseLayout,
      title: { text: `ROP Cloud (first 2 of ${d} dims)`, font: { color: plotTheme.titleColor, size: 11 }, y: 0.98, yanchor: 'top' },
      xaxis: { title: `\u2202log/\u2202log ${q_sym[0]}`, range: ranges[0] || undefined },
      yaxis: { title: `\u2202log/\u2202log ${q_sym[1]}`, range: ranges[1] || undefined },
    };
    Plotly.newPlot(plotId, traces, applyPlotLayoutTheme(layout), { responsive: true, displayModeBar: false, scrollZoom: true });
  }
}

function plotHeatmap(data, plotId) {
  const { logq1, logq2, fret, regime, bounds, q_sym } = data;
  const logFret = fret.map(row => row.map(v => Math.log10(v + 1e-30)));
  const plotTheme = getPlotTheme();

  const traces = [
    {
      z: logFret, x: logq1, y: logq2, type: 'heatmap', colorscale: 'Viridis',
      colorbar: themedColorbar('log(FRET)'),
    },
    {
      z: bounds, x: logq1, y: logq2, type: 'contour',
      contours: { start: 0.5, end: 0.5, size: 1, coloring: 'none' },
      line: { color: plotTheme.contourLineColor, width: 2 }, showscale: false,
    },
  ];

  const layout = {
    autosize: true,
    margin: { t: 40, b: 60, l: 70, r: 20 },
    title: { text: 'FRET + Regime Boundaries', font: { color: plotTheme.titleColor, size: 11 }, y: 0.98, yanchor: 'top' },
    xaxis: { title: `log(${q_sym[0]})` },
    yaxis: { title: `log(${q_sym[1]})` },
  };

  Plotly.newPlot(plotId, traces, applyPlotLayoutTheme(layout), { responsive: true, displayModeBar: false, scrollZoom: true });
}

// ===== Reset View =====
function resetView() {
  panX = 0; panY = 0; scale = 1.0;
  applyViewportTransform();
  updateConnections();
}

// ===== Save / Load Workspace =====

function getNodeSerialData(nodeId, type) {
  switch (type) {
    case 'reaction-network': {
      const { reactions, kds } = getReactionsFromNode(nodeId);
      return { reactions: reactions.map((rule, i) => ({ rule, kd: kds[i] })) };
    }
    case 'model-builder': {
      const info = nodeRegistry[nodeId]?.data || {};
      return {
        built: !!info.built,
        modelContext: cloneSerializable(info.modelContext),
      };
    }
    case 'regime-graph': {
      const graphMode = document.getElementById(`${nodeId}-graph-mode`)?.value || 'qk';
      const changeQK = document.getElementById(`${nodeId}-change-qk`)?.value || '';
      const viewMode = '3d';
      return {
        graphMode,
        changeQK,
        viewMode,
        config: nodeRegistry[nodeId]?.data?.config,
        graphData: cloneSerializable(nodeRegistry[nodeId]?.data?.graphData),
      };
    }
    case 'siso-params': {
      const changeQK = document.getElementById(`${nodeId}-siso-select`)?.value || '';
      const observeX = document.getElementById(`${nodeId}-target-x`)?.value || '';
      const pathScope = document.getElementById(`${nodeId}-path-scope`)?.value || 'feasible';
      const minVolumeMean = parseFloat(document.getElementById(`${nodeId}-min-volume`)?.value || '0');
      const keepSingular = document.getElementById(`${nodeId}-keep-singular`)?.checked ?? true;
      const keepNonasymptotic = document.getElementById(`${nodeId}-keep-nonasym`)?.checked ?? false;
      const min = parseFloat(document.getElementById(`${nodeId}-min`)?.value || '-6');
      const max = parseFloat(document.getElementById(`${nodeId}-max`)?.value || '6');
      return {
        changeQK,
        observeX,
        pathScope,
        minVolumeMean,
        keepSingular,
        keepNonasymptotic,
        min,
        max,
        config: nodeRegistry[nodeId]?.data?.config,
      };
    }
    case 'siso-result': {
      return {
        selectedPath: nodeRegistry[nodeId]?.data?.selectedPath || null,
        behaviorData: cloneSerializable(nodeRegistry[nodeId]?.data?.behaviorData),
        trajectoryData: cloneSerializable(nodeRegistry[nodeId]?.data?.trajectoryData),
      };
    }
    case 'qk-poly-result': {
      return {
        selection: cloneSerializable(nodeRegistry[nodeId]?.data?.selection),
        polyhedronPayload: cloneSerializable(nodeRegistry[nodeId]?.data?.polyhedronPayload),
      };
    }
    case 'rop-cloud': {
      const mode = document.getElementById(`${nodeId}-sampling-mode`)?.value || 'x_space';
      const samples = parseInt(document.getElementById(`${nodeId}-samples`)?.value || '10000');
      const span = parseInt(document.getElementById(`${nodeId}-span`)?.value || '6');
      const logxMin = parseFloat(document.getElementById(`${nodeId}-logx-min`)?.value || '-6');
      const logxMax = parseFloat(document.getElementById(`${nodeId}-logx-max`)?.value || '6');
      const targetSpecies = document.getElementById(`${nodeId}-target-species`)?.value || '';
      return {
        mode,
        samples,
        span,
        logxMin,
        logxMax,
        targetSpecies,
        ropCloudData: cloneSerializable(nodeRegistry[nodeId]?.data?.ropCloudData),
        ropCloudPreset: nodeRegistry[nodeId]?.data?.ropCloudPreset || null,
        ropCloudRanges: cloneSerializable(nodeRegistry[nodeId]?.data?.ropCloudRanges),
      };
    }
    case 'fret-heatmap': {
      const grid = parseInt(document.getElementById(`${nodeId}-grid`)?.value || '80');
      return {
        grid,
        fretHeatmapData: cloneSerializable(nodeRegistry[nodeId]?.data?.fretHeatmapData),
      };
    }
    case 'parameter-scan-1d': {
      const param = document.getElementById(`${nodeId}-param`)?.value || '';
      const min = parseFloat(document.getElementById(`${nodeId}-min`)?.value || '-6');
      const max = parseFloat(document.getElementById(`${nodeId}-max`)?.value || '6');
      const points = parseInt(document.getElementById(`${nodeId}-points`)?.value || '200');
      const expr = (document.getElementById(`${nodeId}-expr`)?.value || '').trim();
      return {
        param_symbol: param,
        param_min: min,
        param_max: max,
        n_points: points,
        output_exprs: expr ? [expr] : [],
        scan1DResult: cloneSerializable(nodeRegistry[nodeId]?.data?.scan1DResult),
      };
    }
    case 'parameter-scan-2d': {
      const param1 = document.getElementById(`${nodeId}-param1`)?.value || '';
      const param2 = document.getElementById(`${nodeId}-param2`)?.value || '';
      const min1 = parseFloat(document.getElementById(`${nodeId}-min1`)?.value || '-6');
      const max1 = parseFloat(document.getElementById(`${nodeId}-max1`)?.value || '6');
      const min2 = parseFloat(document.getElementById(`${nodeId}-min2`)?.value || '-6');
      const max2 = parseFloat(document.getElementById(`${nodeId}-max2`)?.value || '6');
      const grid = parseInt(document.getElementById(`${nodeId}-grid`)?.value || '80');
      const expr = (document.getElementById(`${nodeId}-expr`)?.value || '').trim();
      return {
        param1_symbol: param1,
        param2_symbol: param2,
        param1_min: min1,
        param1_max: max1,
        param2_min: min2,
        param2_max: max2,
        n_grid: grid,
        output_expr: expr,
        scan2DResult: cloneSerializable(nodeRegistry[nodeId]?.data?.scan2DResult),
      };
    }
    case 'scan-1d-params': {
      const param = document.getElementById(`${nodeId}-param`)?.value || '';
      const min = parseFloat(document.getElementById(`${nodeId}-min`)?.value || '-6');
      const max = parseFloat(document.getElementById(`${nodeId}-max`)?.value || '6');
      const points = parseInt(document.getElementById(`${nodeId}-points`)?.value || '200');
      const expr = (document.getElementById(`${nodeId}-expr`)?.value || '').trim();
      return {
        param_symbol: param,
        param_min: min,
        param_max: max,
        n_points: points,
        output_exprs: expr ? [expr] : []
      };
    }
    case 'rop-cloud-params': {
      const mode = document.getElementById(`${nodeId}-sampling-mode`)?.value || 'x_space';
      const samples = parseInt(document.getElementById(`${nodeId}-samples`)?.value || '10000');
      const span = parseInt(document.getElementById(`${nodeId}-span`)?.value || '6');
      const logxMin = parseFloat(document.getElementById(`${nodeId}-logx-min`)?.value || '-6');
      const logxMax = parseFloat(document.getElementById(`${nodeId}-logx-max`)?.value || '6');
      const targetSpecies = document.getElementById(`${nodeId}-target-species`)?.value || '';
      return { mode, samples, span, logxMin, logxMax, targetSpecies, config: nodeRegistry[nodeId]?.data?.config };
    }
    case 'fret-params': {
      const grid = parseInt(document.getElementById(`${nodeId}-grid`)?.value || '80');
      const min = parseFloat(document.getElementById(`${nodeId}-min`)?.value || '-6');
      const max = parseFloat(document.getElementById(`${nodeId}-max`)?.value || '6');
      return { grid, min, max, config: nodeRegistry[nodeId]?.data?.config };
    }
    case 'scan-2d-params': {
      const param1 = document.getElementById(`${nodeId}-param1`)?.value || '';
      const param2 = document.getElementById(`${nodeId}-param2`)?.value || '';
      const min1 = parseFloat(document.getElementById(`${nodeId}-min1`)?.value || '-6');
      const max1 = parseFloat(document.getElementById(`${nodeId}-max1`)?.value || '6');
      const min2 = parseFloat(document.getElementById(`${nodeId}-min2`)?.value || '-6');
      const max2 = parseFloat(document.getElementById(`${nodeId}-max2`)?.value || '6');
      const points = parseInt(document.getElementById(`${nodeId}-points`)?.value || '50');
      const expr = (document.getElementById(`${nodeId}-expr`)?.value || '').trim();
      return {
        param1_symbol: param1,
        param2_symbol: param2,
        param1_min: min1,
        param1_max: max1,
        param2_min: min2,
        param2_max: max2,
        n_grid: points,
        output_expr: expr
      };
    }
    case 'rop-polyhedron': {
      const dimension = parseInt(document.getElementById(`${nodeId}-dimension`)?.value || '2', 10);
      const pairs = [];
      const axisCount = dimension === 3 ? 3 : 2;
      for (let i = 1; i <= axisCount; i++) {
        pairs.push({
          x_symbol: document.getElementById(`${nodeId}-x${i}`)?.value || '',
          qk_symbol: document.getElementById(`${nodeId}-qk${i}`)?.value || '',
        });
      }
      return {
        dimension,
        pairs,
        add_inner_points: document.getElementById(`${nodeId}-add-inner-points`)?.checked ?? true,
        npoints: parseInt(document.getElementById(`${nodeId}-npoints`)?.value || '5000', 10),
        singular_extends: parseFloat(document.getElementById(`${nodeId}-singular-extends`)?.value || '2'),
        config: nodeRegistry[nodeId]?.data?.config,
        ropPlotData: cloneSerializable(nodeRegistry[nodeId]?.data?.ropPlotData),
        fitInnerPoints: nodeRegistry[nodeId]?.data?.fitInnerPoints ?? false,
      };
    }
    case 'rop-poly-params': {
      const dimension = parseInt(document.getElementById(`${nodeId}-dimension`)?.value || '2');
      const pairs = [];
      const axisCount = dimension === 3 ? 3 : 2;
      for (let i = 1; i <= axisCount; i++) {
        pairs.push({
          x_symbol: document.getElementById(`${nodeId}-x${i}`)?.value || '',
          qk_symbol: document.getElementById(`${nodeId}-qk${i}`)?.value || '',
        });
      }
      const addInnerPoints = document.getElementById(`${nodeId}-add-inner-points`)?.checked ?? true;
      const npoints = parseInt(document.getElementById(`${nodeId}-npoints`)?.value || '5000');
      const singularExtends = parseFloat(document.getElementById(`${nodeId}-singular-extends`)?.value || '2');
      return {
        dimension,
        pairs,
        add_inner_points: addInnerPoints,
        npoints,
        singular_extends: singularExtends,
        config: nodeRegistry[nodeId]?.data?.config,
      };
    }
    case 'atlas-spec': {
      return readAtlasSpecEditorState(nodeId);
    }
    case 'scan-1d-result': {
      return {
        scan1DResult: cloneSerializable(nodeRegistry[nodeId]?.data?.scan1DResult),
      };
    }
    case 'rop-cloud-result': {
      return {
        ropCloudData: cloneSerializable(nodeRegistry[nodeId]?.data?.ropCloudData),
        ropCloudPreset: nodeRegistry[nodeId]?.data?.ropCloudPreset || null,
        ropCloudRanges: cloneSerializable(nodeRegistry[nodeId]?.data?.ropCloudRanges),
      };
    }
    case 'fret-result': {
      return {
        fretHeatmapData: cloneSerializable(nodeRegistry[nodeId]?.data?.fretHeatmapData),
      };
    }
    case 'scan-2d-result': {
      return {
        scan2DResult: cloneSerializable(nodeRegistry[nodeId]?.data?.scan2DResult),
      };
    }
    case 'rop-poly-result': {
      return {
        ropPlotData: cloneSerializable(nodeRegistry[nodeId]?.data?.ropPlotData),
        fitInnerPoints: nodeRegistry[nodeId]?.data?.fitInnerPoints ?? false,
      };
    }
    case 'atlas-builder': {
      return {
        atlasData: cloneSerializable(nodeRegistry[nodeId]?.data?.atlasData),
        lastSpec: cloneSerializable(nodeRegistry[nodeId]?.data?.lastSpec),
        sqlitePath: nodeRegistry[nodeId]?.data?.sqlitePath || '',
      };
    }
    case 'atlas-query-config': {
      return readAtlasQueryEditorState(nodeId);
    }
    case 'atlas-query-result': {
      return {
        queryData: cloneSerializable(nodeRegistry[nodeId]?.data?.queryData),
        lastQuery: cloneSerializable(nodeRegistry[nodeId]?.data?.lastQuery),
      };
    }
    case 'markdown-note': {
      const textarea = document.getElementById(`${nodeId}-markdown`);
      return {
        markdown: textarea?.value ?? nodeRegistry[nodeId]?.data?.markdown ?? '',
      };
    }
    default:
      return {};
  }
}

function serializeState() {
  const nodes = [];
  for (const [id, info] of Object.entries(nodeRegistry)) {
    const el = document.getElementById(id);
    if (!el) continue;
    nodes.push({
      id,
      type: info.type,
      x: parseFloat(el.style.left) || 0,
      y: parseFloat(el.style.top) || 0,
      width: el.offsetWidth,
      height: el.offsetHeight,
      data: getNodeSerialData(id, info.type),
    });
  }
  return {
    version: WORKSPACE_DOCUMENT_VERSION,
    timestamp: new Date().toISOString(),
    canvas: { panX, panY, scale },
    nodes,
    connections: connections.map(c => ({ ...c })),
  };
}

function defaultSaveState() {
  const data = serializeState();
  const blob = new Blob([JSON.stringify(data, null, 2)], { type: 'application/json' });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = `biocircuits-explorer-workspace-${Date.now()}.json`;
  a.click();
  URL.revokeObjectURL(url);
  showToast('Workspace saved');
  lastWorkspaceShellSnapshot = window.BiocircuitsExplorerWorkspaceShell.serializeWorkspace();
  return true;
}

function saveState() {
  return window.BiocircuitsExplorerWorkspaceShell.saveWorkspace();
}

function defaultLoadState() {
  const input = document.createElement('input');
  input.type = 'file';
  input.accept = '.json';
  input.onchange = (e) => {
    const file = e.target.files[0];
    if (!file) return;
    const reader = new FileReader();
    reader.onload = (ev) => {
      try {
        const data = validateWorkspaceDocument(JSON.parse(ev.target.result));
        applyState(data);
        lastWorkspaceShellSnapshot = window.BiocircuitsExplorerWorkspaceShell.serializeWorkspace();
        showToast('Workspace loaded');
      } catch (err) {
        showToast('Failed to load: ' + err.message);
      }
    };
    reader.readAsText(file);
  };
  input.click();
  return true;
}

function loadState() {
  return window.BiocircuitsExplorerWorkspaceShell.loadWorkspace();
}

function applyState(data) {
  data = validateWorkspaceDocument(data);

  // 1. Clear existing canvas
  for (const id of Object.keys(nodeRegistry)) {
    const el = document.getElementById(id);
    if (el) el.remove();
  }
  Object.keys(nodeRegistry).forEach(k => delete nodeRegistry[k]);
  connections.length = 0;
  svgLayer.querySelectorAll('.wire').forEach(w => w.remove());

  // Reset model state (user will need to rebuild)
  state.sessionId = null;
  state.model = null;
  state.qK_syms = [];

  // 2. Restore canvas position
  if (data.canvas) {
    panX = data.canvas.panX || 0;
    panY = data.canvas.panY || 0;
    scale = data.canvas.scale || 1.0;
    applyViewportTransform();
  }

  // 3. Create nodes and build ID mapping
  const idMap = {}; // oldId → newId
  for (const saved of data.nodes) {
    const newId = createNode(saved.type, saved.x, saved.y);
    if (!newId) continue;
    idMap[saved.id] = newId;

    // Restore width
    const el = document.getElementById(newId);
    if (el && saved.width) el.style.width = `${saved.width}px`;
    if (el && saved.height) el.style.height = `${saved.height}px`;

    // Restore node-specific data
    restoreNodeData(newId, saved.type, saved.data || {});
  }

  // 4. Restore connections (with remapped IDs)
  if (data.connections) {
    for (const conn of data.connections) {
      const fromNode = idMap[conn.fromNode];
      const toNode = idMap[conn.toNode];
      if (fromNode && toNode) {
        connections.push({
          fromNode,
          fromPort: conn.fromPort,
          toNode,
          toPort: conn.toPort,
        });
      }
    }
  }

  // 5. Update wires
  updateConnections();

  // 6. Re-run any model-builders now that their reactions connections exist again.
  triggerAllAutoModelBuilds();

  // 7. Refresh ROP cloud target options now that connections are restored
  for (const [id, info] of Object.entries(nodeRegistry)) {
    if (info.type !== 'rop-cloud' && info.type !== 'rop-cloud-params') continue;
    updateROPCloudMode(id);
    const savedTarget = info.data?.targetSpecies;
    const sel = document.getElementById(`${id}-target-species`);
    if (sel && savedTarget && Array.from(sel.options).some(o => o.value === savedTarget)) {
      sel.value = savedTarget;
    }
  }
}

function restoreNodeData(nodeId, type, data) {
  switch (type) {
    case 'reaction-network': {
      // Clear default rows added by onInit
      const list = document.getElementById(`${nodeId}-reactions-list`);
      if (list) list.innerHTML = '';
      // Add saved reactions
      if (data.reactions && data.reactions.length > 0) {
        data.reactions.forEach(r => addReactionRow(nodeId, r.rule, r.kd));
      }
      break;
    }
    case 'regime-graph': {
      const modeEl = document.getElementById(`${nodeId}-graph-mode`);
      const changeEl = document.getElementById(`${nodeId}-change-qk`);
      const restoredMode = data.graphMode || data.config?.graphMode;
      const restoredChangeQK = data.changeQK || data.config?.changeQK;
      if (nodeRegistry[nodeId]) {
        nodeRegistry[nodeId].data = nodeRegistry[nodeId].data || {};
        nodeRegistry[nodeId].data.config = data.config || data;
      }
      if (modeEl && restoredMode) modeEl.value = restoredMode;
      if (changeEl && restoredChangeQK) changeEl.value = restoredChangeQK;
      updateRegimeGraphMode(nodeId);
      break;
    }
    case 'rop-cloud': {
      const modeEl = document.getElementById(`${nodeId}-sampling-mode`);
      const samplesEl = document.getElementById(`${nodeId}-samples`);
      const spanEl = document.getElementById(`${nodeId}-span`);
      const logxMinEl = document.getElementById(`${nodeId}-logx-min`);
      const logxMaxEl = document.getElementById(`${nodeId}-logx-max`);
      const targetEl = document.getElementById(`${nodeId}-target-species`);
      if (modeEl && data.mode) modeEl.value = data.mode;
      if (samplesEl && data.samples != null) samplesEl.value = data.samples;
      if (spanEl && data.span != null) spanEl.value = data.span;
      if (logxMinEl && data.logxMin != null) logxMinEl.value = data.logxMin;
      if (logxMaxEl && data.logxMax != null) logxMaxEl.value = data.logxMax;
      if (nodeRegistry[nodeId]) nodeRegistry[nodeId].data.targetSpecies = data.targetSpecies || '';
      updateROPCloudMode(nodeId);
      if (targetEl && data.targetSpecies) targetEl.value = data.targetSpecies;
      break;
    }
    case 'fret-heatmap': {
      const gridEl = document.getElementById(`${nodeId}-grid`);
      if (gridEl && data.grid != null) gridEl.value = data.grid;
      break;
    }
    case 'parameter-scan-1d': {
      const paramEl = document.getElementById(`${nodeId}-param`);
      const minEl = document.getElementById(`${nodeId}-min`);
      const maxEl = document.getElementById(`${nodeId}-max`);
      const pointsEl = document.getElementById(`${nodeId}-points`);
      const exprEl = document.getElementById(`${nodeId}-expr`);
      if (paramEl && data.param_symbol) paramEl.value = data.param_symbol;
      if (minEl && data.param_min != null) minEl.value = data.param_min;
      if (maxEl && data.param_max != null) maxEl.value = data.param_max;
      if (pointsEl && data.n_points != null) pointsEl.value = data.n_points;
      if (exprEl && data.output_exprs && data.output_exprs[0]) exprEl.value = data.output_exprs[0];
      break;
    }
    case 'parameter-scan-2d': {
      const param1El = document.getElementById(`${nodeId}-param1`);
      const param2El = document.getElementById(`${nodeId}-param2`);
      const min1El = document.getElementById(`${nodeId}-min1`);
      const max1El = document.getElementById(`${nodeId}-max1`);
      const min2El = document.getElementById(`${nodeId}-min2`);
      const max2El = document.getElementById(`${nodeId}-max2`);
      const gridEl = document.getElementById(`${nodeId}-grid`);
      const exprEl = document.getElementById(`${nodeId}-expr`);
      if (param1El && data.param1_symbol) param1El.value = data.param1_symbol;
      if (param2El && data.param2_symbol) param2El.value = data.param2_symbol;
      if (min1El && data.param1_min != null) min1El.value = data.param1_min;
      if (max1El && data.param1_max != null) max1El.value = data.param1_max;
      if (min2El && data.param2_min != null) min2El.value = data.param2_min;
      if (max2El && data.param2_max != null) max2El.value = data.param2_max;
      if (gridEl && data.n_grid != null) gridEl.value = data.n_grid;
      if (exprEl && data.output_expr) exprEl.value = data.output_expr;
      break;
    }
    case 'scan-1d-params': {
      const paramEl = document.getElementById(`${nodeId}-param`);
      const minEl = document.getElementById(`${nodeId}-min`);
      const maxEl = document.getElementById(`${nodeId}-max`);
      const pointsEl = document.getElementById(`${nodeId}-points`);
      const exprEl = document.getElementById(`${nodeId}-expr`);
      if (paramEl && data.param_symbol) paramEl.value = data.param_symbol;
      if (minEl && data.param_min != null) minEl.value = data.param_min;
      if (maxEl && data.param_max != null) maxEl.value = data.param_max;
      if (pointsEl && data.n_points != null) pointsEl.value = data.n_points;
      if (exprEl && data.output_exprs && data.output_exprs[0]) exprEl.value = data.output_exprs[0];
      break;
    }
    case 'siso-params': {
      const changeEl = document.getElementById(`${nodeId}-siso-select`);
      const targetEl = document.getElementById(`${nodeId}-target-x`);
      const scopeEl = document.getElementById(`${nodeId}-path-scope`);
      const minVolumeEl = document.getElementById(`${nodeId}-min-volume`);
      const keepSingularEl = document.getElementById(`${nodeId}-keep-singular`);
      const keepNonasymEl = document.getElementById(`${nodeId}-keep-nonasym`);
      const minEl = document.getElementById(`${nodeId}-min`);
      const maxEl = document.getElementById(`${nodeId}-max`);
      if (changeEl && data.changeQK) changeEl.value = data.changeQK;
      if (targetEl && data.observeX) targetEl.value = data.observeX;
      if (scopeEl && data.pathScope) scopeEl.value = data.pathScope;
      if (minVolumeEl && data.minVolumeMean != null) minVolumeEl.value = data.minVolumeMean;
      if (keepSingularEl && data.keepSingular != null) keepSingularEl.checked = data.keepSingular;
      if (keepNonasymEl && data.keepNonasymptotic != null) keepNonasymEl.checked = data.keepNonasymptotic;
      if (minEl && data.min != null) minEl.value = data.min;
      if (maxEl && data.max != null) maxEl.value = data.max;
      break;
    }
    case 'siso-result': {
      if (nodeRegistry[nodeId]) {
        nodeRegistry[nodeId].data.selectedPath = data.selectedPath || null;
      }
      break;
    }
    case 'rop-cloud-params': {
      const modeEl = document.getElementById(`${nodeId}-sampling-mode`);
      const samplesEl = document.getElementById(`${nodeId}-samples`);
      const spanEl = document.getElementById(`${nodeId}-span`);
      const logxMinEl = document.getElementById(`${nodeId}-logx-min`);
      const logxMaxEl = document.getElementById(`${nodeId}-logx-max`);
      const targetEl = document.getElementById(`${nodeId}-target-species`);
      if (modeEl && data.mode) modeEl.value = data.mode;
      if (samplesEl && data.samples != null) samplesEl.value = data.samples;
      if (spanEl && data.span != null) spanEl.value = data.span;
      if (logxMinEl && data.logxMin != null) logxMinEl.value = data.logxMin;
      if (logxMaxEl && data.logxMax != null) logxMaxEl.value = data.logxMax;
      if (nodeRegistry[nodeId]) nodeRegistry[nodeId].data.targetSpecies = data.targetSpecies || '';
      updateROPCloudMode(nodeId);
      if (targetEl && data.targetSpecies) targetEl.value = data.targetSpecies;
      if (data.config && nodeRegistry[nodeId]) {
        nodeRegistry[nodeId].data.config = data.config;
      }
      break;
    }
    case 'fret-params': {
      const gridEl = document.getElementById(`${nodeId}-grid`);
      const minEl = document.getElementById(`${nodeId}-min`);
      const maxEl = document.getElementById(`${nodeId}-max`);
      if (gridEl && data.grid != null) gridEl.value = data.grid;
      if (minEl && data.min != null) minEl.value = data.min;
      if (maxEl && data.max != null) maxEl.value = data.max;
      if (data.config && nodeRegistry[nodeId]) {
        nodeRegistry[nodeId].data.config = data.config;
      }
      break;
    }
    case 'siso-params': {
      const selectEl = document.getElementById(`${nodeId}-siso-select`);
      const minEl = document.getElementById(`${nodeId}-min`);
      const maxEl = document.getElementById(`${nodeId}-max`);
      if (selectEl && data.changeQK) selectEl.value = data.changeQK;
      if (minEl && data.min != null) minEl.value = data.min;
      if (maxEl && data.max != null) maxEl.value = data.max;
      if (data.config && nodeRegistry[nodeId]) {
        nodeRegistry[nodeId].data.config = data.config;
      }
      break;
    }
    case 'scan-2d-params': {
      const param1El = document.getElementById(`${nodeId}-param1`);
      const param2El = document.getElementById(`${nodeId}-param2`);
      const min1El = document.getElementById(`${nodeId}-min1`);
      const max1El = document.getElementById(`${nodeId}-max1`);
      const min2El = document.getElementById(`${nodeId}-min2`);
      const max2El = document.getElementById(`${nodeId}-max2`);
      const pointsEl = document.getElementById(`${nodeId}-points`);
      const exprEl = document.getElementById(`${nodeId}-expr`);
      if (param1El && data.param1_symbol) param1El.value = data.param1_symbol;
      if (param2El && data.param2_symbol) param2El.value = data.param2_symbol;
      if (min1El && data.param1_min != null) min1El.value = data.param1_min;
      if (max1El && data.param1_max != null) max1El.value = data.param1_max;
      if (min2El && data.param2_min != null) min2El.value = data.param2_min;
      if (max2El && data.param2_max != null) max2El.value = data.param2_max;
      if (pointsEl && data.n_grid != null) pointsEl.value = data.n_grid;
      if (exprEl && data.output_expr) exprEl.value = data.output_expr;
      break;
    }
    case 'rop-polyhedron': {
      const dimensionEl = document.getElementById(`${nodeId}-dimension`);
      const addInnerEl = document.getElementById(`${nodeId}-add-inner-points`);
      const npointsEl = document.getElementById(`${nodeId}-npoints`);
      const singularExtendsEl = document.getElementById(`${nodeId}-singular-extends`);
      if (dimensionEl && data.dimension != null) dimensionEl.value = data.dimension;
      if (addInnerEl && data.add_inner_points != null) addInnerEl.checked = data.add_inner_points;
      if (npointsEl && data.npoints != null) npointsEl.value = data.npoints;
      if (singularExtendsEl && data.singular_extends != null) singularExtendsEl.value = data.singular_extends;
      (data.pairs || []).forEach((pair, idx) => {
        const axis = idx + 1;
        const xEl = document.getElementById(`${nodeId}-x${axis}`);
        const qkEl = document.getElementById(`${nodeId}-qk${axis}`);
        if (xEl && pair.x_symbol) xEl.value = pair.x_symbol;
        if (qkEl && pair.qk_symbol) qkEl.value = pair.qk_symbol;
      });
      updateROPPolyDimension(nodeId);
      if (nodeRegistry[nodeId]) {
        nodeRegistry[nodeId].data = nodeRegistry[nodeId].data || {};
        nodeRegistry[nodeId].data.config = data.config || data;
      }
      break;
    }
    case 'rop-poly-params': {
      const dimensionEl = document.getElementById(`${nodeId}-dimension`);
      const addInnerEl = document.getElementById(`${nodeId}-add-inner-points`);
      const npointsEl = document.getElementById(`${nodeId}-npoints`);
      const singularExtendsEl = document.getElementById(`${nodeId}-singular-extends`);
      if (dimensionEl && data.dimension != null) dimensionEl.value = data.dimension;
      if (addInnerEl && data.add_inner_points != null) addInnerEl.checked = data.add_inner_points;
      if (npointsEl && data.npoints != null) npointsEl.value = data.npoints;
      if (singularExtendsEl && data.singular_extends != null) singularExtendsEl.value = data.singular_extends;
      (data.pairs || []).forEach((pair, idx) => {
        const axis = idx + 1;
        const xEl = document.getElementById(`${nodeId}-x${axis}`);
        const qkEl = document.getElementById(`${nodeId}-qk${axis}`);
        if (xEl && pair.x_symbol) xEl.value = pair.x_symbol;
        if (qkEl && pair.qk_symbol) qkEl.value = pair.qk_symbol;
      });
      updateROPPolyDimension(nodeId);
      if (data.config && nodeRegistry[nodeId]) {
        nodeRegistry[nodeId].data.config = data.config;
      }
      break;
    }
    case 'atlas-spec': {
      const sourceLabelEl = document.getElementById(`${nodeId}-source-label`);
      const libraryLabelEl = document.getElementById(`${nodeId}-library-label`);
      const sqlitePathEl = document.getElementById(`${nodeId}-sqlite-path`);
      const persistSqliteEl = document.getElementById(`${nodeId}-persist-sqlite`);
      const skipExistingEl = document.getElementById(`${nodeId}-skip-existing`);
      const profileNameEl = document.getElementById(`${nodeId}-profile-name`);
      const maxBaseSpeciesEl = document.getElementById(`${nodeId}-max-base-species`);
      const maxReactionsEl = document.getElementById(`${nodeId}-max-reactions`);
      const maxSupportEl = document.getElementById(`${nodeId}-max-support`);
      const pathScopeEl = document.getElementById(`${nodeId}-path-scope`);
      const minVolumeEl = document.getElementById(`${nodeId}-min-volume`);
      const keepSingularEl = document.getElementById(`${nodeId}-keep-singular`);
      const keepNonasymEl = document.getElementById(`${nodeId}-keep-nonasym`);
      const includePathRecordsEl = document.getElementById(`${nodeId}-include-path-records`);
      const enableEnumerationEl = document.getElementById(`${nodeId}-enable-enumeration`);
      const enumModeEl = document.getElementById(`${nodeId}-enum-mode`);
      const baseSpeciesCountsEl = document.getElementById(`${nodeId}-base-species-counts`);
      const minEnumReactionsEl = document.getElementById(`${nodeId}-min-enum-reactions`);
      const maxEnumReactionsEl = document.getElementById(`${nodeId}-max-enum-reactions`);
      const enumLimitEl = document.getElementById(`${nodeId}-enum-limit`);
      const explicitNetworksEl = document.getElementById(`${nodeId}-explicit-networks`);
      if (sourceLabelEl && data.sourceLabel != null) sourceLabelEl.value = data.sourceLabel;
      if (libraryLabelEl && data.libraryLabel != null) libraryLabelEl.value = data.libraryLabel;
      if (sqlitePathEl && data.sqlitePath != null) sqlitePathEl.value = data.sqlitePath;
      if (persistSqliteEl && data.persistSqlite != null) persistSqliteEl.checked = data.persistSqlite;
      if (skipExistingEl && data.skipExisting != null) skipExistingEl.checked = data.skipExisting;
      if (profileNameEl && data.profileName) profileNameEl.value = data.profileName;
      if (maxBaseSpeciesEl && data.maxBaseSpecies != null) maxBaseSpeciesEl.value = data.maxBaseSpecies;
      if (maxReactionsEl && data.maxReactions != null) maxReactionsEl.value = data.maxReactions;
      if (maxSupportEl && data.maxSupport != null) maxSupportEl.value = data.maxSupport;
      if (pathScopeEl && data.pathScope) pathScopeEl.value = data.pathScope;
      if (minVolumeEl && data.minVolumeMean != null) minVolumeEl.value = data.minVolumeMean;
      if (keepSingularEl && data.keepSingular != null) keepSingularEl.checked = data.keepSingular;
      if (keepNonasymEl && data.keepNonasymptotic != null) keepNonasymEl.checked = data.keepNonasymptotic;
      if (includePathRecordsEl && data.includePathRecords != null) includePathRecordsEl.checked = data.includePathRecords;
      if (enableEnumerationEl && data.enableEnumeration != null) enableEnumerationEl.checked = data.enableEnumeration;
      if (enumModeEl && data.enumerationMode) enumModeEl.value = data.enumerationMode;
      if (baseSpeciesCountsEl && data.baseSpeciesCountsText != null) baseSpeciesCountsEl.value = data.baseSpeciesCountsText;
      if (minEnumReactionsEl && data.minEnumerationReactions != null) minEnumReactionsEl.value = data.minEnumerationReactions;
      if (maxEnumReactionsEl && data.maxEnumerationReactions != null) maxEnumReactionsEl.value = data.maxEnumerationReactions;
      if (enumLimitEl && data.enumerationLimit != null) enumLimitEl.value = data.enumerationLimit;
      if (explicitNetworksEl && data.explicitNetworksText != null) explicitNetworksEl.value = data.explicitNetworksText;
      if (data.config && nodeRegistry[nodeId]) nodeRegistry[nodeId].data.config = data.config;
      break;
    }
    case 'atlas-query-config': {
      const sqlitePathEl = document.getElementById(`${nodeId}-query-sqlite-path`);
      const preferPersistedAtlasEl = document.getElementById(`${nodeId}-prefer-persisted-atlas`);
      const goalIoEl = document.getElementById(`${nodeId}-goal-io`);
      const goalMotifEl = document.getElementById(`${nodeId}-goal-motif`);
      const goalExactEl = document.getElementById(`${nodeId}-goal-exact`);
      const goalWitnessEl = document.getElementById(`${nodeId}-goal-witness`);
      const goalTransitionsEl = document.getElementById(`${nodeId}-goal-transitions`);
      const goalForbidRegimesEl = document.getElementById(`${nodeId}-goal-forbid-regimes`);
      const goalRobustEl = document.getElementById(`${nodeId}-goal-robust`);
      const goalFeasibleEl = document.getElementById(`${nodeId}-goal-feasible`);
      const goalMinVolumeEl = document.getElementById(`${nodeId}-goal-min-volume`);
      const motifLabelsEl = document.getElementById(`${nodeId}-motif-labels`);
      const motifMatchModeEl = document.getElementById(`${nodeId}-motif-match-mode`);
      const exactLabelsEl = document.getElementById(`${nodeId}-exact-labels`);
      const exactMatchModeEl = document.getElementById(`${nodeId}-exact-match-mode`);
      const inputSymbolsEl = document.getElementById(`${nodeId}-input-symbols`);
      const outputSymbolsEl = document.getElementById(`${nodeId}-output-symbols`);
      const requireRobustEl = document.getElementById(`${nodeId}-require-robust`);
      const minRobustPathsEl = document.getElementById(`${nodeId}-min-robust-path-count`);
      const maxBaseSpeciesEl = document.getElementById(`${nodeId}-query-max-base-species`);
      const maxReactionsEl = document.getElementById(`${nodeId}-query-max-reactions`);
      const maxSupportEl = document.getElementById(`${nodeId}-query-max-support`);
      const maxSupportMassEl = document.getElementById(`${nodeId}-query-max-support-mass`);
      const requiredRegimesEl = document.getElementById(`${nodeId}-required-regimes`);
      const forbiddenRegimesEl = document.getElementById(`${nodeId}-forbidden-regimes`);
      const requiredTransitionsEl = document.getElementById(`${nodeId}-required-transitions`);
      const forbiddenTransitionsEl = document.getElementById(`${nodeId}-forbidden-transitions`);
      const requiredPathSequencesEl = document.getElementById(`${nodeId}-required-path-sequences`);
      const forbidSingularEl = document.getElementById(`${nodeId}-forbid-singular-on-witness`);
      const maxWitnessPathLengthEl = document.getElementById(`${nodeId}-max-witness-path-length`);
      const requireWitnessFeasibleEl = document.getElementById(`${nodeId}-require-witness-feasible`);
      const requireWitnessRobustEl = document.getElementById(`${nodeId}-require-witness-robust`);
      const minWitnessVolumeMeanEl = document.getElementById(`${nodeId}-min-witness-volume-mean`);
      const rankingModeEl = document.getElementById(`${nodeId}-ranking-mode`);
      const collapseByNetworkEl = document.getElementById(`${nodeId}-collapse-by-network`);
      const paretoOnlyEl = document.getElementById(`${nodeId}-pareto-only`);
      const limitEl = document.getElementById(`${nodeId}-query-limit`);
      if (sqlitePathEl && data.sqlitePath != null) sqlitePathEl.value = data.sqlitePath;
      if (preferPersistedAtlasEl && data.preferPersistedAtlas != null) preferPersistedAtlasEl.checked = data.preferPersistedAtlas;
      if (goalIoEl && data.goalIoText != null) goalIoEl.value = data.goalIoText;
      if (goalMotifEl && data.goalMotifText != null) goalMotifEl.value = data.goalMotifText;
      if (goalExactEl && data.goalExactText != null) goalExactEl.value = data.goalExactText;
      if (goalWitnessEl && data.goalWitnessText != null) goalWitnessEl.value = data.goalWitnessText;
      if (goalTransitionsEl && data.goalTransitionsText != null) goalTransitionsEl.value = data.goalTransitionsText;
      if (goalForbidRegimesEl && data.goalForbidRegimesText != null) goalForbidRegimesEl.value = data.goalForbidRegimesText;
      if (goalRobustEl && data.goalRobust != null) goalRobustEl.checked = data.goalRobust;
      if (goalFeasibleEl && data.goalFeasible != null) goalFeasibleEl.checked = data.goalFeasible;
      if (goalMinVolumeEl && data.goalMinVolumeMean != null) goalMinVolumeEl.value = data.goalMinVolumeMean;
      if (motifLabelsEl && data.motifLabelsText != null) motifLabelsEl.value = data.motifLabelsText;
      if (motifMatchModeEl && data.motifMatchMode) motifMatchModeEl.value = data.motifMatchMode;
      if (exactLabelsEl && data.exactLabelsText != null) exactLabelsEl.value = data.exactLabelsText;
      if (exactMatchModeEl && data.exactMatchMode) exactMatchModeEl.value = data.exactMatchMode;
      if (inputSymbolsEl && data.inputSymbolsText != null) inputSymbolsEl.value = data.inputSymbolsText;
      if (outputSymbolsEl && data.outputSymbolsText != null) outputSymbolsEl.value = data.outputSymbolsText;
      if (requireRobustEl && data.requireRobust != null) requireRobustEl.checked = data.requireRobust;
      if (minRobustPathsEl && data.minRobustPathCount != null) minRobustPathsEl.value = data.minRobustPathCount;
      if (maxBaseSpeciesEl && data.maxBaseSpecies != null) maxBaseSpeciesEl.value = data.maxBaseSpecies;
      if (maxReactionsEl && data.maxReactions != null) maxReactionsEl.value = data.maxReactions;
      if (maxSupportEl && data.maxSupport != null) maxSupportEl.value = data.maxSupport;
      if (maxSupportMassEl && data.maxSupportMass != null) maxSupportMassEl.value = data.maxSupportMass;
      if (requiredRegimesEl && data.requiredRegimesText != null) requiredRegimesEl.value = data.requiredRegimesText;
      if (forbiddenRegimesEl && data.forbiddenRegimesText != null) forbiddenRegimesEl.value = data.forbiddenRegimesText;
      if (requiredTransitionsEl && data.requiredTransitionsText != null) requiredTransitionsEl.value = data.requiredTransitionsText;
      if (forbiddenTransitionsEl && data.forbiddenTransitionsText != null) forbiddenTransitionsEl.value = data.forbiddenTransitionsText;
      if (requiredPathSequencesEl && data.requiredPathSequencesText != null) requiredPathSequencesEl.value = data.requiredPathSequencesText;
      if (forbidSingularEl && data.forbidSingularOnWitness != null) forbidSingularEl.checked = data.forbidSingularOnWitness;
      if (maxWitnessPathLengthEl && data.maxWitnessPathLength != null) maxWitnessPathLengthEl.value = data.maxWitnessPathLength;
      if (requireWitnessFeasibleEl && data.requireWitnessFeasible != null) requireWitnessFeasibleEl.checked = data.requireWitnessFeasible;
      if (requireWitnessRobustEl && data.requireWitnessRobust != null) requireWitnessRobustEl.checked = data.requireWitnessRobust;
      if (minWitnessVolumeMeanEl && data.minWitnessVolumeMean != null) minWitnessVolumeMeanEl.value = data.minWitnessVolumeMean;
      if (rankingModeEl && data.rankingMode) rankingModeEl.value = data.rankingMode;
      if (collapseByNetworkEl && data.collapseByNetwork != null) collapseByNetworkEl.checked = data.collapseByNetwork;
      if (paretoOnlyEl && data.paretoOnly != null) paretoOnlyEl.checked = data.paretoOnly;
      if (limitEl && data.limit != null) limitEl.value = data.limit;
      restoreAtlasQueryBuilderState(nodeId, data);
      refreshAtlasQueryDesigner(nodeId);
      if (data.config && nodeRegistry[nodeId]) nodeRegistry[nodeId].data.config = data.config;
      break;
    }
    case 'markdown-note': {
      const textarea = document.getElementById(`${nodeId}-markdown`);
      if (textarea && data.markdown != null) {
        textarea.value = data.markdown;
      }
      if (nodeRegistry[nodeId]) {
        nodeRegistry[nodeId].data = nodeRegistry[nodeId].data || {};
        nodeRegistry[nodeId].data.markdown = data.markdown || '';
      }
      break;
    }
  }

  restoreCachedNodeRuntime(nodeId, type, data);
}

function restoreCachedNodeRuntime(nodeId, type, data) {
  const info = nodeRegistry[nodeId];
  if (!info) return;
  info.data = info.data || {};

  switch (type) {
    case 'model-builder': {
      if (!data.modelContext) break;
      info.data.built = data.built !== false;
      info.data.modelContext = data.modelContext;
      const infoEl = document.getElementById(`${nodeId}-model-info`);
      const infoText = document.getElementById(`${nodeId}-model-info-text`);
      const model = data.modelContext.model;
      if (infoEl && infoText && model) {
        infoEl.style.display = '';
        infoText.textContent = `n=${model.n}, d=${model.d}, r=${model.r}\nSpecies: ${model.x_sym.join(', ')}\nTotals: ${model.q_sym.join(', ')}\nConstants: ${model.K_sym.join(', ')}`;
      }
      break;
    }
    case 'regime-graph': {
      if (!data.graphData) break;
      info.data.graphData = data.graphData;
      const contentEl = document.getElementById(`${nodeId}-content`);
      if (!contentEl) break;
      contentEl.innerHTML = `<div class="plot-container" id="${nodeId}-plot"></div>`;
      setTimeout(() => {
        plotRegimeGraph(data.graphData, `${nodeId}-plot`, { viewMode: info.data.config?.viewMode || '3d' });
        setupPlotResize(nodeId, `${nodeId}-plot`);
      }, 50);
      break;
    }
    case 'siso-result': {
      info.data.selectedPath = data.selectedPath || null;
      info.data.behaviorData = data.behaviorData || null;
      info.data.trajectoryData = data.trajectoryData || null;
      const contentEl = document.getElementById(`${nodeId}-content`);
      if (!contentEl || !data.behaviorData) break;
      const changeQK = data.behaviorData.change_qK || data.selectedPath?.change_qK || '';
      contentEl.innerHTML = renderBehaviorFamiliesResult(nodeId, changeQK, data.behaviorData);
      if (data.selectedPath?.path_idx != null) {
        contentEl.querySelectorAll('.path-item, .path-chip').forEach(item => {
          const currentIdx = parseInt(item.dataset.pathIdx || item.dataset.idx, 10);
          item.classList.toggle('selected', currentIdx === data.selectedPath.path_idx);
        });
      }
      if (data.trajectoryData && document.getElementById(`${nodeId}-traj-plot`)) {
        const plotEl = document.getElementById(`${nodeId}-traj-plot`);
        if (plotEl) plotEl.style.display = '';
        plotTrajectory(data.trajectoryData, `${nodeId}-traj-plot`);
      }
      break;
    }
    case 'qk-poly-result': {
      if (!data.polyhedronPayload || !data.selection) break;
      info.data.selection = data.selection;
      info.data.polyhedronPayload = data.polyhedronPayload;
      const contentEl = document.getElementById(`${nodeId}-content`);
      if (!contentEl) break;
      const rendered = renderQKPolyhedronResult(nodeId, data.selection, data.polyhedronPayload);
      contentEl.innerHTML = rendered.html;
      if (rendered.canPlot) {
        setTimeout(() => {
          plotQKPolyhedron(data.polyhedronPayload.polyhedra?.[0], data.polyhedronPayload.qk_symbols || [], `${nodeId}-plot`);
          setupPlotResize(nodeId, `${nodeId}-plot`);
        }, 50);
      }
      break;
    }
    case 'rop-cloud':
    case 'rop-cloud-result': {
      if (!data.ropCloudData) break;
      info.data.ropCloudData = data.ropCloudData;
      info.data.ropCloudPreset = data.ropCloudPreset || 'robust';
      info.data.ropCloudRanges = data.ropCloudRanges || null;
      const contentEl = document.getElementById(`${nodeId}-content`);
      if (!contentEl) break;
      renderROPCloudOutput(nodeId, contentEl, data.ropCloudData);
      break;
    }
    case 'fret-heatmap':
    case 'fret-result': {
      if (!data.fretHeatmapData) break;
      info.data.fretHeatmapData = data.fretHeatmapData;
      const contentEl = document.getElementById(`${nodeId}-content`);
      if (!contentEl) break;
      contentEl.innerHTML = `<div class="plot-container" id="${nodeId}-plot"></div>`;
      setTimeout(() => {
        plotHeatmap(data.fretHeatmapData, `${nodeId}-plot`);
        setupPlotResize(nodeId, `${nodeId}-plot`);
      }, 50);
      break;
    }
    case 'parameter-scan-1d':
    case 'scan-1d-result': {
      if (!data.scan1DResult) break;
      info.data.scan1DResult = data.scan1DResult;
      const contentEl = document.getElementById(`${nodeId}-content`);
      if (!contentEl) break;
      contentEl.innerHTML = `<div class="plot-container" id="${nodeId}-plot"></div>`;
      setTimeout(() => {
        plotParameterScan1D(data.scan1DResult, `${nodeId}-plot`);
        setupPlotResize(nodeId, `${nodeId}-plot`);
      }, 50);
      break;
    }
    case 'parameter-scan-2d':
    case 'scan-2d-result': {
      if (!data.scan2DResult) break;
      info.data.scan2DResult = data.scan2DResult;
      const contentEl = document.getElementById(`${nodeId}-content`);
      if (!contentEl) break;
      contentEl.innerHTML = `<div class="plot-container" id="${nodeId}-plot"></div>`;
      setTimeout(() => {
        plotParameterScan2D(data.scan2DResult, `${nodeId}-plot`);
        setupPlotResize(nodeId, `${nodeId}-plot`);
      }, 50);
      break;
    }
    case 'rop-polyhedron':
    case 'rop-poly-result': {
      if (!data.ropPlotData) break;
      info.data.ropPlotData = data.ropPlotData;
      info.data.fitInnerPoints = !!data.fitInnerPoints;
      const contentEl = document.getElementById(`${nodeId}-content`);
      if (!contentEl) break;
      renderROPPolyhedronOutput(nodeId, contentEl, data.ropPlotData, data.config || {});
      const fitEl = document.getElementById(`${nodeId}-fit-inner-points`);
      if (fitEl) {
        fitEl.checked = !!data.fitInnerPoints;
        info.data.fitInnerPoints = !!data.fitInnerPoints;
      }
      break;
    }
    case 'atlas-builder': {
      if (!data.atlasData) break;
      info.data.atlasData = data.atlasData;
      info.data.lastSpec = data.lastSpec || null;
      info.data.sqlitePath = data.sqlitePath || '';
      const contentEl = document.getElementById(`${nodeId}-content`);
      if (contentEl) contentEl.innerHTML = renderAtlasBuilderResult(data.atlasData);
      break;
    }
    case 'atlas-query-result': {
      if (!data.queryData) break;
      info.data.queryData = data.queryData;
      info.data.lastQuery = data.lastQuery || null;
      const contentEl = document.getElementById(`${nodeId}-content`);
      if (contentEl) contentEl.innerHTML = renderAtlasQueryResult(data.queryData);
      break;
    }
    default:
      break;
  }
}

// ===== Parameter Scan 1D Helper Functions =====
const plotResizeObservers = new Map(); // nodeId -> ResizeObserver
const nodeResizeObservers = new Map(); // nodeId -> ResizeObserver
const plotInteractionGuards = new WeakSet();

function setupNodeResizeObserver(nodeId, nodeEl) {
  cleanupNodeResizeObserver(nodeId);
  if (!nodeEl) return;

  let rafId = null;
  const observer = new ResizeObserver(() => {
    if (rafId) cancelAnimationFrame(rafId);
    rafId = requestAnimationFrame(() => {
      const plotEls = nodeEl.querySelectorAll('.plot-container');
      plotEls.forEach(plotEl => {
        if (!plotEl.classList.contains('js-plotly-plot')) return;
        Plotly.Plots.resize(plotEl);
      });
      updateConnections();
      rafId = null;
    });
  });

  observer.observe(nodeEl);
  nodeResizeObservers.set(nodeId, observer);
}

function cleanupNodeResizeObserver(nodeId) {
  if (nodeResizeObservers.has(nodeId)) {
    nodeResizeObservers.get(nodeId).disconnect();
    nodeResizeObservers.delete(nodeId);
  }
}

function setupPlotInteractionGuard(plotEl) {
  if (!plotEl || plotInteractionGuards.has(plotEl)) return;

  plotEl.addEventListener('wheel', (e) => {
    e.stopPropagation();
  }, { passive: true });

  plotEl.addEventListener('pointerdown', (e) => {
    e.stopPropagation();
  });

  plotInteractionGuards.add(plotEl);
}

function setupPlotResize(nodeId, plotId) {
  // Clean up existing observer
  if (plotResizeObservers.has(nodeId)) {
    plotResizeObservers.get(nodeId).disconnect();
  }

  const plotEl = document.getElementById(plotId);
  if (!plotEl) return;
  setupPlotInteractionGuard(plotEl);

  const observer = new ResizeObserver(() => {
    Plotly.Plots.resize(plotEl);
    updateConnections();
  });

  observer.observe(plotEl);
  plotResizeObservers.set(nodeId, observer);
}

function cleanupPlotResize(nodeId) {
  if (plotResizeObservers.has(nodeId)) {
    plotResizeObservers.get(nodeId).disconnect();
    plotResizeObservers.delete(nodeId);
  }
}

function insertSpecies1D(nodeId) {
  const helper = document.getElementById(`${nodeId}-species-helper`);
  const expr = document.getElementById(`${nodeId}-expr`);
  if (helper.value && expr) {
    expr.value += (expr.value ? ' + ' : '') + helper.value;
    helper.value = '';
    const info = nodeRegistry[nodeId];
    if (info && info.type === 'scan-1d-params') {
      triggerConfigUpdate(nodeId, info.type);
    }
  }
}

function updateScan1DConfig(nodeId) {
  const paramSelect = document.getElementById(`${nodeId}-param`);
  const minInput = document.getElementById(`${nodeId}-min`);
  const maxInput = document.getElementById(`${nodeId}-max`);
  const pointsInput = document.getElementById(`${nodeId}-points`);
  const exprInput = document.getElementById(`${nodeId}-expr`);

  if (!paramSelect.value) {
    alert('Please select a scan parameter');
    return;
  }

  if (!exprInput.value.trim()) {
    alert('Please enter an output expression');
    return;
  }

  // Store config in node data
  nodeRegistry[nodeId].data.config = {
    param_symbol: paramSelect.value,
    param_min: parseFloat(minInput.value),
    param_max: parseFloat(maxInput.value),
    n_points: parseInt(pointsInput.value),
    output_exprs: [exprInput.value.trim()],
  };

  showToast('Configuration updated');
}

async function runParameterScan1D(nodeId) {
  const paramSymbol = document.getElementById(`${nodeId}-param`).value;
  const min = parseFloat(document.getElementById(`${nodeId}-min`).value);
  const max = parseFloat(document.getElementById(`${nodeId}-max`).value);
  const points = parseInt(document.getElementById(`${nodeId}-points`).value);

  const expr = document.getElementById(`${nodeId}-expr`).value.trim();
  if (!expr) {
    alert('Please enter an output expression');
    return;
  }

  setNodeLoading(nodeId, true);
  const contentEl = document.getElementById(`${nodeId}-content`);

  try {
    const sessionId = getSessionIdForNode(nodeId);
    if (!sessionId) throw new Error('Build the connected model first');
    const data = await api('parameter_scan_1d', {
      session_id: sessionId,
      param_symbol: paramSymbol,
      param_min: min,
      param_max: max,
      n_points: points,
      output_exprs: [expr],
    });
    if (nodeRegistry[nodeId]) {
      nodeRegistry[nodeId].data = nodeRegistry[nodeId].data || {};
      nodeRegistry[nodeId].data.scan1DResult = data;
    }

    contentEl.innerHTML = `<div class="plot-container" id="${nodeId}-plot"></div>`;
    commitWorkspaceSnapshot('scan-1d');
    setTimeout(() => {
      plotParameterScan1D(data, `${nodeId}-plot`);
      setupPlotResize(nodeId, `${nodeId}-plot`);
    }, 50);
  } catch (e) {
    contentEl.innerHTML = `<div class="node-error">${e.message}</div>`;
  } finally {
    setNodeLoading(nodeId, false);
  }
}

async function executeScan1DResult(nodeId) {
  // Find connected params node
  const conn = connections.find(c => c.toNode === nodeId && c.toPort === 'params');
  if (!conn) {
    alert('Please connect to a Scan 1D Config node');
    return;
  }

  const paramsNode = nodeRegistry[conn.fromNode];
  if (!paramsNode) {
    alert('Config node has no configuration. Please configure it first.');
    return;
  }

  // Sync latest DOM values to avoid stale config when user changed fields just before Run.
  triggerConfigUpdate(conn.fromNode, paramsNode.type || 'scan-1d-params');
  const config = paramsNode.data?.config;
  if (!config) {
    alert('Config node has no configuration. Please configure it first.');
    return;
  }

  // Validate required fields
  if (!config.param_symbol) {
    alert('Please select a scan parameter in the config node');
    return;
  }

  if (!config.output_exprs || !config.output_exprs[0]) {
    alert('Please enter an output expression in the config node');
    return;
  }

  setNodeLoading(nodeId, true);
  const contentEl = document.getElementById(`${nodeId}-content`);

  try {
    const sessionId = getSessionIdForNode(nodeId);
    if (!sessionId) throw new Error('Build the connected model first');
    const data = await api('parameter_scan_1d', {
      session_id: sessionId,
      param_symbol: config.param_symbol,
      param_min: config.param_min,
      param_max: config.param_max,
      n_points: config.n_points,
      output_exprs: config.output_exprs,
    });
    if (nodeRegistry[nodeId]) {
      nodeRegistry[nodeId].data = nodeRegistry[nodeId].data || {};
      nodeRegistry[nodeId].data.scan1DResult = data;
    }

    contentEl.innerHTML = `<div class="plot-container" id="${nodeId}-plot"></div>`;
    commitWorkspaceSnapshot('scan-1d');
    setTimeout(() => {
      plotParameterScan1D(data, `${nodeId}-plot`);
      setupPlotResize(nodeId, `${nodeId}-plot`);
    }, 50);
  } catch (e) {
    contentEl.innerHTML = `<div class="node-error">${e.message}</div>`;
  } finally {
    setNodeLoading(nodeId, false);
  }
}

function updateROPCloudConfig(nodeId) {
  const mode = document.getElementById(`${nodeId}-sampling-mode`).value;
  const nSamples = parseInt(document.getElementById(`${nodeId}-samples`).value);

  const config = {
    mode: mode,
    n_samples: nSamples,
  };

  if (mode === 'x_space') {
    const targetSpecies = document.getElementById(`${nodeId}-target-species`)?.value || '';
    const logxMin = parseFloat(document.getElementById(`${nodeId}-logx-min`).value);
    const logxMax = parseFloat(document.getElementById(`${nodeId}-logx-max`).value);
    config.target_species = targetSpecies;
    config.logx_min = logxMin;
    config.logx_max = logxMax;
  } else {
    const span = parseInt(document.getElementById(`${nodeId}-span`).value);
    config.span = span;
  }

  nodeRegistry[nodeId].data.config = config;
  showToast('Configuration updated');
}

async function executeROPCloudResult(nodeId) {
  const conn = connections.find(c => c.toNode === nodeId && c.toPort === 'params');
  if (!conn) {
    alert('Please connect to a ROP Cloud Config node');
    return;
  }

  const paramsNode = nodeRegistry[conn.fromNode];
  if (!paramsNode) {
    alert('Config node has no configuration. Please configure it first.');
    return;
  }

  const config = getNodeSerialData(conn.fromNode, paramsNode.type || 'rop-cloud-params');
  paramsNode.data = paramsNode.data || {};
  paramsNode.data.config = config;
  setNodeLoading(nodeId, true);
  const contentEl = document.getElementById(`${nodeId}-content`);

  try {
    let data;
    if (config.mode === 'qk') {
      const sessionId = getSessionIdForNode(nodeId);
      if (!sessionId) throw new Error('Build the connected model first, or switch to x-space mode');
      const modelConn = connections.find(c => c.toNode === conn.fromNode && c.toPort === 'model');
      if (!modelConn) throw new Error('qK mode requires Model input connection');
      data = await api('rop_cloud', {
        sampling_mode: 'qk',
        session_id: sessionId,
        n_samples: config.samples,
        span: config.span,
      });
    } else {
      const rxConn = connections.find(c => c.toNode === conn.fromNode && c.toPort === 'reactions');
      if (!rxConn) throw new Error('x-space mode requires Reactions input connection');
      const { reactions } = getReactionsFromNode(rxConn.fromNode);
      if (!reactions.length) throw new Error('Add at least one reaction in the connected Reaction Network');
      data = await api('rop_cloud', {
        sampling_mode: 'x_space',
        reactions: reactions,
        n_samples: config.samples,
        logx_min: config.logxMin,
        logx_max: config.logxMax,
        target_species: config.targetSpecies || '',
      });
    }

    renderROPCloudOutput(nodeId, contentEl, data);
  } catch (e) {
    contentEl.innerHTML = `<div class="node-error">${e.message}</div>`;
  } finally {
    setNodeLoading(nodeId, false);
  }
}

function updateFRETConfig(nodeId) {
  const grid = parseInt(document.getElementById(`${nodeId}-grid`).value);
  nodeRegistry[nodeId].data.config = { n_grid: grid };
  showToast('Configuration updated');
}

async function executeFRETResult(nodeId) {
  const conn = connections.find(c => c.toNode === nodeId && c.toPort === 'params');
  if (!conn) {
    alert('Please connect to a FRET Config node');
    return;
  }

  const paramsNode = nodeRegistry[conn.fromNode];
  if (!paramsNode) {
    alert('Config node has no configuration. Please configure it first.');
    return;
  }

  const config = getNodeSerialData(conn.fromNode, paramsNode.type || 'fret-params');
  paramsNode.data = paramsNode.data || {};
  paramsNode.data.config = config;
  setNodeLoading(nodeId, true);
  const contentEl = document.getElementById(`${nodeId}-content`);

  try {
    const sessionId = getSessionIdForNode(nodeId);
    if (!sessionId) throw new Error('Build the connected model first');
    const data = await api('fret_heatmap', {
      session_id: sessionId,
      n_grid: config.grid,
      logq_min: config.min,
      logq_max: config.max,
    });
    if (nodeRegistry[nodeId]) {
      nodeRegistry[nodeId].data = nodeRegistry[nodeId].data || {};
      nodeRegistry[nodeId].data.fretHeatmapData = data;
    }

    contentEl.innerHTML = `<div class="plot-container" id="${nodeId}-plot"></div>`;
    commitWorkspaceSnapshot('fret-heatmap');
    setTimeout(() => {
      plotHeatmap(data, `${nodeId}-plot`);
      setupPlotResize(nodeId, `${nodeId}-plot`);
    }, 50);
  } catch (e) {
    contentEl.innerHTML = `<div class="node-error">${e.message}</div>`;
  } finally {
    setNodeLoading(nodeId, false);
  }
}

function insertSpecies2D(nodeId) {
  const helper = document.getElementById(`${nodeId}-species-helper`);
  const expr = document.getElementById(`${nodeId}-expr`);
  if (helper.value && expr) {
    expr.value += (expr.value ? ' + ' : '') + helper.value;
    helper.value = '';
    const info = nodeRegistry[nodeId];
    if (info && info.type === 'scan-2d-params') {
      triggerConfigUpdate(nodeId, info.type);
    }
  }
}

function updateScan2DConfig(nodeId) {
  const param1 = document.getElementById(`${nodeId}-param1`).value;
  const param2 = document.getElementById(`${nodeId}-param2`).value;
  const min1 = parseFloat(document.getElementById(`${nodeId}-min1`).value);
  const max1 = parseFloat(document.getElementById(`${nodeId}-max1`).value);
  const min2 = parseFloat(document.getElementById(`${nodeId}-min2`).value);
  const max2 = parseFloat(document.getElementById(`${nodeId}-max2`).value);
  const points = parseInt(document.getElementById(`${nodeId}-points`).value);
  const expr = document.getElementById(`${nodeId}-expr`).value.trim();

  if (!param1 || !param2) {
    alert('Please select both parameters');
    return;
  }

  if (!expr) {
    alert('Please enter an output expression');
    return;
  }

  nodeRegistry[nodeId].data.config = {
    param_symbol_1: param1,
    param_symbol_2: param2,
    param_min_1: min1,
    param_max_1: max1,
    param_min_2: min2,
    param_max_2: max2,
    n_points: points,
    output_exprs: [expr],
  };

  showToast('Configuration updated');
}

async function executeScan2DResult(nodeId) {
  const conn = connections.find(c => c.toNode === nodeId && c.toPort === 'params');
  if (!conn) {
    alert('Please connect to a Scan 2D Config node');
    return;
  }

  const paramsNode = nodeRegistry[conn.fromNode];
  if (!paramsNode) {
    alert('Config node has no configuration. Please configure it first.');
    return;
  }

  // Sync latest DOM values to avoid stale config when user changed fields just before Run.
  triggerConfigUpdate(conn.fromNode, paramsNode.type || 'scan-2d-params');
  const config = paramsNode.data?.config;
  if (!config) {
    alert('Config node has no configuration. Please configure it first.');
    return;
  }

  // Validate required fields
  if (!config.param1_symbol || !config.param2_symbol) {
    alert('Please select both X and Y axis parameters in the config node');
    return;
  }

  if (!config.output_expr) {
    alert('Please enter an output expression in the config node');
    return;
  }

  setNodeLoading(nodeId, true);
  const contentEl = document.getElementById(`${nodeId}-content`);

  try {
    const sessionId = getSessionIdForNode(nodeId);
    if (!sessionId) throw new Error('Build the connected model first');
    const data = await api('parameter_scan_2d', {
      session_id: sessionId,
      ...config
    });
    if (nodeRegistry[nodeId]) {
      nodeRegistry[nodeId].data = nodeRegistry[nodeId].data || {};
      nodeRegistry[nodeId].data.scan2DResult = data;
    }

    contentEl.innerHTML = `<div class="plot-container" id="${nodeId}-plot"></div>`;
    commitWorkspaceSnapshot('scan-2d');
    setTimeout(() => {
      plotParameterScan2D(data, `${nodeId}-plot`);
      setupPlotResize(nodeId, `${nodeId}-plot`);
    }, 50);
  } catch (e) {
    contentEl.innerHTML = `<div class="node-error">${e.message}</div>`;
  } finally {
    setNodeLoading(nodeId, false);
  }
}

function updateROPPolyConfig(nodeId) {
  const expr = document.getElementById(`${nodeId}-expr`).value.trim();
  const param1 = document.getElementById(`${nodeId}-param1`).value;
  const param2 = document.getElementById(`${nodeId}-param2`).value;
  const asymptotic = document.getElementById(`${nodeId}-asymptotic`).checked;
  const maxVertices = parseInt(document.getElementById(`${nodeId}-max-vertices`).value);

  if (!expr) {
    alert('Please enter an output expression');
    return;
  }

  if (!param1 || !param2) {
    alert('Please select both parameters');
    return;
  }

  nodeRegistry[nodeId].data.config = {
    output_expr: expr,
    param_symbol_1: param1,
    param_symbol_2: param2,
    asymptotic_only: asymptotic,
    max_vertices: maxVertices,
  };

  showToast('Configuration updated');
}

async function executeROPPolyResult(nodeId) {
  const conn = connections.find(c => c.toNode === nodeId && c.toPort === 'params');
  if (!conn) {
    alert('Please connect to a ROP Polyhedron Config node');
    return;
  }

  const paramsNode = nodeRegistry[conn.fromNode];
  const config = getNodeSerialData(conn.fromNode, 'rop-poly-params');
  if (!paramsNode || !config || !(config.pairs || []).length) {
    alert('Config node has no configuration. Please configure it first.');
    return;
  }
  paramsNode.data = paramsNode.data || {};
  paramsNode.data.config = config;
  setNodeLoading(nodeId, true);
  const contentEl = document.getElementById(`${nodeId}-content`);

  try {
    const sessionId = getSessionIdForNode(nodeId);
    if (!sessionId) throw new Error('Build the connected model first');
    const data = await api('rop_polyhedron', {
      session_id: sessionId,
      ...config
    });
    renderROPPolyhedronOutput(nodeId, contentEl, data, config);
  } catch (e) {
    contentEl.innerHTML = `<div class="node-error">${e.message}</div>`;
  } finally {
    setNodeLoading(nodeId, false);
  }
}

function plotParameterScan1D(data, plotId) {
  const { param_symbol, param_values, output_exprs, output_traj } = data;
  const plotTheme = getPlotTheme();

  const traces = output_exprs.map((expr, i) => ({
    x: param_values,
    y: output_traj.map(row => row[i]),
    mode: 'lines',
    name: expr,
    line: { width: 2 },
  }));

  const layout = {
    autosize: true,
    margin: { t: 40, b: 60, l: 70, r: 20 },
    title: { text: `Parameter Scan: ${param_symbol}`, font: { color: plotTheme.titleColor, size: 11 }, y: 0.98, yanchor: 'top' },
    xaxis: { title: `log10(${param_symbol})` },
    yaxis: { title: 'log10(concentration)' },
    legend: { x: 1, xanchor: 'right', y: 1 },
  };

  Plotly.newPlot(plotId, traces, applyPlotLayoutTheme(layout), { responsive: true, displayModeBar: false, scrollZoom: true });
}

// ===== Parameter Scan 2D Helper Functions =====
function insertSpecies2D(nodeId) {
  const helper = document.getElementById(`${nodeId}-species-helper`);
  const expr = document.getElementById(`${nodeId}-expr`);
  if (helper.value && expr) {
    expr.value += (expr.value ? ' + ' : '') + helper.value;
    helper.value = '';
    const info = nodeRegistry[nodeId];
    if (info && info.type === 'scan-2d-params') {
      triggerConfigUpdate(nodeId, info.type);
    }
  }
}

async function runParameterScan2D(nodeId) {
  const param1 = document.getElementById(`${nodeId}-param1`).value;
  const param2 = document.getElementById(`${nodeId}-param2`).value;
  const min1 = parseFloat(document.getElementById(`${nodeId}-min1`).value);
  const max1 = parseFloat(document.getElementById(`${nodeId}-max1`).value);
  const min2 = parseFloat(document.getElementById(`${nodeId}-min2`).value);
  const max2 = parseFloat(document.getElementById(`${nodeId}-max2`).value);
  const grid = parseInt(document.getElementById(`${nodeId}-grid`).value);
  const expr = document.getElementById(`${nodeId}-expr`).value.trim();

  if (!expr) {
    alert('Please enter an output expression');
    return;
  }

  setNodeLoading(nodeId, true);
  const contentEl = document.getElementById(`${nodeId}-content`);

  try {
    const sessionId = getSessionIdForNode(nodeId);
    if (!sessionId) throw new Error('Build the connected model first');
    const data = await api('parameter_scan_2d', {
      session_id: sessionId,
      param1_symbol: param1,
      param2_symbol: param2,
      param1_min: min1,
      param1_max: max1,
      param2_min: min2,
      param2_max: max2,
      n_grid: grid,
      output_expr: expr,
    });
    if (nodeRegistry[nodeId]) {
      nodeRegistry[nodeId].data = nodeRegistry[nodeId].data || {};
      nodeRegistry[nodeId].data.scan2DResult = data;
    }

    contentEl.innerHTML = `<div class="plot-container" id="${nodeId}-plot"></div>`;
    commitWorkspaceSnapshot('scan-2d');
    setTimeout(() => {
      plotParameterScan2D(data, `${nodeId}-plot`);
      setupPlotResize(nodeId, `${nodeId}-plot`);
    }, 50);
  } catch (e) {
    contentEl.innerHTML = `<div class="node-error">${e.message}</div>`;
  } finally {
    setNodeLoading(nodeId, false);
  }
}

function plotParameterScan2D(data, plotId) {
  const { param1_symbol, param2_symbol, param1_values, param2_values, output_expr, output_grid, regime_grid } = data;
  const plotTheme = getPlotTheme();

  // Create 3D surface plot
  const traces = [
    {
      z: output_grid,
      x: param1_values,
      y: param2_values,
      type: 'surface',
      colorscale: 'Viridis',
      colorbar: themedColorbar(`log(${output_expr})`),
      contours: {
        z: {
        show: true,
          usecolormap: true,
       highlightcolor: "#42f462",
          project: { z: true }
        }
      }
    }
  ];

  const layout = {
    autosize: true,
    margin: { t: 40, b: 60, l: 70, r: 20 },
    title: {
      text: `${output_expr} vs ${param1_symbol}, ${param2_symbol}`,
      font: { color: plotTheme.titleColor, size: 11 },
      y: 0.98,
      yanchor: 'top'
    },
    scene: {
      xaxis: {
        title: `log10(${param1_symbol})`,
      },
      yaxis: {
        title: `log10(${param2_symbol})`,
      },
      zaxis: {
        title: `log10(${output_expr})`,
      },
    }
  };

  Plotly.newPlot(plotId, traces, applyPlotLayoutTheme(layout), { responsive: true, displayModeBar: false, scrollZoom: true });
}

// ===== ROP Polyhedron Helper Functions =====
function updateROPPolyDimension(nodeId) {
  const dimension = parseInt(document.getElementById(`${nodeId}-dimension`)?.value || '2', 10);
  const axis3XRow = document.getElementById(`${nodeId}-axis3-x-row`);
  const axis3QKRow = document.getElementById(`${nodeId}-axis3-qk-row`);
  const showAxis3 = dimension === 3;
  if (axis3XRow) axis3XRow.style.display = showAxis3 ? '' : 'none';
  if (axis3QKRow) axis3QKRow.style.display = showAxis3 ? '' : 'none';
}

async function runROPPolyhedron(nodeId) {
  const config = getNodeSerialData(nodeId, 'rop-polyhedron');
  if (!(config.pairs || []).length || config.pairs.some(pair => !pair.x_symbol || !pair.qk_symbol)) {
    alert('Please select species and qK symbols for each ROP axis');
    return;
  }

  nodeRegistry[nodeId].data = nodeRegistry[nodeId].data || {};
  nodeRegistry[nodeId].data.config = config;
  setNodeLoading(nodeId, true);
  const contentEl = document.getElementById(`${nodeId}-content`);

  try {
    const sessionId = getSessionIdForNode(nodeId);
    if (!sessionId) throw new Error('Build the connected model first');
    const data = await api('rop_polyhedron', {
      session_id: sessionId,
      ...config,
    });
    renderROPPolyhedronOutput(nodeId, contentEl, data, config);
  } catch (e) {
    contentEl.innerHTML = `<div class="node-error">${e.message}</div>`;
  } finally {
    setNodeLoading(nodeId, false);
  }
}

function renderROPPolyhedronOutput(nodeId, contentEl, data, config = {}) {
  const axisSummary = (data.pairs || config.pairs || []).map((pair, idx) => {
    const xSymbol = pair.x_symbol || pair.xSymbol || '?';
    const qkSymbol = pair.qk_symbol || pair.qkSymbol || '?';
    return `<span class="summary-chip">A${idx + 1}: ${xSymbol} / ${qkSymbol}</span>`;
  }).join('');
  const hasInnerPoints = (data.inner_points || []).length > 0;

  nodeRegistry[nodeId].data = nodeRegistry[nodeId].data || {};
  nodeRegistry[nodeId].data.ropPlotData = data;
  nodeRegistry[nodeId].data.fitInnerPoints = false;

  contentEl.innerHTML = `
    <div class="siso-summary-line">
      <span class="summary-chip"><strong>${data.dimension || config.dimension}D</strong></span>
      ${axisSummary}
    </div>
    <div class="siso-summary-line">
      <label class="summary-chip ${hasInnerPoints ? '' : 'text-dim'}" style="display:inline-flex;align-items:center;gap:6px;cursor:${hasInnerPoints ? 'pointer' : 'default'};">
        <input type="checkbox" id="${nodeId}-fit-inner-points" onchange="refreshROPPolyhedronPlot('${nodeId}')" ${hasInnerPoints ? '' : 'disabled'}>
        Fit inner points
      </label>
    </div>
    <div class="plot-container" id="${nodeId}-plot"></div>
  `;

  commitWorkspaceSnapshot('rop-polyhedron');
  setTimeout(() => {
    refreshROPPolyhedronPlot(nodeId);
    setupPlotResize(nodeId, `${nodeId}-plot`);
  }, 50);
}

function refreshROPPolyhedronPlot(nodeId) {
  const data = nodeRegistry[nodeId]?.data?.ropPlotData;
  if (!data) return;
  const fitInnerPoints = document.getElementById(`${nodeId}-fit-inner-points`)?.checked ?? false;
  if (nodeRegistry[nodeId]) {
    nodeRegistry[nodeId].data = nodeRegistry[nodeId].data || {};
    nodeRegistry[nodeId].data.fitInnerPoints = fitInnerPoints;
  }
  plotROPPolyhedron(data, `${nodeId}-plot`, { fitInnerPoints });
  commitWorkspaceSnapshot('rop-polyhedron-fit');
}

function getROPPlotBounds(data, options = {}) {
  const dimension = data.dimension || 2;
  const fitInnerPoints = options.fitInnerPoints === true;
  const mins = Array(dimension).fill(Infinity);
  const maxs = Array(dimension).fill(-Infinity);
  let hasFiniteCoords = false;

  const includeCoord = (coord) => {
    if (!Array.isArray(coord) || coord.length < dimension) return;
    const values = coord.slice(0, dimension).map(Number);
    if (values.some(v => !Number.isFinite(v))) return;
    hasFiniteCoords = true;
    for (let i = 0; i < dimension; i++) {
      if (values[i] < mins[i]) mins[i] = values[i];
      if (values[i] > maxs[i]) maxs[i] = values[i];
    }
  };

  const includeSegment = (segment) => {
    includeCoord(segment.from);
    includeCoord(segment.to);
  };

  (data.points || []).forEach(point => includeCoord(point.coords));
  (data.direct_edges || []).forEach(includeSegment);
  (data.indirect_edges || []).forEach(includeSegment);
  (data.direct_rays || []).forEach(includeSegment);
  (data.indirect_rays || []).forEach(includeSegment);
  if (fitInnerPoints) {
    (data.inner_points || []).forEach(includeCoord);
  }

  if (!hasFiniteCoords) return null;

  return mins.map((minValue, idx) => {
    const maxValue = maxs[idx];
    if (!Number.isFinite(minValue) || !Number.isFinite(maxValue)) return null;
    let pad = (maxValue - minValue) * 0.08;
    if (!(pad > 0)) {
      const scale = Math.max(Math.abs(minValue), Math.abs(maxValue), 1);
      pad = scale * 0.08;
    }
    return [minValue - pad, maxValue + pad];
  });
}

function plotROPPolyhedron(data, plotId, options = {}) {
  const plotTheme = getPlotTheme();
  const lightTheme = prefersLightTheme();
  const ropPolyColors = lightTheme
    ? {
        directEdge: '#5b728a',
        indirectEdge: '#8ca0b4',
        directRay: '#2f94b7',
        indirectRay: '#67aac2',
        innerPoint: '#5c7288',
        regularVertex: '#2f9e44',
        asymptoticVertex: '#d9480f',
        legacyEdge: '#2b8a3e',
        legacyVertex: '#c92a2a',
      }
    : {
        directEdge: '#e5e7eb',
        indirectEdge: '#9aa0a6',
        directRay: '#8ecae6',
        indirectRay: '#8ecae6',
        innerPoint: 'rgba(148, 163, 184, 0.18)',
        regularVertex: '#b7efc5',
        asymptoticVertex: '#ffb4a2',
        legacyEdge: '#00ff00',
        legacyVertex: '#ff0000',
      };

  if (data.points && data.direct_edges) {
    const dimension = data.dimension || 2;
    const is3D = dimension === 3;
    const axisLabels = data.axis_labels || [];
    const traces = [];
    const ranges = getROPPlotBounds(data, options);

    const pushSegmentTrace = (segment, name, color, dash = 'solid', width = 2, opacity = 1, showLegend = false) => {
      const from = segment.from || [];
      const to = segment.to || [];
      const common = {
        mode: 'lines',
        name,
        showlegend: showLegend,
        hoverinfo: 'skip',
        opacity,
        line: is3D ? { color, width } : { color, width, dash },
      };
      if (is3D) {
        traces.push({
          x: [from[0], to[0]],
          y: [from[1], to[1]],
          z: [from[2], to[2]],
          type: 'scatter3d',
          ...common,
        });
      } else {
        traces.push({
          x: [from[0], to[0]],
          y: [from[1], to[1]],
          type: 'scatter',
          ...common,
        });
      }
    };

    (data.direct_edges || []).forEach((edge, idx) => pushSegmentTrace(edge, 'Direct edge', ropPolyColors.directEdge, 'solid', 2, 1, idx === 0));
    (data.indirect_edges || []).forEach((edge, idx) => pushSegmentTrace(edge, 'Indirect edge', ropPolyColors.indirectEdge, 'dash', 2, 1, idx === 0));
    (data.direct_rays || []).forEach((edge, idx) => pushSegmentTrace(edge, 'Singular ray', ropPolyColors.directRay, 'solid', 4, 0.95, idx === 0));
    (data.indirect_rays || []).forEach((edge, idx) => pushSegmentTrace(edge, 'Indirect singular ray', ropPolyColors.indirectRay, 'dash', 4, 0.85, idx === 0));

    if ((data.inner_points || []).length) {
      const inner = data.inner_points;
      const common = {
        mode: 'markers',
        name: 'Inner points',
        marker: {
          color: ropPolyColors.innerPoint,
          opacity: lightTheme ? 0.78 : 0.18,
          size: is3D ? (lightTheme ? 3 : 2) : (lightTheme ? 6 : 5),
          line: lightTheme ? { color: plotTheme.nodeOutlineColor, width: 0.6 } : { width: 0 },
        },
        hoverinfo: 'skip',
      };
      if (is3D) {
        traces.push({
          x: inner.map(point => point[0]),
          y: inner.map(point => point[1]),
          z: inner.map(point => point[2]),
          type: 'scatter3d',
          ...common,
        });
      } else {
        traces.push({
          x: inner.map(point => point[0]),
          y: inner.map(point => point[1]),
          type: 'scatter',
          ...common,
        });
      }
    }

    if ((data.points || []).length) {
      const points = data.points;
      const pointColor = point => point.point_type === 'asymptotic' ? ropPolyColors.asymptoticVertex : ropPolyColors.regularVertex;
      const hoverText = points.map(point => `Vertex ${point.vertex_idx}<br>Type: ${point.point_type}<br>Perm: [${(point.perm || []).join(',')}]`);
      const common = {
        mode: 'markers',
        name: 'Vertices',
        marker: {
          size: is3D ? 6 : 9,
          color: points.map(pointColor),
          line: { color: plotTheme.nodeOutlineColor, width: 1 },
        },
        hovertext: hoverText,
        hoverinfo: 'text',
      };
      if (is3D) {
        traces.push({
          x: points.map(point => point.coords[0]),
          y: points.map(point => point.coords[1]),
          z: points.map(point => point.coords[2]),
          type: 'scatter3d',
          ...common,
        });
      } else {
        traces.push({
          x: points.map(point => point.coords[0]),
          y: points.map(point => point.coords[1]),
          type: 'scatter',
          ...common,
        });
      }
    }

    const layout = {
      autosize: true,
      margin: { t: 40, b: 60, l: 70, r: 20 },
      title: {
        text: `ROP Polyhedron (${dimension}D)`,
        font: { color: plotTheme.titleColor, size: 11 },
        y: 0.98,
        yanchor: 'top',
      },
      showlegend: true,
      legend: { font: { color: plotTheme.fontColor, size: 9 } },
    };

    if (is3D) {
      layout.scene = {
        xaxis: { title: axisLabels[0] || 'Axis 1', range: ranges?.[0] },
        yaxis: { title: axisLabels[1] || 'Axis 2', range: ranges?.[1] },
        zaxis: { title: axisLabels[2] || 'Axis 3', range: ranges?.[2] },
      };
    } else {
      layout.xaxis = { title: axisLabels[0] || 'Axis 1', range: ranges?.[0] };
      layout.yaxis = { title: axisLabels[1] || 'Axis 2', range: ranges?.[1] };
    }

    Plotly.newPlot(plotId, traces, applyPlotLayoutTheme(layout), { responsive: true, displayModeBar: false, scrollZoom: true });
    return;
  }

  const { output_expr, param1_symbol, param2_symbol, vertices, edges } = data;
  const traces = [];

  (edges || []).forEach(edge => {
    traces.push({
      x: edge.ro1,
      y: edge.ro2,
      mode: 'lines',
      line: { color: ropPolyColors.legacyEdge, width: 2 },
      showlegend: false,
      hoverinfo: 'skip',
    });
  });

  if ((vertices || []).length > 0) {
    const vertexRO1 = vertices.map(v => v.ro1);
    const vertexRO2 = vertices.map(v => v.ro2);
    const hoverText = vertices.map(v => `Vertex ${v.idx}<br>Nullity: ${v.nullity}<br>Perm: [${v.perm.join(',')}]`);
    traces.push({
      x: vertexRO1,
      y: vertexRO2,
      mode: 'markers',
      marker: {
        color: ropPolyColors.legacyVertex,
        size: 8,
        line: { color: plotTheme.nodeOutlineColor, width: 1 },
      },
      name: 'Vertices',
      hovertext: hoverText,
      hoverinfo: 'text',
    });
  }

  const layout = {
    autosize: true,
    margin: { t: 40, b: 60, l: 70, r: 20 },
    title: { text: `ROP Polyhedron: ${output_expr}`, font: { color: plotTheme.titleColor, size: 11 }, y: 0.98, yanchor: 'top' },
    xaxis: { title: `∂log(${output_expr})/∂log(${param1_symbol})` },
    yaxis: { title: `∂log(${output_expr})/∂log(${param2_symbol})` },
    showlegend: true,
  };

  Plotly.newPlot(plotId, traces, applyPlotLayoutTheme(layout), { responsive: true, displayModeBar: false, scrollZoom: true });
}

// ===== Tab Navigation =====
function setupTabNavigation(nodeId) {
  const node = document.getElementById(nodeId);
  if (!node) return;

  const tabButtons = node.querySelectorAll('.tab-btn');
  const tabContents = node.querySelectorAll('.tab-content');

  tabButtons.forEach(btn => {
    btn.addEventListener('click', () => {
      const targetTab = btn.getAttribute('data-tab');

      // Remove active class from all buttons and contents
      tabButtons.forEach(b => b.classList.remove('active'));
      tabContents.forEach(c => c.classList.remove('active'));

      // Add active class to clicked button and corresponding content
      btn.classList.add('active');
      const targetContent = node.querySelector(`.tab-content[data-tab="${targetTab}"]`);
      if (targetContent) {
        targetContent.classList.add('active');
      }
    });
  });
}

// ===== Auto-update Config Nodes =====
function setupAutoUpdate(nodeId, nodeType) {
  const node = document.getElementById(nodeId);
  if (!node) return;

  // Save initial default config
  triggerConfigUpdate(nodeId, nodeType);

  // Find all inputs with auto-update class
  const inputs = node.querySelectorAll('.auto-update');

  inputs.forEach(input => {
    const eventType = input.tagName === 'SELECT' ? 'change' :
                      input.type === 'checkbox' ? 'change' : 'input';

    input.addEventListener(eventType, () => {
      // Debounce for text inputs
      if (input.type === 'text' || input.type === 'number') {
        clearTimeout(input._autoUpdateTimer);
        input._autoUpdateTimer = setTimeout(() => {
          triggerConfigUpdate(nodeId, nodeType);
        }, 500);
      } else {
        // Immediate for selects and checkboxes
        triggerConfigUpdate(nodeId, nodeType);
      }
    });
  });
}

function triggerConfigUpdate(nodeId, nodeType) {
  // Store config in node data
  const info = nodeRegistry[nodeId];
  if (!info) return;

  info.data = info.data || {};
  info.data.config = getNodeSerialData(nodeId, nodeType);
  if (nodeType === 'atlas-query-config') refreshAtlasQueryDesigner(nodeId);
}


// ===== Auto-build Model =====
function setupAutoModelBuild(nodeId) {
  // Check if there's a connection to reaction-network
  const checkAndBuild = () => {
    const existingContext = nodeRegistry[nodeId]?.data?.modelContext;
    if (existingContext?.sessionId && nodeRegistry[nodeId]?.data?.built !== false) {
      return;
    }
    const rxConn = connections.find(c => c.toNode === nodeId && c.toPort === 'reactions');
    if (rxConn) {
      const { reactions } = getReactionsFromNode(rxConn.fromNode);
      if (reactions.length > 0) {
        // Valid reactions exist, auto-build
        setTimeout(() => buildModel(nodeId), 100);
      }
    }
  };

  // Initial check
  checkAndBuild();

  // Store the check function for later use
  if (!nodeRegistry[nodeId]) return;
  nodeRegistry[nodeId]._autoBuildCheck = checkAndBuild;
}

// Trigger auto-build when reactions change
function triggerAutoModelBuild(reactionNodeId) {
  // Find all connected model-builder nodes
  const modelBuilders = connections
    .filter(c => c.fromNode === reactionNodeId && c.fromPort === 'reactions')
    .map(c => c.toNode);

  modelBuilders.forEach(mbId => {
    const info = nodeRegistry[mbId];
    if (info && info._autoBuildCheck) {
      // Debounce the build
      clearTimeout(info._autoBuildTimer);
      info._autoBuildTimer = setTimeout(() => {
        info._autoBuildCheck();
      }, 500);
    }
  });
}

function triggerAllAutoModelBuilds() {
  Object.entries(nodeRegistry).forEach(([nodeId, info]) => {
    if (info.type !== 'model-builder' || !info._autoBuildCheck) return;
    info._autoBuildCheck();
  });
}

function isRunnableNode(nodeId) {
  const info = nodeRegistry[nodeId];
  if (!info) return false;
  return typeof NODE_TYPES[info.type]?.execute === 'function';
}

function connectedNodeIDs() {
  const ids = new Set();
  connections.forEach(conn => {
    ids.add(conn.fromNode);
    ids.add(conn.toNode);
  });
  return ids;
}

async function runConnectedWorkspace() {
  const connectedIDs = connectedNodeIDs();
  if (!connectedIDs.size) {
    showToast('No connected nodes to run');
    return;
  }

  const runnableIDs = Array.from(connectedIDs).filter(isRunnableNode);
  if (!runnableIDs.length) {
    showToast('No connected executable nodes found');
    return;
  }

  const remaining = new Set(runnableIDs);
  let ranCount = 0;

  while (remaining.size) {
    let progressed = false;

    for (const nodeId of Array.from(remaining)) {
      const runnableDeps = connections
        .filter(conn => conn.toNode === nodeId)
        .map(conn => conn.fromNode)
        .filter(isRunnableNode);

      if (runnableDeps.some(depId => remaining.has(depId))) {
        continue;
      }

      remaining.delete(nodeId);
      progressed = true;

      const info = nodeRegistry[nodeId];
      if (!info) continue;

      try {
        if (info.type === 'model-builder' && info.data?.modelContext?.sessionId) {
          ranCount += 1;
          continue;
        }
        await NODE_TYPES[info.type].execute(nodeId);
        ranCount += 1;
      } catch (error) {
        console.error(`Run Connected failed at ${nodeId} (${info.type})`, error);
        showToast(`Run Connected failed at ${NODE_TYPES[info.type]?.title || info.type}`);
        return;
      }
    }

    if (!progressed) {
      console.warn('Run Connected stopped due to unresolved dependency cycle', Array.from(remaining));
      break;
    }
  }

  commitWorkspaceSnapshot('run-connected');
  showToast(`Ran ${ranCount} connected node${ranCount === 1 ? '' : 's'}`);
}

function refreshThemeAwarePlots() {
  Object.entries(nodeRegistry).forEach(([nodeId, info]) => {
    const nodeData = info?.data || {};

    try {
      switch (info.type) {
        case 'regime-graph':
          if (nodeData.graphData && document.getElementById(`${nodeId}-plot`)) {
            plotRegimeGraph(nodeData.graphData, `${nodeId}-plot`, { viewMode: nodeData.config?.viewMode || '3d' });
          }
          break;

        case 'siso-result':
          if (nodeData.trajectoryData && document.getElementById(`${nodeId}-traj-plot`)) {
            plotTrajectory(nodeData.trajectoryData, `${nodeId}-traj-plot`);
          }
          break;

        case 'qk-poly-result': {
          const poly = nodeData.polyhedronPayload?.polyhedra?.[0];
          const qkSymbols = nodeData.polyhedronPayload?.qk_symbols || [];
          if (poly && document.getElementById(`${nodeId}-plot`)) {
            plotQKPolyhedron(poly, qkSymbols, `${nodeId}-plot`);
          }
          break;
        }

        case 'scan-1d-result':
        case 'parameter-scan-1d':
          if (nodeData.scan1DResult && document.getElementById(`${nodeId}-plot`)) {
            plotParameterScan1D(nodeData.scan1DResult, `${nodeId}-plot`);
          }
          break;

        case 'rop-cloud':
        case 'rop-cloud-result':
          if (nodeData.ropCloudData && document.getElementById(`${nodeId}-plot`)) {
            plotROPCloud(nodeData.ropCloudData, `${nodeId}-plot`, { ranges: nodeData.ropCloudRanges });
          }
          break;

        case 'fret-result':
        case 'fret-heatmap':
          if (nodeData.fretHeatmapData && document.getElementById(`${nodeId}-plot`)) {
            plotHeatmap(nodeData.fretHeatmapData, `${nodeId}-plot`);
          }
          break;

        case 'scan-2d-result':
        case 'parameter-scan-2d':
          if (nodeData.scan2DResult && document.getElementById(`${nodeId}-plot`)) {
            plotParameterScan2D(nodeData.scan2DResult, `${nodeId}-plot`);
          }
          break;

        case 'rop-poly-result':
        case 'rop-polyhedron':
          if (nodeData.ropPlotData && document.getElementById(`${nodeId}-plot`)) {
            plotROPPolyhedron(nodeData.ropPlotData, `${nodeId}-plot`, { fitInnerPoints: nodeData.fitInnerPoints });
          }
          break;

        default:
          break;
      }
    } catch (error) {
      console.warn('Failed to refresh themed plot', info.type, error);
    }
  });
}

function installThemeChangeObserver() {
  void applyThemeMode(storedThemeMode(), { persist: false, refreshPlots: false });
  if (!colorSchemeMediaQuery) return;
  const rerender = () => {
    if (themeState.mode !== 'auto') return;
    void applyThemeMode('auto', { persist: false, refreshPlots: true });
  };

  if (typeof colorSchemeMediaQuery.addEventListener === 'function') {
    colorSchemeMediaQuery.addEventListener('change', rerender);
  } else if (typeof colorSchemeMediaQuery.addListener === 'function') {
    colorSchemeMediaQuery.addListener(rerender);
  }
}

function installWorkspaceShellObservers() {
  const queueSync = (reason) => queueWorkspaceShellSync(reason);

  ['input', 'change', 'keyup', 'mouseup'].forEach((eventName) => {
    document.addEventListener(eventName, () => queueSync(eventName), true);
  });

  document.addEventListener('click', () => {
    window.requestAnimationFrame(() => queueSync('click'));
  }, true);

  window.setInterval(() => queueSync('poll'), 1500);
}

installWorkspaceShellObservers();
installThemeChangeObserver();
window.BiocircuitsExplorerWorkspaceShell.markReady();
