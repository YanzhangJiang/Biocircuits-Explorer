import { api, escapeHtml, renderNodeError } from '../api.js';
import { stableJson } from '../stable-json.js';
import {
  ensureModelSession,
  getModelContextForNode,
  getModelForNode,
  hasModelContextForNode,
  setNodeLoading,
  setupAutoUpdate,
} from '../nodes.js';
import {
  connections,
  getWorkspaceRuntimeEpoch,
  nodeRegistry,
} from '../state.js';
import {
  begin as beginLifecycle,
  commit as commitLifecycle,
  createExecutionLifecycle,
  fail as failLifecycle,
  inspectExecutionLifecycle,
  invalidate as invalidateLifecycle,
  isCurrent as isCurrentLifecycle,
  readCurrentResult,
  release as releaseLifecycle,
  serializeExecutionLifecycle,
} from '../execution-lifecycle-core.js';
import {
  executeRegimeGraph,
  installRegimeGraphInvalidation,
  updateRegimeGraphMode,
} from '../regime-graph.js';

const VERTICES_TABLE_ENDPOINT = '/api/v1/find_vertices';
const VERTICES_TABLE_LIFECYCLE_KEY = '_verticesTableExecutionLifecycle';

function verticesUpstreamSignature(nodeId) {
  const visited = new Set();
  const edgeKeys = new Set();
  const visit = (current) => {
    if (!current || visited.has(current)) return;
    visited.add(current);
    for (const connection of connections) {
      if (connection?.toNode !== current) continue;
      edgeKeys.add([
        connection.fromNode,
        connection.fromPort,
        connection.toNode,
        connection.toPort,
      ].map(value => String(value ?? '')).join('\u0000'));
      visit(connection.fromNode);
    }
  };
  visit(nodeId);
  return JSON.stringify([...edgeKeys].sort());
}

function verticesModelIdentity(nodeId) {
  const context = getModelContextForNode(nodeId);
  if (!context) return null;
  return {
    networkIrHash: context.networkIrHash || context.network_ir_hash || null,
    inputFingerprint: context.inputFingerprint || null,
    builtForRevision: context.builtForRevision ?? null,
  };
}

function verticesBuilderIdentity(nodeId) {
  const visited = new Set();
  const findBuilder = (current) => {
    if (!current || visited.has(current)) return null;
    visited.add(current);
    const info = nodeRegistry[current];
    if (info?.type === 'model-builder') {
      return {
        nodeId: current,
        inputRevision: Number.isSafeInteger(info._modelInputRevision)
          ? info._modelInputRevision
          : 0,
      };
    }
    for (const connection of connections) {
      if (connection?.toNode !== current) continue;
      const builder = findBuilder(connection.fromNode);
      if (builder) return builder;
    }
    return null;
  };
  return findBuilder(nodeId);
}

function verticesInputFingerprint(nodeId, model = verticesModelIdentity(nodeId)) {
  return stableJson({
    model,
    upstream: verticesUpstreamSignature(nodeId),
  });
}

function verticesExecutionContext(nodeId) {
  const owner = nodeRegistry[nodeId];
  const model = verticesModelIdentity(nodeId);
  if (!owner || !model) return null;
  return {
    owner,
    workspaceEpoch: getWorkspaceRuntimeEpoch(),
    inputFingerprint: verticesInputFingerprint(nodeId, model),
    endpoint: VERTICES_TABLE_ENDPOINT,
  };
}

function verticesPreflightContext(nodeId) {
  const owner = nodeRegistry[nodeId];
  if (!owner) return null;
  return {
    owner,
    workspaceEpoch: getWorkspaceRuntimeEpoch(),
    inputFingerprint: stableJson({
      builder: verticesBuilderIdentity(nodeId),
      upstream: verticesUpstreamSignature(nodeId),
    }),
    endpoint: VERTICES_TABLE_ENDPOINT,
  };
}

function verticesLifecycleFor(owner) {
  if (!owner[VERTICES_TABLE_LIFECYCLE_KEY]) {
    owner[VERTICES_TABLE_LIFECYCLE_KEY] = createExecutionLifecycle();
  }
  return owner[VERTICES_TABLE_LIFECYCLE_KEY];
}

function syncVerticesLifecycle(owner) {
  const lifecycle = owner?.[VERTICES_TABLE_LIFECYCLE_KEY];
  if (!owner || !lifecycle) return;
  owner.data = owner.data || {};
  owner.data.lifecycle = serializeExecutionLifecycle(lifecycle);
}

function beginVerticesTableExecution(nodeId, context) {
  const owner = nodeRegistry[nodeId];
  if (!owner || owner.type !== 'vertices-table' || context?.owner !== owner) return null;
  setNodeLoading(nodeId, false);
  const lifecycle = verticesLifecycleFor(owner);
  const ticket = beginLifecycle(lifecycle, context);
  syncVerticesLifecycle(owner);
  return ticket;
}

function isCurrentVerticesTableExecution(nodeId, ticket, context = null) {
  const owner = nodeRegistry[nodeId];
  const lifecycle = owner?.[VERTICES_TABLE_LIFECYCLE_KEY];
  const currentContext = context || verticesPreflightContext(nodeId);
  return !!ticket && !!lifecycle && owner === ticket.owner && !!currentContext &&
    isCurrentLifecycle(lifecycle, ticket, currentContext);
}

function commitVerticesTableExecution(nodeId, ticket, context, result) {
  const owner = nodeRegistry[nodeId];
  const lifecycle = owner?.[VERTICES_TABLE_LIFECYCLE_KEY];
  if (!lifecycle || owner !== ticket?.owner) return false;
  const committed = commitLifecycle(lifecycle, ticket, {
    context,
    result,
    evidence: {
      class: 'current-computation',
      endpoint: VERTICES_TABLE_ENDPOINT,
      vertexCount: Array.isArray(result?.vertices) ? result.vertices.length : 0,
    },
  });
  syncVerticesLifecycle(owner);
  return committed;
}

function failVerticesTableExecution(nodeId, ticket, context, error) {
  const owner = nodeRegistry[nodeId];
  const lifecycle = owner?.[VERTICES_TABLE_LIFECYCLE_KEY];
  if (!lifecycle || owner !== ticket?.owner) return false;
  const failed = failLifecycle(lifecycle, ticket, { context, error });
  syncVerticesLifecycle(owner);
  return failed;
}

function releaseVerticesTableExecution(nodeId, ticket) {
  const owner = nodeRegistry[nodeId];
  const lifecycle = owner?.[VERTICES_TABLE_LIFECYCLE_KEY];
  if (!lifecycle || owner !== ticket?.owner) return false;
  const released = releaseLifecycle(lifecycle, ticket);
  syncVerticesLifecycle(owner);
  setNodeLoading(nodeId, inspectExecutionLifecycle(lifecycle).loading);
  return released;
}

export function inspectVerticesTableExecution(nodeId) {
  const lifecycle = nodeRegistry[nodeId]?.[VERTICES_TABLE_LIFECYCLE_KEY];
  return lifecycle ? inspectExecutionLifecycle(lifecycle) : null;
}

export function readCurrentVerticesTableResult(nodeId) {
  const lifecycle = nodeRegistry[nodeId]?.[VERTICES_TABLE_LIFECYCLE_KEY];
  return lifecycle ? readCurrentResult(lifecycle) : null;
}

export function invalidateVerticesTableExecution(
  nodeId,
  reason = 'vertices-input-changed',
) {
  const owner = nodeRegistry[nodeId];
  const lifecycle = owner?.[VERTICES_TABLE_LIFECYCLE_KEY];
  if (!lifecycle) return false;
  const runtime = inspectExecutionLifecycle(lifecycle);
  if (runtime.owner !== owner || runtime.workspaceEpoch == null) return false;
  const invalidated = invalidateLifecycle(lifecycle, {
    owner,
    workspaceEpoch: runtime.workspaceEpoch,
    reason,
  });
  if (!invalidated) return false;
  syncVerticesLifecycle(owner);
  setNodeLoading(nodeId, false);
  const contentEl = document.getElementById(`${nodeId}-content`);
  if (contentEl) {
    contentEl.innerHTML = '<span class="text-dim">Inputs changed — recompute the vertices.</span>';
  }
  return true;
}

function invalidateOwnedVerticesTicket(nodeId, ticket, reason) {
  const runtime = inspectVerticesTableExecution(nodeId);
  if (nodeRegistry[nodeId] !== ticket?.owner || runtime?.currentTicket !== ticket) return false;
  return invalidateVerticesTableExecution(nodeId, reason);
}

function renderVerticesTable(data) {
  if (!Array.isArray(data?.vertices)) {
    throw new Error('Backend returned an invalid vertices payload.');
  }
  let html = '<table><thead><tr><th>#</th><th>Perm</th><th>Species</th><th>Type</th><th>Nullity</th></tr></thead><tbody>';
  data.vertices.forEach((vertex) => {
    const typeTag = vertex.asymptotic
      ? '<span class="tag tag-asym">Asymp</span>'
      : '<span class="tag tag-nonasym">Non-A</span>';
    const singTag = vertex.singular
      ? ' <span class="tag tag-singular">Sing</span>'
      : ' <span class="tag tag-invertible">Inv</span>';
    const speciesStr = Array.isArray(vertex.species) ? vertex.species.join(', ') : '';
    const permutation = Array.isArray(vertex.perm) ? vertex.perm.join(',') : '';
    html += `<tr><td>${escapeHtml(vertex.idx)}</td><td>[${escapeHtml(permutation)}]</td><td style="font-family:monospace;font-size:10px;">${escapeHtml(speciesStr)}</td><td>${typeTag}${singTag}</td><td>${escapeHtml(vertex.nullity)}</td></tr>`;
  });
  return `${html}</tbody></table>`;
}

export async function executeVerticesTable(nodeId) {
  let context = verticesPreflightContext(nodeId);
  let ticket = beginVerticesTableExecution(nodeId, context);
  if (!ticket) return false;
  const contentEl = document.getElementById(`${nodeId}-content`);
  if (contentEl) contentEl.innerHTML = '<span class="text-dim">Computing vertices...</span>';
  setNodeLoading(nodeId, true);
  let requestIsCurrent = null;

  try {
    const sessionId = await ensureModelSession(nodeId);
    if (!isCurrentVerticesTableExecution(nodeId, ticket, verticesPreflightContext(nodeId))) {
      invalidateOwnedVerticesTicket(nodeId, ticket, 'vertices-context-changed-before-request');
      return false;
    }

    context = verticesExecutionContext(nodeId);
    ticket = beginVerticesTableExecution(nodeId, context);
    if (!ticket) return false;
    setNodeLoading(nodeId, true);
    requestIsCurrent = () => {
      const currentContext = verticesExecutionContext(nodeId);
      return !!currentContext && isCurrentVerticesTableExecution(nodeId, ticket, currentContext);
    };

    const data = await api('find_vertices', { session_id: sessionId }, {
      statusIsCurrent: requestIsCurrent,
    });
    if (!requestIsCurrent()) {
      invalidateOwnedVerticesTicket(nodeId, ticket, 'vertices-context-changed-during-request');
      return false;
    }

    const html = renderVerticesTable(data);
    const commitContext = verticesExecutionContext(nodeId);
    if (!commitContext || !commitVerticesTableExecution(nodeId, ticket, commitContext, data)) {
      return false;
    }
    const liveContent = document.getElementById(`${nodeId}-content`);
    if (!liveContent || nodeRegistry[nodeId] !== ticket.owner) return false;
    liveContent.innerHTML = html;
    return true;
  } catch (error) {
    if (requestIsCurrent && !requestIsCurrent()) {
      invalidateOwnedVerticesTicket(nodeId, ticket, 'vertices-context-changed-during-request');
      return false;
    }
    const failureContext = requestIsCurrent
      ? verticesExecutionContext(nodeId)
      : verticesPreflightContext(nodeId);
    if (!failureContext ||
        !failVerticesTableExecution(nodeId, ticket, failureContext, error)) return false;
    const liveContent = document.getElementById(`${nodeId}-content`);
    renderNodeError(liveContent, error);
    return false;
  } finally {
    releaseVerticesTableExecution(nodeId, ticket);
  }
}

export const RESULT_TYPES = {
  'model-summary': {
    category: 'result',
    headerClass: 'header-result',
    title: 'Model Summary',
    inputs: [{ port: 'model', type: 'ModelArtifact', label: 'Model' }],
    outputs: [],
    defaultWidth: 300,
    createBody(nodeId) {
      return `<div class="viewer-content" id="${nodeId}-content"><span class="text-dim">Connect to a Model Builder to see summary.</span></div>`;
    },
    async execute(nodeId) {
      const contentEl = document.getElementById(`${nodeId}-content`);
      const m = getModelForNode(nodeId);
      if (!m) { contentEl.innerHTML = '<span class="text-dim">No model built yet.</span>'; return false; }
      contentEl.innerHTML = `
        <table>
          <tr><th>Property</th><th>Value</th></tr>
          <tr><td>Species (n)</td><td>${escapeHtml(m.n)}</td></tr>
          <tr><td>Totals (d)</td><td>${escapeHtml(m.d)}</td></tr>
          <tr><td>Reactions (r)</td><td>${escapeHtml(m.r)}</td></tr>
          <tr><td>Species</td><td>${escapeHtml(m.x_sym.join(', '))}</td></tr>
          <tr><td>Totals</td><td>${escapeHtml(m.q_sym.join(', '))}</td></tr>
          <tr><td>Constants</td><td>${escapeHtml(m.K_sym.join(', '))}</td></tr>
        </table>
        <div style="margin-top:8px;"><strong>N matrix:</strong></div>
        <pre style="font-size:10px;color:#aaa;margin:4px 0;">${escapeHtml(m.N.map(r => r.map(v => String(v).padStart(3)).join(' ')).join('\n'))}</pre>
        <div><strong>L matrix:</strong></div>
        <pre style="font-size:10px;color:#aaa;margin:4px 0;">${escapeHtml(m.L.map(r => r.map(v => String(v).padStart(3)).join(' ')).join('\n'))}</pre>
      `;
      return true;
    },
  },
  'vertices-table': {
    category: 'result',
    headerClass: 'header-result',
    title: 'Vertices Table',
    inputs: [{ port: 'model', type: 'ModelArtifact', label: 'Model' }],
    outputs: [],
    defaultWidth: 380,
    createBody(nodeId) {
      return `<div class="viewer-content" id="${nodeId}-content"><span class="text-dim">Waiting for model...</span></div>`;
    },
    async execute(nodeId) {
      return executeVerticesTable(nodeId);
    },
  },
  'regime-graph': {
    category: 'result',
    headerClass: 'header-result',
    title: 'Regime Graph',
    inputs: [{ port: 'model', type: 'ModelArtifact', label: 'Model' }],
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
      installRegimeGraphInvalidation(nodeId);
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
      return executeRegimeGraph(nodeId);
    },
  },
};
