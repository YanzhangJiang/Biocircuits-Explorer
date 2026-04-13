// Biocircuits Explorer — Model Building & Reaction Functions

import { state, nodeRegistry, connections } from './state.js';
import { api, showToast, handleNodeError, escapeHtml } from './api.js';
import { setNodeLoading, triggerAutoModelBuild } from './nodes.js';
import { NODE_TYPES } from './node-types/index.js';
import { commitWorkspaceSnapshot } from './workspace.js';

// ===== Reaction Editor =====
export function getReactionsFromNode(nodeId) {
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

export function addReactionRow(nodeId, rule = '', kd = 1e-3) {
  const list = document.getElementById(`${nodeId}-reactions-list`);
  if (!list) return;
  const row = document.createElement('div');
  row.className = 'reaction-row';
  row.innerHTML = `
    <input type="text" class="reaction-input" value="${rule}" placeholder="A + B <-> C">
    <input type="number" class="kd-input" value="${kd == null ? '' : kd}" step="any" min="1e-12" placeholder="required">
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
export async function buildModel(modelBuilderNodeId, options = {}) {
  const shouldTriggerDownstream = options.triggerDownstream !== false;
  const throwOnFailure = options.throwOnFailure === true;
  const fail = (message) => {
    showToast(message);
    if (throwOnFailure) {
      throw new Error(message);
    }
    return false;
  };
  // Find connected reaction-network
  const conn = connections.find(c => c.toNode === modelBuilderNodeId && c.toPort === 'reactions');
  if (!conn) {
    return fail('Model Builder has no Reaction Network connected');
  }
  const rnNodeId = conn.fromNode;
  const { reactions, kds } = getReactionsFromNode(rnNodeId);
  if (reactions.length === 0) {
    return fail('Add at least one reaction');
  }
  if (kds.some(kd => kd == null || kd <= 0)) {
    return fail('Model Builder requires Kd for every reaction (> 0)');
  }

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
    return true;
  } catch (e) {
    handleNodeError(e, modelBuilderNodeId, 'Build model');
    if (throwOnFailure) {
      throw e;
    }
    return false;
  } finally {
    setNodeLoading(modelBuilderNodeId, false);
  }
}

// ===== Downstream Viewer Auto-Execution =====
export function triggerDownstreamNodes(fromNodeId, fromPort) {
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
        handleNodeError(e, conn.toNode, `Downstream ${viewerInfo.type}`);
      }).finally(() => {
        // Remove transmitting state immediately after execution completes
        if (wire) wire.classList.remove('transmitting');
      });
    }
  }
}

export async function onModelBuilt(modelBuilderNodeId) {
  triggerDownstreamNodes(modelBuilderNodeId, 'model');
}
