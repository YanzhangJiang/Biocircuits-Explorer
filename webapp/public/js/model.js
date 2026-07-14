// Biocircuits Explorer — Model Building & Reaction Functions

import { state, nodeRegistry, connections } from './state.js';
import { api, showToast, handleNodeError } from './api.js';
import { triggerAutoModelBuild } from './nodes.js';
import { setNodeLoading } from './node-loading.js';
import { NODE_TYPES } from './node-types/index.js';
import { commitWorkspaceSnapshot } from './workspace.js';
import {
  MODEL_BUILD_ENDPOINT,
  beginModelBuild,
  blockModelBuild,
  commitModelBuild,
  failModelBuild,
  invalidateModelBuilder,
  isCurrentModelBuild,
  modelBuildContext,
  modelInputRevision,
  ownsCurrentModelBuild,
  readCurrentModelBuildResult,
  releaseModelBuild,
} from './model-lifecycle.js';
import { executionDependencyConnections } from './execution-lifecycle.js';

// ===== Reaction Editor =====
export function getReactionsFromNode(nodeId) {
  const info = nodeRegistry[nodeId];
  // Identity-defined reaction sources publish their rules as
  // config.resolvedDefinition.raw_rules instead of DOM reaction rows:
  //  - network-id-definition: resolved from a compressed atlas id
  //  - design-target:         the candidate network the user selected from the screen
  if (info?.type === 'network-id-definition' || info?.type === 'design-target') {
    if (info.type === 'design-target') {
      const selectionLifecycle = info.data?.selectionLifecycle;
      if (selectionLifecycle?.state !== 'current' || selectionLifecycle?.freshness !== 'current') {
        return { reactions: [], kds: [] };
      }
    }
    const config = info.data?.config || {};
    const resolved = config.resolvedDefinition || null;
    const reactions = Array.isArray(resolved?.raw_rules)
      ? resolved.raw_rules.map(rule => String(rule))
      : [];
    return {
      reactions,
      kds: reactions.map(() => 1),
    };
  }

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

  const reactionInput = document.createElement('input');
  reactionInput.type = 'text';
  reactionInput.className = 'reaction-input';
  reactionInput.value = String(rule ?? '');
  reactionInput.placeholder = 'A + B <-> C';

  const kdInput = document.createElement('input');
  kdInput.type = 'number';
  kdInput.className = 'kd-input';
  kdInput.value = kd == null ? '' : String(kd);
  kdInput.step = 'any';
  kdInput.min = '1e-12';
  kdInput.placeholder = 'required';

  const removeBtn = document.createElement('button');
  removeBtn.className = 'btn-remove';
  removeBtn.title = 'Remove';
  removeBtn.textContent = '\u00d7';

  row.append(reactionInput, kdInput, removeBtn);

  removeBtn.onclick = () => {
    row.remove();
    triggerAutoModelBuild(nodeId);
  };

  // Add event listeners for auto-build
  [reactionInput, kdInput].forEach(input => {
    input.addEventListener('input', () => {
      triggerAutoModelBuild(nodeId, { delay: 1000 });
    });
  });

  list.appendChild(row);
}

// ===== Build Model =====
function captureModelBuildInput(modelBuilderNodeId, connectionResolver = executionDependencyConnections) {
  const dependencyConnections = typeof connectionResolver === 'function'
    ? connectionResolver()
    : connections;
  const conn = dependencyConnections
    .find(c => c.toNode === modelBuilderNodeId && c.toPort === 'reactions');
  const sourceNodeId = conn?.fromNode || null;
  const { reactions, kds } = sourceNodeId
    ? getReactionsFromNode(sourceNodeId)
    : { reactions: [], kds: [] };
  return {
    sourceNodeId,
    reactions,
    kds,
    fingerprint: JSON.stringify([sourceNodeId, reactions, kds]),
  };
}

export async function buildModel(modelBuilderNodeId, options = {}) {
  const shouldTriggerDownstream = options.triggerDownstream !== false;
  const throwOnFailure = options.throwOnFailure === true;
  const connectionResolver = options.connectionResolver || executionDependencyConnections;
  const input = captureModelBuildInput(modelBuilderNodeId, connectionResolver);
  const existingContext = readCurrentModelBuildResult(modelBuilderNodeId);
  if (existingContext?.inputFingerprint && existingContext.inputFingerprint !== input.fingerprint) {
    invalidateModelBuilder(modelBuilderNodeId, 'model-input-fingerprint-changed');
  }
  const beginContext = modelBuildContext(
    modelBuilderNodeId,
    input.fingerprint,
    MODEL_BUILD_ENDPOINT,
  );
  const ticket = beginContext ? beginModelBuild(modelBuilderNodeId, beginContext) : null;
  if (!ticket) {
    const message = 'Model Builder is no longer available';
    showToast(message);
    if (throwOnFailure) throw new Error(message);
    return false;
  }
  const currentAttempt = () => {
    const currentInput = captureModelBuildInput(modelBuilderNodeId, connectionResolver);
    return {
      input: currentInput,
      context: modelBuildContext(
        modelBuilderNodeId,
        currentInput.fingerprint,
        MODEL_BUILD_ENDPOINT,
      ),
    };
  };
  const requestIsCurrent = () => {
    const current = currentAttempt();
    return current.input.fingerprint === beginContext.inputFingerprint &&
      !!current.context && isCurrentModelBuild(modelBuilderNodeId, ticket, current.context);
  };
  let preflightError = null;
  const block = (message) => {
    const current = currentAttempt();
    if (!current.context || !blockModelBuild(
      modelBuilderNodeId,
      ticket,
      current.context,
      message,
    )) return false;
    showToast(message);
    if (throwOnFailure) {
      preflightError = new Error(message);
      throw preflightError;
    }
    return false;
  };

  setNodeLoading(modelBuilderNodeId, true);
  try {
    if (!input.sourceNodeId) return block('Model Builder has no reaction source connected');
    const { reactions, kds } = input;
    if (reactions.length === 0) return block('Add at least one reaction');
    if (kds.some(kd => kd == null || kd <= 0)) {
      return block('Model Builder requires Kd for every reaction (> 0)');
    }

    const data = await api('build_model', { reactions, kd: kds }, { statusIsCurrent: requestIsCurrent });
    if (!ownsCurrentModelBuild(modelBuilderNodeId, ticket)) return false;
    const current = currentAttempt();
    if (current.input.fingerprint !== beginContext.inputFingerprint) {
      invalidateModelBuilder(modelBuilderNodeId, 'model-input-fingerprint-changed-during-build');
      return false;
    }
    if (!current.context) return false;
    const modelContext = {
      sessionId: data.session_id,
      // The NetworkIR hash is the content-addressed identity of this model; the
      // session id is just a cache handle over it. Kept for provenance and so a
      // rebuild can be requested by hash.
      networkIrHash: data.network_ir_hash || null,
      networkIr: data.network_ir || null,
      // bne-result envelope: provenance (algorithm/version, input hashes) for
      // this model build. Surfaced for display and as a client-side cache key.
      artifact: data.artifact || null,
      model: data,
      qK_syms: [...data.q_sym, ...data.K_sym],
      builtForRevision: modelInputRevision(modelBuilderNodeId),
      inputFingerprint: beginContext.inputFingerprint,
      sourceNodeId: input.sourceNodeId,
    };
    if (!commitModelBuild(modelBuilderNodeId, ticket, current.context, {
      modelContext,
      evidence: {
        source_endpoint: MODEL_BUILD_ENDPOINT,
        network_ir_hash: data.network_ir_hash || null,
      },
    })) return false;
    const liveInfo = nodeRegistry[modelBuilderNodeId];
    if (!liveInfo || liveInfo !== ticket.owner) return false;
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

    showToast('Model built successfully');
    commitWorkspaceSnapshot('model-built');

    // Trigger all downstream viewers
    if (shouldTriggerDownstream) {
      onModelBuilt(modelBuilderNodeId);
    }
    return true;
  } catch (e) {
    if (e === preflightError) throw e;
    if (!ownsCurrentModelBuild(modelBuilderNodeId, ticket)) return false;
    const current = currentAttempt();
    if (current.input.fingerprint !== beginContext.inputFingerprint) {
      invalidateModelBuilder(modelBuilderNodeId, 'model-input-fingerprint-changed-during-build');
      return false;
    }
    if (!current.context || !failModelBuild(modelBuilderNodeId, ticket, current.context, e)) {
      return false;
    }
    handleNodeError(e, modelBuilderNodeId, 'Build model');
    if (throwOnFailure) {
      throw e;
    }
    return false;
  } finally {
    if (releaseModelBuild(modelBuilderNodeId, ticket)) {
      setNodeLoading(modelBuilderNodeId, false);
    }
  }
}

// ===== Downstream UI preparation =====
export function triggerDownstreamNodes(fromNodeId, fromPort) {
  const downstream = connections.filter(c => c.fromNode === fromNodeId && c.fromPort === fromPort);
  for (const conn of downstream) {
    const viewerInfo = nodeRegistry[conn.toNode];
    if (!viewerInfo) continue;
    const typeDef = NODE_TYPES[viewerInfo.type];
    if (typeDef && typeDef.prepare) {
      // Mark input wires as transmitting
      const wireId = `wire-${conn.fromNode}-${conn.toNode}`;
      const wire = document.getElementById(wireId);
      if (wire) wire.classList.add('transmitting');

      // Preparation is UI/config-only. Scientific computation is owned by the
      // serial workflow scheduler or the node's explicit Run action.
      typeDef.prepare(conn.toNode).catch(e => {
        handleNodeError(e, conn.toNode, `Prepare ${viewerInfo.type}`);
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
