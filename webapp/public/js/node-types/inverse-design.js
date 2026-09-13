// Typed inverse design: authored target → supernetwork optimization → designed network.
// Saved outputs are historical; only the live lifecycle can supply a network.
import { escapeHtml } from '../api.js';
import { runLocalDesignJob } from '../design-network-job.js';
import { compileDesignTarget } from '../design-target-agent.js';
import { validateDesignTarget } from '../design-target-adapters.js';
import { renderTargetEditor, installTargetEditor, readEditorTarget, readTrainingTarget, synchronizeTrainingTarget, refreshTargetEditor, resetTargetExample, clearTargetDrawing } from '../design-target-editor.js';
import { makeTargetDrawing, readTargetDrawing } from '../design-target-drawing.js';
import { ensureNodeData, getWorkspaceRuntimeEpoch, nodeRegistry } from '../state.js';
import { setupAutoUpdate } from '../nodes.js';
import { setNodeLoading } from '../node-loading.js';
import { commitWorkspaceSnapshot } from '../workspace.js';
import { ChangeAttrCommand, CompoundCommand, dispatch } from '../commands.js';
import { stableJson } from '../stable-json.js';
import { executionDependencyConnections } from '../execution-lifecycle.js';
import { invalidateModelBuildersForReactionSource } from '../model-lifecycle.js';
import {
  begin, block, commit, createExecutionLifecycle, fail, inspectExecutionLifecycle,
  invalidate, isCurrent, readCurrentResult, release, restoreHistorical,
  serializeExecutionLifecycle,
} from '../execution-lifecycle-core.js';
import { blockedOutcome, cancelledOutcome, failedOutcome, staleOutcome, succeededOutcome } from '../execution-outcome.js';
import {
  INVERSE_DESIGN_DEFAULTS, designedNetworkFromResult,
  normalizeInverseDesignResult, validateInverseDesignRequest,
} from '../inverse-design-core.js';
import { renderDesignedNetwork, renderInverseDesignResult, renderInverseDesignProgress, renderInverseDesignPreview, renderDesignTargetErrorFloor, inverseDesignCompletionStatus, recordLearningSample } from '../inverse-design-render.js';

export const INVERSE_DESIGN_ENDPOINT = '/api/v1/design_network';
const TYPES = new Set(['inverse-design-target', 'gradient-design', 'designed-network']);
const TARGET_FIELDS = ['description', 'target-mode', 'target-json', 'drawing-json', 'reference-state', 'target-points', 'image-resolution', 'image-invert', 'aux-monomers', 'max-complex-size', 'max-reactions', 'allow-homomers', 'chemistry-json'];
const OPTIMIZER_FIELDS = ['learning-rate', 'epochs', 'restarts', 'prune-rounds', 'prune-fraction', 'prune-tolerance', 'max-rmse', 'optimize-totals', 'seed'];
const EVIDENCE = Object.freeze({
  evidence_tier: 'sampled_equilibrium_replay', prediction_basis: 'selected_network',
});

function lifecycleFor(info) {
  if (!info._inverseDesignLifecycle) info._inverseDesignLifecycle = createExecutionLifecycle();
  return info._inverseDesignLifecycle;
}

function syncLifecycle(info) {
  if (info?._inverseDesignLifecycle) {
    info.data = info.data || {};
    info.data.lifecycle = serializeExecutionLifecycle(info._inverseDesignLifecycle);
  }
}

function restoredEvidence(restored, inferred) {
  if (Object.hasOwn(restored.lifecycle || {}, 'evidence')) return restored.lifecycle.evidence;
  if (Object.hasOwn(restored, 'evidence')) return restored.evidence;
  return inferred;
}

function field(nodeId, name) {
  const control = document.getElementById(`${nodeId}-${name}`);
  if (!control) throw new Error(`Missing inverse design control: ${name}`);
  return control.value;
}

function editorValues(nodeId, fields) {
  return Object.fromEntries(fields.map(name => [name, field(nodeId, name)]));
}

function numericField(nodeId, name) {
  const value = field(nodeId, name).trim();
  if (!/^[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:e[+-]?\d+)?$/i.test(value)) {
    throw new Error(`${name.replaceAll('-', ' ')} must be a finite decimal number.`);
  }
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) throw new Error(`${name.replaceAll('-', ' ')} must be finite.`);
  return parsed;
}

function connectionSignature(nodeId, graph = executionDependencyConnections()) {
  return graph.filter(connection => connection.toNode === nodeId)
    .map(({ fromNode, fromPort, toNode, toPort }) => ({ fromNode, fromPort, toNode, toPort }))
    .sort((left, right) => stableJson(left).localeCompare(stableJson(right)));
}

function sourceNodeId(nodeId, port, type) {
  const inputs = executionDependencyConnections().filter(connection =>
    connection.toNode === nodeId && connection.toPort === port);
  if (inputs.length !== 1 || nodeRegistry[inputs[0].fromNode]?.type !== type ||
      inputs[0].fromPort !== port) {
    throw new Error(type === 'inverse-design-target'
      ? 'Connect one Inverse Design Target to the request input.'
      : 'Connect one Gradient Design result to the result input.');
  }
  return inputs[0].fromNode;
}

function rawTargetRequest(nodeId) {
  if (nodeRegistry[nodeId]?._designEditorError) throw new Error(nodeRegistry[nodeId]._designEditorError);
  if (nodeRegistry[nodeId]?._legacyInverseTarget) throw new Error('This target was saved with the earlier candidate-library method. Draw, upload, compile, or explicitly edit a new numerical target before running.');
  let extra;
  try { extra = JSON.parse(field(nodeId, 'chemistry-json')); } catch { throw new Error('Additional chemistry constraints must be valid JSON.'); }
  if (!extra || typeof extra !== 'object' || Array.isArray(extra)) throw new Error('Additional chemistry constraints must be an object.');
  return {
    target: readTrainingTarget(nodeId),
    chemistry: {
      ...extra,
      auxiliary_monomers: numericField(nodeId, 'aux-monomers'),
      max_complex_size: numericField(nodeId, 'max-complex-size'),
      max_reactions: numericField(nodeId, 'max-reactions'),
      allow_homomers: field(nodeId, 'allow-homomers') === 'true',
    },
  };
}

function optimizerOptions(nodeId) {
  return Object.fromEntries(OPTIMIZER_FIELDS.map(name => [name.replaceAll('-', '_'), name === 'optimize-totals' ? field(nodeId, name) === 'true' : numericField(nodeId, name)]));
}

// Fingerprints read controls directly, before debounced persistence. They also
// bind upstream lifecycle revision so a repeated run cannot reuse a prior fit.
function contextFor(nodeId) {
  const owner = nodeRegistry[nodeId];
  if (!owner || !TYPES.has(owner.type)) return null;
  let inputs;
  try {
    if (owner.type === 'inverse-design-target') {
      inputs = editorValues(nodeId, TARGET_FIELDS);
    } else if (owner.type === 'gradient-design') {
      const source = sourceNodeId(nodeId, 'inverse-design-request', 'inverse-design-target');
      inputs = {
        graph: connectionSignature(nodeId), target: editorValues(source, TARGET_FIELDS),
        targetRevision: nodeRegistry[source]._inverseDesignLifecycle
          ? inspectExecutionLifecycle(nodeRegistry[source]._inverseDesignLifecycle).revision : null,
        options: editorValues(nodeId, OPTIMIZER_FIELDS),
      };
    } else {
      const source = sourceNodeId(nodeId, 'inverse-design-result', 'gradient-design');
      const result = readCurrentGradientResult(source);
      if (!result) throw new Error('A current gradient result is required.');
      inputs = {
        graph: connectionSignature(nodeId),
        gradientRevision: inspectExecutionLifecycle(nodeRegistry[source]._inverseDesignLifecycle).revision,
        network: result.result.selected_network,
      };
    }
  } catch (error) {
    inputs = { invalid: true, reason: error.message, graph: connectionSignature(nodeId) };
  }
  return {
    owner, workspaceEpoch: getWorkspaceRuntimeEpoch(),
    inputFingerprint: stableJson(inputs), endpoint: INVERSE_DESIGN_ENDPOINT,
  };
}

function currentArtifact(nodeId) {
  const info = nodeRegistry[nodeId];
  if (!info?._inverseDesignLifecycle) return null;
  const runtime = inspectExecutionLifecycle(info._inverseDesignLifecycle);
  const context = contextFor(nodeId);
  if (!context || !runtime.currentTicket ||
      !isCurrent(info._inverseDesignLifecycle, runtime.currentTicket, context)) return null;
  return readCurrentResult(info._inverseDesignLifecycle);
}

export function readCurrentGradientResult(nodeId) {
  return nodeRegistry[nodeId]?.type === 'gradient-design' ? currentArtifact(nodeId) : null;
}

export function readCurrentDesignedNetwork(nodeId) {
  if (nodeRegistry[nodeId]?.type !== 'designed-network') return null;
  const network = currentArtifact(nodeId);
  return network ? JSON.parse(JSON.stringify(network)) : null;
}

function setContent(nodeId, html) {
  const target = document.getElementById(`${nodeId}-content`);
  if (target) target.innerHTML = html;
}

function startLearningProgress(nodeId, owner, current) {
  const panel = document.getElementById(`${nodeId}-learning`);
  if (!panel) return { update() {}, finish() {}, active: false };
  const status = document.getElementById(`${nodeId}-learning-status`);
  const metrics = document.getElementById(`${nodeId}-learning-metrics`);
  const clock = document.getElementById(`${nodeId}-learning-clock`);
  const run = document.getElementById(`${nodeId}-run-design`);
  const stop = document.getElementById(`${nodeId}-stop-design`);
  const started = Date.now(), epoch = getWorkspaceRuntimeEpoch();
  let timer = null, lastUpdate = started, signature = '';
  const history = { evaluated: 0, samples: [], events: [], restart: null, pruneKey: '' };
  const owns = () => nodeRegistry[nodeId] === owner && owner._inverseDesignProgress === session && getWorkspaceRuntimeEpoch() === epoch;
  const updateClock = () => {
    const seconds = Math.floor((Date.now() - started) / 1000);
    const quiet = Math.floor((Date.now() - lastUpdate) / 1000);
    clock.textContent = `Elapsed ${seconds < 60 ? `${seconds}s` : `${Math.floor(seconds / 60)}m ${seconds % 60}s`}${session.active && quiet >= 5 ? ` · waiting for the next update (${quiet}s)` : ''}`;
  };
  const tick = () => {
    if (!owns()) { clearTimeout(timer); return; }
    if (!current()) { session.finish('Learning stopped — inputs changed.'); return; }
    updateClock();
    timer = setTimeout(tick, 1000);
  };
  const session = {
    active: true,
    update(job) {
      if (!owns() || !session.active || !current()) return;
      const next = stableJson({ status: job.status, progress: job.progress || job });
      if (signature !== next) {
        signature = next; lastUpdate = Date.now();
        recordLearningSample(history, job);
        const view = renderInverseDesignProgress(job, history);
        // Keep the live region stable; announce stage changes, not each tick.
        if (status.textContent !== view.status) status.textContent = view.status;
        metrics.innerHTML = view.html;
        metrics.hidden = false;
        run.textContent = view.button;
      }
      updateClock();
    },
    finish(message = '') {
      session.active = false;
      clearTimeout(timer);
      if (!owns()) return;
      updateClock();
      panel.hidden = !message;
      panel.classList.remove('is-learning');
      status.textContent = message;
      metrics.hidden = true;
      run.disabled = false;
      run.textContent = 'Design and prune network';
      run.removeAttribute('aria-busy');
      stop.disabled = true;
    },
  };
  owner._inverseDesignProgress = session;
  panel.hidden = false;
  panel.classList.add('is-learning');
  run.disabled = true; run.setAttribute('aria-busy', 'true');
  stop.disabled = false;
  session.update({ status: 'submitting' });
  timer = setTimeout(tick, 1000);
  return session;
}

function retireNode(nodeId, reason) {
  const info = nodeRegistry[nodeId];
  if (!info || !TYPES.has(info.type)) return;
  if (info._inverseDesignLifecycle) {
    const runtime = inspectExecutionLifecycle(info._inverseDesignLifecycle);
    if (runtime.owner === info && runtime.workspaceEpoch != null) {
      invalidate(info._inverseDesignLifecycle, {
        owner: info, workspaceEpoch: runtime.workspaceEpoch, reason,
      });
    }
    syncLifecycle(info);
  }
  info._designTargetAbort?.abort();
  delete info._designTargetAbort;
  delete info._designImageTicket;
  delete info._designReferenceLoading;
  delete info._designStrokeTicket;
  info._inverseDesignAbort?.abort();
  delete info._inverseDesignAbort;
  info._inverseDesignProgress?.finish(reason === 'inverse-design-user-cancelled'
    ? 'Stopping learning…' : reason === 'inverse-design-run-started' ? '' : 'Learning stopped — inputs changed.');
  const data = info.data || {};
  delete data.inverseDesignRequest;
  delete data.inverseDesignResult;
  delete data.inverseDesignDrawing;
  delete data.designedNetwork;
  setNodeLoading(nodeId, false);
  const message = info.type === 'inverse-design-target'
    ? 'Target changed. Review and validate the current goal before running.'
    : info.type === 'gradient-design'
      ? 'Inputs changed. Run gradient design to fit the current target.'
      : 'No current network. Run the connected gradient design and extract its network.';
  setContent(nodeId, `<span class="text-dim" role="status">${message}</span>`);
  if (info.type === 'designed-network') invalidateModelBuildersForReactionSource(nodeId, reason);
}

export function invalidateInverseDesignDownstream(nodeId, reason = 'inverse-design-input-changed') {
  const graph = executionDependencyConnections();
  const queue = [nodeId], visited = new Set([nodeId]);
  while (queue.length) {
    const source = queue.shift();
    for (const connection of graph) {
      if (connection.fromNode !== source || visited.has(connection.toNode)) continue;
      visited.add(connection.toNode);
      queue.push(connection.toNode);
      retireNode(connection.toNode, reason);
    }
  }
}

export function invalidateInverseDesignNode(nodeId, reason = 'inverse-design-input-changed') {
  retireNode(nodeId, reason);
  invalidateInverseDesignDownstream(nodeId, reason);
}

function invalidateForConnections(nodeId, before, after, reason) {
  if (stableJson(connectionSignature(nodeId, before)) !== stableJson(connectionSignature(nodeId, after))) {
    invalidateInverseDesignNode(nodeId, reason);
  }
}

export function handleInverseDesignConnectionsChanged(before, after = executionDependencyConnections(), reason = 'inverse-design-connections-changed') {
  for (const [nodeId, info] of Object.entries(nodeRegistry)) {
    if (TYPES.has(info.type)) invalidateForConnections(nodeId, before, after, reason);
  }
}

function installLifecycle(nodeId, { controls = false } = {}) {
  const owner = nodeRegistry[nodeId];
  if (!owner) return;
  owner._onConnectionsChanged = (before, after, reason) => {
    if (nodeRegistry[nodeId] === owner) invalidateForConnections(nodeId, before, after, reason);
  };
  if (!controls) return;
  const node = document.getElementById(nodeId);
  node?.querySelectorAll('.auto-update').forEach(control => {
    const event = control.tagName === 'SELECT' ? 'change' : 'input';
    control.addEventListener(event, () => {
      if (control.id === `${nodeId}-target-json`) delete owner._legacyInverseTarget;
      invalidateInverseDesignNode(nodeId);
    });
  });
  // Our invalidation listeners are installed first, before any debounce.
  setupAutoUpdate(nodeId, owner.type);
}

function settleLoading(nodeId, owner, lifecycle, ticket) {
  release(lifecycle, ticket);
  if (nodeRegistry[nodeId] !== owner) return;
  const runtime = inspectExecutionLifecycle(lifecycle);
  if (runtime.currentTicket === ticket || (!runtime.currentTicket && !runtime.loading)) {
    setNodeLoading(nodeId, false);
  }
  syncLifecycle(owner);
}

function staleGradientOutcome(nodeId, owner, lifecycle, ticket) {
  // Context drift without a DOM event (workspace epoch or programmatic edit)
  // must also terminate the old ticket. Never retire a newer run's ticket.
  if (nodeRegistry[nodeId] === owner && inspectExecutionLifecycle(lifecycle).currentTicket === ticket) {
    invalidateInverseDesignNode(nodeId, 'inverse-design-context-changed');
  }
  return staleOutcome(nodeId, { code: 'inverse-design-input-changed' });
}

export function applyInverseDesignPreset(nodeId) {
  try { resetTargetExample(nodeId); return true; }
  catch (error) { setContent(nodeId, `<div class="node-error" role="status">${escapeHtml(error.message)}</div>`); return false; }
}

export function clearInverseDesignDrawing(nodeId) { clearTargetDrawing(nodeId); }
export function refreshInverseDesignTarget(nodeId) { refreshTargetEditor(nodeId); }

function targetPatchValues(result) {
  const request = validateInverseDesignRequest({ target: result.target, chemistry: result.chemistry });
  const editableTarget = validateDesignTarget(result.target);
  return {
    'target-json': JSON.stringify(editableTarget, null, 2), description: editableTarget.description,
    'target-mode': editableTarget.source, 'reference-state': '', 'drawing-json': '',
    'aux-monomers': String(request.chemistry.auxiliary_monomers),
    'max-complex-size': String(request.chemistry.max_complex_size),
    'max-reactions': String(request.chemistry.max_reactions),
    'allow-homomers': String(request.chemistry.allow_homomers),
    'chemistry-json': JSON.stringify(Object.fromEntries(Object.entries(request.chemistry).filter(([key]) => !['auxiliary_monomers', 'max_complex_size', 'max_reactions', 'allow_homomers'].includes(key))), null, 2),
  };
}

// GraphPatch initialization happens before publication and must not add an
// extra history item; the enclosing patch owns this whole target insertion.
export function populateInverseDesignTarget(nodeId, result) {
  if (nodeRegistry[nodeId]?.type !== 'inverse-design-target') return false;
  const values = targetPatchValues(result);
  for (const [suffix, value] of Object.entries(values)) {
    const control = document.getElementById(`${nodeId}-${suffix}`);
    if (!control) throw new Error(`Missing target field ${suffix}.`);
    control.value = value;
  }
  delete nodeRegistry[nodeId]._legacyInverseTarget;
  nodeRegistry[nodeId]._designTargetMode = values['target-mode'];
  refreshTargetEditor(nodeId);
  return true;
}

export function applyCompiledInverseDesignTarget(nodeId, result) {
  if (nodeRegistry[nodeId]?.type !== 'inverse-design-target') return false;
  const edits = Object.entries(targetPatchValues(result)).filter(([suffix, value]) => field(nodeId, suffix) !== value)
    .map(([suffix, value]) => new ChangeAttrCommand({ nodeId, key: `${nodeId}-${suffix}`, before: field(nodeId, suffix), after: value }));
  if (edits.length) {
    const command = new CompoundCommand(edits, 'Compile design goal');
    for (const method of ['apply', 'revert']) {
      const perform = command[method].bind(command);
      command[method] = () => {
        const owner = nodeRegistry[nodeId];
        if (owner) owner._designModeMutation = true;
        try { perform(); } finally { if (owner) delete owner._designModeMutation; refreshTargetEditor(nodeId); }
      };
    }
    dispatch(command);
  }
  refreshTargetEditor(nodeId);
  commitWorkspaceSnapshot('inverse-design-agent-target');
  return true;
}

export async function compileInverseDesignTarget(nodeId) {
  const owner = nodeRegistry[nodeId];
  if (owner?.type !== 'inverse-design-target') return false;
  invalidateInverseDesignNode(nodeId, 'design-target-compiling');
  const controller = new AbortController(); owner._designTargetAbort = controller;
  const epoch = getWorkspaceRuntimeEpoch(), fingerprint = stableJson(editorValues(nodeId, TARGET_FIELDS));
  let replyOwned = false;
  const current = () => nodeRegistry[nodeId] === owner && owner._designTargetAbort === controller &&
    getWorkspaceRuntimeEpoch() === epoch && stableJson(editorValues(nodeId, TARGET_FIELDS)) === fingerprint;
  try {
    setContent(nodeId, '<span class="text-dim" role="status">Design Agent is translating the goal into explicit input / output targets…</span>');
    let target;
    try { target = readEditorTarget(nodeId, { allowPendingDescription: true, allowPendingReference: true }); } catch { /* A description can replace invalid numerical data. */ }
    const result = await compileDesignTarget(field(nodeId, 'description'), { target, signal: controller.signal });
    if (!current()) return false;
    replyOwned = true;
    delete owner._designTargetAbort;
    applyCompiledInverseDesignTarget(nodeId, result);
    setContent(nodeId, `<div role="status"><strong>Goal compiled.</strong> ${escapeHtml(result.interpretation || 'Review the editable target and constraints before running.')} ${(result.warnings || []).map(escapeHtml).join(' ')}</div>`);
    return true;
  } catch (error) {
    if (current() || replyOwned && nodeRegistry[nodeId] === owner && getWorkspaceRuntimeEpoch() === epoch) setContent(nodeId, `<div class="node-error" role="status">${escapeHtml(error.message)}</div>`);
    return false;
  } finally { if (owner._designTargetAbort === controller) delete owner._designTargetAbort; }
}

export function prepareInverseDesignRequest(nodeId, { commitSnapshot = true } = {}) {
  const owner = nodeRegistry[nodeId];
  if (!owner || owner.type !== 'inverse-design-target') {
    return blockedOutcome(nodeId, { code: 'missing-target', message: 'Inverse Design Target is unavailable.' });
  }
  if (owner._designReferenceLoading || owner._designTargetAbort) {
    const message = owner._designReferenceLoading
      ? 'The pattern image is still loading. Wait for it, then trace an ordered path before running.'
      : 'Design Agent is still compiling the description. Review its numerical target before running.';
    setContent(nodeId, `<span class="text-dim" role="status">${message}</span>`);
    return blockedOutcome(nodeId, { outputs: { 'inverse-design-request': 'missing' }, code: 'target-authoring-pending', message });
  }
  invalidateInverseDesignNode(nodeId, 'inverse-design-target-preparing');
  const lifecycle = lifecycleFor(owner);
  let ticket = null;
  try {
    synchronizeTrainingTarget(nodeId);
    ticket = begin(lifecycle, contextFor(nodeId));
    const request = validateInverseDesignRequest(rawTargetRequest(nodeId));
    if (!commit(lifecycle, ticket, {
      context: contextFor(nodeId), result: request,
      evidence: { artifact_kind: 'inverse-design-request', target_basis: 'explicit_numeric_samples' },
    })) return staleOutcome(nodeId, { code: 'target-changed' });
    ensureNodeData(nodeId).inverseDesignRequest = request;
    syncLifecycle(owner);
    setContent(nodeId, `<div role="status"><strong>Target ready.</strong> ${request.target.samples.length} samples · ${request.target.inputs.length} → ${request.target.outputs.length} dimensions · automatic network construction.</div>${renderDesignTargetErrorFloor(request.target)}`);
    if (commitSnapshot) commitWorkspaceSnapshot('inverse-design-target-prepared');
    return succeededOutcome(nodeId, { outputs: { 'inverse-design-request': 'present' } });
  } catch (error) {
    ticket ||= begin(lifecycle, contextFor(nodeId));
    block(lifecycle, ticket, { context: contextFor(nodeId), reason: error.message });
    syncLifecycle(owner);
    setContent(nodeId, `<div class="node-error" role="status">${escapeHtml(error.message)}</div>`);
    return blockedOutcome(nodeId, { outputs: { 'inverse-design-request': 'missing' }, code: 'invalid-target', message: error.message });
  } finally {
    settleLoading(nodeId, owner, lifecycle, ticket);
  }
}

export function cancelGradientDesign(nodeId) {
  const owner = nodeRegistry[nodeId];
  if (owner?.type !== 'gradient-design' || !owner._inverseDesignAbort) return false;
  owner._inverseDesignAbort._designUserCancelled = true;
  invalidateInverseDesignNode(nodeId, 'inverse-design-user-cancelled');
  setContent(nodeId, '<span class="text-dim" role="status">Design stopped. Run again when the target and settings are ready.</span>');
  return true;
}

export function reviewInverseDesignBudget(nodeId) {
  if (nodeRegistry[nodeId]?.type !== 'gradient-design') return;
  const control = document.getElementById(`${nodeId}-epochs`);
  control?.focus();
}

export async function executeGradientDesign(nodeId) {
  const owner = nodeRegistry[nodeId];
  if (!owner || owner.type !== 'gradient-design') {
    return blockedOutcome(nodeId, { code: 'missing-gradient-node' });
  }
  invalidateInverseDesignNode(nodeId, 'inverse-design-run-started');
  const lifecycle = lifecycleFor(owner);
  let ticket = null, request = null, controller = null, learning = null;
  try {
    const source = sourceNodeId(nodeId, 'inverse-design-request', 'inverse-design-target');
    // A shared target may feed several independent optimizers. Re-preparing
    // an already current target would retire every sibling branch mid-run.
    let targetRequest = currentArtifact(source);
    if (!targetRequest) {
      const prepared = prepareInverseDesignRequest(source, { commitSnapshot: false });
      if (prepared.status !== 'succeeded') throw new Error(prepared.message || 'Validate the connected numerical target.');
      targetRequest = currentArtifact(source);
    }
    request = validateInverseDesignRequest({ ...targetRequest, optimization: optimizerOptions(nodeId) });
    const authoredTarget = readEditorTarget(source), mode = field(source, 'target-mode');
    const points = readTargetDrawing(field(source, 'drawing-json') || '', authoredTarget, mode);
    const drawing = points ? makeTargetDrawing(request.target, mode, points, authoredTarget) : '';
    ticket = begin(lifecycle, contextFor(nodeId));
    controller = new AbortController();
    owner._inverseDesignAbort = controller;
    syncLifecycle(owner);
    setNodeLoading(nodeId, true);
    const targetNotice = renderDesignTargetErrorFloor(request.target, request.optimization.max_rmse);
    setContent(nodeId, targetNotice + '<span class="text-dim">Waiting for the first evaluated response. The target and current response will update here during learning.</span>');
    const current = () => {
      const context = contextFor(nodeId);
      return !!context && isCurrent(lifecycle, ticket, context);
    };
    learning = startLearningProgress(nodeId, owner, current);
    const raw = await runLocalDesignJob(request, {
      signal: controller.signal, statusIsCurrent: current,
      onProgress: job => {
        learning.update(job);
        if (!current()) return;
        const preview = renderInverseDesignPreview(job.progress || job, request, drawing);
        if (preview !== null) setContent(nodeId, preview + targetNotice);
      },
    });
    if (!current()) return controller._designUserCancelled ? cancelledOutcome(nodeId, { code: 'design-user-cancelled' }) : staleGradientOutcome(nodeId, owner, lifecycle, ticket);
    const result = normalizeInverseDesignResult(raw, request);
    const rendered = renderInverseDesignResult(result, request, drawing);
    if (!commit(lifecycle, ticket, {
      context: contextFor(nodeId), result: { request, result },
      evidence: result.selected_network?.status === 'ok' ? EVIDENCE : { evidence_tier: 'numerical_fit_only' },
    })) return staleGradientOutcome(nodeId, owner, lifecycle, ticket);
    const data = ensureNodeData(nodeId);
    data.inverseDesignRequest = request;
    data.inverseDesignResult = result;
    data.inverseDesignDrawing = drawing;
    syncLifecycle(owner);
    setContent(nodeId, rendered);
    learning.finish(inverseDesignCompletionStatus(result));
    commitWorkspaceSnapshot('inverse-design-gradient-fit');
    return succeededOutcome(nodeId, {
      outputs: { 'inverse-design-result': 'present' },
      evidence: serializeExecutionLifecycle(lifecycle).evidence,
      message: inverseDesignCompletionStatus(result),
    });
  } catch (error) {
    if (controller?._designUserCancelled) {
      learning?.finish('Learning stopped.');
      return cancelledOutcome(nodeId, { outputs: { 'inverse-design-result': 'missing' }, code: 'design-user-cancelled', message: 'Design stopped.' });
    }
    const context = contextFor(nodeId);
    if (ticket && (!context || !isCurrent(lifecycle, ticket, context))) {
      return staleGradientOutcome(nodeId, owner, lifecycle, ticket);
    }
    const wasRunning = !!ticket;
    if (!ticket) ticket = begin(lifecycle, context);
    if (wasRunning) fail(lifecycle, ticket, { context, error });
    else block(lifecycle, ticket, { context, reason: error.message });
    syncLifecycle(owner);
    learning?.finish('Learning failed — see the error below.');
    setContent(nodeId, `<div class="node-error" role="status">${escapeHtml(error.message)}</div>`);
    const outcome = wasRunning ? failedOutcome : blockedOutcome;
    return outcome(nodeId, { outputs: { 'inverse-design-result': 'missing' }, code: wasRunning ? 'inverse-design-failed' : 'invalid-design-input', message: error.message });
  } finally {
    if (learning?.active) learning.finish('Learning stopped — inputs changed.');
    if (owner._inverseDesignAbort === controller) delete owner._inverseDesignAbort;
    if (ticket) settleLoading(nodeId, owner, lifecycle, ticket);
  }
}

export function executeDesignedNetwork(nodeId) {
  const owner = nodeRegistry[nodeId];
  if (!owner || owner.type !== 'designed-network') return blockedOutcome(nodeId, { code: 'missing-network-node' });
  invalidateInverseDesignNode(nodeId, 'designed-network-rebuilding');
  const lifecycle = lifecycleFor(owner);
  const ticket = begin(lifecycle, contextFor(nodeId));
  try {
    const source = sourceNodeId(nodeId, 'inverse-design-result', 'gradient-design');
    const artifact = readCurrentGradientResult(source);
    if (!artifact) throw new Error('Run the connected Gradient Design node first. Saved or outdated results cannot supply a network.');
    const network = designedNetworkFromResult(artifact.result);
    if (!network) throw new Error(artifact.result.selected_network?.reason || 'No valid network was found. Increase the search budget or review the chemistry settings and run again.');
    if (!commit(lifecycle, ticket, { context: contextFor(nodeId), result: network, evidence: EVIDENCE })) {
      return staleOutcome(nodeId, { code: 'gradient-result-changed' });
    }
    ensureNodeData(nodeId).designedNetwork = network;
    syncLifecycle(owner);
    setContent(nodeId, renderDesignedNetwork(network));
    commitWorkspaceSnapshot('inverse-design-network-output');
    return succeededOutcome(nodeId, { outputs: { reactions: 'present' }, evidence: EVIDENCE });
  } catch (error) {
    block(lifecycle, ticket, { context: contextFor(nodeId), reason: error.message });
    syncLifecycle(owner);
    setContent(nodeId, `<div class="node-error" role="status">${escapeHtml(error.message)}</div>`);
    return blockedOutcome(nodeId, { outputs: { reactions: 'missing' }, code: 'no-current-designed-network', message: error.message });
  } finally {
    settleLoading(nodeId, owner, lifecycle, ticket);
  }
}

const LEGACY = '<div class="node-info workspace-historical-warning" role="status"><strong>Earlier candidate-library workflow.</strong> This saved result belongs to the former Kd-only method. Review a target in the new editor and run network construction, optimization and pruning. It cannot supply a current network.</div>';

const HISTORICAL = '<div class="node-info workspace-historical-warning" role="status"><strong>Historical result.</strong> Run the connected workflow again to make this saved evidence available downstream.</div>';

export function restoreInverseDesignTargetView(nodeId, data = null) {
  const owner = nodeRegistry[nodeId], restored = data || owner?.data;
  if (!owner || !restored) return false;
  refreshTargetEditor(nodeId);
  const modeControl = document.getElementById(`${nodeId}-target-mode`);
  if (modeControl) owner._designTargetMode = modeControl.value;
  const legacy = !restored.inverseDesignRequest?.target && (restored.inverseDesignRequest || restored.config?.inverseCandidateReactions || restored.config?.inverseTargetSamples || restored.inverseCandidateReactions || restored.inverseTargetSamples);
  if (legacy) {
    owner._legacyInverseTarget = true;
    restoreHistorical(lifecycleFor(owner), { context: contextFor(nodeId), result: null, evidence: restoredEvidence(restored, { evidence_tier: 'legacy_candidate_fit' }) });
    syncLifecycle(owner); setContent(nodeId, LEGACY + `<section class="inverse-result-section"><div class="reaction-header"><span class="reaction-header-label">Saved numerical specification</span></div><pre>${escapeHtml(JSON.stringify(restored.inverseDesignRequest || restored.config || restored, null, 2))}</pre></section>`); return true;
  }
  if (!restored.inverseDesignRequest) return false;
  try {
    const request = validateInverseDesignRequest(restored.inverseDesignRequest);
    restoreHistorical(lifecycleFor(owner), {
      context: contextFor(nodeId), result: request,
      evidence: restoredEvidence(restored,
        { artifact_kind: 'inverse-design-request', target_basis: 'explicit_numeric_samples' }),
    });
    syncLifecycle(owner);
    setContent(nodeId, '<span class="text-dim" role="status">Saved target. Validate the current numerical samples before use.</span>');
    return true;
  } catch (error) {
    invalidateInverseDesignNode(nodeId, 'invalid-restored-target');
    setContent(nodeId, `<div class="node-error" role="status">Saved target rejected: ${escapeHtml(error.message)}</div>`);
    return false;
  }
}

export function restoreInverseDesignResultView(nodeId, data = null) {
  const owner = nodeRegistry[nodeId], restored = data || owner?.data;
  if (!owner || !restored?.inverseDesignResult) return false;
  if (!restored.inverseDesignRequest?.target) {
    restoreHistorical(lifecycleFor(owner), { context: contextFor(nodeId), result: null, evidence: restoredEvidence(restored, { evidence_tier: 'legacy_candidate_fit' }) });
    syncLifecycle(owner); setContent(nodeId, LEGACY); return true;
  }
  try {
    const request = validateInverseDesignRequest(restored.inverseDesignRequest);
    const result = normalizeInverseDesignResult(restored.inverseDesignResult, request);
    restoreHistorical(lifecycleFor(owner), {
      context: contextFor(nodeId), result: { request, result },
      evidence: restoredEvidence(restored,
        result.selected_network?.status === 'ok' ? EVIDENCE : { evidence_tier: 'numerical_fit_only' }),
    });
    syncLifecycle(owner);
    setContent(nodeId, HISTORICAL + renderInverseDesignResult(result, request, restored.inverseDesignDrawing));
    return true;
  } catch (error) {
    invalidateInverseDesignNode(nodeId, 'invalid-restored-gradient-result');
    setContent(nodeId, HISTORICAL + `<div class="node-error" role="status">Saved result rejected: ${escapeHtml(error.message)}</div>`);
    return false;
  }
}

export function restoreDesignedNetworkView(nodeId, data = null) {
  const owner = nodeRegistry[nodeId], restored = data || owner?.data;
  const network = restored?.designedNetwork;
  if (!owner || !network) return false;
  if (!network.totals || !network.outputs) {
    restoreHistorical(lifecycleFor(owner), { context: contextFor(nodeId), result: null, evidence: restoredEvidence(restored, { evidence_tier: 'legacy_candidate_fit' }) });
    syncLifecycle(owner); setContent(nodeId, LEGACY); return true;
  }
  try {
    if (!Array.isArray(network.reactions) ||
        !network.reactions.every(rule => typeof rule === 'string' && rule.trim()) ||
        !Array.isArray(network.kds) || network.kds.length !== network.reactions.length ||
        !network.kds.every(kd => typeof kd === 'number' && Number.isFinite(kd) && kd > 0)) {
      throw new Error('Expected reaction rules with one positive optimized Kd per reaction.');
    }
    restoreHistorical(lifecycleFor(owner), {
      context: contextFor(nodeId), result: network,
      evidence: restoredEvidence(restored, EVIDENCE),
    });
    syncLifecycle(owner);
    setContent(nodeId, HISTORICAL + renderDesignedNetwork(network, { historical: true }));
    return true;
  } catch (error) {
    invalidateInverseDesignNode(nodeId, 'invalid-restored-designed-network');
    setContent(nodeId, HISTORICAL + `<div class="node-error" role="status">Saved network rejected: ${escapeHtml(error.message)}</div>`);
    return false;
  }
}

function numericControl(nodeId, suffix, label, value, { min = 0, max = null, step = 'any' } = {}) {
  return `<div class="param-row"><label for="${nodeId}-${suffix}">${label}</label><input type="number" id="${nodeId}-${suffix}" class="auto-update" value="${value}" min="${min}"${max == null ? '' : ` max="${max}"`} step="${step}"></div>`;
}

export const INVERSE_DESIGN_TYPES = {
  'inverse-design-target': {
    category: 'parameter', headerClass: 'header-parameter', title: 'Inverse Design Target',
    inputs: [], outputs: [{ port: 'inverse-design-request', type: 'InverseDesignRequest', label: 'Design request' }],
    defaultWidth: 520,
    defaultHeight: 1800,
    createBody(nodeId) {
      const defaults = INVERSE_DESIGN_DEFAULTS.chemistry;
      return `<div class="inverse-design-form">${renderTargetEditor(nodeId)}
        <section class="node-panel" aria-label="Network design constraints">
          <div class="node-panel-header"><span class="node-panel-title">Network design constraints</span></div>
          <div class="node-panel-body">
          <div class="inverse-parameter-grid">
          ${numericControl(nodeId, 'aux-monomers', 'Auxiliary monomers', defaults.auxiliary_monomers, { min: 0, max: 4, step: 1 })}
          ${numericControl(nodeId, 'max-complex-size', 'Maximum complex size', defaults.max_complex_size, { min: 2, max: 8, step: 1 })}
          ${numericControl(nodeId, 'max-reactions', 'Reaction budget', defaults.max_reactions, { min: 1, max: 256, step: 1 })}
          <div class="param-row"><label for="${nodeId}-allow-homomers">Homomer binding</label><select id="${nodeId}-allow-homomers" class="auto-update"><option value="true">Allowed</option><option value="false">Excluded</option></select></div>
          </div>
          <label class="inverse-field-label" for="${nodeId}-chemistry-json">Additional chemistry rules</label><textarea id="${nodeId}-chemistry-json" class="auto-update design-target-json" rows="3" spellcheck="false">{}</textarea>
          <div class="node-info" title="Build legal networks from the input species and auxiliary monomers. Optional rules: max_copies, forbidden_complexes and binding_gates.">Networks are built from the input species and auxiliary monomers.</div>
          </div>
        </section>
        <button class="btn btn-run" data-action="prepareInverseDesignRequest" data-node="${nodeId}">Validate target</button>
        <div id="${nodeId}-content" class="node-info" role="status" tabindex="0"><span class="text-dim">Draw, upload, edit data, or compile a description. Then validate the goal.</span></div>
      </div>`;
    },
    onInit(nodeId) { installLifecycle(nodeId, { controls: true }); installTargetEditor(nodeId, { onInvalidate: reason => invalidateInverseDesignNode(nodeId, reason) }); },
    prepare: prepareInverseDesignRequest,
  },
  'gradient-design': {
    category: 'process', headerClass: 'header-process', title: 'Gradient Design',
    inputs: [{ port: 'inverse-design-request', type: 'InverseDesignRequest', label: 'Design request' }],
    outputs: [{ port: 'inverse-design-result', type: 'InverseDesignResult', label: 'Design result' }],
    defaultWidth: 680,
    defaultHeight: 750,
    createBody(nodeId) {
      const defaults = INVERSE_DESIGN_DEFAULTS.optimization;
      return `<div class="inverse-design-form">
        <div class="node-info">Optimize Kd and non-input concentrations, then prune branches and refit the target.</div>
        <div class="inverse-parameter-grid">
        <section class="node-panel" aria-label="Parameter optimization">
          <div class="node-panel-header"><span class="node-panel-title">Parameter optimization</span></div>
          <div class="node-panel-body">
          ${numericControl(nodeId, 'learning-rate', 'Learning rate', defaults.learning_rate, { min: 0.000001 })}
          ${numericControl(nodeId, 'epochs', 'Epochs per fit', defaults.epochs, { min: 1, max: 2000, step: 1 })}
          ${numericControl(nodeId, 'restarts', 'Initializations', defaults.restarts, { min: 1, max: 8, step: 1 })}
          <div class="param-row"><label for="${nodeId}-optimize-totals">Non-input totals</label><select id="${nodeId}-optimize-totals" class="auto-update"><option value="true">Optimize with Kd</option><option value="false">Keep fixed</option></select></div>
          ${numericControl(nodeId, 'seed', 'Random seed', defaults.seed, { min: 0, max: 2147483647, step: 1 })}
          </div>
        </section>
        <section class="node-panel" aria-label="Pruning and target tolerance">
          <div class="node-panel-header"><span class="node-panel-title">Pruning and target tolerance</span></div>
          <div class="node-panel-body">
          ${numericControl(nodeId, 'prune-rounds', 'Pruning rounds', defaults.prune_rounds, { min: 0, max: 12, step: 1 })}
          ${numericControl(nodeId, 'prune-fraction', 'Fraction per pruning step', defaults.prune_fraction, { min: 0.01, max: 1 })}
          ${numericControl(nodeId, 'prune-tolerance', 'Allowed RMSD increase', defaults.prune_tolerance)}
          ${numericControl(nodeId, 'max-rmse', 'Maximum RMSD per output', defaults.max_rmse)}
          </div>
        </section>
        </div>
        <div class="node-info">The search stops at its configured limits, even if the target is not met. Pruning preserves precursors and readouts, refits each deletion, and keeps it only within the error tolerance.</div>
        <div class="design-target-actions"><button id="${nodeId}-run-design" class="btn btn-run" data-action="executeGradientDesign" data-node="${nodeId}">Design and prune network</button><button id="${nodeId}-stop-design" class="btn btn-small" data-action="cancelGradientDesign" data-node="${nodeId}" disabled>Stop</button></div>
        <div id="${nodeId}-learning" class="node-panel node-panel--chart inverse-learning" hidden>
          <div class="node-panel-header"><span class="node-panel-title">Learning</span><span id="${nodeId}-learning-clock" class="text-dim inverse-note"></span></div>
          <div class="node-panel-body">
          <div id="${nodeId}-learning-status" class="inverse-learning-status" role="status" aria-live="polite" aria-atomic="true"></div>
          <div id="${nodeId}-learning-metrics"></div>
          </div>
        </div>
      </div><div id="${nodeId}-content" class="viewer-content inverse-design-viewer" tabindex="0" role="region" aria-label="Design results"><span class="text-dim" role="status">Connect an Inverse Design Target to begin.</span></div>`;
    },
    onInit(nodeId) { installLifecycle(nodeId, { controls: true }); },
    execute: executeGradientDesign,
  },
  'designed-network': {
    category: 'result', headerClass: 'header-result', title: 'Designed Network',
    inputs: [{ port: 'inverse-design-result', type: 'InverseDesignResult', label: 'Design result' }],
    outputs: [{ port: 'reactions', type: 'NetworkIR', label: 'Fitted network' }],
    defaultWidth: 480,
    defaultHeight: 400,
    createBody(nodeId) {
      return `<button class="btn btn-run" data-action="executeDesignedNetwork" data-node="${nodeId}">Extract designed network</button>
        <div id="${nodeId}-content" class="viewer-content inverse-design-viewer" tabindex="0" role="region" aria-label="Designed network details"><span class="text-dim" role="status">Output the designed network structure, optimized Kd, concentrations and readouts.</span></div>`;
    },
    onInit(nodeId) { installLifecycle(nodeId); },
    execute: executeDesignedNetwork,
  },
};
