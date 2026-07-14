// Biocircuits Explorer — ParameterPlacer CHAIN: a "tunable design station".
//
// params (placer-params) + result (placer-result) pair; Quick Add builds
// reaction-network -> model-builder -> placer-params -> placer-result.
// Embodies "tunable" as an experience across multiple design HANDLES:
//   MENU      — "Show achievable menu": the reaction-order ladder this circuit can
//               be tuned to + its regime SEQUENCE (the transitions a threshold can
//               sit at).                                    [/api/v1/placer_menu]
//   SLOPE     — click a rung (or type a target RO + Solve): solve the regime
//               polytope for a concrete kd.                 [/api/v1/place_parameters]
//   THRESHOLD — place a chosen transition (EC50/knee) at a target input dose.
//                                                           [/api/v1/placer_threshold]
//   LIVE TUNE — a Kd slider re-plots the dose-response.     [/api/v1/placer_curve]
import { api, syncSelectOptions, escapeHtml } from '../api.js';
import {
  ensureModelSession,
  getModelContextForNode,
  getModelForNode,
  setNodeLoading,
  setupAutoUpdate,
  setupPlotResize,
  triggerConfigUpdate,
} from '../nodes.js';
import { plotParameterScan1D } from '../scan.js';
import { commitWorkspaceSnapshot } from '../workspace.js';
import {
  connections,
  getWorkspaceRuntimeEpoch,
  nodeRegistry,
} from '../state.js';
import {
  begin as beginLifecycle,
  block as blockLifecycle,
  commit as commitLifecycle,
  createExecutionLifecycle,
  fail as failLifecycle,
  inspectExecutionLifecycle,
  invalidate as invalidateLifecycle,
  isCurrent as isCurrentLifecycle,
  readCurrentResult,
  release as releaseLifecycle,
  restoreHistorical as restoreHistoricalLifecycle,
  serializeExecutionLifecycle,
} from '../execution-lifecycle-core.js';
import {
  executionDependencyConnections,
  PLACER_RESULT_LIFECYCLE_KEY,
} from '../execution-lifecycle.js';

const PLACER_ENDPOINTS = Object.freeze({
  menu: '/api/v1/placer_menu',
  solve: '/api/v1/place_parameters',
  program: '/api/v1/placer_realize_program',
  threshold: '/api/v1/placer_threshold',
  level: '/api/v1/placer_level',
  curve: '/api/v1/placer_curve',
});

function canonicalFingerprintValue(value) {
  if (Array.isArray(value)) return value.map(canonicalFingerprintValue);
  if (!value || typeof value !== 'object') return value;
  const normalized = {};
  Object.keys(value).sort().forEach(key => {
    normalized[key] = canonicalFingerprintValue(value[key]);
  });
  return normalized;
}

function stableFingerprint(value) {
  return JSON.stringify(canonicalFingerprintValue(value));
}

function upstreamConnectionSignature(nodeId) {
  const graph = executionDependencyConnections();
  const visited = new Set();
  const edges = new Set();
  const visit = current => {
    if (visited.has(current)) return;
    visited.add(current);
    graph.forEach(conn => {
      if (conn?.toNode !== current) return;
      edges.add([conn.fromNode, conn.fromPort, conn.toNode, conn.toPort]
        .map(value => String(value ?? '')).join('\u0000'));
      visit(conn.fromNode);
    });
  };
  visit(nodeId);
  return [...edges].sort();
}

function placerModelIdentity(nodeId) {
  const context = getModelContextForNode(nodeId);
  if (!context) return null;
  const model = context.model || {};
  return {
    networkIrHash: context.networkIrHash || model.network_ir_hash || null,
    networkIr: context.networkIr || model.network_ir || null,
    modelInputFingerprint: context.inputFingerprint || null,
    sourceNodeId: context.sourceNodeId || null,
    symbols: {
      q: Array.isArray(model.q_sym) ? model.q_sym : [],
      K: Array.isArray(model.K_sym) ? model.K_sym : [],
      x: Array.isArray(model.x_sym) ? model.x_sym : [],
    },
  };
}

function placerLifecycleFor(owner) {
  if (!owner[PLACER_RESULT_LIFECYCLE_KEY]) {
    owner[PLACER_RESULT_LIFECYCLE_KEY] = createExecutionLifecycle();
  }
  return owner[PLACER_RESULT_LIFECYCLE_KEY];
}

function syncPlacerLifecycle(owner, lifecycle = owner?.[PLACER_RESULT_LIFECYCLE_KEY]) {
  if (!owner || !lifecycle) return;
  owner.data = owner.data || {};
  owner.data.lifecycle = serializeExecutionLifecycle(lifecycle);
}

function placerContext(nodeId, endpoint, request) {
  const owner = nodeRegistry[nodeId];
  if (!owner) return null;
  return {
    owner,
    workspaceEpoch: getWorkspaceRuntimeEpoch(),
    inputFingerprint: stableFingerprint({
      endpoint,
      request,
      upstreamConnections: upstreamConnectionSignature(nodeId),
      model: placerModelIdentity(nodeId),
    }),
    endpoint,
  };
}

function currentPlacerEnvelope(nodeId) {
  const lifecycle = nodeRegistry[nodeId]?.[PLACER_RESULT_LIFECYCLE_KEY];
  return lifecycle ? readCurrentResult(lifecycle) : null;
}

export function readCurrentPlacerResult(nodeId) {
  return currentPlacerEnvelope(nodeId)?.placement || null;
}

export function inspectPlacerExecution(nodeId) {
  const lifecycle = nodeRegistry[nodeId]?.[PLACER_RESULT_LIFECYCLE_KEY];
  return lifecycle ? inspectExecutionLifecycle(lifecycle) : null;
}

function placerEvidence(endpoint, response) {
  return {
    endpoint,
    evidence_grade: 'current-computation',
    partial: response?.partial === true,
  };
}

function settlePlacerLoading(nodeId, lifecycle, ticket) {
  const owner = nodeRegistry[nodeId];
  if (!owner || owner[PLACER_RESULT_LIFECYCLE_KEY] !== lifecycle) return;
  releaseLifecycle(lifecycle, ticket);
  syncPlacerLifecycle(owner, lifecycle);
  setNodeLoading(nodeId, inspectExecutionLifecycle(lifecycle).loading);
}

function clearPlacerRuntimeTimers(owner) {
  if (!owner) return;
  if (owner._placerPlotTimer != null) clearTimeout(owner._placerPlotTimer);
  if (owner._placerTuneTimer != null) clearTimeout(owner._placerTuneTimer);
  delete owner._placerPlotTimer;
  delete owner._placerTuneTimer;
  delete owner._placerPendingTune;
}

function cloneJson(value) {
  if (value == null) return value;
  if (typeof globalThis.structuredClone === 'function') return globalThis.structuredClone(value);
  return JSON.parse(JSON.stringify(value));
}

function clearPersistedPlacerResults(owner) {
  if (!owner?.data) return;
  delete owner.data.placerResult;
  delete owner.data.placerMenu;
  delete owner.data.evidence;
}

function retirePlacerBeforePrerequisite(nodeId, reason) {
  const owner = nodeRegistry[nodeId];
  const lifecycle = owner?.[PLACER_RESULT_LIFECYCLE_KEY];
  if (!owner || !lifecycle) return;
  clearPlacerRuntimeTimers(owner);
  const runtime = inspectExecutionLifecycle(lifecycle);
  if (runtime.owner === owner && runtime.workspaceEpoch != null) {
    invalidateLifecycle(lifecycle, {
      owner,
      workspaceEpoch: runtime.workspaceEpoch,
      reason,
    });
    syncPlacerLifecycle(owner, lifecycle);
  }
  setNodeLoading(nodeId, false);
}

function currentContextOrDrift(run, descriptorResolver) {
  try {
    return placerContext(run.nodeId, run.endpoint, descriptorResolver());
  } catch (error) {
    const owner = nodeRegistry[run.nodeId];
    if (!owner) return null;
    return {
      owner,
      workspaceEpoch: getWorkspaceRuntimeEpoch(),
      inputFingerprint: `unavailable:${error?.message || 'placer-context-error'}`,
      endpoint: run.endpoint,
    };
  }
}

function currentRunEnvelope(lifecycle) {
  const runtime = inspectExecutionLifecycle(lifecycle);
  return {
    envelope: readCurrentResult(lifecycle),
    evidence: runtime.evidence,
    sessionId: runtime.sessionId,
  };
}

async function acquirePlacerRun(nodeId, endpoint, descriptor, descriptorResolver) {
  const owner = nodeRegistry[nodeId];
  if (!owner) return null;
  const lifecycle = placerLifecycleFor(owner);
  const preserved = currentRunEnvelope(lifecycle);
  retirePlacerBeforePrerequisite(nodeId, 'placer-replacement-run-started');

  let sessionId;
  try {
    sessionId = await ensureModelSession(nodeId, {
      connectionResolver: executionDependencyConnections,
    });
  } catch (error) {
    if (nodeRegistry[nodeId] !== owner) return null;
    const context = placerContext(nodeId, endpoint, descriptor);
    if (!context) return null;
    const ticket = beginLifecycle(lifecycle, context);
    failLifecycle(lifecycle, ticket, { context, error });
    clearPersistedPlacerResults(owner);
    syncPlacerLifecycle(owner, lifecycle);
    const contentEl = document.getElementById(`${nodeId}-content`);
    if (contentEl) contentEl.innerHTML = `<div class="node-error">${escapeHtml(error.message)}</div>`;
    settlePlacerLoading(nodeId, lifecycle, ticket);
    return null;
  }

  if (nodeRegistry[nodeId] !== owner) return null;
  const beginContext = placerContext(nodeId, endpoint, descriptor);
  if (!beginContext) return null;
  const ticket = beginLifecycle(lifecycle, beginContext);
  clearPersistedPlacerResults(owner);
  syncPlacerLifecycle(owner, lifecycle);
  setNodeLoading(nodeId, true);
  const run = {
    beginContext,
    endpoint,
    lifecycle,
    nodeId,
    owner,
    preserved,
    sessionId,
    ticket,
  };
  const currentContext = currentContextOrDrift(run, descriptorResolver);
  if (!currentContext || !isCurrentLifecycle(lifecycle, ticket, currentContext)) {
    if (currentContext) {
      failLifecycle(lifecycle, ticket, {
        context: currentContext,
        error: new Error('Parameter Placer inputs changed before execution began'),
      });
      syncPlacerLifecycle(owner, lifecycle);
    }
    settlePlacerLoading(nodeId, lifecycle, ticket);
    return null;
  }
  return run;
}

function storePlacerEnvelope(run, envelope, evidence) {
  const { owner } = run;
  owner.data = owner.data || {};
  if (envelope.placement) owner.data.placerResult = envelope.placement;
  else delete owner.data.placerResult;
  if (envelope.menu) owner.data.placerMenu = envelope.menu;
  else delete owner.data.placerMenu;
  if (envelope.placement) owner.data.evidence = evidence;
  else delete owner.data.evidence;
  syncPlacerLifecycle(owner, run.lifecycle);
}

function commitPlacerRun(run, descriptorResolver, envelope, response, sessionId = run.sessionId) {
  const context = currentContextOrDrift(run, descriptorResolver);
  if (!context) return false;
  const evidence = placerEvidence(run.endpoint, response);
  if (!commitLifecycle(run.lifecycle, run.ticket, {
    context,
    result: envelope,
    evidence,
    sessionId,
  })) return false;
  storePlacerEnvelope(run, envelope, evidence);
  return true;
}

function failPlacerRun(run, descriptorResolver, error) {
  const context = currentContextOrDrift(run, descriptorResolver);
  if (!context) return false;
  const wasCurrent = isCurrentLifecycle(run.lifecycle, run.ticket, context);
  const failed = failLifecycle(run.lifecycle, run.ticket, { context, error });
  if (failed) clearPersistedPlacerResults(run.owner);
  syncPlacerLifecycle(run.owner, run.lifecycle);
  return wasCurrent && failed;
}

function recordBlockedPlacerRun(nodeId, endpoint, reason) {
  const owner = nodeRegistry[nodeId];
  if (!owner) return false;
  const lifecycle = placerLifecycleFor(owner);
  retirePlacerBeforePrerequisite(nodeId, 'placer-blocked-at-preflight');
  const context = placerContext(nodeId, endpoint, { blocked: reason });
  if (!context) return false;
  const ticket = beginLifecycle(lifecycle, context);
  blockLifecycle(lifecycle, ticket, { context, reason });
  clearPersistedPlacerResults(owner);
  syncPlacerLifecycle(owner, lifecycle);
  settlePlacerLoading(nodeId, lifecycle, ticket);
  return false;
}

function scheduleCurrentPlacerPlot(run, descriptorResolver, callback, delay = 50) {
  const { owner } = run;
  clearPlacerRuntimeTimers(owner);
  const timer = setTimeout(() => {
    if (nodeRegistry[run.nodeId] !== owner) return;
    const context = currentContextOrDrift(run, descriptorResolver);
    if (!context || !isCurrentLifecycle(run.lifecycle, run.ticket, context)) return;
    if (owner._placerPlotTimer === timer) delete owner._placerPlotTimer;
    callback();
  }, delay);
  owner._placerPlotTimer = timer;
  return timer;
}

export const PLACER_TYPES = {
  'placer-params': {
    category: 'parameter',
    headerClass: 'header-parameter',
    title: 'Parameter Placer Config',
    inputs: [{ port: 'model', type: 'ModelArtifact', label: 'Model' }],
    outputs: [{ port: 'params', type: 'ParameterPlacerConfig', label: 'Config' }],
    defaultWidth: 320,
    createBody(nodeId) {
      return `
        <div class="param-row">
          <label>Input total:</label>
          <select id="${nodeId}-input" class="auto-update"></select>
        </div>
        <div class="param-row">
          <label>Output species:</label>
          <select id="${nodeId}-output" class="auto-update"></select>
        </div>
        <div class="param-row">
          <label>Target reaction order:</label>
          <input type="number" id="${nodeId}-target" value="1" step="1" class="auto-update">
        </div>
        <div class="param-row">
          <label>Kd bounds (log10):</label>
          <div style="display:flex;gap:4px;align-items:center;">
            <input type="number" id="${nodeId}-kdlo" value="-3" step="0.5" style="width:70px;" class="auto-update">
            to
            <input type="number" id="${nodeId}-kdhi" value="3" step="0.5" style="width:70px;" class="auto-update">
          </div>
        </div>
      `;
    },
    onInit(nodeId) { setupAutoUpdate(nodeId, 'placer-params'); },
    async prepare(nodeId) {
      const model = getModelForNode(nodeId);
      if (!model) return;
      syncSelectOptions(document.getElementById(`${nodeId}-input`), model.q_sym);
      syncSelectOptions(document.getElementById(`${nodeId}-output`), model.x_sym);
      triggerConfigUpdate(nodeId, 'placer-params');
    },
  },
  'placer-result': {
    category: 'result',
    headerClass: 'header-result',
    title: 'Parameter Placer',
    inputs: [{ port: 'params', type: 'ParameterPlacerConfig', label: 'Config' }],
    outputs: [],
    defaultWidth: 480,
    createBody(nodeId) {
      return `
        <button class="btn btn-run" data-action="realizePlacerProgram" data-node="${nodeId}">Realize target program</button>
        <div style="display:flex;gap:6px;flex-wrap:wrap;margin-top:6px;">
          <button class="btn btn-small" data-action="loadPlacerMenu" data-node="${nodeId}">Show achievable menu</button>
          <button class="btn btn-small" data-action="executePlacerResult" data-node="${nodeId}">Solve single RO</button>
        </div>
        <div id="${nodeId}-menu" style="margin-top:6px;"></div>
        <div class="viewer-content" id="${nodeId}-content">
          <span class="text-dim"><b>Realize target program</b> solves a Kd <i>ordering</i> so the dose-response walks the whole regime program (no sampling). <b>Show achievable menu</b> and <b>Solve single RO</b> are per-regime fine-tuning handles.</span>
        </div>
      `;
    },
    async execute(nodeId) { return realizePlacerProgram(nodeId); },
  },
};

function getPlacerConfig(nodeId, { notify = true, synchronize = true } = {}) {
  const conn = connections.find(c => c.toNode === nodeId && c.toPort === 'params');
  if (!conn) {
    if (notify) alert('Please connect to a Parameter Placer Config node');
    return null;
  }
  const paramsNode = nodeRegistry[conn.fromNode];
  if (!paramsNode) {
    if (notify) alert('Config node missing');
    return null;
  }
  if (synchronize) triggerConfigUpdate(conn.fromNode, paramsNode.type || 'placer-params');
  const config = paramsNode.data?.config;
  if (!config) {
    if (notify) alert('Configure the Placer Config node first');
    return null;
  }
  if (!config.input_sym || !config.output_sym) {
    if (notify) alert('Select an input total and an output species in the config node');
    return null;
  }
  return cloneJson(config);
}

function kdBoundsFromConfig(config) {
  return Number.isFinite(config?.kd_lo) && Number.isFinite(config?.kd_hi)
    ? [config.kd_lo, config.kd_hi]
    : null;
}

function descriptorForConfig(endpoint, config, request) {
  return { endpoint, config, request };
}

// MENU — achievable RO ladder (clickable rungs) + regime sequence + threshold control.
export async function loadPlacerMenu(nodeId) {
  const config = getPlacerConfig(nodeId);
  if (!config) {
    return recordBlockedPlacerRun(
      nodeId, PLACER_ENDPOINTS.menu, 'Parameter Placer configuration is unavailable',
    );
  }
  const menuEl = document.getElementById(`${nodeId}-menu`);
  const request = { input_sym: config.input_sym, output_sym: config.output_sym };
  const descriptor = descriptorForConfig(PLACER_ENDPOINTS.menu, config, request);
  const descriptorResolver = () => {
    const currentConfig = getPlacerConfig(nodeId, { notify: false });
    if (!currentConfig) throw new Error('Parameter Placer configuration disconnected');
    return descriptorForConfig(PLACER_ENDPOINTS.menu, currentConfig, {
      input_sym: currentConfig.input_sym,
      output_sym: currentConfig.output_sym,
    });
  };
  const run = await acquirePlacerRun(
    nodeId, PLACER_ENDPOINTS.menu, descriptor, descriptorResolver,
  );
  if (!run) return null;
  try {
    const data = await api('placer_menu', {
      session_id: run.sessionId,
      ...request,
    }, {
      statusIsCurrent: () => isCurrentLifecycle(
        run.lifecycle, run.ticket, run.beginContext,
      ),
    });
    const envelope = {
      placement: run.preserved.envelope?.placement || null,
      menu: cloneJson(data),
    };
    if (!commitPlacerRun(run, descriptorResolver, envelope, data)) return null;
    const ladder = data.ladder || [];
    const path = data.path || [];
    if (!ladder.length) {
      if (menuEl) {
        menuEl.innerHTML = `<span class="text-dim">No achievable reaction orders found for this output.</span>`;
      }
      return data;
    }
    // (a) slope rungs
    const chips = ladder.map(ro =>
      `<span class="summary-chip placer-chip" data-ro="${ro}" title="Solve for RO = ${ro}" style="cursor:pointer;">${fmtNum(ro)}</span>`).join(' ');
    // (b) regime sequence + transitions for threshold placement
    const seq = path.map(p => p.ro === null ? '∞' : fmtNum(p.ro)).join(' → ');
    const transitions = [];
    for (let i = 0; i + 1 < path.length; i++) {
      const a = path[i], b = path[i + 1];
      if (a.ro === null || b.ro === null || a.ro === b.ro) continue;
      transitions.push({ from: a.vertex_idx, to: b.vertex_idx, label: `RO ${fmtNum(a.ro)} → ${fmtNum(b.ro)}` });
    }
    const transOpts = transitions.map(t =>
      `<option value="${t.from}:${t.to}">${t.label}</option>`).join('');
    const threshBlock = transitions.length ? `
      <div style="margin-top:8px;border-top:0.5px solid var(--panel-border);padding-top:6px;">
        <div style="font-size:12px;color:var(--text-dim);margin-bottom:3px;">Place a threshold (set where a transition happens):</div>
        <div style="display:flex;gap:5px;align-items:center;flex-wrap:wrap;">
          <select id="${nodeId}-trans">${transOpts}</select>
          <span style="font-size:12px;">at input</span>
          <input type="number" id="${nodeId}-thresh-dose" value="1" step="any" style="width:90px;">
          <button class="btn" id="${nodeId}-thresh-btn">Place</button>
        </div>
      </div>` : '';
    if (!menuEl) return data;
    menuEl.innerHTML =
      `<div style="font-size:12px;color:var(--text-dim);margin-bottom:3px;">Achievable reaction orders (click a rung to solve):</div>` +
      `<div style="display:flex;gap:4px;flex-wrap:wrap;">${chips}</div>` +
      (seq ? `<div style="font-size:12px;color:var(--text-dim);margin-top:5px;">Response regimes: ${seq}</div>` : '') +
      threshBlock;
    menuEl.querySelectorAll('.placer-chip').forEach(chip => {
      chip.addEventListener('click', () => solvePlacer(nodeId, parseFloat(chip.dataset.ro)));
    });
    const tb = document.getElementById(`${nodeId}-thresh-btn`);
    if (tb) tb.addEventListener('click', () => placeThreshold(nodeId));
    return data;
  } catch (e) {
    if (failPlacerRun(run, descriptorResolver, e) && menuEl) {
      menuEl.innerHTML = `<div class="node-error">${escapeHtml(e.message)}</div>`;
    }
    return null;
  } finally {
    settlePlacerLoading(nodeId, run.lifecycle, run.ticket);
  }
}

// SLOPE — "Solve (target RO)" button uses the config's typed target.
export async function executePlacerResult(nodeId) {
  const config = getPlacerConfig(nodeId);
  if (!config) {
    return recordBlockedPlacerRun(
      nodeId, PLACER_ENDPOINTS.solve, 'Parameter Placer configuration is unavailable',
    );
  }
  if (!Number.isFinite(config.target_ro)) {
    alert('Enter a numeric target reaction order');
    return recordBlockedPlacerRun(
      nodeId, PLACER_ENDPOINTS.solve, 'Target reaction order must be numeric',
    );
  }
  return solvePlacer(nodeId, config.target_ro);
}

async function solvePlacer(nodeId, targetRO) {
  const config = getPlacerConfig(nodeId);
  if (!config) {
    return recordBlockedPlacerRun(
      nodeId, PLACER_ENDPOINTS.solve, 'Parameter Placer configuration is unavailable',
    );
  }
  const kdBounds = kdBoundsFromConfig(config);
  const request = {
    input_sym: config.input_sym,
    output_sym: config.output_sym,
    target_ro: targetRO,
    kd_bounds: kdBounds,
  };
  const descriptor = descriptorForConfig(PLACER_ENDPOINTS.solve, config, request);
  const descriptorResolver = () => {
    const currentConfig = getPlacerConfig(nodeId, { notify: false });
    if (!currentConfig) throw new Error('Parameter Placer configuration disconnected');
    return descriptorForConfig(PLACER_ENDPOINTS.solve, currentConfig, {
      input_sym: currentConfig.input_sym,
      output_sym: currentConfig.output_sym,
      target_ro: targetRO,
      kd_bounds: kdBoundsFromConfig(currentConfig),
    });
  };
  const run = await acquirePlacerRun(
    nodeId, PLACER_ENDPOINTS.solve, descriptor, descriptorResolver,
  );
  if (!run) return false;
  const contentEl = document.getElementById(`${nodeId}-content`);
  try {
    const data = await api('place_parameters', {
      session_id: run.sessionId,
      ...request,
    }, {
      statusIsCurrent: () => isCurrentLifecycle(
        run.lifecycle, run.ticket, run.beginContext,
      ),
    });
    const placement = createPlacementResult(
      PLACER_ENDPOINTS.solve,
      'solve',
      config,
      data,
      data.kd_bounds || kdBounds || [-3, 3],
    );
    const envelope = {
      placement,
      menu: run.preserved.envelope?.menu || null,
    };
    if (!commitPlacerRun(run, descriptorResolver, envelope, data)) return false;
    const summary =
      `<span class="summary-chip">target RO = ${escapeHtml(String(targetRO))}</span>` +
      `<span class="summary-chip">predicted = ${fmtNum(data.predicted_RO)}</span>` +
      `<span class="summary-chip">measured = ${fmtNum(data.measured_RO)}</span>` +
      `<span class="summary-chip">${passBadge(data.pass)}</span>`;
    renderSolved(nodeId, contentEl, placement, summary, run, descriptorResolver);
    commitWorkspaceSnapshot('parameter-placer');
    return true;
  } catch (e) {
    if (failPlacerRun(run, descriptorResolver, e) && contentEl) {
      contentEl.innerHTML = `<div class="node-error">${escapeHtml(e.message)}</div>`;
    }
    return false;
  } finally {
    settlePlacerLoading(nodeId, run.lifecycle, run.ticket);
  }
}

// PROGRAM — realize the whole regime program (the design target is a program, not a
// single slope): solve a Kd ORDERING so the swept dose-response walks the full
// regime sequence. [/api/v1/placer_realize_program]
export async function realizePlacerProgram(nodeId) {
  const config = getPlacerConfig(nodeId);
  if (!config) {
    return recordBlockedPlacerRun(
      nodeId, PLACER_ENDPOINTS.program, 'Parameter Placer configuration is unavailable',
    );
  }
  const kdBounds = kdBoundsFromConfig(config);
  const request = {
    input_sym: config.input_sym,
    output_sym: config.output_sym,
    kd_bounds: kdBounds,
  };
  const descriptor = descriptorForConfig(PLACER_ENDPOINTS.program, config, request);
  const descriptorResolver = () => {
    const currentConfig = getPlacerConfig(nodeId, { notify: false });
    if (!currentConfig) throw new Error('Parameter Placer configuration disconnected');
    return descriptorForConfig(PLACER_ENDPOINTS.program, currentConfig, {
      input_sym: currentConfig.input_sym,
      output_sym: currentConfig.output_sym,
      kd_bounds: kdBoundsFromConfig(currentConfig),
    });
  };
  const run = await acquirePlacerRun(
    nodeId, PLACER_ENDPOINTS.program, descriptor, descriptorResolver,
  );
  if (!run) return false;
  const contentEl = document.getElementById(`${nodeId}-content`);
  try {
    const data = await api('placer_realize_program', {
      session_id: run.sessionId,
      ...request,
    }, {
      statusIsCurrent: () => isCurrentLifecycle(
        run.lifecycle, run.ticket, run.beginContext,
      ),
    });
    const placement = createPlacementResult(
      PLACER_ENDPOINTS.program,
      'program',
      config,
      data,
      kdBounds || [-3, 3],
    );
    const envelope = {
      placement,
      menu: run.preserved.envelope?.menu || null,
    };
    if (!commitPlacerRun(run, descriptorResolver, envelope, data)) return false;
    const bps = (data.breakpoints || []).map(fmtSci).join(', ');
    const summary =
      `<span class="summary-chip">target ${escapeHtml(signArrows(data.target_signs))}</span>` +
      `<span class="summary-chip">realized ${escapeHtml(signArrows(data.measured_signs))}</span>` +
      `<span class="summary-chip">${passBadge(data.pass)}</span>` +
      (bps ? `<span class="summary-chip">breakpoints: ${escapeHtml(bps)}</span>` : '');
    renderSolved(nodeId, contentEl, placement, summary, run, descriptorResolver);
    // The realized RO itinerary, as a mono detail line under the summary chips.
    const detail = document.createElement('div');
    detail.className = 'text-dim';
    detail.style.cssText = "font-family:'Consolas',monospace;";
    detail.textContent =
      `target program:  ${fmtSeq(data.target_program)}    realized:  ${fmtSeq(data.measured_program)}`;
    contentEl?.querySelector('.siso-summary-line')?.after(detail);
    commitWorkspaceSnapshot('parameter-placer');
    return true;
  } catch (e) {
    if (failPlacerRun(run, descriptorResolver, e) && contentEl) {
      contentEl.innerHTML = `<div class="node-error">${escapeHtml(e.message)}</div>`;
    }
    return false;
  } finally {
    settlePlacerLoading(nodeId, run.lifecycle, run.ticket);
  }
}

// THRESHOLD — place a chosen transition at a target input dose.
async function placeThreshold(nodeId) {
  const config = getPlacerConfig(nodeId);
  if (!config) {
    return recordBlockedPlacerRun(
      nodeId, PLACER_ENDPOINTS.threshold, 'Parameter Placer configuration is unavailable',
    );
  }
  const transSel = document.getElementById(`${nodeId}-trans`);
  const doseEl = document.getElementById(`${nodeId}-thresh-dose`);
  if (!transSel || !transSel.value) {
    alert('Pick a transition');
    return recordBlockedPlacerRun(nodeId, PLACER_ENDPOINTS.threshold, 'Pick a transition');
  }
  const [fromIdx, toIdx] = transSel.value.split(':').map(Number);
  const target = parseFloat(doseEl?.value);
  if (!Number.isFinite(target) || target <= 0) {
    alert('Enter a positive target input dose');
    return recordBlockedPlacerRun(
      nodeId, PLACER_ENDPOINTS.threshold, 'Target input dose must be positive',
    );
  }
  const request = {
    input_sym: config.input_sym,
    output_sym: config.output_sym,
    from_idx: fromIdx,
    to_idx: toIdx,
    target_input: target,
  };
  const descriptor = descriptorForConfig(PLACER_ENDPOINTS.threshold, config, request);
  const descriptorResolver = () => {
    const currentConfig = getPlacerConfig(nodeId, { notify: false });
    const currentTransition = document.getElementById(`${nodeId}-trans`)?.value || '';
    const currentTarget = parseFloat(document.getElementById(`${nodeId}-thresh-dose`)?.value);
    if (!currentConfig || !currentTransition) throw new Error('Threshold inputs disconnected');
    const [currentFrom, currentTo] = currentTransition.split(':').map(Number);
    return descriptorForConfig(PLACER_ENDPOINTS.threshold, currentConfig, {
      input_sym: currentConfig.input_sym,
      output_sym: currentConfig.output_sym,
      from_idx: currentFrom,
      to_idx: currentTo,
      target_input: currentTarget,
    });
  };
  const run = await acquirePlacerRun(
    nodeId, PLACER_ENDPOINTS.threshold, descriptor, descriptorResolver,
  );
  if (!run) return false;
  const contentEl = document.getElementById(`${nodeId}-content`);
  try {
    const data = await api('placer_threshold', {
      session_id: run.sessionId,
      ...request,
    }, {
      statusIsCurrent: () => isCurrentLifecycle(
        run.lifecycle, run.ticket, run.beginContext,
      ),
    });
    const placement = createPlacementResult(
      PLACER_ENDPOINTS.threshold,
      'threshold',
      config,
      data,
      [-3, 3],
    );
    const envelope = {
      placement,
      menu: run.preserved.envelope?.menu || null,
    };
    if (!commitPlacerRun(run, descriptorResolver, envelope, data)) return false;
    const summary =
      `<span class="summary-chip">${escapeHtml(transSel.options[transSel.selectedIndex].text)}</span>` +
      `<span class="summary-chip">target dose = ${fmtSci(target)}</span>` +
      `<span class="summary-chip">placed at = ${fmtSci(data.measured_breakpoint)}</span>` +
      `<span class="summary-chip">${passBadge(data.pass)}</span>`;
    renderSolved(nodeId, contentEl, placement, summary, run, descriptorResolver);
    commitWorkspaceSnapshot('parameter-placer');
    return true;
  } catch (e) {
    if (failPlacerRun(run, descriptorResolver, e) && contentEl) {
      contentEl.innerHTML = `<div class="node-error">${escapeHtml(e.message)}</div>`;
    }
    return false;
  } finally {
    settlePlacerLoading(nodeId, run.lifecycle, run.ticket);
  }
}

function createPlacementResult(endpoint, action, config, data, kdBounds) {
  return {
    endpoint,
    action,
    config: cloneJson(config),
    kd: (data.kd || []).slice(),
    totals: cloneJson(data.totals || {}),
    kdBounds: (kdBounds || [-3, 3]).slice(),
    response: cloneJson(data),
  };
}

// Common render: summary chips + solved kd/totals/dominance + live Kd slider + curve.
function renderSolved(nodeId, contentEl, placement, summaryHtml, run, descriptorResolver) {
  if (!contentEl) return;
  const data = placement.response || {};
  const kd = placement.kd || [];
  const totals = placement.totals || {};
  const kdRows = kd.map((v, i) => `Kd${i + 1} = ${fmtSci(v)}`).join(', ');
  const totalRows = Object.keys(totals).sort().map(k => `${escapeHtml(k)} = ${fmtSci(totals[k])}`).join(', ');
  const dominance = (data.dominance_ordering || []).map(escapeHtml).join('; ');
  const bounds = placement.kdBounds || [-3, 3];
  const kdOpts = kd.map((_, i) => `<option value="${i}">Kd${i + 1}</option>`).join('');
  const sliderBlock = kd.length ? `
    <div class="param-row" style="display:block;margin-top:8px;border-top:0.5px solid var(--panel-border);padding-top:6px;">
      <div style="font-size:12px;color:var(--text-dim);margin-bottom:3px;">Live tune (drag to move the response along the ladder):</div>
      <div style="display:flex;gap:6px;align-items:center;">
        <select id="${nodeId}-kdsel">${kdOpts}</select>
        <input type="range" id="${nodeId}-kdslider" min="${bounds[0]}" max="${bounds[1]}" step="0.05" value="${Math.log10(kd[0] || 1)}" style="flex:1;">
        <span id="${nodeId}-kdval" style="font-size:12px;min-width:74px;">Kd1 = ${fmtSci(kd[0])}</span>
      </div>
    </div>` : '';
  const totalOpts = Object.keys(totals).map(t => `<option value="${t}">${t}</option>`).join('');
  const levelBlock = (kd.length && totalOpts) ? `
    <div class="param-row" style="display:block;margin-top:8px;border-top:0.5px solid var(--panel-border);padding-top:6px;">
      <div style="font-size:12px;color:var(--text-dim);margin-bottom:3px;">Set output level (numeric — the quantitative layer):</div>
      <div style="display:flex;gap:5px;align-items:center;flex-wrap:wrap;">
        adjust <select id="${nodeId}-leveltotal">${totalOpts}</select>
        so [${escapeHtml(placement.config?.output_sym || '')}]=<input type="number" id="${nodeId}-leveltarget" value="1" step="any" style="width:72px;">
        at input <input type="number" id="${nodeId}-levelinput" value="100" step="any" style="width:72px;">
        <button class="btn" id="${nodeId}-levelbtn">Set</button>
      </div>
      <div id="${nodeId}-levelnote" style="font-size:12px;color:var(--text-dim);margin-top:3px;"></div>
    </div>` : '';

  contentEl.innerHTML = `
    <div class="siso-summary-line" style="flex-wrap:wrap;gap:6px;">${summaryHtml}</div>
    <div class="param-row" style="display:block;margin-top:6px;">
      <div><strong>Solved Kd:</strong> ${kdRows || '<span class="text-dim">none</span>'}</div>
      <div><strong>Totals:</strong> ${totalRows || '<span class="text-dim">none</span>'}</div>
      <div><strong>Dominance:</strong> ${dominance || '<span class="text-dim">n/a</span>'}</div>
    </div>
    ${sliderBlock}
    ${levelBlock}
    <div class="plot-container" id="${nodeId}-plot"></div>
  `;

  scheduleCurrentPlacerPlot(run, descriptorResolver, () => {
    if (data.dose_response_curve) {
      plotParameterScan1D(data.dose_response_curve, `${nodeId}-plot`);
      setupPlotResize(nodeId, `${nodeId}-plot`);
    }
  });

  const sel = document.getElementById(`${nodeId}-kdsel`);
  const slider = document.getElementById(`${nodeId}-kdslider`);
  if (sel && slider) {
    sel.addEventListener('change', () => {
      const idx = parseInt(sel.value, 10) || 0;
      const cur = (readCurrentPlacerResult(nodeId)?.kd || [])[idx];
      if (Number.isFinite(cur)) slider.value = String(Math.log10(cur));
      const valEl = document.getElementById(`${nodeId}-kdval`);
      if (valEl) valEl.textContent = `Kd${idx + 1} = ${fmtSci(cur)}`;
    });
    slider.addEventListener('input', () => {
      const owner = nodeRegistry[nodeId];
      if (!owner) return;
      let pending = owner._placerPendingTune;
      if (!pending) {
        const lifecycle = owner[PLACER_RESULT_LIFECYCLE_KEY];
        const runtime = lifecycle ? inspectExecutionLifecycle(lifecycle) : null;
        const currentPlacement = lifecycle ? readCurrentResult(lifecycle)?.placement : null;
        if (!runtime?.sessionId || !currentPlacement) return;
        pending = {
          owner,
          placement: cloneJson(currentPlacement),
          menu: cloneJson(readCurrentResult(lifecycle)?.menu || null),
          sessionId: runtime.sessionId,
        };
        owner._placerPendingTune = pending;
        retirePlacerBeforePrerequisite(nodeId, 'placer-live-tune-input-changed');
        owner._placerPendingTune = pending;
      }
      const idx = parseInt(sel.value, 10) || 0;
      pending.kd = (pending.kd || pending.placement.kd || []).slice();
      pending.kd[idx] = Math.pow(10, parseFloat(slider.value));
      const valEl = document.getElementById(`${nodeId}-kdval`);
      if (valEl) valEl.textContent = `Kd${idx + 1} = ${fmtSci(pending.kd[idx])}`;
      if (owner._placerTuneTimer != null) clearTimeout(owner._placerTuneTimer);
      const timer = setTimeout(() => {
        if (owner._placerTuneTimer === timer) delete owner._placerTuneTimer;
        if (owner._placerPendingTune === pending) delete owner._placerPendingTune;
        void livePlacerTune(nodeId, pending);
      }, 120);
      owner._placerTuneTimer = timer;
    });
  }
  const levelBtn = document.getElementById(`${nodeId}-levelbtn`);
  if (levelBtn) levelBtn.addEventListener('click', () => placeLevel(nodeId));
}

// LEVEL handle (numeric, quantitative layer): adjust a total so the output reaches
// a target level at an operating input. [/api/v1/placer_level]
async function placeLevel(nodeId) {
  const st = readCurrentPlacerResult(nodeId);
  if (!st) {
    return recordBlockedPlacerRun(
      nodeId, PLACER_ENDPOINTS.level,
      'Run Parameter Placer again before changing a quantitative level',
    );
  }
  const adjust = document.getElementById(`${nodeId}-leveltotal`)?.value;
  const target = parseFloat(document.getElementById(`${nodeId}-leveltarget`)?.value);
  const operating = parseFloat(document.getElementById(`${nodeId}-levelinput`)?.value);
  if (!adjust || !Number.isFinite(target) || target <= 0 || !Number.isFinite(operating) || operating <= 0) {
    alert('Pick a total and enter a positive target level + operating input');
    return recordBlockedPlacerRun(
      nodeId, PLACER_ENDPOINTS.level, 'Level controls must contain positive values',
    );
  }
  const request = {
    input_sym: st.config.input_sym,
    output_sym: st.config.output_sym,
    kd: st.kd,
    totals: st.totals,
    operating_input: operating,
    target_level: target,
    adjust_sym: adjust,
  };
  const descriptor = descriptorForConfig(PLACER_ENDPOINTS.level, st.config, request);
  const descriptorResolver = () => {
    const currentConfig = getPlacerConfig(nodeId, { notify: false });
    const currentAdjust = document.getElementById(`${nodeId}-leveltotal`)?.value || '';
    const currentTarget = parseFloat(document.getElementById(`${nodeId}-leveltarget`)?.value);
    const currentOperating = parseFloat(document.getElementById(`${nodeId}-levelinput`)?.value);
    if (!currentConfig || !currentAdjust) throw new Error('Level controls disconnected');
    return descriptorForConfig(PLACER_ENDPOINTS.level, currentConfig, {
      input_sym: currentConfig.input_sym,
      output_sym: currentConfig.output_sym,
      kd: st.kd,
      totals: st.totals,
      operating_input: currentOperating,
      target_level: currentTarget,
      adjust_sym: currentAdjust,
    });
  };
  const run = await acquirePlacerRun(
    nodeId, PLACER_ENDPOINTS.level, descriptor, descriptorResolver,
  );
  if (!run) return false;
  const note = document.getElementById(`${nodeId}-levelnote`);
  try {
    const data = await api('placer_level', {
      session_id: run.sessionId,
      ...request,
    }, {
      statusIsCurrent: () => isCurrentLifecycle(
        run.lifecycle, run.ticket, run.beginContext,
      ),
    });
    const placement = {
      ...cloneJson(st),
      endpoint: PLACER_ENDPOINTS.level,
      action: 'level',
      totals: cloneJson(data.totals || st.totals),
      response: { ...cloneJson(st.response || {}), ...cloneJson(data) },
    };
    const envelope = {
      placement,
      menu: run.preserved.envelope?.menu || null,
    };
    if (!commitPlacerRun(run, descriptorResolver, envelope, data)) return false;
    if (note) note.innerHTML = `${escapeHtml(data.adjusted_total)} = ${fmtSci(data.adjusted_value)} → ` +
      `[${escapeHtml(st.config.output_sym)}] ≈ ${fmtSci(data.achieved_level)}` +
      (data.feasible ? '' : ' <span style="color:var(--status-error);">(not reachable in range)</span>');
    if (data.dose_response_curve) {
      plotParameterScan1D(data.dose_response_curve, `${nodeId}-plot`);
      setupPlotResize(nodeId, `${nodeId}-plot`);
    }
    commitWorkspaceSnapshot('parameter-placer-level');
    return true;
  } catch (e) {
    if (failPlacerRun(run, descriptorResolver, e) && note) {
      note.innerHTML = `<span class="node-error">${escapeHtml(e.message)}</span>`;
    }
    return false;
  } finally {
    settlePlacerLoading(nodeId, run.lifecycle, run.ticket);
  }
}

async function livePlacerTune(nodeId, pending) {
  const owner = nodeRegistry[nodeId];
  if (!owner || pending?.owner !== owner || !pending.sessionId) return false;
  const lifecycle = placerLifecycleFor(owner);
  const request = {
    input_sym: pending.placement.config.input_sym,
    output_sym: pending.placement.config.output_sym,
    kd: pending.kd,
    totals: pending.placement.totals,
  };
  const descriptor = descriptorForConfig(
    PLACER_ENDPOINTS.curve, pending.placement.config, request,
  );
  const descriptorResolver = () => {
    const currentConfig = getPlacerConfig(nodeId, { notify: false });
    if (!currentConfig) throw new Error('Parameter Placer configuration disconnected');
    return descriptorForConfig(PLACER_ENDPOINTS.curve, currentConfig, {
      input_sym: currentConfig.input_sym,
      output_sym: currentConfig.output_sym,
      kd: pending.kd,
      totals: pending.placement.totals,
    });
  };
  const beginContext = placerContext(nodeId, PLACER_ENDPOINTS.curve, descriptor);
  if (!beginContext) return false;
  const ticket = beginLifecycle(lifecycle, beginContext);
  clearPersistedPlacerResults(owner);
  const run = {
    beginContext,
    endpoint: PLACER_ENDPOINTS.curve,
    lifecycle,
    nodeId,
    owner,
    preserved: { envelope: { placement: pending.placement, menu: pending.menu } },
    sessionId: pending.sessionId,
    ticket,
  };
  syncPlacerLifecycle(owner, lifecycle);
  setNodeLoading(nodeId, true);
  try {
    const curve = await api('placer_curve', {
      session_id: pending.sessionId,
      ...request,
    }, {
      statusIsCurrent: () => isCurrentLifecycle(lifecycle, ticket, beginContext),
    });
    const placement = {
      ...cloneJson(pending.placement),
      endpoint: PLACER_ENDPOINTS.curve,
      action: 'curve',
      kd: pending.kd.slice(),
      response: {
        ...cloneJson(pending.placement.response || {}),
        dose_response_curve: cloneJson(curve),
      },
    };
    const envelope = { placement, menu: pending.menu || null };
    if (!commitPlacerRun(run, descriptorResolver, envelope, curve, pending.sessionId)) return false;
    plotParameterScan1D(curve, `${nodeId}-plot`);
    setupPlotResize(nodeId, `${nodeId}-plot`);
    commitWorkspaceSnapshot('parameter-placer-live-tune');
    return true;
  } catch (e) {
    if (failPlacerRun(run, descriptorResolver, e)) {
      console.warn('live tune failed:', e.message);
    }
    return false;
  } finally {
    settlePlacerLoading(nodeId, lifecycle, ticket);
  }
}

function renderHistoricalPlacerResult(nodeId, placement, lifecycle) {
  const contentEl = document.getElementById(`${nodeId}-content`);
  if (!contentEl) return;
  const kd = placement?.kd || [];
  const totals = placement?.totals || {};
  const response = placement?.response || {};
  contentEl.dataset.resultState = 'historical';
  contentEl.innerHTML = `
    <div class="workspace-historical-warning text-dim" role="status">
      Historical saved Parameter Placer result — rerun before tuning or using it as current evidence.
    </div>
    <div class="param-row" style="display:block;margin-top:6px;">
      <div><strong>Solved Kd:</strong> ${kd.map((value, idx) => `Kd${idx + 1} = ${fmtSci(value)}`).join(', ') || '<span class="text-dim">none</span>'}</div>
      <div><strong>Totals:</strong> ${Object.keys(totals).sort().map(key => `${escapeHtml(key)} = ${fmtSci(totals[key])}`).join(', ') || '<span class="text-dim">none</span>'}</div>
    </div>
    <div class="plot-container" id="${nodeId}-plot"></div>
  `;
  if (!response.dose_response_curve) return;
  const owner = nodeRegistry[nodeId];
  const revision = inspectExecutionLifecycle(lifecycle).revision;
  const timer = setTimeout(() => {
    if (nodeRegistry[nodeId] !== owner) return;
    const runtime = inspectExecutionLifecycle(lifecycle);
    if (runtime.revision !== revision || runtime.state !== 'historical' ||
        runtime.freshness !== 'historical') return;
    if (owner._placerPlotTimer === timer) delete owner._placerPlotTimer;
    plotParameterScan1D(response.dose_response_curve, `${nodeId}-plot`);
    setupPlotResize(nodeId, `${nodeId}-plot`);
  }, 50);
  owner._placerPlotTimer = timer;
}

export function restorePlacerResultView(nodeId, data = {}) {
  const owner = nodeRegistry[nodeId];
  if (!owner) return false;
  const placement = data.placerResult || null;
  const menu = data.placerMenu || null;
  if (!placement && !menu) return false;
  const lifecycle = placerLifecycleFor(owner);
  clearPlacerRuntimeTimers(owner);
  const endpoint = typeof placement?.endpoint === 'string' && placement.endpoint.trim()
    ? placement.endpoint
    : PLACER_ENDPOINTS.program;
  restoreHistoricalLifecycle(lifecycle, {
    context: placerContext(nodeId, endpoint, {
      restored: true,
      placement,
      menu,
    }),
    result: { placement, menu },
    evidence: data.lifecycle?.evidence || data.evidence || null,
  });
  const restored = inspectExecutionLifecycle(lifecycle).result;
  owner.data = owner.data || {};
  if (restored.placement) owner.data.placerResult = restored.placement;
  else delete owner.data.placerResult;
  if (restored.menu) owner.data.placerMenu = restored.menu;
  else delete owner.data.placerMenu;
  if (data.evidence != null) owner.data.evidence = cloneJson(data.evidence);
  syncPlacerLifecycle(owner, lifecycle);
  if (restored.placement) renderHistoricalPlacerResult(nodeId, restored.placement, lifecycle);
  return true;
}

function passBadge(pass) {
  return pass ? '<span style="color:var(--status-ok);font-weight:600;">PASS</span>'
              : '<span style="color:var(--status-error);font-weight:600;">FAIL</span>';
}
function fmtSci(x) {
  const n = Number(x);
  if (!Number.isFinite(n)) return String(x);
  return (n !== 0 && (Math.abs(n) < 1e-3 || Math.abs(n) >= 1e4)) ? n.toExponential(3) : Number(n.toPrecision(4)).toString();
}
function fmtNum(x) {
  const n = Number(x);
  return Number.isFinite(n) ? (Number.isInteger(n) ? String(n) : n.toFixed(3)) : escapeHtml(String(x));
}
function fmtSeq(arr) {
  return (arr || []).map(fmtNum).join(' → ') || '∅';
}
function signArrows(s) {
  return String(s || '').split('').join(' → ') || '∅';
}
