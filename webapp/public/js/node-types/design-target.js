// Biocircuits Explorer — Design Target workflow.
// Design Spec Config authors the explicit DesignabilitySpec. Design Target consumes
// that spec, screens the atlas, and emits the selected reaction network downstream.
//
// The Design Target node is a reaction source: selecting a verified or exploratory
// candidate emits that network downstream like reaction-network / network-id-definition;
// an exact finite-window card also emits its pinned ROP shape reference artifact.
// "Build & tune ->" is a convenience on verified candidates only; Model Builder and
// Placer remain separate nodes in the graph.
//   spec -> design target -> model-builder -> placer
//   [/api/v1/design_screen]
import { api, escapeHtml, showToast, syncSelectOptions } from '../api.js';
import {
  captureEditorGraphPlanningGraph,
  createEditorGraphPatchCommand,
  setNodeLoading,
  getModelForNode, triggerConfigUpdate, triggerAutoModelBuild,
} from '../nodes.js';
import { buildModel } from '../model.js';
import {
  connections,
  getWorkspaceRuntimeEpoch,
  nodeIdCounter,
  nodeRegistry,
} from '../state.js';
import { commitWorkspaceSnapshot } from '../workspace.js';
import { dispatch } from '../commands.js';
import { planDesignBuildAndTuneWorkflow } from '../graph-patch.js';
import {
  blockedOutcome,
  failedOutcome,
  staleOutcome,
  succeededOutcome,
} from '../execution-outcome.js';
import {
  begin as beginLifecycle,
  block as blockLifecycle,
  commit as commitLifecycle,
  createExecutionLifecycle,
  fail as failLifecycle,
  inspectExecutionLifecycle,
  invalidate as invalidateLifecycle,
  isCurrent as isCurrentLifecycle,
  release as releaseLifecycle,
  restoreHistorical as restoreHistoricalLifecycle,
  serializeExecutionLifecycle,
} from '../execution-lifecycle-core.js';
import { loadPlacerMenu, realizePlacerProgram } from './placer.js';
import {
  DESIGNABILITY_SPEC_VERSION,
  buildDesignabilitySpecFromLegacyTarget,
  buildDesignScreenRequest,
  buildDesignScreenRequestFromSpec,
  designCandidateKey,
  renderDesignScreenResults,
} from '../design-screen-render.js';

const DESIGN_CONFIG_OUTPUT_FEATURES = new Set(['threshold', 'fold_change', 'level']);
const DESIGN_CONFIG_SHAPES = new Set(['monotonic', 'bell_shaped']);
const WORKSPACE_ROP_SHAPE_REFERENCE_VERSION = 'bne-workspace-rop-shape-reference/v1.0.0';
const ROP_SHAPE_REQUEST_VERSION = 'bne-rop-shape-optimize-request/v1.0.0';
const NETWORK_IR_VERSION = 'bne-ir/v1.0.0';
const SHA256_HEX = /^[0-9a-f]{64}$/;
const CELL_ID = /^sha256:[0-9a-f]{64}$/;
const PATH_IDENTITY = /^path:[1-9][0-9]*$/;
const DESIGN_SCREEN_ENDPOINT = '/api/v1/design_screen';
const DESIGN_SCREEN_LIFECYCLE_KEY = '_designScreenLifecycle';
const DESIGN_SELECTION_LIFECYCLE_KEY = '_designSelectionLifecycle';
const DESIGN_HISTORICAL_CONTEXT = 'design-target-historical-context/v1';
const EXACT_WINDOW_EVIDENCE_GRADES = new Set([
  'enforced_exact',
  'enforced_exact+sampled_forward',
]);

function deepCloneJson(value) {
  if (typeof globalThis.structuredClone === 'function') return globalThis.structuredClone(value);
  return JSON.parse(JSON.stringify(value));
}

function canonicalLifecycleValue(value) {
  if (Array.isArray(value)) return value.map(canonicalLifecycleValue);
  if (!value || typeof value !== 'object') return value;
  const normalized = {};
  for (const key of Object.keys(value).sort()) {
    normalized[key] = canonicalLifecycleValue(value[key]);
  }
  return normalized;
}

function stableLifecycleFingerprint(value) {
  return JSON.stringify(canonicalLifecycleValue(value));
}

function persistedLifecycleKey(runtimeKey) {
  return runtimeKey === DESIGN_SELECTION_LIFECYCLE_KEY
    ? 'selectionLifecycle'
    : 'lifecycle';
}

function isPersistedHistoricalLifecycle(value) {
  return isPlainObject(value) &&
    value.state === 'historical' &&
    value.freshness === 'historical';
}

function restoredDesignLifecycleContext(info, runtimeKey) {
  const role = runtimeKey === DESIGN_SELECTION_LIFECYCLE_KEY ? 'selection' : 'screen';
  return {
    owner: info,
    workspaceEpoch: getWorkspaceRuntimeEpoch(),
    // The restored result itself is deliberately absent from this bounded
    // owner context. Historical payload bytes never become an input identity.
    inputFingerprint: stableLifecycleFingerprint({
      contract: DESIGN_HISTORICAL_CONTEXT,
      nodeType: info.type,
      role,
    }),
    endpoint: DESIGN_SCREEN_ENDPOINT,
  };
}

function lifecycleForOwner(info, key) {
  if (!info[key]) {
    const lifecycle = createExecutionLifecycle();
    info[key] = lifecycle;
    const dataKey = persistedLifecycleKey(key);
    const saved = info.data?.[dataKey];
    if (isPersistedHistoricalLifecycle(saved)) {
      restoreHistoricalLifecycle(lifecycle, {
        context: restoredDesignLifecycleContext(info, key),
        // Workspace serialization intentionally stores only freshness and
        // evidence. Do not reconstruct a result or a backend session here.
        result: null,
        evidence: Object.hasOwn(saved, 'evidence') ? saved.evidence : null,
      });
      info.data[dataKey] = serializeExecutionLifecycle(lifecycle);
    }
  }
  return info[key];
}

function syncDesignLifecycleSnapshots(info) {
  if (!info) return;
  info.data = info.data || {};
  if (info[DESIGN_SCREEN_LIFECYCLE_KEY]) {
    info.data.lifecycle = serializeExecutionLifecycle(info[DESIGN_SCREEN_LIFECYCLE_KEY]);
  }
  if (info[DESIGN_SELECTION_LIFECYCLE_KEY]) {
    info.data.selectionLifecycle = serializeExecutionLifecycle(
      info[DESIGN_SELECTION_LIFECYCLE_KEY],
    );
  }
}

function invalidateBoundLifecycle(info, key, reason) {
  const lifecycle = info?.[key];
  if (!lifecycle) return false;
  const runtime = inspectExecutionLifecycle(lifecycle);
  if (runtime.owner !== info || runtime.workspaceEpoch == null) return false;
  return invalidateLifecycle(lifecycle, {
    owner: info,
    workspaceEpoch: runtime.workspaceEpoch,
    reason,
  });
}

function designLifecycleContext(nodeId, inputFingerprint) {
  const owner = nodeRegistry[nodeId];
  if (!owner) return null;
  return {
    owner,
    workspaceEpoch: getWorkspaceRuntimeEpoch(),
    inputFingerprint,
    endpoint: DESIGN_SCREEN_ENDPOINT,
  };
}

export function inspectDesignTargetLifecycles(nodeId) {
  const info = nodeRegistry[nodeId];
  const inspectBoundLifecycle = key => {
    if (!info) return null;
    const saved = info.data?.[persistedLifecycleKey(key)];
    if (!info[key] && !isPersistedHistoricalLifecycle(saved)) return null;
    return inspectExecutionLifecycle(lifecycleForOwner(info, key));
  };
  return {
    screen: inspectBoundLifecycle(DESIGN_SCREEN_LIFECYCLE_KEY),
    selection: inspectBoundLifecycle(DESIGN_SELECTION_LIFECYCLE_KEY),
  };
}

function hasExactObjectKeys(value, required, optional = []) {
  if (!isPlainObject(value)) return false;
  const requiredSet = new Set(required);
  const allowed = new Set([...required, ...optional]);
  const keys = Object.keys(value);
  return required.every(key => Object.prototype.hasOwnProperty.call(value, key)) &&
    keys.every(key => allowed.has(key)) &&
    keys.length >= requiredSet.size;
}

function jsonValuesEqual(left, right) {
  if (left === right) return true;
  if (Array.isArray(left) || Array.isArray(right)) {
    return Array.isArray(left) && Array.isArray(right) && left.length === right.length &&
      left.every((value, index) => jsonValuesEqual(value, right[index]));
  }
  if (!isPlainObject(left) || !isPlainObject(right)) return false;
  const leftKeys = Object.keys(left).sort();
  const rightKeys = Object.keys(right).sort();
  return leftKeys.length === rightKeys.length &&
    leftKeys.every((key, index) => key === rightKeys[index] && jsonValuesEqual(left[key], right[key]));
}

function isFiniteNumber(value) {
  return typeof value === 'number' && Number.isFinite(value);
}

function isPositiveNumber(value) {
  return isFiniteNumber(value) && value > 0;
}

function isSha256Hex(value) {
  return typeof value === 'string' && SHA256_HEX.test(value);
}

function isCanonicalFullNetworkIR(network) {
  const keys = [
    'ir_schema_version', 'label', 'species', 'reactions', 'observables',
    'parameter_distributions', 'compartments', 'provenance', 'extensions',
  ];
  return hasExactObjectKeys(network, keys) &&
    network.ir_schema_version === NETWORK_IR_VERSION &&
    typeof network.label === 'string' &&
    Array.isArray(network.species) && network.species.length > 0 &&
    Array.isArray(network.reactions) && network.reactions.length > 0 &&
    Array.isArray(network.observables) && network.observables.length > 0 &&
    Array.isArray(network.parameter_distributions) && network.parameter_distributions.length > 0 &&
    Array.isArray(network.compartments) &&
    isPlainObject(network.provenance) &&
    isPlainObject(network.extensions);
}

function isCompletePinnedReference(reference, networkHash, reactionCount) {
  const required = [
    'reference_hash', 'network_ir_hash', 'operating_points_log10', 'kd', 'totals',
    'path_identity', 'cell_id',
  ];
  if (!hasExactObjectKeys(reference, required, ['artifact_ref'])) return false;
  if (!isSha256Hex(reference.reference_hash) || reference.network_ir_hash !== networkHash) return false;
  if (!Array.isArray(reference.operating_points_log10) || reference.operating_points_log10.length < 2 ||
      !reference.operating_points_log10.every(isFiniteNumber)) return false;
  if (!Array.isArray(reference.kd) || reference.kd.length !== reactionCount ||
      !reference.kd.every(isPositiveNumber)) return false;
  if (!isPlainObject(reference.totals) || Object.keys(reference.totals).length === 0 ||
      !Object.entries(reference.totals).every(([key, value]) => key.length > 0 && isPositiveNumber(value))) {
    return false;
  }
  return typeof reference.path_identity === 'string' && PATH_IDENTITY.test(reference.path_identity) &&
    typeof reference.cell_id === 'string' && CELL_ID.test(reference.cell_id) &&
    (reference.artifact_ref == null ||
      (typeof reference.artifact_ref === 'string' && reference.artifact_ref.length > 0));
}

function isExactFiniteWindowSpec(spec, input, output) {
  const behavior = spec?.target?.behavior_spec;
  const window = behavior?.input_window?.input_log10;
  const robustness = spec?.constraints?.robustness;
  const hasAmbiguousRadius = isPlainObject(robustness) &&
    Object.prototype.hasOwnProperty.call(robustness, 'min_chebyshev_radius');
  return isPlainObject(spec) && spec.schema_version === DESIGNABILITY_SPEC_VERSION &&
    !hasAmbiguousRadius &&
    isPlainObject(behavior) && behavior.feature_space === 'reaction_order' &&
    behavior.input === input && behavior.output === output &&
    Array.isArray(behavior.program) && behavior.program.length >= 2 &&
    behavior.program.every(item => isPlainObject(item) &&
      item.kind === 'reaction_order' && item.operator === '=' && isFiniteNumber(item.value)) &&
    isPlainObject(spec.constraints?.parameter_bounds) &&
    Array.isArray(window) && window.length === 2 && window.every(isFiniteNumber) && window[0] < window[1];
}

function isCompleteOptimizationPolicy(policy) {
  return hasExactObjectKeys(policy, ['minimum_parameter_margin', 'effect_tolerance']) &&
    isFiniteNumber(policy.minimum_parameter_margin) && policy.minimum_parameter_margin >= 0 &&
    isFiniteNumber(policy.effect_tolerance) && policy.effect_tolerance >= 0;
}

function isCompleteWorkBudget(budget) {
  return hasExactObjectKeys(budget, ['max_paths', 'max_cells', 'max_replays', 'require_exhaustive']) &&
    Number.isInteger(budget.max_paths) && budget.max_paths >= 1 && budget.max_paths <= 2000 &&
    Number.isInteger(budget.max_cells) && budget.max_cells >= 1 && budget.max_cells <= 10000 &&
    Number.isInteger(budget.max_replays) && budget.max_replays >= 1 && budget.max_replays <= 16 &&
    typeof budget.require_exhaustive === 'boolean';
}

function isCompleteReplayPolicy(replay) {
  if (!hasExactObjectKeys(replay, [
    'input_window_log10', 'sample_points', 'require_complete', 'store_curve', 'metrics',
  ])) return false;
  const window = replay.input_window_log10;
  const metrics = replay.metrics;
  return Array.isArray(window) && window.length === 2 && window.every(isFiniteNumber) &&
    window[0] >= -20 && window[1] <= 20 && window[0] < window[1] &&
    Number.isInteger(replay.sample_points) && replay.sample_points >= 11 && replay.sample_points <= 1000 &&
    replay.require_complete === true && replay.store_curve === true &&
    Array.isArray(metrics) && metrics.length === 1 &&
    hasExactObjectKeys(metrics[0], ['kind', 'min_prominence_log10']) &&
    metrics[0].kind === 'two_peak' && isFiniteNumber(metrics[0].min_prominence_log10) &&
    metrics[0].min_prominence_log10 >= 0;
}

function candidateEntries(response) {
  if (!isPlainObject(response)) return [];
  const entries = [];
  for (const card of Array.isArray(response.verified_recommendations) ? response.verified_recommendations : []) {
    entries.push({ source: 'verified', card });
  }
  const exploratory = Array.isArray(response.screened_candidates)
    ? response.screened_candidates
    : (Array.isArray(response.near_misses) ? response.near_misses : []);
  for (const card of exploratory) entries.push({ source: 'exploratory', card });
  for (const cell of Array.isArray(response.minimal_certificates) ? response.minimal_certificates : []) {
    for (const card of Array.isArray(cell?.networks) ? cell.networks : []) {
      entries.push({ source: 'minimal', card });
    }
  }
  return entries;
}

export function extractRopShapeReferenceFromCard(card) {
  if (!isPlainObject(card) || card.pass !== true || card.screen_status !== 'verified_exact' ||
      card.certificate_grade !== 'exact-window-siso-rop-path' ||
      !EXACT_WINDOW_EVIDENCE_GRADES.has(card.evidence_grade)) return null;

  const nid = typeof card.nid === 'string' ? card.nid : '';
  const input = typeof card.inp === 'string' ? card.inp : '';
  const output = typeof card.out === 'string' ? card.out : '';
  if (!nid || !input || !output) return null;

  const handoff = card.optimization_handoff_template;
  const fixed = card.fixed_topology_reference;
  if (!hasExactObjectKeys(handoff, ['endpoint', 'method', 'body_template', 'required_fill']) ||
      handoff.endpoint !== '/api/v1/rop_shape_optimize' || handoff.method !== 'POST' ||
      !Array.isArray(handoff.required_fill) || handoff.required_fill.length !== 1 ||
      handoff.required_fill[0] !== 'edit_intent') return null;
  if (!hasExactObjectKeys(fixed, [
    'network', 'network_ir_hash', 'network_canonical_code', 'input', 'output',
    'reference', 'evidence_scope',
  ])) return null;

  const body = handoff.body_template;
  const bodyKeys = [
    'schema_version', 'network', 'expected_network_ir_hash', 'designability_spec',
    'reference', 'edit_intent', 'optimization', 'work_budget', 'replay',
  ];
  if (!hasExactObjectKeys(body, bodyKeys) || body.schema_version !== ROP_SHAPE_REQUEST_VERSION ||
      body.edit_intent !== null || !isCanonicalFullNetworkIR(body.network) ||
      !isSha256Hex(body.expected_network_ir_hash)) return null;
  if (!isExactFiniteWindowSpec(body.designability_spec, input, output) ||
      !isCompleteOptimizationPolicy(body.optimization) ||
      !isCompleteWorkBudget(body.work_budget) || !isCompleteReplayPolicy(body.replay)) return null;
  if (!isCompletePinnedReference(body.reference, body.expected_network_ir_hash, body.network.reactions.length)) {
    return null;
  }
  const specWindow = body.designability_spec.target.behavior_spec.input_window.input_log10;
  const programLength = body.designability_spec.target.behavior_spec.program.length;
  if (body.reference.operating_points_log10.length !== programLength ||
      !body.reference.operating_points_log10.every(value => value >= specWindow[0] && value <= specWindow[1]) ||
      !jsonValuesEqual(body.replay.input_window_log10, specWindow)) return null;

  if (fixed.network_ir_hash !== body.expected_network_ir_hash ||
      fixed.network_canonical_code !== nid || fixed.input !== input || fixed.output !== output ||
      typeof fixed.evidence_scope !== 'string' || fixed.evidence_scope.length === 0 ||
      !jsonValuesEqual(fixed.network, body.network) ||
      !jsonValuesEqual(fixed.reference, body.reference)) return null;

  const artifact = {
    schema_version: WORKSPACE_ROP_SHAPE_REFERENCE_VERSION,
    selected_candidate_key: designCandidateKey(nid, input, output),
    fixed_topology_reference: fixed,
    optimization_handoff_template: handoff,
    evidence_scope: fixed.evidence_scope,
  };
  return deepCloneJson(artifact);
}

export function refreshRopShapeReferenceForSelection(config, response) {
  if (!isPlainObject(config)) return null;
  const selectedKey = typeof config.selectedCandidateKey === 'string'
    ? config.selectedCandidateKey
    : '';
  const matches = selectedKey
    ? candidateEntries(response).filter(({ card }) =>
        isPlainObject(card) && designCandidateKey(card.nid, card.inp, card.out) === selectedKey)
    : [];
  const artifact = matches.length === 1 && matches[0].source === 'verified'
    ? extractRopShapeReferenceFromCard(matches[0].card)
    : null;
  if (artifact && artifact.selected_candidate_key === selectedKey) {
    config.ropShapeReference = artifact;
    return artifact;
  }
  delete config.ropShapeReference;
  return null;
}

export function invalidateConnectedRopShapeArtifacts(designNodeId) {
  const configNodeIds = new Set(connections
    .filter(connection =>
      connection.fromNode === designNodeId &&
      connection.fromPort === 'rop-shape-reference' &&
      connection.toPort === 'rop-shape-reference')
    .map(connection => connection.toNode));
  const resultNodeIds = new Set();

  for (const configNodeId of configNodeIds) {
    const configInfo = nodeRegistry[configNodeId];
    if (!configInfo || configInfo.type !== 'rop-shape-edit-config') continue;
    configInfo.data = configInfo.data || {};
    invalidateBoundLifecycle(
      configInfo,
      '_ropShapePrepareLifecycle',
      'design-target-selection-changed',
    );
    if (configInfo._ropShapePrepareLifecycle) {
      configInfo.data.lifecycle = serializeExecutionLifecycle(
        configInfo._ropShapePrepareLifecycle,
      );
    }
    delete configInfo.data.ropShapeRequest;
    if (isPlainObject(configInfo.data.config)) {
      delete configInfo.data.config.ropShapeRequest;
    }
    const status = document.getElementById(`${configNodeId}-rop-shape-status`);
    if (status) {
      status.innerHTML = '<div class="node-info"><strong>Needs Prepare.</strong> ' +
        'The upstream Design Target reference changed; validate and prepare again.</div>';
    }

    connections
      .filter(connection =>
        connection.fromNode === configNodeId &&
        connection.fromPort === 'rop-shape-request' &&
        connection.toPort === 'rop-shape-request')
      .forEach(connection => resultNodeIds.add(connection.toNode));
  }

  for (const resultNodeId of resultNodeIds) {
    const resultInfo = nodeRegistry[resultNodeId];
    if (!resultInfo || resultInfo.type !== 'rop-shape-result') continue;
    resultInfo.data = resultInfo.data || {};
    invalidateBoundLifecycle(
      resultInfo,
      '_ropShapeResultLifecycle',
      'design-target-selection-changed',
    );
    if (resultInfo._ropShapeResultLifecycle) {
      resultInfo.data.lifecycle = serializeExecutionLifecycle(
        resultInfo._ropShapeResultLifecycle,
      );
    }
    const currentToken = Number.isSafeInteger(resultInfo.data.ropShapeRunToken)
      ? resultInfo.data.ropShapeRunToken
      : 0;
    resultInfo.data.ropShapeRunToken = currentToken + 1;
    delete resultInfo.data.ropShapeRequest;
    delete resultInfo.data.ropShapeResult;
    setNodeLoading(resultNodeId, false);
    const content = document.getElementById(`${resultNodeId}-content`);
    if (content) {
      content.innerHTML = '<div class="node-info"><strong>Upstream changed.</strong> ' +
        'Run ROP shape optimization again for the current Design Target reference.</div>';
    }
  }

  return {
    configNodeCount: configNodeIds.size,
    resultNodeCount: resultNodeIds.size,
  };
}

export function didRopShapeReferenceChange(
  previousSelectedKey,
  previousReference,
  currentSelectedKey,
  currentReference,
) {
  const previousKey = typeof previousSelectedKey === 'string' ? previousSelectedKey : null;
  const currentKey = typeof currentSelectedKey === 'string' ? currentSelectedKey : null;
  return previousKey !== currentKey ||
    !jsonValuesEqual(previousReference ?? null, currentReference ?? null);
}

export const DESIGN_TARGET_TYPES = {
  'design-spec-config': {
    category: 'parameter',
    headerClass: 'header-parameter',
    title: 'Design Spec Config',
    inputs: [],
    outputs: [{ port: 'designability-spec', type: 'DesignabilitySpec', label: 'DesignabilitySpec' }],
    defaultWidth: 440,
    createBody(nodeId) {
      return `
        <div class="param-row">
          <label>Target kind:</label>
          <select id="${nodeId}-spec-kind" data-action="designSpecKindChange" data-node="${nodeId}">
            <option value="sign">qualitative signs</option>
            <option value="exact">reaction-order values</option>
            <option value="label">behavior label</option>
            <option value="behavior_spec">structured behavior spec</option>
          </select>
        </div>
        <div class="param-row">
          <label>Target:</label>
          <div id="${nodeId}-spec-target-wrap" style="flex:1;">
            <input type="text" id="${nodeId}-spec-target" placeholder="+-+   or   1, 0, -1" style="flex:1;">
          </div>
        </div>
        <div class="param-row">
          <label>Behavior I/O:</label>
          <input type="text" id="${nodeId}-spec-input" placeholder="tA" style="width:96px;">
          <input type="text" id="${nodeId}-spec-output" placeholder="C_A_A" style="flex:1;">
        </div>
        <div class="param-row">
          <label>Kd log10:</label>
          <input type="number" id="${nodeId}-spec-kd-lo" value="-3" step="0.5" style="width:72px;">
          <input type="number" id="${nodeId}-spec-kd-hi" value="3" step="0.5" style="width:72px;">
          <label>Total:</label>
          <input type="number" id="${nodeId}-spec-total-lo" value="-3" step="0.5" style="width:72px;">
          <input type="number" id="${nodeId}-spec-total-hi" value="3" step="0.5" style="width:72px;">
        </div>
        <div class="param-row">
          <label>Min radius:</label>
          <input type="number" id="${nodeId}-spec-radius" value="0" min="0" step="0.05" style="width:82px;">
          <label>Volume LB:</label>
          <input type="number" id="${nodeId}-spec-volume" placeholder="lower bound" min="0" step="0.01" style="width:94px;">
          <label>Exact solves:</label>
          <input type="number" id="${nodeId}-spec-max-exact" value="3" min="0" step="1" style="width:72px;">
        </div>
        <div class="param-row">
          <label>Input window:</label>
          <input type="number" id="${nodeId}-spec-window-lo" placeholder="lo" step="0.5" style="width:72px;">
          <input type="number" id="${nodeId}-spec-window-hi" placeholder="hi" step="0.5" style="width:72px;">
          <input type="text" id="${nodeId}-spec-operating-points" placeholder="ops -2,2" style="width:96px;">
          <label>Output:</label>
          <select id="${nodeId}-spec-output-feature" style="width:124px;">
            <option value="">species</option>
            <option value="threshold">threshold</option>
            <option value="fold_change">fold-change</option>
            <option value="level">level</option>
          </select>
          <input type="number" id="${nodeId}-spec-output-value" placeholder="value" step="0.1" style="width:82px;">
          <input type="number" id="${nodeId}-spec-output-samples" placeholder="samples" min="11" max="1001" step="2" style="width:76px;">
          <input type="number" id="${nodeId}-spec-output-tolerance" placeholder="tol" min="0" step="0.000001" style="width:68px;">
        </div>
        <div class="param-row">
          <label>Shape:</label>
          <select id="${nodeId}-spec-shape" style="width:132px;">
            <option value="">none</option>
            <option value="monotonic">monotonic</option>
            <option value="bell_shaped">bell-shaped</option>
          </select>
          <select id="${nodeId}-spec-shape-monotonicity" style="width:112px;">
            <option value="any">any</option>
            <option value="increasing">increasing</option>
            <option value="decreasing">decreasing</option>
          </select>
          <input type="number" id="${nodeId}-spec-shape-prominence" placeholder="prom" min="0" step="0.1" style="width:68px;">
          <input type="number" id="${nodeId}-spec-shape-samples" placeholder="samples" min="11" max="1001" step="2" style="width:76px;">
          <input type="number" id="${nodeId}-spec-shape-tolerance" placeholder="tol" min="0" step="0.000001" style="width:68px;">
        </div>
        <div class="param-row">
          <label>Dyn / spacing:</label>
          <input type="number" id="${nodeId}-spec-dynamic-range" placeholder="fold" min="0" step="0.1" style="width:76px;">
          <input type="number" id="${nodeId}-spec-dynamic-samples" placeholder="samples" min="11" max="1001" step="2" style="width:76px;">
          <input type="number" id="${nodeId}-spec-transition-spacing" placeholder="dec" min="0" step="0.1" style="width:76px;">
          <input type="text" id="${nodeId}-spec-transition-order" placeholder="order 0,1" style="width:92px;">
        </div>
        <div class="param-row">
          <label>Rank:</label>
          <select id="${nodeId}-spec-rank-primary" style="width:150px;">
            <option value="">solver default</option>
            <option value="chebyshev_radius">Chebyshev radius</option>
            <option value="tunable_volume">volume lower bound</option>
            <option value="dynamic_range">dynamic range</option>
            <option value="transition_spacing">transition spacing</option>
            <option value="complexity">complexity</option>
            <option value="evidence_grade">evidence grade</option>
            <option value="certificate_grade">certificate grade</option>
          </select>
          <select id="${nodeId}-spec-rank-secondary" style="width:150px;">
            <option value="">then default</option>
            <option value="chebyshev_radius">Chebyshev radius</option>
            <option value="tunable_volume">volume lower bound</option>
            <option value="dynamic_range">dynamic range</option>
            <option value="transition_spacing">transition spacing</option>
            <option value="complexity">complexity</option>
            <option value="evidence_grade">evidence grade</option>
            <option value="certificate_grade">certificate grade</option>
          </select>
        </div>
        <div class="param-row">
          <label>Extra d/r/mu:</label>
          <input type="number" id="${nodeId}-spec-extra-species" value="1" min="0" step="1" style="width:54px;">
          <input type="number" id="${nodeId}-spec-extra-reactions" value="1" min="0" step="1" style="width:54px;">
          <input type="number" id="${nodeId}-spec-extra-mu" value="1" min="0" step="1" style="width:54px;">
          <label>Max d/r/mu:</label>
          <input type="number" id="${nodeId}-spec-max-species" placeholder="d" min="1" step="1" style="width:54px;">
          <input type="number" id="${nodeId}-spec-max-reactions" placeholder="r" min="0" step="1" style="width:54px;">
          <input type="number" id="${nodeId}-spec-max-mu" placeholder="μ" min="1" step="1" style="width:54px;">
        </div>
        <div class="param-row">
          <label><input type="checkbox" id="${nodeId}-spec-allow-near-minimal" checked> allow near-minimal candidates</label>
          <label><input type="checkbox" id="${nodeId}-spec-block-hard" checked> block hard unsupported</label>
        </div>
        <div class="param-row" style="align-items:flex-start;">
          <label>Spec JSON:</label>
          <textarea id="${nodeId}-spec-json" rows="5" spellcheck="false" style="flex:1;font-family:var(--font-mono, monospace);font-size:12px;"></textarea>
        </div>
        <button class="btn btn-run" data-action="validateDesignSpecConfig" data-node="${nodeId}">Validate spec</button>
        <div class="node-info" id="${nodeId}-spec-preview" style="display:none;"></div>
      `;
    },
    onInit(nodeId) {
      installDesignSpecInputInvalidation(nodeId);
    },
    async prepare(nodeId) {
      return validateDesignSpecConfig(nodeId);
    },
  },
  'design-target': {
    category: 'viewer',
    headerClass: 'header-viewer',
    title: 'Design Target',
    inputs: [{ port: 'designability-spec', type: 'DesignabilitySpec', label: 'DesignabilitySpec' }],
    outputs: [
      { port: 'reactions', type: 'NetworkIR', label: 'Reactions' },
      { port: 'rop-shape-reference', type: 'ROPShapeReferenceArtifact', label: 'ROP Shape Reference' },
    ],
    defaultWidth: 460,
    createBody(nodeId) {
      return `
        <div class="param-row">
          <label>Target kind:</label>
          <select id="${nodeId}-kind" data-action="designTargetKindChange" data-node="${nodeId}">
            <option value="sign">qualitative (signs, e.g. + - +)</option>
            <option value="exact">precise RO (e.g. 1.5, 0, -1)</option>
            <option value="label">behavior label</option>
          </select>
        </div>
        <div class="param-row">
          <label>Target:</label>
          <div id="${nodeId}-target-wrap" style="flex:1;">
            <input type="text" id="${nodeId}-target" placeholder="+-+   or   1, 0, -1" style="flex:1;">
          </div>
        </div>
        <button class="btn btn-run" data-action="runDesignSearch" data-node="${nodeId}">Screen candidates</button>
        <div class="viewer-content" id="${nodeId}-content">
          <span class="text-dim">Connect a Design Spec Config for explicit constraints, or use the local target fields as a legacy shorthand. Verified recommendations require an active DesignabilitySpec with explicit parameter bounds; proxy screens stay exploratory.</span>
        </div>
      `;
    },
    onInit(nodeId) {
      installDesignTargetInputInvalidation(nodeId);
    },
    async execute(nodeId) {
      return runDesignSearch(nodeId);
    },
  },
};

function parseFiniteField(nodeId, suffix, fallback = null) {
  const raw = document.getElementById(`${nodeId}${suffix}`)?.value;
  const text = raw == null ? '' : String(raw).trim();
  if (!text) {
    if (fallback != null) return Number(fallback);
    throw new Error(`Invalid number in ${suffix.replace(/^-/, '')}`);
  }
  const value = Number(text);
  if (Number.isFinite(value)) return value;
  throw new Error(`Invalid number in ${suffix.replace(/^-/, '')}`);
}

function parseOptionalFiniteField(nodeId, suffix) {
  const raw = document.getElementById(`${nodeId}${suffix}`)?.value;
  if (raw == null || String(raw).trim() === '') return null;
  const value = Number(raw);
  if (Number.isFinite(value)) return value;
  throw new Error(`Invalid number in ${suffix.replace(/^-/, '')}`);
}

function parseIntField(nodeId, suffix, fallback = 0) {
  const raw = document.getElementById(`${nodeId}${suffix}`)?.value;
  const text = raw == null || String(raw).trim() === '' ? String(fallback) : String(raw).trim();
  const value = Number(text);
  if (Number.isInteger(value) && value >= 0) return value;
  throw new Error(`Invalid integer in ${suffix.replace(/^-/, '')}`);
}

function parseOptionalIntField(nodeId, suffix) {
  const raw = document.getElementById(`${nodeId}${suffix}`)?.value;
  if (raw == null || String(raw).trim() === '') return null;
  const value = Number(String(raw).trim());
  if (Number.isInteger(value)) return value;
  throw new Error(`Invalid integer in ${suffix.replace(/^-/, '')}`);
}

function parseOptionalSamplePointsField(nodeId, suffix, label) {
  const value = parseOptionalIntField(nodeId, suffix);
  if (value == null) return null;
  if (value < 11 || value > 1001) throw new Error(`${label} samples must be between 11 and 1001`);
  return value;
}

function parseOptionalNonnegativeField(nodeId, suffix, label) {
  const value = parseOptionalFiniteField(nodeId, suffix);
  if (value == null) return null;
  if (value < 0) throw new Error(`${label} must be non-negative`);
  return value;
}

function parseOptionalFiniteListField(nodeId, suffix, label) {
  const raw = document.getElementById(`${nodeId}${suffix}`)?.value;
  if (raw == null || String(raw).trim() === '') return null;
  if (/(^|,)\s*(,|$)/.test(String(raw))) throw new Error(`${label} must be a comma-separated numeric list`);
  const values = String(raw).trim().split(/[,\s]+/).map(Number);
  if (!values.length || values.some(value => !Number.isFinite(value))) {
    throw new Error(`${label} must be a comma-separated numeric list`);
  }
  return values;
}

function parseOptionalNonnegativeIntListField(nodeId, suffix, label) {
  const values = parseOptionalFiniteListField(nodeId, suffix, label);
  if (values == null) return null;
  if (values.some(value => !Number.isInteger(value) || value < 0)) {
    throw new Error(`${label} must be 0-based integer indices`);
  }
  return values;
}

function readOptionalSpecTargetClauses(nodeId, hard = true) {
  const target = {};
  const winLo = parseOptionalFiniteField(nodeId, '-spec-window-lo');
  const winHi = parseOptionalFiniteField(nodeId, '-spec-window-hi');
  const operatingPoints = parseOptionalFiniteListField(nodeId, '-spec-operating-points', 'Operating points');
  if (winLo != null || winHi != null) {
    if (winLo == null || winHi == null) throw new Error('Input window needs both lower and upper bounds');
    if (winLo > winHi) throw new Error('Input window lower bound exceeds upper bound');
    target.input_window = { input_log10: [winLo, winHi], hard };
  }
  if (operatingPoints != null) {
    if (!target.input_window) throw new Error('Operating points require input window bounds');
    target.input_window.operating_points_log10 = operatingPoints;
  }

  const feature = document.getElementById(`${nodeId}-spec-output-feature`)?.value || '';
  if (feature) {
    const value = parseOptionalFiniteField(nodeId, '-spec-output-value');
    const samplePoints = parseOptionalSamplePointsField(nodeId, '-spec-output-samples', 'Output feature');
    const tolerance = parseOptionalNonnegativeField(nodeId, '-spec-output-tolerance', 'Output feature tolerance');
    if (value == null) throw new Error('Output feature value is required');
    if (samplePoints == null) throw new Error('Output feature samples are required');
    if (tolerance == null) throw new Error('Output feature tolerance is required');
    target.output_feature = {
      feature,
      operator: feature === 'threshold' ? '>=' : '=',
      value,
      sample_points: samplePoints,
      tolerance_log10: tolerance,
      hard,
    };
  }

  const shape = document.getElementById(`${nodeId}-spec-shape`)?.value || '';
  if (shape) {
    const monotonicity = document.getElementById(`${nodeId}-spec-shape-monotonicity`)?.value || 'any';
    const prominence = parseOptionalNonnegativeField(nodeId, '-spec-shape-prominence', 'Shape prominence');
    const samplePoints = parseOptionalSamplePointsField(nodeId, '-spec-shape-samples', 'Shape');
    const tolerance = parseOptionalNonnegativeField(nodeId, '-spec-shape-tolerance', 'Shape tolerance');
    if (samplePoints == null) throw new Error('Shape samples are required');
    if (tolerance == null) throw new Error('Shape tolerance is required');
    if (shape === 'bell_shaped' && prominence == null) {
      throw new Error('Bell-shaped shape needs min prominence');
    }
    target.shape = {
      class: shape,
      ...(shape === 'monotonic' ? { monotonicity } : {}),
      ...(prominence == null ? {} : { min_prominence_log10: prominence }),
      sample_points: samplePoints,
      tolerance_log10: tolerance,
      hard,
    };
  }
  return target;
}

function readOptionalSpecConstraintClauses(nodeId, hard = true) {
  const constraints = {};
  const minFold = parseOptionalFiniteField(nodeId, '-spec-dynamic-range');
  if (minFold != null) {
    if (minFold < 0) throw new Error('Dynamic range must be non-negative');
    const samplePoints = parseOptionalSamplePointsField(nodeId, '-spec-dynamic-samples', 'Dynamic range');
    if (samplePoints == null) throw new Error('Dynamic range samples are required');
    constraints.dynamic_range = {
      min_fold_change: minFold,
      sample_points: samplePoints,
      hard,
    };
  }
  const spacing = parseOptionalFiniteField(nodeId, '-spec-transition-spacing');
  const transitionOrder = parseOptionalNonnegativeIntListField(nodeId, '-spec-transition-order', 'Transition order');
  if (spacing != null) {
    if (spacing < 0) throw new Error('Transition spacing must be non-negative');
  }
  if (spacing != null || transitionOrder != null) {
    constraints.transitions = {
      ...(spacing == null ? {} : { min_spacing_decades: spacing }),
      ...(transitionOrder == null ? {} : { order: transitionOrder }),
      hard,
    };
  }
  const maxSpecies = parseOptionalIntField(nodeId, '-spec-max-species');
  const maxReactions = parseOptionalIntField(nodeId, '-spec-max-reactions');
  const maxMu = parseOptionalIntField(nodeId, '-spec-max-mu');
  const allowNearMinimal = document.getElementById(`${nodeId}-spec-allow-near-minimal`)?.checked !== false;
  if (maxSpecies != null || maxReactions != null || maxMu != null || !allowNearMinimal) {
    if (maxSpecies != null && maxSpecies < 1) throw new Error('Network max species must be at least 1');
    if (maxReactions != null && maxReactions < 0) throw new Error('Network max reactions must be non-negative');
    if (maxMu != null && maxMu < 1) throw new Error('Network max μ must be at least 1');
    constraints.network = {
      ...(maxSpecies == null ? {} : { max_species: maxSpecies }),
      ...(maxReactions == null ? {} : { max_reactions: maxReactions }),
      ...(maxMu == null ? {} : { max_mu: maxMu }),
      allow_near_minimal: allowNearMinimal,
    };
  }
  return constraints;
}

function readSpecRankingPolicy(nodeId) {
  const prefer = [];
  for (const suffix of ['-spec-rank-primary', '-spec-rank-secondary']) {
    const value = document.getElementById(`${nodeId}${suffix}`)?.value || '';
    if (value && !prefer.includes(value)) prefer.push(value);
  }
  return {
    verified_only: true,
    ...(prefer.length ? { prefer } : {}),
  };
}

function assertManualConfigSupportedClauses(spec) {
  const feature = spec?.target?.output_feature?.feature;
  if (feature) {
    const featureName = String(feature);
    if (!DESIGN_CONFIG_OUTPUT_FEATURES.has(featureName)) {
      if (featureName === 'dynamic_range') {
        throw new Error('Unsupported output_feature dynamic_range: dynamic range belongs in constraints.dynamic_range.');
      }
      throw new Error(`Unsupported output_feature ${featureName}: use a solver-backed Design Spec Config output feature.`);
    }
    const outputFeature = spec.target.output_feature;
    if (!hasExplicitFiniteNumber(outputFeature, 'value')) throw new Error('Output feature value is required');
    if (!hasExplicitSamplePoints(outputFeature, 'sample_points')) throw new Error('Output feature samples are required');
    if (!hasExplicitNonnegativeNumber(outputFeature, 'tolerance_log10')) throw new Error('Output feature tolerance is required');
  }
  const shape = spec?.target?.shape?.class;
  if (shape) {
    const shapeName = String(shape);
    if (!DESIGN_CONFIG_SHAPES.has(shapeName)) {
      if (shapeName === 'threshold' || shapeName === 'fold_change' || shapeName === 'level') {
        throw new Error(`Unsupported target.shape ${shapeName}: ${shapeName} belongs in target.output_feature.`);
      }
      throw new Error(`Unsupported target.shape ${shapeName}: use a solver-backed Design Spec Config shape.`);
    }
    const shapeClause = spec.target.shape;
    if (!hasExplicitSamplePoints(shapeClause, 'sample_points')) throw new Error('Shape samples are required');
    if (!hasExplicitNonnegativeNumber(shapeClause, 'tolerance_log10')) throw new Error('Shape tolerance is required');
    if (shapeName === 'bell_shaped' &&
        !hasExplicitNonnegativeNumber(shapeClause, 'min_prominence_log10') &&
        !hasExplicitNonnegativeNumber(shapeClause, 'min_prominence_decades')) {
      throw new Error('Bell-shaped shape needs min prominence');
    }
  }
  const dynamicRange = spec?.constraints?.dynamic_range;
  if (dynamicRange != null) {
    if (!isPlainObject(dynamicRange)) throw new Error('Dynamic range must be an object');
    if (hasExplicitFiniteNumber(dynamicRange, 'min_fold_change') &&
        !hasExplicitSamplePoints(dynamicRange, 'sample_points')) {
      throw new Error('Dynamic range samples are required');
    }
  }
}

function hasOwnKey(obj, key) {
  return Object.prototype.hasOwnProperty.call(obj || {}, key);
}

function hasExplicitFiniteNumber(obj, key) {
  return hasOwnKey(obj, key) && typeof obj[key] === 'number' && Number.isFinite(obj[key]);
}

function hasExplicitNonnegativeNumber(obj, key) {
  return hasExplicitFiniteNumber(obj, key) && obj[key] >= 0;
}

function hasExplicitSamplePoints(obj, key) {
  return hasOwnKey(obj, key) &&
    typeof obj[key] === 'number' &&
    Number.isInteger(obj[key]) &&
    obj[key] >= 11 &&
    obj[key] <= 1001;
}

function assertPlainSpecObject(value, path) {
  if (!isPlainObject(value)) throw new Error(`${path} must be an object`);
}

function assertSpecNoUnknownKeys(obj, allowed, path, label = path) {
  assertPlainSpecObject(obj, path);
  const allowedSet = new Set(allowed);
  for (const key of Object.keys(obj)) {
    if (!allowedSet.has(key)) throw new Error(`Unknown ${label} key: ${path}.${key}`);
  }
}

function assertSpecBooleanField(obj, key, path) {
  if (hasOwnKey(obj, key) && typeof obj[key] !== 'boolean') {
    throw new Error(`${path}.${key} must be boolean`);
  }
}

function assertSpecStringField(obj, key, path) {
  if (hasOwnKey(obj, key) && typeof obj[key] !== 'string') {
    throw new Error(`${path}.${key} must be a string`);
  }
}

function assertSpecFiniteField(obj, key, path) {
  if (!hasExplicitFiniteNumber(obj, key)) {
    throw new Error(`${path}.${key} must be a finite number`);
  }
}

function assertSpecOptionalNonnegativeField(obj, key, path) {
  if (hasOwnKey(obj, key) && !hasExplicitNonnegativeNumber(obj, key)) {
    throw new Error(`${path}.${key} must be a finite non-negative number`);
  }
}

function assertSpecOptionalFractionField(obj, key, path) {
  if (hasOwnKey(obj, key) &&
      (!hasExplicitNonnegativeNumber(obj, key) || obj[key] > 1)) {
    throw new Error(`${path}.${key} must be a finite number from 0 to 1`);
  }
}

function assertSpecOptionalNonnegativeIntField(obj, key, path) {
  if (hasOwnKey(obj, key) &&
      !(typeof obj[key] === 'number' && Number.isInteger(obj[key]) && obj[key] >= 0)) {
    throw new Error(`${path}.${key} must be a non-negative integer`);
  }
}

function assertSpecOptionalPositiveIntField(obj, key, path) {
  if (hasOwnKey(obj, key) &&
      !(typeof obj[key] === 'number' && Number.isInteger(obj[key]) && obj[key] >= 1)) {
    throw new Error(`${path}.${key} must be a positive integer`);
  }
}

function assertSpecBounds(value, path) {
  if (!Array.isArray(value) || value.length !== 2 || value.some(v => typeof v !== 'number' || !Number.isFinite(v))) {
    throw new Error(`${path} must be a two-number finite bounds array`);
  }
  if (value[0] > value[1]) throw new Error(`${path} lower bound exceeds upper bound`);
}

function assertSpecWindow(win, path) {
  assertSpecNoUnknownKeys(win, ['input_log10', 'hard', 'min_spacing_decades', 'operating_points_log10'], path, 'input_window');
  if (hasOwnKey(win, 'input_log10')) assertSpecBounds(win.input_log10, `${path}.input_log10`);
  assertSpecBooleanField(win, 'hard', path);
  assertSpecOptionalNonnegativeField(win, 'min_spacing_decades', path);
  if (!hasOwnKey(win, 'input_log10') && hasOwnKey(win, 'min_spacing_decades')) {
    throw new Error(`${path}.min_spacing_decades requires ${path}.input_log10`);
  }
  if (hasOwnKey(win, 'operating_points_log10')) {
    const points = win.operating_points_log10;
    if (!Array.isArray(points) || points.some(v => typeof v !== 'number' || !Number.isFinite(v))) {
      throw new Error(`${path}.operating_points_log10 must be a finite numeric array`);
    }
    if (!hasOwnKey(win, 'input_log10')) {
      throw new Error(`${path}.operating_points_log10 requires ${path}.input_log10`);
    }
  }
}

function assertSpecBehaviorStep(step, path) {
  assertSpecNoUnknownKeys(step, ['kind', 'value', 'operator', 'hard'], path, 'behavior_step');
  if (step.kind !== 'reaction_order') throw new Error(`${path}.kind must be reaction_order`);
  if (!hasExplicitFiniteNumber(step, 'value')) throw new Error(`${path}.value must be a finite reaction-order number`);
  if (hasOwnKey(step, 'operator') && step.operator !== '=' && step.operator !== '==') {
    throw new Error(`${path}.operator must be = or ==`);
  }
  assertSpecBooleanField(step, 'hard', path);
}

function assertSpecBehaviorSpec(behavior, path) {
  assertSpecNoUnknownKeys(behavior, ['input', 'output', 'program', 'feature_space', 'input_window'], path, 'behavior_spec');
  if (typeof behavior.input !== 'string' || !behavior.input.trim()) throw new Error(`${path}.input must be a non-empty string`);
  if (typeof behavior.output !== 'string' || !behavior.output.trim()) throw new Error(`${path}.output must be a non-empty string`);
  if (!Array.isArray(behavior.program) || behavior.program.length === 0) {
    throw new Error(`${path}.program must contain at least one reaction-order step`);
  }
  behavior.program.forEach((step, index) => assertSpecBehaviorStep(step, `${path}.program/${index}`));
  if (hasOwnKey(behavior, 'feature_space') && behavior.feature_space !== 'reaction_order') {
    throw new Error(`${path}.feature_space must be reaction_order`);
  }
  if (hasOwnKey(behavior, 'input_window')) assertSpecWindow(behavior.input_window, `${path}.input_window`);
}

function assertSpecLegacyTarget(legacy, path) {
  assertPlainSpecObject(legacy, path);
  assertSpecNoUnknownKeys(legacy, ['target_kind', 'target'], path, 'legacy_target');
  if (!['sign', 'exact', 'label'].includes(legacy.target_kind)) {
    throw new Error(`${path}.target_kind must be sign, exact, or label`);
  }
  if (!hasOwnKey(legacy, 'target')) throw new Error(`${path}.target is required`);
  if (legacy.target_kind === 'exact') {
    if (typeof legacy.target === 'string' || !Array.isArray(legacy.target)) {
      throw new Error(`${path}.target for exact legacy targets must be an array`);
    }
    if (legacy.target.length === 0) {
      throw new Error(`${path}.target for exact legacy targets must contain at least one finite number`);
    }
    if (legacy.target.some(value => typeof value !== 'number' || !Number.isFinite(value))) {
      throw new Error(`${path}.target values must be finite non-Bool real numbers`);
    }
  }
}

function assertSpecOutputFeature(feature, path) {
  assertSpecNoUnknownKeys(feature, ['feature', 'operator', 'value', 'sample_points', 'tolerance_log10', 'hard'], path, 'output_feature');
  if (!['threshold', 'level', 'fold_change'].includes(feature.feature)) {
    throw new Error(`${path}.feature must be threshold, level, or fold_change`);
  }
  if (hasOwnKey(feature, 'operator') && !['=', '>=', '<='].includes(feature.operator)) {
    throw new Error(`${path}.operator must be >=, <=, or =`);
  }
  if (!hasExplicitSamplePoints(feature, 'sample_points')) throw new Error(`${path}.sample_points must be an integer from 11 to 1001`);
  if (!hasExplicitNonnegativeNumber(feature, 'tolerance_log10')) throw new Error(`${path}.tolerance_log10 must be a finite non-negative number`);
  assertSpecBooleanField(feature, 'hard', path);
}

function specBehaviorProgramLength(target) {
  const program = target?.behavior_spec?.program;
  return Array.isArray(program) ? program.length : null;
}

function specHasBehaviorInputWindow(target) {
  const bounds = target?.behavior_spec?.input_window?.input_log10;
  return Array.isArray(bounds) &&
    bounds.length === 2 &&
    bounds.every(value => typeof value === 'number' && Number.isFinite(value)) &&
    bounds[0] <= bounds[1];
}

function assertSpecOperatingPointsRealizable(inputWindow, transitions, path) {
  const points = inputWindow?.operating_points_log10;
  const bounds = inputWindow?.input_log10;
  if (!Array.isArray(points) || !Array.isArray(bounds) || bounds.length !== 2) return;
  const [lo, hi] = bounds;
  for (const point of points) {
    if (point < lo || point > hi) {
      throw new Error(`${path} entries must stay within input_window.input_log10`);
    }
  }
  const windowSpacing = hasOwnKey(inputWindow, 'min_spacing_decades') ? inputWindow.min_spacing_decades : 0;
  const transitionSpacing = hasOwnKey(transitions, 'min_spacing_decades') ? transitions.min_spacing_decades : 0;
  const spacing = Math.max(windowSpacing, transitionSpacing);
  for (let index = 0; index < points.length - 1; index += 1) {
    if (points[index + 1] - points[index] + 1e-9 < spacing) {
      throw new Error(`${path} must be ordered by effective min_spacing_decades`);
    }
  }
}

function specHasParameterBounds(spec) {
  return isPlainObject(spec.constraints) && hasOwnKey(spec.constraints, 'parameter_bounds');
}

function positiveSpecBudget(candidateBudget, key) {
  return isPlainObject(candidateBudget) &&
    hasOwnKey(candidateBudget, key) &&
    typeof candidateBudget[key] === 'number' &&
    Number.isInteger(candidateBudget[key]) &&
    candidateBudget[key] > 0;
}

function assertParameterBoundsPrerequisites(spec) {
  if (specHasParameterBounds(spec)) return;

  const candidateBudget = spec.candidate_budget;
  if (positiveSpecBudget(candidateBudget, 'max_exact_placements')) {
    throw new Error('candidate_budget.max_exact_placements requires constraints.parameter_bounds for solver-backed verified recommendations');
  }
  if (positiveSpecBudget(candidateBudget, 'max_verified_recommendations')) {
    throw new Error('candidate_budget.max_verified_recommendations requires constraints.parameter_bounds for solver-backed verified recommendations');
  }

  const rankingPolicy = spec.ranking_policy || {};
  if (rankingPolicy.verified_only === true) {
    throw new Error('ranking_policy.verified_only requires constraints.parameter_bounds for verified recommendations');
  }
  const prefer = rankingPolicy.prefer;
  if (Array.isArray(prefer)) {
    prefer.forEach((preference, index) => {
      if (['chebyshev_radius', 'tunable_volume', 'dynamic_range', 'transition_spacing'].includes(preference)) {
        throw new Error(`ranking_policy.prefer/${index} ${preference} requires constraints.parameter_bounds for a solver-backed verified-card metric`);
      }
    });
  }

  const target = spec.target || {};
  if (target.output_feature) {
    throw new Error('target.output_feature requires constraints.parameter_bounds for sampled finite-dose verification');
  }
  if (target.shape) {
    throw new Error('target.shape requires constraints.parameter_bounds for sampled finite-dose verification');
  }

  const constraints = spec.constraints || {};
  if (constraints.dynamic_range) {
    throw new Error('constraints.dynamic_range requires constraints.parameter_bounds for sampled finite-dose verification');
  }
  if (constraints.transitions) {
    throw new Error('constraints.transitions requires constraints.parameter_bounds for exact-window verification');
  }
  const robustness = constraints.robustness || {};
  for (const key of ['min_chebyshev_radius', 'min_tunable_volume_lower_bound', 'min_tunable_volume']) {
    if (hasOwnKey(robustness, key)) {
      throw new Error(`constraints.robustness.${key} requires constraints.parameter_bounds for feasible-region geometry`);
    }
  }
}

function assertSampledSolverPrerequisites(spec) {
  const target = spec.target || {};
  const programLength = specBehaviorProgramLength(target);
  const hasWindow = specHasBehaviorInputWindow(target);
  const transitions = spec.constraints?.transitions;
  const outputFeature = target.output_feature;
  if (outputFeature) {
    if (!['threshold', 'level', 'fold_change'].includes(outputFeature.feature)) {
      throw new Error('target.output_feature must be threshold, level, or fold_change for sampled finite-dose verification');
    }
    const op = outputFeature.operator || '=';
    if (!['>=', '<=', '='].includes(op)) {
      throw new Error('target.output_feature.operator must be >=, <=, or = for sampled finite-dose verification');
    }
    if (!hasExplicitFiniteNumber(outputFeature, 'value')) {
      throw new Error('target.output_feature.value must be a finite number');
    }
    if (outputFeature.feature === 'fold_change' && outputFeature.value <= 0) {
      throw new Error('fold_change output_feature value must be positive');
    }
    if (!hasWindow) {
      throw new Error('target.output_feature requires target.behavior_spec.input_window.input_log10');
    }
  }
  if (target.shape && !hasWindow) {
    throw new Error('target.shape requires target.behavior_spec.input_window.input_log10');
  }
  const dynamicRange = spec.constraints?.dynamic_range;
  if (dynamicRange && !hasWindow) {
    throw new Error('constraints.dynamic_range requires target.behavior_spec.input_window.input_log10');
  }
  const inputWindow = target.behavior_spec?.input_window;
  if (inputWindow?.operating_points_log10) {
    if (!hasWindow) {
      throw new Error('behavior_spec.input_window.operating_points_log10 requires target.behavior_spec.input_window.input_log10');
    }
    if (programLength == null || inputWindow.operating_points_log10.length !== programLength) {
      throw new Error('behavior_spec.input_window.operating_points_log10 length must match behavior_spec.program length');
    }
    assertSpecOperatingPointsRealizable(
      inputWindow,
      transitions,
      'behavior_spec.input_window.operating_points_log10',
    );
  }
  if (transitions?.order) {
    if (!hasWindow) throw new Error('constraints.transitions.order requires target.behavior_spec.input_window.input_log10');
    if (programLength == null || transitions.order.length !== programLength) {
      throw new Error('constraints.transitions.order length must match behavior_spec.program length');
    }
    if (transitions.order.some(idx => idx >= programLength)) {
      throw new Error('constraints.transitions.order must be a full 0-based permutation of behavior_spec.program indices');
    }
  }
  if (hasOwnKey(transitions, 'min_spacing_decades')) {
    if (!hasWindow) throw new Error('constraints.transitions.min_spacing_decades requires target.behavior_spec.input_window.input_log10');
    if (programLength == null || programLength < 2) {
      throw new Error('transition spacing requires behavior_spec.program with at least two finite-window witnesses');
    }
  }
  const prefer = spec.ranking_policy?.prefer;
  if (Array.isArray(prefer)) {
    prefer.forEach((preference, index) => {
      if (preference === 'dynamic_range' && !dynamicRange) {
        throw new Error(`ranking_policy.prefer/${index} dynamic_range requires constraints.dynamic_range to produce a solver-backed metric`);
      }
      if (preference === 'transition_spacing' && !hasOwnKey(transitions, 'min_spacing_decades')) {
        throw new Error(`ranking_policy.prefer/${index} transition_spacing requires constraints.transitions.min_spacing_decades to produce a solver-backed metric`);
      }
      if (preference === 'condition_number' || preference === 'sampled_robustness') {
        throw new Error(`ranking_policy.prefer/${index} ${preference} has no solver-backed verified-card metric yet`);
      }
    });
  }
  assertParameterBoundsPrerequisites(spec);
}

function assertSpecShape(shape, path) {
  assertSpecNoUnknownKeys(shape, ['class', 'monotonicity', 'sample_points', 'tolerance_log10', 'min_prominence_log10', 'min_prominence_decades', 'hard'], path, 'shape');
  if (!['monotonic', 'bell_shaped'].includes(shape.class)) {
    throw new Error(`${path}.class must be monotonic or bell_shaped`);
  }
  if (!hasExplicitSamplePoints(shape, 'sample_points')) throw new Error(`${path}.sample_points must be an integer from 11 to 1001`);
  if (!hasExplicitNonnegativeNumber(shape, 'tolerance_log10')) throw new Error(`${path}.tolerance_log10 must be a finite non-negative number`);
  if (shape.class === 'monotonic' && !['increasing', 'decreasing', 'any'].includes(shape.monotonicity)) {
    throw new Error(`${path}.monotonicity must be increasing, decreasing, or any`);
  }
  if (shape.class === 'monotonic' && hasOwnKey(shape, 'min_prominence_log10')) {
    throw new Error(`${path}.min_prominence_log10 is not supported for monotonic target.shape`);
  }
  if (shape.class === 'monotonic' && hasOwnKey(shape, 'min_prominence_decades')) {
    throw new Error(`${path}.min_prominence_decades is not supported for monotonic target.shape`);
  }
  if (shape.class === 'bell_shaped' &&
      !hasExplicitNonnegativeNumber(shape, 'min_prominence_log10') &&
      !hasExplicitNonnegativeNumber(shape, 'min_prominence_decades')) {
    throw new Error(`${path} bell_shaped requires min_prominence_log10 or min_prominence_decades`);
  }
  if (shape.class === 'bell_shaped' &&
      hasOwnKey(shape, 'min_prominence_log10') &&
      hasOwnKey(shape, 'min_prominence_decades')) {
    throw new Error(`${path}.min_prominence_decades cannot be combined with min_prominence_log10`);
  }
  assertSpecBooleanField(shape, 'hard', path);
}

function assertSpecTemporalDynamics(temporal, path) {
  assertSpecNoUnknownKeys(temporal, ['stimulus', 'trace', 'peak_width_seconds', 'hard'], path, 'temporal_dynamics');
  if (hasOwnKey(temporal, 'stimulus') && !isPlainObject(temporal.stimulus)) {
    throw new Error(`${path}.stimulus must be an object`);
  }
  if (hasOwnKey(temporal, 'trace') && !Array.isArray(temporal.trace)) {
    throw new Error(`${path}.trace must be an array`);
  }
  if (hasOwnKey(temporal, 'peak_width_seconds')) {
    const peak = temporal.peak_width_seconds;
    assertSpecNoUnknownKeys(peak, ['min', 'max'], `${path}.peak_width_seconds`, 'peak_width_seconds');
    if (!hasOwnKey(peak, 'min') && !hasOwnKey(peak, 'max')) {
      throw new Error(`${path}.peak_width_seconds requires min or max`);
    }
    assertSpecOptionalNonnegativeField(peak, 'min', `${path}.peak_width_seconds`);
    assertSpecOptionalNonnegativeField(peak, 'max', `${path}.peak_width_seconds`);
    if (hasOwnKey(peak, 'min') && hasOwnKey(peak, 'max') && peak.min > peak.max) {
      throw new Error(`${path}.peak_width_seconds min must be <= max`);
    }
  }
  assertSpecBooleanField(temporal, 'hard', path);
}

function assertSpecTarget(target, path) {
  assertSpecNoUnknownKeys(target, ['legacy_target', 'behavior_spec', 'input_window', 'output_feature', 'temporal_dynamics', 'shape'], path, 'target');
  if (hasOwnKey(target, 'legacy_target') && hasOwnKey(target, 'behavior_spec')) {
    throw new Error(`${path} cannot mix legacy_target and behavior_spec`);
  }
  if (hasOwnKey(target, 'behavior_spec') && hasOwnKey(target, 'input_window')) {
    throw new Error(`${path}.input_window cannot be combined with target.behavior_spec; use target.behavior_spec.input_window`);
  }
  if (!['legacy_target', 'behavior_spec', 'output_feature', 'temporal_dynamics', 'shape'].some(key => hasOwnKey(target, key))) {
    throw new Error(`${path} requires legacy_target, behavior_spec, output_feature, temporal_dynamics, or shape`);
  }
  if (hasOwnKey(target, 'legacy_target')) assertSpecLegacyTarget(target.legacy_target, `${path}.legacy_target`);
  if (hasOwnKey(target, 'behavior_spec')) assertSpecBehaviorSpec(target.behavior_spec, `${path}.behavior_spec`);
  if (hasOwnKey(target, 'input_window')) assertSpecWindow(target.input_window, `${path}.input_window`);
  if (hasOwnKey(target, 'output_feature')) assertSpecOutputFeature(target.output_feature, `${path}.output_feature`);
  if (hasOwnKey(target, 'temporal_dynamics')) assertSpecTemporalDynamics(target.temporal_dynamics, `${path}.temporal_dynamics`);
  if (hasOwnKey(target, 'shape')) assertSpecShape(target.shape, `${path}.shape`);
}

function assertSpecParameterBounds(bounds, path) {
  assertSpecNoUnknownKeys(bounds, ['basis', 'kd_log10', 'total_log10', 'by_class'], path, 'parameter_bounds');
  if (hasOwnKey(bounds, 'basis') && bounds.basis !== 'log10_qK') throw new Error(`${path}.basis must be log10_qK`);
  if (!hasOwnKey(bounds, 'kd_log10') && !hasOwnKey(bounds, 'total_log10') && !hasOwnKey(bounds, 'by_class')) {
    throw new Error(`${path} requires kd_log10, total_log10, or by_class`);
  }
  if (hasOwnKey(bounds, 'kd_log10')) assertSpecBounds(bounds.kd_log10, `${path}.kd_log10`);
  if (hasOwnKey(bounds, 'total_log10')) assertSpecBounds(bounds.total_log10, `${path}.total_log10`);
  if (hasOwnKey(bounds, 'by_class')) {
    const byClass = bounds.by_class;
    assertSpecNoUnknownKeys(byClass, ['kd', 'total'], `${path}.by_class`, 'parameter_bounds.by_class');
    if (!hasOwnKey(byClass, 'kd') && !hasOwnKey(byClass, 'total')) {
      throw new Error(`${path}.by_class requires kd or total`);
    }
    if (hasOwnKey(byClass, 'kd')) assertSpecBounds(byClass.kd, `${path}.by_class.kd`);
    if (hasOwnKey(byClass, 'total')) assertSpecBounds(byClass.total, `${path}.by_class.total`);
    if (hasOwnKey(bounds, 'kd_log10') && hasOwnKey(byClass, 'kd')) {
      throw new Error(`${path}.kd_log10 and ${path}.by_class.kd cannot both be specified`);
    }
    if (hasOwnKey(bounds, 'total_log10') && hasOwnKey(byClass, 'total')) {
      throw new Error(`${path}.total_log10 and ${path}.by_class.total cannot both be specified`);
    }
  }
}

function assertSpecRobustness(robustness, path) {
  assertSpecNoUnknownKeys(robustness, ['min_chebyshev_radius', 'min_tunable_volume_lower_bound', 'min_tunable_volume', 'condition_number_max', 'min_sampled_pass_fraction', 'hard'], path, 'robustness');
  if (hasOwnKey(robustness, 'min_tunable_volume_lower_bound') && hasOwnKey(robustness, 'min_tunable_volume')) {
    throw new Error(`${path}.min_tunable_volume_lower_bound and ${path}.min_tunable_volume cannot both be specified`);
  }
  for (const key of ['min_chebyshev_radius', 'min_tunable_volume_lower_bound', 'min_tunable_volume', 'condition_number_max']) {
    assertSpecOptionalNonnegativeField(robustness, key, path);
  }
  assertSpecOptionalFractionField(robustness, 'min_sampled_pass_fraction', path);
  assertSpecBooleanField(robustness, 'hard', path);
}

function assertSpecDynamicRange(dynamicRange, path) {
  assertSpecNoUnknownKeys(dynamicRange, ['min_fold_change', 'sample_points', 'hard'], path, 'dynamic_range');
  assertSpecFiniteField(dynamicRange, 'min_fold_change', path);
  if (dynamicRange.min_fold_change < 0) throw new Error(`${path}.min_fold_change must be non-negative`);
  if (!hasExplicitSamplePoints(dynamicRange, 'sample_points')) throw new Error(`${path}.sample_points must be an integer from 11 to 1001`);
  assertSpecBooleanField(dynamicRange, 'hard', path);
}

function assertSpecTransitions(transitions, path) {
  assertSpecNoUnknownKeys(transitions, ['min_spacing_decades', 'order', 'hard'], path, 'transitions');
  if (!hasOwnKey(transitions, 'min_spacing_decades') && !hasOwnKey(transitions, 'order')) {
    throw new Error(`${path} requires min_spacing_decades or order`);
  }
  assertSpecOptionalNonnegativeField(transitions, 'min_spacing_decades', path);
  if (hasOwnKey(transitions, 'order')) {
    const order = transitions.order;
    if (!Array.isArray(order) || order.length === 0 || order.some(value => !Number.isInteger(value) || value < 0)) {
      throw new Error(`${path}.order must be a non-empty array of 0-based integer indices`);
    }
    if (new Set(order).size !== order.length) throw new Error(`${path}.order entries must be unique`);
  }
  assertSpecBooleanField(transitions, 'hard', path);
}

function assertSpecNetwork(network, path) {
  assertSpecNoUnknownKeys(network, ['max_species', 'max_reactions', 'max_mu', 'allow_near_minimal'], path, 'network');
  assertSpecOptionalPositiveIntField(network, 'max_species', path);
  assertSpecOptionalNonnegativeIntField(network, 'max_reactions', path);
  assertSpecOptionalPositiveIntField(network, 'max_mu', path);
  assertSpecBooleanField(network, 'allow_near_minimal', path);
}

function assertSpecConstraints(constraints, path) {
  assertSpecNoUnknownKeys(constraints, ['network', 'parameter_bounds', 'robustness', 'dynamic_range', 'transitions'], path, 'constraints');
  if (hasOwnKey(constraints, 'network')) assertSpecNetwork(constraints.network, `${path}.network`);
  if (hasOwnKey(constraints, 'parameter_bounds')) assertSpecParameterBounds(constraints.parameter_bounds, `${path}.parameter_bounds`);
  if (hasOwnKey(constraints, 'robustness')) assertSpecRobustness(constraints.robustness, `${path}.robustness`);
  if (hasOwnKey(constraints, 'dynamic_range')) assertSpecDynamicRange(constraints.dynamic_range, `${path}.dynamic_range`);
  if (hasOwnKey(constraints, 'transitions')) assertSpecTransitions(constraints.transitions, `${path}.transitions`);
}

function assertSpecCandidateBudget(candidateBudget, path) {
  assertSpecNoUnknownKeys(candidateBudget, ['mode', 'max_extra_species', 'max_extra_reactions', 'max_extra_mu', 'max_screened', 'max_verified_recommendations', 'max_recommended', 'max_near_misses', 'max_exact_placements'], path, 'candidate_budget');
  if (hasOwnKey(candidateBudget, 'mode') && !['near_minimal', 'all_matches'].includes(candidateBudget.mode)) {
    throw new Error(`${path}.mode must be near_minimal or all_matches`);
  }
  for (const key of ['max_extra_species', 'max_extra_reactions', 'max_extra_mu', 'max_screened', 'max_verified_recommendations', 'max_recommended', 'max_near_misses', 'max_exact_placements']) {
    assertSpecOptionalNonnegativeIntField(candidateBudget, key, path);
  }
}

function assertSpecRankingPolicy(rankingPolicy, path) {
  assertSpecNoUnknownKeys(rankingPolicy, ['verified_only', 'prefer'], path, 'ranking_policy');
  if (hasOwnKey(rankingPolicy, 'verified_only') && rankingPolicy.verified_only !== true) {
    throw new Error(`${path}.verified_only must be true`);
  }
  if (hasOwnKey(rankingPolicy, 'prefer')) {
    const allowed = new Set(['evidence_grade', 'certificate_grade', 'chebyshev_radius', 'tunable_volume', 'dynamic_range', 'transition_spacing', 'sampled_robustness', 'condition_number', 'complexity']);
    const prefer = rankingPolicy.prefer;
    if (!Array.isArray(prefer) || prefer.some(value => typeof value !== 'string' || !allowed.has(value))) {
      throw new Error(`${path}.prefer contains an unsupported ranking preference`);
    }
  }
}

function assertSpecAuditPolicy(auditPolicy, path) {
  assertSpecNoUnknownKeys(auditPolicy, ['unsupported', 'path_format', 'include_supported'], path, 'audit_policy');
  if (hasOwnKey(auditPolicy, 'unsupported') && auditPolicy.unsupported !== 'block_if_hard') {
    throw new Error(`${path}.unsupported must be block_if_hard`);
  }
  if (hasOwnKey(auditPolicy, 'path_format') && auditPolicy.path_format !== 'json_pointer') {
    throw new Error(`${path}.path_format must be json_pointer`);
  }
  assertSpecBooleanField(auditPolicy, 'include_supported', path);
}

function assertSpecSource(source, path) {
  if (!isPlainObject(source)) {
    throw new Error('source must be an object with source.kind');
  }
  assertSpecNoUnknownKeys(source, ['kind', 'node_id', 'agent_message_id', 'provenance'], path, 'source');
  if (!['manual_config', 'agent_design', 'legacy_shorthand', 'imported_json', 'test_fixture', 'hand_authored'].includes(source.kind)) {
    throw new Error('source.kind must be a valid DesignabilitySpec source kind');
  }
  assertSpecStringField(source, 'node_id', path);
  assertSpecStringField(source, 'agent_message_id', path);
  if (hasOwnKey(source, 'provenance') && !isPlainObject(source.provenance)) {
    throw new Error('source.provenance must be an object');
  }
}

function assertDesignabilitySpecFrontendContract(spec) {
  assertSpecNoUnknownKeys(spec, ['schema_version', 'source', 'target', 'constraints', 'candidate_budget', 'ranking_policy', 'audit_policy'], 'DesignabilitySpec', 'DesignabilitySpec');
  if (spec.schema_version !== DESIGNABILITY_SPEC_VERSION) {
    throw new Error(`DesignabilitySpec must use ${DESIGNABILITY_SPEC_VERSION}`);
  }
  assertSpecSource(spec.source, 'source');
  assertSpecTarget(spec.target, 'target');
  if (hasOwnKey(spec, 'constraints')) assertSpecConstraints(spec.constraints, 'constraints');
  if (hasOwnKey(spec, 'candidate_budget')) assertSpecCandidateBudget(spec.candidate_budget, 'candidate_budget');
  if (hasOwnKey(spec, 'ranking_policy')) assertSpecRankingPolicy(spec.ranking_policy, 'ranking_policy');
  if (hasOwnKey(spec, 'audit_policy')) assertSpecAuditPolicy(spec.audit_policy, 'audit_policy');
  assertSampledSolverPrerequisites(spec);
}

function parseSpecJsonOverride(nodeId) {
  const raw = (document.getElementById(`${nodeId}-spec-json`)?.value || '').trim();
  if (!raw) return null;
  const parsed = JSON.parse(raw);
  if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) {
    throw new Error('Spec JSON must be an object');
  }
  return parsed;
}

function isPlainObject(value) {
  return Boolean(value) && typeof value === 'object' && !Array.isArray(value);
}

function normalizeRobustnessVolumeAlias(robustness) {
  if (!isPlainObject(robustness)) return robustness;
  const next = { ...robustness };
  if (Object.prototype.hasOwnProperty.call(next, 'min_tunable_volume')) {
    if (Object.prototype.hasOwnProperty.call(next, 'min_tunable_volume_lower_bound') &&
        next.min_tunable_volume_lower_bound !== next.min_tunable_volume) {
      throw new Error('Use only min_tunable_volume_lower_bound; min_tunable_volume is a legacy alias.');
    }
    next.min_tunable_volume_lower_bound = next.min_tunable_volume;
    delete next.min_tunable_volume;
  }
  return next;
}

function normalizeSpecConstraints(constraints) {
  if (!isPlainObject(constraints)) return constraints;
  const next = { ...constraints };
  if (Object.prototype.hasOwnProperty.call(next, 'robustness')) {
    next.robustness = normalizeRobustnessVolumeAlias(next.robustness);
  }
  return next;
}

function mergeSpecConstraints(baseConstraints, overrideConstraints) {
  const base = normalizeSpecConstraints(baseConstraints || {});
  if (!isPlainObject(overrideConstraints)) return normalizeSpecConstraints(overrideConstraints || base);
  const override = normalizeSpecConstraints(overrideConstraints);
  const merged = { ...(isPlainObject(base) ? base : {}), ...override };
  for (const key of ['parameter_bounds', 'robustness', 'dynamic_range', 'transitions', 'network']) {
    if (isPlainObject(base?.[key]) && isPlainObject(override?.[key])) {
      merged[key] = { ...base[key], ...override[key] };
    }
  }
  return normalizeSpecConstraints(merged);
}

function parseReactionOrderProgram(rawValue) {
  const raw = String(rawValue || '').trim();
  if (!raw) throw new Error('Enter a reaction-order program');
  if (/(^|,)\s*(,|$)/.test(raw)) throw new Error('A reaction-order program looks like 1, 0, -1');
  const values = raw.split(/[,\s]+/).map(Number);
  if (!values.length || values.some(v => !Number.isFinite(v))) {
    throw new Error('A reaction-order program looks like 1, 0, -1');
  }
  return values.map(value => ({
    kind: 'reaction_order',
    operator: '=',
    value,
  }));
}

function mergeSpecOverride(baseSpec, override) {
  if (!override) return baseSpec;
  if (override.schema_version) {
    const spec = {
      ...override,
    };
    if (isPlainObject(override.source)) {
      spec.source = {
        ...override.source,
        ...(baseSpec.source?.node_id ? { node_id: baseSpec.source.node_id } : {}),
      };
    }
    return spec;
  }
  const hasTopLevelSpecKeys = ['target', 'constraints', 'candidate_budget', 'ranking_policy', 'audit_policy']
    .some(key => Object.prototype.hasOwnProperty.call(override, key));
  if (!hasTopLevelSpecKeys) {
    return {
      ...baseSpec,
      target: {
        ...(baseSpec.target || {}),
        ...override,
      },
    };
  }
  return {
    ...baseSpec,
    ...override,
    source: {
      ...(baseSpec.source || {}),
      ...(override.source || {}),
      kind: override.source?.kind || baseSpec.source?.kind || 'manual_config',
    },
    target: {
      ...(baseSpec.target || {}),
      ...(override.target || {}),
    },
    constraints: mergeSpecConstraints(baseSpec.constraints, override.constraints),
    candidate_budget: {
      ...(baseSpec.candidate_budget || {}),
      ...(override.candidate_budget || {}),
    },
    ranking_policy: {
      ...(baseSpec.ranking_policy || {}),
      ...(override.ranking_policy || {}),
    },
    audit_policy: {
      ...(baseSpec.audit_policy || {}),
      ...(override.audit_policy || {}),
    },
  };
}

function buildDesignabilitySpecFromBehaviorSpec(nodeId, rawProgram, options = {}) {
  const input = (document.getElementById(`${nodeId}-spec-input`)?.value || '').trim();
  const output = (document.getElementById(`${nodeId}-spec-output`)?.value || '').trim();
  if (!input) throw new Error('Enter the behavior input total, for example tA');
  if (!output) throw new Error('Enter the behavior output species, for example C_A_A');
  return {
    schema_version: DESIGNABILITY_SPEC_VERSION,
    source: {
      kind: options.sourceKind || 'manual_config',
      node_id: nodeId,
    },
    target: {
      behavior_spec: {
        feature_space: 'reaction_order',
        input,
        output,
        program: parseReactionOrderProgram(rawProgram),
      },
    },
    constraints: options.constraints || {},
    candidate_budget: {
      mode: 'near_minimal',
      max_extra_species: 1,
      max_extra_reactions: 1,
      max_extra_mu: 1,
      max_recommended: 24,
      max_verified_recommendations: 24,
      max_screened: 24,
      max_near_misses: 12,
      max_exact_placements: 3,
      ...(options.candidateBudget || {}),
    },
    ranking_policy: {
      verified_only: true,
      ...(options.rankingPolicy || {}),
    },
    audit_policy: {
      unsupported: 'block_if_hard',
      path_format: 'json_pointer',
      include_supported: true,
      ...(options.auditPolicy || {}),
    },
  };
}

export function readDesignSpecConfig(nodeId) {
  const kind = document.getElementById(`${nodeId}-spec-kind`)?.value || 'sign';
  const override = parseSpecJsonOverride(nodeId);
  const auditPolicy = {
    unsupported: 'block_if_hard',
    path_format: 'json_pointer',
    include_supported: true,
  };
  if (override?.schema_version) {
    assertSpecSource(override.source, 'source');
    const spec = mergeSpecOverride({
      schema_version: DESIGNABILITY_SPEC_VERSION,
      source: { kind: 'manual_config', node_id: nodeId },
      target: {},
      constraints: {},
      candidate_budget: {},
      ranking_policy: { verified_only: true },
      audit_policy: auditPolicy,
    }, override);
    if (spec.schema_version !== DESIGNABILITY_SPEC_VERSION) {
      throw new Error(`DesignabilitySpec must use ${DESIGNABILITY_SPEC_VERSION}`);
    }
    assertManualConfigSupportedClauses(spec);
    assertDesignabilitySpecFrontendContract(spec);
    if (nodeRegistry[nodeId]) {
      nodeRegistry[nodeId].data = nodeRegistry[nodeId].data || {};
      nodeRegistry[nodeId].data.config = nodeRegistry[nodeId].data.config || {};
      nodeRegistry[nodeId].data.config.designabilitySpec = spec;
    }
    return spec;
  }
  const raw = (document.getElementById(`${nodeId}-spec-target`)?.value || '').trim();
  const kdLo = parseFiniteField(nodeId, '-spec-kd-lo', -3);
  const kdHi = parseFiniteField(nodeId, '-spec-kd-hi', 3);
  const totalLo = parseFiniteField(nodeId, '-spec-total-lo', -3);
  const totalHi = parseFiniteField(nodeId, '-spec-total-hi', 3);
  if (kdLo > kdHi) throw new Error('Kd lower bound exceeds upper bound');
  if (totalLo > totalHi) throw new Error('Total lower bound exceeds upper bound');
  const blockHard = document.getElementById(`${nodeId}-spec-block-hard`)?.checked !== false;

  const candidateBudget = {
    mode: 'near_minimal',
    max_extra_species: parseIntField(nodeId, '-spec-extra-species', 1),
    max_extra_reactions: parseIntField(nodeId, '-spec-extra-reactions', 1),
    max_extra_mu: parseIntField(nodeId, '-spec-extra-mu', 1),
    max_recommended: 24,
    max_verified_recommendations: 24,
    max_screened: 24,
    max_near_misses: 12,
    max_exact_placements: parseIntField(nodeId, '-spec-max-exact', (kind === 'exact' || kind === 'behavior_spec') ? 3 : 0),
  };
  const minVolume = parseOptionalNonnegativeField(nodeId, '-spec-volume', 'Min tunable volume lower bound');
  const constraints = {
    parameter_bounds: {
      kd_log10: [kdLo, kdHi],
      total_log10: [totalLo, totalHi],
    },
    robustness: {
      min_chebyshev_radius: parseFiniteField(nodeId, '-spec-radius', 0),
      ...(minVolume == null ? {} : { min_tunable_volume_lower_bound: minVolume }),
    },
    ...readOptionalSpecConstraintClauses(nodeId, blockHard),
  };
  const rankingPolicy = readSpecRankingPolicy(nodeId);
  const targetClauses = readOptionalSpecTargetClauses(nodeId, blockHard);
  const base = kind === 'behavior_spec'
    ? buildDesignabilitySpecFromBehaviorSpec(nodeId, raw, {
        sourceKind: 'manual_config',
        constraints,
        candidateBudget,
        rankingPolicy,
        auditPolicy,
      })
    : raw
      ? buildDesignabilitySpecFromLegacyTarget(kind, raw, {
        sourceKind: 'manual_config',
        nodeId,
        constraints,
        candidateBudget,
        rankingPolicy,
        auditPolicy,
      })
      : {
        schema_version: DESIGNABILITY_SPEC_VERSION,
        source: { kind: 'manual_config', node_id: nodeId },
        target: {},
        constraints,
        candidate_budget: candidateBudget,
        ranking_policy: rankingPolicy,
        audit_policy: auditPolicy,
      };
  if (kind === 'behavior_spec' && targetClauses.input_window && base.target?.behavior_spec) {
    base.target.behavior_spec = {
      ...base.target.behavior_spec,
      input_window: targetClauses.input_window,
    };
    delete targetClauses.input_window;
  }
  base.target = {
    ...(base.target || {}),
    ...targetClauses,
  };
  const spec = mergeSpecOverride(base, override);
  if (!spec.target || Object.keys(spec.target).length === 0) {
    throw new Error('Configure a target behavior or provide target/spec JSON');
  }
  if (spec.schema_version !== DESIGNABILITY_SPEC_VERSION) {
    throw new Error(`DesignabilitySpec must use ${DESIGNABILITY_SPEC_VERSION}`);
  }
  assertManualConfigSupportedClauses(spec);
  assertDesignabilitySpecFrontendContract(spec);
  if (nodeRegistry[nodeId]) {
    nodeRegistry[nodeId].data = nodeRegistry[nodeId].data || {};
    nodeRegistry[nodeId].data.config = nodeRegistry[nodeId].data.config || {};
    nodeRegistry[nodeId].data.config.designabilitySpec = spec;
  }
  return spec;
}

function designSpecSourceForTarget(nodeId) {
  const conn = connections.find(c => c.toNode === nodeId && c.toPort === 'designability-spec');
  if (!conn) return null;
  const info = nodeRegistry[conn.fromNode];
  if (!info) throw new Error('Connected DesignabilitySpec source is missing');
  if (info.type !== 'design-spec-config') {
    throw new Error('Connect a Design Spec Config node to the DesignabilitySpec input');
  }
  return conn.fromNode;
}

export async function validateDesignSpecConfig(nodeId) {
  const preview = document.getElementById(`${nodeId}-spec-preview`);
  try {
    const spec = readDesignSpecConfig(nodeId);
    const data = await api('validate_designability_spec', spec);
    if (preview) {
      preview.style.display = '';
      const auditCount = Array.isArray(data.constraint_audit) ? data.constraint_audit.length : 0;
      if (data.blocked_by_unsupported_hard_clause) {
        const auditRows = (Array.isArray(data.constraint_audit) ? data.constraint_audit : [])
          .slice(0, 4)
          .map(item => `<div class="text-dim">${escapeHtml(item.path || '(unknown path)')} · ${escapeHtml(item.support_level || 'unknown')}</div>`)
          .join('');
        preview.innerHTML = `<span class="tag tag-atlas-failed">blocked</span> ` +
          `<span class="text-dim">${escapeHtml(auditCount)} audited constraint${auditCount === 1 ? '' : 's'}</span>` +
          auditRows;
      } else {
        preview.innerHTML = `<span class="tag tag-atlas-ok">valid</span> ` +
          `<span class="text-dim">${escapeHtml(auditCount)} audited constraint${auditCount === 1 ? '' : 's'}</span>`;
      }
    }
    showToast('DesignabilitySpec validated');
    return data;
  } catch (e) {
    if (preview) {
      preview.style.display = '';
      preview.innerHTML = `<span class="node-error">${escapeHtml(e.message)}</span>`;
    } else {
      alert(e.message);
    }
    return null;
  }
}

// Behavior labels (the 16 base-motifs) with a representative RO program each,
// fetched once from /api/v1/design_labels and cached. Powers the label picker.
let _DESIGN_LABELS = null;
let _DESIGN_LABELS_PROMISE = null;
export async function loadDesignLabels() {
  if (Array.isArray(_DESIGN_LABELS)) return _DESIGN_LABELS;
  if (_DESIGN_LABELS_PROMISE) return _DESIGN_LABELS_PROMISE;
  _DESIGN_LABELS_PROMISE = api('design_labels', {})
    .then(data => {
      _DESIGN_LABELS = Array.isArray(data.labels) ? data.labels : [];
      return _DESIGN_LABELS;
    })
    .finally(() => { _DESIGN_LABELS_PROMISE = null; });
  return _DESIGN_LABELS_PROMISE;
}

// Swap the Target field to match the chosen kind: a picker of the 16 base-motifs
// (label mode, showing each label's RO translation) or a free-text input
// (sign / exact). Both use id "${nodeId}-target" so runDesignSearch reads .value.
export async function onDesignTargetKindChange(nodeId) {
  invalidateDesignTargetInputs(nodeId, 'design-target-kind-changed');
  await updateDesignTargetField(nodeId, {
    kindSuffix: '-kind',
    wrapSuffix: '-target-wrap',
    targetSuffix: '-target',
  });
}

export async function onDesignSpecKindChange(nodeId) {
  invalidateDesignTargetsFromSpec(nodeId, 'design-spec-kind-changed');
  await updateDesignTargetField(nodeId, {
    kindSuffix: '-spec-kind',
    wrapSuffix: '-spec-target-wrap',
    targetSuffix: '-spec-target',
  });
}

async function updateDesignTargetField(nodeId, suffixes) {
  const kindEl = document.getElementById(`${nodeId}${suffixes.kindSuffix}`);
  const wrap = document.getElementById(`${nodeId}${suffixes.wrapSuffix}`);
  if (!kindEl || !wrap) return;
  const kind = kindEl.value;
  const priorTarget = document.getElementById(`${nodeId}${suffixes.targetSuffix}`);
  const priorValue = priorTarget?.value || '';
  const priorWasSelect = String(priorTarget?.tagName || '').toUpperCase() === 'SELECT';
  if (kind === 'label') {
    wrap.innerHTML = `<select id="${nodeId}${suffixes.targetSuffix}" style="flex:1;" disabled><option>Loading labels...</option></select>`;
    let labels;
    try {
      labels = await loadDesignLabels();
    } catch (e) {
      if (document.getElementById(`${nodeId}${suffixes.kindSuffix}`)?.value === 'label') {
        wrap.innerHTML = `<select id="${nodeId}${suffixes.targetSuffix}" style="flex:1;" disabled><option>Labels unavailable</option></select>`;
      }
      console.warn('[design-target] Failed to load labels', e);
      return;
    }
    if (document.getElementById(`${nodeId}${suffixes.kindSuffix}`)?.value !== 'label') return;
    const opts = labels.map(l =>
      `<option value="${escapeHtml(l.label)}">${escapeHtml(l.label)} — ${escapeHtml(l.ro_program)} (${escapeHtml(l.count)})</option>`).join('');
    wrap.innerHTML = `<select id="${nodeId}${suffixes.targetSuffix}" style="flex:1;">${opts || '<option value="">No labels available</option>'}</select>`;
    const targetEl = document.getElementById(`${nodeId}${suffixes.targetSuffix}`);
    if (targetEl && labels.some(l => l.label === priorValue)) targetEl.value = priorValue;
  } else {
    const ph = kind === 'sign' ? '+-+' : '1, 0, -1';
    wrap.innerHTML = `<input type="text" id="${nodeId}${suffixes.targetSuffix}" placeholder="${ph}" style="flex:1;">`;
    const targetEl = document.getElementById(`${nodeId}${suffixes.targetSuffix}`);
    if (targetEl && priorValue && !priorWasSelect) targetEl.value = priorValue;
  }
}

function installDesignTargetInputInvalidation(nodeId) {
  const info = nodeRegistry[nodeId];
  const root = document.getElementById(nodeId);
  if (!info || !root?.addEventListener || info._designInputInvalidationInstalled) return;
  info._designInputInvalidationInstalled = true;
  const invalidateInput = event => {
    const id = String(event?.target?.id || '');
    if (id === `${nodeId}-kind` || id === `${nodeId}-target`) {
      invalidateDesignTargetInputs(nodeId, 'design-target-input-changed');
    }
  };
  root.addEventListener('input', invalidateInput);
  root.addEventListener('change', invalidateInput);
}

function invalidateDesignTargetsFromSpec(specNodeId, reason) {
  const targetIds = new Set(connections
    .filter(connection =>
      connection.fromNode === specNodeId &&
      connection.fromPort === 'designability-spec' &&
      connection.toPort === 'designability-spec')
    .map(connection => connection.toNode));
  for (const targetId of targetIds) invalidateDesignTargetInputs(targetId, reason);
  return [...targetIds];
}

function installDesignSpecInputInvalidation(nodeId) {
  const info = nodeRegistry[nodeId];
  const root = document.getElementById(nodeId);
  if (!info || !root?.addEventListener || info._designSpecInvalidationInstalled) return;
  info._designSpecInvalidationInstalled = true;
  const invalidateInput = event => {
    const id = String(event?.target?.id || '');
    if (id.startsWith(`${nodeId}-spec-`)) {
      invalidateDesignTargetsFromSpec(nodeId, 'design-spec-input-changed');
    }
  };
  root.addEventListener('input', invalidateInput);
  root.addEventListener('change', invalidateInput);
}

export function invalidateDesignTargetInputs(nodeId, reason = 'design-target-input-changed') {
  const info = nodeRegistry[nodeId];
  if (!info || info.type !== 'design-target') return false;
  info.data = info.data || {};
  lifecycleForOwner(info, DESIGN_SCREEN_LIFECYCLE_KEY);
  lifecycleForOwner(info, DESIGN_SELECTION_LIFECYCLE_KEY);
  invalidateBoundLifecycle(info, DESIGN_SCREEN_LIFECYCLE_KEY, reason);
  invalidateBoundLifecycle(info, DESIGN_SELECTION_LIFECYCLE_KEY, reason);
  delete info.data.designSearchResponse;
  const config = info.data.config = info.data.config || {};
  delete config.resolvedDefinition;
  delete config.ropShapeReference;
  invalidateConnectedRopShapeArtifacts(nodeId);
  syncDesignLifecycleSnapshots(info);
  setNodeLoading(nodeId, false);
  return true;
}

function readDesignSearchRequest(nodeId) {
  const specSourceNodeId = designSpecSourceForTarget(nodeId);
  if (specSourceNodeId) {
    return buildDesignScreenRequestFromSpec(readDesignSpecConfig(specSourceNodeId));
  }
  const kindElement = document.getElementById(`${nodeId}-kind`);
  const targetElement = document.getElementById(`${nodeId}-target`);
  if (!kindElement || !targetElement) throw new Error('Design Target controls are unavailable');
  return buildDesignScreenRequest(kindElement.value, String(targetElement.value || '').trim());
}

function designSearchAttemptFingerprint(nodeId) {
  let specSourceNodeId = null;
  try { specSourceNodeId = designSpecSourceForTarget(nodeId); }
  catch { specSourceNodeId = 'invalid-source'; }
  return stableLifecycleFingerprint({
    kind: document.getElementById(`${nodeId}-kind`)?.value ?? null,
    target: document.getElementById(`${nodeId}-target`)?.value ?? null,
    specSourceNodeId,
    specConfig: specSourceNodeId && nodeRegistry[specSourceNodeId]?.data?.config
      ? nodeRegistry[specSourceNodeId].data.config
      : null,
  });
}

function designSearchRequestFingerprint(request) {
  return stableLifecycleFingerprint({ endpoint: DESIGN_SCREEN_ENDPOINT, request });
}

function currentDesignSearchContext(nodeId) {
  const owner = nodeRegistry[nodeId];
  if (!owner) return null;
  let inputFingerprint;
  try {
    inputFingerprint = designSearchRequestFingerprint(readDesignSearchRequest(nodeId));
  } catch {
    inputFingerprint = stableLifecycleFingerprint({
      invalid: true,
      attempt: designSearchAttemptFingerprint(nodeId),
    });
  }
  return designLifecycleContext(nodeId, inputFingerprint);
}

function designScreenEvidence(data) {
  const evidence = {
    source_endpoint: DESIGN_SCREEN_ENDPOINT,
    truncated: data?.truncated === true,
  };
  if (Number.isInteger(data?.eligible_count)) evidence.eligible_count = data.eligible_count;
  if (Number.isInteger(data?.evaluated_count)) evidence.evaluated_count = data.evaluated_count;
  return evidence;
}

function candidateEvidence(card) {
  const evidence = {};
  for (const key of ['evidence_grade', 'certificate_grade', 'screen_status']) {
    if (typeof card?.[key] === 'string' && card[key]) evidence[key] = card[key];
  }
  return Object.keys(evidence).length ? evidence : null;
}

function selectedCandidateEntry(response, selectedKey) {
  if (!selectedKey) return null;
  const matches = candidateEntries(response).filter(({ card }) =>
    isPlainObject(card) &&
    designCandidateKey(card.nid, card.inp, card.out) === selectedKey);
  return matches.length === 1 ? matches[0] : null;
}

function publishSelectedNetwork(nodeId, entry, rules) {
  const info = nodeRegistry[nodeId];
  if (!info || !entry?.card || !Array.isArray(rules) || !rules.length) return false;
  const screen = info[DESIGN_SCREEN_LIFECYCLE_KEY]
    ? inspectExecutionLifecycle(info[DESIGN_SCREEN_LIFECYCLE_KEY])
    : null;
  if (screen?.state !== 'current' || screen.freshness !== 'current') return false;

  const card = entry.card;
  const selectedKey = designCandidateKey(card.nid, card.inp, card.out);
  const inputFingerprint = stableLifecycleFingerprint({
    screenInputFingerprint: screen.inputFingerprint,
    selectedKey,
    card,
  });
  const context = designLifecycleContext(nodeId, inputFingerprint);
  if (!context) return false;
  const lifecycle = lifecycleForOwner(info, DESIGN_SELECTION_LIFECYCLE_KEY);
  const ticket = beginLifecycle(lifecycle, context);
  const result = {
    selectedCandidateKey: selectedKey,
    raw_rules: rules,
  };
  if (!commitLifecycle(lifecycle, ticket, {
    context,
    result,
    evidence: candidateEvidence(card),
  })) return false;

  const config = info.data.config = info.data.config || {};
  config.resolvedDefinition = { raw_rules: rules };
  config.selectedNid = card.nid;
  config.suggestedInput = card.inp;
  config.suggestedOutput = card.out;
  config.selectedCandidateKey = selectedKey;
  syncDesignLifecycleSnapshots(info);
  return true;
}

function refreshSelectedOutputFromScreen(nodeId, response) {
  const info = nodeRegistry[nodeId];
  const config = info?.data?.config;
  if (!info || !config?.selectedCandidateKey) return null;
  const entry = selectedCandidateEntry(response, config.selectedCandidateKey);
  if (!entry) return null;
  let rules;
  try { rules = nidToRules(entry.card.nid); }
  catch { return null; }
  if (!publishSelectedNetwork(nodeId, entry, rules)) return null;
  return entry;
}

function staleDesignSearchOutcome(nodeId) {
  return staleOutcome(nodeId, {
    code: 'design_screen_obsolete',
    message: 'Design Screen response no longer owns the current node inputs',
    outputs: { reactions: 'missing', 'rop-shape-reference': 'missing' },
  });
}

function settleDesignNodeLoading(nodeId, lifecycle, ticket) {
  if (!ticket || nodeRegistry[nodeId] !== ticket.owner) return;
  const runtime = inspectExecutionLifecycle(lifecycle);
  if (runtime.currentTicket === ticket ||
      (runtime.currentTicket == null && runtime.loading === false)) {
    setNodeLoading(nodeId, false);
  }
}

export async function runDesignSearch(nodeId) {
  const owner = nodeRegistry[nodeId];
  if (!owner || owner.type !== 'design-target') {
    return blockedOutcome(nodeId, {
      code: 'missing_design_target',
      message: 'Design Target node is unavailable',
      outputs: { reactions: 'missing', 'rop-shape-reference': 'missing' },
    });
  }
  const lifecycle = lifecycleForOwner(owner, DESIGN_SCREEN_LIFECYCLE_KEY);
  lifecycleForOwner(owner, DESIGN_SELECTION_LIFECYCLE_KEY);
  invalidateDesignTargetInputs(nodeId, 'design-search-restarted');

  let request;
  try {
    request = readDesignSearchRequest(nodeId);
  } catch (e) {
    const context = designLifecycleContext(nodeId, stableLifecycleFingerprint({
      invalid: true,
      attempt: designSearchAttemptFingerprint(nodeId),
    }));
    if (context) {
      const ticket = beginLifecycle(lifecycle, context);
      blockLifecycle(lifecycle, ticket, {
        context,
        reason: e?.message || String(e),
      });
      syncDesignLifecycleSnapshots(nodeRegistry[nodeId]);
    }
    alert(e.message);
    return blockedOutcome(nodeId, {
      code: 'invalid_design_target',
      message: e?.message || String(e),
      outputs: { reactions: 'missing', 'rop-shape-reference': 'missing' },
    });
  }
  const beginContext = designLifecycleContext(nodeId, designSearchRequestFingerprint(request));
  if (!beginContext) return staleDesignSearchOutcome(nodeId);
  const ticket = beginLifecycle(lifecycle, beginContext);
  syncDesignLifecycleSnapshots(owner);
  setNodeLoading(nodeId, true);
  try {
    const data = await api('design_screen', request, {
      statusIsCurrent: () => {
        const context = currentDesignSearchContext(nodeId);
        return !!context && isCurrentLifecycle(lifecycle, ticket, context);
      },
    });
    const currentContext = currentDesignSearchContext(nodeId);
    if (!currentContext || !commitLifecycle(lifecycle, ticket, {
      context: currentContext,
      result: data,
      evidence: designScreenEvidence(data),
    })) {
      syncDesignLifecycleSnapshots(ticket.owner);
      return staleDesignSearchOutcome(nodeId);
    }

    const info = nodeRegistry[nodeId];
    if (info !== ticket.owner) return staleDesignSearchOutcome(nodeId);
    info.data = info.data || {};
    // Search results are runtime evidence for resolving the selected card.
    // Keep them outside config so workspace serialization stays compact.
    info.data.designSearchResponse = deepCloneJson(data);
    syncDesignLifecycleSnapshots(info);
    const cfg = info.data.config = info.data.config || {};
    const previousSelectedKey = cfg.selectedCandidateKey;
    const previousReference = cfg.ropShapeReference;
    refreshSelectedOutputFromScreen(nodeId, info.data.designSearchResponse);
    const currentReference = refreshRopShapeReferenceForSelection(
      cfg,
      info.data.designSearchResponse,
    );
    if (didRopShapeReferenceChange(
      previousSelectedKey,
      previousReference,
      cfg.selectedCandidateKey,
      currentReference,
    )) {
      invalidateConnectedRopShapeArtifacts(nodeId);
      commitWorkspaceSnapshot('design-target-reference-refresh');
    }
    const contentEl = document.getElementById(`${nodeId}-content`);
    if (!contentEl || !isCurrentLifecycle(lifecycle, ticket, currentContext)) {
      return staleDesignSearchOutcome(nodeId);
    }
    contentEl.innerHTML = renderDesignScreenResults(nodeId, data, {
      selectedNid: cfg.selectedNid || null,
      selectedInput: cfg.suggestedInput || null,
      selectedOutput: cfg.suggestedOutput || null,
      selectedCandidateKey: cfg.selectedCandidateKey || null,
    });
    wireNetworkRows(nodeId, contentEl);
    const selectedRules = cfg.resolvedDefinition?.raw_rules;
    return succeededOutcome(nodeId, {
      outputs: {
        reactions: Array.isArray(selectedRules) && selectedRules.length > 0 ? 'present' : 'missing',
        'rop-shape-reference': cfg.ropShapeReference ? 'present' : 'missing',
      },
      code: Array.isArray(selectedRules) && selectedRules.length > 0
        ? null
        : 'manual_candidate_selection_required',
      message: Array.isArray(selectedRules) && selectedRules.length > 0
        ? null
        : 'Select a screened candidate before running downstream compute nodes',
    });
  } catch (e) {
    const currentContext = currentDesignSearchContext(nodeId);
    const failed = !!currentContext && failLifecycle(lifecycle, ticket, {
      context: currentContext,
      error: e,
    });
    if (!failed) {
      syncDesignLifecycleSnapshots(ticket.owner);
      return staleDesignSearchOutcome(nodeId);
    }
    const info = nodeRegistry[nodeId];
    syncDesignLifecycleSnapshots(info);
    const contentEl = document.getElementById(`${nodeId}-content`);
    if (info === ticket.owner && contentEl) {
      contentEl.innerHTML = `<div class="node-error">${escapeHtml(e.message)}</div>`;
    }
    return failedOutcome(nodeId, {
      code: 'design_screen_failed',
      message: e?.message || String(e),
      outputs: { reactions: 'missing', 'rop-shape-reference': 'missing' },
    });
  } finally {
    releaseLifecycle(lifecycle, ticket);
    settleDesignNodeLoading(nodeId, lifecycle, ticket);
  }
}

function wireNetworkRows(nodeId, contentEl) {
  contentEl.querySelectorAll('.design-net-row').forEach(row => {
    const pick = () => selectNetwork(nodeId, row.dataset.nid, row.dataset.inp, row.dataset.out);
    row.addEventListener('click', pick);
    row.addEventListener('keydown', e => {
      if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); pick(); }
    });
    const btn = row.querySelector('.design-build-btn');
    if (btn) btn.addEventListener('click', e => {
      e.stopPropagation();   // don't double-fire the row's select handler
      buildAndTune(nodeId, row.dataset.nid, row.dataset.inp, row.dataset.out, btn);
    });
  });
}

// ── nid → reaction rules ───────────────────────────────────────────────────
// nid is the network in monomer-index code, reactions joined by '|', each side a
// '+'-sum of complex tokens `[m1,m2,...]`. The atlas naming (verified against the
// bundled slices) is: monomer [i] → letter (1→A, 2→B, …); complex → 'C_'+letters
// joined by '_'. e.g. [1]+[1]<->[1,1] → "A + A <-> C_A_A"; [1]+[2]<->[1,2] →
// "A + B <-> C_A_B". ReactionParser keeps these names verbatim, so the built
// model's species/totals match the slice's out / inp ("tA") tokens exactly.
function complexToken(monomers) {
  const letters = monomers.map(i => String.fromCharCode(64 + i)); // 1→A, 2→B, …
  return letters.length === 1 ? letters[0] : 'C_' + letters.join('_');
}
function sideToSpecies(side) {
  const toks = side.trim().match(/\[[0-9,]+\]/g) || [];
  return toks.map(t => complexToken(t.replace(/[[\]]/g, '').split(',').map(Number))).join(' + ');
}
export function nidToRules(nid) {
  return String(nid).split('|').map(rxn => {
    const parts = rxn.split(/<->|<=>|↔/);
    if (parts.length !== 2) throw new Error(`Cannot parse reaction in nid: ${rxn}`);
    return `${sideToSpecies(parts[0])} <-> ${sideToSpecies(parts[1])}`;
  });
}
function shortNid(nid) {
  const s = String(nid);
  return s.length > 28 ? s.slice(0, 26) + '…' : s;
}

// ── SELECT → emit Reactions and, when exact, a ROP shape reference ─────────
// Publishes the chosen network's rules as config.resolvedDefinition.raw_rules —
// the same shape getReactionsFromNode reads for network-id-definition — so any
// model-builder wired to this node's Reactions port rebuilds from it.
export function selectNetwork(designNodeId, nid, inp, out) {
  const info = nodeRegistry[designNodeId];
  const response = info?.data?.designSearchResponse;
  const selectedKey = designCandidateKey(nid, inp, out);
  const entry = selectedCandidateEntry(response, selectedKey);
  const screen = info?.[DESIGN_SCREEN_LIFECYCLE_KEY]
    ? inspectExecutionLifecycle(info[DESIGN_SCREEN_LIFECYCLE_KEY])
    : null;
  if (!info || !entry || screen?.state !== 'current' || screen.freshness !== 'current') {
    return null;
  }
  let rules;
  try { rules = nidToRules(nid); }
  catch (e) { alert(`Could not convert network: ${e.message}`); return null; }
  info.data = info.data || {};
  const cfg = info.data.config = info.data.config || {};
  const previousSelectedKey = cfg.selectedCandidateKey;
  const previousReference = cfg.ropShapeReference;
  if (previousSelectedKey !== selectedKey) {
    // Retire every dependent request/result before exposing the new selection.
    invalidateConnectedRopShapeArtifacts(designNodeId);
  }
  if (!publishSelectedNetwork(designNodeId, entry, rules)) return null;
  const currentReference = refreshRopShapeReferenceForSelection(
    cfg,
    info.data.designSearchResponse,
  );
  if (didRopShapeReferenceChange(
    previousSelectedKey,
    previousReference,
    cfg.selectedCandidateKey,
    currentReference,
  )) {
    invalidateConnectedRopShapeArtifacts(designNodeId);
  }

  const contentEl = document.getElementById(`${designNodeId}-content`);
  if (contentEl) {
    contentEl.querySelectorAll('.design-net-row').forEach(r => {
      const on = designCandidateKey(r.dataset.nid, r.dataset.inp, r.dataset.out) === cfg.selectedCandidateKey;
      r.classList.toggle('selected', on);
      const badge = r.querySelector('.design-emit-badge');
      if (badge) badge.style.display = on ? '' : 'none';
    });
    const emit = document.getElementById(`${designNodeId}-emit`);
    if (emit) emit.innerHTML = `emitting <strong>${escapeHtml(shortNid(nid))}</strong>`;
  }
  triggerAutoModelBuild(designNodeId);   // rebuild any model-builder already wired to us
  commitWorkspaceSnapshot('design-target-select');
  return rules;
}

// ── The GLUE: emit the network AND spawn a wired tunable station from our port ──
function resetBuildAndTuneButton(btn) {
  if (!btn) return;
  btn.disabled = false;
  btn.textContent = 'Build & tune →';
}

function reportBuildAndTuneFailure(designNodeId, error) {
  const message = error?.message || String(error);
  const contentEl = document.getElementById(`${designNodeId}-content`);
  if (contentEl) {
    const err = document.createElement('div');
    err.className = 'node-error';
    err.textContent = `Build & tune failed: ${message}`;
    contentEl.appendChild(err);
  }
  showToast(`Build & tune failed: ${message}`);
}

function buildAndTuneDeferredTask(plan, { inputSymbol, outputSymbol, btn }) {
  const { modelBuilderNodeId, paramsNodeId, resultNodeId } = plan.patch.metadata;
  return {
    id: `${modelBuilderNodeId}:build-and-tune`,
    delay: 100,
    async run(context) {
      if (!context.isCurrent() || context.signal.aborted) return;
      if (btn) {
        btn.disabled = true;
        btn.textContent = 'Building…';
      }

      await buildModel(modelBuilderNodeId, {
        triggerDownstream: false,
        throwOnFailure: true,
      });
      if (!context.isCurrent() || context.signal.aborted) return;

      const model = getModelForNode(paramsNodeId);
      if (!model) throw new Error('Model build did not produce a model');

      // Atlas tokens must exist in the built model. A mismatch is a naming
      // error, not permission to silently tune a different input or output.
      const xSyms = (model.x_sym || []).map(String);
      const qSyms = (model.q_sym || []).map(String);
      if (!qSyms.includes(inputSymbol)) {
        throw new Error(`input total "${inputSymbol}" not in built model (${qSyms.join(', ')})`);
      }
      if (!xSyms.includes(outputSymbol)) {
        throw new Error(`output species "${outputSymbol}" not in built model (${xSyms.join(', ')})`);
      }
      syncSelectOptions(
        document.getElementById(`${paramsNodeId}-input`),
        qSyms,
        inputSymbol,
      );
      syncSelectOptions(
        document.getElementById(`${paramsNodeId}-output`),
        xSyms,
        outputSymbol,
      );
      triggerConfigUpdate(paramsNodeId, 'placer-params');
      if (!context.isCurrent() || context.signal.aborted) return;

      // The design target is a whole regime program, so Build & Tune realizes
      // that program after populating the optional fine-tune menu.
      await loadPlacerMenu(resultNodeId);
      if (!context.isCurrent() || context.signal.aborted) return;
      await realizePlacerProgram(resultNodeId);
      if (!context.isCurrent() || context.signal.aborted) return;
      if (btn) btn.textContent = 'Built';
    },
  };
}

export function spawnBuildAndTuneGraph(
  designNodeId,
  { inputSymbol, outputSymbol, btn = null } = {},
) {
  if (typeof inputSymbol !== 'string' || !inputSymbol.trim() ||
      typeof outputSymbol !== 'string' || !outputSymbol.trim()) {
    const diagnostic = {
      kind: 'error',
      code: 'invalid-build-and-tune-symbols',
      message: 'Build & Tune requires non-empty input and output symbols.',
    };
    resetBuildAndTuneButton(btn);
    showToast(diagnostic.message);
    return { ok: false, patch: null, diagnostic };
  }

  const plan = planDesignBuildAndTuneWorkflow({
    graph: captureEditorGraphPlanningGraph(),
    nextNodeOrdinal: nodeIdCounter + 1,
    designNodeId,
  });
  if (!plan.ok) {
    resetBuildAndTuneButton(btn);
    showToast(plan.diagnostic.message);
    return plan;
  }

  if (btn) {
    btn.disabled = true;
    btn.textContent = 'Building…';
  }
  try {
    const command = createEditorGraphPatchCommand(plan, {
      deferredTasks: [buildAndTuneDeferredTask(plan, {
        inputSymbol: inputSymbol.trim(),
        outputSymbol: outputSymbol.trim(),
        btn,
      })],
      onDeferredError(error) {
        resetBuildAndTuneButton(btn);
        reportBuildAndTuneFailure(designNodeId, error);
      },
      onEpochCancel() {
        resetBuildAndTuneButton(btn);
      },
    });
    dispatch(command);
    return { ...plan, command };
  } catch (error) {
    resetBuildAndTuneButton(btn);
    reportBuildAndTuneFailure(designNodeId, error);
    const diagnostic = {
      kind: 'error',
      code: error?.code || 'build-and-tune-commit-failed',
      message: error?.message || String(error),
    };
    return { ok: false, patch: null, diagnostic };
  }
}

export function buildAndTune(designNodeId, nid, inp, out, btn) {
  const rules = selectNetwork(designNodeId, nid, inp, out);
  if (!rules) {
    return {
      ok: false,
      patch: null,
      diagnostic: {
        kind: 'error',
        code: 'design-target-selection-failed',
        message: 'Build & Tune requires a current selected Design Target candidate.',
      },
    };
  }
  return spawnBuildAndTuneGraph(designNodeId, {
    inputSymbol: inp,
    outputSymbol: out,
    btn,
  });
}
