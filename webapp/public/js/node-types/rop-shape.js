// Fixed-topology ROP shape optimization workspace nodes.
//
// The config node consumes a pinned, versioned Design Target reference. It
// rebuilds the request from that current upstream artifact every time; a saved
// request is display/history data only and is never trusted as fresh input.

import { escapeHtml, optimizeRopShape } from '../api.js';
import { sameJson, stableJson } from '../stable-json.js';
import { renderRopShapeOptimizationResult } from '../rop-shape-render.js';
import {
  connections,
  ensureNodeData,
  getWorkspaceRuntimeEpoch,
  nodeRegistry,
} from '../state.js';
import { setNodeLoading, setupAutoUpdate } from '../nodes.js';
import { commitWorkspaceSnapshot } from '../workspace.js';
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

export const ROP_SHAPE_REFERENCE_VERSION = 'bne-workspace-rop-shape-reference/v1.0.0';
export const ROP_SHAPE_REQUEST_VERSION = 'bne-rop-shape-optimize-request/v1.0.0';
export const ROP_SHAPE_ENDPOINT = '/api/v1/rop_shape_optimize';

const INTENT_KINDS = new Set([
  'broaden',
  'separate',
  'widen_center',
  'translate_group',
  'linear_witness',
]);
const REQUEST_KEYS = [
  'schema_version',
  'network',
  'expected_network_ir_hash',
  'designability_spec',
  'reference',
  'edit_intent',
  'optimization',
  'work_budget',
  'replay',
];
const SHA256_RE = /^[0-9a-f]{64}$/;
const DECIMAL_RE = /^[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?$/;
const ROP_SHAPE_PREPARE_LIFECYCLE_KEY = '_ropShapePrepareLifecycle';
const ROP_SHAPE_RESULT_LIFECYCLE_KEY = '_ropShapeResultLifecycle';

function invalid(path, message) {
  throw new Error(`Invalid ROP shape ${path}: ${message}`);
}

function isPlainObject(value) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return false;
  const proto = Object.getPrototypeOf(value);
  return proto === Object.prototype || proto === null;
}

function requireObject(value, path) {
  if (!isPlainObject(value)) invalid(path, 'must be an object');
  return value;
}

function requireOnlyKeys(value, allowed, path) {
  const allowedSet = new Set(allowed);
  for (const key of Object.keys(value)) {
    if (!allowedSet.has(key)) invalid(path, `unknown field ${key}`);
  }
}

function requireKeys(value, required, path) {
  for (const key of required) {
    if (!Object.prototype.hasOwnProperty.call(value, key)) {
      invalid(path, `missing field ${key}`);
    }
  }
}

function requireString(value, path) {
  if (typeof value !== 'string' || !value.trim()) invalid(path, 'must be a non-empty string');
  return value.trim();
}

function requireFinite(value, path, { minimum = null, maximum = null } = {}) {
  let number;
  if (typeof value === 'number') {
    number = value;
  } else if (typeof value === 'string' && value.trim() && DECIMAL_RE.test(value.trim())) {
    number = Number(value.trim());
  } else {
    invalid(path, 'must be a finite decimal number');
  }
  if (!Number.isFinite(number)) invalid(path, 'must be finite');
  if (minimum != null && number < minimum) invalid(path, `must be at least ${minimum}`);
  if (maximum != null && number > maximum) invalid(path, `must be at most ${maximum}`);
  return number;
}

function requireInteger(value, path, { minimum = null, maximum = null } = {}) {
  const number = requireFinite(value, path, { minimum, maximum });
  if (!Number.isSafeInteger(number)) invalid(path, 'must be an integer');
  return number;
}

function requireJsonNumber(value, path, options = {}) {
  if (typeof value !== 'number') invalid(path, 'must be a JSON number');
  return requireFinite(value, path, options);
}

function requireJsonInteger(value, path, options = {}) {
  if (typeof value !== 'number') invalid(path, 'must be a JSON integer');
  return requireInteger(value, path, options);
}

function requireSha256(value, path) {
  if (typeof value !== 'string' || !SHA256_RE.test(value)) {
    invalid(path, 'must be a lowercase SHA-256 hex digest');
  }
  return value;
}

function cloneJsonValue(value, path, seen) {
  if (value === null || typeof value === 'string' || typeof value === 'boolean') return value;
  if (typeof value === 'number') {
    if (!Number.isFinite(value)) invalid(path, 'contains a non-finite number');
    return value;
  }
  if (Array.isArray(value)) {
    if (seen.has(value)) invalid(path, 'contains a cycle');
    seen.add(value);
    const cloned = value.map((item, index) => cloneJsonValue(item, `${path}[${index}]`, seen));
    seen.delete(value);
    return cloned;
  }
  if (!isPlainObject(value)) invalid(path, 'contains a non-JSON value');
  if (seen.has(value)) invalid(path, 'contains a cycle');
  seen.add(value);
  const cloned = {};
  for (const key of Object.keys(value)) {
    if (value[key] === undefined) invalid(`${path}.${key}`, 'must not be undefined');
    Object.defineProperty(cloned, key, {
      value: cloneJsonValue(value[key], `${path}.${key}`, seen),
      enumerable: true,
      configurable: true,
      writable: true,
    });
  }
  seen.delete(value);
  return cloned;
}

export function cloneRopShapeJson(value, path = 'payload') {
  return cloneJsonValue(value, path, new WeakSet());
}

function stableRopShapeFingerprint(value) {
  return stableJson(value);
}

function ropShapeLifecycleFor(info, key) {
  if (!info[key]) info[key] = createExecutionLifecycle();
  return info[key];
}

function syncRopShapeLifecycle(info, lifecycle) {
  if (!info || !lifecycle) return;
  info.data = info.data || {};
  info.data.lifecycle = serializeExecutionLifecycle(lifecycle);
}

function invalidateBoundRopShapeLifecycle(info, key, reason) {
  const lifecycle = info?.[key];
  if (!lifecycle) return false;
  const runtime = inspectExecutionLifecycle(lifecycle);
  if (runtime.owner !== info || runtime.workspaceEpoch == null) return false;
  const invalidated = invalidateLifecycle(lifecycle, {
    owner: info,
    workspaceEpoch: runtime.workspaceEpoch,
    reason,
  });
  syncRopShapeLifecycle(info, lifecycle);
  return invalidated;
}

function ropShapeLifecycleContext(nodeId, inputFingerprint) {
  const owner = nodeRegistry[nodeId];
  if (!owner) return null;
  return {
    owner,
    workspaceEpoch: getWorkspaceRuntimeEpoch(),
    inputFingerprint,
    endpoint: ROP_SHAPE_ENDPOINT,
  };
}

function settleRopShapeLoading(nodeId, lifecycle, ticket) {
  if (!ticket || nodeRegistry[nodeId] !== ticket.owner) return;
  const runtime = inspectExecutionLifecycle(lifecycle);
  if (runtime.currentTicket === ticket ||
      (runtime.currentTicket == null && runtime.loading === false)) {
    setNodeLoading(nodeId, false);
  }
}

function retireObsoleteRopShapeTicket(lifecycle, ticket, context, owner) {
  if (context) {
    failLifecycle(lifecycle, ticket, {
      context,
      error: new Error('ROP shape execution no longer owns the current inputs'),
    });
  }
  syncRopShapeLifecycle(owner, lifecycle);
  return null;
}

function ropShapeEvidence(result) {
  const evidence = {};
  for (const key of [
    'certificate_grade',
    'finite_replay_evidence_grade',
    'geometric_evidence_grade',
  ]) {
    if (typeof result?.[key] === 'string' && result[key]) evidence[key] = result[key];
  }
  return Object.keys(evidence).length ? evidence : null;
}

export function inspectRopShapeLifecycle(nodeId) {
  const info = nodeRegistry[nodeId];
  if (!info) return null;
  const key = info.type === 'rop-shape-edit-config'
    ? ROP_SHAPE_PREPARE_LIFECYCLE_KEY
    : ROP_SHAPE_RESULT_LIFECYCLE_KEY;
  return info[key] ? inspectExecutionLifecycle(info[key]) : null;
}

function witnessLimit(options, path) {
  if (options?.witnessCount == null) return null;
  const count = requireInteger(options.witnessCount, `${path}.witnessCount`, { minimum: 1 });
  return count - 1;
}

export function parseRopShapeStepList(raw, {
  path = 'step list',
  exactLength = null,
  minimumLength = 1,
  witnessCount = null,
} = {}) {
  let values;
  if (typeof raw === 'string') {
    if (!raw.trim()) invalid(path, 'must not be empty');
    const tokens = raw.split(',');
    if (tokens.some(token => !token.trim() || !/^\d+$/.test(token.trim()))) {
      invalid(path, 'must be comma-separated non-negative integers');
    }
    values = tokens.map(token => Number(token.trim()));
  } else if (Array.isArray(raw)) {
    if (raw.some(value => typeof value !== 'number')) {
      invalid(path, 'array entries must be JSON integers');
    }
    values = raw.slice();
  } else {
    invalid(path, 'must be an array or comma-separated string');
  }

  const maxStep = witnessCount == null ? null : witnessLimit({ witnessCount }, path);
  const parsed = values.map((value, index) => requireInteger(value, `${path}[${index}]`, {
    minimum: 0,
    maximum: maxStep,
  }));
  if (exactLength != null && parsed.length !== exactLength) {
    invalid(path, `must contain exactly ${exactLength} steps`);
  }
  if (parsed.length < minimumLength) invalid(path, `must contain at least ${minimumLength} steps`);
  if (new Set(parsed).size !== parsed.length) invalid(path, 'must not contain duplicate steps');
  return parsed;
}

function requireAscendingPair(steps, path) {
  if (steps[0] >= steps[1]) invalid(path, 'must be in left-to-right order');
  return steps;
}

function requireDisjoint(left, right, path) {
  const leftSet = new Set(left);
  if (right.some(step => leftSet.has(step))) invalid(path, 'step groups must be disjoint');
}

export function buildBroadenEditIntent({
  id,
  leftSpanSteps,
  rightSpanSteps,
  sharedMagnitude = true,
  witnessCount = null,
} = {}) {
  if (sharedMagnitude !== true) invalid('edit_intent.shared_magnitude', 'must be literal true');
  const left = requireAscendingPair(parseRopShapeStepList(leftSpanSteps, {
    path: 'edit_intent.left_span_steps', exactLength: 2, witnessCount,
  }), 'edit_intent.left_span_steps');
  const right = requireAscendingPair(parseRopShapeStepList(rightSpanSteps, {
    path: 'edit_intent.right_span_steps', exactLength: 2, witnessCount,
  }), 'edit_intent.right_span_steps');
  requireDisjoint(left, right, 'edit_intent broaden spans');
  return {
    id: requireString(id, 'edit_intent.id'),
    kind: 'broaden',
    left_span_steps: left,
    right_span_steps: right,
    shared_magnitude: true,
  };
}

export function buildSeparateEditIntent({
  id,
  steps,
  preserveMidpointTolerance,
  witnessCount = null,
} = {}) {
  const pair = requireAscendingPair(parseRopShapeStepList(steps, {
    path: 'edit_intent.steps', exactLength: 2, witnessCount,
  }), 'edit_intent.steps');
  return {
    id: requireString(id, 'edit_intent.id'),
    kind: 'separate',
    steps: pair,
    preserve_midpoint_tolerance_log10: requireFinite(
      preserveMidpointTolerance,
      'edit_intent.preserve_midpoint_tolerance_log10',
      { minimum: 0 },
    ),
  };
}

export function buildWidenCenterEditIntent({
  id,
  steps,
  anchorStep,
  anchorTolerance,
  witnessCount = null,
} = {}) {
  const pair = requireAscendingPair(parseRopShapeStepList(steps, {
    path: 'edit_intent.steps', exactLength: 2, witnessCount,
  }), 'edit_intent.steps');
  const maxStep = witnessCount == null ? null : witnessLimit({ witnessCount }, 'edit_intent.anchor_step');
  const anchor = requireInteger(anchorStep, 'edit_intent.anchor_step', { minimum: 0, maximum: maxStep });
  if (pair.includes(anchor)) invalid('edit_intent.anchor_step', 'must differ from both gap endpoints');
  return {
    id: requireString(id, 'edit_intent.id'),
    kind: 'widen_center',
    steps: pair,
    anchor_step: anchor,
    anchor_tolerance_log10: requireFinite(
      anchorTolerance,
      'edit_intent.anchor_tolerance_log10',
      { minimum: 0 },
    ),
  };
}

export function buildTranslateGroupEditIntent({
  id,
  groupSteps,
  preserveSteps,
  preserveTolerance,
  sense,
  sharedShift = true,
  witnessCount = null,
} = {}) {
  if (sharedShift !== true) invalid('edit_intent.shared_shift', 'must be literal true');
  const group = parseRopShapeStepList(groupSteps, {
    path: 'edit_intent.group_steps', minimumLength: 1, witnessCount,
  });
  const preserve = parseRopShapeStepList(preserveSteps, {
    path: 'edit_intent.preserve_steps', minimumLength: 1, witnessCount,
  });
  requireDisjoint(group, preserve, 'edit_intent translate groups');
  if (sense !== 'positive' && sense !== 'negative') {
    invalid('edit_intent.sense', 'must be positive or negative');
  }
  return {
    id: requireString(id, 'edit_intent.id'),
    kind: 'translate_group',
    group_steps: group,
    preserve_steps: preserve,
    preserve_tolerance_log10: requireFinite(
      preserveTolerance,
      'edit_intent.preserve_tolerance_log10',
      { minimum: 0 },
    ),
    sense,
    shared_shift: true,
  };
}

function buildLinearTerms(raw, path, witnessCount) {
  if (!Array.isArray(raw) || raw.length < 1) invalid(path, 'must be a non-empty array');
  const seen = new Set();
  return raw.map((term, index) => {
    const termPath = `${path}[${index}]`;
    const cloned = requireObject(cloneRopShapeJson(term, termPath), termPath);
    requireOnlyKeys(cloned, ['step', 'coefficient'], termPath);
    requireKeys(cloned, ['step', 'coefficient'], termPath);
    const maxStep = witnessCount == null ? null : witnessLimit({ witnessCount }, termPath);
    if (typeof cloned.step !== 'number') invalid(`${termPath}.step`, 'must be a JSON integer');
    const step = requireInteger(cloned.step, `${termPath}.step`, { minimum: 0, maximum: maxStep });
    if (seen.has(step)) invalid(path, 'must not repeat a step');
    seen.add(step);
    if (typeof cloned.coefficient !== 'number') invalid(`${termPath}.coefficient`, 'must be a JSON number');
    const coefficient = requireFinite(cloned.coefficient, `${termPath}.coefficient`);
    if (coefficient === 0) invalid(`${termPath}.coefficient`, 'must be nonzero');
    return { step, coefficient };
  });
}

export function buildLinearWitnessEditIntent({
  id,
  definition,
  witnessCount = null,
} = {}) {
  let parsed = definition;
  if (typeof definition === 'string') {
    try {
      parsed = JSON.parse(definition);
    } catch (error) {
      invalid('edit_intent linear JSON', error.message);
    }
  }
  const source = requireObject(cloneRopShapeJson(parsed, 'edit_intent linear JSON'), 'edit_intent linear JSON');
  requireOnlyKeys(source, ['id', 'kind', 'constraints', 'objective'], 'edit_intent linear JSON');
  requireKeys(source, ['constraints', 'objective'], 'edit_intent linear JSON');
  if (source.kind != null && source.kind !== 'linear_witness') {
    invalid('edit_intent.kind', 'must be linear_witness');
  }
  const resolvedId = requireString(id ?? source.id, 'edit_intent.id');
  if (source.id != null && requireString(source.id, 'edit_intent linear JSON.id') !== resolvedId) {
    invalid('edit_intent linear JSON.id', 'must match the Intent ID field');
  }

  const objectiveRaw = requireObject(source.objective, 'edit_intent.objective');
  requireOnlyKeys(objectiveRaw, ['id', 'sense', 'terms'], 'edit_intent.objective');
  requireKeys(objectiveRaw, ['id', 'sense', 'terms'], 'edit_intent.objective');
  if (objectiveRaw.sense !== 'maximize' && objectiveRaw.sense !== 'minimize') {
    invalid('edit_intent.objective.sense', 'must be maximize or minimize');
  }
  const objective = {
    id: requireString(objectiveRaw.id, 'edit_intent.objective.id'),
    sense: objectiveRaw.sense,
    terms: buildLinearTerms(objectiveRaw.terms, 'edit_intent.objective.terms', witnessCount),
  };

  if (!Array.isArray(source.constraints)) invalid('edit_intent.constraints', 'must be an array');
  const constraintIds = new Set();
  const constraints = source.constraints.map((raw, index) => {
    const path = `edit_intent.constraints[${index}]`;
    const constraint = requireObject(raw, path);
    requireOnlyKeys(constraint, ['id', 'terms', 'operator', 'rhs_log10', 'hard'], path);
    requireKeys(constraint, ['id', 'terms', 'operator', 'rhs_log10', 'hard'], path);
    const constraintId = requireString(constraint.id, `${path}.id`);
    if (constraintIds.has(constraintId)) invalid('edit_intent.constraints', 'constraint IDs must be unique');
    constraintIds.add(constraintId);
    if (!['<=', '>=', '='].includes(constraint.operator)) {
      invalid(`${path}.operator`, 'must be <=, >=, or =');
    }
    if (constraint.hard !== true) invalid(`${path}.hard`, 'must be literal true');
    if (typeof constraint.rhs_log10 !== 'number') invalid(`${path}.rhs_log10`, 'must be a JSON number');
    return {
      id: constraintId,
      terms: buildLinearTerms(constraint.terms, `${path}.terms`, witnessCount),
      operator: constraint.operator,
      rhs_log10: requireFinite(constraint.rhs_log10, `${path}.rhs_log10`),
      hard: true,
    };
  });

  return { id: resolvedId, kind: 'linear_witness', constraints, objective };
}

export function buildRopShapeEditIntent(kind, config, { witnessCount = null } = {}) {
  if (!INTENT_KINDS.has(kind)) invalid('edit_intent.kind', 'is unsupported');
  const values = requireObject(config, 'edit config');
  const id = values.intentId;
  if (kind === 'broaden') {
    return buildBroadenEditIntent({
      id,
      leftSpanSteps: values.leftSpan,
      rightSpanSteps: values.rightSpan,
      sharedMagnitude: values.shared,
      witnessCount,
    });
  }
  if (kind === 'separate') {
    return buildSeparateEditIntent({
      id,
      steps: values.steps,
      preserveMidpointTolerance: values.midpointTolerance,
      witnessCount,
    });
  }
  if (kind === 'widen_center') {
    return buildWidenCenterEditIntent({
      id,
      steps: values.steps,
      anchorStep: values.anchorStep,
      anchorTolerance: values.anchorTolerance,
      witnessCount,
    });
  }
  if (kind === 'translate_group') {
    return buildTranslateGroupEditIntent({
      id,
      groupSteps: values.group,
      preserveSteps: values.preserve,
      preserveTolerance: values.anchorTolerance,
      sense: values.sense,
      sharedShift: values.shared,
      witnessCount,
    });
  }
  return buildLinearWitnessEditIntent({
    id,
    definition: values.linearIntentJson,
    witnessCount,
  });
}

export function validateRopShapeEditIntent(raw, { witnessCount = null } = {}) {
  const intent = requireObject(cloneRopShapeJson(raw, 'edit_intent'), 'edit_intent');
  const kind = intent.kind;
  if (kind === 'broaden') {
    requireOnlyKeys(intent, ['id', 'kind', 'left_span_steps', 'right_span_steps', 'shared_magnitude'], 'edit_intent');
    requireKeys(intent, ['id', 'kind', 'left_span_steps', 'right_span_steps', 'shared_magnitude'], 'edit_intent');
    return buildBroadenEditIntent({
      id: intent.id,
      leftSpanSteps: intent.left_span_steps,
      rightSpanSteps: intent.right_span_steps,
      sharedMagnitude: intent.shared_magnitude,
      witnessCount,
    });
  }
  if (kind === 'separate') {
    requireOnlyKeys(intent, ['id', 'kind', 'steps', 'preserve_midpoint_tolerance_log10'], 'edit_intent');
    requireKeys(intent, ['id', 'kind', 'steps', 'preserve_midpoint_tolerance_log10'], 'edit_intent');
    requireJsonNumber(intent.preserve_midpoint_tolerance_log10,
      'edit_intent.preserve_midpoint_tolerance_log10', { minimum: 0 });
    return buildSeparateEditIntent({
      id: intent.id,
      steps: intent.steps,
      preserveMidpointTolerance: intent.preserve_midpoint_tolerance_log10,
      witnessCount,
    });
  }
  if (kind === 'widen_center') {
    requireOnlyKeys(intent, ['id', 'kind', 'steps', 'anchor_step', 'anchor_tolerance_log10'], 'edit_intent');
    requireKeys(intent, ['id', 'kind', 'steps', 'anchor_step', 'anchor_tolerance_log10'], 'edit_intent');
    requireJsonInteger(intent.anchor_step, 'edit_intent.anchor_step', { minimum: 0 });
    requireJsonNumber(intent.anchor_tolerance_log10, 'edit_intent.anchor_tolerance_log10', { minimum: 0 });
    return buildWidenCenterEditIntent({
      id: intent.id,
      steps: intent.steps,
      anchorStep: intent.anchor_step,
      anchorTolerance: intent.anchor_tolerance_log10,
      witnessCount,
    });
  }
  if (kind === 'translate_group') {
    requireOnlyKeys(intent, [
      'id', 'kind', 'group_steps', 'preserve_steps', 'preserve_tolerance_log10', 'sense', 'shared_shift',
    ], 'edit_intent');
    requireKeys(intent, [
      'id', 'kind', 'group_steps', 'preserve_steps', 'preserve_tolerance_log10', 'sense', 'shared_shift',
    ], 'edit_intent');
    requireJsonNumber(intent.preserve_tolerance_log10,
      'edit_intent.preserve_tolerance_log10', { minimum: 0 });
    return buildTranslateGroupEditIntent({
      id: intent.id,
      groupSteps: intent.group_steps,
      preserveSteps: intent.preserve_steps,
      preserveTolerance: intent.preserve_tolerance_log10,
      sense: intent.sense,
      sharedShift: intent.shared_shift,
      witnessCount,
    });
  }
  if (kind === 'linear_witness') {
    return buildLinearWitnessEditIntent({
      id: intent.id,
      definition: intent,
      witnessCount,
    });
  }
  invalid('edit_intent.kind', 'must be broaden, separate, widen_center, translate_group, or linear_witness');
}

function validateReference(reference, expectedHash) {
  requireObject(reference, 'request.reference');
  requireOnlyKeys(reference, [
    'reference_hash', 'artifact_ref', 'network_ir_hash', 'operating_points_log10',
    'kd', 'totals', 'path_identity', 'cell_id',
  ], 'request.reference');
  requireKeys(reference, [
    'reference_hash', 'network_ir_hash', 'operating_points_log10', 'kd', 'totals',
  ], 'request.reference');
  requireSha256(reference.reference_hash, 'request.reference.reference_hash');
  const networkHash = requireSha256(reference.network_ir_hash, 'request.reference.network_ir_hash');
  if (networkHash !== expectedHash) invalid('request.reference.network_ir_hash', 'does not match expected_network_ir_hash');
  if (!Array.isArray(reference.operating_points_log10) || reference.operating_points_log10.length < 2) {
    invalid('request.reference.operating_points_log10', 'must contain at least two points');
  }
  reference.operating_points_log10.forEach((value, index) => {
    requireJsonNumber(value, `request.reference.operating_points_log10[${index}]`);
  });
  if (!Array.isArray(reference.kd) || reference.kd.length < 1) invalid('request.reference.kd', 'must not be empty');
  reference.kd.forEach((value, index) => requireJsonNumber(
    value, `request.reference.kd[${index}]`, { minimum: Number.MIN_VALUE }));
  requireObject(reference.totals, 'request.reference.totals');
  if (!Object.keys(reference.totals).length) invalid('request.reference.totals', 'must not be empty');
  for (const [symbol, value] of Object.entries(reference.totals)) {
    requireString(symbol, 'request.reference.totals key');
    requireJsonNumber(value, `request.reference.totals.${symbol}`, { minimum: Number.MIN_VALUE });
  }
  if (reference.artifact_ref != null) requireString(reference.artifact_ref, 'request.reference.artifact_ref');
  if (reference.path_identity != null) requireString(reference.path_identity, 'request.reference.path_identity');
  if (reference.cell_id != null) requireString(reference.cell_id, 'request.reference.cell_id');
}

function validateOptimization(optimization) {
  requireObject(optimization, 'request.optimization');
  requireOnlyKeys(optimization, ['minimum_parameter_margin', 'effect_tolerance'], 'request.optimization');
  requireKeys(optimization, ['minimum_parameter_margin', 'effect_tolerance'], 'request.optimization');
  requireJsonNumber(optimization.minimum_parameter_margin,
    'request.optimization.minimum_parameter_margin', { minimum: 0 });
  requireJsonNumber(optimization.effect_tolerance, 'request.optimization.effect_tolerance', { minimum: 0 });
}

function validateWorkBudget(budget) {
  requireObject(budget, 'request.work_budget');
  requireOnlyKeys(budget, ['max_paths', 'max_cells', 'max_replays', 'require_exhaustive'], 'request.work_budget');
  requireKeys(budget, ['max_paths', 'max_cells', 'max_replays', 'require_exhaustive'], 'request.work_budget');
  requireJsonInteger(budget.max_paths, 'request.work_budget.max_paths', { minimum: 1, maximum: 2000 });
  requireJsonInteger(budget.max_cells, 'request.work_budget.max_cells', { minimum: 1, maximum: 256 });
  requireJsonInteger(budget.max_replays, 'request.work_budget.max_replays', { minimum: 1, maximum: 2 });
  if (typeof budget.require_exhaustive !== 'boolean') {
    invalid('request.work_budget.require_exhaustive', 'must be boolean');
  }
}

function validateReplay(replay) {
  requireObject(replay, 'request.replay');
  requireOnlyKeys(replay, [
    'input_window_log10', 'sample_points', 'require_complete', 'store_curve', 'metrics',
  ], 'request.replay');
  requireKeys(replay, [
    'input_window_log10', 'sample_points', 'require_complete', 'store_curve', 'metrics',
  ], 'request.replay');
  const window = replay.input_window_log10;
  if (!Array.isArray(window) || window.length !== 2) invalid('request.replay.input_window_log10', 'must contain two bounds');
  const lo = requireJsonNumber(window[0], 'request.replay.input_window_log10[0]', { minimum: -20, maximum: 20 });
  const hi = requireJsonNumber(window[1], 'request.replay.input_window_log10[1]', { minimum: -20, maximum: 20 });
  if (lo >= hi) invalid('request.replay.input_window_log10', 'must be strictly increasing');
  requireJsonInteger(replay.sample_points, 'request.replay.sample_points', { minimum: 11, maximum: 1000 });
  if (replay.require_complete !== true) invalid('request.replay.require_complete', 'must be literal true');
  if (replay.store_curve !== true) invalid('request.replay.store_curve', 'must be literal true');
  if (!Array.isArray(replay.metrics) || replay.metrics.length !== 1) {
    invalid('request.replay.metrics', 'must contain exactly one two_peak metric');
  }
  const metric = requireObject(replay.metrics[0], 'request.replay.metrics[0]');
  requireOnlyKeys(metric, ['kind', 'min_prominence_log10'], 'request.replay.metrics[0]');
  requireKeys(metric, ['kind', 'min_prominence_log10'], 'request.replay.metrics[0]');
  if (metric.kind !== 'two_peak') invalid('request.replay.metrics[0].kind', 'must be two_peak');
  requireJsonNumber(metric.min_prominence_log10,
    'request.replay.metrics[0].min_prominence_log10', { minimum: 0 });
}

function validateRequest(raw, { allowNullIntent = false } = {}) {
  const request = requireObject(cloneRopShapeJson(raw, 'request'), 'request');
  requireOnlyKeys(request, REQUEST_KEYS, 'request');
  requireKeys(request, REQUEST_KEYS, 'request');
  if (request.schema_version !== ROP_SHAPE_REQUEST_VERSION) {
    invalid('request.schema_version', `must be ${ROP_SHAPE_REQUEST_VERSION}`);
  }
  requireObject(request.network, 'request.network');
  requireObject(request.designability_spec, 'request.designability_spec');
  if (request.network.ir_schema_version !== 'bne-ir/v1.0.0') {
    invalid('request.network.ir_schema_version', 'must be bne-ir/v1.0.0');
  }
  if (request.designability_spec.schema_version !== 'bne-designability/v1.0.0') {
    invalid('request.designability_spec.schema_version', 'must be bne-designability/v1.0.0');
  }
  const handoffRobustness = request.designability_spec.constraints?.robustness;
  if (isPlainObject(handoffRobustness) &&
      Object.prototype.hasOwnProperty.call(handoffRobustness, 'min_chebyshev_radius')) {
    invalid(
      'request.designability_spec.constraints.robustness.min_chebyshev_radius',
      'must be projected into request.optimization.minimum_parameter_margin',
    );
  }
  const expectedHash = requireSha256(request.expected_network_ir_hash, 'request.expected_network_ir_hash');
  validateReference(request.reference, expectedHash);
  if (!Array.isArray(request.network.reactions) ||
      request.reference.kd.length !== request.network.reactions.length) {
    invalid('request.reference.kd', 'length must match request.network.reactions');
  }
  const behavior = request.designability_spec.target?.behavior_spec;
  if (!isPlainObject(behavior) || !Array.isArray(behavior.program)) {
    invalid('request.designability_spec.target.behavior_spec.program', 'must be an array');
  }
  if (request.reference.operating_points_log10.length !== behavior.program.length) {
    invalid('request.reference.operating_points_log10',
      'length must match the DesignabilitySpec behavior program');
  }
  validateOptimization(request.optimization);
  validateWorkBudget(request.work_budget);
  validateReplay(request.replay);
  const witnessCount = request.reference.operating_points_log10.length;
  if (allowNullIntent) {
    if (request.edit_intent !== null) invalid('request.edit_intent', 'handoff template must leave it null');
  } else {
    request.edit_intent = validateRopShapeEditIntent(request.edit_intent, { witnessCount });
  }
  return request;
}

export function validateCanonicalRopShapeRequest(raw) {
  return validateRequest(raw, { allowNullIntent: false });
}

export function admitRopShapeResultForRequest(rawResult, rawRequest) {
  const request = validateCanonicalRopShapeRequest(rawRequest);
  const result = requireObject(cloneRopShapeJson(rawResult, 'optimization result'), 'optimization result');
  if (!sameJson(result.normalized_request, request)) {
    invalid('optimization result.normalized_request', 'does not match the request executed by this node');
  }
  return result;
}

export function validatePinnedRopShapeReferenceArtifact(raw) {
  const artifact = requireObject(cloneRopShapeJson(raw, 'reference artifact'), 'reference artifact');
  requireOnlyKeys(artifact, [
    'schema_version', 'selected_candidate_key', 'fixed_topology_reference',
    'optimization_handoff_template', 'evidence_scope',
  ], 'reference artifact');
  requireKeys(artifact, [
    'schema_version', 'selected_candidate_key', 'fixed_topology_reference',
    'optimization_handoff_template',
  ], 'reference artifact');
  if (artifact.schema_version !== ROP_SHAPE_REFERENCE_VERSION) {
    invalid('reference artifact.schema_version', `must be ${ROP_SHAPE_REFERENCE_VERSION}`);
  }
  requireString(artifact.selected_candidate_key, 'reference artifact.selected_candidate_key');
  if (artifact.evidence_scope != null) requireString(artifact.evidence_scope, 'reference artifact.evidence_scope');

  const fixed = requireObject(artifact.fixed_topology_reference, 'reference artifact.fixed_topology_reference');
  requireOnlyKeys(fixed, [
    'network', 'network_ir_hash', 'network_canonical_code', 'input', 'output',
    'reference', 'evidence_scope',
  ], 'reference artifact.fixed_topology_reference');
  requireKeys(fixed, ['network', 'network_ir_hash', 'input', 'output', 'reference', 'evidence_scope'],
    'reference artifact.fixed_topology_reference');
  requireObject(fixed.network, 'reference artifact.fixed_topology_reference.network');
  const fixedHash = requireSha256(fixed.network_ir_hash, 'reference artifact.fixed_topology_reference.network_ir_hash');
  requireString(fixed.input, 'reference artifact.fixed_topology_reference.input');
  requireString(fixed.output, 'reference artifact.fixed_topology_reference.output');
  requireString(fixed.evidence_scope, 'reference artifact.fixed_topology_reference.evidence_scope');
  const canonicalCode = requireString(fixed.network_canonical_code,
    'reference artifact.fixed_topology_reference.network_canonical_code');
  if (artifact.selected_candidate_key !== `${canonicalCode}::${fixed.input}::${fixed.output}`) {
    invalid('reference artifact.selected_candidate_key', 'does not match the pinned network/input/output identity');
  }

  const handoff = requireObject(artifact.optimization_handoff_template,
    'reference artifact.optimization_handoff_template');
  requireOnlyKeys(handoff, ['endpoint', 'method', 'body_template', 'required_fill'],
    'reference artifact.optimization_handoff_template');
  requireKeys(handoff, ['endpoint', 'method', 'body_template', 'required_fill'],
    'reference artifact.optimization_handoff_template');
  if (handoff.endpoint !== ROP_SHAPE_ENDPOINT) invalid('handoff.endpoint', `must be ${ROP_SHAPE_ENDPOINT}`);
  if (handoff.method !== 'POST') invalid('handoff.method', 'must be POST');
  if (!Array.isArray(handoff.required_fill) || handoff.required_fill.length !== 1 ||
      handoff.required_fill[0] !== 'edit_intent') {
    invalid('handoff.required_fill', 'must be exactly ["edit_intent"]');
  }
  const template = validateRequest(handoff.body_template, { allowNullIntent: true });
  requireString(template.reference.path_identity, 'handoff.body_template.reference.path_identity');
  requireString(template.reference.cell_id, 'handoff.body_template.reference.cell_id');
  if (template.expected_network_ir_hash !== fixedHash) {
    invalid('handoff.body_template.expected_network_ir_hash', 'does not match fixed_topology_reference');
  }
  if (!sameJson(template.network, fixed.network)) {
    invalid('handoff.body_template.network', 'does not match fixed_topology_reference.network');
  }
  if (!sameJson(template.reference, fixed.reference)) {
    invalid('handoff.body_template.reference', 'does not match fixed_topology_reference.reference');
  }
  return artifact;
}

export function buildCanonicalRopShapeRequestFromHandoff(
  rawArtifact,
  rawIntent,
  { optimization = {}, workBudget = {}, replay = {} } = {},
) {
  const artifact = validatePinnedRopShapeReferenceArtifact(rawArtifact);
  const template = artifact.optimization_handoff_template.body_template;
  const witnessCount = template.reference.operating_points_log10.length;
  const request = cloneRopShapeJson(template, 'request template');
  request.edit_intent = validateRopShapeEditIntent(rawIntent, { witnessCount });

  const optimizationOverrides = requireObject(optimization, 'optimization overrides');
  requireOnlyKeys(optimizationOverrides, ['minimumParameterMargin', 'effectTolerance'], 'optimization overrides');
  const templateMinimumParameterMargin = requireFinite(
    template.optimization.minimum_parameter_margin,
    'request template.optimization.minimum_parameter_margin',
    { minimum: 0 },
  );
  const requestedMinimumParameterMargin = requireFinite(
    optimizationOverrides.minimumParameterMargin ?? templateMinimumParameterMargin,
    'request.optimization.minimum_parameter_margin',
    { minimum: 0 },
  );
  if (requestedMinimumParameterMargin < templateMinimumParameterMargin) {
    invalid(
      'request.optimization.minimum_parameter_margin',
      `must be at least the Design Screen floor ${templateMinimumParameterMargin}`,
    );
  }
  request.optimization = {
    // The handoff floor records what Design Screen already required of the
    // selected reference. A workspace override may strengthen that floor but
    // must never silently weaken the user's upstream requirement.
    minimum_parameter_margin: requestedMinimumParameterMargin,
    effect_tolerance: requireFinite(
      optimizationOverrides.effectTolerance ?? template.optimization.effect_tolerance,
      'request.optimization.effect_tolerance',
      { minimum: 0 },
    ),
  };

  const budgetOverrides = requireObject(workBudget, 'work budget overrides');
  requireOnlyKeys(budgetOverrides, [
    'maxPaths', 'maxCells', 'maxReplays', 'requireExhaustive',
  ], 'work budget overrides');
  request.work_budget = {
    max_paths: requireInteger(budgetOverrides.maxPaths ?? template.work_budget.max_paths,
      'request.work_budget.max_paths', { minimum: 1, maximum: 2000 }),
    max_cells: requireInteger(budgetOverrides.maxCells ?? template.work_budget.max_cells,
      'request.work_budget.max_cells', { minimum: 1, maximum: 256 }),
    max_replays: requireInteger(budgetOverrides.maxReplays ?? template.work_budget.max_replays,
      'request.work_budget.max_replays', { minimum: 1, maximum: 2 }),
    require_exhaustive: budgetOverrides.requireExhaustive ?? template.work_budget.require_exhaustive,
  };
  if (typeof request.work_budget.require_exhaustive !== 'boolean') {
    invalid('request.work_budget.require_exhaustive', 'must be boolean');
  }

  const replayOverrides = requireObject(replay, 'replay overrides');
  requireOnlyKeys(replayOverrides, ['samplePoints', 'minProminence'], 'replay overrides');
  request.replay = cloneRopShapeJson(template.replay, 'request.replay');
  request.replay.sample_points = requireInteger(
    replayOverrides.samplePoints ?? template.replay.sample_points,
    'request.replay.sample_points',
    { minimum: 11, maximum: 1000 },
  );
  request.replay.metrics = [{
    kind: 'two_peak',
    min_prominence_log10: requireFinite(
      replayOverrides.minProminence ?? template.replay.metrics[0].min_prominence_log10,
      'request.replay.metrics[0].min_prominence_log10',
      { minimum: 0 },
    ),
  }];
  return validateCanonicalRopShapeRequest(request);
}

export const buildRopShapeRequestFromHandoff = buildCanonicalRopShapeRequestFromHandoff;

function fieldValue(nodeId, suffix) {
  const element = document.getElementById(`${nodeId}${suffix}`);
  if (!element) invalid(`editor field ${suffix}`, 'is unavailable');
  return element.value;
}

function fieldChecked(nodeId, suffix) {
  const element = document.getElementById(`${nodeId}${suffix}`);
  if (!element) invalid(`editor field ${suffix}`, 'is unavailable');
  return element.checked === true;
}

function readEditorConfig(nodeId) {
  return {
    kind: fieldValue(nodeId, '-rop-shape-kind'),
    intentId: fieldValue(nodeId, '-intent-id'),
    leftSpan: fieldValue(nodeId, '-left-span'),
    rightSpan: fieldValue(nodeId, '-right-span'),
    steps: fieldValue(nodeId, '-steps'),
    group: fieldValue(nodeId, '-group'),
    preserve: fieldValue(nodeId, '-preserve'),
    anchorStep: fieldValue(nodeId, '-anchor-step'),
    anchorTolerance: fieldValue(nodeId, '-anchor-tolerance'),
    midpointTolerance: fieldValue(nodeId, '-midpoint-tolerance'),
    effectTolerance: fieldValue(nodeId, '-effect-tolerance'),
    minimumParameterMargin: fieldValue(nodeId, '-minimum-parameter-margin'),
    sense: fieldValue(nodeId, '-sense'),
    shared: fieldChecked(nodeId, '-shared'),
    maxPaths: fieldValue(nodeId, '-max-paths'),
    maxCells: fieldValue(nodeId, '-max-cells'),
    maxReplays: fieldValue(nodeId, '-max-replays'),
    requireExhaustive: fieldChecked(nodeId, '-require-exhaustive'),
    replaySamplePoints: fieldValue(nodeId, '-replay-sample-points'),
    replayMinProminence: fieldValue(nodeId, '-replay-min-prominence'),
    linearIntentJson: fieldValue(nodeId, '-linear-intent-json'),
  };
}

function setConfigStatus(nodeId, html) {
  const status = document.getElementById(`${nodeId}-rop-shape-status`);
  if (status) status.innerHTML = html;
}

function currentPinnedReference(nodeId) {
  const connection = connections.find(conn =>
    conn.toNode === nodeId && conn.toPort === 'rop-shape-reference');
  if (!connection) invalid('config input', 'connect a Design Target ROP Shape Reference');
  const source = nodeRegistry[connection.fromNode];
  if (!source) invalid('config input', 'upstream node is missing');
  if (source.type !== 'design-target') invalid('config input', 'must come from a Design Target node');
  const selectionLifecycle = source.data?.selectionLifecycle;
  if (selectionLifecycle?.state !== 'current' || selectionLifecycle?.freshness !== 'current') {
    invalid('config input', 'upstream Design Target selection is historical or invalidated; rerun and select it again');
  }
  const sourceConfig = source.data?.config;
  const selectedKey = requireString(sourceConfig?.selectedCandidateKey,
    'upstream config.selectedCandidateKey');
  const artifact = validatePinnedRopShapeReferenceArtifact(sourceConfig?.ropShapeReference);
  if (artifact.selected_candidate_key !== selectedKey) {
    invalid('reference artifact.selected_candidate_key', 'does not match the current upstream selection');
  }
  return artifact;
}

function ropShapeRequestFingerprint(request) {
  return stableRopShapeFingerprint({ endpoint: ROP_SHAPE_ENDPOINT, request });
}

function rawRopShapeEditorFingerprint(nodeId) {
  const suffixes = [
    '-rop-shape-kind', '-intent-id', '-left-span', '-right-span', '-steps', '-group',
    '-preserve', '-anchor-step', '-anchor-tolerance', '-midpoint-tolerance',
    '-effect-tolerance', '-minimum-parameter-margin', '-sense', '-shared', '-max-paths',
    '-max-cells', '-max-replays', '-require-exhaustive', '-replay-sample-points',
    '-replay-min-prominence', '-linear-intent-json',
  ];
  const fields = {};
  for (const suffix of suffixes) {
    const element = document.getElementById(`${nodeId}${suffix}`);
    fields[suffix] = element?.type === 'checkbox'
      ? element.checked === true
      : (element?.value ?? null);
  }
  const referenceConnection = connections.find(connection =>
    connection.toNode === nodeId && connection.toPort === 'rop-shape-reference');
  return stableRopShapeFingerprint({
    fields,
    referenceSource: referenceConnection?.fromNode ?? null,
    selectedCandidateKey: referenceConnection
      ? nodeRegistry[referenceConnection.fromNode]?.data?.config?.selectedCandidateKey ?? null
      : null,
  });
}

function buildCurrentRopShapeRequest(nodeId) {
  const config = readEditorConfig(nodeId);
  const artifact = currentPinnedReference(nodeId);
  const witnessCount = artifact.fixed_topology_reference.reference.operating_points_log10.length;
  const intent = buildRopShapeEditIntent(config.kind, config, { witnessCount });
  const request = buildCanonicalRopShapeRequestFromHandoff(artifact, intent, {
    optimization: {
      minimumParameterMargin: config.minimumParameterMargin,
      effectTolerance: config.effectTolerance,
    },
    workBudget: {
      maxPaths: config.maxPaths,
      maxCells: config.maxCells,
      maxReplays: config.maxReplays,
      requireExhaustive: config.requireExhaustive,
    },
    replay: {
      samplePoints: config.replaySamplePoints,
      minProminence: config.replayMinProminence,
    },
  });
  return { request, intent, witnessCount };
}

function currentRopShapePrepareContext(nodeId) {
  if (!nodeRegistry[nodeId]) return null;
  let inputFingerprint;
  try {
    inputFingerprint = ropShapeRequestFingerprint(buildCurrentRopShapeRequest(nodeId).request);
  } catch {
    inputFingerprint = stableRopShapeFingerprint({
      invalid: true,
      attempt: rawRopShapeEditorFingerprint(nodeId),
    });
  }
  return ropShapeLifecycleContext(nodeId, inputFingerprint);
}

export function updateRopShapeIntentVisibility(nodeId) {
  const kind = document.getElementById(`${nodeId}-rop-shape-kind`)?.value;
  const rows = {
    broaden: ['left-span-row', 'right-span-row', 'shared-row'],
    separate: ['steps-row', 'midpoint-tolerance-row'],
    widen_center: ['steps-row', 'anchor-step-row', 'anchor-tolerance-row'],
    translate_group: ['group-row', 'preserve-row', 'anchor-tolerance-row', 'sense-row', 'shared-row'],
    linear_witness: ['linear-intent-row'],
  };
  const visible = new Set(rows[kind] || []);
  for (const suffix of new Set(Object.values(rows).flat())) {
    const row = document.getElementById(`${nodeId}-${suffix}`);
    if (row) row.style.display = visible.has(suffix) ? '' : 'none';
  }
  return INTENT_KINDS.has(kind);
}

export function invalidateRopShapePreparedRequest(nodeId) {
  const info = nodeRegistry[nodeId];
  if (info) {
    invalidateBoundRopShapeLifecycle(
      info,
      ROP_SHAPE_PREPARE_LIFECYCLE_KEY,
      'rop-shape-config-input-changed',
    );
  }
  delete ensureNodeData(nodeId).ropShapeRequest;
  setConfigStatus(nodeId,
    '<div class="node-info"><strong>Needs Prepare.</strong> Configuration changed; the prior request is no longer an output.</div>');
  connections
    .filter(connection => connection.fromNode === nodeId && connection.fromPort === 'rop-shape-request')
    .forEach(connection => invalidateRopShapeResultRun(connection.toNode, {
      message: 'Configuration changed; rerun ROP shape optimization.',
    }));
}

export function invalidateRopShapeResultRun(nodeId, {
  message = null,
} = {}) {
  const info = nodeRegistry[nodeId];
  if (info) {
    invalidateBoundRopShapeLifecycle(
      info,
      ROP_SHAPE_RESULT_LIFECYCLE_KEY,
      'rop-shape-input-changed',
    );
  }
  const data = ensureNodeData(nodeId);
  const current = Number.isSafeInteger(data.ropShapeRunToken) ? data.ropShapeRunToken : 0;
  data.ropShapeRunToken = current + 1;
  delete data.ropShapeRequest;
  delete data.ropShapeResult;
  setNodeLoading(nodeId, false);
  if (message) {
    const content = document.getElementById(`${nodeId}-content`);
    if (content) content.innerHTML = `<div class="node-info">${escapeHtml(message)}</div>`;
  }
  return data.ropShapeRunToken;
}

export function isCurrentRopShapeResultRun(nodeId, token) {
  return Number.isSafeInteger(token) && ensureNodeData(nodeId).ropShapeRunToken === token;
}

function installRopShapeRequestInvalidation(nodeId) {
  const node = document.getElementById(nodeId);
  if (!node) return;
  node.querySelectorAll('.auto-update').forEach(control => {
    const eventType = control.tagName === 'SELECT' || control.type === 'checkbox' ? 'change' : 'input';
    control.addEventListener(eventType, () => invalidateRopShapePreparedRequest(nodeId));
  });
}

export async function prepareRopShapeRequest(nodeId, {
  throwOnFailure = false,
  commit = true,
} = {}) {
  const owner = nodeRegistry[nodeId];
  if (!owner || owner.type !== 'rop-shape-edit-config') {
    if (throwOnFailure) invalid('config node', 'is unavailable');
    return null;
  }
  const lifecycle = ropShapeLifecycleFor(owner, ROP_SHAPE_PREPARE_LIFECYCLE_KEY);
  let ticket = null;
  // A fresh prepare attempt invalidates the prior output immediately. Failed
  // validation must not leave a stale request flowing from this node.
  invalidateRopShapePreparedRequest(nodeId);
  setNodeLoading(nodeId, true);
  try {
    const { request, intent, witnessCount } = buildCurrentRopShapeRequest(nodeId);
    const beginContext = ropShapeLifecycleContext(nodeId, ropShapeRequestFingerprint(request));
    if (!beginContext) return null;
    ticket = beginLifecycle(lifecycle, beginContext);
    syncRopShapeLifecycle(owner, lifecycle);
    const currentContext = currentRopShapePrepareContext(nodeId);
    if (!currentContext || !commitLifecycle(lifecycle, ticket, {
      context: currentContext,
      result: request,
      evidence: {
        source_endpoint: ROP_SHAPE_ENDPOINT,
        artifact_kind: 'rop-shape-request',
      },
    })) {
      syncRopShapeLifecycle(owner, lifecycle);
      return null;
    }
    if (nodeRegistry[nodeId] !== owner) return null;
    ensureNodeData(nodeId).ropShapeRequest = cloneRopShapeJson(request, 'prepared request');
    syncRopShapeLifecycle(owner, lifecycle);
    setConfigStatus(nodeId,
      `<div class="node-info"><strong>Prepared.</strong> ${escapeHtml(intent.kind)} over ` +
      `${witnessCount} pinned program steps; Run the connected result node.</div>`);
    if (commit) commitWorkspaceSnapshot('rop-shape-request-prepared');
    return request;
  } catch (error) {
    let current = false;
    if (ticket) {
      const currentContext = currentRopShapePrepareContext(nodeId);
      current = !!currentContext && failLifecycle(lifecycle, ticket, {
        context: currentContext,
        error,
      });
    } else {
      const context = ropShapeLifecycleContext(nodeId, stableRopShapeFingerprint({
        invalid: true,
        attempt: rawRopShapeEditorFingerprint(nodeId),
      }));
      if (context) {
        ticket = beginLifecycle(lifecycle, context);
        current = blockLifecycle(lifecycle, ticket, {
          context,
          reason: error?.message || String(error),
        });
      }
    }
    syncRopShapeLifecycle(owner, lifecycle);
    if (current && nodeRegistry[nodeId] === owner) {
      setConfigStatus(nodeId, `<div class="node-error">${escapeHtml(error.message)}</div>`);
    }
    if (throwOnFailure && current) throw error;
    return null;
  } finally {
    if (ticket) releaseLifecycle(lifecycle, ticket);
    settleRopShapeLoading(nodeId, lifecycle, ticket);
    if (!ticket && nodeRegistry[nodeId] === owner) setNodeLoading(nodeId, false);
  }
}

function connectedConfigNodeId(resultNodeId) {
  const connection = connections.find(conn =>
    conn.toNode === resultNodeId && conn.toPort === 'rop-shape-request');
  if (!connection) invalid('result input', 'connect a ROP Shape Edit Config');
  const source = nodeRegistry[connection.fromNode];
  if (!source || source.type !== 'rop-shape-edit-config') {
    invalid('result input', 'must come from a ROP Shape Edit Config node');
  }
  return connection.fromNode;
}

function currentRopShapeResultContext(nodeId) {
  if (!nodeRegistry[nodeId]) return null;
  let inputFingerprint;
  try {
    const configNodeId = connectedConfigNodeId(nodeId);
    const request = nodeRegistry[configNodeId]?.data?.ropShapeRequest;
    if (!request) invalid('result input', 'current prepared request is missing');
    inputFingerprint = ropShapeRequestFingerprint(request);
  } catch {
    inputFingerprint = stableRopShapeFingerprint({
      invalid: true,
      resultNodeId: nodeId,
      configConnection: connections.find(connection =>
        connection.toNode === nodeId && connection.toPort === 'rop-shape-request') || null,
    });
  }
  return ropShapeLifecycleContext(nodeId, inputFingerprint);
}

export async function executeRopShapeResult(nodeId, { throwOnFailure = false } = {}) {
  const owner = nodeRegistry[nodeId];
  if (!owner || owner.type !== 'rop-shape-result') return null;
  const lifecycle = ropShapeLifecycleFor(owner, ROP_SHAPE_RESULT_LIFECYCLE_KEY);
  let ticket = null;
  // Starting a fresh attempt immediately retires any historical/stale output,
  // even if upstream prepare fails before the backend request can begin.
  invalidateRopShapeResultRun(nodeId, { message: 'Preparing ROP shape optimization…' });
  setNodeLoading(nodeId, true);
  try {
    // Re-prepare from the current Design Target selection. A restored/stale
    // request on the config node is deliberately not an execution input.
    const request = await prepareRopShapeRequest(connectedConfigNodeId(nodeId), {
      throwOnFailure: true,
      commit: false,
    });
    if (!request) return null;
    // Begin only after prepare has invalidated every downstream result. The
    // ticket binds the exact prepared request, owner object, workspace epoch,
    // and canonical endpoint.
    invalidateRopShapeResultRun(nodeId, { message: 'Running ROP shape optimization…' });
    const beginContext = ropShapeLifecycleContext(nodeId, ropShapeRequestFingerprint(request));
    if (!beginContext) return null;
    ticket = beginLifecycle(lifecycle, beginContext);
    syncRopShapeLifecycle(owner, lifecycle);
    setNodeLoading(nodeId, true);
    const rawResult = await optimizeRopShape(request, {
      statusIsCurrent: () => {
        const context = currentRopShapeResultContext(nodeId);
        return !!context && isCurrentLifecycle(lifecycle, ticket, context);
      },
    });
    let currentContext = currentRopShapeResultContext(nodeId);
    if (!currentContext || !isCurrentLifecycle(lifecycle, ticket, currentContext)) {
      return retireObsoleteRopShapeTicket(lifecycle, ticket, currentContext, owner);
    }
    const result = admitRopShapeResultForRequest(rawResult, request);
    currentContext = currentRopShapeResultContext(nodeId);
    if (!currentContext || !isCurrentLifecycle(lifecycle, ticket, currentContext)) {
      return retireObsoleteRopShapeTicket(lifecycle, ticket, currentContext, owner);
    }
    const rendered = renderRopShapeOptimizationResult(result);
    currentContext = currentRopShapeResultContext(nodeId);
    if (!currentContext || !commitLifecycle(lifecycle, ticket, {
      context: currentContext,
      result: { request, result },
      evidence: ropShapeEvidence(result),
    })) {
      syncRopShapeLifecycle(owner, lifecycle);
      return null;
    }
    if (nodeRegistry[nodeId] !== owner) return null;
    const resultData = ensureNodeData(nodeId);
    resultData.ropShapeRequest = cloneRopShapeJson(request, 'executed request');
    resultData.ropShapeResult = cloneRopShapeJson(result, 'executed result');
    syncRopShapeLifecycle(owner, lifecycle);
    if (!isCurrentLifecycle(lifecycle, ticket, currentContext)) return null;
    const content = document.getElementById(`${nodeId}-content`);
    if (content) content.innerHTML = rendered;
    commitWorkspaceSnapshot('rop-shape-optimization');
    return result;
  } catch (error) {
    let current = false;
    if (ticket) {
      const currentContext = currentRopShapeResultContext(nodeId);
      current = !!currentContext && failLifecycle(lifecycle, ticket, {
        context: currentContext,
        error,
      });
    } else {
      const context = ropShapeLifecycleContext(nodeId, stableRopShapeFingerprint({
        invalid: true,
        stage: 'prepare',
        resultNodeId: nodeId,
      }));
      if (context) {
        ticket = beginLifecycle(lifecycle, context);
        current = blockLifecycle(lifecycle, ticket, {
          context,
          reason: error?.message || String(error),
        });
      }
    }
    syncRopShapeLifecycle(owner, lifecycle);
    const content = document.getElementById(`${nodeId}-content`);
    if (current && nodeRegistry[nodeId] === owner && content) {
      content.innerHTML = `<div class="node-error">${escapeHtml(error.message)}</div>`;
    }
    if (throwOnFailure && current) throw error;
    return null;
  } finally {
    if (ticket) releaseLifecycle(lifecycle, ticket);
    settleRopShapeLoading(nodeId, lifecycle, ticket);
    if (!ticket && nodeRegistry[nodeId] === owner) setNodeLoading(nodeId, false);
  }
}

export function restoreRopShapeResultView(nodeId, data = null) {
  const content = document.getElementById(`${nodeId}-content`);
  if (!content) return false;
  const owner = nodeRegistry[nodeId];
  const restored = data || nodeRegistry[nodeId]?.data || {};
  const liveData = nodeRegistry[nodeId]?.data;
  const historical = '<div class="design-screen-truncation-warning rop-shape-restored-warning" role="status">' +
    '<strong>Historical/restored artifact.</strong> This is saved evidence, not a fresh backend run. ' +
    'Click Run to rebuild the request from the current pinned Design Target selection.</div>';
  try {
    if (!restored.ropShapeRequest || !restored.ropShapeResult) {
      invalid('saved artifact pair', 'requires both ropShapeRequest and ropShapeResult');
    }
    const admitted = admitRopShapeResultForRequest(
      restored.ropShapeResult,
      restored.ropShapeRequest,
    );
    const rendered = renderRopShapeOptimizationResult(admitted);
    if (!owner || nodeRegistry[nodeId] !== owner) return false;
    const lifecycle = ropShapeLifecycleFor(owner, ROP_SHAPE_RESULT_LIFECYCLE_KEY);
    const context = ropShapeLifecycleContext(
      nodeId,
      ropShapeRequestFingerprint(restored.ropShapeRequest),
    );
    if (!context) return false;
    restoreHistoricalLifecycle(lifecycle, {
      context,
      result: {
        ropShapeRequest: restored.ropShapeRequest,
        ropShapeResult: admitted,
      },
      evidence: ropShapeEvidence(admitted),
    });
    syncRopShapeLifecycle(owner, lifecycle);
    const historicalResult = inspectExecutionLifecycle(lifecycle).result;
    if (liveData) {
      liveData.ropShapeRequest = historicalResult.ropShapeRequest;
      liveData.ropShapeResult = historicalResult.ropShapeResult;
      delete liveData.sessionId;
      delete liveData.session_id;
    }
    content.innerHTML = historical + rendered;
    return true;
  } catch (error) {
    if (liveData) invalidateRopShapeResultRun(nodeId);
    content.innerHTML = historical +
      `<div class="node-error">Saved artifact rejected: ${escapeHtml(error.message)}</div>`;
    return false;
  }
}

const LINEAR_DEFAULT = JSON.stringify({
  constraints: [],
  objective: {
    id: 'separation',
    sense: 'maximize',
    terms: [{ step: 5, coefficient: 1 }, { step: 1, coefficient: -1 }],
  },
}, null, 2);

export const ROP_SHAPE_TYPES = {
  'rop-shape-edit-config': {
    category: 'parameter',
    headerClass: 'header-parameter',
    title: 'ROP Shape Edit Config',
    inputs: [{ port: 'rop-shape-reference', type: 'ROPShapeReferenceArtifact', label: 'ROP Shape Reference' }],
    outputs: [{ port: 'rop-shape-request', type: 'ROPShapeRequestArtifact', label: 'ROP Shape Request' }],
    defaultWidth: 460,
    createBody(nodeId) {
      return `
        <div class="param-row">
          <label>Edit:</label>
          <select id="${nodeId}-rop-shape-kind" class="auto-update" data-action="updateRopShapeIntentVisibility" data-node="${nodeId}">
            <option value="broaden">broaden both spans</option>
            <option value="separate">separate two steps</option>
            <option value="widen_center">widen around an anchor</option>
            <option value="translate_group">translate a step group</option>
            <option value="linear_witness">canonical linear witness</option>
          </select>
        </div>
        <div class="param-row"><label>Intent ID:</label><input id="${nodeId}-intent-id" class="auto-update" value="shape-edit"></div>
        <div class="param-row" id="${nodeId}-left-span-row"><label>Left span:</label><input id="${nodeId}-left-span" class="auto-update" value="0, 2" placeholder="0, 2"></div>
        <div class="param-row" id="${nodeId}-right-span-row"><label>Right span:</label><input id="${nodeId}-right-span" class="auto-update" value="4, 6" placeholder="4, 6"></div>
        <div class="param-row" id="${nodeId}-steps-row" style="display:none;"><label>Steps:</label><input id="${nodeId}-steps" class="auto-update" value="1, 5" placeholder="1, 5"></div>
        <div class="param-row" id="${nodeId}-group-row" style="display:none;"><label>Move group:</label><input id="${nodeId}-group" class="auto-update" value="4, 5, 6"></div>
        <div class="param-row" id="${nodeId}-preserve-row" style="display:none;"><label>Preserve:</label><input id="${nodeId}-preserve" class="auto-update" value="0, 1, 2, 3"></div>
        <div class="param-row" id="${nodeId}-anchor-step-row" style="display:none;"><label>Anchor step:</label><input type="number" id="${nodeId}-anchor-step" class="auto-update" value="3" min="0" step="1"></div>
        <div class="param-row" id="${nodeId}-anchor-tolerance-row" style="display:none;"><label>Anchor/preserve tol.:</label><input type="number" id="${nodeId}-anchor-tolerance" class="auto-update" value="0.2" min="0" step="0.01"></div>
        <div class="param-row" id="${nodeId}-midpoint-tolerance-row" style="display:none;"><label>Midpoint tol.:</label><input type="number" id="${nodeId}-midpoint-tolerance" class="auto-update" value="0.2" min="0" step="0.01"></div>
        <div class="param-row" id="${nodeId}-sense-row" style="display:none;"><label>Direction:</label><select id="${nodeId}-sense" class="auto-update"><option value="positive">positive</option><option value="negative">negative</option></select></div>
        <div class="param-row" id="${nodeId}-shared-row"><label><input type="checkbox" id="${nodeId}-shared" class="auto-update" checked> shared magnitude/shift (required)</label></div>
        <div class="param-row" id="${nodeId}-linear-intent-row" style="display:none;align-items:flex-start;"><label>Linear intent:</label><textarea id="${nodeId}-linear-intent-json" class="auto-update" rows="8" spellcheck="false" style="flex:1;font-family:var(--font-mono, monospace);font-size:11px;">${LINEAR_DEFAULT}</textarea></div>
        <div class="param-row"><label>Min parameter margin:</label><input type="number" id="${nodeId}-minimum-parameter-margin" class="auto-update" value="0.01" min="0" step="0.01"></div>
        <div class="param-row"><label>Effect tolerance:</label><input type="number" id="${nodeId}-effect-tolerance" class="auto-update" value="0.02" min="0" step="0.01"></div>
        <div class="param-row"><label>Max paths:</label><input type="number" id="${nodeId}-max-paths" class="auto-update" value="2000" min="1" max="2000" step="1"></div>
        <div class="param-row"><label>Max cells:</label><input type="number" id="${nodeId}-max-cells" class="auto-update" value="256" min="1" max="256" step="1"></div>
        <div class="param-row"><label>Max replays:</label><input type="number" id="${nodeId}-max-replays" class="auto-update" value="1" min="1" max="2" step="1"></div>
        <div class="param-row"><label><input type="checkbox" id="${nodeId}-require-exhaustive" class="auto-update" checked> require exhaustive population</label></div>
        <div class="param-row"><label>Replay samples:</label><input type="number" id="${nodeId}-replay-sample-points" class="auto-update" value="281" min="11" max="1000" step="2"></div>
        <div class="param-row"><label>Min prominence:</label><input type="number" id="${nodeId}-replay-min-prominence" class="auto-update" value="0.5" min="0" step="0.05"></div>
        <button class="btn btn-run" data-action="prepareRopShapeRequest" data-node="${nodeId}">Validate / Prepare</button>
        <div id="${nodeId}-rop-shape-status" style="margin-top:6px;"><span class="text-dim">Connect a pinned Design Target reference, choose an edit, then prepare.</span></div>
      `;
    },
    onInit(nodeId) {
      updateRopShapeIntentVisibility(nodeId);
      installRopShapeRequestInvalidation(nodeId);
      setupAutoUpdate(nodeId, 'rop-shape-edit-config');
    },
    async prepare(nodeId) {
      return prepareRopShapeRequest(nodeId, { throwOnFailure: true });
    },
  },
  'rop-shape-result': {
    category: 'result',
    headerClass: 'header-result',
    title: 'ROP Shape Optimization Result',
    inputs: [{ port: 'rop-shape-request', type: 'ROPShapeRequestArtifact', label: 'ROP Shape Request' }],
    outputs: [{ port: 'rop-shape-result', type: 'ROPShapeResultArtifact', label: 'ROP Shape Result' }],
    defaultWidth: 680,
    createBody(nodeId) {
      return `
        <button class="btn btn-run" data-action="executeRopShapeResult" data-node="${nodeId}">Run</button>
        <div class="viewer-content" id="${nodeId}-content">
          <span class="text-dim">Connect a ROP Shape Edit Config and click Run.</span>
        </div>
      `;
    },
    async execute(nodeId) {
      return executeRopShapeResult(nodeId, { throwOnFailure: true });
    },
  },
};
