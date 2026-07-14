const RESULT_VERSION = 'bne-rop-shape-optimization/v1.0.0';
const REQUEST_VERSION = 'bne-rop-shape-optimize-request/v1.0.0';
const REPLAY_VERSION = 'bne-rop-shape-replay/v1.0.0';
const COMPILER_VERSION = 'bne-rop-shape-compiler/v1.0.0';
const RESULT_ARTIFACT_VERSION = 'bne-result/v1.0.0';
const SHA256_HEX = /^[0-9a-f]{64}$/;

const INTENT_KINDS = new Set([
  'broaden',
  'separate',
  'widen_center',
  'translate_group',
  'linear_witness',
]);

const REPLAY_METRIC_STATUSES = new Set([
  'invalid_shape',
  'under_resolved',
  'invalid_threshold',
  'partial_solver_failure',
  'nonfinite_sample',
  'invalid_grid',
  'two_peaks_not_found',
  'half_prominence_crossing_missing',
  'pass',
  'prominence_below_minimum',
]);

const FINITE_REPLAY_GRADES = Object.freeze({
  not_run: 'not_run',
  pass: 'sampled-forward-complete',
  partial: 'sampled-forward-partial',
  failed: 'sampled-forward-failed',
  numerical_error: 'sampled-forward-failed',
});

const GEOMETRIC_STATUSES = new Set([
  'global_optimal_over_declared_cells',
  'best_over_evaluated_cells',
  'infeasible_over_declared_cells',
  'infeasible_over_evaluated_cells',
  'unbounded_over_declared_cells',
  'unbounded_over_evaluated_cells',
  'numerical_error',
]);

const REPLAY_STATUSES = new Set(['not_run', 'pass', 'failed', 'partial', 'numerical_error']);

export const ROP_SHAPE_RESULT_VERSION = RESULT_VERSION;

function invalid(message) {
  throw new Error(`Invalid ROP shape optimization result: ${message}`);
}

function objectAt(value, path) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) invalid(`${path} must be an object`);
  return value;
}

function exactKeysAt(value, path, required, optional = []) {
  const object = objectAt(value, path);
  const allowed = new Set([...required, ...optional]);
  for (const key of required) {
    if (!Object.prototype.hasOwnProperty.call(object, key)) invalid(`${path}.${key} is required`);
  }
  for (const key of Object.keys(object)) {
    if (!allowed.has(key)) invalid(`${path}.${key} is not allowed by the v1 contract`);
  }
  return object;
}

function stringAt(value, path) {
  if (typeof value !== 'string' || !value.trim()) invalid(`${path} must be a non-empty string`);
  return value;
}

function booleanAt(value, path) {
  if (typeof value !== 'boolean') invalid(`${path} must be a boolean`);
  return value;
}

function finiteAt(value, path) {
  if (typeof value !== 'number' || !Number.isFinite(value)) invalid(`${path} must be finite`);
  return value;
}

function nonnegativeFiniteAt(value, path) {
  const formatted = finiteAt(value, path);
  if (formatted < 0) invalid(`${path} must be nonnegative`);
  return formatted;
}

function positiveFiniteAt(value, path) {
  const formatted = finiteAt(value, path);
  if (formatted <= 0) invalid(`${path} must be positive`);
  return formatted;
}

function sha256At(value, path) {
  if (typeof value !== 'string' || !SHA256_HEX.test(value)) {
    invalid(`${path} must be a lowercase SHA-256 hex digest`);
  }
  return value;
}

function optionalFiniteAt(value, path) {
  return value == null ? null : finiteAt(value, path);
}

function countAt(value, path) {
  if (!Number.isInteger(value) || value < 0) invalid(`${path} must be a nonnegative integer`);
  return value;
}

function boundedIntegerAt(value, path, minimum, maximum) {
  if (!Number.isInteger(value) || value < minimum || value > maximum) {
    invalid(`${path} must be an integer from ${minimum} through ${maximum}`);
  }
  return value;
}

function positiveIntegerArrayAt(value, path, { nonempty = false } = {}) {
  if (!Array.isArray(value) || (nonempty && value.length === 0)) {
    invalid(`${path} must be ${nonempty ? 'a non-empty' : 'an'} array`);
  }
  return value.map((item, index) => {
    if (!Number.isInteger(item) || item < 1) invalid(`${path}[${index}] must be a positive integer`);
    return item;
  });
}

function finiteArrayAt(value, path, { nonempty = false } = {}) {
  if (!Array.isArray(value) || (nonempty && value.length === 0)) {
    invalid(`${path} must be ${nonempty ? 'a non-empty' : 'an'} array`);
  }
  return value.map((item, index) => finiteAt(item, `${path}[${index}]`));
}

function stringArrayAt(value, path) {
  if (!Array.isArray(value)) invalid(`${path} must be an array`);
  return value.map((item, index) => stringAt(item, `${path}[${index}]`));
}

function positiveFiniteArrayAt(value, path, { nonempty = false } = {}) {
  if (!Array.isArray(value) || (nonempty && value.length === 0)) {
    invalid(`${path} must be ${nonempty ? 'a non-empty' : 'an'} array`);
  }
  return value.map((item, index) => positiveFiniteAt(item, `${path}[${index}]`));
}

function positiveFiniteMapAt(value, path, { nonempty = false } = {}) {
  const object = objectAt(value, path);
  const entries = Object.entries(object);
  if (nonempty && entries.length === 0) invalid(`${path} must not be empty`);
  for (const [key, item] of entries) {
    stringAt(key, `${path} key`);
    positiveFiniteAt(item, `${path}.${key}`);
  }
  return object;
}

function canonicalComparable(value) {
  if (Array.isArray(value)) return value.map(canonicalComparable);
  if (value && typeof value === 'object') {
    return Object.fromEntries(
      Object.keys(value).sort().map(key => [key, canonicalComparable(value[key])]),
    );
  }
  return value;
}

function sameStructuredValue(left, right) {
  return JSON.stringify(canonicalComparable(left)) === JSON.stringify(canonicalComparable(right));
}

function requireSameStructuredValue(left, right, message) {
  if (!sameStructuredValue(left, right)) invalid(message);
}

function validateNormalizedRequest(raw) {
  const request = exactKeysAt(raw, 'normalized_request', [
    'schema_version',
    'network',
    'expected_network_ir_hash',
    'designability_spec',
    'reference',
    'edit_intent',
    'optimization',
    'work_budget',
    'replay',
  ]);
  if (request.schema_version !== REQUEST_VERSION) {
    invalid(`normalized_request.schema_version must be ${REQUEST_VERSION}`);
  }

  const network = exactKeysAt(request.network, 'normalized_request.network', [
    'ir_schema_version',
    'label',
    'species',
    'reactions',
    'observables',
    'parameter_distributions',
    'compartments',
    'provenance',
    'extensions',
  ]);
  if (network.ir_schema_version !== 'bne-ir/v1.0.0') {
    invalid('normalized_request.network must be canonical NetworkIR v1');
  }
  stringAt(network.label, 'normalized_request.network.label');
  for (const key of ['species', 'observables', 'parameter_distributions', 'compartments']) {
    if (!Array.isArray(network[key])) invalid(`normalized_request.network.${key} must be an array`);
  }
  if (!Array.isArray(network.reactions) || network.reactions.length === 0) {
    invalid('normalized_request.network.reactions must be a non-empty array');
  }
  const reactionRules = network.reactions.map((reaction, index) => {
    const value = objectAt(reaction, `normalized_request.network.reactions[${index}]`);
    return stringAt(value.formula, `normalized_request.network.reactions[${index}].formula`);
  });
  objectAt(network.provenance, 'normalized_request.network.provenance');
  objectAt(network.extensions, 'normalized_request.network.extensions');

  const expectedNetworkHash = sha256At(
    request.expected_network_ir_hash,
    'normalized_request.expected_network_ir_hash',
  );
  const spec = objectAt(request.designability_spec, 'normalized_request.designability_spec');
  if (spec.schema_version !== 'bne-designability/v1.0.0') {
    invalid('normalized_request.designability_spec must be DesignabilitySpec v1');
  }
  const target = objectAt(spec.target, 'normalized_request.designability_spec.target');
  const behavior = objectAt(
    target.behavior_spec,
    'normalized_request.designability_spec.target.behavior_spec',
  );
  const input = stringAt(
    behavior.input,
    'normalized_request.designability_spec.target.behavior_spec.input',
  );
  const output = stringAt(
    behavior.output,
    'normalized_request.designability_spec.target.behavior_spec.output',
  );
  if (behavior.feature_space !== 'reaction_order') {
    invalid('normalized_request.designability_spec target feature_space must be reaction_order');
  }
  if (!Array.isArray(behavior.program) || behavior.program.length < 2) {
    invalid('normalized_request.designability_spec target program must contain at least two steps');
  }
  const programLength = behavior.program.length;

  const reference = objectAt(request.reference, 'normalized_request.reference');
  const referenceHash = sha256At(
    reference.reference_hash,
    'normalized_request.reference.reference_hash',
  );
  const referenceNetworkHash = sha256At(
    reference.network_ir_hash,
    'normalized_request.reference.network_ir_hash',
  );
  const referenceOperatingPoints = finiteArrayAt(
    reference.operating_points_log10,
    'normalized_request.reference.operating_points_log10',
    { nonempty: true },
  );
  if (referenceOperatingPoints.length !== programLength) {
    invalid('normalized_request reference operating points must match the target program length');
  }
  const referenceKd = positiveFiniteArrayAt(
    reference.kd,
    'normalized_request.reference.kd',
    { nonempty: true },
  );
  if (referenceKd.length !== network.reactions.length) {
    invalid('normalized_request reference kd count must match NetworkIR reactions');
  }
  positiveFiniteMapAt(reference.totals, 'normalized_request.reference.totals', { nonempty: true });

  const editIntent = objectAt(request.edit_intent, 'normalized_request.edit_intent');
  const intentId = stringAt(editIntent.id, 'normalized_request.edit_intent.id');
  const intentKind = stringAt(editIntent.kind, 'normalized_request.edit_intent.kind');
  if (!INTENT_KINDS.has(intentKind)) {
    invalid(`unsupported normalized_request.edit_intent.kind ${intentKind}`);
  }

  const optimization = exactKeysAt(request.optimization, 'normalized_request.optimization', [
    'minimum_parameter_margin',
    'effect_tolerance',
  ]);
  nonnegativeFiniteAt(
    optimization.minimum_parameter_margin,
    'normalized_request.optimization.minimum_parameter_margin',
  );
  const effectTolerance = nonnegativeFiniteAt(
    optimization.effect_tolerance,
    'normalized_request.optimization.effect_tolerance',
  );

  const workBudget = exactKeysAt(request.work_budget, 'normalized_request.work_budget', [
    'max_paths',
    'max_cells',
    'max_replays',
    'require_exhaustive',
  ]);
  boundedIntegerAt(workBudget.max_paths, 'normalized_request.work_budget.max_paths', 1, 2000);
  boundedIntegerAt(workBudget.max_cells, 'normalized_request.work_budget.max_cells', 1, 10000);
  boundedIntegerAt(workBudget.max_replays, 'normalized_request.work_budget.max_replays', 1, 16);
  booleanAt(workBudget.require_exhaustive, 'normalized_request.work_budget.require_exhaustive');

  const replay = exactKeysAt(request.replay, 'normalized_request.replay', [
    'input_window_log10',
    'sample_points',
    'require_complete',
    'store_curve',
    'metrics',
  ]);
  const replayWindow = finiteArrayAt(
    replay.input_window_log10,
    'normalized_request.replay.input_window_log10',
  );
  if (replayWindow.length !== 2 || replayWindow[0] >= replayWindow[1] ||
      replayWindow.some(value => value < -20 || value > 20)) {
    invalid('normalized_request.replay.input_window_log10 must be an increasing pair within [-20, 20]');
  }
  const replaySamplePoints = boundedIntegerAt(
    replay.sample_points,
    'normalized_request.replay.sample_points',
    11,
    1000,
  );
  if (replay.require_complete !== true || replay.store_curve !== true) {
    invalid('normalized_request replay must require completeness and store the curve');
  }
  if (!Array.isArray(replay.metrics) || replay.metrics.length !== 1) {
    invalid('normalized_request.replay.metrics must contain exactly one metric');
  }
  const metricRequest = objectAt(replay.metrics[0], 'normalized_request.replay.metrics[0]');
  if (metricRequest.kind !== 'two_peak') {
    invalid('normalized_request replay metric must be two_peak');
  }
  nonnegativeFiniteAt(
    metricRequest.min_prominence_log10,
    'normalized_request.replay.metrics[0].min_prominence_log10',
  );

  return {
    request,
    network,
    reactionRules,
    expectedNetworkHash,
    referenceHash,
    referenceNetworkHash,
    editIntent,
    intentId,
    intentKind,
    input,
    output,
    programLength,
    reactionCount: network.reactions.length,
    effectTolerance,
    replayWindow,
    replaySamplePoints,
  };
}

function validateFixedTopology(raw, normalized) {
  const topology = exactKeysAt(raw, 'fixed_topology', [
    'normalized_network',
    'network_ir_hash',
    'network_canonical_code',
    'network_identity_semantics',
    'input',
    'output',
    'topology_preserved',
  ]);
  const networkHash = sha256At(topology.network_ir_hash, 'fixed_topology.network_ir_hash');
  if (topology.topology_preserved !== true) invalid('fixed_topology.topology_preserved must be true');
  const input = stringAt(topology.input, 'fixed_topology.input');
  const output = stringAt(topology.output, 'fixed_topology.output');
  if (networkHash !== normalized.expectedNetworkHash ||
      networkHash !== normalized.referenceNetworkHash) {
    invalid('fixed_topology network hash conflicts with normalized request identity');
  }
  if (input !== normalized.input || output !== normalized.output) {
    invalid('fixed_topology input/output conflict with the normalized DesignabilitySpec');
  }
  requireSameStructuredValue(
    topology.normalized_network,
    normalized.network,
    'fixed_topology.normalized_network conflicts with normalized_request.network',
  );
  const semantics = stringAt(
    topology.network_identity_semantics,
    'fixed_topology.network_identity_semantics',
  );
  if (semantics === 'canonical_code_available') {
    stringAt(topology.network_canonical_code, 'fixed_topology.network_canonical_code');
  } else if (semantics === 'positional_content_hash_only') {
    if (topology.network_canonical_code !== null) {
      invalid('positional fixed-topology identity must use a null canonical code');
    }
  } else {
    invalid(`unsupported fixed_topology.network_identity_semantics ${semantics}`);
  }
  return { networkHash, input, output };
}

function validateCompiledEdit(raw, normalized) {
  const compiled = exactKeysAt(raw, 'compiled_edit', [
    'compiler_version',
    'source_intent_id',
    'intent',
    'constraints',
    'objective',
    'direction',
    'auxiliary_coordinates',
    'index_basis',
    'units',
  ]);
  if (compiled.compiler_version !== COMPILER_VERSION) {
    invalid(`compiled_edit.compiler_version must be ${COMPILER_VERSION}`);
  }
  const sourceIntentId = stringAt(compiled.source_intent_id, 'compiled_edit.source_intent_id');
  const intent = objectAt(compiled.intent, 'compiled_edit.intent');
  const intentId = stringAt(intent.id, 'compiled_edit.intent.id');
  const intentKind = stringAt(intent.kind, 'compiled_edit.intent.kind');
  if (!INTENT_KINDS.has(intentKind)) invalid(`unsupported compiled_edit.intent.kind ${intentKind}`);
  if (sourceIntentId !== intentId || sourceIntentId !== normalized.intentId) {
    invalid('compiled_edit source intent id conflicts with normalized_request.edit_intent.id');
  }
  if (intentKind !== normalized.intentKind) {
    invalid('compiled_edit source intent kind conflicts with normalized_request.edit_intent.kind');
  }
  requireSameStructuredValue(
    intent,
    normalized.editIntent,
    'compiled_edit.intent conflicts with normalized_request.edit_intent',
  );
  if (!Array.isArray(compiled.constraints)) invalid('compiled_edit.constraints must be an array');
  const objective = objectAt(compiled.objective, 'compiled_edit.objective');
  const objectiveKind = stringAt(objective.kind, 'compiled_edit.objective.kind');
  if (!['linear_operating_point', 'max_min_linear_operating_point_improvement'].includes(objectiveKind)) {
    invalid(`unsupported compiled_edit.objective.kind ${objectiveKind}`);
  }
  if (!Array.isArray(compiled.auxiliary_coordinates)) {
    invalid('compiled_edit.auxiliary_coordinates must be an array');
  }
  const auxiliary = stringArrayAt(
    compiled.auxiliary_coordinates,
    'compiled_edit.auxiliary_coordinates',
  );
  if (new Set(auxiliary).size !== auxiliary.length) {
    invalid('compiled_edit.auxiliary_coordinates must be unique');
  }
  const expectedAuxiliary = objectiveKind === 'max_min_linear_operating_point_improvement'
    ? ['alpha']
    : [];
  requireSameStructuredValue(
    auxiliary,
    expectedAuxiliary,
    'compiled_edit auxiliary coordinates conflict with its objective kind',
  );
  if (intentKind === 'linear_witness') {
    if (compiled.direction !== null) invalid('linear_witness compiled_edit.direction must be null');
  } else {
    const direction = exactKeysAt(compiled.direction, 'compiled_edit.direction', [
      'values',
      'l2_norm',
      'normalization',
      'alpha_units',
    ]);
    finiteArrayAt(direction.values, 'compiled_edit.direction.values', { nonempty: true });
    positiveFiniteAt(direction.l2_norm, 'compiled_edit.direction.l2_norm');
    if (direction.normalization !== 'not_normalized' ||
        direction.alpha_units !== 'declared_raw_direction_scale') {
      invalid('compiled_edit.direction must preserve the declared raw direction scale');
    }
  }
  if (compiled.index_basis !== 'zero_based_program_step') {
    invalid('compiled_edit.index_basis must be zero_based_program_step');
  }
  if (compiled.units !== 'log10_input') invalid('compiled_edit.units must be log10_input');
  return compiled;
}

function validateSolverContract(raw) {
  const solver = exactKeysAt(raw, 'solver_contract', [
    'lp_backend',
    'objective_policy',
    'parameter_margin_basis',
    'effect_limit_semantics',
    'active_row_shadow_price_semantics',
    'compiler_version',
  ]);
  const expected = {
    lp_backend: 'Clarabel',
    objective_policy: 'global_epsilon_lexicographic_effect_then_parameter_margin',
    parameter_margin_basis: 'equality_feasible_log10_qK_subspace',
    effect_limit_semantics: 'closed_polyhedral_support_limit',
    active_row_shadow_price_semantics:
      'objective_derivative_with_respect_to_compiled_rhs_not_primal_parameter_derivative',
    compiler_version: COMPILER_VERSION,
  };
  for (const [key, value] of Object.entries(expected)) {
    if (solver[key] !== value) invalid(`solver_contract.${key} is unsupported`);
  }
  return solver;
}

function formatCoverage(raw) {
  const coverage = exactKeysAt(raw, 'coverage', [
    'eligible_path_count',
    'evaluated_path_count',
    'eligible_cell_count',
    'evaluated_cell_count',
    'feasible_cell_count',
    'replay_candidate_count',
    'replayed_count',
    'truncated',
    'truncation_reasons',
  ]);
  const formatted = {
    eligiblePathCount: countAt(coverage.eligible_path_count, 'coverage.eligible_path_count'),
    evaluatedPathCount: countAt(coverage.evaluated_path_count, 'coverage.evaluated_path_count'),
    eligibleCellCount: countAt(coverage.eligible_cell_count, 'coverage.eligible_cell_count'),
    evaluatedCellCount: countAt(coverage.evaluated_cell_count, 'coverage.evaluated_cell_count'),
    feasibleCellCount: countAt(coverage.feasible_cell_count, 'coverage.feasible_cell_count'),
    replayCandidateCount: countAt(coverage.replay_candidate_count, 'coverage.replay_candidate_count'),
    replayedCount: countAt(coverage.replayed_count, 'coverage.replayed_count'),
    truncated: booleanAt(coverage.truncated, 'coverage.truncated'),
    truncationReasons: stringArrayAt(coverage.truncation_reasons, 'coverage.truncation_reasons'),
  };
  if (formatted.evaluatedPathCount > formatted.eligiblePathCount) {
    invalid('coverage.evaluated_path_count exceeds eligible_path_count');
  }
  if (formatted.evaluatedCellCount > formatted.eligibleCellCount) {
    invalid('coverage.evaluated_cell_count exceeds eligible_cell_count');
  }
  if (formatted.feasibleCellCount > formatted.evaluatedCellCount) {
    invalid('coverage.feasible_cell_count exceeds evaluated_cell_count');
  }
  if (formatted.replayedCount > formatted.replayCandidateCount) {
    invalid('coverage.replayed_count exceeds replay_candidate_count');
  }
  if (new Set(formatted.truncationReasons).size !== formatted.truncationReasons.length) {
    invalid('coverage.truncation_reasons must be unique');
  }
  if (formatted.truncated && formatted.truncationReasons.length === 0) {
    invalid('truncated coverage requires at least one truncation reason');
  }
  if (!formatted.truncated && formatted.truncationReasons.length !== 0) {
    invalid('non-truncated coverage forbids truncation reasons');
  }
  if (!formatted.truncated &&
      (formatted.evaluatedPathCount !== formatted.eligiblePathCount ||
       formatted.evaluatedCellCount !== formatted.eligibleCellCount)) {
    invalid('non-truncated coverage must evaluate every eligible path and cell');
  }
  return formatted;
}

function formatActiveConstraints(raw) {
  if (!Array.isArray(raw)) invalid('selected.active_constraints must be an array');
  return raw.map((item, index) => {
    const path = `selected.active_constraints[${index}]`;
    const row = exactKeysAt(item, path, [
      'row_id',
      'row_kind',
      'point_residual',
      'ball_residual',
      'normalized_residual',
      'dual',
      'shadow_price',
      'shadow_price_semantics',
    ]);
    finiteAt(row.point_residual, `${path}.point_residual`);
    finiteAt(row.ball_residual, `${path}.ball_residual`);
    finiteAt(row.normalized_residual, `${path}.normalized_residual`);
    const dual = row.dual === null ? null : finiteAt(row.dual, `${path}.dual`);
    const shadowPrice = row.shadow_price === null
      ? null
      : finiteAt(row.shadow_price, `${path}.shadow_price`);
    const semantics = stringAt(
      row.shadow_price_semantics,
      `${path}.shadow_price_semantics`,
    );
    if (semantics !== 'derivative_of_objective_value_with_respect_to_compiled_rhs') {
      invalid(`selected.active_constraints[${index}] has unknown shadow-price semantics`);
    }
    return {
      rowId: stringAt(row.row_id, `selected.active_constraints[${index}].row_id`),
      rowKind: stringAt(row.row_kind, `selected.active_constraints[${index}].row_kind`),
      dual,
      shadowPrice,
    };
  });
}

function formatSelected(raw, normalized) {
  const selected = exactKeysAt(raw, 'selected', [
    'cell_id',
    'path_identity',
    'path_idx',
    'witness_identity',
    'witness_vertex_indices',
    'full_path_vertex_indices',
    'predicted_profile',
    'witness_input_log10',
    'background_log_qK',
    'kd',
    'totals',
    'primary_effect',
    'parameter_margin',
    'active_constraints',
    'solver',
  ]);
  if (typeof selected.cell_id !== 'string' || !/^sha256:[0-9a-f]{64}$/.test(selected.cell_id)) {
    invalid('selected.cell_id must be a sha256-prefixed content identity');
  }
  if (typeof selected.path_identity !== 'string' || !/^path:[1-9][0-9]*$/.test(selected.path_identity)) {
    invalid('selected.path_identity must be a canonical one-based path identity');
  }
  const pathIndex = countAt(selected.path_idx, 'selected.path_idx');
  if (pathIndex < 1 || selected.path_identity !== `path:${pathIndex}`) {
    invalid('selected.path_idx must match selected.path_identity');
  }

  if (!Array.isArray(selected.witness_identity) || selected.witness_identity.length === 0) {
    invalid('selected.witness_identity must be a non-empty array');
  }
  const witnessIdentity = selected.witness_identity.map((value, index) => {
    if (typeof value !== 'string' || !/^step:[0-9]+:vertex:[1-9][0-9]*$/.test(value)) {
      invalid(`selected.witness_identity[${index}] is not canonical`);
    }
    return value;
  });
  if (new Set(witnessIdentity).size !== witnessIdentity.length) {
    invalid('selected.witness_identity must be unique');
  }
  const witnessVertexIndices = positiveIntegerArrayAt(
    selected.witness_vertex_indices,
    'selected.witness_vertex_indices',
    { nonempty: true },
  );
  positiveIntegerArrayAt(
    selected.full_path_vertex_indices,
    'selected.full_path_vertex_indices',
    { nonempty: true },
  );
  const predictedProfile = finiteArrayAt(
    selected.predicted_profile,
    'selected.predicted_profile',
    { nonempty: true },
  );
  const witnessInput = finiteArrayAt(
    selected.witness_input_log10,
    'selected.witness_input_log10',
    { nonempty: true },
  );
  for (const [label, length] of [
    ['witness_identity', witnessIdentity.length],
    ['witness_vertex_indices', witnessVertexIndices.length],
    ['predicted_profile', predictedProfile.length],
    ['witness_input_log10', witnessInput.length],
  ]) {
    if (length !== normalized.programLength) {
      invalid(`selected.${label} length must match the normalized target program`);
    }
  }

  const background = exactKeysAt(selected.background_log_qK, 'selected.background_log_qK', [
    'symbols',
    'values',
  ]);
  const backgroundSymbols = stringArrayAt(
    background.symbols,
    'selected.background_log_qK.symbols',
  );
  if (new Set(backgroundSymbols).size !== backgroundSymbols.length) {
    invalid('selected.background_log_qK.symbols must be unique');
  }
  const backgroundValues = finiteArrayAt(
    background.values,
    'selected.background_log_qK.values',
  );
  if (backgroundSymbols.length !== backgroundValues.length) {
    invalid('selected.background_log_qK symbols and values must have equal lengths');
  }

  const kd = positiveFiniteArrayAt(selected.kd, 'selected.kd', { nonempty: true });
  if (kd.length !== normalized.reactionCount) {
    invalid('selected.kd count must match normalized NetworkIR reactions');
  }
  positiveFiniteMapAt(selected.totals, 'selected.totals', { nonempty: true });

  const primary = exactKeysAt(selected.primary_effect, 'selected.primary_effect', [
    'objective_id',
    'sense',
    'value',
    'effect_bound',
    'semantics',
    'effect_kind',
    'reference_value',
    'closure_support_value',
    'cell_primary_value',
    'selected_value',
    'closure_support_improvement',
    'selected_improvement',
    'effect_tolerance',
  ]);
  stringAt(primary.objective_id, 'selected.primary_effect.objective_id');
  if (!['maximize', 'minimize'].includes(primary.sense)) {
    invalid('selected.primary_effect.sense is unsupported');
  }
  for (const key of [
    'value',
    'effect_bound',
    'reference_value',
    'closure_support_value',
    'cell_primary_value',
    'selected_value',
  ]) finiteAt(primary[key], `selected.primary_effect.${key}`);
  if (primary.semantics !== 'closed_polyhedral_support_limit_and_secondary_realization') {
    invalid('selected.primary_effect.semantics is unsupported');
  }
  if (!['linear', 'balanced_minimum_improvement'].includes(primary.effect_kind)) {
    invalid('selected.primary_effect.effect_kind is unsupported');
  }

  const margin = exactKeysAt(selected.parameter_margin, 'selected.parameter_margin', [
    'value',
    'basis',
    'coordinate_basis',
    'dimension',
    'equality_rank',
    'coordinates',
    'basis_matrix',
    'rank_relative_tolerance',
    'rank_absolute_threshold',
    'zero_dimensional_convention',
  ]);
  const basis = stringAt(margin.basis, 'selected.parameter_margin.basis');
  const coordinateBasis = stringAt(
    margin.coordinate_basis,
    'selected.parameter_margin.coordinate_basis',
  );
  if (basis !== 'equality_feasible_log10_qK_subspace') {
    invalid('selected.parameter_margin.basis is not parameter-only');
  }
  if (coordinateBasis !== 'unweighted_euclidean_log10_qK') {
    invalid('selected.parameter_margin.coordinate_basis is unsupported');
  }
  const dimension = countAt(margin.dimension, 'selected.parameter_margin.dimension');
  const equalityRank = countAt(margin.equality_rank, 'selected.parameter_margin.equality_rank');
  const coordinates = stringArrayAt(margin.coordinates, 'selected.parameter_margin.coordinates');
  if (new Set(coordinates).size !== coordinates.length) {
    invalid('selected.parameter_margin.coordinates must be unique');
  }
  const marginValue = nonnegativeFiniteAt(margin.value, 'selected.parameter_margin.value');
  if (dimension + equalityRank !== coordinates.length) {
    invalid('selected.parameter_margin rank and dimension do not match its coordinates');
  }
  if (!Array.isArray(margin.basis_matrix) || margin.basis_matrix.length !== coordinates.length) {
    invalid('selected.parameter_margin.basis_matrix row count must match its coordinates');
  }
  margin.basis_matrix.forEach((row, index) => {
    const values = finiteArrayAt(row, `selected.parameter_margin.basis_matrix[${index}]`);
    if (values.length !== dimension) {
      invalid(`selected.parameter_margin.basis_matrix[${index}] must have dimension columns`);
    }
  });
  positiveFiniteAt(
    margin.rank_relative_tolerance,
    'selected.parameter_margin.rank_relative_tolerance',
  );
  nonnegativeFiniteAt(
    margin.rank_absolute_threshold,
    'selected.parameter_margin.rank_absolute_threshold',
  );
  if (dimension === 0 && marginValue !== 0) {
    invalid('zero-dimensional parameter margin must use radius zero');
  }
  const zeroDimensionalConvention = stringAt(
    margin.zero_dimensional_convention,
    'selected.parameter_margin.zero_dimensional_convention',
  );
  if ((dimension === 0 && zeroDimensionalConvention !== 'radius_zero') ||
      (dimension > 0 && zeroDimensionalConvention !== 'not_applicable')) {
    invalid('selected.parameter_margin has an inconsistent 0D convention');
  }
  const effectTolerance = nonnegativeFiniteAt(
    primary.effect_tolerance,
    'selected.primary_effect.effect_tolerance',
  );
  if (effectTolerance !== normalized.effectTolerance) {
    invalid('selected.primary_effect.effect_tolerance conflicts with normalized_request.optimization');
  }
  const closureSupportImprovement = finiteAt(
    primary.closure_support_improvement,
    'selected.primary_effect.closure_support_improvement',
  );
  const selectedImprovement = finiteAt(
    primary.selected_improvement,
    'selected.primary_effect.selected_improvement',
  );
  const effectGap = closureSupportImprovement - selectedImprovement;
  if (effectGap < -1e-7 || effectGap > effectTolerance + 1e-7) {
    invalid('selected improvement is outside the declared closure-support epsilon band');
  }

  const solver = exactKeysAt(selected.solver, 'selected.solver', [
    'name',
    'version',
    'termination_status',
    'validation_tolerance',
    'active_tolerance',
    'rank_tolerance',
    'primary_termination_status',
    'primary_message',
    'secondary_message',
    'core_status',
  ]);
  if (solver.name !== 'Clarabel') invalid('selected.solver.name must be Clarabel');
  stringAt(solver.version, 'selected.solver.version');
  stringAt(solver.termination_status, 'selected.solver.termination_status');
  stringAt(solver.primary_termination_status, 'selected.solver.primary_termination_status');
  for (const key of ['validation_tolerance', 'active_tolerance', 'rank_tolerance']) {
    positiveFiniteAt(solver[key], `selected.solver.${key}`);
  }
  for (const key of ['primary_message', 'secondary_message']) {
    if (typeof solver[key] !== 'string') invalid(`selected.solver.${key} must be a string`);
  }
  if (!['optimal', 'infeasible', 'unbounded', 'numerical_error'].includes(solver.core_status)) {
    invalid('selected.solver.core_status is unsupported');
  }
  if (solver.core_status !== 'optimal') {
    invalid('selected.solver.core_status must be optimal for a selected geometric optimum');
  }

  return {
    cellId: selected.cell_id,
    pathIdentity: selected.path_identity,
    primaryEffect: {
      effectKind: stringAt(primary.effect_kind, 'selected.primary_effect.effect_kind'),
      closureSupportImprovement,
      selectedImprovement,
      effectTolerance,
    },
    parameterMargin: {
      value: marginValue,
      basis,
      coordinateBasis,
      dimension,
      equalityRank,
      coordinates,
      zeroDimensionalConvention,
    },
    activeConstraints: formatActiveConstraints(selected.active_constraints),
  };
}

function formatDirection(raw) {
  if (raw == null) return null;
  const direction = objectAt(raw, 'directional_request_interval');
  if (direction.normalization !== 'not_normalized') {
    invalid('directional_request_interval.normalization must be not_normalized');
  }
  if (direction.alpha_units !== 'declared_raw_direction_scale') {
    invalid('directional_request_interval.alpha_units must preserve the declared direction scale');
  }
  const directionValues = finiteArrayAt(
    direction.direction,
    'directional_request_interval.direction',
    { nonempty: true },
  );
  const norm = finiteAt(direction.direction_l2_norm, 'directional_request_interval.direction_l2_norm');
  if (norm <= 0) invalid('directional_request_interval.direction_l2_norm must be positive');
  if (!Array.isArray(direction.union_intervals)) {
    invalid('directional_request_interval.union_intervals must be an array');
  }
  const intervals = direction.union_intervals.map((item, index) => {
    const interval = objectAt(item, `directional_request_interval.union_intervals[${index}]`);
    const formatted = {
      alphaMin: optionalFiniteAt(
        interval.alpha_min,
        `directional_request_interval.union_intervals[${index}].alpha_min`,
      ),
      alphaMax: optionalFiniteAt(
        interval.alpha_max,
        `directional_request_interval.union_intervals[${index}].alpha_max`,
      ),
      lowerUnbounded: booleanAt(
        interval.lower_unbounded,
        `directional_request_interval.union_intervals[${index}].lower_unbounded`,
      ),
      upperUnbounded: booleanAt(
        interval.upper_unbounded,
        `directional_request_interval.union_intervals[${index}].upper_unbounded`,
      ),
    };
    if (formatted.lowerUnbounded !== (formatted.alphaMin == null) ||
        formatted.upperUnbounded !== (formatted.alphaMax == null)) {
      invalid(`directional_request_interval.union_intervals[${index}] has inconsistent endpoints`);
    }
    if (formatted.alphaMin != null && formatted.alphaMax != null &&
        formatted.alphaMin > formatted.alphaMax) {
      invalid(`directional_request_interval.union_intervals[${index}] is reversed`);
    }
    return formatted;
  });
  const scope = stringAt(direction.scope, 'directional_request_interval.scope');
  if (!['declared_cells', 'evaluated_cells'].includes(scope)) {
    invalid(`unsupported directional_request_interval.scope ${scope}`);
  }
  return {
    direction: directionValues,
    norm,
    scope,
    complete: booleanAt(
      direction.complete_over_evaluated_cells,
      'directional_request_interval.complete_over_evaluated_cells',
    ),
    numericalErrorCount: countAt(
      direction.numerical_error_count,
      'directional_request_interval.numerical_error_count',
    ),
    intervals,
  };
}

function validateReplayRequest(raw, context) {
  const request = exactKeysAt(raw, 'replay.request', ['endpoint', 'method', 'body']);
  if (request.endpoint !== '/api/v1/placer_curve' || request.method !== 'POST') {
    invalid('replay.request must be the canonical POST /api/v1/placer_curve request');
  }
  const body = exactKeysAt(request.body, 'replay.request.body', [
    'rules',
    'input_sym',
    'output_sym',
    'kd',
    'totals',
    'param_min',
    'param_max',
    'n_points',
  ]);
  const rules = stringArrayAt(body.rules, 'replay.request.body.rules');
  if (rules.length === 0) invalid('replay.request.body.rules must not be empty');
  requireSameStructuredValue(
    rules,
    context.normalized.reactionRules,
    'replay.request.body.rules conflict with the normalized NetworkIR',
  );
  if (stringAt(body.input_sym, 'replay.request.body.input_sym') !== context.topology.input ||
      stringAt(body.output_sym, 'replay.request.body.output_sym') !== context.topology.output) {
    invalid('replay.request input/output conflict with fixed_topology');
  }
  positiveFiniteArrayAt(body.kd, 'replay.request.body.kd', { nonempty: true });
  positiveFiniteMapAt(body.totals, 'replay.request.body.totals', { nonempty: true });
  requireSameStructuredValue(
    body.kd,
    context.selected.kd,
    'replay.request.body.kd do not identify the selected realization',
  );
  requireSameStructuredValue(
    body.totals,
    context.selected.totals,
    'replay.request.body.totals do not identify the selected realization',
  );
  const paramMin = finiteAt(body.param_min, 'replay.request.body.param_min');
  const paramMax = finiteAt(body.param_max, 'replay.request.body.param_max');
  if (paramMin !== context.normalized.replayWindow[0] ||
      paramMax !== context.normalized.replayWindow[1]) {
    invalid('replay.request input window conflicts with normalized_request.replay');
  }
  const samplePoints = boundedIntegerAt(body.n_points, 'replay.request.body.n_points', 11, 1000);
  if (samplePoints !== context.normalized.replaySamplePoints) {
    invalid('replay.request n_points conflicts with normalized_request.replay.sample_points');
  }
  return { request, samplePoints };
}

function validateReplayCurve(raw, samplePoints) {
  const curve = exactKeysAt(raw, 'replay.curve', [
    'param_values',
    'output_traj',
    'valid',
    'partial',
  ]);
  const paramValues = finiteArrayAt(curve.param_values, 'replay.curve.param_values');
  if (paramValues.length < 11 || paramValues.length > 1000) {
    invalid('replay.curve.param_values must contain 11 through 1000 finite samples');
  }
  if (!Array.isArray(curve.output_traj)) invalid('replay.curve.output_traj must be an array');
  const outputTrajectory = curve.output_traj.map((row, rowIndex) =>
    finiteArrayAt(row, `replay.curve.output_traj[${rowIndex}]`, { nonempty: true }));
  if (!Array.isArray(curve.valid)) invalid('replay.curve.valid must be an array');
  const valid = curve.valid.map((value, index) =>
    booleanAt(value, `replay.curve.valid[${index}]`));
  const partial = booleanAt(curve.partial, 'replay.curve.partial');
  if (paramValues.length !== samplePoints || outputTrajectory.length !== samplePoints ||
      valid.length !== samplePoints) {
    invalid('replay curve arrays must all match the canonical replay request n_points');
  }
  return { curve, partial, valid };
}

function validateTwoPeakMetrics(raw, samplePoints) {
  const optionalFields = [
    'reason',
    'peak_candidate_count',
    'peak_indices',
    'peak_input_log10',
    'peak_output_log10',
    'valley_index',
    'valley_input_log10',
    'valley_output_log10',
    'peak_separation_log10',
    'left_prominence_log10',
    'right_prominence_log10',
    'left_half_prominence_width_log10',
    'right_half_prominence_width_log10',
    'central_half_prominence_interval_log10',
    'half_prominence_crossings_log10',
    'min_prominence_log10',
  ];
  const metrics = exactKeysAt(raw, 'replay.metrics', [
    'schema_version',
    'status',
    'sample_points',
    'complete',
    'pass',
  ], optionalFields);
  if (metrics.schema_version !== REPLAY_VERSION) {
    invalid(`replay.metrics.schema_version must be ${REPLAY_VERSION}`);
  }
  const status = stringAt(metrics.status, 'replay.metrics.status');
  if (!REPLAY_METRIC_STATUSES.has(status)) invalid(`unsupported replay.metrics.status ${status}`);
  const metricSamplePoints = countAt(metrics.sample_points, 'replay.metrics.sample_points');
  if (metricSamplePoints !== samplePoints) {
    invalid('replay.metrics.sample_points conflicts with the stored replay curve');
  }
  const complete = booleanAt(metrics.complete, 'replay.metrics.complete');
  const pass = booleanAt(metrics.pass, 'replay.metrics.pass');
  if (Object.prototype.hasOwnProperty.call(metrics, 'reason') && typeof metrics.reason !== 'string') {
    invalid('replay.metrics.reason must be a string');
  }

  const validateOptionalNonnegative = key => {
    if (Object.prototype.hasOwnProperty.call(metrics, key)) {
      nonnegativeFiniteAt(metrics[key], `replay.metrics.${key}`);
    }
  };
  for (const key of [
    'peak_separation_log10',
    'left_prominence_log10',
    'right_prominence_log10',
    'left_half_prominence_width_log10',
    'right_half_prominence_width_log10',
    'central_half_prominence_interval_log10',
    'min_prominence_log10',
  ]) validateOptionalNonnegative(key);

  if (Object.prototype.hasOwnProperty.call(metrics, 'peak_candidate_count')) {
    countAt(metrics.peak_candidate_count, 'replay.metrics.peak_candidate_count');
  }
  if (Object.prototype.hasOwnProperty.call(metrics, 'peak_indices')) {
    if (!Array.isArray(metrics.peak_indices) || metrics.peak_indices.length !== 2) {
      invalid('replay.metrics.peak_indices must contain exactly two indices');
    }
    const indices = metrics.peak_indices.map((value, index) =>
      boundedIntegerAt(value, `replay.metrics.peak_indices[${index}]`, 1, samplePoints));
    if (new Set(indices).size !== indices.length) invalid('replay.metrics.peak_indices must be unique');
  }
  for (const [key, length] of [
    ['peak_input_log10', 2],
    ['peak_output_log10', 2],
    ['half_prominence_crossings_log10', 4],
  ]) {
    if (Object.prototype.hasOwnProperty.call(metrics, key)) {
      const values = finiteArrayAt(metrics[key], `replay.metrics.${key}`);
      if (values.length !== length) invalid(`replay.metrics.${key} must contain exactly ${length} values`);
    }
  }
  if (Object.prototype.hasOwnProperty.call(metrics, 'valley_index')) {
    boundedIntegerAt(metrics.valley_index, 'replay.metrics.valley_index', 1, samplePoints);
  }
  for (const key of ['valley_input_log10', 'valley_output_log10']) {
    if (Object.prototype.hasOwnProperty.call(metrics, key)) {
      finiteAt(metrics[key], `replay.metrics.${key}`);
    }
  }

  return { metrics, status, complete, pass };
}

function requirePassingMetricFields(metrics) {
  const required = [
    'reason',
    'peak_candidate_count',
    'peak_indices',
    'peak_input_log10',
    'peak_output_log10',
    'valley_index',
    'valley_input_log10',
    'valley_output_log10',
    'peak_separation_log10',
    'left_prominence_log10',
    'right_prominence_log10',
    'left_half_prominence_width_log10',
    'right_half_prominence_width_log10',
    'central_half_prominence_interval_log10',
    'half_prominence_crossings_log10',
    'min_prominence_log10',
  ];
  for (const key of required) {
    if (!Object.prototype.hasOwnProperty.call(metrics, key)) {
      invalid(`replay.metrics.${key} is required for a passing replay`);
    }
  }
  if (metrics.peak_candidate_count < 2) {
    invalid('replay.metrics.peak_candidate_count must be at least two for a pass');
  }
}

function formatReplay(raw, context) {
  const replay = exactKeysAt(raw, 'replay', [
    'status',
    'request',
    'request_hash',
    'curve',
    'metrics',
    'result_hash',
    'complete',
    'pass',
  ]);
  const status = stringAt(replay.status, 'replay.status');
  if (!REPLAY_STATUSES.has(status)) invalid(`unsupported replay.status ${status}`);
  const complete = booleanAt(replay.complete, 'replay.complete');
  const pass = booleanAt(replay.pass, 'replay.pass');
  if (status === 'pass' && (!complete || !pass)) invalid('replay pass must be complete and passing');
  if (status !== 'pass' && pass) invalid('only replay.status=pass may set replay.pass=true');
  if (status === 'failed' && (!complete || pass)) {
    invalid('replay.status=failed must be complete and non-passing');
  }
  if (['not_run', 'partial', 'numerical_error'].includes(status) && (complete || pass)) {
    invalid(`replay.status=${status} must be incomplete and non-passing`);
  }
  if (status === 'not_run') {
    for (const key of ['request', 'request_hash', 'curve', 'metrics', 'result_hash']) {
      if (replay[key] !== null) invalid(`replay.${key} must be null when replay was not run`);
    }
    return { status, complete, pass, metrics: null };
  }
  if (status === 'numerical_error') {
    for (const key of ['request', 'request_hash', 'curve', 'metrics']) {
      if (replay[key] !== null) invalid(`replay.${key} must be null after a replay numerical error`);
    }
    sha256At(replay.result_hash, 'replay.result_hash');
    return { status, complete, pass, metrics: null };
  }

  if (context.selected == null) invalid('an executed replay requires a selected realization');
  const replayRequest = validateReplayRequest(replay.request, context);
  sha256At(replay.request_hash, 'replay.request_hash');
  sha256At(replay.result_hash, 'replay.result_hash');
  const curve = validateReplayCurve(replay.curve, replayRequest.samplePoints);
  const metricResult = validateTwoPeakMetrics(replay.metrics, replayRequest.samplePoints);
  if (metricResult.complete !== complete || metricResult.pass !== pass) {
    invalid('replay metrics complete/pass declarations conflict with replay status');
  }
  if ((metricResult.status === 'pass') !== (status === 'pass')) {
    invalid('replay.metrics.status=pass must agree exactly with replay.status=pass');
  }
  if (complete && (curve.partial || !curve.valid.every(value => value === true))) {
    invalid('a complete replay requires a non-partial curve with every sample valid');
  }
  if (status === 'pass') {
    if (curve.partial || !curve.valid.every(value => value === true)) {
      invalid('passing replay requires a non-partial curve with every sample valid');
    }
    requirePassingMetricFields(metricResult.metrics);
  }

  const metrics = metricResult.metrics;
  return {
    status,
    complete,
    pass,
    metrics: {
      status: metricResult.status,
      reason: typeof metrics.reason === 'string' ? metrics.reason : '',
      peakInputs: Array.isArray(metrics.peak_input_log10)
        ? metrics.peak_input_log10
        : [],
      peakSeparation: optionalFiniteAt(metrics.peak_separation_log10, 'replay.metrics.peak_separation_log10'),
      leftProminence: optionalFiniteAt(metrics.left_prominence_log10, 'replay.metrics.left_prominence_log10'),
      rightProminence: optionalFiniteAt(metrics.right_prominence_log10, 'replay.metrics.right_prominence_log10'),
      leftWidth: optionalFiniteAt(
        metrics.left_half_prominence_width_log10,
        'replay.metrics.left_half_prominence_width_log10',
      ),
      rightWidth: optionalFiniteAt(
        metrics.right_half_prominence_width_log10,
        'replay.metrics.right_half_prominence_width_log10',
      ),
      centralInterval: optionalFiniteAt(
        metrics.central_half_prominence_interval_log10,
        'replay.metrics.central_half_prominence_interval_log10',
      ),
    },
  };
}

function validateArtifact(raw, identity, warnings) {
  const artifact = objectAt(raw, 'artifact');
  if (artifact.artifact_schema_version !== RESULT_ARTIFACT_VERSION) {
    invalid(`artifact.artifact_schema_version must be ${RESULT_ARTIFACT_VERSION}`);
  }
  if (artifact.kind !== 'rop_shape_optimize') invalid('artifact.kind must be rop_shape_optimize');
  const inputHashes = exactKeysAt(artifact.input_hashes, 'artifact.input_hashes', [
    'request',
    'network_ir',
    'designability_spec',
    'reference',
  ]);
  const requestHash = sha256At(inputHashes.request, 'artifact.input_hashes.request');
  const networkHash = sha256At(inputHashes.network_ir, 'artifact.input_hashes.network_ir');
  sha256At(inputHashes.designability_spec, 'artifact.input_hashes.designability_spec');
  const referenceHash = sha256At(inputHashes.reference, 'artifact.input_hashes.reference');
  if (requestHash !== identity.requestHash || networkHash !== identity.networkHash ||
      referenceHash !== identity.referenceHash) {
    invalid('artifact input hashes conflict with the response request/network/reference identity');
  }
  const algorithm = exactKeysAt(artifact.algorithm, 'artifact.algorithm', [
    'name',
    'version',
    'config_hash',
  ]);
  if (algorithm.name !== 'fixed_topology_rop_shape_optimizer') {
    invalid('artifact.algorithm.name must identify the fixed-topology optimizer');
  }
  stringAt(algorithm.version, 'artifact.algorithm.version');
  const configHash = sha256At(algorithm.config_hash, 'artifact.algorithm.config_hash');
  if (configHash !== identity.requestHash) {
    invalid('artifact.algorithm.config_hash must equal the normalized request hash');
  }
  stringAt(artifact.created_at, 'artifact.created_at');
  if (Object.prototype.hasOwnProperty.call(artifact, 'warnings')) {
    const artifactWarnings = stringArrayAt(artifact.warnings, 'artifact.warnings');
    requireSameStructuredValue(
      artifactWarnings,
      warnings,
      'artifact.warnings conflict with top-level warnings',
    );
  }
  return artifact;
}

export function formatRopShapeOptimizationResult(raw) {
  const data = exactKeysAt(raw, 'result', [
    'schema_version',
    'request_hash',
    'normalized_request',
    'fixed_topology',
    'geometric_status',
    'geometric_status_message',
    'feasible',
    'coverage',
    'compiled_edit',
    'selected',
    'replay',
    'certificate_grade',
    'geometric_evidence_grade',
    'finite_replay_evidence_grade',
    'solver_contract',
    'result_hash',
    'artifact',
    'warnings',
  ], ['directional_request_interval']);
  if (data.schema_version !== RESULT_VERSION) {
    invalid(`unsupported schema_version ${String(data.schema_version || '(missing)')}`);
  }
  const requestHash = sha256At(data.request_hash, 'request_hash');
  const resultHash = sha256At(data.result_hash, 'result_hash');
  const normalized = validateNormalizedRequest(data.normalized_request);
  const topology = validateFixedTopology(data.fixed_topology, normalized);
  validateCompiledEdit(data.compiled_edit, normalized);
  validateSolverContract(data.solver_contract);

  const geometricStatus = stringAt(data.geometric_status, 'geometric_status');
  if (!GEOMETRIC_STATUSES.has(geometricStatus)) invalid(`unsupported geometric_status ${geometricStatus}`);
  const feasible = booleanAt(data.feasible, 'feasible');
  const coverage = formatCoverage(data.coverage);
  const hasSelectedGeometry = new Set([
    'global_optimal_over_declared_cells',
    'best_over_evaluated_cells',
  ]).has(geometricStatus);
  if (hasSelectedGeometry !== feasible) invalid('feasible conflicts with geometric_status');
  if (hasSelectedGeometry && data.selected == null) invalid('optimal geometric status requires selected');
  if (!hasSelectedGeometry && data.selected != null) invalid('non-optimal geometric status forbids selected');
  if (geometricStatus === 'best_over_evaluated_cells' && !coverage.truncated) {
    invalid('best_over_evaluated_cells requires truncated coverage');
  }
  const declaredPopulationStatuses = new Set([
    'global_optimal_over_declared_cells',
    'infeasible_over_declared_cells',
    'unbounded_over_declared_cells',
  ]);
  const evaluatedPopulationStatuses = new Set([
    'best_over_evaluated_cells',
    'infeasible_over_evaluated_cells',
    'unbounded_over_evaluated_cells',
  ]);
  if (declaredPopulationStatuses.has(geometricStatus) && coverage.truncated) {
    invalid('declared-cell geometric status cannot have truncated coverage');
  }
  if (evaluatedPopulationStatuses.has(geometricStatus) && !coverage.truncated) {
    invalid('evaluated-cell geometric status requires truncated coverage');
  }
  const direction = formatDirection(data.directional_request_interval);
  if (direction && coverage.truncated !== (direction.scope === 'evaluated_cells')) {
    invalid('directional interval scope conflicts with path/cell truncation');
  }
  if (direction && direction.complete !== (direction.numericalErrorCount === 0)) {
    invalid('directional interval completeness conflicts with its numerical-error count');
  }
  const selected = data.selected == null ? null : formatSelected(data.selected, normalized);
  if (selected == null && coverage.replayCandidateCount !== 0) {
    invalid('coverage.replay_candidate_count must be zero without a selected realization');
  }
  if (selected != null && coverage.replayCandidateCount !== 1) {
    invalid('the v1 selected realization must declare exactly one replay candidate');
  }
  if (selected != null && coverage.feasibleCellCount === 0) {
    invalid('a selected realization requires at least one feasible evaluated cell');
  }
  const replay = formatReplay(data.replay, {
    normalized,
    topology,
    selected: data.selected,
  });
  if ((replay.status === 'not_run') !== (coverage.replayedCount === 0)) {
    invalid('replay status conflicts with replayed coverage count');
  }
  if (coverage.replayedCount > 1) invalid('the v1 optimizer may execute at most one replay');

  if (data.certificate_grade !== 'exact-window-siso-rop-path-optimization') {
    invalid('certificate_grade is unsupported');
  }
  if (data.geometric_evidence_grade !== 'exact_path_polyhedral') {
    invalid('geometric_evidence_grade must remain exact_path_polyhedral');
  }
  const finiteReplayGrade = stringAt(
    data.finite_replay_evidence_grade,
    'finite_replay_evidence_grade',
  );
  if (finiteReplayGrade !== FINITE_REPLAY_GRADES[replay.status]) {
    invalid('finite_replay_evidence_grade conflicts with replay.status');
  }
  const warnings = stringArrayAt(data.warnings, 'warnings');
  validateArtifact(data.artifact, {
    requestHash,
    networkHash: topology.networkHash,
    referenceHash: normalized.referenceHash,
    resultHash,
  }, warnings);
  return {
    schemaVersion: RESULT_VERSION,
    requestHash,
    resultHash,
    geometricStatus,
    geometricMessage: stringAt(data.geometric_status_message, 'geometric_status_message'),
    feasible,
    coverage,
    selected,
    direction,
    replay,
    certificateGrade: data.certificate_grade,
    geometricEvidenceGrade: data.geometric_evidence_grade,
    finiteReplayEvidenceGrade: finiteReplayGrade,
    warnings,
  };
}

function escapeHtml(value) {
  return String(value ?? '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function fmt(value, digits = 4) {
  if (!Number.isFinite(value)) return 'n/a';
  const abs = Math.abs(value);
  if (abs !== 0 && (abs >= 1e5 || abs < 1e-4)) return value.toExponential(3);
  return value.toFixed(digits).replace(/\.?0+$/, '');
}

function renderCoverage(coverage) {
  const warning = coverage.truncated
    ? `<div class="design-screen-truncation-warning rop-shape-truncation-warning" role="status">` +
      `<span class="tag tag-atlas-failed">truncated</span> ` +
      `Only evaluated path/cell populations are covered; omitted cells remain unknown. ` +
      `Reasons: ${escapeHtml(coverage.truncationReasons.join(', ') || 'unspecified')}.</div>`
    : '';
  return `<div class="siso-summary-line rop-shape-coverage">` +
    `<span class="summary-chip">paths <strong>${coverage.evaluatedPathCount} / ${coverage.eligiblePathCount}</strong></span>` +
    `<span class="summary-chip">cells <strong>${coverage.evaluatedCellCount} / ${coverage.eligibleCellCount}</strong></span>` +
    `<span class="summary-chip">feasible cells <strong>${coverage.feasibleCellCount}</strong></span>` +
    `<span class="summary-chip">replays <strong>${coverage.replayedCount} / ${coverage.replayCandidateCount}</strong></span>` +
    `</div>${warning}`;
}

function renderSelected(selected) {
  if (!selected) return `<section class="siso-section rop-shape-selected">` +
    `<div class="text-dim">No geometric realization was selected.</div></section>`;
  const effect = selected.primaryEffect;
  const margin = selected.parameterMargin;
  const active = selected.activeConstraints.length
    ? selected.activeConstraints.map(row => {
      const sensitivity = row.shadowPrice == null
        ? 'compiled-RHS sensitivity unavailable'
        : `compiled-RHS sensitivity ${fmt(row.shadowPrice)}`;
      return `<div class="path-item rop-shape-active-row">` +
        `<span><strong>${escapeHtml(row.rowId)}</strong> · ${escapeHtml(row.rowKind)}</span>` +
        `<span class="text-dim">${escapeHtml(sensitivity)}</span></div>`;
    }).join('')
    : '<div class="text-dim">No active source rows were reported.</div>';
  return `<section class="siso-section rop-shape-selected">` +
    `<div class="siso-section-head"><div class="siso-section-title">Selected geometric realization</div>` +
    `<div class="text-dim">${escapeHtml(selected.pathIdentity)} · ${escapeHtml(selected.cellId)}</div></div>` +
    `<div class="siso-summary-line">` +
    `<span class="summary-chip">closure support improvement <strong>${fmt(effect.closureSupportImprovement)}</strong></span>` +
    `<span class="summary-chip">selected realized improvement <strong>${fmt(effect.selectedImprovement)}</strong></span>` +
    `<span class="summary-chip">effect epsilon <strong>${fmt(effect.effectTolerance)}</strong></span>` +
    `</div>` +
    `<div class="text-dim">Closure support is the closed-polyhedron limit; selected improvement is the epsilon-secondary realization used for parameter placement.</div>` +
    `<div class="siso-summary-line rop-shape-parameter-margin">` +
    `<span class="summary-chip">parameter-only margin <strong>${fmt(margin.value)}</strong></span>` +
    `<span class="summary-chip">dimension <strong>${margin.dimension}</strong></span>` +
    `<span class="summary-chip">equality rank <strong>${margin.equalityRank}</strong></span>` +
    `</div>` +
    `<div class="text-dim">basis: ${escapeHtml(margin.basis)} · ${escapeHtml(margin.coordinateBasis)}` +
    `${margin.dimension === 0 ? ' · 0D convention: radius zero' : ''}</div>` +
    `<div class="siso-section-title">Active constraints</div>` +
    `<div class="text-dim">Shadow prices are objective sensitivity to the compiled row RHS, not a primal biochemical-parameter derivative.</div>` +
    active + `</section>`;
}

function renderDirection(direction) {
  if (!direction) return '';
  const intervals = direction.intervals.length
    ? direction.intervals.map(interval => {
      const lo = interval.lowerUnbounded ? '-∞' : fmt(interval.alphaMin);
      const hi = interval.upperUnbounded ? '+∞' : fmt(interval.alphaMax);
      return `<span class="summary-chip">[${escapeHtml(lo)}, ${escapeHtml(hi)}]</span>`;
    }).join('')
    : '<span class="text-dim">no feasible interval</span>';
  return `<section class="siso-section rop-shape-direction">` +
    `<div class="siso-section-head"><div class="siso-section-title">Directional request interval</div>` +
    `<div class="text-dim">${escapeHtml(direction.scope)}</div></div>` +
    `<div class="text-dim">direction [${direction.direction.map(value => fmt(value)).join(', ')}] · ` +
    `L2 norm ${fmt(direction.norm)} · not normalized</div>` +
    `<div class="siso-summary-line">${intervals}</div>` +
    `${direction.complete ? '' : `<div class="design-screen-truncation-warning" role="status">` +
      `Directional union has ${direction.numericalErrorCount} unresolved cell interval(s).</div>`}` +
    `</section>`;
}

function metricChip(label, value) {
  return value == null ? '' : `<span class="summary-chip">${escapeHtml(label)} <strong>${fmt(value)}</strong></span>`;
}

function renderReplay(replay) {
  const metrics = replay.metrics;
  const failed = !replay.complete || !replay.pass;
  const statusClass = replay.pass
    ? 'tag-atlas-ok'
    : replay.status === 'not_run'
      ? 'tag-atlas-muted'
      : 'tag-atlas-failed';
  const metricHtml = metrics == null ? '' :
    `<div class="siso-summary-line rop-shape-replay-metrics">` +
    (metrics.peakInputs.length
      ? `<span class="summary-chip">sampled peaks <strong>${metrics.peakInputs.map(value => fmt(value)).join(', ')}</strong></span>`
      : '') +
    metricChip('sampled separation', metrics.peakSeparation) +
    metricChip('left prominence', metrics.leftProminence) +
    metricChip('right prominence', metrics.rightProminence) +
    metricChip('left half-width', metrics.leftWidth) +
    metricChip('right half-width', metrics.rightWidth) +
    metricChip('central interval', metrics.centralInterval) +
    `</div><div class="text-dim">${escapeHtml(metrics.reason)}</div>`;
  return `<section class="siso-section rop-shape-replay">` +
    `<div class="siso-section-head"><div class="siso-section-title">Finite replay</div>` +
    `<div><span class="tag ${statusClass}">${escapeHtml(replay.status)}</span></div></div>` +
    `<div class="siso-summary-line">` +
    `<span class="summary-chip">complete <strong>${replay.complete ? 'yes' : 'no'}</strong></span>` +
    `<span class="summary-chip">pass <strong>${replay.pass ? 'yes' : 'no'}</strong></span>` +
    `</div>${metricHtml}` +
    `${failed ? `<div class="design-screen-truncation-warning" role="status">` +
      `Finite replay did not provide a complete passing shape; geometric evidence is kept separate.</div>` : ''}` +
    `</section>`;
}

export function renderRopShapeOptimizationResult(raw) {
  const result = formatRopShapeOptimizationResult(raw);
  const statusClass = result.feasible ? 'tag-atlas-ok' : 'tag-atlas-failed';
  const warnings = result.warnings.length
    ? `<section class="siso-section rop-shape-warnings"><div class="siso-section-title">Warnings</div>` +
      result.warnings.map(warning => `<div class="text-dim">${escapeHtml(warning)}</div>`).join('') +
      `</section>`
    : '';
  return `<div class="rop-shape-result" data-schema-version="${RESULT_VERSION}">` +
    `<div class="siso-section-head"><div class="siso-section-title">ROP shape optimization</div>` +
    `<span class="tag ${statusClass}">${escapeHtml(result.geometricStatus)}</span></div>` +
    `<div class="text-dim">${escapeHtml(result.geometricMessage)}</div>` +
    renderCoverage(result.coverage) +
    renderSelected(result.selected) +
    renderDirection(result.direction) +
    renderReplay(result.replay) + warnings + `</div>`;
}
