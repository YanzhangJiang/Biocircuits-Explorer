import assert from 'node:assert/strict';
import {
  buildDesignScreenRequest,
  designCandidateKey,
  renderDesignScreenResults,
} from '../public/js/design-screen-render.js';
import {
  formatRopShapeOptimizationResult,
  renderRopShapeOptimizationResult,
  ROP_SHAPE_RESULT_VERSION,
} from '../public/js/rop-shape-render.js';

let passed = 0;
function test(name, fn) {
  fn();
  passed += 1;
  console.log(`  ok - ${name}`);
}

const payload = {
  schema_version: 'bne-design-screen/v0.3.0',
  designable: true,
  verified_designable: true,
  n_matches: 12,
  screened_count: 2,
  eligible_count: 12,
  evaluated_count: 2,
  truncated: true,
  constraint_audit: [
    { path: '/target/legacy_target', support_level: 'enforced_exact', stage: 'atlas_match' },
  ],
  designability_spec_normalized: {
    schema_version: 'bne-designability/v1.0.0',
    source: { kind: 'test_fixture' },
    target: { legacy_target: { target_kind: 'exact', target: [1] } },
  },
  verified_recommendations: [
    {
      nid: '[1]+[2]<->[1,2]',
      inp: 'tA',
      out: 'C_A_B',
      tunability_score: 0.82,
      certificate_grade: 'exact-union-siso-rop',
      evidence_grade: 'enforced_exact',
      screen_status: 'verified_exact',
      pass: true,
      complexity: { d: 2, r: 1, mu: 2 },
      metrics: {
        chebyshev_radius: 0.7,
        chebyshev_radius_source: 'theta_union_cell',
        tunable_volume: 0.42,
        tunable_volume_source: 'chebyshev_ball_lower_bound',
        transition_spacing: 1.2,
        dynamic_range: 2.1,
        condition_number: 1.5,
        parameter_breakpoint_sensitivity: 0.3,
      },
      active_failures: [],
      parameter_recommendation: {
        theta_star: {
          status: 'computed',
          source: 'feasible_region_chebyshev',
          source_type: 'feasible_region_chebyshev',
          bounds_verified: true,
          log_qK: [0, 0, 0],
          kd: [1],
          totals: { tA: 1, tB: 1 },
        },
        downstream: { model_builder: true, placer: true },
      },
    },
  ],
  screened_candidates: [
    {
      nid: '[1]+[2]<->[1,2]',
      inp: 'tB',
      out: 'C_A_B',
      tunability_score: 0.65,
      certificate_grade: 'proxy-only',
      evidence_grade: 'proxy_only',
      screen_status: 'screened_proxy',
      pass: false,
      complexity: { d: 2, r: 1, mu: 2 },
      metrics: {
        ranking_margin_proxy: 0.5,
        tunable_volume: 0.21,
        transition_spacing: 1.2,
        dynamic_range: 1.9,
        condition_number: 1.8,
        parameter_breakpoint_sensitivity: 0.4,
      },
      active_failures: ['proxy_only_not_recommendation'],
      parameter_recommendation: {
        theta_star: {
          status: 'not_computed',
          source: 'declared_box_center_seed_only',
          log_qK: [0, 0, 0],
          kd: [1],
          totals: { tA: 1, tB: 1 },
        },
        downstream: { model_builder: true, placer: true },
      },
    },
    {
      nid: '[1]+[1]<->[1,1]',
      inp: 'tA',
      out: 'C_A_A',
      tunability_score: 0.18,
      certificate_grade: 'proxy-only',
      evidence_grade: 'proxy_only',
      screen_status: 'screened_proxy',
      pass: false,
      complexity: { d: 1, r: 1, mu: 1 },
      metrics: {
        ranking_margin_proxy: 0,
        tunable_volume: 0,
        transition_spacing: 1,
        dynamic_range: 0,
        condition_number: 3,
        parameter_breakpoint_sensitivity: 1,
      },
      active_failures: ['no_atlas_volume_proxy'],
      parameter_recommendation: {
        theta_star: {
          status: 'not_computed',
          source: 'declared_box_center_seed_only',
          log_qK: [0, 0],
          kd: [1],
          totals: { tA: 1 },
        },
        downstream: { model_builder: true, placer: true },
      },
    },
  ],
  minimal_certificates: [
    {
      d: 2,
      r: 1,
      mu: 2,
      networks: [
        {
          nid: '[1]+[2]<->[1,2]',
          inp: 'tA',
          out: 'C_A_B',
          certificate_grade: 'minimal-structural-certificate',
        },
      ],
    },
  ],
};

const ropRequestHash = '1'.repeat(64);
const ropNetworkHash = '2'.repeat(64);
const ropReferenceHash = '3'.repeat(64);
const ropResultHash = '4'.repeat(64);
const ropReplayRequestHash = '5'.repeat(64);
const ropReplayResultHash = '6'.repeat(64);
const ropDesignabilitySpecHash = '7'.repeat(64);

const ropNetwork = {
  ir_schema_version: 'bne-ir/v1.0.0',
  label: 'renderer-contract-fixture',
  species: [{ name: 'A' }, { name: 'B' }, { name: 'C' }],
  reactions: [{ formula: 'A + B <-> C', kd: 1 }],
  observables: [{ name: 'C', expression: 'C' }],
  parameter_distributions: [],
  compartments: [],
  provenance: { source: 'test_fixture' },
  extensions: {},
};

const ropEditIntent = {
  id: 'broaden-fixture',
  kind: 'broaden',
  left_span_steps: [0, 1],
  right_span_steps: [3, 4],
  shared_magnitude: true,
};

const ropNormalizedRequest = {
  schema_version: 'bne-rop-shape-optimize-request/v1.0.0',
  network: ropNetwork,
  expected_network_ir_hash: ropNetworkHash,
  designability_spec: {
    schema_version: 'bne-designability/v1.0.0',
    source: { kind: 'test_fixture' },
    target: {
      behavior_spec: {
        input: 'tB',
        output: 'C',
        feature_space: 'reaction_order',
        program: [1, 0, -1, 0, 1].map(value => ({
          kind: 'reaction_order',
          operator: '=',
          value,
          hard: true,
        })),
        input_window: { input_log10: [-3, 3], hard: true },
      },
    },
    constraints: { parameter_bounds: { kd_log10: [-4, 4] } },
  },
  reference: {
    reference_hash: ropReferenceHash,
    network_ir_hash: ropNetworkHash,
    operating_points_log10: [-2.4, -1.2, 0, 1.2, 2.4],
    kd: [1],
    totals: { tA: 1 },
    path_identity: 'path:7',
    cell_id: `sha256:${'a'.repeat(64)}`,
  },
  edit_intent: ropEditIntent,
  optimization: {
    minimum_parameter_margin: 0.1,
    effect_tolerance: 0.1,
  },
  work_budget: {
    max_paths: 100,
    max_cells: 100,
    max_replays: 1,
    require_exhaustive: true,
  },
  replay: {
    input_window_log10: [-3, 3],
    sample_points: 11,
    require_complete: true,
    store_curve: true,
    metrics: [{ kind: 'two_peak', min_prominence_log10: 0.5 }],
  },
};

const ropReplayRequest = {
  endpoint: '/api/v1/placer_curve',
  method: 'POST',
  body: {
    rules: ['A + B <-> C'],
    input_sym: 'tB',
    output_sym: 'C',
    kd: [1],
    totals: { tA: 1 },
    param_min: -3,
    param_max: 3,
    n_points: 11,
  },
};

const ropReplayCurve = {
  param_values: [-3, -2.4, -1.8, -1.2, -0.6, 0, 0.6, 1.2, 1.8, 2.4, 3],
  output_traj: [[0], [1], [2], [1], [0.5], [0], [0.5], [1], [2], [1], [0]],
  valid: Array(11).fill(true),
  partial: false,
};

const ropPassingMetrics = {
  schema_version: 'bne-rop-shape-replay/v1.0.0',
  status: 'pass',
  reason: 'two complete sampled peaks passed',
  sample_points: 11,
  complete: true,
  pass: true,
  peak_candidate_count: 2,
  peak_indices: [3, 9],
  peak_input_log10: [-2.1, 2.3],
  peak_output_log10: [2, 2],
  valley_index: 6,
  valley_input_log10: 0,
  valley_output_log10: 0,
  peak_separation_log10: 4.4,
  left_prominence_log10: 1.2,
  right_prominence_log10: 1.1,
  left_half_prominence_width_log10: 0.8,
  right_half_prominence_width_log10: 0.9,
  central_half_prominence_interval_log10: 1.7,
  half_prominence_crossings_log10: [-2.8, -2, 1.9, 2.8],
  min_prominence_log10: 0.5,
};

const ropShapeResult = {
  schema_version: ROP_SHAPE_RESULT_VERSION,
  request_hash: ropRequestHash,
  normalized_request: ropNormalizedRequest,
  fixed_topology: {
    normalized_network: ropNetwork,
    network_ir_hash: ropNetworkHash,
    network_canonical_code: '[1]+[2]<->[1,2]',
    network_identity_semantics: 'canonical_code_available',
    input: 'tB',
    output: 'C',
    topology_preserved: true,
  },
  geometric_status: 'global_optimal_over_declared_cells',
  geometric_status_message: 'all declared cells evaluated',
  feasible: true,
  coverage: {
    eligible_path_count: 4,
    evaluated_path_count: 4,
    eligible_cell_count: 12,
    evaluated_cell_count: 12,
    feasible_cell_count: 5,
    replay_candidate_count: 1,
    replayed_count: 1,
    truncated: false,
    truncation_reasons: [],
  },
  compiled_edit: {
    compiler_version: 'bne-rop-shape-compiler/v1.0.0',
    source_intent_id: 'broaden-fixture',
    intent: ropEditIntent,
    constraints: [],
    objective: {
      id: 'broaden-fixture',
      kind: 'max_min_linear_operating_point_improvement',
      sense: 'maximize',
      groups: [
        { terms: [{ step: 1, coefficient: 1 }], reference_value: 1.2 },
        { terms: [{ step: 4, coefficient: 1 }], reference_value: 1.2 },
      ],
      reference_value: 0,
    },
    direction: {
      values: [-0.5, 0.5, 0, -0.5, 0.5],
      l2_norm: 1,
      normalization: 'not_normalized',
      alpha_units: 'declared_raw_direction_scale',
    },
    auxiliary_coordinates: ['alpha'],
    index_basis: 'zero_based_program_step',
    units: 'log10_input',
  },
  selected: {
    cell_id: `sha256:${'a'.repeat(64)}`,
    path_identity: 'path:7',
    path_idx: 7,
    witness_identity: [
      'step:0:vertex:1',
      'step:1:vertex:2',
      'step:2:vertex:3',
      'step:3:vertex:4',
      'step:4:vertex:5',
    ],
    witness_vertex_indices: [1, 2, 3, 4, 5],
    full_path_vertex_indices: [1, 2, 3, 4, 5],
    predicted_profile: [1, 0, -1, 0, 1],
    witness_input_log10: [-2.4, -1.2, 0, 1.2, 2.4],
    background_log_qK: {
      symbols: ['tA', 'Kd1', 'Kd2', 'Kd3'],
      values: [0, 0, 0, 0],
    },
    kd: [1],
    totals: { tA: 1 },
    primary_effect: {
      objective_id: 'broaden-fixture',
      sense: 'maximize',
      value: 2.35,
      effect_bound: 2.3,
      semantics: 'closed_polyhedral_support_limit_and_secondary_realization',
      effect_kind: 'balanced_minimum_improvement',
      reference_value: 0,
      closure_support_value: 2.4,
      cell_primary_value: 2.4,
      selected_value: 2.35,
      closure_support_improvement: 2.4,
      selected_improvement: 2.35,
      effect_tolerance: 0.1,
    },
    parameter_margin: {
      value: 0.42,
      basis: 'equality_feasible_log10_qK_subspace',
      coordinate_basis: 'unweighted_euclidean_log10_qK',
      dimension: 3,
      equality_rank: 1,
      coordinates: ['tB', 'Kd1', 'Kd2', 'Kd3'],
      basis_matrix: [
        [1, 0, 0],
        [0, 1, 0],
        [0, 0, 1],
        [0, 0, 0],
      ],
      rank_relative_tolerance: 1e-8,
      rank_absolute_threshold: 0,
      zero_dimensional_convention: 'not_applicable',
    },
    active_constraints: [
      {
        row_id: 'request:left_ear_gap',
        row_kind: 'witness_constraint',
        point_residual: 0,
        ball_residual: 0,
        normalized_residual: 0,
        dual: -0.75,
        shadow_price: 0.75,
        shadow_price_semantics: 'derivative_of_objective_value_with_respect_to_compiled_rhs',
      },
      {
        row_id: 'parameter_bound:Kd2:upper',
        row_kind: 'cell_inequality',
        point_residual: 0,
        ball_residual: 0,
        normalized_residual: 0,
        dual: null,
        shadow_price: null,
        shadow_price_semantics: 'derivative_of_objective_value_with_respect_to_compiled_rhs',
      },
    ],
    solver: {
      name: 'Clarabel',
      version: 'test',
      termination_status: 'OPTIMAL',
      validation_tolerance: 1e-7,
      active_tolerance: 1e-7,
      rank_tolerance: 1e-8,
      primary_termination_status: 'OPTIMAL',
      primary_message: 'optimal LP solution',
      secondary_message: 'optimal LP solution',
      core_status: 'optimal',
    },
  },
  directional_request_interval: {
    direction: [-1, -1, 0, 1, 1],
    direction_l2_norm: 2,
    normalization: 'not_normalized',
    alpha_units: 'declared_raw_direction_scale',
    cell_intervals: [],
    union_intervals: [
      { alpha_min: -0.5, alpha_max: 2.4, lower_unbounded: false, upper_unbounded: false },
    ],
    complete_over_evaluated_cells: true,
    numerical_error_count: 0,
    scope: 'declared_cells',
  },
  replay: {
    status: 'pass',
    request: ropReplayRequest,
    request_hash: ropReplayRequestHash,
    curve: ropReplayCurve,
    metrics: ropPassingMetrics,
    result_hash: ropReplayResultHash,
    complete: true,
    pass: true,
  },
  certificate_grade: 'exact-window-siso-rop-path-optimization',
  geometric_evidence_grade: 'exact_path_polyhedral',
  finite_replay_evidence_grade: 'sampled-forward-complete',
  solver_contract: {
    lp_backend: 'Clarabel',
    objective_policy: 'global_epsilon_lexicographic_effect_then_parameter_margin',
    parameter_margin_basis: 'equality_feasible_log10_qK_subspace',
    effect_limit_semantics: 'closed_polyhedral_support_limit',
    active_row_shadow_price_semantics:
      'objective_derivative_with_respect_to_compiled_rhs_not_primal_parameter_derivative',
    compiler_version: 'bne-rop-shape-compiler/v1.0.0',
  },
  result_hash: ropResultHash,
  artifact: {
    artifact_schema_version: 'bne-result/v1.0.0',
    kind: 'rop_shape_optimize',
    input_hashes: {
      request: ropRequestHash,
      network_ir: ropNetworkHash,
      designability_spec: ropDesignabilitySpecHash,
      reference: ropReferenceHash,
    },
    algorithm: {
      name: 'fixed_topology_rop_shape_optimizer',
      version: 'test',
      config_hash: ropRequestHash,
    },
    warnings: [],
    created_at: '2026-07-11T00:00:00Z',
  },
  warnings: [],
};

test('buildDesignScreenRequest parses qualitative signs for design_screen', () => {
  const req = buildDesignScreenRequest('sign', '+ - +');
  assert.equal(req.target_kind, undefined);
  assert.equal(req.target, undefined);
  assert.equal(req.designability_spec.schema_version, 'bne-designability/v1.0.0');
  assert.deepEqual(req.designability_spec.target.legacy_target, {
    target_kind: 'sign',
    target: '+-+',
  });
  assert.equal(req.designability_spec.candidate_budget.max_exact_placements, 0);
});

test('buildDesignScreenRequest parses exact RO targets', () => {
  const req = buildDesignScreenRequest('exact', '1, 0, -1');
  assert.deepEqual(req.designability_spec.target.legacy_target.target, [1, 0, -1]);
  assert.equal(req.designability_spec.candidate_budget.max_exact_placements, 3);
});

test('buildDesignScreenRequest rejects malformed exact RO targets', () => {
  assert.throws(() => buildDesignScreenRequest('exact', '1,'), /A precise target/);
  assert.throws(() => buildDesignScreenRequest('exact', '1, foo'), /A precise target/);
});

test('buildDesignScreenRequest rejects unknown target kinds', () => {
  assert.throws(() => buildDesignScreenRequest('unknown', '+-+'), /Unknown target kind/);
});

test('renderDesignScreenResults separates verified, screened, and minimal groups without wizard wording', () => {
  const html = renderDesignScreenResults('node-1', payload, null);
  assert.doesNotMatch(html, /Recommended for tuning/);
  assert.match(html, /Verified recommendations|Exploratory screened candidates/);
  assert.doesNotMatch(html, /Near misses/);
  assert.match(html, /exact-union-siso-rop/);
  assert.match(html, /proxy_only_not_recommendation/);
  assert.match(html, /no_atlas_volume_proxy/);
  assert.match(html, /Minimal certificates/);
  assert.match(html, /data-nid="\[1\]\+\[2\]&lt;-&gt;\[1,2\]"/);
  assert.match(html, /data-inp="tA"/);
  assert.match(html, /data-out="C_A_B"/);
  assert.doesNotMatch(html, /score 0\.82/);
  assert.doesNotMatch(html.toLowerCase(), /wizard/);
  assert.equal((html.match(/design-build-btn/g) || []).length, 1);
  assert.match(html, /2 \/ 12/);
  assert.match(html, /evaluated \/ eligible/);
  assert.match(html, /truncated/);
  assert.match(html, /Only <strong>2 of 12<\/strong> eligible candidates were evaluated/);
  assert.match(html, /results are not exhaustive/);
});

test('renderDesignScreenResults keeps v0.2 count rendering when v0.3 fields are absent', () => {
  const legacyPayload = {
    ...payload,
    schema_version: 'bne-design-screen/v0.2.0',
    eligible_count: undefined,
    evaluated_count: undefined,
    truncated: undefined,
  };
  const html = renderDesignScreenResults('node-legacy', legacyPayload, null);

  assert.match(html, /<strong>2<\/strong> screened/);
  assert.doesNotMatch(html, /evaluated \/ eligible/);
  assert.doesNotMatch(html, /design-screen-truncation-warning/);
  assert.doesNotMatch(html, /results are not exhaustive/);
});

test('renderDesignScreenResults shows v0.3 coverage without warning when evaluation is complete', () => {
  const completePayload = {
    ...payload,
    eligible_count: 12,
    evaluated_count: 12,
    truncated: false,
  };
  const html = renderDesignScreenResults('node-complete', completePayload, null);

  assert.match(html, /12 \/ 12/);
  assert.match(html, /evaluated \/ eligible/);
  assert.doesNotMatch(html, /design-screen-truncation-warning/);
  assert.doesNotMatch(html, /results are not exhaustive/);
});

test('renderDesignScreenResults never treats legacy recommended as verified', () => {
  const legacyOnly = {
    ...payload,
    verified_recommendations: undefined,
    recommended: payload.screened_candidates,
    screened_candidates: [],
    near_misses: [],
  };
  const html = renderDesignScreenResults('node-1', legacyOnly, null);

  assert.match(html, /No verified recommendation is available/);
  assert.doesNotMatch(html, /proxy_only_not_recommendation/);
  assert.equal((html.match(/design-build-btn/g) || []).length, 0);
});

test('renderDesignScreenResults refuses proxy-only cards in the verified section', () => {
  const proxyInVerified = {
    ...payload,
    verified_recommendations: [payload.screened_candidates[0]],
    screened_candidates: [],
    near_misses: [],
  };
  const html = renderDesignScreenResults('node-1', proxyInVerified, null);

  assert.match(html, /No verified recommendation is available/);
  assert.match(html, /Exploratory screened candidates/);
  assert.match(html, /proxy_only_not_recommendation/);
  assert.equal((html.match(/design-build-btn/g) || []).length, 0);
});

test('renderDesignScreenResults requires positive verified evidence for verified section', () => {
  const missingEvidence = {
    ...payload,
    verified_recommendations: [
      {
        ...payload.verified_recommendations[0],
        evidence_grade: undefined,
        screen_status: undefined,
      },
    ],
    screened_candidates: [],
    near_misses: [],
  };
  const html = renderDesignScreenResults('node-1', missingEvidence, null);

  assert.match(html, /No verified recommendation is available/);
  assert.match(html, /Exploratory screened candidates/);
  assert.match(html, /exact-union-siso-rop/);
  assert.equal((html.match(/design-build-btn/g) || []).length, 0);
});

test('renderDesignScreenResults exposes unsupported constraint audit instead of looking fully supported', () => {
  const audited = {
    ...payload,
    constraint_audit: [
      {
        path: '/target/temporal_dynamics/peak_width_seconds',
        kind: 'temporal_dynamics',
        support_level: 'unsupported',
        hard: true,
        reason: 'Temporal dynamics are schema-recognized but not solver-backed.',
      },
      {
        path: '/constraints/robustness/condition_number_max',
        kind: 'condition_number',
        support_level: 'unsupported',
        hard: false,
        reason: 'No unique parameter-to-breakpoint mapping is implemented.',
      },
    ],
  };
  const html = renderDesignScreenResults('node-1', audited, null);

  assert.match(html, /Constraint audit/);
  assert.match(html, /unsupported/);
  assert.match(html, /\/target\/temporal_dynamics\/peak_width_seconds/);
  assert.match(html, /Temporal dynamics are schema-recognized/);
  assert.match(html, /\/constraints\/robustness\/condition_number_max/);
  assert.match(html, /No verified recommendation is available/);
  assert.equal((html.match(/design-build-btn/g) || []).length, 0);
});

test('renderDesignScreenResults keeps unsupported audit visible when design is blocked', () => {
  const blocked = {
    ...payload,
    designable: false,
    verified_recommendations: [],
    screened_candidates: [],
    minimal_certificates: [],
    constraint_audit: [
      {
        path: '/target/temporal_dynamics',
        kind: 'temporal_dynamics',
        support_level: 'unsupported',
        hard: true,
        reason: 'Temporal dynamics are not implemented as solver constraints.',
      },
    ],
  };
  const html = renderDesignScreenResults('node-1', blocked, null);

  assert.match(html, /not designable/);
  assert.match(html, /Constraint audit/);
  assert.match(html, /\/target\/temporal_dynamics/);
  assert.match(html, /No verified recommendation is available/);
});

test('renderDesignScreenResults does not show proxy/default metric chips as verified metrics', () => {
  const proxyMetrics = {
    ...payload,
    verified_recommendations: [
      {
        ...payload.verified_recommendations[0],
        metrics: {
          chebyshev_radius: 0.9,
          chebyshev_radius_source: 'atlas_volume_radius_proxy_not_exact_feasible_region',
          tunable_volume: 0.7,
          tunable_volume_source: 'proxy_box_volume',
          ranking_margin_proxy: 0.5,
          condition_number: 4.2,
        },
      },
    ],
    screened_candidates: [],
    near_misses: [],
  };
  const html = renderDesignScreenResults('node-1', proxyMetrics, null);

  assert.match(html, /Verified recommendations/);
  assert.doesNotMatch(html, /rho 0\.9/);
  assert.doesNotMatch(html, /vol 0\.7/);
  assert.doesNotMatch(html, /cond 4\.2/);
  assert.doesNotMatch(html, /score 0\.82/);
  assert.doesNotMatch(html, /ranking_margin_proxy/);
});

test('renderDesignScreenResults shows witness-backed transition spacing as a verified metric', () => {
  const witnessSpacing = {
    ...payload,
    verified_recommendations: [
      {
        ...payload.verified_recommendations[0],
        metrics: {
          transition_spacing: 0.6,
          witness_min_spacing_decades: 0.6,
          witness_spacing_basis: 'requested_transition_order',
        },
      },
    ],
    screened_candidates: [],
    near_misses: [],
  };
  const html = renderDesignScreenResults('node-1', witnessSpacing, null);

  assert.match(html, /Verified recommendations/);
  assert.match(html, /space 0\.6/);
  assert.equal((html.match(/design-build-btn/g) || []).length, 1);
});

test('renderDesignScreenResults marks the selected canonical network', () => {
  const selection = {
    selectedNid: '[1]+[2]<->[1,2]',
    selectedInput: 'tB',
    selectedOutput: 'C_A_B',
    selectedCandidateKey: designCandidateKey('[1]+[2]<->[1,2]', 'tB', 'C_A_B'),
  };
  const html = renderDesignScreenResults('node-1', payload, selection);
  assert.equal((html.match(/design-net-row selected/g) || []).length, 1);
  assert.match(html, /data-inp="tB"/);
  assert.match(html, /emitting/);
});

test('ROP shape formatter and renderer keep geometry, margin, RHS sensitivity, and replay distinct', () => {
  const formatted = formatRopShapeOptimizationResult(ropShapeResult);
  assert.equal(formatted.geometricStatus, 'global_optimal_over_declared_cells');
  assert.equal(formatted.selected.primaryEffect.closureSupportImprovement, 2.4);
  assert.equal(formatted.selected.primaryEffect.selectedImprovement, 2.35);
  assert.equal(formatted.selected.parameterMargin.dimension, 3);
  assert.equal(formatted.selected.activeConstraints[0].shadowPrice, 0.75);
  assert.deepEqual(formatted.direction.intervals[0], {
    alphaMin: -0.5,
    alphaMax: 2.4,
    lowerUnbounded: false,
    upperUnbounded: false,
  });

  const html = renderRopShapeOptimizationResult(ropShapeResult);
  assert.match(html, /global_optimal_over_declared_cells/);
  assert.ok(html.includes('paths <strong>4 / 4'));
  assert.ok(html.includes('cells <strong>12 / 12'));
  assert.match(html, /closure support improvement <strong>2.4/);
  assert.match(html, /selected realized improvement <strong>2.35/);
  assert.match(html, /parameter-only margin <strong>0.42/);
  assert.match(html, /dimension <strong>3/);
  assert.match(html, /request:left_ear_gap/);
  assert.match(html, /compiled-RHS sensitivity 0.75/);
  assert.match(html, /not a primal biochemical-parameter derivative/);
  assert.match(html, /Directional request interval/);
  assert.match(html, /L2 norm 2 · not normalized/);
  assert.ok(html.includes('[-0.5, 2.4]'));
  assert.match(html, /complete <strong>yes/);
  assert.match(html, /pass <strong>yes/);
  assert.match(html, /sampled peaks <strong>-2.1, 2.3/);
  assert.match(html, /sampled separation <strong>4.4/);
  assert.doesNotMatch(html, /results are not exhaustive/);
});

test('ROP shape renderer fails closed on unknown version and malformed evidence', () => {
  assert.throws(
    () => renderRopShapeOptimizationResult({
      ...ropShapeResult,
      schema_version: 'bne-rop-shape-optimization/v2.0.0',
    }),
    /unsupported schema_version.*v2\.0\.0/,
  );
  assert.throws(
    () => formatRopShapeOptimizationResult({
      ...ropShapeResult,
      coverage: {
        ...ropShapeResult.coverage,
        evaluated_cell_count: 13,
      },
    }),
    /evaluated_cell_count exceeds eligible_cell_count/,
  );
  assert.throws(
    () => formatRopShapeOptimizationResult({
      ...ropShapeResult,
      selected: {
        ...ropShapeResult.selected,
        parameter_margin: {
          ...ropShapeResult.selected.parameter_margin,
          basis: 'augmented_theta_tau_space',
        },
      },
    }),
    /parameter_margin\.basis is not parameter-only/,
  );
});

test('ROP shape renderer rejects provenance-stripped passing forgeries', () => {
  const forgedSubset = {
    schema_version: ROP_SHAPE_RESULT_VERSION,
    geometric_status: 'global_optimal_over_declared_cells',
    geometric_status_message: 'forged subset',
    feasible: true,
    coverage: {
      eligible_path_count: 1,
      evaluated_path_count: 1,
      eligible_cell_count: 1,
      evaluated_cell_count: 1,
      feasible_cell_count: 1,
      replay_candidate_count: 1,
      replayed_count: 1,
      truncated: false,
      truncation_reasons: [],
    },
    selected: {
      cell_id: 'not-a-hash',
      path_identity: 'not-a-path-id',
      primary_effect: {
        effect_kind: 'linear',
        effect_tolerance: 0.1,
        closure_support_improvement: 1,
        selected_improvement: 1,
      },
      parameter_margin: {
        value: 0.2,
        basis: 'equality_feasible_log10_qK_subspace',
        coordinate_basis: 'unweighted_euclidean_log10_qK',
        dimension: 1,
        equality_rank: 0,
        coordinates: ['x'],
        zero_dimensional_convention: 'not_applicable',
      },
      active_constraints: [],
    },
    replay: {
      status: 'pass',
      complete: true,
      pass: true,
      metrics: { status: 'failed', reason: 'curve invalid' },
    },
    warnings: [],
  };
  assert.throws(
    () => formatRopShapeOptimizationResult(forgedSubset),
    /request_hash is required/,
  );

  assert.throws(
    () => formatRopShapeOptimizationResult({
      ...ropShapeResult,
      replay: {
        status: 'pass',
        complete: true,
        pass: true,
        metrics: { status: 'failed', reason: 'curve invalid' },
      },
    }),
    /replay\.request is required/,
  );
});

test('ROP shape renderer binds v1 hashes, topology, compiler, evidence, solver, and artifact identity', () => {
  assert.throws(
    () => formatRopShapeOptimizationResult({ ...ropShapeResult, result_hash: 'not-a-sha256' }),
    /result_hash must be a lowercase SHA-256/,
  );
  assert.throws(
    () => formatRopShapeOptimizationResult({
      ...ropShapeResult,
      normalized_request: {
        ...ropNormalizedRequest,
        schema_version: 'bne-rop-shape-optimize-request/v2.0.0',
      },
    }),
    /normalized_request\.schema_version/,
  );
  assert.throws(
    () => formatRopShapeOptimizationResult({
      ...ropShapeResult,
      fixed_topology: { ...ropShapeResult.fixed_topology, topology_preserved: false },
    }),
    /topology_preserved must be true/,
  );
  assert.throws(
    () => formatRopShapeOptimizationResult({
      ...ropShapeResult,
      compiled_edit: { ...ropShapeResult.compiled_edit, source_intent_id: 'forged-intent' },
    }),
    /source intent id conflicts/,
  );
  assert.throws(
    () => formatRopShapeOptimizationResult({
      ...ropShapeResult,
      finite_replay_evidence_grade: 'sampled-forward-failed',
    }),
    /finite_replay_evidence_grade conflicts/,
  );
  assert.throws(
    () => formatRopShapeOptimizationResult({
      ...ropShapeResult,
      solver_contract: { ...ropShapeResult.solver_contract, lp_backend: 'forged' },
    }),
    /solver_contract\.lp_backend is unsupported/,
  );
  assert.throws(
    () => formatRopShapeOptimizationResult({
      ...ropShapeResult,
      artifact: {
        ...ropShapeResult.artifact,
        algorithm: { ...ropShapeResult.artifact.algorithm, config_hash: '8'.repeat(64) },
      },
    }),
    /config_hash must equal the normalized request hash/,
  );
});

test('ROP shape selected realization requires the full v1 witness and optimal solver contract', () => {
  for (const field of ['solver', 'witness_identity', 'witness_input_log10']) {
    const selected = { ...ropShapeResult.selected };
    delete selected[field];
    assert.throws(
      () => formatRopShapeOptimizationResult({ ...ropShapeResult, selected }),
      new RegExp(`selected\\.${field} is required`),
    );
  }
  assert.throws(
    () => formatRopShapeOptimizationResult({
      ...ropShapeResult,
      selected: {
        ...ropShapeResult.selected,
        solver: { ...ropShapeResult.selected.solver, core_status: 'infeasible' },
      },
    }),
    /selected\.solver\.core_status must be optimal/,
  );
  assert.throws(
    () => formatRopShapeOptimizationResult({
      ...ropShapeResult,
      selected: {
        ...ropShapeResult.selected,
        primary_effect: {
          ...ropShapeResult.selected.primary_effect,
          effect_tolerance: 0.2,
        },
      },
    }),
    /effect_tolerance conflicts with normalized_request\.optimization/,
  );
});

test('ROP shape normalized request locks reaction-order dimensions and replay bounds', () => {
  assert.throws(
    () => formatRopShapeOptimizationResult({
      ...ropShapeResult,
      normalized_request: {
        ...ropNormalizedRequest,
        designability_spec: {
          ...ropNormalizedRequest.designability_spec,
          target: {
            behavior_spec: {
              ...ropNormalizedRequest.designability_spec.target.behavior_spec,
              feature_space: 'forged',
            },
          },
        },
      },
    }),
    /feature_space must be reaction_order/,
  );
  assert.throws(
    () => formatRopShapeOptimizationResult({
      ...ropShapeResult,
      normalized_request: {
        ...ropNormalizedRequest,
        reference: {
          ...ropNormalizedRequest.reference,
          operating_points_log10: [-1, 1],
        },
      },
    }),
    /operating points must match the target program length/,
  );
  assert.throws(
    () => formatRopShapeOptimizationResult({
      ...ropShapeResult,
      normalized_request: {
        ...ropNormalizedRequest,
        reference: { ...ropNormalizedRequest.reference, kd: [1, 2] },
      },
    }),
    /reference kd count must match NetworkIR reactions/,
  );
  assert.throws(
    () => formatRopShapeOptimizationResult({
      ...ropShapeResult,
      normalized_request: {
        ...ropNormalizedRequest,
        replay: { ...ropNormalizedRequest.replay, input_window_log10: [-21, 3] },
      },
    }),
    /within \[-20, 20\]/,
  );
});

test('ROP shape replay pass requires the canonical request and complete valid curve', () => {
  assert.throws(
    () => formatRopShapeOptimizationResult({
      ...ropShapeResult,
      replay: {
        ...ropShapeResult.replay,
        request: {
          ...ropReplayRequest,
          endpoint: '/api/placer_curve',
        },
      },
    }),
    /canonical POST \/api\/v1\/placer_curve/,
  );
  assert.throws(
    () => formatRopShapeOptimizationResult({
      ...ropShapeResult,
      replay: {
        ...ropShapeResult.replay,
        curve: {
          ...ropReplayCurve,
          valid: [false, ...ropReplayCurve.valid.slice(1)],
        },
      },
    }),
    /every sample valid/,
  );
  assert.throws(
    () => formatRopShapeOptimizationResult({
      ...ropShapeResult,
      replay: {
        ...ropShapeResult.replay,
        metrics: {
          ...ropPassingMetrics,
          status: 'prominence_below_minimum',
        },
      },
    }),
    /metrics\.status=pass must agree/,
  );
});

test('ROP shape renderer marks path/cell truncation as non-exhaustive', () => {
  const warnings = ['Optimization is best over evaluated cells.'];
  const truncated = {
    ...ropShapeResult,
    geometric_status: 'best_over_evaluated_cells',
    geometric_status_message: 'best evaluated cell only',
    coverage: {
      ...ropShapeResult.coverage,
      evaluated_path_count: 3,
      evaluated_cell_count: 8,
      feasible_cell_count: 3,
      truncated: true,
      truncation_reasons: ['max_cells'],
    },
    directional_request_interval: {
      ...ropShapeResult.directional_request_interval,
      scope: 'evaluated_cells',
    },
    artifact: { ...ropShapeResult.artifact, warnings },
    warnings,
  };
  const html = renderRopShapeOptimizationResult(truncated);
  assert.match(html, /best_over_evaluated_cells/);
  assert.match(html, /truncated/);
  assert.match(html, /omitted cells remain unknown/);
  assert.match(html, /Reasons: max_cells/);
  assert.ok(html.includes('paths <strong>3 / 4'));
  assert.ok(html.includes('cells <strong>8 / 12'));
  assert.throws(
    () => formatRopShapeOptimizationResult({
      ...truncated,
      geometric_status: 'global_optimal_over_declared_cells',
    }),
    /declared-cell geometric status cannot have truncated coverage/,
  );
});

test('ROP shape renderer does not promote failed or partial replay', () => {
  const warnings = ['The geometric result did not pass finite replay.'];
  const failedReplay = {
    ...ropShapeResult,
    replay: {
      ...ropShapeResult.replay,
      status: 'failed',
      complete: true,
      pass: false,
      metrics: {
        ...ropPassingMetrics,
        status: 'prominence_below_minimum',
        reason: 'right sampled peak did not meet prominence threshold',
        pass: false,
      },
    },
    finite_replay_evidence_grade: 'sampled-forward-failed',
    artifact: { ...ropShapeResult.artifact, warnings },
    warnings,
  };
  const html = renderRopShapeOptimizationResult(failedReplay);
  assert.match(html, /Finite replay/);
  assert.match(html, />failed</);
  assert.match(html, /complete <strong>yes/);
  assert.match(html, /pass <strong>no/);
  assert.match(html, /did not provide a complete passing shape/);
  assert.match(html, /geometric evidence is kept separate/);
  assert.match(html, /right sampled peak did not meet prominence threshold/);
  assert.throws(
    () => formatRopShapeOptimizationResult({
      ...failedReplay,
      replay: { ...failedReplay.replay, pass: true },
    }),
    /only replay\.status=pass may set replay\.pass=true/,
  );
});

test('Design Screen only labels exact canonical optimization handoffs as shape optimizable', () => {
  const canonicalHandoff = {
    endpoint: '/api/v1/rop_shape_optimize',
    method: 'POST',
    body_template: {
      schema_version: 'bne-rop-shape-optimize-request/v1.0.0',
      edit_intent: null,
    },
    optimizer_result: { geometric_status: 'must-not-render' },
  };
  const ready = {
    ...payload,
    verified_recommendations: [{
      ...payload.verified_recommendations[0],
      optimization_handoff_template: canonicalHandoff,
    }],
    screened_candidates: [],
  };
  const readyHtml = renderDesignScreenResults('node-optimizer-ready', ready, null);
  assert.match(readyHtml, /shape optimizable/);
  assert.equal((readyHtml.match(/design-shape-optimization-ready/g) || []).length, 1);
  assert.doesNotMatch(readyHtml, /must-not-render/);
  assert.equal((readyHtml.match(/Verified recommendations/g) || []).length, 1);

  const legacy = {
    ...ready,
    verified_recommendations: [{
      ...ready.verified_recommendations[0],
      optimization_handoff_template: {
        ...canonicalHandoff,
        endpoint: '/api/rop_shape_optimize',
      },
    }],
  };
  const legacyHtml = renderDesignScreenResults('node-legacy-optimizer', legacy, null);
  assert.doesNotMatch(legacyHtml, /shape optimizable/);
});

console.log(`\nAll ${passed} design target render tests passed.`);
