import assert from 'node:assert/strict';
import {
  buildDesignScreenRequest,
  designCandidateKey,
  renderDesignScreenResults,
} from '../public/js/design-screen-render.js';

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

console.log(`\nAll ${passed} design target render tests passed.`);
