import assert from 'node:assert/strict';
import fs from 'node:fs';

globalThis.window = {
  matchMedia: () => null,
  addEventListener() {},
  location: { protocol: 'http:', hostname: '127.0.0.1', port: '8000' },
  sessionStorage: { getItem: () => null, setItem() {} },
  crypto: { randomUUID: () => 'rop-shape-node-test' },
  setTimeout,
};

const elements = new Map();
globalThis.document = {
  readyState: 'loading',
  documentElement: { dataset: {}, style: { setProperty() {} } },
  getElementById(id) { return elements.get(id) || null; },
  addEventListener() {},
  querySelectorAll() { return []; },
};

// Load the registry first, matching the production module graph. The node
// module imports shared node/workspace helpers that intentionally refer back to
// the already-instantiating registry.
await import('../public/js/node-types/index.js');
const {
  ROP_SHAPE_REFERENCE_VERSION,
  ROP_SHAPE_REQUEST_VERSION,
  ROP_SHAPE_ENDPOINT,
  ROP_SHAPE_TYPES,
  parseRopShapeStepList,
  buildBroadenEditIntent,
  buildSeparateEditIntent,
  buildWidenCenterEditIntent,
  buildTranslateGroupEditIntent,
  buildLinearWitnessEditIntent,
  buildCanonicalRopShapeRequestFromHandoff,
  admitRopShapeResultForRequest,
  validatePinnedRopShapeReferenceArtifact,
  invalidateRopShapePreparedRequest,
  prepareRopShapeRequest,
  executeRopShapeResult,
  inspectRopShapeLifecycle,
  restoreRopShapeResultView,
} = await import('../public/js/node-types/rop-shape.js');
const {
  advanceWorkspaceRuntimeEpoch,
  nodeRegistry,
  setConnections,
} = await import('../public/js/state.js');

let passed = 0;
async function test(name, fn) {
  await fn();
  passed += 1;
  console.log(`  ok - ${name}`);
}

const NETWORK_HASH = 'a'.repeat(64);
const REFERENCE_HASH = 'b'.repeat(64);
const CELL_HASH = `sha256:${'c'.repeat(64)}`;
const NETWORK_CODE = '[1]+[2]<->[1,2]';
const BENCHMARK = JSON.parse(fs.readFileSync(
  new URL('../../benchmarks/rop_shape_control/cat_fixed_topology_results.json', import.meta.url),
  'utf8',
));

function makeArtifact() {
  const network = {
    ir_schema_version: 'bne-ir/v1.0.0',
    label: 'pinned-network',
    species: [
      { name: 'A', role: 'free' },
      { name: 'B', role: 'free' },
      { name: 'C_A_B', role: 'complex' },
    ],
    reactions: [{
      formula: 'A + B <-> C_A_B', kd: 1, kind: 'binding', reversible: true, metadata: {},
    }],
    observables: [{ name: 'C_A_B', expression: 'C_A_B' }],
    parameter_distributions: [{ name: 'Kd_1', distribution: 'fixed', value: 1 }],
    compartments: [],
    provenance: {},
    extensions: {},
  };
  const reference = {
    reference_hash: REFERENCE_HASH,
    network_ir_hash: NETWORK_HASH,
    operating_points_log10: [-3, -2, -1, 0, 1, 2, 3],
    kd: [1],
    totals: { tA: 1 },
    path_identity: 'path:7',
    cell_id: CELL_HASH,
  };
  const fixed = {
    network,
    network_ir_hash: NETWORK_HASH,
    network_canonical_code: NETWORK_CODE,
    input: 'tA',
    output: 'C_A_B',
    reference,
    evidence_scope: 'one pinned exact-window Design Screen cell',
  };
  return {
    schema_version: ROP_SHAPE_REFERENCE_VERSION,
    selected_candidate_key: `${NETWORK_CODE}::tA::C_A_B`,
    fixed_topology_reference: fixed,
    optimization_handoff_template: {
      endpoint: ROP_SHAPE_ENDPOINT,
      method: 'POST',
      required_fill: ['edit_intent'],
      body_template: {
        schema_version: ROP_SHAPE_REQUEST_VERSION,
        network,
        expected_network_ir_hash: NETWORK_HASH,
        designability_spec: {
          schema_version: 'bne-designability/v1.0.0',
          source: { kind: 'test_fixture' },
          target: {
            behavior_spec: {
              feature_space: 'reaction_order',
              input: 'tA',
              output: 'C_A_B',
              program: [1, 0, -1, 0, 1, 0, -1].map(value => ({
                kind: 'reaction_order', operator: '=', value, hard: true,
              })),
              input_window: { input_log10: [-3, 3], hard: true },
            },
          },
          constraints: {
            parameter_bounds: {
              basis: 'log10_qK',
              by_class: { kd: [-10, 10], total: [-5, 5] },
            },
          },
          candidate_budget: {},
          ranking_policy: { verified_only: true },
          audit_policy: {
            unsupported: 'block_if_hard', path_format: 'json_pointer', include_supported: true,
          },
        },
        reference,
        edit_intent: null,
        optimization: { minimum_parameter_margin: 0, effect_tolerance: 0.02 },
        work_budget: {
          max_paths: 2000,
          max_cells: 256,
          max_replays: 1,
          require_exhaustive: false,
        },
        replay: {
          input_window_log10: [-3, 3],
          sample_points: 281,
          require_complete: true,
          store_curve: true,
          metrics: [{ kind: 'two_peak', min_prominence_log10: 0.5 }],
        },
      },
    },
    evidence_scope: fixed.evidence_scope,
  };
}

function copy(value) {
  return JSON.parse(JSON.stringify(value));
}

function benchmarkSavedPair() {
  const result = copy(BENCHMARK.edits[0].direct_lp.result);
  // The benchmark was produced through the portable job budget. A browser
  // workspace artifact uses the narrower synchronous HTTP budget.
  result.normalized_request.work_budget.max_cells = 256;
  // Current workspace handoffs pin the selected cell explicitly; the frozen
  // benchmark predates that producer-side strengthening.
  result.normalized_request.reference.cell_id = result.selected.cell_id;
  return { request: copy(result.normalized_request), result };
}

function artifactFromSavedPair(pair) {
  const { request, result } = pair;
  const fixed = {
    network: copy(request.network),
    network_ir_hash: request.expected_network_ir_hash,
    network_canonical_code: result.fixed_topology.network_canonical_code,
    input: result.fixed_topology.input,
    output: result.fixed_topology.output,
    reference: copy(request.reference),
    evidence_scope: 'saved benchmark fixture used for workspace race admission',
  };
  return {
    schema_version: ROP_SHAPE_REFERENCE_VERSION,
    selected_candidate_key: `${fixed.network_canonical_code}::${fixed.input}::${fixed.output}`,
    fixed_topology_reference: fixed,
    optimization_handoff_template: {
      endpoint: ROP_SHAPE_ENDPOINT,
      method: 'POST',
      required_fill: ['edit_intent'],
      body_template: { ...copy(request), edit_intent: null },
    },
    evidence_scope: fixed.evidence_scope,
  };
}

function installEditorControls(nodeId, request) {
  const intent = request.edit_intent;
  const fields = {
    '-rop-shape-kind': { value: intent.kind },
    '-intent-id': { value: intent.id },
    '-left-span': { value: intent.left_span_steps?.join(', ') || '0, 2' },
    '-right-span': { value: intent.right_span_steps?.join(', ') || '4, 6' },
    '-steps': { value: intent.steps?.join(', ') || '1, 5' },
    '-group': { value: intent.group_steps?.join(', ') || '4, 5, 6' },
    '-preserve': { value: intent.preserve_steps?.join(', ') || '0, 1, 2, 3' },
    '-anchor-step': { value: String(intent.anchor_step ?? 3) },
    '-anchor-tolerance': {
      value: String(intent.anchor_tolerance_log10 ?? intent.preserve_tolerance_log10 ?? 0.2),
    },
    '-midpoint-tolerance': { value: String(intent.preserve_midpoint_tolerance_log10 ?? 0.2) },
    '-effect-tolerance': { value: String(request.optimization.effect_tolerance) },
    '-minimum-parameter-margin': { value: String(request.optimization.minimum_parameter_margin) },
    '-sense': { value: intent.sense || 'positive' },
    '-shared': { checked: intent.shared_magnitude ?? intent.shared_shift ?? true },
    '-max-paths': { value: String(request.work_budget.max_paths) },
    '-max-cells': { value: String(request.work_budget.max_cells) },
    '-max-replays': { value: String(request.work_budget.max_replays) },
    '-require-exhaustive': { checked: request.work_budget.require_exhaustive },
    '-replay-sample-points': { value: String(request.replay.sample_points) },
    '-replay-min-prominence': { value: String(request.replay.metrics[0].min_prominence_log10) },
    '-linear-intent-json': {
      value: JSON.stringify({ constraints: [], objective: {
        id: 'unused', sense: 'maximize', terms: [{ step: 1, coefficient: 1 }],
      } }),
    },
  };
  for (const [suffix, field] of Object.entries(fields)) elements.set(`${nodeId}${suffix}`, field);
  elements.set(`${nodeId}-rop-shape-status`, { innerHTML: '' });
}

function loadingElement() {
  const classes = new Set();
  return {
    classList: {
      add(value) { classes.add(value); },
      remove(value) { classes.delete(value); },
      contains(value) { return classes.has(value); },
    },
  };
}

async function waitForPending(pending, count) {
  for (let attempt = 0; attempt < 20 && pending.length < count; attempt += 1) {
    await Promise.resolve();
  }
  assert.equal(pending.length, count, `expected ${count} pending fetch calls`);
}

await test('workspace node definitions expose the exact shape artifact ports and actions', () => {
  const config = ROP_SHAPE_TYPES['rop-shape-edit-config'];
  const result = ROP_SHAPE_TYPES['rop-shape-result'];
  assert.equal(config.category, 'parameter');
  assert.deepEqual(config.inputs, [{ port: 'rop-shape-reference', type: 'ROPShapeReferenceArtifact', label: 'ROP Shape Reference' }]);
  assert.deepEqual(config.outputs, [{ port: 'rop-shape-request', type: 'ROPShapeRequestArtifact', label: 'ROP Shape Request' }]);
  assert.equal(typeof config.prepare, 'function');
  assert.equal(config.execute, undefined);
  assert.equal(result.category, 'result');
  assert.deepEqual(result.inputs, [{ port: 'rop-shape-request', type: 'ROPShapeRequestArtifact', label: 'ROP Shape Request' }]);
  assert.deepEqual(result.outputs, [{ port: 'rop-shape-result', type: 'ROPShapeResultArtifact', label: 'ROP Shape Result' }]);
  assert.equal(typeof result.execute, 'function');

  const configHtml = config.createBody('shape-config');
  for (const suffix of [
    '-rop-shape-kind', '-intent-id', '-left-span', '-right-span', '-steps', '-group', '-preserve',
    '-anchor-step', '-anchor-tolerance', '-midpoint-tolerance', '-effect-tolerance',
    '-minimum-parameter-margin', '-sense', '-shared', '-max-paths', '-max-cells', '-max-replays',
    '-require-exhaustive', '-replay-sample-points', '-replay-min-prominence', '-linear-intent-json',
  ]) {
    assert.match(configHtml, new RegExp(`id="shape-config${suffix}"`));
  }
  assert.match(configHtml, /data-action="updateRopShapeIntentVisibility"/);
  assert.match(configHtml, /data-action="prepareRopShapeRequest"/);
  assert.match(configHtml, /id="shape-config-intent-id" class="auto-update" value="shape-edit"/);
  assert.match(result.createBody('shape-result'), /data-action="executeRopShapeResult"/);
});

await test('step parsing is strict, bounded, ordered by the caller, and duplicate-free', () => {
  assert.deepEqual(parseRopShapeStepList('0, 2, 6', { witnessCount: 7 }), [0, 2, 6]);
  assert.deepEqual(parseRopShapeStepList([0, 2], { exactLength: 2, witnessCount: 3 }), [0, 2]);
  assert.throws(() => parseRopShapeStepList('0,,2'), /comma-separated non-negative integers/);
  assert.throws(() => parseRopShapeStepList('0, 1.5'), /comma-separated non-negative integers/);
  assert.throws(() => parseRopShapeStepList([0, '2']), /array entries must be JSON integers/);
  assert.throws(() => parseRopShapeStepList('0, 0'), /duplicate/);
  assert.throws(() => parseRopShapeStepList('0, 7', { witnessCount: 7 }), /at most 6/);
});

await test('all five edit-intent builders emit only canonical schema fields', () => {
  assert.deepEqual(buildBroadenEditIntent({
    id: 'broaden', leftSpanSteps: '0, 2', rightSpanSteps: '4, 6', witnessCount: 7,
  }), {
    id: 'broaden', kind: 'broaden', left_span_steps: [0, 2], right_span_steps: [4, 6],
    shared_magnitude: true,
  });
  assert.deepEqual(buildSeparateEditIntent({
    id: 'separate', steps: '1, 5', preserveMidpointTolerance: 0.2, witnessCount: 7,
  }), {
    id: 'separate', kind: 'separate', steps: [1, 5], preserve_midpoint_tolerance_log10: 0.2,
  });
  assert.deepEqual(buildWidenCenterEditIntent({
    id: 'widen', steps: '2, 4', anchorStep: 3, anchorTolerance: 0.1, witnessCount: 7,
  }), {
    id: 'widen', kind: 'widen_center', steps: [2, 4], anchor_step: 3,
    anchor_tolerance_log10: 0.1,
  });
  assert.deepEqual(buildTranslateGroupEditIntent({
    id: 'translate', groupSteps: '4, 5, 6', preserveSteps: '0, 1, 2',
    preserveTolerance: 0.1, sense: 'negative', witnessCount: 7,
  }), {
    id: 'translate', kind: 'translate_group', group_steps: [4, 5, 6],
    preserve_steps: [0, 1, 2], preserve_tolerance_log10: 0.1,
    sense: 'negative', shared_shift: true,
  });
  assert.deepEqual(buildLinearWitnessEditIntent({
    id: 'linear', witnessCount: 7, definition: {
      constraints: [{
        id: 'anchor', terms: [{ step: 3, coefficient: 1 }],
        operator: '=', rhs_log10: 0, hard: true,
      }],
      objective: {
        id: 'separation', sense: 'maximize',
        terms: [{ step: 5, coefficient: 1 }, { step: 1, coefficient: -1 }],
      },
    },
  }), {
    id: 'linear', kind: 'linear_witness',
    constraints: [{
      id: 'anchor', terms: [{ step: 3, coefficient: 1 }],
      operator: '=', rhs_log10: 0, hard: true,
    }],
    objective: {
      id: 'separation', sense: 'maximize',
      terms: [{ step: 5, coefficient: 1 }, { step: 1, coefficient: -1 }],
    },
  });
});

await test('edit-intent builders fail closed on ambiguous or non-finite input', () => {
  assert.throws(() => buildBroadenEditIntent({
    id: 'bad', leftSpanSteps: [0, 2], rightSpanSteps: [2, 6], witnessCount: 7,
  }), /disjoint/);
  assert.throws(() => buildBroadenEditIntent({
    id: 'bad', leftSpanSteps: [0, 2], rightSpanSteps: [4, 6], sharedMagnitude: false,
  }), /literal true/);
  assert.throws(() => buildWidenCenterEditIntent({
    id: 'bad', steps: [2, 4], anchorStep: 2, anchorTolerance: 0.1,
  }), /differ from both/);
  assert.throws(() => buildTranslateGroupEditIntent({
    id: 'bad', groupSteps: [4], preserveSteps: [0], preserveTolerance: Infinity, sense: 'positive',
  }), /must be finite|finite decimal/);
  assert.throws(() => buildLinearWitnessEditIntent({
    id: 'bad', definition: {
      constraints: [],
      objective: { id: 'o', sense: 'maximize', terms: [{ step: 1, coefficient: '1' }] },
    },
  }), /JSON number/);
  assert.throws(() => buildLinearWitnessEditIntent({
    id: 'bad', definition: {
      constraints: [{
        id: 'c', terms: [{ step: 1, coefficient: 1 }],
        operator: '<=', rhs_log10: '0.2', hard: true,
      }],
      objective: { id: 'o', sense: 'maximize', terms: [{ step: 1, coefficient: 1 }] },
    },
  }), /rhs_log10.*JSON number/);
});

await test('canonical handoff merge deep-copies input and applies bounded numeric overrides', () => {
  const artifact = makeArtifact();
  const before = copy(artifact);
  const intent = buildSeparateEditIntent({
    id: 'separate', steps: [1, 5], preserveMidpointTolerance: 0.2, witnessCount: 7,
  });
  const request = buildCanonicalRopShapeRequestFromHandoff(artifact, intent, {
    optimization: { minimumParameterMargin: 0.1, effectTolerance: 0.01 },
    workBudget: { maxPaths: 20, maxCells: 64, maxReplays: 2, requireExhaustive: true },
    replay: { samplePoints: 101, minProminence: 0.25 },
  });
  assert.deepEqual(artifact, before, 'merge must not mutate the upstream artifact');
  assert.notEqual(request.network, artifact.fixed_topology_reference.network);
  assert.deepEqual(request.edit_intent, intent);
  assert.deepEqual(request.optimization, { minimum_parameter_margin: 0.1, effect_tolerance: 0.01 });
  assert.deepEqual(request.work_budget, {
    max_paths: 20, max_cells: 64, max_replays: 2, require_exhaustive: true,
  });
  assert.equal(request.replay.sample_points, 101);
  assert.equal(request.replay.metrics[0].min_prominence_log10, 0.25);
  request.network.label = 'mutated-output';
  assert.equal(artifact.fixed_topology_reference.network.label, 'pinned-network');
});

await test('workspace optimization overrides cannot weaken the Design Screen margin floor', () => {
  const artifact = makeArtifact();
  artifact.optimization_handoff_template.body_template.optimization.minimum_parameter_margin = 0.25;
  const intent = buildSeparateEditIntent({
    id: 'separate', steps: [1, 5], preserveMidpointTolerance: 0.2, witnessCount: 7,
  });
  const inherited = buildCanonicalRopShapeRequestFromHandoff(artifact, intent);
  assert.equal(inherited.optimization.minimum_parameter_margin, 0.25);
  assert.throws(() => buildCanonicalRopShapeRequestFromHandoff(artifact, intent, {
    optimization: { minimumParameterMargin: 0.249 },
  }), /at least the Design Screen floor 0\.25/);
  const stronger = buildCanonicalRopShapeRequestFromHandoff(artifact, intent, {
    optimization: { minimumParameterMargin: 0.4 },
  });
  assert.equal(stronger.optimization.minimum_parameter_margin, 0.4);
});

await test('pinned reference admission rejects stale, noncanonical, or internally inconsistent artifacts', () => {
  const wrongVersion = makeArtifact();
  wrongVersion.schema_version = 'bne-workspace-rop-shape-reference/v2.0.0';
  assert.throws(() => validatePinnedRopShapeReferenceArtifact(wrongVersion), /schema_version/);

  const wrongEndpoint = makeArtifact();
  wrongEndpoint.optimization_handoff_template.endpoint = '/api/rop_shape_optimize';
  assert.throws(() => validatePinnedRopShapeReferenceArtifact(wrongEndpoint), /handoff.endpoint/);

  const filled = makeArtifact();
  filled.optimization_handoff_template.body_template.edit_intent = { kind: 'separate' };
  assert.throws(() => validatePinnedRopShapeReferenceArtifact(filled), /must leave it null/);

  const wrongFill = makeArtifact();
  wrongFill.optimization_handoff_template.required_fill = ['edit_intent', 'network'];
  assert.throws(() => validatePinnedRopShapeReferenceArtifact(wrongFill), /required_fill/);

  const wrongSelection = makeArtifact();
  wrongSelection.selected_candidate_key = 'old::tA::C_A_B';
  assert.throws(() => validatePinnedRopShapeReferenceArtifact(wrongSelection), /pinned network\/input\/output identity/);

  const mismatchedNetwork = makeArtifact();
  mismatchedNetwork.fixed_topology_reference.network = { label: 'other' };
  assert.throws(() => validatePinnedRopShapeReferenceArtifact(mismatchedNetwork), /does not match fixed_topology_reference.network/);

  const missingCell = makeArtifact();
  delete missingCell.optimization_handoff_template.body_template.reference.cell_id;
  delete missingCell.fixed_topology_reference.reference.cell_id;
  assert.throws(() => validatePinnedRopShapeReferenceArtifact(missingCell), /cell_id/);

  const nonfinite = makeArtifact();
  nonfinite.optimization_handoff_template.body_template.reference.operating_points_log10[0] = Infinity;
  assert.throws(() => validatePinnedRopShapeReferenceArtifact(nonfinite), /non-finite/);

  const wrongIrVersion = makeArtifact();
  wrongIrVersion.optimization_handoff_template.body_template.network.ir_schema_version = 'bne-ir/v2.0.0';
  wrongIrVersion.fixed_topology_reference.network.ir_schema_version = 'bne-ir/v2.0.0';
  assert.throws(() => validatePinnedRopShapeReferenceArtifact(wrongIrVersion), /ir_schema_version/);

  const wrongSpecVersion = makeArtifact();
  wrongSpecVersion.optimization_handoff_template.body_template.designability_spec.schema_version =
    'bne-designability/v2.0.0';
  assert.throws(() => validatePinnedRopShapeReferenceArtifact(wrongSpecVersion), /designability_spec.schema_version/);

  const ambiguousScreenRadius = makeArtifact();
  ambiguousScreenRadius.optimization_handoff_template.body_template.designability_spec
    .constraints.robustness = { min_chebyshev_radius: 0.1 };
  assert.throws(() => validatePinnedRopShapeReferenceArtifact(ambiguousScreenRadius),
    /min_chebyshev_radius.*projected/);
});

await test('result admission binds normalized_request to the exact request executed by the node', () => {
  const artifact = makeArtifact();
  const intent = buildSeparateEditIntent({
    id: 'separate', steps: [1, 5], preserveMidpointTolerance: 0.2, witnessCount: 7,
  });
  const request = buildCanonicalRopShapeRequestFromHandoff(artifact, intent);
  const admitted = admitRopShapeResultForRequest({ normalized_request: copy(request) }, request);
  assert.notEqual(admitted.normalized_request, request);
  assert.deepEqual(admitted.normalized_request, request);

  const mismatched = copy(request);
  mismatched.edit_intent.id = 'another-request';
  assert.throws(() => admitRopShapeResultForRequest({ normalized_request: mismatched }, request),
    /does not match the request executed by this node/);
  assert.throws(() => admitRopShapeResultForRequest({}, request),
    /does not match the request executed by this node/);
});

await test('browser synchronous budgets and replay controls reject fractional or oversized work', () => {
  const artifact = makeArtifact();
  const intent = buildSeparateEditIntent({
    id: 'separate', steps: [1, 5], preserveMidpointTolerance: 0.2, witnessCount: 7,
  });
  assert.throws(() => buildCanonicalRopShapeRequestFromHandoff(artifact, intent, {
    workBudget: { maxCells: 1.5 },
  }), /must be an integer/);
  assert.throws(() => buildCanonicalRopShapeRequestFromHandoff(artifact, intent, {
    workBudget: { maxCells: 257 },
  }), /at most 256/);
  assert.throws(() => buildCanonicalRopShapeRequestFromHandoff(artifact, intent, {
    workBudget: { maxReplays: 3 },
  }), /at most 2/);
  assert.throws(() => buildCanonicalRopShapeRequestFromHandoff(artifact, intent, {
    replay: { samplePoints: 10 },
  }), /at least 11/);
});

await test('config changes and failed fresh result runs clear stale derived artifacts', async () => {
  nodeRegistry.config = {
    type: 'rop-shape-edit-config',
    data: { ropShapeRequest: { stale: true } },
  };
  elements.set('config-rop-shape-status', { innerHTML: '' });
  invalidateRopShapePreparedRequest('config');
  assert.equal(nodeRegistry.config.data.ropShapeRequest, undefined);
  assert.match(elements.get('config-rop-shape-status').innerHTML, /Needs Prepare/);

  nodeRegistry.result = {
    type: 'rop-shape-result',
    data: {
      ropShapeRequest: { stale: true },
      ropShapeResult: { stale: true },
    },
  };
  const content = { innerHTML: '' };
  elements.set('result-content', content);
  setConnections([]);
  const value = await executeRopShapeResult('result');
  assert.equal(value, null);
  assert.equal(nodeRegistry.result.data.ropShapeRequest, undefined);
  assert.equal(nodeRegistry.result.data.ropShapeResult, undefined);
  assert.match(content.innerHTML, /node-error/);
});

await test('restored output requires one valid request/result pair and rejects cross-request evidence', () => {
  const valid = benchmarkSavedPair();
  const content = { innerHTML: '' };
  elements.set('restored-content', content);
  nodeRegistry.restored = {
    type: 'rop-shape-result',
    data: {
      ropShapeRequest: valid.request,
      ropShapeResult: valid.result,
      sessionId: 'saved-session-must-not-survive',
    },
  };
  assert.equal(restoreRopShapeResultView('restored', nodeRegistry.restored.data), true);
  assert.match(content.innerHTML, /Historical\/restored artifact/);
  assert.match(content.innerHTML, /ROP shape optimization/);
  assert.ok(nodeRegistry.restored.data.ropShapeRequest);
  assert.ok(nodeRegistry.restored.data.ropShapeResult);
  assert.equal(nodeRegistry.restored.data.sessionId, undefined);
  assert.deepEqual(nodeRegistry.restored.data.lifecycle, {
    state: 'historical',
    freshness: 'historical',
    evidence: {
      certificate_grade: valid.result.certificate_grade,
      finite_replay_evidence_grade: valid.result.finite_replay_evidence_grade,
      geometric_evidence_grade: valid.result.geometric_evidence_grade,
    },
  });
  assert.equal(inspectRopShapeLifecycle('restored').sessionId, null);

  const requestB = copy(valid.request);
  requestB.edit_intent.id = 'different-saved-request';
  nodeRegistry.restored.data = {
    ropShapeRequest: requestB,
    ropShapeResult: copy(valid.result),
  };
  assert.equal(restoreRopShapeResultView('restored', nodeRegistry.restored.data), false);
  assert.match(content.innerHTML, /Saved artifact rejected/);
  assert.equal(nodeRegistry.restored.data.ropShapeRequest, undefined);
  assert.equal(nodeRegistry.restored.data.ropShapeResult, undefined);

  nodeRegistry.restored.data = { ropShapeResult: copy(valid.result) };
  assert.equal(restoreRopShapeResultView('restored', nodeRegistry.restored.data), false);
  assert.match(content.innerHTML, /requires both ropShapeRequest and ropShapeResult/);
  assert.equal(nodeRegistry.restored.data.ropShapeResult, undefined);
});

await test('a historical Design Target selection cannot prepare a fresh ROP Shape request', async () => {
  const pair = benchmarkSavedPair();
  const artifact = artifactFromSavedPair(pair);
  nodeRegistry['historical-target'] = {
    type: 'design-target',
    data: {
      config: {
        selectedCandidateKey: artifact.selected_candidate_key,
        ropShapeReference: artifact,
      },
      selectionLifecycle: {
        state: 'historical', freshness: 'historical', evidence: { evidence_grade: 'enforced_exact' },
      },
    },
  };
  nodeRegistry['historical-config'] = { type: 'rop-shape-edit-config', data: {} };
  installEditorControls('historical-config', pair.request);
  setConnections([{
    fromNode: 'historical-target',
    fromPort: 'rop-shape-reference',
    toNode: 'historical-config',
    toPort: 'rop-shape-reference',
  }]);

  assert.equal(await prepareRopShapeRequest('historical-config'), null);
  assert.equal(nodeRegistry['historical-config'].data.ropShapeRequest, undefined);
  assert.equal(inspectRopShapeLifecycle('historical-config').state, 'blocked');
  assert.match(
    elements.get('historical-config-rop-shape-status').innerHTML,
    /historical or invalidated/,
  );
});

await test('latest result run wins and config invalidation cancels in-flight output and loading', async () => {
  const pair = benchmarkSavedPair();
  const artifact = artifactFromSavedPair(pair);
  const selectedKey = artifact.selected_candidate_key;
  nodeRegistry['race-target'] = {
    type: 'design-target',
    data: {
      config: { selectedCandidateKey: selectedKey, ropShapeReference: artifact },
      selectionLifecycle: { state: 'current', freshness: 'current', evidence: null },
    },
  };
  nodeRegistry['race-config'] = { type: 'rop-shape-edit-config', data: {} };
  nodeRegistry['race-result'] = { type: 'rop-shape-result', data: {} };
  installEditorControls('race-config', pair.request);
  const resultNode = loadingElement();
  const resultContent = { innerHTML: '' };
  elements.set('race-result', resultNode);
  elements.set('race-result-content', resultContent);
  setConnections([
    {
      fromNode: 'race-target', fromPort: 'rop-shape-reference',
      toNode: 'race-config', toPort: 'rop-shape-reference',
    },
    {
      fromNode: 'race-config', fromPort: 'rop-shape-request',
      toNode: 'race-result', toPort: 'rop-shape-request',
    },
  ]);

  const priorFetch = globalThis.fetch;
  const pending = [];
  try {
    globalThis.fetch = (url, options) => new Promise(resolve => {
      pending.push({
        request: JSON.parse(options.body),
        resolve,
        url,
      });
    });
    const responseFor = (entry, warning) => {
      const result = copy(pair.result);
      result.normalized_request = copy(entry.request);
      result.warnings = [warning];
      result.artifact.warnings = [warning];
      entry.resolve({
        ok: true,
        status: 200,
        headers: { get: () => 'application/json' },
        json: async () => result,
      });
    };

    const runA = executeRopShapeResult('race-result');
    const runB = executeRopShapeResult('race-result');
    for (let attempt = 0; attempt < 20 && pending.length < 2; attempt += 1) await Promise.resolve();
    assert.equal(pending.length, 2,
      `${elements.get('race-config-rop-shape-status')?.innerHTML || ''} ${resultContent.innerHTML}`);
    assert.equal(pending[0].url, '/api/v1/rop_shape_optimize');
    assert.equal(resultNode.classList.contains('loading'), true);

    responseFor(pending[1], 'run-b-won');
    const resultB = await runB;
    assert.ok(resultB, resultContent.innerHTML);
    assert.equal(resultB.warnings[0], 'run-b-won');
    assert.equal(nodeRegistry['race-result'].data.ropShapeResult.warnings[0], 'run-b-won');
    assert.equal(nodeRegistry['race-result'].data.lifecycle.state, 'current');
    assert.equal(nodeRegistry['race-result'].data.lifecycle.freshness, 'current');

    responseFor(pending[0], 'run-a-late');
    assert.equal(await runA, null);
    assert.equal(nodeRegistry['race-result'].data.ropShapeResult.warnings[0], 'run-b-won');

    const runC = executeRopShapeResult('race-result');
    await waitForPending(pending, 3);
    assert.equal(resultNode.classList.contains('loading'), true);
    invalidateRopShapePreparedRequest('race-config');
    assert.equal(resultNode.classList.contains('loading'), false);
    assert.equal(nodeRegistry['race-result'].data.ropShapeRequest, undefined);
    assert.equal(nodeRegistry['race-result'].data.ropShapeResult, undefined);
    assert.match(resultContent.innerHTML, /Configuration changed; rerun/);
    responseFor(pending[2], 'run-c-stale');
    assert.equal(await runC, null);
    assert.equal(nodeRegistry['race-result'].data.ropShapeResult, undefined);
    assert.equal(resultNode.classList.contains('loading'), false);
    assert.equal(nodeRegistry['race-result'].data.lifecycle.state, 'invalidated');

    const oldOwnerRun = executeRopShapeResult('race-result');
    await waitForPending(pending, 4);
    const oldOwner = nodeRegistry['race-result'];
    nodeRegistry['race-result'] = { type: 'rop-shape-result', data: {} };
    const replacementRun = executeRopShapeResult('race-result');
    await waitForPending(pending, 5);
    assert.notEqual(nodeRegistry['race-result'], oldOwner);

    responseFor(pending[3], 'old-owner-stale');
    assert.equal(await oldOwnerRun, null);
    assert.equal(resultNode.classList.contains('loading'), true,
      'the old owner finally must not clear replacement loading');
    assert.equal(nodeRegistry['race-result'].data.ropShapeResult, undefined);

    responseFor(pending[4], 'replacement-won');
    const replacement = await replacementRun;
    assert.equal(replacement.warnings[0], 'replacement-won');
    assert.equal(nodeRegistry['race-result'].data.ropShapeResult.warnings[0], 'replacement-won');

    const priorEpochRun = executeRopShapeResult('race-result');
    await waitForPending(pending, 6);
    advanceWorkspaceRuntimeEpoch();
    responseFor(pending[5], 'prior-epoch-stale');
    assert.equal(await priorEpochRun, null);
    assert.equal(nodeRegistry['race-result'].data.ropShapeResult, undefined);
    assert.equal(nodeRegistry['race-result'].data.lifecycle.state, 'invalidated');
    assert.equal(resultNode.classList.contains('loading'), false);
  } finally {
    globalThis.fetch = priorFetch;
  }
});

console.log(`\nAll ${passed} ROP shape node contract tests passed.`);
