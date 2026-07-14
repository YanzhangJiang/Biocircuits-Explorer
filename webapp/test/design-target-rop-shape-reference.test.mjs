import assert from 'node:assert/strict';

const elements = new Map();
globalThis.window = {
  matchMedia: () => null,
  addEventListener() {},
  location: { protocol: 'http:', hostname: '127.0.0.1', port: '8000' },
};
globalThis.document = {
  readyState: 'loading',
  documentElement: { dataset: {}, style: { setProperty() {} } },
  getElementById(id) { return elements.get(id) || null; },
  addEventListener() {},
  querySelectorAll() { return []; },
};

const { NODE_TYPES } = await import('../public/js/node-types/index.js');
const {
  didRopShapeReferenceChange,
  extractRopShapeReferenceFromCard,
  invalidateConnectedRopShapeArtifacts,
  refreshRopShapeReferenceForSelection,
} = await import('../public/js/node-types/design-target.js');
const { nodeRegistry, setConnections } = await import('../public/js/state.js');

const HASH_A = 'a'.repeat(64);
const HASH_B = 'b'.repeat(64);
const CELL_ID = `sha256:${'c'.repeat(64)}`;

function exactWindowCard() {
  const network = {
    ir_schema_version: 'bne-ir/v1.0.0',
    label: 'selected-exact-window-network',
    species: [
      { name: 'A', role: 'free' },
      { name: 'B', role: 'free' },
      { name: 'C_A_B', role: 'complex' },
    ],
    reactions: [{
      formula: 'A + B <-> C_A_B',
      kd: 1,
      kind: 'binding',
      reversible: true,
      metadata: {},
    }],
    observables: [{ name: 'C_A_B', expression: 'C_A_B' }],
    parameter_distributions: [{ name: 'Kd_1', distribution: 'fixed', value: 1 }],
    compartments: [],
    provenance: {},
    extensions: {},
  };
  const reference = {
    reference_hash: HASH_B,
    network_ir_hash: HASH_A,
    operating_points_log10: [-2, 0, 2],
    kd: [1],
    totals: { tA: 1, tB: 1 },
    path_identity: 'path:1',
    cell_id: CELL_ID,
  };
  const bodyTemplate = {
    schema_version: 'bne-rop-shape-optimize-request/v1.0.0',
    network,
    expected_network_ir_hash: HASH_A,
    designability_spec: {
      schema_version: 'bne-designability/v1.0.0',
      source: { kind: 'test_fixture' },
      target: {
        behavior_spec: {
          feature_space: 'reaction_order',
          input: 'tA',
          output: 'C_A_B',
          program: [
            { kind: 'reaction_order', operator: '=', value: 1, hard: true },
            { kind: 'reaction_order', operator: '=', value: 0, hard: true },
            { kind: 'reaction_order', operator: '=', value: -1, hard: true },
          ],
          input_window: { input_log10: [-5, 5], hard: true },
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
      audit_policy: { unsupported: 'block_if_hard', path_format: 'json_pointer', include_supported: true },
    },
    reference,
    edit_intent: null,
    optimization: { minimum_parameter_margin: 0, effect_tolerance: 0.02 },
    work_budget: { max_paths: 2000, max_cells: 256, max_replays: 1, require_exhaustive: false },
    replay: {
      input_window_log10: [-5, 5],
      sample_points: 281,
      require_complete: true,
      store_curve: true,
      metrics: [{ kind: 'two_peak', min_prominence_log10: 0.5 }],
    },
  };
  const fixedTopologyReference = {
    network,
    network_ir_hash: HASH_A,
    network_canonical_code: '[1]+[2]<->[1,2]',
    input: 'tA',
    output: 'C_A_B',
    reference,
    evidence_scope: 'selected exact-window Design Screen cell; optimization remains fixed-topology',
  };
  return {
    nid: '[1]+[2]<->[1,2]',
    inp: 'tA',
    out: 'C_A_B',
    pass: true,
    screen_status: 'verified_exact',
    evidence_grade: 'enforced_exact+sampled_forward',
    certificate_grade: 'exact-window-siso-rop-path',
    fixed_topology_reference: fixedTopologyReference,
    optimization_handoff_template: {
      endpoint: '/api/v1/rop_shape_optimize',
      method: 'POST',
      body_template: bodyTemplate,
      required_fill: ['edit_intent'],
    },
  };
}

function secondExactWindowCard() {
  const card = exactWindowCard();
  card.nid = '[1]+[1]<->[1,1]';
  card.fixed_topology_reference.network_canonical_code = card.nid;
  return card;
}

function fakeClassList(...initial) {
  const values = new Set(initial);
  return {
    contains(value) { return values.has(value); },
    remove(value) { values.delete(value); },
  };
}

let passed = 0;
function test(name, fn) {
  fn();
  passed += 1;
  console.log(`  ok - ${name}`);
}

test('Design Target preserves reactions and exposes the ROP shape reference output', () => {
  assert.deepEqual(
    NODE_TYPES['design-target'].outputs.map(output => output.port),
    ['reactions', 'rop-shape-reference'],
  );
});

test('exact finite-window card produces the versioned detached workspace artifact', () => {
  const card = exactWindowCard();
  const artifact = extractRopShapeReferenceFromCard(card);
  assert.ok(artifact);
  assert.deepEqual(Object.keys(artifact).sort(), [
    'evidence_scope',
    'fixed_topology_reference',
    'optimization_handoff_template',
    'schema_version',
    'selected_candidate_key',
  ]);
  assert.equal(artifact.schema_version, 'bne-workspace-rop-shape-reference/v1.0.0');
  assert.equal(artifact.selected_candidate_key, '[1]+[2]<->[1,2]::tA::C_A_B');
  assert.equal(artifact.optimization_handoff_template.endpoint, '/api/v1/rop_shape_optimize');
  assert.equal(artifact.optimization_handoff_template.method, 'POST');
  assert.equal(artifact.optimization_handoff_template.body_template.edit_intent, null);
  assert.equal(artifact.fixed_topology_reference.network_ir_hash, HASH_A);

  card.optimization_handoff_template.body_template.reference.kd[0] = 99;
  card.fixed_topology_reference.network.reactions[0].formula = 'mutated';
  assert.deepEqual(artifact.optimization_handoff_template.body_template.reference.kd, [1]);
  assert.equal(
    artifact.fixed_topology_reference.network.reactions[0].formula,
    'A + B <-> C_A_B',
  );
});

test('proxy, legacy, incomplete, and identity-inconsistent cards fail closed', () => {
  const cases = [];

  const proxy = exactWindowCard();
  proxy.screen_status = 'screened_proxy';
  proxy.evidence_grade = 'proxy_only';
  cases.push(proxy);

  const nonWindow = exactWindowCard();
  nonWindow.certificate_grade = 'exact-union-siso-rop';
  cases.push(nonWindow);

  const legacyEndpoint = exactWindowCard();
  legacyEndpoint.optimization_handoff_template.endpoint = '/api/rop_shape_optimize';
  cases.push(legacyEndpoint);

  const wrongMethod = exactWindowCard();
  wrongMethod.optimization_handoff_template.method = 'GET';
  cases.push(wrongMethod);

  const staleBody = exactWindowCard();
  staleBody.optimization_handoff_template.body_template.schema_version =
    'bne-rop-shape-optimize-request/v0.9.0';
  cases.push(staleBody);

  const prefilledEdit = exactWindowCard();
  prefilledEdit.optimization_handoff_template.body_template.edit_intent = { kind: 'separate' };
  cases.push(prefilledEdit);

  const wrongHash = exactWindowCard();
  wrongHash.fixed_topology_reference.network_ir_hash = 'd'.repeat(64);
  cases.push(wrongHash);

  const mismatchedReference = exactWindowCard();
  mismatchedReference.fixed_topology_reference.reference = {
    ...mismatchedReference.fixed_topology_reference.reference,
    reference_hash: 'e'.repeat(64),
  };
  cases.push(mismatchedReference);

  const wrongCanonicalCode = exactWindowCard();
  wrongCanonicalCode.fixed_topology_reference.network_canonical_code = '[1]+[1]<->[1,1]';
  cases.push(wrongCanonicalCode);

  const noFiniteWindow = exactWindowCard();
  delete noFiniteWindow.optimization_handoff_template.body_template
    .designability_spec.target.behavior_spec.input_window;
  cases.push(noFiniteWindow);

  const noParameterBounds = exactWindowCard();
  delete noParameterBounds.optimization_handoff_template.body_template
    .designability_spec.constraints.parameter_bounds;
  cases.push(noParameterBounds);

  const ambiguousScreenRadius = exactWindowCard();
  ambiguousScreenRadius.optimization_handoff_template.body_template
    .designability_spec.constraints.robustness = { min_chebyshev_radius: 0.1 };
  cases.push(ambiguousScreenRadius);

  const wrongWitnessCount = exactWindowCard();
  wrongWitnessCount.optimization_handoff_template.body_template.reference.operating_points_log10 = [-2, 2];
  wrongWitnessCount.fixed_topology_reference.reference.operating_points_log10 = [-2, 2];
  cases.push(wrongWitnessCount);

  for (const card of cases) assert.equal(extractRopShapeReferenceFromCard(card), null);
});

test('selected key stores, refreshes, and clears the reference fail closed', () => {
  const card = exactWindowCard();
  const key = '[1]+[2]<->[1,2]::tA::C_A_B';
  const config = { selectedCandidateKey: key };
  const first = refreshRopShapeReferenceForSelection(config, {
    verified_recommendations: [card],
  });
  assert.ok(first);
  assert.equal(config.ropShapeReference.selected_candidate_key, key);

  const refreshedCard = exactWindowCard();
  refreshedCard.optimization_handoff_template.body_template.reference.reference_hash = 'f'.repeat(64);
  refreshedCard.fixed_topology_reference.reference.reference_hash = 'f'.repeat(64);
  const refreshed = refreshRopShapeReferenceForSelection(config, {
    verified_recommendations: [refreshedCard],
  });
  assert.ok(refreshed);
  assert.equal(
    config.ropShapeReference.fixed_topology_reference.reference.reference_hash,
    'f'.repeat(64),
  );

  const proxy = exactWindowCard();
  proxy.screen_status = 'screened_proxy';
  proxy.evidence_grade = 'proxy_only';
  assert.equal(refreshRopShapeReferenceForSelection(config, {
    verified_recommendations: [],
    screened_candidates: [proxy],
  }), null);
  assert.equal(Object.hasOwn(config, 'ropShapeReference'), false);

  config.ropShapeReference = first;
  assert.equal(refreshRopShapeReferenceForSelection(config, {
    verified_recommendations: [exactWindowCard()],
    screened_candidates: [proxy],
  }), null, 'ambiguous duplicate candidate keys must not retain an exact artifact');
  assert.equal(Object.hasOwn(config, 'ropShapeReference'), false);
});

test('switching the Design Target reference clears prepared and result artifacts downstream', () => {
  const designNodeId = 'design-a';
  const configNodeId = 'shape-config-a';
  const resultNodeId = 'shape-result-a';
  const modelNodeId = 'model-builder-a';
  const firstCard = exactWindowCard();
  const secondCard = secondExactWindowCard();
  const firstKey = '[1]+[2]<->[1,2]::tA::C_A_B';
  const secondKey = '[1]+[1]<->[1,1]::tA::C_A_B';
  const designConfig = { selectedCandidateKey: firstKey };
  refreshRopShapeReferenceForSelection(designConfig, {
    verified_recommendations: [firstCard],
  });

  nodeRegistry[designNodeId] = { type: 'design-target', data: { config: designConfig } };
  nodeRegistry[configNodeId] = {
    type: 'rop-shape-edit-config',
    data: {
      ropShapeRequest: { id: 'prepared-for-a' },
      config: { ropShapeRequest: { id: 'nested-prepared-for-a' }, untouched: true },
    },
  };
  nodeRegistry[resultNodeId] = {
    type: 'rop-shape-result',
    data: {
      ropShapeRequest: { id: 'executed-for-a' },
      ropShapeResult: { id: 'result-for-a' },
      ropShapeRunToken: 7,
    },
  };
  nodeRegistry[modelNodeId] = {
    type: 'model-builder',
    data: { modelContext: { id: 'must-survive' } },
  };
  setConnections([
    {
      fromNode: designNodeId,
      fromPort: 'rop-shape-reference',
      toNode: configNodeId,
      toPort: 'rop-shape-reference',
    },
    {
      fromNode: configNodeId,
      fromPort: 'rop-shape-request',
      toNode: resultNodeId,
      toPort: 'rop-shape-request',
    },
    {
      fromNode: designNodeId,
      fromPort: 'reactions',
      toNode: modelNodeId,
      toPort: 'reactions',
    },
  ]);
  const configStatus = { innerHTML: '' };
  const resultContent = { innerHTML: '' };
  const resultElement = { classList: fakeClassList('loading') };
  const resultWire = { classList: fakeClassList('transmitting') };
  elements.set(`${configNodeId}-rop-shape-status`, configStatus);
  elements.set(`${resultNodeId}-content`, resultContent);
  elements.set(resultNodeId, resultElement);
  elements.set(`wire-${configNodeId}-${resultNodeId}`, resultWire);

  try {
    designConfig.selectedCandidateKey = secondKey;
    const second = refreshRopShapeReferenceForSelection(designConfig, {
      verified_recommendations: [secondCard],
    });
    assert.equal(second.selected_candidate_key, secondKey);

    const invalidated = invalidateConnectedRopShapeArtifacts(designNodeId);
    assert.deepEqual(invalidated, { configNodeCount: 1, resultNodeCount: 1 });
    assert.equal(nodeRegistry[configNodeId].data.ropShapeRequest, undefined);
    assert.equal(nodeRegistry[configNodeId].data.config.ropShapeRequest, undefined);
    assert.equal(nodeRegistry[configNodeId].data.config.untouched, true);
    assert.match(configStatus.innerHTML, /Needs Prepare/);
    assert.equal(nodeRegistry[resultNodeId].data.ropShapeRequest, undefined);
    assert.equal(nodeRegistry[resultNodeId].data.ropShapeResult, undefined);
    assert.equal(nodeRegistry[resultNodeId].data.ropShapeRunToken, 8);
    assert.equal(resultElement.classList.contains('loading'), false);
    assert.equal(resultWire.classList.contains('transmitting'), false);
    assert.match(resultContent.innerHTML, /Upstream changed/);
    assert.match(resultContent.innerHTML, /Run ROP shape optimization again/);
    assert.deepEqual(nodeRegistry[modelNodeId].data.modelContext, { id: 'must-survive' });
  } finally {
    setConnections([]);
    for (const nodeId of [designNodeId, configNodeId, resultNodeId, modelNodeId]) {
      delete nodeRegistry[nodeId];
    }
    elements.clear();
  }
});

test('refreshing an unchanged Design Target reference retains prepared downstream artifacts', () => {
  const designNodeId = 'design-same';
  const configNodeId = 'shape-config-same';
  const resultNodeId = 'shape-result-same';
  const key = '[1]+[2]<->[1,2]::tA::C_A_B';
  const designConfig = { selectedCandidateKey: key };
  refreshRopShapeReferenceForSelection(designConfig, {
    verified_recommendations: [exactWindowCard()],
  });
  const previousReference = designConfig.ropShapeReference;

  nodeRegistry[designNodeId] = { type: 'design-target', data: { config: designConfig } };
  nodeRegistry[configNodeId] = {
    type: 'rop-shape-edit-config',
    data: {
      ropShapeRequest: { id: 'prepared-same' },
      config: { ropShapeRequest: { id: 'nested-prepared-same' } },
    },
  };
  nodeRegistry[resultNodeId] = {
    type: 'rop-shape-result',
    data: {
      ropShapeRequest: { id: 'executed-same' },
      ropShapeResult: { id: 'result-same' },
      ropShapeRunToken: 3,
    },
  };
  setConnections([
    {
      fromNode: designNodeId,
      fromPort: 'rop-shape-reference',
      toNode: configNodeId,
      toPort: 'rop-shape-reference',
    },
    {
      fromNode: configNodeId,
      fromPort: 'rop-shape-request',
      toNode: resultNodeId,
      toPort: 'rop-shape-request',
    },
  ]);

  try {
    const currentReference = refreshRopShapeReferenceForSelection(designConfig, {
      verified_recommendations: [exactWindowCard()],
    });
    const changed = didRopShapeReferenceChange(
      key,
      previousReference,
      designConfig.selectedCandidateKey,
      currentReference,
    );
    assert.equal(changed, false);
    if (changed) invalidateConnectedRopShapeArtifacts(designNodeId);
    assert.deepEqual(nodeRegistry[configNodeId].data.ropShapeRequest, { id: 'prepared-same' });
    assert.deepEqual(nodeRegistry[configNodeId].data.config.ropShapeRequest, {
      id: 'nested-prepared-same',
    });
    assert.deepEqual(nodeRegistry[resultNodeId].data.ropShapeResult, { id: 'result-same' });
    assert.equal(nodeRegistry[resultNodeId].data.ropShapeRunToken, 3);
  } finally {
    setConnections([]);
    for (const nodeId of [designNodeId, configNodeId, resultNodeId]) delete nodeRegistry[nodeId];
  }
});

test('legacy workspace config without a selected reference remains compatible', () => {
  const legacyConfig = { resolvedDefinition: { raw_rules: ['A + B <-> C_A_B'] } };
  assert.equal(refreshRopShapeReferenceForSelection(legacyConfig, {}), null);
  assert.deepEqual(legacyConfig, { resolvedDefinition: { raw_rules: ['A + B <-> C_A_B'] } });
});

console.log(`\nAll ${passed} Design Target ROP shape reference tests passed.`);
