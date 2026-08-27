import assert from 'node:assert/strict';
import fs from 'node:fs';

import {
  buildROFieldViewModel,
  renderROFieldArtifact,
  ROFieldRenderError,
  RO_FIELD_SCHEMA_VERSION,
} from '../public/js/ro-field-render.js';
import {
  DEFAULT_RO_FIELD_REQUEST,
  extractROFieldResponse,
  fetchROField,
  RO_FIELD_ENDPOINT,
} from '../public/js/ro-field-demo.js';

let passed = 0;
const pending = [];
function test(name, fn) {
  pending.push(Promise.resolve().then(fn).then(() => {
    passed += 1;
    console.log(`  ok - ${name}`);
  }));
}

function copy(value) {
  return JSON.parse(JSON.stringify(value));
}

class FakeElement {
  constructor(tagName, ownerDocument) {
    this.tagName = tagName;
    this.ownerDocument = ownerDocument;
    this.attributes = new Map();
    this.children = [];
    this.textContent = '';
    this.className = '';
  }

  setAttribute(name, value) {
    this.attributes.set(name, String(value));
  }

  appendChild(child) {
    this.children.push(child);
    return child;
  }

  replaceChildren(...children) {
    this.children = children;
  }

  addEventListener() {}
}

class FakeDocument {
  createElement(tagName) {
    return new FakeElement(tagName, this);
  }

  createElementNS(_namespace, tagName) {
    return new FakeElement(tagName, this);
  }
}

function descendants(root, predicate) {
  const matches = [];
  root.children.forEach(child => {
    if (predicate(child)) matches.push(child);
    matches.push(...descendants(child, predicate));
  });
  return matches;
}

function commonArtifact(rank = 2) {
  const axes = Array.from({ length: rank }, (_, index) => ({
    axis_id: `axis_${index}`,
    symbol: `t${index}`,
    coordinate_kind: 'conserved_total',
    orientation: 'increasing_physical_value',
    reference: { value: 1, unit: 'uM' },
    bounds: { lower: -2 - index, upper: 3 + index },
  }));
  const outputs = [
    {
      output_id: 'out_0',
      symbol: 'x0',
      observable_kind: 'species_concentration',
      reference: { value: 1, unit: 'uM' },
    },
    {
      output_id: 'out_1',
      symbol: 'x1',
      observable_kind: 'species_concentration',
      reference: { value: 1, unit: 'uM' },
    },
  ];
  return {
    schema_version: RO_FIELD_SCHEMA_VERSION,
    field_id: 'browser-contract-fixture',
    representation: 'sampled_grid',
    partial: false,
    domain: {
      domain_kind: 'axis_aligned_log_box',
      coordinate_space: 'dimensionless_log_ratio',
      log_basis: 'log10',
      axis_order: axes.map(axis => axis.axis_id),
      axes,
      fixed_background: [],
    },
    outputs: {
      output_order: outputs.map(output => output.output_id),
      items: outputs,
    },
    component_order: outputs.flatMap(output => axes.map(axis => ({
      output_id: output.output_id,
      input_axis_id: axis.axis_id,
    }))),
    data: {},
    coverage: {
      population_kind: 'grid_points',
      eligible_count: 0,
      evaluated_count: 0,
      valid_count: 0,
      invalid_count: 0,
      omitted_count: 0,
      enumeration_complete: true,
      truncated: false,
      truncation: null,
      budget: {
        work_unit_kind: 'solver_samples',
        max_evaluated_items: 64,
        max_stored_items: 64,
        max_payload_bytes: 1_048_576,
        deadline_seconds: null,
      },
      storage: {
        mode: 'inline',
        complete: true,
        stored_count: 0,
        payload_bytes: 2,
        content_sha256: 'a'.repeat(64),
        artifacts: [],
      },
    },
    evidence: {
      evidence_class: 'sampled_numerical',
      status: 'complete',
      claim_scope: 'declared_domain_model_configuration_only',
      validity_policy: 'invalid_is_gap',
      completeness_claim: 'complete_over_declared_population',
      limitations: ['renderer contract fixture'],
    },
    provenance: { producer: 'ro-field-render.test.mjs' },
  };
}

function sampled2x3Artifact() {
  const artifact = commonArtifact(2);
  const gridShape = [2, 3];
  const pointCount = 6;
  const outputValues = [];
  const reactionOrderValues = [];
  for (let point = 0; point < pointCount; point += 1) {
    outputValues.push(point + 0.25, 100 + point);
    // output-major, then input-fastest inside each row-major domain sample.
    reactionOrderValues.push(
      (point * 100) + 1,
      (point * 100) + 2,
      (point * 100) + 11,
      (point * 100) + 12,
    );
  }
  artifact.data = {
    sampling_scheme: 'cartesian_product',
    axis_coordinates: [[-2, 3], [-3, 0, 4]],
    grid_shape: gridShape,
    output_shape: [...gridShape, 2],
    reaction_order_shape: [...gridShape, 2, 2],
    flatten_order: 'row_major_last_axis_fastest',
    output_values: outputValues,
    reaction_order_values: reactionOrderValues,
    regime_ids: Array.from({ length: pointCount }, (_, index) => `r${index}`),
    validity: Array(pointCount).fill(true),
  };
  artifact.coverage = {
    ...artifact.coverage,
    eligible_count: pointCount,
    evaluated_count: pointCount,
    valid_count: pointCount,
    storage: {
      ...artifact.coverage.storage,
      stored_count: pointCount,
    },
  };
  return artifact;
}

function boxHalfspaces(lowerX, upperX, lowerY, upperY) {
  return [
    { coefficients: [1, 0], upper_bound: upperX, source: 'domain_upper' },
    { coefficients: [-1, 0], upper_bound: -lowerX, source: 'domain_lower' },
    { coefficients: [0, 1], upper_bound: upperY, source: 'domain_upper' },
    { coefficients: [0, -1], upper_bound: -lowerY, source: 'domain_lower' },
  ];
}

function exactArtifact() {
  const artifact = commonArtifact(2);
  artifact.representation = 'exact_cell_complex';
  artifact.partial = true;
  artifact.evidence.evidence_class = 'exact_polyhedral';
  artifact.evidence.status = 'partial';
  artifact.evidence.completeness_claim = 'no_positive_claim';
  artifact.coverage = {
    ...artifact.coverage,
    population_kind: 'cell_complex_items',
    eligible_count: 4,
    evaluated_count: 4,
    valid_count: 1,
    invalid_count: 3,
    budget: {
      ...artifact.coverage.budget,
      work_unit_kind: 'source_regime_candidates',
    },
    storage: {
      ...artifact.coverage.storage,
      stored_count: 4,
    },
  };
  const firstLabel = {
    label_id: 'label_left_a',
    source_regime_ids: ['regime_a'],
    output_offset: [0, 1],
    reaction_order_matrix: [[1, 2], [11, 12]],
  };
  const secondLabel = {
    label_id: 'label_left_b',
    source_regime_ids: ['regime_b'],
    output_offset: [0, 1],
    reaction_order_matrix: [[3, 4], [13, 14]],
  };
  const regularLabel = {
    label_id: 'label_right',
    source_regime_ids: ['regime_c'],
    output_offset: [2, 3],
    reaction_order_matrix: [[5, 6], [15, 16]],
  };
  artifact.data = {
    coefficient_encoding: 'float64',
    source_candidate_regime_count: 3,
    regular_candidate_regime_count: 3,
    cell_order: ['left', 'right'],
    cells: [
      {
        cell_id: 'left',
        dimension: 2,
        status: 'set_valued',
        set_valued: true,
        polyhedron: { halfspaces: boxHalfspaces(-2, 0, -3, 4) },
        vertices: [[-2, -3], [0, -3], [0, 4], [-2, 4]],
        area: 14,
        source_regime_ids: ['regime_a', 'regime_b'],
        label_order: ['label_left_a', 'label_left_b'],
        affine_labels: [firstLabel, secondLabel],
      },
      {
        cell_id: 'right',
        dimension: 2,
        status: 'regular',
        set_valued: false,
        polyhedron: { halfspaces: boxHalfspaces(0, 3, -3, 4) },
        vertices: [[0, -3], [3, -3], [3, 4], [0, 4]],
        area: 21,
        source_regime_ids: ['regime_c'],
        label_order: ['label_right'],
        affine_labels: [regularLabel],
      },
    ],
    facet_order: ['transition'],
    facets: [
      {
        facet_id: 'transition',
        dimension: 1,
        kind: 'singular_boundary',
        polyhedron: {
          halfspaces: [
            { coefficients: [1, 0], upper_bound: 0, source: 'regime' },
            { coefficients: [-1, 0], upper_bound: 0, source: 'regime' },
            { coefficients: [0, 1], upper_bound: 4, source: 'domain_upper' },
            { coefficients: [0, -1], upper_bound: 3, source: 'domain_lower' },
          ],
        },
        endpoints: [[0, -3], [0, 4]],
        normal: [1, 0],
        offset: 0,
        mixed_sign: false,
        domain_side: null,
        singular_stratum_ids: ['singular_origin'],
        incident_cell_ids: ['left', 'right'],
      },
    ],
    singular_stratum_order: ['singular_origin'],
    singular_strata: [
      {
        stratum_id: 'singular_origin',
        dimension: 0,
        vertices: [[0, 0]],
        source_regime_ids: ['regime_a', 'regime_c'],
        nullities: [2],
        reasons: ['lower_dimensional_slice'],
      },
    ],
    gaps: [
      {
        gap_id: 'gap_top',
        reason: 'singular',
        region: {
          halfspaces: boxHalfspaces(-0.1, 0.1, 3.8, 4),
        },
        detail: 'deliberate viewer fixture',
      },
    ],
  };
  return artifact;
}

test('non-symmetric 2x3 grid decodes last-axis-fastest without transposition', () => {
  const view = buildROFieldViewModel(sampled2x3Artifact(), {
    outputId: 'out_1',
    inputAxisId: 'axis_1',
  });
  assert.equal(view.kind, 'sampled_2d');
  assert.deepEqual(view.gridShape, [2, 3]);
  assert.deepEqual(view.points[0][1].coordinates, [-2, 0]);
  assert.equal(view.points[0][1].pointIndex, 1);
  assert.equal(view.points[0][1].outputValue, 101);
  assert.equal(view.points[0][1].reactionOrder, 112);
  assert.deepEqual(view.points[1][2].coordinates, [3, 4]);
  assert.equal(view.points[1][2].pointIndex, 5);
  assert.equal(view.points[1][2].outputValue, 105);
  assert.equal(view.points[1][2].reactionOrder, 512);
  assert.equal(view.displaySemantics, 'display_only_not_interpolated');
  assert.ok(view.warnings.some(warning => /blank space between markers is unsampled/.test(warning)));
});

test('sampled renderer places fixed-size point markers at non-uniform physical coordinates', () => {
  const documentRef = new FakeDocument();
  const container = new FakeElement('div', documentRef);
  const rendered = renderROFieldArtifact(container, sampled2x3Artifact());
  assert.equal(rendered.viewModel.displaySemantics, 'display_only_not_interpolated');

  const svg = descendants(container, node => node.tagName === 'svg')[0];
  assert.equal(svg.attributes.get('data-display-semantics'), 'display_only_not_interpolated');
  const markers = descendants(svg, node => (
    node.tagName === 'circle' && node.attributes.has('data-point-index')
  ));
  assert.equal(markers.length, 6);
  assert.equal(descendants(svg, node => (
    node.tagName === 'rect' && node.attributes.has('data-point-index')
  )).length, 0);

  const byIndex = new Map(markers.map(marker => [
    Number(marker.attributes.get('data-point-index')),
    marker,
  ]));
  assert.equal(Number(byIndex.get(0).attributes.get('cx')), 72);
  assert.equal(Number(byIndex.get(0).attributes.get('cy')), 476);
  assert.equal(Number(byIndex.get(1).attributes.get('cy')), 476 - ((3 / 7) * 440));
  assert.equal(Number(byIndex.get(2).attributes.get('cy')), 36);
  assert.equal(Number(byIndex.get(5).attributes.get('cx')), 672);
  assert.ok(markers.every(marker => marker.attributes.get('r') === '13'));

  const firstSpacing = Number(byIndex.get(0).attributes.get('cy'))
    - Number(byIndex.get(1).attributes.get('cy'));
  const secondSpacing = Number(byIndex.get(1).attributes.get('cy'))
    - Number(byIndex.get(2).attributes.get('cy'));
  assert.ok(Math.abs((firstSpacing / secondSpacing) - (3 / 4)) < 1e-12);

  const displayMetadata = descendants(container, node => (
    node.tagName === 'dd' && node.textContent === 'display_only_not_interpolated'
  ));
  assert.equal(displayMetadata.length, 1);
});

test('invalid numeric samples fail closed and null samples remain explicit gaps', () => {
  const invalid = sampled2x3Artifact();
  invalid.data.validity[1] = false;
  assert.throws(
    () => buildROFieldViewModel(invalid),
    error => error instanceof ROFieldRenderError && /null gaps, never numeric zero/.test(error.message),
  );

  invalid.data.output_values.splice(2, 2, null, null);
  invalid.data.reaction_order_values.splice(4, 4, null, null, null, null);
  invalid.data.regime_ids[1] = null;
  invalid.partial = true;
  invalid.coverage.valid_count = 5;
  invalid.coverage.invalid_count = 1;
  invalid.evidence.status = 'partial';
  invalid.evidence.completeness_claim = 'no_positive_claim';
  const view = buildROFieldViewModel(invalid);
  assert.equal(view.points[0][1].valid, false);
  assert.equal(view.points[0][1].outputValue, null);
  assert.equal(view.points[0][1].reactionOrder, null);
  assert.ok(view.warnings.some(warning => /explicit gaps/.test(warning)));

  const documentRef = new FakeDocument();
  const container = new FakeElement('div', documentRef);
  renderROFieldArtifact(container, invalid);
  const gapMarker = descendants(container, node => (
    node.tagName === 'circle' && node.attributes.get('data-point-index') === '1'
  ))[0];
  assert.equal(gapMarker.attributes.get('data-valid'), 'false');
  assert.equal(gapMarker.attributes.get('fill'), 'url(#ro-field-gap-pattern)');
  assert.ok(descendants(container, node => node.tagName === 'text' && node.textContent === 'gap').length >= 1);
});

test('component_order drift and malformed tensor shapes fail closed', () => {
  const wrongOrder = sampled2x3Artifact();
  wrongOrder.component_order.reverse();
  assert.throws(() => buildROFieldViewModel(wrongOrder), /output-major/);

  const wrongShape = sampled2x3Artifact();
  wrongShape.data.reaction_order_shape = [3, 2, 2, 2];
  assert.throws(() => buildROFieldViewModel(wrongShape), /reaction_order_shape/);
});

test('future schema versions and unsupported flatten order fail closed', () => {
  const future = sampled2x3Artifact();
  future.schema_version = 'bne-ro-field/v2.0.0';
  assert.throws(() => buildROFieldViewModel(future), /expected exactly bne-ro-field\/v1\.0\.0/);

  const columnMajor = sampled2x3Artifact();
  columnMajor.data.flatten_order = 'column_major_first_axis_fastest';
  assert.throws(() => buildROFieldViewModel(columnMajor), /only admits row_major/);
});

test('exact 2D view preserves multiple labels and reads explicit geometry', () => {
  const view = buildROFieldViewModel(exactArtifact(), {
    outputId: 'out_1',
    inputAxisId: 'axis_1',
  });
  assert.equal(view.kind, 'exact_2d');
  assert.equal(view.cells[0].setValued, true);
  assert.deepEqual(view.cells[0].selectedValues, [12, 14]);
  assert.deepEqual(view.cells[1].selectedValues, [16]);
  assert.deepEqual(view.facets[0].endpoints, [[0, -3], [0, 4]]);
  assert.equal(view.facets[0].mixedSign, false);
  assert.equal(view.singularStrata[0].id, 'singular_origin');
  assert.equal(view.gaps[0].reason, 'singular');
  assert.ok(view.warnings.some(warning => /set-valued/.test(warning)));
  assert.ok(view.warnings.some(warning => /singular stratum/.test(warning)));
  assert.ok(view.warnings.some(warning => /gap/.test(warning)));
});

test('exact 2D requires canonical explicit polygon and facet geometry', () => {
  const artifact = exactArtifact();
  delete artifact.data.cells[1].vertices;
  delete artifact.data.facets[0].endpoints;
  assert.throws(() => buildROFieldViewModel(artifact), /missing required key/);
});

test('explicit exact geometry does not require an optional H-representation', () => {
  const artifact = exactArtifact();
  artifact.data.cells.forEach(cell => { delete cell.polyhedron; });
  artifact.data.facets.forEach(facet => { delete facet.polyhedron; });
  const view = buildROFieldViewModel(artifact);
  assert.equal(view.cells.length, 2);
  assert.deepEqual(view.facets[0].endpoints, [[0, -3], [0, 4]]);
});

test('coverage, evidence, and complete inline storage inconsistencies fail closed', () => {
  const badCoverage = sampled2x3Artifact();
  badCoverage.coverage.valid_count = 5;
  badCoverage.coverage.invalid_count = 1;
  badCoverage.partial = true;
  badCoverage.evidence.status = 'partial';
  badCoverage.evidence.completeness_claim = 'no_positive_claim';
  assert.throws(() => buildROFieldViewModel(badCoverage), /serialized representation population/);

  const badStorage = sampled2x3Artifact();
  badStorage.coverage.storage.stored_count = 5;
  assert.throws(() => buildROFieldViewModel(badStorage), /store every evaluated item/);

  const badEvidence = sampled2x3Artifact();
  badEvidence.evidence.status = 'unknown';
  assert.throws(() => buildROFieldViewModel(badEvidence), /complete fields require/);
});

test('malformed exact area, status, candidates, and facet references fail closed', () => {
  const badArea = exactArtifact();
  badArea.data.cells[0].area = 999;
  assert.throws(() => buildROFieldViewModel(badArea), /polygon area/);

  const badStatus = exactArtifact();
  badStatus.data.cells[0].status = 'banana';
  assert.throws(() => buildROFieldViewModel(badStatus), /unsupported status/);

  const missingCount = exactArtifact();
  delete missingCount.data.source_candidate_regime_count;
  assert.throws(() => buildROFieldViewModel(missingCount), /missing required key/);

  const badNormal = exactArtifact();
  badNormal.data.facets[0].normal = [2, 0];
  assert.throws(() => buildROFieldViewModel(badNormal), /canonical unit normal/);

  const badReference = exactArtifact();
  badReference.data.facets[0].singular_stratum_ids = ['missing'];
  assert.throws(() => buildROFieldViewModel(badReference), /unknown singular stratum/);
});

test('directional and higher-rank exact artifacts are rejected by this renderer', () => {
  const directional = sampled2x3Artifact();
  directional.representation = 'directional_path';
  assert.throws(() => buildROFieldViewModel(directional), /unsupported reaction-order field representation/);

  const rankThree = commonArtifact(3);
  rankThree.representation = 'exact_cell_complex';
  rankThree.coverage.population_kind = 'cell_complex_items';
  rankThree.coverage.budget.work_unit_kind = 'source_regime_candidates';
  rankThree.evidence.evidence_class = 'exact_polyhedral';
  rankThree.data = exactArtifact().data;
  assert.throws(() => buildROFieldViewModel(rankThree), /require exactly two axes/);
});

test('higher-dimensional sampled artifacts expose metadata without fabricating a projection', () => {
  const artifact = commonArtifact(3);
  artifact.outputs.items = [artifact.outputs.items[0]];
  artifact.outputs.output_order = ['out_0'];
  artifact.component_order = artifact.domain.axis_order.map(inputAxisId => ({
    output_id: 'out_0', input_axis_id: inputAxisId,
  }));
  artifact.data = {
    sampling_scheme: 'cartesian_product',
    axis_coordinates: [[-1, 1], [-1, 1], [-1, 1]],
    grid_shape: [2, 2, 2],
    output_shape: [2, 2, 2, 1],
    reaction_order_shape: [2, 2, 2, 1, 3],
    flatten_order: 'row_major_last_axis_fastest',
    output_values: Array(8).fill(1),
    reaction_order_values: Array(24).fill(1),
    regime_ids: Array(8).fill('r'),
    validity: Array(8).fill(true),
  };
  artifact.coverage.eligible_count = 8;
  artifact.coverage.evaluated_count = 8;
  artifact.coverage.valid_count = 8;
  artifact.coverage.storage.stored_count = 8;
  const view = buildROFieldViewModel(artifact);
  assert.equal(view.kind, 'slice_required');
  assert.match(view.message, /3D sampled field admitted/);
  assert.match(view.message, /does not project or fabricate/);
});

test('demo request stays byte-for-byte equivalent as JSON to the schema fixture', () => {
  const fixture = JSON.parse(fs.readFileSync(
    new URL('../../tests/fixtures/ro_field_request/sampled-inline-network.json', import.meta.url),
    'utf8',
  ));
  assert.deepEqual(DEFAULT_RO_FIELD_REQUEST, fixture);
});

test('demo calls only the canonical inline endpoint and strictly admits the response version', async () => {
  const field = sampled2x3Artifact();
  let call = null;
  const fetchImpl = async (url, options) => {
    call = { url, options };
    return {
      ok: true,
      status: 200,
      json: async () => ({ ro_field: field, artifact: null }),
    };
  };
  const result = await fetchROField(DEFAULT_RO_FIELD_REQUEST, { fetchImpl });
  assert.equal(call.url, RO_FIELD_ENDPOINT);
  assert.equal(call.options.method, 'POST');
  assert.equal(JSON.parse(call.options.body).storage.mode, 'inline');
  assert.equal(result.roField, field);

  const future = copy(field);
  future.schema_version = 'bne-ro-field/v2.0.0';
  assert.throws(() => extractROFieldResponse({ ro_field: future }), /must be exactly/);

  const persistent = copy(DEFAULT_RO_FIELD_REQUEST);
  persistent.storage.mode = 'artifact_reference';
  await assert.rejects(() => fetchROField(persistent, { fetchImpl }), /only sends storage.mode="inline"/);
});

await Promise.all(pending);
console.log(`ro-field renderer contracts: ${passed}/${passed} passed`);
