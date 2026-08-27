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

function exactFacet(facetId, kind, endpoints, incidentCellIds, domainSide, singularIds = []) {
  const dx = endpoints[1][0] - endpoints[0][0];
  const dy = endpoints[1][1] - endpoints[0][1];
  const magnitude = Math.hypot(dx, dy);
  let normal = [dy / magnitude, -dx / magnitude];
  if (normal[0] < 0 || (Object.is(normal[0], 0) && normal[1] < 0)) {
    normal = normal.map(value => -value);
  }
  normal = normal.map(value => (Object.is(value, -0) ? 0 : value));
  return {
    facet_id: facetId,
    dimension: 1,
    kind,
    endpoints,
    normal,
    offset: -((normal[0] * endpoints[0][0]) + (normal[1] * endpoints[0][1])),
    mixed_sign: normal[0] * normal[1] < 0,
    domain_side: domainSide,
    singular_stratum_ids: singularIds,
    incident_cell_ids: incidentCellIds,
  };
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
    facet_order: [
      'left-bottom', 'left-outer', 'left-top', 'transition',
      'right-bottom', 'right-outer', 'right-top',
    ],
    facets: [
      exactFacet('left-bottom', 'domain_boundary', [[-2, -3], [0, -3]], ['left'], 'axis2_lower'),
      exactFacet('left-outer', 'domain_boundary', [[-2, -3], [-2, 4]], ['left'], 'axis1_lower'),
      exactFacet('left-top', 'domain_boundary', [[-2, 4], [0, 4]], ['left'], 'axis2_upper'),
      exactFacet(
        'transition', 'singular_boundary', [[0, -3], [0, 4]],
        ['left', 'right'], null, ['singular_origin'],
      ),
      exactFacet('right-bottom', 'domain_boundary', [[0, -3], [3, -3]], ['right'], 'axis2_lower'),
      exactFacet('right-outer', 'domain_boundary', [[3, -3], [3, 4]], ['right'], 'axis1_upper'),
      exactFacet('right-top', 'domain_boundary', [[0, 4], [3, 4]], ['right'], 'axis2_upper'),
    ],
    singular_stratum_order: ['singular_origin'],
    singular_strata: [
      {
        stratum_id: 'singular_origin',
        dimension: 1,
        vertices: [[0, -1], [0, 1]],
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

function completeExactArtifact() {
  const artifact = exactArtifact();
  artifact.partial = false;
  artifact.evidence.status = 'complete';
  artifact.evidence.completeness_claim = 'complete_over_declared_population';
  artifact.data.source_candidate_regime_count = 2;
  artifact.data.regular_candidate_regime_count = 2;
  const left = artifact.data.cells[0];
  left.status = 'regular';
  left.set_valued = false;
  left.source_regime_ids = ['regime_a'];
  left.label_order = ['label_left_a'];
  left.affine_labels = [left.affine_labels[0]];
  artifact.data.singular_stratum_order = [];
  artifact.data.singular_strata = [];
  artifact.data.gaps = [];
  const transition = artifact.data.facets.find(facet => facet.facet_id === 'transition');
  transition.kind = 'regime_transition';
  transition.singular_stratum_ids = [];
  artifact.coverage.eligible_count = 2;
  artifact.coverage.evaluated_count = 2;
  artifact.coverage.valid_count = 2;
  artifact.coverage.invalid_count = 0;
  artifact.coverage.storage.stored_count = 2;
  return artifact;
}

function stripedExactArtifact({ count = 100, interfaceDelta = 0 } = {}) {
  const artifact = completeExactArtifact();
  artifact.domain.axes.forEach(axis => {
    axis.bounds = { lower: -2, upper: 2 };
  });
  const nominalWidth = 4 / count;
  const cells = Array.from({ length: count }, (_, index) => {
    const nominalLeft = -2 + (index * nominalWidth);
    const nominalRight = -2 + ((index + 1) * nominalWidth);
    const left = index === 0 ? -2 : nominalLeft - (interfaceDelta / 2);
    const right = index === count - 1 ? 2 : nominalRight + (interfaceDelta / 2);
    const sourceId = `regime_${index}`;
    const labelId = `label_${index}`;
    return {
      cell_id: `cell_${index}`,
      dimension: 2,
      status: 'regular',
      set_valued: false,
      vertices: [[left, -2], [right, -2], [right, 2], [left, 2]],
      // Deliberately keep the nominal declaration: each local discrepancy is
      // below the v1 per-cell tolerance, while the geometric total can drift.
      area: nominalWidth * 4,
      source_regime_ids: [sourceId],
      label_order: [labelId],
      affine_labels: [{
        label_id: labelId,
        source_regime_ids: [sourceId],
        output_offset: [0, 1],
        reaction_order_matrix: [[index + 1, 2], [3, 4]],
      }],
    };
  });
  const facets = [];
  cells.forEach((cell, index) => {
    const left = cell.vertices[0][0];
    const right = cell.vertices[1][0];
    facets.push(exactFacet(
      `bottom_${index}`, 'domain_boundary', [[left, -2], [right, -2]],
      [cell.cell_id], 'axis2_lower',
    ));
    facets.push(exactFacet(
      `top_${index}`, 'domain_boundary', [[left, 2], [right, 2]],
      [cell.cell_id], 'axis2_upper',
    ));
  });
  facets.push(exactFacet(
    'outer_left', 'domain_boundary', [[-2, -2], [-2, 2]],
    ['cell_0'], 'axis1_lower',
  ));
  facets.push(exactFacet(
    'outer_right', 'domain_boundary', [[2, -2], [2, 2]],
    [`cell_${count - 1}`], 'axis1_upper',
  ));
  for (let index = 1; index < count; index += 1) {
    const boundary = -2 + (index * nominalWidth);
    facets.push(exactFacet(
      `transition_${index}`, 'regime_transition', [[boundary, -2], [boundary, 2]],
      [`cell_${index - 1}`, `cell_${index}`], null,
    ));
  }
  artifact.data = {
    coefficient_encoding: 'float64',
    source_candidate_regime_count: count,
    regular_candidate_regime_count: count,
    cell_order: cells.map(cell => cell.cell_id),
    cells,
    facet_order: facets.map(facet => facet.facet_id),
    facets,
    singular_stratum_order: [],
    singular_strata: [],
    gaps: [],
  };
  artifact.coverage.eligible_count = count;
  artifact.coverage.evaluated_count = count;
  artifact.coverage.valid_count = count;
  artifact.coverage.storage.stored_count = count;
  artifact.coverage.budget.max_evaluated_items = Math.max(128, count);
  artifact.coverage.budget.max_stored_items = Math.max(128, count);
  return artifact;
}

function splitBottomFacetArtifact({ count = 100, gap = 3e-10 } = {}) {
  const artifact = stripedExactArtifact({ count: 1 });
  const retained = artifact.data.facets.filter(facet => facet.facet_id !== 'bottom_0');
  const width = 4 / count;
  const split = Array.from({ length: count }, (_, index) => {
    const nominalLeft = -2 + (index * width);
    const nominalRight = -2 + ((index + 1) * width);
    const left = index === 0 ? -2 : nominalLeft + (gap / 2);
    const right = index === count - 1 ? 2 : nominalRight - (gap / 2);
    return exactFacet(
      `bottom_part_${index}`, 'domain_boundary', [[left, -2], [right, -2]],
      ['cell_0'], 'axis2_lower',
    );
  });
  artifact.data.facets = [...split, ...retained];
  artifact.data.facet_order = artifact.data.facets.map(facet => facet.facet_id);
  return artifact;
}

function rationalizePolyhedron(polyhedron) {
  const rationalString = value => {
    if (Number.isInteger(value)) return String(value);
    const text = String(value);
    const digits = text.startsWith('-') ? text.slice(1) : text;
    const decimalPlaces = digits.split('.')[1]?.length ?? 0;
    const denominator = 10 ** decimalPlaces;
    const numerator = Math.round(value * denominator);
    return `${numerator}/${denominator}`;
  };
  polyhedron.halfspaces.forEach(halfspace => {
    halfspace.coefficients = halfspace.coefficients.map(rationalString);
    halfspace.upper_bound = rationalString(halfspace.upper_bound);
  });
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
  const transition = view.facets.find(facet => facet.id === 'transition');
  assert.deepEqual(transition.endpoints, [[0, -3], [0, 4]]);
  assert.equal(transition.mixedSign, false);
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

test('exact point geometry admits JSON numbers only', () => {
  const artifact = exactArtifact();
  artifact.data.cells[0].vertices = artifact.data.cells[0].vertices.map(
    point => point.map(String),
  );
  assert.throws(() => buildROFieldViewModel(artifact), /expected a finite number/);
});

test('explicit exact geometry does not require an optional H-representation', () => {
  const artifact = exactArtifact();
  artifact.data.cells.forEach(cell => { delete cell.polyhedron; });
  artifact.data.facets.forEach(facet => { delete facet.polyhedron; });
  const view = buildROFieldViewModel(artifact);
  assert.equal(view.cells.length, 2);
  assert.deepEqual(
    view.facets.find(facet => facet.id === 'transition').endpoints,
    [[0, -3], [0, 4]],
  );
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

test('singular facet incidence is the exact finite-segment overlap population', () => {
  const remoteOnly = exactArtifact();
  remoteOnly.data.singular_strata[0].vertices = [[0, 10], [0, 11]];
  assert.throws(
    () => buildROFieldViewModel(remoteOnly),
    /positively overlapping one-dimensional strata/,
  );

  const omitted = exactArtifact();
  const omittedTransition = omitted.data.facets.find(
    facet => facet.facet_id === 'transition',
  );
  omittedTransition.kind = 'regime_transition';
  omittedTransition.singular_stratum_ids = [];
  assert.throws(
    () => buildROFieldViewModel(omitted),
    /positively overlapping one-dimensional strata/,
  );

  const extra = exactArtifact();
  extra.data.source_candidate_regime_count += 1;
  extra.data.singular_stratum_order.push('singular_remote');
  extra.data.singular_strata.push({
    stratum_id: 'singular_remote',
    dimension: 1,
    vertices: [[0, 10], [0, 11]],
    source_regime_ids: ['regime_d'],
    nullities: [2],
    reasons: ['lower_dimensional_slice'],
  });
  extra.data.facets.find(
    facet => facet.facet_id === 'transition',
  ).singular_stratum_ids.push('singular_remote');
  assert.throws(
    () => buildROFieldViewModel(extra),
    /positively overlapping one-dimensional strata/,
  );

  const kindDrift = exactArtifact();
  kindDrift.data.facets.find(
    facet => facet.facet_id === 'transition',
  ).kind = 'regime_transition';
  assert.throws(
    () => buildROFieldViewModel(kindDrift),
    /regime transitions cannot reference singular strata/,
  );
});

test('complete exact evidence must cover the declared domain', () => {
  const artifact = completeExactArtifact();
  artifact.domain.axes.forEach(axis => {
    axis.bounds = { lower: -2, upper: 2 };
  });
  const cell = artifact.data.cells[0];
  cell.cell_id = 'small';
  cell.vertices = [[-1, -1], [1, -1], [1, 1], [-1, 1]];
  cell.area = 4;
  cell.polyhedron = { halfspaces: boxHalfspaces(-1, 1, -1, 1) };
  artifact.data.cells = [cell];
  artifact.data.cell_order = ['small'];
  artifact.data.source_candidate_regime_count = 1;
  artifact.data.regular_candidate_regime_count = 1;
  artifact.data.facets = [];
  artifact.data.facet_order = [];
  artifact.coverage.eligible_count = 1;
  artifact.coverage.evaluated_count = 1;
  artifact.coverage.valid_count = 1;
  artifact.coverage.storage.stored_count = 1;
  assert.throws(
    () => buildROFieldViewModel(artifact),
    /gap-free exact cells do not cover the complete declared domain/,
  );
});

test('translated shoelace area remains stable for a resolvable high-offset domain', () => {
  const artifact = stripedExactArtifact({ count: 1 });
  const lower = 1e8;
  const upper = lower + 1;
  artifact.domain.axes.forEach(axis => {
    axis.bounds = { lower, upper };
  });
  const cell = artifact.data.cells[0];
  cell.vertices = [[lower, lower], [upper, lower], [upper, upper], [lower, upper]];
  cell.area = 1;
  artifact.data.facets = [
    exactFacet('high_bottom', 'domain_boundary', [[lower, lower], [upper, lower]],
      ['cell_0'], 'axis2_lower'),
    exactFacet('high_left', 'domain_boundary', [[lower, lower], [lower, upper]],
      ['cell_0'], 'axis1_lower'),
    exactFacet('high_right', 'domain_boundary', [[upper, lower], [upper, upper]],
      ['cell_0'], 'axis1_upper'),
    exactFacet('high_top', 'domain_boundary', [[lower, upper], [upper, upper]],
      ['cell_0'], 'axis2_upper'),
  ];
  artifact.data.facet_order = artifact.data.facets.map(facet => facet.facet_id);

  const view = buildROFieldViewModel(artifact);
  assert.equal(view.cells[0].geometricArea, 1);
});

test('positive-area cell overlap and incomplete facet closure fail closed', () => {
  const overlap = completeExactArtifact();
  const duplicate = copy(overlap.data.cells[0]);
  duplicate.cell_id = 'overlap';
  duplicate.source_regime_ids = ['regime_overlap'];
  duplicate.label_order = ['label_overlap'];
  duplicate.affine_labels[0].label_id = 'label_overlap';
  duplicate.affine_labels[0].source_regime_ids = ['regime_overlap'];
  overlap.data.cells.push(duplicate);
  overlap.data.cell_order.push('overlap');
  overlap.data.source_candidate_regime_count = 3;
  overlap.data.regular_candidate_regime_count = 3;
  overlap.coverage.eligible_count = 3;
  overlap.coverage.evaluated_count = 3;
  overlap.coverage.valid_count = 3;
  overlap.coverage.storage.stored_count = 3;
  assert.throws(() => buildROFieldViewModel(overlap), /overlap with positive area/);

  const openBoundary = completeExactArtifact();
  openBoundary.data.facets = openBoundary.data.facets.filter(
    facet => facet.facet_id !== 'left-bottom',
  );
  openBoundary.data.facet_order = openBoundary.data.facets.map(facet => facet.facet_id);
  assert.throws(() => buildROFieldViewModel(openBoundary), /has no facet/);
});

test('cumulative sub-tolerance strip overlaps and holes fail global coverage', () => {
  const cumulativeOverlap = stripedExactArtifact({ interfaceDelta: 3e-10 });
  assert.throws(
    () => buildROFieldViewModel(cumulativeOverlap),
    /cumulative cell overlap with positive area exceeds the global tolerance/,
  );

  const cumulativeHoles = stripedExactArtifact({ interfaceDelta: -3e-10 });
  assert.throws(
    () => buildROFieldViewModel(cumulativeHoles),
    /gap-free exact cells do not cover the complete declared domain/,
  );

  const cancellingErrors = stripedExactArtifact({ count: 3 });
  const overlapWidth = 5e-9;
  const holeWidth = 1e-8;
  cancellingErrors.data.cells[0].vertices[1][0] += overlapWidth / 2;
  cancellingErrors.data.cells[0].vertices[2][0] += overlapWidth / 2;
  cancellingErrors.data.cells[1].vertices[0][0] -= overlapWidth / 2;
  cancellingErrors.data.cells[1].vertices[3][0] -= overlapWidth / 2;
  cancellingErrors.data.cells[1].vertices[1][0] -= holeWidth / 2;
  cancellingErrors.data.cells[1].vertices[2][0] -= holeWidth / 2;
  cancellingErrors.data.cells[2].vertices[0][0] += holeWidth / 2;
  cancellingErrors.data.cells[2].vertices[3][0] += holeWidth / 2;
  assert.throws(
    () => buildROFieldViewModel(cancellingErrors),
    /combined global overlap\/gap budget/,
  );
});

test('global geometry budgets still admit an exact 100-strip partition', () => {
  const view = buildROFieldViewModel(stripedExactArtifact());
  assert.equal(view.cells.length, 100);
  assert.equal(view.facets.length, 301);
});

test('scale-aware cross tolerances admit a resolvable micrometer-scale partition', () => {
  const artifact = stripedExactArtifact({ count: 2 });
  const scale = 1e-6;
  artifact.domain.axes.forEach(axis => {
    axis.bounds.lower *= scale;
    axis.bounds.upper *= scale;
  });
  artifact.data.cells.forEach(cell => {
    cell.vertices = cell.vertices.map(point => point.map(value => value * scale));
    cell.area *= scale * scale;
  });
  artifact.data.facets.forEach(facet => {
    facet.endpoints = facet.endpoints.map(point => point.map(value => value * scale));
    facet.offset *= scale;
  });

  const view = buildROFieldViewModel(artifact);
  assert.equal(view.cells.length, 2);
  assert.equal(view.facets.length, 7);

  const badOffset = structuredClone(artifact);
  badOffset.data.facets[0].offset += 1e-9;
  assert.throws(() => buildROFieldViewModel(badOffset), /does not contain endpoint/);

  const badArea = structuredClone(artifact);
  badArea.data.cells[0].area = 1e-9;
  assert.throws(() => buildROFieldViewModel(badArea), /polygon area/);
});

test('cumulative sub-tolerance facet gaps fail one global length budget', () => {
  const artifact = splitBottomFacetArtifact({ count: 100, gap: 3e-10 });
  assert.throws(
    () => buildROFieldViewModel(artifact),
    /cumulative uncovered facet length exceeds the global tolerance/,
  );
});

test('rational label identity does not collapse distinct values above 2^53', () => {
  const artifact = exactArtifact();
  artifact.data.coefficient_encoding = 'integer_or_rational_string';
  artifact.data.cells.forEach(cell => {
    rationalizePolyhedron(cell.polyhedron);
    cell.affine_labels.forEach(label => {
      label.output_offset = label.output_offset.map(String);
      label.reaction_order_matrix = label.reaction_order_matrix.map(row => row.map(String));
    });
  });
  artifact.data.gaps.forEach(gap => rationalizePolyhedron(gap.region));
  artifact.data.cells[0].affine_labels[0].reaction_order_matrix[0][0] = '9007199254740992';
  artifact.data.cells[0].affine_labels[1].reaction_order_matrix[0][0] = '9007199254740993';

  const view = buildROFieldViewModel(artifact);
  const identities = view.cells[0].labels.map(label => label.selectedExactValue);
  assert.deepEqual(identities, ['9007199254740992/1', '9007199254740993/1']);
  assert.notEqual(identities[0], identities[1]);

  const documentRef = new FakeDocument();
  const container = new FakeElement('div', documentRef);
  renderROFieldArtifact(container, artifact);
  const leftCell = descendants(container, node => (
    node.tagName === 'polygon' && node.attributes.get('data-cell-id') === 'left'
  ))[0];
  const tooltip = leftCell.children.find(child => child.tagName === 'title').textContent;
  assert.match(tooltip, /9007199254740992\/1/);
  assert.match(tooltip, /9007199254740993\/1/);
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
  assert.equal(call.options.signal instanceof AbortSignal, true);
  assert.equal(JSON.parse(call.options.body).storage.mode, 'inline');
  assert.equal(result.roField, field);

  const future = copy(field);
  future.schema_version = 'bne-ro-field/v2.0.0';
  assert.throws(() => extractROFieldResponse({ ro_field: future }), /must be exactly/);

  const persistent = copy(DEFAULT_RO_FIELD_REQUEST);
  persistent.storage.mode = 'artifact_reference';
  await assert.rejects(() => fetchROField(persistent, { fetchImpl }), /only sends storage.mode="inline"/);
});

test('demo request deadline rejects a hanging fetch and aborts its signal', async () => {
  let requestSignal = null;
  const hangingFetch = async (_url, options) => {
    requestSignal = options.signal;
    return new Promise(() => {});
  };
  await assert.rejects(
    () => fetchROField(DEFAULT_RO_FIELD_REQUEST, { fetchImpl: hangingFetch, timeoutMs: 5 }),
    error => error?.name === 'TimeoutError' && /timed out after 5 ms/.test(error.message),
  );
  assert.equal(requestSignal.aborted, true);
});

test('demo caller cancellation rejects an in-flight fetch and aborts its downstream signal', async () => {
  const caller = new AbortController();
  const reason = new Error('caller stopped the request');
  let requestSignal = null;
  const hangingFetch = async (_url, options) => {
    requestSignal = options.signal;
    return new Promise(() => {});
  };
  const pendingRequest = fetchROField(DEFAULT_RO_FIELD_REQUEST, {
    fetchImpl: hangingFetch,
    signal: caller.signal,
    timeoutMs: 100,
  });
  await Promise.resolve();
  caller.abort(reason);
  await assert.rejects(pendingRequest, error => error === reason);
  assert.equal(requestSignal.aborted, true);
});

test('demo deadline also bounds a response body that never settles', async () => {
  let requestSignal = null;
  const hangingJson = async (_url, options) => {
    requestSignal = options.signal;
    return {
      ok: true,
      status: 200,
      json: async () => new Promise(() => {}),
    };
  };
  await assert.rejects(
    () => fetchROField(DEFAULT_RO_FIELD_REQUEST, { fetchImpl: hangingJson, timeoutMs: 5 }),
    error => error?.name === 'TimeoutError' && /timed out after 5 ms/.test(error.message),
  );
  assert.equal(requestSignal.aborted, true);
});

test('demo pre-aborted caller fails before dispatching fetch', async () => {
  const caller = new AbortController();
  const reason = new Error('already cancelled');
  caller.abort(reason);
  let calls = 0;
  const fetchImpl = async () => {
    calls += 1;
    return new Promise(() => {});
  };
  await assert.rejects(
    () => fetchROField(DEFAULT_RO_FIELD_REQUEST, {
      fetchImpl,
      signal: caller.signal,
      timeoutMs: 100,
    }),
    error => error === reason,
  );
  assert.equal(calls, 0);
});

await Promise.all(pending);
console.log(`ro-field renderer contracts: ${passed}/${passed} passed`);
