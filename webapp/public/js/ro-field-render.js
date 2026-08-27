export const RO_FIELD_SCHEMA_VERSION = 'bne-ro-field/v1.0.0';

const SVG_NS = 'http://www.w3.org/2000/svg';
const SUPPORTED_REPRESENTATIONS = new Set([
  'sampled_grid',
  'exact_cell_complex',
]);
const MAX_INLINE_GRID_POINTS = 4_096;
const MAX_INLINE_CELLS = 128;
const MAX_INLINE_FACETS = 512;
const GEOMETRY_TOLERANCE = 1e-9;

export class ROFieldRenderError extends Error {
  constructor(path, message) {
    super(`${path}: ${message}`);
    this.name = 'ROFieldRenderError';
    this.path = path;
  }
}

function fail(path, message) {
  throw new ROFieldRenderError(path, message);
}

function isObject(value) {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

function objectAt(value, path) {
  if (!isObject(value)) fail(path, 'expected an object');
  return value;
}

function arrayAt(value, path) {
  if (!Array.isArray(value)) fail(path, 'expected an array');
  return value;
}

function exactKeys(value, allowed, required, path) {
  const object = objectAt(value, path);
  const observed = Object.keys(object);
  const unknown = observed.filter(key => !allowed.includes(key));
  if (unknown.length > 0) fail(path, `unsupported key(s): ${unknown.sort().join(', ')}`);
  const missing = required.filter(key => !Object.hasOwn(object, key));
  if (missing.length > 0) fail(path, `missing required key(s): ${missing.sort().join(', ')}`);
  return object;
}

function booleanAt(value, path) {
  if (typeof value !== 'boolean') fail(path, 'expected a boolean');
  return value;
}

function nonEmptyString(value, path) {
  if (typeof value !== 'string' || value.length === 0) fail(path, 'expected a non-empty string');
  return value;
}

function identifier(value, path, { safe = false } = {}) {
  nonEmptyString(value, path);
  const pattern = safe
    ? /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/
    : /^[A-Za-z][A-Za-z0-9._:-]{0,127}$/;
  if (!pattern.test(value)) fail(path, 'expected a safe v1 identifier');
  return value;
}

function uniqueStringArray(value, path, { minimum = 0, identifiers = false } = {}) {
  const values = arrayAt(value, path).map((item, index) => (
    identifiers ? identifier(item, `${path}[${index}]`) : nonEmptyString(item, `${path}[${index}]`)
  ));
  if (values.length < minimum) fail(path, `expected at least ${minimum} item(s)`);
  if (new Set(values).size !== values.length) fail(path, 'values must be unique');
  return values;
}

function finiteNumber(value, path) {
  if (typeof value !== 'number' || !Number.isFinite(value)) {
    fail(path, 'expected a finite number');
  }
  return value;
}

function coefficientNumber(value, path, encoding = null) {
  if (encoding === 'float64' && typeof value !== 'number') {
    fail(path, 'coefficient_encoding=float64 requires JSON numbers');
  }
  if (encoding === 'integer_or_rational_string' && typeof value !== 'string') {
    fail(path, 'coefficient_encoding=integer_or_rational_string requires strings');
  }
  if (typeof value === 'number') return finiteNumber(value, path);
  if (typeof value !== 'string' || !/^-?[0-9]+(?:\/[1-9][0-9]*)?$/.test(value)) {
    fail(path, 'expected a finite number or an integer/rational coefficient string');
  }
  const [numerator, denominator = '1'] = value.split('/');
  const parsed = Number(numerator) / Number(denominator);
  if (!Number.isFinite(parsed)) fail(path, 'coefficient is outside the renderable range');
  return parsed;
}

function integer(value, path, minimum = 0) {
  if (!Number.isInteger(value) || value < minimum) {
    fail(path, `expected an integer >= ${minimum}`);
  }
  return value;
}

function exactArray(actual, expected, path) {
  if (!Array.isArray(actual) || actual.length !== expected.length) {
    fail(path, `expected [${expected.join(', ')}]`);
  }
  expected.forEach((value, index) => {
    if (actual[index] !== value) fail(path, `expected [${expected.join(', ')}]`);
  });
}

function checkedProduct(shape, path) {
  let value = 1;
  shape.forEach((dimension, index) => {
    integer(dimension, `${path}[${index}]`, 1);
    if (value > Number.MAX_SAFE_INTEGER / dimension) {
      fail(path, 'shape product exceeds the safe integer range');
    }
    value *= dimension;
  });
  return value;
}

function assertFiniteOrNullArray(values, path) {
  arrayAt(values, path).forEach((value, index) => {
    if (value !== null) finiteNumber(value, `${path}[${index}]`);
  });
}

function assertOrder(order, items, idKey, path) {
  arrayAt(order, `${path}.order`);
  arrayAt(items, `${path}.items`);
  const ids = items.map((item, index) => {
    objectAt(item, `${path}.items[${index}]`);
    if (typeof item[idKey] !== 'string' || item[idKey].length === 0) {
      fail(`${path}.items[${index}].${idKey}`, 'expected a non-empty identifier');
    }
    return item[idKey];
  });
  exactArray(order, ids, `${path}.order`);
  if (new Set(ids).size !== ids.length) fail(`${path}.items`, 'identifiers must be unique');
  return ids;
}

function normalizeCommon(artifact) {
  objectAt(artifact, '$');
  if (artifact.schema_version !== RO_FIELD_SCHEMA_VERSION) {
    fail('$.schema_version', `unsupported schema; expected exactly ${RO_FIELD_SCHEMA_VERSION}`);
  }
  if (!SUPPORTED_REPRESENTATIONS.has(artifact.representation)) {
    fail('$.representation', 'unsupported reaction-order field representation');
  }
  identifier(artifact.field_id, '$.field_id', { safe: true });
  booleanAt(artifact.partial, '$.partial');
  objectAt(artifact.provenance, '$.provenance');

  const coverage = exactKeys(
    artifact.coverage,
    [
      'population_kind', 'eligible_count', 'evaluated_count', 'valid_count', 'invalid_count',
      'omitted_count', 'enumeration_complete', 'truncated', 'truncation', 'budget', 'storage',
    ],
    [
      'population_kind', 'eligible_count', 'evaluated_count', 'valid_count', 'invalid_count',
      'omitted_count', 'enumeration_complete', 'truncated', 'truncation', 'budget', 'storage',
    ],
    '$.coverage',
  );
  const expectedPopulation = artifact.representation === 'sampled_grid'
    ? 'grid_points' : 'cell_complex_items';
  if (coverage.population_kind !== expectedPopulation) {
    fail('$.coverage.population_kind', `expected ${expectedPopulation}`);
  }
  const eligible = integer(coverage.eligible_count, '$.coverage.eligible_count');
  const evaluated = integer(coverage.evaluated_count, '$.coverage.evaluated_count');
  const valid = integer(coverage.valid_count, '$.coverage.valid_count');
  const invalid = integer(coverage.invalid_count, '$.coverage.invalid_count');
  const omitted = integer(coverage.omitted_count, '$.coverage.omitted_count');
  if (evaluated !== valid + invalid) {
    fail('$.coverage.evaluated_count', 'must equal valid_count + invalid_count');
  }
  if (eligible !== evaluated + omitted) {
    fail('$.coverage.eligible_count', 'must equal evaluated_count + omitted_count');
  }
  const enumerationComplete = booleanAt(
    coverage.enumeration_complete,
    '$.coverage.enumeration_complete',
  );
  const truncated = booleanAt(coverage.truncated, '$.coverage.truncated');
  if (truncated) {
    const truncation = exactKeys(
      coverage.truncation,
      ['reason', 'detail'],
      ['reason', 'detail'],
      '$.coverage.truncation',
    );
    if (!['work_budget', 'time_budget', 'storage_budget', 'cancelled', 'operator_limit', 'other']
      .includes(truncation.reason)) fail('$.coverage.truncation.reason', 'unsupported reason');
    nonEmptyString(truncation.detail, '$.coverage.truncation.detail');
    if (enumerationComplete || omitted === 0) {
      fail('$.coverage.truncated', 'truncation requires incomplete enumeration and omitted items');
    }
  } else if (coverage.truncation !== null) {
    fail('$.coverage.truncation', 'must be null when truncated is false');
  }
  if (enumerationComplete && (truncated || omitted !== 0)) {
    fail('$.coverage.enumeration_complete', 'complete enumeration cannot omit or truncate items');
  }

  const budget = exactKeys(
    coverage.budget,
    ['work_unit_kind', 'max_evaluated_items', 'max_stored_items', 'max_payload_bytes', 'deadline_seconds'],
    ['work_unit_kind', 'max_evaluated_items', 'max_stored_items', 'max_payload_bytes', 'deadline_seconds'],
    '$.coverage.budget',
  );
  const expectedWorkKind = artifact.representation === 'sampled_grid'
    ? 'solver_samples' : 'source_regime_candidates';
  if (budget.work_unit_kind !== expectedWorkKind) {
    fail('$.coverage.budget.work_unit_kind', `expected ${expectedWorkKind}`);
  }
  const maxEvaluated = integer(
    budget.max_evaluated_items,
    '$.coverage.budget.max_evaluated_items',
    1,
  );
  const maxStored = integer(budget.max_stored_items, '$.coverage.budget.max_stored_items', 1);
  const maxPayload = integer(budget.max_payload_bytes, '$.coverage.budget.max_payload_bytes', 1);
  if (budget.deadline_seconds !== null
      && !(finiteNumber(budget.deadline_seconds, '$.coverage.budget.deadline_seconds') > 0)) {
    fail('$.coverage.budget.deadline_seconds', 'must be positive or null');
  }

  const storage = exactKeys(
    coverage.storage,
    ['mode', 'complete', 'stored_count', 'payload_bytes', 'content_sha256', 'artifacts'],
    ['mode', 'complete', 'stored_count', 'payload_bytes', 'content_sha256', 'artifacts'],
    '$.coverage.storage',
  );
  if (storage.mode !== 'inline') fail('$.coverage.storage.mode', 'renderer supports inline storage only');
  const storageComplete = booleanAt(storage.complete, '$.coverage.storage.complete');
  const stored = integer(storage.stored_count, '$.coverage.storage.stored_count');
  const payloadBytes = integer(storage.payload_bytes, '$.coverage.storage.payload_bytes');
  if (!/^[0-9a-f]{64}$/.test(storage.content_sha256)) {
    fail('$.coverage.storage.content_sha256', 'expected a lowercase SHA-256 value');
  }
  if (arrayAt(storage.artifacts, '$.coverage.storage.artifacts').length !== 0) {
    fail('$.coverage.storage.artifacts', 'inline storage cannot contain artifact references');
  }
  if (stored > evaluated) fail('$.coverage.storage.stored_count', 'cannot exceed evaluated_count');
  if (stored > maxStored) fail('$.coverage.storage.stored_count', 'exceeds max_stored_items');
  if (payloadBytes > maxPayload) fail('$.coverage.storage.payload_bytes', 'exceeds max_payload_bytes');
  if (storageComplete && stored !== evaluated) {
    fail('$.coverage.storage.stored_count', 'complete inline storage must store every evaluated item');
  }
  const partialExpected = invalid > 0 || omitted > 0 || truncated
    || !enumerationComplete || !storageComplete;
  if (artifact.partial !== partialExpected) {
    fail('$.partial', 'does not agree with coverage and inline storage');
  }

  const evidence = exactKeys(
    artifact.evidence,
    ['evidence_class', 'status', 'claim_scope', 'validity_policy', 'completeness_claim', 'limitations'],
    ['evidence_class', 'status', 'claim_scope', 'validity_policy', 'completeness_claim', 'limitations'],
    '$.evidence',
  );
  const expectedEvidence = artifact.representation === 'sampled_grid'
    ? 'sampled_numerical' : 'exact_polyhedral';
  if (evidence.evidence_class !== expectedEvidence) {
    fail('$.evidence.evidence_class', `expected ${expectedEvidence}`);
  }
  if (!['complete', 'partial', 'failed', 'unknown'].includes(evidence.status)) {
    fail('$.evidence.status', 'unsupported status');
  }
  if (evidence.claim_scope !== 'declared_domain_model_configuration_only') {
    fail('$.evidence.claim_scope', 'unsupported claim scope');
  }
  if (evidence.validity_policy !== 'invalid_is_gap') {
    fail('$.evidence.validity_policy', 'unsupported validity policy');
  }
  if (!['complete_over_declared_population', 'best_over_evaluated_prefix', 'no_positive_claim']
    .includes(evidence.completeness_claim)) {
    fail('$.evidence.completeness_claim', 'unsupported completeness claim');
  }
  uniqueStringArray(evidence.limitations, '$.evidence.limitations', { minimum: 1 });
  if (artifact.partial) {
    if (evidence.status === 'complete') fail('$.evidence.status', 'partial fields cannot be complete');
    if (evidence.completeness_claim === 'complete_over_declared_population') {
      fail('$.evidence.completeness_claim', 'partial fields cannot claim complete coverage');
    }
    if (invalid > 0 && evidence.completeness_claim !== 'no_positive_claim') {
      fail('$.evidence.completeness_claim', 'invalid items require no_positive_claim');
    }
  } else if (evidence.status !== 'complete'
      || evidence.completeness_claim !== 'complete_over_declared_population') {
    fail('$.evidence', 'complete fields require complete status and completeness claim');
  }

  const domain = objectAt(artifact.domain, '$.domain');
  const axes = arrayAt(domain.axes, '$.domain.axes');
  if (axes.length === 0) fail('$.domain.axes', 'at least one input axis is required');
  const axisIds = axes.map((axis, index) => {
    objectAt(axis, `$.domain.axes[${index}]`);
    if (typeof axis.axis_id !== 'string' || axis.axis_id.length === 0) {
      fail(`$.domain.axes[${index}].axis_id`, 'expected a non-empty identifier');
    }
    const bounds = objectAt(axis.bounds, `$.domain.axes[${index}].bounds`);
    const lower = finiteNumber(bounds.lower, `$.domain.axes[${index}].bounds.lower`);
    const upper = finiteNumber(bounds.upper, `$.domain.axes[${index}].bounds.upper`);
    if (!(lower < upper)) fail(`$.domain.axes[${index}].bounds`, 'lower must be less than upper');
    return axis.axis_id;
  });
  exactArray(domain.axis_order, axisIds, '$.domain.axis_order');
  if (new Set(axisIds).size !== axisIds.length) fail('$.domain.axes', 'axis identifiers must be unique');

  const outputs = objectAt(artifact.outputs, '$.outputs');
  const outputIds = assertOrder(
    outputs.output_order,
    outputs.items,
    'output_id',
    '$.outputs',
  );
  const expectedComponents = outputIds.flatMap(outputId => (
    axisIds.map(inputAxisId => ({ output_id: outputId, input_axis_id: inputAxisId }))
  ));
  const componentOrder = arrayAt(artifact.component_order, '$.component_order');
  if (componentOrder.length !== expectedComponents.length) {
    fail('$.component_order', 'must be the output-major output/input Cartesian product');
  }
  expectedComponents.forEach((expected, index) => {
    const actual = objectAt(componentOrder[index], `$.component_order[${index}]`);
    if (actual.output_id !== expected.output_id || actual.input_axis_id !== expected.input_axis_id) {
      fail('$.component_order', 'must be the output-major output/input Cartesian product');
    }
  });

  const warnings = [];
  if (artifact.partial) {
    warnings.push('Partial field: invalid, omitted, truncated, or unstored regions remain gaps/unknown.');
  }
  if (coverage.truncated === true) {
    warnings.push('Work was truncated; unevaluated regions are unknown, not negative results.');
  }
  if (coverage.storage?.complete === false) {
    warnings.push('Storage is incomplete; this inline view is not the complete field.');
  }

  return {
    artifact,
    representation: artifact.representation,
    rank: axisIds.length,
    axes,
    axisIds,
    outputs: outputs.items,
    outputIds,
    warnings,
    coverageCounts: { eligible, evaluated, valid, invalid, omitted },
    storageComplete,
    budgetLimits: { maxEvaluated, maxStored, maxPayload },
  };
}

function assertCoverageCounts(common, evaluated, valid, invalid) {
  const actual = common.coverageCounts;
  if (actual.evaluated !== evaluated || actual.valid !== valid || actual.invalid !== invalid) {
    fail('$.coverage', 'counts do not match the serialized representation population');
  }
}

function selectedIndices(common, selection) {
  const requestedOutput = selection.outputId ?? common.outputIds[0];
  const requestedAxis = selection.inputAxisId ?? common.axisIds[0];
  const outputIndex = common.outputIds.indexOf(requestedOutput);
  const inputIndex = common.axisIds.indexOf(requestedAxis);
  if (outputIndex < 0) fail('selection.outputId', `unknown output ${String(requestedOutput)}`);
  if (inputIndex < 0) fail('selection.inputAxisId', `unknown input axis ${String(requestedAxis)}`);
  return { outputIndex, inputIndex, outputId: requestedOutput, inputAxisId: requestedAxis };
}

function normalizeSampled(common, selection) {
  const data = objectAt(common.artifact.data, '$.data');
  if (data.flatten_order !== 'row_major_last_axis_fastest') {
    fail('$.data.flatten_order', 'the demo renderer only admits row_major_last_axis_fastest');
  }
  if (data.sampling_scheme !== 'cartesian_product') {
    fail('$.data.sampling_scheme', 'the demo renderer only admits Cartesian sampled grids');
  }
  const gridShape = arrayAt(data.grid_shape, '$.data.grid_shape').slice();
  if (gridShape.length !== common.rank) {
    fail('$.data.grid_shape', 'rank must equal the declared input-domain rank');
  }
  const pointCount = checkedProduct(gridShape, '$.data.grid_shape');
  if (pointCount > MAX_INLINE_GRID_POINTS) {
    fail('$.data.grid_shape', `demo-scale limit is ${MAX_INLINE_GRID_POINTS} inline grid points`);
  }
  if (pointCount > common.budgetLimits.maxEvaluated) {
    fail('$.data.grid_shape', 'sample population exceeds the declared work budget');
  }
  exactArray(data.output_shape, [...gridShape, common.outputIds.length], '$.data.output_shape');
  exactArray(
    data.reaction_order_shape,
    [...gridShape, common.outputIds.length, common.axisIds.length],
    '$.data.reaction_order_shape',
  );

  const coordinates = arrayAt(data.axis_coordinates, '$.data.axis_coordinates');
  if (coordinates.length !== common.rank) {
    fail('$.data.axis_coordinates', 'expected one coordinate vector per input axis');
  }
  coordinates.forEach((values, axisIndex) => {
    arrayAt(values, `$.data.axis_coordinates[${axisIndex}]`);
    if (values.length !== gridShape[axisIndex]) {
      fail(`$.data.axis_coordinates[${axisIndex}]`, 'length must match grid_shape');
    }
    values.forEach((value, coordinateIndex) => {
      finiteNumber(value, `$.data.axis_coordinates[${axisIndex}][${coordinateIndex}]`);
      if (coordinateIndex > 0 && !(values[coordinateIndex - 1] < value)) {
        fail(`$.data.axis_coordinates[${axisIndex}]`, 'coordinates must be strictly increasing');
      }
      const bounds = common.axes[axisIndex].bounds;
      if (value < bounds.lower || value > bounds.upper) {
        fail(`$.data.axis_coordinates[${axisIndex}][${coordinateIndex}]`, 'coordinate is outside the declared domain');
      }
    });
  });

  const outputValues = arrayAt(data.output_values, '$.data.output_values');
  const reactionOrderValues = arrayAt(data.reaction_order_values, '$.data.reaction_order_values');
  const validity = arrayAt(data.validity, '$.data.validity');
  const regimeIds = arrayAt(data.regime_ids, '$.data.regime_ids');
  const outputBlock = common.outputIds.length;
  const roBlock = common.outputIds.length * common.axisIds.length;
  if (outputValues.length !== pointCount * outputBlock) {
    fail('$.data.output_values', 'length does not match output_shape');
  }
  if (reactionOrderValues.length !== pointCount * roBlock) {
    fail('$.data.reaction_order_values', 'length does not match reaction_order_shape');
  }
  if (validity.length !== pointCount || regimeIds.length !== pointCount) {
    fail('$.data.validity', 'validity and regime_ids must contain one entry per grid point');
  }
  assertFiniteOrNullArray(outputValues, '$.data.output_values');
  assertFiniteOrNullArray(reactionOrderValues, '$.data.reaction_order_values');
  validity.forEach((valid, pointIndex) => {
    if (typeof valid !== 'boolean') fail(`$.data.validity[${pointIndex}]`, 'expected a boolean');
    const outputsAtPoint = outputValues.slice(pointIndex * outputBlock, (pointIndex + 1) * outputBlock);
    const roAtPoint = reactionOrderValues.slice(pointIndex * roBlock, (pointIndex + 1) * roBlock);
    if (valid && (outputsAtPoint.some(value => value === null) || roAtPoint.some(value => value === null))) {
      fail(`$.data.validity[${pointIndex}]`, 'a valid sample must have finite output and reaction-order values');
    }
    if (!valid && (outputsAtPoint.some(value => value !== null) || roAtPoint.some(value => value !== null))) {
      fail(`$.data.validity[${pointIndex}]`, 'an invalid sample must use null gaps, never numeric zero');
    }
    if (valid) identifier(regimeIds[pointIndex], `$.data.regime_ids[${pointIndex}]`);
    if (!valid && regimeIds[pointIndex] !== null) {
      fail(`$.data.regime_ids[${pointIndex}]`, 'an invalid sample must use a null regime id');
    }
  });

  const invalidCount = validity.filter(value => !value).length;
  assertCoverageCounts(common, pointCount, pointCount - invalidCount, invalidCount);
  const displaySemantics = 'display_only_not_interpolated';
  const sampledDisplayWarning = `${displaySemantics}: sampled values are discrete point evidence; `
    + 'blank space between markers is unsampled and is not a verified continuous field.';

  if (common.rank > 2) {
    return {
      ...common,
      kind: 'slice_required',
      displaySemantics,
      selection: selectedIndices(common, selection),
      gridShape,
      pointCount,
      warnings: [...common.warnings, sampledDisplayWarning],
      message: `${common.rank}D sampled field admitted. Request a declared 2D slice; this demo does not project or fabricate one.`,
    };
  }
  if (common.rank !== 2) {
    fail('$.domain.axes', 'the demo renderer requires a 2D sampled field; only higher-dimensional fields expose slice metadata');
  }

  const selected = selectedIndices(common, selection);
  const points = Array.from({ length: gridShape[0] }, () => Array(gridShape[1]));
  for (let input0Index = 0; input0Index < gridShape[0]; input0Index += 1) {
    for (let input1Index = 0; input1Index < gridShape[1]; input1Index += 1) {
      // Contract order: the final domain axis is fastest, then output, then RO input.
      const pointIndex = (input0Index * gridShape[1]) + input1Index;
      const outputOffset = (pointIndex * outputBlock) + selected.outputIndex;
      const roOffset = (pointIndex * roBlock)
        + (selected.outputIndex * common.axisIds.length)
        + selected.inputIndex;
      points[input0Index][input1Index] = {
        input0Index,
        input1Index,
        pointIndex,
        coordinates: [coordinates[0][input0Index], coordinates[1][input1Index]],
        valid: validity[pointIndex],
        outputValue: outputValues[outputOffset],
        reactionOrder: reactionOrderValues[roOffset],
        regimeId: regimeIds[pointIndex],
      };
    }
  }
  const warnings = [...common.warnings];
  warnings.push(sampledDisplayWarning);
  if (invalidCount > 0) warnings.push(`${invalidCount} sampled grid point(s) are rendered as explicit gaps.`);
  return {
    ...common,
    kind: 'sampled_2d',
    displaySemantics,
    selection: selected,
    gridShape,
    pointCount,
    coordinates,
    points,
    warnings,
  };
}

function halfspaces2D(polyhedron, path, encoding = null) {
  const halfspaces = arrayAt(objectAt(polyhedron, path).halfspaces, `${path}.halfspaces`);
  return halfspaces.map((halfspace, index) => {
    const currentPath = `${path}.halfspaces[${index}]`;
    objectAt(halfspace, currentPath);
    const coefficients = arrayAt(halfspace.coefficients, `${currentPath}.coefficients`);
    if (coefficients.length !== 2) fail(`${currentPath}.coefficients`, 'expected rank-2 coefficients');
    return {
      a: coefficientNumber(coefficients[0], `${currentPath}.coefficients[0]`, encoding),
      b: coefficientNumber(coefficients[1], `${currentPath}.coefficients[1]`, encoding),
      upper: coefficientNumber(halfspace.upper_bound, `${currentPath}.upper_bound`, encoding),
    };
  });
}

function validatePolyhedronRank(polyhedron, rank, path, encoding = null, requireNonEmpty = false) {
  const object = exactKeys(polyhedron, ['halfspaces'], ['halfspaces'], path);
  const halfspaces = arrayAt(object.halfspaces, `${path}.halfspaces`);
  if (requireNonEmpty && halfspaces.length === 0) fail(`${path}.halfspaces`, 'real geometry constraints are required');
  halfspaces.forEach((halfspace, index) => {
    exactKeys(
      halfspace,
      ['coefficients', 'upper_bound', 'source'],
      ['coefficients', 'upper_bound', 'source'],
      `${path}.halfspaces[${index}]`,
    );
    const coefficients = arrayAt(
      objectAt(halfspace, `${path}.halfspaces[${index}]`).coefficients,
      `${path}.halfspaces[${index}].coefficients`,
    );
    if (coefficients.length !== rank) {
      fail(`${path}.halfspaces[${index}].coefficients`, `expected rank-${rank} coefficients`);
    }
    coefficients.forEach((value, coefficientIndex) => {
      coefficientNumber(value, `${path}.halfspaces[${index}].coefficients[${coefficientIndex}]`, encoding);
    });
    coefficientNumber(halfspace.upper_bound, `${path}.halfspaces[${index}].upper_bound`, encoding);
    if (!['regime', 'domain_lower', 'domain_upper', 'singular_boundary'].includes(halfspace.source)) {
      fail(`${path}.halfspaces[${index}].source`, 'unsupported halfspace source');
    }
  });
}

function uniquePoints(points) {
  const result = [];
  points.forEach(point => {
    if (!result.some(other => (
      Math.abs(point[0] - other[0]) <= GEOMETRY_TOLERANCE
      && Math.abs(point[1] - other[1]) <= GEOMETRY_TOLERANCE
    ))) result.push(point);
  });
  return result;
}

function orderPolygon(points) {
  if (points.length < 3) return points.slice().sort((left, right) => (
    left[0] - right[0] || left[1] - right[1]
  ));
  const center = points.reduce(
    (sum, point) => [sum[0] + point[0] / points.length, sum[1] + point[1] / points.length],
    [0, 0],
  );
  return points.slice().sort((left, right) => (
    Math.atan2(left[1] - center[1], left[0] - center[0])
    - Math.atan2(right[1] - center[1], right[0] - center[0])
  ));
}

function deriveVertices2D(polyhedron, path, encoding = null) {
  const halfspaces = halfspaces2D(polyhedron, path, encoding);
  const candidates = [];
  for (let first = 0; first < halfspaces.length; first += 1) {
    for (let second = first + 1; second < halfspaces.length; second += 1) {
      const left = halfspaces[first];
      const right = halfspaces[second];
      const determinant = (left.a * right.b) - (left.b * right.a);
      if (Math.abs(determinant) <= GEOMETRY_TOLERANCE) continue;
      const x = ((left.upper * right.b) - (left.b * right.upper)) / determinant;
      const y = ((left.a * right.upper) - (left.upper * right.a)) / determinant;
      const feasible = halfspaces.every(({ a, b, upper }) => (
        (a * x) + (b * y) <= upper + GEOMETRY_TOLERANCE
      ));
      if (feasible) candidates.push([x, y]);
    }
  }
  return orderPolygon(uniquePoints(candidates));
}

function explicitVertices(value, rank, path) {
  const vertices = arrayAt(value, path);
  return vertices.map((vertex, vertexIndex) => {
    const coordinates = arrayAt(vertex, `${path}[${vertexIndex}]`);
    if (coordinates.length !== rank) {
      fail(`${path}[${vertexIndex}]`, `expected a rank-${rank} point`);
    }
    return coordinates.map((coordinate, coordinateIndex) => (
      coefficientNumber(coordinate, `${path}[${vertexIndex}][${coordinateIndex}]`)
    ));
  });
}

function signedPolygonArea(vertices) {
  if (vertices.length < 3) return 0;
  let twiceArea = 0;
  vertices.forEach((vertex, index) => {
    const next = vertices[(index + 1) % vertices.length];
    twiceArea += (vertex[0] * next[1]) - (next[0] * vertex[1]);
  });
  return twiceArea / 2;
}

function normalizeAffineLabels(cell, path, common, selected, encoding) {
  const labels = arrayAt(cell.affine_labels, `${path}.affine_labels`);
  const labelOrder = arrayAt(cell.label_order, `${path}.label_order`);
  const normalized = labels.map((label, labelIndex) => {
    const labelPath = `${path}.affine_labels[${labelIndex}]`;
    exactKeys(
      label,
      ['label_id', 'source_regime_ids', 'output_offset', 'reaction_order_matrix'],
      ['label_id', 'source_regime_ids', 'output_offset', 'reaction_order_matrix'],
      labelPath,
    );
    identifier(label.label_id, `${labelPath}.label_id`);
    const sourceRegimeIds = uniqueStringArray(
      label.source_regime_ids,
      `${labelPath}.source_regime_ids`,
      { minimum: 1, identifiers: true },
    );
    const offsets = arrayAt(label.output_offset, `${labelPath}.output_offset`);
    if (offsets.length !== common.outputIds.length) {
      fail(`${labelPath}.output_offset`, 'length must equal the output count');
    }
    offsets.forEach((value, index) => coefficientNumber(value, `${labelPath}.output_offset[${index}]`, encoding));
    const matrix = arrayAt(label.reaction_order_matrix, `${labelPath}.reaction_order_matrix`);
    if (matrix.length !== common.outputIds.length) {
      fail(`${labelPath}.reaction_order_matrix`, 'row count must equal the output count');
    }
    const numericMatrix = matrix.map((row, outputIndex) => {
      const values = arrayAt(row, `${labelPath}.reaction_order_matrix[${outputIndex}]`);
      if (values.length !== common.axisIds.length) {
        fail(`${labelPath}.reaction_order_matrix[${outputIndex}]`, 'column count must equal the input count');
      }
      return values.map((value, inputIndex) => coefficientNumber(
        value,
        `${labelPath}.reaction_order_matrix[${outputIndex}][${inputIndex}]`,
        encoding,
      ));
    });
    return {
      id: label.label_id,
      sourceRegimeIds,
      outputOffset: offsets.map((value, index) => coefficientNumber(value, `${labelPath}.output_offset[${index}]`, encoding)),
      matrix: numericMatrix,
      selectedValue: numericMatrix[selected.outputIndex][selected.inputIndex],
    };
  });
  exactArray(labelOrder, normalized.map(label => label.id), `${path}.label_order`);
  if (new Set(normalized.map(label => label.id)).size !== normalized.length) {
    fail(`${path}.affine_labels`, 'label identifiers must be unique');
  }
  return normalized;
}

function normalizeExact(common, selection) {
  if (common.rank !== 2) {
    fail('$.domain.axes', 'exact_cell_complex v1 and this renderer require exactly two axes');
  }
  const data = objectAt(common.artifact.data, '$.data');
  exactKeys(
    data,
    [
      'coefficient_encoding', 'source_candidate_regime_count', 'regular_candidate_regime_count',
      'cell_order', 'cells', 'facet_order', 'facets', 'singular_stratum_order',
      'singular_strata', 'gaps',
    ],
    [
      'coefficient_encoding', 'source_candidate_regime_count', 'regular_candidate_regime_count',
      'cell_order', 'cells', 'facet_order', 'facets', 'singular_stratum_order',
      'singular_strata', 'gaps',
    ],
    '$.data',
  );
  const encoding = data.coefficient_encoding;
  if (!['float64', 'integer_or_rational_string'].includes(encoding)) {
    fail('$.data.coefficient_encoding', 'unsupported coefficient encoding');
  }
  const sourceCandidateCount = integer(
    data.source_candidate_regime_count,
    '$.data.source_candidate_regime_count',
  );
  const regularCandidateCount = integer(
    data.regular_candidate_regime_count,
    '$.data.regular_candidate_regime_count',
  );
  if (regularCandidateCount > sourceCandidateCount) {
    fail('$.data.regular_candidate_regime_count', 'cannot exceed source_candidate_regime_count');
  }
  if (sourceCandidateCount > common.budgetLimits.maxEvaluated) {
    fail('$.data.source_candidate_regime_count', 'exceeds the declared work budget');
  }
  const selected = selectedIndices(common, selection);
  const cells = arrayAt(data.cells, '$.data.cells');
  const facets = arrayAt(data.facets, '$.data.facets');
  const singularSource = arrayAt(data.singular_strata, '$.data.singular_strata');
  const gaps = arrayAt(data.gaps, '$.data.gaps');
  if (cells.length > MAX_INLINE_CELLS) {
    fail('$.data.cells', `demo-scale limit is ${MAX_INLINE_CELLS} inline cells`);
  }
  if (facets.length > MAX_INLINE_FACETS) {
    fail('$.data.facets', `demo-scale limit is ${MAX_INLINE_FACETS} inline facets`);
  }
  exactArray(data.cell_order, cells.map(cell => cell?.cell_id), '$.data.cell_order');
  exactArray(data.facet_order, facets.map(facet => facet?.facet_id), '$.data.facet_order');
  exactArray(
    data.singular_stratum_order,
    singularSource.map(item => item?.stratum_id),
    '$.data.singular_stratum_order',
  );

  const fullCellSources = new Set();
  const normalizedCells = cells.map((cell, index) => {
    const path = `$.data.cells[${index}]`;
    exactKeys(
      cell,
      [
        'cell_id', 'dimension', 'status', 'vertices', 'area', 'polyhedron',
        'source_regime_ids', 'label_order', 'affine_labels', 'set_valued',
      ],
      [
        'cell_id', 'dimension', 'status', 'vertices', 'area',
        'source_regime_ids', 'label_order', 'affine_labels', 'set_valued',
      ],
      path,
    );
    identifier(cell.cell_id, `${path}.cell_id`);
    if (cell.dimension !== 2) fail(`${path}.dimension`, 'expected 2');
    const vertices = explicitVertices(cell.vertices, 2, `${path}.vertices`);
    if (vertices.length < 3) fail(`${path}.vertices`, 'a 2D cell requires at least three vertices');
    if (new Set(vertices.map(point => JSON.stringify(point))).size !== vertices.length) {
      fail(`${path}.vertices`, 'vertices must be unique');
    }
    const lexicographic = vertices.slice().sort((left, right) => left[0] - right[0] || left[1] - right[1]);
    if (vertices[0][0] !== lexicographic[0][0] || vertices[0][1] !== lexicographic[0][1]) {
      fail(`${path}.vertices`, 'the lexicographically smallest vertex must be first');
    }
    const signedArea = signedPolygonArea(vertices);
    if (!(signedArea > 0)) fail(`${path}.vertices`, 'vertices must be counter-clockwise with positive area');
    const area = finiteNumber(cell.area, `${path}.area`);
    if (!(area > 0) || Math.abs(area - signedArea) > 1e-8 * Math.max(1, area, signedArea)) {
      fail(`${path}.area`, 'must match the ordered polygon area');
    }
    vertices.forEach((point, vertexIndex) => {
      point.forEach((coordinate, axisIndex) => {
        const bounds = common.axes[axisIndex].bounds;
        if (coordinate < bounds.lower - GEOMETRY_TOLERANCE
            || coordinate > bounds.upper + GEOMETRY_TOLERANCE) {
          fail(`${path}.vertices[${vertexIndex}][${axisIndex}]`, 'cell vertex is outside the declared domain');
        }
      });
    });
    if (cell.polyhedron !== undefined) {
      validatePolyhedronRank(cell.polyhedron, 2, `${path}.polyhedron`, encoding, true);
    }
    const sourceRegimeIds = uniqueStringArray(
      cell.source_regime_ids,
      `${path}.source_regime_ids`,
      { minimum: 1, identifiers: true },
    );
    sourceRegimeIds.forEach(source => {
      if (fullCellSources.has(source)) fail(`${path}.source_regime_ids`, 'one source cannot own two cells');
      fullCellSources.add(source);
    });
    const labels = normalizeAffineLabels(cell, path, common, selected, encoding);
    const labelSources = new Set();
    labels.forEach(label => label.sourceRegimeIds.forEach(source => {
      if (labelSources.has(source)) fail(`${path}.affine_labels`, 'label source partitions overlap');
      labelSources.add(source);
    }));
    if (sourceRegimeIds.length !== labelSources.size
        || sourceRegimeIds.some(source => !labelSources.has(source))) {
      fail(`${path}.affine_labels`, 'label sources must partition cell source_regime_ids exactly');
    }
    const setValued = booleanAt(cell.set_valued, `${path}.set_valued`);
    if (!['regular', 'set_valued'].includes(cell.status)) fail(`${path}.status`, 'unsupported status');
    if ((cell.status === 'set_valued') !== setValued) fail(`${path}.status`, 'status and set_valued disagree');
    if (setValued && labels.length < 2) {
      fail(`${path}.affine_labels`, 'a set-valued cell must preserve at least two affine labels');
    }
    if (!setValued && labels.length !== 1) {
      fail(`${path}.affine_labels`, 'a regular cell must have exactly one affine label');
    }
    const signatures = labels.map(label => JSON.stringify([label.outputOffset, label.matrix]));
    if (new Set(signatures).size !== signatures.length) {
      fail(`${path}.affine_labels`, 'equal affine values must be merged');
    }
    return {
      id: cell.cell_id,
      status: cell.status,
      setValued,
      vertices,
      area,
      sourceRegimeIds,
      labels,
      selectedValues: labels.map(label => label.selectedValue),
    };
  });
  const knownCellIds = new Set(normalizedCells.map(cell => cell.id));
  if (knownCellIds.size !== normalizedCells.length) fail('$.data.cells', 'cell identifiers must be unique');
  if (regularCandidateCount !== fullCellSources.size) {
    fail('$.data.regular_candidate_regime_count', 'must equal the full-cell source population');
  }

  const representedSources = new Set(fullCellSources);
  const singularStrata = singularSource.map((stratum, index) => {
    const path = `$.data.singular_strata[${index}]`;
    exactKeys(
      stratum,
      ['stratum_id', 'dimension', 'vertices', 'source_regime_ids', 'nullities', 'reasons'],
      ['stratum_id', 'dimension', 'vertices', 'source_regime_ids', 'nullities', 'reasons'],
      path,
    );
    const id = identifier(stratum.stratum_id, `${path}.stratum_id`);
    const dimension = integer(stratum.dimension, `${path}.dimension`, 0);
    if (![0, 1].includes(dimension)) fail(`${path}.dimension`, 'expected 0 or 1');
    const vertices = explicitVertices(stratum.vertices, 2, `${path}.vertices`);
    if (vertices.length !== dimension + 1) {
      fail(`${path}.vertices`, 'vertex count must agree with the stratum dimension');
    }
    const sorted = vertices.slice().sort((left, right) => left[0] - right[0] || left[1] - right[1]);
    if (JSON.stringify(sorted) !== JSON.stringify(vertices)) {
      fail(`${path}.vertices`, 'vertices must be lexicographically ordered');
    }
    const sourceRegimeIds = uniqueStringArray(
      stratum.source_regime_ids,
      `${path}.source_regime_ids`,
      { minimum: 1, identifiers: true },
    );
    sourceRegimeIds.forEach(source => representedSources.add(source));
    const nullities = arrayAt(stratum.nullities, `${path}.nullities`).map((value, valueIndex) => (
      integer(value, `${path}.nullities[${valueIndex}]`)
    ));
    if (nullities.length === 0 || new Set(nullities).size !== nullities.length) {
      fail(`${path}.nullities`, 'expected non-empty unique nullities');
    }
    const reasons = uniqueStringArray(stratum.reasons, `${path}.reasons`, { minimum: 1 });
    if (reasons.some(reason => !['singular_regime', 'lower_dimensional_slice'].includes(reason))) {
      fail(`${path}.reasons`, 'unsupported singular-stratum reason');
    }
    return { id, dimension, vertices, sourceRegimeIds, nullities, reasons };
  });
  const knownStrata = new Map(singularStrata.map(stratum => [stratum.id, stratum]));
  if (knownStrata.size !== singularStrata.length) fail('$.data.singular_strata', 'identifiers must be unique');
  if (representedSources.size > sourceCandidateCount) {
    fail('$.data.source_candidate_regime_count', 'represented sources exceed inspected candidates');
  }

  const normalizedFacets = facets.map((facet, index) => {
    const path = `$.data.facets[${index}]`;
    exactKeys(
      facet,
      [
        'facet_id', 'dimension', 'kind', 'endpoints', 'polyhedron', 'incident_cell_ids',
        'singular_stratum_ids', 'normal', 'offset', 'mixed_sign', 'domain_side',
      ],
      [
        'facet_id', 'dimension', 'kind', 'endpoints', 'incident_cell_ids',
        'singular_stratum_ids', 'normal', 'offset', 'mixed_sign', 'domain_side',
      ],
      path,
    );
    identifier(facet.facet_id, `${path}.facet_id`);
    if (facet.dimension !== 1) fail(`${path}.dimension`, 'expected 1');
    if (!['domain_boundary', 'regime_transition', 'singular_boundary'].includes(facet.kind)) {
      fail(`${path}.kind`, 'unsupported facet kind');
    }
    const endpoints = explicitVertices(facet.endpoints, 2, `${path}.endpoints`);
    if (facet.polyhedron !== undefined) {
      validatePolyhedronRank(facet.polyhedron, 2, `${path}.polyhedron`, encoding, true);
    }
    if (endpoints.length !== 2) fail(`${path}.endpoints`, 'a 2D facet requires exactly two endpoints');
    const orderedEndpoints = endpoints.slice().sort((left, right) => left[0] - right[0] || left[1] - right[1]);
    if (JSON.stringify(orderedEndpoints) !== JSON.stringify(endpoints)) {
      fail(`${path}.endpoints`, 'endpoints must be lexicographically ordered');
    }
    const dx = endpoints[1][0] - endpoints[0][0];
    const dy = endpoints[1][1] - endpoints[0][1];
    const magnitude = Math.hypot(dx, dy);
    if (!(magnitude > 0)) fail(`${path}.endpoints`, 'facet must have positive length');
    const incidentCellIds = uniqueStringArray(
      facet.incident_cell_ids,
      `${path}.incident_cell_ids`,
      { minimum: 1, identifiers: true },
    );
    incidentCellIds.forEach((cellId, cellIndex) => {
      if (!knownCellIds.has(cellId)) fail(`${path}.incident_cell_ids[${cellIndex}]`, 'unknown incident cell');
      const cell = normalizedCells.find(item => item.id === cellId);
      endpoints.forEach((endpoint, endpointIndex) => {
        const matches = cell.vertices.some(vertex => (
          Math.abs(vertex[0] - endpoint[0]) <= GEOMETRY_TOLERANCE
          && Math.abs(vertex[1] - endpoint[1]) <= GEOMETRY_TOLERANCE
        ));
        if (!matches) fail(`${path}.endpoints[${endpointIndex}]`, 'not a vertex of every incident cell');
      });
    });
    const expectedIncidence = facet.kind === 'domain_boundary' ? 1 : 2;
    if (incidentCellIds.length !== expectedIncidence) {
      fail(`${path}.incident_cell_ids`, `expected ${expectedIncidence} incident cell(s)`);
    }
    const singularStratumIds = uniqueStringArray(
      facet.singular_stratum_ids,
      `${path}.singular_stratum_ids`,
      { identifiers: true },
    );
    singularStratumIds.forEach((stratumId, stratumIndex) => {
      const stratum = knownStrata.get(stratumId);
      if (!stratum) fail(`${path}.singular_stratum_ids[${stratumIndex}]`, 'unknown singular stratum');
      stratum.vertices.forEach(point => {
        const cross = dx * (point[1] - endpoints[0][1]) - dy * (point[0] - endpoints[0][0]);
        if (Math.abs(cross) > 1e-8 * Math.max(1, magnitude)) {
          fail(`${path}.singular_stratum_ids[${stratumIndex}]`, 'stratum does not lie on the facet');
        }
      });
    });
    if (facet.kind === 'singular_boundary' && singularStratumIds.length === 0) {
      fail(`${path}.singular_stratum_ids`, 'singular boundaries require a stratum reference');
    }
    if (facet.kind === 'regime_transition' && singularStratumIds.length !== 0) {
      fail(`${path}.singular_stratum_ids`, 'regime transitions cannot reference singular strata');
    }
    const normal = explicitVertices([facet.normal], 2, `${path}.normal-wrapper`)[0];
    let expectedNormal = [dy / magnitude, -dx / magnitude];
    if (expectedNormal[0] < -GEOMETRY_TOLERANCE
        || (Math.abs(expectedNormal[0]) <= GEOMETRY_TOLERANCE && expectedNormal[1] < 0)) {
      expectedNormal = expectedNormal.map(value => -value);
    }
    if (normal.some((value, normalIndex) => (
      Math.abs(value - expectedNormal[normalIndex]) > 1e-8
    ))) fail(`${path}.normal`, 'must be the canonical unit normal');
    const offset = finiteNumber(facet.offset, `${path}.offset`);
    endpoints.forEach((endpoint, endpointIndex) => {
      if (Math.abs((normal[0] * endpoint[0]) + (normal[1] * endpoint[1]) + offset) > 1e-8) {
        fail(`${path}.offset`, `does not contain endpoint ${endpointIndex}`);
      }
    });
    const mixedSign = booleanAt(facet.mixed_sign, `${path}.mixed_sign`);
    if (mixedSign !== (normal[0] * normal[1] < 0)) {
      fail(`${path}.mixed_sign`, 'does not agree with the canonical normal');
    }
    let domainSide = null;
    if (facet.kind === 'domain_boundary') {
      if (!['axis1_lower', 'axis1_upper', 'axis2_lower', 'axis2_upper'].includes(facet.domain_side)) {
        fail(`${path}.domain_side`, 'domain boundary requires a valid side');
      }
      domainSide = facet.domain_side;
      const axisIndex = domainSide.startsWith('axis1') ? 0 : 1;
      const bound = domainSide.endsWith('lower')
        ? common.axes[axisIndex].bounds.lower : common.axes[axisIndex].bounds.upper;
      if (endpoints.some(endpoint => Math.abs(endpoint[axisIndex] - bound) > 1e-8)) {
        fail(`${path}.domain_side`, 'does not match the facet endpoints');
      }
    } else if (facet.domain_side !== null) {
      fail(`${path}.domain_side`, 'internal facets require null domain_side');
    }
    return {
      id: facet.facet_id,
      kind: facet.kind,
      endpoints,
      incidentCellIds,
      normal,
      offset,
      mixedSign,
      domainSide,
      singularStratumIds,
    };
  });
  if (new Set(normalizedFacets.map(facet => facet.id)).size !== normalizedFacets.length) {
    fail('$.data.facets', 'facet identifiers must be unique');
  }

  const normalizedGaps = gaps.map((gap, index) => {
    const path = `$.data.gaps[${index}]`;
    exactKeys(gap, ['gap_id', 'reason', 'region', 'detail'], ['gap_id', 'reason', 'region'], path);
    identifier(gap.gap_id, `${path}.gap_id`);
    if (!['singular', 'higher_nullity', 'invalid_geometry', 'unclassified', 'truncated'].includes(gap.reason)) {
      fail(`${path}.reason`, 'unsupported gap reason');
    }
    validatePolyhedronRank(gap.region, 2, `${path}.region`, encoding, true);
    const vertices = deriveVertices2D(gap.region, `${path}.region`, encoding);
    if (vertices.length === 0) fail(`${path}.region`, 'gap geometry has no renderable finite point');
    return { id: gap.gap_id, reason: gap.reason, detail: gap.detail ?? '', vertices };
  });
  if (new Set(normalizedGaps.map(gap => gap.id)).size !== normalizedGaps.length) {
    fail('$.data.gaps', 'gap identifiers must be unique');
  }

  const setValuedCount = normalizedCells.filter(cell => cell.setValued).length;
  assertCoverageCounts(
    common,
    normalizedCells.length + singularStrata.length + normalizedGaps.length,
    normalizedCells.length - setValuedCount,
    setValuedCount + singularStrata.length + normalizedGaps.length,
  );

  const warnings = [...common.warnings];
  if (setValuedCount > 0) {
    warnings.push(`${setValuedCount} coincident cell(s) are set-valued; no single reaction order is selected.`);
  }
  if (singularStrata.length > 0) {
    warnings.push(`${singularStrata.length} singular stratum/strata are shown as invalid boundaries.`);
  }
  if (normalizedGaps.length > 0) {
    warnings.push(`${normalizedGaps.length} exact-region gap(s) are shown as unknown/invalid.`);
  }
  return {
    ...common,
    kind: 'exact_2d',
    selection: selected,
    cells: normalizedCells,
    facets: normalizedFacets,
    singularStrata,
    gaps: normalizedGaps,
    sourceCandidateCount,
    regularCandidateCount,
    warnings,
  };
}

export function buildROFieldViewModel(artifact, selection = {}) {
  const common = normalizeCommon(artifact);
  if (common.representation === 'sampled_grid') return normalizeSampled(common, selection);
  if (common.representation === 'exact_cell_complex') return normalizeExact(common, selection);
  fail('$.representation', 'unsupported reaction-order field representation');
}

function element(documentRef, tag, className, text) {
  const node = documentRef.createElement(tag);
  if (className) node.className = className;
  if (text !== undefined) node.textContent = text;
  return node;
}

function svgElement(documentRef, tag, attributes = {}) {
  const node = documentRef.createElementNS(SVG_NS, tag);
  Object.entries(attributes).forEach(([name, value]) => node.setAttribute(name, String(value)));
  return node;
}

function appendTitle(documentRef, node, text) {
  const title = svgElement(documentRef, 'title');
  title.textContent = text;
  node.appendChild(title);
}

function selectedLabel(view) {
  const output = view.outputs[view.selection.outputIndex];
  const axis = view.axes[view.selection.inputIndex];
  return `${output.symbol ?? output.output_id} / ${axis.symbol ?? axis.axis_id}`;
}

function valueColor(value, minimum, maximum) {
  if (!Number.isFinite(value)) return '#d9dee5';
  const span = maximum - minimum;
  const unit = span <= Number.EPSILON ? 0.5 : (value - minimum) / span;
  const clamped = Math.max(0, Math.min(1, unit));
  const red = Math.round(43 + (207 * clamped));
  const green = Math.round(124 + (Math.abs(clamped - 0.5) * -70));
  const blue = Math.round(184 - (135 * clamped));
  return `rgb(${red}, ${green}, ${blue})`;
}

function addSvgFrame(documentRef, svg, view, plot) {
  const frame = svgElement(documentRef, 'rect', {
    x: plot.left,
    y: plot.top,
    width: plot.width,
    height: plot.height,
    fill: 'none',
    stroke: '#2d3748',
    'stroke-width': 1.5,
  });
  svg.appendChild(frame);
  const xLabel = svgElement(documentRef, 'text', {
    x: plot.left + (plot.width / 2),
    y: plot.top + plot.height + 42,
    'text-anchor': 'middle',
    class: 'ro-field-axis-label',
  });
  xLabel.textContent = view.axes[0].symbol ?? view.axes[0].axis_id;
  svg.appendChild(xLabel);
  const yLabel = svgElement(documentRef, 'text', {
    x: 18,
    y: plot.top + (plot.height / 2),
    transform: `rotate(-90 18 ${plot.top + (plot.height / 2)})`,
    'text-anchor': 'middle',
    class: 'ro-field-axis-label',
  });
  yLabel.textContent = view.axes[1].symbol ?? view.axes[1].axis_id;
  svg.appendChild(yLabel);
}

function makeSvg(documentRef) {
  const svg = svgElement(documentRef, 'svg', {
    viewBox: '0 0 720 540',
    role: 'img',
    class: 'ro-field-svg',
    'aria-label': 'Two-dimensional multi-input reaction-order field',
  });
  const defs = svgElement(documentRef, 'defs');
  const gapPattern = svgElement(documentRef, 'pattern', {
    id: 'ro-field-gap-pattern',
    width: 10,
    height: 10,
    patternUnits: 'userSpaceOnUse',
    patternTransform: 'rotate(45)',
  });
  gapPattern.appendChild(svgElement(documentRef, 'rect', {
    width: 10, height: 10, fill: '#f4f5f7',
  }));
  gapPattern.appendChild(svgElement(documentRef, 'line', {
    x1: 0, y1: 0, x2: 0, y2: 10, stroke: '#8a94a3', 'stroke-width': 3,
  }));
  const setPattern = svgElement(documentRef, 'pattern', {
    id: 'ro-field-set-valued-pattern',
    width: 12,
    height: 12,
    patternUnits: 'userSpaceOnUse',
  });
  setPattern.appendChild(svgElement(documentRef, 'rect', {
    width: 12, height: 12, fill: '#fff4cc',
  }));
  setPattern.appendChild(svgElement(documentRef, 'path', {
    d: 'M0,12 L12,0 M-3,3 L3,-3 M9,15 L15,9',
    stroke: '#a15c00',
    'stroke-width': 2,
  }));
  defs.appendChild(gapPattern);
  defs.appendChild(setPattern);
  svg.appendChild(defs);
  return svg;
}

function renderSampledSvg(documentRef, view) {
  const svg = makeSvg(documentRef);
  svg.setAttribute('data-display-semantics', view.displaySemantics);
  const transform = coordinateTransform(view);
  const finiteValues = view.points.flat().filter(point => point.valid).map(point => point.reactionOrder);
  const minimum = finiteValues.length ? Math.min(...finiteValues) : 0;
  const maximum = finiteValues.length ? Math.max(...finiteValues) : 1;
  view.points.forEach(column => column.forEach(point => {
    const [x, y] = transform.point(point.coordinates);
    const marker = svgElement(documentRef, 'circle', {
      cx: x,
      cy: y,
      r: point.valid ? 13 : 15,
      fill: point.valid ? valueColor(point.reactionOrder, minimum, maximum) : 'url(#ro-field-gap-pattern)',
      stroke: point.valid ? '#ffffff' : '#b42318',
      'stroke-width': point.valid ? 2 : 3,
      'data-point-index': point.pointIndex,
      'data-valid': point.valid,
      'data-coordinate-x': point.coordinates[0],
      'data-coordinate-y': point.coordinates[1],
    });
    appendTitle(
      documentRef,
      marker,
      point.valid
        ? `${view.axes[0].symbol ?? view.axisIds[0]}=${point.coordinates[0]}, ${view.axes[1].symbol ?? view.axisIds[1]}=${point.coordinates[1]}; DISCRETE SAMPLE; output=${point.outputValue}; RO=${point.reactionOrder}; regime=${point.regimeId ?? 'none'}`
        : `${view.axes[0].symbol ?? view.axisIds[0]}=${point.coordinates[0]}, ${view.axes[1].symbol ?? view.axisIds[1]}=${point.coordinates[1]}; GAP (invalid sample)`,
    );
    svg.appendChild(marker);
    if (!point.valid) {
      const label = svgElement(documentRef, 'text', {
        x,
        y,
        'text-anchor': 'middle',
        'dominant-baseline': 'middle',
        class: 'ro-field-gap-label',
      });
      label.textContent = 'gap';
      svg.appendChild(label);
    }
  }));
  addSvgFrame(documentRef, svg, view, transform.plot);
  return svg;
}

function coordinateTransform(view) {
  const plot = { left: 72, top: 36, width: 600, height: 440 };
  const xBounds = view.axes[0].bounds;
  const yBounds = view.axes[1].bounds;
  return {
    plot,
    point([x, y]) {
      return [
        plot.left + (((x - xBounds.lower) / (xBounds.upper - xBounds.lower)) * plot.width),
        plot.top + plot.height - (((y - yBounds.lower) / (yBounds.upper - yBounds.lower)) * plot.height),
      ];
    },
  };
}

function pointsAttribute(transform, vertices) {
  return vertices.map(vertex => transform.point(vertex).join(',')).join(' ');
}

function renderExactSvg(documentRef, view) {
  const svg = makeSvg(documentRef);
  const transform = coordinateTransform(view);
  const singleValues = view.cells
    .filter(cell => !cell.setValued && cell.selectedValues.length === 1)
    .map(cell => cell.selectedValues[0]);
  const minimum = singleValues.length ? Math.min(...singleValues) : 0;
  const maximum = singleValues.length ? Math.max(...singleValues) : 1;

  view.cells.forEach(cell => {
    const polygon = svgElement(documentRef, 'polygon', {
      points: pointsAttribute(transform, cell.vertices),
      fill: cell.setValued
        ? 'url(#ro-field-set-valued-pattern)'
        : valueColor(cell.selectedValues[0], minimum, maximum),
      stroke: cell.setValued ? '#a15c00' : '#ffffff',
      'stroke-width': cell.setValued ? 3 : 1.5,
      'data-cell-id': cell.id,
      'data-set-valued': cell.setValued,
    });
    appendTitle(
      documentRef,
      polygon,
      `${cell.id}; ${cell.setValued ? 'SET-VALUED' : `RO=${cell.selectedValues[0]}`}; area=${cell.area}; labels=${cell.labels.map(label => label.id).join(', ')}`,
    );
    svg.appendChild(polygon);
  });

  view.gaps.forEach(gap => {
    const transformed = pointsAttribute(transform, gap.vertices);
    const gapNode = gap.vertices.length >= 3
      ? svgElement(documentRef, 'polygon', {
        points: transformed,
        fill: 'url(#ro-field-gap-pattern)',
        stroke: '#b42318',
        'stroke-width': 2.5,
      })
      : svgElement(documentRef, 'polyline', {
        points: transformed,
        fill: 'none',
        stroke: '#b42318',
        'stroke-width': 7,
        'stroke-dasharray': '7 5',
      });
    appendTitle(documentRef, gapNode, `GAP ${gap.id}: ${gap.reason}${gap.detail ? `; ${gap.detail}` : ''}`);
    svg.appendChild(gapNode);
  });

  view.facets.forEach(facet => {
    const [start, end] = facet.endpoints.map(transform.point);
    const line = svgElement(documentRef, 'line', {
      x1: start[0], y1: start[1], x2: end[0], y2: end[1],
      stroke: facet.mixedSign ? '#c2410c' : (facet.kind === 'singular_boundary' ? '#b42318' : '#263342'),
      'stroke-width': facet.mixedSign || facet.kind === 'singular_boundary' ? 4 : 2,
      'stroke-dasharray': facet.kind === 'singular_boundary' ? '8 5' : 'none',
      'data-facet-id': facet.id,
    });
    appendTitle(
      documentRef,
      line,
      `${facet.id}; ${facet.kind}; incident=${facet.incidentCellIds.join(', ')}${facet.mixedSign ? '; mixed-sign transition' : ''}`,
    );
    svg.appendChild(line);
  });

  view.singularStrata.forEach(stratum => {
    if (stratum.vertices.length === 1) {
      const [centerX, centerY] = transform.point(stratum.vertices[0]);
      const circle = svgElement(documentRef, 'circle', {
        cx: centerX, cy: centerY, r: 7, fill: '#b42318', stroke: '#ffffff', 'stroke-width': 2,
      });
      appendTitle(documentRef, circle, `SINGULAR ${stratum.id}; ${stratum.reasons.join(', ')}`);
      svg.appendChild(circle);
    } else {
      const polyline = svgElement(documentRef, 'polyline', {
        points: pointsAttribute(transform, stratum.vertices),
        fill: 'none',
        stroke: '#b42318',
        'stroke-width': 5,
        'stroke-dasharray': '10 6',
      });
      appendTitle(documentRef, polyline, `SINGULAR ${stratum.id}; ${stratum.reasons.join(', ')}`);
      svg.appendChild(polyline);
    }
  });
  addSvgFrame(documentRef, svg, view, transform.plot);
  return svg;
}

function appendMetadata(documentRef, container, view) {
  const metadata = element(documentRef, 'dl', 'ro-field-metadata');
  const pairs = [
    ['Field', view.artifact.field_id],
    ['Schema', view.artifact.schema_version],
    ['Representation', view.representation],
    ['Input rank', String(view.rank)],
    ['Selected component', selectedLabel(view)],
    ['Evidence', view.artifact.evidence?.evidence_class ?? 'not declared'],
    ['Storage in response', view.artifact.coverage?.storage?.mode ?? 'not declared'],
  ];
  if (view.displaySemantics) pairs.push(['Display semantics', view.displaySemantics]);
  pairs.forEach(([term, description]) => {
    metadata.appendChild(element(documentRef, 'dt', null, term));
    metadata.appendChild(element(documentRef, 'dd', null, description));
  });
  container.appendChild(metadata);
}

function appendWarnings(documentRef, container, warnings) {
  if (warnings.length === 0) return;
  const warningBox = element(documentRef, 'section', 'ro-field-warnings');
  warningBox.setAttribute('role', 'alert');
  warningBox.appendChild(element(documentRef, 'h3', null, 'Evidence and validity warnings'));
  const list = element(documentRef, 'ul');
  warnings.forEach(warning => list.appendChild(element(documentRef, 'li', null, warning)));
  warningBox.appendChild(list);
  container.appendChild(warningBox);
}

function appendSelectionControls(documentRef, container, view, onChange) {
  const controls = element(documentRef, 'div', 'ro-field-component-controls');
  const outputLabel = element(documentRef, 'label', null, 'Output ');
  const outputSelect = element(documentRef, 'select');
  outputSelect.setAttribute('aria-label', 'Reaction-order output');
  view.outputs.forEach((output, index) => {
    const option = element(documentRef, 'option', null, output.symbol ?? output.output_id);
    option.value = output.output_id;
    option.selected = index === view.selection.outputIndex;
    outputSelect.appendChild(option);
  });
  outputLabel.appendChild(outputSelect);
  const axisLabel = element(documentRef, 'label', null, 'Derivative input ');
  const axisSelect = element(documentRef, 'select');
  axisSelect.setAttribute('aria-label', 'Reaction-order derivative input');
  view.axes.forEach((axis, index) => {
    const option = element(documentRef, 'option', null, axis.symbol ?? axis.axis_id);
    option.value = axis.axis_id;
    option.selected = index === view.selection.inputIndex;
    axisSelect.appendChild(option);
  });
  axisLabel.appendChild(axisSelect);
  const update = () => onChange({ outputId: outputSelect.value, inputAxisId: axisSelect.value });
  outputSelect.addEventListener('change', update);
  axisSelect.addEventListener('change', update);
  controls.appendChild(outputLabel);
  controls.appendChild(axisLabel);
  container.appendChild(controls);
}

export function renderROFieldArtifact(container, artifact, options = {}) {
  if (!container || typeof container.replaceChildren !== 'function') {
    fail('container', 'expected a DOM element with replaceChildren');
  }
  const documentRef = container.ownerDocument ?? globalThis.document;
  if (!documentRef?.createElement || !documentRef?.createElementNS) {
    fail('container.ownerDocument', 'DOM and SVG creation are unavailable');
  }
  let selection = {
    outputId: options.outputId,
    inputAxisId: options.inputAxisId,
  };
  let currentView = null;

  const paint = () => {
    currentView = buildROFieldViewModel(artifact, selection);
    container.replaceChildren();
    const heading = element(documentRef, 'h2', 'ro-field-result-title', 'Reaction-order field');
    container.appendChild(heading);
    appendMetadata(documentRef, container, currentView);
    appendWarnings(documentRef, container, currentView.warnings);
    appendSelectionControls(documentRef, container, currentView, nextSelection => {
      selection = nextSelection;
      paint();
    });
    if (currentView.kind === 'sampled_2d') {
      container.appendChild(renderSampledSvg(documentRef, currentView));
    } else if (currentView.kind === 'exact_2d') {
      container.appendChild(renderExactSvg(documentRef, currentView));
    } else {
      const notice = element(documentRef, 'p', 'ro-field-slice-needed', currentView.message);
      notice.setAttribute('role', 'status');
      container.appendChild(notice);
    }
  };
  paint();
  return {
    get viewModel() { return currentView; },
    select(nextSelection) {
      selection = { ...selection, ...nextSelection };
      paint();
      return currentView;
    },
  };
}
