// All authoring routes produce physical input concentrations and target values
// in the chosen readout coordinates. Keep this module independent of the DOM.
const SCHEMA = 'bne-design-target/v1.0.0';
const MAX_SAMPLES = 4096;
const SOURCES = new Set(['curve', 'image', 'trajectory', 'data', 'agent']);
const IDENTIFIER = /^[A-Za-z][A-Za-z0-9_]*$/;

function object(value, label) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error(`${label} must be an object.`);
  return value;
}

function number(value, label) {
  if (typeof value !== 'number' || !Number.isFinite(value)) throw new Error(`${label} must be a finite number.`);
  return value;
}

function integer(value, label, min, max) {
  number(value, label);
  if (!Number.isInteger(value) || value < min || value > max) throw new Error(`${label} must be an integer from ${min} to ${max}.`);
  return value;
}

function text(value, label, { identifier = false } = {}) {
  if (typeof value !== 'string' || !value.trim() || new TextEncoder().encode(value).length > 80 || /[\u0000-\u001f\u007f]/.test(value)) {
    throw new Error(`${label} must be a non-empty label of at most 80 UTF-8 bytes.`);
  }
  const result = value.trim();
  if (identifier && (!IDENTIFIER.test(result) || result.length > 40)) throw new Error(`${label} must be a chemical identifier of at most 40 characters (letters, digits, and underscores, starting with a letter).`);
  return result;
}

// Reject values that JSON.stringify would silently omit or change to null.
export function cloneDesignTarget(value, parents = new Set()) {
  if (value === null || typeof value === 'string' || typeof value === 'boolean') return value;
  if (typeof value === 'number') return number(value, 'Target data');
  if (typeof value !== 'object' || parents.has(value)) throw new Error('Target data must be acyclic plain JSON.');
  if (!Array.isArray(value) && Object.getPrototypeOf(value) !== Object.prototype && Object.getPrototypeOf(value) !== null) {
    throw new Error('Target data must contain only ordinary JSON objects.');
  }
  if (Object.getOwnPropertySymbols(value).length) throw new Error('Target data must contain only JSON keys.');
  parents.add(value);
  const copy = Array.isArray(value)
    ? Array.from(value, entry => cloneDesignTarget(entry, parents))
    : Object.fromEntries(Object.entries(value).map(([key, entry]) => [key, cloneDesignTarget(entry, parents)]));
  parents.delete(value);
  return copy;
}

function inputDefinition(raw, index = 0) {
  const label = `inputs[${index + 1}]`;
  object(raw, label);
  const name = text(raw.name, `${label}.name`, { identifier: true });
  const min = number(raw.min, `${label}.min`);
  const max = number(raw.max, `${label}.max`);
  if (min <= 0 || max <= min) throw new Error(`${label} requires a positive minimum and a maximum greater than its minimum.`);
  const scale = raw.scale ?? 'linear';
  if (!['linear', 'log'].includes(scale)) throw new Error(`${label}.scale must be linear or log.`);
  return { name, min, max, scale };
}

function outputDefinition(raw, index = 0) {
  const label = `outputs[${index + 1}]`;
  object(raw, label);
  const name = text(raw.name, `${label}.name`);
  const species = text(raw.species, `${label}.species`, { identifier: true });
  const transform = raw.transform ?? 'linear';
  if (!['linear', 'log10'].includes(transform)) throw new Error(`${label}.transform must be linear or log10.`);
  const offset = number(raw.offset ?? 0, `${label}.offset`);
  const optimize_offset = raw.optimize_offset ?? false;
  if (typeof optimize_offset !== 'boolean') throw new Error(`${label}.optimize_offset must be true or false.`);
  const result = { name, species, transform, offset, optimize_offset };
  if (raw.min !== undefined) result.min = number(raw.min, `${label}.min`);
  if (raw.max !== undefined) result.max = number(raw.max, `${label}.max`);
  if (result.min !== undefined && result.max !== undefined && result.max <= result.min) {
    throw new Error(`${label}.max must exceed its minimum.`);
  }
  return result;
}

function definitions(raw, label, normalize) {
  if (!Array.isArray(raw) || raw.length < 1 || raw.length > 3) throw new Error(`${label} must contain 1 to 3 dimensions.`);
  const result = Array.from(raw, normalize);
  if (new Set(result.map(entry => entry.name)).size !== result.length) throw new Error(`${label} names must be unique.`);
  return result;
}

function dimensions(inputs, outputs, inputCount, outputCount, kind) {
  const normalizedInputs = definitions(inputs, 'inputs', inputDefinition);
  const normalizedOutputs = definitions(outputs, 'outputs', outputDefinition);
  if (normalizedInputs.length !== inputCount || normalizedOutputs.length !== outputCount) {
    throw new Error(`${kind} requires ${inputCount} input dimension${inputCount === 1 ? '' : 's'} and ${outputCount} output dimension${outputCount === 1 ? '' : 's'}. Use numerical samples for another mapping.`);
  }
  return [normalizedInputs, normalizedOutputs];
}

export function validateDesignTarget(raw) {
  const target = object(cloneDesignTarget(raw), 'Design target');
  if (target.schema_version !== SCHEMA) throw new Error(`Design target schema_version must be ${SCHEMA}.`);
  if (typeof target.description !== 'string' || new TextEncoder().encode(target.description).length > 16000) throw new Error('Target description must be text of at most 16000 UTF-8 bytes.');
  if (!SOURCES.has(target.source)) throw new Error('Target source must be curve, image, trajectory, data, or agent.');
  const inputs = definitions(target.inputs, 'inputs', inputDefinition);
  const outputs = definitions(target.outputs, 'outputs', outputDefinition);
  // Match the physical solver's supported bounds before starting a job. Pure
  // coordinate adapters remain useful for previewing a wider finite range.
  function bounded(value, label, min, max) {
    if (value < min || value > max) throw new Error(`${label} must be between ${min} and ${max}.`);
  }
  inputs.forEach((input, index) => {
    bounded(input.min, `inputs[${index + 1}].min`, 1e-8, 1e8);
    bounded(input.max, `inputs[${index + 1}].max`, 1e-8, 1e8);
  });
  outputs.forEach((output, index) => {
    bounded(output.offset, `outputs[${index + 1}].offset`, -8, 8);
    if (output.min !== undefined) bounded(output.min, `outputs[${index + 1}].min`, -1e8, 1e8);
    if (output.max !== undefined) bounded(output.max, `outputs[${index + 1}].max`, -1e8, 1e8);
  });
  const validation = target.validation_samples ?? [];
  if (!Array.isArray(target.samples) || target.samples.length < 1 || !Array.isArray(validation) || target.samples.length + validation.length > MAX_SAMPLES) {
    throw new Error(`A target needs training samples and no more than ${MAX_SAMPLES} combined training and validation samples.`);
  }
  function samples(rows, label) {
    return rows.map((rawSample, index) => {
      const sampleLabel = `${label}[${index + 1}]`;
      object(rawSample, sampleLabel);
      function values(rawValues, defs, axis) {
        if (!Array.isArray(rawValues) || rawValues.length !== defs.length) throw new Error(`${sampleLabel}.${axis} must have ${defs.length} values.`);
        return rawValues.map((value, dimension) => {
          number(value, `${sampleLabel}.${axis}[${dimension + 1}]`);
          if (axis === 'inputs') {
            bounded(value, `${sampleLabel}.inputs[${dimension + 1}]`, 1e-8, 1e8);
            const { min, max } = defs[dimension];
            const tolerance = Number.EPSILON * 8 * Math.max(Math.abs(min), Math.abs(max));
            if (value <= 0 || value < min - tolerance || value > max + tolerance) throw new Error(`${sampleLabel}.inputs[${dimension + 1}] must be positive and within the declared input range.`);
          } else bounded(value, `${sampleLabel}.outputs[${dimension + 1}]`, -1e8, 1e8);
          return value;
        });
      }
      const weight = number(rawSample.weight ?? 1, `${sampleLabel}.weight`);
      bounded(weight, `${sampleLabel}.weight`, 1e-12, 1e12);
      return { inputs: values(rawSample.inputs, inputs, 'inputs'), outputs: values(rawSample.outputs, outputs, 'outputs'), weight };
    });
  }
  const result = { schema_version: SCHEMA, description: target.description, source: target.source, inputs, outputs, samples: samples(target.samples, 'samples') };
  if (target.validation_samples !== undefined) result.validation_samples = samples(validation, 'validation_samples');
  return result;
}

function normalized(value, label) {
  number(value, label);
  if (value < 0 || value > 1) throw new Error(`${label} must be between 0 and 1.`);
  return value;
}

export function resolveInput(rawInput, t) {
  const input = inputDefinition(rawInput);
  normalized(t, 'Input position');
  if (t === 0) return input.min;
  if (t === 1) return input.max;
  return input.scale === 'log'
    ? Math.exp(Math.log(input.min) * (1 - t) + Math.log(input.max) * t)
    : input.min * (1 - t) + input.max * t;
}

function points(raw) {
  if (!Array.isArray(raw) || raw.length < 2 || raw.length > 65536) throw new Error('Drawing needs 2 to 65536 points.');
  return raw.map((point, index) => {
    object(point, `Point ${index + 1}`);
    return { x: normalized(point.x, `Point ${index + 1} x`), y: normalized(point.y, `Point ${index + 1} y`) };
  });
}

function outputRange(output, options) {
  const min = number(output.min ?? options.outputMin ?? 0, 'Output minimum');
  const max = number(output.max ?? options.outputMax ?? 1, 'Output maximum');
  if (max <= min) throw new Error('Output maximum must exceed its minimum.');
  // These are already readout coordinates; a log10 readout is not logged again.
  return t => number(min * (1 - t) + max * t, 'Mapped output');
}

// Numerical refinement may ADD evaluations along the authored segments. It
// must never discard a vertex, smooth a corner, sort a stroke, replace a
// repeated x, or invent a constant extension outside the drawn domain.
function drawingEvaluations(rawPoints, count) {
  integer(count, 'Minimum drawing resolution', 2, MAX_SAMPLES);
  const ordered = points(rawPoints).filter((point, index, list) => index === 0 || point.x !== list[index - 1].x || point.y !== list[index - 1].y);
  if (ordered.length < 2) throw new Error('A drawing needs at least two distinct consecutive points.');
  if (ordered.length > MAX_SAMPLES) throw new Error(`This drawing exceeds the ${MAX_SAMPLES}-point training limit. It has not been simplified.`);
  const lengths = [0];
  for (let index = 1; index < ordered.length; index += 1) {
    lengths.push(lengths[index - 1] + Math.hypot(ordered[index].x - ordered[index - 1].x, ordered[index].y - ordered[index - 1].y));
  }
  const total = lengths[lengths.length - 1];
  const evaluations = ordered.map((point, index) => ({ ...point, t: lengths[index] / total }));
  // Exact original coordinates win at coincident positions. Any resource
  // limit rejects the request explicitly instead of changing its target.
  const positions = new Set(evaluations.map(point => point.t));
  let segment = 0;
  for (let index = 0; index < count; index += 1) {
    const t = index / (count - 1);
    if (positions.has(t)) continue;
    const distance = t * total;
    while (segment < ordered.length - 2 && lengths[segment + 1] < distance) segment += 1;
    const u = (distance - lengths[segment]) / (lengths[segment + 1] - lengths[segment]);
    const start = ordered[segment];
    const end = ordered[segment + 1];
    evaluations.push({ t, x: start.x * (1 - u) + end.x * u, y: start.y * (1 - u) + end.y * u });
  }
  if (evaluations.length > MAX_SAMPLES) throw new Error(`Drawing refinement exceeds ${MAX_SAMPLES} evaluations. Lower the minimum resolution; every original vertex will still be kept.`);
  return evaluations.sort((a, b) => a.t - b.t);
}

export function samplesFromCurve(rawPoints, rawInputs, rawOutputs, options = {}) {
  const [inputs, outputs] = dimensions(rawInputs, rawOutputs, 1, 1, 'A response curve');
  const output = outputRange(outputs[0], options);
  return drawingEvaluations(rawPoints, options.count ?? 2).map(point => ({
    inputs: [resolveInput(inputs[0], point.x)], outputs: [output(point.y)], weight: 1,
  }));
}

export function samplesFromTrajectory(rawPoints, rawInputs, rawOutputs, options = {}) {
  const [inputs, outputs] = dimensions(rawInputs, rawOutputs, 1, 2, 'A parametric trajectory');
  const ranges = outputs.map(output => outputRange(output, options));
  return drawingEvaluations(rawPoints, options.count ?? 2).map(point => ({
    inputs: [resolveInput(inputs[0], point.t)], outputs: [ranges[0](point.x), ranges[1](point.y)], weight: 1,
  }));
}

export function samplesFromImage(image, rawInputs, rawOutputs, options = {}) {
  const [inputs, outputs] = dimensions(rawInputs, rawOutputs, 2, 1, 'An image response field');
  object(image, 'Image');
  const width = integer(image.width, 'Image width', 1, 16384);
  const height = integer(image.height, 'Image height', 1, 16384);
  if (width * height > 16777216) throw new Error('Image must contain no more than 16777216 pixels.');
  const data = image.data;
  if ((!Array.isArray(data) && !ArrayBuffer.isView(data)) || data.length !== width * height * 4) throw new Error('Image data must contain exactly four RGBA channels per pixel.');
  const columns = integer(options.columns ?? 12, 'Image sample columns', 2, MAX_SAMPLES);
  const rows = integer(options.rows ?? 12, 'Image sample rows', 2, MAX_SAMPLES);
  if (columns * rows > MAX_SAMPLES) throw new Error(`Image sampling must not exceed ${MAX_SAMPLES} samples.`);
  if (options.invert !== undefined && typeof options.invert !== 'boolean') throw new Error('Image invert must be true or false.');
  const output = outputRange(outputs[0], options);
  function luminance(x, y) {
    const offset = (y * width + x) * 4;
    const rgba = Array.from({ length: 4 }, (_, index) => {
      const channel = number(data[offset + index], 'Image channel');
      if (channel < 0 || channel > 255) throw new Error('Image channels must be between 0 and 255.');
      return channel / 255;
    });
    return (0.2126 * rgba[0] + 0.7152 * rgba[1] + 0.0722 * rgba[2]) * rgba[3] + 1 - rgba[3];
  }
  return Array.from({ length: columns * rows }, (_, index) => {
    const x = (index % columns) / (columns - 1);
    const imageY = Math.floor(index / columns) / (rows - 1);
    const px = x * (width - 1);
    const py = imageY * (height - 1);
    const x0 = Math.floor(px), x1 = Math.ceil(px), y0 = Math.floor(py), y1 = Math.ceil(py);
    const dx = px - x0, dy = py - y0;
    const intensity = (luminance(x0, y0) * (1 - dx) + luminance(x1, y0) * dx) * (1 - dy)
      + (luminance(x0, y1) * (1 - dx) + luminance(x1, y1) * dx) * dy;
    return { inputs: [resolveInput(inputs[0], x), resolveInput(inputs[1], 1 - imageY)], outputs: [output(options.invert ? 1 - intensity : intensity)], weight: 1 };
  });
}

const defaultInput = { name: 'X', min: 0.05, max: 10, scale: 'log' };
export const DEFAULT_TARGET = Object.freeze({
  schema_version: SCHEMA,
  description: 'A saturating response as the input concentration increases.',
  source: 'curve',
  inputs: Object.freeze([Object.freeze(defaultInput)]),
  outputs: Object.freeze([Object.freeze({ name: 'response', species: 'A', transform: 'linear', offset: 0, optimize_offset: false, min: 0, max: 1 })]),
  samples: Object.freeze(Array.from({ length: 24 }, (_, index) => {
    const input = resolveInput(defaultInput, index / 23);
    return Object.freeze({ inputs: Object.freeze([input]), outputs: Object.freeze([input / (0.5 + input)]), weight: 1 });
  })),
});
