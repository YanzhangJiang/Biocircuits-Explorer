import assert from 'node:assert/strict';
import test from 'node:test';
import {
  DEFAULT_TARGET, cloneDesignTarget, validateDesignTarget, resolveInput,
  samplesFromCurve, samplesFromTrajectory, samplesFromImage,
} from '../public/js/design-target-adapters.js';

const input = { name: 'X', min: 1, max: 100, scale: 'log' };
const secondInput = { name: 'Y', min: 1, max: 5, scale: 'linear' };
const output = { name: 'response', species: 'A', transform: 'linear', offset: 0, optimize_offset: false };
const secondOutput = { ...output, name: 'vertical response', species: 'B' };
const near = (actual, expected) => assert.ok(Math.abs(actual - expected) < 1e-12, `${actual} differs from ${expected}`);

test('the default target is immutable and validation returns independent normalized JSON', () => {
  const normalized = validateDesignTarget(DEFAULT_TARGET);
  assert.equal(normalized.schema_version, 'bne-design-target/v1.0.0');
  assert.equal(normalized.samples.length, 24);
  assert.deepEqual(normalized.inputs, [{ name: 'X', min: 0.05, max: 10, scale: 'log' }]);
  assert.deepEqual(normalized.samples[0].inputs, [0.05]);
  assert.deepEqual(normalized.samples.at(-1).inputs, [10]);
  assert.ok(normalized.samples.every((sample, index, rows) => index === 0 || sample.outputs[0] > rows[index - 1].outputs[0]));
  normalized.outputs[0].min = -1;
  normalized.samples[0].inputs[0] = 7;
  assert.equal(DEFAULT_TARGET.outputs[0].min, 0);
  assert.equal(DEFAULT_TARGET.samples[0].inputs[0], 0.05);
  assert.throws(() => { DEFAULT_TARGET.samples[0].inputs[0] = 7; }, TypeError);
});

test('validation supports configurable dimensions, validation sets, labels, and output ranges', () => {
  const raw = cloneDesignTarget(DEFAULT_TARGET);
  raw.inputs = [input, secondInput, { name: 'Z', min: 0.1, max: 3 }];
  raw.outputs = [output, secondOutput, { name: '三号读出', species: 'AB', transform: 'log10', min: -5, max: 2 }];
  raw.samples = [{ inputs: [2, 3, 1], outputs: [1, -2, -4] }];
  raw.validation_samples = [{ inputs: [1, 5, 3], outputs: [3, 2, -1], weight: 0.25 }];
  const actual = validateDesignTarget(raw);
  assert.equal(actual.inputs[2].scale, 'linear');
  assert.equal(actual.outputs[2].offset, 0);
  assert.equal(actual.outputs[2].optimize_offset, false);
  assert.equal(actual.outputs[2].min, -5);
  assert.equal(actual.samples[0].weight, 1);
  assert.equal(actual.validation_samples[0].weight, 0.25);
  raw.samples[0].outputs[0] = 90;
  assert.equal(actual.samples[0].outputs[0], 1);
});

test('validation rejects malformed dimensions, physical inputs, output definitions, and samples', () => {
  const mutations = [
    target => { target.schema_version = 'wrong'; },
    target => { target.source = 'preset-guess'; },
    target => { target.inputs = []; },
    target => { target.outputs = [output, output]; },
    target => { target.inputs = [input, input]; },
    target => { target.inputs = Array.from({ length: 4 }, (_, index) => ({ ...input, name: `X${index}` })); },
    target => { target.inputs[0].min = 0; },
    target => { target.inputs[0].max = target.inputs[0].min; },
    target => { target.inputs[0].max = Infinity; },
    target => { target.inputs[0].scale = 'ln'; },
    target => { target.inputs[0].name = 'A + B'; },
    target => { target.outputs[0].species = 'A();'; },
    target => { target.outputs[0].offset = NaN; },
    target => { target.outputs[0].optimize_offset = 'yes'; },
    target => { target.outputs[0].transform = 'arbitrary()'; },
    target => { target.outputs[0].min = 2; },
    target => { target.samples = []; },
    target => { target.samples[0].inputs = [1, 2]; },
    target => { target.samples[0].outputs = []; },
    target => { target.samples[0].inputs[0] = -1; },
    target => { target.samples[0].inputs[0] = 11; },
    target => { target.samples[0].weight = 0; },
    target => { target.samples[0].outputs[0] = '0.5'; },
    target => { target.samples[0].outputs[0] = Infinity; },
    target => { target.validation_samples = [{ inputs: [1], outputs: [] }]; },
    target => { target.metadata = new Date(); },
  ];
  for (const mutate of mutations) {
    const target = cloneDesignTarget(DEFAULT_TARGET);
    mutate(target);
    assert.throws(() => validateDesignTarget(target), undefined, mutate.toString());
  }
});

test('training and validation share the 4096 observation budget', () => {
  const target = cloneDesignTarget(DEFAULT_TARGET);
  target.samples = Array.from({ length: 4095 }, () => ({ inputs: [1], outputs: [0.5] }));
  target.validation_samples = [{ inputs: [1], outputs: [0.5] }];
  assert.equal(validateDesignTarget(target).samples.length, 4095);
  target.validation_samples.push({ inputs: [1], outputs: [0.5] });
  assert.throws(() => validateDesignTarget(target), /4096/);
});

test('admission matches solver physical bounds and UTF-8 text budgets', () => {
  const mutations = [
    target => { target.inputs[0].min = 1e-9; },
    target => { target.inputs[0].max = 1e9; },
    target => { target.outputs[0].offset = 9; },
    target => { target.outputs[0].min = -1e9; },
    target => { target.samples[0].outputs[0] = 1e9; },
    target => { target.samples[0].weight = 1e-13; },
    target => { target.samples[0].weight = 1e13; },
    target => { target.description = '设'.repeat(5334); },
    target => { target.outputs[0].name = '设'.repeat(27); },
    target => { target.inputs[0].name = 'A'.repeat(41); },
    target => { target.samples = new Array(2); },
    target => { target.samples[0].outputs = new Array(1); },
  ];
  for (const mutate of mutations) {
    const target = cloneDesignTarget(DEFAULT_TARGET);
    mutate(target);
    assert.throws(() => validateDesignTarget(target));
  }
});

test('JSON cloning rejects cycles and values that persistence would silently lose', () => {
  const cyclic = {}; cyclic.self = cyclic;
  for (const raw of [cyclic, { value: undefined }, [Infinity], { value: () => 1 }, new Map(), { [Symbol('x')]: 1 }]) {
    assert.throws(() => cloneDesignTarget(raw));
  }
});

test('input coordinates map to physical linear or logarithmic concentrations', () => {
  assert.equal(resolveInput(input, 0), 1);
  near(resolveInput(input, 0.5), 10);
  assert.equal(resolveInput(input, 1), 100);
  assert.equal(resolveInput(secondInput, 0.25), 2);
  near(resolveInput({ ...input, min: 1e-200, max: 1e200 }, 0.5), 1);
  for (const position of [-0.01, 1.01, NaN, '0.5']) assert.throws(() => resolveInput(input, position));
});

test('curve interpolation uses mathematical y-up coordinates and physical output ranges', () => {
  const samples = samplesFromCurve([{ x: 1, y: 1 }, { x: 0, y: 0 }], [input], [output], { count: 3, outputMin: -1, outputMax: 3 });
  assert.deepEqual(samples.map(sample => sample.outputs), [[3], [1], [-1]]);
  near(samples[1].inputs[0], 10);
  assert.ok(samples.every(sample => sample.weight === 1));
  const logged = samplesFromCurve([{ x: 0, y: 0 }, { x: 1, y: 1 }], [input], [{ ...output, transform: 'log10', min: -3, max: -1 }], { count: 2 });
  assert.deepEqual(logged.map(sample => sample.outputs), [[-3], [-1]]);
});

test('all drawn coordinates remain training targets, including repeated x and partial endpoints', () => {
  const drawing = [{ x: 0.25, y: 0.2 }, { x: 0.5, y: 0.1 }, { x: 0.75, y: 0.8 }, { x: 0.5, y: 0.6 }];
  const samples = samplesFromCurve(drawing, [input], [output]);
  assert.deepEqual(samples.map(sample => sample.outputs[0]), drawing.map(point => point.y));
  samples.forEach((sample, index) => near(sample.inputs[0], resolveInput(input, drawing[index].x)));
  assert.ok(samples.every(sample => sample.inputs[0] > input.min && sample.inputs[0] < input.max), 'Unwritten endpoints must not be invented');
  assert.deepEqual(samplesFromCurve([{ x: 0.2, y: 0 }, { x: 0.2, y: 1 }], [input], [output]).map(row => row.outputs[0]), [0, 1]);
});

test('a narrow feature between a coarse grid is still optimized, at every resolution', () => {
  const drawing = [{ x: 0, y: 0 }, { x: 0.10001, y: 0 }, { x: 0.10002, y: 1 }, { x: 0.10003, y: 0 }, { x: 1, y: 0 }];
  for (const count of [2, 3, 24, 64]) {
    const samples = samplesFromCurve(drawing, [input], [output], { count });
    for (const vertex of drawing) {
      assert.ok(samples.some(sample => sample.inputs[0] === resolveInput(input, vertex.x) && sample.outputs[0] === vertex.y), `Missing original vertex at ${vertex.x}`);
    }
    assert.ok(samples.reduce((loss, row) => loss + row.outputs[0] ** 2, 0) > 0, 'A flat zero response cannot receive zero loss by omitting the spike');
  }
});

test('trajectory samples preserve draw order and use arc length, not point index', () => {
  const drawing = [{ x: 0, y: 0 }, { x: 0, y: 0 }, { x: 0.25, y: 0 }, { x: 1, y: 0 }, { x: 1, y: 1 }];
  const samples = samplesFromTrajectory(drawing, [input], [output, { ...secondOutput, min: -2, max: 2 }], { count: 5 });
  assert.deepEqual(samples.map(sample => sample.outputs), [[0, -2], [0.25, -2], [0.5, -2], [1, -2], [1, 0], [1, 2]]);
  near(samples[3].inputs[0], 10);
  const reverse = samplesFromTrajectory([...drawing].reverse(), [input], [output, secondOutput], { count: 3 });
  assert.deepEqual(reverse.map(sample => sample.outputs), [[1, 1], [1, 0], [0.25, 0], [0, 0]]);
  assert.throws(() => samplesFromTrajectory([{ x: 0, y: 0 }, { x: 0, y: 0 }], [input], [output, secondOutput]), /distinct/);
});

test('image sampling maps the top row to maximum y and uses RGBA luminance', () => {
  const image = { width: 2, height: 2, data: new Uint8ClampedArray([
    0, 0, 0, 255, 255, 255, 255, 255,
    255, 0, 0, 255, 0, 0, 0, 0,
  ]) };
  const samples = samplesFromImage(image, [input, secondInput], [output], { columns: 2, rows: 2 });
  assert.deepEqual(samples.map(sample => sample.inputs), [[1, 5], [100, 5], [1, 1], [100, 1]]);
  [0, 1, 0.2126, 1].forEach((expected, index) => near(samples[index].outputs[0], expected));
  const inverted = samplesFromImage(image, [input, secondInput], [output], { columns: 2, rows: 2, invert: true, outputMin: -2, outputMax: 2 });
  near(inverted[0].outputs[0], 2);
  near(inverted[1].outputs[0], -2);
  const interpolated = samplesFromImage(image, [input, secondInput], [output], { columns: 3, rows: 3 });
  near(interpolated[4].outputs[0], (0 + 1 + 0.2126 + 1) / 4);
});

test('adapters reject mismatched dimensions, invalid normalized drawings, and malformed images', () => {
  const drawing = [{ x: 0, y: 0 }, { x: 1, y: 1 }];
  const image = { width: 1, height: 1, data: [0, 0, 0, 255] };
  assert.throws(() => samplesFromCurve(drawing, [input, secondInput], [output]), /requires 1 input/);
  assert.throws(() => samplesFromTrajectory(drawing, [input], [output]), /2 output/);
  assert.throws(() => samplesFromImage(image, [input], [output]), /2 input/);
  assert.throws(() => samplesFromCurve([{ x: -1, y: 0 }, { x: 1, y: 1 }], [input], [output]), /between 0 and 1/);
  assert.throws(() => samplesFromCurve(drawing, [input], [output], { count: 4097 }), /4096/);
  assert.throws(() => samplesFromImage({ ...image, data: [0, 0, 0] }, [input, secondInput], [output]), /RGBA/);
  assert.throws(() => samplesFromImage({ ...image, data: [0, 0, NaN, 255] }, [input, secondInput], [output]), /finite/);
  assert.throws(() => samplesFromImage(image, [input, secondInput], [output], { columns: 65, rows: 65 }), /4096/);
});
