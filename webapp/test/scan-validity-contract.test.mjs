import assert from 'node:assert/strict';
import {
  finitePlotValue,
  formatAtlasLandscapeResponseSummary,
  formatPartialValidityNotice,
  maskGridByOutputValidity,
  prepareAtlasLandscapePlotData,
  prepareFretHeatmapPlotData,
  prepareRopCloudPlotData,
  prepareScan1DPlotData,
  prepareScan2DPlotData,
} from '../public/js/plot-validity.js';

let passed = 0;
function test(name, fn) {
  fn();
  passed += 1;
  console.log(`  ok - ${name}`);
}

test('1D scan masks invalid rows and serialized non-finite values', () => {
  const prepared = prepareScan1DPlotData({
    output_traj: [[1, 10], [2, 20], ['NaN', 30], [4, 'Inf']],
    valid: [true, false, true, true],
    partial: true,
  });

  assert.deepEqual(prepared.outputTraj, [
    [1, 10],
    [null, null],
    [null, 30],
    [4, null],
  ]);
  assert.equal(prepared.invalidCount, 3);
  assert.equal(prepared.totalCount, 4);
  assert.equal(prepared.partial, true);
  assert.equal(
    formatPartialValidityNotice(prepared),
    'Partial result: 3/4 points did not converge; gaps are not plotted.',
  );
});

test('2D scan masks validity-grid cells and never coerces string NaN/Inf', () => {
  const prepared = prepareScan2DPlotData({
    output_grid: [[1, 2], ['-Inf', 4]],
    validity_grid: [[true, false], [true, true]],
    partial: true,
  });

  assert.deepEqual(prepared.outputGrid, [[1, null], [null, 4]]);
  assert.equal(prepared.invalidCount, 2);
  assert.equal(prepared.totalCount, 4);
  assert.equal(prepared.partial, true);
});

test('atlas landscape applies the same cell-level gap contract', () => {
  const prepared = prepareAtlasLandscapePlotData({
    output_grid: [[0.5, 'NaN', 1.5], [2.0, 2.5, 3.0]],
    validity_grid: [[true, true, false], [true, true, true]],
    partial: true,
  });

  assert.deepEqual(prepared.outputGrid, [
    [0.5, null, null],
    [2.0, 2.5, 3.0],
  ]);
  assert.equal(prepared.invalidCount, 2);
  assert.match(formatPartialValidityNotice(prepared), /2\/6 points did not converge/);
  assert.deepEqual(
    maskGridByOutputValidity([[0, 1, 0], [1, 0, 1]], prepared.outputGrid),
    [[0, null, null], [1, 0, 1]],
  );
  assert.match(formatAtlasLandscapeResponseSummary({
    param1_symbol: 'tA',
    param2_symbol: 'tB',
    output_grid: [[0.5, 'NaN', 1.5], [2.0, 2.5, 3.0]],
    validity_grid: [[true, true, false], [true, true, true]],
    regime_grid: [[1, 1, 2], [2, 2, 3]],
    partial: true,
  }), /Partial result: 2\/6 points did not converge/);
});

test('legacy non-partial responses retain finite values and show no warning', () => {
  const legacy1D = prepareScan1DPlotData({ output_traj: [[1], [2], [3]] });
  const legacy2D = prepareScan2DPlotData({ output_grid: [[1, 2], [3, 4]] });
  const legacyAtlas = prepareAtlasLandscapePlotData({ output_grid: [[5, 6]] });

  assert.deepEqual(legacy1D.outputTraj, [[1], [2], [3]]);
  assert.deepEqual(legacy2D.outputGrid, [[1, 2], [3, 4]]);
  assert.deepEqual(legacyAtlas.outputGrid, [[5, 6]]);
  assert.equal(formatPartialValidityNotice(legacy1D), '');
  assert.equal(formatPartialValidityNotice(legacy2D), '');
  assert.equal(formatPartialValidityNotice(legacyAtlas), '');
});

test('FRET heatmaps mask failed solves and report partial validity', () => {
  const prepared = prepareFretHeatmapPlotData({
    fret: [[1, 'NaN'], [0.5, 2]],
    validity_grid: [[true, false], [true, true]],
    partial: true,
  });

  assert.deepEqual(prepared.outputGrid, [[1, null], [0.5, 2]]);
  assert.equal(prepared.invalidCount, 1);
  assert.equal(prepared.totalCount, 4);
  assert.equal(prepared.partial, true);
  assert.match(formatPartialValidityNotice(prepared), /1\/4 points/);
});

test('ROP clouds jointly drop invalid coordinates and colors without misalignment', () => {
  const prepared = prepareRopCloudPlotData({
    reaction_orders: [[1, 2], [3, 4], ['NaN', 6], [7, 8]],
    fret_values: [10, 20, 30, 'NaN'],
    valid: [true, false, true, true],
    partial: true,
  });

  assert.deepEqual(prepared.reactionOrders, [[1, 2]]);
  assert.deepEqual(prepared.fretValues, [10]);
  assert.equal(prepared.invalidCount, 3);
  assert.equal(prepared.totalCount, 4);
  assert.equal(prepared.partial, true);
});

test('finitePlotValue accepts only finite JavaScript numbers', () => {
  assert.equal(finitePlotValue(0), 0);
  assert.equal(finitePlotValue(-1.25), -1.25);
  for (const value of ['NaN', 'Inf', '-Inf', '1.25', NaN, Infinity, -Infinity, null]) {
    assert.equal(finitePlotValue(value), null);
  }
});

console.log(`\nAll ${passed} scan-validity contract tests passed.`);
