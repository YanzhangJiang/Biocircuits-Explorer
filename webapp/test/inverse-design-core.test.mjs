import assert from 'node:assert/strict';
import test from 'node:test';
import {
  INVERSE_DESIGN_DEFAULTS, validateInverseDesignRequest,
  normalizeInverseDesignResult, designedNetworkFromResult, designTargetErrorFloor,
} from '../public/js/inverse-design-core.js';

test('identical-input error floors use weighted residuals without changing or simplifying the target', () => {
  const t = target({ samples: [
    { inputs: [1], outputs: [0], weight: 1 },
    { inputs: [1], outputs: [2], weight: 3 },
    { inputs: [2], outputs: [7], weight: 2 },
  ] });
  const before = structuredClone(t);
  const [floor] = designTargetErrorFloor(t);
  assert.equal(floor.dataset, 'Training');
  // Weighted optimum at X=1 is 1.5: (1*1.5² + 3*0.5²) / 6 = 0.5.
  assert.ok(Math.abs(floor.per_output_rmsd[0] - Math.sqrt(0.5)) < 1e-12);
  assert.equal(floor.conflicting_groups, 1);
  assert.deepEqual(t, before);
  assert.deepEqual(designTargetErrorFloor(target()), []);
  t.samples[1].inputs[0] = 1 + Number.EPSILON;
  assert.deepEqual(designTargetErrorFloor(t), [], 'nearby inputs are never merged');
});

test('error floors respect complete input tuples, output dimensions and validation weights', () => {
  const t = { inputs: [{ name: 'X' }, { name: 'Y' }], outputs: [{ name: 'a' }, { name: 'b' }], samples: [
    { inputs: [1, 2], outputs: [0, 1], weight: 1e300 },
    { inputs: [1, 3], outputs: [2, 1], weight: 1e300 },
  ], validation_samples: [
    { inputs: [1, 2], outputs: [0, 1], weight: 1e300 },
    { inputs: [1, 2], outputs: [2, 1], weight: 1e300 },
  ] };
  const floors = designTargetErrorFloor(t);
  assert.equal(floors.length, 1);
  assert.equal(floors[0].dataset, 'Validation');
  assert.deepEqual(floors[0].per_output_rmsd, [1, 0]);
  // Repeated X/Y output coordinates at different trajectory inputs do not conflict.
  assert.deepEqual(designTargetErrorFloor({ outputs: t.outputs, samples: [
    { inputs: [1], outputs: [0, 1] }, { inputs: [2], outputs: [0, 2] },
  ] }), []);
});

test('termination records preserve evaluated budgets and reject contradictory stopping claims', () => {
  const req = request(), raw = response(req);
  raw.termination = { reason: 'target_met', epochs_per_fit: 150, initializations: 2, pruning_round_limit: 3,
    iterations: 150, fit_stops: [{ phase: 'fit', restart: 1, prune_round: 0, reason: 'iteration_limit', iterations: 150, best_step: 83 }] };
  assert.deepEqual(normalizeInverseDesignResult(raw, req).termination, raw.termination);
  for (const mutate of [
    value => { value.reason = 'search_budget_exhausted'; },
    value => { value.iterations = 900; },
    value => { value.epochs_per_fit = 999; },
    value => { value.fit_stops[0].reason = 'converged'; },
    value => { value.fit_stops[0].iterations = 149; },
    value => { value.fit_stops.push(structuredClone(value.fit_stops[0])); value.iterations *= 2; },
  ]) {
    const invalid = structuredClone(raw); mutate(invalid.termination);
    assert.throws(() => normalizeInverseDesignResult(invalid, req));
  }
});

function target(overrides = {}) {
  return {
    schema_version: 'bne-design-target/v1.0.0', description: 'Draw a decreasing response', source: 'curve',
    inputs: [{ name: 'X', min: 0.1, max: 10, scale: 'log' }],
    outputs: [{ name: 'response', species: 'A', transform: 'linear', offset: 0, optimize_offset: false }],
    samples: [{ inputs: [0.1], outputs: [0.9], weight: 1 }, { inputs: [1], outputs: [0.5], weight: 2 }, { inputs: [10], outputs: [0.1], weight: 1 }],
    ...overrides,
  };
}
function request(overrides = {}) {
  return validateInverseDesignRequest({ target: target(), ...overrides });
}
function response(input = request()) {
  return {
    status: 'ok', target_met: true, target: structuredClone(input.target),
    initial_reaction_count: 3, final_reaction_count: 1,
    selected_network: {
      status: 'ok', rules: ['X + A <-> AX'], kd: [0.48], totals: { A: 1.1, B: 0.7 },
      outputs: structuredClone(input.target.outputs),
      predictions: input.target.samples.map(sample => sample.outputs.map(value => value + 0.01)),
      targets: input.target.samples.map(sample => sample.outputs.slice()), rmse: 0.01, fit_loss: 0.00005, per_output_rmse: input.target.outputs.map(() => 0.01),
      evidence_tier: 'sampled_equilibrium_replay', prediction_basis: 'selected_network',
      physical_audit: { cold_replay: true, max_log10_mass_residual: 1e-12, max_stepwise_log10_mass_action_residual: 1e-12, training_samples: input.target.samples.length, validation_samples: input.target.validation_samples?.length ?? 0 },
    },
    pruning_history: [{ before: 3, after: 1, accepted: true, rmse: 0.01, reason: 'Refit remained within tolerance.' },
      { before: 1, after: 0, accepted: false, rmse: 0.2, reason: 'Removing the leaf exceeded tolerance.' }],
    optimization_history: [{ phase: 'initial', step: 0, loss: 0.2 }, { phase: 'refit', step: 150, loss: 0.0001 }],
    algorithm: { name: 'implicit_equilibrium_gradient_and_occupancy_pruning' }, warnings: [],
    metadata: { notes: ['sampled', null] },
  };
}

test('request starts from explicit target and applies chemistry and pruning defaults', () => {
  const source = { target: target(), ignoredUIField: 'not persisted' };
  const value = validateInverseDesignRequest(source);
  assert.deepEqual(value.chemistry, INVERSE_DESIGN_DEFAULTS.chemistry);
  assert.deepEqual(value.optimization, INVERSE_DESIGN_DEFAULTS.optimization);
  assert.equal(value.optimization.optimize_totals, true);
  assert.equal(value.optimization.prune_rounds, 3);
  assert.equal('ignoredUIField' in value, false);
  assert.equal('reactions' in value, false);
  assert.equal('lambda' in value.optimization, false);
  value.target.samples[0].inputs[0] = 99;
  value.chemistry.max_reactions = 8;
  assert.equal(source.target.samples[0].inputs[0], 0.1);
  assert.equal(INVERSE_DESIGN_DEFAULTS.chemistry.max_reactions, 48);
});

test('legacy sparse-library requests cannot be submitted as target-driven designs', () => {
  assert.throws(() => validateInverseDesignRequest({ reactions: ['A + B <-> AB'], output_exprs: ['AB'], samples: [{ totals: { tA: 1, tB: 1 }, target: [0.5] }] }));
  assert.throws(() => request({ target: { ...target(), schema_version: 'old' } }));
});

test('optimization and chemistry reject coercion, nonfinite settings and excessive budgets', () => {
  for (const optimization of [
    { epochs: true }, { epochs: 0 }, { epochs: 2001 }, { epochs: 1.5 }, { learning_rate: '0.03' },
    { learning_rate: Infinity }, { restarts: 0 }, { restarts: 9 }, { prune_rounds: 13 },
    { prune_rounds: -1 }, { prune_fraction: 0 }, { prune_fraction: 1.01 }, { prune_tolerance: -1 },
    { max_rmse: NaN }, { optimize_totals: 1 }, { seed: -1 }, { lambda: 0.01 }, new Date(),
  ]) assert.throws(() => request({ optimization }), JSON.stringify(optimization));
  for (const chemistry of [
    { auxiliary_monomers: -1 }, { auxiliary_monomers: 5 }, { max_complex_size: 1 }, { max_complex_size: 9 },
    { max_reactions: 0 }, { max_reactions: 257 }, { allow_homomers: 'true' }, new Date(),
  ]) assert.throws(() => request({ chemistry }), JSON.stringify(chemistry));
  assert.equal(request({ optimization: { prune_rounds: 0, prune_tolerance: 0, max_rmse: 0 }, chemistry: { auxiliary_monomers: 0 } }).optimization.prune_rounds, 0);
});

test('sample dimensions and declared physical input ranges are validated at admission', () => {
  for (const value of [
    target({ samples: [] }), target({ samples: [{ inputs: [0], outputs: [1] }] }),
    target({ samples: [{ inputs: [1, 2], outputs: [1] }] }), target({ samples: [{ inputs: [1], outputs: [1, 2] }] }),
    target({ samples: [{ inputs: [1], outputs: [Infinity] }] }), target({ samples: [{ inputs: [20], outputs: [1] }] }),
  ]) assert.throws(() => request({ target: value }));
});

test('result preserves selected replay, fitted Kd, totals, readouts and independent JSON', () => {
  const input = request();
  const raw = response(input);
  const result = normalizeInverseDesignResult(raw, input);
  assert.deepEqual(result, raw);
  const network = designedNetworkFromResult(result);
  assert.deepEqual(network.reactions, ['X + A <-> AX']);
  assert.deepEqual(network.kds, [0.48]);
  assert.deepEqual(network.totals, { A: 1.1, B: 0.7 });
  assert.deepEqual(network.outputs, input.target.outputs);
  assert.equal(network.target_met, true);
  assert.equal(network.evidence_tier, 'sampled_equilibrium_replay');
  network.totals.A = 100;
  network.outputs[0].offset = 20;
  result.target.samples[0].outputs[0] = 99;
  assert.equal(raw.selected_network.totals.A, 1.1);
  assert.equal(raw.selected_network.outputs[0].offset, 0);
  assert.equal(raw.target.samples[0].outputs[0], 0.9);
  assert.deepEqual(JSON.parse(JSON.stringify(result)), result);
});

test('successful physical replay can export an unbound zero-reaction design without NetworkIR', () => {
  const input = request({ target: target({ samples: target().samples.map(sample => ({ ...sample, outputs: [1.1] })) }) });
  const raw = response(input);
  raw.final_reaction_count = 0;
  raw.selected_network.rules = [];
  raw.selected_network.kd = [];
  raw.selected_network.predictions = raw.selected_network.targets.map(row => row.slice());
  raw.selected_network.rmse = 0;
  raw.selected_network.per_output_rmse = [0];
  raw.selected_network.fit_loss = 0;
  raw.pruning_history = [{ before: 3, after: 0, accepted: true, rmse: 0, reason: 'Unbound A at its fitted total reproduces the constant target.' }];
  const result = normalizeInverseDesignResult(raw, input);
  assert.deepEqual(designedNetworkFromResult(result).reactions, []);
  assert.deepEqual(designedNetworkFromResult(result).kds, []);
});

test('NetworkIR is optional but must match selected reactions and optimized Kd when supplied', () => {
  const raw = response();
  raw.selected_network.network_ir = { reactions: [{ formula: 'X + A <-> AX', kd: 0.48 }] };
  assert.ok(designedNetworkFromResult(normalizeInverseDesignResult(raw, request())).network_ir);
  raw.selected_network.network_ir.reactions[0].kd = 1;
  assert.throws(() => normalizeInverseDesignResult(raw, request()), /IR Kd/);
  assert.equal(designedNetworkFromResult(raw), null);
});

test('output offsets only change when the target explicitly permits optimization', () => {
  const raw = response();
  raw.selected_network.outputs[0].offset = 0.25;
  assert.throws(() => normalizeInverseDesignResult(raw, request()), /fixed readout/);
  const input = request({ target: target({ outputs: [{ name: 'response', species: 'A', transform: 'log10', offset: -2, optimize_offset: true }] }) });
  const optimized = response(input);
  optimized.selected_network.outputs[0].offset = -1.83;
  assert.equal(normalizeInverseDesignResult(optimized, input).selected_network.outputs[0].offset, -1.83);
});

test('result cannot substitute targets, dimensions, fixed output roles or reaction counts', () => {
  for (const mutate of [
    value => { value.target.description = 'Different goal'; },
    value => { value.target.samples[0].weight = 5; },
    value => { value.selected_network.targets[0][0] += 1; },
    value => { value.selected_network.predictions.pop(); },
    value => { value.selected_network.predictions[0].push(1); },
    value => { value.selected_network.outputs[0].species = 'B'; },
    value => { value.selected_network.outputs[0].transform = 'log10'; },
    value => { value.final_reaction_count = 2; },
    value => { value.initial_reaction_count = 0; },
  ]) {
    const raw = response(); mutate(raw);
    assert.throws(() => normalizeInverseDesignResult(raw, request()));
  }
});

test('result rejects unsuccessful equilibrium, nonfinite physical parameters and fabricated evidence tiers', () => {
  for (const mutate of [
    value => { value.selected_network.status = 'invalid'; },
    value => { value.selected_network.prediction_basis = 'initial_network'; },
    value => { value.selected_network.evidence_tier = 'fit_without_replay'; },
    value => { value.selected_network.kd[0] = 0; },
    value => { value.selected_network.kd[0] = true; },
    value => { value.selected_network.totals.A = -1; },
    value => { value.selected_network.totals.B = Infinity; },
    value => { value.selected_network.outputs[0].offset = NaN; },
    value => { value.selected_network.predictions[0][0] = NaN; },
    value => { value.selected_network.rmse = Infinity; },
    value => { value.selected_network.fit_loss = -1; },
  ]) {
    const raw = response(); mutate(raw);
    assert.throws(() => normalizeInverseDesignResult(raw, request()));
    assert.equal(designedNetworkFromResult(raw), null);
  }
});

test('unmet target remains an inspectable design; exceeding tolerance cannot claim success', () => {
  const raw = response();
  raw.selected_network.rmse = 0.2;
  raw.selected_network.per_output_rmse = [0.2];
  raw.selected_network.fit_loss = 0.02;
  raw.selected_network.predictions = raw.selected_network.targets.map(row => [row[0] + 0.2]);
  assert.throws(() => normalizeInverseDesignResult(raw, request()), /RMSE tolerance/);
  raw.target_met = false;
  assert.equal(designedNetworkFromResult(normalizeInverseDesignResult(raw, request())).target_met, false);
});

test('held-out validation is required when requested and bound to those sample targets', () => {
  const input = request({ target: target({ validation_samples: [{ inputs: [2], outputs: [0.3], weight: 1 }] }) });
  const raw = response(input);
  assert.throws(() => normalizeInverseDesignResult(raw, input), /missing.*validation/);
  raw.validation = { predictions: [[0.31]], targets: [[0.3]], rmse: 0.01, per_output_rmse: [0.01] };
  assert.equal(normalizeInverseDesignResult(raw, input).validation.rmse, 0.01);
  raw.validation.targets[0][0] = 0.4;
  assert.throws(() => normalizeInverseDesignResult(raw, input), /Validation targets/);
  raw.validation.targets[0][0] = 0.3;
  raw.validation.rmse = 0.1;
  raw.validation.per_output_rmse = [0.1];
  raw.validation.predictions = [[0.4]];
  assert.throws(() => normalizeInverseDesignResult(raw, input), /Validation replay exceeds/);
});

test('pruning decisions, optimization history and metadata remain finite ordinary JSON', () => {
  for (const mutate of [
    value => { value.pruning_history[0].accepted = 'yes'; },
    value => { value.pruning_history[0].after = 4; },
    value => { value.pruning_history[0].rmse = NaN; },
    value => { value.pruning_history[0].reason = ''; },
    value => { value.optimization_history[0].loss = Infinity; },
    value => { value.optimization_history[0].step = -1; },
    value => { value.metadata.bad = undefined; },
    value => { value.metadata.bad = new Date(); },
    value => { value.metadata.bad = value; },
  ]) {
    const raw = response(); mutate(raw);
    assert.throws(() => normalizeInverseDesignResult(raw, request()));
  }
});

test('legacy result snapshots never become fresh target-design handoffs', () => {
  assert.equal(designedNetworkFromResult({ rules: ['A + B <-> AB'], kd: [1] }), null);
  const legacy = response();
  delete legacy.target;
  assert.equal(designedNetworkFromResult(legacy), null);
  const incomplete = response();
  delete incomplete.selected_network.totals;
  assert.equal(designedNetworkFromResult(incomplete), null);
});

test('explicit valency and conditional binding constraints survive request admission unchanged', () => {
  const chemistry = { max_copies: { X: 2, A: 1 }, forbidden_complexes: ['X2_A', 'X2_A'],
    binding_gates: [{ monomer: 'Y', requires: { X: 2 }, unless_core_count_at_least: 2 }] };
  const normalized = request({ chemistry });
  assert.deepEqual(normalized.chemistry.max_copies, chemistry.max_copies);
  assert.deepEqual(normalized.chemistry.forbidden_complexes, ['X2_A']);
  assert.deepEqual(normalized.chemistry.binding_gates, chemistry.binding_gates);
  normalized.chemistry.binding_gates[0].requires.X = 3;
  assert.equal(chemistry.binding_gates[0].requires.X, 2);
  for (const settings of [{ max_copies: { X: 9 } }, { max_copies: { 'X()': 1 } },
    { binding_gates: [{ monomer: 'Y', requires: {} }] }, { binding_gates: [{ monomer: 'Y', requires: { X: 2 }, unknown: true }] },
    { forbidden_complexes: ['A;alert(1)'] }, { unknown: true }]) assert.throws(() => request({ chemistry: settings }));
});

test('numeric control boundaries match the engine instead of allowing guaranteed rejected jobs', () => {
  for (const optimization of [{ learning_rate: 1e-6 }, { learning_rate: 0.51 }, { prune_fraction: 0.001 },
    { prune_tolerance: 1.01 }, { max_rmse: 1e8 + 1 }]) assert.throws(() => request({ optimization }));
  assert.equal(request({ optimization: { learning_rate: 1e-5 } }).optimization.learning_rate, 1e-5);
  assert.equal(request({ optimization: { learning_rate: 0.5 } }).optimization.learning_rate, 0.5);
});

test('empty validation and editor-only output ranges do not alter the numerical target contract', () => {
  const input = request({ target: target({ validation_samples: [], outputs: [{ ...target().outputs[0], min: 0, max: 1 }] }) });
  assert.equal(Object.hasOwn(input.target, 'validation_samples'), false);
  assert.equal(Object.hasOwn(input.target.outputs[0], 'min'), false);
  const raw = response(input);
  raw.target.validation_samples = [];
  raw.target.outputs[0].min = 0;
  raw.target.outputs[0].max = 1;
  assert.ok(normalizeInverseDesignResult(raw, input));
});

test('physical residual audit rejects stale, non-cold, or nonconserving selected replays', () => {
  for (const mutate of [
    value => { delete value.selected_network.physical_audit; },
    value => { value.selected_network.physical_audit.cold_replay = false; },
    value => { value.selected_network.physical_audit.training_samples = 2; },
    value => { value.selected_network.physical_audit.max_log10_mass_residual = 1e-5; },
    value => { value.selected_network.physical_audit.max_stepwise_log10_mass_action_residual = -1; },
  ]) {
    const raw = response(); mutate(raw);
    assert.throws(() => normalizeInverseDesignResult(raw, request()));
    assert.equal(designedNetworkFromResult(raw), null);
  }
});

test('a rejected pruning attempt may report unavailable RMSE after an equilibrium failure', () => {
  const raw = response();
  raw.pruning_history[1].rmse = null;
  raw.pruning_history[1].reason = 'Refit equilibrium failed; no valid loss exists.';
  assert.equal(normalizeInverseDesignResult(raw, request()).pruning_history[1].rmse, null);
  raw.pruning_history[1].accepted = true;
  assert.throws(() => normalizeInverseDesignResult(raw, request()));
});

test('sample residuals substantiate reported weighted per-output RMSE and half-MSE', () => {
  for (const mutate of [
    value => { value.selected_network.rmse = 0; },
    value => { value.selected_network.per_output_rmse = [0]; },
    value => { value.selected_network.fit_loss = 0.0001; },
    value => { value.selected_network.predictions[1][0] = 0.99; },
  ]) {
    const raw = response(); mutate(raw);
    assert.throws(() => normalizeInverseDesignResult(raw, request()), /residuals/);
  }
});

test('one accurate output cannot mask an unmet second output behind a small aggregate RMSE', () => {
  const input = request({ target: target({
    outputs: [...target().outputs, { name: 'vertical', species: 'B', transform: 'linear', offset: 0, optimize_offset: false }],
    samples: target().samples.map(sample => ({ ...sample, outputs: [...sample.outputs, 0.5] })),
  }) });
  const raw = response(input);
  raw.selected_network.predictions = raw.selected_network.targets.map(row => [row[0] + 0.001, row[1] + 0.07]);
  raw.selected_network.per_output_rmse = [0.001, 0.07];
  raw.selected_network.fit_loss = (0.001 ** 2 + 0.07 ** 2) / 4;
  raw.selected_network.rmse = Math.sqrt(2 * raw.selected_network.fit_loss);
  assert.ok(raw.selected_network.rmse < input.optimization.max_rmse);
  assert.throws(() => normalizeInverseDesignResult(raw, input), /tolerance for an output/);
  raw.target_met = false;
  assert.equal(normalizeInverseDesignResult(raw, input).target_met, false);
});
