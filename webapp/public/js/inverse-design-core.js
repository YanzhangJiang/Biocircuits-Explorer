// Shared admission boundary for browser and native-webview inverse design.
// A new design always starts with an explicit, editable mathematical target.
import { validateDesignTarget } from './design-target-adapters.js';

export const INVERSE_DESIGN_DEFAULTS = Object.freeze({
  chemistry: Object.freeze({ auxiliary_monomers: 2, max_complex_size: 3, max_reactions: 48, allow_homomers: true }),
  optimization: Object.freeze({ epochs: 150, learning_rate: 0.03, restarts: 2, prune_rounds: 3,
    prune_fraction: 0.2, prune_tolerance: 0.02, max_rmse: 0.05, optimize_totals: true, seed: 1 }),
});

const LIMITS = Object.freeze({ auxiliary_monomers: 4, max_complex_size: 8, max_reactions: 256,
  epochs: 2000, restarts: 8, prune_rounds: 12, seed: 2147483647 });

// For a deterministic equilibrium response, every exactly identical input
// tuple has one predicted value per output. Minimizing its weighted residuals
// over even an unrestricted function gives this necessary error floor. This
// calculation neither averages/replaces target rows nor merges nearby inputs.
export function designTargetErrorFloor(target) {
  return ['samples', 'validation_samples'].flatMap(key => {
    const samples = target[key] || [];
    if (!samples.length) return [];
    const groups = new Map();
    for (const sample of samples) {
      const tuple = JSON.stringify(sample.inputs);
      if (!groups.has(tuple)) groups.set(tuple, []);
      groups.get(tuple).push(sample);
    }
    const conflicts = [...groups.values()].filter(rows => rows.some(row =>
      row.outputs.some((value, dimension) => value !== rows[0].outputs[dimension])));
    if (!conflicts.length) return [];
    const maxWeight = Math.max(...samples.map(row => row.weight ?? 1));
    const weight = row => (row.weight ?? 1) / maxWeight;
    const weightSum = samples.reduce((sum, row) => sum + weight(row), 0);
    const perOutput = target.outputs.map((_, dimension) => {
      const scale = Math.max(...samples.map(row => Math.abs(row.outputs[dimension]))) || 1;
      let squared = 0;
      for (const rows of conflicts) {
        if (rows.every(row => row.outputs[dimension] === rows[0].outputs[dimension])) continue;
        const groupWeight = rows.reduce((sum, row) => sum + weight(row), 0);
        if (!groupWeight) continue;
        const mean = rows.reduce((sum, row) => sum + weight(row) * (row.outputs[dimension] / scale), 0) / groupWeight;
        squared += rows.reduce((sum, row) => sum + weight(row) * (row.outputs[dimension] / scale - mean) ** 2, 0);
      }
      return scale * Math.sqrt(squared / weightSum);
    });
    return [{ dataset: key === 'samples' ? 'Training' : 'Validation',
      conflicting_groups: conflicts.length, per_output_rmsd: perOutput }];
  });
}

function record(value, label) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error(`${label} must be an object.`);
  return value;
}
function finite(value, label, { positive = false, nonnegative = false, min = -Infinity, max = Infinity } = {}) {
  if (typeof value !== 'number' || !Number.isFinite(value)) throw new Error(`${label} must be a finite number.`);
  if (positive && value <= 0) throw new Error(`${label} must be positive.`);
  if (nonnegative && value < 0) throw new Error(`${label} must be non-negative.`);
  if (value < min) throw new Error(`${label} must be at least ${min}.`);
  if (value > max) throw new Error(`${label} must not exceed ${max}.`);
  return value;
}
function integer(value, label, min, max) {
  finite(value, label);
  if (!Number.isInteger(value) || value < min || value > max) throw new Error(`${label} must be an integer from ${min} to ${max}.`);
  return value;
}
function boolean(value, label) {
  if (typeof value !== 'boolean') throw new Error(`${label} must be a boolean.`);
  return value;
}
function string(value, label) {
  if (typeof value !== 'string' || !value.trim()) throw new Error(`${label} must be a non-empty string.`);
  return value.trim();
}
function strings(value, label, min = 0) {
  if (!Array.isArray(value) || value.length < min) throw new Error(`${label} must be an array of strings.`);
  return Array.from(value, (entry, index) => string(entry, `${label}[${index + 1}]`));
}
function numbers(value, label, count, options) {
  if (!Array.isArray(value) || value.length !== count) throw new Error(`${label} must contain ${count} values.`);
  return Array.from(value, (entry, index) => finite(entry, `${label}[${index + 1}]`, options));
}
function matrix(value, label, rows, cols) {
  if (!Array.isArray(value) || value.length !== rows) throw new Error(`${label} must contain ${rows} sample rows.`);
  return Array.from(value, (row, index) => numbers(row, `${label}[${index + 1}]`, cols));
}
function equalList(actual, expected, label) {
  if (actual.length !== expected.length || actual.some((entry, index) => entry !== expected[index])) {
    throw new Error(`${label} does not match the design target or selected network.`);
  }
}
function equalMatrix(actual, expected, label) {
  if (actual.length !== expected.length) throw new Error(`${label} has a different sample count.`);
  actual.forEach((row, index) => equalList(row, expected[index], label));
}
function canonical(value) {
  if (Array.isArray(value)) return `[${value.map(canonical).join(',')}]`;
  if (value && typeof value === 'object') return `{${Object.keys(value).sort().map(key => `${JSON.stringify(key)}:${canonical(value[key])}`).join(',')}}`;
  return JSON.stringify(value);
}
// Preserve evidence metadata without admitting values silently lost by JSON.
function jsonCopy(value, label = 'Result', parents = new Set()) {
  if (value === null || typeof value === 'string' || typeof value === 'boolean') return value;
  if (typeof value === 'number') return finite(value, label);
  if (typeof value !== 'object' || parents.has(value)) throw new Error(`${label} must be JSON serializable.`);
  if (!Array.isArray(value) && Object.getPrototypeOf(value) !== Object.prototype && Object.getPrototypeOf(value) !== null) {
    throw new Error(`${label} must be ordinary JSON data.`);
  }
  if (Object.getOwnPropertySymbols(value).length) throw new Error(`${label} must contain only JSON keys.`);
  parents.add(value);
  const copy = Array.isArray(value)
    ? Array.from(value, (entry, index) => jsonCopy(entry, `${label}[${index + 1}]`, parents))
    : Object.fromEntries(Object.entries(value).map(([key, entry]) => [key, jsonCopy(entry, `${label}.${key}`, parents)]));
  parents.delete(value);
  return copy;
}

function identifier(value, label) {
  const name = string(value, label);
  if (!/^[A-Za-z][A-Za-z0-9_]*$/.test(name)) throw new Error(`${label} must be a chemical identifier.`);
  return name;
}
function chemistryConstraints(chemistry) {
  const allowed = new Set([...Object.keys(INVERSE_DESIGN_DEFAULTS.chemistry), 'max_copies', 'forbidden_complexes', 'binding_gates']);
  if (Object.keys(chemistry).some(key => !allowed.has(key))) throw new Error('Unrecognized chemistry constraint.');
  const extras = {};
  const counts = (value, label, min) => Object.fromEntries(Object.entries(record(value, label)).map(([name, count]) =>
    [identifier(name, label), integer(count, `${label}.${name}`, min, 8)]));
  if (chemistry.max_copies !== undefined) extras.max_copies = counts(chemistry.max_copies, 'max_copies', 0);
  if (chemistry.forbidden_complexes !== undefined) {
    const names = strings(chemistry.forbidden_complexes, 'forbidden_complexes');
    if (names.length > 256) throw new Error('forbidden_complexes must contain at most 256 species.');
    extras.forbidden_complexes = [...new Set(names.map(name => identifier(name, 'forbidden complex')))];
  }
  if (chemistry.binding_gates !== undefined) {
    if (!Array.isArray(chemistry.binding_gates) || chemistry.binding_gates.length > 24) throw new Error('binding_gates must contain at most 24 rules.');
    extras.binding_gates = Array.from(chemistry.binding_gates, raw => {
      const gate = record(raw, 'binding gate');
      if (Object.keys(gate).some(key => !['monomer', 'requires', 'unless_core_count_at_least'].includes(key))) throw new Error('Unrecognized binding gate constraint.');
      const requires = counts(gate.requires, 'binding gate requires', 1);
      if (!Object.keys(requires).length) throw new Error('A binding gate requires at least one monomer.');
      return { monomer: identifier(gate.monomer, 'gated monomer'), requires,
        ...(gate.unless_core_count_at_least !== undefined ? { unless_core_count_at_least: integer(gate.unless_core_count_at_least, 'unless_core_count_at_least', 0, 8) } : {}) };
    });
  }
  return extras;
}

export function validateInverseDesignRequest(value) {
  const input = record(value, 'Inverse design request');
  const target = validateDesignTarget(input.target);
  target.outputs = target.outputs.map(({ name, species, transform, offset, optimize_offset }) =>
    ({ name, species, transform, offset, optimize_offset }));
  if (!target.validation_samples?.length) delete target.validation_samples;
  const chemistry = { ...INVERSE_DESIGN_DEFAULTS.chemistry, ...record(jsonCopy(input.chemistry ?? {}, 'chemistry'), 'chemistry') };
  const optimization = { ...INVERSE_DESIGN_DEFAULTS.optimization, ...record(jsonCopy(input.optimization ?? {}, 'optimization'), 'optimization') };
  const extras = chemistryConstraints(chemistry);
  if (Object.keys(optimization).some(key => !Object.hasOwn(INVERSE_DESIGN_DEFAULTS.optimization, key))) throw new Error('Unrecognized optimization setting.');
  // Explicitly select endpoint fields; a legacy candidate request cannot become
  // a target-driven run through coercion or invisible default geometry.
  return {
    target,
    chemistry: {
      auxiliary_monomers: integer(chemistry.auxiliary_monomers, 'auxiliary_monomers', 0, LIMITS.auxiliary_monomers),
      max_complex_size: integer(chemistry.max_complex_size, 'max_complex_size', 2, LIMITS.max_complex_size),
      max_reactions: integer(chemistry.max_reactions, 'max_reactions', 1, LIMITS.max_reactions),
      allow_homomers: boolean(chemistry.allow_homomers, 'allow_homomers'),
      ...extras,
    },
    optimization: {
      epochs: integer(optimization.epochs, 'epochs', 1, LIMITS.epochs),
      learning_rate: finite(optimization.learning_rate, 'learning_rate', { min: 1e-5, max: 0.5 }),
      restarts: integer(optimization.restarts, 'restarts', 1, LIMITS.restarts),
      prune_rounds: integer(optimization.prune_rounds, 'prune_rounds', 0, LIMITS.prune_rounds),
      prune_fraction: finite(optimization.prune_fraction, 'prune_fraction', { min: 0.01, max: 1 }),
      prune_tolerance: finite(optimization.prune_tolerance, 'prune_tolerance', { nonnegative: true, max: 1 }),
      max_rmse: finite(optimization.max_rmse, 'max_rmse', { nonnegative: true, max: 1e8 }),
      optimize_totals: boolean(optimization.optimize_totals, 'optimize_totals'),
      seed: integer(optimization.seed, 'seed', 0, LIMITS.seed),
    },
  };
}

function validateReadouts(outputs, expected) {
  if (!Array.isArray(outputs) || outputs.length !== expected.length) throw new Error('Selected network readouts do not match target dimensions.');
  return Array.from(outputs, (output, index) => {
    record(output, `selected_network.outputs[${index + 1}]`);
    const wanted = expected[index];
    for (const key of ['name', 'species', 'transform']) {
      if (output[key] !== wanted[key]) throw new Error(`Selected network output ${key} does not match the target.`);
    }
    finite(output.offset, 'Selected output offset');
    if (!wanted.optimize_offset && output.offset !== wanted.offset) throw new Error('A fixed readout offset must not change during optimization.');
    if (output.optimize_offset !== undefined && output.optimize_offset !== wanted.optimize_offset) {
      throw new Error('Selected output offset optimization must match the target.');
    }
    return output;
  });
}

function targetSemantics(target) {
  // Output plotting ranges are editor metadata; concrete sample targets are
  // the numerical objective. Empty validation has identical semantics absent.
  return { ...target, outputs: target.outputs.map(({ name, species, transform, offset, optimize_offset }) =>
    ({ name, species, transform, offset, optimize_offset })), validation_samples: target.validation_samples ?? [] };
}
function near(actual, expected, label) {
  if (!Number.isFinite(expected) || Math.abs(actual - expected) > 1e-12 + 1e-8 * Math.max(Math.abs(actual), Math.abs(expected))) {
    throw new Error(`${label} does not match the replayed sample residuals.`);
  }
}
function replayMetrics(replay, samples, outputs, label) {
  const weights = samples.map(sample => sample.weight);
  const maxWeight = Math.max(...weights);
  const scaledWeights = weights.map(weight => weight / maxWeight);
  const weightSum = scaledWeights.reduce((sum, weight) => sum + weight, 0);
  const perOutput = outputs.map((_, dimension) => {
    const scale = Math.max(...samples.map((sample, index) => Math.max(Math.abs(sample.outputs[dimension]), Math.abs(replay.predictions[index][dimension])))) || 1;
    const squared = samples.reduce((sum, sample, index) => sum + scaledWeights[index] *
      (replay.predictions[index][dimension] / scale - sample.outputs[dimension] / scale) ** 2, 0);
    return scale * Math.sqrt(squared / weightSum);
  });
  const reported = numbers(replay.per_output_rmse, `${label}.per_output_rmse`, outputs.length, { nonnegative: true });
  reported.forEach((rmse, index) => near(rmse, perOutput[index], `${label}.per_output_rmse`));
  const scale = Math.max(...perOutput) || 1;
  const rmse = scale * Math.sqrt(perOutput.reduce((sum, value) => sum + (value / scale) ** 2, 0) / outputs.length);
  finite(replay.rmse, `${label}.rmse`, { nonnegative: true });
  near(replay.rmse, rmse, `${label}.rmse`);
  if (replay.fit_loss !== undefined) {
    finite(replay.fit_loss, `${label}.fit_loss`, { nonnegative: true });
    near(replay.fit_loss, (rmse / Math.SQRT2) ** 2, `${label}.fit_loss`);
  }
}
function validateAudit(audit, target) {
  record(audit, 'selected_network.physical_audit');
  if (audit.cold_replay !== true) throw new Error('Selected network requires cold physical replay.');
  for (const key of ['max_log10_mass_residual', 'max_stepwise_log10_mass_action_residual']) {
    finite(audit[key], `physical_audit.${key}`, { nonnegative: true, max: 1e-7 });
  }
  if (audit.training_samples !== target.samples.length || audit.validation_samples !== (target.validation_samples?.length ?? 0)) {
    throw new Error('Physical audit sample counts do not match the design target.');
  }
}

function validateReplay(selected, target) {
  record(selected, 'selected_network');
  if (selected.status !== 'ok') throw new Error(selected.reason || 'The selected network did not pass equilibrium replay.');
  selected.rules = strings(selected.rules, 'selected_network.rules');
  selected.kd = numbers(selected.kd, 'selected_network.kd', selected.rules.length, { positive: true });
  if (new Set(selected.rules).size !== selected.rules.length) throw new Error('Selected reaction rules must be unique.');
  if (selected.evidence_tier !== 'sampled_equilibrium_replay' || selected.prediction_basis !== 'selected_network') {
    throw new Error('The designed network requires sampled equilibrium replay evidence.');
  }
  const totals = record(selected.totals, 'selected_network.totals');
  for (const [name, value] of Object.entries(totals)) {
    if (!/^[A-Za-z][A-Za-z0-9_]*$/.test(name)) throw new Error('Selected network total names must be chemical identifiers.');
    finite(value, `selected_network.totals.${name}`, { positive: true });
  }
  if (target.inputs.some(input => Object.hasOwn(totals, input.name))) throw new Error('Fitted totals must contain non-input monomers only.');
  if (selected.inputs !== undefined && canonical(selected.inputs) !== canonical(target.inputs)) throw new Error('Selected network inputs do not match the target.');
  if (selected.monomers !== undefined) {
    const monomers = strings(selected.monomers, 'selected_network.monomers', 1);
    const fixed = monomers.filter(name => !target.inputs.some(input => input.name === name)).sort();
    equalList(Object.keys(totals).sort(), fixed, 'Selected non-input total names');
    if (target.inputs.some(input => !monomers.includes(input.name))) throw new Error('The selected network is missing an input monomer.');
  }
  if (selected.species !== undefined) {
    const species = strings(selected.species, 'selected_network.species', 1);
    if (target.outputs.some(output => !species.includes(output.species))) throw new Error('The selected network is missing a readout species.');
  }
  selected.outputs = validateReadouts(selected.outputs, target.outputs);
  selected.predictions = matrix(selected.predictions, 'selected_network.predictions', target.samples.length, target.outputs.length);
  selected.targets = matrix(selected.targets, 'selected_network.targets', target.samples.length, target.outputs.length);
  equalMatrix(selected.targets, target.samples.map(sample => sample.outputs), 'Selected network targets');
  finite(selected.rmse, 'selected_network.rmse', { nonnegative: true });
  finite(selected.fit_loss, 'selected_network.fit_loss', { nonnegative: true });
  replayMetrics(selected, target.samples, target.outputs, 'selected_network');
  validateAudit(selected.physical_audit, target);
  // Backend NetworkIR is useful for downstream handoff but is optional. Its
  // reaction parameters, when present, must describe the replayed network.
  if (selected.network_ir !== undefined) {
    const ir = record(selected.network_ir, 'selected_network.network_ir');
    if (!Array.isArray(ir.reactions)) throw new Error('Selected network IR reactions must be an array.');
    equalList(ir.reactions.map(reaction => record(reaction, 'network_ir.reactions').formula), selected.rules, 'Selected network IR rules');
    equalList(ir.reactions.map(reaction => reaction.kd), selected.kd, 'Selected network IR Kd values');
  }
  return selected;
}

export function normalizeInverseDesignResult(raw, requestValue) {
  const request = validateInverseDesignRequest(requestValue);
  const result = jsonCopy(record(raw, 'Inverse design result'));
  if (result.status !== 'ok') throw new Error('Inverse design did not return a valid optimization result.');
  result.target = validateDesignTarget(result.target);
  if (canonical(targetSemantics(result.target)) !== canonical(targetSemantics(request.target))) throw new Error('Result target does not match the requested design target.');
  boolean(result.target_met, 'target_met');
  result.selected_network = validateReplay(result.selected_network, request.target);
  integer(result.initial_reaction_count, 'initial_reaction_count', 0, request.chemistry.max_reactions);
  integer(result.final_reaction_count, 'final_reaction_count', 0, result.initial_reaction_count);
  if (result.final_reaction_count !== result.selected_network.rules.length) throw new Error('Final reaction count does not match the replayed network.');
  if (result.target_met && result.selected_network.per_output_rmse.some(value => value > request.optimization.max_rmse)) throw new Error('The selected network exceeds the requested RMSE tolerance for an output.');
  if (!Array.isArray(result.pruning_history)) throw new Error('pruning_history must be an array.');
  result.pruning_history.forEach((attempt, index) => {
    record(attempt, `pruning_history[${index + 1}]`);
    integer(attempt.before, 'pruning before', 0, result.initial_reaction_count);
    integer(attempt.after, 'pruning after', 0, attempt.before);
    boolean(attempt.accepted, 'pruning accepted');
    if (attempt.rmse === null && !attempt.accepted) {
      // A failed physical solve has no numerical error estimate.
    } else finite(attempt.rmse, 'pruning RMSE', { nonnegative: true });
    string(attempt.reason, 'pruning reason');
  });
  if (!Array.isArray(result.optimization_history)) throw new Error('optimization_history must be an array.');
  result.optimization_history.forEach((entry, index) => {
    record(entry, `optimization_history[${index + 1}]`);
    string(entry.phase, 'optimization phase');
    integer(entry.step, 'optimization step', 0, Number.MAX_SAFE_INTEGER);
    finite(entry.loss, 'optimization loss', { nonnegative: true });
  });
  if (result.validation !== undefined) {
    const validation = record(result.validation, 'validation');
    const samples = request.target.validation_samples ?? [];
    if (!samples.length) throw new Error('Validation replay requires explicit validation samples.');
    matrix(validation.predictions, 'validation.predictions', samples.length, request.target.outputs.length);
    const targets = matrix(validation.targets, 'validation.targets', samples.length, request.target.outputs.length);
    equalMatrix(targets, samples.map(sample => sample.outputs), 'Validation targets');
    replayMetrics(validation, samples, request.target.outputs, 'validation');
    if (result.target_met && validation.per_output_rmse.some(value => value > request.optimization.max_rmse)) throw new Error('Validation replay exceeds the requested RMSE tolerance.');
  } else if (request.target.validation_samples?.length) {
    throw new Error('The result is missing the requested validation replay.');
  }
  const targetMet = result.selected_network.per_output_rmse.every(value => value <= request.optimization.max_rmse) &&
    (!result.validation || result.validation.per_output_rmse.every(value => value <= request.optimization.max_rmse));
  if (result.target_met !== targetMet) throw new Error('target_met does not match the per-output replay errors.');
  if (result.termination !== undefined) {
    const stop = record(result.termination, 'termination');
    if (stop.reason !== (result.target_met ? 'target_met' : 'search_budget_exhausted')) throw new Error('Termination reason does not match target compliance.');
    for (const [key, option] of [['epochs_per_fit', 'epochs'], ['initializations', 'restarts'], ['pruning_round_limit', 'prune_rounds']]) {
      if (stop[key] !== request.optimization[option]) throw new Error('Termination budget does not match the request.');
    }
    if (!Array.isArray(stop.fit_stops) || !stop.fit_stops.length || stop.fit_stops.length > stop.initializations * (stop.pruning_round_limit + 1)) throw new Error('Termination requires bounded fit records.');
    const seen = new Set();
    for (const fit of stop.fit_stops) {
      record(fit, 'fit stop');
      integer(fit.restart, 'fit initialization', 1, stop.initializations);
      integer(fit.prune_round, 'fit pruning round', 0, stop.pruning_round_limit);
      integer(fit.iterations, 'fit iterations', 0, stop.epochs_per_fit);
      integer(fit.best_step, 'best evaluated fit step', 0, fit.iterations);
      if (fit.phase !== (fit.prune_round ? 'prune_refit' : 'fit') ||
          !['iteration_limit', 'invalid_equilibrium_update'].includes(fit.reason) ||
          (fit.reason === 'iteration_limit') !== (fit.iterations === stop.epochs_per_fit)) throw new Error('Invalid fit stopping reason.');
      const key = `${fit.restart}:${fit.prune_round}`;
      if (seen.has(key)) throw new Error('Duplicate fit stopping record.');
      seen.add(key);
    }
    if (stop.iterations !== stop.fit_stops.reduce((sum, fit) => sum + fit.iterations, 0)) throw new Error('Termination iteration count does not match evaluated fits.');
  }
  record(result.algorithm, 'algorithm');
  result.warnings = strings(result.warnings ?? [], 'warnings');
  return result;
}

export function designedNetworkFromResult(raw) {
  try {
    // Requiring the dedicated target schema prevents legacy sparse-library
    // results from being promoted into fresh target-design outputs on restore.
    const result = jsonCopy(record(raw, 'Inverse design result'));
    if (result.status !== 'ok' || typeof result.target_met !== 'boolean') return null;
    const target = validateDesignTarget(result.target);
    const selected = validateReplay(result.selected_network, target);
    if (result.final_reaction_count !== selected.rules.length) return null;
    return {
      reactions: selected.rules.slice(), kds: selected.kd.slice(), totals: { ...selected.totals },
      outputs: jsonCopy(selected.outputs), target: jsonCopy(target), target_met: result.target_met,
      rmse: selected.rmse, fit_loss: selected.fit_loss, per_output_rmse: selected.per_output_rmse.slice(),
      evidence_tier: selected.evidence_tier, prediction_basis: selected.prediction_basis,
      ...(selected.network_ir ? { network_ir: jsonCopy(selected.network_ir) } : {}),
      physical_audit: jsonCopy(selected.physical_audit),
      ...(selected.model_handoff ? { model_handoff: jsonCopy(selected.model_handoff) } : {}),
    };
  } catch {
    return null;
  }
}
