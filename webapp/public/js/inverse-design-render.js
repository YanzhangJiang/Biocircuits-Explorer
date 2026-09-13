// Synchronous, escaped renderers bind all displayed evidence to the owning run.
import { targetDrawingRows } from './design-target-drawing.js';
import { designTargetErrorFloor } from './inverse-design-core.js';
import { escapeHtml } from './api.js';

const escape = escapeHtml;

function number(value) {
  return typeof value === 'number' && Number.isFinite(value) ? Number(value.toPrecision(5)).toString() : '—';
}

export const INVERSE_FIT_PHASES = new Set(['fit', 'prune_refit', 'fit_step_rejected', 'prune_refit_step_rejected']);

// A reported RMSD is taken as-is; a bare nonnegative loss converts back to RMSD.
function progressRmse(progress) {
  return Number.isFinite(progress.rmse) ? progress.rmse
    : Number.isFinite(progress.loss) && progress.loss >= 0 ? Math.sqrt(2 * progress.loss) : null;
}

// Fritsch–Carlson monotone cubic interpolation: a smooth path through the
// samples that cannot overshoot them, so the curve never invents extrema.
function monotoneCubicPath(points) {
  const slopes = [], tangents = [];
  for (let i = 0; i < points.length - 1; i += 1) {
    slopes.push((points[i + 1].y - points[i].y) / (points[i + 1].x - points[i].x));
  }
  tangents.push(slopes[0]);
  for (let i = 1; i < points.length - 1; i += 1) {
    // A local extremum sits exactly at its sample: a zero tangent there keeps
    // every segment's control points inside the segment's endpoint range.
    tangents.push(slopes[i - 1] * slopes[i] <= 0 ? 0 : (slopes[i - 1] + slopes[i]) / 2);
  }
  tangents.push(slopes[points.length - 2]);
  for (let i = 0; i < points.length - 1; i += 1) {
    if (slopes[i] === 0) { tangents[i] = 0; tangents[i + 1] = 0; continue; }
    const a = tangents[i] / slopes[i], b = tangents[i + 1] / slopes[i];
    if (a * a + b * b > 9) {
      const scale = 3 / Math.sqrt(a * a + b * b);
      tangents[i] = scale * a * slopes[i];
      tangents[i + 1] = scale * b * slopes[i];
    }
  }
  let path = `M${points[0].x},${points[0].y}`;
  for (let i = 0; i < points.length - 1; i += 1) {
    const h = points[i + 1].x - points[i].x;
    path += `C${points[i].x + h / 3},${points[i].y + (tangents[i] * h) / 3} ${points[i + 1].x - h / 3},${points[i + 1].y - (tangents[i + 1] * h) / 3} ${points[i + 1].x},${points[i + 1].y}`;
  }
  return path;
}

function polylinePath(points) {
  return points.map((point, position) => `${position ? 'L' : 'M'}${point.x},${point.y}`).join(' ');
}

// Smooth ordered curves only when they are a single-valued function of the
// horizontal axis; anything else (few points, repeated or folding abscissae,
// nonfinite coordinates) keeps plain line segments.
function smoothPath(points) {
  if (points.length < 4 || !points.every(point => Number.isFinite(point.x) && Number.isFinite(point.y))) return null;
  let ordered = points;
  if (!points.every((point, index) => index === 0 || point.x > points[index - 1].x)) {
    if (!points.every((point, index) => index === 0 || point.x < points[index - 1].x)) return null;
    ordered = [...points].reverse();
  }
  return monotoneCubicPath(ordered);
}

export function renderDesignTargetErrorFloor(target, tolerance = null) {
  const floors = designTargetErrorFloor(target);
  if (!floors.length) return '';
  const unreachable = Number.isFinite(tolerance) && floors.some(floor =>
    floor.per_output_rmsd.some(value => value > tolerance + 1e-12 + value * 1e-10));
  return `<div class="inverse-note inverse-target-error-floor" role="note"><strong>Target error floor</strong>
    <p>Identical inputs have different requested outputs. A static equilibrium response returns one value per output at the same input combination.</p>
    ${floors.map(floor => `<p>${floor.dataset} RMSD lower bound: ${target.outputs.map((output, index) => `${escape(output.name)} ≥ ${number(floor.per_output_rmsd[index])}`).join('; ')}.</p>`).join('')}
    ${unreachable ? `<p>The requested RMSD ${number(tolerance)} is below this bound. More iterations cannot reach that tolerance with the current input/output definition.</p>` : ''}
    <p>Your original stroke and every target evaluation are preserved.</p></div>`;
}

export function inverseDesignCompletionStatus(result) {
  if (result.target_met) return 'Learning complete — target met.';
  const rejected = result.termination?.fit_stops?.some(fit => fit.reason === 'invalid_equilibrium_update') ||
    result.optimization_history?.some(row => row.phase.endsWith('_step_rejected'));
  return rejected ? 'Search stopped — target not met; a numerical update ended a fit early.'
    : 'Search stopped — budget exhausted; target not met.';
}

function renderSearchTermination(result, request) {
  if (result.target_met) return '';
  const budget = result.algorithm?.optimization || request.optimization;
  const stop = result.termination;
  const fits = stop?.fit_stops;
  return `<div class="inverse-note inverse-search-termination" role="note">
    <strong>${escape(inverseDesignCompletionStatus(result))}</strong>
    ${Number.isInteger(budget.epochs) && Number.isInteger(budget.restarts) ? `<p>Search budget: ${budget.epochs} iterations per fit · ${budget.restarts} initializations · up to ${budget.prune_rounds} pruning rounds per initialization.</p>` : ''}
    ${fits?.length && Number.isInteger(stop.iterations) ? `<p>Evaluated ${stop.iterations} parameter updates across ${fits.length} fits; ${fits.filter(fit => fit.reason === 'iteration_limit').length} fits reached their iteration limit.</p>` : ''}
    <p>Reaching a search limit does not establish convergence. The best checked response found is shown below.</p>
    <button class="btn btn-small" data-action="reviewInverseDesignBudget">Adjust search budget</button>
  </div>`;
}

// Only a reported fit has an iteration fraction. Pruning, initialization and
// final checks have no predictable total duration or honest overall percentage.
export function renderInverseDesignProgress(job, history = null) {
  const progress = job.progress || job;
  const phase = job.status === 'succeeded' ? 'exporting' : progress.phase;
  const stages = {
    constructing: 'Preparing the supernetwork…',
    initializing: 'Learning — initializing network parameters',
    fit: 'Learning — optimizing the supernetwork',
    checking_fit: 'Checking the fitted network…',
    pruning: 'Learning — pruning network branches',
    prune_refit: 'Learning — refitting after pruning',
    checking_pruned: 'Checking the pruned network…',
    pruning_decision: 'Learning — evaluating the pruning step',
    selecting: 'Selecting the designed network…',
    checking_selected: 'Checking the selected network…',
    exporting: 'Preparing the network output…',
    fit_step_rejected: 'Retaining the best evaluated fit…',
    prune_refit_step_rejected: 'Retaining the best evaluated refit…',
  };
  const status = job.status === 'submitting' ? 'Starting learning — submitting the target…'
    : job.status === 'queued' ? 'Queued — waiting for the local optimizer…'
    : job.status === 'cancel_requested' ? 'Stopping learning…'
    : job.status === 'failed' ? 'Learning failed'
    : job.status === 'cancelled' ? 'Learning stopped'
    : stages[phase] || 'Learning — waiting for an optimization update…';
  const count = value => Number.isInteger(value) && value >= 0 ? value : null;
  const fraction = (value, total) => count(value) === null ? '—'
    : `${value}${count(total) === null ? '' : ` / ${total}`}`;
  const fitting = INVERSE_FIT_PHASES.has(phase);
  const step = fitting ? count(progress.step) : null, epochs = count(progress.epochs);
  const rmse = progressRmse(progress);
  const metric = (label, value) => `<div><span>${label}</span><strong>${escape(value)}</strong></div>`;
  const reactions = count(progress.reactions), initial = count(progress.initial_reactions);
  const pruning = progress.last_pruning;
  const decision = pruning && typeof pruning.accepted === 'boolean' && count(pruning.before) !== null && count(pruning.after) !== null
    ? `<p class="inverse-note inverse-pruning-update">Last pruning: ${pruning.accepted
      ? `${pruning.before} → ${pruning.after} reactions kept after refitting.`
      : `${pruning.before} → ${pruning.after} rejected; retained ${pruning.before} reactions.`}</p>` : '';
  const curve = history?.samples?.length ? renderLearningCurve(history.samples, history.events) : '';
  return {
    status,
    button: job.status === 'submitting' ? 'Starting…' : job.status === 'queued' ? 'Queued…'
      : job.status === 'succeeded' ? 'Finishing…' : 'Learning…',
    html: `<div class="inverse-metrics inverse-learning-metrics">
      ${metric('Iteration in this fit', fraction(step, epochs))}
      ${metric('Initialization', fraction(progress.restart, progress.restarts))}
      ${metric('Pruning round', fraction(progress.prune_round, progress.prune_rounds))}
      ${metric('Training RMSD', number(rmse))}
      ${metric('Best RMSD in this fit', number(fitting ? progress.best_rmse : null))}
      ${metric('Current reactions', reactions === null ? '—' : initial === null ? reactions : `${initial} → ${reactions}`)}
    </div>${curve}${step !== null && epochs > 0 ? `<progress class="inverse-learning-bar" max="${epochs}" value="${Math.min(step, epochs)}" aria-label="Iterations in the current fit"></progress>` : ''}${decision}`,
  };
}

// Adaptive logarithmic ticks from the 1-2-3-5 family: the densest mantissa
// set that keeps the axis readable wins; degenerate ranges show min/max only.
function logTicks(low, high) {
  const first = Math.floor(Math.log10(low)), last = Math.ceil(Math.log10(high));
  for (const mantissas of [[1, 2, 3, 5], [1, 2, 5], [1]]) {
    const ticks = [];
    for (let exponent = first; exponent <= last; exponent += 1) {
      for (const mantissa of mantissas) {
        const value = mantissa * 10 ** exponent;
        if (value >= low && value <= high) ticks.push(value);
      }
    }
    if (ticks.length >= 2 && ticks.length <= 5) return ticks;
  }
  return [];
}

// Minimal live optimizer curve: L-shaped axes, no grid, logarithmic RMSD
// axis, the latest value emphasized, and restart/pruning decisions as dashed
// vertical markers. Only evaluated fitting updates ever become points.
export function renderLearningCurve(samples, events = []) {
  const points = (samples || []).filter(sample =>
    Number.isFinite(sample?.x) && Number.isFinite(sample?.y) && sample.y > 0);
  if (!points.length) return '';
  const width = 320, height = 150, left = 42, right = 10, top = 8, bottom = 24;
  const firstX = points[0].x, lastX = points.at(-1).x;
  const low = Math.min(...points.map(point => point.y)), high = Math.max(...points.map(point => point.y));
  const x = axis([firstX, lastX], left, width - right);
  const y = axis([low, high], height - bottom, top, { padding: 0.15, log: true });
  const coordinates = points.map(point => ({ x: x(point.x), y: y(point.y) }));
  const ticks = logTicks(low, high);
  const yTicks = (ticks.length ? ticks : [low, high]).map(value =>
    `<text x="${left - 5}" y="${y(value) + 3}" text-anchor="end">${number(value)}</text>`).join('');
  const markers = (events || []).filter(event => Number.isFinite(event?.x) && event.x > firstX && event.x <= lastX)
    .map(event => `<line class="inverse-learning-event" x1="${x(event.x)}" y1="${top}" x2="${x(event.x)}" y2="${height - bottom}"><title>${event.kind === 'prune' ? 'Pruning decision' : 'New initialization'}</title></line>`).join('');
  const latest = coordinates.at(-1);
  return `<div class="inverse-learning-curve"><div class="inverse-learning-curve-heading"><span>Training RMSD</span><strong>${number(points.at(-1).y)}</strong></div>
    <svg class="inverse-learning-curve-chart" viewBox="0 0 ${width} ${height}" role="img" aria-label="Training RMSD across ${points.length} evaluated updates, logarithmic vertical axis; latest ${number(points.at(-1).y)}">
    <path class="inverse-learning-axis" d="M${left} ${top}V${height - bottom}H${width - right}"/>
    ${markers}
    <path class="inverse-learning-rmse" d="${smoothPath(coordinates) || polylinePath(coordinates)}"/>
    <circle class="inverse-learning-rmse-latest" cx="${latest.x}" cy="${latest.y}" r="2.5"/>
    ${yTicks}
    <text x="${left}" y="${height - 7}" text-anchor="start">${number(firstX)}</text>
    <text x="${width - right}" y="${height - 7}" text-anchor="end">${number(lastX)}</text></svg></div>`;
}

export const LEARNING_CURVE_CAPACITY = 600;

// Accumulate one evaluated fitting update. The horizontal axis is a monotone
// sequence of reported evaluations, so restarts (which reset `step`) never
// move it backwards. Beyond the capacity every other point is dropped, which
// keeps the oldest-to-newest span without renumbering event markers.
export function recordLearningSample(history, job) {
  const progress = job.progress || job;
  const phase = job.status === 'succeeded' ? 'exporting' : progress.phase;
  const rmse = progressRmse(progress);
  if (!INVERSE_FIT_PHASES.has(phase) || rmse === null) return false;
  history.evaluated = (history.evaluated || 0) + 1;
  history.samples.push({ x: history.evaluated, y: rmse });
  if (Number.isInteger(progress.restart)) {
    if (Number.isInteger(history.restart) && progress.restart !== history.restart) {
      history.events.push({ x: history.evaluated, kind: 'restart' });
    }
    history.restart = progress.restart;
  }
  const pruning = progress.last_pruning;
  if (pruning && typeof pruning.accepted === 'boolean') {
    const key = `${progress.restart}:${progress.prune_round}:${pruning.before}:${pruning.after}:${pruning.accepted}`;
    if (key !== history.pruneKey) {
      history.pruneKey = key;
      history.events.push({ x: history.evaluated, kind: 'prune' });
    }
  }
  if (history.samples.length > LEARNING_CURVE_CAPACITY) {
    const latest = history.samples.at(-1);
    history.samples = history.samples.filter((_, index) => index % 2 === 0);
    if (history.samples.at(-1) !== latest) history.samples.push(latest);
  }
  return true;
}

function readout(output) {
  return `${escape(output.name)} = ${output.transform === 'log10' ? `log₁₀(${escape(output.species)})` : escape(output.species)} ${output.offset < 0 ? '−' : '+'} ${number(Math.abs(output.offset))}`;
}

export function renderDesignedNetwork(network, { historical = false } = {}) {
  const rows = network.reactions.map((rule, index) =>
    `<tr><td><code>${escape(rule)}</code></td><td>${number(network.kds[index])}</td></tr>`).join('');
  const totalRows = Object.entries(network.totals ?? {}).map(([name, value]) =>
    `<tr><td>${escape(name)}</td><td>${number(value)}</td></tr>`).join('');
  return `<div class="inverse-result-heading"><strong>${network.reactions.length} retained reaction${network.reactions.length === 1 ? '' : 's'}</strong>
    <span class="text-dim">${historical ? 'Saved parameters' : network.target_met ? 'Target met' : 'Target not met'}</span></div>
    ${rows ? `<table><caption class="inverse-sr-only">Designed reactions and dissociation constants</caption>
      <thead><tr><th scope="col">Reaction</th><th scope="col">Kd</th></tr></thead><tbody>${rows}</tbody></table>` : '<p class="text-dim inverse-note">The selected design requires no binding reactions.</p>'}
    ${totalRows ? `<table><caption class="inverse-sr-only">Final non-input total concentrations</caption><thead><tr><th scope="col">Monomer total</th><th scope="col">Concentration</th></tr></thead><tbody>${totalRows}</tbody></table>` : ''}
    ${network.outputs?.length ? `<p class="inverse-note">Readout: ${network.outputs.map(readout).join('; ')}</p>` : ''}
    <p class="text-dim inverse-note">${historical
    ? 'Run the connected workflow again before using this network.'
    : network.model_handoff?.available === false ? escape(network.model_handoff.reason)
    : 'Connect the reaction output to Model Builder to analyze this network with its fitted Kd, total concentrations, and readouts.'}</p>`;
}

// Scale before subtraction so even opposite-sign, finite 1e308 values have a
// finite SVG domain. Axes preserve physical input spacing (including log axes).
function axis(values, start, end, { padding = 0, log = false } = {}) {
  const transformed = values.map(value => log ? Math.log10(value) : value);
  const scale = Math.max(...transformed.map(value => Math.abs(value))) || 1;
  const low = Math.min(...transformed) / scale;
  const high = Math.max(...transformed) / scale;
  const span = high - low || 1;
  const minimum = low - span * padding;
  const extent = high - low + 2 * span * padding || 1;
  return value => start + (((log ? Math.log10(value) : value) / scale - minimum) / extent) * (end - start);
}

function chartValues(rows, target, output) {
  return [...rows.map(row => row[output]), target.outputs[output]?.min, target.outputs[output]?.max].filter(Number.isFinite);
}

function drawingPlotTarget(target, drawing, drawingRows) {
  const ranges = drawingRows ? JSON.parse(drawing).outputRanges : null;
  return ranges ? { ...target, outputs: target.outputs.map((output, index) => ({ ...output, ...ranges[index] })) } : target;
}

function fitChart(predictions, targets, output, label, target, drawingRows, responseLabel = 'Network response') {
  const width = 590, height = 210, left = 52, right = 18, top = 16, bottom = 43;
  const values = chartValues([...predictions, ...targets, ...(drawingRows || []).map(row => row.outputs)], target, output);
  const low = Math.min(...values), high = Math.max(...values);
  const singleInput = target.inputs.length === 1;
  const input = target.inputs[0];
  const xs = singleInput ? target.samples.map(sample => sample.inputs[0]) : target.samples.map((_, index) => index + 1);
  const xLow = singleInput ? input.min : 1, xHigh = singleInput ? input.max : Math.max(2, targets.length);
  const x = axis([xLow, xHigh], left, width - right, { log: singleInput && input.scale === 'log' });
  const y = axis(values, height - bottom, top, { padding: 0.08 });
  const order = xs.map((value, index) => ({ value, index })).sort((a, b) => a.value - b.value);
  const curve = points => (singleInput ? smoothPath(points) : null) || polylinePath(points);
  const coordinates = rows => order.map(({ index }) => ({ x: x(xs[index]), y: y(rows[index][output]) }));
  const targetPath = drawingRows
    ? curve(drawingRows.map(row => ({ x: x(row.inputs[0]), y: y(row.outputs[output]) })))
    : curve(coordinates(targets));
  const points = singleInput
    ? `<path class="inverse-target-curve" d="${targetPath}"><title>Target response curve</title></path><path class="inverse-fit-curve" d="${curve(coordinates(predictions))}"><title>Network response; connected numerical evaluations</title></path>`
    : targets.map((row, index) => `<circle class="inverse-target-point" cx="${x(xs[index])}" cy="${y(row[output])}" r="3"><title>Sample ${index + 1}: target ${number(row[output])}</title></circle>
    <rect class="inverse-fit-point" x="${x(xs[index]) - 2.5}" y="${y(predictions[index][output]) - 2.5}" width="5" height="5"><title>Sample ${index + 1}: fitted ${number(predictions[index][output])}</title></rect>`).join('');
  const xLabel = singleInput ? `${escape(input.name)} total${input.scale === 'log' ? ' (log scale)' : ''}` : 'Supplied sample';
  return `<figure class="inverse-fit-figure"><figcaption>${escape(label)} · <span class="inverse-target-legend">Your target</span> · <span class="inverse-fit-legend">${escape(responseLabel)}</span></figcaption>
    <svg class="inverse-fit-chart" viewBox="0 0 ${width} ${height}" role="img" aria-label="Target and fitted ${escape(label)} at ${targets.length} supplied samples; horizontal axis ${xLabel}">
      <path class="inverse-chart-axis" d="M${left} ${top}V${height - bottom}H${width - right}"/>
      <text x="${left - 5}" y="${y(high) + 4}" text-anchor="end">${number(high)}</text>
      <text x="${left - 5}" y="${y(low) + 4}" text-anchor="end">${number(low)}</text>
      <text x="${left}" y="${height - bottom + 17}" text-anchor="start">${number(xLow)}</text>
      <text x="${width - right}" y="${height - bottom + 17}" text-anchor="end">${number(singleInput ? xHigh : targets.length)}</text>
      <text x="${(width + left - right) / 2}" y="${height - 5}" text-anchor="middle">${xLabel}</text>${points}</svg></figure>`;
}

function trajectoryChart(selected, target, drawingRows, responseLabel = 'Network response') {
  const width = 590, height = 260, left = 55, right = 18, top = 16, bottom = 42;
  const rows = [...selected.targets, ...selected.predictions, ...(drawingRows || []).map(row => row.outputs)];
  const x = axis(chartValues(rows, target, 0), left, width - right, { padding: 0.08 });
  const y = axis(chartValues(rows, target, 1), height - bottom, top, { padding: 0.08 });
  const order = target.samples.map((sample, index) => ({ input: sample.inputs[0], index })).sort((a, b) => a.input - b.input);
  const coordinates = values => order.map(({ index }) => ({ x: x(values[index][0]), y: y(values[index][1]) }));
  // Parametric trajectories are smoothed only when the horizontal output is
  // monotone along the stroke/sample order; folds keep line segments.
  const curve = points => smoothPath(points) || polylinePath(points);
  const targetPath = drawingRows
    ? curve(drawingRows.map(row => ({ x: x(row.outputs[0]), y: y(row.outputs[1]) })))
    : curve(coordinates(selected.targets));
  const ticks = [0, 1].map(dimension => {
    const values = chartValues(rows, target, dimension);
    const low = Math.min(...values), high = Math.max(...values);
    return dimension === 0 ? [low, high].map(value => `<text x="${x(value)}" y="${height - bottom + 16}" text-anchor="middle">${number(value)}</text>`).join('')
      : [low, high].map(value => `<text x="${left - 6}" y="${y(value) + 4}" text-anchor="end">${number(value)}</text>`).join('');
  }).join('');
  return `<figure class="inverse-fit-figure"><figcaption>Output trajectory · ordered by ${escape(target.inputs[0].name)} · <span class="inverse-target-legend">Your target</span> · <span class="inverse-fit-legend">${escape(responseLabel)}</span></figcaption>
    <svg class="inverse-fit-chart" viewBox="0 0 ${width} ${height}" role="img" aria-label="Two-output target and network response, ordered by ${escape(target.inputs[0].name)}">
      <path class="inverse-chart-axis" d="M${left} ${top}V${height - bottom}H${width - right}"/>
      <path class="inverse-target-curve" d="${targetPath}"/>
      <path class="inverse-fit-curve" d="${curve(coordinates(selected.predictions))}"/>
      ${ticks}
      <text x="${width / 2}" y="${height - 6}" text-anchor="middle">${escape(target.outputs[0].name)}</text>
      <text x="${left}" y="12">${escape(target.outputs[1].name)}</text></svg></figure>`;
}

function fieldChart(selected, target, output, responseLabel = 'Network response') {
  const width = 590, height = 235, top = 32, bottom = 43, size = 235, gap = 55;
  const values = [...selected.targets, ...selected.predictions].map(row => row[output]);
  const color = axis(values, 0.15, 1);
  const panels = [['Your target', selected.targets], [responseLabel, selected.predictions]].map(([label, rows], panel) => {
    const left = 43 + panel * (size + gap);
    const x = axis([target.inputs[0].min, target.inputs[0].max], left, left + size, { log: target.inputs[0].scale === 'log' });
    const y = axis([target.inputs[1].min, target.inputs[1].max], height - bottom, top, { log: target.inputs[1].scale === 'log' });
    const points = rows.map((row, index) => `<circle class="inverse-fit-point" cx="${x(target.samples[index].inputs[0])}" cy="${y(target.samples[index].inputs[1])}" r="4" opacity="${color(row[output])}"><title>${target.inputs.map((input, dimension) => `${escape(input.name)}=${number(target.samples[index].inputs[dimension])}`).join(', ')}; ${escape(label)} ${number(row[output])}</title></circle>`).join('');
    return `<text x="${left + size / 2}" y="15" text-anchor="middle">${escape(label)}</text><path class="inverse-chart-axis" d="M${left} ${top}V${height - bottom}H${left + size}"/>
      <text x="${left}" y="${height - bottom + 17}">${number(target.inputs[0].min)}</text><text x="${left + size}" y="${height - bottom + 17}" text-anchor="end">${number(target.inputs[0].max)}</text>
      <text x="${left + size / 2}" y="${height - 4}" text-anchor="middle">${escape(target.inputs[0].name)}${target.inputs[0].scale === 'log' ? ' (log)' : ''}</text>
      <text x="${left - 5}" y="${top + 4}" text-anchor="end">${number(target.inputs[1].max)}</text><text x="${left - 5}" y="${height - bottom}" text-anchor="end">${number(target.inputs[1].min)}</text>${points}`;
  }).join('');
  return `<figure class="inverse-fit-figure"><figcaption>${escape(target.outputs[output].name)} field · vertical axis ${escape(target.inputs[1].name)}${target.inputs[1].scale === 'log' ? ' (log)' : ''} · shared intensity ${number(Math.min(...values))} to ${number(Math.max(...values))}</figcaption>
    <svg class="inverse-fit-chart" viewBox="0 0 ${width} ${height}" role="img" aria-label="Target and network response field at supplied inputs; darker points indicate larger output values">${panels}</svg></figure>`;
}

export function renderInverseDesignPreview(progress, request, drawing = '') {
  if (!Object.hasOwn(progress, 'predictions')) return null;
  const target = request.target, predictions = progress.predictions;
  if (!Array.isArray(predictions) || predictions.length !== target.samples.length ||
      predictions.some(row => !Array.isArray(row) || row.length !== target.outputs.length || row.some(value => !Number.isFinite(value))) ||
      !Number.isFinite(progress.rmse) || progress.rmse < 0) {
    return '<p class="node-error">The response preview does not match the current target. Waiting for a valid evaluation.</p>';
  }
  const selected = { predictions, targets: target.samples.map(sample => sample.outputs) };
  const drawingRows = targetDrawingRows(drawing, target);
  const plotTarget = drawingPlotTarget(target, drawing, drawingRows);
  const fit = target.inputs.length === 1 && target.outputs.length === 2
    ? trajectoryChart(selected, plotTarget, drawingRows, 'Current response')
    : target.outputs.map((output, index) => target.inputs.length === 2
      ? fieldChart(selected, target, index, 'Current response')
      : fitChart(predictions, selected.targets, index, output.name, plotTarget, drawingRows, 'Current response')).join('');
  const step = progress.evaluated_step ?? progress.step;
  const context = [Number.isInteger(progress.restart) ? `Initialization ${progress.restart}` : '',
    Number.isInteger(step) ? `Evaluated iteration ${step}` : '',
    Number.isInteger(progress.reactions) ? `${progress.reactions} reactions` : ''].filter(Boolean).join(' · ');
  return `<section class="inverse-live-preview inverse-result-section node-panel node-panel--chart" data-phase="${escape(progress.phase)}" data-step="${escape(step)}">
    <div class="node-panel-header"><span class="node-panel-title">Latest evaluated response</span><span class="text-dim">RMSD ${number(progress.rmse)}</span></div>
    <div class="node-panel-body">
    <p class="text-dim inverse-note">${escape(context)}</p>${fit}
    ${Array.isArray(progress.per_output_rmse) ? `<p class="inverse-note">RMSD per output: ${target.outputs.map((output, index) => `${escape(output.name)} ${number(progress.per_output_rmse[index])}`).join('; ')}.</p>` : ''}
    </div></section>`;
}

export function renderInverseDesignResult(result, request, drawing = '') {
  const selected = result.selected_network;
  if (selected?.status !== 'ok') return `<div class="node-error" role="status">${escape(selected?.reason || 'No selected network passed equilibrium replay.')}</div>`;
  const target = request.target;
  const drawingRows = targetDrawingRows(drawing, target);
  const plotTarget = drawingPlotTarget(target, drawing, drawingRows);
  const history = { samples: [], events: [], restart: null };
  for (const entry of Array.isArray(result.optimization_history) ? result.optimization_history : []) {
    if (!INVERSE_FIT_PHASES.has(entry?.phase)) continue;
    const rmse = progressRmse(entry);
    if (rmse === null) continue;
    history.samples.push({ x: history.samples.length + 1, y: rmse });
    if (Number.isInteger(entry.restart)) {
      if (Number.isInteger(history.restart) && entry.restart !== history.restart) {
        history.events.push({ x: history.samples.length, kind: 'restart' });
      }
      history.restart = entry.restart;
    }
  }
  const historyCurve = history.samples.length >= 2 ? renderLearningCurve(history.samples, history.events) : '';
  const summary = `<section class="inverse-result-section node-panel" aria-label="Result summary"><div class="node-panel-header"><span class="node-panel-title">Result summary</span></div>
    <div class="node-panel-body">${renderSearchTermination(result, request)}<div class="inverse-result-heading"><strong>${result.target_met ? 'Target met' : 'Target not met'}</strong>
    <span class="text-dim">${result.initial_reaction_count} → ${result.final_reaction_count} reactions</span></div>
    <div class="inverse-metrics"><div><span>Fit error (RMSD)</span><strong>${number(selected.rmse)}</strong></div>
      <div><span>Maximum RMSD per output</span><strong>${number(request.optimization.max_rmse)}</strong></div>
      <div><span>Training evaluations</span><strong>${target.samples.length}</strong></div></div>
    ${historyCurve}
    ${result.validation ? `<p class="inverse-note">Validation RMSD: ${number(result.validation.rmse)} across ${target.validation_samples.length} held-out samples. Per output: ${target.outputs.map((output, index) => `${escape(output.name)} ${number(result.validation.per_output_rmse?.[index])}`).join('; ')}.</p>` : ''}
    <p class="inverse-note">Per-output RMSD: ${target.outputs.map((output, index) => `${escape(output.name)} ${number(selected.per_output_rmse?.[index])}`).join('; ')}.</p>
    <p class="text-dim inverse-note">Supernetwork: ${result.initial_reaction_count} reactions. Designed network: ${result.final_reaction_count} reactions. Open the Designed Network output for its structure, Kd and concentrations.</p></div></section>`;
  const fit = (target.inputs.length === 1 && target.outputs.length === 2 ? trajectoryChart(selected, plotTarget, drawingRows) : '')
    + target.outputs.map((output, index) => target.inputs.length === 2
      ? fieldChart(selected, target, index)
      : fitChart(selected.predictions, selected.targets, index, output.name, plotTarget, drawingRows)).join('');
  const pruningRows = result.pruning_history.map((attempt, index) => `<tr><td>${index + 1}</td><td>${attempt.before} → ${attempt.after}</td><td>${attempt.accepted ? 'Accepted' : 'Rejected'}</td><td>${number(attempt.rmse)}</td><td>${escape(attempt.reason)}</td></tr>`).join('');
  const sampleRows = target.samples.map((sample, index) => `<tr><td>${index + 1}</td>
    <td>${target.inputs.map((input, dimension) => `${escape(input.name)}=${number(sample.inputs[dimension])}`).join(', ')}</td>
    <td>${selected.targets[index].map(number).join(', ')}</td><td>${selected.predictions[index].map(number).join(', ')}</td><td>${number(sample.weight)}</td></tr>`).join('');
  return `${summary}
    <section class="inverse-result-section inverse-fit-panel node-panel node-panel--chart"><div class="node-panel-header"><span class="node-panel-title">Fit against the target</span></div><div class="node-panel-body">${fit}</div></section>
    ${renderDesignTargetErrorFloor(target, request.optimization.max_rmse)}
    <section class="inverse-result-section node-panel"><div class="node-panel-header"><span class="node-panel-title">Pruning and refitting · ${result.pruning_history.filter(attempt => attempt.accepted).length} accepted / ${result.pruning_history.length} evaluated</span></div>
      ${pruningRows ? `<div class="node-panel-body node-panel-body--flush"><div class="inverse-table-scroll" tabindex="0" role="region" aria-label="Pruning evaluations"><table><thead><tr><th scope="col">Attempt</th><th scope="col">Reactions</th><th scope="col">Decision</th><th scope="col">RMSD</th><th scope="col">Reason</th></tr></thead><tbody>${pruningRows}</tbody></table></div></div>` : '<div class="node-panel-body"><p class="text-dim inverse-note">No pruning proposals were evaluated for this run.</p></div>'}</section>
    <section class="inverse-result-section node-panel"><div class="node-panel-header"><span class="node-panel-title">Final Kd, total concentrations, and readouts</span></div><div class="node-panel-body">${renderDesignedNetwork({ reactions: selected.rules, kds: selected.kd, totals: selected.totals, outputs: selected.outputs, target_met: result.target_met, model_handoff: selected.model_handoff })}</div></section>
    <section class="inverse-result-section node-panel"><div class="node-panel-header"><span class="node-panel-title">${target.samples.length} training evaluations</span></div><div class="node-panel-body node-panel-body--flush"><div class="inverse-table-scroll" tabindex="0" role="region" aria-label="Training evaluations"><table><thead><tr><th scope="col">Evaluation</th><th scope="col">Input totals</th><th scope="col">Target</th><th scope="col">Network response</th><th scope="col">Weight</th></tr></thead><tbody>${sampleRows}</tbody></table></div></div></section>
    <section class="inverse-result-section node-panel"><div class="node-panel-header"><span class="node-panel-title">Numerical checks</span></div><div class="node-panel-body"><p class="inverse-note">The selected network was evaluated after pruning. Its computed response does not replace or simplify the design target. Evaluation at the supplied inputs does not establish a globally minimal network.</p>${selected.physical_audit ? `<p class="inverse-note">Cold equilibrium solve: ${selected.physical_audit.training_samples} training evaluations and ${selected.physical_audit.validation_samples} validation evaluations. Maximum log₁₀ conservation residual: ${number(selected.physical_audit.max_log10_mass_residual)}. Maximum stepwise log₁₀ mass-action residual: ${number(selected.physical_audit.max_stepwise_log10_mass_action_residual)}.</p>` : ''}</div></section>
    ${result.warnings?.length ? `<div class="inverse-note" role="status">${result.warnings.map(escape).join('<br>')}</div>` : ''}`;
}
