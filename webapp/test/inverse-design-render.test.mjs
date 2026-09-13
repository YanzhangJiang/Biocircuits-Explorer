import assert from 'node:assert/strict';
import test from 'node:test';
import { makeTargetDrawing, targetDrawingRows } from '../public/js/design-target-drawing.js';

globalThis.window = {
  matchMedia: () => null,
  addEventListener() {},
  location: { protocol: 'http:', hostname: '127.0.0.1', port: '8000' },
  sessionStorage: { getItem() { return null; }, setItem() {} },
};
globalThis.document = {
  readyState: 'loading',
  documentElement: { dataset: {}, style: { setProperty() {} } },
  getElementById() { return null; },
  addEventListener() {},
  querySelectorAll() { return []; },
};
globalThis.HTMLSelectElement = class HTMLSelectElement {};

const { renderDesignedNetwork, renderInverseDesignResult, renderInverseDesignProgress, renderInverseDesignPreview, renderDesignTargetErrorFloor, inverseDesignCompletionStatus, renderLearningCurve, recordLearningSample, LEARNING_CURVE_CAPACITY } = await import('../public/js/inverse-design-render.js');

test('unmet results explain the actual search budget without claiming convergence or success', () => {
  const { result, request } = fixture();
  result.target_met = false;
  request.optimization = { epochs: 150, restarts: 2, prune_rounds: 3, max_rmse: 0.0001 };
  result.termination = { iterations: 1200, fit_stops: Array.from({ length: 8 }, () => ({ reason: 'iteration_limit' })) };
  let html = renderInverseDesignResult(result, request);
  assert.match(inverseDesignCompletionStatus(result), /budget exhausted; target not met/);
  assert.match(html, /150 iterations per fit · 2 initializations/);
  assert.match(html, /1200 parameter updates across 8 fits/);
  assert.match(html, /does not establish convergence/);
  assert.match(html, /data-action="reviewInverseDesignBudget"/);
  assert.doesNotMatch(html, /Learning complete/);
  result.termination.fit_stops[0].reason = 'invalid_equilibrium_update';
  html = renderInverseDesignResult(result, request);
  assert.match(html, /numerical update ended a fit early/);
  assert.match(html, /7 fits reached their iteration limit/);
});

test('conflicting values report a necessary error floor before a run without promising feasibility above it', () => {
  const t = { outputs: [{ name: '<img onerror=bad>' }], samples: [
    { inputs: [1], outputs: [0], weight: 1 }, { inputs: [1], outputs: [1], weight: 1 },
  ] };
  const before = structuredClone(t);
  const html = renderDesignTargetErrorFloor(t, 0.0001);
  assert.match(html, /Training RMSD lower bound/);
  assert.match(html, /≥ 0.5/);
  assert.match(html, /requested RMSD 0.0001 is below this bound/);
  assert.match(html, /original stroke and every target evaluation are preserved/);
  assert.doesNotMatch(html, /<img/);
  assert.deepEqual(t, before);
  const above = renderDesignTargetErrorFloor(t, 0.6);
  assert.doesNotMatch(above, /cannot reach|is below|feasible|Target met/);
});

test('learning progress reports evaluated metrics without pretending to know overall completion', () => {
  const waiting = renderInverseDesignProgress({ status: 'queued' });
  assert.match(waiting.status, /Queued/);
  assert.doesNotMatch(waiting.html, /<progress|NaN|undefined/);
  const state = { phase: 'prune_refit', step: 8, epochs: 20, restart: 2, restarts: 3,
    prune_round: 1, prune_rounds: 4, loss: 0.125, best_rmse: 0.4, reactions: 0, initial_reactions: 8 };
  const fitting = renderInverseDesignProgress({ status: 'running', progress: state });
  assert.match(fitting.status, /refitting after pruning/);
  assert.match(fitting.html, /8 \/ 20/);
  assert.match(fitting.html, /Training RMSD<\/span><strong>0.5/);
  assert.match(fitting.html, /8 → 0/);
  assert.match(fitting.html, /max="20" value="8"/);
  const check = renderInverseDesignProgress({ status: 'running', progress: { ...state, phase: 'checking_pruned' } });
  assert.doesNotMatch(check.html, /<progress/);
  const rejected = renderInverseDesignProgress({ status: 'running', progress: { phase: 'pruning_decision',
    last_pruning: { accepted: false, before: 8, after: 5 } } });
  assert.match(rejected.html, /8 → 5 rejected; retained 8 reactions/);
  const malicious = renderInverseDesignProgress({ status: 'running', progress: { phase: '<script>bad</script>', rmse: Infinity, step: '<img>' } });
  assert.doesNotMatch(malicious.html, /<script|<img|Infinity/);
});

function fixture() {
  const target = {
    inputs: [{ name: 'X', min: 0.1, max: 10, scale: 'log' }],
    outputs: [{ name: 'response', species: 'A', transform: 'linear', offset: 0 }],
    samples: [{ inputs: [0.1], outputs: [0.9], weight: 1 }, { inputs: [1], outputs: [0.5], weight: 1 }, { inputs: [10], outputs: [0.1], weight: 1 }],
  };
  return {
    request: { target, optimization: { max_rmse: 0.05 } },
    result: {
      target_met: true, initial_reaction_count: 8, final_reaction_count: 1,
      predictions: [[321]], fit_loss: 987,
      selected_network: {
        status: 'ok', rules: ['X + A <-> AX'], kd: [0.48], totals: { A: 1.1 }, outputs: target.outputs,
        rmse: 0.01, fit_loss: 0.0001, targets: [[0.9], [0.5], [0.1]], predictions: [[0.91], [0.51], [0.11]],
      },
      pruning_history: [{ before: 8, after: 1, accepted: true, rmse: 0.01, reason: 'Refit remained within tolerance.' },
        { before: 1, after: 0, accepted: false, rmse: 0.2, reason: 'Refit exceeded tolerance.' }],
      warnings: [],
    },
  };
}

test('the original target line is retained in fit comparisons without inventing denser solver predictions', () => {
  const { result, request } = fixture();
  const drawing = makeTargetDrawing(request.target, 'curve', [
    { x: 0, y: 0.9 }, { x: 0.25, y: 0.8 }, { x: 0.5, y: 0.5 },
    { x: 0.75, y: 0.25 }, { x: 1, y: 0.1 },
  ]);
  const html = renderInverseDesignResult(result, request, drawing);
  // Monotone cubic smoothing keeps one segment per sample interval (C) and
  // still passes through every supplied point; short curves keep segments (L).
  const vertices = name => html.match(new RegExp(`class="${name}" d="([^"]+)"`))[1].match(/[MLC]/g).length;
  assert.equal(vertices('inverse-target-curve'), 5);
  assert.equal(vertices('inverse-fit-curve'), 3);
  assert.doesNotMatch(html, /<circle|<rect/);
  const altered = structuredClone(request);
  altered.target.samples[0].outputs[0] = 0.2;
  const stale = renderInverseDesignResult(result, altered, drawing);
  assert.equal(stale.match(/class="inverse-target-curve" d="([^"]+)"/)[1].match(/[MLC]/g).length, 3);
});

test('binding original geometry to a numerical run preserves user-defined physical output ranges', () => {
  const { request } = fixture();
  const authored = structuredClone(request.target);
  authored.outputs[0].min = -2;
  authored.outputs[0].max = 6;
  const points = [{ x: 0.2, y: 0 }, { x: 0.3, y: 1 }, { x: 0.8, y: 0.5 }];
  const drawing = makeTargetDrawing(request.target, 'curve', points, authored);
  assert.deepEqual(targetDrawingRows(drawing, request.target).map(row => row.outputs), [[-2], [6], [2]]);
});

test('summary presents RMSD and the final response comparison immediately', () => {
  const { result, request } = fixture();
  const html = renderInverseDesignResult(result, request);
  assert.match(html, /Target met/);
  assert.match(html, /8 → 1 reactions/);
  assert.match(html, /Fit error \(RMSD\)/);
  assert.doesNotMatch(html, /Replayed network/);
  assert.match(html, /Supernetwork: 8 reactions\. Designed network: 1 reactions/);
  assert.match(html, /<section class="inverse-result-section inverse-fit-panel node-panel node-panel--chart">/);
  assert.match(html, /Fit against the target/);
  assert.doesNotMatch(html, /<details\b|<summary\b/);
  assert.match(html, /inverse-fit-curve/);
  assert.match(html, /1 accepted \/ 2 evaluated/);
  assert.match(html, /Accepted/);
  assert.match(html, /Rejected/);
  assert.match(html, /Refit exceeded tolerance/);
  assert.doesNotMatch(html, /987|321|Sparse objective|Candidate reactions/);
  assert.match(html, /does not establish a globally minimal network/);
});

test('live response curves and RMSD use one complete evaluation and never publish partial or nonfinite rows', () => {
  const { result, request } = fixture();
  const progress = { phase: 'fit', step: 25, restart: 1, reactions: 8, rmse: 0.01, predictions: result.selected_network.predictions };
  const preview = renderInverseDesignPreview(progress, request);
  assert.match(preview, /RMSD 0.01/);
  assert.match(preview, /Evaluated iteration 25/);
  assert.match(preview, /Your target/);
  assert.match(preview, /Current response/);
  assert.match(preview, /inverse-fit-curve/);
  assert.equal(renderInverseDesignPreview({ phase: 'selecting' }, request), null);
  for (const predictions of [[[1]], [[NaN], [0.5], [0.1]], [[1, 2], [0.5], [0.1]]]) {
    const invalid = renderInverseDesignPreview({ ...progress, predictions }, request);
    assert.match(invalid, /does not match/);
    assert.doesNotMatch(invalid, /<svg/);
  }
});

test('one-input charts use physical concentration positions and label logarithmic spacing', () => {
  const { result, request } = fixture();
  const html = renderInverseDesignResult(result, request);
  assert.match(html, /X total \(log scale\)/);
  assert.doesNotMatch(html, /inverse-target-point|inverse-fit-point/);
  const pathXs = rendered => [...rendered.match(/class="inverse-target-curve" d="([^"]+)"/)[1].matchAll(/[ML]([^,]+),/g)].map(match => Number(match[1]));
  const xs = pathXs(html);
  assert.equal(xs.length, 3);
  assert.ok(Math.abs((xs[0] + xs[2]) / 2 - xs[1]) < 1e-10);
  request.target.inputs[0].scale = 'linear';
  const linearXs = pathXs(renderInverseDesignResult(result, request));
  assert.ok(linearXs[1] < (linearXs[0] + linearXs[2]) / 2);
});

test('unmet tolerance is clearly labeled and does not erase a replayed network', () => {
  const { result, request } = fixture();
  result.target_met = false;
  result.selected_network.rmse = 0.2;
  const html = renderInverseDesignResult(result, request);
  assert.match(html, /Target not met/);
  assert.match(html, /0\.2/);
  assert.match(html, /<svg/);
});

test('failed replay displays its escaped reason without rendering unrelated fit evidence', () => {
  const { result, request } = fixture();
  result.selected_network = { status: 'invalid', reason: 'Equilibrium failed for <A>.' };
  const html = renderInverseDesignResult(result, request);
  assert.match(html, /Equilibrium failed for &lt;A&gt;/);
  assert.doesNotMatch(html, /<svg|987|321/);
});

test('extreme finite input/output values produce finite SVG coordinates', () => {
  const { result, request } = fixture();
  request.target.inputs[0] = { name: 'X', min: 1e-308, max: 1e308, scale: 'log' };
  request.target.samples.forEach((sample, i) => { sample.inputs[0] = [1e-308, 1, 1e308][i]; });
  result.selected_network.targets = [[1e308], [0], [-1e308]];
  result.selected_network.predictions = [[9e307], [0], [-9e307]];
  const html = renderInverseDesignResult(result, request);
  assert.match(html, /<svg/);
  assert.doesNotMatch(html, /NaN|Infinity/);
});

test('one-input two-output designs show their parametric output trajectory', () => {
  const { result, request } = fixture();
  request.target.outputs.push({ name: 'vertical', species: 'B', transform: 'log10', offset: -1 });
  result.selected_network.targets = [[0.9, 0.1], [0.5, 0.9], [0.1, 0.2]];
  result.selected_network.predictions = [[0.89, 0.11], [0.51, 0.88], [0.11, 0.19]];
  const html = renderInverseDesignResult(result, request);
  assert.match(html, /Output trajectory · ordered by X/);
  assert.match(html, /Two-output target and network response/);
  assert.equal((html.match(/<svg/g) || []).length, 3);
  assert.doesNotMatch(html, /NaN|Infinity/);
});

test('two-input targets can inspect target and network response fields using a common intensity scale', () => {
  const { result, request } = fixture();
  request.target.inputs.push({ name: 'Y', min: 0.1, max: 2, scale: 'linear' });
  request.target.samples.forEach((sample, index) => sample.inputs.push([0.1, 1, 2][index]));
  const html = renderInverseDesignResult(result, request);
  assert.match(html, /response field · vertical axis Y/);
  assert.match(html, /shared intensity 0\.1 to 0\.91/);
  assert.match(html, /Target and network response field/);
  assert.match(html, /X=1, Y=1; Network response 0\.51/);
  assert.doesNotMatch(html, /NaN|Infinity/);
});

test('fitted totals/readouts are shown, labels escaped, and saved results remain historical', () => {
  const html = renderDesignedNetwork({ reactions: ['A <img src=x onerror=evil()>'], kds: [0.123456], totals: { A: 1.23 },
    outputs: [{ name: '<script>', species: 'A', transform: 'log10', offset: 0.3 }] }, { historical: true });
  assert.doesNotMatch(html, /<img|<script>/);
  assert.match(html, /&lt;img/);
  assert.match(html, /&lt;script&gt; = log₁₀\(A\) \+ 0\.3/);
  assert.match(html, /0\.12346/);
  assert.match(html, /1\.23/);
  assert.match(html, /Saved parameters/);
  assert.match(html, /Run the connected workflow again/);
});

test('zero-reaction designs do not produce an empty reaction table or unusable-result warning', () => {
  const html = renderDesignedNetwork({ reactions: [], kds: [], totals: { A: 1 }, outputs: [], target_met: true });
  assert.match(html, /0 retained reactions/);
  assert.match(html, /requires no binding reactions/);
  assert.match(html, /Target met/);
  assert.doesNotMatch(html, /No usable network|<tbody><\/tbody>/);
});

test('live learning progress plots accumulated training RMSD without losing the existing metrics or progress bar', () => {
  const history = { evaluated: 0, samples: [], events: [], restart: null, pruneKey: '' };
  const job = step => ({ status: 'running', progress: { phase: 'fit', step, epochs: 20, restart: 1, restarts: 2, rmse: 1 / (step + 1), best_rmse: 1 / (step + 2), reactions: 8 } });
  recordLearningSample(history, job(0));
  recordLearningSample(history, job(1));
  recordLearningSample(history, job(2));
  const view = renderInverseDesignProgress(job(2), history);
  assert.match(view.html, /inverse-learning-curve/);
  assert.match(view.html, /inverse-learning-rmse/);
  assert.match(view.html, /Iteration in this fit/);
  assert.match(view.html, /Training RMSD<\/span><strong>0\.33333/);
  assert.match(view.html, /<progress class="inverse-learning-bar" max="20" value="2"/);
  assert.doesNotMatch(view.html, /NaN|undefined/);
  const bare = renderInverseDesignProgress(job(2));
  assert.doesNotMatch(bare.html, /inverse-learning-curve/);
});

test('learning samples skip non-fitting phases, mark restarts and pruning decisions once, and thin beyond capacity', () => {
  const history = { evaluated: 0, samples: [], events: [], restart: null, pruneKey: '' };
  assert.equal(recordLearningSample(history, { status: 'running', progress: { phase: 'constructing' } }), false);
  assert.equal(recordLearningSample(history, { status: 'running', progress: { phase: 'checking_fit', rmse: 0.5 } }), false);
  assert.equal(recordLearningSample(history, { status: 'running', progress: { phase: 'fit', step: 0, rmse: Number.NaN } }), false);
  assert.equal(history.samples.length, 0);
  recordLearningSample(history, { status: 'running', progress: { phase: 'fit', step: 0, restart: 1, rmse: 2 } });
  recordLearningSample(history, { status: 'running', progress: { phase: 'fit', step: 0, restart: 2, rmse: 1.5, last_pruning: { accepted: true, before: 8, after: 5 } } });
  recordLearningSample(history, { status: 'running', progress: { phase: 'prune_refit', step: 1, restart: 2, rmse: 1, last_pruning: { accepted: true, before: 8, after: 5 } } });
  assert.deepEqual(history.samples.map(sample => [sample.x, sample.y]), [[1, 2], [2, 1.5], [3, 1]]);
  assert.deepEqual(history.events, [{ x: 2, kind: 'restart' }, { x: 2, kind: 'prune' }], 'step resets never move the axis backwards');
  for (let i = 0; i < LEARNING_CURVE_CAPACITY + 10; i += 1) {
    recordLearningSample(history, { status: 'running', progress: { phase: 'fit', step: i, restart: 2, rmse: 0.5, last_pruning: { accepted: true, before: 8, after: 5 } } });
  }
  assert.ok(history.samples.length <= LEARNING_CURVE_CAPACITY);
  assert.equal(history.samples.at(-1).x, history.evaluated, 'thinning keeps the latest point');
  assert.ok(history.samples.every((sample, index) => index === 0 || sample.x > history.samples[index - 1].x), 'x stays monotone');
});

test('the learning curve renders adaptive log ticks, event markers and the latest value, and refuses empty or nonpositive data', () => {
  assert.equal(renderLearningCurve([]), '');
  assert.equal(renderLearningCurve([{ x: 1, y: 0 }, { x: 2, y: Number.NaN }]), '');
  const samples = [{ x: 1, y: 2 }, { x: 2, y: 0.5 }, { x: 3, y: 0.3 }, { x: 4, y: 0.2 }, { x: 5, y: 0.1 }];
  const html = renderLearningCurve(samples, [{ x: 3, kind: 'restart' }, { x: 4, kind: 'prune' }]);
  assert.match(html, /Training RMSD/);
  assert.match(html, /<strong>0\.1<\/strong>/);
  assert.match(html, /logarithmic vertical axis/);
  assert.equal((html.match(/class="inverse-learning-event"/g) || []).length, 2);
  for (const tick of ['0.1', '0.2', '0.5', '1', '2']) assert.match(html, new RegExp(`>${tick}</text>`));
  assert.doesNotMatch(html, /NaN|undefined/);
});

test('smoothed single-input response curves never overshoot the supplied sample range', () => {
  const { result, request } = fixture();
  request.target.samples = [0.1, 0.3, 1, 3, 10].map((value, index) => ({ inputs: [value], outputs: [[0.9, 0.2, 0.8, 0.1, 0.5][index]], weight: 1 }));
  result.selected_network.targets = request.target.samples.map(sample => sample.outputs.slice());
  result.selected_network.predictions = [[0.85], [0.25], [0.75], [0.15], [0.45]];
  const html = renderInverseDesignResult(result, request);
  const d = html.match(/class="inverse-fit-curve" d="([^"]+)"/)[1];
  assert.match(d, /C/, 'five ordered samples are smoothed with cubic segments');
  const chunks = [...d.matchAll(/[MLC]([^MLC]+)/g)].map(match => match[1].trim().split(/\s+/).map(token => token.split(',').map(Number)));
  const anchors = chunks.map(tokens => tokens.at(-1));
  const minY = Math.min(...anchors.map(point => point[1])), maxY = Math.max(...anchors.map(point => point[1]));
  for (const point of chunks.flat()) {
    assert.ok(point.every(Number.isFinite), `finite coordinate ${point}`);
    assert.ok(point[1] >= minY - 1e-6 && point[1] <= maxY + 1e-6, `smoothed coordinate ${point} stays within the sample span`);
  }
});

test('parametric trajectories smooth only a monotone horizontal output', () => {
  const { result, request } = fixture();
  request.target.outputs.push({ name: 'vertical', species: 'B', transform: 'linear', offset: 0 });
  const horizontal = [0.2, 0.8, 0.3, 0.9];
  request.target.samples = [0.1, 1, 3, 10].map((value, index) => ({ inputs: [value], outputs: [horizontal[index], [0.1, 0.5, 0.7, 0.2][index]], weight: 1 }));
  result.selected_network.targets = request.target.samples.map(sample => sample.outputs.slice());
  result.selected_network.predictions = request.target.samples.map(sample => [sample.outputs[0] + 0.01, sample.outputs[1]]);
  const folding = renderInverseDesignResult(result, request).match(/class="inverse-fit-curve" d="([^"]+)"/)[1];
  assert.doesNotMatch(folding, /C/, 'a folding trajectory keeps straight segments');
  const monotone = [0.1, 0.3, 0.6, 0.9];
  request.target.samples.forEach((sample, index) => { sample.outputs[0] = monotone[index]; });
  result.selected_network.targets = request.target.samples.map(sample => sample.outputs.slice());
  result.selected_network.predictions = request.target.samples.map(sample => [sample.outputs[0], sample.outputs[1]]);
  const smoothed = renderInverseDesignResult(result, request).match(/class="inverse-fit-curve" d="([^"]+)"/)[1];
  assert.match(smoothed, /C/, 'a monotone trajectory is smoothed');
});

test('the result panel plots the recorded optimization history only when it carries evaluated RMSD values', () => {
  const { result, request } = fixture();
  assert.doesNotMatch(renderInverseDesignResult(result, request), /inverse-learning-rmse/);
  result.optimization_history = [
    { phase: 'fit', step: 0, restart: 1, rmse: 2 }, { phase: 'fit', step: 5, restart: 1, rmse: 0.5 },
    { phase: 'fit', step: 10, restart: 1, rmse: 0.2 }, { phase: 'prune_refit', step: 0, restart: 1, prune_round: 1, rmse: 0.25 },
    { phase: 'fit', step: 0, restart: 2, rmse: 1 }, { phase: 'fit', step: 5, restart: 2, rmse: 0.1 },
  ];
  const html = renderInverseDesignResult(result, request);
  assert.match(html, /inverse-learning-rmse/);
  assert.match(html, /New initialization/);
  assert.doesNotMatch(html, /NaN|undefined/);
  result.optimization_history = [{ phase: 'constructing' }, { phase: 'fit', step: 0, rmse: 2 }, { phase: 'checking_fit' }];
  assert.doesNotMatch(renderInverseDesignResult(result, request), /inverse-learning-rmse/);
});
