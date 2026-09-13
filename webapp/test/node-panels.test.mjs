import assert from 'node:assert/strict';
import test from 'node:test';

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

const { NODE_TYPES } = await import('../public/js/node-types/index.js');
const { renderInverseDesignResult, renderInverseDesignPreview } = await import('../public/js/inverse-design-render.js');

// Every figure and table section must live inside an inset panel so charts and
// previews read as framed regions, distinct from the node's text/config flow.
function sections(html) {
  // A zero-width lookahead never splits at index 0, so the preamble (if any)
  // is the only chunk not starting with <section.
  return html.split(/(?=<section\b)/).filter(chunk => chunk.startsWith('<section'));
}

function assertChartsPanelled(html) {
  const chunks = sections(html);
  assert.ok(chunks.length > 0, 'expected <section> panels in the rendered markup');
  const preamble = html.slice(0, html.indexOf('<section'));
  assert.equal((preamble.match(/inverse-fit-figure/g) || []).length, 0, 'figure outside any panel');
  let figures = 0;
  for (const chunk of chunks) {
    const found = (chunk.match(/inverse-fit-figure/g) || []).length;
    if (!found) continue;
    figures += found;
    assert.match(chunk, /^<section class="[^"]*\bnode-panel--chart\b/, `figure outside a chart panel: ${chunk.slice(0, 120)}`);
  }
  assert.ok(figures > 0, 'expected at least one .inverse-fit-figure to check');
}

function assertScalableSvgs(html) {
  const tags = html.match(/<svg\b[^>]*>/g) || [];
  assert.ok(tags.length > 0, 'expected at least one chart SVG');
  for (const tag of tags) {
    assert.match(tag, /\bviewBox="/, `SVG lost its viewBox: ${tag}`);
    assert.doesNotMatch(tag, /\b(?:width|height)="\d/, `SVG must not pin pixel width/height: ${tag}`);
  }
}

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
      selected_network: {
        status: 'ok', rules: ['X + A <-> AX'], kd: [0.48], totals: { A: 1.1 }, outputs: target.outputs,
        rmse: 0.01, fit_loss: 0.0001, targets: [[0.9], [0.5], [0.1]], predictions: [[0.91], [0.51], [0.11]],
      },
      pruning_history: [{ before: 8, after: 1, accepted: true, rmse: 0.01, reason: 'Refit remained within tolerance.' }],
      warnings: [],
    },
  };
}

test('the inverse-design-target body frames its editor sections as panels without altering control contracts', () => {
  const html = NODE_TYPES['inverse-design-target'].createBody('np1');
  assert.equal((html.match(/class="node-panel[ "]/g) || []).length, 5);
  for (const title of ['Target preview', 'Numerical target · all dimensions and samples', 'Network design constraints']) {
    assert.ok(html.includes(`node-panel-title">${title}`), title);
  }
  assert.match(html, /<label class="node-panel-title" for="np1-description">Design goal<\/label>/);
  assert.match(html, /<label class="node-panel-title" for="np1-target-mode">Target entrance<\/label>/);
  assert.match(html, /class="node-panel node-panel--chart design-target-plot-panel"/);
  assert.doesNotMatch(html, /viewer-content design-target-plot-panel|design-target-plot-panel[^"]*viewer-content/);
  for (const suffix of ['description', 'target-mode', 'reference-state', 'drawing-json', 'target-coordinates', 'target-drawing', 'target-canvas',
    'target-preview', 'target-editor-status', 'target-image', 'target-image-file', 'image-resolution-row', 'image-invert-row', 'target-roles',
    'target-points-row', 'target-json', 'aux-monomers', 'max-complex-size', 'max-reactions', 'allow-homomers', 'chemistry-json', 'content']) {
    assert.ok(html.includes(`id="np1-${suffix}"`), suffix);
  }
  for (const action of ['compileInverseDesignTarget', 'refreshInverseDesignTarget', 'applyInverseDesignPreset', 'clearInverseDesignDrawing', 'prepareInverseDesignRequest']) {
    assert.ok(html.includes(`data-action="${action}"`), action);
  }
  assertScalableSvgs(html);
});

test('the gradient-design body panels its config sections and learning progress while keeping the results viewer flexible', () => {
  const html = NODE_TYPES['gradient-design'].createBody('np2');
  for (const title of ['Parameter optimization', 'Pruning and target tolerance', 'Learning']) {
    assert.ok(html.includes(`node-panel-title">${title}`), title);
  }
  assert.match(html, /id="np2-learning" class="node-panel node-panel--chart inverse-learning" hidden>/);
  assert.match(html, /<div class="node-panel-header"><span class="node-panel-title">Learning<\/span><span id="np2-learning-clock"/);
  assert.match(html, /id="np2-learning-status" class="inverse-learning-status" role="status"/);
  assert.match(html, /<div class="node-panel-body">\s*<div id="np2-learning-status"/);
  assert.ok(html.includes('id="np2-learning-metrics"'));
  assert.match(html, /id="np2-content" class="viewer-content inverse-design-viewer"/);
  for (const suffix of ['learning-rate', 'epochs', 'restarts', 'optimize-totals', 'seed', 'prune-rounds', 'prune-fraction', 'prune-tolerance', 'max-rmse']) {
    assert.ok(html.includes(`id="np2-${suffix}"`), suffix);
  }
  assert.ok(html.includes('id="np2-run-design" class="btn btn-run" data-action="executeGradientDesign"'));
  assert.ok(html.includes('id="np2-stop-design" class="btn btn-small" data-action="cancelGradientDesign"'));
});

test('every fit figure in the gradient result sits inside a chart panel and charts stay scalable', () => {
  const { result, request } = fixture();
  const html = renderInverseDesignResult(result, request);
  assertChartsPanelled(html);
  assertScalableSvgs(html);
  for (const title of ['Result summary', 'Fit against the target', 'Pruning and refitting · 1 accepted / 1 evaluated',
    'Final Kd, total concentrations, and readouts', '3 training evaluations', 'Numerical checks']) {
    assert.ok(html.includes(`node-panel-title">${title}`), title);
  }
  assert.equal((html.match(/node-panel-body--flush/g) || []).length, 2, 'both table panels use the flush scroll body');
  const twoOutput = structuredClone(request);
  twoOutput.target.outputs.push({ name: 'vertical', species: 'B', transform: 'linear', offset: 0 });
  twoOutput.target.samples = twoOutput.target.samples.map((sample, index) => ({ ...sample, outputs: [sample.outputs[0], [0.2, 0.6, 0.1][index]] }));
  const trajectoryResult = structuredClone(result);
  trajectoryResult.selected_network.targets = twoOutput.target.samples.map(sample => sample.outputs.slice());
  trajectoryResult.selected_network.predictions = twoOutput.target.samples.map(sample => [sample.outputs[0], sample.outputs[1]]);
  const multi = renderInverseDesignResult(trajectoryResult, twoOutput);
  assertChartsPanelled(multi);
  assertScalableSvgs(multi);
});

test('the live evaluated response preview is a chart panel with kept progress attributes', () => {
  const { result, request } = fixture();
  const progress = { phase: 'fit', step: 25, restart: 1, reactions: 8, rmse: 0.01, predictions: result.selected_network.predictions };
  const html = renderInverseDesignPreview(progress, request);
  assert.match(html, /^<section class="inverse-live-preview inverse-result-section node-panel node-panel--chart"/);
  assert.match(html, /data-phase="fit" data-step="25"/);
  assert.match(html, /node-panel-title">Latest evaluated response<\/span><span class="text-dim">RMSD 0.01/);
  assertChartsPanelled(html);
  assertScalableSvgs(html);
});
