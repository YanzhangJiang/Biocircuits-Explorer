import AxeBuilder from '@axe-core/playwright';
import { expect, test } from '@playwright/test';
import { DEFAULT_TARGET } from '../public/js/design-target-adapters.js';
import { TARGET_PLOT } from '../public/js/design-target-coordinates.js';

const types = ['inverse-design-target', 'gradient-design', 'designed-network'];
const fittedKd = [0.8, 0.45];
const fittedTotals = { A: 0.37, B: 0.61 };
const fittedRules = ['X + A <-> AX', 'X + B <-> BX'];
const copy = value => JSON.parse(JSON.stringify(value));

// Synthetic transport fixtures verify authoring, ownership and handoff. They
// provide no evidence about chemical feasibility or scientific accuracy.
function fittedResponse(request) {
  const targets = request.target.samples.map(sample => sample.outputs);
  return {
    status: 'ok', target_met: true, target: request.target,
    initial_reaction_count: 6, final_reaction_count: 2,
    selected_network: {
      status: 'ok', rules: fittedRules, kd: fittedKd, totals: fittedTotals,
      outputs: request.target.outputs, predictions: targets, targets, rmse: 0, fit_loss: 0, per_output_rmse: request.target.outputs.map(() => 0),
      network_ir: { schema_version: 'bne-network-ir/v1.0.0', reactions: fittedRules.map((formula, index) => ({ formula, kd: fittedKd[index] })) },
      evidence_tier: 'sampled_equilibrium_replay', prediction_basis: 'selected_network',
      physical_audit: { cold_replay: true, max_log10_mass_residual: 0, max_stepwise_log10_mass_action_residual: 0, training_samples: targets.length, validation_samples: request.target.validation_samples?.length ?? 0 },
    },
    pruning_history: [
      { before: 6, after: 3, accepted: true, rmse: 0.001, reason: 'Mock low-occupancy branch refitted.' },
      { before: 3, after: 2, accepted: true, rmse: 0, reason: 'Mock precursor-safe deletion refitted.' },
      { before: 2, after: 1, accepted: false, rmse: 0.2, reason: 'Mock replay exceeds pruning tolerance.' },
    ],
    optimization_history: [{ phase: 'initial_fit', step: 1, loss: 0.1 }, { phase: 'prune_refit', step: 2, loss: 0 }],
    algorithm: { implementation: 'browser-transport-fixture' }, warnings: [],
    ...(request.target.validation_samples?.length ? { validation: {
      rmse: 0, per_output_rmse: request.target.outputs.map(() => 0), targets: request.target.validation_samples.map(sample => sample.outputs),
      predictions: request.target.validation_samples.map(sample => sample.outputs),
    } } : {}),
  };
}
function compiledResponse() {
  const target = copy(DEFAULT_TARGET);
  target.description = 'An editable triangular response.';
  target.source = 'agent';
  target.inputs = [{ name: 'X', min: 0.2, max: 4, scale: 'linear' }];
  target.samples = [
    { inputs: [0.2], outputs: [0], weight: 1 },
    { inputs: [2.1], outputs: [1], weight: 2 },
    { inputs: [4], outputs: [0], weight: 1 },
  ];
  return { target, chemistry: { auxiliary_monomers: 2, max_complex_size: 2, max_reactions: 24, allow_homomers: false },
    interpretation: 'A triangular response with an emphasized middle sample.', warnings: ['Review the editable target before optimization.'] };
}

async function openInverseDesign(page, { chain = true, delayedSubmission = false, holdJob = false, delayedCompiler = false, resultForRequest = fittedResponse } = {}) {
  const requests = [], builds = [], cancellations = [], jobReads = [], compilerRequests = [], errors = [];
  const jobs = new Map();
  const jobStates = new Map();
  let pendingSubmission = null, pendingCompile = null;
  page.on('pageerror', error => errors.push(error.message));
  await page.addInitScript(() => {
    localStorage.setItem('bcx-node-view', 'workspace');
    localStorage.setItem('biocircuits-explorer.theme-mode', 'light');
  });
  await page.route('**/health', route => route.fulfill({ json: { ok: true, engine: { ready: true } } }));
  await page.route('**/compile-target', async route => {
    if (route.request().method() === 'OPTIONS') return route.fulfill({ status: 204, headers: {
      'access-control-allow-origin': 'http://127.0.0.1:4173',
      'access-control-allow-methods': 'POST', 'access-control-allow-headers': 'authorization,content-type',
    } });
    compilerRequests.push({ body: route.request().postDataJSON(), headers: route.request().headers() });
    const finish = () => route.fulfill({ json: compiledResponse(), headers: { 'access-control-allow-origin': 'http://127.0.0.1:4173' } });
    if (delayedCompiler) pendingCompile = finish;
    else await finish();
  });
  await page.route('**/api/v1/**', async route => {
    const path = new URL(route.request().url()).pathname;
    const data = route.request().postDataJSON();
    if (path.endsWith('/design_network')) {
      requests.push(data);
      const job_id = `inverse-browser-job-${requests.length}`;
      jobs.set(job_id, data);
      jobStates.set(job_id, { status: holdJob ? 'running' : 'succeeded', phase: 'prune_refit' });
      const finish = () => route.fulfill({ status: 202, json: { job_id, status: 'queued', phase: 'initial_fit' } });
      if (delayedSubmission) pendingSubmission = finish;
      else await finish();
    } else if (path.includes('/jobs/')) {
      const [, job_id, suffix = ''] = path.match(/\/jobs\/([^/]+)(?:\/(\w+))?$/);
      expect(jobs.has(job_id)).toBe(true);
      if (suffix === 'cancel') {
        cancellations.push(job_id);
        jobStates.set(job_id, { status: 'cancelled' });
        await route.fulfill({ json: { job_id, status: 'cancelled' } });
      } else if (suffix === 'result') {
        await route.fulfill({ json: { job: { job_id, status: 'succeeded' }, result: resultForRequest(jobs.get(job_id)) } });
      } else {
        jobReads.push(job_id);
        await route.fulfill({ json: { job_id, ...jobStates.get(job_id) } });
      }
    } else if (path.endsWith('/build_model')) {
      builds.push(data);
      await route.fulfill({ json: { session_id: 'inverse-e2e', network_ir_hash: 'a'.repeat(64), n: 5, d: 3, r: 2,
        x_sym: ['X', 'A', 'B', 'AX', 'BX'], q_sym: ['tX', 'tA', 'tB'], K_sym: ['K_AX', 'K_BX'] } });
    } else {
      const json = path.endsWith('/debug/logs') ? { entries: [], next_seq: 0 } : { version: 'local-fixture' };
      await route.fulfill({ json });
    }
  });
  await page.goto('/index-node.html#workspace');
  await page.waitForFunction(() => !!window.BiocircuitsExplorerWorkspaceShell);
  if (chain) {
    await page.getByRole('button', { name: 'Quick Add', exact: true }).click();
    await page.locator('#legacy-nodes-menu [data-type="inverse-design"]').click();
    await expect(page.locator('#canvas > .node')).toHaveCount(3);
  }
  return { requests, builds, cancellations, jobReads, compilerRequests, errors,
    publishProgress: progress => jobStates.set(`inverse-browser-job-${requests.length}`, { status: 'running', progress }),
    finishJob: (error = null) => jobStates.set(`inverse-browser-job-${requests.length}`, error ? { status: 'failed', error } : { status: 'succeeded' }),
    finishSubmission: async () => { await expect.poll(() => !!pendingSubmission).toBe(true); await pendingSubmission(); },
    finishCompile: async () => { await expect.poll(() => !!pendingCompile).toBe(true); await pendingCompile(); } };
}
async function snapshot(page) {
  return page.evaluate(() => JSON.parse(window.BiocircuitsExplorerWorkspaceShell.serializeWorkspace()));
}
const targetNode = page => page.locator('.node[data-node-type="inverse-design-target"]');
const control = (page, suffix) => targetNode(page).locator(`[id$="-${suffix}"]`);
async function targetValue(page) { return JSON.parse(await control(page, 'target-json').inputValue()); }
async function runConnected(page) {
  await targetNode(page).locator('.node-header').click();
  return page.evaluate(() => window.BiocircuitsExplorerWorkspaceShell.runConnectedWorkspace());
}
async function startConnected(page) {
  await targetNode(page).locator('.node-header').click();
  await page.evaluate(() => { window.__inverseRun = window.BiocircuitsExplorerWorkspaceShell.runConnectedWorkspace(); });
}
async function editRole(page, selector, value) {
  const field = control(page, 'target-roles').locator(selector);
  if (await field.evaluate(element => element.tagName) === 'SELECT') await field.selectOption(value);
  else { await field.fill(value); await field.press('Tab'); }
}
async function draw(page, points) {
  const canvas = control(page, 'target-canvas');
  await canvas.scrollIntoViewIfNeeded();
  const box = await canvas.boundingBox();
  const position = ([x, y]) => ({ x: box.x + box.width * (TARGET_PLOT.left + x * TARGET_PLOT.dx) / TARGET_PLOT.width,
    y: box.y + box.height * (TARGET_PLOT.top + (1 - y) * TARGET_PLOT.dy) / TARGET_PLOT.height });
  const first = position(points[0]);
  await page.mouse.move(first.x, first.y); await page.mouse.down();
  for (const point of points.slice(1)) {
    const next = position(point);
    await page.mouse.move(next.x, next.y, { steps: 5 });
  }
  await page.mouse.up();
}
async function expectNoCurrentOutput(page) {
  const output = (await snapshot(page)).nodes.find(node => node.type === 'designed-network');
  expect(output.data.lifecycle?.freshness).not.toBe('current');
  expect(await page.evaluate(async id => (await import('/js/model.js')).getReactionsFromNode(id), output.id)).toEqual({ reactions: [], kds: [] });
}

test('target-driven job workflow is atomic, records pruning and forwards fitted Kd, totals and readouts', async ({ page }) => {
  const app = await openInverseDesign(page);
  const original = await snapshot(page);
  expect(original.nodes.map(node => node.type)).toEqual(types);
  expect(original.connections).toHaveLength(2);
  await expect(targetNode(page)).not.toContainText('Candidate reactions');
  expect(app.requests).toHaveLength(0);
  await page.keyboard.press('Control+z');
  await expect(page.locator('#canvas > .node')).toHaveCount(0);
  await page.keyboard.press('Control+Shift+z');
  await expect(page.locator('#canvas > .node')).toHaveCount(3);
  expect((await snapshot(page)).nodes.map(node => node.id)).toEqual(original.nodes.map(node => node.id));
  const report = await runConnected(page);
  expect(report.summary.failed).toBe(0);
  expect(report.summary.blocked).toBe(0);
  expect(app.requests).toHaveLength(1);
  expect(app.jobReads).toEqual(['inverse-browser-job-1']);
  expect(app.requests[0]).toMatchObject({ target: { schema_version: 'bne-design-target/v1.0.0' }, optimization: { optimize_totals: true, prune_rounds: 3 } });
  expect(app.requests[0].reactions).toBeUndefined();
  const saved = await snapshot(page);
  const output = saved.nodes.find(node => node.type === 'designed-network');
  expect(output.data.designedNetwork).toMatchObject({ kds: fittedKd, totals: fittedTotals, outputs: app.requests[0].target.outputs });
  expect(output.data.lifecycle.freshness).toBe('current');
  await expect(page.getByText('Pruning and refitting · 2 accepted / 3 evaluated', { exact: true })).toBeVisible();
  await page.evaluate(id => window.addQuickAddChain('siso-analysis', { selectedSourceId: id }), output.id);
  await expect.poll(() => app.builds.length).toBe(1);
  expect(app.builds[0].network.reactions.map(reaction => reaction.kd)).toEqual(fittedKd);
  const model = (await snapshot(page)).nodes.find(node => node.type === 'model-builder');
  const context = await page.evaluate(async id => (await import('/js/nodes.js')).getModelContextFromBuilder(id), model.id);
  expect(context).toMatchObject({ totals: fittedTotals, outputs: app.requests[0].target.outputs, parameterDefaults: { tA: 0.37, tB: 0.61 } });
  await page.evaluate(document => window.BiocircuitsExplorerWorkspaceShell.applyWorkspaceFromJSONString(JSON.stringify(document)), saved);
  expect((await snapshot(page)).nodes.find(node => node.type === 'designed-network').data.lifecycle.freshness).toBe('historical');
  await expectNoCurrentOutput(page);
  // A restore stages hidden nodes before publication. All restored actions
  // must regain accessibility visibility and pointer hit-testing afterwards.
  for (const name of ['Compile with Design Agent', 'Saturating example', 'Clear drawing', 'Validate target', 'Design and prune network', 'Extract designed network']) {
    const button = page.getByRole('button', { name, exact: true });
    await expect(button).toBeVisible();
    await button.click({ trial: true });
  }
  await expect(page.getByRole('button', { name: 'Stop', exact: true })).toBeVisible();
  await expect(page.getByRole('button', { name: 'Stop', exact: true })).toBeDisabled();
  await page.getByRole('button', { name: 'Validate target', exact: true }).click();
  await expect(control(page, 'content')).toContainText('Target ready.');
  await page.getByRole('button', { name: 'Design and prune network', exact: true }).click();
  await expect(page.locator('.node[data-node-type="gradient-design"] [id$="-content"]')).toContainText('Target met');
  await page.getByRole('button', { name: 'Extract designed network', exact: true }).click();
  expect((await snapshot(page)).nodes.find(node => node.type === 'designed-network').data.lifecycle.freshness).toBe('current');
  expect(app.requests).toHaveLength(2);
  expect(app.errors).toEqual([]);
});

test('learning responds before submission, displays real fit and pruning progress, and Stop is clickable', async ({ page }) => {
  const app = await openInverseDesign(page, { delayedSubmission: true, holdJob: true });
  const gradient = page.locator('.node[data-node-type="gradient-design"]');
  const run = gradient.locator('[data-action="executeGradientDesign"]');
  const stop = gradient.getByRole('button', { name: 'Stop', exact: true });
  const panel = gradient.locator('.inverse-learning');
  const status = panel.getByRole('status');
  const original = await targetValue(page);
  await run.click();
  await expect(status).toContainText('Starting learning');
  await expect(run).toBeDisabled();
  await expect(stop).toBeEnabled();
  await expect(gradient).toHaveClass(/loading/);
  await expect(panel.locator('[id$="-learning-clock"]')).toContainText(/Elapsed [1-9]\d*s/);
  await app.finishSubmission();
  const progress = { phase: 'fit', step: 20, epochs: 100, restart: 1, restarts: 2, prune_round: 0, prune_rounds: 3,
    rmse: 0.3, best_rmse: 0.25, reactions: 16, initial_reactions: 16 };
  app.publishProgress(progress);
  await expect(status).toHaveText('Learning — optimizing the supernetwork');
  await expect(panel.locator('progress')).toHaveAttribute('value', '20');
  await expect(panel).toContainText('20 / 100');
  await expect(panel).toContainText('0.3');
  app.publishProgress({ ...progress, step: 42, rmse: 0.14, best_rmse: 0.12 });
  await expect(panel.locator('progress')).toHaveAttribute('value', '42');
  await expect(panel).toContainText('0.14');
  app.publishProgress({ ...progress, phase: 'prune_refit', step: 7, prune_round: 2, reactions: 9, rmse: 0.13, best_rmse: 0.11,
    last_pruning: { round: 1, before: 16, after: 12, accepted: true } });
  await expect(status).toHaveText('Learning — refitting after pruning');
  await expect(panel).toContainText('2 / 3');
  await expect(panel).toContainText('16 → 9');
  await expect(panel).toContainText('16 → 12 reactions kept after refitting');
  await expect(panel.locator('progress')).toHaveAttribute('value', '7');
  expect(await targetValue(page)).toEqual(original);
  const accessibility = await new AxeBuilder({ page }).include('.node[data-node-type="gradient-design"]').analyze();
  expect(accessibility.violations).toEqual([]);
  await stop.click();
  await expect.poll(() => app.cancellations).toEqual(['inverse-browser-job-1']);
  await expect(status).toHaveText('Learning stopped.');
  await expect(run).toHaveText('Design and prune network');
  await expect(run).toBeEnabled();
  await expect(stop).toBeDisabled();
  await expect(gradient).not.toHaveClass(/loading/);
  const stoppedClock = await panel.locator('[id$="-learning-clock"]').textContent();
  await page.waitForTimeout(1100);
  await expect(panel.locator('[id$="-learning-clock"]')).toHaveText(stoppedClock);
  await expectNoCurrentOutput(page);
  expect(app.errors).toEqual([]);
});

test('an unmet search exposes its stop reason, target error floor and editable budget without altering the target', async ({ page }) => {
  const app = await openInverseDesign(page, { resultForRequest: request => {
    const result = fittedResponse(request);
    result.target_met = false;
    Object.assign(result.selected_network, { predictions: [[0.5], [0.5]], rmse: 0.5, fit_loss: 0.125, per_output_rmse: [0.5] });
    result.termination = { reason: 'search_budget_exhausted', epochs_per_fit: 150, initializations: 2,
      pruning_round_limit: 3, iterations: 150,
      fit_stops: [{ phase: 'fit', restart: 1, prune_round: 0, reason: 'iteration_limit', iterations: 150, best_step: 40 }] };
    return result;
  } });
  const target = copy(DEFAULT_TARGET);
  target.source = 'data';
  target.samples = [{ inputs: [1], outputs: [0], weight: 1 }, { inputs: [1], outputs: [1], weight: 1 }];
  await control(page, 'target-mode').selectOption('data');
  await control(page, 'target-json').fill(JSON.stringify(target, null, 2));
  await control(page, 'target-json').press('Tab');
  const original = await targetValue(page);
  const gradient = page.locator('.node[data-node-type="gradient-design"]');
  await gradient.locator('[id$="-max-rmse"]').fill('0.0001');
  await gradient.locator('[id$="-max-rmse"]').press('Tab');
  await gradient.locator('[data-action="executeGradientDesign"]').click();
  await expect(gradient.locator('[id$="-learning-status"]')).toHaveText('Search stopped — budget exhausted; target not met.');
  await expect(gradient.locator('.inverse-search-termination')).toContainText('150 iterations per fit · 2 initializations');
  await expect(gradient.locator('.inverse-target-error-floor')).toContainText('Training RMSD lower bound: response ≥ 0.5');
  await expect(gradient.locator('.inverse-target-error-floor')).toContainText('requested RMSD 0.0001 is below this bound');
  await expect(gradient.locator('.inverse-fit-chart')).toBeVisible();
  await gradient.getByRole('button', { name: 'Adjust search budget' }).click();
  await expect(gradient.locator('[id$="-epochs"]')).toBeFocused();
  expect(await targetValue(page)).toEqual(original);
  expect(app.requests).toHaveLength(1);
  expect(app.errors).toEqual([]);
});

test('learning failures and completion settle the visible status and restored workspaces do not keep a live timer', async ({ page }) => {
  const app = await openInverseDesign(page, { holdJob: true });
  const gradient = page.locator('.node[data-node-type="gradient-design"]');
  const run = gradient.locator('[data-action="executeGradientDesign"]');
  const panel = gradient.locator('.inverse-learning');
  await run.click();
  await expect(run).toBeDisabled();
  app.finishJob('Numerical test failure');
  await expect(panel.getByRole('status')).toContainText('Learning failed');
  await expect(gradient.locator('[id$="-content"]')).toContainText('Numerical test failure');
  await expect(run).toBeEnabled();
  await run.click();
  await expect.poll(() => app.requests.length).toBe(2);
  app.publishProgress({ phase: 'fit', step: 80, epochs: 100, rmse: 0.02, best_rmse: 0.015 });
  await expect(panel.locator('progress')).toHaveAttribute('value', '80');
  app.finishJob();
  await expect(panel.getByRole('status')).toHaveText('Learning complete — target met.');
  await expect(panel.locator('[id$="-learning-metrics"]')).toBeHidden();
  await expect(run).toBeEnabled();
  const saved = await snapshot(page);
  expect(JSON.stringify(saved)).not.toContain('_inverseDesignProgress');
  await page.evaluate(document => window.BiocircuitsExplorerWorkspaceShell.applyWorkspaceFromJSONString(JSON.stringify(document)), saved);
  await expect(gradient.locator('.inverse-learning')).toBeHidden();
  await expect(gradient).not.toHaveClass(/loading/);
  expect(app.errors).toEqual([]);
});

test('live RMSD and the current response update together while the authored target stays fixed', async ({ page }) => {
  const app = await openInverseDesign(page, { holdJob: true });
  await draw(page, [[0.1, 0], [0.3, 0.8], [0.6, 0.2], [0.9, 1]]);
  const original = await targetValue(page), drawing = await control(page, 'drawing-json').inputValue();
  const gradient = page.locator('.node[data-node-type="gradient-design"]');
  await gradient.locator('[data-action="executeGradientDesign"]').click();
  await expect.poll(() => app.requests.length).toBe(1);
  const frame = (scale, phase, step) => {
    const target = app.requests[0].target;
    const predictions = target.samples.map(row => row.outputs.map(value => value * scale));
    const weight = target.samples.reduce((sum, row) => sum + row.weight, 0);
    const rmse = Math.sqrt(target.samples.reduce((sum, row, i) => sum + row.weight * (predictions[i][0] - row.outputs[0]) ** 2, 0) / weight);
    return { phase, step, epochs: 100, restart: 1, restarts: 2, prune_round: phase === 'fit' ? 0 : 1, prune_rounds: 3,
      reactions: phase === 'fit' ? 16 : 12, initial_reactions: 16, rmse, best_rmse: rmse, predictions, per_output_rmse: [rmse] };
  };
  const first = frame(0.2, 'fit', 12);
  app.publishProgress(first);
  const preview = gradient.locator('.inverse-live-preview');
  await expect(preview).toBeVisible();
  await expect(preview).toHaveAttribute('data-step', '12');
  await expect(preview).toContainText(`RMSD ${Number(first.rmse.toPrecision(5))}`);
  await expect(gradient.locator('.inverse-learning')).toContainText('Training RMSD');
  const targetPath = await preview.locator('.inverse-target-curve').getAttribute('d');
  const responsePath = await preview.locator('.inverse-fit-curve').getAttribute('d');
  const next = frame(0.8, 'prune_refit', 26);
  app.publishProgress(next);
  await expect(preview).toHaveAttribute('data-step', '26');
  await expect(preview).toContainText(`RMSD ${Number(next.rmse.toPrecision(5))}`);
  expect(await preview.locator('.inverse-fit-curve').getAttribute('d')).not.toBe(responsePath);
  expect(await preview.locator('.inverse-target-curve').getAttribute('d')).toBe(targetPath);
  expect(await targetValue(page)).toEqual(original);
  expect(await control(page, 'drawing-json').inputValue()).toBe(drawing);
  await expect(preview.locator('.inverse-target-point')).toHaveCount(0);
  await gradient.getByRole('button', { name: 'Stop', exact: true }).click();
  await expect(preview).toHaveCount(0);
  await expect.poll(() => app.cancellations.length).toBe(1);
  await expectNoCurrentOutput(page);
  expect(app.errors).toEqual([]);
});

test('target ticks, hover and keyboard inspection use physical coordinates and axes remain editable', async ({ page }) => {
  const app = await openInverseDesign(page);
  await editRole(page, '[data-edit="input"][data-key="min"]', '1');
  await editRole(page, '[data-edit="input"][data-key="max"]', '100');
  await editRole(page, '[data-edit="output"][data-key="min"]', '-2');
  await editRole(page, '[data-edit="output"][data-key="max"]', '6');
  const canvas = control(page, 'target-canvas');
  await canvas.scrollIntoViewIfNeeded();
  const box = await canvas.boundingBox();
  const hover = async (x, y) => page.mouse.move(box.x + box.width * (TARGET_PLOT.left + TARGET_PLOT.dx * x) / TARGET_PLOT.width,
    box.y + box.height * (TARGET_PLOT.top + TARGET_PLOT.dy * (1 - y)) / TARGET_PLOT.height);
  const original = await targetValue(page);
  await hover(0.5, 0.75);
  await expect(control(page, 'target-coordinates')).toHaveText('X · X = 10   Y · response = 4');
  await expect(canvas.locator('.design-target-cursor')).toBeVisible();
  await expect(canvas.locator('.design-target-tick')).toHaveCount(10);
  await expect(canvas.locator('.design-target-axis-label').first()).toContainText('log');
  await canvas.focus();
  await page.keyboard.press('ArrowUp');
  await expect(control(page, 'target-coordinates')).toHaveText('X · X = 10   Y · response = 4.08');
  expect(await targetValue(page)).toEqual(original);
  await control(page, 'target-axis-settings').click();
  await expect(control(page, 'target-roles').locator('[data-edit="input"][data-key="min"]')).toBeFocused();
  await editRole(page, '[data-edit="input"][data-key="scale"]', 'linear');
  await canvas.scrollIntoViewIfNeeded();
  const nextBox = await canvas.boundingBox();
  await page.mouse.move(nextBox.x + nextBox.width * (TARGET_PLOT.left + TARGET_PLOT.dx * 0.5) / TARGET_PLOT.width,
    nextBox.y + nextBox.height * (TARGET_PLOT.top + TARGET_PLOT.dy * 0.25) / TARGET_PLOT.height);
  await expect(control(page, 'target-coordinates')).toContainText('X · X = 50.5');
  for (const theme of ['light', 'dark']) {
    await page.evaluate(async mode => {
      (await import('/js/theme.js')).applyThemeMode(mode);
      // Audit settled colors, not a transient button color during theme changes.
      await Promise.all(document.getAnimations().filter(animation =>
        animation.effect?.getTiming().iterations !== Infinity).map(animation => animation.finished.catch(() => {})));
    }, theme);
    const results = await new AxeBuilder({ page }).include('.node[data-node-type="inverse-design-target"]').analyze();
    expect(results.violations.filter(violation => ['serious', 'critical'].includes(violation.impact))).toEqual([]);
  }
  expect(app.errors).toEqual([]);
});

test('a handdrawn response uses editable physical ranges and is undoable without an Agent', async ({ page }) => {
  const app = await openInverseDesign(page);
  await editRole(page, '[data-edit="input"][data-key="scale"]', 'linear');
  await editRole(page, '[data-edit="input"][data-key="min"]', '1');
  await editRole(page, '[data-edit="input"][data-key="max"]', '5');
  await editRole(page, '[data-edit="output"][data-key="max"]', '2');
  await control(page, 'target-points').fill('9'); await control(page, 'target-points').press('Tab');
  const before = await targetValue(page);
  await draw(page, [[0, 0.1], [0.5, 0.9], [1, 0.2]]);
  const drawn = await targetValue(page);
  const originalLine = await control(page, 'target-canvas').locator('.design-target-line').getAttribute('points');
  await expect(control(page, 'target-canvas').locator('circle')).toHaveCount(0);
  expect(JSON.parse(await control(page, 'drawing-json').inputValue()).points.length).toBeGreaterThan(9);
  expect(drawn.source).toBe('curve'); expect(drawn.samples.length).toBeGreaterThan(9);
  const vertices = JSON.parse(await control(page, 'drawing-json').inputValue()).points;
  for (const vertex of vertices) expect(drawn.samples.some(sample => Math.abs(sample.inputs[0] - (1 + 4 * vertex.x)) < 1e-10 && Math.abs(sample.outputs[0] - 2 * vertex.y) < 1e-10)).toBe(true);
  expect(drawn.samples[0].inputs[0]).toBeCloseTo(1, 6); expect(drawn.samples.at(-1).inputs[0]).toBeCloseTo(5, 6);
  expect(Math.max(...drawn.samples.map(sample => sample.outputs[0]))).toBeGreaterThan(1.6);
  expect(drawn.samples.at(-1).outputs[0]).toBeLessThan(0.6);
  await targetNode(page).locator('.node-header').click();
  await page.keyboard.press('Control+z'); expect(await targetValue(page)).toEqual(before);
  await page.keyboard.press('Control+Shift+z'); expect(await targetValue(page)).toEqual(drawn);
  expect((await runConnected(page)).summary.failed).toBe(0);
  expect(app.requests[0].target.samples).toEqual(drawn.samples);
  const saved = await snapshot(page);
  const resultDrawing = JSON.parse(saved.nodes.find(node => node.type === 'gradient-design').data.inverseDesignDrawing);
  const sourceDrawing = JSON.parse(saved.nodes.find(node => node.type === 'inverse-design-target').data.inverseDrawingJSON);
  expect(resultDrawing.points).toEqual(sourceDrawing.points);
  expect(resultDrawing.outputRanges).toEqual(sourceDrawing.outputRanges);
  await page.evaluate(document => window.BiocircuitsExplorerWorkspaceShell.applyWorkspaceFromJSONString(JSON.stringify(document)), saved);
  expect(await control(page, 'target-canvas').locator('.design-target-line').getAttribute('points')).toBe(originalLine);
  const fit = page.locator('.node[data-node-type="gradient-design"] .inverse-fit-chart');
  await expect(fit).toBeVisible();
  await expect(fit.locator('.inverse-target-curve')).toHaveCount(1);
  await expect(fit.locator('.inverse-fit-curve')).toHaveCount(1);
  await expect(fit.locator('circle, rect')).toHaveCount(0);
  expect(app.compilerRequests).toHaveLength(0); expect(app.errors).toEqual([]);
});

test('inverse design uses shared parameter/process/result colors and shared control typography in both themes', async ({ page }) => {
  await page.setViewportSize({ width: 2000, height: 1100 });
  await openInverseDesign(page);
  await runConnected(page);
  const references = await page.evaluate(async () => {
    const { createNode } = await import('/js/nodes.js');
    return {
      input: createNode('reaction-network', 2500, 0),
      process: createNode('model-builder', 2800, 0),
      result: createNode('scan-1d-result', 3100, 0),
      controls: createNode('scan-1d-params', 3500, 0),
    };
  });
  await expect(page.locator('.node[data-node-type="inverse-design-target"] details, .node[data-node-type="gradient-design"] details, .node[data-node-type="designed-network"] details')).toHaveCount(0);
  for (const theme of ['light', 'dark']) {
    await page.evaluate(async value => (await import('/js/theme.js')).applyThemeMode(value), theme);
    const styles = await page.evaluate(ids => {
      const css = (element, key) => getComputedStyle(element)[key];
      const node = type => document.querySelector(`.node[data-node-type="${type}"]`);
      const pairs = [['inverse-design-target', ids.controls], ['gradient-design', ids.process], ['designed-network', ids.result]];
      const ref = document.querySelector(`#${ids.controls} input[type="number"]`);
      return {
        headers: pairs.map(([type, id]) => [css(node(type).querySelector('.node-header'), 'backgroundColor'), css(document.querySelector(`#${id} .node-header`), 'backgroundColor')]),
        titles: pairs.map(([type]) => css(node(type).querySelector('.node-header'), 'fontSize')),
        fields: [...node('inverse-design-target').querySelectorAll('input[data-edit], select[data-edit], textarea.inverse-description')].map(field => [css(field, 'fontFamily'), css(field, 'fontSize')]),
        panels: [node('gradient-design').querySelector('.viewer-content'), node('designed-network').querySelector('.viewer-content')].map(panel => css(panel, 'backgroundColor')),
        referencePanel: css(document.querySelector(`#${ids.result} .viewer-content`), 'backgroundColor'),
        chartPanel: css(node('inverse-design-target').querySelector('.design-target-plot-panel'), 'backgroundColor'),
        insetToken: (() => { const probe = document.createElement('div'); probe.style.background = 'var(--node-inset-bg)'; document.body.appendChild(probe); const value = getComputedStyle(probe).backgroundColor; probe.remove(); return value; })(),
        notices: [...node('gradient-design').querySelectorAll('.inverse-design-form > .node-info')].map(panel => css(panel, 'backgroundColor')),
        referenceInfo: css(document.querySelector(`#${ids.process} .node-info`), 'backgroundColor'),
        directInputs: [...node('inverse-design-target').querySelectorAll('[data-edit]'), ...node('gradient-design').querySelectorAll('.auto-update')].every(field => field.checkVisibility()),
        reference: [css(ref, 'fontFamily'), css(ref, 'fontSize')],
      };
    }, references);
    for (const [actual, expected] of styles.headers) expect(actual).toBe(expected);
    expect(styles.titles).toEqual(['13px', '13px', '13px']);
    for (const value of styles.fields) expect(value).toEqual(styles.reference);
    for (const value of styles.panels) expect(value).toBe(styles.referencePanel);
    expect(styles.chartPanel).toBe(styles.insetToken);
    for (const value of styles.notices) expect(value).toBe(styles.referenceInfo);
    expect(styles.directInputs).toBe(true);
    await expect(page.locator('.node[data-node-type="gradient-design"] details, .node[data-node-type="designed-network"] details')).toHaveCount(0);
    if (process.env.INVERSE_LAYOUT_SCREENSHOTS) {
      for (const type of types) {
        const node = page.locator(`.node[data-node-type="${type}"]`);
        await node.locator('.node-content').evaluate(body => { body.scrollTop = 0; });
        await node.screenshot({ path: `/private/tmp/inverse-layout-${type}-${theme}.png`, animations: 'disabled' });
      }
      await control(page, 'target-roles').scrollIntoViewIfNeeded();
      await targetNode(page).screenshot({ path: `/private/tmp/inverse-layout-target-parameters-${theme}.png`, animations: 'disabled' });
    }
  }
});

test('image upload maps brightness to a two-input field and dimensions remain editable', async ({ page }) => {
  const app = await openInverseDesign(page);
  await control(page, 'target-mode').selectOption('image');
  await control(page, 'image-resolution').fill('2'); await control(page, 'image-resolution').press('Tab');
  // Original four-pixel PNG exercises actual image decode and file upload.
  const png = await page.evaluate(() => {
    const canvas = document.createElement('canvas'); canvas.width = 2; canvas.height = 2;
    canvas.getContext('2d').putImageData(new ImageData(new Uint8ClampedArray([
      0, 0, 0, 255, 255, 255, 255, 255, 255, 255, 255, 255, 0, 0, 0, 255,
    ]), 2, 2), 0, 0);
    return canvas.toDataURL('image/png').split(',')[1];
  });
  const file = { name: 'checker-pattern.png', mimeType: 'image/png', buffer: Buffer.from(png, 'base64') };
  await control(page, 'target-image-file').setInputFiles(file);
  await expect(control(page, 'target-editor-status')).toContainText('2 × 2 brightness samples');
  const target = await targetValue(page);
  expect(target.source).toBe('image'); expect(target.inputs).toHaveLength(2);
  expect(target.samples.map(sample => sample.outputs[0])).toEqual([0, 1, 1, 0]);
  expect(target.samples.map(sample => sample.inputs[1])).toEqual([10, 10, 0.05, 0.05]);
  await control(page, 'image-invert').selectOption('true');
  await control(page, 'target-image-file').setInputFiles(file);
  await expect.poll(async () => (await targetValue(page)).samples[0].outputs[0]).toBe(1);
  expect((await runConnected(page)).summary.failed).toBe(0);
  await expect(page.getByRole('img', { name: 'Target and network response field at supplied inputs; darker points indicate larger output values' })).toBeVisible();
  await control(page, 'target-mode').selectOption('data');
  await editRole(page, '[data-edit="input-count"]', '1');
  await editRole(page, '[data-edit="output-count"]', '3');
  const adjusted = await targetValue(page);
  expect(adjusted.inputs).toHaveLength(1); expect(adjusted.outputs).toHaveLength(3);
  expect(adjusted.samples.every(sample => sample.inputs.length === 1 && sample.outputs.length === 3)).toBe(true);
  await expectNoCurrentOutput(page);
  expect(app.compilerRequests).toHaveLength(0); expect(app.errors).toEqual([]);
});

test('a one-input two-output trajectory follows drawing order and publishes both readouts', async ({ page }) => {
  const app = await openInverseDesign(page);
  await control(page, 'target-mode').selectOption('trajectory');
  await editRole(page, '[data-edit="input"][data-key="name"]', 'u');
  await editRole(page, '[data-edit="output"][data-index="0"][data-key="name"]', 'x');
  await editRole(page, '[data-edit="output"][data-index="1"][data-key="name"]', 'y');
  await draw(page, [[0.1, 0.1], [0.9, 0.1], [0.9, 0.9], [0.1, 0.9], [0.1, 0.1]]);
  const target = await targetValue(page);
  expect(target.source).toBe('trajectory');
  expect(target.inputs.map(input => input.name)).toEqual(['u']);
  expect(target.outputs.map(output => output.name)).toEqual(['x', 'y']);
  expect(target.samples.every(sample => sample.inputs.length === 1 && sample.outputs.length === 2)).toBe(true);
  expect(target.samples.slice(1).every((sample, index) => sample.inputs[0] > target.samples[index].inputs[0])).toBe(true);
  expect(target.samples[0].outputs[0]).toBeCloseTo(target.samples.at(-1).outputs[0], 1);
  expect(target.samples[0].outputs[1]).toBeCloseTo(target.samples.at(-1).outputs[1], 1);
  expect((await runConnected(page)).summary.failed).toBe(0);
  expect(app.requests[0].target.outputs).toHaveLength(2);
  await expect(page.getByRole('img', { name: 'Two-output target and network response, ordered by u' })).toBeVisible();
  expect(app.errors).toEqual([]);
});

test('an uploaded pattern becomes a one-input XY target only after an ordered trace, with atomic Undo and restore', async ({ page }) => {
  const app = await openInverseDesign(page);
  await control(page, 'target-mode').selectOption('trajectory');
  await editRole(page, '[data-edit="input"][data-key="name"]', 'u');
  await editRole(page, '[data-edit="output"][data-index="0"][data-key="name"]', 'x');
  await editRole(page, '[data-edit="output"][data-index="1"][data-key="name"]', 'y');
  const before = await targetValue(page);
  const png = await page.evaluate(() => {
    const canvas = document.createElement('canvas'); canvas.width = 32; canvas.height = 32;
    const context = canvas.getContext('2d');
    context.fillStyle = 'white'; context.fillRect(0, 0, 32, 32);
    context.strokeStyle = 'black'; context.lineWidth = 2;
    context.beginPath(); context.moveTo(3, 29); context.lineTo(29, 29); context.lineTo(29, 3); context.stroke();
    return canvas.toDataURL('image/png').split(',')[1];
  });
  await control(page, 'target-image-file').setInputFiles({ name: 'ordered-L-pattern.png', mimeType: 'image/png', buffer: Buffer.from(png, 'base64') });
  await expect(control(page, 'target-editor-status')).toContainText('trace a single path in travel order');
  await expect(control(page, 'target-canvas').locator('.design-target-reference-image')).toHaveAttribute('href', /^data:image\/png;base64,/);
  expect(JSON.parse(await control(page, 'reference-state').inputValue()).pending).toBe(true);
  expect(await targetValue(page)).toEqual(before);
  expect((await runConnected(page)).summary.blocked).toBeGreaterThan(0);
  expect(app.requests).toHaveLength(0);

  await draw(page, [[0.1, 0.1], [0.9, 0.1], [0.9, 0.9]]);
  const traced = await targetValue(page);
  expect(JSON.parse(await control(page, 'reference-state').inputValue()).pending).toBe(false);
  expect(traced.source).toBe('trajectory');
  expect(traced.inputs.map(input => input.name)).toEqual(['u']);
  expect(traced.outputs.map(output => output.name)).toEqual(['x', 'y']);
  expect(traced.samples[0].outputs[0]).toBeCloseTo(0.1, 2);
  expect(traced.samples.at(-1).outputs[1]).toBeCloseTo(0.9, 2);
  expect(traced.samples.slice(1).every((sample, index) => sample.inputs[0] > traced.samples[index].inputs[0])).toBe(true);
  await page.keyboard.press('Control+z');
  expect(JSON.parse(await control(page, 'reference-state').inputValue()).pending).toBe(true);
  expect(await targetValue(page)).toEqual(before);
  expect((await runConnected(page)).summary.blocked).toBeGreaterThan(0);
  expect(app.requests).toHaveLength(0);
  await page.keyboard.press('Control+Shift+z');
  expect(JSON.parse(await control(page, 'reference-state').inputValue()).pending).toBe(false);
  expect(await targetValue(page)).toEqual(traced);
  expect((await runConnected(page)).summary.failed).toBe(0);
  expect(app.requests[0].target.samples).toEqual(traced.samples);
  expect(app.requests[0].target.inputs).toHaveLength(1);
  expect(app.requests[0].target.outputs).toHaveLength(2);

  const saved = await snapshot(page);
  expect(JSON.stringify(saved)).not.toContain('data:image/');
  await page.evaluate(document => window.BiocircuitsExplorerWorkspaceShell.applyWorkspaceFromJSONString(JSON.stringify(document)), saved);
  await expectNoCurrentOutput(page);
  expect(await targetValue(page)).toEqual(traced);
  expect(JSON.parse(await control(page, 'reference-state').inputValue()).pending).toBe(false);
  await expect(control(page, 'target-canvas').locator('.design-target-reference-image')).toHaveCount(0);
  expect((await runConnected(page)).summary.failed).toBe(0);
  expect(app.requests).toHaveLength(2);
  expect(app.compilerRequests).toHaveLength(0);
  expect(app.errors).toEqual([]);
});

test('node Agent compilation shares authenticated settings and is one undoable editable target change', async ({ page }) => {
  const app = await openInverseDesign(page);
  await page.evaluate(async () => {
    window.setDesignChatEndpoint('http://127.0.0.1:8765/design-chat', 'test-memory-bearer');
    (await import('/js/llm-settings.js')).setLLMConfig({ provider: 'openai', model: 'mock-compiler', apiKey: 'test-memory-llm-key', effort: 'high' });
  });
  await control(page, 'description').fill('Create a triangular response with a peak in the middle.');
  await control(page, 'description').press('Tab');
  const before = await targetValue(page), description = await control(page, 'description').inputValue();
  await page.getByRole('button', { name: 'Compile with Design Agent', exact: true }).click();
  await expect(targetNode(page).getByText('Goal compiled.', { exact: true })).toBeVisible();
  expect(app.compilerRequests).toHaveLength(1);
  expect(app.compilerRequests[0].headers.authorization).toBe('Bearer test-memory-bearer');
  expect(app.compilerRequests[0].body).toMatchObject({ message: description, llm: { model: 'mock-compiler', apiKey: 'test-memory-llm-key', effort: 'high' } });
  expect(await targetValue(page)).toEqual(compiledResponse().target);
  expect(await control(page, 'target-mode').inputValue()).toBe('agent');
  await targetNode(page).locator('.node-header').click();
  await page.keyboard.press('Control+z'); expect(await targetValue(page)).toEqual(before);
  expect(await control(page, 'description').inputValue()).toBe(description);
  await page.keyboard.press('Control+Shift+z'); expect(await targetValue(page)).toEqual(compiledResponse().target);
  await editRole(page, '[data-edit="input"][data-key="max"]', '6');
  expect((await targetValue(page)).samples.at(-1).inputs[0]).toBe(6);
  await control(page, 'description').fill('Replace the triangular goal with a different behavior.');
  expect((await runConnected(page)).summary.blocked).toBeGreaterThan(0);
  expect(app.requests).toHaveLength(0);
  // Switching explicitly to numerical data accepts manual target authoring;
  // a changed Agent description alone must never reuse the former objective.
  await control(page, 'target-mode').selectOption('data');
  expect((await runConnected(page)).summary.failed).toBe(0);
  expect(app.requests).toHaveLength(1);
  expect(app.requests[0].target.source).toBe('data');
  const persisted = await snapshot(page), stored = await page.evaluate(() => JSON.stringify(localStorage));
  expect(JSON.stringify(persisted) + stored).not.toContain('test-memory-bearer');
  expect(JSON.stringify(persisted) + stored).not.toContain('test-memory-llm-key');
  expect(app.errors).toEqual([]);
});

test('Design Agent exports a complete target workflow as one atomic Undo/Redo operation', async ({ page }) => {
  const app = await openInverseDesign(page, { chain: false });
  await page.getByRole('button', { name: 'Design Agent', exact: true }).click();
  await page.getByRole('textbox', { name: 'Message the design agent' }).fill('Create a triangular response.');
  await page.getByRole('button', { name: 'Compile target', exact: true }).click();
  await page.getByRole('button', { name: 'Open inverse design in Workspace ↗', exact: true }).click();
  await expect(page.locator('html')).toHaveAttribute('data-node-view', 'workspace');
  await expect(page.locator('#canvas > .node')).toHaveCount(3);
  const exported = await snapshot(page);
  expect(exported.nodes.map(node => node.type)).toEqual(types); expect(exported.connections).toHaveLength(2);
  expect(await targetValue(page)).toEqual(compiledResponse().target);
  await targetNode(page).locator('.node-header').click();
  await page.keyboard.press('Control+z'); await expect(page.locator('#canvas > .node')).toHaveCount(0);
  await page.keyboard.press('Control+Shift+z'); await expect(page.locator('#canvas > .node')).toHaveCount(3);
  expect((await snapshot(page)).nodes.map(node => node.id)).toEqual(exported.nodes.map(node => node.id));
  expect(await targetValue(page)).toEqual(compiledResponse().target);
  expect(app.requests).toHaveLength(0); expect(app.errors).toEqual([]);
});

for (const phase of ['submission', 'running']) {
  test(`target edits cancel the exact ${phase} job and prevent stale output publication`, async ({ page }) => {
    const app = await openInverseDesign(page, { delayedSubmission: phase === 'submission', holdJob: true });
    await startConnected(page); await expect.poll(() => app.requests.length).toBe(1);
    if (phase === 'running') await expect.poll(() => app.jobReads.length).toBeGreaterThan(0);
    await control(page, 'description').fill('An edited goal retires the pending result.');
    if (phase === 'submission') await app.finishSubmission();
    const report = await page.evaluate(() => window.__inverseRun);
    expect(report.summary.stale + report.summary.blocked + report.summary.failed).toBeGreaterThan(0);
    await expect.poll(() => app.cancellations).toEqual(['inverse-browser-job-1']);
    await expectNoCurrentOutput(page); expect(app.errors).toEqual([]);
  });
}

test('editing the target while Agent compilation is pending prevents a late overwrite', async ({ page }) => {
  const app = await openInverseDesign(page, { delayedCompiler: true });
  await page.getByRole('button', { name: 'Compile with Design Agent', exact: true }).click();
  await expect.poll(() => app.compilerRequests.length).toBe(1);
  await control(page, 'description').fill('Keep this newer manually edited goal.');
  const before = await targetValue(page);
  await app.finishCompile();
  await expect(control(page, 'description')).toHaveValue('Keep this newer manually edited goal.');
  expect(await targetValue(page)).toEqual(before);
  await expect(targetNode(page).getByText('Goal compiled.', { exact: true })).toHaveCount(0);
  expect(app.errors).toEqual([]);
});

test('target entrances and completed pruning results meet the serious/critical accessibility gate in both themes', async ({ page }) => {
  test.setTimeout(60_000);
  await openInverseDesign(page); expect((await runConnected(page)).summary.failed).toBe(0);
  for (const theme of ['light', 'dark']) {
    await page.evaluate(async mode => (await import('/js/theme.js')).applyThemeMode(mode), theme);
    for (const mode of ['curve', 'image', 'trajectory', 'data', 'agent']) {
      // Inspect completed results first; subsequent mode edits retire them.
      if (mode !== 'curve') await control(page, 'target-mode').selectOption(mode);
      if (['curve', 'trajectory'].includes(mode)) await expect(control(page, 'target-points')).toBeVisible();
      else await expect(control(page, 'target-points')).toBeHidden();
      for (const suffix of ['image-resolution', 'image-invert']) {
        if (mode === 'image') await expect(control(page, suffix)).toBeVisible();
        else await expect(control(page, suffix)).toBeHidden();
      }
      const results = await new AxeBuilder({ page }).include('#canvas').analyze();
      expect(results.violations.filter(v => ['serious', 'critical'].includes(v.impact))).toEqual([]);
    }
    await control(page, 'target-mode').selectOption('curve'); expect((await runConnected(page)).summary.failed).toBe(0);
  }
});

test('resizing grows the flexible results region, and content scrolls instead of crushing at the minimum size', async ({ page }) => {
  await page.setViewportSize({ width: 1600, height: 1300 });
  const app = await openInverseDesign(page, { chain: false });
  await page.evaluate(() => {
    window.addNodeFromMenu('inverse-design-target');
    window.addNodeFromMenu('gradient-design');
  });
  await expect(page.locator('#canvas > .node')).toHaveCount(2);
  await page.evaluate(() => {
    const place = (type, x, y) => {
      const el = document.querySelector(`.node[data-node-type="${type}"]`);
      el.style.left = `${x}px`;
      el.style.top = `${y}px`;
    };
    place('inverse-design-target', 30, 40);
    place('gradient-design', 640, 40);
  });
  const gradient = page.locator('.node[data-node-type="gradient-design"]');
  const viewer = gradient.locator('.inverse-design-viewer');
  const form = gradient.locator('.inverse-design-form');
  const before = {
    viewer: await viewer.evaluate(el => el.offsetHeight),
    form: await form.evaluate(el => el.offsetHeight),
  };
  const handle = await gradient.locator('.node-resize').boundingBox();
  expect(handle).not.toBeNull();
  const startX = handle.x + handle.width / 2, startY = handle.y + handle.height / 2;
  await page.mouse.move(startX, startY);
  await page.mouse.down();
  await page.mouse.move(startX, startY + 200, { steps: 8 });
  await page.mouse.up();
  const after = {
    viewer: await viewer.evaluate(el => el.offsetHeight),
    form: await form.evaluate(el => el.offsetHeight),
  };
  // The +200px lands entirely in the flexible results region; the form is rigid.
  expect(after.viewer - before.viewer).toBeGreaterThanOrEqual(180);
  expect(after.viewer - before.viewer).toBeLessThanOrEqual(220);
  expect(after.form).toBe(before.form);

  // Shrink to the per-type floor: the inline min clamps, content scrolls, and
  // nothing is crushed horizontally out of the node.
  for (const node of [targetNode(page), gradient]) {
    await node.evaluate(el => { el.style.width = '1px'; el.style.height = '1px'; });
  }
  const floors = { 'inverse-design-target': { w: 460, h: 750 }, 'gradient-design': { w: 480, h: 680 } };
  for (const [type, floor] of Object.entries(floors)) {
    const node = page.locator(`.node[data-node-type="${type}"]`);
    expect(await node.evaluate(el => ({ w: el.offsetWidth, h: el.offsetHeight }))).toEqual(floor);
    expect(await node.locator('.node-content').evaluate(el => el.scrollWidth - el.clientWidth)).toBeLessThanOrEqual(1);
  }
  const geometry = await targetNode(page).evaluate(nodeEl => {
    const svg = nodeEl.querySelector('[id$="-target-canvas"]');
    const panel = nodeEl.querySelector('.design-target-plot-panel');
    const box = svg.getBoundingClientRect(), frame = panel.getBoundingClientRect();
    return {
      left: box.left - frame.left, top: box.top - frame.top,
      right: frame.right - box.right, bottom: frame.bottom - box.bottom,
    };
  });
  for (const [edge, slack] of Object.entries(geometry)) {
    expect(slack, `target preview SVG stays inside its panel at the ${edge} edge`).toBeGreaterThanOrEqual(-1);
  }
  expect(app.errors).toEqual([]);
});
