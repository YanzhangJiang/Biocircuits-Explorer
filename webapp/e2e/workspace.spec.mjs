import { readFileSync } from 'node:fs';

import AxeBuilder from '@axe-core/playwright';
import { expect, test } from '@playwright/test';

const futureWorkspace = JSON.parse(readFileSync(
  new URL('../../tests/fixtures/workspace/future-v3.json', import.meta.url),
  'utf8',
));

const buildModelResponse = Object.freeze({
  session_id: 'playwright-local-session',
  network_ir_hash: 'playwright-network-ir-hash',
  n: 2,
  d: 2,
  r: 2,
  x_sym: ['E', 'S', 'C_ES', 'P', 'C_EP'],
  q_sym: ['q_E', 'q_S'],
  K_sym: ['K_ES', 'K_EP'],
});

const browserErrorsByPage = new WeakMap();

const emptyBehaviorFamiliesResponse = Object.freeze({
  change_qK: 'q_E',
  observe_x: 'C_ES',
  exact_families: [],
  paths: [],
  included_paths: 0,
  excluded_paths: 0,
  exclusion_counts: {},
});

async function installLocalApiMocks(page) {
  await page.route('http://127.0.0.1:8765/health', async (route) => {
    await route.fulfill({
      status: 200,
      contentType: 'application/json',
      body: JSON.stringify({
        ok: true,
        service: 'biocircuits-design-chat',
        engine: { ready: true },
      }),
    });
  });
  await page.route('**/api/v1/**', async (route) => {
    const url = new URL(route.request().url());
    let body;
    switch (url.pathname) {
      case '/api/v1/build_model':
        body = buildModelResponse;
        break;
      case '/api/v1/behavior_families':
        body = emptyBehaviorFamiliesResponse;
        break;
      case '/api/v1/version':
        body = { version: 'playwright-local', commit: 'local-fixture' };
        break;
      case '/api/v1/debug/logs':
        body = { entries: [], next_seq: 0 };
        break;
      default:
        body = { error: `Unexpected local E2E endpoint: ${url.pathname}` };
        await route.fulfill({ status: 404, contentType: 'application/json', body: JSON.stringify(body) });
        return;
    }
    await route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify(body) });
  });
}

async function openWorkspace(page) {
  const browserErrors = [];
  browserErrorsByPage.set(page, browserErrors);
  page.on('console', (message) => {
    if (message.type() === 'error') browserErrors.push(`console: ${message.text()}`);
  });
  page.on('pageerror', (error) => browserErrors.push(`pageerror: ${error.message}`));
  await page.addInitScript(() => {
    window.localStorage.setItem('bcx-node-view', 'workspace');
    window.localStorage.setItem('biocircuits-explorer.theme-mode', 'light');
  });
  await installLocalApiMocks(page);
  await page.goto('/index-node.html#workspace');
  await page.waitForFunction(() => (
    window.BiocircuitsExplorerWorkspaceShell?.contractVersion === 2 &&
    window.BiocircuitsExplorerWorkspaceShell?.workspaceVersion === 2
  ));
  await expect(page.locator('html')).toHaveAttribute('data-node-view', 'workspace');
}

function expectNoBrowserErrors(page) {
  expect(browserErrorsByPage.get(page) || []).toEqual([]);
}

async function quickAddSiso(page) {
  await page.getByRole('button', { name: 'Quick Add' }).click();
  await page.locator('#legacy-nodes-menu .menu-item[data-type="siso-analysis"]').click();
  await expect(page.locator('#canvas > .node')).toHaveCount(4);
  return page.evaluate(() => JSON.parse(
    window.BiocircuitsExplorerWorkspaceShell.serializeWorkspace(),
  ));
}

function topology(document) {
  return {
    canvas: document.canvas,
    nodes: document.nodes.map(({ id, type, x, y }) => ({ id, type, x, y })),
    connections: document.connections,
  };
}

test('Quick Add is atomic and one Undo/Redo preserves node IDs', async ({ page }) => {
  await openWorkspace(page);

  const applied = topology(await quickAddSiso(page));
  expect(applied.nodes.map(node => node.type)).toEqual([
    'reaction-network',
    'model-builder',
    'siso-params',
    'siso-result',
  ]);
  expect(applied.connections).toHaveLength(3);
  const paramsBox = await page.locator('.node[data-node-type="siso-params"]').boundingBox();
  const resultBox = await page.locator('.node[data-node-type="siso-result"]').boundingBox();
  expect(paramsBox).not.toBeNull();
  expect(resultBox).not.toBeNull();
  expect(Math.round(resultBox.x - (paramsBox.x + paramsBox.width))).toBe(60);

  await page.keyboard.press('Control+z');
  await expect(page.locator('#canvas > .node')).toHaveCount(0);
  const undone = JSON.parse(await page.evaluate(() => (
    window.BiocircuitsExplorerWorkspaceShell.serializeWorkspace()
  )));
  expect(undone.nodes).toEqual([]);
  expect(undone.connections).toEqual([]);

  await page.keyboard.press('Control+Shift+z');
  await expect(page.locator('#canvas > .node')).toHaveCount(4);
  const redone = topology(JSON.parse(await page.evaluate(() => (
    window.BiocircuitsExplorerWorkspaceShell.serializeWorkspace()
  ))));
  expect(redone).toEqual(applied);
  expectNoBrowserErrors(page);
});

test('Quick Add offers existing networks with keyboard access and atomic Undo/Redo', async ({ page }) => {
  await openWorkspace(page);
  const before = await page.evaluate(() => {
    window.addNodeFromMenu('reaction-network');
    window.addNodeFromMenu('designed-network');
    return JSON.parse(window.BiocircuitsExplorerWorkspaceShell.serializeWorkspace());
  });
  const [source, designed] = before.nodes;
  const openPicker = async () => {
    await page.getByRole('button', { name: 'Quick Add', exact: true }).click();
    await page.locator('#legacy-nodes-menu [data-type="parameter-scan-1d"]').click();
  };
  await openPicker();
  const picker = page.getByRole('dialog', { name: 'Quick Add · Parameter Scan (1D)', exact: true });
  await expect(picker).toBeVisible();
  await expect(picker.getByRole('button', { name: `Reaction Network · ${source.id}` })).toBeVisible();
  await expect(picker.getByRole('button', { name: `Designed Network · ${designed.id}` })).toBeVisible();
  await expect(page.locator('#canvas > .node')).toHaveCount(2);
  await expect(page.locator('#toast-container')).not.toContainText('multiple compatible reaction sources');

  for (const theme of ['light', 'dark']) {
    await page.evaluate(async mode => (await import('/js/theme.js')).applyThemeMode(mode), theme);
    const accessibility = await new AxeBuilder({ page }).include('#quick-add-choice').analyze();
    expect(accessibility.violations.filter(item => ['serious', 'critical'].includes(item.impact))).toEqual([]);
    if (process.env.QUICK_ADD_SCREENSHOTS) {
      await picker.screenshot({ path: `/private/tmp/quick-add-choice-${theme}.png` });
    }
  }
  // Undo/Delete inside the modal must not change the selected background graph.
  await page.keyboard.press('Control+z');
  await page.keyboard.press('Delete');
  await expect(page.locator('#canvas > .node')).toHaveCount(2);
  await page.keyboard.press('Escape');
  await expect(picker).toBeHidden();
  expect(topology(await page.evaluate(() => JSON.parse(window.BiocircuitsExplorerWorkspaceShell.serializeWorkspace())))).toEqual(topology(before));

  await openPicker();
  const choice = picker.getByRole('button', { name: `Reaction Network · ${source.id}` });
  await choice.focus();
  await page.keyboard.press('Enter');
  await expect(picker).toBeHidden();
  await expect(page.locator('#canvas > .node')).toHaveCount(5);
  const applied = await page.evaluate(() => JSON.parse(window.BiocircuitsExplorerWorkspaceShell.serializeWorkspace()));
  const builder = applied.nodes.find(item => item.type === 'model-builder');
  expect(applied.connections).toContainEqual({ fromNode: source.id, fromPort: 'reactions', toNode: builder.id, toPort: 'reactions' });
  expect(applied.connections.some(item => item.fromNode === designed.id)).toBe(false);
  await page.keyboard.press('Control+z');
  await expect(page.locator('#canvas > .node')).toHaveCount(2);
  expect(topology(await page.evaluate(() => JSON.parse(window.BiocircuitsExplorerWorkspaceShell.serializeWorkspace())))).toEqual(topology(before));
  await page.keyboard.press('Control+Shift+z');
  await expect(page.locator('#canvas > .node')).toHaveCount(5);
  expect(topology(await page.evaluate(() => JSON.parse(window.BiocircuitsExplorerWorkspaceShell.serializeWorkspace())))).toEqual(topology(applied));
  expectNoBrowserErrors(page);
});

test('native Quick Add entrypoint exposes a new network without modifier keys', async ({ page }) => {
  await openWorkspace(page);
  const before = await page.evaluate(() => {
    window.addNodeFromMenu('reaction-network');
    window.addNodeFromMenu('designed-network');
    const snapshot = JSON.parse(window.BiocircuitsExplorerWorkspaceShell.serializeWorkspace());
    window.addQuickAddChain('parameter-scan-1d');
    return snapshot;
  });
  await page.getByRole('dialog').getByRole('button', { name: 'New Reaction Network', exact: true }).click();
  await expect(page.getByRole('dialog')).toBeHidden();
  await expect(page.locator('#canvas > .node')).toHaveCount(6);
  const applied = await page.evaluate(() => JSON.parse(window.BiocircuitsExplorerWorkspaceShell.serializeWorkspace()));
  const oldIds = before.nodes.map(item => item.id);
  expect(applied.connections).toHaveLength(3);
  expect(applied.connections.every(item => !oldIds.includes(item.fromNode) && !oldIds.includes(item.toNode))).toBe(true);
  await page.keyboard.press('Control+z');
  await expect(page.locator('#canvas > .node')).toHaveCount(2);
  expect(topology(await page.evaluate(() => JSON.parse(window.BiocircuitsExplorerWorkspaceShell.serializeWorkspace())))).toEqual(topology(before));
  expectNoBrowserErrors(page);
});

test('Quick Add resolves multiple builders and respects a selected builder source', async ({ page }) => {
  await openWorkspace(page);
  const graph = await page.evaluate(() => {
    for (const type of ['reaction-network', 'model-builder', 'model-builder', 'designed-network']) window.addNodeFromMenu(type);
    const snapshot = JSON.parse(window.BiocircuitsExplorerWorkspaceShell.serializeWorkspace());
    const [source, first, second] = snapshot.nodes;
    snapshot.connections = [first, second].map(builder => ({ fromNode: source.id, fromPort: 'reactions', toNode: builder.id, toPort: 'reactions' }));
    window.BiocircuitsExplorerWorkspaceShell.applyWorkspaceFromJSONString(JSON.stringify(snapshot));
    return JSON.parse(window.BiocircuitsExplorerWorkspaceShell.serializeWorkspace());
  });
  const [source, , builder] = graph.nodes;
  await page.evaluate(id => window.addQuickAddChain('parameter-scan-1d', { selectedSourceId: id }), source.id);
  const picker = page.getByRole('dialog');
  await expect(picker).toBeVisible();
  await expect(picker).toContainText('several Model Builders');
  await expect(picker.getByRole('button', { name: 'New Reaction Network', exact: true })).toBeHidden();
  await picker.getByRole('button', { name: `Model Builder · ${builder.id}` }).click();
  await expect(picker).toBeHidden();
  await expect(page.locator('#canvas > .node')).toHaveCount(6);
  const added = await page.evaluate(() => JSON.parse(window.BiocircuitsExplorerWorkspaceShell.serializeWorkspace()));
  const params = added.nodes.find(item => item.type === 'scan-1d-params');
  expect(added.connections).toContainEqual({ fromNode: builder.id, fromPort: 'model', toNode: params.id, toPort: 'model' });
  await page.keyboard.press('Control+z');
  await expect(page.locator('#canvas > .node')).toHaveCount(4);
  await page.evaluate(async id => (await import('/js/selection.js')).selectOnly(id), builder.id);
  await page.evaluate(() => window.addQuickAddChain('parameter-scan-1d'));
  await expect(picker).toBeHidden();
  await expect(page.locator('#canvas > .node')).toHaveCount(6);
  expectNoBrowserErrors(page);
});

test('replacing a workspace cancels pending Quick Add choices', async ({ page }) => {
  await openWorkspace(page);
  const replacement = await page.evaluate(() => {
    window.addNodeFromMenu('reaction-network');
    window.addNodeFromMenu('designed-network');
    const snapshot = JSON.parse(window.BiocircuitsExplorerWorkspaceShell.serializeWorkspace());
    window.addQuickAddChain('parameter-scan-1d');
    return snapshot;
  });
  await expect(page.getByRole('dialog')).toBeVisible();
  await page.evaluate(snapshot => window.BiocircuitsExplorerWorkspaceShell.applyWorkspaceFromJSONString(JSON.stringify(snapshot)), replacement);
  await expect(page.getByRole('dialog')).toBeHidden();
  await expect(page.locator('#canvas > .node')).toHaveCount(2);
  await page.keyboard.press('Control+z');
  await expect(page.locator('#canvas > .node')).toHaveCount(2);
  expectNoBrowserErrors(page);
});

test('connected workflow entrypoints return structured reports', async ({ page }) => {
  await openWorkspace(page);
  await quickAddSiso(page);

  await page.locator('.node[data-node-type="siso-result"] .node-header').click();
  const selectedReport = await page.evaluate(() => (
    window.BiocircuitsExplorerWorkspaceShell.runConnectedWorkspace()
  ));
  expect(selectedReport).toMatchObject({
    status: 'succeeded',
    code: 'ready',
    plan: { ok: true },
    summary: {
      executed: 3,
      reused: 0,
      blocked: 0,
      failed: 0,
      cancelled: 0,
      stale: 0,
    },
  });
  const document = await page.evaluate(() => JSON.parse(
    window.BiocircuitsExplorerWorkspaceShell.serializeWorkspace(),
  ));
  const typeById = Object.fromEntries(document.nodes.map(({ id, type }) => [id, type]));
  const outcomeSemantics = selectedReport.plan.order
    .filter(nodeId => selectedReport.outcomes[nodeId])
    .map(nodeId => ({
      type: typeById[nodeId],
      status: selectedReport.outcomes[nodeId].status,
      work: selectedReport.outcomes[nodeId].work,
      code: selectedReport.outcomes[nodeId].code,
      outputs: selectedReport.outcomes[nodeId].outputs,
    }));
  expect(outcomeSemantics).toEqual([
    {
      type: 'model-builder',
      status: 'succeeded',
      work: 'executed',
      code: null,
      outputs: { model: 'present' },
    },
    {
      type: 'siso-params',
      status: 'succeeded',
      work: 'executed',
      code: null,
      outputs: {},
    },
    {
      type: 'siso-result',
      status: 'succeeded',
      work: 'executed',
      code: 'siso_path_not_selected',
      outputs: { result: 'missing' },
    },
  ]);
  const sisoResultId = document.nodes.find(({ type }) => type === 'siso-result').id;
  expect(selectedReport.outcomes[sisoResultId].message).toBe(
    'Select a current SISO path before running qK',
  );

  const allReport = await page.evaluate(() => (
    window.BiocircuitsExplorerWorkspaceShell.runAllConnectedWorkspace()
  ));
  expect(allReport).toMatchObject({
    status: 'succeeded',
    code: 'ready',
    plan: { ok: true },
    summary: {
      executed: 3,
      reused: 0,
      blocked: 0,
      failed: 0,
      cancelled: 0,
      stale: 0,
    },
  });
  expect(allReport.plan.nodeIds).toEqual(selectedReport.plan.nodeIds);
  expectNoBrowserErrors(page);
});

test('Workspace v2 saves historical results and rejects a future document atomically', async ({ page }) => {
  await openWorkspace(page);
  const currentResultDocument = {
    version: 2,
    schema_version: 'bne-workspace/v2.0.0',
    timestamp: '2026-07-15T00:00:00.000Z',
    canvas: { panX: 0, panY: 0, scale: 1 },
    nodes: [{
      id: 'saved-siso',
      type: 'siso-result',
      x: 120,
      y: 140,
      width: 420,
      height: 320,
      data: {
        behaviorData: emptyBehaviorFamiliesResponse,
        lifecycle: {
          state: 'current',
          freshness: 'current',
          evidence: { grade: 'engine-computed', source: 'local-fixture' },
        },
      },
    }],
    connections: [],
  };

  const applied = await page.evaluate((document) => (
    window.BiocircuitsExplorerWorkspaceShell.applyWorkspaceFromJSONString(JSON.stringify(document))
  ), currentResultDocument);
  expect(applied).toBe(true);
  const resultContent = page.locator('.node[data-node-type="siso-result"] .viewer-content');
  await expect(resultContent).toHaveAttribute('data-result-state', 'historical');
  await expect(resultContent.getByRole('status')).toContainText('Historical saved result');

  await page.evaluate(() => {
    window.__playwrightSavedWorkspace = null;
    window.BiocircuitsExplorerWorkspaceShell.registerHost({
      shellDidBecomeReady() {},
      saveWorkspaceJSONString(jsonString) {
        window.__playwrightSavedWorkspace = jsonString;
      },
    });
  });
  await page.getByRole('button', { name: 'Save Workspace' }).click();
  const saved = JSON.parse(await page.evaluate(() => window.__playwrightSavedWorkspace));
  expect(saved.version).toBe(2);
  expect(saved.schema_version).toBe('bne-workspace/v2.0.0');
  expect(saved.nodes).toHaveLength(1);
  expect(saved.nodes[0].data.lifecycle).toMatchObject({
    state: 'historical',
    freshness: 'historical',
  });

  const beforeFutureApply = topology(saved);
  const rejection = await page.evaluate((document) => {
    try {
      window.BiocircuitsExplorerWorkspaceShell.applyWorkspaceFromJSONString(JSON.stringify(document));
      return null;
    } catch (error) {
      return { name: error.name, code: error.code, message: error.message };
    }
  }, futureWorkspace);
  expect(rejection).toMatchObject({ name: 'WorkspaceV2Error', code: 'future-version' });
  const afterFutureApply = topology(JSON.parse(await page.evaluate(() => (
    window.BiocircuitsExplorerWorkspaceShell.serializeWorkspace()
  ))));
  expect(afterFutureApply).toEqual(beforeFutureApply);
  expectNoBrowserErrors(page);
});

test('the key workspace surface has no serious or critical axe findings', async ({ page }) => {
  await openWorkspace(page);

  const switchStyleBefore = await page.locator('#view-switch-agent').evaluate((button) => ({
    color: getComputedStyle(button).color,
    background: getComputedStyle(button.closest('.view-switch')).backgroundColor,
  }));
  const results = await new AxeBuilder({ page })
    .include('#header')
    .include('#editor')
    .withTags(['wcag2a', 'wcag2aa', 'wcag21a', 'wcag21aa'])
    .analyze();
  const switchStyleAfter = await page.locator('#view-switch-agent').evaluate((button) => ({
    color: getComputedStyle(button).color,
    background: getComputedStyle(button.closest('.view-switch')).backgroundColor,
  }));
  expect(switchStyleAfter).toEqual(switchStyleBefore);
  const blockers = results.violations.filter(({ impact }) => (
    impact === 'serious' || impact === 'critical'
  ));
  expect(blockers, JSON.stringify(blockers, null, 2)).toEqual([]);
  const contrastPass = results.passes.find(({ id }) => id === 'color-contrast');
  const agentContrastNode = contrastPass?.nodes.find(({ target }) => (
    target.includes('#view-switch-agent')
  ));
  const agentContrast = [...(agentContrastNode?.any || []), ...(agentContrastNode?.all || [])]
    .find(({ id }) => id === 'color-contrast')?.data;
  expect(agentContrast?.contrastRatio).toBeGreaterThanOrEqual(4.5);
  expectNoBrowserErrors(page);
});

test('Quick Add topology matches the deterministic visual baseline', async ({ page }) => {
  await openWorkspace(page);
  await quickAddSiso(page);

  await page.addStyleTag({ content: `
    *, *::before, *::after {
      animation: none !important;
      caret-color: transparent !important;
      transition: none !important;
    }
    html, body {
      background: #f4f6f8 !important;
      color-scheme: light !important;
    }
    #header, #grid-bg, #debug-console, #toast-container, #align-toolbar {
      display: none !important;
    }
    #editor {
      background: #f4f6f8 !important;
      inset: 0 !important;
      top: 0 !important;
    }
    .node {
      box-shadow: none !important;
      height: 140px !important;
      overflow: hidden !important;
      resize: none !important;
    }
    .node-header {
      border-radius: 6px 6px 0 0 !important;
      height: 32px !important;
      min-height: 32px !important;
      padding: 0 !important;
    }
    .node-header > * { visibility: hidden !important; }
    .node-body {
      height: 108px !important;
      min-height: 108px !important;
      padding: 0 !important;
      position: relative !important;
    }
    .node-body > *:not(.socket-row) { display: none !important; }
    .node-body .socket-row {
      position: absolute !important;
      top: 46px !important;
    }
    .node-body .socket-row.left { left: 0 !important; }
    .node-body .socket-row.right { right: 0 !important; }
    .node-body .socket-label { display: none !important; }
    .socket, .socket.connected {
      box-shadow: none !important;
      transform: none !important;
    }
    .node-resize { display: none !important; }
  ` });
  await page.waitForTimeout(100);

  await expect(page).toHaveScreenshot('quick-add-topology.png', {
    animations: 'disabled',
    caret: 'hide',
    clip: { x: 40, y: 120, width: 1560, height: 360 },
    // The baseline contains only flat boxes, sockets, and wires: all text,
    // shadows, transitions, grid painting, and carets are removed above. The
    // small ratio allows only cross-OS Chromium edge anti-aliasing, while a
    // topology/geometry change still affects far more than two percent.
    maxDiffPixelRatio: 0.02,
    threshold: 0.2,
  });
  expectNoBrowserErrors(page);
});

test('node resize clamps to the per-type minimum size', async ({ page }) => {
  await openWorkspace(page);
  await page.evaluate(() => window.addNodeFromMenu('markdown-note'));
  const node = page.locator('.node[data-node-type="markdown-note"]');
  await expect(node).toHaveCount(1);

  // createNode pins the per-type floor inline (js/node-sizes.js): 280x220
  // for markdown-note, below its 400x300 default size.
  const inlineMin = await node.evaluate((el) => ({
    minWidth: el.style.minWidth,
    minHeight: el.style.minHeight,
  }));
  expect(inlineMin).toEqual({ minWidth: '280px', minHeight: '220px' });

  // Drag the resize handle far up-left: the gesture must stop at 280x220 —
  // the per-type floor, not the old blanket 240x100 — and never below it.
  const box = await node.locator('.node-resize').boundingBox();
  expect(box).not.toBeNull();
  const startX = box.x + box.width / 2;
  const startY = box.y + box.height / 2;
  await page.mouse.move(startX, startY);
  await page.mouse.down();
  await page.mouse.move(Math.max(5, startX - 600), Math.max(5, startY - 600), { steps: 12 });
  await page.mouse.up();

  const size = await node.evaluate((el) => ({
    styleWidth: el.style.width,
    styleHeight: el.style.height,
    offsetWidth: el.offsetWidth,
    offsetHeight: el.offsetHeight,
  }));
  expect(size).toEqual({
    styleWidth: '280px',
    styleHeight: '220px',
    offsetWidth: 280,
    offsetHeight: 220,
  });
  expectNoBrowserErrors(page);
});
