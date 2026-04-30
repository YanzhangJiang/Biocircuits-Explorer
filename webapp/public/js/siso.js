import { nodeRegistry, connections, SISO_FAMILY_COLORS, ensureNodeData, getNodeData } from './state.js';
import { api, showToast, handleNodeError, escapeHtml, splitCommaList, parseOptionalFloat, cloneSerializable } from './api.js';
import { hexToRgba, getFamilyColor, applyPlotLayoutTheme, getPlotTheme, applyPlotAxisTheme, applyPlotSceneAxisTheme, themedColorbar } from './theme.js';
import { plotTrajectory, convexHull2D, formatPolyNumber, formatPolyConstraint, renderPolyCoordinateTable } from './plotting.js';
import { setNodeLoading, setupPlotResize, setupPlotInteractionGuard, getModelContextForNode, getSessionIdForNode, getQKSymbolsForNode, findUpstreamNodeByType } from './nodes.js';
import { triggerDownstreamNodes } from './model.js';
import { commitWorkspaceSnapshot, getNodeSerialData } from './workspace.js';
import { NODE_TYPES } from './node-types/index.js';

const SISO_PLOT_MODE_SINGLE = 'single';
const SISO_PLOT_MODE_OVERLAY = 'overlay';
const SISO_PLOT_MODE_VALUES = new Set([SISO_PLOT_MODE_SINGLE, SISO_PLOT_MODE_OVERLAY]);
const SISO_OVERLAY_CONCURRENCY = 4;

export function recomputeSISO(nodeId) {
  const typeDef = NODE_TYPES['siso-analysis'];
  if (typeDef.execute) typeDef.execute(nodeId);
}

export function formatVolumeSummary(vol) {
  if (!vol || vol.mean == null) return 'n/a';
  const mean = Number(vol.mean);
  const std = Number(vol.std ?? Math.sqrt(vol.var ?? 0));
  if (!Number.isFinite(mean)) return 'n/a';
  if (!Number.isFinite(std)) return mean.toExponential(2);
  return `${mean.toExponential(2)} ± ${std.toExponential(1)}`;
}

export function renderExclusionCounts(exclusionCounts) {
  const entries = Object.entries(exclusionCounts || {});
  if (!entries.length) return '';
  const items = entries.map(([reason, count]) => `<span class="tag tag-nonasym">${reason}: ${count}</span>`).join(' ');
  return `<div class="siso-inline-tags"><strong>Excluded paths</strong>: ${items}</div>`;
}

export function buildPathFamilyMaps(data) {
  const exactFamilyByPath = new Map();

  (data.exact_families || []).forEach(family => {
    (family.path_indices || []).forEach(pathIdx => exactFamilyByPath.set(pathIdx, family.family_idx));
  });

  return { exactFamilyByPath };
}

function getSISOPlotMode(nodeId) {
  const rawMode = nodeRegistry[nodeId]?.data?.sisoPlotMode;
  return SISO_PLOT_MODE_VALUES.has(rawMode) ? rawMode : SISO_PLOT_MODE_SINGLE;
}

function setSISOPlotMode(nodeId, mode) {
  const safeMode = SISO_PLOT_MODE_VALUES.has(mode) ? mode : SISO_PLOT_MODE_SINGLE;
  const nodeData = ensureNodeData(nodeId);
  nodeData.sisoPlotMode = safeMode;
  const selectEl = document.getElementById(`${nodeId}-plot-mode`);
  if (selectEl) selectEl.value = safeMode;
  return safeMode;
}

function selectedPathIdxForNode(nodeId) {
  const raw = getNodeData(nodeId).selectedPath?.path_idx;
  const parsed = parseInt(raw, 10);
  return Number.isFinite(parsed) ? parsed : null;
}

function sisoOverlayPathCandidates(data) {
  const { exactFamilyByPath } = buildPathFamilyMaps(data || {});
  return (data?.paths || [])
    .filter(path => path.feasible)
    .sort((a, b) => {
      const exactA = exactFamilyByPath.get(a.path_idx) || Number.MAX_SAFE_INTEGER;
      const exactB = exactFamilyByPath.get(b.path_idx) || Number.MAX_SAFE_INTEGER;
      return exactA - exactB || a.path_idx - b.path_idx;
    });
}

function sisoOverlayRequestKey(data, config, pathIndices) {
  return JSON.stringify({
    change_qK: data?.change_qK || config?.change_qK || '',
    observe_x: data?.observe_x || config?.observe_x || '',
    start: config?.min ?? -6,
    stop: config?.max ?? 6,
    path_indices: pathIndices,
  });
}

function trajectoryOutputIndex(trajectory, behaviorData) {
  const names = trajectory?.x_sym || [];
  const target = behaviorData?.observe_x || '';
  const byName = names.findIndex(name => name === target);
  if (byName >= 0) return byName;
  const byBackendIndex = Number(behaviorData?.observe_x_idx) - 1;
  return Number.isInteger(byBackendIndex) && byBackendIndex >= 0 ? byBackendIndex : 0;
}

async function mapWithConcurrency(items, limit, worker) {
  const results = new Array(items.length);
  let nextIndex = 0;
  const workers = Array.from({ length: Math.min(limit, items.length) }, async () => {
    while (nextIndex < items.length) {
      const currentIndex = nextIndex;
      nextIndex += 1;
      results[currentIndex] = await worker(items[currentIndex], currentIndex);
    }
  });
  await Promise.all(workers);
  return results;
}

function renderSISOPlotControls(nodeId, data) {
  const mode = getSISOPlotMode(nodeId);
  const familyCount = (data.exact_families || []).length;
  const includedCount = data.included_paths ?? (data.paths || []).filter(path => path.included).length;
  const fixedLabel = `${escapeHtml(data.change_qK || data.change_label || 'input')} -> ${escapeHtml(data.observe_x || 'output')}`;

  return `
    <section class="siso-section siso-plot-section">
      <div class="siso-section-head">
        <div class="siso-section-title">Behavior Plot</div>
        <div class="text-dim">${fixedLabel}</div>
      </div>
      <div class="siso-plot-toolbar">
        <label for="${nodeId}-plot-mode">Mode</label>
        <select id="${nodeId}-plot-mode" data-action="updateSISOPlotMode" data-node="${nodeId}">
          <option value="${SISO_PLOT_MODE_SINGLE}" ${mode === SISO_PLOT_MODE_SINGLE ? 'selected' : ''}>Selected trajectory</option>
          <option value="${SISO_PLOT_MODE_OVERLAY}" ${mode === SISO_PLOT_MODE_OVERLAY ? 'selected' : ''}>All path overlay</option>
        </select>
        <button type="button" class="btn btn-small siso-plot-refresh" data-action="refreshSISOPlot" data-node="${nodeId}">Refresh</button>
      </div>
      <div class="siso-summary-line">
        <span class="summary-chip">exact families ${familyCount}</span>
        <span class="summary-chip">included paths ${includedCount}</span>
      </div>
      <div class="plot-container siso-active-plot" id="${nodeId}-traj-plot" style="display:none;"></div>
    </section>
  `;
}

function sisoConditionPanelId(nodeId, pathIdx) {
  return `${nodeId}-path-condition-${pathIdx}`;
}

function renderSISOConditionPanel(nodeId, pathIdx) {
  return `
    <div
      class="siso-condition-panel"
      id="${sisoConditionPanelId(nodeId, pathIdx)}"
      data-loaded="false"
      style="display:none;"
    >
      <span class="text-dim">Condition not loaded.</span>
    </div>
  `;
}

function renderSISOConditionContent(data) {
  const conditions = data.conditions || [];
  const conditionRows = conditions.length
    ? conditions.map((condition, idx) => `
        <div class="siso-condition-row">
          <span class="siso-condition-index">C${idx + 1}</span>
          <code>${escapeHtml(condition)}</code>
        </div>
      `).join('')
    : '<div class="text-dim">No path conditions returned.</div>';

  const qkSymbols = (data.qk_symbols || []).map(escapeHtml).join(', ') || 'n/a';
  return `
    <div class="siso-condition-meta">
      <span class="summary-chip">Path #${data.path_idx}</span>
      <span class="summary-chip">Fixed coordinates: ${qkSymbols}</span>
    </div>
    <div class="siso-condition-list">${conditionRows}</div>
  `;
}

export function buildSISOSelection(nodeId, changeQK, pathIdx) {
  const nodeData = nodeRegistry[nodeId]?.data;
  const behaviorData = nodeData?.behaviorData;
  if (!behaviorData) return null;

  const path = (behaviorData.paths || []).find(p => p.path_idx === pathIdx);
  if (!path) return null;

  const { exactFamilyByPath } = buildPathFamilyMaps(behaviorData);
  return {
    path_idx: pathIdx,
    change_qK: behaviorData.change_qK || changeQK,
    observe_x: behaviorData.observe_x,
    exact_family_idx: exactFamilyByPath.get(pathIdx) || null,
    exact_label: path.exact_label,
    feasible: path.feasible,
    included: path.included,
    vertex_indices: path.vertex_indices,
    perms: path.perms,
  };
}

export function setSISOSelection(nodeId, changeQK, pathIdx) {
  const nodeData = nodeRegistry[nodeId]?.data;
  if (!nodeData) return null;
  const selection = buildSISOSelection(nodeId, changeQK, pathIdx);
  if (!selection) return null;
  nodeData.selectedPath = selection;
  commitWorkspaceSnapshot('siso-selection');
  triggerDownstreamNodes(nodeId, 'result');
  return selection;
}

export function clearSISOSelection(nodeId, notify = true) {
  const nodeData = nodeRegistry[nodeId]?.data;
  if (!nodeData) return;
  nodeData.selectedPath = null;
  commitWorkspaceSnapshot('siso-selection-cleared');
  if (notify) triggerDownstreamNodes(nodeId, 'result');
}

export function renderPathChips(nodeId, changeQK, pathIndices, accent) {
  const selectedPathIdx = selectedPathIdxForNode(nodeId);
  return (pathIndices || []).map(pathIdx => `
    <button
      type="button"
      class="path-chip ${selectedPathIdx === pathIdx ? 'selected' : ''}"
      data-path-idx="${pathIdx}"
      style="--path-chip-accent:${accent}; --path-chip-soft:${hexToRgba(accent, 0.16)};"
      data-action="plotSISOPath"
      data-node="${nodeId}" data-qk="${changeQK}" data-idx="${pathIdx}"
    >#${pathIdx}</button>
  `).join('');
}

export function renderFamilyTable(nodeId, changeQK, families) {
  if (!families.length) return '';

  const rows = families.map(family => {
    const accent = getFamilyColor(family.family_idx, 0);
    const badgeStyle = `--badge-accent:${accent}; --badge-soft:${hexToRgba(accent, 0.18)};`;
    const profile = family.exact_label;

    return `
      <tr>
        <td><span class="family-badge family-badge-exact" style="${badgeStyle}">E${family.family_idx}</span></td>
        <td class="siso-profile-cell">${profile}</td>
        <td>
          <div class="family-path-chips">
            ${renderPathChips(nodeId, changeQK, family.path_indices, accent)}
          </div>
        </td>
        <td>${formatVolumeSummary(family.total_volume)}</td>
      </tr>
    `;
  }).join('');

  const headerRow = '<tr><th>#</th><th>RO profile</th><th>Paths</th><th>Volume</th></tr>';

  return `
    <section class="siso-section">
      <div class="siso-section-head">
        <div class="siso-section-title">Exact Families</div>
        <div class="text-dim">${families.length} families</div>
      </div>
      <div class="siso-table-wrap scroll-panel">
        <table class="siso-family-table">
          <thead>${headerRow}</thead>
          <tbody>${rows}</tbody>
        </table>
      </div>
    </section>
  `;
}

export function renderBehaviorFamiliesResult(nodeId, changeQK, data) {
  const { exactFamilyByPath } = buildPathFamilyMaps(data);
  const selectedPathIdx = selectedPathIdxForNode(nodeId);
  const feasiblePaths = (data.paths || [])
    .filter(path => path.feasible)
    .sort((a, b) => {
      const exactA = exactFamilyByPath.get(a.path_idx) || Number.MAX_SAFE_INTEGER;
      const exactB = exactFamilyByPath.get(b.path_idx) || Number.MAX_SAFE_INTEGER;
      return exactA - exactB || a.path_idx - b.path_idx;
    });

  let html = '';

  html += renderFamilyTable(nodeId, changeQK, data.exact_families || []);

  html += `
    <section class="siso-section">
      <div class="siso-section-head">
        <div class="siso-section-title">Feasible Paths</div>
        <div class="text-dim">Sorted by exact family</div>
      </div>
      <div class="path-list siso-feasible-list scroll-panel">
  `;

  feasiblePaths.forEach(path => {
    const permStr = path.perms.map(pr => `[${pr.join(',')}]`).join(' → ');
    const exactFamilyIdx = exactFamilyByPath.get(path.path_idx);
    const exactAccent = getFamilyColor(exactFamilyIdx || path.path_idx, 0);
    const includeTag = path.included
      ? '<span class="tag tag-asym">Included</span>'
      : `<span class="tag tag-nonasym">${path.exclusion_reason || 'Excluded'}</span>`;

    html += `
      <div
        class="path-item siso-path-item ${path.included ? 'is-included' : 'is-excluded'} ${selectedPathIdx === path.path_idx ? 'selected' : ''}"
        data-idx="${path.path_idx}"
        data-path-idx="${path.path_idx}"
        data-qk="${changeQK}"
        data-node="${nodeId}"
        style="--exact-accent:${exactAccent}; --exact-soft:${hexToRgba(exactAccent, 0.14)};"
        data-action="selectSISOPath"
      >
        <div class="siso-path-head">
          <div class="siso-path-title">Path #${path.path_idx}</div>
          <div class="siso-path-actions">
            <button type="button" class="btn btn-small siso-inline-btn" data-action="plotSISOPath" data-node="${nodeId}" data-qk="${changeQK}" data-idx="${path.path_idx}">Plot</button>
            <button type="button" class="btn btn-small siso-inline-btn" data-action="toggleSISOPathCondition" data-node="${nodeId}" data-qk="${changeQK}" data-idx="${path.path_idx}">Condition</button>
          </div>
        </div>
        <div class="siso-path-badges">
          ${exactFamilyIdx ? `<span class="family-badge family-badge-exact" style="--badge-accent:${exactAccent}; --badge-soft:${hexToRgba(exactAccent, 0.18)};">Exact ${exactFamilyIdx}</span>` : ''}
          ${includeTag}
        </div>
        <div class="siso-path-detail">${permStr}</div>
        <div class="siso-path-meta">
          <span>RO ${path.exact_label}</span>
          <span>Vol ${formatVolumeSummary(path.volume)}</span>
        </div>
        ${renderSISOConditionPanel(nodeId, path.path_idx)}
      </div>
    `;
  });

  html += `
      </div>
    </section>
  `;

  html += renderSISOPlotControls(nodeId, data);
  return html;
}

function renderSISOBehaviorOverlayPlot(nodeId, overlayData) {
  const plotId = `${nodeId}-traj-plot`;
  const plotEl = document.getElementById(plotId);
  if (!plotEl || !overlayData) return;

  const traces = (overlayData.trajectories || []).map((trajectory, idx) => {
    const accent = trajectory.exact_family_idx
      ? getFamilyColor(trajectory.exact_family_idx, 0)
      : getFamilyColor(idx + 1, 0);
    const familyLabel = trajectory.exact_family_idx ? `E${trajectory.exact_family_idx}` : 'unclassified';
    return {
      x: trajectory.change_values,
      y: trajectory.output_values,
      type: 'scatter',
      mode: 'lines',
      name: `Path #${trajectory.path_idx}`,
      customdata: trajectory.change_values.map(() => [
        trajectory.path_idx,
        familyLabel,
        trajectory.exact_label || 'n/a',
        trajectory.included ? 'included' : (trajectory.exclusion_reason || 'excluded'),
      ]),
      line: {
        color: accent,
        width: trajectory.included ? 1.8 : 1.2,
      },
      opacity: trajectory.included ? 0.88 : 0.42,
      hovertemplate: 'Path #%{customdata[0]} · %{customdata[1]}<br>log input=%{x:.3g}<br>log output=%{y:.3g}<br>%{customdata[2]}<br>%{customdata[3]}<extra></extra>',
    };
  });

  if (!traces.length) {
    Plotly.purge(plotEl);
    plotEl.style.display = 'none';
    return;
  }

  const plotTheme = getPlotTheme();
  const hiddenFailures = (overlayData.failures || []).length;
  const titleSuffix = hiddenFailures ? ` (${hiddenFailures} failed)` : '';
  const layout = {
    showlegend: traces.length <= 80,
    margin: { t: 42, b: 58, l: 70, r: 20 },
    title: {
      text: `All paths: ${overlayData.change_qK} -> ${overlayData.observe_x}${titleSuffix}`,
      font: { color: plotTheme.titleColor, size: 11 },
      y: 0.98,
      yanchor: 'top',
    },
    xaxis: { title: `log ${overlayData.change_qK}` },
    yaxis: { title: `log(${overlayData.observe_x})` },
    legend: { font: { color: plotTheme.fontColor, size: 9 } },
  };

  plotEl.style.display = '';
  Plotly.newPlot(plotId, traces, applyPlotLayoutTheme(layout), { responsive: true, displayModeBar: false, scrollZoom: true });
  setupPlotInteractionGuard(plotEl);
  setupPlotResize(nodeId, plotId);
}

export function plotSISOBehaviorOverlay(nodeId) {
  renderSISOBehaviorOverlayPlot(nodeId, nodeRegistry[nodeId]?.data?.overlayTrajectoryData);
}

export async function loadAndPlotSISOBehaviorOverlay(nodeId, { force = false } = {}) {
  const nodeData = nodeRegistry[nodeId]?.data;
  const data = nodeData?.behaviorData;
  const config = getConnectedSISOConfig(nodeId);
  const sessionId = getSessionIdForNode(nodeId);
  const plotEl = document.getElementById(`${nodeId}-traj-plot`);
  if (!plotEl || !data || !config || !sessionId) return;

  const paths = sisoOverlayPathCandidates(data);
  if (!paths.length) {
    if (nodeData) nodeData.overlayTrajectoryData = null;
    Plotly.purge(plotEl);
    plotEl.style.display = 'none';
    return;
  }

  const pathIndices = paths.map(path => path.path_idx);
  const requestKey = sisoOverlayRequestKey(data, config, pathIndices);
  if (!force && nodeData?.overlayTrajectoryData?.requestKey === requestKey) {
    renderSISOBehaviorOverlayPlot(nodeId, nodeData.overlayTrajectoryData);
    return;
  }

  const requestId = (nodeData?.sisoOverlayRequestId || 0) + 1;
  if (nodeData) nodeData.sisoOverlayRequestId = requestId;

  const { exactFamilyByPath } = buildPathFamilyMaps(data);
  setNodeLoading(nodeId, true);
  try {
    const results = await mapWithConcurrency(paths, SISO_OVERLAY_CONCURRENCY, async path => {
      try {
        const trajectory = await api('siso_trajectory', {
          session_id: sessionId,
          change_qK: data.change_qK || config.change_qK,
          path_idx: path.path_idx,
          start: config?.min ?? -6,
          stop: config?.max ?? 6,
        });
        const outputIdx = trajectoryOutputIndex(trajectory, data);
        const outputValues = (trajectory.logx || []).map(row => row?.[outputIdx]).map(Number);
        return {
          ok: true,
          path_idx: path.path_idx,
          exact_family_idx: exactFamilyByPath.get(path.path_idx) || null,
          exact_label: path.exact_label,
          included: path.included,
          exclusion_reason: path.exclusion_reason,
          change_values: trajectory.change_values || [],
          output_values: outputValues,
        };
      } catch (error) {
        return {
          ok: false,
          path_idx: path.path_idx,
          error: error?.message || String(error),
        };
      }
    });

    if (nodeRegistry[nodeId]?.data?.sisoOverlayRequestId !== requestId) return;

    const overlayData = {
      requestKey,
      change_qK: data.change_qK || config.change_qK,
      observe_x: data.observe_x || config.observe_x,
      start: config?.min ?? -6,
      stop: config?.max ?? 6,
      trajectories: results.filter(result => result?.ok),
      failures: results.filter(result => result && !result.ok),
    };

    if (nodeRegistry[nodeId]) {
      ensureNodeData(nodeId).overlayTrajectoryData = overlayData;
    }
    renderSISOBehaviorOverlayPlot(nodeId, overlayData);
    commitWorkspaceSnapshot('siso-all-path-overlay');
  } finally {
    setNodeLoading(nodeId, false);
  }
}

export async function refreshSISOPlot(nodeId) {
  const mode = getSISOPlotMode(nodeId);
  if (mode === SISO_PLOT_MODE_OVERLAY) {
    await loadAndPlotSISOBehaviorOverlay(nodeId, { force: true });
    return;
  }

  const selection = getNodeData(nodeId).selectedPath;
  if (selection?.path_idx != null) {
    await plotSISOPath(nodeId, selection.change_qK, selection.path_idx);
    return;
  }

  const plotEl = document.getElementById(`${nodeId}-traj-plot`);
  if (plotEl) {
    Plotly.purge(plotEl);
    plotEl.style.display = 'none';
  }
}

export async function updateSISOPlotMode(nodeId, mode) {
  const safeMode = setSISOPlotMode(nodeId, mode);
  commitWorkspaceSnapshot('siso-plot-mode');
  if (safeMode === SISO_PLOT_MODE_OVERLAY) {
    await loadAndPlotSISOBehaviorOverlay(nodeId);
    return;
  }
  await refreshSISOPlot(nodeId);
}

export function normalizeSISOConfig(rawConfig) {
  if (!rawConfig) return null;
  const nested = rawConfig.config || {};
  return {
    change_qK: rawConfig.change_qK ?? rawConfig.changeQK ?? nested.change_qK ?? nested.changeQK ?? '',
    observe_x: rawConfig.observe_x ?? rawConfig.observeX ?? nested.observe_x ?? nested.observeX ?? '',
    path_scope: rawConfig.path_scope ?? rawConfig.pathScope ?? nested.path_scope ?? nested.pathScope ?? 'feasible',
    min_volume_mean: rawConfig.min_volume_mean ?? rawConfig.minVolumeMean ?? nested.min_volume_mean ?? nested.minVolumeMean ?? 0,
    keep_singular: rawConfig.keep_singular ?? rawConfig.keepSingular ?? nested.keep_singular ?? nested.keepSingular ?? true,
    keep_nonasymptotic: rawConfig.keep_nonasymptotic ?? rawConfig.keepNonasymptotic ?? nested.keep_nonasymptotic ?? nested.keepNonasymptotic ?? false,
    min: rawConfig.min ?? nested.min ?? -6,
    max: rawConfig.max ?? nested.max ?? 6,
  };
}

export function getConnectedSISOConfig(resultNodeId) {
  const paramsConn = connections.find(c => c.toNode === resultNodeId && c.toPort === 'params');
  if (!paramsConn) return null;
  const paramsNodeId = paramsConn.fromNode;
  const liveConfig = normalizeSISOConfig(getNodeSerialData(paramsNodeId, 'siso-params'));
  if (liveConfig) {
    if (nodeRegistry[paramsNodeId]) {
      ensureNodeData(paramsNodeId).config = liveConfig;
    }
    return liveConfig;
  }
  return normalizeSISOConfig(getNodeData(paramsNodeId).config);
}

export async function computeSISOResult(nodeId) {
  // Find the connected params node
  const paramsConn = connections.find(c => c.toNode === nodeId && c.toPort === 'params');
  if (!paramsConn) {
    showToast('Connect a SISO Config node first');
    return;
  }

  const paramsNode = nodeRegistry[paramsConn.fromNode];
  const config = getConnectedSISOConfig(nodeId);
  if (!paramsNode || !config) {
    showToast('SISO Config node has no configuration');
    return;
  }

  setNodeLoading(nodeId, true);
  const contentEl = document.getElementById(`${nodeId}-content`);
  const previousSelectedPath = getNodeData(nodeId).selectedPath?.path_idx || null;
  const plotMode = getSISOPlotMode(nodeId);

  try {
    const sessionId = getSessionIdForNode(nodeId);
    if (!sessionId) throw new Error('Build the connected model first');
    if (nodeRegistry[nodeId]) {
      const nd = ensureNodeData(nodeId);
      nd.sisoTrajectoryRequestId = (nd.sisoTrajectoryRequestId || 0) + 1;
    }
    const data = await api('behavior_families', {
      session_id: sessionId,
      change_qK: config.change_qK,
      observe_x: config.observe_x,
      path_scope: config.path_scope,
      min_volume_mean: config.min_volume_mean,
      keep_singular: config.keep_singular,
      keep_nonasymptotic: config.keep_nonasymptotic,
      deduplicate: true,
      compute_volume: true,
    });
    if (nodeRegistry[nodeId]) {
      ensureNodeData(nodeId).behaviorData = data;
    }
    contentEl.innerHTML = renderBehaviorFamiliesResult(nodeId, config.change_qK, data);
    commitWorkspaceSnapshot('siso-behavior');
    const pathStillExists = previousSelectedPath && (data.paths || []).some(path => path.path_idx === previousSelectedPath);
    if (plotMode === SISO_PLOT_MODE_OVERLAY) {
      if (pathStillExists) setSISOSelection(nodeId, config.change_qK, previousSelectedPath);
      else clearSISOSelection(nodeId, previousSelectedPath !== null);
      await loadAndPlotSISOBehaviorOverlay(nodeId, { force: true });
    } else if (pathStillExists) {
      await plotSISOPath(nodeId, config.change_qK, previousSelectedPath);
    } else {
      clearSISOSelection(nodeId, previousSelectedPath !== null);
    }
  } catch (e) {
    handleNodeError(e, nodeId, 'SISO behavior analysis');
    contentEl.innerHTML = `<div class="node-error">${e.message}</div>`;
  } finally {
    setNodeLoading(nodeId, false);
  }
}

export function recomputeROPCloud(nodeId) {
  const typeDef = NODE_TYPES['rop-cloud'];
  if (typeDef.execute) typeDef.execute(nodeId);
}

export function recomputeHeatmap(nodeId) {
  const typeDef = NODE_TYPES['fret-heatmap'];
  if (typeDef.execute) typeDef.execute(nodeId);
}

// ===== SISO Path Selection =====
export async function plotSISOPath(nodeId, changeQK, pathIdx, selectedEl = null) {
  const config = getConnectedSISOConfig(nodeId);
  const sessionId = getSessionIdForNode(nodeId);
  if (!sessionId) return;
  setSISOPlotMode(nodeId, SISO_PLOT_MODE_SINGLE);
  const nodeData = nodeRegistry[nodeId]?.data;
  const requestId = (nodeData?.sisoTrajectoryRequestId || 0) + 1;
  if (nodeData) {
    nodeData.sisoTrajectoryRequestId = requestId;
  }
  const contentEl = document.getElementById(`${nodeId}-content`);
  if (contentEl) {
    contentEl.querySelectorAll('.path-item, .path-chip').forEach(p => {
      const currentIdx = parseInt(p.dataset.pathIdx || p.dataset.idx, 10);
      p.classList.toggle('selected', currentIdx === pathIdx);
    });
  }
  setSISOSelection(nodeId, changeQK, pathIdx);
  if (contentEl && selectedEl) {
    const listItem = contentEl.querySelector(`.path-item[data-path-idx="${pathIdx}"]`);
    if (listItem && listItem !== selectedEl) {
      listItem.scrollIntoView({ block: 'nearest', behavior: 'smooth' });
    }
  }
  try {
    const data = await api('siso_trajectory', {
      session_id: sessionId,
      change_qK: changeQK,
      path_idx: pathIdx,
      start: config?.min ?? -6,
      stop: config?.max ?? 6,
    });
    if (nodeRegistry[nodeId]?.data?.sisoTrajectoryRequestId !== requestId) return;
    if (nodeRegistry[nodeId]) {
      ensureNodeData(nodeId).trajectoryData = data;
    }
    const plotEl = document.getElementById(`${nodeId}-traj-plot`);
    if (plotEl) {
      plotEl.style.display = '';
      plotTrajectory(data, `${nodeId}-traj-plot`);
    }
    commitWorkspaceSnapshot('siso-trajectory');
  } catch (e) {
    handleNodeError(e, nodeId, 'SISO trajectory');
  }
}

export async function selectSISOPath(el) {
  const pathIdx = parseInt(el.dataset.idx);
  const changeQK = el.dataset.qk;
  const nodeId = el.dataset.node;
  await plotSISOPath(nodeId, changeQK, pathIdx, el);
}

export async function toggleSISOPathCondition(el) {
  const pathIdx = parseInt(el.dataset.idx, 10);
  const changeQK = el.dataset.qk;
  const nodeId = el.dataset.node;
  const panel = document.getElementById(sisoConditionPanelId(nodeId, pathIdx));
  if (!panel) return;

  const isVisible = panel.style.display !== 'none';
  if (isVisible) {
    panel.style.display = 'none';
    el.classList.remove('active');
    return;
  }

  panel.style.display = '';
  el.classList.add('active');
  if (panel.dataset.loaded === 'true') return;

  panel.innerHTML = '<span class="text-dim">Loading path condition...</span>';
  try {
    const sessionId = getSessionIdForNode(nodeId);
    if (!sessionId) throw new Error('Build the connected model first');
    const data = await api('siso_path_condition', {
      session_id: sessionId,
      change_qK: changeQK,
      path_idx: pathIdx,
    });
    panel.innerHTML = renderSISOConditionContent(data);
    panel.dataset.loaded = 'true';
  } catch (error) {
    handleNodeError(error, nodeId, 'SISO path condition');
    panel.innerHTML = `<div class="node-error">${escapeHtml(error?.message || String(error))}</div>`;
  }
}

export function getConnectedSISOSelection(nodeId) {
  const conn = connections.find(c => c.toNode === nodeId && c.toPort === 'result');
  if (!conn) return null;
  const sourceInfo = nodeRegistry[conn.fromNode];
  if (!sourceInfo) return null;
  return {
    sourceNodeId: conn.fromNode,
    sourceInfo,
    selection: sourceInfo.data?.selectedPath || null,
  };
}

export function plotQKPolyhedron(polyData, qkSymbols, plotId) {
  if (!polyData || polyData.dimension !== 2 || !polyData.is_bounded || !(polyData.vertices || []).length) return;

  const plotTheme = getPlotTheme();
  const points = polyData.vertices.map(vertex => ({ x: Number(vertex[0]), y: Number(vertex[1]) }))
    .filter(point => Number.isFinite(point.x) && Number.isFinite(point.y));
  if (!points.length) return;

  const hull = convexHull2D(points);
  const traces = [];
  if (hull.length >= 2) {
    const closed = hull.concat(hull[0]);
    traces.push({
      x: closed.map(point => point.x),
      y: closed.map(point => point.y),
      mode: 'lines',
      type: 'scatter',
      fill: 'toself',
      fillcolor: 'rgba(108, 140, 255, 0.14)',
      line: { color: '#6c8cff', width: 2 },
      name: 'Boundary',
      hoverinfo: 'skip',
    });
  }
  traces.push({
    x: points.map(point => point.x),
    y: points.map(point => point.y),
    mode: 'markers',
    type: 'scatter',
    marker: { color: '#ff922b', size: 8 },
    name: 'Vertices',
    hovertemplate: `${qkSymbols[0]}=%{x:.3f}<br>${qkSymbols[1]}=%{y:.3f}<extra></extra>`,
  });

  const layout = {
    autosize: true,
    margin: { t: 40, b: 60, l: 70, r: 20 },
    title: { text: 'qK-space Polyhedron', font: { color: plotTheme.titleColor, size: 11 }, y: 0.98, yanchor: 'top' },
    xaxis: { title: qkSymbols[0] },
    yaxis: { title: qkSymbols[1] },
    showlegend: true,
  };

  Plotly.newPlot(plotId, traces, applyPlotLayoutTheme(layout), { responsive: true, displayModeBar: false, scrollZoom: true });
}

export function renderQKPolyhedronResult(nodeId, selection, payload) {
  const poly = payload.polyhedra?.[0];
  if (!poly) {
    return { html: '<div class="node-error">No polyhedron data returned for the selected path.</div>', canPlot: false };
  }

  const qkSymbols = payload.qk_symbols || [];
  const linearConstraints = new Set(poly.linear_constraints || []);
  const rayLineality = new Set(poly.ray_lineality || []);
  const vertices = poly.vertices || [];
  const rays = poly.rays || [];
  const canPlot = poly.dimension === 2 && poly.is_bounded && vertices.length > 0;

  const constraintRows = (poly.A || []).map((row, idx) => `
    <tr>
      <td>C${idx + 1}</td>
      <td class="siso-profile-cell">${formatPolyConstraint(row, poly.b?.[idx], qkSymbols, linearConstraints.has(idx + 1))}</td>
    </tr>
  `).join('');

  let html = `
    <div class="siso-summary-line">
      <span class="summary-chip"><strong>Path #${selection.path_idx}</strong></span>
      ${selection.exact_family_idx ? `<span class="family-badge family-badge-exact">E${selection.exact_family_idx}</span>` : ''}
      <span class="summary-chip">${selection.observe_x}</span>
      <span class="summary-chip">${payload.change_qK} scanned</span>
    </div>
    <div class="siso-scope-note">
      Fixed coordinates: ${qkSymbols.length ? qkSymbols.join(', ') : 'n/a'}<br>
      Dimension ${poly.dimension}, constraints ${poly.n_constraints ?? (poly.A || []).length}, vertices ${poly.n_vertices ?? vertices.length}, rays ${poly.n_rays ?? rays.length}.
    </div>
  `;

  if (!canPlot) {
    html += `
      <div class="text-dim">
        Direct geometric plotting is only shown for bounded 2D path polyhedra. This selected path is ${poly.dimension}D${poly.is_bounded ? '' : ' and unbounded'}.
      </div>
    `;
  } else {
    html += `<div class="plot-container" id="${nodeId}-plot"></div>`;
  }

  html += `
    <section class="siso-section">
      <div class="siso-section-head">
        <div class="siso-section-title">H-Representation</div>
        <div class="text-dim">${(poly.A || []).length} rows</div>
      </div>
      <div class="siso-table-wrap scroll-panel">
        <table class="siso-family-table">
          <thead><tr><th>#</th><th>Constraint</th></tr></thead>
          <tbody>${constraintRows || '<tr><td colspan="2" class="text-dim">No constraints</td></tr>'}</tbody>
        </table>
      </div>
    </section>
  `;

  if (vertices.length) {
    html += `
      <section class="siso-section">
        <div class="siso-section-head">
          <div class="siso-section-title">Vertices</div>
          <div class="text-dim">${vertices.length} points</div>
        </div>
        ${renderPolyCoordinateTable(vertices, qkSymbols, 'vertices')}
      </section>
    `;
  }

  if (rays.length) {
    html += `
      <section class="siso-section">
        <div class="siso-section-head">
          <div class="siso-section-title">Rays</div>
          <div class="text-dim">${rays.length} directions</div>
        </div>
        ${renderPolyCoordinateTable(rays, qkSymbols, 'rays', rayLineality)}
      </section>
    `;
  }

  return { html, canPlot };
}

export async function executeQKPolyResult(nodeId) {
  const contentEl = document.getElementById(`${nodeId}-content`);
  const source = getConnectedSISOSelection(nodeId);
  if (!source) {
    if (contentEl) contentEl.innerHTML = '<span class="text-dim">Connect to a SISO Behaviors node first.</span>';
    return;
  }

  const selection = source.selection;
  if (!selection) {
    if (contentEl) contentEl.innerHTML = '<span class="text-dim">Select a path in the upstream SISO Behaviors node.</span>';
    return;
  }

  setNodeLoading(nodeId, true);
  try {
    const sessionId = getSessionIdForNode(nodeId);
    if (!sessionId) throw new Error('Build the connected model first');
    const payload = await api('siso_polyhedra', {
      session_id: sessionId,
      change_qK: selection.change_qK,
      path_indices: [selection.path_idx],
    });
    if (nodeRegistry[nodeId]) {
      const nd = ensureNodeData(nodeId);
      nd.selection = selection;
      nd.polyhedronPayload = payload;
    }
    const rendered = renderQKPolyhedronResult(nodeId, selection, payload);
    contentEl.innerHTML = rendered.html;
    commitWorkspaceSnapshot('qk-polyhedron');
    if (rendered.canPlot) {
      setTimeout(() => {
        plotQKPolyhedron(payload.polyhedra?.[0], payload.qk_symbols || [], `${nodeId}-plot`);
        setupPlotResize(nodeId, `${nodeId}-plot`);
      }, 50);
    }
  } catch (e) {
    handleNodeError(e, nodeId, 'qK polyhedron');
    contentEl.innerHTML = `<div class="node-error">${e.message}</div>`;
  } finally {
    setNodeLoading(nodeId, false);
  }
}
