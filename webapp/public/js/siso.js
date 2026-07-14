import {
  nodeRegistry,
  connections,
  ensureNodeData,
  getNodeData,
  getWorkspaceRuntimeEpoch,
} from './state.js';
import { api, showToast, handleNodeError, renderNodeError, escapeHtml } from './api.js';
import { hexToRgba, getFamilyColor, applyPlotLayoutTheme, getPlotTheme } from './theme.js';
import { plotTrajectory, convexHull2D, formatPolyConstraint, renderPolyCoordinateTable } from './plotting.js';
import { setNodeLoading, setupPlotResize, setupPlotInteractionGuard, getSessionIdForNode, findUpstreamNodeByType, ensureModelSession } from './nodes.js';
import { triggerDownstreamNodes } from './model.js';
import { commitWorkspaceSnapshot, getNodeSerialData } from './workspace.js';
import { NODE_TYPES } from './node-types/index.js';
import {
  blockedOutcome,
  failedOutcome,
  staleOutcome,
  succeededOutcome,
} from './execution-outcome.js';
import {
  begin as beginLifecycle,
  block as blockLifecycle,
  commit as commitLifecycle,
  createExecutionLifecycle,
  fail as failLifecycle,
  inspectExecutionLifecycle,
  invalidate as invalidateLifecycle,
  isCurrent as isCurrentLifecycle,
  release as releaseLifecycle,
  restoreHistorical as restoreHistoricalLifecycle,
  serializeExecutionLifecycle,
} from './execution-lifecycle-core.js';

const SISO_PLOT_MODE_SINGLE = 'single';
const SISO_PLOT_MODE_OVERLAY = 'overlay';
const SISO_PLOT_MODE_VALUES = new Set([SISO_PLOT_MODE_SINGLE, SISO_PLOT_MODE_OVERLAY]);
const SISO_OVERLAY_CONCURRENCY = 4;
const SISO_ENDPOINT = '/api/v1/behavior_families';
const QK_POLY_ENDPOINT = '/api/v1/siso_polyhedra';
const SISO_LIFECYCLE_KEY = '_sisoResultLifecycle';
const QK_POLY_LIFECYCLE_KEY = '_qkPolyResultLifecycle';

function canonicalLifecycleValue(value) {
  if (Array.isArray(value)) return value.map(canonicalLifecycleValue);
  if (!value || typeof value !== 'object') return value;
  const normalized = {};
  for (const key of Object.keys(value).sort()) {
    normalized[key] = canonicalLifecycleValue(value[key]);
  }
  return normalized;
}

function stableLifecycleFingerprint(value) {
  return JSON.stringify(canonicalLifecycleValue(value));
}

function upstreamConnectionIdentity(nodeId) {
  const visited = new Set();
  const edges = [];
  const visit = current => {
    if (!current || visited.has(current)) return;
    visited.add(current);
    for (const connection of connections) {
      if (connection?.toNode !== current) continue;
      edges.push([
        connection.fromNode,
        connection.fromPort,
        connection.toNode,
        connection.toPort,
      ]);
      visit(connection.fromNode);
    }
  };
  visit(nodeId);
  return edges.sort((left, right) => JSON.stringify(left).localeCompare(JSON.stringify(right)));
}

function modelIdentityForNode(nodeId) {
  const builderId = findUpstreamNodeByType(nodeId, 'model-builder');
  const modelContext = builderId ? nodeRegistry[builderId]?.data?.modelContext : null;
  return {
    builderId: builderId || null,
    inputFingerprint: modelContext?.inputFingerprint || null,
    networkIrHash: modelContext?.networkIrHash || modelContext?.model?.network_ir_hash || null,
    networkIr: modelContext?.networkIr || modelContext?.model?.network_ir || null,
  };
}

function lifecycleContext(owner, endpoint, inputFingerprint) {
  if (!owner) return null;
  return {
    owner,
    workspaceEpoch: getWorkspaceRuntimeEpoch(),
    inputFingerprint,
    endpoint,
  };
}

function sisoResultSnapshot(info) {
  return {
    behaviorData: info?.data?.behaviorData || null,
    selectedPath: info?.data?.selectedPath || null,
    trajectoryData: info?.data?.trajectoryData || null,
    overlayTrajectoryData: info?.data?.overlayTrajectoryData || null,
  };
}

function qkPolyResultSnapshot(info) {
  return {
    selection: info?.data?.selection || null,
    polyhedronPayload: info?.data?.polyhedronPayload || null,
  };
}

function hasSISOStoredResult(info) {
  return !!(info?.data?.behaviorData || info?.data?.selectedPath || info?.data?.trajectoryData);
}

function hasQKStoredResult(info) {
  return !!(info?.data?.selection || info?.data?.polyhedronPayload);
}

function currentSISOResultContext(nodeId) {
  const owner = nodeRegistry[nodeId];
  if (!owner) return null;
  let config = null;
  try {
    config = getConnectedSISOConfig(nodeId);
  } catch {
    config = null;
  }
  return lifecycleContext(owner, SISO_ENDPOINT, stableLifecycleFingerprint({
    connections: upstreamConnectionIdentity(nodeId),
    config,
    model: modelIdentityForNode(nodeId),
  }));
}

function currentQKPolyContext(nodeId) {
  const owner = nodeRegistry[nodeId];
  if (!owner) return null;
  const connection = connections.find(candidate =>
    candidate.toNode === nodeId && candidate.toPort === 'result');
  const sourceInfo = connection ? nodeRegistry[connection.fromNode] : null;
  const sourceLifecycle = sourceInfo
    ? lifecycleFor(sourceInfo, SISO_LIFECYCLE_KEY, {
      endpoint: SISO_ENDPOINT,
      contextFactory: () => currentSISOResultContext(connection.fromNode),
      hasStoredResult: hasSISOStoredResult,
      snapshot: sisoResultSnapshot,
    })
    : null;
  const sourceRuntime = sourceLifecycle ? inspectExecutionLifecycle(sourceLifecycle) : null;
  return lifecycleContext(owner, QK_POLY_ENDPOINT, stableLifecycleFingerprint({
    connections: upstreamConnectionIdentity(nodeId),
    model: modelIdentityForNode(nodeId),
    sourceNodeId: connection?.fromNode || null,
    sourceFingerprint: sourceRuntime?.inputFingerprint || null,
    sourceFreshness: sourceRuntime?.freshness || 'empty',
    selection: sourceRuntime?.freshness === 'current' ? sourceInfo?.data?.selectedPath || null : null,
  }));
}

function lifecycleFor(info, key, {
  endpoint,
  contextFactory,
  hasStoredResult,
  snapshot,
}) {
  if (!info[key]) {
    const lifecycle = createExecutionLifecycle();
    info[key] = lifecycle;
    if (hasStoredResult(info)) {
      const context = contextFactory();
      if (context) {
        restoreHistoricalLifecycle(lifecycle, {
          context: { ...context, endpoint },
          result: snapshot(info),
          evidence: info.data?.lifecycle?.evidence || null,
        });
        syncLifecycle(info, lifecycle);
      }
    }
  }
  return info[key];
}

function sisoLifecycleFor(info) {
  return lifecycleFor(info, SISO_LIFECYCLE_KEY, {
    endpoint: SISO_ENDPOINT,
    contextFactory: () => {
      const nodeId = Object.keys(nodeRegistry).find(id => nodeRegistry[id] === info);
      return nodeId ? currentSISOResultContext(nodeId) : null;
    },
    hasStoredResult: hasSISOStoredResult,
    snapshot: sisoResultSnapshot,
  });
}

function qkPolyLifecycleFor(info) {
  return lifecycleFor(info, QK_POLY_LIFECYCLE_KEY, {
    endpoint: QK_POLY_ENDPOINT,
    contextFactory: () => {
      const nodeId = Object.keys(nodeRegistry).find(id => nodeRegistry[id] === info);
      return nodeId ? currentQKPolyContext(nodeId) : null;
    },
    hasStoredResult: hasQKStoredResult,
    snapshot: qkPolyResultSnapshot,
  });
}

function syncLifecycle(info, lifecycle) {
  if (!info || !lifecycle) return;
  info.data = info.data || {};
  info.data.lifecycle = serializeExecutionLifecycle(lifecycle);
}

function clearTimer(info, key) {
  if (info?.[key] == null) return;
  clearTimeout(info[key]);
  delete info[key];
}

function settleLoading(nodeId, lifecycle, ticket) {
  if (!ticket || nodeRegistry[nodeId] !== ticket.owner) return;
  const runtime = inspectExecutionLifecycle(lifecycle);
  if (runtime.currentTicket === ticket ||
      (runtime.currentTicket == null && runtime.loading === false)) {
    setNodeLoading(nodeId, false);
  }
}

function invalidateBoundLifecycle(info, lifecycle, reason) {
  const runtime = inspectExecutionLifecycle(lifecycle);
  if (runtime.owner !== info || runtime.workspaceEpoch == null) return false;
  return invalidateLifecycle(lifecycle, {
    owner: info,
    workspaceEpoch: runtime.workspaceEpoch,
    reason,
  });
}

function setResultMessage(nodeId, message) {
  const contentEl = document.getElementById(`${nodeId}-content`);
  if (!contentEl) return;
  contentEl.dataset.resultState = 'invalidated';
  contentEl.innerHTML = `<span class="text-dim">${escapeHtml(message)}</span>`;
}

export function invalidateSISOResult(nodeId, reason = 'siso-input-changed') {
  const info = nodeRegistry[nodeId];
  if (!info || info.type !== 'siso-result') return false;
  const lifecycle = sisoLifecycleFor(info);
  invalidateBoundLifecycle(info, lifecycle, reason);
  clearTimer(info, '_sisoPlotTimer');
  info.data = info.data || {};
  info.data.sisoTrajectoryRequestId = (info.data.sisoTrajectoryRequestId || 0) + 1;
  info.data.sisoOverlayRequestId = (info.data.sisoOverlayRequestId || 0) + 1;
  delete info.data.behaviorData;
  delete info.data.selectedPath;
  delete info.data.trajectoryData;
  delete info.data.overlayTrajectoryData;
  syncLifecycle(info, lifecycle);
  setNodeLoading(nodeId, false);
  setResultMessage(nodeId, 'Inputs changed — run SISO Behaviors again.');
  return true;
}

export function invalidateQKPolyResult(nodeId, reason = 'qk-input-changed') {
  const info = nodeRegistry[nodeId];
  if (!info || info.type !== 'qk-poly-result') return false;
  const lifecycle = qkPolyLifecycleFor(info);
  invalidateBoundLifecycle(info, lifecycle, reason);
  clearTimer(info, '_qkPolyPlotTimer');
  info.data = info.data || {};
  delete info.data.selection;
  delete info.data.polyhedronPayload;
  syncLifecycle(info, lifecycle);
  setNodeLoading(nodeId, false);
  setResultMessage(nodeId, 'Upstream path changed — run qK Polyhedron again.');
  return true;
}

export function invalidateSISOResultsDownstreamOf(sourceNodeId, reason = 'siso-upstream-changed') {
  const visited = new Set([sourceNodeId]);
  const queue = [sourceNodeId];
  const invalidated = [];
  while (queue.length) {
    const current = queue.shift();
    for (const connection of connections) {
      if (connection?.fromNode !== current || visited.has(connection.toNode)) continue;
      visited.add(connection.toNode);
      queue.push(connection.toNode);
      const type = nodeRegistry[connection.toNode]?.type;
      if (type === 'siso-result' && invalidateSISOResult(connection.toNode, reason)) {
        invalidated.push(connection.toNode);
      } else if (type === 'qk-poly-result' && invalidateQKPolyResult(connection.toNode, reason)) {
        invalidated.push(connection.toNode);
      }
    }
  }
  return invalidated;
}

export function installSISOConfigInvalidation(nodeId) {
  const node = document.getElementById(nodeId);
  if (!node) return;
  node.querySelectorAll('.auto-update').forEach(control => {
    const eventType = control.tagName === 'SELECT' || control.type === 'checkbox' ? 'change' : 'input';
    control.addEventListener(eventType, () => {
      invalidateSISOResultsDownstreamOf(nodeId, 'siso-config-input-changed');
    });
  });
}

export function inspectSISOResultLifecycle(nodeId) {
  const info = nodeRegistry[nodeId];
  return info?.type === 'siso-result'
    ? inspectExecutionLifecycle(sisoLifecycleFor(info))
    : null;
}

export function inspectQKPolyResultLifecycle(nodeId) {
  const info = nodeRegistry[nodeId];
  return info?.type === 'qk-poly-result'
    ? inspectExecutionLifecycle(qkPolyLifecycleFor(info))
    : null;
}

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
  const items = entries.map(([reason, count]) => `<span class="tag tag-nonasym">${escapeHtml(reason)}: ${escapeHtml(count)}</span>`).join(' ');
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
        <span class="summary-chip">exact families ${escapeHtml(familyCount)}</span>
        <span class="summary-chip">included paths ${escapeHtml(includedCount)}</span>
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
      id="${escapeHtml(sisoConditionPanelId(nodeId, pathIdx))}"
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
      <span class="summary-chip">Path #${escapeHtml(data.path_idx)}</span>
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
  const info = nodeRegistry[nodeId];
  const nodeData = info?.data;
  if (!nodeData) return null;
  const lifecycle = sisoLifecycleFor(info);
  const runtime = inspectExecutionLifecycle(lifecycle);
  if (runtime.state !== 'current' || runtime.freshness !== 'current') {
    showToast('Run SISO Behaviors before selecting a current path');
    return null;
  }
  const selection = buildSISOSelection(nodeId, changeQK, pathIdx);
  if (!selection) return null;
  invalidateQKResultsDownstreamOfSISO(nodeId, 'siso-path-selection-changed');
  nodeData.selectedPath = selection;
  commitWorkspaceSnapshot('siso-selection');
  triggerDownstreamNodes(nodeId, 'result');
  return selection;
}

export function clearSISOSelection(nodeId, notify = true) {
  const nodeData = nodeRegistry[nodeId]?.data;
  if (!nodeData) return;
  invalidateQKResultsDownstreamOfSISO(nodeId, 'siso-path-selection-cleared');
  nodeData.selectedPath = null;
  commitWorkspaceSnapshot('siso-selection-cleared');
  if (notify) triggerDownstreamNodes(nodeId, 'result');
}

function invalidateQKResultsDownstreamOfSISO(nodeId, reason) {
  for (const connection of connections) {
    if (connection?.fromNode !== nodeId || connection.fromPort !== 'result') continue;
    if (nodeRegistry[connection.toNode]?.type === 'qk-poly-result') {
      invalidateQKPolyResult(connection.toNode, reason);
    }
  }
}

export function renderPathChips(nodeId, changeQK, pathIndices, accent) {
  const selectedPathIdx = selectedPathIdxForNode(nodeId);
  return (pathIndices || []).map(pathIdx => `
    <button
      type="button"
      class="path-chip ${selectedPathIdx === pathIdx ? 'selected' : ''}"
      data-path-idx="${escapeHtml(pathIdx)}"
      style="--path-chip-accent:${accent}; --path-chip-soft:${hexToRgba(accent, 0.16)};"
      data-action="plotSISOPath"
      data-node="${nodeId}" data-qk="${escapeHtml(changeQK)}" data-idx="${escapeHtml(pathIdx)}"
    >#${escapeHtml(pathIdx)}</button>
  `).join('');
}

// Reveal the collapsed feasible-path cards for one exact family (the "Show all N paths in E…"
// toggle). Items past the per-family cap are rendered with display:none + data-of="<family>";
// this drops the inline display and removes the toggle button it was clicked on.
export function expandSISOPaths(el) {
  const fam = el && el.dataset ? el.dataset.of : null;
  if (fam == null) return;
  const scope = el.closest('.siso-feasible-list') || document;
  scope.querySelectorAll(`.siso-path-item[data-of="${fam}"]`).forEach((item) => { item.style.display = ''; });
  const wrap = el.closest('.siso-path-showall-wrap');
  if (wrap) wrap.style.display = 'none'; else el.style.display = 'none';
}

export function renderFamilyTable(nodeId, changeQK, families) {
  if (!families.length) return '';

  const rows = families.map(family => {
    const accent = getFamilyColor(family.family_idx, 0);
    const badgeStyle = `--badge-accent:${accent}; --badge-soft:${hexToRgba(accent, 0.18)};`;
    const profile = escapeHtml(family.exact_label);

    return `
      <tr>
        <td><span class="family-badge family-badge-exact" style="${badgeStyle}">E${escapeHtml(family.family_idx)}</span></td>
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

  // Complex networks can yield hundreds of feasible paths → an endless panel. Show at most
  // FAM_CAP per exact family (E); the rest are rendered collapsed behind a per-family
  // "Show all N paths in E…" toggle (paths are already sorted by family, so each family is
  // a contiguous run). The selected path always stays visible regardless of the cap.
  const FAM_CAP = 6;
  const famKey = (p) => exactFamilyByPath.get(p.path_idx) ?? 'none';
  const famTotals = {};
  feasiblePaths.forEach((p) => { const k = famKey(p); famTotals[k] = (famTotals[k] || 0) + 1; });
  const famSeen = {};
  feasiblePaths.forEach(path => {
    const fam = famKey(path);
    const n = famSeen[fam] || 0; famSeen[fam] = n + 1;
    const overflow = n >= FAM_CAP && selectedPathIdx !== path.path_idx;
    if (n === FAM_CAP && famTotals[fam] > FAM_CAP) {
      html += `
      <div class="siso-path-showall-wrap">
        <button type="button" class="btn btn-small siso-showall-btn" data-action="toggleSISOPaths"
          data-node="${nodeId}" data-qk="${escapeHtml(changeQK)}" data-of="${escapeHtml(fam)}">
          Show all ${escapeHtml(famTotals[fam])} paths in ${fam === 'none' ? 'this group' : `E${escapeHtml(fam)}`} ▾
        </button>
      </div>`;
    }
    const permStr = path.perms.map(pr => `[${pr.join(',')}]`).join(' → ');
    const exactFamilyIdx = exactFamilyByPath.get(path.path_idx);
    const exactAccent = getFamilyColor(exactFamilyIdx || path.path_idx, 0);
    const includeTag = path.included
      ? '<span class="tag tag-asym">Included</span>'
      : `<span class="tag tag-nonasym">${escapeHtml(path.exclusion_reason || 'Excluded')}</span>`;

    html += `
      <div
        class="path-item siso-path-item ${path.included ? 'is-included' : 'is-excluded'} ${selectedPathIdx === path.path_idx ? 'selected' : ''}"
        data-idx="${escapeHtml(path.path_idx)}"
        data-path-idx="${escapeHtml(path.path_idx)}"
        data-qk="${escapeHtml(changeQK)}"
        data-node="${nodeId}"
        ${overflow ? `data-of="${escapeHtml(fam)}"` : ''}
        style="--exact-accent:${exactAccent}; --exact-soft:${hexToRgba(exactAccent, 0.14)};${overflow ? 'display:none;' : ''}"
        data-action="selectSISOPath"
      >
        <div class="siso-path-head">
          <div class="siso-path-title">Path #${escapeHtml(path.path_idx)}</div>
          <div class="siso-path-actions">
            <button type="button" class="btn btn-small siso-inline-btn" data-action="plotSISOPath" data-node="${nodeId}" data-qk="${escapeHtml(changeQK)}" data-idx="${escapeHtml(path.path_idx)}">Plot</button>
            <button type="button" class="btn btn-small siso-inline-btn" data-action="toggleSISOPathCondition" data-node="${nodeId}" data-qk="${escapeHtml(changeQK)}" data-idx="${escapeHtml(path.path_idx)}">Condition</button>
          </div>
        </div>
        <div class="siso-path-badges">
          ${exactFamilyIdx ? `<span class="family-badge family-badge-exact" style="--badge-accent:${exactAccent}; --badge-soft:${hexToRgba(exactAccent, 0.18)};">Exact ${escapeHtml(exactFamilyIdx)}</span>` : ''}
          ${includeTag}
        </div>
        <div class="siso-path-detail">${escapeHtml(permStr)}</div>
        <div class="siso-path-meta">
          <span>RO ${escapeHtml(path.exact_label)}</span>
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
  const owner = nodeRegistry[nodeId];
  const nodeData = owner?.data;
  const data = nodeData?.behaviorData;
  const config = getConnectedSISOConfig(nodeId);
  const sessionId = getSessionIdForNode(nodeId);
  const plotEl = document.getElementById(`${nodeId}-traj-plot`);
  if (!plotEl || !data || !config || !sessionId) return;
  const lifecycle = owner?.type === 'siso-result' ? sisoLifecycleFor(owner) : null;
  const lifecycleRuntime = lifecycle ? inspectExecutionLifecycle(lifecycle) : null;
  if (lifecycleRuntime && lifecycleRuntime.freshness !== 'current') return;

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
  const workspaceEpoch = getWorkspaceRuntimeEpoch();
  const inputFingerprint = currentSISOResultContext(nodeId)?.inputFingerprint || '';
  const requestIsCurrent = () => {
    const currentOwner = nodeRegistry[nodeId];
    const context = currentSISOResultContext(nodeId);
    return currentOwner === owner && getWorkspaceRuntimeEpoch() === workspaceEpoch &&
      currentOwner?.data?.sisoOverlayRequestId === requestId &&
      context?.inputFingerprint === inputFingerprint &&
      (!lifecycle || inspectExecutionLifecycle(lifecycle).freshness === 'current');
  };

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

    if (!requestIsCurrent()) return;

    const overlayData = {
      requestKey,
      change_qK: data.change_qK || config.change_qK,
      observe_x: data.observe_x || config.observe_x,
      start: config?.min ?? -6,
      stop: config?.max ?? 6,
      trajectories: results.filter(result => result?.ok),
      failures: results.filter(result => result && !result.ok),
    };

    if (requestIsCurrent()) {
      ensureNodeData(nodeId).overlayTrajectoryData = overlayData;
    }
    if (!requestIsCurrent()) return;
    renderSISOBehaviorOverlayPlot(nodeId, overlayData);
    commitWorkspaceSnapshot('siso-all-path-overlay');
  } finally {
    if (requestIsCurrent()) setNodeLoading(nodeId, false);
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
  const owner = nodeRegistry[nodeId];
  if (!owner || owner.type !== 'siso-result') {
    return blockedOutcome(nodeId, {
      code: 'missing_siso_result',
      message: 'SISO Behaviors node is unavailable',
      outputs: { result: 'missing' },
    });
  }
  const lifecycle = sisoLifecycleFor(owner);
  const previousSelectedPath = getNodeData(nodeId).selectedPath?.path_idx || null;
  invalidateSISOResult(nodeId, 'siso-execution-restarted');
  invalidateSISOResultsDownstreamOf(nodeId, 'upstream-siso-execution-restarted');

  // Find the connected params node
  const paramsConn = connections.find(c => c.toNode === nodeId && c.toPort === 'params');
  if (!paramsConn) {
    const context = currentSISOResultContext(nodeId);
    if (context) {
      const ticket = beginLifecycle(lifecycle, context);
      blockLifecycle(lifecycle, ticket, { context, reason: 'Connect a SISO Config node first' });
      syncLifecycle(owner, lifecycle);
    }
    showToast('Connect a SISO Config node first');
    return blockedOutcome(nodeId, {
      code: 'missing_siso_config',
      message: 'Connect a SISO Config node first',
      outputs: { result: 'missing' },
    });
  }

  const paramsNode = nodeRegistry[paramsConn.fromNode];
  const config = getConnectedSISOConfig(nodeId);
  if (!paramsNode || !config) {
    const context = currentSISOResultContext(nodeId);
    if (context) {
      const ticket = beginLifecycle(lifecycle, context);
      blockLifecycle(lifecycle, ticket, { context, reason: 'SISO Config node has no configuration' });
      syncLifecycle(owner, lifecycle);
    }
    showToast('SISO Config node has no configuration');
    return blockedOutcome(nodeId, {
      code: 'invalid_siso_config',
      message: 'SISO Config node has no configuration',
      outputs: { result: 'missing' },
    });
  }

  setNodeLoading(nodeId, true);
  const contentEl = document.getElementById(`${nodeId}-content`);
  const plotMode = getSISOPlotMode(nodeId);
  const attemptRevision = (owner._sisoAttemptRevision || 0) + 1;
  owner._sisoAttemptRevision = attemptRevision;
  let ticket = null;

  try {
    const sessionId = await ensureModelSession(nodeId);
    if (nodeRegistry[nodeId] !== owner || owner._sisoAttemptRevision !== attemptRevision) {
      return staleOutcome(nodeId, {
        code: 'siso_execution_superseded',
        message: 'A newer SISO execution owns this node',
        outputs: { result: 'missing' },
      });
    }
    const beginContext = currentSISOResultContext(nodeId);
    if (!beginContext) {
      return staleOutcome(nodeId, {
        code: 'siso_execution_context_missing',
        outputs: { result: 'missing' },
      });
    }
    ticket = beginLifecycle(lifecycle, beginContext);
    syncLifecycle(owner, lifecycle);
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
    }, {
      statusIsCurrent: () => {
        const context = currentSISOResultContext(nodeId);
        return !!context && isCurrentLifecycle(lifecycle, ticket, context);
      },
    });
    const currentContext = currentSISOResultContext(nodeId);
    if (!currentContext || !isCurrentLifecycle(lifecycle, ticket, currentContext)) {
      if (currentContext) {
        failLifecycle(lifecycle, ticket, {
          context: currentContext,
          error: new Error('SISO execution no longer owns the current inputs'),
        });
      }
      syncLifecycle(owner, lifecycle);
      return staleOutcome(nodeId, {
        code: 'siso_execution_superseded',
        outputs: { result: 'missing' },
      });
    }
    const pathStillExists = previousSelectedPath != null &&
      (data.paths || []).some(path => path.path_idx === previousSelectedPath);
    const nd = ensureNodeData(nodeId);
    nd.behaviorData = data;
    nd.selectedPath = pathStillExists
      ? buildSISOSelection(nodeId, config.change_qK, previousSelectedPath)
      : null;
    nd.trajectoryData = null;
    nd.overlayTrajectoryData = null;
    if (!commitLifecycle(lifecycle, ticket, {
      context: currentContext,
      result: sisoResultSnapshot(owner),
      evidence: {
        source_endpoint: SISO_ENDPOINT,
        included_paths: data?.included_paths ?? null,
        excluded_paths: data?.excluded_paths ?? null,
      },
      sessionId,
    })) {
      syncLifecycle(owner, lifecycle);
      return staleOutcome(nodeId, {
        code: 'siso_execution_superseded',
        outputs: { result: 'missing' },
      });
    }
    syncLifecycle(owner, lifecycle);
    if (nodeRegistry[nodeId] !== owner || !isCurrentLifecycle(lifecycle, ticket, currentContext)) {
      return staleOutcome(nodeId, {
        code: 'siso_execution_superseded',
        outputs: { result: 'missing' },
      });
    }
    contentEl.innerHTML = renderBehaviorFamiliesResult(nodeId, config.change_qK, data);
    commitWorkspaceSnapshot('siso-behavior');
    if (plotMode === SISO_PLOT_MODE_OVERLAY) {
      await loadAndPlotSISOBehaviorOverlay(nodeId, { force: true });
    } else if (pathStillExists) {
      await plotSISOPath(nodeId, config.change_qK, previousSelectedPath);
    }
    const selection = getNodeData(nodeId).selectedPath;
    return succeededOutcome(nodeId, {
      outputs: { result: selection?.path_idx != null ? 'present' : 'missing' },
      code: selection?.path_idx != null ? null : 'siso_path_not_selected',
      message: selection?.path_idx != null ? null : 'Select a current SISO path before running qK',
    });
  } catch (e) {
    const currentContext = currentSISOResultContext(nodeId);
    let current = false;
    if (ticket && currentContext) {
      current = failLifecycle(lifecycle, ticket, { context: currentContext, error: e });
    } else if (currentContext && nodeRegistry[nodeId] === owner &&
               owner._sisoAttemptRevision === attemptRevision) {
      ticket = beginLifecycle(lifecycle, currentContext);
      current = failLifecycle(lifecycle, ticket, { context: currentContext, error: e });
    }
    syncLifecycle(owner, lifecycle);
    if (!current && ticket) {
      return staleOutcome(nodeId, {
        code: 'siso_execution_superseded',
        outputs: { result: 'missing' },
      });
    }
    handleNodeError(e, nodeId, 'SISO behavior analysis');
    if (nodeRegistry[nodeId] === owner) renderNodeError(contentEl, e);
    return failedOutcome(nodeId, {
      code: 'siso_execution_failed',
      message: e?.message || String(e),
      outputs: { result: 'missing' },
    });
  } finally {
    if (ticket) {
      releaseLifecycle(lifecycle, ticket);
      if (owner._sisoAttemptRevision === attemptRevision) {
        settleLoading(nodeId, lifecycle, ticket);
      }
    } else if (nodeRegistry[nodeId] === owner && owner._sisoAttemptRevision === attemptRevision) {
      setNodeLoading(nodeId, false);
    }
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
  const owner = nodeRegistry[nodeId];
  if (!owner || owner.type !== 'siso-result') return;
  const lifecycle = sisoLifecycleFor(owner);
  const runtime = inspectExecutionLifecycle(lifecycle);
  if (runtime.state !== 'current' || runtime.freshness !== 'current') {
    showToast('Run SISO Behaviors before plotting a current path');
    return;
  }
  const config = getConnectedSISOConfig(nodeId);
  const sessionId = getSessionIdForNode(nodeId);
  if (!sessionId) return;
  setSISOPlotMode(nodeId, SISO_PLOT_MODE_SINGLE);
  const nodeData = nodeRegistry[nodeId]?.data;
  const requestId = (nodeData?.sisoTrajectoryRequestId || 0) + 1;
  if (nodeData) {
    nodeData.sisoTrajectoryRequestId = requestId;
  }
  const workspaceEpoch = getWorkspaceRuntimeEpoch();
  const inputFingerprint = currentSISOResultContext(nodeId)?.inputFingerprint || '';
  const requestIsCurrent = () => {
    const context = currentSISOResultContext(nodeId);
    return nodeRegistry[nodeId] === owner && getWorkspaceRuntimeEpoch() === workspaceEpoch &&
      owner.data?.sisoTrajectoryRequestId === requestId &&
      context?.inputFingerprint === inputFingerprint &&
      inspectExecutionLifecycle(lifecycle).freshness === 'current';
  };
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
    }, {
      statusIsCurrent: requestIsCurrent,
    });
    if (!requestIsCurrent()) return;
    if (nodeRegistry[nodeId] === owner) {
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
    const sessionId = await ensureModelSession(nodeId);
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
  const lifecycle = sisoLifecycleFor(sourceInfo);
  const runtime = inspectExecutionLifecycle(lifecycle);
  return {
    sourceNodeId: conn.fromNode,
    sourceInfo,
    freshness: runtime.freshness,
    selection: runtime.state === 'current' && runtime.freshness === 'current'
      ? sourceInfo.data?.selectedPath || null
      : null,
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
      <span class="summary-chip"><strong>Path #${escapeHtml(selection.path_idx)}</strong></span>
      ${selection.exact_family_idx ? `<span class="family-badge family-badge-exact">E${escapeHtml(selection.exact_family_idx)}</span>` : ''}
      <span class="summary-chip">${escapeHtml(selection.observe_x)}</span>
      <span class="summary-chip">${escapeHtml(payload.change_qK)} scanned</span>
    </div>
    <div class="siso-scope-note">
      Fixed coordinates: ${qkSymbols.length ? escapeHtml(qkSymbols.join(', ')) : 'n/a'}<br>
      Dimension ${escapeHtml(poly.dimension)}, constraints ${escapeHtml(poly.n_constraints ?? (poly.A || []).length)}, vertices ${escapeHtml(poly.n_vertices ?? vertices.length)}, rays ${escapeHtml(poly.n_rays ?? rays.length)}.
    </div>
  `;

  if (!canPlot) {
    html += `
      <div class="text-dim">
        Direct geometric plotting is only shown for bounded 2D path polyhedra. This selected path is ${escapeHtml(poly.dimension)}D${poly.is_bounded ? '' : ' and unbounded'}.
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
  const owner = nodeRegistry[nodeId];
  if (!owner || owner.type !== 'qk-poly-result') {
    return blockedOutcome(nodeId, {
      code: 'missing_qk_result',
      message: 'qK Polyhedron node is unavailable',
    });
  }
  const lifecycle = qkPolyLifecycleFor(owner);
  invalidateQKPolyResult(nodeId, 'qk-execution-restarted');
  const contentEl = document.getElementById(`${nodeId}-content`);
  const source = getConnectedSISOSelection(nodeId);
  if (!source) {
    const context = currentQKPolyContext(nodeId);
    if (context) {
      const ticket = beginLifecycle(lifecycle, context);
      blockLifecycle(lifecycle, ticket, { context, reason: 'Connect to a SISO Behaviors node first' });
      syncLifecycle(owner, lifecycle);
    }
    if (contentEl) contentEl.innerHTML = '<span class="text-dim">Connect to a SISO Behaviors node first.</span>';
    return blockedOutcome(nodeId, {
      code: 'missing_siso_source',
      message: 'Connect to a SISO Behaviors node first',
    });
  }

  if (source.freshness === 'historical' || source.freshness === 'invalidated') {
    const reason = source.freshness === 'historical'
      ? 'Run the restored SISO Behaviors node before using its selected path'
      : 'Run the invalidated SISO Behaviors node before using its selected path';
    const context = currentQKPolyContext(nodeId);
    if (context) {
      const ticket = beginLifecycle(lifecycle, context);
      blockLifecycle(lifecycle, ticket, { context, reason });
      syncLifecycle(owner, lifecycle);
    }
    if (contentEl) contentEl.innerHTML = `<span class="text-dim">${escapeHtml(reason)}</span>`;
    return blockedOutcome(nodeId, {
      code: source.freshness === 'historical'
        ? 'upstream_output_historical'
        : 'upstream_output_invalidated',
      message: reason,
    });
  }

  const selection = source.selection;
  if (!selection) {
    const context = currentQKPolyContext(nodeId);
    if (context) {
      const ticket = beginLifecycle(lifecycle, context);
      blockLifecycle(lifecycle, ticket, {
        context,
        reason: 'Select a current path in the upstream SISO Behaviors node',
      });
      syncLifecycle(owner, lifecycle);
    }
    if (contentEl) contentEl.innerHTML = '<span class="text-dim">Select a path in the upstream SISO Behaviors node.</span>';
    return blockedOutcome(nodeId, {
      code: 'upstream_output_missing',
      message: 'Select a current path in the upstream SISO Behaviors node',
    });
  }

  setNodeLoading(nodeId, true);
  const attemptRevision = (owner._qkPolyAttemptRevision || 0) + 1;
  owner._qkPolyAttemptRevision = attemptRevision;
  let ticket = null;
  try {
    const sessionId = await ensureModelSession(nodeId);
    if (nodeRegistry[nodeId] !== owner || owner._qkPolyAttemptRevision !== attemptRevision) {
      return staleOutcome(nodeId, { code: 'qk_execution_superseded' });
    }
    const beginContext = currentQKPolyContext(nodeId);
    if (!beginContext) {
      return staleOutcome(nodeId, { code: 'qk_execution_context_missing' });
    }
    ticket = beginLifecycle(lifecycle, beginContext);
    syncLifecycle(owner, lifecycle);
    const payload = await api('siso_polyhedra', {
      session_id: sessionId,
      change_qK: selection.change_qK,
      path_indices: [selection.path_idx],
    }, {
      statusIsCurrent: () => {
        const context = currentQKPolyContext(nodeId);
        return !!context && isCurrentLifecycle(lifecycle, ticket, context);
      },
    });
    const currentContext = currentQKPolyContext(nodeId);
    if (!currentContext || !commitLifecycle(lifecycle, ticket, {
      context: currentContext,
      result: { selection, polyhedronPayload: payload },
      evidence: { source_endpoint: QK_POLY_ENDPOINT },
      sessionId,
    })) {
      syncLifecycle(owner, lifecycle);
      return staleOutcome(nodeId, { code: 'qk_execution_superseded' });
    }
    syncLifecycle(owner, lifecycle);
    if (nodeRegistry[nodeId] === owner) {
      const nd = ensureNodeData(nodeId);
      nd.selection = selection;
      nd.polyhedronPayload = payload;
    }
    const rendered = renderQKPolyhedronResult(nodeId, selection, payload);
    contentEl.innerHTML = rendered.html;
    commitWorkspaceSnapshot('qk-polyhedron');
    if (rendered.canPlot) {
      clearTimer(owner, '_qkPolyPlotTimer');
      const timer = setTimeout(() => {
        if (owner._qkPolyPlotTimer === timer) delete owner._qkPolyPlotTimer;
        const context = currentQKPolyContext(nodeId);
        if (nodeRegistry[nodeId] !== owner || !context ||
            !isCurrentLifecycle(lifecycle, ticket, context)) return;
        plotQKPolyhedron(payload.polyhedra?.[0], payload.qk_symbols || [], `${nodeId}-plot`);
        setupPlotResize(nodeId, `${nodeId}-plot`);
      }, 50);
      owner._qkPolyPlotTimer = timer;
    }
    return succeededOutcome(nodeId);
  } catch (e) {
    const currentContext = currentQKPolyContext(nodeId);
    let current = false;
    if (ticket && currentContext) {
      current = failLifecycle(lifecycle, ticket, { context: currentContext, error: e });
    } else if (currentContext && nodeRegistry[nodeId] === owner &&
               owner._qkPolyAttemptRevision === attemptRevision) {
      ticket = beginLifecycle(lifecycle, currentContext);
      current = failLifecycle(lifecycle, ticket, { context: currentContext, error: e });
    }
    syncLifecycle(owner, lifecycle);
    if (!current && ticket) return staleOutcome(nodeId, { code: 'qk_execution_superseded' });
    handleNodeError(e, nodeId, 'qK polyhedron');
    if (nodeRegistry[nodeId] === owner) renderNodeError(contentEl, e);
    return failedOutcome(nodeId, {
      code: 'qk_polyhedron_failed',
      message: e?.message || String(e),
    });
  } finally {
    if (ticket) {
      releaseLifecycle(lifecycle, ticket);
      if (owner._qkPolyAttemptRevision === attemptRevision) {
        settleLoading(nodeId, lifecycle, ticket);
      }
    } else if (nodeRegistry[nodeId] === owner &&
               owner._qkPolyAttemptRevision === attemptRevision) {
      setNodeLoading(nodeId, false);
    }
  }
}
