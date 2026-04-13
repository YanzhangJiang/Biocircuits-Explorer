import { nodeRegistry, connections, SISO_FAMILY_COLORS, ensureNodeData, getNodeData } from './state.js';
import { api, showToast, handleNodeError, escapeHtml, splitCommaList, parseOptionalFloat, cloneSerializable } from './api.js';
import { hexToRgba, getFamilyColor, applyPlotLayoutTheme, getPlotTheme, applyPlotAxisTheme, applyPlotSceneAxisTheme, themedColorbar } from './theme.js';
import { plotTrajectory, convexHull2D, formatPolyNumber, formatPolyConstraint, renderPolyCoordinateTable } from './plotting.js';
import { setNodeLoading, setupPlotResize, getModelContextForNode, getSessionIdForNode, getQKSymbolsForNode, findUpstreamNodeByType } from './nodes.js';
import { triggerDownstreamNodes } from './model.js';
import { commitWorkspaceSnapshot, getNodeSerialData } from './workspace.js';
import { NODE_TYPES } from './node-types/index.js';

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
  return (pathIndices || []).map(pathIdx => `
    <button
      type="button"
      class="path-chip"
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
  const feasiblePaths = (data.paths || [])
    .filter(path => path.feasible)
    .sort((a, b) => {
      const exactA = exactFamilyByPath.get(a.path_idx) || Number.MAX_SAFE_INTEGER;
      const exactB = exactFamilyByPath.get(b.path_idx) || Number.MAX_SAFE_INTEGER;
      return exactA - exactB || a.path_idx - b.path_idx;
    });
  const includedFeasiblePaths = feasiblePaths.filter(path => path.included);

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
        class="path-item siso-path-item ${path.included ? 'is-included' : 'is-excluded'}"
        data-idx="${path.path_idx}"
        data-path-idx="${path.path_idx}"
        data-qk="${changeQK}"
        data-node="${nodeId}"
        style="--exact-accent:${exactAccent}; --exact-soft:${hexToRgba(exactAccent, 0.14)};"
        data-action="selectSISOPath"
      >
        <div class="siso-path-head">
          <div class="siso-path-title">Path #${path.path_idx}</div>
          <button type="button" class="btn btn-small siso-inline-btn" data-action="plotSISOPath" data-node="${nodeId}" data-qk="${changeQK}" data-idx="${path.path_idx}">Plot</button>
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
      </div>
    `;
  });

  html += `
      </div>
    </section>
  `;

  const showPlot = includedFeasiblePaths.length > 0 ? '' : 'display:none;';
  html += `<div class="plot-container" id="${nodeId}-traj-plot" style="${showPlot}"></div>`;
  return html;
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
    if (pathStillExists) {
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
