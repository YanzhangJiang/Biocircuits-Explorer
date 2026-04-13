// Biocircuits Explorer — Parameter Scan & ROP Polyhedron Functions

import { nodeRegistry, connections, ensureNodeData, getNodeData } from './state.js';
import { api, showToast, handleNodeError, cloneSerializable, splitCommaList, parseOptionalFloat, parseOptionalInteger, syncSelectOptions } from './api.js';
import { applyPlotLayoutTheme, getPlotTheme, applyPlotAxisTheme, applyPlotSceneAxisTheme, hexToRgba, themedColorbar, prefersLightTheme } from './theme.js';
import { convexHull2D } from './plotting.js';
import { setNodeLoading, setupPlotResize, setupPlotInteractionGuard, getSessionIdForNode, getModelForNode, getQKSymbolsForNode, getModelContextForNode, findUpstreamNodeByType, triggerConfigUpdate } from './nodes.js';
import { getConnectedSISOConfig, getConnectedSISOSelection, normalizeSISOConfig } from './siso.js';
import { commitWorkspaceSnapshot, getNodeSerialData } from './workspace.js';

// ===== Parameter Scan 1D Helper Functions =====

export function insertSpecies1D(nodeId) {
  const helper = document.getElementById(`${nodeId}-species-helper`);
  const expr = document.getElementById(`${nodeId}-expr`);
  if (helper.value && expr) {
    expr.value += (expr.value ? ' + ' : '') + helper.value;
    helper.value = '';
    const info = nodeRegistry[nodeId];
    if (info && info.type === 'scan-1d-params') {
      triggerConfigUpdate(nodeId, info.type);
    }
  }
}

export function updateScan1DConfig(nodeId) {
  const paramSelect = document.getElementById(`${nodeId}-param`);
  const minInput = document.getElementById(`${nodeId}-min`);
  const maxInput = document.getElementById(`${nodeId}-max`);
  const pointsInput = document.getElementById(`${nodeId}-points`);
  const exprInput = document.getElementById(`${nodeId}-expr`);

  if (!paramSelect.value) {
    alert('Please select a scan parameter');
    return;
  }

  if (!exprInput.value.trim()) {
    alert('Please enter an output expression');
    return;
  }

  // Store config in node data
  nodeRegistry[nodeId].data.config = {
    param_symbol: paramSelect.value,
    param_min: parseFloat(minInput.value),
    param_max: parseFloat(maxInput.value),
    n_points: parseInt(pointsInput.value),
    output_exprs: [exprInput.value.trim()],
  };

  showToast('Configuration updated');
}

export async function runParameterScan1D(nodeId) {
  const paramSymbol = document.getElementById(`${nodeId}-param`).value;
  const min = parseFloat(document.getElementById(`${nodeId}-min`).value);
  const max = parseFloat(document.getElementById(`${nodeId}-max`).value);
  const points = parseInt(document.getElementById(`${nodeId}-points`).value);

  const expr = document.getElementById(`${nodeId}-expr`).value.trim();
  if (!expr) {
    alert('Please enter an output expression');
    return;
  }

  setNodeLoading(nodeId, true);
  const contentEl = document.getElementById(`${nodeId}-content`);

  try {
    const sessionId = getSessionIdForNode(nodeId);
    if (!sessionId) throw new Error('Build the connected model first');
    const data = await api('parameter_scan_1d', {
      session_id: sessionId,
      param_symbol: paramSymbol,
      param_min: min,
      param_max: max,
      n_points: points,
      output_exprs: [expr],
    });
    if (nodeRegistry[nodeId]) {
      ensureNodeData(nodeId).scan1DResult = data;
    }

    contentEl.innerHTML = `<div class="plot-container" id="${nodeId}-plot"></div>`;
    commitWorkspaceSnapshot('scan-1d');
    setTimeout(() => {
      plotParameterScan1D(data, `${nodeId}-plot`);
      setupPlotResize(nodeId, `${nodeId}-plot`);
    }, 50);
  } catch (e) {
    handleNodeError(e, nodeId, 'Parameter scan 1D');
    contentEl.innerHTML = `<div class="node-error">${e.message}</div>`;
  } finally {
    setNodeLoading(nodeId, false);
  }
}

export async function executeScan1DResult(nodeId) {
  // Find connected params node
  const conn = connections.find(c => c.toNode === nodeId && c.toPort === 'params');
  if (!conn) {
    alert('Please connect to a Scan 1D Config node');
    return;
  }

  const paramsNode = nodeRegistry[conn.fromNode];
  if (!paramsNode) {
    alert('Config node has no configuration. Please configure it first.');
    return;
  }

  // Sync latest DOM values to avoid stale config when user changed fields just before Run.
  triggerConfigUpdate(conn.fromNode, paramsNode.type || 'scan-1d-params');
  const config = paramsNode.data?.config;
  if (!config) {
    alert('Config node has no configuration. Please configure it first.');
    return;
  }

  // Validate required fields
  if (!config.param_symbol) {
    alert('Please select a scan parameter in the config node');
    return;
  }

  if (!config.output_exprs || !config.output_exprs[0]) {
    alert('Please enter an output expression in the config node');
    return;
  }

  setNodeLoading(nodeId, true);
  const contentEl = document.getElementById(`${nodeId}-content`);

  try {
    const sessionId = getSessionIdForNode(nodeId);
    if (!sessionId) throw new Error('Build the connected model first');
    const data = await api('parameter_scan_1d', {
      session_id: sessionId,
      param_symbol: config.param_symbol,
      param_min: config.param_min,
      param_max: config.param_max,
      n_points: config.n_points,
      output_exprs: config.output_exprs,
    });
    if (nodeRegistry[nodeId]) {
      ensureNodeData(nodeId).scan1DResult = data;
    }

    contentEl.innerHTML = `<div class="plot-container" id="${nodeId}-plot"></div>`;
    commitWorkspaceSnapshot('scan-1d');
    setTimeout(() => {
      plotParameterScan1D(data, `${nodeId}-plot`);
      setupPlotResize(nodeId, `${nodeId}-plot`);
    }, 50);
  } catch (e) {
    handleNodeError(e, nodeId, 'Parameter scan 1D');
    contentEl.innerHTML = `<div class="node-error">${e.message}</div>`;
  } finally {
    setNodeLoading(nodeId, false);
  }
}

// ===== Parameter Scan 1D Plotting =====

export function plotParameterScan1D(data, plotId) {
  const { param_symbol, param_values, output_exprs, output_traj } = data;
  const plotTheme = getPlotTheme();

  const traces = output_exprs.map((expr, i) => ({
    x: param_values,
    y: output_traj.map(row => row[i]),
    mode: 'lines',
    name: expr,
    line: { width: 2 },
  }));

  const layout = {
    autosize: true,
    margin: { t: 40, b: 60, l: 70, r: 20 },
    title: { text: `Parameter Scan: ${param_symbol}`, font: { color: plotTheme.titleColor, size: 11 }, y: 0.98, yanchor: 'top' },
    xaxis: { title: `log10(${param_symbol})` },
    yaxis: { title: 'log10(concentration)' },
    legend: { x: 1, xanchor: 'right', y: 1 },
  };

  Plotly.newPlot(plotId, traces, applyPlotLayoutTheme(layout), { responsive: true, displayModeBar: false, scrollZoom: true });
}

// ===== Parameter Scan 2D Helper Functions =====

export function insertSpecies2D(nodeId) {
  const helper = document.getElementById(`${nodeId}-species-helper`);
  const expr = document.getElementById(`${nodeId}-expr`);
  if (helper.value && expr) {
    expr.value += (expr.value ? ' + ' : '') + helper.value;
    helper.value = '';
    const info = nodeRegistry[nodeId];
    if (info && info.type === 'scan-2d-params') {
      triggerConfigUpdate(nodeId, info.type);
    }
  }
}

export function updateScan2DConfig(nodeId) {
  const param1 = document.getElementById(`${nodeId}-param1`).value;
  const param2 = document.getElementById(`${nodeId}-param2`).value;
  const min1 = parseFloat(document.getElementById(`${nodeId}-min1`).value);
  const max1 = parseFloat(document.getElementById(`${nodeId}-max1`).value);
  const min2 = parseFloat(document.getElementById(`${nodeId}-min2`).value);
  const max2 = parseFloat(document.getElementById(`${nodeId}-max2`).value);
  const points = parseInt(document.getElementById(`${nodeId}-points`).value);
  const expr = document.getElementById(`${nodeId}-expr`).value.trim();

  if (!param1 || !param2) {
    alert('Please select both parameters');
    return;
  }

  if (!expr) {
    alert('Please enter an output expression');
    return;
  }

  nodeRegistry[nodeId].data.config = {
    param_symbol_1: param1,
    param_symbol_2: param2,
    param_min_1: min1,
    param_max_1: max1,
    param_min_2: min2,
    param_max_2: max2,
    n_points: points,
    output_exprs: [expr],
  };

  showToast('Configuration updated');
}

export async function executeScan2DResult(nodeId) {
  const conn = connections.find(c => c.toNode === nodeId && c.toPort === 'params');
  if (!conn) {
    alert('Please connect to a Scan 2D Config node');
    return;
  }

  const paramsNode = nodeRegistry[conn.fromNode];
  if (!paramsNode) {
    alert('Config node has no configuration. Please configure it first.');
    return;
  }

  // Sync latest DOM values to avoid stale config when user changed fields just before Run.
  triggerConfigUpdate(conn.fromNode, paramsNode.type || 'scan-2d-params');
  const config = paramsNode.data?.config;
  if (!config) {
    alert('Config node has no configuration. Please configure it first.');
    return;
  }

  // Validate required fields
  if (!config.param1_symbol || !config.param2_symbol) {
    alert('Please select both X and Y axis parameters in the config node');
    return;
  }

  if (!config.output_expr) {
    alert('Please enter an output expression in the config node');
    return;
  }

  setNodeLoading(nodeId, true);
  const contentEl = document.getElementById(`${nodeId}-content`);

  try {
    const sessionId = getSessionIdForNode(nodeId);
    if (!sessionId) throw new Error('Build the connected model first');
    const data = await api('parameter_scan_2d', {
      session_id: sessionId,
      ...config
    });
    if (nodeRegistry[nodeId]) {
      ensureNodeData(nodeId).scan2DResult = data;
    }

    contentEl.innerHTML = `<div class="plot-container" id="${nodeId}-plot"></div>`;
    commitWorkspaceSnapshot('scan-2d');
    setTimeout(() => {
      plotParameterScan2D(data, `${nodeId}-plot`);
      setupPlotResize(nodeId, `${nodeId}-plot`);
    }, 50);
  } catch (e) {
    handleNodeError(e, nodeId, 'Parameter scan 2D');
    contentEl.innerHTML = `<div class="node-error">${e.message}</div>`;
  } finally {
    setNodeLoading(nodeId, false);
  }
}

export async function runParameterScan2D(nodeId) {
  const param1 = document.getElementById(`${nodeId}-param1`).value;
  const param2 = document.getElementById(`${nodeId}-param2`).value;
  const min1 = parseFloat(document.getElementById(`${nodeId}-min1`).value);
  const max1 = parseFloat(document.getElementById(`${nodeId}-max1`).value);
  const min2 = parseFloat(document.getElementById(`${nodeId}-min2`).value);
  const max2 = parseFloat(document.getElementById(`${nodeId}-max2`).value);
  const grid = parseInt(document.getElementById(`${nodeId}-grid`).value);
  const expr = document.getElementById(`${nodeId}-expr`).value.trim();

  if (!expr) {
    alert('Please enter an output expression');
    return;
  }

  setNodeLoading(nodeId, true);
  const contentEl = document.getElementById(`${nodeId}-content`);

  try {
    const sessionId = getSessionIdForNode(nodeId);
    if (!sessionId) throw new Error('Build the connected model first');
    const data = await api('parameter_scan_2d', {
      session_id: sessionId,
      param1_symbol: param1,
      param2_symbol: param2,
      param1_min: min1,
      param1_max: max1,
      param2_min: min2,
      param2_max: max2,
      n_grid: grid,
      output_expr: expr,
    });
    if (nodeRegistry[nodeId]) {
      ensureNodeData(nodeId).scan2DResult = data;
    }

    contentEl.innerHTML = `<div class="plot-container" id="${nodeId}-plot"></div>`;
    commitWorkspaceSnapshot('scan-2d');
    setTimeout(() => {
      plotParameterScan2D(data, `${nodeId}-plot`);
      setupPlotResize(nodeId, `${nodeId}-plot`);
    }, 50);
  } catch (e) {
    handleNodeError(e, nodeId, 'Parameter scan 2D');
    contentEl.innerHTML = `<div class="node-error">${e.message}</div>`;
  } finally {
    setNodeLoading(nodeId, false);
  }
}

// ===== Parameter Scan 2D Plotting =====

export function plotParameterScan2D(data, plotId) {
  const { param1_symbol, param2_symbol, param1_values, param2_values, output_expr, output_grid, regime_grid } = data;
  const plotTheme = getPlotTheme();

  // Create 3D surface plot
  const traces = [
    {
      z: output_grid,
      x: param1_values,
      y: param2_values,
      type: 'surface',
      colorscale: 'Viridis',
      colorbar: themedColorbar(`log(${output_expr})`),
      contours: {
        z: {
        show: true,
          usecolormap: true,
       highlightcolor: "#42f462",
          project: { z: true }
        }
      }
    }
  ];

  const layout = {
    autosize: true,
    margin: { t: 40, b: 60, l: 70, r: 20 },
    title: {
      text: `${output_expr} vs ${param1_symbol}, ${param2_symbol}`,
      font: { color: plotTheme.titleColor, size: 11 },
      y: 0.98,
      yanchor: 'top'
    },
    scene: {
      xaxis: {
        title: `log10(${param1_symbol})`,
      },
      yaxis: {
        title: `log10(${param2_symbol})`,
      },
      zaxis: {
        title: `log10(${output_expr})`,
      },
    }
  };

  Plotly.newPlot(plotId, traces, applyPlotLayoutTheme(layout), { responsive: true, displayModeBar: false, scrollZoom: true });
}

// ===== ROP Polyhedron Config & Execution =====

export function updateROPPolyConfig(nodeId) {
  const expr = document.getElementById(`${nodeId}-expr`).value.trim();
  const param1 = document.getElementById(`${nodeId}-param1`).value;
  const param2 = document.getElementById(`${nodeId}-param2`).value;
  const asymptotic = document.getElementById(`${nodeId}-asymptotic`).checked;
  const maxVertices = parseInt(document.getElementById(`${nodeId}-max-vertices`).value);

  if (!expr) {
    alert('Please enter an output expression');
    return;
  }

  if (!param1 || !param2) {
    alert('Please select both parameters');
    return;
  }

  nodeRegistry[nodeId].data.config = {
    output_expr: expr,
    param_symbol_1: param1,
    param_symbol_2: param2,
    asymptotic_only: asymptotic,
    max_vertices: maxVertices,
  };

  showToast('Configuration updated');
}

export async function executeROPPolyResult(nodeId) {
  const conn = connections.find(c => c.toNode === nodeId && c.toPort === 'params');
  if (!conn) {
    alert('Please connect to a ROP Polyhedron Config node');
    return;
  }

  const paramsNode = nodeRegistry[conn.fromNode];
  const config = getNodeSerialData(conn.fromNode, 'rop-poly-params');
  if (!paramsNode || !config || !(config.pairs || []).length) {
    alert('Config node has no configuration. Please configure it first.');
    return;
  }
  paramsNode.data = paramsNode.data || {};
  paramsNode.data.config = config;
  setNodeLoading(nodeId, true);
  const contentEl = document.getElementById(`${nodeId}-content`);

  try {
    const sessionId = getSessionIdForNode(nodeId);
    if (!sessionId) throw new Error('Build the connected model first');
    const data = await api('rop_polyhedron', {
      session_id: sessionId,
      ...config
    });
    renderROPPolyhedronOutput(nodeId, contentEl, data, config);
  } catch (e) {
    handleNodeError(e, nodeId, 'ROP polyhedron');
    contentEl.innerHTML = `<div class="node-error">${e.message}</div>`;
  } finally {
    setNodeLoading(nodeId, false);
  }
}

// ===== ROP Polyhedron Helper Functions =====

export function updateROPPolyDimension(nodeId) {
  const dimension = parseInt(document.getElementById(`${nodeId}-dimension`)?.value || '2', 10);
  const axis3XRow = document.getElementById(`${nodeId}-axis3-x-row`);
  const axis3QKRow = document.getElementById(`${nodeId}-axis3-qk-row`);
  const showAxis3 = dimension === 3;
  if (axis3XRow) axis3XRow.style.display = showAxis3 ? '' : 'none';
  if (axis3QKRow) axis3QKRow.style.display = showAxis3 ? '' : 'none';
}

export async function runROPPolyhedron(nodeId) {
  const config = getNodeSerialData(nodeId, 'rop-polyhedron');
  if (!(config.pairs || []).length || config.pairs.some(pair => !pair.x_symbol || !pair.qk_symbol)) {
    alert('Please select species and qK symbols for each ROP axis');
    return;
  }

  ensureNodeData(nodeId).config = config;
  setNodeLoading(nodeId, true);
  const contentEl = document.getElementById(`${nodeId}-content`);

  try {
    const sessionId = getSessionIdForNode(nodeId);
    if (!sessionId) throw new Error('Build the connected model first');
    const data = await api('rop_polyhedron', {
      session_id: sessionId,
      ...config,
    });
    renderROPPolyhedronOutput(nodeId, contentEl, data, config);
  } catch (e) {
    handleNodeError(e, nodeId, 'ROP polyhedron');
    contentEl.innerHTML = `<div class="node-error">${e.message}</div>`;
  } finally {
    setNodeLoading(nodeId, false);
  }
}

export function renderROPPolyhedronOutput(nodeId, contentEl, data, config = {}) {
  const axisSummary = (data.pairs || config.pairs || []).map((pair, idx) => {
    const xSymbol = pair.x_symbol || pair.xSymbol || '?';
    const qkSymbol = pair.qk_symbol || pair.qkSymbol || '?';
    return `<span class="summary-chip">A${idx + 1}: ${xSymbol} / ${qkSymbol}</span>`;
  }).join('');
  const hasInnerPoints = (data.inner_points || []).length > 0;

  const nd = ensureNodeData(nodeId);
  nd.ropPlotData = data;
  nd.fitInnerPoints = false;

  contentEl.innerHTML = `
    <div class="siso-summary-line">
      <span class="summary-chip"><strong>${data.dimension || config.dimension}D</strong></span>
      ${axisSummary}
    </div>
    <div class="siso-summary-line">
      <label class="summary-chip ${hasInnerPoints ? '' : 'text-dim'}" style="display:inline-flex;align-items:center;gap:6px;cursor:${hasInnerPoints ? 'pointer' : 'default'};">
        <input type="checkbox" id="${nodeId}-fit-inner-points" data-action="refreshROPPolyhedronPlot" data-node="${nodeId}" ${hasInnerPoints ? '' : 'disabled'}>
        Fit inner points
      </label>
    </div>
    <div class="plot-container" id="${nodeId}-plot"></div>
  `;

  commitWorkspaceSnapshot('rop-polyhedron');
  setTimeout(() => {
    refreshROPPolyhedronPlot(nodeId);
    setupPlotResize(nodeId, `${nodeId}-plot`);
  }, 50);
}

export function refreshROPPolyhedronPlot(nodeId) {
  const data = getNodeData(nodeId).ropPlotData;
  if (!data) return;
  const fitInnerPoints = document.getElementById(`${nodeId}-fit-inner-points`)?.checked ?? false;
  if (nodeRegistry[nodeId]) {
    ensureNodeData(nodeId).fitInnerPoints = fitInnerPoints;
  }
  plotROPPolyhedron(data, `${nodeId}-plot`, { fitInnerPoints });
  commitWorkspaceSnapshot('rop-polyhedron-fit');
}

export function getROPPlotBounds(data, options = {}) {
  const dimension = data.dimension || 2;
  const fitInnerPoints = options.fitInnerPoints === true;
  const mins = Array(dimension).fill(Infinity);
  const maxs = Array(dimension).fill(-Infinity);
  let hasFiniteCoords = false;

  const includeCoord = (coord) => {
    if (!Array.isArray(coord) || coord.length < dimension) return;
    const values = coord.slice(0, dimension).map(Number);
    if (values.some(v => !Number.isFinite(v))) return;
    hasFiniteCoords = true;
    for (let i = 0; i < dimension; i++) {
      if (values[i] < mins[i]) mins[i] = values[i];
      if (values[i] > maxs[i]) maxs[i] = values[i];
    }
  };

  const includeSegment = (segment) => {
    includeCoord(segment.from);
    includeCoord(segment.to);
  };

  (data.points || []).forEach(point => includeCoord(point.coords));
  (data.direct_edges || []).forEach(includeSegment);
  (data.indirect_edges || []).forEach(includeSegment);
  (data.direct_rays || []).forEach(includeSegment);
  (data.indirect_rays || []).forEach(includeSegment);
  if (fitInnerPoints) {
    (data.inner_points || []).forEach(includeCoord);
  }

  if (!hasFiniteCoords) return null;

  return mins.map((minValue, idx) => {
    const maxValue = maxs[idx];
    if (!Number.isFinite(minValue) || !Number.isFinite(maxValue)) return null;
    let pad = (maxValue - minValue) * 0.08;
    if (!(pad > 0)) {
      const scale = Math.max(Math.abs(minValue), Math.abs(maxValue), 1);
      pad = scale * 0.08;
    }
    return [minValue - pad, maxValue + pad];
  });
}

export function plotROPPolyhedron(data, plotId, options = {}) {
  const plotTheme = getPlotTheme();
  const lightTheme = prefersLightTheme();
  const ropPolyColors = lightTheme
    ? {
        directEdge: '#5b728a',
        indirectEdge: '#8ca0b4',
        directRay: '#2f94b7',
        indirectRay: '#67aac2',
        innerPoint: '#5c7288',
        regularVertex: '#2f9e44',
        asymptoticVertex: '#d9480f',
        legacyEdge: '#2b8a3e',
        legacyVertex: '#c92a2a',
      }
    : {
        directEdge: '#e5e7eb',
        indirectEdge: '#9aa0a6',
        directRay: '#8ecae6',
        indirectRay: '#8ecae6',
        innerPoint: 'rgba(148, 163, 184, 0.18)',
        regularVertex: '#b7efc5',
        asymptoticVertex: '#ffb4a2',
        legacyEdge: '#00ff00',
        legacyVertex: '#ff0000',
      };

  if (data.points && data.direct_edges) {
    const dimension = data.dimension || 2;
    const is3D = dimension === 3;
    const axisLabels = data.axis_labels || [];
    const traces = [];
    const ranges = getROPPlotBounds(data, options);

    const pushSegmentTrace = (segment, name, color, dash = 'solid', width = 2, opacity = 1, showLegend = false) => {
      const from = segment.from || [];
      const to = segment.to || [];
      const common = {
        mode: 'lines',
        name,
        showlegend: showLegend,
        hoverinfo: 'skip',
        opacity,
        line: is3D ? { color, width } : { color, width, dash },
      };
      if (is3D) {
        traces.push({
          x: [from[0], to[0]],
          y: [from[1], to[1]],
          z: [from[2], to[2]],
          type: 'scatter3d',
          ...common,
        });
      } else {
        traces.push({
          x: [from[0], to[0]],
          y: [from[1], to[1]],
          type: 'scatter',
          ...common,
        });
      }
    };

    (data.direct_edges || []).forEach((edge, idx) => pushSegmentTrace(edge, 'Direct edge', ropPolyColors.directEdge, 'solid', 2, 1, idx === 0));
    (data.indirect_edges || []).forEach((edge, idx) => pushSegmentTrace(edge, 'Indirect edge', ropPolyColors.indirectEdge, 'dash', 2, 1, idx === 0));
    (data.direct_rays || []).forEach((edge, idx) => pushSegmentTrace(edge, 'Singular ray', ropPolyColors.directRay, 'solid', 4, 0.95, idx === 0));
    (data.indirect_rays || []).forEach((edge, idx) => pushSegmentTrace(edge, 'Indirect singular ray', ropPolyColors.indirectRay, 'dash', 4, 0.85, idx === 0));

    if ((data.inner_points || []).length) {
      const inner = data.inner_points;
      const common = {
        mode: 'markers',
        name: 'Inner points',
        marker: {
          color: ropPolyColors.innerPoint,
          opacity: lightTheme ? 0.78 : 0.18,
          size: is3D ? (lightTheme ? 3 : 2) : (lightTheme ? 6 : 5),
          line: lightTheme ? { color: plotTheme.nodeOutlineColor, width: 0.6 } : { width: 0 },
        },
        hoverinfo: 'skip',
      };
      if (is3D) {
        traces.push({
          x: inner.map(point => point[0]),
          y: inner.map(point => point[1]),
          z: inner.map(point => point[2]),
          type: 'scatter3d',
          ...common,
        });
      } else {
        traces.push({
          x: inner.map(point => point[0]),
          y: inner.map(point => point[1]),
          type: 'scatter',
          ...common,
        });
      }
    }

    if ((data.points || []).length) {
      const points = data.points;
      const pointColor = point => point.point_type === 'asymptotic' ? ropPolyColors.asymptoticVertex : ropPolyColors.regularVertex;
      const hoverText = points.map(point => `Vertex ${point.vertex_idx}<br>Type: ${point.point_type}<br>Perm: [${(point.perm || []).join(',')}]`);
      const common = {
        mode: 'markers',
        name: 'Vertices',
        marker: {
          size: is3D ? 6 : 9,
          color: points.map(pointColor),
          line: { color: plotTheme.nodeOutlineColor, width: 1 },
        },
        hovertext: hoverText,
        hoverinfo: 'text',
      };
      if (is3D) {
        traces.push({
          x: points.map(point => point.coords[0]),
          y: points.map(point => point.coords[1]),
          z: points.map(point => point.coords[2]),
          type: 'scatter3d',
          ...common,
        });
      } else {
        traces.push({
          x: points.map(point => point.coords[0]),
          y: points.map(point => point.coords[1]),
          type: 'scatter',
          ...common,
        });
      }
    }

    const layout = {
      autosize: true,
      margin: { t: 40, b: 60, l: 70, r: 20 },
      title: {
        text: `ROP Polyhedron (${dimension}D)`,
        font: { color: plotTheme.titleColor, size: 11 },
        y: 0.98,
        yanchor: 'top',
      },
      showlegend: true,
      legend: { font: { color: plotTheme.fontColor, size: 9 } },
    };

    if (is3D) {
      layout.scene = {
        xaxis: { title: axisLabels[0] || 'Axis 1', range: ranges?.[0] },
        yaxis: { title: axisLabels[1] || 'Axis 2', range: ranges?.[1] },
        zaxis: { title: axisLabels[2] || 'Axis 3', range: ranges?.[2] },
      };
    } else {
      layout.xaxis = { title: axisLabels[0] || 'Axis 1', range: ranges?.[0] };
      layout.yaxis = { title: axisLabels[1] || 'Axis 2', range: ranges?.[1] };
    }

    Plotly.newPlot(plotId, traces, applyPlotLayoutTheme(layout), { responsive: true, displayModeBar: false, scrollZoom: true });
    return;
  }

  const { output_expr, param1_symbol, param2_symbol, vertices, edges } = data;
  const traces = [];

  (edges || []).forEach(edge => {
    traces.push({
      x: edge.ro1,
      y: edge.ro2,
      mode: 'lines',
      line: { color: ropPolyColors.legacyEdge, width: 2 },
      showlegend: false,
      hoverinfo: 'skip',
    });
  });

  if ((vertices || []).length > 0) {
    const vertexRO1 = vertices.map(v => v.ro1);
    const vertexRO2 = vertices.map(v => v.ro2);
    const hoverText = vertices.map(v => `Vertex ${v.idx}<br>Nullity: ${v.nullity}<br>Perm: [${v.perm.join(',')}]`);
    traces.push({
      x: vertexRO1,
      y: vertexRO2,
      mode: 'markers',
      marker: {
        color: ropPolyColors.legacyVertex,
        size: 8,
        line: { color: plotTheme.nodeOutlineColor, width: 1 },
      },
      name: 'Vertices',
      hovertext: hoverText,
      hoverinfo: 'text',
    });
  }

  const layout = {
    autosize: true,
    margin: { t: 40, b: 60, l: 70, r: 20 },
    title: { text: `ROP Polyhedron: ${output_expr}`, font: { color: plotTheme.titleColor, size: 11 }, y: 0.98, yanchor: 'top' },
    xaxis: { title: `\u2202log(${output_expr})/\u2202log(${param1_symbol})` },
    yaxis: { title: `\u2202log(${output_expr})/\u2202log(${param2_symbol})` },
    showlegend: true,
  };

  Plotly.newPlot(plotId, traces, applyPlotLayoutTheme(layout), { responsive: true, displayModeBar: false, scrollZoom: true });
}
