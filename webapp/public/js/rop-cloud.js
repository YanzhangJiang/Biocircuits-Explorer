import { nodeRegistry, connections, ensureNodeData, getNodeData } from './state.js';
import { api, showToast, handleNodeError, cloneSerializable, splitCommaList, parseOptionalFloat, parseOptionalInteger, syncSelectOptions } from './api.js';
import { applyPlotLayoutTheme, getPlotTheme, hexToRgba, themedColorbar } from './theme.js';
import { quantileSorted, plotHeatmap } from './plotting.js';
import { setNodeLoading, setupPlotResize, setupPlotInteractionGuard, getSessionIdForNode, getModelForNode, getModelContextForNode, findUpstreamNodeByType } from './nodes.js';
import { getReactionsFromNode } from './model.js';
import { normalizeSISOConfig, getConnectedSISOConfig } from './siso.js';
import { commitWorkspaceSnapshot, getNodeSerialData } from './workspace.js';

export function parseSpeciesFromReactionSide(side) {
  const species = [];
  side.split('+').forEach(term => {
    const t = term.trim();
    if (!t) return;
    const m = t.match(/^([0-9]+)?\s*([A-Za-z][A-Za-z0-9_]*)$/);
    if (m) species.push(m[2]);
  });
  return species;
}

export function inferSpeciesOrderFromReactions(reactions) {
  const allSet = new Set();
  const productSet = new Set();
  reactions.forEach(rule => {
    const m = rule.match(/<->|<=>|↔/);
    if (!m) return;
    const parts = rule.split(m[0]);
    if (parts.length !== 2) return;
    const left = parseSpeciesFromReactionSide(parts[0]);
    const right = parseSpeciesFromReactionSide(parts[1]);
    left.forEach(s => allSet.add(s));
    right.forEach(s => {
      allSet.add(s);
      productSet.add(s);
    });
  });

  const allSpecies = Array.from(allSet).sort();
  const productSpecies = Array.from(productSet).sort();
  const freeSpecies = allSpecies.filter(s => !productSet.has(s));
  const orderedSpecies = [...freeSpecies, ...productSpecies];
  return { species: orderedSpecies, productSpecies };
}

export function refreshROPCloudTargetOptions(nodeId, reactions = null) {
  const sel = document.getElementById(`${nodeId}-target-species`);
  if (!sel) return;

  if (!reactions) {
    const rxConn = connections.find(c => c.toNode === nodeId && c.toPort === 'reactions');
    if (rxConn) reactions = getReactionsFromNode(rxConn.fromNode).reactions;
  }
  reactions = reactions || [];

  const { species, productSpecies } = inferSpeciesOrderFromReactions(reactions);
  const preferred = productSpecies.length ? productSpecies : species;
  const orderedTargets = [...preferred, ...species.filter(s => !preferred.includes(s))];

  const prev = sel.value;
  sel.innerHTML = '';
  if (!orderedTargets.length) {
    const opt = document.createElement('option');
    opt.value = '';
    opt.textContent = '(target)';
    sel.appendChild(opt);
    return;
  }

  orderedTargets.forEach(sym => {
    const opt = document.createElement('option');
    opt.value = sym;
    opt.textContent = sym;
    sel.appendChild(opt);
  });

  if (prev && orderedTargets.includes(prev)) {
    sel.value = prev;
  }
}

export function updateROPCloudMode(nodeId) {
  const mode = document.getElementById(`${nodeId}-sampling-mode`)?.value || 'x_space';
  const xParams = document.getElementById(`${nodeId}-xspace-params`);
  const qkParams = document.getElementById(`${nodeId}-qk-params`);
  if (xParams) xParams.style.display = mode === 'x_space' ? '' : 'none';
  if (qkParams) qkParams.style.display = mode === 'qk' ? '' : 'none';
  if (mode === 'x_space') refreshROPCloudTargetOptions(nodeId);
}

export function getROPCloudPlotAxes(data) {
  const { reaction_orders = [], q_sym = [], d = 0 } = data;
  const plottedDims = d === 3 ? 3 : 2;
  const labels = [];
  for (let i = 0; i < plottedDims; i++) {
    labels.push(`\u2202log/\u2202log ${q_sym[i] || `q${i + 1}`}`);
  }
  return {
    plottedDims,
    labels,
    x: reaction_orders.map(row => row[0]).filter(Number.isFinite),
    y: reaction_orders.map(row => row[1]).filter(Number.isFinite),
    z: plottedDims === 3 ? reaction_orders.map(row => row[2]).filter(Number.isFinite) : [],
  };
}

export function computeROPCloudAxisRange(values, preset = 'robust') {
  const finite = (values || []).map(Number).filter(Number.isFinite).sort((a, b) => a - b);
  if (!finite.length) return [null, null];
  const minValue = finite[0];
  const maxValue = finite[finite.length - 1];

  let lo = minValue;
  let hi = maxValue;
  if (preset === 'robust' && finite.length > 4) {
    lo = quantileSorted(finite, 0.01);
    hi = quantileSorted(finite, 0.99);
  }

  if (!(Number.isFinite(lo) && Number.isFinite(hi))) return [null, null];
  if (Math.abs(hi - lo) < 1e-9) {
    const pad = Math.max(1, Math.abs(lo) * 0.08);
    return [lo - pad, hi + pad];
  }
  const pad = (hi - lo) * 0.06;
  return [lo - pad, hi + pad];
}

export function getROPCloudPresetRanges(data, preset = 'robust') {
  const axes = getROPCloudPlotAxes(data);
  const ranges = [
    computeROPCloudAxisRange(axes.x, preset),
    computeROPCloudAxisRange(axes.y, preset),
  ];
  if (axes.plottedDims === 3) {
    ranges.push(computeROPCloudAxisRange(axes.z, preset));
  }
  return ranges;
}

export function syncROPCloudFOVInputs(nodeId, ranges = []) {
  ranges.forEach((range, idx) => {
    const axis = idx + 1;
    const minEl = document.getElementById(`${nodeId}-fov-${axis}-min`);
    const maxEl = document.getElementById(`${nodeId}-fov-${axis}-max`);
    if (minEl && Number.isFinite(range?.[0])) minEl.value = range[0].toFixed(2);
    if (maxEl && Number.isFinite(range?.[1])) maxEl.value = range[1].toFixed(2);
  });
}

export function readROPCloudFOVRanges(nodeId, plottedDims) {
  const ranges = [];
  for (let i = 1; i <= plottedDims; i++) {
    const minVal = parseFloat(document.getElementById(`${nodeId}-fov-${i}-min`)?.value ?? '');
    const maxVal = parseFloat(document.getElementById(`${nodeId}-fov-${i}-max`)?.value ?? '');
    if (Number.isFinite(minVal) && Number.isFinite(maxVal) && minVal < maxVal) {
      ranges.push([minVal, maxVal]);
    } else {
      ranges.push(null);
    }
  }
  return ranges;
}

export function renderROPCloudOutput(nodeId, contentEl, data) {
  const axes = getROPCloudPlotAxes(data);
  const existingData = getNodeData(nodeId);
  const currentPreset = existingData.ropCloudPreset || 'robust';
  const presetRanges = getROPCloudPresetRanges(data, currentPreset);
  const savedRanges = Array.isArray(existingData.ropCloudRanges) ? existingData.ropCloudRanges : null;
  const initialRanges = savedRanges && savedRanges.length === axes.plottedDims ? savedRanges : presetRanges;

  const nd = ensureNodeData(nodeId);
  nd.ropCloudData = data;
  nd.ropCloudPreset = currentPreset;
  nd.ropCloudRanges = initialRanges;

  const rangeRows = axes.labels.map((label, idx) => `
    <div class="cloud-fov-row">
      <span class="cloud-fov-axis">${label}</span>
      <input type="number" step="0.1" id="${nodeId}-fov-${idx + 1}-min" data-action="refreshROPCloudPlot" data-node="${nodeId}">
      <span class="cloud-fov-sep">to</span>
      <input type="number" step="0.1" id="${nodeId}-fov-${idx + 1}-max" data-action="refreshROPCloudPlot" data-node="${nodeId}">
    </div>
  `).join('');

  contentEl.innerHTML = `
    <div class="siso-summary-line">
      <button type="button" class="btn btn-small" data-action="applyROPCloudFOVPreset" data-node="${nodeId}" data-preset="robust">Robust</button>
      <button type="button" class="btn btn-small" data-action="applyROPCloudFOVPreset" data-node="${nodeId}" data-preset="full">Full</button>
      <span class="summary-chip">Field of view</span>
    </div>
    <div class="cloud-fov-panel">
      ${rangeRows}
    </div>
    <div class="plot-container" id="${nodeId}-plot"></div>
  `;

  syncROPCloudFOVInputs(nodeId, initialRanges);
  commitWorkspaceSnapshot('rop-cloud');
  setTimeout(() => {
    refreshROPCloudPlot(nodeId);
    setupPlotResize(nodeId, `${nodeId}-plot`);
  }, 50);
}

export function applyROPCloudFOVPreset(nodeId, preset) {
  const nd = getNodeData(nodeId);
  if (!nd.ropCloudData) return;
  const ranges = getROPCloudPresetRanges(nd.ropCloudData, preset);
  const d = ensureNodeData(nodeId);
  d.ropCloudPreset = preset;
  d.ropCloudRanges = ranges;
  syncROPCloudFOVInputs(nodeId, ranges);
  refreshROPCloudPlot(nodeId);
}

export function refreshROPCloudPlot(nodeId) {
  const nodeData = nodeRegistry[nodeId]?.data;
  const data = nodeData?.ropCloudData;
  if (!data) return;
  const plottedDims = getROPCloudPlotAxes(data).plottedDims;
  const ranges = readROPCloudFOVRanges(nodeId, plottedDims);
  nodeData.ropCloudRanges = ranges;
  plotROPCloud(data, `${nodeId}-plot`, { ranges });
  commitWorkspaceSnapshot('rop-cloud-fov');
}

export function plotROPCloud(data, plotId, options = {}) {
  const { reaction_orders, fret_values, q_sym, d } = data;
  const plotTheme = getPlotTheme();
  const baseLayout = {
    autosize: true,
    margin: { t: 40, b: 60, l: 70, r: 20 },
  };
  const ranges = Array.isArray(options.ranges) ? options.ranges : [];

  if (d === 2) {
    const x = reaction_orders.map(r => r[0]);
    const y = reaction_orders.map(r => r[1]);
    const traces = [{
      x, y, mode: 'markers', type: 'scatter',
      marker: {
        size: 2, color: fret_values.map(v => Math.log10(v + 1e-30)),
        colorscale: 'Viridis', showscale: true,
        colorbar: themedColorbar('log(FRET)'),
      },
    }];
    const layout = {
      ...baseLayout,
      title: { text: 'ROP Cloud (2D)', font: { color: plotTheme.titleColor, size: 11 }, y: 0.98, yanchor: 'top' },
      xaxis: { title: `\u2202log/\u2202log ${q_sym[0]}`, range: ranges[0] || undefined },
      yaxis: { title: `\u2202log/\u2202log ${q_sym[1]}`, range: ranges[1] || undefined },
    };
    Plotly.newPlot(plotId, traces, applyPlotLayoutTheme(layout), { responsive: true, displayModeBar: false, scrollZoom: true });
  } else if (d === 3) {
    const x = reaction_orders.map(r => r[0]);
    const y = reaction_orders.map(r => r[1]);
    const z = reaction_orders.map(r => r[2]);
    const traces = [{
      x, y, z, mode: 'markers', type: 'scatter3d',
      marker: {
        size: 2, color: fret_values.map(v => Math.log10(v + 1e-30)),
        colorscale: 'Viridis', showscale: true,
        colorbar: themedColorbar('log(FRET)'),
      },
    }];
    const layout = {
      ...baseLayout,
      title: { text: 'ROP Cloud (3D)', font: { color: plotTheme.titleColor, size: 11 }, y: 0.98, yanchor: 'top' },
      scene: {
        xaxis: { title: `\u2202log/\u2202log ${q_sym[0]}`, range: ranges[0] || undefined },
        yaxis: { title: `\u2202log/\u2202log ${q_sym[1]}`, range: ranges[1] || undefined },
        zaxis: { title: `\u2202log/\u2202log ${q_sym[2]}`, range: ranges[2] || undefined },
      },
    };
    Plotly.newPlot(plotId, traces, applyPlotLayoutTheme(layout), { responsive: true, displayModeBar: false, scrollZoom: true });
  } else {
    const x = reaction_orders.map(r => r[0]);
    const y = reaction_orders.map(r => r[1]);
    const traces = [{
      x, y, mode: 'markers', type: 'scatter',
      marker: { size: 2, color: '#6c8cff', opacity: 0.3 },
    }];
    const layout = {
      ...baseLayout,
      title: { text: `ROP Cloud (first 2 of ${d} dims)`, font: { color: plotTheme.titleColor, size: 11 }, y: 0.98, yanchor: 'top' },
      xaxis: { title: `\u2202log/\u2202log ${q_sym[0]}`, range: ranges[0] || undefined },
      yaxis: { title: `\u2202log/\u2202log ${q_sym[1]}`, range: ranges[1] || undefined },
    };
    Plotly.newPlot(plotId, traces, applyPlotLayoutTheme(layout), { responsive: true, displayModeBar: false, scrollZoom: true });
  }
}

export function updateROPCloudConfig(nodeId) {
  const mode = document.getElementById(`${nodeId}-sampling-mode`).value;
  const nSamples = parseInt(document.getElementById(`${nodeId}-samples`).value);

  const config = {
    mode: mode,
    n_samples: nSamples,
  };

  if (mode === 'x_space') {
    const targetSpecies = document.getElementById(`${nodeId}-target-species`)?.value || '';
    const logxMin = parseFloat(document.getElementById(`${nodeId}-logx-min`).value);
    const logxMax = parseFloat(document.getElementById(`${nodeId}-logx-max`).value);
    config.target_species = targetSpecies;
    config.logx_min = logxMin;
    config.logx_max = logxMax;
  } else {
    const span = parseInt(document.getElementById(`${nodeId}-span`).value);
    config.span = span;
  }

  nodeRegistry[nodeId].data.config = config;
  showToast('Configuration updated');
}

export async function executeROPCloudResult(nodeId) {
  const conn = connections.find(c => c.toNode === nodeId && c.toPort === 'params');
  if (!conn) {
    alert('Please connect to a ROP Cloud Config node');
    return;
  }

  const paramsNode = nodeRegistry[conn.fromNode];
  if (!paramsNode) {
    alert('Config node has no configuration. Please configure it first.');
    return;
  }

  const config = getNodeSerialData(conn.fromNode, paramsNode.type || 'rop-cloud-params');
  paramsNode.data = paramsNode.data || {};
  paramsNode.data.config = config;
  setNodeLoading(nodeId, true);
  const contentEl = document.getElementById(`${nodeId}-content`);

  try {
    let data;
    if (config.mode === 'qk') {
      const sessionId = getSessionIdForNode(nodeId);
      if (!sessionId) throw new Error('Build the connected model first, or switch to x-space mode');
      const modelConn = connections.find(c => c.toNode === conn.fromNode && c.toPort === 'model');
      if (!modelConn) throw new Error('qK mode requires Model input connection');
      data = await api('rop_cloud', {
        sampling_mode: 'qk',
        session_id: sessionId,
        n_samples: config.samples,
        span: config.span,
      });
    } else {
      const rxConn = connections.find(c => c.toNode === conn.fromNode && c.toPort === 'reactions');
      if (!rxConn) throw new Error('x-space mode requires Reactions input connection');
      const { reactions } = getReactionsFromNode(rxConn.fromNode);
      if (!reactions.length) throw new Error('Add at least one reaction in the connected Reaction Network');
      data = await api('rop_cloud', {
        sampling_mode: 'x_space',
        reactions: reactions,
        n_samples: config.samples,
        logx_min: config.logxMin,
        logx_max: config.logxMax,
        target_species: config.targetSpecies || '',
      });
    }

    renderROPCloudOutput(nodeId, contentEl, data);
  } catch (e) {
    handleNodeError(e, nodeId, 'ROP cloud');
    contentEl.innerHTML = `<div class="node-error">${e.message}</div>`;
  } finally {
    setNodeLoading(nodeId, false);
  }
}

export function updateFRETConfig(nodeId) {
  const grid = parseInt(document.getElementById(`${nodeId}-grid`).value);
  nodeRegistry[nodeId].data.config = { n_grid: grid };
  showToast('Configuration updated');
}

export async function executeFRETResult(nodeId) {
  const conn = connections.find(c => c.toNode === nodeId && c.toPort === 'params');
  if (!conn) {
    alert('Please connect to a FRET Config node');
    return;
  }

  const paramsNode = nodeRegistry[conn.fromNode];
  if (!paramsNode) {
    alert('Config node has no configuration. Please configure it first.');
    return;
  }

  const config = getNodeSerialData(conn.fromNode, paramsNode.type || 'fret-params');
  paramsNode.data = paramsNode.data || {};
  paramsNode.data.config = config;
  setNodeLoading(nodeId, true);
  const contentEl = document.getElementById(`${nodeId}-content`);

  try {
    const sessionId = getSessionIdForNode(nodeId);
    if (!sessionId) throw new Error('Build the connected model first');
    const data = await api('fret_heatmap', {
      session_id: sessionId,
      n_grid: config.grid,
      logq_min: config.min,
      logq_max: config.max,
    });
    if (nodeRegistry[nodeId]) {
      ensureNodeData(nodeId).fretHeatmapData = data;
    }

    contentEl.innerHTML = `<div class="plot-container" id="${nodeId}-plot"></div>`;
    commitWorkspaceSnapshot('fret-heatmap');
    setTimeout(() => {
      plotHeatmap(data, `${nodeId}-plot`);
      setupPlotResize(nodeId, `${nodeId}-plot`);
    }, 50);
  } catch (e) {
    handleNodeError(e, nodeId, 'FRET heatmap');
    contentEl.innerHTML = `<div class="node-error">${e.message}</div>`;
  } finally {
    setNodeLoading(nodeId, false);
  }
}
