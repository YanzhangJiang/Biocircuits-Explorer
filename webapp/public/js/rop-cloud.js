import {
  nodeRegistry,
  connections,
  ensureNodeData,
  getNodeData,
  getWorkspaceRuntimeEpoch,
} from './state.js';
import { api, showToast, handleNodeError, renderNodeError, escapeHtml } from './api.js';
import { applyPlotLayoutTheme, getPlotTheme, themedColorbar } from './theme.js';
import { quantileSorted, plotHeatmap } from './plotting.js';
import {
  formatPartialValidityNotice,
  prepareRopCloudPlotData,
} from './plot-validity.js';
import { setNodeLoading, setupPlotResize, findUpstreamNodeByType, ensureModelSession } from './nodes.js';
import { getReactionsFromNode } from './model.js';
import { commitWorkspaceSnapshot, getNodeSerialData } from './workspace.js';
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

const ROP_CLOUD_ENDPOINT = '/api/v1/rop_cloud';
const FRET_ENDPOINT = '/api/v1/fret_heatmap';
const ROP_CLOUD_LIFECYCLE_KEY = '_ropCloudResultLifecycle';
const FRET_LIFECYCLE_KEY = '_fretResultLifecycle';

function canonicalLifecycleValue(value) {
  if (Array.isArray(value)) return value.map(canonicalLifecycleValue);
  if (!value || typeof value !== 'object') return value;
  const normalized = {};
  for (const key of Object.keys(value).sort()) normalized[key] = canonicalLifecycleValue(value[key]);
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
      edges.push([connection.fromNode, connection.fromPort, connection.toNode, connection.toPort]);
      visit(connection.fromNode);
    }
  };
  visit(nodeId);
  return edges.sort((left, right) => JSON.stringify(left).localeCompare(JSON.stringify(right)));
}

function modelIdentityForNode(nodeId) {
  const builderId = findUpstreamNodeByType(nodeId, 'model-builder');
  const context = builderId ? nodeRegistry[builderId]?.data?.modelContext : null;
  return {
    builderId: builderId || null,
    inputFingerprint: context?.inputFingerprint || null,
    networkIrHash: context?.networkIrHash || context?.model?.network_ir_hash || null,
    networkIr: context?.networkIr || context?.model?.network_ir || null,
  };
}

function connectedConfig(nodeId, expectedType) {
  const connection = connections.find(candidate =>
    candidate.toNode === nodeId && candidate.toPort === 'params');
  const source = connection ? nodeRegistry[connection.fromNode] : null;
  if (!connection || !source || source.type !== expectedType) return null;
  const config = getNodeSerialData(connection.fromNode, expectedType);
  return { connection, source, config };
}

function reactionIdentityForConfig(configNodeId) {
  const connection = connections.find(candidate =>
    candidate.toNode === configNodeId && candidate.toPort === 'reactions');
  if (!connection) return { sourceNodeId: null, reactions: [], kds: [] };
  const { reactions, kds } = getReactionsFromNode(connection.fromNode);
  return { sourceNodeId: connection.fromNode, reactions, kds };
}

function lifecycleContext(owner, endpoint, fingerprint) {
  if (!owner) return null;
  return {
    owner,
    workspaceEpoch: getWorkspaceRuntimeEpoch(),
    inputFingerprint: fingerprint,
    endpoint,
  };
}

function currentROPCloudContext(nodeId) {
  const owner = nodeRegistry[nodeId];
  if (!owner) return null;
  const connected = connectedConfig(nodeId, 'rop-cloud-params');
  return lifecycleContext(owner, ROP_CLOUD_ENDPOINT, stableLifecycleFingerprint({
    connections: upstreamConnectionIdentity(nodeId),
    config: connected?.config || null,
    model: modelIdentityForNode(nodeId),
    networkIr: connected ? reactionIdentityForConfig(connected.connection.fromNode) : null,
  }));
}

function currentFRETContext(nodeId) {
  const owner = nodeRegistry[nodeId];
  if (!owner) return null;
  const connected = connectedConfig(nodeId, 'fret-params');
  return lifecycleContext(owner, FRET_ENDPOINT, stableLifecycleFingerprint({
    connections: upstreamConnectionIdentity(nodeId),
    config: connected?.config || null,
    model: modelIdentityForNode(nodeId),
  }));
}

function syncLifecycle(info, lifecycle) {
  if (!info || !lifecycle) return;
  info.data = info.data || {};
  info.data.lifecycle = serializeExecutionLifecycle(lifecycle);
}

function lifecycleFor(info, key, endpoint, contextFactory, resultKey) {
  if (!info[key]) {
    const lifecycle = createExecutionLifecycle();
    info[key] = lifecycle;
    if (info.data?.[resultKey]) {
      const context = contextFactory();
      if (context) {
        restoreHistoricalLifecycle(lifecycle, {
          context,
          result: info.data[resultKey],
          evidence: info.data?.lifecycle?.evidence || null,
        });
        syncLifecycle(info, lifecycle);
      }
    }
  }
  return info[key];
}

function ropCloudLifecycleFor(info) {
  const nodeId = Object.keys(nodeRegistry).find(id => nodeRegistry[id] === info);
  return lifecycleFor(info, ROP_CLOUD_LIFECYCLE_KEY, ROP_CLOUD_ENDPOINT,
    () => nodeId ? currentROPCloudContext(nodeId) : null, 'ropCloudData');
}

function fretLifecycleFor(info) {
  const nodeId = Object.keys(nodeRegistry).find(id => nodeRegistry[id] === info);
  return lifecycleFor(info, FRET_LIFECYCLE_KEY, FRET_ENDPOINT,
    () => nodeId ? currentFRETContext(nodeId) : null, 'fretHeatmapData');
}

function clearTimer(info, key) {
  if (info?.[key] == null) return;
  clearTimeout(info[key]);
  delete info[key];
}

function invalidateBound(info, lifecycle, reason) {
  const runtime = inspectExecutionLifecycle(lifecycle);
  if (runtime.owner !== info || runtime.workspaceEpoch == null) return false;
  return invalidateLifecycle(lifecycle, {
    owner: info,
    workspaceEpoch: runtime.workspaceEpoch,
    reason,
  });
}

function settleLoading(nodeId, lifecycle, ticket) {
  if (!ticket || nodeRegistry[nodeId] !== ticket.owner) return;
  const runtime = inspectExecutionLifecycle(lifecycle);
  if (runtime.currentTicket === ticket ||
      (runtime.currentTicket == null && runtime.loading === false)) {
    setNodeLoading(nodeId, false);
  }
}

function setInvalidatedMessage(nodeId, message) {
  const contentEl = document.getElementById(`${nodeId}-content`);
  if (!contentEl) return;
  contentEl.dataset.resultState = 'invalidated';
  contentEl.innerHTML = `<span class="text-dim">${escapeHtml(message)}</span>`;
}

export function invalidateROPCloudResult(nodeId, reason = 'rop-cloud-input-changed') {
  const info = nodeRegistry[nodeId];
  if (!info || info.type !== 'rop-cloud-result') return false;
  const lifecycle = ropCloudLifecycleFor(info);
  invalidateBound(info, lifecycle, reason);
  clearTimer(info, '_ropCloudPlotTimer');
  info.data = info.data || {};
  delete info.data.ropCloudData;
  delete info.data.ropCloudRanges;
  syncLifecycle(info, lifecycle);
  setNodeLoading(nodeId, false);
  setInvalidatedMessage(nodeId, 'Inputs changed — run ROP Cloud again.');
  return true;
}

export function invalidateFRETResult(nodeId, reason = 'fret-input-changed') {
  const info = nodeRegistry[nodeId];
  if (!info || info.type !== 'fret-result') return false;
  const lifecycle = fretLifecycleFor(info);
  invalidateBound(info, lifecycle, reason);
  clearTimer(info, '_fretPlotTimer');
  info.data = info.data || {};
  delete info.data.fretHeatmapData;
  syncLifecycle(info, lifecycle);
  setNodeLoading(nodeId, false);
  setInvalidatedMessage(nodeId, 'Inputs changed — run FRET again.');
  return true;
}

export function invalidateDerivedResultsDownstreamOf(sourceNodeId, reason = 'derived-input-changed') {
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
      if (type === 'rop-cloud-result' && invalidateROPCloudResult(connection.toNode, reason)) {
        invalidated.push(connection.toNode);
      } else if (type === 'fret-result' && invalidateFRETResult(connection.toNode, reason)) {
        invalidated.push(connection.toNode);
      }
    }
  }
  return invalidated;
}

export function installDerivedResultInvalidation(nodeId) {
  const node = document.getElementById(nodeId);
  if (!node) return;
  node.querySelectorAll('.auto-update').forEach(control => {
    const eventType = control.tagName === 'SELECT' || control.type === 'checkbox' ? 'change' : 'input';
    control.addEventListener(eventType, () => {
      invalidateDerivedResultsDownstreamOf(nodeId, 'config-input-changed');
    });
  });
}

export function inspectROPCloudResultLifecycle(nodeId) {
  const info = nodeRegistry[nodeId];
  return info?.type === 'rop-cloud-result'
    ? inspectExecutionLifecycle(ropCloudLifecycleFor(info))
    : null;
}

export function inspectFRETResultLifecycle(nodeId) {
  const info = nodeRegistry[nodeId];
  return info?.type === 'fret-result'
    ? inspectExecutionLifecycle(fretLifecycleFor(info))
    : null;
}

export function parseSpeciesFromReactionSide(side) {
  const species = [];
  side.split('+').forEach(term => {
    const t = term.trim();
    if (!t) return;
    // SBML SId permits a leading underscore; mirror the Julia parser.
    const m = t.match(/^([0-9]+)?\s*([A-Za-z_][A-Za-z0-9_]*)$/);
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
  const { q_sym = [], d = 0 } = data;
  const reaction_orders = prepareRopCloudPlotData(data).reactionOrders;
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

export function renderROPCloudOutput(nodeId, contentEl, data, {
  plotGuard = null,
  timerOwner = null,
} = {}) {
  const axes = getROPCloudPlotAxes(data);
  const existingData = getNodeData(nodeId);
  const currentPreset = existingData.ropCloudPreset || 'robust';
  const presetRanges = getROPCloudPresetRanges(data, currentPreset);
  const savedRanges = Array.isArray(existingData.ropCloudRanges) ? existingData.ropCloudRanges : null;
  const initialRanges = savedRanges && savedRanges.length === axes.plottedDims ? savedRanges : presetRanges;
  const validityNotice = formatPartialValidityNotice(prepareRopCloudPlotData(data));

  const nd = ensureNodeData(nodeId);
  nd.ropCloudData = data;
  nd.ropCloudPreset = currentPreset;
  nd.ropCloudRanges = initialRanges;

  const rangeRows = axes.labels.map((label, idx) => `
    <div class="cloud-fov-row">
      <span class="cloud-fov-axis">${escapeHtml(label)}</span>
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
      ${validityNotice ? `<span class="summary-chip">${escapeHtml(validityNotice)}</span>` : ''}
    </div>
    <div class="cloud-fov-panel">
      ${rangeRows}
    </div>
    <div class="plot-container" id="${nodeId}-plot"></div>
  `;

  syncROPCloudFOVInputs(nodeId, initialRanges);
  commitWorkspaceSnapshot('rop-cloud');
  if (timerOwner) clearTimer(timerOwner, '_ropCloudPlotTimer');
  const timer = setTimeout(() => {
    if (timerOwner?._ropCloudPlotTimer === timer) delete timerOwner._ropCloudPlotTimer;
    if (plotGuard && !plotGuard()) return;
    refreshROPCloudPlot(nodeId);
    setupPlotResize(nodeId, `${nodeId}-plot`);
  }, 50);
  if (timerOwner) timerOwner._ropCloudPlotTimer = timer;
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
  const { q_sym, d } = data;
  const prepared = prepareRopCloudPlotData(data);
  const reaction_orders = prepared.reactionOrders;
  const fret_values = prepared.fretValues;
  const validityNotice = formatPartialValidityNotice(prepared);
  const plotTheme = getPlotTheme();
  const baseLayout = {
    autosize: true,
    margin: { t: 40, b: 60, l: 70, r: 20 },
    annotations: validityNotice ? [{
      text: validityNotice,
      xref: 'paper', yref: 'paper', x: 0, y: 1.08,
      xanchor: 'left', yanchor: 'bottom', showarrow: false,
      font: { color: plotTheme.fontColor, size: 10 },
    }] : [],
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
  const owner = nodeRegistry[nodeId];
  if (!owner || owner.type !== 'rop-cloud-result') return false;
  const lifecycle = ropCloudLifecycleFor(owner);
  invalidateROPCloudResult(nodeId, 'rop-cloud-execution-restarted');
  const conn = connections.find(c => c.toNode === nodeId && c.toPort === 'params');
  if (!conn) {
    const context = currentROPCloudContext(nodeId);
    if (context) {
      const ticket = beginLifecycle(lifecycle, context);
      blockLifecycle(lifecycle, ticket, {
        context,
        reason: 'Connect to a ROP Cloud Config node',
      });
      syncLifecycle(owner, lifecycle);
    }
    alert('Please connect to a ROP Cloud Config node');
    return false;
  }

  const paramsNode = nodeRegistry[conn.fromNode];
  if (!paramsNode) {
    const context = currentROPCloudContext(nodeId);
    if (context) {
      const ticket = beginLifecycle(lifecycle, context);
      blockLifecycle(lifecycle, ticket, {
        context,
        reason: 'ROP Cloud Config node has no configuration',
      });
      syncLifecycle(owner, lifecycle);
    }
    alert('Config node has no configuration. Please configure it first.');
    return false;
  }

  const config = getNodeSerialData(conn.fromNode, paramsNode.type || 'rop-cloud-params');
  paramsNode.data = paramsNode.data || {};
  paramsNode.data.config = config;
  setNodeLoading(nodeId, true);
  const contentEl = document.getElementById(`${nodeId}-content`);
  const attemptRevision = (owner._ropCloudAttemptRevision || 0) + 1;
  owner._ropCloudAttemptRevision = attemptRevision;
  let ticket = null;

  try {
    let data;
    let sessionId = null;
    if (config.mode === 'qk') {
      const modelConn = connections.find(c => c.toNode === conn.fromNode && c.toPort === 'model');
      if (!modelConn) throw new Error('qK mode requires Model input connection');
      sessionId = await ensureModelSession(nodeId);
      if (nodeRegistry[nodeId] !== owner || owner._ropCloudAttemptRevision !== attemptRevision) {
        return false;
      }
      const beginContext = currentROPCloudContext(nodeId);
      if (!beginContext) return false;
      ticket = beginLifecycle(lifecycle, beginContext);
      syncLifecycle(owner, lifecycle);
      data = await api('rop_cloud', {
        sampling_mode: 'qk',
        session_id: sessionId,
        n_samples: config.samples,
        span: config.span,
      }, {
        statusIsCurrent: () => {
          const context = currentROPCloudContext(nodeId);
          return !!context && isCurrentLifecycle(lifecycle, ticket, context);
        },
      });
    } else {
      const rxConn = connections.find(c => c.toNode === conn.fromNode && c.toPort === 'reactions');
      if (!rxConn) throw new Error('x-space mode requires Reactions input connection');
      const { reactions } = getReactionsFromNode(rxConn.fromNode);
      if (!reactions.length) throw new Error('Add at least one reaction in the connected Reaction Network');
      const beginContext = currentROPCloudContext(nodeId);
      if (!beginContext) return false;
      ticket = beginLifecycle(lifecycle, beginContext);
      syncLifecycle(owner, lifecycle);
      data = await api('rop_cloud', {
        sampling_mode: 'x_space',
        reactions: reactions,
        n_samples: config.samples,
        logx_min: config.logxMin,
        logx_max: config.logxMax,
        target_species: config.targetSpecies || '',
      }, {
        statusIsCurrent: () => {
          const context = currentROPCloudContext(nodeId);
          return !!context && isCurrentLifecycle(lifecycle, ticket, context);
        },
      });
    }

    const currentContext = currentROPCloudContext(nodeId);
    if (!currentContext || !commitLifecycle(lifecycle, ticket, {
      context: currentContext,
      result: data,
      evidence: {
        source_endpoint: ROP_CLOUD_ENDPOINT,
        partial: data?.partial === true,
        valid_count: data?.valid_count ?? null,
        invalid_count: data?.invalid_count ?? null,
      },
      sessionId,
    })) {
      syncLifecycle(owner, lifecycle);
      return false;
    }
    syncLifecycle(owner, lifecycle);
    if (nodeRegistry[nodeId] !== owner) return false;
    renderROPCloudOutput(nodeId, contentEl, data, {
      timerOwner: owner,
      plotGuard: () => {
        const context = currentROPCloudContext(nodeId);
        return nodeRegistry[nodeId] === owner && !!context &&
          isCurrentLifecycle(lifecycle, ticket, context);
      },
    });
    return true;
  } catch (e) {
    let current = false;
    const context = currentROPCloudContext(nodeId);
    if (ticket && context) {
      current = failLifecycle(lifecycle, ticket, { context, error: e });
    } else if (context && nodeRegistry[nodeId] === owner &&
               owner._ropCloudAttemptRevision === attemptRevision) {
      ticket = beginLifecycle(lifecycle, context);
      current = failLifecycle(lifecycle, ticket, { context, error: e });
    }
    syncLifecycle(owner, lifecycle);
    if (!current) return false;
    handleNodeError(e, nodeId, 'ROP cloud');
    if (nodeRegistry[nodeId] === owner) renderNodeError(contentEl, e);
    return false;
  } finally {
    if (ticket) {
      releaseLifecycle(lifecycle, ticket);
      if (owner._ropCloudAttemptRevision === attemptRevision) {
        settleLoading(nodeId, lifecycle, ticket);
      }
    } else if (nodeRegistry[nodeId] === owner &&
               owner._ropCloudAttemptRevision === attemptRevision) {
      setNodeLoading(nodeId, false);
    }
  }
}

export function updateFRETConfig(nodeId) {
  const grid = parseInt(document.getElementById(`${nodeId}-grid`).value);
  nodeRegistry[nodeId].data.config = { n_grid: grid };
  showToast('Configuration updated');
}

export async function executeFRETResult(nodeId) {
  const owner = nodeRegistry[nodeId];
  if (!owner || owner.type !== 'fret-result') return false;
  const lifecycle = fretLifecycleFor(owner);
  invalidateFRETResult(nodeId, 'fret-execution-restarted');
  const conn = connections.find(c => c.toNode === nodeId && c.toPort === 'params');
  if (!conn) {
    const context = currentFRETContext(nodeId);
    if (context) {
      const ticket = beginLifecycle(lifecycle, context);
      blockLifecycle(lifecycle, ticket, { context, reason: 'Connect to a FRET Config node' });
      syncLifecycle(owner, lifecycle);
    }
    alert('Please connect to a FRET Config node');
    return false;
  }

  const paramsNode = nodeRegistry[conn.fromNode];
  if (!paramsNode) {
    const context = currentFRETContext(nodeId);
    if (context) {
      const ticket = beginLifecycle(lifecycle, context);
      blockLifecycle(lifecycle, ticket, { context, reason: 'FRET Config node has no configuration' });
      syncLifecycle(owner, lifecycle);
    }
    alert('Config node has no configuration. Please configure it first.');
    return false;
  }

  const config = getNodeSerialData(conn.fromNode, paramsNode.type || 'fret-params');
  paramsNode.data = paramsNode.data || {};
  paramsNode.data.config = config;
  setNodeLoading(nodeId, true);
  const contentEl = document.getElementById(`${nodeId}-content`);
  const attemptRevision = (owner._fretAttemptRevision || 0) + 1;
  owner._fretAttemptRevision = attemptRevision;
  let ticket = null;

  try {
    const sessionId = await ensureModelSession(nodeId);
    if (nodeRegistry[nodeId] !== owner || owner._fretAttemptRevision !== attemptRevision) return false;
    const beginContext = currentFRETContext(nodeId);
    if (!beginContext) return false;
    ticket = beginLifecycle(lifecycle, beginContext);
    syncLifecycle(owner, lifecycle);
    const data = await api('fret_heatmap', {
      session_id: sessionId,
      n_grid: config.grid,
      logq_min: config.min,
      logq_max: config.max,
    }, {
      statusIsCurrent: () => {
        const context = currentFRETContext(nodeId);
        return !!context && isCurrentLifecycle(lifecycle, ticket, context);
      },
    });
    const currentContext = currentFRETContext(nodeId);
    if (!currentContext || !commitLifecycle(lifecycle, ticket, {
      context: currentContext,
      result: data,
      evidence: {
        source_endpoint: FRET_ENDPOINT,
        partial: data?.partial === true,
        valid_count: data?.valid_count ?? null,
        invalid_count: data?.invalid_count ?? null,
      },
      sessionId,
    })) {
      syncLifecycle(owner, lifecycle);
      return false;
    }
    syncLifecycle(owner, lifecycle);
    if (nodeRegistry[nodeId] !== owner) return false;
    ensureNodeData(nodeId).fretHeatmapData = data;

    contentEl.innerHTML = `<div class="plot-container" id="${nodeId}-plot"></div>`;
    commitWorkspaceSnapshot('fret-heatmap');
    clearTimer(owner, '_fretPlotTimer');
    const timer = setTimeout(() => {
      if (owner._fretPlotTimer === timer) delete owner._fretPlotTimer;
      const context = currentFRETContext(nodeId);
      if (nodeRegistry[nodeId] !== owner || !context ||
          !isCurrentLifecycle(lifecycle, ticket, context)) return;
      plotHeatmap(data, `${nodeId}-plot`);
      setupPlotResize(nodeId, `${nodeId}-plot`);
    }, 50);
    owner._fretPlotTimer = timer;
    return true;
  } catch (e) {
    let current = false;
    const context = currentFRETContext(nodeId);
    if (ticket && context) {
      current = failLifecycle(lifecycle, ticket, { context, error: e });
    } else if (context && nodeRegistry[nodeId] === owner &&
               owner._fretAttemptRevision === attemptRevision) {
      ticket = beginLifecycle(lifecycle, context);
      current = failLifecycle(lifecycle, ticket, { context, error: e });
    }
    syncLifecycle(owner, lifecycle);
    if (!current) return false;
    handleNodeError(e, nodeId, 'FRET heatmap');
    if (nodeRegistry[nodeId] === owner) renderNodeError(contentEl, e);
    return false;
  } finally {
    if (ticket) {
      releaseLifecycle(lifecycle, ticket);
      if (owner._fretAttemptRevision === attemptRevision) {
        settleLoading(nodeId, lifecycle, ticket);
      }
    } else if (nodeRegistry[nodeId] === owner && owner._fretAttemptRevision === attemptRevision) {
      setNodeLoading(nodeId, false);
    }
  }
}
