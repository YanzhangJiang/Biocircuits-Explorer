import {
  nodeRegistry,
  connections,
  ensureNodeData,
  getWorkspaceRuntimeEpoch,
} from './state.js';
import { api, handleNodeError, renderNodeError, syncSelectOptions } from './api.js';
import { applyPlotLayoutTheme, getPlotTheme } from './theme.js';
import { setNodeLoading, setupPlotResize, getModelContextForNode, ensureModelSession } from './nodes.js';
import { commitWorkspaceSnapshot } from './workspace.js';
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

const REGIME_GRAPH_ENDPOINT = '/api/v1/build_graph';
const REGIME_GRAPH_LIFECYCLE_KEY = '_regimeGraphLifecycle';

function canonicalLifecycleValue(value) {
  if (Array.isArray(value)) return value.map(canonicalLifecycleValue);
  if (!value || typeof value !== 'object') return value;
  const normalized = {};
  for (const key of Object.keys(value).sort()) normalized[key] = canonicalLifecycleValue(value[key]);
  return normalized;
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

function modelIdentity(nodeId) {
  const context = getModelContextForNode(nodeId);
  return {
    networkIrHash: context?.networkIrHash || context?.model?.network_ir_hash || null,
    networkIr: context?.networkIr || context?.model?.network_ir || null,
    inputFingerprint: context?.inputFingerprint || null,
  };
}

function readRegimeGraphConfig(nodeId) {
  const info = nodeRegistry[nodeId];
  const modeEl = document.getElementById(`${nodeId}-graph-mode`);
  const changeEl = document.getElementById(`${nodeId}-change-qk`);
  const saved = info?.data?.config || {};
  return {
    graphMode: modeEl?.value || saved.graphMode || 'qk',
    changeQK: changeEl?.value || saved.changeQK || '',
    viewMode: '3d',
  };
}

function currentRegimeGraphContext(nodeId) {
  const owner = nodeRegistry[nodeId];
  if (!owner) return null;
  return {
    owner,
    workspaceEpoch: getWorkspaceRuntimeEpoch(),
    inputFingerprint: JSON.stringify(canonicalLifecycleValue({
      config: readRegimeGraphConfig(nodeId),
      model: modelIdentity(nodeId),
      upstream: upstreamConnectionIdentity(nodeId),
    })),
    endpoint: REGIME_GRAPH_ENDPOINT,
  };
}

function syncRegimeGraphLifecycle(info, lifecycle) {
  if (!info || !lifecycle) return;
  info.data = info.data || {};
  info.data.lifecycle = serializeExecutionLifecycle(lifecycle);
}

function regimeGraphLifecycleFor(info) {
  if (!info[REGIME_GRAPH_LIFECYCLE_KEY]) {
    const lifecycle = createExecutionLifecycle();
    info[REGIME_GRAPH_LIFECYCLE_KEY] = lifecycle;
    if (info.data?.graphData) {
      const nodeId = Object.keys(nodeRegistry).find(id => nodeRegistry[id] === info);
      const context = nodeId ? currentRegimeGraphContext(nodeId) : null;
      if (context) {
        restoreHistoricalLifecycle(lifecycle, {
          context,
          result: info.data.graphData,
          evidence: info.data?.lifecycle?.evidence || null,
        });
        syncRegimeGraphLifecycle(info, lifecycle);
      }
    }
  }
  return info[REGIME_GRAPH_LIFECYCLE_KEY];
}

function clearRegimeGraphTimer(info) {
  if (info?._regimeGraphPlotTimer == null) return;
  clearTimeout(info._regimeGraphPlotTimer);
  delete info._regimeGraphPlotTimer;
}

function settleRegimeGraphLoading(nodeId, lifecycle, ticket) {
  if (!ticket || nodeRegistry[nodeId] !== ticket.owner) return;
  const runtime = inspectExecutionLifecycle(lifecycle);
  if (runtime.currentTicket === ticket ||
      (runtime.currentTicket == null && runtime.loading === false)) {
    setNodeLoading(nodeId, false);
  }
}

export function invalidateRegimeGraphResult(nodeId, reason = 'regime-graph-input-changed') {
  const info = nodeRegistry[nodeId];
  if (!info || info.type !== 'regime-graph') return false;
  const lifecycle = regimeGraphLifecycleFor(info);
  const runtime = inspectExecutionLifecycle(lifecycle);
  if (runtime.owner === info && runtime.workspaceEpoch != null) {
    invalidateLifecycle(lifecycle, {
      owner: info,
      workspaceEpoch: runtime.workspaceEpoch,
      reason,
    });
  }
  clearRegimeGraphTimer(info);
  info.data = info.data || {};
  delete info.data.graphData;
  syncRegimeGraphLifecycle(info, lifecycle);
  setNodeLoading(nodeId, false);
  const contentEl = document.getElementById(`${nodeId}-content`);
  if (contentEl) {
    contentEl.dataset.resultState = 'invalidated';
    contentEl.innerHTML = '<span class="text-dim">Inputs changed — rebuild the Regime Graph.</span>';
  }
  return true;
}

export function installRegimeGraphInvalidation(nodeId) {
  const node = document.getElementById(nodeId);
  if (!node) return;
  node.querySelectorAll('.auto-update').forEach(control => {
    const eventType = control.tagName === 'SELECT' || control.type === 'checkbox' ? 'change' : 'input';
    control.addEventListener(eventType, () => {
      invalidateRegimeGraphResult(nodeId, 'regime-graph-config-input-changed');
    });
  });
}

export function inspectRegimeGraphLifecycle(nodeId) {
  const info = nodeRegistry[nodeId];
  return info?.type === 'regime-graph'
    ? inspectExecutionLifecycle(regimeGraphLifecycleFor(info))
    : null;
}

export function updateRegimeGraphMode(nodeId) {
  const modeEl = document.getElementById(`${nodeId}-graph-mode`);
  const changeRow = document.getElementById(`${nodeId}-change-qk-row`);
  if (!modeEl || !changeRow) return;
  changeRow.style.display = modeEl.value === 'siso' ? '' : 'none';
}

export async function executeRegimeGraph(nodeId) {
  const contentEl = document.getElementById(`${nodeId}-content`);
  if (!contentEl) return false;

  const owner = nodeRegistry[nodeId];
  if (!owner || owner.type !== 'regime-graph') return false;
  const lifecycle = regimeGraphLifecycleFor(owner);
  invalidateRegimeGraphResult(nodeId, 'regime-graph-execution-restarted');
  const modelContext = getModelContextForNode(nodeId);
  const qKSymbols = modelContext?.qK_syms || [];
  const modeEl = document.getElementById(`${nodeId}-graph-mode`);
  const changeEl = document.getElementById(`${nodeId}-change-qk`);
  const info = owner;
  const config = info?.data?.config || {};

  syncSelectOptions(changeEl, qKSymbols, config.changeQK || changeEl?.value, 0);
  updateRegimeGraphMode(nodeId);

  const graphMode = modeEl?.value || 'qk';
  const changeQK = changeEl?.value || qKSymbols[0] || '';
  const viewMode = '3d';

  info.data = info.data || {};
  info.data.config = { graphMode, changeQK, viewMode };

  if (graphMode === 'siso' && !changeQK) {
    const context = currentRegimeGraphContext(nodeId);
    if (context) {
      const ticket = beginLifecycle(lifecycle, context);
      blockLifecycle(lifecycle, ticket, {
        context,
        reason: 'Select a qK coordinate for SISO graph',
      });
      syncRegimeGraphLifecycle(owner, lifecycle);
    }
    renderNodeError(contentEl, new Error('Select a qK coordinate for SISO graph'));
    return false;
  }

  setNodeLoading(nodeId, true);
  const attemptRevision = (owner._regimeGraphAttemptRevision || 0) + 1;
  owner._regimeGraphAttemptRevision = attemptRevision;
  let ticket = null;
  try {
    const sessionId = await ensureModelSession(nodeId);
    if (nodeRegistry[nodeId] !== owner ||
        owner._regimeGraphAttemptRevision !== attemptRevision) return false;
    const beginContext = currentRegimeGraphContext(nodeId);
    if (!beginContext) return false;
    ticket = beginLifecycle(lifecycle, beginContext);
    syncRegimeGraphLifecycle(owner, lifecycle);

    const payload = {
      session_id: sessionId,
      graph_mode: graphMode,
    };
    if (graphMode === 'siso') payload.change_qK = changeQK;

    const data = await api('build_graph', payload, {
      statusIsCurrent: () => {
        const context = currentRegimeGraphContext(nodeId);
        return !!context && isCurrentLifecycle(lifecycle, ticket, context);
      },
    });
    const currentContext = currentRegimeGraphContext(nodeId);
    if (!currentContext || !commitLifecycle(lifecycle, ticket, {
      context: currentContext,
      result: data,
      evidence: { source_endpoint: REGIME_GRAPH_ENDPOINT },
      sessionId,
    })) {
      syncRegimeGraphLifecycle(owner, lifecycle);
      return false;
    }
    syncRegimeGraphLifecycle(owner, lifecycle);
    if (nodeRegistry[nodeId] !== owner) return false;
    ensureNodeData(nodeId).graphData = data;
    contentEl.innerHTML = `<div class="plot-container" id="${nodeId}-plot"></div>`;
    commitWorkspaceSnapshot('regime-graph');
    clearRegimeGraphTimer(owner);
    const timer = setTimeout(() => {
      if (owner._regimeGraphPlotTimer === timer) delete owner._regimeGraphPlotTimer;
      const context = currentRegimeGraphContext(nodeId);
      if (nodeRegistry[nodeId] !== owner || !context ||
          !isCurrentLifecycle(lifecycle, ticket, context)) return;
      plotRegimeGraph(data, `${nodeId}-plot`, { viewMode });
      setupPlotResize(nodeId, `${nodeId}-plot`);
    }, 50);
    owner._regimeGraphPlotTimer = timer;
    return true;
  } catch (e) {
    let current = false;
    const context = currentRegimeGraphContext(nodeId);
    if (ticket && context) {
      current = failLifecycle(lifecycle, ticket, { context, error: e });
    } else if (context && nodeRegistry[nodeId] === owner &&
               owner._regimeGraphAttemptRevision === attemptRevision) {
      ticket = beginLifecycle(lifecycle, context);
      current = failLifecycle(lifecycle, ticket, { context, error: e });
    }
    syncRegimeGraphLifecycle(owner, lifecycle);
    if (!current) return false;
    handleNodeError(e, nodeId, 'Regime graph');
    if (nodeRegistry[nodeId] === owner) renderNodeError(contentEl, e);
    return false;
  } finally {
    if (ticket) {
      releaseLifecycle(lifecycle, ticket);
      if (owner._regimeGraphAttemptRevision === attemptRevision) {
        settleRegimeGraphLoading(nodeId, lifecycle, ticket);
      }
    } else if (nodeRegistry[nodeId] === owner &&
               owner._regimeGraphAttemptRevision === attemptRevision) {
      setNodeLoading(nodeId, false);
    }
  }
}

// Node color encodes each regime's local Jacobian classification. Ordered
// best→worst so the legend reads top-down. Kept as the single source of truth
// for both the marker colors and the on-plot legend below.
export const REGIME_GRAPH_LEGEND = [
  { color: '#51cf66', label: 'Asymptotic · invertible' },
  { color: '#ffd43b', label: 'Non-asymptotic' },
  { color: '#ff6b6b', label: 'Singular' },
];

export function getRegimeGraphNodeColor(node) {
  if (node.singular) return '#ff6b6b';
  if (!node.asymptotic) return '#ffd43b';
  return '#51cf66';
}

export function parseRegimeGraphNodeSpecies(label) {
  if (!label) return [];
  return String(label)
    .replace(/^\s*\[/, '')
    .replace(/\]\s*$/, '')
    .split(',')
    .map(token => token.trim().replace(/^:+/, ''))
    .filter(Boolean);
}

export function formatRegimeGraphPermMapping(node) {
  const species = parseRegimeGraphNodeSpecies(node.label);
  if (!Array.isArray(node.perm) || !node.perm.length || species.length !== node.perm.length) {
    return node.label || 'n/a';
  }
  return node.perm.map((idx, i) => `${idx}→${species[i]}`).join(', ');
}

export function getRegimeGraph3DMarkerSize(baseSize, nodeCount) {
  const n = Math.max(1, Number(nodeCount) || 1);
  const countFactor = Math.max(0.95, Math.min(2.05, 1.9 - 0.016 * (n - 1)));
  const size = (baseSize * 0.58 + 5.5) * countFactor;
  return Math.max(13, Math.min(34, size));
}

export function getRegimeGraph3DTextSize(nodeCount) {
  const n = Math.max(1, Number(nodeCount) || 1);
  return Math.max(10, Math.min(15, 15 - 0.11 * (n - 1)));
}

export function getRegimeGraph3DCamera(nodeCount) {
  const n = Math.max(1, Number(nodeCount) || 1);
  const eyeScale = Math.max(0.88, Math.min(1.18, 0.92 + 0.012 * (n - 1)));
  return {
    center: { x: 0, y: 0, z: 0 },
    eye: { x: eyeScale * 1.04, y: eyeScale * 1.06, z: eyeScale * 0.8 },
    up: { x: 0, y: 0, z: 1 },
  };
}

export function buildCircularGraphPositions(nodes, dimensions = 2) {
  const positions = {};
  const n = Math.max(nodes.length, 1);
  const radius = 2 + Math.sqrt(n) * 0.5;
  nodes.forEach((node, idx) => {
    const angle = (2 * Math.PI * idx) / n;
    const base = {
      x: radius * Math.cos(angle),
      y: radius * Math.sin(angle),
      z: 0,
    };
    if (dimensions === 3) {
      base.z = 1.1 * Math.sin(angle * 1.7 + idx * 0.4) + ((idx % 5) - 2) * 0.35;
    }
    positions[node.id] = base;
  });
  return positions;
}

export function relaxGraphLayout(nodes, edges, positions, dimensions = 2, iterations = 180) {
  const hasZ = dimensions === 3;
  for (let iter = 0; iter < iterations; iter++) {
    const forces = {};
    nodes.forEach(node => {
      forces[node.id] = { x: 0, y: 0, z: 0 };
    });

    for (let i = 0; i < nodes.length; i++) {
      for (let j = i + 1; j < nodes.length; j++) {
        const a = nodes[i].id;
        const b = nodes[j].id;
        let dx = positions[a].x - positions[b].x;
        let dy = positions[a].y - positions[b].y;
        let dz = hasZ ? positions[a].z - positions[b].z : 0;
        let dist = Math.sqrt(dx * dx + dy * dy + dz * dz) + 0.02;
        let repulsion = (hasZ ? 3.4 : 3.0) / (dist * dist);
        forces[a].x += repulsion * dx / dist;
        forces[a].y += repulsion * dy / dist;
        forces[b].x -= repulsion * dx / dist;
        forces[b].y -= repulsion * dy / dist;
        if (hasZ) {
          forces[a].z += repulsion * dz / dist;
          forces[b].z -= repulsion * dz / dist;
        }
      }
    }

    edges.forEach(edge => {
      const source = positions[edge.source];
      const target = positions[edge.target];
      if (!source || !target) return;
      let dx = target.x - source.x;
      let dy = target.y - source.y;
      let dz = hasZ ? target.z - source.z : 0;
      let dist = Math.sqrt(dx * dx + dy * dy + dz * dz) + 0.02;
      let attraction = dist * (hasZ ? 0.08 : 0.1);
      forces[edge.source].x += attraction * dx / dist;
      forces[edge.source].y += attraction * dy / dist;
      forces[edge.target].x -= attraction * dx / dist;
      forces[edge.target].y -= attraction * dy / dist;
      if (hasZ) {
        forces[edge.source].z += attraction * dz / dist;
        forces[edge.target].z -= attraction * dz / dist;
      }
    });

    nodes.forEach(node => {
      const centerPull = hasZ ? 0.02 : 0.015;
      forces[node.id].x -= positions[node.id].x * centerPull;
      forces[node.id].y -= positions[node.id].y * centerPull;
      if (hasZ) forces[node.id].z -= positions[node.id].z * centerPull;
    });

    const cooling = (hasZ ? 0.18 : 0.16) * (1 - 0.65 * (iter / Math.max(iterations, 1)));
    nodes.forEach(node => {
      positions[node.id].x += forces[node.id].x * cooling;
      positions[node.id].y += forces[node.id].y * cooling;
      if (hasZ) positions[node.id].z += forces[node.id].z * cooling;
    });
  }
}

export function normalizeGraphLayout(nodes, positions, dimensions = 2) {
  if (!nodes.length) return;
  const axes = dimensions === 3 ? ['x', 'y', 'z'] : ['x', 'y'];
  axes.forEach(axis => {
    const values = nodes.map(node => Number(positions[node.id]?.[axis]) || 0);
    const mean = values.reduce((sum, value) => sum + value, 0) / Math.max(values.length, 1);
    nodes.forEach(node => {
      positions[node.id][axis] -= mean;
    });
  });

  let maxAbs = 0;
  nodes.forEach(node => {
    maxAbs = Math.max(maxAbs, Math.abs(positions[node.id].x), Math.abs(positions[node.id].y));
    if (dimensions === 3) maxAbs = Math.max(maxAbs, Math.abs(positions[node.id].z));
  });
  const scale = maxAbs > 0 ? 5.2 / maxAbs : 1;
  nodes.forEach(node => {
    positions[node.id].x *= scale;
    positions[node.id].y *= scale;
    if (dimensions === 3) positions[node.id].z *= scale;
  });
}

export function computeGraphLayout(nodes, edges, viewMode = '2d') {
  const dimensions = viewMode === '3d' ? 3 : 2;
  const hasBackendPositions = nodes.every(node => Number.isFinite(node.x) && Number.isFinite(node.y));
  const positions = hasBackendPositions ? {} : buildCircularGraphPositions(nodes, dimensions);

  if (hasBackendPositions) {
    nodes.forEach((node, idx) => {
      positions[node.id] = {
        x: Number(node.x),
        y: Number(node.y),
        z: dimensions === 3
          ? ((idx % 5) - 2) * 0.45 + (node.singular ? 0.55 : 0) + (!node.asymptotic ? -0.25 : 0.15)
          : 0,
      };
    });
  }

  if (dimensions === 3 || !hasBackendPositions) {
    relaxGraphLayout(nodes, edges, positions, dimensions, dimensions === 3 ? 220 : 200);
  }
  normalizeGraphLayout(nodes, positions, dimensions);
  return positions;
}

export function plotRegimeGraph(data, plotId, options = {}) {
  const { nodes = [], edges = [], graph_label, change_qK } = data;
  const viewMode = options.viewMode === '2d' ? '2d' : '3d';
  const is3D = viewMode === '3d';
  const plotTheme = getPlotTheme();
  const positions = computeGraphLayout(nodes, edges, viewMode);

  // Node size scaling: use volume-based sizes from backend with improved scaling
  const rawSizes = nodes.map(node => Number(node.size)).filter(size => Number.isFinite(size));
  const minRawSize = rawSizes.length ? Math.min(...rawSizes) : 44;
  const maxRawSize = rawSizes.length ? Math.max(...rawSizes) : 44;
  const rawSpan = Math.max(maxRawSize - minRawSize, 1e-9);
  const scaledNodeSize = new Map();
  nodes.forEach(node => {
    const raw = Number(node.size);
    if (!Number.isFinite(raw)) {
      scaledNodeSize.set(node.id, 30);
      return;
    }
    // Use logarithmic scaling for better visual distribution when volume varies greatly
    const normalized = rawSpan < 1e-6 ? 0.5 : (raw - minRawSize) / rawSpan;
    const logScaled = rawSpan > 100
      ? Math.log10(1 + normalized * 9) // log scale for large ranges
      : normalized; // linear scale for small ranges
    const scaled = 18 + 40 * logScaled;
    scaledNodeSize.set(node.id, Math.max(12, Math.min(60, scaled)));
  });

  const traces = [];
  const nodeById = new Map(nodes.map(node => [node.id, node]));
  const arrowAnnotations = [];
  const edgeLabelAnnotations = [];
  const edgeMidX = [];
  const edgeMidY = [];
  const edgeMidZ = [];
  const edgeHoverText = [];
  const labeledEdgeMidX = [];
  const labeledEdgeMidY = [];
  const labeledEdgeMidZ = [];
  const labeledEdgeText = [];
  const coneX = [];
  const coneY = [];
  const coneZ = [];
  const coneU = [];
  const coneV = [];
  const coneW = [];

  // Pre-compute edge distances for adaptive arrow sizing
  const edgeDistances = edges.map(edge => {
    const sourcePos = positions[edge.source];
    const targetPos = positions[edge.target];
    if (!sourcePos || !targetPos) return 0;
    const dx = targetPos.x - sourcePos.x;
    const dy = targetPos.y - sourcePos.y;
    const dz = is3D ? targetPos.z - sourcePos.z : 0;
    return Math.sqrt(dx * dx + dy * dy + dz * dz);
  }).filter(d => d > 0);

  const minDist = edgeDistances.length > 0 ? Math.min(...edgeDistances) : 0.5;
  const maxDist = edgeDistances.length > 0 ? Math.max(...edgeDistances) : 2.0;

  edges.forEach((edge, edgeIdx) => {
    const sourceNode = nodeById.get(edge.source);
    const targetNode = nodeById.get(edge.target);
    const sourcePos = positions[edge.source];
    const targetPos = positions[edge.target];
    if (!sourceNode || !targetNode || !sourcePos || !targetPos) return;

    const dx = targetPos.x - sourcePos.x;
    const dy = targetPos.y - sourcePos.y;
    const dz = is3D ? targetPos.z - sourcePos.z : 0;
    const dist = Math.sqrt(dx * dx + dy * dy + dz * dz) + 1e-9;
    const ux = dx / dist;
    const uy = dy / dist;
    const uz = is3D ? dz / dist : 0;
    const sourcePad = (scaledNodeSize.get(edge.source) || 30) * 0.016;
    const targetPad = (scaledNodeSize.get(edge.target) || 30) * 0.02;
    const x0 = sourcePos.x + ux * sourcePad;
    const y0 = sourcePos.y + uy * sourcePad;
    const z0 = sourcePos.z + uz * sourcePad;
    const x1 = targetPos.x - ux * targetPad;
    const y1 = targetPos.y - uy * targetPad;
    const z1 = targetPos.z - uz * targetPad;
    const directionText = (edge.label || '').trim();
    const edgeInfoText = `Edge: #${edge.source} → #${edge.target}<br>Direction: ${directionText || 'n/a'}<br>From: [${sourceNode.perm.join(',')}]<br>To: [${targetNode.perm.join(',')}]`;

    if (is3D) {
      traces.push({
        x: [x0, x1],
        y: [y0, y1],
        z: [z0, z1],
        mode: 'lines',
        type: 'scatter3d',
        line: { color: plotTheme.edgeLine3DColor, width: 7 },
        hoverinfo: 'skip',
        showlegend: false,
      });

      // Cone size is controlled solely by trace-level sizeref; feed unit vectors
      // so every cone is the same physical size regardless of edge length or
      // per-graph distance distribution. (Previously u/v/w were scaled by an
      // adaptive arrowLength, which combined with sizemode:'absolute' produced
      // ~3x intra-graph and ~10x inter-graph cone-size variation.)
      coneX.push(x1);
      coneY.push(y1);
      coneZ.push(z1);
      coneU.push(ux);
      coneV.push(uy);
      coneW.push(uz);
    } else {
      traces.push({
        x: [x0, x1],
        y: [y0, y1],
        mode: 'lines',
        type: 'scatter',
        line: { color: plotTheme.edgeLineColor, width: 1.5 },
        hoverinfo: 'skip',
        showlegend: false,
      });

      // Adaptive arrow length for 2D based on edge distance distribution
      const relDist = (dist - minDist) / Math.max(maxDist - minDist, 1e-9);
      const baseArrowLength = 0.06 + 0.10 * Math.sqrt(relDist);
      const arrowLength = Math.min(dist * 0.30, Math.max(0.04, baseArrowLength));
      arrowAnnotations.push({
        x: x1,
        y: y1,
        ax: x1 - ux * arrowLength,
        ay: y1 - uy * arrowLength,
        xref: 'x',
        yref: 'y',
        axref: 'x',
        ayref: 'y',
        showarrow: true,
        text: '',
        arrowhead: 2,
        arrowsize: 1.1,
        arrowwidth: 1.4,
        arrowcolor: plotTheme.edgeArrowColor,
        opacity: 0.95,
      });
    }

    const midX = (x0 + x1) / 2;
    const midY = (y0 + y1) / 2;
    const midZ = (z0 + z1) / 2;
    edgeMidX.push(midX);
    edgeMidY.push(midY);
    if (is3D) edgeMidZ.push(midZ);
    edgeHoverText.push(edgeInfoText);

    if (directionText) {
      labeledEdgeMidX.push(midX);
      labeledEdgeMidY.push(midY);
      if (is3D) labeledEdgeMidZ.push(midZ);
      labeledEdgeText.push(directionText);
    }

    if (!is3D && directionText) {
      const normalX = -uy;
      const normalY = ux;
      const offsetMag = 0.18 + (edgeIdx % 3) * 0.06;
      edgeLabelAnnotations.push({
        x: midX + normalX * offsetMag,
        y: midY + normalY * offsetMag,
        xref: 'x',
        yref: 'y',
        text: directionText,
        showarrow: false,
        font: { size: 10, color: plotTheme.edgeLabelColor },
        bgcolor: plotTheme.annotationBg,
        bordercolor: plotTheme.annotationBorderColor,
        borderwidth: 1,
        borderpad: 2,
        opacity: 0.98,
      });
    }
  });

  if (edges.length) {
    if (is3D) {
      traces.push({
        type: 'cone',
        x: coneX,
        y: coneY,
        z: coneZ,
        u: coneU,
        v: coneV,
        w: coneW,
        anchor: 'tip',
        sizemode: 'absolute',
        sizeref: 0.18,
        colorscale: [[0, plotTheme.edgeConeColor], [1, plotTheme.edgeConeColor]],
        showscale: false,
        hoverinfo: 'skip',
      });
      if (labeledEdgeText.length) {
        traces.push({
          x: labeledEdgeMidX,
          y: labeledEdgeMidY,
          z: labeledEdgeMidZ,
          mode: 'text',
          type: 'scatter3d',
          text: labeledEdgeText,
          textposition: 'top center',
          textfont: { size: getRegimeGraph3DTextSize(nodes.length), color: plotTheme.edgeLabelColor },
          hoverinfo: 'skip',
          showlegend: false,
        });
      }
      traces.push({
        x: edgeMidX,
        y: edgeMidY,
        z: edgeMidZ,
        mode: 'markers',
        type: 'scatter3d',
        marker: { size: 7, color: plotTheme.edgeHoverMarkerColor, line: { width: 0 } },
        hovertext: edgeHoverText,
        hoverinfo: 'text',
        showlegend: false,
      });
    } else {
      traces.push({
        x: edgeMidX,
        y: edgeMidY,
        mode: 'markers',
        type: 'scatter',
        marker: { size: 10, color: plotTheme.edgeHoverMarkerColor, line: { width: 0 } },
        hovertext: edgeHoverText,
        hoverinfo: 'text',
        showlegend: false,
      });
    }
  }

  const nodeColors = nodes.map(getRegimeGraphNodeColor);
  const nodeHoverText = nodes.map(node => {
    const volumeStr = node.volume !== null && node.volume !== undefined
      ? `volume=${Number(node.volume).toExponential(3)}`
      : 'volume=n/a';
    return `#${node.id}<br>Dominant species: ${node.label || 'n/a'}<br>Perm mapping: ${formatRegimeGraphPermMapping(node)}<br>${node.asymptotic ? 'Asymptotic' : 'Non-Asymp'} ${node.singular ? 'Singular' : 'Invertible'}<br>nullity=${node.nullity}<br>${volumeStr}`;
  });

  if (is3D) {
    traces.push({
      x: nodes.map(node => positions[node.id].x),
      y: nodes.map(node => positions[node.id].y),
      z: nodes.map(node => positions[node.id].z),
      mode: 'markers+text',
      type: 'scatter3d',
      marker: {
        size: nodes.map(node => getRegimeGraph3DMarkerSize(scaledNodeSize.get(node.id) || 30, nodes.length)),
        color: nodeColors,
        line: { width: 1.2, color: plotTheme.nodeOutlineColor },
      },
      text: nodes.map(node => `#${node.id}`),
      textposition: 'middle center',
      textfont: { size: getRegimeGraph3DTextSize(nodes.length), color: plotTheme.nodeTextColor },
      hovertext: nodeHoverText,
      hoverinfo: 'text',
      showlegend: false,
    });
  } else {
    traces.push({
      x: nodes.map(node => positions[node.id].x),
      y: nodes.map(node => positions[node.id].y),
      mode: 'markers+text',
      type: 'scatter',
      marker: {
        size: nodes.map(node => scaledNodeSize.get(node.id) || 30),
        color: nodeColors,
        line: { width: 1.2, color: plotTheme.nodeOutlineColor },
      },
      text: nodes.map(node => `#${node.id}`),
      textposition: 'middle center',
      textfont: { size: 9, color: plotTheme.nodeTextColor },
      hovertext: nodeHoverText,
      hoverinfo: 'text',
      showlegend: false,
    });

    traces.push({
      x: nodes.map(node => positions[node.id].x),
      y: nodes.map(node => positions[node.id].y - (scaledNodeSize.get(node.id) || 30) * 0.02 - 0.14),
      text: nodes.map(node => node.label || ''),
      mode: 'text',
      type: 'scatter',
      textposition: 'middle center',
      textfont: { size: 9, color: plotTheme.subtleTextColor },
      hoverinfo: 'skip',
      showlegend: false,
    });
  }

  // Legend: one dummy trace per color category. They carry no real points
  // (x/y/z are null) so nothing is drawn on the graph, but each shows up as a
  // labeled swatch explaining what the red/yellow/green node colors mean.
  REGIME_GRAPH_LEGEND.forEach(({ color, label }) => {
    const legendTrace = {
      x: [null],
      y: [null],
      mode: 'markers',
      type: is3D ? 'scatter3d' : 'scatter',
      marker: { size: 10, color, line: { width: 1.2, color: plotTheme.nodeOutlineColor } },
      name: label,
      hoverinfo: 'skip',
      showlegend: true,
    };
    if (is3D) legendTrace.z = [null];
    traces.push(legendTrace);
  });

  const titleText = `${graph_label || 'Regime Graph'}${change_qK ? ` (${change_qK})` : ''}<br><span style="font-size:11px;color:${plotTheme.subtleTextColor};">${viewMode.toUpperCase()} view · ${nodes.length} vertices, ${edges.length} edges</span>`;
  const layout = {
    autosize: true,
    showlegend: true,
    legend: {
      x: 0,
      y: 1,
      xanchor: 'left',
      yanchor: 'top',
      font: { size: 10 },
      itemsizing: 'constant',
      borderwidth: 0,
      // Purely informational — clicking shouldn't toggle the (empty) traces.
      itemclick: false,
      itemdoubleclick: false,
    },
    margin: { t: 40, b: 20, l: 20, r: 20 },
    dragmode: is3D ? 'orbit' : 'pan',
    title: {
      text: titleText,
      font: { color: plotTheme.titleColor, size: 13 },
      y: 0.98,
      yanchor: 'top',
    },
  };

  if (is3D) {
    layout.scene = {
      xaxis: { visible: false, showbackground: false, showgrid: false, zeroline: false },
      yaxis: { visible: false, showbackground: false, showgrid: false, zeroline: false },
      zaxis: { visible: false, showbackground: false, showgrid: false, zeroline: false },
      bgcolor: plotTheme.sceneBg,
      aspectmode: 'data',
      camera: getRegimeGraph3DCamera(nodes.length),
    };
  } else {
    layout.xaxis = { visible: false };
    layout.yaxis = { visible: false, scaleanchor: 'x' };
    layout.annotations = arrowAnnotations.concat(edgeLabelAnnotations);
  }

  Plotly.newPlot(plotId, traces, applyPlotLayoutTheme(layout), { responsive: true, displayModeBar: false, scrollZoom: true });
}
