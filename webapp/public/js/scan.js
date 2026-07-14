// Biocircuits Explorer — Parameter Scan & ROP Polyhedron Functions

import {
  nodeRegistry,
  connections,
  ensureNodeData,
  getNodeData,
  getWorkspaceRuntimeEpoch,
} from './state.js';
import { api, showToast, handleNodeError, renderNodeError, escapeHtml, cloneSerializable } from './api.js';
import { applyPlotLayoutTheme, getPlotTheme, themedColorbar, prefersLightTheme } from './theme.js';
import { setNodeLoading, setupPlotResize, getModelContextFromBuilder, triggerConfigUpdate, ensureModelSession } from './nodes.js';
import { commitWorkspaceSnapshot, getNodeSerialData } from './workspace.js';
import { formatPartialValidityNotice, prepareScan1DPlotData, prepareScan2DPlotData } from './plot-validity.js';
import {
  beginScanExecution,
  blockScanExecution,
  clearStoredScanResult,
  commitScanExecution,
  executionDependencyConnections,
  failScanExecution,
  inspectScanExecution,
  invalidateScanExecution,
  isCurrentScanExecution,
  releaseScanExecution,
  scanUpstreamSignature,
  scheduleCurrentScanPlot,
} from './execution-lifecycle.js';
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

function syncScanValidityNotice(plotId, prepared) {
  if (typeof document === 'undefined') return;
  const plotEl = document.getElementById(plotId);
  if (!plotEl?.parentNode) return;
  const noticeId = `${plotId}-validity-status`;
  let noticeEl = document.getElementById(noticeId);
  const message = formatPartialValidityNotice(prepared);
  if (!message) {
    noticeEl?.remove();
    return;
  }
  if (!noticeEl) {
    noticeEl = document.createElement('div');
    noticeEl.id = noticeId;
    noticeEl.className = 'scan-validity-status text-dim';
    plotEl.parentNode.insertBefore(noticeEl, plotEl);
  }
  noticeEl.textContent = message;
}

function scanModelIdentity(nodeId) {
  const dependencyConnections = executionDependencyConnections();
  const visited = new Set();
  const findBuilder = (current) => {
    if (!current || visited.has(current)) return null;
    visited.add(current);
    if (nodeRegistry[current]?.type === 'model-builder') return current;
    for (const conn of dependencyConnections) {
      if (conn?.toNode !== current) continue;
      const found = findBuilder(conn.fromNode);
      if (found) return found;
    }
    return null;
  };
  const builderNodeId = findBuilder(nodeId);
  const context = builderNodeId ? getModelContextFromBuilder(builderNodeId) : null;
  if (!builderNodeId || !context) return null;
  return {
    builderNodeId,
    networkIrHash: context.networkIrHash || context.network_ir_hash || null,
    inputFingerprint: context.inputFingerprint || null,
    builtForRevision: context.builtForRevision ?? null,
  };
}

function scanDependencyFingerprint(
  nodeId,
  endpoint,
  config,
  modelIdentity = scanModelIdentity(nodeId),
) {
  return JSON.stringify({
    endpoint,
    config,
    model: modelIdentity,
    upstream: scanUpstreamSignature(nodeId),
  });
}

function canonicalScanEndpoint(endpoint) {
  return `/api/v1/${String(endpoint || '').replace(/^\/?(?:api\/v1\/)?/, '')}`;
}

function scanTicketContext(nodeId, ticket) {
  const owner = nodeRegistry[nodeId];
  if (!ticket || owner !== ticket.owner) return null;
  return {
    owner,
    workspaceEpoch: getWorkspaceRuntimeEpoch(),
    inputFingerprint: ticket.inputFingerprint,
    endpoint: ticket.endpoint,
  };
}

function scanExecutionContext(nodeId, definition, config, modelIdentity = scanModelIdentity(nodeId)) {
  const owner = nodeRegistry[nodeId];
  if (!owner || !config || !modelIdentity) return null;
  const endpoint = canonicalScanEndpoint(definition.endpoint);
  return {
    owner,
    workspaceEpoch: getWorkspaceRuntimeEpoch(),
    inputFingerprint: scanDependencyFingerprint(nodeId, endpoint, config, modelIdentity),
    endpoint,
  };
}

function invalidateOwnedScanTicket(nodeId, ticket, reason) {
  const runtime = inspectScanExecution(nodeId);
  if (nodeRegistry[nodeId] !== ticket?.owner || runtime?.currentTicket !== ticket) return false;
  return invalidateScanExecution(nodeId, reason);
}

function setScanAttemptMessage(nodeId, message) {
  const contentEl = document.getElementById(`${nodeId}-content`);
  if (contentEl) {
    contentEl.innerHTML = `<span class="text-dim scan-result-status">${message}</span>`;
  }
}

function invalidScanAttempt(nodeId, ticket, message) {
  if (message) alert(message);
  const context = scanTicketContext(nodeId, ticket);
  if (context && isCurrentScanExecution(nodeId, ticket, context)) {
    blockScanExecution(nodeId, ticket, context, message || 'invalid-scan-input');
  }
  return null;
}

function readLegacyScan1DConfig(nodeId) {
  return {
    param_symbol: document.getElementById(`${nodeId}-param`)?.value || '',
    param_min: parseFloat(document.getElementById(`${nodeId}-min`)?.value),
    param_max: parseFloat(document.getElementById(`${nodeId}-max`)?.value),
    n_points: parseInt(document.getElementById(`${nodeId}-points`)?.value, 10),
    output_exprs: [document.getElementById(`${nodeId}-expr`)?.value?.trim() || ''],
  };
}

function readLegacyScan2DConfig(nodeId) {
  return {
    param1_symbol: document.getElementById(`${nodeId}-param1`)?.value || '',
    param2_symbol: document.getElementById(`${nodeId}-param2`)?.value || '',
    param1_min: parseFloat(document.getElementById(`${nodeId}-min1`)?.value),
    param1_max: parseFloat(document.getElementById(`${nodeId}-max1`)?.value),
    param2_min: parseFloat(document.getElementById(`${nodeId}-min2`)?.value),
    param2_max: parseFloat(document.getElementById(`${nodeId}-max2`)?.value),
    n_grid: parseInt(document.getElementById(`${nodeId}-grid`)?.value, 10),
    output_expr: document.getElementById(`${nodeId}-expr`)?.value?.trim() || '',
  };
}

function readConnectedScanConfig(nodeId, expectedType) {
  const conn = executionDependencyConnections()
    .find(candidate => candidate.toNode === nodeId && candidate.toPort === 'params');
  if (!conn || nodeRegistry[conn.fromNode]?.type !== expectedType) return null;
  return cloneSerializable(getNodeSerialData(conn.fromNode, expectedType));
}

function prepareConnectedScanConfig(nodeId, expectedType, ticket) {
  const conn = executionDependencyConnections()
    .find(candidate => candidate.toNode === nodeId && candidate.toPort === 'params');
  if (!conn) {
    return invalidScanAttempt(nodeId, ticket,
      `Please connect to a ${expectedType === 'scan-1d-params' ? 'Scan 1D' : 'Scan 2D'} Config node`);
  }
  const paramsNode = nodeRegistry[conn.fromNode];
  if (!paramsNode || paramsNode.type !== expectedType) {
    return invalidScanAttempt(nodeId, ticket, 'Config node has no configuration. Please configure it first.');
  }

  // Synchronize the exact DOM values used by this click. Excluding this result
  // node prevents the synchronization itself from retiring its new ticket;
  // any other result fed by the config is still invalidated.
  triggerConfigUpdate(conn.fromNode, expectedType, { preserveExecutionNodeId: nodeId });
  return cloneSerializable(paramsNode.data?.config || getNodeSerialData(conn.fromNode, expectedType));
}

function validateScanConfig(nodeId, ticket, dimension, config) {
  if (!isCurrentScanExecution(nodeId, ticket)) return null;
  if (!config) {
    return invalidScanAttempt(nodeId, ticket, 'Config node has no configuration. Please configure it first.');
  }
  if (dimension === 1) {
    if (!config.param_symbol) {
      return invalidScanAttempt(nodeId, ticket, 'Please select a scan parameter');
    }
    if (!config.output_exprs?.[0]) {
      return invalidScanAttempt(nodeId, ticket, 'Please enter an output expression');
    }
  } else {
    if (!config.param1_symbol || !config.param2_symbol) {
      return invalidScanAttempt(nodeId, ticket, 'Please select both X and Y axis parameters');
    }
    if (!config.output_expr) {
      return invalidScanAttempt(nodeId, ticket, 'Please enter an output expression');
    }
  }
  return config;
}

async function executeScanRequest(nodeId, definition) {
  let ticket = beginScanExecution(nodeId, definition.endpoint);
  if (!ticket) return false;
  clearStoredScanResult(nodeId, 'Preparing scan...');

  const prepared = validateScanConfig(
    nodeId,
    ticket,
    definition.dimension,
    definition.prepareConfig(ticket),
  );
  if (!prepared || !isCurrentScanExecution(nodeId, ticket)) return false;

  setNodeLoading(nodeId, true);
  setScanAttemptMessage(nodeId, 'Computing scan...');
  let requestIsCurrent = null;
  let executionContext = scanTicketContext(nodeId, ticket);
  try {
    const sessionId = await ensureModelSession(nodeId, {
      connectionResolver: executionDependencyConnections,
    });
    if (!isCurrentScanExecution(nodeId, ticket)) return false;

    const currentConfig = definition.readCurrentConfig();
    const modelIdentity = scanModelIdentity(nodeId);
    if (!currentConfig || !modelIdentity) {
      invalidateScanExecution(nodeId, 'scan-dependency-missing-before-request');
      return false;
    }
    executionContext = scanExecutionContext(nodeId, definition, currentConfig, modelIdentity);
    ticket = beginScanExecution(nodeId, definition.endpoint, executionContext);
    if (!ticket) return false;
    setNodeLoading(nodeId, true);
    const dependencyFingerprint = executionContext.inputFingerprint;
    const request = { session_id: sessionId, ...currentConfig };
    requestIsCurrent = () => {
      const liveConfig = definition.readCurrentConfig();
      const context = scanExecutionContext(nodeId, definition, liveConfig);
      return !!context && isCurrentScanExecution(nodeId, ticket, context);
    };

    const data = await api(definition.endpoint, request, { statusIsCurrent: requestIsCurrent });
    if (!requestIsCurrent()) {
      invalidateOwnedScanTicket(nodeId, ticket, 'scan-dependency-changed-during-request');
      return false;
    }

    const committedContext = scanExecutionContext(nodeId, definition, definition.readCurrentConfig());
    const meta = {
      contract: 'bne-scan-execution/v1',
      endpoint: executionContext.endpoint,
      request: cloneSerializable(request),
      requestFingerprint: dependencyFingerprint,
      model: cloneSerializable(modelIdentity),
      upstreamSignature: scanUpstreamSignature(nodeId),
      executedAt: new Date().toISOString(),
      historical: false,
    };
    if (!committedContext || !commitScanExecution(nodeId, ticket, committedContext, {
      result: { result: data, meta },
      evidence: {
        class: 'current-computation',
        endpoint: executionContext.endpoint,
        partial: data?.partial === true,
      },
    })) return false;

    const info = nodeRegistry[nodeId];
    info.data = info.data || {};
    info.data[definition.resultKey] = data;
    info.data[definition.metaKey] = meta;

    const contentEl = document.getElementById(`${nodeId}-content`);
    if (!contentEl || !requestIsCurrent()) return false;
    contentEl.dataset.resultState = 'current';
    contentEl.innerHTML = `<div class="plot-container" id="${nodeId}-plot"></div>`;
    commitWorkspaceSnapshot(definition.snapshotLabel);
    scheduleCurrentScanPlot(nodeId, ticket, () => {
      if (!requestIsCurrent()) {
        if (isCurrentScanExecution(nodeId, ticket)) {
          invalidateScanExecution(nodeId, 'scan-dependency-changed-before-plot');
        }
        return;
      }
      definition.plot(data, `${nodeId}-plot`);
      setupPlotResize(nodeId, `${nodeId}-plot`);
    });
    return true;
  } catch (error) {
    if (requestIsCurrent && !requestIsCurrent()) {
      invalidateOwnedScanTicket(nodeId, ticket, 'scan-dependency-changed-during-request');
      return false;
    }
    const failureContext = requestIsCurrent
      ? scanExecutionContext(nodeId, definition, definition.readCurrentConfig())
      : scanTicketContext(nodeId, ticket);
    if (!failureContext || !failScanExecution(nodeId, ticket, failureContext, error)) return false;
    handleNodeError(error, nodeId, definition.operationLabel);
    const contentEl = document.getElementById(`${nodeId}-content`);
    renderNodeError(contentEl, error);
    return false;
  } finally {
    releaseScanExecution(nodeId, ticket);
  }
}

export function setupLegacyScanInputInvalidation(nodeId) {
  const node = document.getElementById(nodeId);
  if (!node) return;
  node.querySelectorAll('input:not([data-action]), select:not([data-action])').forEach((input) => {
    const eventType = input.tagName === 'SELECT' ? 'change' : 'input';
    input.addEventListener(eventType, () => {
      invalidateScanExecution(nodeId, 'scan-config-input-changed');
    });
  });
}

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
    } else if (info?.type === 'parameter-scan-1d') {
      invalidateScanExecution(nodeId, 'scan-output-expression-changed');
    }
  }
}

export function updateScan1DConfig(nodeId) {
  const paramSelect = document.getElementById(`${nodeId}-param`);
  const exprInput = document.getElementById(`${nodeId}-expr`);

  if (!paramSelect.value) {
    alert('Please select a scan parameter');
    return;
  }

  if (!exprInput.value.trim()) {
    alert('Please enter an output expression');
    return;
  }

  triggerConfigUpdate(nodeId, 'scan-1d-params');
  showToast('Configuration updated');
}

export async function runParameterScan1D(nodeId) {
  return executeScanRequest(nodeId, {
    endpoint: 'parameter_scan_1d',
    dimension: 1,
    resultKey: 'scan1DResult',
    metaKey: 'scan1DResultMeta',
    snapshotLabel: 'scan-1d',
    operationLabel: 'Parameter scan 1D',
    prepareConfig: () => readLegacyScan1DConfig(nodeId),
    readCurrentConfig: () => readLegacyScan1DConfig(nodeId),
    plot: plotParameterScan1D,
  });
}

export async function executeScan1DResult(nodeId) {
  return executeScanRequest(nodeId, {
    endpoint: 'parameter_scan_1d',
    dimension: 1,
    resultKey: 'scan1DResult',
    metaKey: 'scan1DResultMeta',
    snapshotLabel: 'scan-1d',
    operationLabel: 'Parameter scan 1D',
    prepareConfig: ticket => prepareConnectedScanConfig(nodeId, 'scan-1d-params', ticket),
    readCurrentConfig: () => readConnectedScanConfig(nodeId, 'scan-1d-params'),
    plot: plotParameterScan1D,
  });
}

// ===== Parameter Scan 1D Plotting =====

export function plotParameterScan1D(data, plotId) {
  const { param_symbol, param_values, output_exprs } = data;
  const plotTheme = getPlotTheme();
  const prepared = prepareScan1DPlotData(data);
  syncScanValidityNotice(plotId, prepared);

  const traces = output_exprs.map((expr, i) => ({
    x: param_values,
    y: prepared.outputTraj.map(row => row[i] ?? null),
    mode: 'lines',
    name: expr,
    line: { width: 2 },
    connectgaps: false,
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
    } else if (info?.type === 'parameter-scan-2d') {
      invalidateScanExecution(nodeId, 'scan-output-expression-changed');
    }
  }
}

export function updateScan2DConfig(nodeId) {
  const param1 = document.getElementById(`${nodeId}-param1`).value;
  const param2 = document.getElementById(`${nodeId}-param2`).value;
  const expr = document.getElementById(`${nodeId}-expr`).value.trim();

  if (!param1 || !param2) {
    alert('Please select both parameters');
    return;
  }

  if (!expr) {
    alert('Please enter an output expression');
    return;
  }

  triggerConfigUpdate(nodeId, 'scan-2d-params');
  showToast('Configuration updated');
}

export async function executeScan2DResult(nodeId) {
  return executeScanRequest(nodeId, {
    endpoint: 'parameter_scan_2d',
    dimension: 2,
    resultKey: 'scan2DResult',
    metaKey: 'scan2DResultMeta',
    snapshotLabel: 'scan-2d',
    operationLabel: 'Parameter scan 2D',
    prepareConfig: ticket => prepareConnectedScanConfig(nodeId, 'scan-2d-params', ticket),
    readCurrentConfig: () => readConnectedScanConfig(nodeId, 'scan-2d-params'),
    plot: plotParameterScan2D,
  });
}

export async function runParameterScan2D(nodeId) {
  return executeScanRequest(nodeId, {
    endpoint: 'parameter_scan_2d',
    dimension: 2,
    resultKey: 'scan2DResult',
    metaKey: 'scan2DResultMeta',
    snapshotLabel: 'scan-2d',
    operationLabel: 'Parameter scan 2D',
    prepareConfig: () => readLegacyScan2DConfig(nodeId),
    readCurrentConfig: () => readLegacyScan2DConfig(nodeId),
    plot: plotParameterScan2D,
  });
}

// ===== Parameter Scan 2D Plotting =====

export function plotParameterScan2D(data, plotId) {
  const { param1_symbol, param2_symbol, param1_values, param2_values, output_expr } = data;
  const plotTheme = getPlotTheme();
  const prepared = prepareScan2DPlotData(data);
  syncScanValidityNotice(plotId, prepared);

  // Create 3D surface plot
  const traces = [
    {
      z: prepared.outputGrid,
      x: param1_values,
      y: param2_values,
      type: 'surface',
      connectgaps: false,
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

const ROP_POLY_ENDPOINT = '/api/v1/rop_polyhedron';
const ROP_POLY_LIFECYCLE_KEY = '_ropPolyResultLifecycle';

function canonicalROPPolyValue(value) {
  if (Array.isArray(value)) return value.map(canonicalROPPolyValue);
  if (!value || typeof value !== 'object') return value;
  const normalized = {};
  for (const key of Object.keys(value).sort()) normalized[key] = canonicalROPPolyValue(value[key]);
  return normalized;
}

function currentROPPolyContext(nodeId) {
  const owner = nodeRegistry[nodeId];
  if (!owner) return null;
  const connection = connections.find(candidate =>
    candidate.toNode === nodeId && candidate.toPort === 'params');
  let config = null;
  try {
    config = connection ? getNodeSerialData(connection.fromNode, 'rop-poly-params') : null;
  } catch {
    config = null;
  }
  return {
    owner,
    workspaceEpoch: getWorkspaceRuntimeEpoch(),
    inputFingerprint: JSON.stringify(canonicalROPPolyValue({
      endpoint: ROP_POLY_ENDPOINT,
      config,
      model: scanModelIdentity(nodeId),
      upstream: scanUpstreamSignature(nodeId),
    })),
    endpoint: ROP_POLY_ENDPOINT,
  };
}

function syncROPPolyLifecycle(info, lifecycle) {
  if (!info || !lifecycle) return;
  info.data = info.data || {};
  info.data.lifecycle = serializeExecutionLifecycle(lifecycle);
}

function ropPolyLifecycleFor(info) {
  if (!info[ROP_POLY_LIFECYCLE_KEY]) {
    const lifecycle = createExecutionLifecycle();
    info[ROP_POLY_LIFECYCLE_KEY] = lifecycle;
    if (info.data?.ropPlotData) {
      const nodeId = Object.keys(nodeRegistry).find(id => nodeRegistry[id] === info);
      const context = nodeId ? currentROPPolyContext(nodeId) : null;
      if (context) {
        restoreHistoricalLifecycle(lifecycle, {
          context,
          result: info.data.ropPlotData,
          evidence: info.data?.lifecycle?.evidence || null,
        });
        syncROPPolyLifecycle(info, lifecycle);
      }
    }
  }
  return info[ROP_POLY_LIFECYCLE_KEY];
}

function clearROPPolyPlotTimer(info) {
  if (info?._ropPolyPlotTimer == null) return;
  clearTimeout(info._ropPolyPlotTimer);
  delete info._ropPolyPlotTimer;
}

function settleROPPolyLoading(nodeId, lifecycle, ticket) {
  if (!ticket || nodeRegistry[nodeId] !== ticket.owner) return;
  const runtime = inspectExecutionLifecycle(lifecycle);
  if (runtime.currentTicket === ticket ||
      (runtime.currentTicket == null && runtime.loading === false)) {
    setNodeLoading(nodeId, false);
  }
}

export function invalidateROPPolyResult(nodeId, reason = 'rop-poly-input-changed') {
  const info = nodeRegistry[nodeId];
  if (!info || info.type !== 'rop-poly-result') return false;
  const lifecycle = ropPolyLifecycleFor(info);
  const runtime = inspectExecutionLifecycle(lifecycle);
  if (runtime.owner === info && runtime.workspaceEpoch != null) {
    invalidateLifecycle(lifecycle, {
      owner: info,
      workspaceEpoch: runtime.workspaceEpoch,
      reason,
    });
  }
  clearROPPolyPlotTimer(info);
  info.data = info.data || {};
  delete info.data.ropPlotData;
  delete info.data.fitInnerPoints;
  syncROPPolyLifecycle(info, lifecycle);
  setNodeLoading(nodeId, false);
  const contentEl = document.getElementById(`${nodeId}-content`);
  if (contentEl) {
    contentEl.dataset.resultState = 'invalidated';
    contentEl.innerHTML = '<span class="text-dim">Inputs changed — run ROP Polyhedron again.</span>';
  }
  return true;
}

export function invalidateROPPolyResultsDownstreamOf(sourceNodeId, reason = 'rop-poly-config-changed') {
  const invalidated = [];
  for (const connection of connections) {
    if (connection?.fromNode !== sourceNodeId) continue;
    if (nodeRegistry[connection.toNode]?.type !== 'rop-poly-result') continue;
    if (invalidateROPPolyResult(connection.toNode, reason)) invalidated.push(connection.toNode);
  }
  return invalidated;
}

export function installROPPolyConfigInvalidation(nodeId) {
  const node = document.getElementById(nodeId);
  if (!node) return;
  node.querySelectorAll('.auto-update').forEach(control => {
    const eventType = control.tagName === 'SELECT' || control.type === 'checkbox' ? 'change' : 'input';
    control.addEventListener(eventType, () => {
      invalidateROPPolyResultsDownstreamOf(nodeId, 'rop-poly-config-input-changed');
    });
  });
}

export function inspectROPPolyResultLifecycle(nodeId) {
  const info = nodeRegistry[nodeId];
  return info?.type === 'rop-poly-result'
    ? inspectExecutionLifecycle(ropPolyLifecycleFor(info))
    : null;
}

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
  const owner = nodeRegistry[nodeId];
  if (!owner || owner.type !== 'rop-poly-result') return false;
  const lifecycle = ropPolyLifecycleFor(owner);
  invalidateROPPolyResult(nodeId, 'rop-poly-execution-restarted');
  const conn = connections.find(c => c.toNode === nodeId && c.toPort === 'params');
  if (!conn) {
    const context = currentROPPolyContext(nodeId);
    if (context) {
      const ticket = beginLifecycle(lifecycle, context);
      blockLifecycle(lifecycle, ticket, {
        context,
        reason: 'Connect to a ROP Polyhedron Config node',
      });
      syncROPPolyLifecycle(owner, lifecycle);
    }
    alert('Please connect to a ROP Polyhedron Config node');
    return false;
  }

  const paramsNode = nodeRegistry[conn.fromNode];
  const config = getNodeSerialData(conn.fromNode, 'rop-poly-params');
  if (!paramsNode || !config || !(config.pairs || []).length) {
    const context = currentROPPolyContext(nodeId);
    if (context) {
      const ticket = beginLifecycle(lifecycle, context);
      blockLifecycle(lifecycle, ticket, {
        context,
        reason: 'ROP Polyhedron Config node has no complete axis configuration',
      });
      syncROPPolyLifecycle(owner, lifecycle);
    }
    alert('Config node has no configuration. Please configure it first.');
    return false;
  }
  paramsNode.data = paramsNode.data || {};
  paramsNode.data.config = config;
  setNodeLoading(nodeId, true);
  const contentEl = document.getElementById(`${nodeId}-content`);
  const attemptRevision = (owner._ropPolyAttemptRevision || 0) + 1;
  owner._ropPolyAttemptRevision = attemptRevision;
  let ticket = null;

  try {
    const sessionId = await ensureModelSession(nodeId);
    if (nodeRegistry[nodeId] !== owner || owner._ropPolyAttemptRevision !== attemptRevision) return false;
    const beginContext = currentROPPolyContext(nodeId);
    if (!beginContext) return false;
    ticket = beginLifecycle(lifecycle, beginContext);
    syncROPPolyLifecycle(owner, lifecycle);
    const data = await api('rop_polyhedron', {
      session_id: sessionId,
      ...config
    }, {
      statusIsCurrent: () => {
        const context = currentROPPolyContext(nodeId);
        return !!context && isCurrentLifecycle(lifecycle, ticket, context);
      },
    });
    const currentContext = currentROPPolyContext(nodeId);
    if (!currentContext || !commitLifecycle(lifecycle, ticket, {
      context: currentContext,
      result: data,
      evidence: { source_endpoint: ROP_POLY_ENDPOINT },
      sessionId,
    })) {
      syncROPPolyLifecycle(owner, lifecycle);
      return false;
    }
    syncROPPolyLifecycle(owner, lifecycle);
    if (nodeRegistry[nodeId] !== owner) return false;
    renderROPPolyhedronOutput(nodeId, contentEl, data, config, {
      timerOwner: owner,
      plotGuard: () => {
        const context = currentROPPolyContext(nodeId);
        return nodeRegistry[nodeId] === owner && !!context &&
          isCurrentLifecycle(lifecycle, ticket, context);
      },
    });
    return true;
  } catch (e) {
    let current = false;
    const context = currentROPPolyContext(nodeId);
    if (ticket && context) {
      current = failLifecycle(lifecycle, ticket, { context, error: e });
    } else if (context && nodeRegistry[nodeId] === owner &&
               owner._ropPolyAttemptRevision === attemptRevision) {
      ticket = beginLifecycle(lifecycle, context);
      current = failLifecycle(lifecycle, ticket, { context, error: e });
    }
    syncROPPolyLifecycle(owner, lifecycle);
    if (!current) return false;
    handleNodeError(e, nodeId, 'ROP polyhedron');
    if (nodeRegistry[nodeId] === owner) renderNodeError(contentEl, e);
    return false;
  } finally {
    if (ticket) {
      releaseLifecycle(lifecycle, ticket);
      if (owner._ropPolyAttemptRevision === attemptRevision) {
        settleROPPolyLoading(nodeId, lifecycle, ticket);
      }
    } else if (nodeRegistry[nodeId] === owner &&
               owner._ropPolyAttemptRevision === attemptRevision) {
      setNodeLoading(nodeId, false);
    }
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
    const sessionId = await ensureModelSession(nodeId);
    const data = await api('rop_polyhedron', {
      session_id: sessionId,
      ...config,
    });
    renderROPPolyhedronOutput(nodeId, contentEl, data, config);
  } catch (e) {
    handleNodeError(e, nodeId, 'ROP polyhedron');
    renderNodeError(contentEl, e);
  } finally {
    setNodeLoading(nodeId, false);
  }
}

export function renderROPPolyhedronOutput(nodeId, contentEl, data, config = {}, {
  plotGuard = null,
  timerOwner = null,
} = {}) {
  const axisSummary = (data.pairs || config.pairs || []).map((pair, idx) => {
    const xSymbol = pair.x_symbol || pair.xSymbol || '?';
    const qkSymbol = pair.qk_symbol || pair.qkSymbol || '?';
    return `<span class="summary-chip">A${idx + 1}: ${escapeHtml(xSymbol)} / ${escapeHtml(qkSymbol)}</span>`;
  }).join('');
  const hasInnerPoints = (data.inner_points || []).length > 0;

  const nd = ensureNodeData(nodeId);
  nd.ropPlotData = data;
  nd.fitInnerPoints = false;

  contentEl.innerHTML = `
    <div class="siso-summary-line">
      <span class="summary-chip"><strong>${escapeHtml(data.dimension || config.dimension)}D</strong></span>
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
  if (timerOwner) clearROPPolyPlotTimer(timerOwner);
  const timer = setTimeout(() => {
    if (timerOwner?._ropPolyPlotTimer === timer) delete timerOwner._ropPolyPlotTimer;
    if (plotGuard && !plotGuard()) return;
    refreshROPPolyhedronPlot(nodeId);
    setupPlotResize(nodeId, `${nodeId}-plot`);
  }, 50);
  if (timerOwner) timerOwner._ropPolyPlotTimer = timer;
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
