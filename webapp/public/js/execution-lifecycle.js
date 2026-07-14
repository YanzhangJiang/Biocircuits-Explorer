// Latest-wins lifecycle for scan result nodes. This module intentionally
// depends only on shared graph state and the small loading helper so model,
// connection, and config mutators can retire obsolete executions without
// importing the plotting/request implementation.
import { nodeRegistry, connections, plotResizeObservers, wiringState } from './state.js';
import { setNodeLoading } from './node-loading.js';

const SCAN_RESULT_CONTRACTS = Object.freeze({
  'parameter-scan-1d': { resultKey: 'scan1DResult', metaKey: 'scan1DResultMeta' },
  'scan-1d-result': { resultKey: 'scan1DResult', metaKey: 'scan1DResultMeta' },
  'parameter-scan-2d': { resultKey: 'scan2DResult', metaKey: 'scan2DResultMeta' },
  'scan-2d-result': { resultKey: 'scan2DResult', metaKey: 'scan2DResultMeta' },
});

const ATLAS_RESULT_CONTRACTS = Object.freeze({
  'atlas-builder': {
    dataKeys: ['atlasData', 'lastSpec', 'sqlitePath'],
    runningMessage: 'Building a current atlas...',
    invalidatedMessage: 'Inputs changed — build the atlas again.',
  },
  'atlas-query-result': {
    dataKeys: ['queryData', 'lastQuery'],
    runningMessage: 'Computing a current atlas query...',
    invalidatedMessage: 'Inputs changed — run the atlas query again.',
  },
  'atlas-inverse-result': {
    dataKeys: ['inverseDesignData', 'lastInverseRequest'],
    runningMessage: 'Computing a current inverse-design result...',
    invalidatedMessage: 'Inputs changed — run inverse design again.',
  },
});

function resultContract(nodeId) {
  return SCAN_RESULT_CONTRACTS[nodeRegistry[nodeId]?.type] || null;
}

function atlasResultContract(nodeId) {
  return ATLAS_RESULT_CONTRACTS[nodeRegistry[nodeId]?.type] || null;
}

function clearPlotTimer(info) {
  if (info?._scanPlotTimer == null) return;
  clearTimeout(info._scanPlotTimer);
  delete info._scanPlotTimer;
}

function setResultMessage(nodeId, message) {
  if (typeof document === 'undefined') return;
  const contentEl = document.getElementById(`${nodeId}-content`);
  if (!contentEl) return;
  delete contentEl.dataset.resultState;
  contentEl.innerHTML = `<span class="text-dim scan-result-status">${message}</span>`;
}

function setAtlasResultMessage(nodeId, message) {
  if (typeof document === 'undefined') return;
  const contentEl = document.getElementById(`${nodeId}-content`);
  if (!contentEl) return;
  delete contentEl.dataset?.resultState;
  contentEl.innerHTML = `<span class="text-dim atlas-result-status">${message}</span>`;
}

function cleanupScanPlotObserver(nodeId) {
  const observer = plotResizeObservers.get(nodeId);
  if (!observer) return;
  observer.disconnect();
  plotResizeObservers.delete(nodeId);
}

export function clearStoredScanResult(nodeId, message = 'Run the scan to compute a current result.') {
  const info = nodeRegistry[nodeId];
  const contract = resultContract(nodeId);
  if (!info || !contract) return false;
  info.data = info.data || {};
  delete info.data[contract.resultKey];
  delete info.data[contract.metaKey];
  cleanupScanPlotObserver(nodeId);
  setResultMessage(nodeId, message);
  return true;
}

export function clearStoredAtlasResult(nodeId, message = 'Run this Atlas step to compute a current result.') {
  const info = nodeRegistry[nodeId];
  const contract = atlasResultContract(nodeId);
  if (!info || !contract) return false;
  info.data = info.data || {};
  contract.dataKeys.forEach(key => delete info.data[key]);
  setAtlasResultMessage(nodeId, message);
  return true;
}

export function beginAtlasNodeExecution(nodeId, kind = 'atlas') {
  const owner = nodeRegistry[nodeId];
  const contract = atlasResultContract(nodeId);
  if (!owner || !contract) return null;

  // Starting an upstream Atlas step immediately makes every dependent Atlas
  // result historical. Retire those consumers before publishing this ticket.
  for (const downstreamId of downstreamNodeIds(nodeId, executionDependencyConnections())) {
    if (downstreamId === nodeId || !atlasResultContract(downstreamId)) continue;
    invalidateAtlasExecution(downstreamId, `${kind}-started`);
  }

  setNodeLoading(nodeId, false);
  const priorRevision = Number.isSafeInteger(owner._atlasExecutionRevision)
    ? owner._atlasExecutionRevision
    : 0;
  const ticket = {
    owner,
    token: Symbol(`${kind}-${nodeId}`),
    kind,
    revision: priorRevision + 1,
  };
  owner._atlasExecutionRevision = ticket.revision;
  owner._atlasExecution = ticket;
  clearStoredAtlasResult(nodeId, contract.runningMessage);
  return ticket;
}

export function isCurrentAtlasNodeExecution(nodeId, ticket) {
  const owner = nodeRegistry[nodeId];
  return !!ticket && owner === ticket.owner &&
    owner._atlasExecution === ticket &&
    owner._atlasExecutionRevision === ticket.revision;
}

export function invalidateAtlasExecution(nodeId, reason = 'atlas-input-changed') {
  const info = nodeRegistry[nodeId];
  const contract = atlasResultContract(nodeId);
  if (!info || !contract) return false;

  const priorRevision = Number.isSafeInteger(info._atlasExecutionRevision)
    ? info._atlasExecutionRevision
    : 0;
  info._atlasExecutionRevision = priorRevision + 1;
  info._atlasInvalidationReason = reason;
  delete info._atlasExecution;
  setNodeLoading(nodeId, false);
  clearStoredAtlasResult(nodeId, contract.invalidatedMessage);
  return true;
}

export function beginScanExecution(nodeId, kind) {
  const info = nodeRegistry[nodeId];
  if (!info || !resultContract(nodeId)) return null;

  clearPlotTimer(info);
  // A new attempt supersedes an older request even when this attempt later
  // fails local validation and never reaches the backend.
  setNodeLoading(nodeId, false);
  const priorRevision = Number.isSafeInteger(info._scanExecutionRevision)
    ? info._scanExecutionRevision
    : 0;
  const ticket = {
    token: Symbol(`${kind || 'scan'}-${nodeId}`),
    revision: priorRevision + 1,
    nodeInfo: info,
    kind: kind || 'scan',
  };
  info._scanExecutionRevision = ticket.revision;
  info._scanExecutionToken = ticket.token;
  // Kept after the HTTP request settles so a delayed plot callback can still
  // prove that it belongs to the latest successful run.
  info._latestScanExecutionToken = ticket.token;
  return ticket;
}

export function isCurrentScanExecution(nodeId, ticket) {
  const info = nodeRegistry[nodeId];
  return !!info && !!ticket &&
    info === ticket.nodeInfo &&
    info._scanExecutionRevision === ticket.revision &&
    info._latestScanExecutionToken === ticket.token;
}

export function releaseScanExecution(nodeId, ticket) {
  if (!isCurrentScanExecution(nodeId, ticket)) return false;
  const info = nodeRegistry[nodeId];
  if (info._scanExecutionToken !== ticket.token) return false;
  delete info._scanExecutionToken;
  return true;
}

export function scheduleCurrentScanPlot(nodeId, ticket, callback, delay = 50) {
  if (!isCurrentScanExecution(nodeId, ticket)) return null;
  const info = nodeRegistry[nodeId];
  clearPlotTimer(info);
  const timer = setTimeout(() => {
    if (nodeRegistry[nodeId] !== ticket.nodeInfo || !isCurrentScanExecution(nodeId, ticket)) return;
    if (ticket.nodeInfo._scanPlotTimer === timer) delete ticket.nodeInfo._scanPlotTimer;
    callback();
  }, delay);
  info._scanPlotTimer = timer;
  return timer;
}

export function scheduleHistoricalScanPlot(nodeId, callback, delay = 50) {
  const info = nodeRegistry[nodeId];
  if (!info || !resultContract(nodeId)) return null;
  clearPlotTimer(info);
  const owner = info;
  const revision = Number.isSafeInteger(info._scanExecutionRevision)
    ? info._scanExecutionRevision
    : 0;
  const timer = setTimeout(() => {
    if (nodeRegistry[nodeId] !== owner || owner._scanExecutionRevision !== revision) return;
    if (owner._scanPlotTimer === timer) delete owner._scanPlotTimer;
    callback();
  }, delay);
  info._scanPlotTimer = timer;
  return timer;
}

export function invalidateScanExecution(nodeId, reason = 'scan-input-changed') {
  const info = nodeRegistry[nodeId];
  if (!info || !resultContract(nodeId)) return false;

  clearPlotTimer(info);
  const priorRevision = Number.isSafeInteger(info._scanExecutionRevision)
    ? info._scanExecutionRevision
    : 0;
  info._scanExecutionRevision = priorRevision + 1;
  info._scanInvalidationReason = reason;
  delete info._scanExecutionToken;
  delete info._latestScanExecutionToken;
  setNodeLoading(nodeId, false);
  clearStoredScanResult(nodeId, 'Inputs changed — run the scan again.');
  return true;
}

function downstreamNodeIds(sourceNodeId, connectionList = connections) {
  const visited = new Set([sourceNodeId]);
  const queue = [sourceNodeId];
  while (queue.length) {
    const current = queue.shift();
    for (const conn of connectionList) {
      if (conn?.fromNode !== current || visited.has(conn.toNode)) continue;
      visited.add(conn.toNode);
      queue.push(conn.toNode);
    }
  }
  return visited;
}

export function invalidateScanExecutionsDownstreamOf(
  sourceNodeId,
  reason = 'upstream-input-changed',
  { exceptNodeId = null } = {},
) {
  const invalidated = [];
  for (const nodeId of downstreamNodeIds(sourceNodeId)) {
    if (nodeId === exceptNodeId || !resultContract(nodeId)) continue;
    if (invalidateScanExecution(nodeId, reason)) invalidated.push(nodeId);
  }
  return invalidated;
}

export function invalidateAtlasExecutionsDownstreamOf(
  sourceNodeId,
  reason = 'atlas-upstream-input-changed',
  { exceptNodeId = null } = {},
) {
  const invalidated = [];
  for (const nodeId of downstreamNodeIds(sourceNodeId, executionDependencyConnections())) {
    if (nodeId === exceptNodeId || !atlasResultContract(nodeId)) continue;
    if (invalidateAtlasExecution(nodeId, reason)) invalidated.push(nodeId);
  }
  return invalidated;
}

function upstreamSignature(connectionList, nodeId) {
  const visitedNodes = new Set();
  const edgeKeys = new Set();
  const visit = (current) => {
    if (visitedNodes.has(current)) return;
    visitedNodes.add(current);
    for (const conn of connectionList) {
      if (conn?.toNode !== current) continue;
      edgeKeys.add([conn.fromNode, conn.fromPort, conn.toNode, conn.toPort]
        .map(value => String(value ?? '')).join('\u0000'));
      visit(conn.fromNode);
    }
  };
  visit(nodeId);
  return JSON.stringify([...edgeKeys].sort());
}

// Interactive rewiring temporarily removes the dragged edge from `connections`.
// Until mouseup commits the gesture, consumers must compare against the graph
// that existed at gesture start; a wire dragged back to its own socket is a
// semantic no-op and must not invalidate an in-flight result.
export function executionDependencyConnections() {
  if (wiringState.isWiring && Array.isArray(wiringState.connSnapshotBefore)) {
    return wiringState.connSnapshotBefore;
  }
  return connections;
}

export function scanUpstreamSignature(nodeId, connectionList = executionDependencyConnections()) {
  return upstreamSignature(connectionList, nodeId);
}

export function invalidateScanExecutionsForConnectionChange(
  before,
  after,
  reason = 'scan-upstream-connection-changed',
) {
  const invalidated = [];
  for (const [nodeId, info] of Object.entries(nodeRegistry)) {
    if (!SCAN_RESULT_CONTRACTS[info?.type]) continue;
    if (upstreamSignature(before, nodeId) === upstreamSignature(after, nodeId)) continue;
    if (invalidateScanExecution(nodeId, reason)) invalidated.push(nodeId);
  }
  return invalidated;
}

export function atlasUpstreamSignature(nodeId, connectionList = executionDependencyConnections()) {
  return upstreamSignature(connectionList, nodeId);
}

export function invalidateAtlasExecutionsForConnectionChange(
  before,
  after,
  reason = 'atlas-upstream-connection-changed',
) {
  const invalidated = [];
  for (const [nodeId, info] of Object.entries(nodeRegistry)) {
    if (!ATLAS_RESULT_CONTRACTS[info?.type]) continue;
    if (upstreamSignature(before, nodeId) === upstreamSignature(after, nodeId)) continue;
    if (invalidateAtlasExecution(nodeId, reason)) invalidated.push(nodeId);
  }
  return invalidated;
}

export function isScanConfigNodeType(type) {
  return type === 'scan-1d-params' || type === 'scan-2d-params';
}

export function isAtlasConfigNodeType(type) {
  return type === 'atlas-spec' || type === 'atlas-query-config';
}
