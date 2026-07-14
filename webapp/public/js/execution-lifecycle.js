// Latest-wins lifecycle for scan result nodes. This module intentionally
// depends only on shared graph state and the small loading helper so model,
// connection, and config mutators can retire obsolete executions without
// importing the plotting/request implementation.
import {
  connections,
  getWorkspaceRuntimeEpoch,
  nodeRegistry,
  plotResizeObservers,
  wiringState,
} from './state.js';
import { setNodeLoading } from './node-loading.js';
import {
  begin as beginLifecycle,
  block as blockLifecycle,
  commit as commitLifecycle,
  createExecutionLifecycle,
  fail as failLifecycle,
  inspectExecutionLifecycle,
  invalidate as invalidateLifecycle,
  isCurrent as isCurrentLifecycle,
  readCurrentResult,
  release as releaseLifecycle,
  restoreHistorical as restoreHistoricalLifecycle,
  serializeExecutionLifecycle,
} from './execution-lifecycle-core.js';

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

const PLACER_RESULT_CONTRACTS = Object.freeze({
  'placer-result': {
    dataKeys: ['placerResult', 'placerMenu', 'evidence'],
    invalidatedMessage: 'Inputs changed — run Parameter Placer again.',
  },
});

const ATLAS_LIFECYCLE_KEY = '_atlasExecutionLifecycle';
export const PLACER_RESULT_LIFECYCLE_KEY = '_placerResultLifecycle';
const SCAN_LIFECYCLE_KEY = '_scanExecutionLifecycle';

function canonicalFingerprintValue(value) {
  if (Array.isArray(value)) return value.map(canonicalFingerprintValue);
  if (!value || typeof value !== 'object') return value;
  const normalized = {};
  for (const key of Object.keys(value).sort()) {
    normalized[key] = canonicalFingerprintValue(value[key]);
  }
  return normalized;
}

function stableFingerprint(value) {
  return JSON.stringify(canonicalFingerprintValue(value));
}

function scanEndpoint(kind = '', nodeType = '') {
  const hint = `${kind} ${nodeType}`.toLowerCase();
  return hint.includes('2d')
    ? '/api/v1/parameter_scan_2d'
    : '/api/v1/parameter_scan_1d';
}

function scanLifecycleFor(owner) {
  if (!owner[SCAN_LIFECYCLE_KEY]) {
    owner[SCAN_LIFECYCLE_KEY] = createExecutionLifecycle();
  }
  return owner[SCAN_LIFECYCLE_KEY];
}

function syncScanLifecycle(owner) {
  const lifecycle = owner?.[SCAN_LIFECYCLE_KEY];
  if (!owner || !lifecycle) return;
  owner.data = owner.data || {};
  owner.data.lifecycle = serializeExecutionLifecycle(lifecycle);
}

function defaultScanContext(nodeId, kind) {
  const owner = nodeRegistry[nodeId];
  if (!owner) return null;
  return {
    owner,
    workspaceEpoch: getWorkspaceRuntimeEpoch(),
    inputFingerprint: stableFingerprint({
      kind,
      nodeType: owner.type,
      upstream: scanUpstreamSignature(nodeId),
    }),
    endpoint: scanEndpoint(kind, owner.type),
  };
}

function atlasEndpoint(kind, nodeType = '') {
  if (nodeType === 'atlas-builder' || kind === 'atlas-builder' || kind === 'build') {
    return '/api/v1/build_atlas';
  }
  if (nodeType === 'atlas-query-result' || kind === 'atlas-query' || kind === 'query') {
    return '/api/v1/query_atlas';
  }
  if (nodeType === 'atlas-inverse-result' || kind === 'atlas-inverse' || kind === 'inverse') {
    return '/api/v1/run_inverse_design';
  }
  return '/api/v1/atlas';
}

function defaultAtlasContext(nodeId, kind) {
  const owner = nodeRegistry[nodeId];
  if (!owner) return null;
  return {
    owner,
    workspaceEpoch: getWorkspaceRuntimeEpoch(),
    inputFingerprint: stableFingerprint({
      kind,
      nodeType: owner.type,
      upstream: atlasUpstreamSignature(nodeId),
    }),
    endpoint: atlasEndpoint(kind, owner.type),
  };
}

function atlasLifecycleFor(owner) {
  if (!owner[ATLAS_LIFECYCLE_KEY]) {
    owner[ATLAS_LIFECYCLE_KEY] = createExecutionLifecycle();
  }
  return owner[ATLAS_LIFECYCLE_KEY];
}

function syncAtlasLifecycle(owner) {
  const lifecycle = owner?.[ATLAS_LIFECYCLE_KEY];
  if (!owner || !lifecycle) return;
  owner.data = owner.data || {};
  owner.data.lifecycle = serializeExecutionLifecycle(lifecycle);
}

function placerResultContract(nodeId) {
  return PLACER_RESULT_CONTRACTS[nodeRegistry[nodeId]?.type] || null;
}

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

export function clearStoredPlacerResult(
  nodeId,
  message = 'Run Parameter Placer to compute a current result.',
) {
  const info = nodeRegistry[nodeId];
  const contract = placerResultContract(nodeId);
  if (!info || !contract) return false;
  if (info._placerPlotTimer != null) clearTimeout(info._placerPlotTimer);
  if (info._placerTuneTimer != null) clearTimeout(info._placerTuneTimer);
  delete info._placerPlotTimer;
  delete info._placerTuneTimer;
  delete info._placerPendingTune;
  info.data = info.data || {};
  contract.dataKeys.forEach(key => delete info.data[key]);
  setAtlasResultMessage(nodeId, message);
  return true;
}

export function beginAtlasNodeExecution(nodeId, kind = 'atlas', executionContext = null) {
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
  const context = executionContext || defaultAtlasContext(nodeId, kind);
  if (!context || context.owner !== owner) return null;
  const lifecycle = atlasLifecycleFor(owner);
  const ticket = beginLifecycle(lifecycle, context);
  clearStoredAtlasResult(nodeId, contract.runningMessage);
  syncAtlasLifecycle(owner);
  return ticket;
}

export function isCurrentAtlasNodeExecution(nodeId, ticket, executionContext = null) {
  const owner = nodeRegistry[nodeId];
  const lifecycle = owner?.[ATLAS_LIFECYCLE_KEY];
  if (!ticket || !lifecycle || owner !== ticket.owner) return false;
  const context = executionContext || {
    owner,
    workspaceEpoch: getWorkspaceRuntimeEpoch(),
    inputFingerprint: ticket.inputFingerprint,
    endpoint: ticket.endpoint,
  };
  return isCurrentLifecycle(lifecycle, ticket, context);
}

export function commitAtlasNodeExecution(nodeId, ticket, context, { result, evidence = null }) {
  const owner = nodeRegistry[nodeId];
  const lifecycle = owner?.[ATLAS_LIFECYCLE_KEY];
  if (!lifecycle || owner !== ticket?.owner) return false;
  const committed = commitLifecycle(lifecycle, ticket, { context, result, evidence });
  syncAtlasLifecycle(owner);
  return committed;
}

export function failAtlasNodeExecution(nodeId, ticket, context, error, evidence = null) {
  const owner = nodeRegistry[nodeId];
  const lifecycle = owner?.[ATLAS_LIFECYCLE_KEY];
  if (!lifecycle || owner !== ticket?.owner) return false;
  const failed = failLifecycle(lifecycle, ticket, { context, error, evidence });
  if (failed) clearStoredAtlasResult(nodeId, 'Atlas execution failed — fix the error and rerun.');
  syncAtlasLifecycle(owner);
  return failed;
}

export function blockAtlasNodeExecution(nodeId, ticket, context, reason, evidence = null) {
  const owner = nodeRegistry[nodeId];
  const lifecycle = owner?.[ATLAS_LIFECYCLE_KEY];
  if (!lifecycle || owner !== ticket?.owner) return false;
  const blocked = blockLifecycle(lifecycle, ticket, { context, reason, evidence });
  if (blocked) clearStoredAtlasResult(nodeId, 'Atlas execution is blocked — connect valid inputs and rerun.');
  syncAtlasLifecycle(owner);
  return blocked;
}

export function releaseAtlasNodeExecution(nodeId, ticket) {
  const owner = nodeRegistry[nodeId];
  const lifecycle = owner?.[ATLAS_LIFECYCLE_KEY];
  if (!lifecycle || owner !== ticket?.owner) return false;
  const released = releaseLifecycle(lifecycle, ticket);
  syncAtlasLifecycle(owner);
  setNodeLoading(nodeId, inspectExecutionLifecycle(lifecycle).loading);
  return released;
}

export function inspectAtlasNodeExecution(nodeId) {
  const lifecycle = nodeRegistry[nodeId]?.[ATLAS_LIFECYCLE_KEY];
  return lifecycle ? inspectExecutionLifecycle(lifecycle) : null;
}

export function readCurrentAtlasNodeResult(nodeId) {
  const lifecycle = nodeRegistry[nodeId]?.[ATLAS_LIFECYCLE_KEY];
  return lifecycle ? readCurrentResult(lifecycle) : null;
}

function restoredAtlasInputIdentity(nodeId, owner, data) {
  let persistedInput = null;
  if (owner.type === 'atlas-builder') {
    persistedInput = { lastSpec: data.lastSpec || null, sqlitePath: data.sqlitePath || '' };
  } else if (owner.type === 'atlas-query-result') {
    persistedInput = { lastQuery: data.lastQuery || null };
  } else if (owner.type === 'atlas-inverse-result') {
    persistedInput = { lastInverseRequest: data.lastInverseRequest || null };
  }
  return {
    nodeType: owner.type,
    persistedInput,
    upstream: atlasUpstreamSignature(nodeId),
  };
}

export function restoreAtlasNodeExecution(nodeId, data = {}) {
  const owner = nodeRegistry[nodeId];
  const contract = atlasResultContract(nodeId);
  if (!owner || !contract) return false;
  const resultKey = contract.dataKeys[0];
  if (data[resultKey] == null) return false;
  const lifecycle = atlasLifecycleFor(owner);
  restoreHistoricalLifecycle(lifecycle, {
    context: {
      owner,
      workspaceEpoch: getWorkspaceRuntimeEpoch(),
      inputFingerprint: stableFingerprint(restoredAtlasInputIdentity(nodeId, owner, data)),
      endpoint: atlasEndpoint('', owner.type),
    },
    result: data[resultKey],
    evidence: data.lifecycle?.evidence || data.evidence || null,
  });
  owner.data = owner.data || {};
  owner.data[resultKey] = inspectExecutionLifecycle(lifecycle).result;
  syncAtlasLifecycle(owner);
  return true;
}

export function invalidateAtlasExecution(nodeId, reason = 'atlas-input-changed') {
  const info = nodeRegistry[nodeId];
  const contract = atlasResultContract(nodeId);
  if (!info || !contract) return false;

  const lifecycle = info[ATLAS_LIFECYCLE_KEY];
  if (lifecycle) {
    const runtime = inspectExecutionLifecycle(lifecycle);
    if (runtime.owner === info && runtime.workspaceEpoch != null) {
      invalidateLifecycle(lifecycle, {
        owner: info,
        workspaceEpoch: runtime.workspaceEpoch,
        reason,
      });
      syncAtlasLifecycle(info);
    }
  } else if (info.data?.lifecycle) {
    info.data.lifecycle = { state: 'invalidated', freshness: 'invalidated', evidence: null };
  }
  setNodeLoading(nodeId, false);
  clearStoredAtlasResult(nodeId, contract.invalidatedMessage);
  return true;
}

export function invalidatePlacerExecution(nodeId, reason = 'placer-input-changed') {
  const info = nodeRegistry[nodeId];
  const contract = placerResultContract(nodeId);
  if (!info || !contract) return false;
  const lifecycle = info[PLACER_RESULT_LIFECYCLE_KEY];
  if (lifecycle) {
    const runtime = inspectExecutionLifecycle(lifecycle);
    if (runtime.owner === info && runtime.workspaceEpoch != null) {
      invalidateLifecycle(lifecycle, {
        owner: info,
        workspaceEpoch: runtime.workspaceEpoch,
        reason,
      });
      info.data = info.data || {};
      info.data.lifecycle = serializeExecutionLifecycle(lifecycle);
    }
  } else if (info.data?.lifecycle) {
    info.data.lifecycle = { state: 'invalidated', freshness: 'invalidated', evidence: null };
  }
  setNodeLoading(nodeId, false);
  clearStoredPlacerResult(nodeId, contract.invalidatedMessage);
  return true;
}

export function beginScanExecution(nodeId, kind = 'scan', executionContext = null) {
  const owner = nodeRegistry[nodeId];
  if (!owner || !resultContract(nodeId)) return null;

  clearPlotTimer(owner);
  // A new attempt supersedes an older request even when this attempt later
  // fails local validation and never reaches the backend.
  setNodeLoading(nodeId, false);
  const context = executionContext || defaultScanContext(nodeId, kind);
  if (!context || context.owner !== owner) return null;
  const lifecycle = scanLifecycleFor(owner);
  const ticket = beginLifecycle(lifecycle, context);
  syncScanLifecycle(owner);
  return ticket;
}

export function isCurrentScanExecution(nodeId, ticket, executionContext = null) {
  const owner = nodeRegistry[nodeId];
  const lifecycle = owner?.[SCAN_LIFECYCLE_KEY];
  if (!ticket || !lifecycle || owner !== ticket.owner) return false;
  const context = executionContext || {
    owner,
    workspaceEpoch: getWorkspaceRuntimeEpoch(),
    inputFingerprint: ticket.inputFingerprint,
    endpoint: ticket.endpoint,
  };
  return isCurrentLifecycle(lifecycle, ticket, context);
}

export function commitScanExecution(nodeId, ticket, context, { result, evidence = null }) {
  const owner = nodeRegistry[nodeId];
  const lifecycle = owner?.[SCAN_LIFECYCLE_KEY];
  if (!lifecycle || owner !== ticket?.owner) return false;
  const committed = commitLifecycle(lifecycle, ticket, { context, result, evidence });
  syncScanLifecycle(owner);
  return committed;
}

export function failScanExecution(nodeId, ticket, context, error, evidence = null) {
  const owner = nodeRegistry[nodeId];
  const lifecycle = owner?.[SCAN_LIFECYCLE_KEY];
  if (!lifecycle || owner !== ticket?.owner) return false;
  const failed = failLifecycle(lifecycle, ticket, { context, error, evidence });
  if (failed) clearStoredScanResult(nodeId, 'Scan execution failed — fix the error and rerun.');
  syncScanLifecycle(owner);
  return failed;
}

export function blockScanExecution(nodeId, ticket, context, reason, evidence = null) {
  const owner = nodeRegistry[nodeId];
  const lifecycle = owner?.[SCAN_LIFECYCLE_KEY];
  if (!lifecycle || owner !== ticket?.owner) return false;
  const blocked = blockLifecycle(lifecycle, ticket, { context, reason, evidence });
  if (blocked) clearStoredScanResult(nodeId, 'Scan did not run — fix the inputs and try again.');
  syncScanLifecycle(owner);
  return blocked;
}

export function releaseScanExecution(nodeId, ticket) {
  const owner = nodeRegistry[nodeId];
  const lifecycle = owner?.[SCAN_LIFECYCLE_KEY];
  if (!lifecycle || owner !== ticket?.owner) return false;
  const released = releaseLifecycle(lifecycle, ticket);
  syncScanLifecycle(owner);
  setNodeLoading(nodeId, inspectExecutionLifecycle(lifecycle).loading);
  return released;
}

export function inspectScanExecution(nodeId) {
  const lifecycle = nodeRegistry[nodeId]?.[SCAN_LIFECYCLE_KEY];
  return lifecycle ? inspectExecutionLifecycle(lifecycle) : null;
}

export function readCurrentScanResult(nodeId) {
  const lifecycle = nodeRegistry[nodeId]?.[SCAN_LIFECYCLE_KEY];
  return lifecycle ? readCurrentResult(lifecycle) : null;
}

export function restoreScanExecution(nodeId, data = {}) {
  const owner = nodeRegistry[nodeId];
  const contract = resultContract(nodeId);
  if (!owner || !contract || data[contract.resultKey] == null) return false;

  const savedMeta = data[contract.metaKey] || {};
  const lifecycle = scanLifecycleFor(owner);
  restoreHistoricalLifecycle(lifecycle, {
    context: {
      owner,
      workspaceEpoch: getWorkspaceRuntimeEpoch(),
      inputFingerprint: savedMeta.requestFingerprint || stableFingerprint({
        request: savedMeta.request || null,
        model: savedMeta.model || null,
        upstream: scanUpstreamSignature(nodeId),
      }),
      endpoint: scanEndpoint(savedMeta.endpoint, owner.type),
    },
    result: {
      result: data[contract.resultKey],
      meta: savedMeta,
    },
    evidence: data.lifecycle?.evidence || null,
  });

  const restored = inspectExecutionLifecycle(lifecycle).result;
  owner.data = owner.data || {};
  owner.data[contract.resultKey] = restored.result;
  owner.data[contract.metaKey] = {
    contract: 'bne-scan-execution/v1',
    ...restored.meta,
    historical: true,
  };
  syncScanLifecycle(owner);
  return true;
}

export function scheduleCurrentScanPlot(nodeId, ticket, callback, delay = 50) {
  if (!isCurrentScanExecution(nodeId, ticket)) return null;
  const info = nodeRegistry[nodeId];
  clearPlotTimer(info);
  const timer = setTimeout(() => {
    if (nodeRegistry[nodeId] !== ticket.owner || !isCurrentScanExecution(nodeId, ticket)) return;
    if (ticket.owner._scanPlotTimer === timer) delete ticket.owner._scanPlotTimer;
    callback();
  }, delay);
  info._scanPlotTimer = timer;
  return timer;
}

export function scheduleHistoricalScanPlot(nodeId, callback, delay = 50) {
  const info = nodeRegistry[nodeId];
  const contract = resultContract(nodeId);
  if (!info || !contract || info.data?.[contract.resultKey] == null) return null;
  const existing = info[SCAN_LIFECYCLE_KEY]
    ? inspectExecutionLifecycle(info[SCAN_LIFECYCLE_KEY])
    : null;
  if (!existing || existing.state !== 'historical') {
    restoreScanExecution(nodeId, info.data);
  }
  clearPlotTimer(info);
  const owner = info;
  const lifecycle = owner[SCAN_LIFECYCLE_KEY];
  const revision = inspectExecutionLifecycle(lifecycle).revision;
  const timer = setTimeout(() => {
    if (nodeRegistry[nodeId] !== owner) return;
    const runtime = inspectExecutionLifecycle(lifecycle);
    if (runtime.revision !== revision || runtime.state !== 'historical' ||
        runtime.freshness !== 'historical') return;
    if (owner._scanPlotTimer === timer) delete owner._scanPlotTimer;
    callback();
  }, delay);
  info._scanPlotTimer = timer;
  return timer;
}

export function invalidateScanExecution(nodeId, reason = 'scan-input-changed') {
  const info = nodeRegistry[nodeId];
  const contract = resultContract(nodeId);
  if (!info || !contract) return false;

  clearPlotTimer(info);
  if (!info[SCAN_LIFECYCLE_KEY] && info.data?.[contract.resultKey] != null) {
    restoreScanExecution(nodeId, info.data);
  }
  const lifecycle = info[SCAN_LIFECYCLE_KEY];
  if (lifecycle) {
    const runtime = inspectExecutionLifecycle(lifecycle);
    if (runtime.owner === info && runtime.workspaceEpoch != null) {
      invalidateLifecycle(lifecycle, {
        owner: info,
        workspaceEpoch: runtime.workspaceEpoch,
        reason,
      });
      syncScanLifecycle(info);
    }
  } else if (info.data?.lifecycle) {
    info.data.lifecycle = { state: 'invalidated', freshness: 'invalidated', evidence: null };
  }
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
    if (nodeId === exceptNodeId) continue;
    if (atlasResultContract(nodeId) && invalidateAtlasExecution(nodeId, reason)) {
      invalidated.push(nodeId);
      continue;
    }
    if (placerResultContract(nodeId) && invalidatePlacerExecution(nodeId, reason)) {
      invalidated.push(nodeId);
    }
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
    if (!ATLAS_RESULT_CONTRACTS[info?.type] && !PLACER_RESULT_CONTRACTS[info?.type]) continue;
    if (upstreamSignature(before, nodeId) === upstreamSignature(after, nodeId)) continue;
    if (ATLAS_RESULT_CONTRACTS[info.type] && invalidateAtlasExecution(nodeId, reason)) {
      invalidated.push(nodeId);
    } else if (PLACER_RESULT_CONTRACTS[info.type] && invalidatePlacerExecution(nodeId, reason)) {
      invalidated.push(nodeId);
    }
  }
  return invalidated;
}

export function isScanConfigNodeType(type) {
  return type === 'scan-1d-params' || type === 'scan-2d-params';
}

export function isAtlasConfigNodeType(type) {
  return type === 'atlas-spec' || type === 'atlas-query-config';
}
