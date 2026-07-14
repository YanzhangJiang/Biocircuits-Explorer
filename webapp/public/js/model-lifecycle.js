// Model dependency lifecycle. This module deliberately depends only on shared
// state so graph mutators can retire stale model contexts without importing the
// larger nodes/connections cycle.
import { state, nodeRegistry, connections, setConnections } from './state.js';
import { setNodeLoading } from './node-loading.js';
import {
  invalidateAtlasExecutionsForConnectionChange,
  invalidateScanExecutionsDownstreamOf,
  invalidateScanExecutionsForConnectionChange,
} from './execution-lifecycle.js';

function reactionInputSignature(connectionList, builderId) {
  return JSON.stringify(connectionList
    .filter(conn => conn?.toNode === builderId && conn?.toPort === 'reactions')
    .map(conn => [conn.fromNode, conn.fromPort])
    .sort((left, right) => JSON.stringify(left).localeCompare(JSON.stringify(right))));
}

export function invalidateModelBuilder(builderId, reason = 'model-input-changed') {
  const info = nodeRegistry[builderId];
  if (!info || info.type !== 'model-builder') return false;

  info.data = info.data || {};
  const context = info.data.modelContext || null;
  info.data.built = false;
  if (context) {
    info.data.modelContext = { ...context, sessionId: null };
  }

  const priorRevision = Number.isSafeInteger(info._modelInputRevision)
    ? info._modelInputRevision
    : 0;
  info._modelInputRevision = priorRevision + 1;
  info._modelInvalidationReason = reason;
  delete info._buildToken;
  setNodeLoading(builderId, false);
  invalidateScanExecutionsDownstreamOf(builderId, reason);

  if (context && (state.sessionId === context.sessionId || state.model === context.model)) {
    state.sessionId = null;
    state.model = null;
    state.qK_syms = [];
  }
  return true;
}

export function invalidateBuildersForConnectionChange(before, after, reason = 'graph-changed') {
  const candidates = new Set();
  for (const conn of [...before, ...after]) {
    if (conn?.toPort === 'reactions' && nodeRegistry[conn.toNode]?.type === 'model-builder') {
      candidates.add(conn.toNode);
    }
  }

  const invalidated = [];
  for (const builderId of candidates) {
    if (reactionInputSignature(before, builderId) === reactionInputSignature(after, builderId)) continue;
    if (invalidateModelBuilder(builderId, reason)) invalidated.push(builderId);
  }
  return invalidated;
}

export function replaceConnectionsWithModelInvalidation(nextConnections, reason = 'graph-changed') {
  const before = connections.map(conn => ({ ...conn }));
  const after = (nextConnections || []).map(conn => ({ ...conn }));
  setConnections(after);
  const invalidatedBuilders = invalidateBuildersForConnectionChange(before, after, reason);
  const invalidatedScanExecutions = invalidateScanExecutionsForConnectionChange(before, after, reason);
  const invalidatedAtlasExecutions = invalidateAtlasExecutionsForConnectionChange(before, after, reason);
  return {
    before,
    after,
    invalidatedBuilders,
    invalidatedScanExecutions,
    invalidatedAtlasExecutions,
  };
}

export function invalidateModelBuildersForReactionSource(sourceNodeId, reason = 'reaction-source-changed') {
  const builderIds = [];
  const seen = new Set();
  for (const conn of connections) {
    if (conn.fromNode !== sourceNodeId || conn.fromPort !== 'reactions' || conn.toPort !== 'reactions') continue;
    if (seen.has(conn.toNode) || nodeRegistry[conn.toNode]?.type !== 'model-builder') continue;
    seen.add(conn.toNode);
    if (invalidateModelBuilder(conn.toNode, reason)) builderIds.push(conn.toNode);
  }
  return builderIds;
}

export function modelInputRevision(builderId) {
  const revision = nodeRegistry[builderId]?._modelInputRevision;
  return Number.isSafeInteger(revision) ? revision : 0;
}

export function beginModelBuild(builderId) {
  const info = nodeRegistry[builderId];
  if (!info || info.type !== 'model-builder') return null;
  const ticket = {
    token: Symbol(`model-build-${builderId}`),
    revision: modelInputRevision(builderId),
  };
  info._buildToken = ticket.token;
  return ticket;
}

export function isCurrentModelBuild(builderId, ticket) {
  const info = nodeRegistry[builderId];
  return !!info && !!ticket &&
    info._buildToken === ticket.token &&
    modelInputRevision(builderId) === ticket.revision;
}

export function releaseModelBuild(builderId, ticket) {
  if (!isCurrentModelBuild(builderId, ticket)) return false;
  delete nodeRegistry[builderId]._buildToken;
  return true;
}
