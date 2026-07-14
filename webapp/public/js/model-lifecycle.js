// Model dependency lifecycle. This module deliberately depends only on shared
// state so graph mutators can retire stale model contexts without importing the
// larger nodes/connections cycle.
import {
  state,
  nodeRegistry,
  connections,
  getWorkspaceRuntimeEpoch,
  setConnections,
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
import {
  invalidateAtlasExecutionsForConnectionChange,
  invalidateScanExecutionsDownstreamOf,
  invalidateScanExecutionsForConnectionChange,
} from './execution-lifecycle.js';

export const MODEL_BUILD_ENDPOINT = '/api/v1/build_model';
const MODEL_BUILD_LIFECYCLE_KEY = '_modelBuildLifecycle';

function sessionIdentifierKey(key) {
  return /^session_?id$/i.test(key);
}

export function stripSessionIdentifiers(value, ancestors = new Map()) {
  if (!value || typeof value !== 'object') return value;
  if (ancestors.has(value)) return ancestors.get(value);
  const clone = Array.isArray(value) ? [] : {};
  ancestors.set(value, clone);
  if (Array.isArray(value)) {
    value.forEach(entry => clone.push(stripSessionIdentifiers(entry, ancestors)));
  } else {
    for (const [key, entry] of Object.entries(value)) {
      if (sessionIdentifierKey(key)) continue;
      clone[key] = stripSessionIdentifiers(entry, ancestors);
    }
  }
  return clone;
}

function syncModelBuildLifecycle(info, lifecycle) {
  if (!info || !lifecycle) return;
  info.data = info.data || {};
  info.data.lifecycle = serializeExecutionLifecycle(lifecycle);
}

function contextForOwner(owner, inputFingerprint, endpoint = MODEL_BUILD_ENDPOINT) {
  return {
    owner,
    workspaceEpoch: getWorkspaceRuntimeEpoch(),
    inputFingerprint,
    endpoint,
  };
}

function modelBuildLifecycleFor(info, builderId, { bootstrap = true } = {}) {
  if (!info[MODEL_BUILD_LIFECYCLE_KEY]) {
    const lifecycle = createExecutionLifecycle();
    info[MODEL_BUILD_LIFECYCLE_KEY] = lifecycle;

    // One-time compatibility bridge for a model built before the shared core
    // was installed. Once created, the core is the only currentness owner.
    const context = info.data?.modelContext;
    const revisionMatches = context?.builtForRevision == null ||
      context.builtForRevision === modelInputRevision(builderId);
    if (bootstrap && info.data?.built !== false && context?.sessionId && revisionMatches) {
      const executionContext = contextForOwner(
        info,
        context.inputFingerprint || `legacy-model-context:${builderId}:${modelInputRevision(builderId)}`,
      );
      const ticket = beginLifecycle(lifecycle, executionContext);
      commitLifecycle(lifecycle, ticket, {
        context: executionContext,
        result: context,
        evidence: info.data?.lifecycle?.evidence || null,
        sessionId: context.sessionId,
      });
      syncModelBuildLifecycle(info, lifecycle);
    }
  }
  return info[MODEL_BUILD_LIFECYCLE_KEY];
}

function lifecycleForTicket(ticket) {
  return ticket?.owner?.[MODEL_BUILD_LIFECYCLE_KEY] || null;
}

export function modelBuildContext(
  builderId,
  inputFingerprint,
  endpoint = MODEL_BUILD_ENDPOINT,
) {
  const owner = nodeRegistry[builderId];
  if (!owner || owner.type !== 'model-builder') return null;
  return contextForOwner(owner, inputFingerprint, endpoint);
}

export function inspectModelBuildLifecycle(builderId) {
  const info = nodeRegistry[builderId];
  if (!info || info.type !== 'model-builder') return null;
  return inspectExecutionLifecycle(modelBuildLifecycleFor(info, builderId));
}

export function readCurrentModelBuildResult(builderId) {
  const info = nodeRegistry[builderId];
  if (!info || info.type !== 'model-builder') return null;
  return readCurrentResult(modelBuildLifecycleFor(info, builderId));
}

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
  const priorContext = info.data.modelContext || null;
  info.data.built = false;
  if (priorContext) {
    info.data.modelContext = {
      ...stripSessionIdentifiers(priorContext),
      sessionId: null,
    };
  }

  const priorRevision = Number.isSafeInteger(info._modelInputRevision)
    ? info._modelInputRevision
    : 0;
  // Remove the retired per-node token if this live object predates the shared
  // lifecycle migration. It is compatibility cleanup, never an authority.
  delete info._buildToken;
  info._modelInputRevision = priorRevision + 1;
  info._modelInvalidationReason = reason;
  let lifecycle = modelBuildLifecycleFor(info, builderId);
  let runtime = inspectExecutionLifecycle(lifecycle);
  if (runtime.owner !== info || runtime.workspaceEpoch == null) {
    lifecycle = createExecutionLifecycle();
    info[MODEL_BUILD_LIFECYCLE_KEY] = lifecycle;
    const context = contextForOwner(
      info,
      `invalidated:${builderId}:${info._modelInputRevision}:${reason}`,
    );
    beginLifecycle(lifecycle, context);
    runtime = inspectExecutionLifecycle(lifecycle);
  }
  invalidateLifecycle(lifecycle, {
    owner: info,
    workspaceEpoch: runtime.workspaceEpoch,
    reason,
  });
  syncModelBuildLifecycle(info, lifecycle);
  setNodeLoading(builderId, false);
  invalidateScanExecutionsDownstreamOf(builderId, reason);

  if (priorContext &&
      (state.sessionId === priorContext.sessionId || state.model === priorContext.model)) {
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

export function beginModelBuild(builderId, context) {
  const info = nodeRegistry[builderId];
  if (!info || info.type !== 'model-builder') return null;
  const lifecycle = modelBuildLifecycleFor(info, builderId);
  const ticket = beginLifecycle(lifecycle, context);
  info.data = info.data || {};
  info.data.built = false;
  syncModelBuildLifecycle(info, lifecycle);
  return ticket;
}

export function isCurrentModelBuild(builderId, ticket, context) {
  const lifecycle = lifecycleForTicket(ticket);
  if (!lifecycle || !context || nodeRegistry[builderId] !== context.owner) return false;
  return isCurrentLifecycle(lifecycle, ticket, context);
}

export function ownsCurrentModelBuild(builderId, ticket) {
  const lifecycle = lifecycleForTicket(ticket);
  if (!lifecycle || nodeRegistry[builderId] !== ticket?.owner) return false;
  return inspectExecutionLifecycle(lifecycle).currentTicket === ticket;
}

export function commitModelBuild(builderId, ticket, context, {
  modelContext,
  evidence = null,
} = {}) {
  const lifecycle = lifecycleForTicket(ticket);
  if (!lifecycle) return false;
  const committed = commitLifecycle(lifecycle, ticket, {
    context,
    result: modelContext,
    evidence,
    sessionId: modelContext?.sessionId || null,
  });
  const owner = ticket.owner;
  if (committed && nodeRegistry[builderId] === owner) {
    owner.data = owner.data || {};
    owner.data.built = true;
    owner.data.modelContext = modelContext;
  }
  syncModelBuildLifecycle(owner, lifecycle);
  return committed;
}

export function failModelBuild(builderId, ticket, context, error) {
  const lifecycle = lifecycleForTicket(ticket);
  if (!lifecycle) return false;
  const failed = failLifecycle(lifecycle, ticket, { context, error });
  const owner = ticket.owner;
  if (failed && nodeRegistry[builderId] === owner) {
    owner.data = owner.data || {};
    owner.data.built = false;
  }
  syncModelBuildLifecycle(owner, lifecycle);
  return failed;
}

export function blockModelBuild(builderId, ticket, context, reason) {
  const lifecycle = lifecycleForTicket(ticket);
  if (!lifecycle) return false;
  const blocked = blockLifecycle(lifecycle, ticket, { context, reason });
  const owner = ticket.owner;
  if (blocked && nodeRegistry[builderId] === owner) {
    owner.data = owner.data || {};
    owner.data.built = false;
  }
  syncModelBuildLifecycle(owner, lifecycle);
  return blocked;
}

export function releaseModelBuild(builderId, ticket) {
  const lifecycle = lifecycleForTicket(ticket);
  if (!lifecycle) return false;
  releaseLifecycle(lifecycle, ticket);
  const runtime = inspectExecutionLifecycle(lifecycle);
  syncModelBuildLifecycle(ticket.owner, lifecycle);
  return nodeRegistry[builderId] === ticket.owner && (
    runtime.currentTicket === ticket ||
    (runtime.currentTicket == null && runtime.loading === false)
  );
}

export function restoreHistoricalModelBuild(builderId, modelContext, evidence = null) {
  const info = nodeRegistry[builderId];
  if (!info || info.type !== 'model-builder' || !modelContext) return null;
  info.data = info.data || {};
  info.data.built = false;
  const lifecycle = modelBuildLifecycleFor(info, builderId, { bootstrap: false });
  const restored = stripSessionIdentifiers(modelContext);
  const context = contextForOwner(
    info,
    restored.inputFingerprint || `restored-model-context:${builderId}`,
  );
  restoreHistoricalLifecycle(lifecycle, {
    context,
    result: restored,
    evidence,
  });
  const runtime = inspectExecutionLifecycle(lifecycle);
  info.data.modelContext = runtime.result;
  syncModelBuildLifecycle(info, lifecycle);
  return runtime.result;
}
