// Biocircuits Explorer — Node CRUD, Discovery, Menu, Auto-Update & Observer Functions

import { nodeRegistry, connections, nodeIdCounter, nextNodeId, setNodeIdCounter, plotResizeObservers, nodeResizeObservers, plotInteractionGuards } from './state.js';
import { showToast } from './api.js';
import { applyThemeMode } from './theme.js';
import { NODE_TYPES } from './node-types/index.js';
import { updateConnections } from './connections.js';
import { buildModel, getReactionsFromNode } from './model.js';
import { commitWorkspaceSnapshot, queueWorkspaceShellSync, getNodeSerialData } from './workspace.js';
import { refreshAtlasQueryDesigner } from './atlas.js';
import { dispatch, record, CreateNodeCommand, RemoveNodeCommand, ChangeAttrCommand } from './commands.js';
import {
  invalidateModelBuildersForReactionSource,
  modelInputRevision,
  readCurrentModelBuildResult,
  replaceConnectionsWithModelInvalidation,
} from './model-lifecycle.js';
import {
  invalidateAtlasExecutionsDownstreamOf,
  invalidateScanExecutionsDownstreamOf,
  isScanConfigNodeType,
} from './execution-lifecycle.js';
import { getSelection } from './selection.js';
import {
  allConnectedNodeIds,
  connectedComponentNodeIds,
  planWorkflowExecution,
  runWorkflowPlan,
} from './workflow-execution.js';
import {
  GraphPatchCommand,
  GraphPatchError,
  QUICK_ADD_REACTION_SOURCE_TYPES,
  planQuickAddWorkflow,
} from './graph-patch.js';
import { validateNodeConnection } from './connection-validation.js';
export { setNodeLoading } from './node-loading.js';

// ===== Node Discovery =====

export function getIncomingConnections(nodeId) {
  return connections.filter(conn => conn.toNode === nodeId);
}

export function findUpstreamNode(nodeId, predicate, visited = new Set()) {
  if (!nodeId || visited.has(nodeId)) return null;
  visited.add(nodeId);

  if (predicate(nodeId)) {
    return nodeId;
  }

  for (const conn of getIncomingConnections(nodeId)) {
    const found = findUpstreamNode(conn.fromNode, predicate, visited);
    if (found) return found;
  }

  return null;
}

export function findUpstreamNodeByType(nodeId, type) {
  return findUpstreamNode(nodeId, candidateId => nodeRegistry[candidateId]?.type === type);
}

function findUpstreamNodeInConnections(nodeId, predicate, connectionList, visited = new Set()) {
  if (!nodeId || visited.has(nodeId)) return null;
  visited.add(nodeId);
  if (predicate(nodeId)) return nodeId;
  for (const conn of connectionList) {
    if (conn?.toNode !== nodeId) continue;
    const found = findUpstreamNodeInConnections(conn.fromNode, predicate, connectionList, visited);
    if (found) return found;
  }
  return null;
}

export function getModelContextFromBuilder(modelBuilderNodeId) {
  return readCurrentModelBuildResult(modelBuilderNodeId);
}

export function getModelContextForNode(nodeId) {
  if (!nodeId) return null;
  const modelBuilderNodeId = findUpstreamNodeByType(nodeId, 'model-builder');
  if (!modelBuilderNodeId) return null;
  return getModelContextFromBuilder(modelBuilderNodeId);
}

export function getModelForNode(nodeId) {
  return getModelContextForNode(nodeId)?.model || null;
}

export function getSessionIdForNode(nodeId) {
  return getModelContextForNode(nodeId)?.sessionId || null;
}

export function getQKSymbolsForNode(nodeId) {
  return getModelContextForNode(nodeId)?.qK_syms || [];
}

export function hasModelContextForNode(nodeId) {
  return !!getModelContextForNode(nodeId);
}

// Dedupe concurrent rebuilds: when a workspace loads, several downstream viewers
// may call ensureModelSession at once for the same builder.
const _rebuildInFlight = new Map();

// Return a live backend session id for `nodeId`'s model, rebuilding it from the
// connected reaction network if no session is live (fresh workspace load, server
// restart, or cache eviction). The NetworkIR is the real input; the session is
// just a cache, so this rehydrates transparently instead of forcing the user to
// click "Run" on the Model Builder again.
export async function ensureModelSession(nodeId, { connectionResolver = null } = {}) {
  while (true) {
    const dependencyConnections = typeof connectionResolver === 'function'
      ? connectionResolver()
      : connections;
    const builderId = findUpstreamNodeInConnections(
      nodeId,
      candidateId => nodeRegistry[candidateId]?.type === 'model-builder',
      dependencyConnections,
    );
    const live = builderId ? getModelContextFromBuilder(builderId)?.sessionId : null;
    if (live) return live;

    if (!builderId) throw new Error('Build the connected model first');

    const revision = modelInputRevision(builderId);
    let entry = _rebuildInFlight.get(builderId);
    if (!entry || entry.revision !== revision) {
      const promise = (async () => {
        await buildModel(builderId, {
          triggerDownstream: false,
          throwOnFailure: true,
          connectionResolver,
        });
      })();
      entry = { revision, promise };
      _rebuildInFlight.set(builderId, entry);
      const cleanup = () => {
        if (_rebuildInFlight.get(builderId) === entry) _rebuildInFlight.delete(builderId);
      };
      promise.then(cleanup, cleanup);
    }
    await entry.promise;
    // A build can be superseded while this caller is awaiting it. Loop to the
    // current graph/revision instead of failing a caller on an obsolete result.
  }
}

// ===== Node CRUD =====

const RESTORE_ONLY_NODE_CREATION_MODES = new Set([
  'workspace-restore',
  'workspace-migration',
]);

export function validateNodeCreation(nodeType, opts = {}) {
  const definition = NODE_TYPES[nodeType];
  if (!definition) {
    return {
      ok: false,
      code: 'unknown-node-type',
      message: `Unknown node type: ${String(nodeType)}`,
      definition: null,
    };
  }
  if (definition.availability === 'restore-only' &&
      !RESTORE_ONLY_NODE_CREATION_MODES.has(opts.creationMode)) {
    return {
      ok: false,
      code: 'restore-only-node-type',
      message: `${definition.title || nodeType} is restore-only and cannot be created interactively`,
      definition,
    };
  }
  return { ok: true, code: 'creatable', message: 'Node type is creatable', definition };
}

export function createNode(nodeType, x, y, opts = {}) {
  const creation = validateNodeCreation(nodeType, opts);
  if (!creation.ok) {
    console.error(`[nodes] ${creation.message}`);
    return null;
  }
  const typeDef = creation.definition;

  let nodeId;
  if (opts.id) {
    // Forced id — used when restoring a deleted node so the undo stack and
    // any other commands keep referring to the same id. Keep the global
    // counter ahead of restored ids to avoid future collisions.
    nodeId = opts.id;
    const n = parseInt(String(opts.id).replace(/^node-/, ''), 10);
    if (Number.isFinite(n) && n > nodeIdCounter) setNodeIdCounter(n);
  } else {
    nodeId = `node-${nextNodeId()}`;
  }

  const canvas = document.getElementById('canvas');

  const node = document.createElement('div');
  const isLargeNode = ['viewer', 'result', 'parameter'].includes(typeDef.category);
  node.className = `node${isLargeNode ? ' viewer' : ''}`;
  node.id = nodeId;
  node.dataset.type = typeDef.category;
  node.dataset.nodeType = nodeType;
  node.style.left = `${x}px`;
  node.style.top = `${y}px`;
  if (typeDef.defaultWidth) node.style.width = `${typeDef.defaultWidth}px`;
  if (typeDef.defaultHeight) node.style.height = `${typeDef.defaultHeight}px`;

  // Header
  const header = document.createElement('div');
  header.className = `node-header ${typeDef.headerClass}`;
  header.innerHTML = `
    <span>${typeDef.title}</span>
    <button class="btn-close" data-action="removeNode" data-node="${nodeId}">&times;</button>
  `;
  node.appendChild(header);

  // Body
  const body = document.createElement('div');
  body.className = 'node-body';

  // Input sockets
  typeDef.inputs.forEach(inp => {
    body.innerHTML += `
      <div class="socket-row left">
        <div class="socket input" data-node="${nodeId}" data-port="${inp.port}"></div>
        <span class="socket-label">${inp.label}</span>
      </div>
    `;
  });

  // Custom body content
  if (typeDef.createBody) {
    body.innerHTML += typeDef.createBody(nodeId);
  }

  if (body.querySelector('.tab-nav')) {
    body.classList.add('node-body-tabbed');
  }

  // Output sockets
  typeDef.outputs.forEach(out => {
    body.innerHTML += `
      <div class="socket-row right">
        <span class="socket-label">${out.label}</span>
        <div class="socket output" data-node="${nodeId}" data-port="${out.port}"></div>
      </div>
    `;
  });

  node.appendChild(body);

  // Resize handle
  const resize = document.createElement('div');
  resize.className = 'node-resize';
  node.appendChild(resize);

  canvas.appendChild(node);

  nodeRegistry[nodeId] = { type: nodeType, el: node, data: {} };
  setupNodeResizeObserver(nodeId, node);

  // Run init hook
  if (typeDef.onInit) typeDef.onInit(nodeId);

  return nodeId;
}

export function removeNode(nodeId) {
  const el = document.getElementById(nodeId);
  if (el) el.remove();
  replaceConnectionsWithModelInvalidation(
    connections.filter(c => c.fromNode !== nodeId && c.toNode !== nodeId),
    'node-removed',
  );
  delete nodeRegistry[nodeId];
  cleanupNodeResizeObserver(nodeId);
  cleanupPlotResize(nodeId);
  updateConnections();
}

// Undoable delete: snapshots the node (+ its wires) then removes it, so
// Ctrl+Z restores everything. This is what the node's × button and the
// Delete key route through; the raw removeNode() above stays available for
// non-history paths (workspace load/clear, command internals).
export function deleteNodeWithHistory(nodeId) {
  if (!document.getElementById(nodeId)) return;
  dispatch(new RemoveNodeCommand({ nodeId }));
}

// Generic field setter used by ChangeAttrCommand on undo/redo. `key` is the
// field element's DOM id. Fires the same value event the field normally uses
// so auto-update listeners react as if the user edited it — but note this does
// NOT re-enter the focusout-based capture below, so no history loop.
export function setNodeAttr(_nodeId, key, value) {
  const el = document.getElementById(key);
  if (!el) return;
  if (el.type === 'checkbox') {
    el.checked = !!value;
  } else {
    el.value = value;
  }
  const tag = String(el.tagName || '').toUpperCase();
  const valueEvent = (tag === 'SELECT' || el.type === 'checkbox') ? 'change' : 'input';
  el.dispatchEvent(new Event(valueEvent, { bubbles: true }));
  if (valueEvent !== 'change') {
    el.dispatchEvent(new Event('change', { bubbles: true }));
  }
}

// Capture config-field edits as ChangeAttrCommands. We listen on focusin
// (remember the value) / focusout (compare + record) rather than `change`,
// so we never collide with the existing change-delegation in main.js and
// never re-enter when setNodeAttr fires a synthetic change. Only plain
// fields inside a node body without a data-action are tracked.
export function initAttrHistory(target = document) {
  const lastValue = new WeakMap();

  const isTracked = (el) =>
    el &&
    el.closest('.node-body') &&
    !el.dataset.action &&
    !el.closest('.plot-container') &&
    /^(INPUT|TEXTAREA|SELECT)$/.test(el.tagName) &&
    el.id;

  const readVal = (el) => (el.type === 'checkbox' ? el.checked : el.value);

  target.addEventListener('focusin', (e) => {
    const el = e.target;
    if (isTracked(el)) lastValue.set(el, readVal(el));
  });

  target.addEventListener('focusout', (e) => {
    const el = e.target;
    if (!isTracked(el)) return;
    const before = lastValue.get(el);
    const after = readVal(el);
    if (before === after || before === undefined) return;
    const nodeId = el.closest('.node')?.id || null;
    record(new ChangeAttrCommand({ nodeId, key: el.id, before, after }));
    lastValue.set(el, after);
  });
}

// ===== Auto-Chain Generation =====

function isReactionSourceNodeType(type) {
  return QUICK_ADD_REACTION_SOURCE_TYPES.includes(type);
}

// Find an existing chain ending with a model-builder that has a model output
export function findExistingModelBuilder() {
  for (const [id, info] of Object.entries(nodeRegistry)) {
    if (info.type === 'model-builder') {
      // Check if this model-builder is connected to a reaction source
      const conn = connections.find(c => c.toNode === id && c.toPort === 'reactions');
      if (conn && isReactionSourceNodeType(nodeRegistry[conn.fromNode]?.type)) {
        return { modelBuilderId: id, reactionNetworkId: conn.fromNode };
      }
    }
  }
  return null;
}

export function findExistingReactionNetwork() {
  for (const [id, info] of Object.entries(nodeRegistry)) {
    if (isReactionSourceNodeType(info.type)) return id;
  }
  return null;
}

export function getNodePosition(nodeId) {
  const el = document.getElementById(nodeId);
  if (!el) return { x: 100, y: 150 };
  return { x: parseFloat(el.style.left) || 0, y: parseFloat(el.style.top) || 0 };
}

// Set a node's canvas position and redraw its wires. Used both by the
// MoveNodeCommand (undo/redo) and as the single mutation point a future
// alignment toolbar can call. Kept side-effect-light: no history is
// recorded here — callers that want undo wrap this in a command.
export function moveNode(nodeId, x, y) {
  const el = document.getElementById(nodeId);
  if (!el) return;
  el.style.left = `${x}px`;
  el.style.top = `${y}px`;
  updateConnections();
}

export function getNodeSize(nodeId) {
  const el = document.getElementById(nodeId);
  if (!el) return { w: 260, h: 200 };
  return { w: el.offsetWidth, h: el.offsetHeight };
}

// Count how many viewers are already attached to a model-builder
export function countDownstreamViewers(modelBuilderId) {
  return connections.filter(c => c.fromNode === modelBuilderId && c.fromPort === 'model').length;
}

// Simple collision detection — shift node down if overlapping
export function resolveOverlap(x, y, width, height, excludeNodeId) {
  let maxAttempts = 20;
  let curY = y;
  while (maxAttempts-- > 0) {
    let overlaps = false;
    for (const id of Object.keys(nodeRegistry)) {
      if (id === excludeNodeId) continue;
      const pos = getNodePosition(id);
      const size = getNodeSize(id);
      if (x < pos.x + size.w && x + width > pos.x &&
          curY < pos.y + size.h && curY + height > pos.y) {
        curY = pos.y + size.h + 30;
        overlaps = true;
        break;
      }
    }
    if (!overlaps) break;
  }
  return curY;
}

// ===== Node Addition =====

export function addNodeFromMenu(nodeType) {
  closeDropdown();

  // Simple strategy: just create the node at a reasonable position
  const typeDef = NODE_TYPES[nodeType];
  if (!typeDef) return;

  // Find a good position based on existing nodes
  let x = 80;
  let y = 150;

  // If there are existing nodes, place new node to the right
  const existingNodes = Object.keys(nodeRegistry);
  if (existingNodes.length > 0) {
    let maxX = 0;
    for (const id of existingNodes) {
      const pos = getNodePosition(id);
      const size = getNodeSize(id);
      if (pos.x + size.w > maxX) {
        maxX = pos.x + size.w;
      }
    }
    x = maxX + 60;
  }

  const width = typeDef.defaultWidth || 280;
  y = resolveOverlap(x, y, width, 300, null);

  // Routed through the command layer so "Add node" is undoable (Ctrl+Z).
  dispatch(new CreateNodeCommand({ nodeType, x, y }));
}

export function addResultNode(nodeType) {
  // This function is no longer used - kept for compatibility
  // All nodes are now added via addNodeFromMenu
  addNodeFromMenu(nodeType);
}

// ===== Quick Add Chain Generation =====

function cloneConnections(connectionList) {
  return (connectionList || []).map(connection => ({ ...connection }));
}

export function captureQuickAddGraphSnapshot() {
  return {
    nodes: Object.entries(nodeRegistry).map(([id, info]) => {
      const position = getNodePosition(id);
      return { id, type: info.type, x: position.x, y: position.y };
    }),
    connections: cloneConnections(connections),
    nodeIdCounter,
  };
}

export function captureEditorGraphPlanningGraph() {
  return {
    nodes: Object.entries(nodeRegistry).map(([id, info]) => {
      const position = getNodePosition(id);
      const size = getNodeSize(id);
      return {
        id,
        type: info.type,
        x: position.x,
        y: position.y,
        width: size.w,
        height: size.h,
      };
    }),
    connections: cloneConnections(connections),
  };
}

function quickAddComparableSnapshot(snapshot) {
  const nodes = (snapshot?.nodes || []).map(({ id, type, x, y }) => ({ id, type, x, y }));
  const largestNodeOrdinal = nodes.reduce((largest, node) => {
    const match = /^node-(\d+)$/.exec(node.id);
    return match ? Math.max(largest, Number(match[1])) : largest;
  }, 0);
  return {
    nodes,
    connections: cloneConnections(snapshot?.connections),
    // projectGraphPatch is intentionally generic and preserves extra snapshot
    // fields. Treat the largest newly planned node ID as the projected counter
    // so commit verification can still require exact counter restoration.
    nodeIdCounter: Math.max(snapshot?.nodeIdCounter || 0, largestNodeOrdinal),
  };
}

export function quickAddGraphSnapshotsEqual(left, right) {
  return JSON.stringify(quickAddComparableSnapshot(left)) ===
    JSON.stringify(quickAddComparableSnapshot(right));
}

function restoreQuickAddGraphSnapshot(snapshot, phase) {
  const targetNodes = new Map(snapshot.nodes.map(spec => [spec.id, spec]));

  // Check type identity before making the first mutation. A type mismatch
  // cannot be repaired without serialized node data and must fail closed.
  for (const [id, spec] of targetNodes) {
    const live = nodeRegistry[id];
    if (live && live.type !== spec.type) {
      throw new GraphPatchError(
        'quick-add-node-type-mismatch',
        `Cannot ${phase}: ${id} changed from ${spec.type} to ${live.type}`,
      );
    }
  }

  for (const id of Object.keys(nodeRegistry).reverse()) {
    if (!targetNodes.has(id)) removeNode(id);
  }

  for (const spec of snapshot.nodes) {
    if (!nodeRegistry[spec.id]) {
      const restoredId = createNode(spec.type, spec.x, spec.y, {
        id: spec.id,
        creationMode: 'quick-add-rollback',
      });
      if (restoredId !== spec.id) {
        throw new GraphPatchError(
          'quick-add-node-restore-failed',
          `Cannot ${phase}: failed to restore ${spec.id}`,
        );
      }
    }
    const element = document.getElementById(spec.id);
    if (!element) {
      throw new GraphPatchError(
        'quick-add-node-element-missing',
        `Cannot ${phase}: ${spec.id} has no DOM element`,
      );
    }
    element.style.left = `${spec.x}px`;
    element.style.top = `${spec.y}px`;
  }

  replaceConnectionsWithModelInvalidation(
    cloneConnections(snapshot.connections),
    `quick-add-${phase}`,
  );
  setNodeIdCounter(snapshot.nodeIdCounter);
  updateConnections();
}

export function createEditorGraphPatchAdapter({ initializeNode = null } = {}) {
  return {
    captureSnapshot: captureQuickAddGraphSnapshot,
    stageNode: spec => ({ ...spec }),
    stageConnection: spec => ({ ...spec }),
    commit(transaction, context) {
      for (const spec of transaction.patch.nodes) {
        const createdId = createNode(spec.type, spec.x, spec.y, {
          id: spec.id,
          creationMode: 'graph-patch',
        });
        if (createdId !== spec.id) {
          throw new GraphPatchError(
            'graph-patch-node-create-failed',
            `Graph patch failed to create ${spec.id} (${spec.type})`,
          );
        }
        if (typeof initializeNode === 'function') {
          const initialization = initializeNode(spec, {
            nodeId: createdId,
            transaction,
            context,
          });
          if (initialization && typeof initialization.then === 'function') {
            throw new GraphPatchError(
              'async-graph-patch-initializer',
              'Graph patch node initialization must be synchronous',
            );
          }
        }
      }
      replaceConnectionsWithModelInvalidation(
        cloneConnections(transaction.nextSnapshot.connections),
        'editor-graph-patch',
      );
      updateConnections();
    },
    restoreSnapshot(snapshot, context = {}) {
      restoreQuickAddGraphSnapshot(snapshot, context.phase || 'restore');
    },
  };
}

export function createQuickAddGraphAdapter(options = {}) {
  return createEditorGraphPatchAdapter(options);
}

export function createEditorGraphPatchValidators() {
  return {
    snapshot(snapshot) {
      if (!Number.isSafeInteger(snapshot.nodeIdCounter) || snapshot.nodeIdCounter < 0) {
        return { ok: false, message: 'Graph patch requires a valid node ID counter' };
      }
      return true;
    },
    node(spec, { isNew }) {
      const definition = NODE_TYPES[spec.type];
      if (!definition) {
        return { ok: false, message: `Unknown node type ${spec.type}` };
      }
      if (isNew && definition.availability !== 'active') {
        return {
          ok: false,
          message: `Graph patch cannot create ${spec.type} because it is ${definition.availability}`,
        };
      }
      return true;
    },
    connection(spec, { nextSnapshot }) {
      const projectedRegistry = Object.fromEntries(
        nextSnapshot.nodes.map(nodeSpec => [nodeSpec.id, { type: nodeSpec.type }]),
      );
      return validateNodeConnection(spec, projectedRegistry, NODE_TYPES);
    },
  };
}

export function createQuickAddGraphValidators() {
  return createEditorGraphPatchValidators();
}

function reactionsReadyForBuild(modelBuilderNodeId) {
  const reactionInput = connections.find(connection =>
    connection.toNode === modelBuilderNodeId && connection.toPort === 'reactions');
  if (!reactionInput) return false;
  const { reactions, kds } = getReactionsFromNode(reactionInput.fromNode);
  return reactions.length > 0 && kds.length === reactions.length &&
    kds.every(kd => Number.isFinite(kd) && kd > 0);
}

function reactionsReadyForParams(paramsNodeId) {
  const reactionInput = connections.find(connection =>
    connection.toNode === paramsNodeId && connection.toPort === 'reactions');
  if (!reactionInput) return false;
  return getReactionsFromNode(reactionInput.fromNode).reactions.length > 0;
}

export function createEditorGraphPatchDeferredTasks(patch) {
  return (patch.effects || []).map(effect => {
    if (effect.kind === 'node-auto-build-if-needed') {
      return {
        id: effect.id,
        delay: effect.delay,
        run(context) {
          if (!context.isCurrent() || context.signal.aborted) return undefined;
          if (nodeRegistry[effect.nodeId]?.type !== 'model-builder') return undefined;
          if (getModelContextFromBuilder(effect.nodeId)) return undefined;
          if (!reactionsReadyForBuild(effect.nodeId)) return undefined;
          if (!context.isCurrent() || context.signal.aborted) return undefined;
          return buildModel(effect.nodeId, { triggerDownstream: true });
        },
      };
    }
    if (effect.kind === 'node-execute-if-ready') {
      return {
        id: effect.id,
        delay: effect.delay,
        run(context) {
          if (!context.isCurrent() || context.signal.aborted) return undefined;
          const info = nodeRegistry[effect.nodeId];
          const prepare = NODE_TYPES[info?.type]?.prepare;
          if (typeof prepare !== 'function') return undefined;
          const ready = hasModelContextForNode(effect.nodeId) ||
            (info.type === 'rop-cloud-params' && reactionsReadyForParams(effect.nodeId));
          if (!ready || !context.isCurrent() || context.signal.aborted) return undefined;
          return prepare(effect.nodeId, { triggerDownstream: false });
        },
      };
    }
    throw new GraphPatchError(
      'unknown-graph-patch-effect',
      `Unknown graph patch deferred effect: ${String(effect.kind)}`,
    );
  });
}

export function createQuickAddDeferredTasks(patch) {
  return createEditorGraphPatchDeferredTasks(patch);
}

export function createEditorGraphPatchCommand(plan, options = {}) {
  if (!plan?.ok || !plan.patch) {
    throw new GraphPatchError(
      'invalid-editor-graph-patch-plan',
      'Cannot create a graph command from an unsuccessful graph patch plan',
    );
  }
  return new GraphPatchCommand({
    patch: plan.patch,
    adapter: options.adapter || createEditorGraphPatchAdapter({
      initializeNode: options.initializeNode,
    }),
    validators: options.validators || createEditorGraphPatchValidators(),
    deferredTasks: options.deferredTasks ?? createEditorGraphPatchDeferredTasks(plan.patch),
    scheduler: options.scheduler,
    snapshotEquals: quickAddGraphSnapshotsEqual,
    onEpochCancel: options.onEpochCancel,
    onDeferredError: options.onDeferredError || ((error, details) => {
      console.error(`[graph-patch] Deferred task ${details.task?.id || 'unknown'} failed`, error);
    }),
  });
}

export function createQuickAddGraphCommand(plan, options = {}) {
  return createEditorGraphPatchCommand(plan, options);
}

function selectedQuickAddNodeId(predicate) {
  const candidates = getSelection().filter(id => predicate(nodeRegistry[id]?.type));
  return candidates.length === 1 ? candidates[0] : undefined;
}

export function addQuickAddChain(chainType, options = {}) {
  closeDropdown();

  const createIsolatedSource = options.createIsolatedSource === true;
  const selectedSourceId = Object.prototype.hasOwnProperty.call(options, 'selectedSourceId')
    ? options.selectedSourceId
    : createIsolatedSource
      ? undefined
      : selectedQuickAddNodeId(isReactionSourceNodeType);
  const selectedModelBuilderId = Object.prototype.hasOwnProperty.call(options, 'selectedModelBuilderId')
    ? options.selectedModelBuilderId
    : createIsolatedSource
      ? undefined
      : selectedQuickAddNodeId(type => type === 'model-builder');

  const plan = planQuickAddWorkflow({
    chainType,
    graph: captureEditorGraphPlanningGraph(),
    nextNodeOrdinal: nodeIdCounter + 1,
    anchor: options.anchor,
    selectedSourceId,
    selectedModelBuilderId,
    createIsolatedSource,
  });
  if (!plan.ok) {
    const hint = plan.diagnostic.code === 'manual-source-selection-required'
      ? ' Shift-click Quick Add to create an isolated source.'
      : '';
    showToast(`${plan.diagnostic.message}${hint}`);
    return plan;
  }

  try {
    const command = createQuickAddGraphCommand(plan);
    dispatch(command);
    return { ...plan, command };
  } catch (error) {
    const diagnostic = {
      kind: 'error',
      code: error?.code || 'quick-add-commit-failed',
      message: error?.message || String(error),
    };
    console.error('[quick-add] Graph patch failed', error);
    showToast(`Quick Add failed: ${diagnostic.message}`);
    return { ok: false, patch: null, diagnostic };
  }
}

// ===== Dropdown =====

export function closeDropdown() {
  const addNodeMenu = document.getElementById('add-node-menu');
  const legacyNodesMenu = document.getElementById('legacy-nodes-menu');
  const themeModeMenu = document.getElementById('theme-mode-menu');
  addNodeMenu.classList.remove('open');
  legacyNodesMenu.classList.remove('open');
  themeModeMenu?.classList.remove('open');
}

// ===== Observer Functions =====

export function setupNodeResizeObserver(nodeId, nodeEl) {
  cleanupNodeResizeObserver(nodeId);
  if (!nodeEl) return;

  let rafId = null;
  const observer = new ResizeObserver(() => {
    if (rafId) cancelAnimationFrame(rafId);
    rafId = requestAnimationFrame(() => {
      const plotEls = nodeEl.querySelectorAll('.plot-container');
      plotEls.forEach(plotEl => {
        if (!plotEl.classList.contains('js-plotly-plot')) return;
        Plotly.Plots.resize(plotEl);
      });
      updateConnections();
      rafId = null;
    });
  });

  observer.observe(nodeEl);
  nodeResizeObservers.set(nodeId, observer);
}

export function cleanupNodeResizeObserver(nodeId) {
  if (nodeResizeObservers.has(nodeId)) {
    nodeResizeObservers.get(nodeId).disconnect();
    nodeResizeObservers.delete(nodeId);
  }
}

export function setupPlotInteractionGuard(plotEl) {
  if (!plotEl || plotInteractionGuards.has(plotEl)) return;

  plotEl.addEventListener('wheel', (e) => {
    e.stopPropagation();
  }, { passive: true });

  plotEl.addEventListener('pointerdown', (e) => {
    e.stopPropagation();
  });

  plotInteractionGuards.add(plotEl);
}

export function setupPlotResize(nodeId, plotId) {
  // Clean up existing observer
  if (plotResizeObservers.has(nodeId)) {
    plotResizeObservers.get(nodeId).disconnect();
  }

  const plotEl = document.getElementById(plotId);
  if (!plotEl) return;
  setupPlotInteractionGuard(plotEl);

  const observer = new ResizeObserver(() => {
    Plotly.Plots.resize(plotEl);
    updateConnections();
  });

  observer.observe(plotEl);
  plotResizeObservers.set(nodeId, observer);
}

export function cleanupPlotResize(nodeId) {
  if (plotResizeObservers.has(nodeId)) {
    plotResizeObservers.get(nodeId).disconnect();
    plotResizeObservers.delete(nodeId);
  }
}

// ===== Tab Navigation =====

export function setupTabNavigation(nodeId) {
  const node = document.getElementById(nodeId);
  if (!node) return;

  const tabButtons = node.querySelectorAll('.tab-btn');
  const tabContents = node.querySelectorAll('.tab-content');

  tabButtons.forEach(btn => {
    btn.addEventListener('click', () => {
      const targetTab = btn.getAttribute('data-tab');

      // Remove active class from all buttons and contents
      tabButtons.forEach(b => b.classList.remove('active'));
      tabContents.forEach(c => c.classList.remove('active'));

      // Add active class to clicked button and corresponding content
      btn.classList.add('active');
      const targetContent = node.querySelector(`.tab-content[data-tab="${targetTab}"]`);
      if (targetContent) {
        targetContent.classList.add('active');
      }
    });
  });
}

// ===== Auto-update Config Nodes =====
export function setupAutoUpdate(nodeId, nodeType) {
  const node = document.getElementById(nodeId);
  if (!node) return;

  // Save initial default config
  triggerConfigUpdate(nodeId, nodeType);

  // Find all inputs with auto-update class
  const inputs = node.querySelectorAll('.auto-update');

  inputs.forEach(input => {
    const eventType = input.tagName === 'SELECT' ? 'change' :
                      input.type === 'checkbox' ? 'change' : 'input';

    input.addEventListener(eventType, () => {
      if (nodeType === 'network-id-definition') markReactionSourceDirty(nodeId);
      // Atlas requests may depend on any upstream serial config (including a
      // connected network definition). Retire visible/stored results at the
      // input event, before the debounced serialization catches up.
      invalidateAtlasExecutionsDownstreamOf(nodeId, 'atlas-config-input-changed');
      if (isScanConfigNodeType(nodeType)) {
        // Retire requests immediately; the serialized config update for text
        // and number fields remains debounced below.
        invalidateScanExecutionsDownstreamOf(nodeId, 'scan-config-input-changed');
      }
      // Debounce for text inputs
      if (input.type === 'text' || input.type === 'number') {
        clearTimeout(input._autoUpdateTimer);
        input._autoUpdateTimer = setTimeout(() => {
          triggerConfigUpdate(nodeId, nodeType);
        }, 500);
      } else {
        // Immediate for selects and checkboxes
        triggerConfigUpdate(nodeId, nodeType);
      }
    });
  });
}

export function triggerConfigUpdate(nodeId, nodeType, { preserveExecutionNodeId = null } = {}) {
  // Store config in node data
  const info = nodeRegistry[nodeId];
  if (!info) return;

  info.data = info.data || {};
  const previousConfig = info.data.config;
  const nextConfig = getNodeSerialData(nodeId, nodeType);
  info.data.config = nextConfig;
  const configChanged = JSON.stringify(previousConfig ?? null) !== JSON.stringify(nextConfig ?? null);
  if (configChanged) {
    invalidateAtlasExecutionsDownstreamOf(nodeId, 'atlas-config-changed');
  }
  if (isScanConfigNodeType(nodeType) &&
      configChanged) {
    invalidateScanExecutionsDownstreamOf(nodeId, 'scan-config-changed', {
      exceptNodeId: preserveExecutionNodeId,
    });
  }
  if (nodeType === 'atlas-query-config') refreshAtlasQueryDesigner(nodeId);
  if (nodeType === 'network-id-definition') triggerAutoModelBuild(nodeId);
}

// ===== Auto-build Model =====
export function setupAutoModelBuild(nodeId) {
  // Check if there's a connection to reaction-network
  const checkAndBuild = () => {
    if (getModelContextFromBuilder(nodeId)) {
      return;
    }
    const rxConn = connections.find(c => c.toNode === nodeId && c.toPort === 'reactions');
    if (rxConn) {
      const { reactions } = getReactionsFromNode(rxConn.fromNode);
      if (reactions.length > 0) {
        // Valid reactions exist, auto-build
        setTimeout(() => buildModel(nodeId), 100);
      }
    }
  };

  // Initial check
  checkAndBuild();

  // Store the check function for later use
  if (!nodeRegistry[nodeId]) return;
  nodeRegistry[nodeId]._autoBuildCheck = checkAndBuild;
}

// Trigger auto-build when reactions change
export function markReactionSourceDirty(reactionNodeId) {
  const affected = invalidateModelBuildersForReactionSource(reactionNodeId, 'reaction-source-edited');
  if (affected.length > 0) queueWorkspaceShellSync('model-dirty');
  return affected;
}

export function triggerAutoModelBuild(reactionNodeId, { delay = 500, invalidate = true } = {}) {
  // Find all connected model-builder nodes
  const modelBuilders = connections
    .filter(c => c.fromNode === reactionNodeId && c.fromPort === 'reactions')
    .map(c => c.toNode);

  const affected = invalidate ? markReactionSourceDirty(reactionNodeId) : modelBuilders;
  modelBuilders.forEach(mbId => {
    const info = nodeRegistry[mbId];
    if (info && info._autoBuildCheck) {
      // Debounce the build
      clearTimeout(info._autoBuildTimer);
      info._autoBuildTimer = setTimeout(() => {
        info._autoBuildCheck();
      }, delay);
    }
  });
  return affected;
}

export function triggerAllAutoModelBuilds() {
  Object.entries(nodeRegistry).forEach(([nodeId, info]) => {
    if (info.type !== 'model-builder' || !info._autoBuildCheck) return;
    // Skip nodes whose model is restored from the saved workspace. Their backend
    // session is gone (built=false, sessionId=null) but the cached model is shown;
    // forcing a rebuild here would slam /api/v1/build_model for every chain in the
    // project at load time. User clicks Run (or Run Connected) when ready.
    if (info.data?.modelContext?.model) return;
    info._autoBuildCheck();
  });
}

// ===== Execution =====

export function isRunnableNode(nodeId) {
  const info = nodeRegistry[nodeId];
  if (!info) return false;
  const definition = NODE_TYPES[info.type];
  return ['prepare', 'execute', 'manual'].includes(definition?.execution?.mode);
}

export function connectedNodeIDs() {
  const ids = new Set();
  connections.forEach(conn => {
    ids.add(conn.fromNode);
    ids.add(conn.toNode);
  });
  return ids;
}

function workflowReportMessage(report) {
  const summary = report.summary;
  return [
    `executed ${summary.executed}`,
    `reused ${summary.reused}`,
    `blocked ${summary.blocked}`,
    `failed ${summary.failed}`,
    `cancelled ${summary.cancelled}`,
    `stale ${summary.stale}`,
  ].join(' · ');
}

async function runWorkspaceScope(nodeIds, snapshotLabel) {
  const plan = planWorkflowExecution({ nodeIds, connections, registry: nodeRegistry });
  if (!plan.ok) {
    const report = await runWorkflowPlan(plan, { registry: nodeRegistry, nodeTypes: NODE_TYPES });
    showToast(plan.message);
    return report;
  }
  const report = await runWorkflowPlan(plan, { registry: nodeRegistry, nodeTypes: NODE_TYPES });
  commitWorkspaceSnapshot(snapshotLabel);
  showToast(`Workflow ${report.status}: ${workflowReportMessage(report)}`);
  return report;
}

export async function runConnectedWorkspace() {
  const selected = getSelection().filter(nodeId => nodeRegistry[nodeId]);
  if (!selected.length) {
    showToast('Select a node to run its connected component');
    return null;
  }
  return runWorkspaceScope(
    connectedComponentNodeIds(selected[0], connections),
    'run-selected-component',
  );
}

export async function runAllConnectedWorkspace() {
  const nodeIds = allConnectedNodeIds(connections);
  if (!nodeIds.length) {
    showToast('No connected nodes to run');
    return null;
  }
  return runWorkspaceScope(nodeIds, 'run-all-connected');
}

// ===== Menu Initialisation =====

export function initNodeMenuEvents() {
  const addNodeBtn = document.getElementById('add-node-btn');
  const addNodeMenu = document.getElementById('add-node-menu');
  const legacyNodesBtn = document.getElementById('legacy-nodes-btn');
  const legacyNodesMenu = document.getElementById('legacy-nodes-menu');
  const themeModeBtn = document.getElementById('theme-mode-btn');
  const themeModeMenu = document.getElementById('theme-mode-menu');
  const runConnectedBtn = document.getElementById('run-connected-btn');
  const runAllConnectedBtn = document.getElementById('run-all-connected-btn');

  addNodeBtn.addEventListener('click', (e) => {
    e.stopPropagation();
    addNodeMenu.classList.toggle('open');
    legacyNodesMenu.classList.remove('open');
    themeModeMenu?.classList.remove('open');
  });

  legacyNodesBtn.addEventListener('click', (e) => {
    e.stopPropagation();
    legacyNodesMenu.classList.toggle('open');
    addNodeMenu.classList.remove('open');
    themeModeMenu?.classList.remove('open');
  });

  themeModeBtn?.addEventListener('click', (e) => {
    e.stopPropagation();
    themeModeMenu?.classList.toggle('open');
    addNodeMenu.classList.remove('open');
    legacyNodesMenu.classList.remove('open');
  });

  runConnectedBtn?.addEventListener('click', (e) => {
    e.stopPropagation();
    void runConnectedWorkspace();
  });

  runAllConnectedBtn?.addEventListener('click', (e) => {
    e.stopPropagation();
    void runAllConnectedWorkspace();
  });

  document.addEventListener('click', (e) => {
    if (!addNodeMenu.contains(e.target) && !addNodeBtn.contains(e.target)) {
      addNodeMenu.classList.remove('open');
    }
    if (!legacyNodesMenu.contains(e.target) && !legacyNodesBtn.contains(e.target)) {
      legacyNodesMenu.classList.remove('open');
    }
    if (themeModeMenu && !themeModeMenu.contains(e.target) && !themeModeBtn.contains(e.target)) {
      themeModeMenu.classList.remove('open');
    }
  });

  addNodeMenu.querySelectorAll('.menu-item').forEach(item => {
    item.addEventListener('click', () => {
      addNodeFromMenu(item.dataset.type);
    });
  });

  legacyNodesMenu.querySelectorAll('.menu-item').forEach(item => {
    item.addEventListener('click', (event) => {
      addQuickAddChain(item.dataset.type, { createIsolatedSource: event.shiftKey });
    });
  });

  themeModeMenu?.querySelectorAll('.menu-item').forEach(item => {
    item.addEventListener('click', () => {
      void applyThemeMode(item.dataset.themeMode || 'auto');
      themeModeMenu.classList.remove('open');
    });
  });
}
