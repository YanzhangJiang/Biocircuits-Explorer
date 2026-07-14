// Biocircuits Explorer — Node CRUD, Discovery, Menu, Auto-Update & Observer Functions

import { nodeRegistry, connections, nodeIdCounter, nextNodeId, setNodeIdCounter, canvasState, scale, plotResizeObservers, nodeResizeObservers, plotInteractionGuards, getPortColor } from './state.js';
import { showToast } from './api.js';
import { applyThemeMode } from './theme.js';
import { NODE_TYPES } from './node-types/index.js';
import { updateConnections } from './connections.js';
import { buildModel, triggerDownstreamNodes, getReactionsFromNode } from './model.js';
import { commitWorkspaceSnapshot, queueWorkspaceShellSync, getNodeSerialData } from './workspace.js';
import { refreshAtlasQueryDesigner } from './atlas.js';
import { dispatch, record, CreateNodeCommand, RemoveNodeCommand, ChangeAttrCommand } from './commands.js';
import {
  invalidateModelBuildersForReactionSource,
  modelInputRevision,
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
  const data = nodeRegistry[modelBuilderNodeId]?.data;
  if (!data?.modelContext || data.built === false || !data.modelContext.sessionId) {
    return null;
  }
  if (data.modelContext.builtForRevision != null &&
      data.modelContext.builtForRevision !== modelInputRevision(modelBuilderNodeId)) {
    return null;
  }
  return data.modelContext;
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

export function createNode(nodeType, x, y, opts = {}) {
  const typeDef = NODE_TYPES[nodeType];
  if (!typeDef) { console.error('Unknown node type:', nodeType); return null; }

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
  return type === 'reaction-network' || type === 'network-id-definition' ||
         type === 'design-target';
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
    for (const [id, info] of Object.entries(nodeRegistry)) {
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
export function addQuickAddChain(chainType) {
  closeDropdown();

  if (chainType === 'atlas-preview') {
    const specX = 80;
    const specY = resolveOverlap(specX, 150, 420, 620, null);
    const specId = createNode('atlas-spec', specX, specY);
    const builderX = specX + 480;
    const builderY = resolveOverlap(builderX, specY, 460, 480, null);
    const builderId = createNode('atlas-builder', builderX, builderY);

    connections.push({ fromNode: specId, fromPort: 'atlas-spec', toNode: builderId, toPort: 'atlas-spec' });
    updateConnections();
    return;
  }

  if (chainType === 'atlas-search' || chainType === 'atlas-workflow') {
    const specX = 80;
    const specY = resolveOverlap(specX, 150, 420, 620, null);
    const specId = createNode('atlas-spec', specX, specY);
    const builderX = specX + 480;
    const builderY = resolveOverlap(builderX, specY, 460, 480, null);
    const builderId = createNode('atlas-builder', builderX, builderY);
    const queryX = builderX + 520;
    const queryY = resolveOverlap(queryX, specY, 420, 700, null);
    const queryId = createNode('atlas-query-config', queryX, queryY);
    const resultX = queryX + 460;
    const resultY = resolveOverlap(resultX, queryY, 640, 540, null);
    const resultId = createNode('atlas-query-result', resultX, resultY);

    connections.push({ fromNode: specId, fromPort: 'atlas-spec', toNode: builderId, toPort: 'atlas-spec' });
    connections.push({ fromNode: builderId, fromPort: 'atlas', toNode: resultId, toPort: 'atlas' });
    connections.push({ fromNode: queryId, fromPort: 'atlas-query', toNode: resultId, toPort: 'atlas-query' });
    updateConnections();
    return;
  }

  if (chainType === 'atlas-inverse-design') {
    const specX = 80;
    const specY = resolveOverlap(specX, 150, 420, 620, null);
    const specId = createNode('atlas-spec', specX, specY);
    const queryX = specX + 480;
    const queryY = resolveOverlap(queryX, specY, 420, 700, null);
    const queryId = createNode('atlas-query-config', queryX, queryY);
    const resultX = queryX + 480;
    const resultY = resolveOverlap(resultX, queryY, 700, 620, null);
    const resultId = createNode('atlas-inverse-result', resultX, resultY);

    connections.push({ fromNode: specId, fromPort: 'atlas-spec', toNode: resultId, toPort: 'atlas-spec' });
    connections.push({ fromNode: queryId, fromPort: 'atlas-query', toNode: resultId, toPort: 'atlas-query' });
    updateConnections();
    return;
  }

  // Map legacy node types to their new chain equivalents
  const chainMap = {
    'siso-analysis': { params: 'siso-params', result: 'siso-result' },
    'rop-cloud': { params: 'rop-cloud-params', result: 'rop-cloud-result' },
    'fret-heatmap': { params: 'fret-params', result: 'fret-result' },
    'parameter-scan-1d': { params: 'scan-1d-params', result: 'scan-1d-result' },
    'parameter-scan-2d': { params: 'scan-2d-params', result: 'scan-2d-result' },
    'rop-polyhedron': { params: 'rop-poly-params', result: 'rop-poly-result' },
    'parameter-placer': { params: 'placer-params', result: 'placer-result' },
  };

  const chain = chainMap[chainType];
  if (!chain) {
    console.error('Unknown quick add chain type:', chainType);
    return;
  }

  // Check for existing nodes and reuse them
  let rnId = findExistingReactionNetwork();
  let mbId = null;
  let createdModelBuilder = false;

  if (!rnId) {
    // No reaction network exists, create one. Walk Y down to avoid landing
    // on top of an existing node at the default (80, 150) anchor.
    const safeY = resolveOverlap(80, 150, 280, 300, null);
    rnId = createNode('reaction-network', 80, safeY);
  }

  // Check for existing model-builder connected to this reaction network
  const existing = findExistingModelBuilder();
  if (existing && existing.reactionNetworkId === rnId) {
    mbId = existing.modelBuilderId;
  } else {
    // Create model-builder and connect to reaction network
    const rnPos = getNodePosition(rnId);
    const rnSize = getNodeSize(rnId);
    const mbX = rnPos.x + rnSize.w + 60;
    const mbY = resolveOverlap(mbX, rnPos.y, 260, 200, null);
    mbId = createNode('model-builder', mbX, mbY);
    createdModelBuilder = true;
    connections.push({ fromNode: rnId, fromPort: 'reactions', toNode: mbId, toPort: 'reactions' });
  }

  // Create params and result nodes
  const mbPos = getNodePosition(mbId);
  const mbSize = getNodeSize(mbId);
  const paramsX = mbPos.x + mbSize.w + 60;
  const nDownstream = countDownstreamViewers(mbId);
  const paramsY = resolveOverlap(paramsX, mbPos.y + nDownstream * 50, 320, 300, null);
  const paramsId = createNode(chain.params, paramsX, paramsY);

  const paramsSize = getNodeSize(paramsId);
  const resultX = paramsX + paramsSize.w + 60;
  const resultY = resolveOverlap(resultX, paramsY, 420, 300, null);
  const resultId = createNode(chain.result, resultX, resultY);

  // Connect them
  connections.push({ fromNode: mbId, fromPort: 'model', toNode: paramsId, toPort: 'model' });
  connections.push({ fromNode: paramsId, fromPort: 'params', toNode: resultId, toPort: 'params' });

  // Special case: ROP cloud params also needs reactions connection
  if (chain.params === 'rop-cloud-params') {
    connections.push({ fromNode: rnId, fromPort: 'reactions', toNode: paramsId, toPort: 'reactions' });
  }

  updateConnections();

  const modelBuilderInfo = nodeRegistry[mbId];
  if ((createdModelBuilder || !getModelContextFromBuilder(mbId)) && modelBuilderInfo?._autoBuildCheck) {
    setTimeout(() => {
      modelBuilderInfo._autoBuildCheck();
    }, 100);
  }

  // Auto-populate the params node if model data is available
  const paramsTypeDef = NODE_TYPES[chain.params];
  if (paramsTypeDef && paramsTypeDef.execute) {
    // Check if we have model data or reactions data
    const hasModelData = hasModelContextForNode(paramsId);
    const hasReactionsData = chain.params === 'rop-cloud-params'; // ROP cloud uses reactions

    if (hasModelData || hasReactionsData) {
      setTimeout(() => {
        paramsTypeDef.execute(paramsId).catch(e => {
          console.error(`Failed to auto-populate ${paramsId}:`, e);
        });
      }, 100);
    }
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
    item.addEventListener('click', () => {
      addQuickAddChain(item.dataset.type);
    });
  });

  themeModeMenu?.querySelectorAll('.menu-item').forEach(item => {
    item.addEventListener('click', () => {
      void applyThemeMode(item.dataset.themeMode || 'auto');
      themeModeMenu.classList.remove('open');
    });
  });
}
