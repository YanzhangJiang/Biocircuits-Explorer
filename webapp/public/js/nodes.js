// Biocircuits Explorer — Node CRUD, Discovery, Menu, Auto-Update & Observer Functions

import { nodeRegistry, connections, setConnections, nodeIdCounter, nextNodeId, setNodeIdCounter, canvasState, scale, plotResizeObservers, nodeResizeObservers, plotInteractionGuards, getPortColor } from './state.js';
import { showToast } from './api.js';
import { applyThemeMode } from './theme.js';
import { NODE_TYPES, PREREQ_CHAIN } from './node-types/index.js';
import { updateConnections } from './connections.js';
import { buildModel, triggerDownstreamNodes, getReactionsFromNode } from './model.js';
import { commitWorkspaceSnapshot, queueWorkspaceShellSync, getNodeSerialData } from './workspace.js';
import { refreshAtlasQueryDesigner } from './atlas.js';

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

export function getModelContextFromBuilder(modelBuilderNodeId) {
  const data = nodeRegistry[modelBuilderNodeId]?.data;
  if (!data?.modelContext || data.built === false || !data.modelContext.sessionId) {
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

function invalidateModelBuilder(modelBuilderNodeId) {
  const info = nodeRegistry[modelBuilderNodeId];
  if (!info) return;
  info.data = info.data || {};
  info.data.built = false;
  if (info.data.modelContext) {
    info.data.modelContext = {
      ...info.data.modelContext,
      sessionId: null,
    };
  }
}

// ===== Node CRUD =====

export function createNode(nodeType, x, y) {
  const typeDef = NODE_TYPES[nodeType];
  if (!typeDef) { console.error('Unknown node type:', nodeType); return null; }

  const id = nextNodeId();
  const nodeId = `node-${id}`;

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
  setConnections(connections.filter(c => c.fromNode !== nodeId && c.toNode !== nodeId));
  delete nodeRegistry[nodeId];
  cleanupNodeResizeObserver(nodeId);
  cleanupPlotResize(nodeId);
  updateConnections();
}

// ===== Node Loading State =====
export function setNodeLoading(nodeId, loading) {
  const el = document.getElementById(nodeId);
  if (!el) return;
  if (loading) {
    el.classList.add('loading');
    // Mark all input wires as transmitting
    const inputConns = connections.filter(c => c.toNode === nodeId);
    inputConns.forEach(conn => {
      const wireId = `wire-${conn.fromNode}-${conn.toNode}`;
      const wire = document.getElementById(wireId);
      if (wire) wire.classList.add('transmitting');
    });
  } else {
    el.classList.remove('loading');
    // Remove transmitting state from all input wires
    const inputConns = connections.filter(c => c.toNode === nodeId);
    inputConns.forEach(conn => {
      const wireId = `wire-${conn.fromNode}-${conn.toNode}`;
      const wire = document.getElementById(wireId);
      if (wire) wire.classList.remove('transmitting');
    });
  }
}

// ===== Auto-Chain Generation =====

// Find an existing chain ending with a model-builder that has a model output
export function findExistingModelBuilder() {
  for (const [id, info] of Object.entries(nodeRegistry)) {
    if (info.type === 'model-builder') {
      // Check if this model-builder is connected to a reaction-network
      const conn = connections.find(c => c.toNode === id && c.toPort === 'reactions');
      if (conn && nodeRegistry[conn.fromNode]?.type === 'reaction-network') {
        return { modelBuilderId: id, reactionNetworkId: conn.fromNode };
      }
    }
  }
  return null;
}

export function findExistingReactionNetwork() {
  for (const [id, info] of Object.entries(nodeRegistry)) {
    if (info.type === 'reaction-network') return id;
  }
  return null;
}

export function getNodePosition(nodeId) {
  const el = document.getElementById(nodeId);
  if (!el) return { x: 100, y: 150 };
  return { x: parseFloat(el.style.left) || 0, y: parseFloat(el.style.top) || 0 };
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

  createNode(nodeType, x, y);
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
    // No reaction network exists, create one
    rnId = createNode('reaction-network', 80, 150);
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

export function triggerConfigUpdate(nodeId, nodeType) {
  // Store config in node data
  const info = nodeRegistry[nodeId];
  if (!info) return;

  info.data = info.data || {};
  info.data.config = getNodeSerialData(nodeId, nodeType);
  if (nodeType === 'atlas-query-config') refreshAtlasQueryDesigner(nodeId);
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
export function triggerAutoModelBuild(reactionNodeId) {
  // Find all connected model-builder nodes
  const modelBuilders = connections
    .filter(c => c.fromNode === reactionNodeId && c.fromPort === 'reactions')
    .map(c => c.toNode);

  modelBuilders.forEach(mbId => {
    const info = nodeRegistry[mbId];
    if (info && info._autoBuildCheck) {
      invalidateModelBuilder(mbId);
      queueWorkspaceShellSync('model-dirty');
      // Debounce the build
      clearTimeout(info._autoBuildTimer);
      info._autoBuildTimer = setTimeout(() => {
        info._autoBuildCheck();
      }, 500);
    }
  });
}

export function triggerAllAutoModelBuilds() {
  Object.entries(nodeRegistry).forEach(([nodeId, info]) => {
    if (info.type !== 'model-builder' || !info._autoBuildCheck) return;
    info._autoBuildCheck();
  });
}

// ===== Execution =====

export function isRunnableNode(nodeId) {
  const info = nodeRegistry[nodeId];
  if (!info) return false;
  return typeof NODE_TYPES[info.type]?.execute === 'function';
}

export function connectedNodeIDs() {
  const ids = new Set();
  connections.forEach(conn => {
    ids.add(conn.fromNode);
    ids.add(conn.toNode);
  });
  return ids;
}

export async function runConnectedWorkspace() {
  const connectedIDs = connectedNodeIDs();
  if (!connectedIDs.size) {
    showToast('No connected nodes to run');
    return;
  }

  const runnableIDs = Array.from(connectedIDs).filter(isRunnableNode);
  if (!runnableIDs.length) {
    showToast('No connected executable nodes found');
    return;
  }

  const remaining = new Set(runnableIDs);
  let ranCount = 0;

  while (remaining.size) {
    let progressed = false;

    for (const nodeId of Array.from(remaining)) {
      const runnableDeps = connections
        .filter(conn => conn.toNode === nodeId)
        .map(conn => conn.fromNode)
        .filter(isRunnableNode);

      if (runnableDeps.some(depId => remaining.has(depId))) {
        continue;
      }

      remaining.delete(nodeId);
      progressed = true;

      const info = nodeRegistry[nodeId];
      if (!info) continue;

      try {
        if (info.type === 'model-builder' && getModelContextFromBuilder(nodeId)) {
          ranCount += 1;
          continue;
        }

        if (info.type === 'model-builder') {
          await buildModel(nodeId, { triggerDownstream: false, throwOnFailure: true });
          if (!getModelContextFromBuilder(nodeId)) {
            throw new Error('Model Builder did not produce a usable model');
          }
        } else {
          await NODE_TYPES[info.type].execute(nodeId);
        }
        ranCount += 1;
      } catch (error) {
        console.error(`Run Connected failed at ${nodeId} (${info.type})`, error);
        showToast(`Run Connected failed at ${NODE_TYPES[info.type]?.title || info.type}`);
        return;
      }
    }

    if (!progressed) {
      console.warn('Run Connected stopped due to unresolved dependency cycle', Array.from(remaining));
      break;
    }
  }

  commitWorkspaceSnapshot('run-connected');
  showToast(`Ran ${ranCount} connected node${ranCount === 1 ? '' : 's'}`);
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
