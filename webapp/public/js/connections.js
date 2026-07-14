// Biocircuits Explorer — Connection Drawing & Socket Wiring
import { canvasState, wiringState, connections, setConnections, nodeRegistry, scale, getPortColor } from './state.js';
import { showToast } from './api.js';
import { NODE_TYPES } from './node-types/index.js';
import { hasModelContextForNode } from './nodes.js';
import { getReactionsFromNode } from './model.js';
import {
  invalidateBuildersForConnectionChange,
  replaceConnectionsWithModelInvalidation,
} from './model-lifecycle.js';
import {
  invalidateAtlasExecutionsForConnectionChange,
  invalidateScanExecutionsForConnectionChange,
} from './execution-lifecycle.js';
import { record, SetConnectionsCommand } from './commands.js';
import { PORT_TYPES, resolveNodePort } from './port-types.js';
import {
  assertNodeConnectionValid,
  validateNodeConnection,
} from './connection-validation.js';

const CONFIG_PORT_TYPES = new Set([
  PORT_TYPES.SISOConfig,
  PORT_TYPES.Scan1DConfig,
  PORT_TYPES.Scan2DConfig,
  PORT_TYPES.ROPCloudConfig,
  PORT_TYPES.FRETConfig,
  PORT_TYPES.ROPPolyhedronConfig,
  PORT_TYPES.ParameterPlacerConfig,
]);

// ===== Connection mutators (command performers) =====

// Add one wire, honoring the one-input-one-wire rule: any existing wire on
// the same input socket is evicted and returned so a command can restore it
// on undo. Returns the replaced connection or null.
export function addConnection(conn) {
  assertNodeConnectionValid(conn, nodeRegistry, NODE_TYPES);
  let replaced = null;
  const existing = connections.find(c => c.toNode === conn.toNode && c.toPort === conn.toPort);
  const next = connections.filter(c => c !== existing);
  if (existing) {
    replaced = { ...existing };
  }
  next.push({ ...conn });
  replaceConnectionsWithModelInvalidation(next, 'connection-added-or-rewired');
  updateConnections();
  return replaced;
}

export function removeConnection(conn) {
  replaceConnectionsWithModelInvalidation(connections.filter(c => !(
    c.fromNode === conn.fromNode && c.fromPort === conn.fromPort &&
    c.toNode === conn.toNode && c.toPort === conn.toPort)), 'connection-removed');
  updateConnections();
}

// Replace the whole connection set (backs SetConnectionsCommand).
export function replaceConnections(arr) {
  for (const conn of arr || []) {
    assertNodeConnectionValid(conn, nodeRegistry, NODE_TYPES);
  }
  replaceConnectionsWithModelInvalidation(arr, 'connection-set-replaced');
  updateConnections();
}

function connectionsEqual(a, b) {
  if (a.length !== b.length) return false;
  const keys = list => list.map(conn => [
    conn?.fromNode, conn?.fromPort, conn?.toNode, conn?.toPort,
  ].map(value => String(value ?? '')).join('\u0000')).sort();
  return JSON.stringify(keys(a)) === JSON.stringify(keys(b));
}

export function finalizeInteractiveConnectionChange(before) {
  if (!before || connectionsEqual(before, connections)) return false;
  invalidateBuildersForConnectionChange(before, connections, 'interactive-connection-gesture');
  invalidateScanExecutionsForConnectionChange(before, connections, 'interactive-connection-gesture');
  invalidateAtlasExecutionsForConnectionChange(before, connections, 'interactive-connection-gesture');
  return true;
}

// Module-level DOM refs, set by initSocketEvents()
let svgLayer = null;

// RAF throttle for updateConnections during drag/resize
let _updateConnectionsRAF = null;

export function scheduleUpdateConnections() {
  if (_updateConnectionsRAF) return;
  _updateConnectionsRAF = requestAnimationFrame(() => {
    _updateConnectionsRAF = null;
    updateConnections();
  });
}

function getEditorRect() {
  return document.getElementById('editor')?.getBoundingClientRect() || null;
}

function clientToWorld(clientX, clientY) {
  const rect = getEditorRect();
  if (!rect) return { x: 0, y: 0 };
  return {
    x: (clientX - rect.left - canvasState.panX) / scale,
    y: (clientY - rect.top - canvasState.panY) / scale,
  };
}

// ===== Connection Drawing =====
export function getSocketCenter(socket) {
  const rect = socket.getBoundingClientRect();
  return clientToWorld(rect.left + rect.width / 2, rect.top + rect.height / 2);
}

export function bezierPath(x1, y1, x2, y2) {
  const dx = Math.abs(x2 - x1) * 0.5;
  return `M ${x1} ${y1} C ${x1 + dx} ${y1}, ${x2 - dx} ${y2}, ${x2} ${y2}`;
}

export function updateConnections() {
  if (!svgLayer || !svgLayer.isConnected) {
    svgLayer = document.getElementById('svg-layer');
  }
  if (!svgLayer) return;

  // Store transmitting state before removing wires
  const transmittingWires = new Set();
  svgLayer.querySelectorAll('.wire.connected.transmitting').forEach(w => {
    const id = w.getAttribute('id');
    if (id) transmittingWires.add(id);
  });

  svgLayer.querySelectorAll('.wire.connected').forEach(w => w.remove());
  // Reset all socket connected state
  document.querySelectorAll('.socket.connected').forEach(s => s.classList.remove('connected'));
  connections.forEach(conn => {
    const fromSocket = document.querySelector(`#${conn.fromNode} .socket.output[data-port="${conn.fromPort}"]`);
    const toSocket = document.querySelector(`#${conn.toNode} .socket.input[data-port="${conn.toPort}"]`);
    if (!fromSocket || !toSocket) return;
    fromSocket.classList.add('connected');
    toSocket.classList.add('connected');
    const from = getSocketCenter(fromSocket);
    const to = getSocketCenter(toSocket);
    const path = document.createElementNS('http://www.w3.org/2000/svg', 'path');
    const wireId = `wire-${conn.fromNode}-${conn.toNode}`;
    path.classList.add('wire', 'connected');
    path.setAttribute('id', wireId);
    path.setAttribute('d', bezierPath(from.x, from.y, to.x, to.y));
    const fromNodeType = nodeRegistry[conn.fromNode]?.type;
    const resolvedPort = resolveNodePort(NODE_TYPES, fromNodeType, 'output', conn.fromPort);
    if (!resolvedPort) return;
    path.setAttribute('data-port-type', resolvedPort.type);
    path.style.stroke = getPortColor(conn.fromPort);

    // Restore transmitting state if it was active
    if (transmittingWires.has(wireId)) {
      path.classList.add('transmitting');
    }

    svgLayer.appendChild(path);
  });
}

// ===== Socket Wiring Events =====
export function initSocketEvents() {
  svgLayer = document.getElementById('svg-layer');

  // ===== Socket Wiring =====
  document.addEventListener('mousedown', (e) => {
    const socket = e.target.closest('.socket');
    if (!socket || e.button !== 0) return;

    // Snapshot the connection set before the gesture begins. mouseup records
    // the net change (connect / detach / rewire) as a single undo step.
    wiringState.connSnapshotBefore = connections.map(c => ({ ...c }));

    if (socket.classList.contains('output')) {
      // Start wiring from output
      wiringState.isWiring = true;
      wiringState.wireStartSocket = socket;
      wiringState.wireStartIsOutput = true;
      const sr = getSocketCenter(socket);
      wiringState.tempWire = document.createElementNS('http://www.w3.org/2000/svg', 'path');
      wiringState.tempWire.classList.add('wire', 'active');
      wiringState.tempWire.style.stroke = getPortColor(socket.dataset.port);
      wiringState.tempWire.setAttribute('d', bezierPath(sr.x, sr.y, sr.x, sr.y));
      svgLayer.appendChild(wiringState.tempWire);
      e.preventDefault();
      e.stopPropagation();
    } else if (socket.classList.contains('input')) {
      const nodeId = socket.dataset.node;
      const port = socket.dataset.port;
      const existing = connections.find(c => c.toNode === nodeId && c.toPort === port);

      if (existing) {
        // Disconnect existing wire and start re-dragging from the output end
        // This is a transient drag state. Invalidate only once on mouseup from
        // the gesture's original graph to its final graph, so dragging a wire
        // back to the same socket remains a semantic no-op.
        setConnections(connections.filter(c => c !== existing));
        updateConnections();
        // Start wiring from the original output socket
        const fromSocket = document.querySelector(`#${existing.fromNode} .socket.output[data-port="${existing.fromPort}"]`);
        if (fromSocket) {
          wiringState.isWiring = true;
          wiringState.wireStartSocket = fromSocket;
          wiringState.wireStartIsOutput = true;
          const sr = getSocketCenter(fromSocket);
          wiringState.tempWire = document.createElementNS('http://www.w3.org/2000/svg', 'path');
          wiringState.tempWire.classList.add('wire', 'active');
          wiringState.tempWire.style.stroke = getPortColor(fromSocket.dataset.port);
          wiringState.tempWire.setAttribute('d', bezierPath(sr.x, sr.y, sr.x, sr.y));
          svgLayer.appendChild(wiringState.tempWire);
        }
      } else {
        // No existing connection, start wiring from input
        wiringState.isWiring = true;
        wiringState.wireStartSocket = socket;
        wiringState.wireStartIsOutput = false;
        const sr = getSocketCenter(socket);
        wiringState.tempWire = document.createElementNS('http://www.w3.org/2000/svg', 'path');
        wiringState.tempWire.classList.add('wire', 'active');
        wiringState.tempWire.style.stroke = getPortColor(socket.dataset.port);
        wiringState.tempWire.setAttribute('d', bezierPath(sr.x, sr.y, sr.x, sr.y));
        svgLayer.appendChild(wiringState.tempWire);
      }
      e.preventDefault();
      e.stopPropagation();
    }
  });

  document.addEventListener('mouseup', (e) => {
    if (!wiringState.isWiring || !wiringState.wireStartSocket) return;
    const before = wiringState.connSnapshotBefore;
    const socket = e.target.closest('.socket');
    if (socket && socket !== wiringState.wireStartSocket) {
      let fromSocket, toSocket;
      if (wiringState.wireStartIsOutput && socket.classList.contains('input')) {
        fromSocket = wiringState.wireStartSocket;
        toSocket = socket;
      } else if (!wiringState.wireStartIsOutput && socket.classList.contains('output')) {
        fromSocket = socket;
        toSocket = wiringState.wireStartSocket;
      }
      if (fromSocket && toSocket) {
        const fromPort = fromSocket.dataset.port;
        const toPort = toSocket.dataset.port;
        const fromNode = fromSocket.dataset.node;
        const toNode = toSocket.dataset.node;
        const candidate = { fromNode, fromPort, toNode, toPort };
        // Validate through the canonical node-type + direction + port resolver.
        const validation = validateNodeConnection(candidate, nodeRegistry, NODE_TYPES);
        if (validation.ok) {
          // No self-connections
          if (fromNode !== toNode) {
            // Remove existing connection to this input (one input = one wire)
            const next = connections.filter(c => !(c.toNode === toNode && c.toPort === toPort));
            next.push({ fromNode, fromPort, toNode, toPort });
            setConnections(next);
            updateConnections();

            // Auto-populate config nodes when connected
            const toNodeInfo = nodeRegistry[toNode];
            if (toNodeInfo && toNodeInfo.type) {
              const typeDef = NODE_TYPES[toNodeInfo.type];
              if (typeDef && typeDef.prepare) {
                // Preparation may populate UI/config only; connecting a wire
                // must never launch scientific computation.
                // Check if we have the necessary data before executing
                const toType = validation.to.type;
                const shouldExecute =
                  (toType === PORT_TYPES.ModelArtifact && hasModelContextForNode(toNode)) || // Has model data
                  (toType === PORT_TYPES.NetworkIR && getReactionsFromNode(fromNode).reactions.length > 0) || // Has reactions data
                  CONFIG_PORT_TYPES.has(toType); // Typed config connection

                if (shouldExecute) {
                  setTimeout(() => {
                    typeDef.prepare(toNode).catch(e => {
                      console.error(`Failed to auto-populate ${toNode}:`, e);
                    });
                  }, 100);
                }
              }
            }
          }
        } else {
          showToast(validation.message);
        }
      }
    }

    // Record the net change for undo. Covers every gesture outcome — a new
    // wire, a detach that dropped on empty space, or a rewire — because we
    // diff the whole set against the pre-gesture snapshot. Runs before
    // canvas.js clears wiringState (document listeners fire before window).
    if (finalizeInteractiveConnectionChange(before)) {
      record(new SetConnectionsCommand({ before, after: connections }));
    }
    wiringState.connSnapshotBefore = null;
  });
}
