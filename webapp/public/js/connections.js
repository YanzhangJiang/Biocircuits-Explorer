// Biocircuits Explorer — Connection Drawing & Socket Wiring
import { canvasState, wiringState, connections, setConnections, nodeRegistry, scale, getPortColor } from './state.js';
import { showToast } from './api.js';
import { NODE_TYPES } from './node-types/index.js';
import { hasModelContextForNode } from './nodes.js';
import { getReactionsFromNode } from './model.js';

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
    path.setAttribute('data-port-type', conn.fromPort);
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
        // Validate port type compatibility
        if (fromPort === toPort) {
          const fromNode = fromSocket.dataset.node;
          const toNode = toSocket.dataset.node;
          // No self-connections
          if (fromNode !== toNode) {
            // Remove existing connection to this input (one input = one wire)
            setConnections(connections.filter(c => !(c.toNode === toNode && c.toPort === toPort)));
            connections.push({ fromNode, fromPort, toNode, toPort });
            updateConnections();

            // Auto-populate config nodes when connected
            const toNodeInfo = nodeRegistry[toNode];
            if (toNodeInfo && toNodeInfo.type) {
              const typeDef = NODE_TYPES[toNodeInfo.type];
              if (typeDef && typeDef.execute) {
                // Execute the node to populate dropdowns/options
                // Check if we have the necessary data before executing
                const shouldExecute =
                  (toPort === 'model' && hasModelContextForNode(toNode)) || // Has model data
                  (toPort === 'reactions' && getReactionsFromNode(fromNode).reactions.length > 0) || // Has reactions data
                  (toPort === 'params'); // Params connection

                if (shouldExecute) {
                  setTimeout(() => {
                    typeDef.execute(toNode).catch(e => {
                      console.error(`Failed to auto-populate ${toNode}:`, e);
                    });
                  }, 100);
                }
              }
            }
          }
        } else {
          showToast(`Port mismatch: ${fromPort} ≠ ${toPort}`);
        }
      }
    }
  });
}
