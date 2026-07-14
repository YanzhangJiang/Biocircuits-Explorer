import { portTypesCompatible, resolveNodePort } from './port-types.js';

function invalid(code, message, details = {}) {
  return { ok: false, code, message, ...details };
}

export function validateTypedConnection(conn, fromType, toType, nodeTypes) {
  if (!conn || typeof conn !== 'object') {
    return invalid('invalid-connection', 'Connection must be an object');
  }
  if (!fromType || !toType) {
    return invalid('unknown-node-type', 'Connection endpoint node type is missing');
  }
  const from = resolveNodePort(nodeTypes, fromType, 'output', conn.fromPort);
  if (!from) {
    return invalid(
      'invalid-output-port',
      `Unknown or untyped output ${fromType}.${String(conn.fromPort)}`,
    );
  }
  const to = resolveNodePort(nodeTypes, toType, 'input', conn.toPort);
  if (!to) {
    return invalid(
      'invalid-input-port',
      `Unknown or untyped input ${toType}.${String(conn.toPort)}`,
      { from },
    );
  }
  if (!portTypesCompatible(from.type, to.type)) {
    return invalid(
      'incompatible-port-types',
      `Port type mismatch: ${from.type} cannot connect to ${to.type}`,
      { from, to },
    );
  }
  return { ok: true, code: 'compatible', message: 'Compatible connection', from, to };
}

export function validateNodeConnection(conn, registry, nodeTypes) {
  if (!conn || typeof conn !== 'object') {
    return invalid('invalid-connection', 'Connection must be an object');
  }
  if (conn.fromNode === conn.toNode) {
    return invalid('self-connection', 'A node cannot connect to itself');
  }
  const fromType = registry?.[conn.fromNode]?.type;
  const toType = registry?.[conn.toNode]?.type;
  if (!fromType || !toType) {
    return invalid(
      'missing-endpoint',
      `Connection endpoint is missing: ${String(conn.fromNode)} -> ${String(conn.toNode)}`,
    );
  }
  return validateTypedConnection(conn, fromType, toType, nodeTypes);
}

export function assertNodeConnectionValid(conn, registry, nodeTypes) {
  const result = validateNodeConnection(conn, registry, nodeTypes);
  if (!result.ok) {
    const error = new Error(result.message);
    error.name = 'ConnectionContractError';
    error.code = result.code;
    error.validation = result;
    throw error;
  }
  return result;
}

export function isRestoredConnectionValid(conn, fromType, toType, nodeTypes) {
  return validateTypedConnection(conn, fromType, toType, nodeTypes).ok;
}

export function normalizeRestoredConnection(conn, idMap, savedTypeById, nodeTypes) {
  const fromNode = idMap?.[conn?.fromNode];
  const toNode = idMap?.[conn?.toNode];
  if (!fromNode || !toNode) return null;
  const fromType = savedTypeById?.[conn.fromNode];
  const toType = savedTypeById?.[conn.toNode];
  if (!isRestoredConnectionValid(conn, fromType, toType, nodeTypes)) return null;
  return {
    fromNode,
    fromPort: conn.fromPort,
    toNode,
    toPort: conn.toPort,
  };
}
