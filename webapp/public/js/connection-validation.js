import { portsCompatible } from './port-types.js';

function declaredPort(typeDef, direction, port) {
  const ports = Array.isArray(typeDef?.[direction]) ? typeDef[direction] : [];
  return ports.some((entry) => entry?.port === port);
}

export function isRestoredConnectionValid(conn, fromType, toType, nodeTypes) {
  if (!conn || !fromType || !toType) return false;
  const fromDef = nodeTypes?.[fromType];
  const toDef = nodeTypes?.[toType];
  if (!fromDef || !toDef) return false;
  if (!declaredPort(fromDef, 'outputs', conn.fromPort)) return false;
  if (!declaredPort(toDef, 'inputs', conn.toPort)) return false;
  return portsCompatible(conn.fromPort, conn.toPort);
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
