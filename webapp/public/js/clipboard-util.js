// Pure clipboard remapping helpers (no DOM/imports) so the paste id/wire
// remapping is unit-testable. workspace.js handles the live bits (snapshot,
// id allocation, dispatch).

// Keep only wires whose BOTH endpoints are within the snapshot set; external
// wires can't be reproduced without their other node.
export function internalConnections(snapshots) {
  const ids = new Set(snapshots.map((s) => s.id));
  const seen = new Set();
  const out = [];
  for (const s of snapshots) {
    for (const c of (s.connections || [])) {
      if (!ids.has(c.fromNode) || !ids.has(c.toNode)) continue;
      const key = `${c.fromNode}:${c.fromPort}->${c.toNode}:${c.toPort}`;
      if (seen.has(key)) continue;
      seen.add(key);
      out.push(c);
    }
  }
  return out;
}

// Produce new snapshots with remapped ids (old->new via idMap), offset
// position, and only the internal connections remapped. Each new snapshot
// carries just its own incident internal wires (restoreNode dedups across
// the pair).
export function remapSnapshots(sourceSnapshots, idMap, offset) {
  const conns = internalConnections(sourceSnapshots);
  return sourceSnapshots.map((s) => ({
    id: idMap[s.id],
    type: s.type,
    x: s.x + offset,
    y: s.y + offset,
    width: s.width,
    height: s.height,
    data: s.data,
    connections: conns
      .filter((c) => c.fromNode === s.id || c.toNode === s.id)
      .map((c) => ({
        fromNode: idMap[c.fromNode], fromPort: c.fromPort,
        toNode: idMap[c.toNode], toPort: c.toPort,
      })),
  }));
}
