// Pure 2D rectangle helpers. No imports on purpose — this module must stay
// free of DOM/browser globals so it can be unit-tested directly under node.

export function normalizeRect(x1, y1, x2, y2) {
  return {
    x: Math.min(x1, x2),
    y: Math.min(y1, y2),
    w: Math.abs(x2 - x1),
    h: Math.abs(y2 - y1),
  };
}

// Axis-aligned overlap test (touching edges do not count as intersecting).
export function rectsIntersect(a, b) {
  return a.x < b.x + b.w && a.x + a.w > b.x &&
         a.y < b.y + b.h && a.y + a.h > b.y;
}

// bounds: [{ id, x, y, w, h }] in the same coordinate space as `rect`.
export function nodeIdsInRect(rect, bounds) {
  const out = [];
  for (const nb of bounds) {
    if (rectsIntersect(rect, nb)) out.push(nb.id);
  }
  return out;
}
