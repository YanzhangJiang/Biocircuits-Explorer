// Pure alignment + distribution math for the alignment toolbar. No imports
// (DOM-free) so it can be unit-tested directly. Inputs are node bounds
// [{ id, x, y, w, h }]; outputs are per-id offsets { id: { dx, dy } } that
// the caller turns into MoveNodeCommands.

function selectionBounds(nodes) {
  let minX = Infinity, minY = Infinity, maxR = -Infinity, maxB = -Infinity;
  for (const n of nodes) {
    minX = Math.min(minX, n.x);
    minY = Math.min(minY, n.y);
    maxR = Math.max(maxR, n.x + n.w);
    maxB = Math.max(maxB, n.y + n.h);
  }
  return { minX, minY, maxR, maxB };
}

// modes: left | right | top | bottom | center-h | center-v
//   center-h aligns horizontal center x to the selection's bounding-box
//   center; center-v aligns center y. Needs 2+ nodes.
export function alignOffsets(nodes, mode) {
  if (!nodes || nodes.length < 2) return {};
  const b = selectionBounds(nodes);
  const cx = (b.minX + b.maxR) / 2;
  const cy = (b.minY + b.maxB) / 2;
  const out = {};
  for (const n of nodes) {
    let dx = 0, dy = 0;
    switch (mode) {
      case 'left':     dx = b.minX - n.x; break;
      case 'right':    dx = b.maxR - (n.x + n.w); break;
      case 'top':      dy = b.minY - n.y; break;
      case 'bottom':   dy = b.maxB - (n.y + n.h); break;
      case 'center-h': dx = cx - (n.x + n.w / 2); break;
      case 'center-v': dy = cy - (n.y + n.h / 2); break;
      default: break;
    }
    out[n.id] = { dx, dy };
  }
  return out;
}

// dir: 'h' | 'v'. Distributes nodes so the gaps between adjacent nodes are
// equal, keeping the two extreme nodes fixed. Needs 3+ nodes (with fewer
// there is nothing between the ends to distribute).
export function distributeOffsets(nodes, dir) {
  if (!nodes || nodes.length < 3) return {};
  const out = {};
  if (dir === 'h') {
    const sorted = [...nodes].sort((a, b) => (a.x + a.w / 2) - (b.x + b.w / 2));
    const totalW = sorted.reduce((s, n) => s + n.w, 0);
    const last = sorted[sorted.length - 1];
    const span = (last.x + last.w) - sorted[0].x;
    const gap = (span - totalW) / (sorted.length - 1);
    let cursor = sorted[0].x;
    for (const n of sorted) {
      out[n.id] = { dx: cursor - n.x, dy: 0 };
      cursor += n.w + gap;
    }
  } else {
    const sorted = [...nodes].sort((a, b) => (a.y + a.h / 2) - (b.y + b.h / 2));
    const totalH = sorted.reduce((s, n) => s + n.h, 0);
    const last = sorted[sorted.length - 1];
    const span = (last.y + last.h) - sorted[0].y;
    const gap = (span - totalH) / (sorted.length - 1);
    let cursor = sorted[0].y;
    for (const n of sorted) {
      out[n.id] = { dx: 0, dy: cursor - n.y };
      cursor += n.h + gap;
    }
  }
  return out;
}
