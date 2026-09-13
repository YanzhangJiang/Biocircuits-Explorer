// Biocircuits Explorer — Per-Node-Type Minimum Sizes
//
// Single authority for how small a node may become. Consumed by nodes.js
// (createNode pins inline min-width/min-height so every restore/legacy path is
// clamped by CSS) and by canvas.js (the resize drag clamp). Kept as a
// zero-dependency leaf so both can import it without cycles.
//
// Per-type minimum sizes (px), derived from content: min height must cover
// node chrome (header 36 + body padding 24 = 60) + 36px per socket row
// (inputs + outputs) + the smallest content block that stays usable
// (plot area >= ~280x180, one form row ~43, a button row ~38).
export const FALLBACK_MIN_NODE_SIZE = Object.freeze({ width: 240, height: 140 });
export const NODE_MIN_SIZES = Object.freeze({
  'markdown-note': Object.freeze({ width: 280, height: 220 }),
  'ai-import': Object.freeze({ width: 320, height: 310 }),
  'reaction-network': Object.freeze({ width: 240, height: 200 }),
  'network-id-definition': Object.freeze({ width: 240, height: 240 }),
  'model-builder': Object.freeze({ width: 240, height: 190 }),
  'atlas-builder': Object.freeze({ width: 420, height: 390 }),
  'siso-params': Object.freeze({ width: 300, height: 360 }),
  'siso-result': Object.freeze({ width: 400, height: 470 }),
  'qk-poly-result': Object.freeze({ width: 400, height: 440 }),
  'siso-analysis': Object.freeze({ width: 400, height: 480 }),
  'scan-1d-params': Object.freeze({ width: 300, height: 420 }),
  'scan-2d-params': Object.freeze({ width: 300, height: 520 }),
  'scan-1d-result': Object.freeze({ width: 400, height: 440 }),
  'scan-2d-result': Object.freeze({ width: 460, height: 440 }),
  'parameter-scan-1d': Object.freeze({ width: 340, height: 560 }),
  'parameter-scan-2d': Object.freeze({ width: 340, height: 620 }),
  'placer-params': Object.freeze({ width: 310, height: 310 }),
  'placer-result': Object.freeze({ width: 420, height: 480 }),
  'design-spec-config': Object.freeze({ width: 420, height: 520 }),
  'design-target': Object.freeze({ width: 420, height: 490 }),
  'inverse-design-target': Object.freeze({ width: 460, height: 750 }),
  'gradient-design': Object.freeze({ width: 480, height: 680 }),
  'designed-network': Object.freeze({ width: 400, height: 380 }),
  'rop-cloud-params': Object.freeze({ width: 300, height: 430 }),
  'rop-cloud-result': Object.freeze({ width: 420, height: 520 }),
  'rop-cloud': Object.freeze({ width: 340, height: 560 }),
  'fret-params': Object.freeze({ width: 300, height: 270 }),
  'fret-result': Object.freeze({ width: 400, height: 440 }),
  'fret-heatmap': Object.freeze({ width: 400, height: 480 }),
  'rop-poly-params': Object.freeze({ width: 300, height: 480 }),
  'rop-poly-result': Object.freeze({ width: 460, height: 440 }),
  'rop-polyhedron': Object.freeze({ width: 340, height: 560 }),
  'rop-shape-edit-config': Object.freeze({ width: 420, height: 560 }),
  'rop-shape-result': Object.freeze({ width: 420, height: 410 }),
  'atlas-spec': Object.freeze({ width: 400, height: 440 }),
  'atlas-query-config': Object.freeze({ width: 400, height: 410 }),
  'atlas-query-result': Object.freeze({ width: 460, height: 530 }),
  'atlas-inverse-result': Object.freeze({ width: 460, height: 500 }),
  'model-summary': Object.freeze({ width: 380, height: 300 }),
  'vertices-table': Object.freeze({ width: 380, height: 340 }),
  'regime-graph': Object.freeze({ width: 420, height: 460 }),
  'sbml-import': Object.freeze({ width: 300, height: 320 }),
  'sbml-export': Object.freeze({ width: 240, height: 190 }),
});

export function nodeMinSize(nodeType) {
  return NODE_MIN_SIZES[nodeType] || FALLBACK_MIN_NODE_SIZE;
}

export function clampNodeSize(nodeType, width, height) {
  const min = nodeMinSize(nodeType);
  return {
    width: Number.isFinite(width) ? Math.max(min.width, width) : min.width,
    height: Number.isFinite(height) ? Math.max(min.height, height) : min.height,
  };
}
