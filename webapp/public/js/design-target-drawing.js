// Authoring geometry survives numerical resampling. It is workspace data, not
// another optimizer input: its fingerprint binds it to the numerical target.
import { stableJson } from './stable-json.js';
import { resolveInput } from './design-target-adapters.js';

function fingerprint(target) {
  return stableJson({ inputs: target.inputs, outputs: target.outputs, samples: target.samples });
}

export function makeTargetDrawing(target, mode, points, authoredTarget = target) {
  return JSON.stringify({ version: 2, mode, fingerprint: fingerprint(target), points,
    outputRanges: authoredTarget.outputs.map(output => ({ min: output.min ?? 0, max: output.max ?? 1 })) });
}

export function readTargetDrawing(raw, target, mode) {
  if (!raw || !['curve', 'trajectory'].includes(mode)) return null;
  try {
    const drawing = JSON.parse(raw);
    if (![1, 2].includes(drawing.version) || drawing.mode !== mode || drawing.fingerprint !== fingerprint(target) ||
        !Array.isArray(drawing.points) || drawing.points.length < 2 || drawing.points.length > 4098 ||
        drawing.points.some(point => !point || ![point.x, point.y].every(value =>
          typeof value === 'number' && Number.isFinite(value) && value >= 0 && value <= 1))) return null;
    if (drawing.version === 2 && (!Array.isArray(drawing.outputRanges) || drawing.outputRanges.length !== target.outputs.length ||
        drawing.outputRanges.some(range => !range || ![range.min, range.max].every(value => typeof value === 'number' && Number.isFinite(value)) || range.max <= range.min))) return null;
    return drawing.points;
  } catch { return null; }
}

// The authored stroke is authoritative, including corners and travel order.
export function continuousDrawingPoints(points) {
  return points;
}

export function targetDrawingRows(raw, target) {
  const mode = target.outputs.length === 2 ? 'trajectory' : 'curve';
  if (target.inputs.length !== 1 || ![1, 2].includes(target.outputs.length)) return null;
  const saved = readTargetDrawing(raw, target, mode);
  if (!saved) return null;
  // The numerical request omits presentation ranges. Keep the original
  // normalized-to-physical mapping when binding a stroke to the owning run.
  const ranges = JSON.parse(raw).outputRanges || target.outputs;
  const points = continuousDrawingPoints(saved, mode);
  const distance = [0];
  for (let i = 1; i < points.length; i += 1) distance.push(distance.at(-1) +
    Math.hypot(points[i].x - points[i - 1].x, points[i].y - points[i - 1].y));
  if (mode === 'trajectory' && !distance.at(-1)) return null;
  const output = (value, dimension) => {
    const { min = 0, max = 1 } = ranges[dimension];
    return min + value * (max - min);
  };
  return points.map((point, index) => ({
    inputs: [resolveInput(target.inputs[0], mode === 'trajectory' ? distance[index] / distance.at(-1) : point.x)],
    outputs: mode === 'trajectory' ? [output(point.x, 0), output(point.y, 1)] : [output(point.y, 0)],
  }));
}
