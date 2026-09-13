// One coordinate mapping for ticks, pointer inspection, drawing and image traces.
export const TARGET_PLOT = Object.freeze({ width: 480, height: 250, left: 64, top: 18, dx: 398, dy: 186 });

export function targetOutputRange(output, samples, index) {
  const values = samples.map(sample => sample.outputs[index]);
  const min = output.min ?? Math.min(0, ...values);
  const max = output.max ?? Math.max(1, ...values);
  return { min, max: max > min ? max : min + 1 };
}

export function targetPlotAxes(target, mode) {
  const input = index => ({ ...target.inputs[index], kind: 'input' });
  const output = index => ({ ...target.outputs[index], ...targetOutputRange(target.outputs[index], target.samples, index), scale: 'linear', kind: 'output' });
  const trajectory = mode === 'trajectory' && target.outputs.length >= 2;
  return { x: trajectory ? output(0) : input(0), y: mode === 'image' && target.inputs.length >= 2 ? input(1) : output(trajectory ? 1 : 0) };
}

export function targetAxisValue(axis, fraction) {
  return axis.scale === 'log'
    ? 10 ** ((1 - fraction) * Math.log10(axis.min) + fraction * Math.log10(axis.max))
    : (1 - fraction) * axis.min + fraction * axis.max;
}

export function targetCoordinateNumber(value) {
  if (!Number.isFinite(value)) return '—';
  return value !== 0 && (Math.abs(value) < 0.001 || Math.abs(value) >= 10000)
    ? value.toExponential(2) : String(Number(value.toPrecision(4)));
}

export function targetPointerPoint(box, event) {
  const { width, height, left, top, dx, dy } = TARGET_PLOT;
  const bound = value => value <= 1e-12 ? 0 : value >= 1 - 1e-12 ? 1 : value;
  return { x: bound(((event.clientX - box.left) / box.width * width - left) / dx),
    y: bound(1 - ((event.clientY - box.top) / box.height * height - top) / dy) };
}

export function targetCoordinateReadout(target, mode, point) {
  const { x, y } = targetPlotAxes(target, mode);
  return `X · ${x.name} = ${targetCoordinateNumber(targetAxisValue(x, point.x))}   Y · ${y.name} = ${targetCoordinateNumber(targetAxisValue(y, point.y))}`;
}
