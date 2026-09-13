// All target entrances publish the same editable numerical artifact. Drawing
// and image decode are local; only the explicit Agent action sends a description.
import { escapeHtml } from './api.js';
import { ChangeAttrCommand, CompoundCommand, dispatch } from './commands.js';
import { nodeRegistry, getWorkspaceRuntimeEpoch } from './state.js';
import { DEFAULT_TARGET, validateDesignTarget, samplesFromCurve, samplesFromTrajectory, samplesFromImage, resolveInput } from './design-target-adapters.js';
import { continuousDrawingPoints, makeTargetDrawing, readTargetDrawing } from './design-target-drawing.js';
import { TARGET_PLOT, targetOutputRange as outputRange, targetPlotAxes, targetAxisValue, targetCoordinateNumber, targetPointerPoint, targetCoordinateReadout } from './design-target-coordinates.js';

const e = escapeHtml;
const copy = value => JSON.parse(JSON.stringify(value));
const el = (id, suffix) => document.getElementById(`${id}-${suffix}`);

function authoredPoints(nodeId, target, mode) {
  return readTargetDrawing(el(nodeId, 'drawing-json')?.value, target, mode);
}

function drawingEdit(nodeId, target, mode, points) {
  const control = el(nodeId, 'drawing-json');
  return new ChangeAttrCommand({ nodeId, key: `${nodeId}-drawing-json`, before: control?.value || '',
    after: points ? makeTargetDrawing(target, mode, points) : '' });
}

function referenceState(nodeId) {
  const raw = el(nodeId, 'reference-state')?.value;
  if (!raw) return null;
  let state;
  try { state = JSON.parse(raw); } catch { throw new Error('The saved pattern reference state is invalid. Choose another target entrance to reset it.'); }
  if (!state || typeof state.id !== 'string' || typeof state.name !== 'string' || typeof state.pending !== 'boolean') throw new Error('The saved pattern reference state is invalid.');
  return state;
}

export function readEditorTarget(nodeId, { allowPendingDescription = false, allowPendingReference = false } = {}) {
  if (!allowPendingReference && (referenceState(nodeId)?.pending || nodeRegistry[nodeId]?._designReferenceLoading)) {
    throw new Error('Trace the uploaded pattern in travel order before running. The image alone does not define an ordered single-input, two-output target.');
  }
  let target;
  try { target = JSON.parse(el(nodeId, 'target-json').value); }
  catch { throw new Error('The numerical target is not valid JSON. Correct it or choose an example.'); }
  const description = el(nodeId, 'description').value.trim();
  if (!allowPendingDescription && target.source === 'agent' && String(target.description ?? '').trim() !== description) {
    throw new Error('The description changed after Agent compilation. Compile it again, or choose a manual target entrance and edit its numerical samples.');
  }
  target.description = description;
  return validateDesignTarget(target);
}

// Older workspaces may retain the stroke beside a coarse training grid. A
// fresh run must fit the whole saved stroke, too; it cannot reuse that grid as
// a substitute for the user's target. Manually edited data invalidate its
// geometry fingerprint and remain authoritative.
export function readTrainingTarget(nodeId) {
  const target = readEditorTarget(nodeId);
  const mode = el(nodeId, 'target-mode').value;
  const points = authoredPoints(nodeId, target, mode);
  if (points) {
    target.samples = (mode === 'trajectory' ? samplesFromTrajectory : samplesFromCurve)(
      points, target.inputs, target.outputs, { count: Number(el(nodeId, 'target-points').value) });
  }
  return validateDesignTarget(target);
}

export function synchronizeTrainingTarget(nodeId) {
  const authored = readEditorTarget(nodeId);
  const target = readTrainingTarget(nodeId);
  if (JSON.stringify(authored.samples) === JSON.stringify(target.samples)) return;
  const mode = el(nodeId, 'target-mode').value;
  const points = authoredPoints(nodeId, authored, mode);
  const control = el(nodeId, 'target-json');
  dispatch(new CompoundCommand([
    new ChangeAttrCommand({ nodeId, key: control.id, before: control.value, after: JSON.stringify(target, null, 2) }),
    drawingEdit(nodeId, target, mode, points),
  ], 'Use complete drawn target'));
  refreshTargetEditor(nodeId);
}

function editorTarget(nodeId, options = {}) {
  return readEditorTarget(nodeId, { allowPendingReference: true, ...options });
}

function referenceMarkup(nodeId) {
  const state = referenceState(nodeId);
  const image = state && nodeRegistry[nodeId]?._designReferenceCache?.get(state.id);
  if (!image || el(nodeId, 'target-mode')?.value !== 'trajectory') return '';
  return `<image class="design-target-reference-image" href="${e(image)}" x="${TARGET_PLOT.left}" y="${TARGET_PLOT.top}" width="${TARGET_PLOT.dx}" height="${TARGET_PLOT.dy}" preserveAspectRatio="none" opacity="0.35" pointer-events="none"/>`;
}

export function replaceEditorTarget(nodeId, target, label = 'Edit design target') {
  const control = el(nodeId, 'target-json');
  const after = JSON.stringify(validateDesignTarget(target), null, 2);
  if (!control || control.value === after) return false;
  dispatch(new CompoundCommand([new ChangeAttrCommand({ nodeId, key: control.id, before: control.value, after })], label));
  return true;
}

function options(values, selected) {
  return values.map(value => `<option value="${e(value)}"${value === selected ? ' selected' : ''}>${e(value)}</option>`).join('');
}

export function renderTargetEditor(nodeId) {
  return `<div class="design-target-editor">
    <section class="node-panel" aria-label="Design goal">
    <div class="node-panel-header"><label class="node-panel-title" for="${nodeId}-description">Design goal</label></div>
    <div class="node-panel-body">
    <textarea id="${nodeId}-description" class="auto-update inverse-description" rows="2">${e(DEFAULT_TARGET.description)}</textarea>
    <button class="btn btn-small" data-action="compileInverseDesignTarget" data-node="${nodeId}">Compile with Design Agent</button>
    <div class="node-info" title="Compile a text goal with Design Agent, or define the target directly by drawing, image or data.">Compile a text goal, or define the target directly below.</div>
    </div>
    </section>
    <section class="node-panel" aria-label="Target entrance">
    <div class="node-panel-header"><label class="node-panel-title" for="${nodeId}-target-mode">Target entrance</label></div>
    <div class="node-panel-body">
    <div class="param-row design-target-entrance-row"><select id="${nodeId}-target-mode" class="auto-update" data-action="refreshInverseDesignTarget" data-node="${nodeId}"><option value="curve">Draw a response · 1 → 1</option><option value="image">Image field · 2 → 1</option><option value="trajectory">Pattern / trajectory · 1 → 2</option><option value="data">Numerical data · adjustable dimensions</option><option value="agent">Design Agent interpretation</option></select></div>
    <input id="${nodeId}-reference-state" class="auto-update" type="hidden" value="">
    <input id="${nodeId}-drawing-json" class="auto-update" type="hidden" value="">
    <div id="${nodeId}-target-image" hidden>
      <label class="inverse-field-label" for="${nodeId}-target-image-file">Upload a pattern image</label><input id="${nodeId}-target-image-file" type="file" accept="image/png,image/jpeg,image/webp,image/bmp">
      <div id="${nodeId}-target-image-help" class="node-info">Image columns and rows are the two inputs. Brightness is the output; the vertical axis increases upward. Transparent pixels use a white background.</div>
    </div>
    <div class="design-target-sampling">
      <div id="${nodeId}-image-resolution-row" class="param-row" hidden><label for="${nodeId}-image-resolution">Image samples per axis</label><input id="${nodeId}-image-resolution" class="auto-update" data-action="refreshInverseDesignTarget" data-node="${nodeId}" type="number" value="12" min="2" max="32" step="1"></div>
      <div id="${nodeId}-image-invert-row" class="param-row" hidden><label for="${nodeId}-image-invert">Image brightness</label><select id="${nodeId}-image-invert" class="auto-update" data-action="refreshInverseDesignTarget" data-node="${nodeId}"><option value="false">White → maximum</option><option value="true">Black → maximum</option></select></div>
    </div>
    </div>
    </section>
    <section class="node-panel node-panel--chart design-target-plot-panel" aria-label="Target preview">
    <div class="node-panel-header design-target-preview-header"><span class="node-panel-title">Target preview</span><span class="design-target-actions"><button id="${nodeId}-target-axis-settings" type="button" class="btn btn-small">Axes &amp; ranges</button><button class="btn btn-small" data-action="applyInverseDesignPreset" data-node="${nodeId}">Saturating example</button><button class="btn btn-small" data-action="clearInverseDesignDrawing" data-node="${nodeId}">Clear drawing</button></span></div>
    <div class="node-panel-body design-target-plot-body">
    <div class="design-target-coordinate-row"><output id="${nodeId}-target-coordinates" class="design-target-coordinates" aria-label="Plot coordinates" aria-live="off" title="Hover or draw to inspect X/Y. Arrow keys move the crosshair."></output></div>
    <div id="${nodeId}-target-drawing" class="design-target-drawing">
      <div class="design-target-canvas-wrap"><svg id="${nodeId}-target-canvas" class="design-target-canvas" viewBox="0 0 ${TARGET_PLOT.width} ${TARGET_PLOT.height}" tabindex="0" role="img" aria-label="Draw target response. Drag to draw; arrow keys inspect coordinates; use numerical data for keyboard editing."></svg></div>
    </div>
    <div id="${nodeId}-target-preview" class="design-target-preview"></div>
    <div id="${nodeId}-target-editor-status" class="inverse-note" role="status"></div>
    </div>
    </section>
    <div id="${nodeId}-target-roles"></div>
    <section class="node-panel" aria-label="Numerical target">
    <div class="node-panel-header"><span class="node-panel-title">Numerical target · all dimensions and samples</span></div>
    <div class="node-panel-body">
    <div id="${nodeId}-target-points-row" class="param-row"><label for="${nodeId}-target-points">Minimum curve resolution</label><input id="${nodeId}-target-points" class="auto-update" data-action="refreshInverseDesignTarget" data-node="${nodeId}" type="number" value="24" min="2" max="256" step="1"></div>
    <label class="inverse-field-label" for="${nodeId}-target-json">Editable target specification</label><textarea id="${nodeId}-target-json" class="auto-update design-target-json" rows="5" spellcheck="false">${e(JSON.stringify(DEFAULT_TARGET, null, 2))}</textarea>
    <div class="node-info" title="Every drawn vertex is included in training. Resolution adds evaluations without removing stroke details. Each sample defines positive input concentrations, desired outputs and a weight; validation_samples adds held-out evaluations.">Each sample lists positive inputs, desired outputs and a weight.</div>
    </div>
    </section>
  </div>`;
}

function normalizeInput(input, value) {
  return input.scale === 'log' ? Math.log(value / input.min) / Math.log(input.max / input.min) : (value - input.min) / (input.max - input.min);
}

function svgPreview(target, mode, points = null) {
  const { width, height, left, top, dx, dy } = TARGET_PLOT;
  const project = point => `${left + point.x * dx},${top + (1 - point.y) * dy}`;
  let plot = '';
  if (points) plot = `<polyline class="design-target-line" points="${continuousDrawingPoints(points, mode).map(project).join(' ')}"/>`;
  else if (mode === 'image' && target.inputs.length === 2 && target.outputs.length === 1) {
    const range = outputRange(target.outputs[0], target.samples, 0);
    const side = Math.max(3, Math.min(18, Math.sqrt(dx * dy / target.samples.length)));
    plot = target.samples.map(sample => {
      const x = normalizeInput(target.inputs[0], sample.inputs[0]), y = normalizeInput(target.inputs[1], sample.inputs[1]);
      const alpha = Math.max(0.04, Math.min(1, (sample.outputs[0] - range.min) / (range.max - range.min)));
      return `<rect class="design-target-cell" x="${left + x * dx - side / 2}" y="${top + (1 - y) * dy - side / 2}" width="${side}" height="${side}" opacity="${alpha}"><title>${e(sample.inputs.join(', '))}: ${e(sample.outputs[0])}</title></rect>`;
    }).join('');
  } else {
    const trajectory = mode === 'trajectory' && target.outputs.length >= 2;
    const ranges = target.outputs.map((output, index) => outputRange(output, target.samples, index));
    const normalized = target.samples.map(sample => ({
      x: trajectory ? (sample.outputs[0] - ranges[0].min) / (ranges[0].max - ranges[0].min) : normalizeInput(target.inputs[0], sample.inputs[0]),
      y: (sample.outputs[trajectory ? 1 : 0] - ranges[trajectory ? 1 : 0].min) / (ranges[trajectory ? 1 : 0].max - ranges[trajectory ? 1 : 0].min),
    }));
    const ordered = target.samples.map((sample, index) => ({ input: sample.inputs[0], point: normalized[index] }))
      .sort((a, b) => a.input - b.input).map(row => row.point);
    plot = target.inputs.length === 1
      ? `<polyline class="design-target-line" points="${ordered.map(project).join(' ')}"/>`
      : normalized.map((point, index) => {
        const [x, y] = project(point).split(',');
        return `<circle class="inverse-target-point" cx="${x}" cy="${y}" r="2.4"><title>Sample ${index + 1}: ${e(target.samples[index].outputs.join(', '))}</title></circle>`;
      }).join('');
  }
  const axes = targetPlotAxes(target, mode);
  const label = axis => `${axis.name}${axis.kind === 'input' ? ' total' : ''}${axis.scale === 'log' ? ' (log)' : axis.transform === 'log10' ? ' (log₁₀)' : ''}`;
  const ticks = [0, 0.25, 0.5, 0.75, 1].map(fraction => {
    const x = left + fraction * dx, y = top + (1 - fraction) * dy;
    return `<path class="design-target-grid" d="M${x} ${top}V${top + dy}M${left} ${y}H${left + dx}"/>
      <text class="design-target-tick" x="${x}" y="${top + dy + 16}" text-anchor="${fraction === 0 ? 'start' : fraction === 1 ? 'end' : 'middle'}">${e(targetCoordinateNumber(targetAxisValue(axes.x, fraction)))}</text>
      <text class="design-target-tick" x="${left - 7}" y="${y + 3}" text-anchor="end">${e(targetCoordinateNumber(targetAxisValue(axes.y, fraction)))}</text>`;
  }).join('');
  return `${ticks}<path class="inverse-chart-axis" d="M${left} ${top}V${top + dy}H${left + dx}"/>${plot}
    <text class="design-target-axis-label" x="${left + dx / 2}" y="${height - 8}" text-anchor="middle">${e(label(axes.x))}</text><text class="design-target-axis-label" x="12" y="${top + dy / 2}" transform="rotate(-90 12 ${top + dy / 2})" text-anchor="middle">${e(label(axes.y))}</text>
    <g class="design-target-cursor" hidden pointer-events="none"><path class="design-target-crosshair"/><rect class="design-target-crosshair-point" width="5" height="5"/><rect class="design-target-coordinate-bg" x="${width - 207}" y="${top + 4}" width="199" height="30" rx="3"/><text class="design-target-coordinate-label" x="${width - 201}" y="${top + 16}"></text></g>`;
}

function rolesHTML(target) {
  const field = (kind, index, key, label, value, choices = null) => {
    const attrs = `data-edit="${kind}" data-index="${index}" data-key="${key}"`;
    const control = choices ? `<select ${attrs}>${options(choices, String(value))}</select>`
      : `<input type="${['name', 'species'].includes(key) ? 'text' : 'number'}" ${attrs} value="${e(value)}"${['name', 'species'].includes(key) ? '' : ' step="any"'}${kind === 'input' && key === 'min' ? ' min="0.00000001"' : ''}>`;
    return `<label class="param-row">${label}${control}</label>`;
  };
  return `<section class="node-panel" aria-label="Input and output parameters"><div class="node-panel-header"><span class="node-panel-title">Input / output roles · ${target.inputs.length} → ${target.outputs.length}</span></div>
    <div class="node-panel-body">
    <div class="design-target-dimensions"><label>Inputs <select data-edit="input-count">${options(['1', '2', '3'], String(target.inputs.length))}</select></label><label>Outputs <select data-edit="output-count">${options(['1', '2', '3'], String(target.outputs.length))}</select></label></div>
    <div class="inverse-parameter-grid">
    ${target.inputs.map((input, index) => `<fieldset class="design-target-role"><legend>Input ${index + 1} · total concentration</legend>
      ${field('input', index, 'name', 'Name', input.name)}${field('input', index, 'min', 'Minimum', input.min)}${field('input', index, 'max', 'Maximum', input.max)}${field('input', index, 'scale', 'Scale', input.scale, ['linear', 'log'])}</fieldset>`).join('')}
    ${target.outputs.map((output, index) => { const range = outputRange(output, target.samples, index); return `<fieldset class="design-target-role"><legend>Output ${index + 1} · molecular readout</legend>
      ${field('output', index, 'name', 'Label', output.name)}${field('output', index, 'species', 'Species', output.species)}${field('output', index, 'transform', 'Transform', output.transform, ['linear', 'log10'])}${field('output', index, 'offset', 'Offset', output.offset ?? 0)}${field('output', index, 'min', 'Draw minimum', range.min)}${field('output', index, 'max', 'Draw maximum', range.max)}${field('output', index, 'optimize_offset', 'Fit offset', !!output.optimize_offset, ['false', 'true'])}</fieldset>`; }).join('')}
    </div>
    <div class="node-info" title="Species identify molecular concentrations; offsets apply after the transform. Input ranges rescale sample locations. Review the numerical target after adding dimensions.">Species identify molecular concentrations; offsets apply after the transform.</div>
    </div>
  </section>`;
}

export function refreshTargetEditor(nodeId, { redrawRoles = true } = {}) {
  const canvas = el(nodeId, 'target-canvas');
  if (!canvas) return;
  try {
    const target = editorTarget(nodeId), mode = el(nodeId, 'target-mode').value;
    nodeRegistry[nodeId]._designCoordinateTarget = target;
    const coordinates = el(nodeId, 'target-coordinates');
    if (coordinates) coordinates.textContent = '';
    const partial = target.inputs.length > 1 && mode !== 'image' || target.outputs.length > (mode === 'trajectory' ? 2 : 1);
    if (redrawRoles) {
      el(nodeId, 'target-roles').innerHTML = rolesHTML(target);
    }
    const reference = referenceState(nodeId);
    canvas.innerHTML = referenceMarkup(nodeId) + svgPreview(target, mode,
      reference?.pending && mode === 'trajectory' ? [] : authoredPoints(nodeId, target, mode));
    el(nodeId, 'target-drawing').hidden = ['image', 'data', 'agent'].includes(mode);
    el(nodeId, 'target-image').hidden = !['image', 'trajectory'].includes(mode);
    const imageHelp = el(nodeId, 'target-image-help');
    if (imageHelp) imageHelp.textContent = mode === 'trajectory' ? 'Upload a pattern as a local tracing reference, then draw one stroke in travel order. The input follows path length and the two outputs follow the image coordinates. The image spans the configured output ranges; pixel order is never treated as a trajectory.' : 'Image columns and rows are the two inputs. Brightness is the output; the vertical axis increases upward. Transparent pixels use a white background.';
    if (reference && mode === 'trajectory') {
      const available = nodeRegistry[nodeId]?._designReferenceCache?.has(reference.id);
      el(nodeId, 'target-editor-status').textContent = reference.pending ? (available ? `${reference.name}: trace an ordered path to define the target.` : `${reference.name}: pending trace. Re-upload the local reference image, then draw an ordered path.`) : (available ? `${reference.name}: the ordered trace defines the numerical target.` : 'Saved ordered trajectory ready. The local reference image is not stored; upload it again if you want to retrace.');
    }
    for (const suffix of ['target-points', 'image-resolution', 'image-invert']) {
      const row = el(nodeId, `${suffix}-row`);
      if (row) row.hidden = suffix === 'target-points' ? !['curve', 'trajectory'].includes(mode) : mode !== 'image';
    }
    el(nodeId, 'target-preview').innerHTML = reference?.pending && mode === 'trajectory' ? '<p class="text-dim inverse-note">No ordered target yet. Trace one path over the image; your stroke direction defines increasing input.</p>' : `<p class="inverse-note">${partial ? `Preview projects only ${e(target.inputs[0].name)} and ${e(target.outputs[0].name)}; other dimensions remain in the numerical target. ` : ''}${target.inputs.length} input${target.inputs.length === 1 ? '' : 's'} → ${target.outputs.length} output${target.outputs.length === 1 ? '' : 's'}${target.validation_samples?.length ? ' · validation included' : ''}</p>${['image', 'data', 'agent'].includes(mode) ? `<svg class="design-target-canvas" viewBox="0 0 ${TARGET_PLOT.width} ${TARGET_PLOT.height}" tabindex="0" role="img" aria-label="Numerical target preview. Hover or use arrow keys to inspect coordinates.">${svgPreview(target, mode)}</svg>` : ''}`;
  } catch (error) {
    el(nodeId, 'target-editor-status').textContent = error.message;
  }
}

export function resetTargetExample(nodeId) {
  const target = copy(DEFAULT_TARGET);
  target.description = el(nodeId, 'description').value.trim();
  const control = el(nodeId, 'target-json'), mode = el(nodeId, 'target-mode');
  const command = new CompoundCommand([
    new ChangeAttrCommand({ nodeId, key: mode.id, before: mode.value, after: 'curve' }),
    new ChangeAttrCommand({ nodeId, key: `${nodeId}-reference-state`, before: el(nodeId, 'reference-state')?.value || '', after: '' }),
    new ChangeAttrCommand({ nodeId, key: control.id, before: control.value, after: JSON.stringify(validateDesignTarget(target), null, 2) }),
    drawingEdit(nodeId, target, 'curve', null),
  ], 'Create saturating target example');
  for (const method of ['apply', 'revert']) {
    const perform = command[method].bind(command);
    command[method] = () => {
      const owner = nodeRegistry[nodeId];
      if (owner) owner._designModeMutation = true;
      try { perform(); } finally { if (owner) delete owner._designModeMutation; refreshTargetEditor(nodeId); }
    };
  }
  dispatch(command);
  refreshTargetEditor(nodeId);
}

export function clearTargetDrawing(nodeId) {
  const owner = nodeRegistry[nodeId];
  if (!owner) return;
  delete owner._designStrokeTicket;
  const canvas = el(nodeId, 'target-canvas');
  if (canvas) canvas.innerHTML = referenceMarkup(nodeId) + svgPreview(editorTarget(nodeId), el(nodeId, 'target-mode').value, []);
  el(nodeId, 'target-editor-status').textContent = 'Draw a new stroke to replace the target. Existing samples remain until the stroke is complete.';
}

function resizedRoles(target, inputCount, outputCount) {
  const inputs = Array.from({ length: inputCount }, (_, index) => target.inputs[index] || { name: ['X', 'Y', 'Z'][index], min: 0.05, max: 10, scale: 'log' });
  const outputs = Array.from({ length: outputCount }, (_, index) => target.outputs[index] || { name: ['response', 'output_y', 'output_z'][index], species: ['A', 'B', 'C'][index], transform: 'linear', offset: 0, optimize_offset: false, min: 0, max: 1 });
  const remap = sample => ({ ...sample, inputs: inputs.map((input, index) => sample.inputs[index] ?? resolveInput(input, 0.5)), outputs: outputs.map((_, index) => sample.outputs[index] ?? 0) });
  return { ...target, inputs, outputs, samples: target.samples.map(remap), ...(target.validation_samples ? { validation_samples: target.validation_samples.map(remap) } : {}) };
}

export function installTargetEditor(nodeId, { onInvalidate = () => {} } = {}) {
  const canvas = el(nodeId, 'target-canvas'), owner = nodeRegistry[nodeId];
  if (!canvas || !owner) return;
  const status = message => { if (nodeRegistry[nodeId] === owner) el(nodeId, 'target-editor-status').textContent = message; };
  const validOwner = epoch => nodeRegistry[nodeId] === owner && getWorkspaceRuntimeEpoch() === epoch;
  let cursor = { x: 0.5, y: 0.5 };
  const showAxes = () => {
    const control = el(nodeId, 'target-roles').querySelector('[data-key="min"]');
    control?.focus({ preventScroll: true });
    control?.scrollIntoView?.({ block: 'nearest' });
  };
  el(nodeId, 'target-axis-settings')?.addEventListener('click', showAxes);
  const inspect = (svg, point, keyboard = false) => {
    const target = owner._designCoordinateTarget;
    if (!svg || !target || nodeRegistry[nodeId] !== owner) return;
    cursor = point;
    const mode = el(nodeId, 'target-mode').value;
    const coordinates = el(nodeId, 'target-coordinates');
    if (coordinates) {
      coordinates.setAttribute?.('aria-live', keyboard ? 'polite' : 'off');
      coordinates.textContent = targetCoordinateReadout(target, mode, point);
    }
    const group = svg.querySelector('.design-target-cursor');
    if (!group) return;
    group.removeAttribute('hidden');
    const { left, top, dx, dy, width } = TARGET_PLOT;
    const x = left + point.x * dx, y = top + (1 - point.y) * dy;
    group.querySelector('path').setAttribute('d', `M${x} ${top}V${top + dy}M${left} ${y}H${left + dx}`);
    const marker = group.querySelector('.design-target-crosshair-point');
    marker.setAttribute('x', x - 2.5); marker.setAttribute('y', y - 2.5);
    const axes = targetPlotAxes(target, mode);
    group.querySelector('text').innerHTML = `<tspan x="${width - 201}">X = ${e(targetCoordinateNumber(targetAxisValue(axes.x, point.x)))}</tspan><tspan x="${width - 201}" dy="12">Y = ${e(targetCoordinateNumber(targetAxisValue(axes.y, point.y)))}</tspan>`;
  };
  const inspectKeys = (event, svg) => {
    const moves = { ArrowLeft: [-1, 0], ArrowRight: [1, 0], ArrowUp: [0, 1], ArrowDown: [0, -1] };
    if (!moves[event.key]) return;
    event.preventDefault(); event.stopPropagation();
    const move = moves[event.key], step = event.shiftKey ? 0.1 : 0.01;
    inspect(svg, { x: Math.max(0, Math.min(1, cursor.x + move[0] * step)), y: Math.max(0, Math.min(1, cursor.y + move[1] * step)) }, true);
  };
  canvas.addEventListener('keydown', event => inspectKeys(event, canvas));
  canvas.addEventListener('pointerleave', () => canvas.querySelector('.design-target-cursor')?.setAttribute('hidden', ''));
  const preview = el(nodeId, 'target-preview');
  preview.addEventListener('pointermove', event => {
    const svg = preview.querySelector('svg');
    if (svg) inspect(svg, targetPointerPoint(svg.getBoundingClientRect(), event));
  });
  preview.addEventListener('pointerleave', () => preview.querySelector('.design-target-cursor')?.setAttribute('hidden', ''));
  preview.addEventListener('keydown', event => inspectKeys(event, preview.querySelector('svg')));
  el(nodeId, 'reference-state')?.addEventListener('input', () => refreshTargetEditor(nodeId));
  el(nodeId, 'drawing-json')?.addEventListener('input', () => refreshTargetEditor(nodeId));
  el(nodeId, 'target-json').addEventListener('input', () => {
    delete owner._designEditorError;
    owner._designSourceImage = owner._designImageCache?.get(el(nodeId, 'target-json').value) || null;
    refreshTargetEditor(nodeId);
  });
  const rememberImage = data => {
    if (!owner._designImageCache) owner._designImageCache = new Map();
    owner._designImageCache.set(el(nodeId, 'target-json').value, data);
    while (owner._designImageCache.size > 8) owner._designImageCache.delete(owner._designImageCache.keys().next().value);
    owner._designSourceImage = data;
  };
  const sampling = ['target-points', 'image-resolution', 'image-invert'];
  owner._designSamplingValues = Object.fromEntries(sampling.map(suffix => [suffix, el(nodeId, suffix).value]));
  const replaceSampled = (target, suffix, label, points = null) => {
    const control = el(nodeId, 'target-json'), setting = el(nodeId, suffix);
    const command = new CompoundCommand([
      new ChangeAttrCommand({ nodeId, key: setting.id, before: owner._designSamplingValues[suffix], after: setting.value }),
      new ChangeAttrCommand({ nodeId, key: control.id, before: control.value, after: JSON.stringify(validateDesignTarget(target), null, 2) }),
      drawingEdit(nodeId, target, el(nodeId, 'target-mode').value, points),
    ], label);
    for (const method of ['apply', 'revert']) {
      const perform = command[method].bind(command);
      command[method] = () => {
        const current = nodeRegistry[nodeId];
        if (current) current._designSamplingMutation = true;
        try { perform(); } finally { if (current) { delete current._designSamplingMutation; current._designSamplingValues[suffix] = el(nodeId, suffix).value; } refreshTargetEditor(nodeId); }
      };
    }
    dispatch(command);
  };
  owner._designTargetMode = el(nodeId, 'target-mode').value;
  el(nodeId, 'target-mode').addEventListener('change', () => {
    const mode = el(nodeId, 'target-mode').value;
    if (owner._designModeMutation) { owner._designTargetMode = mode; refreshTargetEditor(nodeId); return; }
    try {
      const beforeMode = owner._designTargetMode;
      let target = editorTarget(nodeId, { allowPendingDescription: mode !== 'agent' });
      if (mode === 'curve') target = resizedRoles(target, 1, 1);
      if (mode === 'image') target = resizedRoles(target, 2, 1);
      if (mode === 'trajectory') target = resizedRoles(target, 1, 2);
      target.source = mode;
      const control = el(nodeId, 'target-json');
      const command = new CompoundCommand([
        new ChangeAttrCommand({ nodeId, key: el(nodeId, 'target-mode').id, before: beforeMode, after: mode }),
        new ChangeAttrCommand({ nodeId, key: `${nodeId}-reference-state`, before: el(nodeId, 'reference-state')?.value || '', after: mode === 'trajectory' ? el(nodeId, 'reference-state')?.value || '' : '' }),
        new ChangeAttrCommand({ nodeId, key: control.id, before: control.value, after: JSON.stringify(validateDesignTarget(target), null, 2) }),
        drawingEdit(nodeId, target, mode, null),
      ], 'Change design target entrance');
      for (const method of ['apply', 'revert']) {
        const perform = command[method].bind(command);
        command[method] = () => {
          const current = nodeRegistry[nodeId];
          if (current) current._designModeMutation = true;
          try { perform(); }
          finally { if (current) delete current._designModeMutation; refreshTargetEditor(nodeId); }
        };
      }
      dispatch(command);
      owner._designTargetMode = mode;
      status(mode === 'trajectory' ? 'Draw in travel order. Input u follows cumulative path length; the two outputs trace the path.' : 'Review the roles and replace or edit the target samples.');
    } catch (error) { status(error.message); }
  });
  el(nodeId, 'target-roles').addEventListener('change', event => {
    const control = event.target;
    if (!control.dataset.edit) return;
    onInvalidate('target-roles-changing');
    delete owner._designEditorError;
    try {
      let target = editorTarget(nodeId);
      const edit = control.dataset.edit;
      if (edit.endsWith('-count')) target = resizedRoles(target, edit === 'input-count' ? Number(control.value) : target.inputs.length, edit === 'output-count' ? Number(control.value) : target.outputs.length);
      else {
        const index = Number(control.dataset.index), key = control.dataset.key;
        const role = (edit === 'input' ? target.inputs : target.outputs)[index];
        const previous = { ...role };
        role[key] = control.type === 'number' ? Number(control.value) : key === 'optimize_offset' ? control.value === 'true' : control.value;
        if (edit === 'input' && ['min', 'max', 'scale'].includes(key)) {
          [...target.samples, ...(target.validation_samples || [])].forEach(sample => { sample.inputs[index] = resolveInput(role, normalizeInput(previous, sample.inputs[index])); });
        }
      }
      replaceEditorTarget(nodeId, target, 'Edit design input/output roles');
      status('Updated the numerical target. Review its preview before running.');
    } catch (error) { owner._designEditorError = error.message; status(error.message); }
  });
  let stroke = null;
  const point = event => targetPointerPoint(canvas.getBoundingClientRect(), event);
  canvas.addEventListener('pointerdown', event => {
    if (event.button !== 0) return;
    if (event.target.matches?.('.design-target-tick, .design-target-axis-label')) {
      event.stopPropagation(); event.preventDefault(); showAxes(); return;
    }
    const mode = el(nodeId, 'target-mode').value;
    if (!['curve', 'trajectory'].includes(mode)) return;
    event.stopPropagation(); event.preventDefault();
    canvas.focus?.({ preventScroll: true });
    inspect(canvas, point(event));
    onInvalidate('target-drawing-started');
    const ticket = {}; owner._designStrokeTicket = ticket;
    stroke = { points: [point(event)], pointerId: event.pointerId, ticket, epoch: getWorkspaceRuntimeEpoch(), fingerprint: el(nodeId, 'target-json').value, referenceFingerprint: el(nodeId, 'reference-state')?.value || '' };
    canvas.setPointerCapture?.(event.pointerId);
  });
  canvas.addEventListener('pointermove', event => {
    inspect(canvas, point(event));
    if (!stroke || event.pointerId !== stroke.pointerId || owner._designStrokeTicket !== stroke.ticket) return;
    event.stopPropagation();
    stroke.points.push(point(event));
    if (stroke.points.length > 4096) {
      stroke = null; delete owner._designStrokeTicket;
      canvas.releasePointerCapture?.(event.pointerId);
      status('This drawing exceeds the 4096-point training limit. The previous target is unchanged; no simplified target was submitted.');
      return;
    }
    try { canvas.innerHTML = referenceMarkup(nodeId) + svgPreview(editorTarget(nodeId), el(nodeId, 'target-mode').value, stroke.points); inspect(canvas, point(event)); }
    catch (error) { stroke = null; status(error.message); }
  });
  canvas.addEventListener('pointerup', event => {
    if (!stroke || event.pointerId !== stroke.pointerId) return;
    event.stopPropagation();
    const completed = stroke; stroke = null;
    if (owner._designStrokeTicket !== completed.ticket) return;
    delete owner._designStrokeTicket;
    completed.points.push(point(event));
    canvas.releasePointerCapture?.(event.pointerId);
    if (!validOwner(completed.epoch) || completed.fingerprint !== el(nodeId, 'target-json').value || completed.referenceFingerprint !== (el(nodeId, 'reference-state')?.value || '')) return;
    try {
      const target = editorTarget(nodeId), mode = el(nodeId, 'target-mode').value;
      const build = mode === 'trajectory' ? samplesFromTrajectory : samplesFromCurve;
      target.samples = build(completed.points, target.inputs, target.outputs, { count: Number(el(nodeId, 'target-points').value) });
      target.source = mode;
      delete target.validation_samples;
      const reference = referenceState(nodeId);
      if (mode === 'trajectory' && reference) {
        const control = el(nodeId, 'target-json'), referenceControl = el(nodeId, 'reference-state');
        dispatch(new CompoundCommand([
          new ChangeAttrCommand({ nodeId, key: control.id, before: control.value, after: JSON.stringify(validateDesignTarget(target), null, 2) }),
          new ChangeAttrCommand({ nodeId, key: referenceControl.id, before: referenceControl.value, after: JSON.stringify({ ...reference, pending: false }) }),
          drawingEdit(nodeId, target, mode, completed.points),
        ], 'Trace pattern into ordered target'));
      } else {
        const control = el(nodeId, 'target-json');
        dispatch(new CompoundCommand([
          new ChangeAttrCommand({ nodeId, key: control.id, before: control.value, after: JSON.stringify(validateDesignTarget(target), null, 2) }),
          drawingEdit(nodeId, target, mode, completed.points),
        ], 'Draw design target'));
      }
      status(mode === 'trajectory' ? 'Target trajectory saved. Every vertex is included in training, in travel order.' : 'Target curve saved. Every drawn vertex is included in training.');
      inspect(canvas, point(event));
    } catch (error) { status(error.message); refreshTargetEditor(nodeId); }
  });
  canvas.addEventListener('pointercancel', event => { if (stroke && event.pointerId === stroke.pointerId) { stroke = null; delete owner._designStrokeTicket; refreshTargetEditor(nodeId); } });
  el(nodeId, 'target-image-file').addEventListener('change', async event => {
    const file = event.target.files?.[0];
    if (!file) return;
    onInvalidate('target-image-loading');
    const epoch = getWorkspaceRuntimeEpoch(), fingerprint = el(nodeId, 'target-json').value, mode = el(nodeId, 'target-mode').value;
    const imageTicket = {}; owner._designImageTicket = imageTicket;
    if (mode === 'trajectory') owner._designReferenceLoading = imageTicket;
    let url;
    try {
      if (file.size > 20 * 1024 * 1024) throw new Error('Choose an image smaller than 20 MB.');
      const image = new Image(); url = URL.createObjectURL(file);
      const loaded = new Promise((resolve, reject) => { image.onload = resolve; image.onerror = () => reject(new Error('The image could not be decoded. Choose a PNG, JPEG, WebP or BMP.')); });
      image.src = url; await loaded;
      if (!validOwner(epoch) || owner._designImageTicket !== imageTicket || fingerprint !== el(nodeId, 'target-json').value || mode !== el(nodeId, 'target-mode').value) return;
      if (mode === 'trajectory') {
        const target = editorTarget(nodeId);
        if (target.inputs.length !== 1 || target.outputs.length !== 2) throw new Error('A pattern trajectory needs one input and two output roles.');
        const scratch = document.createElement('canvas');
        const width = image.naturalWidth || 1024, height = image.naturalHeight || 1024;
        const scale = Math.min(1, 1024 / Math.max(width, height));
        scratch.width = Math.max(1, Math.round(width * scale)); scratch.height = Math.max(1, Math.round(height * scale));
        scratch.getContext('2d').drawImage(image, 0, 0, scratch.width, scratch.height);
        const dataUrl = scratch.toDataURL('image/png');
        const id = `${Date.now()}-${Math.random().toString(36).slice(2)}`;
        if (!owner._designReferenceCache) owner._designReferenceCache = new Map();
        owner._designReferenceCache.set(id, dataUrl);
        while (owner._designReferenceCache.size > 8) owner._designReferenceCache.delete(owner._designReferenceCache.keys().next().value);
        const control = el(nodeId, 'reference-state');
        dispatch(new CompoundCommand([new ChangeAttrCommand({ nodeId, key: control.id, before: control.value, after: JSON.stringify({ id, name: file.name, pending: true }) })], 'Load pattern tracing reference'));
        refreshTargetEditor(nodeId);
        status(`${file.name}: trace a single path in travel order. The image itself is a local reference, not an ordered target.`);
        return;
      }
      const resolution = Number(el(nodeId, 'image-resolution').value);
      if (!Number.isInteger(resolution) || resolution < 2 || resolution > 32) throw new Error('Image sampling must use 2 to 32 points per axis.');
      const target = editorTarget(nodeId);
      const scratch = document.createElement('canvas'); scratch.width = 64; scratch.height = 64;
      const context = scratch.getContext('2d'); context.drawImage(image, 0, 0, 64, 64);
      const imageData = context.getImageData(0, 0, 64, 64);
      target.samples = samplesFromImage(imageData, target.inputs, target.outputs, { columns: resolution, rows: resolution, invert: el(nodeId, 'image-invert').value === 'true' });
      target.source = 'image'; delete target.validation_samples;
      replaceEditorTarget(nodeId, target, 'Import image design target');
      rememberImage(imageData);
      status(`${file.name}: ${resolution} × ${resolution} brightness samples. The decoded image stays on this device.`);
    } catch (error) { if (validOwner(epoch) && owner._designImageTicket === imageTicket) status(error.message); }
    finally { if (owner._designReferenceLoading === imageTicket) delete owner._designReferenceLoading; if (url) URL.revokeObjectURL(url); if (owner._designImageTicket === imageTicket) delete owner._designImageTicket; event.target.value = ''; }
  });
  el(nodeId, 'target-points').addEventListener('change', () => {
    if (owner._designSamplingMutation) return;
    try {
      const target = editorTarget(nodeId), mode = el(nodeId, 'target-mode').value;
      if (!['curve', 'trajectory'].includes(mode)) return;
      if (referenceState(nodeId)?.pending) throw new Error('Trace the uploaded pattern before resampling its trajectory.');
      const trajectory = mode === 'trajectory';
      const ranges = target.outputs.map((output, index) => outputRange(output, target.samples, index));
      if (trajectory && ranges.length !== 2) throw new Error('A trajectory needs one input and two outputs.');
      const points = authoredPoints(nodeId, target, mode) || target.samples.map(sample => ({
        x: trajectory ? (sample.outputs[0] - ranges[0].min) / (ranges[0].max - ranges[0].min) : normalizeInput(target.inputs[0], sample.inputs[0]),
        y: (sample.outputs[trajectory ? 1 : 0] - ranges[trajectory ? 1 : 0].min) / (ranges[trajectory ? 1 : 0].max - ranges[trajectory ? 1 : 0].min),
      }));
      target.samples = (trajectory ? samplesFromTrajectory : samplesFromCurve)(points, target.inputs, target.outputs, { count: Number(el(nodeId, 'target-points').value) });
      replaceSampled(target, 'target-points', 'Resample drawn target', points);
      status('Numerical evaluation updated. The target curve is unchanged.');
    } catch (error) { status(error.message); }
  });
  for (const suffix of ['image-resolution', 'image-invert']) el(nodeId, suffix).addEventListener('change', () => {
    if (owner._designSamplingMutation || el(nodeId, 'target-mode').value !== 'image') return;
    try {
      const target = editorTarget(nodeId);
      if (!owner._designSourceImage) { status('Upload the original image to apply the new image sampling settings. Saved numerical targets remain unchanged.'); return; }
      const resolution = Number(el(nodeId, 'image-resolution').value);
      if (!Number.isInteger(resolution) || resolution < 2 || resolution > 32) throw new Error('Image sampling must use 2 to 32 points per axis.');
      const sourceImage = owner._designSourceImage;
      target.samples = samplesFromImage(sourceImage, target.inputs, target.outputs, { columns: resolution, rows: resolution, invert: el(nodeId, 'image-invert').value === 'true' });
      replaceSampled(target, suffix, 'Resample image target');
      rememberImage(sourceImage);
      status('Updated brightness samples from the uploaded image.');
    } catch (error) { status(error.message); }
  });
  refreshTargetEditor(nodeId);
}
