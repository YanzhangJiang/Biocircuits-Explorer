import assert from 'node:assert/strict';
import { makeTargetDrawing } from '../public/js/design-target-drawing.js';
import { TARGET_PLOT, targetPlotAxes, targetAxisValue, targetCoordinateReadout } from '../public/js/design-target-coordinates.js';
import test, { beforeEach } from 'node:test';

const elements = new Map();
const imageLoads = [];
const objectUrls = new Map();
const revokedUrls = [];
let urlNumber = 0;
let invalidations = [];

function element(id, value = '', tagName = 'DIV') {
  const listeners = new Map();
  const captures = new Set();
  return {
    id, value, tagName, type: tagName === 'SELECT' ? 'select-one' : '',
    innerHTML: '', textContent: '', hidden: false, dataset: {}, files: [], captures,
    querySelector() { return null; },
    focus() { document.activeElement = this; },
    addEventListener(type, listener) {
      if (!listeners.has(type)) listeners.set(type, []);
      listeners.get(type).push(listener);
    },
    dispatchEvent(event) {
      for (const listener of listeners.get(event.type) || []) listener(event);
      return true;
    },
    emit(type, detail = {}) {
      const event = { type, target: this, button: 0, pointerId: 1, stopPropagation() {}, preventDefault() {}, ...detail };
      return Promise.all((listeners.get(type) || []).map(listener => listener(event)));
    },
    getBoundingClientRect: () => ({ left: 100, top: 50, width: 800, height: 400 }),
    setPointerCapture: pointerId => captures.add(pointerId),
    releasePointerCapture: pointerId => captures.delete(pointerId),
    hasPointerCapture: pointerId => captures.has(pointerId),
  };
}

globalThis.window = {
  matchMedia: () => null, addEventListener() {}, dispatchEvent() {},
  location: { protocol: 'http:', hostname: 'localhost', port: '8000' },
  sessionStorage: { getItem: () => null, setItem() {} },
  localStorage: { getItem: () => null, setItem() {} },
};
globalThis.document = {
  getElementById: id => elements.get(id) ?? null,
  createElement(tagName) {
    if (tagName !== 'canvas') return element('', '', tagName.toUpperCase());
    const scratch = { width: 1, height: 1 };
    let image;
    scratch.getContext = () => ({
      drawImage(value) { image = value; },
      getImageData() {
        const data = new Uint8ClampedArray(scratch.width * scratch.height * 4);
        for (let index = 0; index < data.length; index += 4) {
          data.set([image.file.brightness, image.file.brightness, image.file.brightness, 255], index);
        }
        return { width: scratch.width, height: scratch.height, data };
      },
    });
    scratch.toDataURL = () => `data:image/png;base64,${Buffer.from([image.file.brightness]).toString('base64')}`;
    return scratch;
  },
};
globalThis.Image = class {
  set src(url) { this.file = objectUrls.get(url); this.naturalWidth = 64; this.naturalHeight = 64; imageLoads.push(this); }
};
URL.createObjectURL = file => {
  const url = `blob:target-editor-${++urlNumber}`;
  objectUrls.set(url, file);
  return url;
};
URL.revokeObjectURL = url => { revokedUrls.push(url); objectUrls.delete(url); };

const {
  readEditorTarget, readTrainingTarget, synchronizeTrainingTarget, replaceEditorTarget, renderTargetEditor, installTargetEditor,
  refreshTargetEditor, clearTargetDrawing,
} = await import('../public/js/design-target-editor.js');
const { DEFAULT_TARGET, validateDesignTarget } = await import('../public/js/design-target-adapters.js');
const { nodeRegistry, advanceWorkspaceRuntimeEpoch } = await import('../public/js/state.js');
const { registerPerformers, undoStack, undo, redo } = await import('../public/js/commands.js');

const control = suffix => elements.get(`target-${suffix}`);
const pointer = (x, y, pointerId = 1) => ({ clientX: 100 + 800 * (TARGET_PLOT.left + TARGET_PLOT.dx * x) / TARGET_PLOT.width,
  clientY: 50 + 400 * (TARGET_PLOT.top + TARGET_PLOT.dy * (1 - y)) / TARGET_PLOT.height, pointerId });
const near = (actual, expected) => assert.ok(Math.abs(actual - expected) < 1e-10, `${actual} differs from ${expected}`);

function resetHarness(saved = {}) {
  elements.clear(); imageLoads.length = 0; objectUrls.clear(); revokedUrls.length = 0;
  invalidations = [];
  Object.keys(nodeRegistry).forEach(key => delete nodeRegistry[key]);
  nodeRegistry.target = { type: 'inverse-design-target', data: {} };
  undoStack.clear();
  for (const suffix of ['target-canvas', 'target-roles', 'target-editor-status', 'target-drawing', 'target-image', 'target-preview', 'target-points-row', 'image-resolution-row', 'image-invert-row', 'target-coordinates', 'target-axis-settings']) {
    elements.set(`target-${suffix}`, element(`target-${suffix}`));
  }
  for (const [suffix, value, tagName] of [
    ['description', DEFAULT_TARGET.description, 'TEXTAREA'],
    ['target-json', JSON.stringify(DEFAULT_TARGET, null, 2), 'TEXTAREA'],
    ['target-mode', 'curve', 'SELECT'], ['target-points', '3', 'INPUT'],
    ['image-resolution', '2', 'INPUT'], ['image-invert', 'false', 'SELECT'],
    ['target-image-file', '', 'INPUT'], ['reference-state', '', 'INPUT'], ['drawing-json', '', 'INPUT'],
  ]) elements.set(`target-${suffix}`, element(`target-${suffix}`, saved[suffix] ?? value, tagName));
  // Match the production setNodeAttr value-event contract so command apply,
  // Undo and Redo all invoke the real target-editor listeners.
  registerPerformers({
    setAttr(_id, key, value) {
      const field = elements.get(key);
      field.value = value;
      if (field.tagName !== 'SELECT') field.dispatchEvent({ type: 'input', target: field });
      field.dispatchEvent({ type: 'change', target: field });
    },
    afterChange() {},
  });
  installTargetEditor('target', { onInvalidate: reason => invalidations.push(reason) });
}

beforeEach(() => resetHarness());

test('coordinate inspection uses physical linear/log and XY output axes without editing the target', async () => {
  const target = structuredClone(DEFAULT_TARGET);
  target.inputs[0] = { name: 'u', min: 1, max: 100, scale: 'log' };
  target.outputs[0] = { ...target.outputs[0], min: -2, max: 6 };
  near(targetAxisValue(targetPlotAxes(target, 'curve').x, 0.5), 10);
  assert.equal(targetCoordinateReadout(target, 'curve', { x: 0.5, y: 0.75 }), 'X · u = 10   Y · response = 4');
  target.outputs.push({ ...target.outputs[0], name: 'vertical', min: 10, max: 30 });
  assert.equal(targetCoordinateReadout(target, 'trajectory', { x: 0.25, y: 0.75 }), 'X · response = 0   Y · vertical = 25');
  target.inputs.push({ name: 'v', min: 2, max: 6, scale: 'linear' });
  assert.equal(targetCoordinateReadout(target, 'image', { x: 0.5, y: 0.75 }), 'X · u = 10   Y · v = 5');
  const original = control('target-json').value;
  await control('target-canvas').emit('pointermove', pointer(0.5, 0.75));
  assert.match(control('target-coordinates').textContent, /X · X = 0.7071.*Y · response = 0.75/);
  await control('target-canvas').emit('keydown', { key: 'ArrowUp' });
  assert.match(control('target-coordinates').textContent, /Y · response = 0.76/);
  assert.equal(control('target-json').value, original);
  assert.deepEqual(invalidations, []);
});

test('the authored curve survives release, evaluation resampling, Undo and workspace restoration as a line', async () => {
  const canvas = control('target-canvas');
  await canvas.emit('pointerdown', pointer(0, 0.1));
  for (const [x, y] of [[0.15, 0.2], [0.25, 0.85], [0.35, 0.3], [0.65, 0.7], [0.85, 0.8]]) {
    await canvas.emit('pointermove', pointer(x, y));
  }
  await canvas.emit('pointerup', pointer(1, 0.9));
  const original = JSON.parse(control('drawing-json').value).points;
  const line = canvas.innerHTML;
  assert.equal(original.length, 7);
  const originalTraining = readEditorTarget('target').samples;
  assert.ok(originalTraining.length >= original.length);
  for (const vertex of original) assert.ok(originalTraining.some(sample => Math.abs(sample.outputs[0] - vertex.y) < 1e-12));
  assert.match(line, /polyline class="design-target-line"/);
  assert.doesNotMatch(line, /<circle/);
  control('target-points').value = '9';
  await control('target-points').emit('change');
  assert.ok(readEditorTarget('target').samples.length >= 9);
  for (const vertex of original) assert.ok(readEditorTarget('target').samples.some(sample => Math.abs(sample.outputs[0] - vertex.y) < 1e-12));
  assert.deepEqual(JSON.parse(control('drawing-json').value).points, original);
  assert.equal(canvas.innerHTML, line);
  undo();
  assert.deepEqual(readEditorTarget('target').samples, originalTraining);
  assert.equal(canvas.innerHTML, line);
  redo();
  const saved = savedEditorFields();
  resetHarness(saved);
  assert.deepEqual(JSON.parse(control('drawing-json').value).points, original);
  assert.equal(control('target-canvas').innerHTML, line);
});

test('an edited numerical target cannot display an obsolete authored curve', async () => {
  const canvas = control('target-canvas');
  await canvas.emit('pointerdown', pointer(0, 0.2));
  await canvas.emit('pointermove', pointer(0.4, 0.9));
  await canvas.emit('pointerup', pointer(1, 0.3));
  const original = canvas.innerHTML;
  const target = readEditorTarget('target');
  target.samples.forEach(sample => { sample.outputs[0] = 0.5; });
  replaceEditorTarget('target', target);
  assert.notEqual(canvas.innerHTML, original);
  assert.doesNotMatch(canvas.innerHTML, /<circle/);
  undo();
  assert.equal(canvas.innerHTML, original);
});

test('fresh optimization of an older saved stroke includes its original details, and manual data edits take precedence', () => {
  const target = readEditorTarget('target');
  target.samples = [target.samples[0], target.samples.at(-1)];
  target.samples.forEach(sample => { sample.outputs[0] = 0; });
  control('target-json').value = JSON.stringify(target);
  const original = [{ x: 0, y: 0 }, { x: 0.123, y: 1 }, { x: 1, y: 0 }];
  control('drawing-json').value = makeTargetDrawing(target, 'curve', original);
  assert.equal(readEditorTarget('target').samples.length, 2);
  assert.ok(readTrainingTarget('target').samples.some(sample => sample.outputs[0] === 1), 'The saved spike must participate in a new optimization');
  assert.deepEqual(JSON.parse(control('drawing-json').value).points, original);
  synchronizeTrainingTarget('target');
  assert.ok(readEditorTarget('target').samples.some(sample => sample.outputs[0] === 1), 'The editable numerical target must agree with the training request');
  undo();
  assert.deepEqual(readEditorTarget('target').samples, target.samples);
  assert.deepEqual(JSON.parse(control('drawing-json').value).points, original);
  target.samples.forEach(sample => { sample.outputs[0] = 0.2; });
  control('target-json').value = JSON.stringify(target);
  assert.deepEqual(readTrainingTarget('target').samples, target.samples, 'An explicitly edited numerical target cannot be overridden by stale geometry');
});

function savedEditorFields() {
  return Object.fromEntries([...elements].filter(([, field]) => ['INPUT', 'TEXTAREA', 'SELECT'].includes(field.tagName)).map(([id, field]) => [id.slice('target-'.length), field.value]));
}

async function importReference(name, brightness) {
  const fileControl = control('target-image-file');
  fileControl.files = [{ name, size: 100, brightness }];
  const pending = fileControl.emit('change');
  imageLoads.at(-1).onload();
  await pending;
}

async function chooseMode(mode) {
  control('target-mode').value = mode;
  await control('target-mode').emit('change');
  undoStack.clear();
}

async function editRole(edit, index, key, value, type = 'text') {
  await control('target-roles').emit('change', { target: { dataset: { edit, index: String(index), key }, value: String(value), type } });
}

test('target editor offers optional Agent, drawing, image and adjustable numerical routes without reaction-list controls', () => {
  const html = renderTargetEditor('target');
  for (const label of ['Design goal', 'Compile with Design Agent', 'Draw a response', 'Image field', 'Pattern / trajectory', 'Numerical data', 'Editable target specification']) {
    assert.ok(html.includes(label), label);
  }
  assert.match(html, /for="target-target-json"/);
  assert.match(html, /use numerical data for keyboard editing/);
  assert.doesNotMatch(html, /candidate-reactions|Candidate reactions|reaction list/i);
  assert.match(control('target-roles').innerHTML, /Input \/ output roles · 1 → 1/);
});

test('sampling controls follow the chosen entrance while preserving their saved values', async () => {
  assert.equal(control('target-points-row').hidden, false);
  assert.equal(control('image-resolution-row').hidden, true);
  assert.equal(control('image-invert-row').hidden, true);
  await chooseMode('image');
  assert.equal(control('target-points-row').hidden, true);
  assert.equal(control('image-resolution-row').hidden, false);
  assert.equal(control('image-invert-row').hidden, false);
  await chooseMode('data');
  assert.equal(control('target-points-row').hidden, true);
  assert.equal(control('image-resolution-row').hidden, true);
  assert.equal(control('target-points').value, '3');
  assert.equal(control('image-resolution').value, '2');
});

test('reading targets overlays the current description and strictly validates numerical data', () => {
  control('description').value = '  A hand-authored goal  ';
  const target = readEditorTarget('target');
  assert.equal(target.description, 'A hand-authored goal');
  assert.equal(JSON.parse(control('target-json').value).description, DEFAULT_TARGET.description);
  target.samples[0].outputs[0] = -50;
  assert.notEqual(readEditorTarget('target').samples[0].outputs[0], -50);
  control('target-json').value = '{';
  assert.throws(() => readEditorTarget('target'), /not valid JSON/);
  control('target-json').value = JSON.stringify({ ...DEFAULT_TARGET, samples: [{ inputs: [0], outputs: [0] }] });
  assert.throws(() => readEditorTarget('target'), /positive|between/);
});


test('an edited Agent description must be recompiled before its old numerical target can run', () => {
  const target = readEditorTarget('target');
  target.source = 'agent';
  replaceEditorTarget('target', target);
  control('description').value = 'A different design goal';
  assert.throws(() => readEditorTarget('target'), /description changed.*compilation/);
  const context = readEditorTarget('target', { allowPendingDescription: true });
  assert.equal(context.description, 'A different design goal');
  assert.deepEqual(context.samples, target.samples, 'compiler can retain prior role context while interpreting the new goal');
});

test('target replacement is one undoable command and preserves range metadata through JSON persistence', () => {
  const before = control('target-json').value;
  const target = readEditorTarget('target');
  target.outputs[0].min = -2;
  target.outputs[0].max = 3;
  target.outputs[0].offset = -0.4;
  assert.equal(replaceEditorTarget('target', target), true);
  assert.equal(undoStack.depth, 1);
  assert.equal(replaceEditorTarget('target', target), false);
  assert.equal(undoStack.depth, 1);
  const persisted = validateDesignTarget(JSON.parse(control('target-json').value));
  assert.deepEqual(persisted.outputs[0], target.outputs[0]);
  assert.match(control('target-roles').innerHTML, /data-key="min"[^>]*value="-2"/);
  undo();
  assert.equal(control('target-json').value, before);
  redo();
  assert.equal(readEditorTarget('target').outputs[0].min, -2);
});

test('input role ranges remap training and validation samples and Undo restores physical locations', async () => {
  const target = readEditorTarget('target');
  target.validation_samples = [{ inputs: [Math.sqrt(0.05 * 10)], outputs: [0.5], weight: 0.3 }];
  replaceEditorTarget('target', target);
  undoStack.clear();
  const original = control('target-json').value;
  await editRole('input', 0, 'max', 100, 'number');
  const changed = readEditorTarget('target');
  assert.equal(changed.inputs[0].max, 100);
  assert.equal(changed.samples[0].inputs[0], 0.05);
  assert.equal(changed.samples.at(-1).inputs[0], 100);
  near(changed.validation_samples[0].inputs[0], Math.sqrt(0.05 * 100));
  assert.equal(changed.validation_samples[0].weight, 0.3);
  undo();
  assert.equal(control('target-json').value, original);
});

test('image fields and trajectories retain their distinct explicit target schemas', async () => {
  await chooseMode('image');
  const image = readEditorTarget('target');
  assert.equal(image.schema_version, DEFAULT_TARGET.schema_version);
  assert.equal(image.source, 'image');
  assert.equal(image.inputs.length, 2);
  assert.equal(image.outputs.length, 1);
  assert.ok(image.samples.every(sample => sample.inputs.length === 2 && sample.outputs.length === 1));
  assert.equal(control('target-image').hidden, false);
  assert.equal(control('target-drawing').hidden, true);
  await chooseMode('trajectory');
  const trajectory = readEditorTarget('target');
  assert.equal(trajectory.schema_version, DEFAULT_TARGET.schema_version);
  assert.equal(trajectory.inputs.length, 1);
  assert.equal(trajectory.outputs.length, 2);
  assert.equal(control('target-drawing').hidden, false);
  assert.match(control('target-editor-status').textContent, /travel order/);
  await chooseMode('data');
  await control('target-roles').emit('change', { target: { dataset: { edit: 'input-count' }, value: '3' } });
  assert.equal(readEditorTarget('target').inputs.length, 3);
  assert.ok(readEditorTarget('target').samples.every(sample => sample.inputs.length === 3));
});

test('switching entrance undoes its selector, source and dimensions together', async () => {
  const before = control('target-json').value;
  control('target-mode').value = 'image';
  await control('target-mode').emit('change');
  assert.equal(undoStack.depth, 1);
  assert.equal(readEditorTarget('target').inputs.length, 2);
  undo();
  assert.equal(control('target-mode').value, 'curve');
  assert.equal(control('target-json').value, before);
  assert.equal(control('target-image').hidden, true);
  assert.equal(undoStack.depth, 0, 'Undo must not dispatch replacement history');
  redo();
  assert.equal(control('target-mode').value, 'image');
  assert.equal(readEditorTarget('target').source, 'image');
  assert.equal(readEditorTarget('target').inputs.length, 2);
});

test('a completed pointer stroke publishes normalized physical samples and can be undone', async () => {
  const canvas = control('target-canvas');
  const before = control('target-json').value;
  await canvas.emit('pointerdown', pointer(0, 0));
  await canvas.emit('pointermove', pointer(1, 1));
  await canvas.emit('pointerup', pointer(1, 1));
  const drawn = readEditorTarget('target');
  assert.deepEqual(drawn.samples.map(sample => sample.outputs), [[0], [0.5], [1]]);
  assert.equal(drawn.samples[0].inputs[0], 0.05);
  assert.equal(drawn.samples.at(-1).inputs[0], 10);
  assert.equal(invalidations[0], 'target-drawing-started');
  assert.equal(canvas.captures.size, 0);
  undo();
  assert.equal(control('target-json').value, before);
});

test('cancelled, replaced and old-workspace strokes cannot overwrite target samples', async () => {
  const canvas = control('target-canvas');
  const before = control('target-json').value;
  await canvas.emit('pointerdown', pointer(0, 0));
  await canvas.emit('pointermove', pointer(1, 1));
  await canvas.emit('pointercancel', pointer(1, 1));
  await canvas.emit('pointerup', pointer(1, 1));
  assert.equal(control('target-json').value, before);
  await canvas.emit('pointerdown', pointer(0, 0));
  await canvas.emit('pointermove', pointer(1, 1));
  const updated = readEditorTarget('target');
  updated.outputs[0].offset = 0.25;
  replaceEditorTarget('target', updated);
  const edited = control('target-json').value;
  await canvas.emit('pointerup', pointer(1, 1));
  assert.equal(control('target-json').value, edited);
  await canvas.emit('pointerdown', pointer(0, 0));
  await canvas.emit('pointermove', pointer(1, 1));
  advanceWorkspaceRuntimeEpoch();
  await canvas.emit('pointerup', pointer(1, 1));
  assert.equal(control('target-json').value, edited);
});

test('the release point completes a drag even without a delivered pointermove', async () => {
  const canvas = control('target-canvas');
  await canvas.emit('pointerdown', pointer(0, 0));
  await canvas.emit('pointerup', pointer(1, 1));
  assert.deepEqual(readEditorTarget('target').samples.map(sample => sample.outputs), [[0], [0.5], [1]]);
});

test('another pointer cannot alter or prematurely complete the active stroke', async () => {
  const canvas = control('target-canvas');
  const before = control('target-json').value;
  await canvas.emit('pointerdown', pointer(0, 0, 1));
  await canvas.emit('pointermove', pointer(1, 0, 2));
  await canvas.emit('pointerup', pointer(1, 0, 2));
  assert.equal(control('target-json').value, before);
  await canvas.emit('pointermove', pointer(1, 1, 1));
  await canvas.emit('pointerup', pointer(1, 1, 1));
  assert.deepEqual(readEditorTarget('target').samples.map(sample => sample.outputs), [[0], [0.5], [1]]);
});

test('clearing a drawing invalidates any in-flight stroke without deleting existing samples', async () => {
  const canvas = control('target-canvas');
  const before = control('target-json').value;
  await canvas.emit('pointerdown', pointer(0, 0));
  await canvas.emit('pointermove', pointer(1, 1));
  clearTargetDrawing('target');
  await canvas.emit('pointerup', pointer(1, 1));
  assert.equal(control('target-json').value, before);
});

test('invalid numerical input during a stroke is reported without an uncaught pointer exception', async () => {
  const canvas = control('target-canvas');
  await canvas.emit('pointerdown', pointer(0, 0));
  control('target-json').value = '{';
  await canvas.emit('pointermove', pointer(1, 1));
  await canvas.emit('pointerup', pointer(1, 1));
  assert.equal(control('target-json').value, '{');
  assert.match(control('target-editor-status').textContent, /not valid JSON/);
});

test('changing curve resolution resamples its response and Undo restores the prior numerical artifact', async () => {
  const canvas = control('target-canvas');
  await canvas.emit('pointerdown', pointer(0, 0));
  await canvas.emit('pointerup', pointer(1, 1));
  undoStack.clear();
  const before = control('target-json').value;
  control('target-points').value = '5';
  await control('target-points').emit('change');
  readEditorTarget('target').samples.forEach((sample, index) => near(sample.outputs[0], index / 4));
  undo();
  assert.equal(control('target-json').value, before);
  assert.equal(control('target-points').value, '3');
  redo();
  assert.equal(readEditorTarget('target').samples.length, 5);
  assert.equal(control('target-points').value, '5');
});

test('image decode is latest-wins and releases both successful and superseded blob URLs', async () => {
  await chooseMode('image');
  const fileControl = control('target-image-file');
  fileControl.files = [{ name: 'older.png', size: 100, brightness: 0 }];
  const older = fileControl.emit('change');
  fileControl.files = [{ name: 'newer.png', size: 100, brightness: 255 }];
  const newer = fileControl.emit('change');
  imageLoads[1].onload(); await newer;
  const accepted = control('target-json').value;
  assert.equal(readEditorTarget('target').samples.length, 4);
  assert.ok(readEditorTarget('target').samples.every(sample => Math.abs(sample.outputs[0] - 1) < 1e-12));
  imageLoads[0].onload(); await older;
  assert.equal(control('target-json').value, accepted);
  assert.match(control('target-editor-status').textContent, /newer\.png/);
  assert.equal(revokedUrls.length, 2);
  assert.equal(objectUrls.size, 0);
  undo();
  assert.equal(readEditorTarget('target').samples.length, DEFAULT_TARGET.samples.length);
});

test('image completion cannot cross edits, node ownership or workspace epochs', async () => {
  const originalOwner = nodeRegistry.target;
  for (const invalidate of [
    () => { const target = readEditorTarget('target'); target.outputs[0].offset += 1; replaceEditorTarget('target', target); },
    () => { nodeRegistry.target = { type: 'inverse-design-target', data: {} }; },
    () => advanceWorkspaceRuntimeEpoch(),
  ]) {
    nodeRegistry.target = originalOwner;
    await chooseMode('image');
    const fileControl = control('target-image-file');
    fileControl.files = [{ name: 'delayed.png', size: 100, brightness: 0 }];
    const pending = fileControl.emit('change');
    invalidate();
    const before = control('target-json').value;
    imageLoads.at(-1).onload(); await pending;
    assert.equal(control('target-json').value, before);
  }
});

test('image settings resample the accepted local source and Undo restores prior samples', async () => {
  await chooseMode('image');
  const fileControl = control('target-image-file');
  fileControl.files = [{ name: 'black.png', size: 100, brightness: 0 }];
  const imported = fileControl.emit('change');
  imageLoads.at(-1).onload(); await imported;
  undoStack.clear();
  const before = control('target-json').value;
  control('image-resolution').value = '3';
  await control('image-resolution').emit('change');
  assert.equal(readEditorTarget('target').samples.length, 9);
  assert.ok(readEditorTarget('target').samples.every(sample => sample.outputs[0] === 0));
  control('image-invert').value = 'true';
  await control('image-invert').emit('change');
  assert.ok(readEditorTarget('target').samples.every(sample => Math.abs(sample.outputs[0] - 1) < 1e-12));
  undo();
  assert.ok(readEditorTarget('target').samples.every(sample => sample.outputs[0] === 0));
  assert.equal(control('image-invert').value, 'false');
  undo();
  assert.equal(control('target-json').value, before);
  assert.equal(control('image-resolution').value, '2');
});

test('Undo of a later image import restores the earlier bitmap for subsequent inversion', async () => {
  await chooseMode('image');
  const fileControl = control('target-image-file');
  for (const [name, brightness] of [['black.png', 0], ['white.png', 255]]) {
    fileControl.files = [{ name, size: 100, brightness }];
    const pending = fileControl.emit('change');
    imageLoads.at(-1).onload(); await pending;
  }
  assert.equal(undoStack.depth, 2);
  assert.ok(readEditorTarget('target').samples.every(sample => Math.abs(sample.outputs[0] - 1) < 1e-12));
  undo();
  assert.ok(readEditorTarget('target').samples.every(sample => sample.outputs[0] === 0));
  control('image-invert').value = 'true';
  await control('image-invert').emit('change');
  assert.ok(readEditorTarget('target').samples.every(sample => Math.abs(sample.outputs[0] - 1) < 1e-12), 'The restored black image should invert to white; the superseded white bitmap must not be reused.');
  assert.equal(undoStack.canRedo, false);
});

test('saved image samples remain editable when the original image is unavailable', async () => {
  await chooseMode('image');
  const before = control('target-json').value;
  control('image-resolution').value = '3';
  await control('image-resolution').emit('change');
  assert.equal(control('target-json').value, before);
  assert.match(control('target-editor-status').textContent, /Upload the original image/);
});

test('invalid image files report failure without replacing a previously valid target', async () => {
  await chooseMode('image');
  const before = control('target-json').value;
  const fileControl = control('target-image-file');
  fileControl.files = [{ name: 'too-big.png', size: 21 * 1024 * 1024, brightness: 0 }];
  await fileControl.emit('change');
  assert.match(control('target-editor-status').textContent, /20 MB/);
  assert.equal(control('target-json').value, before);
  fileControl.files = [{ name: 'invalid.png', size: 100, brightness: 0 }];
  const failed = fileControl.emit('change');
  imageLoads.at(-1).onerror(); await failed;
  assert.match(control('target-editor-status').textContent, /could not be decoded/);
  assert.equal(control('target-json').value, before);
  assert.equal(objectUrls.size, 0);
});

test('invalid numerical edits report status without fabricating preview data', () => {
  const previous = control('target-canvas').innerHTML;
  control('target-json').value = '{';
  refreshTargetEditor('target');
  assert.match(control('target-editor-status').textContent, /not valid JSON/);
  assert.equal(control('target-canvas').innerHTML, previous);
});

test('trajectory image uploads are local tracing references and require an ordered trace before execution', async () => {
  await chooseMode('trajectory');
  assert.equal(control('target-image').hidden, false, 'Image upload must be available for the one-input, two-output route.');
  const before = control('target-json').value;
  await importReference('trace-reference.png', 0);
  assert.equal(control('target-json').value, before, 'Importing a reference must not invent a travel order or convert its brightness into trajectory targets.');
  const reference = JSON.parse(control('reference-state').value);
  assert.equal(reference.name, 'trace-reference.png');
  assert.equal(reference.pending, true);
  assert.equal(typeof reference.id, 'string');
  assert.doesNotMatch(control('reference-state').value, /data:image/);
  assert.match(control('target-canvas').innerHTML, /data:image\/png;base64,AA==/);
  assert.throws(() => readEditorTarget('target'), /trace|draw|ordered/i);
  const preview = readEditorTarget('target', { allowPendingReference: true });
  assert.equal(preview.inputs.length, 1);
  assert.equal(preview.outputs.length, 2);
  assert.equal(preview.source, 'trajectory');
});

test('trajectory reference image decoding is latest-wins without changing numerical samples', async () => {
  await chooseMode('trajectory');
  const before = control('target-json').value;
  const fileControl = control('target-image-file');
  fileControl.files = [{ name: 'old-reference.png', size: 100, brightness: 0 }];
  const old = fileControl.emit('change');
  fileControl.files = [{ name: 'new-reference.png', size: 100, brightness: 255 }];
  const current = fileControl.emit('change');
  imageLoads[1].onload(); await current;
  const accepted = control('reference-state').value;
  imageLoads[0].onload(); await old;
  assert.equal(control('reference-state').value, accepted);
  assert.equal(JSON.parse(accepted).name, 'new-reference.png');
  assert.equal(control('target-json').value, before);
  assert.match(control('target-canvas').innerHTML, /data:image\/png;base64,\/w==/);
  assert.doesNotMatch(control('target-canvas').innerHTML, /data:image\/png;base64,AA==/);
  assert.equal(objectUrls.size, 0);
  assert.equal(revokedUrls.length, 2);
});

test('completing a reference trace produces ordered one-input two-output samples and a single Undo restores pending state', async () => {
  await chooseMode('trajectory');
  await importReference('ordered-pattern.png', 0);
  const beforeTarget = control('target-json').value;
  const beforeReference = control('reference-state').value;
  undoStack.clear();
  const canvas = control('target-canvas');
  await canvas.emit('pointerdown', pointer(0, 0));
  await canvas.emit('pointermove', pointer(1, 0));
  await canvas.emit('pointerup', pointer(1, 1));
  const target = readEditorTarget('target');
  assert.equal(target.source, 'trajectory');
  assert.equal(target.inputs.length, 1);
  assert.equal(target.outputs.length, 2);
  assert.equal(JSON.parse(control('reference-state').value).pending, false);
  assert.deepEqual(target.samples.map(sample => sample.outputs), [[0, 0], [1, 0], [1, 1]]);
  assert.equal(undoStack.depth, 1);
  undo();
  assert.equal(control('target-json').value, beforeTarget);
  assert.equal(control('reference-state').value, beforeReference);
  assert.throws(() => readEditorTarget('target'), /trace|draw|ordered/i);
  assert.equal(undoStack.depth, 0);
  redo();
  assert.deepEqual(readEditorTarget('target').samples, target.samples);
  assert.equal(JSON.parse(control('reference-state').value).pending, false);
});

test('Undo and Redo of reference imports restore the appropriate cached local image', async () => {
  await chooseMode('trajectory');
  await importReference('first-pattern.png', 0);
  const firstReference = control('reference-state').value;
  await importReference('second-pattern.png', 255);
  const secondReference = control('reference-state').value;
  assert.notEqual(JSON.parse(firstReference).id, JSON.parse(secondReference).id);
  assert.match(control('target-canvas').innerHTML, /data:image\/png;base64,\/w==/);
  undo();
  assert.equal(control('reference-state').value, firstReference);
  assert.match(control('target-canvas').innerHTML, /data:image\/png;base64,AA==/);
  assert.doesNotMatch(control('target-canvas').innerHTML, /data:image\/png;base64,\/w==/);
  redo();
  assert.equal(control('reference-state').value, secondReference);
  assert.match(control('target-canvas').innerHTML, /data:image\/png;base64,\/w==/);
});

test('pending reference state survives workspace reload even when its local bitmap is unavailable', async () => {
  await chooseMode('trajectory');
  await importReference('unfinished-pattern.png', 0);
  const saved = savedEditorFields();
  resetHarness(saved);
  assert.equal(control('reference-state').value, saved['reference-state']);
  assert.equal(control('target-json').value, saved['target-json']);
  assert.throws(() => readEditorTarget('target'), /trace|draw|ordered/i);
  assert.equal(readEditorTarget('target', { allowPendingReference: true }).outputs.length, 2);
  assert.doesNotMatch(control('target-canvas').innerHTML, /data:image/);
  assert.match(control('target-editor-status').textContent, /upload|reopen|reference|unavailable|image/i);
});

test('a completed reference trace remains executable after reload without the local bitmap', async () => {
  await chooseMode('trajectory');
  await importReference('completed-pattern.png', 0);
  const canvas = control('target-canvas');
  await canvas.emit('pointerdown', pointer(0, 0));
  await canvas.emit('pointerup', pointer(1, 1));
  const completed = readEditorTarget('target');
  const saved = savedEditorFields();
  resetHarness(saved);
  assert.deepEqual(readEditorTarget('target'), completed);
  assert.equal(JSON.parse(control('reference-state').value).pending, false);
  assert.doesNotMatch(control('target-canvas').innerHTML, /data:image/);
  assert.match(control('target-editor-status').textContent, /saved|numerical|trace|image|reference/i);
});

test('a stroke begun on an earlier reference cannot complete a replacement reference', async () => {
  await chooseMode('trajectory');
  await importReference('before-stroke.png', 0);
  const beforeTarget = control('target-json').value;
  const canvas = control('target-canvas');
  await canvas.emit('pointerdown', pointer(0, 0));
  await canvas.emit('pointermove', pointer(1, 0));
  await importReference('replacement-pattern.png', 255);
  const replacement = control('reference-state').value;
  await canvas.emit('pointerup', pointer(1, 1));
  assert.equal(control('reference-state').value, replacement);
  assert.equal(JSON.parse(replacement).pending, true);
  assert.equal(control('target-json').value, beforeTarget);
  assert.throws(() => readEditorTarget('target'), /trace|draw|ordered/i);
});

test('loading a trajectory reference blocks execution immediately and stale workspace completion cannot publish it', async () => {
  await chooseMode('trajectory');
  const before = control('target-json').value;
  const fileControl = control('target-image-file');
  fileControl.files = [{ name: 'slow-reference.png', size: 100, brightness: 0 }];
  const pending = fileControl.emit('change');
  assert.throws(() => readEditorTarget('target'), /trace|draw|ordered/i);
  assert.equal(readEditorTarget('target', { allowPendingReference: true }).outputs.length, 2);
  advanceWorkspaceRuntimeEpoch();
  imageLoads.at(-1).onload(); await pending;
  assert.equal(control('reference-state').value, '');
  assert.equal(control('target-json').value, before);
  assert.equal(readEditorTarget('target').outputs.length, 2);
  assert.equal(objectUrls.size, 0);
});

test('a pending reference decode cannot attach its bitmap to a replacement node owner', async () => {
  await chooseMode('trajectory');
  const fileControl = control('target-image-file');
  fileControl.files = [{ name: 'old-owner-reference.png', size: 100, brightness: 0 }];
  const pending = fileControl.emit('change');
  nodeRegistry.target = { type: 'inverse-design-target', data: {} };
  imageLoads.at(-1).onload(); await pending;
  assert.equal(control('reference-state').value, '');
  assert.doesNotMatch(control('target-canvas').innerHTML, /data:image/);
  assert.equal(nodeRegistry.target._designReferenceCache, undefined);
  assert.equal(readEditorTarget('target').outputs.length, 2);
  assert.equal(objectUrls.size, 0);
});

test('a restored pending reference can be abandoned for manual numerical editing and Undo restores its guard', async () => {
  await chooseMode('trajectory');
  await importReference('restored-pending-pattern.png', 0);
  const saved = savedEditorFields();
  resetHarness(saved);
  assert.throws(() => readEditorTarget('target'), /trace|draw|ordered/i);
  control('target-mode').value = 'data';
  await control('target-mode').emit('change');
  const manual = readEditorTarget('target');
  assert.equal(manual.source, 'data');
  assert.equal(manual.inputs.length, 1);
  assert.equal(manual.outputs.length, 2);
  assert.equal(control('reference-state').value, '');
  assert.equal(undoStack.depth, 1);
  undo();
  assert.equal(control('target-mode').value, 'trajectory');
  assert.equal(control('reference-state').value, saved['reference-state']);
  assert.equal(control('target-json').value, saved['target-json']);
  assert.throws(() => readEditorTarget('target'), /trace|draw|ordered/i);
  redo();
  assert.equal(readEditorTarget('target').source, 'data');
});

test('malformed persisted reference state is rejected and choosing a manual entrance resets it', async () => {
  control('reference-state').value = '{';
  assert.throws(() => readEditorTarget('target'), /reference state is invalid/i);
  control('target-mode').value = 'data';
  await control('target-mode').emit('change');
  assert.equal(control('reference-state').value, '');
  assert.equal(readEditorTarget('target').source, 'data');
});
