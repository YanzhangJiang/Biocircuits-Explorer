// Typed-port and schema persistence contracts for the node graph.
// Run: node webapp/test/port-types.test.mjs

import assert from 'node:assert/strict';
import {
  PORT_TYPES, portTypeOf, portTypesCompatible, portsCompatible,
} from '../public/js/port-types.js';

let passed = 0;
function test(name, fn) {
  fn();
  passed += 1;
  console.log(`  ok - ${name}`);
}

// The full set of port ids declared across node types (keep in sync with the
// node-type definitions; this test fails loudly if one loses its type mapping).
const PORT_IDS = [
  'reactions', 'model', 'params', 'result',
  'atlas-spec', 'atlas', 'atlas-query', 'atlas-network',
  'designability-spec',
  'rop-shape-reference', 'rop-shape-request', 'rop-shape-result',
];

const ROP_SHAPE_PORT_TYPES = [
  PORT_TYPES.ROPShapeReferenceArtifact,
  PORT_TYPES.ROPShapeRequestArtifact,
  PORT_TYPES.ROPShapeResultArtifact,
];

test('every known port id maps to a declared type (no raw-id fallback)', () => {
  for (const id of PORT_IDS) {
    const t = portTypeOf(id);
    assert.notEqual(t, id, `port '${id}' should map to a type, not fall back to its id`);
    assert.ok(Object.values(PORT_TYPES).includes(t), `'${id}' → '${t}' must be a PORT_TYPES value`);
  }
});

test('unknown port id falls back to its own id and is only self-compatible', () => {
  assert.equal(portTypeOf('mystery'), 'mystery');
  assert.ok(portsCompatible('mystery', 'mystery'));
  assert.ok(!portsCompatible('mystery', 'model'));
});

test('a declared per-port type overrides the central map', () => {
  assert.equal(portTypeOf('model', 'CustomType'), 'CustomType');
});

test('type compatibility is reflexive and rejects mismatches', () => {
  assert.ok(portTypesCompatible(PORT_TYPES.NetworkIR, PORT_TYPES.NetworkIR));
  assert.ok(!portTypesCompatible(PORT_TYPES.NetworkIR, PORT_TYPES.ModelArtifact));
});

test('ROP shape ports map to three distinct artifact types with no implicit compatibility', () => {
  assert.equal(portTypeOf('rop-shape-reference'), PORT_TYPES.ROPShapeReferenceArtifact);
  assert.equal(portTypeOf('rop-shape-request'), PORT_TYPES.ROPShapeRequestArtifact);
  assert.equal(portTypeOf('rop-shape-result'), PORT_TYPES.ROPShapeResultArtifact);
  assert.equal(new Set(ROP_SHAPE_PORT_TYPES).size, 3);

  for (let output = 0; output < ROP_SHAPE_PORT_TYPES.length; output += 1) {
    for (let input = 0; input < ROP_SHAPE_PORT_TYPES.length; input += 1) {
      assert.equal(
        portTypesCompatible(ROP_SHAPE_PORT_TYPES[output], ROP_SHAPE_PORT_TYPES[input]),
        output === input,
      );
    }
  }
  assert.equal(portsCompatible('rop-shape-reference', 'rop-shape-request'), false);
  assert.equal(portsCompatible('rop-shape-request', 'rop-shape-result'), false);
  assert.equal(portsCompatible('rop-shape-result', 'result'), false);
});

test('behavior-preserving: typed compatibility matches old port-id equality', () => {
  // The old rule was `fromPort === toPort`. With a bijective id→type map, the
  // typed rule must accept/reject exactly the same pairs — no silent widening.
  for (const a of PORT_IDS) {
    for (const b of PORT_IDS) {
      assert.equal(portsCompatible(a, b), a === b, `compat(${a}, ${b})`);
    }
  }
});

globalThis.window = {
  matchMedia: () => null,
  addEventListener() {},
  location: { protocol: 'http:', hostname: '127.0.0.1', port: '8000' },
  sessionStorage: { getItem() { return null; }, setItem() {} },
};
globalThis.document = {
  readyState: 'loading',
  documentElement: { dataset: {}, style: { setProperty() {} } },
  getElementById() { return null; },
  addEventListener() {},
  querySelectorAll() { return []; },
};
globalThis.HTMLSelectElement = class HTMLSelectElement {};

const { NODE_SCHEMAS, serializeNodeBySchema, restoreNodeBySchema } =
  await import('../public/js/node-schema.js');
const { nodeRegistry } = await import('../public/js/state.js');
// node-schema preloads its lazy hook modules without blocking module import.
await new Promise(resolve => setTimeout(resolve, 0));

const ROP_SHAPE_FIELD_CONTRACT = Object.freeze({
  ropShapeKind:           ['-rop-shape-kind', 'string', 'broaden'],
  intentId:               ['-intent-id', 'string', 'shape-edit'],
  leftSpan:               ['-left-span', 'string', '0, 2'],
  rightSpan:              ['-right-span', 'string', '4, 6'],
  steps:                  ['-steps', 'string', '1, 5'],
  group:                  ['-group', 'string', '4, 5, 6'],
  preserve:               ['-preserve', 'string', '0, 1, 2, 3'],
  anchorStep:             ['-anchor-step', 'int', 3],
  anchorTolerance:        ['-anchor-tolerance', 'float', 0.2],
  midpointTolerance:      ['-midpoint-tolerance', 'float', 0.2],
  effectTolerance:        ['-effect-tolerance', 'float', 0.02],
  minimumParameterMargin: ['-minimum-parameter-margin', 'float', 0.01],
  sense:                  ['-sense', 'string', 'positive'],
  shared:                 ['-shared', 'bool', true],
  maxPaths:               ['-max-paths', 'int', 2000],
  maxCells:               ['-max-cells', 'int', 256],
  maxReplays:             ['-max-replays', 'int', 1],
  requireExhaustive:      ['-require-exhaustive', 'bool', true],
  replaySamplePoints:     ['-replay-sample-points', 'int', 281],
  replayMinProminence:    ['-replay-min-prominence', 'float', 0.5],
  linearIntentJson:       ['-linear-intent-json', 'string', undefined],
});

function fakeInput(value, checked = false) {
  let currentValue = String(value ?? '');
  return {
    dataset: {},
    checked,
    get value() { return currentValue; },
    set value(next) { currentValue = String(next); },
  };
}

test('ROP shape edit schema persists every UI control and the prepared request', () => {
  const schema = NODE_SCHEMAS['rop-shape-edit-config'];
  assert.ok(schema);
  assert.deepEqual(Object.keys(schema.fields), Object.keys(ROP_SHAPE_FIELD_CONTRACT));
  for (const [key, [suffix, type, defaultValue]] of Object.entries(ROP_SHAPE_FIELD_CONTRACT)) {
    assert.equal(schema.fields[key].suffix, suffix, `${key} suffix`);
    assert.equal(schema.fields[key].type, type, `${key} type`);
    assert.equal(schema.fields[key].default, defaultValue, `${key} default`);
  }
  assert.deepEqual(schema.data, ['ropShapeRequest']);
  assert.deepEqual(schema.restoreToData, ['ropShapeRequest']);
  assert.equal(typeof schema.afterRestore, 'function');

  const values = {
    ropShapeKind: 'translate_group',
    intentId: 'translate-right-ear',
    leftSpan: '0, 2',
    rightSpan: '4, 6',
    steps: '1, 5',
    group: '4, 5, 6',
    preserve: '0, 1, 2, 3',
    anchorStep: 3,
    anchorTolerance: 0.25,
    midpointTolerance: 0.15,
    effectTolerance: 0.02,
    minimumParameterMargin: 0.1,
    sense: 'positive',
    shared: true,
    maxPaths: 2000,
    maxCells: 256,
    maxReplays: 2,
    requireExhaustive: true,
    replaySamplePoints: 281,
    replayMinProminence: 0.5,
    linearIntentJson: '{"id":"linear","kind":"linear_witness"}',
  };
  const sourceElements = {};
  const restoredElements = {};
  for (const [key, fd] of Object.entries(schema.fields)) {
    sourceElements[`shape-source${fd.suffix}`] =
      fakeInput(values[key], fd.type === 'bool' ? values[key] : false);
    restoredElements[`shape-restored${fd.suffix}`] = fakeInput('', false);
  }
  const request = {
    schema_version: 'bne-rop-shape-optimize-request/v1.0.0',
    edit_intent: { id: 'translate-right-ear', kind: 'translate_group' },
  };
  nodeRegistry['shape-source'] = { data: { ropShapeRequest: request } };
  nodeRegistry['shape-restored'] = { data: {} };

  document.getElementById = id => sourceElements[id] || null;
  const snapshot = serializeNodeBySchema('shape-source', 'rop-shape-edit-config');
  for (const [key, value] of Object.entries(values)) assert.deepEqual(snapshot[key], value);
  assert.deepEqual(snapshot.ropShapeRequest, request);
  assert.notStrictEqual(snapshot.ropShapeRequest, request);

  document.getElementById = id => restoredElements[id] || null;
  assert.equal(
    restoreNodeBySchema('shape-restored', 'rop-shape-edit-config', snapshot),
    true,
  );
  for (const [key, fd] of Object.entries(schema.fields)) {
    const element = restoredElements[`shape-restored${fd.suffix}`];
    if (fd.type === 'bool') assert.equal(element.checked, values[key], key);
    else assert.equal(element.value, String(values[key]), key);
  }
  assert.deepEqual(nodeRegistry['shape-restored'].data.ropShapeRequest, request);

  delete nodeRegistry['shape-source'];
  delete nodeRegistry['shape-restored'];
  document.getElementById = () => null;
});

test('ROP shape result schema persists request/result and declares lazy view restoration', () => {
  const schema = NODE_SCHEMAS['rop-shape-result'];
  assert.ok(schema);
  assert.deepEqual(schema.data, ['ropShapeRequest', 'ropShapeResult']);
  assert.deepEqual(schema.restoreToData, ['ropShapeRequest', 'ropShapeResult']);
  assert.equal(typeof schema.afterRestore, 'function');

  const request = { schema_version: 'bne-rop-shape-optimize-request/v1.0.0' };
  const result = { schema_version: 'bne-rop-shape-optimization/v1.0.0' };
  nodeRegistry['result-source'] = { data: { ropShapeRequest: request, ropShapeResult: result } };
  nodeRegistry['result-restored'] = { data: {} };

  const snapshot = serializeNodeBySchema('result-source', 'rop-shape-result');
  assert.deepEqual(snapshot, { ropShapeRequest: request, ropShapeResult: result });
  assert.notStrictEqual(snapshot.ropShapeRequest, request);
  assert.notStrictEqual(snapshot.ropShapeResult, result);
  assert.equal(restoreNodeBySchema('result-restored', 'rop-shape-result', snapshot), true);
  assert.deepEqual(nodeRegistry['result-restored'].data.ropShapeRequest, request);
  assert.deepEqual(nodeRegistry['result-restored'].data.ropShapeResult, result);

  delete nodeRegistry['result-source'];
  delete nodeRegistry['result-restored'];
});

console.log(`\nAll ${passed} port-type tests passed.`);
