import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import {
  LEGACY_WORKSPACE_NODE_MIGRATIONS,
  WORKSPACE_V2_NODE_TYPES,
  WORKSPACE_V2_PORT_DECLARATIONS,
  WORKSPACE_V2_SCHEMA_VERSION,
  WorkspaceV2Error,
  migrateWorkspaceDocument,
  validateWorkspaceV2Document,
} from '../public/js/workspace-v2.js';

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

const { NODE_TYPES } = await import('../public/js/node-types/index.js');

const here = path.dirname(fileURLToPath(import.meta.url));
const repoRoot = path.resolve(here, '..', '..');
const fixtureRoot = path.join(repoRoot, 'tests', 'fixtures', 'workspace');

function fixture(name) {
  return JSON.parse(fs.readFileSync(path.join(fixtureRoot, name), 'utf8'));
}

function clone(value) {
  return JSON.parse(JSON.stringify(value));
}

function connectionKey(connection) {
  return [connection.fromNode, connection.fromPort, connection.toNode, connection.toPort].join(':');
}

function containsRuntimeSessionField(value) {
  if (Array.isArray(value)) return value.some(containsRuntimeSessionField);
  if (!value || typeof value !== 'object') return false;
  return Object.entries(value).some(([key, child]) =>
    ['sessionId', 'session_id', 'executionToken', 'requestToken'].includes(key) ||
    containsRuntimeSessionField(child));
}

let passed = 0;
function test(name, fn) {
  fn();
  passed += 1;
  console.log(`  ok - ${name}`);
}

test('the shared v1 fixture migrates exactly and without mutating its caller', () => {
  const source = fixture('valid-v1.json');
  const before = clone(source);
  const expected = fixture('valid-v1.expected-v2.json');
  const migrated = migrateWorkspaceDocument(source);

  assert.deepEqual(source, before);
  assert.deepEqual(migrated.document, expected);
  assert.deepEqual(validateWorkspaceV2Document(migrated.document), expected);
  assert.equal(migrated.document.version, 2);
  assert.equal(migrated.document.schema_version, WORKSPACE_V2_SCHEMA_VERSION);
  assert.equal(containsRuntimeSessionField(migrated.document), false);
  assert.ok(migrated.diagnostics.some(entry => entry.code === 'runtime-fields-stripped'));
});

test('v2 restore normalization is deterministic and idempotent', () => {
  const expected = fixture('valid-v1.expected-v2.json');
  const once = migrateWorkspaceDocument(expected);
  const twice = migrateWorkspaceDocument(once.document);

  assert.deepEqual(once.document, expected);
  assert.deepEqual(twice.document, expected);
  assert.deepEqual(once, twice);
});

test('all persisted freshness axes become historical without changing evidence grades', () => {
  const source = {
    version: 2,
    schema_version: WORKSPACE_V2_SCHEMA_VERSION,
    canvas: { panX: 0, panY: 0, scale: 1 },
    nodes: [{
      id: 'design',
      type: 'design-target',
      x: 0,
      y: 0,
      data: {
        config: {
          selectedNid: 'network-1',
          resolvedDefinition: { raw_rules: ['A + B <-> C'] },
        },
        lifecycle: {
          state: 'current', freshness: 'current', evidence: { evidence_grade: 'screened_proxy' },
        },
        selectionLifecycle: {
          state: 'current', freshness: 'current', evidence: { evidence_grade: 'proxy_only' },
        },
      },
    }],
    connections: [],
  };
  const { document } = migrateWorkspaceDocument(source);
  const data = document.nodes[0].data;
  assert.deepEqual(data.lifecycle, {
    state: 'historical', freshness: 'historical', evidence: { evidence_grade: 'screened_proxy' },
  });
  assert.deepEqual(data.selectionLifecycle, {
    state: 'historical', freshness: 'historical', evidence: { evidence_grade: 'proxy_only' },
  });
});

test('a persisted current lifecycle cannot survive restore when its result payload is absent', () => {
  const source = {
    version: 2,
    schema_version: WORKSPACE_V2_SCHEMA_VERSION,
    canvas: { panX: 0, panY: 0, scale: 1 },
    nodes: [{
      id: 'atlas',
      type: 'atlas-builder',
      x: 0,
      y: 0,
      data: {
        lifecycle: {
          state: 'current',
          freshness: 'current',
          evidence: { evidence_grade: 'current-computation' },
        },
      },
    }],
    connections: [],
  };

  const { document } = migrateWorkspaceDocument(source);
  assert.deepEqual(document.nodes[0].data.lifecycle, {
    state: 'historical',
    freshness: 'historical',
    evidence: { evidence_grade: 'current-computation' },
  });
});

test('all six legacy merged nodes expand into explicit typed config/result pairs', () => {
  const { document, diagnostics } = migrateWorkspaceDocument(fixture('valid-v1.json'));
  const legacyTypes = new Set(Object.keys(LEGACY_WORKSPACE_NODE_MIGRATIONS));
  const actualTypes = new Set(document.nodes.map(node => node.type));
  for (const legacyType of legacyTypes) assert.equal(actualTypes.has(legacyType), false);

  assert.equal(diagnostics.filter(entry => entry.code === 'legacy-node-expanded').length, 6);
  for (const entry of diagnostics.filter(item => item.code === 'legacy-node-expanded')) {
    assert.ok(document.nodes.some(node => node.id === entry.details.configNodeId));
    assert.ok(document.nodes.some(node => node.id === entry.details.resultNodeId));
    assert.ok(document.connections.some(connection =>
      connection.fromNode === entry.details.configNodeId &&
      connection.fromPort === 'params' &&
      connection.toNode === entry.details.resultNodeId &&
      connection.toPort === 'params'));
  }
});

test('legacy expansion preserves only existing scientific results and marks them historical', () => {
  const { document } = migrateWorkspaceDocument(fixture('valid-v1.json'));
  const resultNodes = document.nodes.filter(node => node.id.endsWith('--result'));
  assert.equal(resultNodes.length, 6);
  for (const node of resultNodes) {
    assert.deepEqual(node.data.lifecycle, {
      state: 'historical',
      freshness: 'historical',
      evidence: node.data.evidence ?? null,
    });
  }

  const sisoResult = document.nodes.find(node => node.id === 'legacy-siso--result');
  assert.deepEqual(sisoResult.data.evidence, { grade: 'sampled' });
  assert.deepEqual(sisoResult.data.lifecycle.evidence, sisoResult.data.evidence);
  assert.notEqual(sisoResult.data.lifecycle.evidence, sisoResult.data.evidence);
  const model = document.nodes.find(node => node.id === 'model');
  assert.equal(model.data.built, undefined);
  assert.equal(model.data.modelContext.sessionId, undefined);
  assert.equal(model.data.modelContext.network_ir_hash, 'immutable-model-hash');
  assert.deepEqual(model.data.evidence, { grade: 'engine-computed' });
});

test('unknown extension fields survive document, canvas, node, connection, data, and agent records', () => {
  const { document } = migrateWorkspaceDocument(fixture('valid-v1.json'));
  assert.equal(document.pluginDocumentMetadata.owner, 'fixture-extension');
  assert.equal(document.canvas.snapGrid, 16);
  assert.equal(document.nodes[0].pluginNodeMetadata.color, 'blue');
  assert.equal(document.connections[0].routing, 'orthogonal');
  assert.equal(
    document.nodes.find(node => node.id === 'legacy-scan-1d').data.pluginNote,
    'unknown legacy field preserved on config',
  );
  assert.equal(document.designAgent.convo[0].extension, true);
  assert.equal(document.designAgent.pluginConversationMetadata.visible, true);
});

test('the 7x7 strict config fixture keeps seven same-family wires and diagnoses 42 cross-family wires', () => {
  const { document, diagnostics } = migrateWorkspaceDocument(fixture('strict-config-invalid-v1.json'));
  const expected = new Set([
    'siso-p:params:siso-r:params',
    'scan1-p:params:scan1-r:params',
    'scan2-p:params:scan2-r:params',
    'cloud-p:params:cloud-r:params',
    'fret-p:params:fret-r:params',
    'poly-p:params:poly-r:params',
    'placer-p:params:placer-r:params',
  ]);
  assert.deepEqual(new Set(document.connections.map(connectionKey)), expected);
  const rejected = diagnostics.filter(entry =>
    entry.code === 'connection-dropped-incompatible-port-types');
  assert.equal(rejected.length, 42);
  assert.ok(rejected.every(entry => entry.details.outputType !== entry.details.inputType));
});

test('unknown ports, missing endpoints, malformed records, and self loops are dropped explicitly', () => {
  const source = {
    version: 1,
    nodes: [
      { id: 'network', type: 'reaction-network', x: 0, y: 0, data: {} },
      { id: 'model', type: 'model-builder', x: 100, y: 0, data: {} },
    ],
    connections: [
      { fromNode: 'network', fromPort: 'unknown', toNode: 'model', toPort: 'reactions' },
      { fromNode: 'network', fromPort: 'reactions', toNode: 'model', toPort: 'unknown' },
      { fromNode: 'missing', fromPort: 'reactions', toNode: 'model', toPort: 'reactions' },
      { fromNode: 'network', fromPort: 'reactions', toNode: 'network', toPort: 'reactions' },
      null,
    ],
  };
  const { document, diagnostics } = migrateWorkspaceDocument(source);
  assert.deepEqual(document.connections, []);
  assert.deepEqual(
    diagnostics.filter(entry => entry.code.startsWith('connection-dropped')).map(entry => entry.code),
    [
      'connection-dropped-invalid-output-port',
      'connection-dropped-invalid-input-port',
      'connection-dropped-missing-endpoint',
      'connection-dropped-self-loop',
      'connection-dropped-invalid-shape',
    ],
  );
});

test('generated result IDs are deterministic and collision-safe', () => {
  const source = {
    version: 1,
    nodes: [
      { id: 'scan', type: 'parameter-scan-1d', x: 0, y: 0, data: {} },
      { id: 'scan--result', type: 'markdown-note', x: 10, y: 10, data: {} },
    ],
    connections: [],
  };
  const first = migrateWorkspaceDocument(source).document;
  const second = migrateWorkspaceDocument(source).document;
  assert.deepEqual(first, second);
  assert.ok(first.nodes.some(node => node.id === 'scan--result-2' && node.type === 'scan-1d-result'));
});

test('future versions and malformed version fields fail closed with stable error codes', () => {
  assert.throws(
    () => migrateWorkspaceDocument(fixture('future-v3.json')),
    error => error instanceof WorkspaceV2Error && error.code === 'future-version',
  );
  assert.throws(
    () => migrateWorkspaceDocument({ version: '2', nodes: [] }),
    error => error instanceof WorkspaceV2Error && error.code === 'invalid-version',
  );
  assert.throws(
    () => validateWorkspaceV2Document({ version: 1, nodes: [] }),
    error => error instanceof WorkspaceV2Error && error.code === 'migration-required',
  );
  const wrongSchema = fixture('valid-v1.expected-v2.json');
  wrongSchema.schema_version = 'bne-workspace/v3.0.0';
  assert.throws(
    () => migrateWorkspaceDocument(wrongSchema),
    error => error instanceof WorkspaceV2Error && error.code === 'invalid-schema-version',
  );
});

test('v2 validation covers lifecycle and Design Agent conversation records', () => {
  const invalidLifecycle = fixture('valid-v1.expected-v2.json');
  invalidLifecycle.nodes[1].data.lifecycle.freshness = 'fresh-enough';
  assert.throws(
    () => validateWorkspaceV2Document(invalidLifecycle),
    error => error instanceof WorkspaceV2Error && error.code === 'invalid-execution-lifecycle',
  );

  const invalidConversation = fixture('valid-v1.expected-v2.json');
  delete invalidConversation.designAgent.convo[0].text;
  assert.throws(
    () => validateWorkspaceV2Document(invalidConversation),
    error => error instanceof WorkspaceV2Error && error.code === 'invalid-design-agent-turn',
  );
});

test('the schema covers the full v2 document and explicitly preserves unknown fields', () => {
  const schema = JSON.parse(fs.readFileSync(path.join(repoRoot, 'schemas', 'workspace.schema.json'), 'utf8'));
  assert.equal(schema.properties.version.const, 2);
  assert.equal(schema.properties.schema_version.const, WORKSPACE_V2_SCHEMA_VERSION);
  assert.deepEqual(schema.required, [
    'version',
    'schema_version',
    'canvas',
    'nodes',
    'connections',
  ]);
  assert.equal(schema.additionalProperties, true);
  assert.equal(schema.$defs.canvas.additionalProperties, true);
  assert.equal(schema.$defs.node.additionalProperties, true);
  assert.equal(schema.$defs.node.properties.data.additionalProperties, true);
  assert.equal(schema.$defs.connection.additionalProperties, true);
  assert.equal(schema.$defs.designAgentConversation.additionalProperties, true);
  assert.equal(schema.$defs.designAgentTurn.additionalProperties, true);
  assert.deepEqual(new Set(schema.$defs.nodeType.enum), new Set(WORKSPACE_V2_NODE_TYPES));
  for (const legacyType of Object.keys(LEGACY_WORKSPACE_NODE_MIGRATIONS)) {
    assert.equal(schema.$defs.nodeType.enum.includes(legacyType), false);
  }
  assert.equal(schema.properties.designAgent.$ref, '#/$defs/designAgentConversation');
});

test('the pure migration snapshot matches every active runtime node port declaration', () => {
  const activeRuntimeTypes = Object.entries(NODE_TYPES)
    .filter(([, definition]) => definition.availability === 'active')
    .map(([nodeType]) => nodeType);
  assert.deepEqual(new Set(activeRuntimeTypes), new Set(WORKSPACE_V2_NODE_TYPES));

  const signature = definition => Object.fromEntries(['inputs', 'outputs'].map(direction => [
    direction,
    (definition[direction] || [])
      .map(({ port, type }) => `${port}:${type}`)
      .sort(),
  ]));
  for (const nodeType of activeRuntimeTypes) {
    assert.deepEqual(
      signature(WORKSPACE_V2_PORT_DECLARATIONS[nodeType]),
      signature(NODE_TYPES[nodeType]),
      `${nodeType} migration ports drifted from the runtime resolver declarations`,
    );
  }
});

console.log(`\nAll ${passed} Workspace v2 migration contract tests passed.`);
