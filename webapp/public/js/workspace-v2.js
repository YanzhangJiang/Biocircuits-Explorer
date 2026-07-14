// Biocircuits Explorer — pure Workspace v1 -> v2 migration and validation.
//
// This module deliberately has no DOM, graph-state, timer, or network imports.
// It can therefore be shared by browser restore tests and native fixture tests.
// The declarations below are the frozen migration snapshot needed to interpret
// historical v1 documents without loading the browser application. Compatibility
// itself still goes through the one shared resolver from port-types.js.

import { portTypesCompatible, resolveNodePort } from './port-types.js';

export const WORKSPACE_V2_VERSION = 2;
export const WORKSPACE_V2_SCHEMA_VERSION = 'bne-workspace/v2.0.0';

const MIN_SCALE = 0.005;
const MAX_SCALE = 3;
const MAX_CANVAS_PAN = 1_000_000_000;

const MIGRATION_PORTS = Object.freeze({
  'markdown-note': ports(),
  'ai-import': ports(),
  'reaction-network': ports({}, { reactions: 'NetworkIR' }),
  'network-id-definition': ports({}, { reactions: 'NetworkIR', 'atlas-network': 'AtlasNetwork' }),
  'model-builder': ports({ reactions: 'NetworkIR' }, { model: 'ModelArtifact' }),
  'atlas-builder': ports({ 'atlas-spec': 'AtlasSpec' }, { atlas: 'AtlasArtifact' }),
  'siso-params': ports({ model: 'ModelArtifact' }, { params: 'SISOConfig' }),
  'siso-result': ports({ params: 'SISOConfig' }, { result: 'PathResult' }),
  'qk-poly-result': ports({ result: 'PathResult' }),
  'scan-1d-params': ports({ model: 'ModelArtifact' }, { params: 'Scan1DConfig' }),
  'scan-2d-params': ports({ model: 'ModelArtifact' }, { params: 'Scan2DConfig' }),
  'scan-1d-result': ports({ params: 'Scan1DConfig' }),
  'scan-2d-result': ports({ params: 'Scan2DConfig' }),
  'placer-params': ports({ model: 'ModelArtifact' }, { params: 'ParameterPlacerConfig' }),
  'placer-result': ports({ params: 'ParameterPlacerConfig' }),
  'design-spec-config': ports({}, { 'designability-spec': 'DesignabilitySpec' }),
  'design-target': ports(
    { 'designability-spec': 'DesignabilitySpec' },
    { reactions: 'NetworkIR', 'rop-shape-reference': 'ROPShapeReferenceArtifact' },
  ),
  'rop-cloud-params': ports(
    { reactions: 'NetworkIR', model: 'ModelArtifact' },
    { params: 'ROPCloudConfig' },
  ),
  'rop-cloud-result': ports({ params: 'ROPCloudConfig' }),
  'fret-params': ports({ model: 'ModelArtifact' }, { params: 'FRETConfig' }),
  'fret-result': ports({ params: 'FRETConfig' }),
  'rop-poly-params': ports({ model: 'ModelArtifact' }, { params: 'ROPPolyhedronConfig' }),
  'rop-poly-result': ports({ params: 'ROPPolyhedronConfig' }),
  'rop-shape-edit-config': ports(
    { 'rop-shape-reference': 'ROPShapeReferenceArtifact' },
    { 'rop-shape-request': 'ROPShapeRequestArtifact' },
  ),
  'rop-shape-result': ports(
    { 'rop-shape-request': 'ROPShapeRequestArtifact' },
    { 'rop-shape-result': 'ROPShapeResultArtifact' },
  ),
  'atlas-spec': ports({ 'atlas-network': 'AtlasNetwork' }, { 'atlas-spec': 'AtlasSpec' }),
  'atlas-query-config': ports({}, { 'atlas-query': 'AtlasQuery' }),
  'atlas-query-result': ports({ atlas: 'AtlasArtifact', 'atlas-query': 'AtlasQuery' }),
  'atlas-inverse-result': ports({
    'atlas-spec': 'AtlasSpec',
    atlas: 'AtlasArtifact',
    'atlas-query': 'AtlasQuery',
  }),
  'model-summary': ports({ model: 'ModelArtifact' }),
  'vertices-table': ports({ model: 'ModelArtifact' }),
  'regime-graph': ports({ model: 'ModelArtifact' }),
  'sbml-import': ports({}, { reactions: 'NetworkIR' }),
  'sbml-export': ports({ reactions: 'NetworkIR' }),
});

export const WORKSPACE_V2_PORT_DECLARATIONS = Object.freeze(Object.fromEntries(
  Object.entries(MIGRATION_PORTS).map(([nodeType, declaration]) => [nodeType, Object.freeze({
    inputs: Object.freeze(Object.entries(declaration.inputs).map(([port, type]) =>
      Object.freeze({ port, type }))),
    outputs: Object.freeze(Object.entries(declaration.outputs).map(([port, type]) =>
      Object.freeze({ port, type }))),
  })]),
));

export const WORKSPACE_V2_NODE_TYPES = Object.freeze(Object.keys(WORKSPACE_V2_PORT_DECLARATIONS));

const COMMON_RESULT_KEYS = new Set([
  'artifact',
  'certificate_grade',
  'evidence',
  'evidence_grade',
  'lifecycle',
  'provenance',
  'warnings',
]);

export const LEGACY_WORKSPACE_NODE_MIGRATIONS = Object.freeze({
  'siso-analysis': legacyPair('siso-params', 'siso-result', [
    'behaviorData',
    'overlayTrajectoryData',
    'selectedPath',
    'sisoPlotMode',
    'trajectoryData',
  ]),
  'parameter-scan-1d': legacyPair('scan-1d-params', 'scan-1d-result', [
    'scan1DResult',
    'scan1DResultMeta',
  ]),
  'parameter-scan-2d': legacyPair('scan-2d-params', 'scan-2d-result', [
    'scan2DResult',
    'scan2DResultMeta',
  ]),
  'rop-cloud': legacyPair('rop-cloud-params', 'rop-cloud-result', [
    'ropCloudData',
    'ropCloudPreset',
    'ropCloudRanges',
  ]),
  'fret-heatmap': legacyPair('fret-params', 'fret-result', ['fretHeatmapData']),
  'rop-polyhedron': legacyPair('rop-poly-params', 'rop-poly-result', [
    'fitInnerPoints',
    'ropPlotData',
  ]),
});

const DERIVED_DATA_KEYS = Object.freeze({
  'model-builder': ['modelContext'],
  'siso-result': ['behaviorData', 'overlayTrajectoryData', 'selectedPath', 'trajectoryData'],
  'qk-poly-result': ['polyhedronPayload', 'selection'],
  'scan-1d-result': ['scan1DResult'],
  'scan-2d-result': ['scan2DResult'],
  'placer-result': ['placerResult'],
  'design-target': ['config'],
  'rop-cloud-result': ['ropCloudData'],
  'fret-result': ['fretHeatmapData'],
  'rop-poly-result': ['ropPlotData'],
  'rop-shape-result': ['ropShapeResult'],
  'atlas-builder': ['atlasData'],
  'atlas-query-result': ['queryData'],
  'atlas-inverse-result': ['inverseDesignData'],
  'model-summary': ['summaryData'],
  'vertices-table': ['verticesData'],
  'regime-graph': ['graphData'],
});

const RUNTIME_FIELD_NAMES = new Set([
  'abortController',
  'backendSessionId',
  'backend_session_id',
  'executionTicket',
  'executionToken',
  'execution_ticket',
  'execution_token',
  'ownerEpoch',
  'ownerToken',
  'owner_epoch',
  'owner_token',
  'pollTimer',
  'poll_timer',
  'requestToken',
  'request_token',
  'runtimeSessionId',
  'runtime_session_id',
  'sessionId',
  'session_id',
  'timerId',
  'timer_id',
  'workspaceEpoch',
  'workspace_epoch',
]);

export class WorkspaceV2Error extends Error {
  constructor(code, message, path = '$', details = {}) {
    super(message);
    this.name = 'WorkspaceV2Error';
    this.code = code;
    this.path = path;
    this.details = details;
  }
}

function ports(inputs = {}, outputs = {}) {
  return Object.freeze({ inputs: Object.freeze(inputs), outputs: Object.freeze(outputs) });
}

function legacyPair(configType, resultType, resultKeys) {
  return Object.freeze({
    configType,
    resultType,
    resultKeys: Object.freeze([...resultKeys]),
  });
}

function diagnostic(code, severity, path, message, details = {}) {
  return Object.freeze({ code, severity, path, message, details });
}

function isRecord(value) {
  return !!value && typeof value === 'object' && !Array.isArray(value);
}

function cloneJson(value, path = '$', ancestors = new WeakSet()) {
  if (value === null || typeof value === 'string' || typeof value === 'boolean') return value;
  if (typeof value === 'number') {
    if (!Number.isFinite(value)) {
      throw new WorkspaceV2Error('non-json-number', `${path} must be a finite number`, path);
    }
    return value;
  }
  if (typeof value !== 'object') {
    throw new WorkspaceV2Error('non-json-value', `${path} is not JSON-compatible`, path);
  }
  if (ancestors.has(value)) {
    throw new WorkspaceV2Error('cyclic-document', `${path} contains a cycle`, path);
  }
  ancestors.add(value);
  try {
    if (Array.isArray(value)) {
      return value.map((entry, index) => cloneJson(entry, `${path}[${index}]`, ancestors));
    }
    const result = {};
    for (const [key, entry] of Object.entries(value)) {
      if (entry === undefined) continue;
      result[key] = cloneJson(entry, `${path}.${key}`, ancestors);
    }
    return result;
  } finally {
    ancestors.delete(value);
  }
}

function finiteNumber(value, fallback, path) {
  if (value == null) return fallback;
  if (typeof value !== 'number' || !Number.isFinite(value)) {
    throw new WorkspaceV2Error('invalid-number', `${path} must be a finite number`, path);
  }
  return value;
}

function normalizeCanvas(source) {
  if (source != null && !isRecord(source)) {
    throw new WorkspaceV2Error('invalid-canvas', 'canvas must be an object', '$.canvas');
  }
  const canvas = { ...(source || {}) };
  canvas.panX = finiteNumber(canvas.panX, 0, '$.canvas.panX');
  canvas.panY = finiteNumber(canvas.panY, 0, '$.canvas.panY');
  canvas.scale = finiteNumber(canvas.scale, 1, '$.canvas.scale');
  if (canvas.scale < MIN_SCALE || canvas.scale > MAX_SCALE) {
    throw new WorkspaceV2Error(
      'invalid-canvas-scale',
      `canvas.scale must be between ${MIN_SCALE} and ${MAX_SCALE}`,
      '$.canvas.scale',
    );
  }
  if (Math.abs(canvas.panX) > MAX_CANVAS_PAN || Math.abs(canvas.panY) > MAX_CANVAS_PAN) {
    throw new WorkspaceV2Error(
      'invalid-canvas-pan',
      `canvas pan must be between -${MAX_CANVAS_PAN} and ${MAX_CANVAS_PAN}`,
      '$.canvas',
    );
  }
  return canvas;
}

function normalizeNode(source, index, allowedTypes) {
  const path = `$.nodes[${index}]`;
  if (!isRecord(source)) {
    throw new WorkspaceV2Error('invalid-node', `${path} must be an object`, path);
  }
  if (typeof source.id !== 'string' || !source.id.trim()) {
    throw new WorkspaceV2Error('invalid-node-id', `${path}.id must be a non-empty string`, `${path}.id`);
  }
  if (typeof source.type !== 'string' || !allowedTypes.has(source.type)) {
    throw new WorkspaceV2Error(
      'unsupported-node-type',
      `${path}.type is not a supported node type: ${String(source.type)}`,
      `${path}.type`,
    );
  }
  if (source.data != null && !isRecord(source.data)) {
    throw new WorkspaceV2Error('invalid-node-data', `${path}.data must be an object`, `${path}.data`);
  }
  const node = {
    ...source,
    x: finiteNumber(source.x, 0, `${path}.x`),
    y: finiteNumber(source.y, 0, `${path}.y`),
    data: source.data || {},
  };
  for (const dimension of ['width', 'height']) {
    if (source[dimension] == null) continue;
    const value = finiteNumber(source[dimension], null, `${path}.${dimension}`);
    if (value < 0) {
      throw new WorkspaceV2Error(
        'invalid-node-dimension',
        `${path}.${dimension} must not be negative`,
        `${path}.${dimension}`,
      );
    }
    if (value === 0) delete node[dimension];
  }
  return node;
}

function normalizeDesignAgent(source) {
  if (!isRecord(source)) {
    throw new WorkspaceV2Error(
      'invalid-design-agent',
      'designAgent must be an object when present',
      '$.designAgent',
    );
  }
  if (!Array.isArray(source.convo)) {
    throw new WorkspaceV2Error(
      'invalid-design-agent-conversation',
      'designAgent.convo must be an array',
      '$.designAgent.convo',
    );
  }
  if (source.convo.length > 60) {
    throw new WorkspaceV2Error(
      'design-agent-conversation-too-long',
      'designAgent.convo cannot contain more than 60 turns',
      '$.designAgent.convo',
    );
  }
  source.convo.forEach((turn, index) => {
    const path = `$.designAgent.convo[${index}]`;
    if (!isRecord(turn) || !['user', 'agent'].includes(turn.role)) {
      throw new WorkspaceV2Error(
        'invalid-design-agent-turn',
        `${path} must have role user or agent`,
        path,
      );
    }
    if (turn.role === 'user' && typeof turn.text !== 'string') {
      throw new WorkspaceV2Error(
        'invalid-design-agent-turn',
        `${path}.text must be a string for a user turn`,
        `${path}.text`,
      );
    }
    if (turn.role === 'agent' && !isRecord(turn.res)) {
      throw new WorkspaceV2Error(
        'invalid-design-agent-turn',
        `${path}.res must be an object for an agent turn`,
        `${path}.res`,
      );
    }
  });
  if (!isRecord(source.chatState)) {
    throw new WorkspaceV2Error(
      'invalid-design-agent-state',
      'designAgent.chatState must be an object',
      '$.designAgent.chatState',
    );
  }
  return source;
}

function normalizeDocumentShape(source, sourceVersion, nodeTypes) {
  const allowedTypes = new Set(Object.entries(nodeTypes)
    .filter(([, definition]) => definition?.availability !== 'restore-only')
    .map(([nodeType]) => nodeType));
  if (sourceVersion === 1) {
    Object.keys(LEGACY_WORKSPACE_NODE_MIGRATIONS).forEach(type => allowedTypes.add(type));
  }
  if (!Array.isArray(source.nodes)) {
    throw new WorkspaceV2Error('missing-nodes', 'Workspace document must contain a nodes array', '$.nodes');
  }
  if (source.connections != null && !Array.isArray(source.connections)) {
    throw new WorkspaceV2Error(
      'invalid-connections',
      'Workspace connections must be an array',
      '$.connections',
    );
  }
  const seen = new Set();
  const nodes = source.nodes.map((entry, index) => {
    const node = normalizeNode(entry, index, allowedTypes);
    if (seen.has(node.id)) {
      throw new WorkspaceV2Error(
        'duplicate-node-id',
        `nodes[${index}].id duplicates workspace node ${node.id}`,
        `$.nodes[${index}].id`,
      );
    }
    seen.add(node.id);
    return node;
  });
  const normalized = {
    ...source,
    version: sourceVersion,
    canvas: normalizeCanvas(source.canvas),
    nodes,
    connections: source.connections || [],
  };
  if (Object.hasOwn(source, 'designAgent')) {
    normalized.designAgent = normalizeDesignAgent(source.designAgent);
  }
  return normalized;
}

function allocateNodeId(base, reserved) {
  let candidate = base;
  let suffix = 2;
  while (reserved.has(candidate)) {
    candidate = `${base}-${suffix}`;
    suffix += 1;
  }
  reserved.add(candidate);
  return candidate;
}

function splitLegacyData(data, mapping) {
  const resultKeys = new Set([...mapping.resultKeys, ...COMMON_RESULT_KEYS]);
  const configData = {};
  const resultData = {};
  for (const [key, value] of Object.entries(data)) {
    if (resultKeys.has(key)) resultData[key] = value;
    else configData[key] = value;
  }
  return { configData, resultData };
}

function hasStoredDerivedResult(node) {
  const keys = DERIVED_DATA_KEYS[node.type];
  if (!keys) return false;
  if (node.type === 'design-target') {
    return !!node.data?.config?.resolvedDefinition || node.data?.config?.selectedNid != null;
  }
  return keys.some(key => node.data?.[key] != null);
}

function markHistorical(data) {
  const existingEvidence = isRecord(data.lifecycle) && Object.hasOwn(data.lifecycle, 'evidence')
    ? data.lifecycle.evidence
    : Object.hasOwn(data, 'evidence')
      ? data.evidence
      : null;
  data.lifecycle = {
    state: 'historical',
    freshness: 'historical',
    evidence: cloneJson(existingEvidence, '$.lifecycle.evidence'),
  };
  if (isRecord(data.selectionLifecycle)) {
    data.selectionLifecycle = {
      state: 'historical',
      freshness: 'historical',
      evidence: cloneJson(
        Object.hasOwn(data.selectionLifecycle, 'evidence')
          ? data.selectionLifecycle.evidence
          : null,
        '$.selectionLifecycle.evidence',
      ),
    };
  }
  for (const metaKey of ['scan1DResultMeta', 'scan2DResultMeta']) {
    if (isRecord(data[metaKey])) data[metaKey].historical = true;
  }
}

function stripRuntimeFields(value, path, strippedPaths) {
  if (Array.isArray(value)) {
    value.forEach((entry, index) => stripRuntimeFields(entry, `${path}[${index}]`, strippedPaths));
    return;
  }
  if (!isRecord(value)) return;
  for (const key of Object.keys(value)) {
    const childPath = `${path}.${key}`;
    if (RUNTIME_FIELD_NAMES.has(key)) {
      delete value[key];
      strippedPaths.push(childPath);
      continue;
    }
    stripRuntimeFields(value[key], childPath, strippedPaths);
  }
}

function expandLegacyNodes(document, diagnostics) {
  const reserved = new Set(document.nodes.map(node => node.id));
  const internalConnections = [];
  const nodes = [];
  for (const node of document.nodes) {
    const mapping = LEGACY_WORKSPACE_NODE_MIGRATIONS[node.type];
    if (!mapping) {
      nodes.push(node);
      continue;
    }
    const resultId = allocateNodeId(`${node.id}--result`, reserved);
    const { configData, resultData } = splitLegacyData(node.data, mapping);
    const originalType = node.type;
    const configNode = { ...node, type: mapping.configType, data: configData };
    const horizontalOffset = (Number.isFinite(node.width) && node.width > 0 ? node.width : 420) + 40;
    const resultNode = {
      id: resultId,
      type: mapping.resultType,
      x: node.x + horizontalOffset,
      y: node.y,
      data: resultData,
    };
    if (Object.keys(resultData).length > 0) markHistorical(resultData);
    nodes.push(configNode, resultNode);
    internalConnections.push({
      fromNode: configNode.id,
      fromPort: 'params',
      toNode: resultNode.id,
      toPort: 'params',
    });
    diagnostics.push(diagnostic(
      'legacy-node-expanded',
      'warning',
      `$.nodes[id=${JSON.stringify(node.id)}]`,
      `Expanded legacy ${originalType} into ${mapping.configType} and ${mapping.resultType}`,
      {
        sourceType: originalType,
        configType: mapping.configType,
        configNodeId: configNode.id,
        resultType: mapping.resultType,
        resultNodeId: resultNode.id,
        conservative: true,
        preservedConfigFields: Object.keys(configData).sort(),
        preservedResultFields: Object.keys(resultData).filter(key => key !== 'lifecycle').sort(),
        mappingPolicy: 'Unclassified legacy fields stay on the config node; no result data is fabricated.',
      },
    ));
  }
  document.nodes = nodes;
  document.connections.push(...internalConnections);
}

function connectionFailure(connection, nodeById, nodeTypes) {
  if (!isRecord(connection)) {
    return ['connection-dropped-invalid-shape', 'Connection must be an object', {}];
  }
  for (const field of ['fromNode', 'fromPort', 'toNode', 'toPort']) {
    if (typeof connection[field] !== 'string' || !connection[field]) {
      return [
        'connection-dropped-invalid-shape',
        `Connection ${field} must be a non-empty string`,
        { field },
      ];
    }
  }
  if (connection.fromNode === connection.toNode) {
    return ['connection-dropped-self-loop', 'A node cannot connect to itself', {}];
  }
  const fromNode = nodeById.get(connection.fromNode);
  const toNode = nodeById.get(connection.toNode);
  if (!fromNode || !toNode) {
    return [
      'connection-dropped-missing-endpoint',
      'Connection references a missing node',
      { missing: [!fromNode ? connection.fromNode : null, !toNode ? connection.toNode : null].filter(Boolean) },
    ];
  }
  const output = resolveNodePort(
    nodeTypes,
    fromNode.type,
    'output',
    connection.fromPort,
  );
  if (!output) {
    return [
      'connection-dropped-invalid-output-port',
      `Unknown output ${fromNode.type}.${connection.fromPort}`,
      { nodeType: fromNode.type, port: connection.fromPort },
    ];
  }
  const input = resolveNodePort(
    nodeTypes,
    toNode.type,
    'input',
    connection.toPort,
  );
  if (!input) {
    return [
      'connection-dropped-invalid-input-port',
      `Unknown input ${toNode.type}.${connection.toPort}`,
      { nodeType: toNode.type, port: connection.toPort },
    ];
  }
  if (!portTypesCompatible(output.type, input.type)) {
    return [
      'connection-dropped-incompatible-port-types',
      `Port type mismatch: ${output.type} cannot connect to ${input.type}`,
      { outputType: output.type, inputType: input.type },
    ];
  }
  return null;
}

function normalizeConnections(document, diagnostics, nodeTypes) {
  const nodeById = new Map(document.nodes.map(node => [node.id, node]));
  const seen = new Set();
  const kept = [];
  document.connections.forEach((connection, index) => {
    const path = `$.connections[${index}]`;
    const failure = connectionFailure(connection, nodeById, nodeTypes);
    if (failure) {
      diagnostics.push(diagnostic(failure[0], 'warning', path, failure[1], {
        connection,
        ...failure[2],
      }));
      return;
    }
    const key = [connection.fromNode, connection.fromPort, connection.toNode, connection.toPort]
      .join('\u0000');
    if (seen.has(key)) {
      diagnostics.push(diagnostic(
        'connection-dropped-duplicate',
        'warning',
        path,
        'Dropped duplicate semantic connection',
        { connection },
      ));
      return;
    }
    seen.add(key);
    kept.push(connection);
  });
  document.connections = kept;
}

function normalizeRestoredResults(document, diagnostics) {
  const historicalNodeIds = [];
  for (const node of document.nodes) {
    if (node.type === 'model-builder') delete node.data.built;
    const hasPersistedCurrentState = ['lifecycle', 'selectionLifecycle'].some(key => {
      const lifecycle = node.data?.[key];
      return isRecord(lifecycle) && (
        lifecycle.freshness === 'current' ||
        lifecycle.freshness === 'historical' ||
        lifecycle.state === 'running'
      );
    });
    if (!hasStoredDerivedResult(node) && !hasPersistedCurrentState) continue;
    markHistorical(node.data);
    historicalNodeIds.push(node.id);
  }
  if (historicalNodeIds.length > 0) {
    diagnostics.push(diagnostic(
      'derived-results-marked-historical',
      'info',
      '$.nodes',
      'Restored derived results require a fresh execution before reuse',
      { nodeIds: historicalNodeIds },
    ));
  }
}

function validateV2LifecycleRecords(document) {
  const states = new Set([
    'empty',
    'running',
    'current',
    'failed',
    'blocked',
    'invalidated',
    'historical',
  ]);
  const freshnessValues = new Set(['empty', 'current', 'invalidated', 'historical']);
  document.nodes.forEach((node, index) => {
    for (const key of ['lifecycle', 'selectionLifecycle']) {
      if (!Object.hasOwn(node.data, key)) continue;
      const lifecycle = node.data[key];
      const path = `$.nodes[${index}].data.${key}`;
      if (!isRecord(lifecycle) || !states.has(lifecycle.state) ||
          !freshnessValues.has(lifecycle.freshness) || !Object.hasOwn(lifecycle, 'evidence')) {
        throw new WorkspaceV2Error(
          'invalid-execution-lifecycle',
          `${path} must contain a known state, freshness, and evidence field`,
          path,
        );
      }
    }
  });
}

function sourceVersionOf(source) {
  const version = source.version == null ? 1 : source.version;
  if (!Number.isInteger(version)) {
    throw new WorkspaceV2Error('invalid-version', 'Workspace version must be an integer', '$.version');
  }
  if (version < 1) {
    throw new WorkspaceV2Error(
      'unsupported-version',
      `Unsupported workspace version: ${version}`,
      '$.version',
      { supportedVersion: WORKSPACE_V2_VERSION },
    );
  }
  if (version > WORKSPACE_V2_VERSION) {
    throw new WorkspaceV2Error(
      'future-version',
      `Workspace version ${version} is newer than this app supports (${WORKSPACE_V2_VERSION})`,
      '$.version',
      { supportedVersion: WORKSPACE_V2_VERSION },
    );
  }
  return version;
}

export function validateWorkspaceV2Document(
  source,
  { nodeTypes = WORKSPACE_V2_PORT_DECLARATIONS } = {},
) {
  if (!isRecord(source)) {
    throw new WorkspaceV2Error('invalid-document', 'Workspace document must be an object', '$');
  }
  const copy = cloneJson(source);
  const version = sourceVersionOf(copy);
  if (version !== WORKSPACE_V2_VERSION) {
    throw new WorkspaceV2Error(
      'migration-required',
      `Workspace version ${version} must be migrated to version ${WORKSPACE_V2_VERSION}`,
      '$.version',
    );
  }
  if (copy.schema_version !== WORKSPACE_V2_SCHEMA_VERSION) {
    throw new WorkspaceV2Error(
      'invalid-schema-version',
      `Workspace schema_version must be ${WORKSPACE_V2_SCHEMA_VERSION}`,
      '$.schema_version',
      { supportedSchemaVersion: WORKSPACE_V2_SCHEMA_VERSION },
    );
  }
  const document = normalizeDocumentShape(copy, version, nodeTypes);
  validateV2LifecycleRecords(document);
  const diagnostics = [];
  normalizeConnections(document, diagnostics, nodeTypes);
  if (diagnostics.length > 0) {
    const first = diagnostics[0];
    throw new WorkspaceV2Error(
      'invalid-v2-connection',
      `Workspace v2 contains an invalid connection: ${first.message}`,
      first.path,
      { diagnostic: first },
    );
  }
  return document;
}

export function migrateWorkspaceDocument(
  source,
  { nodeTypes = WORKSPACE_V2_PORT_DECLARATIONS } = {},
) {
  if (!isRecord(source)) {
    throw new WorkspaceV2Error('invalid-document', 'Workspace document must be an object', '$');
  }
  const copy = cloneJson(source);
  const sourceVersion = sourceVersionOf(copy);
  const diagnostics = [];
  if (sourceVersion === WORKSPACE_V2_VERSION && copy.schema_version != null &&
      copy.schema_version !== WORKSPACE_V2_SCHEMA_VERSION) {
    throw new WorkspaceV2Error(
      'invalid-schema-version',
      `Workspace schema_version must be ${WORKSPACE_V2_SCHEMA_VERSION}`,
      '$.schema_version',
      { supportedSchemaVersion: WORKSPACE_V2_SCHEMA_VERSION },
    );
  }
  const document = normalizeDocumentShape(copy, sourceVersion, nodeTypes);

  const strippedPaths = [];
  stripRuntimeFields(document, '$', strippedPaths);
  if (strippedPaths.length > 0) {
    diagnostics.push(diagnostic(
      'runtime-fields-stripped',
      'warning',
      '$',
      'Removed transient runtime/session fields from the persisted workspace',
      { paths: strippedPaths.sort() },
    ));
  }

  if (sourceVersion === 1) {
    expandLegacyNodes(document, diagnostics);
    diagnostics.unshift(diagnostic(
      'workspace-version-migrated',
      'info',
      '$.version',
      'Migrated Workspace document from version 1 to version 2',
      { fromVersion: 1, toVersion: WORKSPACE_V2_VERSION },
    ));
  }

  normalizeRestoredResults(document, diagnostics);
  normalizeConnections(document, diagnostics, nodeTypes);
  document.version = WORKSPACE_V2_VERSION;
  document.schema_version = WORKSPACE_V2_SCHEMA_VERSION;
  validateWorkspaceV2Document(document, { nodeTypes });
  return { document, diagnostics };
}
