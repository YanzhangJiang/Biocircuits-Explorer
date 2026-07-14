// Pure Quick Add planning and an atomic graph-patch command core.
//
// This module deliberately has no imports from the DOM editor. Integration
// supplies an adapter for capture/stage/commit/restore and the live port
// validator. Keeping that boundary explicit prevents a partially constructed
// Quick Add chain from leaking into nodeRegistry, connections, or the native
// workspace snapshot.

export const QUICK_ADD_REACTION_SOURCE_TYPES = Object.freeze([
  'reaction-network',
  'network-id-definition',
  'design-target',
  'sbml-import',
]);

const REACTION_SOURCE_TYPES = new Set(QUICK_ADD_REACTION_SOURCE_TYPES);

const ANALYSIS_WORKFLOWS = Object.freeze({
  'siso-analysis': Object.freeze({ params: 'siso-params', result: 'siso-result' }),
  'rop-cloud': Object.freeze({
    params: 'rop-cloud-params',
    result: 'rop-cloud-result',
    needsReactions: true,
  }),
  'fret-heatmap': Object.freeze({ params: 'fret-params', result: 'fret-result' }),
  'parameter-scan-1d': Object.freeze({ params: 'scan-1d-params', result: 'scan-1d-result' }),
  'parameter-scan-2d': Object.freeze({ params: 'scan-2d-params', result: 'scan-2d-result' }),
  'rop-polyhedron': Object.freeze({ params: 'rop-poly-params', result: 'rop-poly-result' }),
  'parameter-placer': Object.freeze({ params: 'placer-params', result: 'placer-result' }),
});

export const AGENT_AUTO_SPAWN_ANALYSES = Object.freeze({
  'model-summary': Object.freeze({ kind: 'simple', result: 'model-summary' }),
  'vertices-table': Object.freeze({ kind: 'simple', result: 'vertices-table' }),
  'regime-graph': Object.freeze({ kind: 'simple', result: 'regime-graph' }),
  'siso-analysis': Object.freeze({ kind: 'pair', params: 'siso-params', result: 'siso-result' }),
  'parameter-scan-1d': Object.freeze({
    kind: 'pair', params: 'scan-1d-params', result: 'scan-1d-result',
  }),
  'parameter-scan-2d': Object.freeze({
    kind: 'pair', params: 'scan-2d-params', result: 'scan-2d-result',
  }),
  'rop-cloud': Object.freeze({
    kind: 'pair', params: 'rop-cloud-params', result: 'rop-cloud-result', needsReactions: true,
  }),
  'rop-polyhedron': Object.freeze({
    kind: 'pair', params: 'rop-poly-params', result: 'rop-poly-result',
  }),
  'fret-heatmap': Object.freeze({
    kind: 'pair', params: 'fret-params', result: 'fret-result',
  }),
});

const ATLAS_WORKFLOWS = new Set([
  'atlas-preview',
  'atlas-search',
  'atlas-workflow',
  'atlas-inverse-design',
]);

const DEFAULT_NODE_SIZES = Object.freeze({
  'ai-import': Object.freeze({ width: 340, height: 600 }),
  'reaction-network': Object.freeze({ width: 280, height: 300 }),
  'model-builder': Object.freeze({ width: 260, height: 200 }),
  'design-spec-config': Object.freeze({ width: 440, height: 300 }),
  'design-target': Object.freeze({ width: 460, height: 500 }),
  'placer-result': Object.freeze({ width: 480, height: 360 }),
  'model-summary': Object.freeze({ width: 380, height: 300 }),
  'vertices-table': Object.freeze({ width: 380, height: 360 }),
  'regime-graph': Object.freeze({ width: 840, height: 840 }),
  'atlas-spec': Object.freeze({ width: 420, height: 620 }),
  'atlas-builder': Object.freeze({ width: 460, height: 480 }),
  'atlas-query-config': Object.freeze({ width: 420, height: 700 }),
  'atlas-query-result': Object.freeze({ width: 640, height: 540 }),
  'atlas-inverse-result': Object.freeze({ width: 700, height: 620 }),
});

// nodes.js assigns every parameter/result category the `.viewer` class; keep
// planning geometry aligned with its CSS min-width so a nominal 60px gap does
// not collapse after the DOM applies layout constraints.
const DEFAULT_PARAMETER_SIZE = Object.freeze({ width: 380, height: 300 });
const DEFAULT_RESULT_SIZE = Object.freeze({ width: 420, height: 300 });
const DEFAULT_NODE_SIZE = Object.freeze({ width: 280, height: 220 });

function cloneValue(value) {
  if (typeof structuredClone === 'function') return structuredClone(value);
  return JSON.parse(JSON.stringify(value));
}

function deepFreeze(value) {
  if (!value || typeof value !== 'object' || Object.isFrozen(value)) return value;
  Object.freeze(value);
  for (const child of Object.values(value)) deepFreeze(child);
  return value;
}

function finiteOr(value, fallback) {
  return Number.isFinite(value) ? value : fallback;
}

function normalizePlannerGraph(value) {
  const source = value && typeof value === 'object' ? value : {};
  let nodes;
  if (Array.isArray(source.nodes)) {
    nodes = source.nodes.map(item => cloneValue(item));
  } else if (source.nodes && typeof source.nodes === 'object') {
    nodes = Object.entries(source.nodes).map(([id, item]) => ({
      id,
      ...(item && typeof item === 'object' ? cloneValue(item) : {}),
    }));
  } else {
    nodes = [];
  }
  const connections = Array.isArray(source.connections)
    ? source.connections.map(item => cloneValue(item))
    : [];
  return { nodes, connections };
}

function nodeSize(type, candidate = {}) {
  const known = DEFAULT_NODE_SIZES[type]
    || (String(type).endsWith('-params') || type === 'design-spec-config'
      ? DEFAULT_PARAMETER_SIZE
      : String(type).endsWith('-result')
        ? DEFAULT_RESULT_SIZE
        : DEFAULT_NODE_SIZE);
  return {
    width: finiteOr(candidate.width, known.width),
    height: finiteOr(candidate.height, known.height),
  };
}

function occupiedRectangle(candidate) {
  const size = nodeSize(candidate.type, candidate);
  return {
    id: candidate.id,
    x: finiteOr(candidate.x, 0),
    y: finiteOr(candidate.y, 0),
    width: size.width,
    height: size.height,
  };
}

function rectanglesOverlap(left, right) {
  return left.x < right.x + right.width && left.x + left.width > right.x
    && left.y < right.y + right.height && left.y + left.height > right.y;
}

function resolvePlannedY(occupied, type, x, desiredY) {
  const size = nodeSize(type);
  let y = finiteOr(desiredY, 150);
  for (let attempt = 0; attempt < 100; attempt += 1) {
    const candidate = { x, y, ...size };
    const collision = occupied.find(item => rectanglesOverlap(candidate, item));
    if (!collision) return y;
    y = collision.y + collision.height + 30;
  }
  return y;
}

function makeIdAllocator(nodes, requestedOrdinal) {
  const used = new Set(nodes.map(item => String(item.id || '')).filter(Boolean));
  let inferred = 1;
  for (const id of used) {
    const match = /^node-(\d+)$/.exec(id);
    if (match) inferred = Math.max(inferred, Number(match[1]) + 1);
  }
  let ordinal = Number.isInteger(requestedOrdinal) && requestedOrdinal > 0
    ? requestedOrdinal
    : inferred;

  return {
    take() {
      while (used.has(`node-${ordinal}`)) ordinal += 1;
      const id = `node-${ordinal}`;
      used.add(id);
      ordinal += 1;
      return id;
    },
    get nextOrdinal() { return ordinal; },
  };
}

function makeDiagnostic(code, message, details = {}) {
  return {
    ok: false,
    patch: null,
    diagnostic: {
      kind: details.kind || 'error',
      code,
      message,
      ...details,
    },
  };
}

function manualSelectionDiagnostic(code, reason, message, candidates) {
  const normalized = candidates.map(item => ({ id: item.id, type: item.type }));
  return makeDiagnostic(code, message, {
    kind: 'manual-selection',
    reason,
    candidates: normalized,
    candidateNodeIds: normalized.map(item => item.id),
  });
}

function makePatchResult(
  workflowType,
  nodes,
  connections,
  metadata,
  effects = [],
  label = `Quick Add: ${workflowType}`,
) {
  const createdNodeIds = nodes.map(item => item.id);
  return {
    ok: true,
    diagnostic: null,
    patch: {
      kind: 'graph-patch',
      label,
      nodes,
      connections,
      effects,
      metadata: {
        workflowType,
        createdNodeIds,
        ...metadata,
      },
    },
  };
}

function planAtlasWorkflow({ workflowType, nodes, occupied, allocator, anchor }) {
  const plannedNodes = [];
  const plannedConnections = [];
  const addNode = (type, x, desiredY) => {
    const y = resolvePlannedY(occupied, type, x, desiredY);
    const spec = { id: allocator.take(), type, x, y };
    plannedNodes.push(spec);
    occupied.push(occupiedRectangle(spec));
    return spec;
  };

  const spec = addNode('atlas-spec', anchor.x, anchor.y);

  if (workflowType === 'atlas-preview') {
    const builder = addNode('atlas-builder', spec.x + 480, spec.y);
    plannedConnections.push({
      fromNode: spec.id,
      fromPort: 'atlas-spec',
      toNode: builder.id,
      toPort: 'atlas-spec',
    });
    return makePatchResult(workflowType, plannedNodes, plannedConnections, {
      reusedNodeIds: [],
      nextNodeOrdinal: allocator.nextOrdinal,
    });
  }

  if (workflowType === 'atlas-inverse-design') {
    const query = addNode('atlas-query-config', spec.x + 480, spec.y);
    const result = addNode('atlas-inverse-result', query.x + 480, query.y);
    plannedConnections.push(
      { fromNode: spec.id, fromPort: 'atlas-spec', toNode: result.id, toPort: 'atlas-spec' },
      { fromNode: query.id, fromPort: 'atlas-query', toNode: result.id, toPort: 'atlas-query' },
    );
    return makePatchResult(workflowType, plannedNodes, plannedConnections, {
      reusedNodeIds: [],
      nextNodeOrdinal: allocator.nextOrdinal,
    });
  }

  const builder = addNode('atlas-builder', spec.x + 480, spec.y);
  const query = addNode('atlas-query-config', builder.x + 520, spec.y);
  const result = addNode('atlas-query-result', query.x + 460, query.y);
  plannedConnections.push(
    { fromNode: spec.id, fromPort: 'atlas-spec', toNode: builder.id, toPort: 'atlas-spec' },
    { fromNode: builder.id, fromPort: 'atlas', toNode: result.id, toPort: 'atlas' },
    { fromNode: query.id, fromPort: 'atlas-query', toNode: result.id, toPort: 'atlas-query' },
  );
  return makePatchResult(workflowType, plannedNodes, plannedConnections, {
    reusedNodeIds: [],
    nextNodeOrdinal: allocator.nextOrdinal,
  });
}

/**
 * Produce a Quick Add patch without reading or mutating editor globals.
 *
 * The caller passes a JSON-like graph snapshot and, when needed, the current
 * node-counter ordinal. Every new node receives its final ID during planning;
 * GraphPatchCommand therefore uses the same IDs on the first apply and redo.
 */
export function planQuickAddWorkflow(options = {}) {
  const workflowType = options.chainType || options.workflowType;
  const workflow = ANALYSIS_WORKFLOWS[workflowType];
  if (!workflow && !ATLAS_WORKFLOWS.has(workflowType)) {
    return makeDiagnostic(
      'unknown-quick-add-workflow',
      `Unknown Quick Add workflow: ${String(workflowType)}`,
    );
  }

  const sourceGraph = normalizePlannerGraph(options.graph || options.snapshot);
  const nodes = sourceGraph.nodes;
  const connections = sourceGraph.connections;
  const allocator = makeIdAllocator(nodes, options.nextNodeOrdinal);
  const occupied = nodes.map(occupiedRectangle);
  const anchor = {
    x: finiteOr(options.anchor?.x, 80),
    y: finiteOr(options.anchor?.y, 150),
  };

  if (ATLAS_WORKFLOWS.has(workflowType)) {
    return planAtlasWorkflow({ workflowType, nodes, occupied, allocator, anchor });
  }

  const sourceCandidates = nodes.filter(item => REACTION_SOURCE_TYPES.has(item.type));
  const createIsolatedSource = options.createIsolatedSource === true;
  if (createIsolatedSource && options.selectedSourceId != null) {
    return makeDiagnostic(
      'conflicting-source-selection',
      'Quick Add cannot both reuse a selected source and create an isolated source.',
    );
  }
  if (createIsolatedSource && options.selectedModelBuilderId != null) {
    return makeDiagnostic(
      'conflicting-model-builder-selection',
      'An isolated Quick Add source cannot reuse a model builder connected to another source.',
    );
  }

  let source = null;
  if (createIsolatedSource) {
    // Intentionally ignore every compatible live source. The fresh source is
    // allocated below with a stable ID and remains part of the same patch.
  } else if (options.selectedSourceId != null) {
    source = sourceCandidates.find(item => item.id === options.selectedSourceId) || null;
    if (!source) {
      return manualSelectionDiagnostic(
        'invalid-source-selection',
        'selected-source-is-not-compatible',
        `Selected node ${String(options.selectedSourceId)} is not a compatible reaction source.`,
        sourceCandidates,
      );
    }
  } else if (sourceCandidates.length > 1) {
    return manualSelectionDiagnostic(
      'manual-source-selection-required',
      'multiple-compatible-sources',
      'Quick Add found multiple compatible reaction sources; select one or explicitly create an isolated source.',
      sourceCandidates,
    );
  } else if (sourceCandidates.length === 1) {
    [source] = sourceCandidates;
  }

  const plannedNodes = [];
  const plannedConnections = [];
  const reusedNodeIds = [];
  let sourceWasCreated = false;
  const addNode = (type, x, desiredY) => {
    const y = resolvePlannedY(occupied, type, x, desiredY);
    const spec = { id: allocator.take(), type, x, y };
    plannedNodes.push(spec);
    occupied.push(occupiedRectangle(spec));
    return spec;
  };

  if (!source) {
    source = addNode('reaction-network', anchor.x, anchor.y);
    sourceWasCreated = true;
  } else {
    reusedNodeIds.push(source.id);
  }

  const connectedBuilders = nodes.filter(item => item.type === 'model-builder' &&
    connections.some(itemConnection =>
      itemConnection.fromNode === source.id && itemConnection.fromPort === 'reactions' &&
      itemConnection.toNode === item.id && itemConnection.toPort === 'reactions'));

  let builder = null;
  if (options.selectedModelBuilderId != null) {
    builder = connectedBuilders.find(item => item.id === options.selectedModelBuilderId) || null;
    if (!builder) {
      return manualSelectionDiagnostic(
        'invalid-model-builder-selection',
        'selected-builder-is-not-connected-to-source',
        `Selected model builder ${String(options.selectedModelBuilderId)} is not connected to ${source.id}.`,
        connectedBuilders,
      );
    }
  } else if (connectedBuilders.length > 1) {
    return manualSelectionDiagnostic(
      'manual-model-builder-selection-required',
      'multiple-compatible-model-builders',
      `Quick Add found multiple model builders connected to ${source.id}; select one before creating the workflow.`,
      connectedBuilders,
    );
  } else if (connectedBuilders.length === 1) {
    [builder] = connectedBuilders;
  }

  const sourcePosition = {
    x: finiteOr(source.x, anchor.x),
    y: finiteOr(source.y, anchor.y),
  };
  const sourceDimensions = nodeSize(source.type, source);
  if (!builder) {
    builder = addNode(
      'model-builder',
      sourcePosition.x + sourceDimensions.width + 60,
      sourcePosition.y,
    );
    plannedConnections.push({
      fromNode: source.id,
      fromPort: 'reactions',
      toNode: builder.id,
      toPort: 'reactions',
    });
  } else {
    reusedNodeIds.push(builder.id);
  }

  const builderPosition = {
    x: finiteOr(builder.x, sourcePosition.x + sourceDimensions.width + 60),
    y: finiteOr(builder.y, sourcePosition.y),
  };
  const builderDimensions = nodeSize(builder.type, builder);
  const downstreamCount = connections.filter(item =>
    item.fromNode === builder.id && item.fromPort === 'model').length;
  const params = addNode(
    workflow.params,
    builderPosition.x + builderDimensions.width + 60,
    builderPosition.y + downstreamCount * 50,
  );
  const paramsDimensions = nodeSize(params.type, params);
  const result = addNode(
    workflow.result,
    params.x + paramsDimensions.width + 60,
    params.y,
  );

  plannedConnections.push(
    { fromNode: builder.id, fromPort: 'model', toNode: params.id, toPort: 'model' },
    { fromNode: params.id, fromPort: 'params', toNode: result.id, toPort: 'params' },
  );
  if (workflow.needsReactions) {
    plannedConnections.push({
      fromNode: source.id,
      fromPort: 'reactions',
      toNode: params.id,
      toPort: 'reactions',
    });
  }

  const effects = [
    {
      id: `${builder.id}:auto-build`,
      kind: 'node-auto-build-if-needed',
      nodeId: builder.id,
      delay: 100,
    },
    {
      id: `${params.id}:auto-populate`,
      kind: 'node-execute-if-ready',
      nodeId: params.id,
      delay: 100,
    },
  ];

  return makePatchResult(workflowType, plannedNodes, plannedConnections, {
    sourceNodeId: source.id,
    sourceWasCreated,
    isolatedSourceCreated: sourceWasCreated && createIsolatedSource,
    modelBuilderNodeId: builder.id,
    paramsNodeId: params.id,
    resultNodeId: result.id,
    reusedNodeIds,
    nextNodeOrdinal: allocator.nextOrdinal,
  }, effects);
}

/** Plan the graph-only portion of Design Target's Build & Tune action. */
export function planDesignBuildAndTuneWorkflow(options = {}) {
  const sourceGraph = normalizePlannerGraph(options.graph || options.snapshot);
  const nodes = sourceGraph.nodes;
  const designNodeId = options.designNodeId;
  const designNode = nodes.find(item => item.id === designNodeId) || null;
  if (!designNode || designNode.type !== 'design-target') {
    return makeDiagnostic(
      'invalid-design-target-source',
      `Build & Tune requires an existing Design Target node: ${String(designNodeId)}`,
    );
  }

  const allocator = makeIdAllocator(nodes, options.nextNodeOrdinal);
  const occupied = nodes.map(occupiedRectangle);
  const plannedNodes = [];
  const addNode = (type, x, desiredY) => {
    const y = resolvePlannedY(occupied, type, x, desiredY);
    const spec = { id: allocator.take(), type, x, y };
    plannedNodes.push(spec);
    occupied.push(occupiedRectangle(spec));
    return spec;
  };

  const designSize = nodeSize(designNode.type, designNode);
  const designX = finiteOr(designNode.x, 80);
  const designY = finiteOr(designNode.y, 120);
  const builder = addNode('model-builder', designX + designSize.width + 60, designY);
  const builderSize = nodeSize(builder.type, builder);
  const params = addNode('placer-params', builder.x + builderSize.width + 60, builder.y);
  const paramsSize = nodeSize(params.type, params);
  const result = addNode('placer-result', params.x + paramsSize.width + 60, params.y);

  return makePatchResult(
    'design-build-and-tune',
    plannedNodes,
    [
      { fromNode: designNodeId, fromPort: 'reactions', toNode: builder.id, toPort: 'reactions' },
      { fromNode: builder.id, fromPort: 'model', toNode: params.id, toPort: 'model' },
      { fromNode: params.id, fromPort: 'params', toNode: result.id, toPort: 'params' },
    ],
    {
      designNodeId,
      modelBuilderNodeId: builder.id,
      paramsNodeId: params.id,
      resultNodeId: result.id,
      nextNodeOrdinal: allocator.nextOrdinal,
    },
    [],
    'Build & Tune',
  );
}

/** Plan the Agent-side DesignabilitySpec export without touching editor globals. */
export function planDesignSpecExportWorkflow(options = {}) {
  const spec = options.spec;
  if (!spec || typeof spec !== 'object' || Array.isArray(spec)) {
    return makeDiagnostic(
      'invalid-design-spec-export',
      'Design Spec export requires a DesignabilitySpec object.',
    );
  }
  if (spec.schema_version !== 'bne-designability/v1.0.0') {
    return makeDiagnostic(
      'unsupported-design-spec-version',
      `Unsupported DesignabilitySpec version: ${String(spec.schema_version)}`,
    );
  }

  const sourceGraph = normalizePlannerGraph(options.graph || options.snapshot);
  const nodes = sourceGraph.nodes;
  const allocator = makeIdAllocator(nodes, options.nextNodeOrdinal);
  const occupied = nodes.map(occupiedRectangle);
  const anchor = {
    x: finiteOr(options.anchor?.x, 90),
    y: finiteOr(options.anchor?.y, 100),
  };
  const plannedNodes = [];
  const addNode = (type, x, desiredY, initialization = null) => {
    const y = resolvePlannedY(occupied, type, x, desiredY);
    const nodeSpec = { id: allocator.take(), type, x, y };
    if (initialization) nodeSpec.initialization = cloneValue(initialization);
    plannedNodes.push(nodeSpec);
    occupied.push(occupiedRectangle(nodeSpec));
    return nodeSpec;
  };

  const configNode = addNode('design-spec-config', anchor.x, anchor.y, {
    kind: 'design-spec-config',
    spec: cloneValue(spec),
  });
  const configSize = nodeSize(configNode.type, configNode);
  const targetNode = addNode(
    'design-target',
    configNode.x + configSize.width + 60,
    configNode.y,
  );

  return makePatchResult(
    'design-spec-export',
    plannedNodes,
    [{
      fromNode: configNode.id,
      fromPort: 'designability-spec',
      toNode: targetNode.id,
      toPort: 'designability-spec',
    }],
    {
      specNodeId: configNode.id,
      designTargetNodeId: targetNode.id,
      nextNodeOrdinal: allocator.nextOrdinal,
    },
    [],
    'Export Design Spec',
  );
}

function validateAgentSpawnResult(result) {
  if (!result || typeof result !== 'object' || !Array.isArray(result.networks)) {
    return makeDiagnostic(
      'invalid-agent-spawn-result',
      'Agent auto-spawn requires a result with a networks array.',
    );
  }
  if (result.networks.length === 0) {
    return makeDiagnostic('empty-agent-spawn-result', 'Agent auto-spawn has no networks to create.');
  }
  for (let networkIndex = 0; networkIndex < result.networks.length; networkIndex += 1) {
    const network = result.networks[networkIndex];
    if (!network || typeof network !== 'object' || !Array.isArray(network.reactions) ||
        !Array.isArray(network.recommended_analyses) || network.reactions.length === 0) {
      return makeDiagnostic(
        'invalid-agent-network',
        `Agent network ${networkIndex} requires reactions and recommended_analyses arrays.`,
      );
    }
    for (let reactionIndex = 0; reactionIndex < network.reactions.length; reactionIndex += 1) {
      const reaction = network.reactions[reactionIndex];
      if (!reaction || typeof reaction.rule !== 'string' || !reaction.rule.trim() ||
          !Number.isFinite(reaction.kd) || reaction.kd <= 0) {
        return makeDiagnostic(
          'invalid-agent-reaction',
          `Agent reaction ${networkIndex}:${reactionIndex} requires a rule and positive finite Kd.`,
        );
      }
    }
    for (const analysis of network.recommended_analyses) {
      if (!analysis || typeof analysis.name !== 'string' ||
          !AGENT_AUTO_SPAWN_ANALYSES[analysis.name]) {
        return makeDiagnostic(
          'unsupported-agent-analysis',
          `Agent auto-spawn does not support analysis ${String(analysis?.name)}.`,
        );
      }
    }
  }
  return null;
}

/** Plan all nodes, initial data descriptors, connections, and delayed setup for Agent auto-spawn. */
export function planAgentAutoSpawnWorkflow(options = {}) {
  const invalidResult = validateAgentSpawnResult(options.result);
  if (invalidResult) return invalidResult;

  const sourceGraph = normalizePlannerGraph(options.graph || options.snapshot);
  const nodes = sourceGraph.nodes;
  const agentNodeId = options.agentNodeId;
  const agentNode = nodes.find(item => item.id === agentNodeId) || null;
  if (!agentNode || agentNode.type !== 'ai-import') {
    return makeDiagnostic(
      'invalid-agent-source',
      `Agent auto-spawn requires an existing AI Import node: ${String(agentNodeId)}`,
    );
  }

  const allocator = makeIdAllocator(nodes, options.nextNodeOrdinal);
  const occupied = nodes.map(occupiedRectangle);
  const plannedNodes = [];
  const plannedConnections = [];
  const effects = [];
  const modelBuilderNodeIds = [];
  const paramsNodeIds = [];
  const addNode = (type, x, desiredY, initialization = null) => {
    const y = resolvePlannedY(occupied, type, x, desiredY);
    const spec = { id: allocator.take(), type, x, y };
    if (initialization) spec.initialization = cloneValue(initialization);
    plannedNodes.push(spec);
    occupied.push(occupiedRectangle(spec));
    return spec;
  };

  const gap = 60;
  const agentSize = nodeSize(agentNode.type, agentNode);
  const agentX = finiteOr(agentNode.x, 0);
  const agentY = finiteOr(agentNode.y, 0);
  const reactionX = agentX + agentSize.width + gap;
  const builderX = reactionX + nodeSize('reaction-network').width + gap;
  const analysisX = builderX + nodeSize('model-builder').width + gap;
  let bandTopY = agentY;

  for (const network of options.result.networks) {
    const reactionNode = addNode('reaction-network', reactionX, bandTopY, {
      kind: 'reaction-network-rules',
      reactions: network.reactions.map(({ rule, kd }) => ({ rule, kd })),
    });
    const builder = addNode('model-builder', builderX, reactionNode.y);
    modelBuilderNodeIds.push(builder.id);
    plannedConnections.push({
      fromNode: reactionNode.id,
      fromPort: 'reactions',
      toNode: builder.id,
      toPort: 'reactions',
    });
    effects.push({
      id: `${builder.id}:auto-build`,
      kind: 'node-auto-build-if-needed',
      nodeId: builder.id,
      delay: 100,
    });

    let analysisY = builder.y;
    let bandBottomY = Math.max(
      reactionNode.y + nodeSize(reactionNode.type).height,
      builder.y + nodeSize(builder.type).height,
    );
    for (const analysis of network.recommended_analyses) {
      const definition = AGENT_AUTO_SPAWN_ANALYSES[analysis.name];
      if (definition.kind === 'simple') {
        const resultNode = addNode(definition.result, analysisX, analysisY);
        plannedConnections.push({
          fromNode: builder.id,
          fromPort: 'model',
          toNode: resultNode.id,
          toPort: 'model',
        });
        const bottom = resultNode.y + nodeSize(resultNode.type).height;
        analysisY = bottom + 30;
        bandBottomY = Math.max(bandBottomY, bottom);
        continue;
      }

      const params = addNode(definition.params, analysisX, analysisY, {
        kind: 'analysis-config',
        analysisName: analysis.name,
        analysis: cloneValue(analysis),
      });
      const paramsSize = nodeSize(params.type, params);
      const resultNode = addNode(definition.result, params.x + paramsSize.width + gap, params.y);
      paramsNodeIds.push(params.id);
      plannedConnections.push(
        { fromNode: builder.id, fromPort: 'model', toNode: params.id, toPort: 'model' },
        { fromNode: params.id, fromPort: 'params', toNode: resultNode.id, toPort: 'params' },
      );
      if (definition.needsReactions) {
        plannedConnections.push({
          fromNode: reactionNode.id,
          fromPort: 'reactions',
          toNode: params.id,
          toPort: 'reactions',
        });
      }
      effects.push({
        id: `${params.id}:auto-populate`,
        kind: 'node-execute-if-ready',
        nodeId: params.id,
        delay: 100,
      });
      const bottom = Math.max(
        params.y + nodeSize(params.type).height,
        resultNode.y + nodeSize(resultNode.type).height,
      );
      analysisY = bottom + 30;
      bandBottomY = Math.max(bandBottomY, bottom);
    }
    bandTopY = bandBottomY + gap;
  }

  return makePatchResult(
    'agent-auto-spawn',
    plannedNodes,
    plannedConnections,
    {
      agentNodeId,
      modelBuilderNodeIds,
      paramsNodeIds,
      spawnedNodeCount: plannedNodes.length,
      nextNodeOrdinal: allocator.nextOrdinal,
    },
    effects,
    'Agent Auto Spawn',
  );
}

export class GraphPatchError extends Error {
  constructor(code, message, options = {}) {
    super(message, options.cause ? { cause: options.cause } : undefined);
    this.name = 'GraphPatchError';
    this.code = code;
    if (options.details !== undefined) this.details = options.details;
  }
}

function assertObject(value, code, message) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    throw new GraphPatchError(code, message);
  }
}

function assertNonemptyString(value, code, message) {
  if (typeof value !== 'string' || !value.trim()) {
    throw new GraphPatchError(code, message);
  }
}

function assertNodeShape(spec, path) {
  assertObject(spec, 'invalid-node', `${path} must be an object`);
  assertNonemptyString(spec.id, 'invalid-node-id', `${path}.id must be a non-empty string`);
  assertNonemptyString(spec.type, 'invalid-node-type', `${path}.type must be a non-empty string`);
  for (const coordinate of ['x', 'y']) {
    if (spec[coordinate] != null && !Number.isFinite(spec[coordinate])) {
      throw new GraphPatchError(
        'invalid-node-geometry',
        `${path}.${coordinate} must be a finite number`,
      );
    }
  }
  for (const dimension of ['width', 'height']) {
    if (spec[dimension] != null &&
        (!Number.isFinite(spec[dimension]) || spec[dimension] < 0)) {
      throw new GraphPatchError(
        'invalid-node-geometry',
        `${path}.${dimension} must be a finite non-negative number`,
      );
    }
  }
}

function assertConnectionShape(spec, path) {
  assertObject(spec, 'invalid-connection', `${path} must be an object`);
  for (const field of ['fromNode', 'fromPort', 'toNode', 'toPort']) {
    assertNonemptyString(
      spec[field],
      'invalid-connection',
      `${path}.${field} must be a non-empty string`,
    );
  }
}

function connectionKey(spec) {
  return [spec.fromNode, spec.fromPort, spec.toNode, spec.toPort].join('\u0000');
}

function inputKey(spec) {
  return [spec.toNode, spec.toPort].join('\u0000');
}

/** Apply an additive patch to a JSON-like snapshot without mutating either. */
export function projectGraphPatch(snapshot, patch) {
  assertObject(snapshot, 'invalid-snapshot', 'Graph snapshot must be an object');
  assertObject(patch, 'invalid-patch', 'Graph patch must be an object');
  if (!Array.isArray(snapshot.nodes) || !Array.isArray(snapshot.connections)) {
    throw new GraphPatchError(
      'invalid-snapshot',
      'Graph snapshot must contain nodes and connections arrays',
    );
  }
  if (!Array.isArray(patch.nodes) || !Array.isArray(patch.connections)) {
    throw new GraphPatchError(
      'invalid-patch',
      'Graph patch must contain nodes and connections arrays',
    );
  }

  const next = cloneValue(snapshot);
  next.nodes = [...next.nodes, ...cloneValue(patch.nodes)];
  next.connections = [...next.connections, ...cloneValue(patch.connections)];

  const nodeIds = new Set();
  for (let index = 0; index < next.nodes.length; index += 1) {
    const spec = next.nodes[index];
    assertNodeShape(spec, `nodes[${index}]`);
    if (nodeIds.has(spec.id)) {
      throw new GraphPatchError('duplicate-node-id', `Graph contains duplicate node ID ${spec.id}`);
    }
    nodeIds.add(spec.id);
  }

  const connectionKeys = new Set();
  const occupiedInputs = new Set();
  for (let index = 0; index < next.connections.length; index += 1) {
    const spec = next.connections[index];
    assertConnectionShape(spec, `connections[${index}]`);
    if (!nodeIds.has(spec.fromNode) || !nodeIds.has(spec.toNode)) {
      throw new GraphPatchError(
        'connection-endpoint-missing',
        `Connection ${spec.fromNode}:${spec.fromPort} -> ${spec.toNode}:${spec.toPort} references a missing node`,
      );
    }
    const exactKey = connectionKey(spec);
    if (connectionKeys.has(exactKey)) {
      throw new GraphPatchError('duplicate-connection', 'Graph contains a duplicate connection');
    }
    connectionKeys.add(exactKey);

    const destination = inputKey(spec);
    if (occupiedInputs.has(destination)) {
      throw new GraphPatchError(
        'input-already-connected',
        `Input ${spec.toNode}:${spec.toPort} has more than one connection`,
      );
    }
    occupiedInputs.add(destination);
  }

  return next;
}

function stableComparable(value) {
  if (Array.isArray(value)) return value.map(stableComparable);
  if (value && typeof value === 'object') {
    return Object.keys(value).sort().reduce((result, key) => {
      result[key] = stableComparable(value[key]);
      return result;
    }, {});
  }
  return value;
}

function snapshotsEqual(left, right) {
  return JSON.stringify(stableComparable(left)) === JSON.stringify(stableComparable(right));
}

function runValidator(name, validator, value, context) {
  if (typeof validator !== 'function') return;
  const result = validator(value, context);
  if (result && typeof result.then === 'function') {
    throw new GraphPatchError(
      'async-validator-not-supported',
      `${name} validator must be synchronous`,
    );
  }
  if (result === false) {
    throw new GraphPatchError(
      `${name}-validation-failed`,
      `${name} validator rejected the graph patch`,
    );
  }
  if (result && typeof result === 'object' && result.ok === false) {
    throw new GraphPatchError(
      `${name}-validation-failed`,
      result.message || `${name} validator rejected the graph patch`,
      { details: result },
    );
  }
}

function requireAdapterFunction(adapter, primaryName, fallbackName = null) {
  const fn = adapter?.[primaryName] || (fallbackName ? adapter?.[fallbackName] : null);
  if (typeof fn !== 'function') {
    throw new TypeError(
      `GraphPatchCommand adapter requires ${primaryName}()${fallbackName ? ` or ${fallbackName}()` : ''}`,
    );
  }
  return fn.bind(adapter);
}

function makeAbortController() {
  if (typeof AbortController === 'function') return new AbortController();
  const signal = { aborted: false, reason: undefined };
  return {
    signal,
    abort(reason) {
      signal.aborted = true;
      signal.reason = reason;
    },
  };
}

/**
 * One undoable command for an additive graph patch.
 *
 * Adapter contract:
 *   captureSnapshot() -> {nodes: [], connections: [], ...}
 *   stageNode(spec, context) -> opaque handle                 (optional)
 *   stageConnection(spec, context) -> opaque handle          (optional)
 *   commit(transaction, context)                             (exactly once)
 *   restoreSnapshot(snapshot, context)
 *   discardStage(transaction, context)                       (optional)
 *
 * stageNode/stageConnection must keep their work outside the live graph. The
 * command nevertheless restores the captured snapshot if staging or commit
 * throws, which protects against an adapter that fails after partial work.
 */
export class GraphPatchCommand {
  constructor({
    patch,
    adapter,
    validators = {},
    deferredTasks = [],
    scheduler = {},
    onDeferredError = null,
    onEpochCancel = null,
    snapshotEquals = snapshotsEqual,
    label = null,
  } = {}) {
    assertObject(patch, 'invalid-patch', 'GraphPatchCommand requires a patch');
    // The command, rather than its caller, owns the planned IDs. Freezing the
    // private copy makes an accidental edit between apply/undo/redo fail fast.
    this.patch = deepFreeze(cloneValue(patch));
    this.kind = 'graph-patch';
    this.label = label || patch.label || 'Apply graph patch';

    this._adapter = adapter;
    this._captureSnapshot = requireAdapterFunction(adapter, 'captureSnapshot', 'capture');
    this._commit = requireAdapterFunction(adapter, 'commit');
    this._restoreSnapshot = requireAdapterFunction(adapter, 'restoreSnapshot', 'restore');
    this._stageNode = typeof adapter?.stageNode === 'function'
      ? adapter.stageNode.bind(adapter)
      : spec => cloneValue(spec);
    this._stageConnection = typeof adapter?.stageConnection === 'function'
      ? adapter.stageConnection.bind(adapter)
      : spec => cloneValue(spec);
    this._discardStage = typeof adapter?.discardStage === 'function'
      ? adapter.discardStage.bind(adapter)
      : null;

    this._validators = {
      snapshot: validators.snapshot,
      node: validators.node,
      connection: validators.connection,
      graph: validators.graph,
    };
    this._snapshotEquals = typeof snapshotEquals === 'function'
      ? snapshotEquals
      : snapshotsEqual;

    this._deferredTasks = deferredTasks.map((task, index) => {
      if (!task || typeof task !== 'object' || typeof task.run !== 'function') {
        throw new TypeError(`deferredTasks[${index}] requires a run() function`);
      }
      const delay = task.delay == null ? 0 : task.delay;
      if (!Number.isFinite(delay) || delay < 0) {
        throw new TypeError(`deferredTasks[${index}].delay must be a finite non-negative number`);
      }
      return { ...task, delay };
    });

    const setTimer = scheduler.setTimeout || globalThis.setTimeout;
    const clearTimer = scheduler.clearTimeout || globalThis.clearTimeout;
    if (typeof setTimer !== 'function' || typeof clearTimer !== 'function') {
      throw new TypeError('GraphPatchCommand scheduler requires setTimeout and clearTimeout');
    }
    this._setTimer = setTimer.bind(scheduler.setTimeout ? scheduler : globalThis);
    this._clearTimer = clearTimer.bind(scheduler.clearTimeout ? scheduler : globalThis);
    this._onDeferredError = typeof onDeferredError === 'function'
      ? onDeferredError
      : error => console.error('[graph-patch] deferred task failed', error);
    this._onEpochCancel = typeof onEpochCancel === 'function' ? onEpochCancel : null;

    this._applied = false;
    this._beforeSnapshot = null;
    this._afterSnapshot = null;
    this._epochCounter = 0;
    this._activeEpoch = null;
  }

  get applied() { return this._applied; }
  get epoch() { return this._activeEpoch?.active ? this._activeEpoch.id : null; }

  _newEpoch() {
    this._epochCounter += 1;
    const epoch = {
      id: this._epochCounter,
      active: true,
      controller: makeAbortController(),
      timers: new Set(),
      cleanups: new Set(),
    };
    this._activeEpoch = epoch;
    return epoch;
  }

  _isCurrentEpoch(epoch) {
    return this._applied && epoch?.active && this._activeEpoch === epoch;
  }

  _reportDeferredError(error, task, epoch) {
    try {
      this._onDeferredError(error, { task, epoch: epoch.id, patch: this.patch });
    } catch (reportError) {
      console.error('[graph-patch] deferred error reporter failed', reportError);
    }
  }

  _cancelEpoch(reason) {
    const epoch = this._activeEpoch;
    if (!epoch) return;
    epoch.active = false;
    this._activeEpoch = null;
    try {
      epoch.controller.abort(reason);
    } catch (error) {
      this._reportDeferredError(error, { id: 'epoch-abort' }, epoch);
    }
    for (const handle of epoch.timers) {
      try {
        this._clearTimer(handle);
      } catch (error) {
        this._reportDeferredError(error, { id: 'timer-cancel' }, epoch);
      }
    }
    epoch.timers.clear();
    for (const cleanup of epoch.cleanups) {
      try {
        cleanup();
      } catch (error) {
        this._reportDeferredError(error, { id: 'deferred-cleanup' }, epoch);
      }
    }
    epoch.cleanups.clear();
    if (this._onEpochCancel) {
      try {
        this._onEpochCancel(Object.freeze({
          reason,
          epoch: epoch.id,
          patch: this.patch,
        }));
      } catch (error) {
        this._reportDeferredError(error, { id: 'epoch-cancel-hook' }, epoch);
      }
    }
  }

  _registerCleanup(result, epoch, task) {
    if (typeof result !== 'function') return;
    if (this._isCurrentEpoch(epoch)) {
      epoch.cleanups.add(result);
      return;
    }
    try {
      result();
    } catch (error) {
      this._reportDeferredError(error, task, epoch);
    }
  }

  _scheduleDeferredTasks(epoch) {
    for (const task of this._deferredTasks) {
      let handle = null;
      const callback = () => {
        if (handle !== null) epoch.timers.delete(handle);
        if (!this._isCurrentEpoch(epoch)) return;
        const context = {
          epoch: epoch.id,
          signal: epoch.controller.signal,
          patch: this.patch,
          isCurrent: () => this._isCurrentEpoch(epoch),
        };
        try {
          const result = task.run(context);
          if (result && typeof result.then === 'function') {
            result.then(cleanup => {
              this._registerCleanup(cleanup, epoch, task);
            }).catch(error => {
              if (this._isCurrentEpoch(epoch)) this._reportDeferredError(error, task, epoch);
            });
          } else {
            this._registerCleanup(result, epoch, task);
          }
        } catch (error) {
          this._reportDeferredError(error, task, epoch);
        }
      };
      handle = this._setTimer(callback, task.delay);
      epoch.timers.add(handle);
    }
  }

  _discard(transaction, context, errors) {
    if (!this._discardStage || !transaction) return;
    try {
      this._discardStage(transaction, context);
    } catch (error) {
      errors.push(error);
    }
  }

  _throwWithCleanupErrors(primaryError, cleanupErrors, message) {
    if (cleanupErrors.length === 0) throw primaryError;
    throw new AggregateError(
      [primaryError, ...cleanupErrors],
      message,
      { cause: primaryError },
    );
  }

  _restoreAndVerify(snapshot, context) {
    this._restoreSnapshot(cloneValue(snapshot), context);
    const restored = cloneValue(this._captureSnapshot());
    if (!this._snapshotEquals(restored, snapshot)) {
      throw new GraphPatchError(
        'restore-snapshot-mismatch',
        'Graph adapter did not restore the requested snapshot',
      );
    }
  }

  apply() {
    if (this._applied) {
      throw new GraphPatchError('already-applied', 'Graph patch is already applied');
    }

    const currentSnapshot = cloneValue(this._captureSnapshot());
    if (this._beforeSnapshot && !this._snapshotEquals(currentSnapshot, this._beforeSnapshot)) {
      throw new GraphPatchError(
        'redo-base-mismatch',
        'Cannot redo graph patch because the graph no longer matches its pre-apply snapshot',
      );
    }

    const epoch = this._newEpoch();
    let transaction = null;
    let stagingStarted = false;
    let commitAttempted = false;

    try {
      const nextSnapshot = projectGraphPatch(currentSnapshot, this.patch);
      const newNodeIds = new Set(this.patch.nodes.map(item => item.id));
      const newConnectionKeys = new Set(this.patch.connections.map(connectionKey));
      const baseContext = {
        phase: this._beforeSnapshot ? 'redo' : 'apply',
        epoch: epoch.id,
        signal: epoch.controller.signal,
        patch: this.patch,
        beforeSnapshot: currentSnapshot,
        nextSnapshot,
      };

      runValidator('snapshot', this._validators.snapshot, currentSnapshot, baseContext);
      for (const spec of nextSnapshot.nodes) {
        runValidator('node', this._validators.node, spec, {
          ...baseContext,
          isNew: newNodeIds.has(spec.id),
        });
      }
      for (const spec of nextSnapshot.connections) {
        runValidator('connection', this._validators.connection, spec, {
          ...baseContext,
          isNew: newConnectionKeys.has(connectionKey(spec)),
        });
      }
      runValidator('graph', this._validators.graph, nextSnapshot, baseContext);

      transaction = {
        patch: cloneValue(this.patch),
        beforeSnapshot: cloneValue(currentSnapshot),
        nextSnapshot: cloneValue(nextSnapshot),
        stagedNodes: [],
        stagedConnections: [],
      };

      stagingStarted = true;
      for (const spec of this.patch.nodes) {
        transaction.stagedNodes.push(this._stageNode(cloneValue(spec), {
          ...baseContext,
          transaction,
        }));
      }
      for (const spec of this.patch.connections) {
        transaction.stagedConnections.push(this._stageConnection(cloneValue(spec), {
          ...baseContext,
          transaction,
        }));
      }

      commitAttempted = true;
      this._commit(transaction, baseContext);
      const committedSnapshot = cloneValue(this._captureSnapshot());
      if (!this._snapshotEquals(committedSnapshot, nextSnapshot)) {
        throw new GraphPatchError(
          'commit-snapshot-mismatch',
          'Graph patch commit did not produce the validated projected snapshot',
        );
      }

      if (!this._beforeSnapshot) this._beforeSnapshot = cloneValue(currentSnapshot);
      this._afterSnapshot = committedSnapshot;
      this._applied = true;
      this._scheduleDeferredTasks(epoch);
      return cloneValue(this.patch);
    } catch (error) {
      this._applied = false;
      this._cancelEpoch('apply-failed');
      const cleanupErrors = [];
      let restoreNeeded = true;
      try {
        restoreNeeded = !this._snapshotEquals(
          cloneValue(this._captureSnapshot()),
          currentSnapshot,
        );
      } catch (inspectionError) {
        cleanupErrors.push(inspectionError);
      }
      if (restoreNeeded) {
        try {
          // Validators and stage adapters are required to be side-effect free,
          // but a changed live snapshot is restored if either boundary leaks.
          this._restoreAndVerify(currentSnapshot, {
            phase: 'apply-rollback',
            patch: this.patch,
            stagingStarted,
            commitAttempted,
          });
        } catch (rollbackError) {
          cleanupErrors.push(rollbackError);
        }
      }
      this._discard(transaction, { phase: 'apply-discard', patch: this.patch }, cleanupErrors);
      this._throwWithCleanupErrors(
        error,
        cleanupErrors,
        'Graph patch apply failed and cleanup was incomplete',
      );
    }
  }

  revert() {
    if (!this._applied) return false;
    const currentSnapshot = cloneValue(this._captureSnapshot());
    const epochId = this.epoch;
    this._cancelEpoch('graph-patch-reverted');

    try {
      this._restoreAndVerify(this._beforeSnapshot, {
        phase: 'revert',
        epoch: epochId,
        patch: this.patch,
      });
    } catch (error) {
      const rollbackErrors = [];
      try {
        this._restoreAndVerify(currentSnapshot, {
          phase: 'revert-rollback',
          epoch: epochId,
          patch: this.patch,
        });
      } catch (rollbackError) {
        rollbackErrors.push(rollbackError);
      }
      this._throwWithCleanupErrors(
        error,
        rollbackErrors,
        'Graph patch revert failed and rollback was incomplete',
      );
    }

    this._applied = false;
    return true;
  }
}
