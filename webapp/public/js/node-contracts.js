import {
  assertExecutionOutcome,
  failedOutcome,
  succeededOutcome,
} from './execution-outcome.js';

export const NODE_ROLES = new Set(['source', 'config', 'compute', 'manual-gate', 'viewer']);

// Exhaustive architectural inventory. UI category remains presentation-only;
// role, availability, and execution mode are behavior contracts.
export const NODE_CONTRACTS = Object.freeze({
  'markdown-note': contract('viewer'),
  'ai-import': contract('source', 'manual', { adapter: 'boolean', capabilities: ['external-provider', 'manual-trigger'] }),
  'reaction-network': contract('source'),
  'network-id-definition': contract('source'),
  'model-builder': contract('compute', 'execute', { adapter: 'boolean', outputs: { model: 'present' } }),
  'atlas-builder': contract('compute', 'execute', { adapter: 'value', outputs: { atlas: 'present' } }),
  'siso-params': contract('config', 'prepare', { adapter: 'void' }),
  'siso-result': contract('compute', 'execute', { adapter: 'structured' }),
  'qk-poly-result': contract('compute', 'execute', { adapter: 'structured' }),
  'siso-analysis': restoreOnly(),
  'scan-1d-params': contract('config', 'prepare', { adapter: 'void' }),
  'scan-2d-params': contract('config', 'prepare', { adapter: 'void' }),
  'scan-1d-result': contract('compute', 'execute', { adapter: 'boolean' }),
  'scan-2d-result': contract('compute', 'execute', { adapter: 'boolean' }),
  'parameter-scan-1d': restoreOnly(),
  'parameter-scan-2d': restoreOnly(),
  'placer-params': contract('config', 'prepare', { adapter: 'void' }),
  'placer-result': contract('compute', 'execute', { adapter: 'boolean' }),
  'design-spec-config': contract('config', 'prepare', { adapter: 'value', outputs: { 'designability-spec': 'present' } }),
  'design-target': contract('manual-gate', 'manual', { adapter: 'structured' }),
  'rop-cloud-params': contract('config', 'prepare', { adapter: 'void' }),
  'rop-cloud-result': contract('compute', 'execute', { adapter: 'boolean' }),
  'rop-cloud': restoreOnly(),
  'fret-params': contract('config', 'prepare', { adapter: 'void' }),
  'fret-result': contract('compute', 'execute', { adapter: 'boolean' }),
  'fret-heatmap': restoreOnly(),
  'rop-poly-params': contract('config', 'prepare', { adapter: 'void' }),
  'rop-poly-result': contract('compute', 'execute', { adapter: 'boolean' }),
  'rop-polyhedron': restoreOnly(),
  'rop-shape-edit-config': contract('config', 'prepare', { adapter: 'value', outputs: { 'rop-shape-request': 'present' } }),
  'rop-shape-result': contract('compute', 'execute', { adapter: 'value', outputs: { 'rop-shape-result': 'present' } }),
  'atlas-spec': contract('config'),
  'atlas-query-config': contract('config'),
  'atlas-query-result': contract('compute', 'execute', { adapter: 'value' }),
  'atlas-inverse-result': contract('compute', 'execute', { adapter: 'value' }),
  'model-summary': contract('compute', 'execute', { adapter: 'boolean' }),
  'vertices-table': contract('compute', 'execute', { adapter: 'boolean' }),
  'regime-graph': contract('compute', 'execute', { adapter: 'boolean' }),
  'sbml-import': contract('source'),
  'sbml-export': contract('manual-gate', 'manual', { adapter: 'boolean', capabilities: ['external-side-effect'] }),
});

function contract(role, mode = 'none', extra = {}) {
  return Object.freeze({
    role,
    availability: 'active',
    execution: Object.freeze({ mode, adapter: extra.adapter || null }),
    outputs: Object.freeze({ ...(extra.outputs || {}) }),
    capabilities: Object.freeze([...(extra.capabilities || [])]),
  });
}

function restoreOnly() {
  return Object.freeze({
    role: 'compute',
    availability: 'restore-only',
    execution: Object.freeze({ mode: 'restore-only', adapter: null }),
    outputs: Object.freeze({}),
    capabilities: Object.freeze(['legacy-merged-node']),
  });
}

function outcomeFromAdapter(nodeId, nodeType, descriptor, raw) {
  if (raw?.contract) return assertExecutionOutcome(raw, { nodeId, nodeType });
  const adapter = descriptor.execution.adapter;
  const options = { outputs: descriptor.outputs };
  if (adapter === 'boolean') {
    return raw === true
      ? succeededOutcome(nodeId, options)
      : failedOutcome(nodeId, {
        code: 'operation_unsuccessful',
        message: `${nodeType} did not complete successfully`,
      });
  }
  if (adapter === 'value') {
    return raw != null && raw !== false
      ? succeededOutcome(nodeId, options)
      : failedOutcome(nodeId, {
        code: 'operation_missing_result',
        message: `${nodeType} did not return its required result`,
      });
  }
  if (adapter === 'void' && raw === undefined) {
    return succeededOutcome(nodeId, options);
  }
  return assertExecutionOutcome(raw, { nodeId, nodeType });
}

function wrapOperation(nodeType, descriptor, operation) {
  return async function structuredNodeOperation(nodeId, options = {}) {
    try {
      const raw = await operation(nodeId, options);
      return outcomeFromAdapter(nodeId, nodeType, descriptor, raw);
    } catch (error) {
      return failedOutcome(nodeId, {
        code: error?.code === 'execution_contract_violation'
          ? 'execution_contract_violation'
          : 'operation_threw',
        message: error?.message || String(error),
        details: { name: error?.name || 'Error' },
      });
    }
  };
}

export function applyNodeContracts(rawNodeTypes) {
  const rawKeys = Object.keys(rawNodeTypes || {}).sort();
  const contractKeys = Object.keys(NODE_CONTRACTS).sort();
  if (JSON.stringify(rawKeys) !== JSON.stringify(contractKeys)) {
    throw new Error('Node contract inventory does not match NODE_TYPES');
  }

  const decorated = {};
  for (const [nodeType, definition] of Object.entries(rawNodeTypes)) {
    const descriptor = NODE_CONTRACTS[nodeType];
    const next = {
      ...definition,
      role: descriptor.role,
      availability: descriptor.availability,
      capabilities: descriptor.capabilities,
      execution: descriptor.execution,
    };
    if (descriptor.availability === 'restore-only') {
      delete next.execute;
      delete next.prepare;
    } else if (descriptor.execution.mode === 'prepare') {
      if (typeof definition.prepare === 'function') {
        next.prepare = wrapOperation(nodeType, descriptor, definition.prepare);
      }
      delete next.execute;
    } else if (descriptor.execution.mode === 'execute' || descriptor.execution.mode === 'manual') {
      if (typeof definition.execute === 'function') {
        next.execute = wrapOperation(nodeType, descriptor, definition.execute);
      }
    }
    decorated[nodeType] = next;
  }
  return decorated;
}
