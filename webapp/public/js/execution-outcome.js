// Structured result contract for every workflow operation. Status, work
// performed, output freshness, and scientific evidence are deliberately
// independent dimensions.

export const EXECUTION_OUTCOME_CONTRACT = 'bne-execution-outcome/v1';
export const EXECUTION_STATUSES = new Set([
  'succeeded',
  'blocked',
  'failed',
  'cancelled',
  'stale',
]);
export const EXECUTION_WORK = new Set(['executed', 'reused', 'none']);
export const OUTPUT_AVAILABILITY = new Set(['present', 'missing', 'historical']);

export class ExecutionContractError extends Error {
  constructor(message, details = {}) {
    super(`ExecutionOutcome contract violation: ${message}`);
    this.name = 'ExecutionContractError';
    this.code = 'execution_contract_violation';
    this.details = details;
  }
}

function plainObject(value) {
  return !!value && typeof value === 'object' && !Array.isArray(value);
}

function normalizeOutputs(outputs) {
  if (!plainObject(outputs)) {
    throw new ExecutionContractError('outputs must be an object');
  }
  const normalized = {};
  for (const [port, availability] of Object.entries(outputs)) {
    if (typeof port !== 'string' || !port || !OUTPUT_AVAILABILITY.has(availability)) {
      throw new ExecutionContractError(
        `invalid output availability for ${String(port)}: ${String(availability)}`,
      );
    }
    normalized[port] = availability;
  }
  return Object.freeze(normalized);
}

export function makeExecutionOutcome({
  nodeId,
  status,
  work = status === 'succeeded' ? 'executed' : 'none',
  outputs = {},
  code = null,
  message = null,
  evidence = null,
  details = null,
} = {}) {
  if (typeof nodeId !== 'string' || !nodeId) {
    throw new ExecutionContractError('nodeId must be a non-empty string');
  }
  if (!EXECUTION_STATUSES.has(status)) {
    throw new ExecutionContractError(`unknown status ${String(status)}`);
  }
  if (!EXECUTION_WORK.has(work)) {
    throw new ExecutionContractError(`unknown work classification ${String(work)}`);
  }
  if (status !== 'succeeded' && work !== 'none') {
    throw new ExecutionContractError(`${status} outcomes cannot claim ${work} work`);
  }
  if (code != null && (typeof code !== 'string' || !code)) {
    throw new ExecutionContractError('code must be null or a non-empty string');
  }
  if (message != null && typeof message !== 'string') {
    throw new ExecutionContractError('message must be null or a string');
  }
  return Object.freeze({
    contract: EXECUTION_OUTCOME_CONTRACT,
    nodeId,
    status,
    work,
    outputs: normalizeOutputs(outputs),
    code,
    message,
    evidence,
    details,
  });
}

export function succeededOutcome(nodeId, options = {}) {
  return makeExecutionOutcome({ nodeId, status: 'succeeded', ...options });
}

export function blockedOutcome(nodeId, options = {}) {
  return makeExecutionOutcome({ nodeId, status: 'blocked', ...options });
}

export function failedOutcome(nodeId, options = {}) {
  return makeExecutionOutcome({ nodeId, status: 'failed', ...options });
}

export function cancelledOutcome(nodeId, options = {}) {
  return makeExecutionOutcome({ nodeId, status: 'cancelled', ...options });
}

export function staleOutcome(nodeId, options = {}) {
  return makeExecutionOutcome({ nodeId, status: 'stale', ...options });
}

export function assertExecutionOutcome(value, { nodeId = null, nodeType = null } = {}) {
  if (!plainObject(value) || value.contract !== EXECUTION_OUTCOME_CONTRACT) {
    throw new ExecutionContractError(
      `${nodeType || 'node'} ${nodeId || ''} returned a non-ExecutionOutcome value`,
      { value },
    );
  }
  const normalized = makeExecutionOutcome(value);
  if (nodeId != null && normalized.nodeId !== nodeId) {
    throw new ExecutionContractError(
      `outcome owner ${normalized.nodeId} does not match executing node ${nodeId}`,
    );
  }
  return value;
}
