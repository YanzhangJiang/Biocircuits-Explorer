// DOM-free lifecycle core for derived node results. Persist only the
// state/freshness/evidence snapshot; runtime owners, tickets, sessions, and
// verified reusable-artifact handles deliberately live outside that shape.

export const DERIVED_RESULT_LIFECYCLE_CONTRACT = 'bne-derived-result-lifecycle/v1';
export const DERIVED_RESULT_STATES = Object.freeze([
  'empty',
  'running',
  'current',
  'failed',
  'blocked',
  'invalidated',
  'historical',
]);
export const RESULT_FRESHNESS = Object.freeze([
  'empty',
  'current',
  'invalidated',
  'historical',
]);

const LIFECYCLE_RUNTIME = new WeakMap();
const TICKET_RUNTIME = new WeakMap();
const SHA256_HEX = /^[0-9a-f]{64}$/i;

export class LifecycleContractError extends Error {
  constructor(message) {
    super(`Derived result lifecycle contract violation: ${message}`);
    this.name = 'LifecycleContractError';
    this.code = 'derived_result_lifecycle_contract_violation';
  }
}

function isPlainObject(value) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return false;
  const prototype = Object.getPrototypeOf(value);
  return prototype === Object.prototype || prototype === null;
}

function requirePlainObject(value, label) {
  if (!isPlainObject(value)) throw new LifecycleContractError(`${label} must be an object`);
  return value;
}

function validWorkspaceEpoch(value) {
  return (typeof value === 'string' && value.trim().length > 0) ||
    (Number.isSafeInteger(value) && value >= 0);
}

function requireContext(context, label = 'current context') {
  requirePlainObject(context, label);
  if (!isPlainObject(context.owner)) {
    throw new LifecycleContractError(`${label}.owner must be a node object`);
  }
  if (!validWorkspaceEpoch(context.workspaceEpoch)) {
    throw new LifecycleContractError(
      `${label}.workspaceEpoch must be a non-empty string or non-negative safe integer`,
    );
  }
  if (typeof context.inputFingerprint !== 'string' || !context.inputFingerprint.trim()) {
    throw new LifecycleContractError(`${label}.inputFingerprint must be a non-empty string`);
  }
  if (typeof context.endpoint !== 'string' || !context.endpoint.trim()) {
    throw new LifecycleContractError(`${label}.endpoint must be a non-empty string`);
  }
  return context;
}

function requireReason(value, label) {
  if (typeof value !== 'string' || !value.trim()) {
    throw new LifecycleContractError(`${label} must be a non-empty string`);
  }
  return value;
}

function cloneSerializable(value, label, ancestors = new Set()) {
  if (value === null || typeof value === 'string' || typeof value === 'boolean') return value;
  if (typeof value === 'number') {
    if (!Number.isFinite(value)) {
      throw new LifecycleContractError(`${label} contains a non-finite number`);
    }
    return value;
  }
  if (typeof value !== 'object') {
    throw new LifecycleContractError(`${label} must contain only JSON-serializable values`);
  }
  if (ancestors.has(value)) throw new LifecycleContractError(`${label} must not contain a cycle`);

  ancestors.add(value);
  let cloned;
  if (Array.isArray(value)) {
    cloned = value.map((entry, index) => cloneSerializable(entry, `${label}[${index}]`, ancestors));
  } else {
    if (!isPlainObject(value)) {
      throw new LifecycleContractError(`${label} must contain only plain objects and arrays`);
    }
    cloned = {};
    for (const [key, entry] of Object.entries(value)) {
      cloned[key] = cloneSerializable(entry, `${label}.${key}`, ancestors);
    }
  }
  ancestors.delete(value);
  return cloned;
}

function sessionIdentifierKey(key) {
  return /^session_?id$/i.test(key);
}

function cloneRestoredValue(value, label, ancestors = new Set()) {
  if (value === null || typeof value === 'string' || typeof value === 'boolean') return value;
  if (typeof value === 'number') {
    if (!Number.isFinite(value)) {
      throw new LifecycleContractError(`${label} contains a non-finite number`);
    }
    return value;
  }
  if (typeof value !== 'object') {
    throw new LifecycleContractError(`${label} must contain only JSON-serializable values`);
  }
  if (ancestors.has(value)) throw new LifecycleContractError(`${label} must not contain a cycle`);

  ancestors.add(value);
  let cloned;
  if (Array.isArray(value)) {
    cloned = value.map((entry, index) => cloneRestoredValue(entry, `${label}[${index}]`, ancestors));
  } else {
    if (!isPlainObject(value)) {
      throw new LifecycleContractError(`${label} must contain only plain objects and arrays`);
    }
    cloned = {};
    for (const [key, entry] of Object.entries(value)) {
      if (sessionIdentifierKey(key)) continue;
      cloned[key] = cloneRestoredValue(entry, `${label}.${key}`, ancestors);
    }
  }
  ancestors.delete(value);
  return cloned;
}

function containsSessionIdentifier(value, visited = new Set()) {
  if (!value || typeof value !== 'object') return false;
  if (visited.has(value)) return true;
  visited.add(value);
  if (!Array.isArray(value) && Object.keys(value).some(sessionIdentifierKey)) return true;
  return Object.values(value).some(entry => containsSessionIdentifier(entry, visited));
}

function deepFreeze(value, visited = new Set()) {
  if (!value || typeof value !== 'object' || visited.has(value)) return value;
  visited.add(value);
  Object.values(value).forEach(entry => deepFreeze(entry, visited));
  return Object.freeze(value);
}

function cloneEvidence(value, { restored = false } = {}) {
  if (value == null) return null;
  const cloned = restored
    ? cloneRestoredValue(value, 'evidence')
    : cloneSerializable(value, 'evidence');
  return deepFreeze(cloned);
}

function parseArtifactKinds(value) {
  if (value == null) return new Set();
  if (!Array.isArray(value) && !(value instanceof Set)) {
    throw new LifecycleContractError('immutableArtifactKinds must be an array or Set');
  }
  const kinds = new Set();
  for (const kind of value) {
    if (typeof kind !== 'string' || !kind.trim()) {
      throw new LifecycleContractError(
        'immutableArtifactKinds entries must be non-empty strings',
      );
    }
    kinds.add(kind);
  }
  return kinds;
}

function emptyArtifactReuse(reason = 'no-artifact') {
  return Object.freeze({ reusable: false, reason, kind: null, hash: null });
}

function runtimeFor(lifecycle) {
  const runtime = LIFECYCLE_RUNTIME.get(lifecycle);
  if (!runtime) throw new LifecycleContractError('unknown lifecycle instance');
  return runtime;
}

function makeLifecycleHandle() {
  const lifecycle = {};
  Object.defineProperties(lifecycle, {
    contract: {
      value: DERIVED_RESULT_LIFECYCLE_CONTRACT,
      enumerable: false,
    },
    toJSON: {
      value() {
        throw new LifecycleContractError(
          'runtime lifecycle cannot be serialized; use serializeExecutionLifecycle()',
        );
      },
      enumerable: false,
    },
  });
  return Object.freeze(lifecycle);
}

function makeTicket(lifecycle, revision, context) {
  const ticket = {};
  Object.defineProperties(ticket, {
    owner: { value: context.owner, enumerable: false },
    revision: { value: revision, enumerable: false },
    workspaceEpoch: { value: context.workspaceEpoch, enumerable: false },
    inputFingerprint: { value: context.inputFingerprint, enumerable: false },
    endpoint: { value: context.endpoint, enumerable: false },
    token: { value: Symbol(`derived-result-execution-${revision}`), enumerable: false },
    toJSON: {
      value() {
        throw new LifecycleContractError('runtime ticket cannot be serialized');
      },
      enumerable: false,
    },
  });
  Object.freeze(ticket);
  TICKET_RUNTIME.set(ticket, lifecycle);
  return ticket;
}

function assignContext(runtime, context) {
  runtime.owner = context.owner;
  runtime.workspaceEpoch = context.workspaceEpoch;
  runtime.inputFingerprint = context.inputFingerprint;
  runtime.endpoint = context.endpoint;
}

function clearRuntimeResult(runtime) {
  runtime.result = null;
  runtime.sessionId = null;
  runtime.reusableArtifact = null;
  runtime.artifactReuse = emptyArtifactReuse();
}

function stopLoading(runtime, ticket) {
  if (runtime.loadingOwner !== ticket) return false;
  runtime.loading = false;
  runtime.loadingOwner = null;
  return true;
}

function invalidateRuntime(runtime, reason, context = null) {
  runtime.revision += 1;
  runtime.state = 'invalidated';
  runtime.freshness = 'invalidated';
  runtime.invalidationReason = reason;
  runtime.error = null;
  runtime.blockReason = null;
  runtime.currentTicket = null;
  runtime.loading = false;
  runtime.loadingOwner = null;
  if (context) assignContext(runtime, context);
  clearRuntimeResult(runtime);
}

function contextMatches(ticket, context) {
  return ticket.owner === context.owner &&
    ticket.workspaceEpoch === context.workspaceEpoch &&
    ticket.inputFingerprint === context.inputFingerprint &&
    ticket.endpoint === context.endpoint;
}

function guardCompletion(lifecycle, ticket, context) {
  const runtime = runtimeFor(lifecycle);
  requireContext(context);
  if (TICKET_RUNTIME.get(ticket) !== lifecycle || runtime.currentTicket !== ticket ||
      runtime.revision !== ticket.revision) {
    return null;
  }
  if (!contextMatches(ticket, context)) {
    invalidateRuntime(runtime, 'execution-context-drift', context);
    return null;
  }
  return runtime;
}

function evaluateRestoredArtifact(runtime, artifact) {
  if (artifact == null) {
    return { reuse: emptyArtifactReuse(), value: null };
  }
  if (!isPlainObject(artifact)) {
    return { reuse: emptyArtifactReuse('malformed-artifact'), value: null };
  }

  const kind = artifact.kind;
  const hash = artifact.hash;
  if (typeof kind !== 'string' || !kind.trim()) {
    return { reuse: emptyArtifactReuse('invalid-kind'), value: null };
  }
  if (!runtime.immutableArtifactKinds.has(kind)) {
    return {
      reuse: Object.freeze({ reusable: false, reason: 'kind-not-allowlisted', kind, hash: null }),
      value: null,
    };
  }
  if (typeof hash !== 'string' || !SHA256_HEX.test(hash)) {
    return {
      reuse: Object.freeze({ reusable: false, reason: 'invalid-hash', kind, hash: null }),
      value: null,
    };
  }
  const normalizedHash = hash.toLowerCase();
  if (!Object.prototype.hasOwnProperty.call(artifact, 'value')) {
    return {
      reuse: Object.freeze({
        reusable: false, reason: 'missing-artifact-value', kind, hash: normalizedHash,
      }),
      value: null,
    };
  }
  if (containsSessionIdentifier(artifact.value)) {
    return {
      reuse: Object.freeze({
        reusable: false, reason: 'contains-session-id', kind, hash: normalizedHash,
      }),
      value: null,
    };
  }
  if (typeof runtime.verifyArtifactHash !== 'function') {
    return {
      reuse: Object.freeze({
        reusable: false, reason: 'hash-verifier-unavailable', kind, hash: normalizedHash,
      }),
      value: null,
    };
  }

  let value;
  try {
    value = deepFreeze(cloneSerializable(artifact.value, 'artifact.value'));
  } catch {
    return {
      reuse: Object.freeze({
        reusable: false, reason: 'malformed-artifact-value', kind, hash: normalizedHash,
      }),
      value: null,
    };
  }

  let verified = false;
  try {
    verified = runtime.verifyArtifactHash({
      kind,
      expectedHash: normalizedHash,
      value,
    }) === true;
  } catch {
    verified = false;
  }
  if (!verified) {
    return {
      reuse: Object.freeze({
        reusable: false, reason: 'hash-verification-failed', kind, hash: normalizedHash,
      }),
      value: null,
    };
  }
  return {
    reuse: Object.freeze({ reusable: true, reason: 'verified', kind, hash: normalizedHash }),
    value,
  };
}

export function createExecutionLifecycle({
  immutableArtifactKinds = [],
  verifyArtifactHash = null,
} = {}) {
  if (verifyArtifactHash != null && typeof verifyArtifactHash !== 'function') {
    throw new LifecycleContractError('verifyArtifactHash must be a function or null');
  }
  const lifecycle = makeLifecycleHandle();
  LIFECYCLE_RUNTIME.set(lifecycle, {
    state: 'empty',
    freshness: 'empty',
    evidence: null,
    revision: 0,
    owner: null,
    workspaceEpoch: null,
    inputFingerprint: null,
    endpoint: null,
    currentTicket: null,
    loading: false,
    loadingOwner: null,
    result: null,
    sessionId: null,
    error: null,
    blockReason: null,
    invalidationReason: null,
    artifactReuse: emptyArtifactReuse(),
    reusableArtifact: null,
    immutableArtifactKinds: parseArtifactKinds(immutableArtifactKinds),
    verifyArtifactHash,
  });
  return lifecycle;
}

export function begin(lifecycle, context) {
  const runtime = runtimeFor(lifecycle);
  requireContext(context, 'execution context');
  runtime.revision += 1;
  const ticket = makeTicket(lifecycle, runtime.revision, context);
  runtime.state = 'running';
  runtime.freshness = 'empty';
  runtime.evidence = null;
  runtime.currentTicket = ticket;
  runtime.loading = true;
  runtime.loadingOwner = ticket;
  runtime.error = null;
  runtime.blockReason = null;
  runtime.invalidationReason = null;
  assignContext(runtime, context);
  clearRuntimeResult(runtime);
  return ticket;
}

export function isCurrent(lifecycle, ticket, context) {
  const runtime = runtimeFor(lifecycle);
  requireContext(context);
  return TICKET_RUNTIME.get(ticket) === lifecycle && runtime.currentTicket === ticket &&
    runtime.revision === ticket.revision && contextMatches(ticket, context);
}

export function commit(lifecycle, ticket, {
  context,
  result,
  evidence = null,
  sessionId = null,
} = {}) {
  const runtime = guardCompletion(lifecycle, ticket, context);
  if (!runtime) return false;
  if (result === undefined) {
    throw new LifecycleContractError('commit result must be provided explicitly');
  }
  if (sessionId != null && (typeof sessionId !== 'string' || !sessionId.trim())) {
    throw new LifecycleContractError('sessionId must be null or a non-empty string');
  }

  runtime.state = 'current';
  runtime.freshness = 'current';
  runtime.evidence = cloneEvidence(evidence);
  runtime.result = result;
  runtime.sessionId = sessionId;
  runtime.error = null;
  runtime.blockReason = null;
  runtime.invalidationReason = null;
  runtime.reusableArtifact = null;
  runtime.artifactReuse = emptyArtifactReuse();
  stopLoading(runtime, ticket);
  return true;
}

export function fail(lifecycle, ticket, {
  context,
  error,
  result,
  evidence = null,
} = {}) {
  const runtime = guardCompletion(lifecycle, ticket, context);
  if (!runtime) return false;
  if (error == null) throw new LifecycleContractError('failure error must be provided');
  const hasResult = result !== undefined;

  runtime.state = 'failed';
  runtime.freshness = hasResult ? 'current' : 'empty';
  runtime.evidence = cloneEvidence(evidence);
  runtime.result = hasResult ? result : null;
  runtime.sessionId = null;
  runtime.error = error;
  runtime.blockReason = null;
  runtime.invalidationReason = null;
  runtime.reusableArtifact = null;
  runtime.artifactReuse = emptyArtifactReuse();
  stopLoading(runtime, ticket);
  return true;
}

export function block(lifecycle, ticket, {
  context,
  reason,
  result,
  evidence = null,
} = {}) {
  const runtime = guardCompletion(lifecycle, ticket, context);
  if (!runtime) return false;
  requireReason(reason, 'block reason');
  const hasResult = result !== undefined;

  runtime.state = 'blocked';
  runtime.freshness = hasResult ? 'current' : 'empty';
  runtime.evidence = cloneEvidence(evidence);
  runtime.result = hasResult ? result : null;
  runtime.sessionId = null;
  runtime.error = null;
  runtime.blockReason = reason;
  runtime.invalidationReason = null;
  runtime.reusableArtifact = null;
  runtime.artifactReuse = emptyArtifactReuse();
  stopLoading(runtime, ticket);
  return true;
}

export function invalidate(lifecycle, {
  owner,
  workspaceEpoch,
  reason,
} = {}) {
  const runtime = runtimeFor(lifecycle);
  if (!isPlainObject(owner)) {
    throw new LifecycleContractError('invalidation owner must be a node object');
  }
  if (!validWorkspaceEpoch(workspaceEpoch)) {
    throw new LifecycleContractError('invalidation workspaceEpoch is invalid');
  }
  requireReason(reason, 'invalidation reason');
  if (runtime.owner !== owner || runtime.workspaceEpoch !== workspaceEpoch) return false;
  invalidateRuntime(runtime, reason);
  return true;
}

export function restoreHistorical(lifecycle, {
  context,
  result,
  evidence = null,
  artifact = null,
} = {}) {
  const runtime = runtimeFor(lifecycle);
  requireContext(context, 'restore context');
  if (result === undefined) {
    throw new LifecycleContractError('restored result must be provided explicitly');
  }

  const restoredResult = cloneRestoredValue(result, 'restored result');
  const restoredEvidence = cloneEvidence(evidence, { restored: true });
  const artifactEvaluation = evaluateRestoredArtifact(runtime, artifact);

  runtime.revision += 1;
  runtime.state = 'historical';
  runtime.freshness = 'historical';
  runtime.evidence = restoredEvidence;
  runtime.currentTicket = null;
  runtime.loading = false;
  runtime.loadingOwner = null;
  runtime.result = restoredResult;
  // Session identity is per live backend process. It is intentionally ignored
  // even when a caller passes a saved sessionId alongside this payload.
  runtime.sessionId = null;
  runtime.error = null;
  runtime.blockReason = null;
  runtime.invalidationReason = null;
  runtime.artifactReuse = artifactEvaluation.reuse;
  runtime.reusableArtifact = artifactEvaluation.value;
  assignContext(runtime, context);
  return true;
}

// Safe for use from `finally`: only the execution that still owns loading can
// release it. If a current run exits without a terminal transition, fail closed
// instead of leaving an apparently usable running result.
export function release(lifecycle, ticket) {
  const runtime = runtimeFor(lifecycle);
  if (TICKET_RUNTIME.get(ticket) !== lifecycle || runtime.loadingOwner !== ticket) return false;
  stopLoading(runtime, ticket);
  if (runtime.currentTicket === ticket && runtime.state === 'running') {
    runtime.state = 'failed';
    runtime.freshness = 'empty';
    runtime.evidence = null;
    runtime.result = null;
    runtime.sessionId = null;
    runtime.error = new LifecycleContractError('execution ended without a terminal transition');
    runtime.currentTicket = null;
  }
  return true;
}

export function readCurrentResult(lifecycle) {
  const runtime = runtimeFor(lifecycle);
  if (runtime.state !== 'current' || runtime.freshness !== 'current') return null;
  return runtime.result;
}

export function readReusableArtifact(lifecycle) {
  const runtime = runtimeFor(lifecycle);
  return runtime.artifactReuse.reusable ? runtime.reusableArtifact : null;
}

export function serializeExecutionLifecycle(lifecycle) {
  const runtime = runtimeFor(lifecycle);
  return deepFreeze({
    state: runtime.state,
    freshness: runtime.freshness,
    evidence: cloneEvidence(runtime.evidence),
  });
}

export function inspectExecutionLifecycle(lifecycle) {
  const runtime = runtimeFor(lifecycle);
  return Object.freeze({
    contract: DERIVED_RESULT_LIFECYCLE_CONTRACT,
    state: runtime.state,
    freshness: runtime.freshness,
    evidence: cloneEvidence(runtime.evidence),
    revision: runtime.revision,
    loading: runtime.loading,
    owner: runtime.owner,
    currentTicket: runtime.currentTicket,
    workspaceEpoch: runtime.workspaceEpoch,
    inputFingerprint: runtime.inputFingerprint,
    endpoint: runtime.endpoint,
    result: runtime.result,
    sessionId: runtime.sessionId,
    error: runtime.error,
    blockReason: runtime.blockReason,
    invalidationReason: runtime.invalidationReason,
    artifactReuse: runtime.artifactReuse,
  });
}
