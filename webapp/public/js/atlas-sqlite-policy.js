// Browser-facing SQLite paths are an operator-only compatibility feature.
// The backend remains authoritative and independently requires the matching
// environment opt-in plus containment under its configured atlas store.

export const ATLAS_HTTP_SQLITE_RUNTIME_FLAG = 'BIOCIRCUITS_EXPLORER_ALLOW_HTTP_SQLITE_PATHS';

const TRUE_FLAG_VALUES = new Set(['1', 'true', 'yes', 'on']);

function browserRuntime(runtime = null) {
  if (runtime) return runtime;
  if (typeof window !== 'undefined') return window;
  return globalThis;
}

export function isAtlasHttpSqlitePathEnabled(runtime = null) {
  const raw = browserRuntime(runtime)?.[ATLAS_HTTP_SQLITE_RUNTIME_FLAG];
  if (raw === true || raw === 1) return true;
  return typeof raw === 'string' && TRUE_FLAG_VALUES.has(raw.trim().toLowerCase());
}

export function atlasHttpSqliteDisabledMessage(operation = 'Atlas request') {
  return `${operation} cannot send a raw SQLite path from this page. ` +
    'Use a connected in-memory Atlas instead, or ask a trusted operator to enable ' +
    'BIOCIRCUITS_EXPLORER_ALLOW_HTTP_SQLITE_PATHS=1 on the backend and ' +
    'window.BIOCIRCUITS_EXPLORER_ALLOW_HTTP_SQLITE_PATHS=true before the app loads. ' +
    'Enabled paths must remain under BIOCIRCUITS_EXPLORER_ATLAS_STORE_ROOT.';
}

export function requireAtlasHttpSqlitePath(rawPath, operation = 'Atlas request', runtime = null) {
  const sqlitePath = String(rawPath || '').trim();
  if (!sqlitePath) return '';
  if (!isAtlasHttpSqlitePathEnabled(runtime)) {
    throw new Error(atlasHttpSqliteDisabledMessage(operation));
  }
  return sqlitePath;
}

// A path explicitly entered by the user must never be silently ignored. An
// automatically suggested path from an in-memory build result may safely fall
// back to that in-memory Atlas when operator path mode is disabled.
export function resolveAtlasHttpSqlitePath({ configuredPath = '', persistedPath = '' } = {}, runtime = null) {
  const configured = String(configuredPath || '').trim();
  if (configured) return requireAtlasHttpSqlitePath(configured, 'Atlas query', runtime);

  const persisted = String(persistedPath || '').trim();
  if (!persisted || !isAtlasHttpSqlitePathEnabled(runtime)) return '';
  return requireAtlasHttpSqlitePath(persisted, 'Atlas query', runtime);
}

export function prepareAtlasSpecForHttp(spec, operation = 'Atlas build', runtime = null) {
  if (!spec || typeof spec !== 'object' || Array.isArray(spec)) return spec;
  const sqlitePath = requireAtlasHttpSqlitePath(spec.sqlite_path, operation, runtime);
  if (!sqlitePath) {
    if (!Object.prototype.hasOwnProperty.call(spec, 'sqlite_path')) return spec;
    const sanitized = { ...spec };
    delete sanitized.sqlite_path;
    return sanitized;
  }
  return { ...spec, sqlite_path: sqlitePath };
}

export function createAtlasQueryHttpRequest({ atlas = null, sqlitePath = '', query = {} } = {}, runtime = null) {
  const allowedPath = requireAtlasHttpSqlitePath(sqlitePath, 'Atlas query', runtime);
  return allowedPath
    ? { sqlite_path: allowedPath, query }
    : { atlas, query };
}

export function createAtlasInverseHttpRequest({
  query = {},
  inverseDesign = {},
  refinement = {},
  allowDuplicateAtlas = false,
  sqlitePath = '',
  atlasSpec = null,
  atlas = null,
} = {}, runtime = null) {
  const allowedPath = requireAtlasHttpSqlitePath(sqlitePath, 'Atlas inverse design', runtime);
  const request = {
    query,
    inverse_design: inverseDesign,
    refinement,
  };
  if (allowDuplicateAtlas) request.allow_duplicate_atlas = true;
  if (allowedPath) request.sqlite_path = allowedPath;
  if (atlasSpec) request.atlas_spec = prepareAtlasSpecForHttp(atlasSpec, 'Atlas inverse design', runtime);
  else if (atlas) request.atlas = atlas;
  return request;
}
