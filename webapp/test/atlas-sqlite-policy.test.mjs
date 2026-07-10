import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

import {
  ATLAS_HTTP_SQLITE_RUNTIME_FLAG,
  createAtlasInverseHttpRequest,
  createAtlasQueryHttpRequest,
  isAtlasHttpSqlitePathEnabled,
  prepareAtlasSpecForHttp,
  requireAtlasHttpSqlitePath,
  resolveAtlasHttpSqlitePath,
} from '../public/js/atlas-sqlite-policy.js';

const disabledRuntime = {};
const enabledRuntime = { [ATLAS_HTTP_SQLITE_RUNTIME_FLAG]: true };

test('browser SQLite path mode fails closed and mirrors explicit backend truthy values', () => {
  assert.equal(isAtlasHttpSqlitePathEnabled(disabledRuntime), false);
  assert.equal(isAtlasHttpSqlitePathEnabled({ [ATLAS_HTTP_SQLITE_RUNTIME_FLAG]: 'unexpected' }), false);
  for (const value of [true, 1, '1', ' true ', 'YES', 'on']) {
    assert.equal(isAtlasHttpSqlitePathEnabled({ [ATLAS_HTTP_SQLITE_RUNTIME_FLAG]: value }), true);
  }
});

test('explicit paths are rejected before request construction without operator opt-in', () => {
  assert.throws(
    () => requireAtlasHttpSqlitePath('atlas.sqlite', 'Atlas query', disabledRuntime),
    error => {
      assert.match(error.message, /cannot send a raw SQLite path/);
      assert.match(error.message, /window\.BIOCIRCUITS_EXPLORER_ALLOW_HTTP_SQLITE_PATHS=true/);
      assert.match(error.message, /BIOCIRCUITS_EXPLORER_ATLAS_STORE_ROOT/);
      return true;
    },
  );
  assert.equal(requireAtlasHttpSqlitePath('   ', 'Atlas query', disabledRuntime), '');
  assert.equal(requireAtlasHttpSqlitePath('  atlas.sqlite  ', 'Atlas query', enabledRuntime), 'atlas.sqlite');
});

test('persisted result paths fall back to the connected in-memory Atlas when disabled', () => {
  assert.equal(resolveAtlasHttpSqlitePath({ persistedPath: 'persisted.sqlite' }, disabledRuntime), '');
  assert.equal(resolveAtlasHttpSqlitePath({ persistedPath: 'persisted.sqlite' }, enabledRuntime), 'persisted.sqlite');
  assert.throws(
    () => resolveAtlasHttpSqlitePath({ configuredPath: 'override.sqlite', persistedPath: 'persisted.sqlite' }, disabledRuntime),
    /cannot send a raw SQLite path/,
  );
  assert.equal(
    resolveAtlasHttpSqlitePath({ configuredPath: ' override.sqlite ', persistedPath: 'persisted.sqlite' }, enabledRuntime),
    'override.sqlite',
  );
});

test('build, query, and inverse request helpers never attach a path without opt-in', () => {
  const atlas = { schema_version: 'atlas/v1', network_entries: [] };
  const query = { limit: 3 };
  const spec = { networks: [{ label: 'demo' }], sqlite_path: '  library.sqlite  ', persist_sqlite: true };

  assert.throws(() => prepareAtlasSpecForHttp(spec, 'Atlas build', disabledRuntime), /cannot send a raw SQLite path/);
  assert.throws(
    () => createAtlasQueryHttpRequest({ atlas, sqlitePath: 'library.sqlite', query }, disabledRuntime),
    /cannot send a raw SQLite path/,
  );
  assert.throws(
    () => createAtlasInverseHttpRequest({ query, atlasSpec: spec }, disabledRuntime),
    /cannot send a raw SQLite path/,
  );

  const memoryQuery = createAtlasQueryHttpRequest({ atlas, query }, disabledRuntime);
  assert.deepEqual(memoryQuery, { atlas, query });
  assert.equal('sqlite_path' in memoryQuery, false);

  assert.deepEqual(
    prepareAtlasSpecForHttp({ networks: [], sqlite_path: '' }, 'Atlas build', disabledRuntime),
    { networks: [] },
    'an empty legacy path key must not trip the backend HTTP path gate',
  );

  const preparedSpec = prepareAtlasSpecForHttp(spec, 'Atlas build', enabledRuntime);
  assert.deepEqual(preparedSpec, { ...spec, sqlite_path: 'library.sqlite' });
  assert.equal(spec.sqlite_path, '  library.sqlite  ', 'preparation must not mutate saved workspace state');

  assert.deepEqual(
    createAtlasQueryHttpRequest({ atlas, sqlitePath: ' library.sqlite ', query }, enabledRuntime),
    { sqlite_path: 'library.sqlite', query },
  );

  assert.deepEqual(
    createAtlasInverseHttpRequest({
      query,
      inverseDesign: { source_label: 'test' },
      refinement: { enabled: false },
      allowDuplicateAtlas: true,
      sqlitePath: ' library.sqlite ',
      atlasSpec: spec,
      atlas,
    }, enabledRuntime),
    {
      query,
      inverse_design: { source_label: 'test' },
      refinement: { enabled: false },
      allow_duplicate_atlas: true,
      sqlite_path: 'library.sqlite',
      atlas_spec: { ...spec, sqlite_path: 'library.sqlite' },
    },
  );
});

test('Atlas UI and execution paths use the operator-only transport policy', async () => {
  const atlasSource = await readFile(new URL('../public/js/atlas.js', import.meta.url), 'utf8');
  const nodeSource = await readFile(new URL('../public/js/node-types/atlas.js', import.meta.url), 'utf8');

  assert.match(atlasSource, /prepareAtlasSpecForHttp\(payload\.spec, 'Atlas build'\)/);
  assert.match(atlasSource, /createAtlasQueryHttpRequest\(/);
  assert.match(atlasSource, /createAtlasInverseHttpRequest\(/);
  assert.match(atlasSource, /resolveAtlasHttpSqlitePath\(/);
  assert.match(nodeSource, /Raw SQLite paths are operator-only and disabled on this page/);
  assert.match(nodeSource, /SQLite-path reuse is available only on operator-enabled deployments/);
});
