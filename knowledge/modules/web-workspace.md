# Web Workspace

`module_id: web-workspace`

## Purpose

Provide the browser node editor and its presentation layer: create and connect
typed workflow nodes, serialize a workspace, call backend APIs, render results,
and expose a stable bridge to the native macOS host.

## Non-goals

- It does not own equilibrium mathematics, ROP evidence, or designability
  verdicts; it must render the backend's evidence distinctions without
  strengthening them.
- It does not own native project-file placement or child-process lifecycle.
- Browser caches and saved node output are not a substitute for a fresh backend
  session or reproducible result artifact.

## Owner paths

- Entrypoints and static surface: `webapp/public/index.html`,
  `webapp/public/index-node.html`, `webapp/public/js/main.js`
- Editor state and commands: `webapp/public/js/state.js`,
  `webapp/public/js/commands.js`, `webapp/public/js/nodes.js`,
  `webapp/public/js/connections.js`
- Serialization and native bridge: `webapp/public/js/workspace.js`,
  `webapp/public/js/node-schema.js`
- Type and restore safety: `webapp/public/js/port-types.js`,
  `webapp/public/js/connection-validation.js`
- Feature UI: `webapp/public/js/node-types/`, `webapp/public/js/atlas.js`,
  `webapp/public/js/design-screen-render.js`, `webapp/public/js/plot-validity.js`,
  `webapp/public/js/atlas-sqlite-policy.js`, `webapp/public/js/sbml-io.js`
- Tooling: `webapp/package.json`, `webapp/eslint.config.mjs`

## Inputs

- Pointer, keyboard, menu, clipboard, file-import, and node-control events.
- Workspace JSON supplied by browser file IO or the native shell bridge.
- Backend JSON responses and the shared schemas under `schemas/`.
- Solver-validity metadata for scans, FRET heatmaps, ROP clouds, and Atlas
  landscapes. Non-finite serialized tokens are never plot coordinates.
- Optional Design Agent responses from the separate Python chat service.

## Outputs

- Backend requests for model construction, scans, ROP, atlas, designability,
  fixed-topology ROP shape optimization, jobs, and import/export operations.
- Versioned workspace JSON containing canvas, nodes, typed connections, and the
  optional Design Agent conversation.
- Rendered plots, tables, evidence groups, SBML downloads, and user diagnostics.

## Contract sources

- Workspace and shell versions plus document validation:
  `webapp/public/js/state.js` and `webapp/public/js/workspace.js`
- Per-node persistence: `webapp/public/js/node-schema.js` plus custom cases in
  `webapp/public/js/workspace.js`
- Port declarations and compatibility: `webapp/public/js/node-types/index.js`,
  `webapp/public/js/port-types.js`, and
  `webapp/public/js/connection-validation.js`
- Canonical artifact shapes: `schemas/network-ir.schema.json`,
  `schemas/designability-spec.schema.json`, and
  `schemas/result-artifact.schema.json`
- Backend protocol discovery and route behavior: `webapp/src/routing.jl`

## Tests

`webapp/test/*.test.mjs` exercises command undo/redo, geometry, typed ports,
event dispatch, restored-connection validation, model request recovery, reaction
species parsing, native clipboard fallback, design-result evidence grouping, and
the Design Spec node contract. The ROP shape workspace contracts also cover the
exact-card reference producer, the config/result node pair, stage-specific port
types, request construction, persistence, and restored historical evidence.
`scan-validity-contract.test.mjs` and
`atlas-sqlite-policy.test.mjs` cover failed-solver gaps and the operator-only
SQLite transport. Julia route and schema tests cover the server side of
requests the workspace emits.

The current committed revision also has focused lifecycle contracts for staged
workspace restore, model invalidation/build ownership, scan execution, and
Atlas execution. Atlas build/query/inverse results are latest-wins: a new run,
upstream config edit, or semantic connection change retires both the pending
request and its previously stored result before downstream code can reuse it.
Interactive rewiring reads the graph snapshot from gesture start, so dragging
a wire back to its original socket remains a semantic no-op.
`design-agent-conversation-owner-contract.test.mjs` applies the same owner rule
to saved Design Agent conversations: turns are single-flight, restore aborts
the previous pending turn, and delayed replies cannot cross workspace epochs.
`model-request.test.mjs` covers Cloud job owner retirement, stale nonterminal
cancellation, terminal no-cancel behavior, bounded polling retries, and fresh
pre-signed URL retry without broker-result fallback.

## CI

`.github/workflows/ci.yml` runs ESLint, all tests listed by
`npm run test:js`, Python agent contracts, the HTML language-copy sync check, the
Julia suite, schema regeneration drift, and selected artifact validation. The
optional esbuild bundle is not built in CI; development and shipped pages load
the source ES modules directly.

## Invariants

- Mutating editor actions routed through the command model remain undoable and
  redoable with stable node identities.
- A restored connection survives only when both ports are declared and their
  artifact types are compatible; invalid saved wires are dropped.
- Workspace documents newer than the supported document version fail closed.
- The native bridge and browser serializer report the same contract and
  workspace versions.
- Saved backend session identifiers are not trusted after reload; computation
  nodes must rebuild the model before further backend work.
- Verified, screened/proxy, minimal-certificate, and unsupported evidence remain
  visually and semantically distinct.
- A failed or non-finite numerical sample renders as a gap. Partial responses
  display that boundary and cannot be made visually indistinguishable from a
  complete result.
- Design Screen v0.3 shows evaluated and eligible counts; a truncated screen
  says that the unevaluated candidates remain unknown.
- The fixed-topology shape workflow uses three distinct typed artifacts:
  Design Target emits a pinned reference, the edit-config node emits a request,
  and the result node emits optimizer evidence. Restored optimizer output is
  historical until a fresh run succeeds.
- Browser requests do not send `sqlite_path` unless an operator explicitly
  enables the page policy. Server opt-in and store-root confinement remain
  separate requirements; the browser flag is not authorization.
- Atlas builder, query, and inverse-design outputs remain current only while
  their node owner, input fingerprint, and committed upstream graph match.
  Starting an upstream Atlas step synchronously retires dependent Atlas
  outputs; an obsolete Run Connected step fails the chain instead of silently
  continuing with an older saved result.
- A Design Agent request owns exactly one conversation epoch and predecessor
  chat state. Workspace restore retires any pending turn before replacing its
  DOM, clears the previous active candidate/results, and reconstructs the right
  pane only from the latest restored agent response; a latest response without
  a valid card keeps the pane cleared.
- Cloud jobs remain bound to the request owner through submission, polling, and
  result download; an owner predicate exception retires the request. Losing the
  owner after obtaining a nonterminal job ID settles activity and makes one
  best-effort cancellation call, while a known terminal job is never cancelled.
  Retryable poll errors use a bounded consecutive budget that resets after each
  successful poll.
- After a cloud job succeeds, the browser obtains a pre-signed result URL and
  performs a plain direct GET. It requires an `application/json` response,
  preserves latest-request ownership across URL lookup and download, and does
  not fall back to relaying the large object through the broker. One retryable
  failure may refresh the pre-signed URL and repeat the direct GET once.

## Known gaps

- CI has no real-browser end-to-end, accessibility, screenshot, or visual
  regression suite; the JavaScript tests use focused harnesses.
- There is no single JSON Schema for the complete workspace document; its shape
  is jointly owned by JavaScript serializers and the Swift decoder.
- Ordinary browser compute calls and the fixed-topology optimizer use canonical
  `/api/v1/*` routes. Broker/auth configuration and local-image transport still
  retain their compatibility paths and need a coordinated deployment migration
  before those aliases can be removed.
- ESLint warnings are allowed, so warning growth is not presently a ratcheted
  quality gate.

## Change protocol

1. For a node field or port change, update its node definition, persistence
   descriptor/custom serializer, compatibility mapping, and focused JS test.
2. For a workspace-shape change, bump the document version only with migration
   logic and synchronize the Swift `WorkspaceDocument` and shell bridge.
3. Preserve backend evidence labels verbatim; add a render test for every new
   evidence grade or downgrade path.
4. Run `npm run lint`, `npm run test:js`, and `npm run check-i18n-sync`; run the
   Julia suite when API payloads or shared schemas change.

## Verified against

- Source commit: `f2ca13c`; the fixed-topology shape-node extension and lifecycle
  contracts were locally verified on 2026-07-15.
- Evidence inspected: browser owner paths, package scripts, focused and full JS
  tests, shared schemas, server routing, CI workflow wiring, and a local
  real-page menu/node/port/intent/fail-closed smoke with zero console errors.
- Historical baseline: `f9c65a5` remains the earlier catalog evidence anchor.
- Boundary: the local smoke did not connect the browser to a running Julia
  optimizer, and it is not accessibility, visual-regression, release, or
  production evidence.
