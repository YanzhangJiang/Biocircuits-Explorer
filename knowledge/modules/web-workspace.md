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
  `webapp/public/js/workspace-v2.js`, `webapp/public/js/node-schema.js`, and
  `schemas/workspace.schema.json`
- Type and restore safety: `webapp/public/js/port-types.js`,
  `webapp/public/js/connection-validation.js`
- Workflow architecture: `webapp/public/js/node-contracts.js`,
  `webapp/public/js/execution-outcome.js`,
  `webapp/public/js/workflow-execution.js`,
  `webapp/public/js/execution-lifecycle-core.js`, and
  `webapp/public/js/graph-patch.js`
- Feature UI: `webapp/public/js/node-types/`, `webapp/public/js/atlas.js`,
  `webapp/public/js/design-screen-render.js`, `webapp/public/js/plot-validity.js`,
  `webapp/public/js/atlas-sqlite-policy.js`, `webapp/public/js/sbml-io.js`
- Standalone experimental RO-field viewer: `webapp/public/ro-field-demo.html`,
  `webapp/public/js/ro-field-demo.js`, and
  `webapp/public/js/ro-field-render.js`
- Tooling: `webapp/package.json`, `webapp/eslint.config.mjs`,
  `webapp/playwright.config.mjs`, and `webapp/e2e/`

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

- Workspace v2 migration, validation, and shell versions:
  `webapp/public/js/state.js`, `webapp/public/js/workspace-v2.js`,
  `webapp/public/js/workspace.js`, and `schemas/workspace.schema.json`
- Per-node persistence: `webapp/public/js/node-schema.js` plus custom cases in
  `webapp/public/js/workspace.js`
- Port declarations and compatibility: `webapp/public/js/node-types/index.js`,
  `webapp/public/js/port-types.js`, and
  `webapp/public/js/connection-validation.js`
- Node roles, structured terminal outcomes, serial scheduling, result
  freshness, and atomic graph construction:
  `knowledge/contracts/workflow-execution.md` and its JavaScript owners listed
  above
- Canonical artifact shapes: `schemas/network-ir.schema.json`,
  `schemas/designability-spec.schema.json`, and
  `schemas/result-artifact.schema.json`
- Backend protocol discovery and route behavior: `webapp/src/routing.jl`

## Tests

`webapp/test/*.test.mjs` covers the exhaustive node inventory, seven strict
configuration-port families, structured outcomes, cycle-safe serial scheduling,
shared result lifecycle, GraphPatch rollback/Undo/Redo, Workspace v1-to-v2
migration, command geometry, restored-connection validation, request ownership,
numerical gaps, and feature renderers. Shared fixtures under
`tests/fixtures/workspace/` are consumed by both JavaScript and Swift; the
expected v2 fixture also has direct JSON Schema coverage in
`tests/test_workspace_schema.py`.

Focused lifecycle contracts cover staged workspace restore, model
invalidation/build ownership, SISO/qK, scans, ROP/FRET/polyhedron, Design
Target/ROP shape, regime graph, Placer, Vertices, and Atlas. Model Summary is a
synchronous, non-persisted renderer and therefore has no reusable derived-result
lifecycle of its own.
Atlas build/query/inverse results are latest-wins: a new run, upstream config
edit, or semantic connection change retires both the pending request and its
previously stored result before downstream code can reuse it.
Interactive rewiring reads the graph snapshot from gesture start, so dragging
a wire back to its original socket remains a semantic no-op.
`design-agent-conversation-owner-contract.test.mjs` applies the same owner rule
to saved Design Agent conversations: turns are single-flight, restore aborts
the previous pending turn, and delayed replies cannot cross workspace epochs.
`model-request.test.mjs` covers Cloud job owner retirement, stale nonterminal
cancellation, terminal no-cancel behavior, bounded polling retries, and fresh
pre-signed URL retry without broker-result fallback.

`ro-field-render.test.mjs` covers non-symmetric last-axis-fastest tensor layout,
invalid null gaps, exact cells with multiple labels, facets and singular strata,
non-uniform sampled coordinates rendered as fixed-size discrete markers with an
explicit no-interpolation label, future-version/storage rejection, and the rule
that rank greater than two needs an explicit 2D slice rather than an invented
browser projection. This page is a
standalone demo and does not add a Workspace v2 node or native document field.

## CI

`.github/workflows/ci.yml` is configured to run zero-warning ESLint, all tests
listed by `npm run test:js`, Python agent contracts, the HTML language-copy sync
check, the Julia suite, schema/repository checks, and a real Chromium Playwright
lane. The browser lane serves the checked-in static application on loopback and
uses local mocked `/api/v1` responses; it includes an axe serious/critical gate
and one deterministic topology screenshot. Workflow configuration is not proof
that a remote run passed. The optional esbuild bundle is not built in CI;
development and shipped pages load the source ES modules directly.

## Invariants

- Mutating editor actions routed through the command model remain undoable and
  redoable with stable node identities.
- Quick Add, Design Target Build & Tune, Design Agent auto-spawn, and Agent
  DesignabilitySpec export publish one validated GraphPatch or restore the
  complete pre-command graph, node ordinal, workspace snapshot, and Undo depth.
  One successful patch is one Undo item and Redo preserves its planned IDs.
- A connection survives creation, restore, paste, or Redo only when both ports
  are declared and their exact artifact types match. The former broad
  `ParamsConfig` type does not exist; the seven configuration families do not
  cross-connect.
- A workflow operation returns `bne-execution-outcome/v1`; the scheduler rejects
  a cycle before running, executes serially in topological order, and blocks
  descendants of failed, blocked, cancelled, stale, missing, or historical
  upstream output without suppressing an independent branch.
- Executable derived results keep scientific evidence separate from freshness.
  A runtime ticket binds node owner, revision, workspace epoch, input
  fingerprint, and endpoint; a mismatch cannot commit or clear a newer run.
- Workspace v2 is jointly consumed by the JavaScript migration/loader, complete
  JSON Schema, native decoder, and shared fixtures. A future version fails
  before graph replacement; v1 merged nodes expand deterministically; restored
  results become historical; transient session/ticket fields are stripped.
- The native bridge and browser serializer report the same contract and
  workspace versions. Saved backend session identifiers are never trusted
  after reload; computation nodes rebuild before further backend work.
- Verified, screened/proxy, minimal-certificate, and unsupported evidence remain
  visually and semantically distinct.
- A failed or non-finite numerical sample renders as a gap. Partial responses
  display that boundary and cannot be made visually indistinguishable from a
  complete result.
- The experimental RO-field viewer renders only bounded inline v1 artifacts.
  Sampled values stay discrete markers at their declared coordinates; blank
  space is explicitly unsampled and not interpolated. The viewer preserves
  set-valued labels and singular/gap warnings and never collapses a
  higher-dimensional field into an undeclared 2D projection.
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

- Playwright uses mocked loopback `/api/v1` responses and Chromium. It does not
  prove browser-to-live-Julia, Design Agent provider, AWS, Slurm, registry, or
  production authentication behavior.
- Accessibility coverage is a serious/critical axe gate over the key workspace
  surface, and visual coverage is one flattened topology baseline. Broader UI
  states, browser engines, assistive technology, and platform rendering remain
  release and product QA work.
- Six merged v1 nodes remain restore-only until the declared Workspace v3
  removal boundary. Named outcome adapters preserve older boolean/value/void
  operations while migration to native structured outcomes continues.
- All tracked first-party clients use `/api/v1/*`, and declared bare `/api/*`
  aliases are measured. Alias removal still requires telemetry and inventory
  for deployed and rollback clients through the declared sunset.
- The RO-field page is not integrated into the typed Workspace/native menus and
  has no browser-side chunk loader or interactive high-dimensional slicer.

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

- Source commit: `b91cf41`; the workflow architecture
  and Workspace v2 extension were inspected on 2026-07-15.
- Evidence inspected: browser owners, shared JavaScript/Swift fixtures, complete
  Workspace Schema, focused and full JS tests, zero-warning lint, server
  routing, and the local real-Chromium Playwright/axe/visual gate with mocked
  canonical endpoints and zero recorded console/page errors.
- Historical baseline: `f9c65a5` remains the earlier catalog evidence anchor.
- Boundary: no remote CI result or browser-to-live backend/provider/cloud run is
  claimed; the accessibility and visual scope is the bounded gate described
  above, not general release qualification.
