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
  `webapp/public/js/design-screen-render.js`, `webapp/public/js/sbml-io.js`
- Tooling: `webapp/package.json`, `webapp/eslint.config.mjs`

## Inputs

- Pointer, keyboard, menu, clipboard, file-import, and node-control events.
- Workspace JSON supplied by browser file IO or the native shell bridge.
- Backend JSON responses and the shared schemas under `schemas/`.
- Optional Design Agent responses from the separate Python chat service.

## Outputs

- Backend requests for model construction, scans, ROP, atlas, designability,
  jobs, and import/export operations.
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
the Design Spec node contract. Julia route and schema tests cover the server side
of requests the workspace emits.

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

## Known gaps

- CI has no real-browser end-to-end, accessibility, screenshot, or visual
  regression suite; the JavaScript tests use focused harnesses.
- There is no single JSON Schema for the complete workspace document; its shape
  is jointly owned by JavaScript serializers and the Swift decoder.
- Browser clients still contain legacy bare `/api/` callers while the backend
  advertises canonical versioned routes; removal needs a coordinated migration.
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

- Source commit: `f9c65a5`
- Evidence inspected: browser owner paths, package scripts, focused JS tests,
  shared schemas, server routing, and CI workflow wiring.
- Boundary: no claim of browser visual fidelity or full user-flow behavior was
  made without a browser end-to-end run.
