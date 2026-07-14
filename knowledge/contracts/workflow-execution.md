---
title: Browser workflow execution contract
status: working-tree
verified_against: development
---

# Browser workflow execution contract

The node editor is a typed, versioned workflow runtime. A wire is valid only
when both endpoint declarations exist and their exact artifact types match; a
node run is successful only when it returns a structured outcome; and a saved
result is reusable only when its lifecycle says it belongs to the current
owner, graph, inputs, endpoint, and workspace epoch.

The executable owners are under `webapp/public/js/`. This page explains their
composition; it does not replace the machine-readable inventories or tests.

## Complete node inventory

`node-contracts.js` is the exhaustive owner for every node's architecture role,
availability, execution mode, transitional adapter, declared outputs, and
capabilities. `applyNodeContracts()` fails during module construction when its
keys differ from the `NODE_TYPES` registry. The inventory test additionally
requires every active Run control to have an execution operation.

The only roles are:

- `source`: introduces a network or external input;
- `config`: prepares a typed request without claiming that computation ran;
- `compute`: produces a derived result;
- `manual-gate`: requires an explicit user or external side-effect decision;
- `viewer`: presents non-computational content.

Six merged v1 nodes are `restore-only`: `siso-analysis`, `rop-cloud`,
`fret-heatmap`, `parameter-scan-1d`, `parameter-scan-2d`, and
`rop-polyhedron`. Workspace v2 expands them into typed config/result nodes.
They cannot be created through the ordinary UI or scheduled. Their definitions
are retained only for v1 restore/migration and have a Workspace v3 removal
boundary.

## Strict port contract

`port-types.js` owns the artifact vocabulary. The former broad `ParamsConfig`
type does not exist. Its seven configuration families are distinct:
`SISOConfig`, `Scan1DConfig`, `Scan2DConfig`, `ROPCloudConfig`, `FRETConfig`,
`ROPPolyhedronConfig`, and `ParameterPlacerConfig`.

`connection-validation.js` resolves an endpoint from node type, direction, and
port name before comparing exact types. Interactive wiring, restore, paste,
Undo/Redo, Quick Add, and GraphPatch validation use this boundary. Missing
declarations, unknown types, misspellings, cross-family config wires, duplicate
input ownership, and self-connections fail closed. The 7×7 matrix admits the
seven same-family pairs and rejects the other 42 combinations.

## Structured execution and serial scheduling

`execution-outcome.js` owns `bne-execution-outcome/v1` with exactly five terminal
statuses: `succeeded`, `blocked`, `failed`, `cancelled`, and `stale`.
`undefined`, `null`, `false`, and arbitrary objects are contract violations
unless a node has a named transitional adapter in the inventory.

`workflow-execution.js` plans before running. It rejects a cycle before any
node executes, then evaluates the selected connected component in topological
order and serially. A failed or blocked node skips its descendants, while an
independent branch may continue. The report distinguishes executed, reused,
blocked, failed, cancelled, and stale work. The default toolbar action runs the
selected node's component; Run All Connected is an explicit wider scope.

Preparation and computation are separate execution modes. In particular, SISO
has a real compute entrypoint. A completed SISO calculation without a selected
path reports that output as `missing`; qK is then `blocked`, not successful.
A historical selected path is likewise not a current upstream value.

## Derived-result lifecycle

`execution-lifecycle-core.js` owns
`bne-derived-result-lifecycle/v1`. Its states are `empty`, `running`, `current`,
`failed`, `blocked`, `invalidated`, and `historical`; freshness is a separate
axis with `empty`, `current`, `invalidated`, and `historical`.

A runtime ticket binds the node object, monotonically changing revision,
workspace runtime epoch, stable input fingerprint, and exact endpoint. Commit,
failure, delayed plot callbacks, and loading cleanup must prove that ticket is
still current. Input controls invalidate synchronously before debounce, so an
older response cannot become current during the delay.

Persisted lifecycle snapshots contain only state, freshness, and scientific
evidence. Runtime tickets and session identifiers are never serialized.
Restored derived results enter `historical` and cannot flow downstream until a
fresh run. Scientific evidence grade and freshness remain independent: restore
does not promote or erase whether an artifact was verified, sampled, partial,
or proxy-only. Cross-restore reuse is restricted to an explicit immutable-kind
allowlist plus a matching SHA-256 identity.

## Atomic graph construction

`graph-patch.js` owns the plan/validate/stage/commit transaction used by Quick
Add, Design Target Build & Tune, and Design Agent auto-spawn. A plan is a pure
description with stable node IDs. `GraphPatchCommand` validates the complete
projected graph, stages every node and connection, and publishes once.

Any creation, initialization, validation, or commit failure restores the prior
topology, node counter, workspace snapshot, and Undo depth. One successful
chain is one Undo item; Redo restores the same IDs. Deferred auto-build or
auto-populate callbacks are scoped to the patch epoch and are cancelled on
Undo. Production graph builders do not append directly to the live connection
array.

Compatible source reuse includes reaction and SBML sources. Multiple compatible
sources require an explicit selection; Shift-Quick-Add creates an isolated
source instead of choosing the first traversal result.

## Workspace v2 boundary

`workspace-v2.js`, `workspace.js`, `schemas/workspace.schema.json`, and the
native `WorkspaceDocument.swift` decoder jointly consume the Workspace v2
contract. Shared fixtures prove deterministic v1-to-v2 migration in JavaScript
and Swift. Invalid v1 cross-family wires are dropped with diagnostics, transient
session state is stripped, and restored derived output becomes historical.
Unknown future document versions fail before replacing the current graph.

## Required checks

- `npm run lint`
- `npm run test:js`
- `npm run test:e2e`
- `npm run check-i18n-sync`
- the no-sign macOS build and `BiocircuitsExplorerMacTests` when workspace or
  bridge semantics change
- `python3 scripts/verify_repository.py --check`

Real-browser tests cover the transaction, workflow report, v2 restore,
historical boundary, stable Undo/Redo identities, accessibility, and a
deterministic workspace screenshot. These local/CI contracts do not prove a
live backend, cloud job, or scientific result.

## Change protocol

1. Change the canonical owner and add a failing contract before implementation.
2. Keep node inventory, port declarations, persistence, and execution
   descriptor aligned for every new or changed node.
3. Route graph construction through GraphPatch and every derived result through
   the shared lifecycle core or document why it is synchronous and
   non-persisted.
4. Bump and migrate the workspace version when persisted meaning changes; keep
   browser, JSON Schema, Swift, and fixtures synchronized.
5. Do not treat local harnesses, mocked endpoints, or configured CI as external
   service evidence.
