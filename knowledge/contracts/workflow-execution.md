---
title: Browser workflow execution contract
status: active
verified_against: b91cf41
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
Add, Design Target Build & Tune, Design Agent auto-spawn, and Agent
DesignabilitySpec export. A plan is a pure description with stable node IDs.
`GraphPatchCommand` validates the complete projected graph, stages every node
and connection, and publishes once.

Any creation, initialization, validation, or commit failure restores the prior
topology, node counter, workspace snapshot, and Undo depth. One successful
chain is one Undo item; Redo restores the same IDs. Deferred auto-build or
auto-populate callbacks are scoped to the patch epoch and are cancelled on
Undo. Production graph builders do not append directly to the live connection
array.

Compatible source reuse includes reaction, SBML, and Designed Network sources.
Multiple compatible sources open a shared Web/macOS chooser with existing
networks and a visible New Reaction Network action. Multiple connected builders
likewise require a choice. A selected builder identifies its source only when
the incoming reaction connection is unique. Cancelling the chooser does not
mutate the graph or Undo history, and a workspace replacement retires it.
Choosing an item replans against the live graph before one atomic commit.
Shift-Quick-Add remains an optional Web shortcut for an isolated source.

## Target-driven inverse-design extension

The corrected working-tree path was verified locally on 2026-09-12 with
real HTTP jobs, browser interaction/lifecycle regressions, shared Web/native
document fixtures, and the installed macOS native Run Connected command.
`inverse-design-target` is a `config` node with a
structured `prepare` operation; `gradient-design` and `designed-network` are
`compute` nodes with structured `execute` operations. The request and result
ports remain distinct `InverseDesignRequest` and `InverseDesignResult`
artifacts. The target artifact is `bne-design-target/v1.0.0`: explicit ordered
input/output roles and weighted numerical samples, with an optional description
and validation samples. Natural-language compilation, drawing, image fields,
ordered trajectories, and numerical editing converge on this editable target.
The user does not author the optimizer's reaction library.

Execution roles do not replace visual node categories: target authoring uses
the shared input header, optimization the process header, and the selected
network the result header. Drawn curves and trajectories retain their original
continuous geometry as undoable workspace data. Inverse-design nodes use no
collapsible sections: input/output roles, numerical targets, chemistry,
optimization, pruning, fitted parameters and evaluation tables are directly
available. Parameter controls use shared `param-row` sizing and labels; status
and explanatory text use `node-info`, and target plots and results use framed
`viewer-content` panels. Long results and tables scroll with keyboard access.
All panel backgrounds, borders and typography use the existing theme tokens.
Drawing persistence retains a fingerprint preventing
stale geometry from being overlaid on an edited numerical target. Every distinct
drawn vertex must enter the training target. Numerical refinement may add
evaluations along its segments, but cannot discard vertices, replace repeated
x values, or extrapolate unwritten endpoints. An older saved stroke must also
use all its vertices on a fresh run. Target previews expose physical axis ticks,
pointer/drawing crosshairs, keyboard coordinate inspection, and direct access to
axis ranges. Linear and logarithmic inputs use the same mapping for drawing and
inspection; a one-input XY trajectory displays its two output coordinates.
Inspection alone cannot mutate the target or retire a running design.

The Inverse Design Quick Add command publishes all three nodes and both wires
as one GraphPatch, with no implicit optimization request during construction.
Drawing/import/Agent edits remain undoable; asynchronous replies must match the
node identity, input revision, and workspace epoch before applying. Run
Connected prepares the target, submits the cancellable `design_network`
`local_async` job, and extracts the selected network serially. Edits synchronously
retire results and pending request ownership, including during job polling.
An obsolete completion cannot clear or commit a newer run.

Gradient Design acknowledges a click before job submission returns and keeps
its learning status beside the run controls. Live local-job progress reports
the actual stage, initialization, fit iteration/budget, pruning round, training
and best-in-fit RMSD, and current reaction count. RMSD is the square root of the
weighted mean squared target deviation, averaged across outputs; the existing
`rmse` API/persistence fields retain that same numerical definition. Only an evaluated fit has an
iteration progress bar; there is no estimated overall completion percentage.
Stage boundaries publish immediately; intermediate metrics publish at most
four times per second. Cooperative engine checkpoints allow status and cancel
requests to run even with one Julia worker. Elapsed time and any wait for a new
update stay visible; Stop remains interactive. Progress views/timers belong to
the live run and are retired on input changes, cancellation or workspace
replacement. Restored results do not resurrect a learning indicator.

An unmet search ends with an explicit budget-exhausted status, never a claim
of completed learning or convergence. The result records the configured fit,
initialization and pruning limits, actual evaluated parameter updates, and
each fit's iteration-limit or invalid-equilibrium-update reason. Numerical
failures and user cancellation remain distinct. The result offers direct
access to budget controls; changing them requires a fresh run with the chosen
seed and does not imply continuation of the previous optimizer state.

Exactly identical full input tuples requesting different output values expose
a necessary per-output RMSD lower bound before and after fitting. It is the
weighted within-tuple residual variance, normalized across the complete
training or validation dataset separately. It does not merge nearby inputs,
change samples, or simplify strokes. A tolerance below this bound cannot be
met by the current deterministic static-response definition; a tolerance above
it is not evidence of feasibility. Optimization remains available to improve
the approximation without changing the authored target.

Published evaluations include every current prediction row and per-output
RMSD from the same equilibrium evaluation as the displayed error. Full response
arrays are materialized at publication cadence and never accumulate in
optimization history. The live comparison is bound to the originating request,
its complete authored target, initialization and evaluated iteration. It is a
preview of optimization, not a current downstream network artifact. Completion
replaces it with the selected network's own checked response and an immediately
visible target comparison. Cancellation and invalidation remove the preview.

The engine generates legal precursor-closed chemistry, optimizes step Kd and
optional noninput totals/explicitly opted-in additive readout offsets, then
prunes and refits with a fixed error ceiling. The returned network must pass its
own cold equilibrium replay. `target_met` additionally requires each declared
output's training and supplied validation RMSE to satisfy the threshold;
aggregate loss or solver success alone cannot establish target compliance.
Validation samples guide model selection but not gradients. Finite numerical
predictions remain unclipped; failed samples cannot become apparent zeros.

The selected network exposes the existing `NetworkIR` reaction-source type
with fitted Kd, noninput totals, input mapping, and readout transformation/offset
preserved through model ownership and handoff. A reaction-free valid equilibrium
remains a report with an explicit Model Builder limitation. Generated-network
forward equilibrium support does not imply exact-regime/ROP support. A failed
replay, changed target, obsolete request, disconnected input, or historical
restored result cannot become a current reaction source. Prior candidate-list
workspaces remain inspectable historical evidence and require an explicit
current target plus a fresh run before reuse.

Scientific strength remains independent of freshness. This is a local Adam
parameter search plus occupancy-based structural pruning over declared bounded
chemistry, not exhaustive network search or the complete research optimizer
suite. The selected topology's own replay evidence cannot be replaced with
predictions from its larger ancestor. A result may be current and physically
valid while reporting that the target was not met.

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
