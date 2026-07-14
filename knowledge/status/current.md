# Current verified snapshot

- Snapshot date: 2026-07-15
- Component-split revision inspected: `603635d`
- Current implementation revision inspected: `b91cf41`
- Historical knowledge baseline retained: `f9c65a5`
- Current-tree contract inventory:
  `python3 scripts/verify_repository.py --check`
- Configured versions:
  [generated from their owners](../generated/reference.md#versions-and-configured-toolchains)
- Scope: repository runtime and local verification evidence, not manuscript
  claims, a remote CI result, or proof that an external deployment succeeded

The current implementation evidence anchor is `b91cf41`. It retains the fixed-topology ROP
shape-optimization and runtime/job hardening at `f2ca13c`, then adds the typed
browser workflow, Workspace v2, measured API migration, real-browser local
quality gate, and external release-evidence preparation described below.
Revision `1177a3d` remains the earlier bounded-runtime evidence anchor.

## Result in plain language

The backend previously had two failure modes that ordinary input-size checks
could miss. Several requests could update the same cached model or data file at
once, and a small-looking request could expand into far more combinations than
the server could safely finish. Numerical endpoints also did not always keep a
failed calculation visibly separate from a valid plotted value.

At `1177a3d`, each shared runtime object has an explicit serialized update path,
and synchronous requests stop when their real work grows beyond a fixed ceiling.
This ceiling is the **work budget**: it limits the computation that unfolds
inside a request, not only the number of fields in its JSON body. Numerical
responses now carry a **validity marker** so a failed sample remains a gap
instead of looking like a measured zero. Design Screen responses also say how
many candidates were eligible, how many were evaluated, and whether the screen
was truncated.

Revision `603635d` only split the large backend module into focused component
files. It was not intended to change behavior. Revision `1177a3d` then added the
concurrency, work-boundary, validation, and numerical-result changes to those
owners.

## Integrated browser workflow and Workspace v2

The node canvas now has one exhaustive architecture inventory for its 40 node
types. Five roles distinguish sources, configurations, compute, manual gates,
and viewers. Seven parameter/configuration families have exact artifact types;
the former universal `ParamsConfig` type is removed, and the 7-by-7 contract
accepts only the seven same-family pairs. Six merged v1 compute nodes remain
restore-only and migrate to explicit config/result pairs; they are neither
creatable nor scheduler-runnable in Workspace v2.

Every scheduled operation returns `bne-execution-outcome/v1`. The planner
rejects a cycle before executing, then runs the selected connected component in
serial topological order. Failed, blocked, cancelled, stale, missing, or
historical upstream output blocks only its descendants. Result freshness is a
separate axis from scientific evidence: shared lifecycle tickets bind the node
owner, revision, workspace epoch, input fingerprint, and exact endpoint. This
boundary now covers the executable derived families, including Atlas,
Placer, scans, and Vertices, so an obsolete response or delayed renderer cannot
publish as current.

Quick Add, Design Target Build & Tune, and Design Agent graph construction use
one GraphPatch transaction. Planning assigns stable IDs without mutating the
editor; validation/staging/commit either publishes the whole chain as one Undo
item or restores the previous graph, node counter, workspace snapshot, and Undo
depth. Redo retains the planned identities.

Workspace v2 is jointly consumed by the JavaScript migration/loader, the
complete `schemas/workspace.schema.json`, and the Swift decoder. Shared fixtures
prove exact v1-to-v2 structural parity: invalid cross-family wires are dropped,
runtime session/ticket fields are recursively stripped, and persisted current
results restore as historical without changing their evidence grade. A future
document version fails before replacing the active browser or native project.

All tracked first-party clients now use `/api/v1/*`. Declared compatibility
aliases remain through their sunset, while
`bcx_http_legacy_requests_total` measures actual bounded route/method/status
usage; canonical, unknown, and v1-only paths are excluded. This is local
contract evidence, not production traffic evidence.

| Workflow check | Local result | Boundary |
|---|---|---|
| Browser lint and unit contracts | Zero-warning ESLint and the full listed JavaScript suite passed | Source/harness coverage; no live provider or cloud path |
| Real-browser gate | Chromium Playwright passed atomic Quick Add/Undo/Redo, exact structured reports, historical/future Workspace v2 behavior, an axe serious/critical check, and one deterministic topology screenshot with zero captured console/page errors | Static loopback server with mocked canonical API responses; not browser-to-live-Julia evidence |
| Workspace parity | The complete v2 Schema accepted the shared expected fixture; JavaScript migration and the Swift decoder produced the same structure | Shared finite fixtures, not every possible extension document |
| macOS host | Local no-sign build and 51/51 Swift unit tests passed on macOS 27 arm64 | No remote CI, UI automation, packaged-process, signature, notarization, or clean-host claim |

The canonical composition and change rules are in the
[browser workflow execution contract](../contracts/workflow-execution.md); the
[Web](../modules/web-workspace.md) and
[macOS](../modules/macos-host.md) module cards retain the owner/test routing.

## Prepared release-candidate evidence boundary

The repository now contains a fail-closed release-candidate evidence Schema,
template, validation tests, and operator runbook. A passed record must pin one
clean commit, configuration hash, OCI digest, and macOS artifact hash and must
carry hashed, redacted observations plus rollback evidence for Registry,
Compose/TLS, AWS, Slurm, and macOS. This pack is prepared but unexecuted: all
five external outcomes remain unknown until an authorized run records them.

## Integrated extension: direct fixed-topology shape control

The previous cat-response prototype asked whether one of three hand-picked
operating-point changes could be built. The integrated revision asks how far one
pinned network can move a typed response edit before its declared path geometry
or parameter bounds stop it. It then chooses a realization with remaining
biochemical parameter room and independently replays the finite curve.

This extension introduces the canonical v1-only
`POST /api/v1/rop_shape_optimize` route, asynchronous local-job support,
typed edits, bounded path/cell enumeration, direct per-cell LPs, explicit
parameter-only margin, active-row/right-hand-side sensitivity, directional
intervals, stored finite replay, Design Screen handoff, an allow-listed Agent
tool, and a fail-closed browser workspace chain. Design Target emits a pinned
reference only for an exact canonical finite-window card; a separate edit-config
node constructs the typed request; a result node calls the v1 route, renders the
evidence layers, and marks restored output as historical until rerun. The three
artifacts use distinct strict port types and are exposed in both Web and macOS
Add Node menus. The Design Screen candidate prior still comes from the tracked
ROP design index (`paper_rop_periodic_table/data/slices.jsonl.gz`, or its explicit
operator override); the optimizer request is self-contained and does not receive
an Atlas SQLite path. The detailed owner and evidence
boundary are in the
[ROP shape-optimization module](../modules/rop-shape-optimization.md) and
[decision 0002](../decisions/0002-rop-shape-margin-and-evidence.md).

The frozen cat benchmark evaluated all 18 eligible paths and all 24 eligible
cells for each of four edits with no truncation. All four direct solves found a
non-grid geometric optimum. Three selected realizations passed complete sampled
finite replay. The `widen_center` realization completed replay but failed its
declared two-peak metric, demonstrating that exact witness geometry is not an
exact nonlinear peak-feature guarantee.

| Integration check | Local result | Boundary |
|---|---|---|
| Full Julia Web suite | No failing tests; the one pre-existing explicit `@test_broken` remains | Local Julia 1.12.6 process, not remote CI |
| Shape core compatibility | 124/124 on Julia 1.12.6 and 124/124 on Julia 1.10.11 | Core LP contract only on 1.10; the full Web suite was not rerun there |
| BindingAndCatalysis suite | All testsets passed; golden-value set 145/145 | Tested models and solver policies only |
| Browser and Python consumers | JS suite passed (including renderer 25/25, reference producer 7/7, shape nodes 12/12); local real-page menu/node/port/intent/fail-closed smoke had zero console errors; Chat API 12/12, Design Agent 39/39, Reader 15/15, and repository Python 103/103 passed | No live LLM provider or browser-to-live-engine optimizer run |
| Frozen cat artifact | Read-only contract 160/160; four geometric optima, three replay passes | One fixed topology and finite replay grid |
| Repository gate | 39 maintained files, 14 schemas, and 49 routes passed regenerated and read-only checks | Local source/catalog consistency only |

These are results for one pinned topology, program, bounds, compiler, and
finite replay grid. They are not evidence of optimality over all networks,
experimental validity, or a live provider/cloud deployment.

## Integrated macOS host maintenance

The integrated revision also hardens the native shell, project
persistence, local helper identity, browser-content boundary, and release
packaging. The configured CI matrix now builds and runs the Swift unit target on
`macos-15-intel` and `macos-26`. On 2026-07-15, a local macOS 27 arm64 run with
Xcode 26.6 passed `build-for-testing` and all unit tests. Browser, Design Chat,
packaged-resource, and release-metadata contracts also passed locally.

This is local evidence retained from revision `f2ca13c` and extended at
`b91cf41`. No remote
CI result, real Cognito flow, packaged helper launch, relocatable Python input,
Developer ID signature, notarization, stapling, Gatekeeper install, or clean-host
qualification is claimed.

The evidence below is local repository evidence. It does not establish that a
remote workflow ran, an image was published, AWS or Slurm accepted a job, or a
signed application was installed.

## Evidence recorded for this snapshot

| Check | Local result through `b91cf41` | Boundary |
|---|---|---|
| Full Julia web suite | Completed with no failing tests; one pre-existing `@test_broken` remains explicitly recorded | Local process only; not evidence of a remote CI run or production traffic |
| `BindingAndCatalysis` suite | All testsets passed; golden-value set 145/145 | Numerical regression coverage is conditional on the tested models and solver policies |
| Browser JavaScript suite | `npm run test:js` passed, including scan-validity, SQLite-policy, and Design Screen rendering contracts | No full browser-to-live-provider Design Agent conversation was exercised |
| Browser quality gate | `npm run lint` passed with zero warnings; local real-Chromium `npm run test:e2e` passed its workflow, v2, axe, and visual contracts | Mocked loopback `/api/v1` endpoints; no remote CI or live Julia/provider/cloud call |
| Native Workspace v2 | Local no-sign build and 51/51 Swift unit tests passed, including shared migration fixtures | No WebView UI automation, packaged-process launch, signing, or clean-host install |
| Release evidence contract | Evidence Schema/template tests passed and the five-lane runbook is prepared | Registry, Compose/TLS, AWS, Slurm, signing/notarization, and rollback lanes were not executed |
| Generated schema check | `julia --project=webapp webapp/scripts/gen_schemas.jl --check` passed | Confirms generated NetworkIR/DesignSpec drift only; hand-authored schemas retain their cataloged coverage levels |
| Deployment contract suite | `python3 tests/test_deployment_contract.py -q` passed, 15/15 | Static and mocked checks do not prove a registry, TLS endpoint, AWS account, or rollback was exercised |

## Verified revision line

| Revision | Result added |
|---|---|
| `d752e15` | Secured loopback startup, Design Chat origin/token handling, native helper launch, and runtime host boundaries |
| `127a2a5` | Added a portable Julia 1.10 lock for the headless HPC environment and removed personal checkout paths |
| `cec0b59` | Synchronized release identity, packaged Design Agent resources, and the split web/HPC CI matrix |
| `01a01be` | Added fail-closed container, image, Compose, TLS, AWS setup, and rollback contracts |
| `603635d` | Moved the backend monolith into focused runtime, model, analysis, placement, scan, geometry, and service files; an assembly contract checks the dependency order and exported surface |
| `1177a3d` | Bounded shared runtime work, serialized mutable state, tightened request validation, preserved numerical failure information, and promoted Design Screen to v0.3 |
| `f2ca13c` | Integrated fixed-topology ROP shape control, durable job ownership, browser lifecycle hardening, deterministic volume contracts, and macOS host maintenance; this is the prior integrated revision |
| `b91cf41` | Added strict typed workflow scheduling, shared result freshness, atomic GraphPatch construction, Workspace v2 JavaScript/Swift parity, measured API aliases, local Chromium/axe/visual gates, and the prepared external release-evidence pack |

## What `1177a3d` establishes

### Shared state and concurrency

- Compiled model bundles have explicit per-bundle ownership, while cache misses
  for the same content hash build once and share the completed result.
- Session aliases and compiled models are bounded state with independent access
  times and least-recently-used eviction behavior.
- Cold regime-graph construction is single-flight, and complete SQLite
  read-modify-write updates are serialized instead of locking only the final
  write.
- The accepted decision record is
  [`0001-shared-runtime-concurrency`](../decisions/0001-shared-runtime-concurrency.md).

### Bounded synchronous work and request handling

- JSON request bodies are limited to 1 MiB at the application boundary and in
  the provided Nginx configuration. Oversized requests return a machine-readable
  413 response.
- Heavy synchronous handlers admit at most a bounded number of concurrent
  requests. Capacity exhaustion returns 429 with retry information; a request
  whose projected or unfolding work exceeds its limit returns 422.
- Model regime candidates, SISO/change paths, atlas builds, corpus reads,
  queries, inverse-design refinement, scans, and geometry sampling now have
  explicit synchronous limits. Larger atlas/library/query/inverse workloads
  belong in the asynchronous job path.
- HTTP SQLite paths are disabled by default. Operator opt-in restricts paths to
  a configured atlas-store root; synchronous writes and persistence remain
  asynchronous-only.

These controls bound one request. They do not prove that every accepted request
will meet a particular latency target on every machine.

### Numerical results and plotting

- One- and two-dimensional scans, atlas landscapes, ROP clouds, and FRET grids
  preserve per-sample success or failure and expose partial-result metadata.
- Non-finite or failed samples are masked before plotting, so the browser keeps
  them as gaps and does not coerce string forms of `NaN` or infinity into data.
- Placement and inverse-design verification do not rank an incomplete numerical
  scan as a successful candidate.

A valid marker means the configured solver produced a finite result for that
sample. It is not experimental validation and does not establish biological
correctness.

### Design Screen v0.3

- `bne-design-screen/v0.3.0` adds `eligible_count`, `evaluated_count`, and
  `truncated` at the top level and in the screen summary.
- The browser displays evaluation coverage and warns when results are not
  exhaustive; it retains a count fallback for v0.2 responses.
- `recommended` remains a compatibility alias for
  `verified_recommendations`. Proxy-only screened candidates are never promoted
  into the verified section.

## Component ownership after `603635d`

`webapp/src/BiocircuitsExplorerBackend.jl` is now an assembly root rather than a
second home for handler implementations. The extracted owners include:

- runtime lifecycle, request support, shared-work limits, and path-work limits;
- model resolution, model handlers, serializers, and numerical computations;
- parameter placement, level solving, parameter scans, and ROP geometry;
- atlas build, corpus, and query budgets; and
- ordinary service handlers.

The exact owner paths and focused tests are listed in
[the module catalog](../catalogs/modules.yaml). The assembly regression test
checks that these files remain included once and in dependency order.

## Earlier verified boundaries retained

- `/api/v1/*` is canonical. Bare `/api/*` routes remain compatibility aliases
  until the sunset projected from executable route metadata into the
  [generated reference](../generated/reference.md#api-routes). All tracked
  first-party clients are canonical; the bounded legacy-request counter informs
  but does not itself authorize alias removal.
- Liveness, readiness, and Prometheus endpoints remain `/health`, `/ready`, and
  `/metrics`.
- The local Design Chat helper remains loopback-only with exact-origin and
  bearer-token checks outside its explicit unauthenticated development mode.
- The Docker workflow and deployment helpers retain their fail-closed version,
  readiness, immutable-reference, AWS-state, and rollback contracts.
- The headless environment retains separate Julia 1.10 and 1.12 dependency
  locks. No real scheduler run is inferred from those files or load checks.
- Atlas and Reader results remain priors in the Design Agent. Fresh engine
  evidence is required before a candidate can be displayed as verified; see
  [the scientific evidence boundary](../contracts/scientific-evidence.md).

## Historical baseline retained

`f9c65a5` remains the baseline revision named by the knowledge manifest and
catalogs. At that inspected revision, the earlier maintenance gate established:

- required Julia tests were active instead of silently treated as expected
  failures;
- inverse-design summaries retained the full reusable-result identity;
- `d=1` single-base homomer candidates were covered by tracked Python and Julia
  checks;
- invalid equilibrium-matrix dimensions failed closed and the SBML bridge kept
  executable identifiers separate from display names;
- local job cancellation was cooperative, terminal states were monotonic, and
  cloud calls did not run while the shared job lock was held;
- the Reader no-fabrication and optional-dependency boundary ran in CI; and
- the degenerate-polyhedron cleanup guard had bounded and unbounded `d=1`
  regression coverage.

Those baseline claims remain historical evidence. Later revisions extend them;
they do not rewrite the catalog's baseline identifier or historical
`current-at-f9c65a5` contract versions.

## Known unknowns and unverified surfaces

1. No remote CI run is claimed for `b91cf41`; the
   evidence table records local commands and configured workflow ownership only.
2. No current evidence establishes publishing to or pulling from a live image
   registry. Image signing, signature verification, and an SBOM release lane
   are also unverified.
3. No live AWS rollout or Slurm run was verified. ECR, Batch, Cognito, S3, IAM,
   quota storage, scheduler behavior, artifact return, and rollback remain
   external unknowns.
4. A signed, notarized, stapled, installed, and Gatekeeper-tested macOS package
   remains unverified.
5. The complete Compose stack with Nginx, a real domain, TLS certificates,
   renewal, readiness admission, and rollback was not run end to end.
6. The deterministic-provider integration contract exercises the production
   Design Agent dispatch against a live local Julia engine through Design Screen
   and Placer. No test contacts a live model provider or proves provider language
   quality, multi-turn convergence, or release-locked retrieval-corpus behavior.
7. Runtime Atlas datasets are optional and not tracked in this checkout. A
   missing dataset manifest means the generic artifact validator cannot prove
   that dataset is reproducibility-pinned.
8. Several hand-authored JSON Schemas still lack instance-level coverage.
   Design Screen v0.3 has direct backend and rendering contracts; that does not
   upgrade unrelated schemas.
9. Paper-side datasets, figures, drafts, and provenance records are deliberately
   untracked. This repository cannot verify manuscript claims or their release
   lineage.
10. Work limits are operational safeguards, not proofs that bounded search is
    scientifically complete. A rejected synchronous request is not evidence
    that the requested circuit or behavior is impossible.

## Verification entry points

Run commands from the repository root:

```bash
JULIA_NUM_THREADS=auto julia --project=webapp webapp/test/runtests.jl
julia --project=Bnc_julia Bnc_julia/test/runtests.jl
(cd webapp && npm run lint && npm run test:js)
(cd webapp && npm run test:e2e && npm run check-i18n-sync)
julia --project=webapp webapp/scripts/gen_schemas.jl --check
python3 tests/test_workspace_schema.py
python3 tests/test_release_candidate_evidence.py
python3 tests/test_deployment_contract.py -q
python3 scripts/verify_repository.py --check
```

Broader release, Python, Reader, HPC, and macOS checks remain separate from
this command list. A successful narrow command does not prove an unrelated
phase complete.

The standard CI owner is
[`.github/workflows/ci.yml`](../../.github/workflows/ci.yml). The
single-image runtime owner is
[`.github/workflows/docker.yml`](../../.github/workflows/docker.yml). Workflow
configuration is executable ownership of the matrix, not evidence that a
particular remote run passed.

## Current knowledge gate

The generated reference and unified drift checker cover routes, schema
identities, version owners, configured toolchains, links, catalog paths,
public-safety rules, and declared artifact fixtures. The check snapshots the
Git-visible tree and fails on validator side effects. Keep unresolved paper,
release, and external-deployment claims explicitly unknown until recorded
evidence exists.
