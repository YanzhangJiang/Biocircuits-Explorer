# 0001: Bound and serialize shared runtime work

- Status: accepted
- Date: 2026-07-10
- Verified against: `1177a3d`
- Historical evidence baseline: `f9c65a5`
- Owners: `backend-runtime`, `engine-rop`, `runtime`
- Supersedes: none
- Superseded by: none

## Problem in plain language

The Web service saves time by reusing a compiled biochemical model, but that
model contains caches that are filled lazily. Two requests must not build or
change those caches at the same time, and one interactive request must not begin
an enumeration large enough to make the process unresponsive. At the same time,
unrelated models and explicit offline jobs should not be forced through one
global serial bottleneck.

The decision is therefore to share by content, serialize work only around the
same shared model, and reject interactive work that exceeds a small, explicit
capacity or allocation boundary. Numerical failure is reported as missing
evidence rather than accepted as a computed result.

## Context and observed constraints

- A compiled `Bnc` instance and the backend bundle around it contain mutable,
  lazily populated regime, SISO, geometry, and handler caches. The shared object
  and request-resolution path are in
  [`webapp/src/model_runtime.jl`](../../webapp/src/model_runtime.jl).
- Regime construction publishes several interdependent fields before it is
  complete. [`Bnc_julia/src/regimes.jl`](../../Bnc_julia/src/regimes.jl) and
  [`Bnc_julia/src/initialize.jl`](../../Bnc_julia/src/initialize.jl) define the
  build, completion marker, and rollback path.
- Some network and source-to-sink path searches grow combinatorially. Checking a
  limit after materializing all candidates would not protect the process.
  Candidate and endpoint budgets are in
  [`webapp/src/sync_work_budget.jl`](../../webapp/src/sync_work_budget.jl), and
  path pre-allocation checks are in
  [`Bnc_julia/src/SISO.jl`](../../Bnc_julia/src/SISO.jl).
- Julia task-local storage is not inherited automatically by a raw
  `Threads.@spawn`. Parallel atlas workers therefore need deliberate policy
  propagation, implemented in [`webapp/src/atlas.jl`](../../webapp/src/atlas.jl).
- The HTTP service is interactive and has no general synchronous cancellation,
  deadline, or fair scheduling mechanism. The jobs path already provides
  identity, status, quota, cooperative cancellation, and worker isolation for
  declared heavy workflows.
- Numerical solvers can return a terminal state without a successful retcode.
  Scan validity is implemented in
  [`Bnc_julia/src/rop/rop_overlay.jl`](../../Bnc_julia/src/rop/rop_overlay.jl)
  and propagated by backend scan/model/placement/refinement handlers.
- Exact base-species relabeling is factorial. The bounded identity policy lives
  in [`webapp/src/canonicalization.jl`](../../webapp/src/canonicalization.jl)
  and [`webapp/src/ir.jl`](../../webapp/src/ir.jl).

## Decision

### Share and lock by content identity

`NetworkIR` content hash is the compiled-model cache key. A refcounted
single-flight lock permits only one cold build for a given hash. The published
value is a `ModelBundle` whose lock is stored outside the protected dictionary;
model-bearing HTTP handlers pin that exact bundle for the request and hold its
lock for the complete handler call. Different content hashes use different
bundle and build locks.

The model cache and session-alias store keep independent locks, last-access
tables, and LRU/TTL decisions. A session is a convenience alias, not identity,
and no shared timestamp lets one cache access refresh every session alias.

### Commit engine regime construction atomically to readers

`find_all_regimes!` holds `_regimes_affine_lock` from its completion check
through the complete regime/graph/affine build. `_regimes_build_complete` is the
commit marker; a non-null intermediate field is not sufficient. If construction
throws, partial state is cleared before unlocking. Public SISO graph access
passes through this path.

### Fail fast at the synchronous boundary

Each Julia HTTP process admits at most two named heavy synchronous handlers. If
both slots are occupied, the next request is not queued: it receives HTTP 429,
`code=sync_capacity_exhausted`, `Retry-After: 1`, and `retryable=true`.

Static model, candidate, sample, grid, expression, corpus, query, and endpoint
cost checks run before their expensive work. A request above one of those
limits receives HTTP 422, `code=sync_budget_exceeded`, and `retryable=false`.
The application accepts at most 1 MiB of JSON; the deployed Nginx path enforces
the matching hard cap before forwarding and returns the same machine-readable
HTTP 413 class.

### Keep path limits specific to interactive Web work

`SISOPaths` and `ChangePaths` accept optional engine allocation limits. In a
synchronous HTTP context the backend supplies at most 2,000 completed paths and
200,000 total materialized path nodes and maps the engine stop to HTTP 422.
Parallel atlas network workers explicitly propagate that context and preserve
typed budget exceptions across task joins.

Direct/offline engine calls and local job dispatch do not enter this context, so
the two Web-only counters default to unlimited there. This does not promise that
offline work is cheap; it leaves partitioning, quota, cancellation, and worker
resources to the job/operator boundary.

### Make numerical failure visible

Numerical consumers request solver status. Invalid points are masked, cannot
seed a subsequent warm start or contribute to a design score, and are exposed
as `valid`/`validity_grid` with `partial=true` when any requested point failed.
Solver success remains numerical evidence under the configured policy, not a
proof of the model or biology.

### Bound exact identity work

Exact relabel-invariant canonicalization is limited to seven free species. For
larger models, `network_ir_hash` uses the positional content representation, and
inverse-design support identity uses its deterministic positional fallback.
The fallback avoids factorial work but does not promise that renamed or reordered
equivalent networks share a hash.

## Alternatives considered

### One global runtime lock

A single process-wide lock would have been simple and would protect all mutable
models. It was not selected because unrelated content hashes would serialize,
making one popular or slow model block independent work even when two heavy
slots are available.

### Copy every model for every request

Per-request model copies would avoid shared mutation. They were not selected
because compilation and regime construction are expensive, copies would
multiply memory use, and session/hash cache behavior would lose its purpose.
This remains a possible isolation strategy for selected worker jobs.

### Queue synchronous work and schedule by estimated cost

A fair/adaptive queue could smooth bursts and use more detailed cost estimates.
It was not selected for this revision because the synchronous path has no
complete deadline, cancellation, ownership, or durable status contract. A
hidden in-process queue would make latency and abandonment worse. Heavy declared
work belongs on the jobs path until those controls exist.

### Apply the Web path caps to every engine caller

Uniform caps would be easy to explain but would break legitimate offline atlas
and research workflows. The engine therefore exposes optional allocation
limits, while the Web adapter supplies a strict policy and jobs retain explicit
operator control.

### Continue exact relabeling at every network size

This would preserve the strongest cache identity, but factorial enumeration can
consume memory and CPU before a later model budget has a chance to reject the
request. The bounded positional fallback is safer until a scalable canonical
algorithm has equivalent evidence.

## Consequences

### Benefits

- Concurrent same-hash cold requests build one model, and concurrent handlers
  cannot observe a partially published bundle or binding-regime graph.
- Different model hashes retain useful concurrency instead of sharing a global
  model lock.
- Interactive overload and structurally excessive work produce stable 429/422
  contracts instead of unbounded allocation or generic 500 responses.
- Path limits stop during materialization, and the policy follows parallel atlas
  workers instead of disappearing at a task boundary.
- Invalid numerical samples remain visible and cannot silently change plots,
  transition detection, placement verification, or inverse-design ranking.
- Large-network hashing is deterministic and bounded even though its identity
  semantics are weaker.

### Costs and risks

- Bundle locking is conservative whole-handler serialization. Two read-mostly
  requests for the same model cannot run concurrently even when their exact
  code paths would be safe.
- The two-slot gate is fixed and fail-fast. It has no fairness, priority,
  deadline, waiting queue, or synchronous cancellation. Because it is acquired
  before the bundle lock, a request can hold a slot while waiting on the same
  model.
- Admission, build locks, bundle locks, engine locks, and LRU tables are
  process-local. They do not coordinate replicas or enforce a cluster-wide
  concurrency limit. File-backed stores must rely on their own cross-process
  protocol; process-keyed locks are only an in-process aid.
- The application 1 MiB check runs after HTTP.jl has assembled `req.body`.
  Nginx supplies the hard pre-buffer boundary only on the verified proxied
  deployment path.
- Offline callers can still request very large path enumerations. They need
  explicit limits or job partitioning to avoid worker exhaustion.
- Positional identity above seven free species can duplicate cache/model work
  for relabeled or reordered equivalent networks.
- Static work estimates and a successful solver retcode do not enforce wall
  time, memory, mathematical uniqueness, or biological validity.

## Migration and rollback

Existing v1 and legacy API paths remain routed to the same handlers. Clients
should branch on the new machine codes: retry 429 responses after the declared
delay, reduce or move 422 work to a supported job/offline workflow, and treat
validity masks as authoritative for numerical samples. Clients must not assume
that every valid request waits for capacity.

Rollback of per-bundle or regime locking requires a replacement concurrency
proof and race tests; removing only the lock would reintroduce partial-publication
risk. Raising a synchronous or path limit requires pre-allocation and pressure
evidence, not only a happy-path benchmark. Replacing positional fallback with a
new canonicalizer requires identity compatibility tests and an explicit cache or
artifact migration plan.

## Verification

From the repository root:

```text
julia --project=webapp webapp/test/runtests.jl
julia --project=webapp Bnc_julia/test/runtests.jl
python3 -m unittest tests.test_deployment_contract
```

The backend suite includes the concurrency/budget, input-validation, and
numerical-validity contracts. Together they prove same-hash single-flight,
per-bundle serialization and cross-bundle progress, independent LRU clocks,
gate/error semantics, path-context propagation, bounded canonical fallback, and
validity/partial propagation. The engine suite proves path pre-allocation and
concurrent completed regime publication; the deployment contract proves the
Nginx body cap.

## Follow-ups

- [ ] Decide whether synchronous work needs a fair, cancellable,
  deadline-aware scheduler or should remain fail-fast (`backend-runtime`).
- [ ] Measure safe read-only engine paths and split the whole-handler bundle lock
  only after targeted first-use/concurrency evidence (`engine-rop`).
- [ ] Define cluster-wide admission/cache coordination if the service becomes a
  multi-replica shared runtime (`backend-runtime`).
- [ ] Add a transport-level body cap for direct HTTP.jl deployments or document
  a reverse proxy as mandatory for untrusted clients (`deployment`).
- [ ] Replace the greater-than-seven positional fallback with a scalable
  relabel-invariant canonicalizer and plan identity migration (`backend-runtime`).
