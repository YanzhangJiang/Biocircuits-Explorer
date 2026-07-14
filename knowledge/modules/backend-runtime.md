---
module: backend-runtime
status: verified
verified_against: 1177a3d
---

# Backend runtime

## Purpose

Expose the engine, atlas, designability, import/export, and job workflows as a
versioned HTTP service without allowing one expensive or concurrent request to
corrupt a shared model or consume unbounded interactive resources. The current
contract shares compiled work by content, serializes mutation only for the same
model bundle, admits at most two heavy synchronous handlers per process, and
reports size, budget, capacity, and numerical failures explicitly.

A process being alive is still not enough to receive traffic: readiness means
the module initialized, the static directory and both browser/native entry pages
exist, and the job store is writable. The bind rule follows the launcher. A
supervised native process defaults to loopback; an unsupervised server defaults
to all interfaces for container/host deployment. An explicit validated host
override wins in either mode.

## Current verified contract

- `NetworkIR` content hash is the compiled-model identity. The cache publishes
  one current canonical `ModelBundle` per hash; concurrent cold misses use a
  refcounted same-hash single-flight lock, while each published bundle has its
  own `ReentrantLock` for mutable model and lazy-handler state. A request that
  already pinned an older bundle may finish safely after cache eviction.
- Request resolution pins the selected bundle in task-local storage for the
  handler call. Model handlers named by `model_runtime.jl` execute under the
  bundle lock, so a request cannot lock one bundle and later resolve another.
- Model-cache and session-alias access times live in separate locked tables.
  Their LRU/TTL policies are independent; sessions remain convenience aliases,
  and a full session table evicts its least-recently-used alias instead of
  turning alias creation into a server error.
- A process-wide gate admits exactly two named heavy synchronous handlers. The
  next request fails immediately with HTTP 429,
  `code=sync_capacity_exhausted`, `Retry-After: 1`, and `retryable=true`.
- Local asynchronous work has a separate process-local queue contract. By
  default at most `min(Threads.nthreads(), 2)` jobs compute concurrently and at
  most 64 jobs may be admitted across running and queued work. Strict positive
  integer environment settings can adjust those limits within hard bounds of
  64 concurrent and 4,096 admitted jobs. A full queue fails before quota
  consumption or durable writes with HTTP 429,
  `code=local_job_capacity_exhausted`, `Retry-After: 1`, and `retryable=true`.
- Canonical job state is serialized by 128 stable job-ID stripes, so JSON
  reads/parsing, atomic rename/fsync, projection repair, and snapshots for one
  job do not hold the process-wide registry lock or block an unrelated stripe.
  The in-memory `JOBS` table is a true LRU cache with a strict default capacity
  of 1,024 records and a validated hard maximum of 65,536; `record.json`
  remains authoritative, and queued/running local or AWS records may be
  evicted and cold-loaded safely.
- A structurally valid request above a declared synchronous limit fails before
  the expensive allocation with HTTP 422, `code=sync_budget_exceeded`, and
  `retryable=false`. JSON request bodies have a 1 MiB application limit; Nginx
  applies the matching hard pre-proxy cap and both paths return a machine-readable
  HTTP 413 contract.
- Synchronous Web SISO/change-path construction is capped at 2,000 paths and
  200,000 materialized path nodes. Parallel atlas network workers explicitly
  propagate the synchronous context and typed budget errors. ROP-shape and
  Atlas local jobs explicitly use the same values as job hard materialization
  bounds; unrelated direct/offline APIs do not inherit them automatically.
- Numerical scans, FRET heatmaps, qK-sampled ROP clouds, and scan-based
  placement/inverse-refinement responses expose validity/partial state and do
  not rank a failed terminal solver value as successful evidence. Point-only
  verification rejects an unsuccessful solve instead of returning it as valid.
- Exact relabel-invariant topology hashing stops at seven free species. Larger
  inputs fall back to the positional `NetworkIR` content hash; cache identity is
  deterministic but no longer guaranteed invariant to renaming or ordering.

## Non-goals

- Owning mathematical ROP semantics; those belong to `Bnc_julia`.
- Treating sessions or in-memory job dictionaries as durable scientific data.
- Making optional Python/LLM output authoritative.
- Coordinating model locks, admission slots, or LRU state across Julia
  processes or replicas.
- Providing a fair queue, request deadlines, or cancellation for synchronous
  handlers; those callers receive fail-fast capacity/budget responses.
- Treating solver convergence as analytic or biological proof.
- Providing TLS or network authentication for every deployment mode; those are
  explicit proxy/auth configuration responsibilities.
- Guaranteeing that every historical endpoint already returns one uniform
  result envelope.

## Owner paths

- Module assembly:
  [`webapp/src/BiocircuitsExplorerBackend.jl`](../../webapp/src/BiocircuitsExplorerBackend.jl)
- Bind policy, lifecycle, routing, and server start:
  [`webapp/src/runtime_lifecycle.jl`](../../webapp/src/runtime_lifecycle.jl),
  [`webapp/src/routing.jl`](../../webapp/src/routing.jl),
  [`webapp/server.jl`](../../webapp/server.jl),
  [`webapp/start.sh`](../../webapp/start.sh)
- Runtime configuration: [`webapp/src/config.jl`](../../webapp/src/config.jl)
- Request parsing and machine error mapping:
  [`webapp/src/request_support.jl`](../../webapp/src/request_support.jl),
  [`webapp/src/serialization.jl`](../../webapp/src/serialization.jl),
  [`webapp/src/routing.jl`](../../webapp/src/routing.jl)
- Synchronous admission, endpoint costs, and SISO path policy:
  [`webapp/src/sync_work_budget.jl`](../../webapp/src/sync_work_budget.jl),
  [`webapp/src/path_work_budget.jl`](../../webapp/src/path_work_budget.jl)
- Model resolution, request pinning, cache, and session aliases:
  [`webapp/src/model_runtime.jl`](../../webapp/src/model_runtime.jl),
  [`webapp/src/model_cache.jl`](../../webapp/src/model_cache.jl),
  [`webapp/src/session_store.jl`](../../webapp/src/session_store.jl)
- Service/model/scan/placement handlers:
  [`webapp/src/service_handlers.jl`](../../webapp/src/service_handlers.jl),
  [`webapp/src/model_handlers.jl`](../../webapp/src/model_handlers.jl),
  [`webapp/src/parameter_scan_handlers.jl`](../../webapp/src/parameter_scan_handlers.jl),
  [`webapp/src/parameter_placement.jl`](../../webapp/src/parameter_placement.jl)
- Static and local-image boundary:
  [`webapp/src/static_assets.jl`](../../webapp/src/static_assets.jl)
- Jobs and AWS request boundary: [`webapp/src/jobs.jl`](../../webapp/src/jobs.jl)
- Authentication: [`webapp/src/auth.jl`](../../webapp/src/auth.jl)
- Serialization and observability:
  [`webapp/src/serialization.jl`](../../webapp/src/serialization.jl),
  [`webapp/src/observability.jl`](../../webapp/src/observability.jl)
- Application/build identity: [`webapp/src/version.jl`](../../webapp/src/version.jl),
  [`VERSION`](../../VERSION), [`tests/version_resource_contract.jl`](../../tests/version_resource_contract.jl)
- Container runtime gate: [`deploy/Dockerfile`](../../deploy/Dockerfile),
  [`.github/workflows/docker.yml`](../../.github/workflows/docker.yml)

## Inputs

- HTTP requests on root probes/static paths, canonical `/api/v1/...`, or known
  compatibility `/api/...` routes.
- Versioned IR/designability payloads, supported legacy network requests, SBML,
  atlas/job specifications, and local image paths.
- Environment settings for bind host/port, parent supervision, public assets,
  job storage, job-cache capacity, local-job concurrency/admission, local-image
  exposure, AWS, Cognito, and quotas.
- Local files or S3 URIs for batch input/status/result artifacts.
- Application identity from an explicit environment value or a `VERSION` file
  in the installed resource bundle/source tree.

## Outputs

- JSON responses and structured request errors, including 413 body, 422 work
  budget, and 429 temporary-capacity contracts.
- Per-sample/per-cell validity and `partial` flags for numerical responses whose
  solver can fail at only part of a requested grid or search.
- Static web assets, liveness/readiness payloads, Prometheus metrics, and
  application/API version discovery.
- Process-local sessions and model caches.
- Atomic local job records/results or S3-backed Batch artifacts.
- Supported local image bytes only when bind/origin/opt-in rules permit them.

## Contract sources

- Bind selection, parent watchdog, and readiness checks:
  [`runtime_lifecycle.jl`](../../webapp/src/runtime_lifecycle.jl) and
  [`routing.jl`](../../webapp/src/routing.jl)
- Host, parent, local-image, and AWS opt-in settings:
  [`config.jl`](../../webapp/src/config.jl)
- Same-origin and loopback local-image rules:
  [`static_assets.jl`](../../webapp/src/static_assets.jl)
- AWS trusted runtime settings versus request overrides:
  [`jobs.jl`](../../webapp/src/jobs.jl)
- Route table, method rules, v1 mapping, and deprecation metadata:
  [`api_contract.jl`](../../webapp/src/api_contract.jl) and
  [`routing.jl`](../../webapp/src/routing.jl)
- Runtime/build version and bundle lookup:
  [`version.jl`](../../webapp/src/version.jl), [`VERSION`](../../VERSION),
  [`packaging/build_backend_app.jl`](../../packaging/build_backend_app.jl), and
  [`scripts/build_macos_dmg.sh`](../../scripts/build_macos_dmg.sh)
- Job lifecycle and result provenance: [`jobs.jl`](../../webapp/src/jobs.jl),
  [`result_artifact.jl`](../../webapp/src/result_artifact.jl), and
  [`result-artifact.schema.json`](../../schemas/result-artifact.schema.json),
  plus the working-tree asynchronous commit marker in
  [`job-result-manifest.schema.json`](../../schemas/job-result-manifest.schema.json)
- Shared-model locking, content-hash build single-flight, and request bundle
  pinning: [`model_runtime.jl`](../../webapp/src/model_runtime.jl)
- Independent model/session LRU timestamps:
  [`model_cache.jl`](../../webapp/src/model_cache.jl) and
  [`session_store.jl`](../../webapp/src/session_store.jl)
- Heavy admission, typed synchronous budgets, and path enumeration adapters:
  [`sync_work_budget.jl`](../../webapp/src/sync_work_budget.jl) and
  [`path_work_budget.jl`](../../webapp/src/path_work_budget.jl)
- Body shape/size and numerical validity:
  [`serialization.jl`](../../webapp/src/serialization.jl),
  [`parameter_scan_handlers.jl`](../../webapp/src/parameter_scan_handlers.jl),
  [`model_handlers.jl`](../../webapp/src/model_handlers.jl), and
  [`inverse_design.jl`](../../webapp/src/inverse_design.jl)

## Tests

- [`webapp/test/runtests.jl`](../../webapp/test/runtests.jl) covers bind defaults
  and override validation; liveness/readiness checks; same-origin, wrong-port,
  DNS-rebinding, loopback, public-bind, and explicit local-image cases; AWS
  request override opt-in; routing, auth/quota, jobs, SBML/IR, handlers, and
  serialization.
- [`webapp/test/jobs_cancellation_contract.jl`](../../webapp/test/jobs_cancellation_contract.jl)
  covers local/AWS cancellation, finish, submit, and dispatch races.
- [`webapp/test/jobs_submission_reconciliation_contract.jl`](../../webapp/test/jobs_submission_reconciliation_contract.jl)
  covers at-most-once AWS submission, crash/response-loss reconciliation,
  strict candidate adoption, and explicit unknown/conflict states.
- [`webapp/test/jobs_cache_concurrency_contract.jl`](../../webapp/test/jobs_cache_concurrency_contract.jl)
  covers cross-job progress during blocked persistence, same-job serialization,
  cold-load single-flight, wrong-directory identity rejection, hard LRU
  eviction/reload, active local/AWS ownership across eviction, projection
  repair, and cache stress/invariants.
- [`webapp/test/cooperative_cancel_checkpoints_contract.jl`](../../webapp/test/cooperative_cancel_checkpoints_contract.jl)
  covers cancellation propagation through long workflows.
- [`tests/version_resource_contract.jl`](../../tests/version_resource_contract.jl)
  constructs an installed bundle layout and proves that runtime version lookup
  finds its copied `VERSION` resource before unavailable source paths.
- [`tests/test_deployment_contract.py`](../../tests/test_deployment_contract.py)
  checks local-start loopback propagation, the single-image runtime gate, and
  the Nginx 1 MiB JSON 413 boundary.
- [`webapp/test/concurrency_and_budget_contract.jl`](../../webapp/test/concurrency_and_budget_contract.jl)
  covers per-bundle serialization, parallelism across bundles, same-hash
  single-flight, independent LRU clocks, the two-slot gate, typed HTTP errors,
  SISO context propagation, and large-network preflight/fallback behavior.
- [`webapp/test/input_validation_contract.jl`](../../webapp/test/input_validation_contract.jl)
  covers finite/type/shape validation, request-body limits, and path-policy
  boundaries.
- [`webapp/test/numerical_validity_contract.jl`](../../webapp/test/numerical_validity_contract.jl)
  covers validity masks and partial-result propagation.
- [`webapp/test/*.test.mjs`](../../webapp/test/) and the Python agent tests cover
  browser/helper request boundaries that consume this runtime.

## CI

[`.github/workflows/ci.yml`](../../.github/workflows/ci.yml) runs the Julia
backend suite, job race contracts, JavaScript/Python consumers, schema/repository
checks, and the installed-layout version-resource test.

[`.github/workflows/docker.yml`](../../.github/workflows/docker.yml) is
configured to build and start one backend image, wait for `/ready`, probe
`/health` and `/api/v1/version`, fetch `/index.html`, and write the job store.
No external workflow run is claimed; it does not start the Nginx/TLS Compose
stack or a real AWS worker.

## Invariants

- With no explicit host, a valid parent PID selects `127.0.0.1`; without a
  supervisor it selects `0.0.0.0`. A host override must be a validated hostname
  or IP without scheme, path, or embedded single-colon port.
- `/health` is cheap liveness. `/ready` returns 200 and `status: ready` only when
  module initialization, static directory, `index.html`, `index-node.html`, and
  writable job storage all pass; otherwise it returns 503 and named checks.
- A supervised loopback runtime may serve supported local image types. Browser
  requests with `Origin` must match the request scheme, host, and port exactly,
  and a supervised request's origin host must be loopback. Non-browser requests
  may omit `Origin`.
- A public/non-supervised bind does not serve local images unless
  `BIOCIRCUITS_EXPLORER_ALLOW_LOCAL_IMAGES` is explicitly enabled. Cross-origin
  browser reads remain rejected when an Origin is present.
- Request payloads cannot replace operator-owned AWS queue, job definition,
  artifact prefix, job-name prefix, container environment, vCPU, or memory
  settings unless `BIOCIRCUITS_EXPLORER_ALLOW_AWS_BATCH_REQUEST_CONFIG` is true.
- `/api/v1` is canonical. Known bare `/api` aliases reach the same handler and
  carry deprecation metadata; probes and static assets are not legacy API calls.
- Application version and build revision remain distinct from API protocol
  identity. Bundle builders copy `VERSION` to
  `share/biocircuits-explorer/VERSION`, and runtime lookup supports that
  installed layout before falling back to source paths or `0.0.0-dev`.
- Request IDs are bounded/sanitized and metrics use low-cardinality route
  labels. `NetworkIR` content identity, not a session ID, is the stable cache
  key.
- One content hash has one current cache-published process-local bundle. A
  same-hash cold miss is single-flight, and each model-bearing handler keeps one
  pinned bundle under its per-bundle lock for the complete call. Different
  bundles do not share that lock; an already-pinned evicted bundle remains valid
  through the end of its request.
- Model-cache and session-alias LRU timestamps are independent. Cache/session
  callbacks run after their internal locks are released, and an alias collision
  cannot silently replace a different live bundle.
- The synchronous heavy gate has two process-local slots. Capacity failure is a
  retryable 429 with `Retry-After: 1`; a static work-limit failure is a
  non-retryable 422. Neither is mapped to 500.
- Local asynchronous admission is also process-local but independent of the
  synchronous gate. A fixed semaphore bounds active computation; admitted jobs
  waiting for a permit remain queued, and admission ownership is released on
  success, failure, or cancellation cleanup.
- Application JSON size is at most 1 MiB. The deployed Nginx route enforces the
  same value before proxying; the application additionally bounds JSON nesting
  and value count after parsing.
- Synchronous Web path materialization stops at 2,000 paths or 200,000 total
  path nodes, including atlas work delegated to spawned Julia tasks. Local jobs
  and direct/offline engine calls remain unchanged by these Web-only counters.
- Failed numerical samples remain invalid and make the containing response
  partial. Invalid values are not eligible for curve transitions, design
  refinement scores, or UI interpolation.
- Exact relabel canonicalization is bounded at seven free species. Beyond that,
  the deterministic positional content hash is accepted with weaker identity
  semantics rather than starting factorial work.
- `JOBS_LOCK` owns only short cache, task/token/admission, describe, and
  submission-owner metadata sections. Canonical state changes acquire the
  stable job stripe first; JSON parsing, deepcopy, file publication, projection
  inspection/repair, long computation, and external AWS/S3 calls never hold
  `JOBS_LOCK`. Same-job state remains serialized, terminal state is monotonic,
  and unrelated stripes can progress while one job's disk I/O is blocked.
- `JOBS` is a bounded process-local LRU, not an authority or active-job pin.
  Its capacity uses `BIOCIRCUITS_EXPLORER_JOB_CACHE_CAPACITY` (default 1,024,
  hard maximum 65,536). Cold misses are single-flight under the job stripe and
  must load a canonical record whose `job_id` matches its directory. Local
  worker/token/admission ownership and initial AWS submission ownership live
  outside the cache, so eviction cannot trigger false restart recovery.
- AWS submission identity is canonical state, not transient request context.
  New records persist the resolved job name, queue, definition, Batch region,
  optional account ID, tag, exact worker command, and artifact URIs before
  remote I/O. `dispatch_started` authorizes SubmitJob only after its exact
  parent-directory fsync is confirmed; a committed-but-unconfirmed boundary
  performs no submit and remains reconciliation-only. Once the boundary
  commits, the broker never issues another SubmitJob for that job ID.
  Reconciliation requires complete one-for-one DescribeJobs coverage of every
  ListJobs ID before applying zero/one/many candidate semantics, and full ARNs
  never degrade to resource-name matching.
- `record.json` is canonical restart state. A remote success without the
  expected result artifact is failed rather than promoted to success. In the
  working tree, new AWS jobs additionally require a result manifest published
  after `result.json`; status polling validates that bounded marker and object
  metadata without downloading the potentially large result payload.
- Canonical job publication on macOS/Linux writes and fsyncs a same-directory
  temporary file, performs one no-fallback atomic rename, and fsyncs the parent
  directory. Rename is the logical commit point: a later directory-fsync
  failure does not roll memory back, but readiness remains false until the
  exact pending directory is synced successfully.
- Every canonical commit owns one monotonically increasing `state_revision`.
  `status.json` is a derived projection: missing, malformed, stale, ahead, or
  same-revision-but-different content is rebuilt from `record.json`. Legacy
  canonical records read as revision zero until their next real commit. A
  projection without a canonical record is never treated as authoritative.
- Local input/result/manifest/terminal-status artifacts use a stricter rule
  than canonical records: directory durability must be confirmed before the
  next commit marker or success state can be published.

## Known gaps

- P2 — The unsupervised default intentionally listens on all interfaces; a
  direct public launch still needs deliberate firewall, TLS proxy, and auth
  configuration.
- P2 — Local-image opt-in on a public bind allows same-origin callers and
  non-browser clients to request supported files by path; operators must not
  enable it for an untrusted deployment.
- P2 — Docker CI covers one local image, not the full Compose/TLS stack, native
  shell, live AWS Batch/S3/Cognito/quota path, or broker/worker lifecycle.
- P2 — The bundle version/resource contract does not build and launch a complete
  macOS bundle in CI.
- P2 — Ordinary browser compute calls and the Python engine client use
  canonical `/api/v1/*` routes. Browser broker/auth configuration and
  local-image transport still retain compatibility paths, so complete alias
  removal remains a coordinated deployment change.
- P2 — Result-envelope adoption remains additive rather than uniform across all
  synchronous historical handlers.
- P2 — Admission, build locks, bundle locks, and LRU tables are process-local;
  they neither coordinate replicas nor impose a cluster-wide capacity limit.
- P2 — The fixed two-slot gate has no fairness, waiting queue, deadline,
  cancellation, or adaptive cost estimate. It is acquired before the bundle
  lock, so same-bundle contention can occupy an admitted slot while waiting.
- P2 — Whole-handler bundle locking is deliberately conservative. Read-mostly
  requests for the same model serialize until the engine's mutable caches have
  a finer-grained concurrency contract.
- P2 — HTTP.jl has already assembled `req.body` when the application checks the
  1 MiB limit. Only the Nginx path is proven to reject oversized bodies before
  Julia buffers them.
- P2 — Direct/offline and job path enumeration is not restricted by the two Web
  path-materialization counters. Local-job task count is now bounded, but a
  single admitted job can still request large work and therefore requires
  explicit partitioning, quota, and cooperative cancellation.
- P2 — Positional hashing above seven free species can split cache identity for
  relabeled/reordered equivalents; scalable exact canonicalization is not yet
  provided.
- P2 — A validity flag reports the configured solver's convergence status, not
  mathematical uniqueness, discretization completeness, or biological truth.
- P2 — Durable local job-store publication is implemented and tested on
  macOS/Linux only; Windows needs a separately owned atomic-replacement and
  directory-durability contract before it can make the same claim.

## Change protocol

1. Classify the change as bind/process, readiness, local file exposure, API,
   cache, persistence, auth, or observability; list every client and deployment
   entrypoint it affects.
2. Change bind/readiness behavior only with focused Julia tests plus native,
   start-script, Docker, and Compose alignment at the evidence level claimed.
3. Keep local-image access fail-closed for public binds and exact-origin for
   browsers; add adversarial origin/host/port tests for every rule change.
4. Keep AWS request overrides disabled by default and test both disabled and
   explicitly enabled paths when adding an override.
5. Preserve v1/legacy response parity until compatibility clients migrate; add
   race tests before changing jobs and never put network/cloud I/O under
   `JOBS_LOCK`.
6. For concurrency changes, prove same-hash single-flight, same-bundle
   serialization, cross-bundle progress, exception cleanup, and typed 422/429
   mapping. Explicitly propagate synchronous context through any new spawned
   worker.
7. For numerical handlers, preserve `valid`/`validity_grid` and `partial`; a
   failed solve may not re-enter ranking or plotting through a fallback value.
8. Copy and test `VERSION` in every new bundle layout; bump public API, artifact,
   or application versions when semantics, not merely implementation, change.

See [runtime topology](../architecture/runtime.md) and
[data provenance](../architecture/data-provenance.md).

## Verified against

- Current source commit: `1177a3d`
- Evidence inspected: module assembly; bind/configuration and readiness;
  per-bundle and same-hash locking; independent cache/session LRU clocks; heavy
  admission and typed budgets; path context propagation; body limits; numerical
  validity; jobs/persistence; focused Julia/JavaScript/Python tests; and both CI
  workflows.
- Historical baseline: `f9c65a5` remains evidence for the pre-P6 service shape
  and compatibility behavior. It does not describe the current shared-runtime,
  synchronous-budget, or numerical-validity contract.
- Boundary: source and local contract evidence plus the configured
  single-image runtime gate are recorded. No external workflow run, complete
  proxy/TLS stack, native packaged app, or live cloud integration is claimed
  verified.
