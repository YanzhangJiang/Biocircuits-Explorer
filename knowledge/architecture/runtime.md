---
title: Runtime and process topology
status: verified
verified_against: 1177a3d
---

# Runtime and process topology

The interactive server has to do two things that pull in opposite directions:
reuse an expensive compiled model across requests, but never let concurrent
requests observe a half-built model or start unbounded work. The verified
runtime resolves this by giving each shared `ModelBundle` its own lock and by
putting a fixed synchronous work budget in front of expensive HTTP handlers.
Different processes still do not share locks, cache entries, or admission
state.

The normal path is one web client talking to one Julia HTTP process. Heavy work
may instead run in a local Julia job or an AWS Batch worker, and the
conversational design surface may add a Python sibling process. Those paths
share wire and artifact contracts, but they deliberately do not share all
in-memory policy.

## Current verified contract

| Boundary | What a caller can rely on |
|---|---|
| Shared model | A compiled model is keyed by `NetworkIR` content hash. Concurrent misses for one hash are single-flight, and ordinary model handlers hold that bundle's lock for the complete handler call. Separate hashes retain separate locks. |
| Cache and session aliases | The model cache and session store keep independent last-access tables and LRU/TTL state. A cache access cannot refresh every session alias through one shared timestamp, and a session ID is never the scientific identity. |
| Heavy synchronous work | At most two named heavy handlers are admitted per Julia process. A request arriving when both slots are occupied fails immediately with HTTP 429, `code=sync_capacity_exhausted`, `Retry-After: 1`, and `retryable=true`; it is not queued. |
| Request and work size | JSON bodies are limited to 1 MiB by the application, while the deployed Nginx path enforces the same limit before proxying. Static work-limit violations return HTTP 422 with `code=sync_budget_exceeded` and `retryable=false`. |
| Path enumeration | Synchronous Web construction of `SISOPaths` or `ChangePaths` stops before materializing more than 2,000 paths or 200,000 total path nodes. Atlas worker tasks explicitly inherit the synchronous context. Local jobs and direct/offline engine calls do not receive these two Web-only caps. |
| Numerical solves | Scans and the numerical endpoints that consume them expose `valid` or `validity_grid` plus `partial`. A failed solve is masked instead of being ranked or plotted as a trustworthy terminal value. |
| Large-network identity | Exact base-species relabeling is attempted only through seven free species. Above that size, `NetworkIR` identity falls back to the positional content hash, so name/order-invariant identity is not promised. |

## Entry points

| Mode | Entry point | Processes |
|---|---|---|
| Source web development | [`webapp/start.sh`](../../webapp/start.sh) | Julia server plus optional Python design-chat sibling |
| Julia server only | [`webapp/server.jl`](../../webapp/server.jl) | one Julia HTTP process |
| Native macOS | [`frontend-swift/`](../../frontend-swift/) | Swift shell, Julia backend, optional Python design-chat sibling, WebView |
| Container | [`deploy/Dockerfile`](../../deploy/Dockerfile) | non-root Julia server; reverse proxy is defined separately in [`docker-compose.yml`](../../deploy/docker-compose.yml) |
| Batch worker | [`webapp/scripts/run_batch_job.jl`](../../webapp/scripts/run_batch_job.jl) | one Julia worker reading explicit input/status/result URIs |

`server.jl` activates the repository-local web project before importing
`BiocircuitsExplorerBackend`. That project resolves `BindingAndCatalysis` by the
relative source declaration in [`webapp/Project.toml`](../../webapp/Project.toml).

## Interactive request path

1. [`api.js`](../../webapp/public/js/api.js) serializes a JSON request. It may
   enrich a session-only request with the cached `NetworkIR` and its hash.
2. [`routing.jl`](../../webapp/src/routing.jl) canonicalizes a v1 or legacy URL,
   enforces methods, adds request IDs/CORS, admits a named heavy handler through
   the two-slot gate, and maps typed errors to HTTP responses.
3. [`model_runtime.jl`](../../webapp/src/model_runtime.jl) resolves and pins one
   bundle for the request. A same-hash cache miss is built once; model-bearing
   handlers then run under that bundle's lock. Service and model handlers live
   in focused files assembled by
   [`BiocircuitsExplorerBackend.jl`](../../webapp/src/BiocircuitsExplorerBackend.jl).
4. [`reaction_parser.jl`](../../webapp/src/reaction_parser.jl) and
   [`ir.jl`](../../webapp/src/ir.jl) convert accepted model inputs to the
   `Bnc` representation. [`model_cache.jl`](../../webapp/src/model_cache.jl)
   reuses compiled models by content identity; [`session_store.jl`](../../webapp/src/session_store.jl)
   is a convenience lookup, not the identity source.
5. [`sync_work_budget.jl`](../../webapp/src/sync_work_budget.jl) checks model,
   candidate, sample, and endpoint-specific cost limits before expensive work.
   [`path_work_budget.jl`](../../webapp/src/path_work_budget.jl) applies the
   Web-only SISO path caps. The handler returns JSON-safe data, after which
   request metrics and optional structured logs are recorded.

Health endpoints are outside the versioned API namespace: `/health` is a cheap
liveness probe, `/ready` checks module initialization and static assets, and
`/metrics` exposes bounded-cardinality Prometheus metrics. Their contract and
method handling are covered in [`webapp/test/runtests.jl`](../../webapp/test/runtests.jl).

## Optional design-chat path

The design UI posts to
[`chat_api.py`](../../webapp/scripts/chat_api.py), which holds no server-side
conversation state; the client sends the state each turn. A configured LLM
drives a bounded tool loop in `design_agent.py`; deterministic compilers and
helpers execute only inside that flow. The tools call the live Julia process
through [`engine_client.py`](../../webapp/scripts/engine_client.py) for model
building, scans, or phenotype checks. Its `/health` response reports corpus
availability and live engine readiness separately.

The Python service can start without an LLM key, but `/design-chat` then returns
`kind=need_key`; the Julia workspace remains available independently.

This process boundary has two consequences:

- restarting Python does not invalidate Julia sessions, but client-held agent
  state must be resent;
- a catalogue or LLM response must never be presented as a fresh engine result
  when the Julia process is unavailable.

## Asynchronous jobs

[`jobs.jl`](../../webapp/src/jobs.jl) accepts only the declared atlas and inverse
design job kinds. Submission creates explicit input, record, status, and result
locations.

### Local asynchronous execution

- A Julia task runs the same dispatch functions as synchronous endpoints.
- The task does not enter the synchronous HTTP context. In particular, the
  2,000-path/200,000-node Web caps are not added to job or direct/offline engine
  calls; job specifications and operator partitioning remain responsible for
  their scale.
- Cancellation is cooperative through `cancel_check` checkpoints; it does not
  interrupt a task with `Base.throwto`.
- State changes and atomic local record writes are serialized under `JOBS_LOCK`;
  long computation and external AWS/S3 calls occur outside that lock.
- Terminal states are immutable. A late completion cannot overwrite a terminal
  cancellation or failure.
- `record.json` is committed atomically before the in-memory view is published;
  `status.json` is a public projection.

### AWS Batch execution

The broker writes or uploads the request, submits a worker, polls Batch, and
reads the result from S3. Long AWS calls are performed without the global job
lock. A cancellation first publishes intent, then uses the remote cancel or
terminate operation appropriate to the observed lifecycle; ambiguous races
remain pending until remote state resolves. A Batch `SUCCEEDED` status is
downgraded to failure when the expected result artifact is missing.

A durable per-job dispatch claim admits only one concurrent remote cancel or
terminate call. A successful dispatch records its completion; a failed dispatch
clears the claim so a later request can retry, and an abandoned claim has a
bounded expiry. Preserve this claim protocol when changing cancellation.

When Cognito is configured, jobs require a verified bearer token. Development
mode can use an explicit user header. Ownership checks deliberately return an
unknown-job error for another user's identifier. Optional submission quota
state is stored in DynamoDB. These boundaries are implemented in
[`auth.jl`](../../webapp/src/auth.jl) and [`jobs.jl`](../../webapp/src/jobs.jl).

## State ownership

| State | Scope | Authority |
|---|---|---|
| Workspace graph and agent conversation | browser/Swift client | client document contract |
| Compiled `ModelBundle` and per-bundle lock | one Julia process, keyed by content hash | derived cache only; rebuild from versioned input |
| Session alias and model-cache LRU clocks | separate tables in one Julia process | convenience/eviction state, never scientific identity |
| Heavy-handler admission count | one Julia process | transient two-slot backpressure only |
| Local job lifecycle | process plus local job store | `record.json`; public status is best-effort derived |
| Atlas corpus | memory or SQLite | schema/version plus content identities and merge history |
| Batch input/result | broker, worker, S3 | explicit URI and per-user partition |
| Research extract/notebook | repository artifact | only as strong as its recorded source and verifier |

## Compatibility path

The router maps `/api/v1/<endpoint>` to the same handler used by the legacy
`/api/<endpoint>` form. Only the legacy form receives the deprecation header.
Jobs, auth, version discovery, and ordinary POST handlers all participate in
this mapping. Static files and root health endpoints do not.

Current browser code still calls the compatibility form. New non-browser
clients should use `/api/v1`; migration of the browser and Python engine client
must land before the legacy route can be removed.

## Failure expectations

- Invalid client input is a request error, not an engine result.
- A JSON body over 1 MiB is HTTP 413 with
  `code=request_body_too_large`; the Nginx route applies the hard pre-proxy cap.
- A request that is structurally valid but exceeds a synchronous work limit is
  HTTP 422, while temporary exhaustion of the two heavy slots is HTTP 429 with
  a one-second retry hint.
- Unknown or stale session state should trigger model resend/rebuild from
  `NetworkIR` rather than guessing a model.
- A failed numerical solve makes the result partial and its sample invalid; it
  must not be converted into a successful observation, curve segment, or design
  score.
- Optional Python/LLM failure must not take down the node workspace.
- A local process crash may lose cache state but should not reinterpret a
  committed job record or versioned artifact.
- Cloud service success without the expected result file is failure.

## Known runtime boundaries

- The admission counter, same-hash build locks, bundle locks, cache locks, and
  engine regime locks are process-local. Multiple replicas coordinate only
  through explicit external stores or protocols, not through these locks.
- The two-slot gate is fail-fast and fixed. It has no fairness queue, deadline,
  request cancellation, or adaptive cost scheduling. Because admission occurs
  before bundle locking, a slot can be occupied while waiting for another
  request using the same model.
- A model bundle uses conservative whole-handler serialization. This protects
  mutable lazy engine and bundle caches, but it also prevents two read-mostly
  handlers for the same content hash from running concurrently.
- `read_json` checks 1 MiB after HTTP.jl has produced `req.body`. Nginx is the
  verified transport-level hard cap for the deployed proxy path; a Julia server
  exposed directly can buffer a larger body before the application returns 413.
- Offline/job path construction is unlimited only with respect to the two
  Web-only path counters. Large enumerations can still exhaust worker resources
  and must be partitioned or cancelled operationally.
- Positional hashing above seven free species is bounded but weaker: equivalent
  networks that differ only by names or ordering may no longer share a cache
  entry. A scalable relabel-invariant canonicalizer is not yet verified.
- `valid=true` records solver success under the configured numerical policy. It
  is not an analytic proof, a biological validation, or a guarantee that a
  coarse scan resolved every feature.

See the [backend runtime card](../modules/backend-runtime.md) for the change
gate and [data provenance](data-provenance.md) for artifact authority.

## Verified against

- Current source contract: `1177a3d` (`Bound shared runtime work and numerical
  validity`). Evidence includes the concurrency/budget, input-validation,
  numerical-validity, engine golden, and Nginx deployment contracts.
- Historical baseline: `f9c65a5` remains the pre-P6 architecture evidence used
  to reconstruct the original process and compatibility paths; it is not the
  current concurrency or synchronous-budget contract.
- Boundary: verification is repository source and local contract evidence. It
  does not claim a live multi-replica deployment, full proxy/TLS integration,
  native packaged-app run, or live AWS execution.
