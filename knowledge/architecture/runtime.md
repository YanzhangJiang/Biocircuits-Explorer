---
title: Runtime and process topology
status: verified
verified_against: f9c65a5
---

# Runtime and process topology

The normal interactive path is one web client talking to one Julia HTTP
process. Heavy work may run in a Julia task or an AWS Batch worker, and the
conversational design surface may add a Python sibling process. These paths
share contracts, but they do not share all in-memory state.

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
   enforces methods, adds request IDs/CORS, and maps errors to HTTP responses.
3. A handler in
   [`BiocircuitsExplorerBackend.jl`](../../webapp/src/BiocircuitsExplorerBackend.jl)
   reads and validates the body, resolves or builds a model, then invokes an
   atlas, designability, or engine function.
4. [`reaction_parser.jl`](../../webapp/src/reaction_parser.jl) and
   [`ir.jl`](../../webapp/src/ir.jl) convert accepted model inputs to the
   `Bnc` representation. [`model_cache.jl`](../../webapp/src/model_cache.jl)
   reuses compiled models by content identity; [`session_store.jl`](../../webapp/src/session_store.jl)
   is a convenience lookup, not the identity source.
5. The handler returns JSON-safe data. Request metrics and optional structured
   logs are recorded after dispatch.

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
| Session lookup, model objects, debug buffers | Julia process | cache only; rebuild from versioned input |
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
- Unknown or stale session state should trigger model resend/rebuild from
  `NetworkIR` rather than guessing a model.
- Optional Python/LLM failure must not take down the node workspace.
- A local process crash may lose cache state but should not reinterpret a
  committed job record or versioned artifact.
- Cloud service success without the expected result file is failure.

See the [backend runtime card](../modules/backend-runtime.md) for the change
gate and [data provenance](data-provenance.md) for artifact authority.
