---
module: backend-runtime
status: verified
verified_against: f9c65a5
---

# Backend runtime

## Purpose

Expose the engine, atlas, designability, import/export, and job workflows as a
versioned HTTP service; serve the web assets; manage bounded process-local
caches; and persist asynchronous job state safely across local or cloud
execution.

## Non-goals

- Owning mathematical ROP semantics; those belong to `Bnc_julia`.
- Treating sessions or in-memory job dictionaries as durable scientific data.
- Making optional Python/LLM output authoritative.
- Guaranteeing that every historical endpoint already returns the same artifact
  envelope shape.

## Owner paths

- Module assembly and handlers:
  [`webapp/src/BiocircuitsExplorerBackend.jl`](../../webapp/src/BiocircuitsExplorerBackend.jl)
- Routing and API versions: [`webapp/src/routing.jl`](../../webapp/src/routing.jl)
- Runtime configuration: [`webapp/src/config.jl`](../../webapp/src/config.jl)
- Jobs and cloud boundary: [`webapp/src/jobs.jl`](../../webapp/src/jobs.jl)
- Authentication: [`webapp/src/auth.jl`](../../webapp/src/auth.jl)
- Sessions and model cache: [`webapp/src/session_store.jl`](../../webapp/src/session_store.jl), [`webapp/src/model_cache.jl`](../../webapp/src/model_cache.jl)
- Serialization, observability, static files: [`webapp/src/serialization.jl`](../../webapp/src/serialization.jl), [`webapp/src/observability.jl`](../../webapp/src/observability.jl), [`webapp/src/static_assets.jl`](../../webapp/src/static_assets.jl)
- Entrypoints: [`webapp/server.jl`](../../webapp/server.jl), [`webapp/scripts/run_batch_job.jl`](../../webapp/scripts/run_batch_job.jl)

## Inputs

- HTTP requests on root health routes, `/api/v1/...`, or compatibility
  `/api/...` routes.
- Versioned IR/designability payloads, supported legacy network requests, SBML,
  and atlas/job specifications.
- Environment configuration for ports, assets, job storage, AWS, Cognito, and
  optional quota enforcement.
- Local files or S3 URIs for batch input/status/result artifacts.

## Outputs

- JSON responses and structured request errors.
- Static web assets, health/readiness payloads, and Prometheus text metrics.
- Process-local sessions/model caches.
- Atomic local job records and results, or S3-backed Batch artifacts.
- Version/build information and optional result-artifact metadata.

## Contract sources

- Route table, method rules, v1 mapping, deprecation metadata, and probes:
  [`routing.jl`](../../webapp/src/routing.jl)
- JSON safety and error classification:
  [`serialization.jl`](../../webapp/src/serialization.jl)
- Runtime/build version: [`version.jl`](../../webapp/src/version.jl), [`VERSION`](../../VERSION)
- IR and SBML boundaries: [`ir.jl`](../../webapp/src/ir.jl), [`sbml.jl`](../../webapp/src/sbml.jl)
- Result provenance: [`result_artifact.jl`](../../webapp/src/result_artifact.jl),
  [`result-artifact.schema.json`](../../schemas/result-artifact.schema.json)
- Job lifecycle and ownership: [`jobs.jl`](../../webapp/src/jobs.jl)

## Tests

- [`webapp/test/runtests.jl`](../../webapp/test/runtests.jl): routing, probes,
  metrics, request IDs, auth/quota, jobs, SBML/IR, handlers, and serialization.
- [`webapp/test/jobs_cancellation_contract.jl`](../../webapp/test/jobs_cancellation_contract.jl):
  local and AWS cancellation/finish/submit/dispatch races and durable state.
- [`webapp/test/cooperative_cancel_checkpoints_contract.jl`](../../webapp/test/cooperative_cancel_checkpoints_contract.jl):
  cancellation propagation through long workflows.
- [`webapp/test/*.test.mjs`](../../webapp/test/): frontend request and command
  contracts.
- [`webapp/scripts/test_chat_api.py`](../../webapp/scripts/test_chat_api.py) and
  [`test_design_agent_contract.py`](../../webapp/scripts/test_design_agent_contract.py):
  optional Python sibling boundary.

## CI

[`.github/workflows/ci.yml`](../../.github/workflows/ci.yml) runs Julia backend
tests, JavaScript lint/unit tests, Python agent contracts, schema drift checks,
artifact checks, and Reader no-fabrication tests. The Docker workflow builds the
image from [`deploy/Dockerfile`](../../deploy/Dockerfile); build success is not
the same as a deployed runtime smoke test.

## Invariants

- `/api/v1` is canonical; known bare `/api` aliases reach the same handler and
  carry deprecation metadata. Root probes and static assets are not legacy API
  calls.
- Request IDs are bounded and sanitized before logging; metrics use
  low-cardinality route labels.
- `NetworkIR` content identity, not a session identifier, is the stable model
  cache key. A missing cache entry can be rebuilt from the supplied IR.
- Long computation and external AWS/S3 calls never hold `JOBS_LOCK`.
- Job transitions follow the declared state graph; terminal snapshots cannot be
  mutated by late work.
- Local cancellation is cooperative. AWS cancellation intent is published
  before remote dispatch and remains monotonic through races.
- Concurrent AWS cancel requests share a persisted dispatch claim: at most one
  issues the remote cancel/terminate call, failure releases the claim for retry,
  and a stale claim expires after a bounded interval.
- `record.json` is the canonical local process-restart state; public status is a
  best-effort projection written after the record commit.
- Cross-user job lookup does not disclose another user's job existence.
- A remotely succeeded process without the expected result artifact is failed.

## Known gaps

- The browser and Python engine client still construct compatibility `/api`
  URLs, so the v1 migration is incomplete.
- The CI Julia matrix does not yet prove every release admitted by project
  compatibility.
- Docker CI builds the image but does not exercise a full broker/worker/S3 or
  native-shell end-to-end deployment.
- Result-envelope adoption is additive and not uniform across every synchronous
  historical handler.
- Local record publication fsyncs the temporary file and renames it, but does not
  fsync the parent directory; sudden-power-loss durability is not established.
  A failed `status.json` projection write is not automatically repaired.

## Change protocol

1. Classify the change as API, process lifecycle, cache, persistence, auth, or
   observability; list every client and deployment entrypoint it affects.
2. Preserve v1/legacy response parity until all compatibility clients migrate.
3. Add race tests before changing the job state machine or lock boundaries;
   never put network or cloud I/O under `JOBS_LOCK`.
4. Run Julia, JavaScript, Python, schema drift, and artifact checks appropriate
   to the changed boundary; build and smoke the container for deployment work.
5. Bump the public API, artifact, or application version when semantics—not
   merely implementation—change.

See [runtime topology](../architecture/runtime.md) and
[data provenance](../architecture/data-provenance.md).

## Verified against

- Source commit: `f9c65a5`
- Evidence inspected: routing, configuration, jobs, authentication,
  persistence, serialization, focused race contracts, and CI workflow wiring.
- Boundary: source and local contract verification only; no live cloud,
  container-stack, or native-host integration is promoted to verified.
