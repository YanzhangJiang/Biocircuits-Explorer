---
module: backend-runtime
status: verified
verified_against: 01a01be
---

# Backend runtime

## Purpose

Expose the engine, atlas, designability, import/export, and job workflows as a
versioned HTTP service while keeping local and deployed trust boundaries
distinct. A process being alive is not enough to receive traffic: readiness
means the module initialized, the static directory and both browser/native entry
pages exist, and the job store is writable.

The bind rule follows the launcher. A supervised native process defaults to
loopback; an unsupervised server defaults to all interfaces for container/host
deployment. An explicit validated host override wins in either mode.

## Non-goals

- Owning mathematical ROP semantics; those belong to `Bnc_julia`.
- Treating sessions or in-memory job dictionaries as durable scientific data.
- Making optional Python/LLM output authoritative.
- Providing TLS or network authentication for every deployment mode; those are
  explicit proxy/auth configuration responsibilities.
- Guaranteeing that every historical endpoint already returns one uniform
  result envelope.

## Owner paths

- Module assembly, bind policy, probes, and handlers:
  [`webapp/src/BiocircuitsExplorerBackend.jl`](../../webapp/src/BiocircuitsExplorerBackend.jl)
- Routing and server start: [`webapp/src/routing.jl`](../../webapp/src/routing.jl),
  [`webapp/server.jl`](../../webapp/server.jl),
  [`webapp/start.sh`](../../webapp/start.sh)
- Runtime configuration: [`webapp/src/config.jl`](../../webapp/src/config.jl)
- Static and local-image boundary:
  [`webapp/src/static_assets.jl`](../../webapp/src/static_assets.jl)
- Jobs and AWS request boundary: [`webapp/src/jobs.jl`](../../webapp/src/jobs.jl)
- Authentication: [`webapp/src/auth.jl`](../../webapp/src/auth.jl)
- Sessions and model cache:
  [`webapp/src/session_store.jl`](../../webapp/src/session_store.jl),
  [`webapp/src/model_cache.jl`](../../webapp/src/model_cache.jl)
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
  job storage, local-image exposure, AWS, Cognito, and quotas.
- Local files or S3 URIs for batch input/status/result artifacts.
- Application identity from an explicit environment value or a `VERSION` file
  in the installed resource bundle/source tree.

## Outputs

- JSON responses and structured request errors.
- Static web assets, liveness/readiness payloads, Prometheus metrics, and
  application/API version discovery.
- Process-local sessions and model caches.
- Atomic local job records/results or S3-backed Batch artifacts.
- Supported local image bytes only when bind/origin/opt-in rules permit them.

## Contract sources

- Bind selection, parent watchdog, and readiness checks:
  [`BiocircuitsExplorerBackend.jl`](../../webapp/src/BiocircuitsExplorerBackend.jl)
  and [`routing.jl`](../../webapp/src/routing.jl)
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
  [`result-artifact.schema.json`](../../schemas/result-artifact.schema.json)

## Tests

- [`webapp/test/runtests.jl`](../../webapp/test/runtests.jl) covers bind defaults
  and override validation; liveness/readiness checks; same-origin, wrong-port,
  DNS-rebinding, loopback, public-bind, and explicit local-image cases; AWS
  request override opt-in; routing, auth/quota, jobs, SBML/IR, handlers, and
  serialization.
- [`webapp/test/jobs_cancellation_contract.jl`](../../webapp/test/jobs_cancellation_contract.jl)
  covers local/AWS cancellation, finish, submit, and dispatch races.
- [`webapp/test/cooperative_cancel_checkpoints_contract.jl`](../../webapp/test/cooperative_cancel_checkpoints_contract.jl)
  covers cancellation propagation through long workflows.
- [`tests/version_resource_contract.jl`](../../tests/version_resource_contract.jl)
  constructs an installed bundle layout and proves that runtime version lookup
  finds its copied `VERSION` resource before unavailable source paths.
- [`tests/test_deployment_contract.py`](../../tests/test_deployment_contract.py)
  checks local-start loopback propagation and the single-image runtime gate.
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
- Long computation and external AWS/S3 calls never hold `JOBS_LOCK`; terminal
  job state is monotonic and cross-user lookup does not disclose existence.
- `record.json` is canonical restart state. A remote success without the
  expected result artifact is failed rather than promoted to success.

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
- P2 — Browser and Python engine clients still construct compatibility `/api`
  URLs, so v1 migration remains incomplete.
- P2 — Result-envelope adoption remains additive rather than uniform across all
  synchronous historical handlers.
- P2 — Local record publication fsyncs and renames the file but not its parent
  directory; sudden-power-loss durability and automatic repair of a failed
  `status.json` projection are not established.

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
6. Copy and test `VERSION` in every new bundle layout; bump public API, artifact,
   or application versions when semantics, not merely implementation, change.

See [runtime topology](../architecture/runtime.md) and
[data provenance](../architecture/data-provenance.md).

## Verified against

- Source commit: `01a01be`
- Evidence inspected: bind/configuration, readiness, local-image policy, AWS
  opt-in, version/resource lookup, routing, jobs, persistence, focused tests,
  and both CI workflows.
- Boundary: source and local contract evidence plus the configured
  single-image runtime gate are recorded. No external workflow run, complete
  proxy/TLS stack, native packaged app, or live cloud integration is claimed
  verified.
