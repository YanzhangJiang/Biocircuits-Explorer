# Packaging and deployment

`module_id: deployment`

## Purpose

Build a relocatable Julia backend bundle or a versioned container, optionally
place the container behind Nginx, and expose liveness, readiness, version,
metrics, authentication bootstrap, and job-broker routes.

## Non-goals

- It does not define the numerical engine, atlas semantics, or asynchronous job
  state machine.
- A container image build is not evidence that a production deployment, TLS
  configuration, cloud identity, or registry publication works.
- Host provisioning scripts are operational helpers, not a declarative or
  reversible infrastructure authority.

## Owner paths

- Image and local stack: `deploy/Dockerfile`, `deploy/docker-compose.yml`,
  `deploy/nginx.conf`
- Relocatable backend bundle: `packaging/`, especially
  `packaging/build_backend_app.jl`
- Build and host helpers: `deploy/build_image.sh`, `deploy/deploy.sh`
- Optional AWS resource bootstrap: `deploy/setup_aws_batch.sh`,
  `deploy/aws-runtime.env.example`, `deploy/aws_setup_permissions_policy.json`
- Backend entry/config/version/routes: `webapp/server.jl`,
  `webapp/src/config.jl`, `webapp/src/version.jl`,
  `webapp/src/routing.jl`
- Release version: `VERSION`, `scripts/set_version.sh`
- Image CI: `.github/workflows/docker.yml`

## Inputs

- The tracked source tree, Julia manifests, local `Bnc_julia`, and `VERSION`.
- For the native bundle, the packaging Julia environment and PackageCompiler.
- Build metadata for version, revision, and creation time.
- Runtime environment for ports, public assets, AWS, Cognito, quotas, job store,
  and optional image selection.
- TLS material and cloud credentials supplied outside Git.

## Outputs

- A non-root OCI image with build metadata labels and a Julia server command.
- A relocatable compiled backend bundle for embedding in the macOS application.
- A Compose stack with the Julia app, persistent job-store volume, and Nginx.
- HTTP `/health`, `/ready`, `/metrics`, and version-discovery responses plus the
  canonical versioned API surface.
- Optional generated runtime environment for cloud resources; it is ignored by
  Git and must not be treated as source configuration.

## Contract sources

- Image contents, user, command, port, and health check: `deploy/Dockerfile`
- Compiled bundle contents and entrypoint:
  `packaging/build_backend_app.jl` and `packaging/Project.toml`
- Runtime environment parsing: `webapp/src/config.jl`
- Liveness/readiness/build metadata and API protocol identity:
  `webapp/src/BiocircuitsExplorerBackend.jl`, `webapp/src/version.jl`, and
  `webapp/src/routing.jl`
- Proxy and service wiring: `deploy/docker-compose.yml` and
  `deploy/nginx.conf`
- Version/tag derivation: `deploy/build_image.sh` and `VERSION`

## Tests

`webapp/test/runtests.jl` covers liveness, readiness, metrics labels, request
IDs, version discovery, canonical API aliases, legacy deprecation behavior, and
method rejection. The Docker workflow checks whether the image can be built.

## CI

`.github/workflows/ci.yml` runs the backend route contracts.
`.github/workflows/docker.yml` performs a BuildKit image build on relevant pull
requests and branch/tag events. It deliberately does not load or run the image,
probe its endpoints, publish it, start Compose, configure Nginx/TLS, or execute
cloud provisioning. No checked-in workflow builds or launches the relocatable
PackageCompiler bundle.

## Invariants

- The runtime user is non-root and the checked-in local engine package is the
  package source used during image construction.
- `/health` is cheap liveness; `/ready` fails closed when required runtime
  checks fail. They are not interchangeable.
- Application version and build revision are exposed independently from the API
  protocol version.
- Canonical callers use the versioned API; legacy bare `/api/` routes are only a
  migration alias and advertise deprecation.
- Credentials, generated runtime environments, job stores, and TLS keys remain
  outside version control.

## Known gaps

- CI does not execute a built image, so startup, container health, static asset
  delivery, writable volumes, and graceful shutdown remain unproven there.
- Dockerfile and Compose currently probe different endpoints; only source-level
  route tests prove the distinction between liveness and readiness.
- Nginx domain and certificate assumptions require deployment-specific
  configuration and have no syntax or integration gate.
- Registry login/push and production rollout are not part of the workflow.
- Bundle relocation, resource completeness, startup, and compatibility with the
  Swift embedding step have no automated gate.
- Some clients still call legacy bare API paths, so the deprecation alias cannot
  be removed until client migration is verified.

## Change protocol

1. Keep `VERSION`, image labels, server build metadata, and tag generation in
   sync; use `scripts/set_version.sh` for intentional release changes.
2. Add or change an environment variable in `config.jl`, the example file, and
   Compose together without committing a real secret value.
3. Change probe semantics with route tests, Dockerfile/Compose/Nginx alignment,
   and an image runtime smoke test.
4. Migrate all clients and retain compatibility tests before removing a legacy
   API alias.
5. For packaging changes, build the bundle in a clean location, copy it into a
   throwaway app layout, launch it, and probe version/readiness before claiming
   relocation support.

## Verified against

- Source commit: `f9c65a5`
- Evidence inspected: image/Compose/Nginx definitions, build and deploy helpers,
  backend route/version contracts, tests, ignore policy, and CI workflows.
- Boundary: image build wiring is verified; no running container, registry,
  host, TLS, Cognito, or AWS stack is claimed verified.
