# Packaging and deployment

`module_id: deployment`

## Purpose

Define how a release becomes a runnable backend image or macOS bundle, and say
exactly how far the evidence reaches. The checked-in Docker workflow is
configured to start one backend image and test that it is ready, alive,
versioned, serving its browser entry page, and able to write job state. The
full Compose/Nginx/TLS rollout is defined in source but has not been run end to
end.

Here, rollback means restoring the previous local image and the configuration
needed to start it. It is not a backup of operator environment files, TLS
material, job data, or cloud resources.

## Non-goals

- It does not define numerical semantics, atlas identity, or the asynchronous
  job state machine.
- A passing single-image check is not evidence that DNS, TLS, Nginx, cloud
  identity, a registry, or a production host works.
- Host and AWS setup scripts are operational helpers, not a declarative or
  fully reversible infrastructure authority.

## Owner paths

- Image and local stack: `deploy/Dockerfile`, `deploy/docker-compose.yml`,
  `deploy/nginx.conf`
- Image identity and build: `deploy/build_image.sh`,
  `deploy/image_reference.sh`, `VERSION`, `scripts/set_version.sh`
- Host rollout and rollback: `deploy/deploy.sh`,
  `deploy/rewrite_rollback_config.py`
- AWS setup and state validation: `deploy/setup_aws_batch.sh`,
  `deploy/validate_aws_batch_state.py`, `deploy/AWS_BATCH.md`,
  `deploy/aws-runtime.env.example`, `deploy/aws_setup_permissions_policy.json`
- macOS backend bundles: `packaging/`, `scripts/build_macos_dmg.sh`
- Runtime probes, configuration, and build identity: `webapp/server.jl`,
  `webapp/src/config.jl`, `webapp/src/version.jl`,
  `webapp/src/BiocircuitsExplorerBackend.jl`, `webapp/src/routing.jl`
- Docker gate: `.github/workflows/docker.yml`

## Inputs

- The tracked source tree, Julia lock files, local `Bnc_julia`, and `VERSION`.
- Version, revision, and build time for image labels and runtime discovery.
- Runtime settings for ports, assets, job storage, AWS, Cognito, quotas, and an
  optional release image reference.
- For host rollout, readable TLS certificate/key files and any cloud
  credentials supplied outside Git.
- For a native bundle, the packaging Julia environment and either the portable
  or PackageCompiler build path.

## Outputs

- A non-root OCI image with release metadata, static assets, and a writable
  job-store mount point.
- A Compose definition for job-store initialization, the Julia app, and an
  HTTPS Nginx proxy.
- `/health` for cheap liveness, `/ready` for traffic admission,
  `/api/v1/version` for application/API identity, and the static web surface.
- A relocatable backend bundle for the macOS application.
- An optional generated AWS runtime environment; it is ignored by Git and is
  operator state, not source configuration.

## Contract sources

- Image contents, non-root user, command, port, and liveness check:
  `deploy/Dockerfile`
- Service ordering, job-store ownership, readiness, and proxy health:
  `deploy/docker-compose.yml` and `deploy/nginx.conf`
- Preflight, immutable-reference checks, rollout completion, and rollback:
  `deploy/deploy.sh`, `deploy/image_reference.sh`, and
  `deploy/rewrite_rollback_config.py`
- Image tags and OCI metadata: `deploy/build_image.sh`, `VERSION`, and
  `scripts/set_version.sh`
- Backend readiness and version payloads:
  `webapp/src/BiocircuitsExplorerBackend.jl` and `webapp/src/version.jl`
- Bundle contents and entrypoints: `packaging/build_backend_app.jl`,
  `packaging/design-runtime-files.txt`, and `scripts/build_macos_dmg.sh`

## Tests

- `webapp/test/runtests.jl` covers liveness, fail-closed readiness, version
  discovery, API identity, and writable job-store behavior.
- `tests/test_deployment_contract.py` checks image/Compose probe separation,
  non-root ownership, release identity, TLS preflight, rendered rollback,
  legacy-static preservation, shell parsing, and Compose parsing when the
  plugin is available.
- `tests/test_build_image.py` checks clean-tree publication, semantic versions,
  OCI revision metadata, tag safety, and ECR immutability setup.
- `tests/test_rewrite_rollback_config.py`, `tests/test_setup_aws_batch.py`, and
  `tests/test_validate_aws_batch_state.py` cover rollback path rewriting and
  fail-closed AWS setup/state validation with mocked commands and fixtures.
- These tests do not start the complete proxy/TLS stack or contact AWS.

## CI

`.github/workflows/docker.yml` is configured to build and load one Linux image,
publish it only to the runner's local Docker daemon, and then:

- waits for `/ready`;
- checks `/health` and `/api/v1/version`;
- fetches `/index.html`; and
- writes a probe file in the container job store.

`.github/workflows/ci.yml` runs the backend and Python deployment contracts.
Neither workflow starts the complete Compose stack, mounts real certificates,
tests Nginx over production TLS, publishes to a registry, performs a host
rollout, or provisions live AWS. CI also does not launch a built macOS backend
bundle.

## Invariants

- The image process runs as `rop`, not root, and its job-store path is writable
  by that user. Compose performs a marker-guarded ownership migration for an
  existing named volume before admitting the app.
- `/health` answers whether the process is alive. `/ready` answers whether the
  module, required static entrypoints, and writable job store can serve real
  traffic. Image health uses the former; Compose traffic admission uses the
  latter.
- The Nginx service is healthy only after its configuration parses and its
  HTTPS `/health` proxy succeeds. `deploy.sh` reports completion only after
  Compose has waited for both backend readiness and proxy health.
- Host mutation starts only after the requested environment file, server names,
  release image reference, and TLS files have passed preflight. TLS must cover
  every configured host, have more than 24 hours remaining, and match its key.
- A deployment image is a digest or a version-plus-commit tag; `latest` is not
  accepted. A tag is overwrite-resistant only when the registry enforces
  immutability, so a digest remains the stronger remote identity.
- Before an upgrade, rollback preserves the running image, a fully rendered
  Compose file, and the referenced Nginx template. For a pre-migration stack it
  also snapshots the legacy host-mounted static files and rewrites the rendered
  file to use that snapshot.
- Rollback deliberately excludes external environment files, certificates,
  persistent job data, databases, object storage, and AWS resources.
- Credentials, generated runtime environments, job stores, and private TLS
  keys remain outside version control.

## Known gaps

- P2 — The complete Compose/Nginx/TLS path has not been run in CI or in this
  audit; source checks and `docker compose config` are not an integration test.
- P2 — `julia:1.12` and `nginx:alpine` are mutable base references, and the
  image installs `awscli` without a pinned Python package version.
- P2 — No checked-in lane publishes to a registry, signs an image, emits an
  SBOM, verifies provenance, or performs a production rollout.
- P2 — Rollback cannot restore external environment/TLS changes, job data, or
  cloud-side mutations; those need separate backup and infrastructure plans.
- P2 — AWS/Cognito setup is tested with mocks and source contracts only; no live
  account, Batch worker, S3 transfer, quota table, or identity flow is verified.
- P2 — Bundle resource lookup and staging have contracts, but CI does not build,
  relocate, launch, and probe the complete macOS backend bundle.

## Change protocol

1. Keep `VERSION`, image labels, runtime build metadata, and image tags in sync;
   use `scripts/set_version.sh` for an intentional release change.
2. Change probes only with backend route tests, Docker/Compose/Nginx alignment,
   and the single-image runtime gate; run the full stack before making a TLS or
   production claim.
3. Change rollback only after listing both the state it restores and the state
   it intentionally leaves external, then add a failure-path test.
4. Add an environment variable to runtime parsing, examples, and Compose
   together without committing a real secret.
5. For packaging changes, build into a clean location, copy into a throwaway app
   layout, launch it, and probe version/readiness before claiming relocation.

## Verified against

- Source commit: `f2ca13c`
- Evidence inspected: Docker/Compose/Nginx definitions, image and host helpers,
  rollback/AWS validators, backend probes, package resources, focused tests,
  and both CI workflows.
- Boundary: the checked-in single-image gate defines readiness, liveness,
  version, static delivery, and a write probe. No external workflow run,
  complete Compose/TLS stack, registry publication, host rollout, signed
  artifact, or live AWS system is claimed verified.
