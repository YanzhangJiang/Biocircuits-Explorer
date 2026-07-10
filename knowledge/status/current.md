# Current verified snapshot

- Snapshot date: 2026-07-10
- Component-split revision inspected: `603635d`
- Current implementation revision inspected: `1177a3d`
- Historical knowledge baseline retained: `f9c65a5`
- Current-tree contract inventory:
  `python3 scripts/verify_repository.py --check`
- Configured versions:
  [generated from their owners](../generated/reference.md#versions-and-configured-toolchains)
- Scope: repository runtime and local verification evidence, not manuscript
  claims, a remote CI result, or proof that an external deployment succeeded

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

The evidence below is local repository evidence. It does not establish that a
remote workflow ran, an image was published, AWS or Slurm accepted a job, or a
signed application was installed.

## Evidence recorded for this snapshot

| Check | Local result at `1177a3d` | Boundary |
|---|---|---|
| Full Julia web suite | Completed with no failing tests; one pre-existing `@test_broken` remains explicitly recorded | Local process only; not evidence of a remote CI run or production traffic |
| `BindingAndCatalysis` suite | 126/126 tests passed | Numerical regression coverage is conditional on the tested models and solver policies |
| Browser JavaScript suite | `npm run test:js` passed, including scan-validity, SQLite-policy, and Design Screen rendering contracts | No full browser-to-live-provider Design Agent conversation was exercised |
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
| `1177a3d` | Bounded shared runtime work, serialized mutable state, tightened request validation, preserved numerical failure information, and promoted Design Screen to v0.3; this is the current inspected revision |

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
  [generated reference](../generated/reference.md#api-routes).
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

1. No remote CI run is claimed for `1177a3d`; the evidence table records local
   commands only.
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
cd webapp && npm run test:js
cd ..
julia --project=webapp webapp/scripts/gen_schemas.jl --check
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
