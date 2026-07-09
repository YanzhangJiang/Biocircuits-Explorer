# Current verified snapshot

- Snapshot date: 2026-07-10
- P5 implementation revision inspected: `01a01be`
- Historical knowledge baseline retained: `f9c65a5`
- Current-tree contract inventory:
  `python3 scripts/verify_repository.py --check`
- Configured versions:
  [generated from their owners](../generated/reference.md#versions-and-configured-toolchains)
- Scope: repository runtime and evidence routing, not manuscript claims or a
  claim that an external deployment succeeded

## Result in plain language

The current revision makes local startup, native helper access, release
identity, and deployment scripts reject more ambiguous states. The ordinary
developer start stays on loopback. The macOS app gives its Design Chat helper a
new private token on each launch. Release and deployment helpers check their
inputs before acting.

These outcomes are backed by source contracts, local tests, and configured CI
jobs. They do not prove that an image was published, a cloud or cluster job
ran, a DMG was notarized, or the complete public TLS stack worked.

## Fifth-phase revision line

| Revision | Result added |
|---|---|
| `d752e15` | Secured loopback startup, Design Chat origin/token handling, native helper launch, and runtime host boundaries |
| `127a2a5` | Added a portable Julia 1.10 lock for the headless HPC environment and removed personal checkout paths |
| `cec0b59` | Synchronized release identity, packaged Design Agent resources, and the split web/HPC CI matrix |
| `01a01be` | Added fail-closed container, image, Compose, TLS, AWS setup, and rollback contracts; this is the current inspected revision |

## What is verified, and how far

| Surface | Current evidence | Boundary |
|---|---|---|
| Main web application | `.github/workflows/ci.yml` configures the Julia service, mathematics engine, and phenotyper suites on Julia 1.12 | Julia 1.10 is not in the main web application matrix |
| Headless HPC environment | CI configures Julia 1.10 and 1.12, selects the matching manifest, instantiates it, and loads `BindingAndCatalysis` | No live Slurm scheduler is contacted |
| Browser and Python layers | CI configures Node.js 20 and Python 3.13; `npm run test:js` and `npm run test:py` include the Design Chat browser and server contracts | No live model provider or full browser-to-engine conversation is exercised |
| Design Chat boundary | Python, JavaScript, and Swift tests cover exact-origin checks, bearer propagation, loopback binding, environment filtering, and fail-closed configuration | `webapp/start.sh` intentionally retains one unauthenticated loopback-only development mode |
| Application container | The Docker workflow is configured to build and start one image, wait for readiness, check health, version and the browser page, and write to the job store | No external workflow run is claimed; it does not start Compose, Nginx, or TLS and it does not publish the image |
| macOS host | The targeted `BiocircuitsExplorerMacTests` bundle passed locally, 7/7, at `01a01be` | No workflow invokes Xcode; signing, notarization, DMG installation, and Gatekeeper are unverified |
| Release and deployment scripts | Repository tests cover version synchronization, package resources, image labels/tags, dirty-tree refusal, TLS input checks, AWS setup state checks, and rollback rewriting | Static and mocked tests do not prove external services accepted or executed the result |

The workflow configuration is executable ownership of the matrix, not evidence
that a particular remote run passed. Record the URL and conclusion of a remote
run separately when that evidence exists.

## Design Chat security boundary

- The Python helper accepts only a literal loopback bind host.
- Every browser request must match one exact configured loopback origin.
- Outside the explicit development mode, launches require a bearer token
  containing at least 32 characters. A token-authenticated native health probe
  may omit `Origin`; browser traffic may not.
- The macOS shell generates a new 256-bit token for every helper launch,
  supplies it to Python and the embedded web view, and keeps it in process
  memory.
- `webapp/start.sh` is the sole checked-in exception: it explicitly enables
  unauthenticated local development while keeping the helper on loopback and
  retaining the exact-origin check.
- A model provider API key is separate from the local bearer token. Missing
  provider credentials produce `need_key`; they do not disable the Julia
  workspace.

The owner sources are `webapp/scripts/chat_api.py`, `webapp/start.sh`,
`webapp/public/js/agent-view.js`, and the Swift backend and web-shell
controllers. The focused checks are `webapp/scripts/test_chat_api.py`,
`webapp/test/design-chat-auth.test.mjs`, and
`frontend-swift/BiocircuitsExplorerMacTests/`.

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

Those baseline claims remain historical evidence. The fifth-phase revisions
above extend them; they do not rewrite the catalog's baseline identifier.

## Interface and scientific state

- `/api/v1/*` is canonical. Bare `/api/*` routes are compatibility aliases and
  return a deprecation header until the executable sunset recorded in the
  [generated route reference](../generated/reference.md#api-routes).
- Liveness, readiness, and Prometheus endpoints are `/health`, `/ready`, and
  `/metrics`.
- Schema identities and instance coverage are cataloged in
  [the contract catalog](../catalogs/contracts.yaml). A schema file alone does
  not prove conformance.
- Atlas and Reader output is a prior in the Design Agent. A candidate requires
  fresh engine evidence before it can be shown as verified; see
  [the scientific evidence boundary](../contracts/scientific-evidence.md).
- The standalone paper repository owns current manuscript wording, figures,
  and release claims. The similarly named directory in this repository is an
  embedded reproducibility snapshot; see
  [repository roles](../research/repositories.md).

## Known unknowns and unverified surfaces

1. No current evidence establishes publishing to or pulling from a live image
   registry.
2. Image signing, signature verification, and an SBOM generation and release
   lane are not established.
3. No live AWS rollout was verified. ECR, Batch, Cognito, S3, IAM, the quota
   store, instance roles, deployment, and rollback remain external unknowns.
4. No real Slurm submission, scheduling, cancellation, completion, or artifact
   return was verified.
5. The macOS unit bundle passed locally, but a signed, notarized, stapled,
   installed, and Gatekeeper-tested DMG remains unverified.
6. The complete Compose stack with Nginx, a real domain, TLS certificates,
   renewal, readiness admission, and rollback was not run end to end.
7. Runtime Atlas datasets are optional and not tracked in this checkout. A
   missing dataset manifest means the generic artifact validator cannot prove
   that dataset is reproducibility-pinned.
8. Several hand-authored JSON Schemas still lack instance-level CI coverage.
9. Embedded and paper-side periodic-table observations still have unresolved
   population meanings and producer lineage. Do not reconcile them by
   arithmetic.
10. No CI job exercises a live model provider, a full Design Agent conversation
    with the Julia engine, or release-locked retrieval corpora.

## Verification entry points

Run commands from the repository root:

```bash
JULIA_NUM_THREADS=auto julia --project=webapp webapp/test/runtests.jl
JULIA_NUM_THREADS=auto julia --project=webapp Bnc_julia/test/runtests.jl
julia --project=webapp webapp/test/test_phenotype_pipeline.jl
julia tests/version_resource_contract.jl
julia --project=packaging -e 'using Pkg; Pkg.instantiate()'
julia --project=packaging packaging/test_design_runtime.jl
cd webapp && npm ci && npm run lint && npm run test:js && npm run test:py && npm run check-i18n-sync
cd ..
python3 -m unittest discover -s tests -p 'test_*.py' -v
python3 -m pip install -r scripts/requirements-verify.txt
python3 scripts/verify_repository.py --check
python3 -m pip install numpy
python3 webapp/scripts/reader/test_reader_nofabrication.py
```

Run the headless load check once with Julia 1.10 and once with Julia 1.12, and
run the two local Xcode commands for macOS work. The exact commands are in
[the agent verification matrix](../../AGENTS.md#verification-matrix).

The standard CI owner is
[`.github/workflows/ci.yml`](../../.github/workflows/ci.yml). The
single-image runtime owner is
[`.github/workflows/docker.yml`](../../.github/workflows/docker.yml). A
successful narrow command does not prove an unrelated phase complete.

## Current knowledge gate

The generated reference and unified drift checker cover routes, schema
identities, version owners, configured toolchains, links, catalog paths,
public-safety rules, and declared artifact fixtures. The check snapshots the
Git-visible tree and fails on validator side effects. Keep unresolved paper
release claims and external deployment claims explicitly unknown until their
recorded evidence exists.
