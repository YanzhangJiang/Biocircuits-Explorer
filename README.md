# Biocircuits Explorer

Biocircuits Explorer helps researchers understand how equilibrium
protein-binding networks respond when an input changes, then search for
networks that can produce a desired response. It combines mathematical
analysis, numerical checks, reusable results, and evidence-labelled designs.

The repository contains a browser workspace, a Julia computation service, a
Python Design Agent service, a native macOS shell, batch/HPC tooling, and the
local `BindingAndCatalysis` mathematics engine.

![Biocircuits Explorer workspace](main.png)

## Start here

For a short architectural orientation, read [PROJECT_SUMMARY.md](PROJECT_SUMMARY.md).
The maintained developer knowledge base begins at
[knowledge/README.md](knowledge/README.md). Coding agents should follow
[AGENTS.md](AGENTS.md).

The browser-facing user guide remains in
[webapp/public/wiki.html](webapp/public/wiki.html), with a Chinese version at
[webapp/public/wiki.zh.html](webapp/public/wiki.zh.html). It is product
documentation, not the source of truth for developer contracts.

## Verified toolchain

- Julia 1.12 is exercised by CI and used by the production Docker image.
- Julia 1.10 is declared compatible in `Project.toml`, but is not currently in
  the CI matrix; treat it as declared rather than verified support.
- Node.js 20 and Python 3.13 are exercised by CI.
- Xcode is needed only for the native macOS shell.
- Docker and AWS tooling are optional deployment paths.

## Quick start

Clone the repository and instantiate the Julia environment:

```bash
git clone https://github.com/YanzhangJiang/Biocircuits-Explorer.git
cd Biocircuits-Explorer
julia --project=webapp -e '
  using Pkg
  Pkg.develop(path="Bnc_julia")
  Pkg.instantiate()
'
```

Start the Julia workspace service and the Python Design Agent service:

```bash
cd webapp
./start.sh
```

Open <http://127.0.0.1:8088>. The Julia service uses port `8088` by default;
the Design Agent uses `8765`. It can start without credentials, but
`/design-chat` returns `need_key` until a key is configured; Julia remains usable.

To start only the Julia service or select a different port:

```bash
BIOCIRCUITS_EXPLORER_PORT=8090 \
  julia -t auto --project=webapp webapp/server.jl
```

Liveness, readiness, metrics, and API discovery are available at:

```bash
curl http://127.0.0.1:8088/health
curl http://127.0.0.1:8088/ready
curl http://127.0.0.1:8088/metrics
curl http://127.0.0.1:8088/api/v1
```

`/api/v1/*` is the canonical API. Bare `/api/*` routes are compatibility
aliases and return an `X-API-Deprecation` header; their declared sunset is
2027-05-25. See [knowledge/contracts/api.md](knowledge/contracts/api.md).

## Repository map

```text
Bnc_julia/                 local BindingAndCatalysis math engine
webapp/src/                Julia service, Atlas, IR, jobs, and API routing
webapp/public/             browser workspace and product guide
webapp/scripts/            Python Design Agent, Reader, synthesis, migrations
schemas/                   versioned interchange schemas
frontend-swift/            native macOS shell
packaging/                 relocatable backend-bundle builder
deploy/                    Docker, Nginx, and optional AWS Batch deployment
src/periodic_table/        bounded periodic-table research primitives
scripts/periodic_table/    periodic-table search and reproduction entrypoints
atlas_specs/               checked-in Atlas build specifications
tests/                     repository-level Python contracts
knowledge/                 maintained, evidence-linked developer knowledge
paper_rop_periodic_table/  embedded reproducibility snapshot, not manuscript authority
```

Module ownership, tests, and known gaps are indexed in
[knowledge/catalogs/modules.yaml](knowledge/catalogs/modules.yaml).

## Verification

Run the two Julia suites from the repository root:

```bash
JULIA_NUM_THREADS=auto julia --project=webapp webapp/test/runtests.jl
JULIA_NUM_THREADS=auto julia --project=webapp Bnc_julia/test/runtests.jl
julia --project=webapp webapp/test/test_phenotype_pipeline.jl
```

Run frontend, Agent, and repository-level Python contracts:

```bash
cd webapp
npm ci
npm run lint
npm run test:js
npm run test:py
npm run check-i18n-sync
cd ..
python3 -m unittest discover -s tests -p 'test_*.py' -v
```

The Function-Space Reader regression needs NumPy but does not require the
large Atlas:

```bash
python3 -m pip install numpy
python3 webapp/scripts/reader/test_reader_nofabrication.py
```

CI also regenerates IR schemas and rejects a schema diff. The complete test
matrix and change-specific gates are in [AGENTS.md](AGENTS.md).

## Evidence boundaries

- A retrieved Atlas or Reader candidate is a prior, not a verified design.
  Re-run it through the Julia engine before presenting it as computed evidence.
- Proxy scores, labels, and finite search results are not proofs of
  realizability, robustness, minimality, or impossibility.
- Scientific numbers belong in versioned artifact manifests and claim ledgers,
  not duplicated prose. Conflicting counts remain explicitly unresolved until
  their population semantics and hashes are reconciled.
- The current manuscript and this computation repository have different
  owners. See [knowledge/research/repositories.md](knowledge/research/repositories.md).

## Packaging and deployment

Build a relocatable backend bundle:

```bash
julia --project=packaging packaging/build_backend_app.jl
```

Build the native macOS shell after the bundle exists:

```bash
xcodebuild -project frontend-swift/BiocircuitsExplorerMac.xcodeproj \
  -scheme BiocircuitsExplorerMac -configuration Debug \
  -destination 'platform=macOS' CODE_SIGNING_ALLOWED=NO build
```

Build the source-based application image:

```bash
docker compose -f deploy/docker-compose.yml build julia-app
```

The full Nginx stack needs deployment-specific TLS and domain configuration.
See [knowledge/modules/deployment.md](knowledge/modules/deployment.md) for its
contracts and currently unverified surfaces.

## Version and license

The application version is owned by `VERSION`. Update synchronized metadata
with `scripts/set_version.sh <version>`. Runtime build information is returned
by `/api/v1/version`.
Biocircuits Explorer is released under the [MIT License](LICENSE).
