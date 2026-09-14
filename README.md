# Biocircuits Explorer

Biocircuits Explorer helps a researcher see how a protein-binding network
responds when an input changes, and search for networks that could produce a
desired response. It runs entirely on one machine: a Julia mathematics engine,
a Julia HTTP service, a browser node workspace, and an optional Python Design
Agent that calls the live engine. A retrieved or suggested candidate is always
shown separately from a result the engine has recomputed.

![Biocircuits Explorer workspace](webapp/public/media/main.png)

## Quick start

```bash
git clone https://github.com/YanzhangJiang/Biocircuits-Explorer.git
cd Biocircuits-Explorer
julia --project=webapp -e 'using Pkg; Pkg.develop(path="Bnc_julia"); Pkg.instantiate()'
cd webapp && ./start.sh
```

Open <http://127.0.0.1:8088>. The workspace listens on `127.0.0.1:8088` and
Design Chat on `127.0.0.1:8765`. Design Chat works without a model-provider key
(requests return `need_key`); the workspace does not need it at all.

Useful endpoints:

```bash
curl http://127.0.0.1:8088/health
curl http://127.0.0.1:8088/ready
curl http://127.0.0.1:8088/api/v1/version
```

All routes live under `/api/v1/*`. The optional macOS shell
(`frontend-swift/`) embeds the same workspace and supervises both local
processes.

## Verify a checkout

```bash
JULIA_NUM_THREADS=auto julia --project=webapp webapp/test/runtests.jl
JULIA_NUM_THREADS=auto julia --project=webapp Bnc_julia/test/runtests.jl
julia --project=webapp webapp/test/test_phenotype_pipeline.jl
cd webapp && npm ci && npm run lint && npm run test:js && npm run test:py && npm run test:e2e && cd ..
python3 -m unittest discover -s tests -p 'test_*.py'
python3 -m pip install -r scripts/requirements-verify.txt
python3 scripts/verify_repository.py --check
```

`.github/workflows/ci.yml` runs the same suites on Julia 1.12, Node 20,
Python 3.13, and one macOS runner.

## Repository map

```text
Bnc_julia/                 BindingAndCatalysis mathematics engine (vendored)
webapp/src/                Julia service: API, model cache, jobs, atlas store
webapp/public/             browser workspace and landing page
webapp/scripts/            Python Design Agent, Reader, synthesis tools
webapp/test/, Bnc_julia/test/  Julia and JS test suites
schemas/                   versioned interchange schemas
frontend-swift/            native macOS shell
packaging/                 relocatable backend-bundle builder
src/periodic_table/, scripts/periodic_table/  bounded periodic-table research primitives
tests/                     repository-level Python checks
knowledge/                 short developer notes, decisions, evidence rules
```

## Scientific evidence rules

- A retrieved candidate is a prior, not a verified design; re-run it through
  the engine before presenting it as computed evidence.
- Proxy scores, labels, and finite search results are not proofs of
  realizability, robustness, minimality, or impossibility.
- Failed or non-finite numerical samples stay visible as gaps.

See [knowledge/contracts/scientific-evidence.md](knowledge/contracts/scientific-evidence.md).

## Version and license

`VERSION` owns the application version; `scripts/set_version.sh <version>`
synchronizes the Julia projects, npm metadata, and Xcode project.
Released under the [MIT License](LICENSE).
