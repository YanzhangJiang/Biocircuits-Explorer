# Current status

Biocircuits Explorer is a single-user, local research tool. One researcher runs
the Julia engine, the Julia HTTP service, the browser workspace, and the
optional Python Design Agent on one machine (laptop or workstation). There is no
multi-tenant, cloud, or cluster lane in this repository.

## What the repository establishes

- `webapp/start.sh` starts the Julia service on `127.0.0.1:8088` and the Design
  Chat helper on `127.0.0.1:8765`. The helper accepts browser requests only from
  the workspace's exact loopback origin.
- The API is `/api/v1/*` only. Heavy synchronous handlers enforce work budgets
  and return a structured `422` when a request would exceed them; larger work
  goes through `/api/v1/jobs` (local asynchronous jobs with cooperative
  cancellation and a durable job store).
- Compiled models are content-addressed by NetworkIR hash; a session id is a
  cache handle over that hash.
- Numerical scans, ROP clouds, FRET grids, and placement keep failed solver
  points as explicit gaps; they are never plotted as values.
- The browser workspace is a typed node graph with atomic Quick Add / undo and a
  versioned (v2) JSON document; the macOS shell embeds it and supervises the two
  local processes.
- Retrieval (atlas, design index, Reader) is a prior. Only a fresh engine result
  is presented as verified. See `contracts/scientific-evidence.md`.

## Verification commands

```bash
JULIA_NUM_THREADS=auto julia --project=webapp webapp/test/runtests.jl
JULIA_NUM_THREADS=auto julia --project=webapp Bnc_julia/test/runtests.jl
julia --project=webapp webapp/test/test_phenotype_pipeline.jl
(cd webapp && npm run lint && npm run test:js && npm run test:py && npm run test:e2e)
python3 -m unittest discover -s tests -p 'test_*.py'
python3 scripts/verify_repository.py --check
```

CI (`.github/workflows/ci.yml`) runs the same suites on Julia 1.12, Node 20,
Python 3.13, and one macOS runner for the native shell's unit tests.

## Deliberately out of scope

- Container, reverse-proxy, TLS, registry, and cloud deployment.
- Slurm / HPC execution environments.
- Prometheus metrics, structured request logs, request-id propagation.
- Per-launch bearer tokens for the loopback Design Chat helper.
- The multi-input reaction-order-field research stack (P5–P8 certificates,
  sparse jobs, campaigns). It lives on the `research/ro-field` branch.
- A signed or notarized macOS package.
