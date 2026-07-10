# Project summary

Biocircuits Explorer helps a researcher describe a protein-binding network,
compute how its outputs change, and search for networks that could match a
desired response. The central result is not merely a list of candidates: the
product keeps a retrieved or suggested candidate separate from a result
recomputed by the mathematics engine.

The current implementation evidence anchor is `1177a3d`. Revision `f9c65a5`
remains the historical evidence baseline for the maintained knowledge catalog.
Later documentation commits may advance the branch without changing which
implementation revision these runtime claims inspected.

## What the current revision establishes

- Local quick start defaults the workspace to `127.0.0.1:8088` and Design Chat
  to `127.0.0.1:8765`.
- Synchronous compute requests are admitted through a two-slot process gate and
  rejected before known unbounded work. Oversized work returns a structured
  `422`; temporary capacity exhaustion returns `429` with `Retry-After`.
- Compiled models are content-addressed and single-flight. Each shared model
  bundle owns its mutable-cache lock, while session aliases and compiled-model
  entries keep separate bounded LRU state.
- Parameter scans, FRET heatmaps, ROP clouds, placement verification, and
  inverse refinement preserve failed-solver points as explicit partial/invalid
  evidence; those points are not plotted or promoted as best designs.
- Raw HTTP `sqlite_path` access is disabled by default. An operator must enable
  it explicitly on both client and server, and the server confines paths to a
  configured store root.
- `webapp/start.sh` is the one explicit unauthenticated Design Chat path. It is
  limited to loopback development and an exact browser origin.
- Outside that explicit development mode, Design Chat launches require an
  exact loopback origin and a bearer token of at least 32 characters. The
  macOS shell creates a new 256-bit token for each helper launch and keeps it
  in memory.
- The main web application CI is configured for Julia 1.12. The separate
  headless HPC environment is configured for Julia 1.10 and 1.12. Local checks
  loaded the selected locks on Julia 1.10.11 and 1.12.6; neither result proves
  a live scheduler.
- The Docker workflow is configured to build and start one application image and probe its
  runtime behavior. It does not exercise Compose, Nginx, or TLS.
- The seven targeted macOS unit tests passed locally at `01a01be`. No workflow
  runs Xcode, so macOS remains outside CI.
- Version and packaging checks keep the application version, packaged
  resources, image labels, and tag rules synchronized and fail closed on
  inconsistent release input.

A configured CI version is not proof that an external run passed. A static
deployment test is not proof that a cloud service, cluster, signed image, or
installer worked in its real environment.

## Runtime ownership

1. `Bnc_julia/` owns the mathematics engine and golden-value behavior.
2. `webapp/src/` owns the Julia API, interchange bridges, reusable-result
   storage, jobs, and persistence.
3. `webapp/public/` and `frontend-swift/` own the browser and macOS clients.
4. `webapp/scripts/` owns the Python Design Agent, Function-Space Reader, and
   synthesis tools.
5. `webapp_hpc/`, `slurm/`, and `deploy/` own headless runtime, scheduler, and
   deployment mechanics; passing their static checks is not a live rollout.
6. `src/periodic_table/` and `scripts/periodic_table/` own bounded searches,
   not universal negative proofs.

Source, schemas, tests, and versioned artifact manifests outrank prose. The
maintained compression layer is `knowledge/`; ignored `doc/`, `docs/`, and old
developer-wiki material are historical until re-verified. This public
computation repository produces artifacts only; draft manuscripts, private
feedback, and paper-side data are deliberately kept outside it.

## Evidence rules

- Use `/api/v1/*`; bare `/api/*` is a deprecated compatibility surface.
- Never present Reader retrieval, proxy margins, or bounded search absence as a
  proof.
- Preserve terminal Job state, atomic canonical-record publication,
  cooperative cancellation checkpoints, and all behavior-identity fields.
- Keep scientific counts attached to a named population, content hash, source
  revision, and reproduce command.
- If a count, claim, path, version, or external outcome lacks current evidence,
  report it as unknown.

## What remains unverified

The current checkout does not establish live registry publication or pull,
image signing, signature verification, an SBOM release lane, live AWS services,
live Slurm execution, a signed and notarized DMG, or the full Compose/Nginx/TLS
stack. It also does not resolve the recorded periodic-table population and
producer-lineage conflicts. Scripts and contract tests exist for parts of these
paths, but they do not upgrade an external result from unknown to verified.

For local work: inspect `git status`, read
[the current status](knowledge/status/current.md), choose the relevant module
in [the module catalog](knowledge/catalogs/modules.yaml), and verify its cited
source and tests before changing it. Exact commands and handoff rules are in
[AGENTS.md](AGENTS.md).
