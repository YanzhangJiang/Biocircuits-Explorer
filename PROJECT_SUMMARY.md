# Project summary

Biocircuits Explorer is a computation and design environment for equilibrium
protein-binding networks. A user describes reversible binding reactions; the
Julia service builds the mathematical model, analyzes how outputs change with
inputs, stores reusable computed behaviors, and searches for designs that match
a requested response. The browser workspace and native macOS shell are clients
of that service. A separate Python Design Agent translates natural-language
intent and can retrieve prior candidates, but retrieval never upgrades a
candidate to verified evidence.

The runtime has five important boundaries:

1. `Bnc_julia/` owns the mathematical engine and golden-value behavior.
2. `webapp/src/` owns the Julia API, IR/SBML bridges, Atlas, jobs, and persistence.
3. `webapp/public/` and `frontend-swift/` own the browser and macOS clients.
4. `webapp/scripts/` owns the Python Agent, Function-Space Reader, and synthesis tools.
5. `src/periodic_table/` and `scripts/periodic_table/` own bounded research searches, not universal negative proofs.

Source, schemas, tests, and artifact manifests outrank prose. The maintained
compression layer is `knowledge/`; ignored `doc/`, `docs/`, and developer wiki
material are historical until re-verified. The computation repository produces
artifacts, the standalone periodic-table paper repository owns current
manuscript claims, and the private weekly-report archive is historical feedback.

The runtime baseline at commit `f9c65a5` passed the complete Julia service
suite, the Bnc constructor/volume/golden suites, frontend and Python contracts,
Reader no-fabrication checks, schema generation, artifact validation, and
repository-level periodic-table tests. Julia 1.12 is CI-verified; Julia 1.10 is
declared compatible but not currently in the CI matrix.

Non-negotiable evidence rules:

- Use `/api/v1/*`; bare `/api/*` is a deprecated compatibility surface.
- Never present Reader retrieval, proxy margins, or bounded search absence as a proof.
- Preserve terminal Job state, atomic canonical-record publication, and cooperative cancellation checkpoints.
- Preserve all behavior-identity fields when deriving Atlas configurations.
- If a count, claim, path, or version lacks current evidence, report it as unknown.

For local work: inspect `git status`, read
`knowledge/status/current.md`, choose the relevant module card, verify its cited
source and contract tests, then make the smallest coherent change. Full commands
and handoff rules are in [AGENTS.md](AGENTS.md).
