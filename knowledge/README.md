# Knowledge base

Short, maintained developer notes. Source, schemas, and executable tests are the
authority; this directory only routes to them and records durable decisions.

- [`status/current.md`](status/current.md) — what the current branch establishes
  and what it deliberately does not.
- [`contracts/scientific-evidence.md`](contracts/scientific-evidence.md) — the
  rules that keep retrieved, screened, and computed results distinguishable.
- [`decisions/`](decisions/README.md) — architecture decision records.
- [`glossary.md`](glossary.md) — reaction order, regime, ROP, and related terms.

Ownership at a glance:

| Area | Owner path | Tests |
|---|---|---|
| Mathematics engine | `Bnc_julia/src` | `Bnc_julia/test/runtests.jl` |
| Julia HTTP service, jobs, atlas store | `webapp/src` | `webapp/test/runtests.jl` |
| Browser workspace | `webapp/public` | `webapp/test/*.test.mjs`, `webapp/e2e` |
| Design Agent / Reader (Python) | `webapp/scripts` | `webapp/scripts/test_*.py` |
| macOS shell | `frontend-swift` | `BiocircuitsExplorerMacTests` |
| Interchange schemas | `schemas/` | `tests/test_workspace_schema.py`, `webapp/scripts/gen_schemas.jl --check` |
| Periodic-table research primitives | `src/periodic_table`, `scripts/periodic_table` | `tests/test_periodic_table.py` |

When a code change alters behavior: update the owning test first, then the schema
if one exists, then this directory only if a durable decision changed.
