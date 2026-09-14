# Project summary

Biocircuits Explorer is a single-user, local tool for reasoning about
protein-binding networks: describe a network, compute how its outputs respond
to inputs (regimes, reaction orders, scans, ROP geometry), and search for
networks that match a desired response. Retrieved candidates stay visibly
separate from engine-recomputed results.

## Runtime ownership

1. `Bnc_julia/` — the mathematics engine and its golden-value behavior.
2. `webapp/src/` — the Julia HTTP API (`/api/v1/*`), content-addressed model
   cache, work budgets, local asynchronous jobs, atlas SQLite store, NetworkIR
   and SBML bridges.
3. `webapp/public/` and `frontend-swift/` — the browser node workspace and the
   macOS shell that embeds it.
4. `webapp/scripts/` — the Python Design Agent (LLM tool-calling over the live
   engine), the Function-Space Reader, and synthesis tools.
5. `src/periodic_table/`, `scripts/periodic_table/` — bounded research
   searches, not universal negative proofs.

## Working rules

- Everything binds to loopback; the Design Chat helper accepts browser requests
  only from the workspace's exact origin.
- Never present Reader retrieval, proxy margins, or bounded search absence as
  a proof.
- Keep terminal job states monotonic and cancellation cooperative.
- Scientific counts belong with a named population and reproduce command, not
  in prose.

The verification commands and the list of things deliberately out of scope are
in [knowledge/status/current.md](knowledge/status/current.md).
