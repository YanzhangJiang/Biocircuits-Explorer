---
title: Scientific evidence contract
status: verified
verified_against: 1177a3d
---

# Scientific evidence contract

The product often answers two questions that sound similar:

1. “Which previously computed candidate should we inspect?”
2. “Did this candidate satisfy the declared target under a named computation?”

The first is retrieval; the second is verification. Mixing them creates polished
but unsupported claims. The same boundary applies inside a numerical run: a
partly computed curve can help diagnose a failure, but it cannot certify the
missing part.

## Evidence classes

### Unknown or unavailable

Use when the engine, corpus, dataset identity, required assumption, or numerical
solve is missing. Return an error, abstention, or `unknown`. Do not substitute a
plausible network, zero, an interpolated value, or a stale number.

### Prior or screened proxy

Examples include atlas seeds, Reader panels, match scores, Reader tiers T1/T2/T3,
shape support, atlas volume annotations, and cards marked `proxy_only` or
`screened_proxy`.

These values can rank work and explain why a candidate was chosen. They do not
show that the candidate was freshly simulated for the current request. The
Design Agent must pass a selected prior through the live compute tool before
presenting it as a verified current-session answer.

### Current computation

A current engine result records the concrete network, inputs, outputs,
parameters or scan policy, algorithm/version, and computed curve, surface,
phenotype, or ROP result. In the conversational layer,
`simulate`/`simulate_2d` create this class of evidence. If the engine reports
offline, the agent must abstain rather than reuse the prior as an answer.

A computation verifies only the declared run. It does not prove robustness over
an unscanned region, global optimality, biological implementability, or a
theorem.

### Complete and partial numerical evidence

An equilibrium point is valid only when the solver reports success and the
reported quantity is finite. A non-converged solve, `NaN`, or infinite value is
an invalid gap. It is not a zero and must not be joined to neighboring points to
invent a continuous response.

Current response contracts expose this distinction:

- 1D scans, placer curves, and qK-space ROP clouds return `valid` and `partial`;
- 2D scans, atlas landscapes, and FRET heatmaps return `validity_grid` and
  `partial`;
- placement verification returns `verification_validity` and
  `verification_partial` where applicable;
- inverse refinement records `refinement_status`, valid/sample counts, and
  `partial`.

Partial output may be displayed as a diagnostic with gaps and an explicit
warning. It cannot certify a dose-response shape, threshold, realized behavior
program, feature, robustness clause, or recommendation. It also cannot rerank a
candidate above complete evidence.

Placement passes only when every required verification point is valid. A partial
refinement receives failure status and a losing finite score; it is excluded
from `best_candidate`. Final inverse-design selection uses the original query
ranking whenever `rerank_by_refinement=false` or no valid refined candidate
exists.

### Designability recommendation

`screened_candidates` are exploratory. A card belongs in
`verified_recommendations` only when the screen has exact or sampled enforced
evidence and all hard constraints required by that path pass. Preserve both
`evidence_grade` and `certificate_grade`; labels such as `proxy-only`,
`finite-grid sampled`, `exact-window-siso-rop-path`, and
`exact-union-siso-rop` carry different scopes.

The screen's own summary states the invariant: screened candidates are never
proof. In `bne-design-screen/v0.3.0`, `eligible_count` reports the available
catalogue population, `evaluated_count` reports the bounded prefix actually
checked, and `truncated` marks omitted work. An unevaluated candidate is unknown,
not a failed or verified candidate. A compatibility alias such as `recommended`
must equal the verified list, not the screened list.

### Enumerative or theorem evidence

Existence can be supported by a replayable witness within a declared grammar.
Absence, minimality, and completeness require a certificate or theorem whose
assumptions cover the searched space. A bounded candidate search explicitly does
not certify absence for non-trivial cells
(`src/periodic_table/candidate_search.py`).

Never upgrade “not found,” “not present in this atlas,” or “zero sampled volume”
to “impossible.” State the grammar, bounds, engine/config identity, and whether
the result is witness-only, enumeration-relative, or analytic.

## Required provenance for a reusable result

At minimum retain:

- exact input network/spec and content hash;
- input/output selection and units or log-coordinate convention;
- algorithm and application revision;
- classifier, quantization, and support semantics where ROP programs are used;
- parameter bounds, sampling/solver policy, random seed where applicable;
- the validity vector/grid, invalid/non-converged points, and
  cancellation/partial status;
- result artifact identity and creation time;
- the command or API request needed to reproduce the result.

The result artifact envelope supplies part of this list. Missing scientific
assumptions must remain in the result-specific payload or release manifest.

Canonical relabeling is exact only through seven free species. Above that bound,
identity falls back to deterministic positional content. A reusable result for a
larger model must therefore preserve the original symbol ordering; do not infer
that a renamed payload has the same artifact identity.

## Transfer into a manuscript

Software output becomes a publication claim only after the manuscript
repository records a claim entry that pins:

- claim ID and exact wording;
- strength: illustrative, sampled, enumeration-relative, or theorem;
- assumptions and excluded interpretations;
- Explorer revision and artifact release identity;
- dataset hashes and semantically defined counts;
- reproduction command and expected check;
- figure/table consumers;
- allowed and forbidden wording.

The standalone paper repository owns that ledger and its artifact lock. Explorer
owns the computation and release manifest. Weekly reports preserve dated history
but cannot override either owner. See `knowledge/research/repositories.md`.

## Hard wording guards

Do not write:

- “verified” for an atlas/Reader-only result;
- “passed,” “matched,” or “reranked” from a curve or surface with invalid gaps;
- “robust” without the parameter region, metric, and complete validity evidence;
- “volume” without saying whether it is normalized mass, box fraction, a bound,
  or geometric volume;
- “complete,” “minimal,” or “impossible” from bounded search alone;
- “all networks” without the grammar and structural bounds;
- a resolved periodic-table slice/network count while the catalog marks its
  semantics `unknown`.

Prefer scoped wording such as “the current engine run satisfied…,” “the valid
subset was diagnostic but incomplete,” “a witness exists within grammar G and
bounds B,” or “no candidate was found by bounded search S.”

## Regression evidence

- `webapp/scripts/test_design_agent_contract.py` checks the agent/tool boundary.
- `webapp/scripts/reader/test_reader_nofabrication.py` checks unavailable-corpus
  behavior and prevents Reader candidates from being flagged as verified.
- `webapp/test/designability_spec_contract.jl` and
  `webapp/test/design_screen_contract.jl` check proxy/verified separation,
  evidence grades, complete sampled evidence, hard constraints, and certificate
  semantics.
- `webapp/test/concurrency_and_budget_contract.jl` checks invalid-refinement
  selection, synchronous work limits, and machine-readable failures.
- scan, placement, and frontend validity contracts check that invalid points are
  exposed and rendered as gaps.
- `tests/test_periodic_table.py` checks result statuses and small/trivial witness
  behavior for the standalone periodic-table layer.

Passing these tests enforces important wording/data boundaries; it does not
certify a manuscript claim that has no pinned artifact and claim entry.

## Verified against

- Current source commit: `1177a3d`.
- Historical baseline: retrieval-versus-verification and provenance wording was
  audited at `f9c65a5`; that historical evidence predates the explicit
  validity/partial contracts and cannot establish them.
