# Glossary

This glossary gives a usable picture before the formal name. Formal equations,
edge cases, and version-specific field names remain in the cited source or schema.
Definitions are verified against `f9c65a5`.

## Product and data language

**NetworkIR**

A versioned JSON description of a biochemical network: species, reactions,
observables, parameter distributions, provenance, and extensions. Its current
family is `bne-ir`; the Julia structures and parser in `webapp/src/ir.jl` own the
runtime meaning.

**DesignSpec**

A versioned, general design request containing a goal, constraints, objectives,
policies, provenance, and extensions. It is not the same object as the stricter
DesignabilitySpec. Source: `webapp/src/ir.jl`.

**DesignabilitySpec**

The strict input to the designability screen. It names the source, target,
constraints, candidate budget, ranking policy, and audit policy. The normalizer
requires the exact current schema version. Source: `webapp/src/designability.jl`.

**Result artifact envelope**

Metadata attached to a computed result so it can identify its kind, input hashes,
algorithm version, configuration hash, creation time, and warnings. It helps
replay and comparison; it does not by itself prove a scientific conclusion.
Source: `webapp/src/result_artifact.jl`.

**Canonical network code**

A topology identity for supported binding networks that is invariant to reaction
ordering and free-species renaming. It is shared by the atlas and the IR layer.
Parameter values and observable choices can still distinguish complete NetworkIR
content hashes. Source: `webapp/src/canonicalization.jl` and `webapp/src/ir.jl`.

**Atlas**

A computed collection of network/input/output slices and their behavior records.
It is a reusable search prior and evidence source under its declared grammar and
configuration. It is not an all-biochemistry universe and is not a substitute for
a fresh computation when the product promises a verified candidate.

**Slice**

One analysis view of a network for a declared input, output, and classifier
configuration. Slice counts and network counts answer different questions; never
infer one from the other without the dataset's declared semantics.

**Phenotype**

A label or set of metrics computed from a simulated response curve or surface.
The one-dimensional pipeline computes a log-log response slope and applies
declared gates; a label is therefore conditional on its grid, parameter policy,
phenotyper version, and validity checks. Source:
`webapp/src/latent_atlas/phenotype_pipeline.jl`.

## Reaction-order language

**Reaction order (ROP, represented as ρ in the forward pipeline)**

How strongly an output changes on a logarithmic scale as an input changes on a
logarithmic scale. The forward phenotype code computes
`d log(output) / d log(input)` by finite differences. The exact engine also emits
output-order tokens along dominance regimes. A local or quantized value must not
be described as a global dose-response law. Sources:
`webapp/src/latent_atlas/phenotype_pipeline.jl` and `Bnc_julia/src/rop/`.

**Regime**

A region in parameter or input space where the same dominance assumptions define
the active asymptotic description. A regime index is not a biological state name
unless a separate mapping establishes that interpretation.

**Behavior program / exact profile**

The ordered reaction-order tokens associated with a path through regimes. Its
identity depends on the quantization digits and scale, `program_identity`, and
`support_semantics`; those fields must travel together. Source:
`webapp/src/behavior_program_codec.jl`.

**Sign sequence**

A compressed sequence of positive, zero, or negative response-order signs. In
the sampled phenotype path, values are dead-banded and repeated signs are
collapsed. It describes the sampled/declared policy, not every possible parameter
setting. Source: `webapp/src/latent_atlas/phenotype_pipeline.jl`.

**SISO**

Single input, single output. It identifies the analysis interface, not the total
number of species or reactions in the network.

**`d`, `r`, and `mu`**

Periodic-table structural coordinates used by the current profile: `d` is the
number of base-species coordinates, `r` is reaction count, and `mu` bounds complex
size. Always cite the grammar/profile because another enumerator could assign
different scope. Sources: `src/periodic_table/complete_definition.py`,
`src/periodic_table/complex_generator.py`, and
`src/periodic_table/network_generator.py`.

## Evidence and design language

**Prior**

Information used to choose or rank candidates before a fresh check. Atlas seeds,
Reader candidates, match scores, and Reader evidence tiers are priors in the
Design Agent workflow. They must not be presented as verified current-session
answers. Source: `webapp/scripts/design_agent.py`.

**Verification**

A named check performed against a declared target under declared assumptions. In
the interactive agent this normally means a current engine computation. In the
designability screen, only cards placed in `verified_recommendations` satisfy the
screen's exact or sampled evidence rule. Verification is scoped; it does not
automatically establish a theorem or an experimental result.

**Designability**

Whether a candidate can meet a declared target and constraints under the screen's
specified search and evidence rules. It is not a context-free property of a
topology. Source: `webapp/src/designability.jl`.

**Feasible region**

The parameter values for which the declared constraints hold. A sampled set, a
single point, a polytope, and a union of polytopes carry different guarantees;
retain the representation and certificate grade.

**Design margin / Chebyshev radius**

The radius of the largest admitted ball under the screen's chosen coordinates
and bounds. It is a lower-bound-style measure of local parameter room, not the
exact volume of every feasible parameter setting. Source:
`schemas/designability-spec.schema.json`.

**Witness**

A concrete network, path, parameter point, or result that demonstrates existence
for a stated property. A witness can prove “at least one” within scope; it cannot
prove absence.

**Certificate**

Machine-checkable evidence with an explicit grade and assumptions. “Proxy-only,”
sampled, exact-linear, exact-window, structural, and union-of-polytopes grades are
not interchangeable. Source: `schemas/designability-screen.schema.json`.

**Support**

An overloaded word. It may mean complex composition, atlas occurrence, path
count, or statistical/shape support. Write the qualified field name instead of
the bare word in new documents.

**Unknown**

The required state when evidence is absent, conflicting, or semantically
ambiguous. Unknown is not zero, false, impossible, or “probably unchanged.”
