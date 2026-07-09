---
module: engine-rop
status: verified
verified_against: f9c65a5
---

# Engine: binding networks and reaction-order geometry

## Purpose

Turn a valid equilibrium binding network into conserved-coordinate models,
asymptotic regimes, regime graphs, SISO/change paths, reaction-order values,
polyhedra, parameter scans, and equilibrium concentrations. This is the
mathematical engine used by the backend, atlas, and designability solver.

## Non-goals

- HTTP routing, authentication, job persistence, and UI state.
- Treating catalogue matches, sampled volumes, or paper prose as mathematical
  proof.
- Silently repairing an invalid reaction/conservation matrix.
- Providing temporal-dynamics evidence for the current designability contract.

## Owner paths

- Public package and core types: [`Bnc_julia/src/BindingAndCatalysis.jl`](../../Bnc_julia/src/BindingAndCatalysis.jl)
- Model construction: [`Bnc_julia/src/initialize.jl`](../../Bnc_julia/src/initialize.jl)
- Regime and graph algorithms: [`Bnc_julia/src/regimes.jl`](../../Bnc_julia/src/regimes.jl), [`Bnc_julia/src/Bnc_regime.jl`](../../Bnc_julia/src/Bnc_regime.jl)
- SISO paths: [`Bnc_julia/src/SISO.jl`](../../Bnc_julia/src/SISO.jl)
- ROP overlay: [`Bnc_julia/src/rop/`](../../Bnc_julia/src/rop/)
- Numerical mapping and scans: [`Bnc_julia/src/qK_x_mapping.jl`](../../Bnc_julia/src/qK_x_mapping.jl), [`Bnc_julia/src/numeric.jl`](../../Bnc_julia/src/numeric.jl)
- Volume estimator: [`Bnc_julia/src/volume_calc.jl`](../../Bnc_julia/src/volume_calc.jl)

## Inputs

- Stoichiometric matrix `N` or conservation matrix `L`, plus optional species,
  total, and binding-constant symbols.
- Optional catalysis matrices for engine APIs that support them.
- A selected total/parameter direction and observed species or linear output.
- Parameter points, ranges, or polyhedral/volume policies.

## Outputs

- A `Bnc` model and cached regime/graph structures.
- Regime indices, permutations, singularity/nullity flags, affine maps, and
  polyhedral conditions.
- SISO/change paths, exact reaction-order profiles, behavior families, and ROP
  plotting geometry.
- Equilibrium concentrations, scan arrays, and normalized probability/fraction
  estimates represented by `Volume`.

## Contract sources

- Constructor invariants and public model behavior:
  [`initialize.jl`](../../Bnc_julia/src/initialize.jl)
- SISO accessors and path semantics:
  [`SISO.jl`](../../Bnc_julia/src/SISO.jl)
- ROP public surface:
  [`rop_exports.jl`](../../Bnc_julia/src/rop/rop_exports.jl)
- Backend conversion from reaction strings:
  [`reaction_parser.jl`](../../webapp/src/reaction_parser.jl)
- Engine-facing API serialization:
  [`BiocircuitsExplorerBackend.jl`](../../webapp/src/BiocircuitsExplorerBackend.jl)

## Tests

- [`Bnc_julia/test/runtests.jl`](../../Bnc_julia/test/runtests.jl): constructor
  rejection, normalized-volume contract, and golden characterization networks.
- [`webapp/test/runtests.jl`](../../webapp/test/runtests.jl): model building,
  graphs, paths, scans, ROP polyhedra, IR bridges, and backend serialization.
- [`webapp/test/d1_atlas_contract.jl`](../../webapp/test/d1_atlas_contract.jl):
  single-base homomer network through engine and atlas.
- [`webapp/test/test_phenotype_pipeline.jl`](../../webapp/test/test_phenotype_pipeline.jl):
  engine-backed phenotype behavior and determinism.

## CI

The `test-julia` job in [`.github/workflows/ci.yml`](../../.github/workflows/ci.yml)
instantiates the web project with the local `Bnc_julia`, runs the backend suite,
the engine golden suite, and the phenotype suite. The current matrix exercises
the production Julia line declared by that workflow; package compatibility is
broader than the matrix currently proves.

## Invariants

- `N` reaction rows and `L` conservation rows must have full row rank, share the
  same species columns, satisfy `d + r = n` and `N * L' = 0`, and make `[L; N]`
  nonsingular. Invalid input is rejected; reactions are not silently deleted.
- Species, total, and binding-constant symbol counts must match model dimensions.
- A model's cached regimes and graphs must be invalidated when coordinate or
  catalysis changes alter their meaning.
- Engine consumers use `get_SISO_graph`, `get_sources`, and `get_sinks` rather
  than depending on obsolete internal fields.
- Reaction-order values at higher-nullity regimes are not uniquely identified;
  callers must preserve that state instead of inventing a scalar.
- The single-polyhedron `calc_volume` overload returns `Volume`; its mean is a
  normalized Gaussian mass or uniform-box fraction in `[0, 1]`, not geometric
  volume in coordinate units.

## Known gaps

- The golden suite is a strong regression baseline, but many values are
  characterization snapshots rather than independent analytic proofs.
- CI currently exercises one Julia release line even though project
  compatibility admits another supported line.
- The package mixes exact/polyhedral and numerical algorithms; evidence strength
  must be named by the caller rather than inferred from the module name.
- Temporal response dynamics are outside the equilibrium designability path.

## Change protocol

1. State whether the change affects construction, regime identity, ROP/path
   semantics, numerical solving, or only presentation.
2. Add a minimal analytic sanity case and a characterization regression for any
   changed mathematical output.
3. Run the engine golden suite and all backend tests that serialize the changed
   object; run phenotype and atlas tests when path or `qK2x` behavior changes.
4. Bump or record the consuming algorithm/config version if persisted artifact
   meaning changes.
5. Rebuild downstream atlas or research artifacts; never compare them across
   changed semantics by filename alone.

See [system architecture](../architecture/overview.md) and
[data provenance](../architecture/data-provenance.md).

## Verified against

- Source commit: `f9c65a5`
- Evidence inspected: engine constructors and public APIs, backend adapters,
  golden and integration contracts, and CI workflow wiring.
- Boundary: no current claim of independent analytic proof, biological
  validation, or support beyond the declared model and solver policies.
