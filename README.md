# Biocircuits Explorer

Biocircuits Explorer is an interactive tool for **Reaction Order Polyhedra (ROP)** analysis of equilibrium binding networks. It provides a browser UI for constructing binding networks, enumerating structural regimes, visualizing regime graphs, and exploring SISO paths and polyhedral geometry.

![Demo](webapp/demo2.gif)

## Repository Layout

```text
Biocircuits-Explorer/
├── Bnc_julia/    # Local copy of BindingAndCatalysis.jl
├── webapp/       # Julia HTTP backend and frontend assets
├── packaging/    # PackageCompiler build scripts for standalone backend bundles
├── frontend-swift/ # Native macOS SwiftUI shell
├── deploy/       # Docker + Nginx deployment files
├── LICENSE
└── README.md
```

## Project Wiki

A repo-local wiki is available under [`wiki/`](wiki/README.md), including quick start, architecture, API notes, atlas workflows, packaging, deployment, and development guidance.

## Requirements

- Julia 1.10 or newer
- A modern browser
- Xcode / `xcodebuild` for the macOS SwiftUI app
- Docker + Docker Compose for server deployment

## Local Development

Clone the repository:

```bash
git clone https://github.com/YanzhangJiang/Biocircuits-Explorer.git
cd Biocircuits-Explorer
```

Install Julia dependencies from the repository root:

```bash
julia --project=webapp -e 'using Pkg; Pkg.develop(path="Bnc_julia"); Pkg.instantiate(); Pkg.precompile()'
```

Start the web app locally:

```bash
cd webapp
./start.sh
```

The server listens on `http://127.0.0.1:8088` by default. To use another port:

```bash
cd webapp
BIOCIRCUITS_EXPLORER_PORT=8090 julia -t auto --project=. server.jl
```

## macOS SwiftUI Development

First build the standalone backend bundle:

```bash
julia --project=packaging packaging/build_backend_app.jl
```

This generates:

```text
dist/BiocircuitsExplorerBackend/
dist/BiocircuitsExplorerBackend/bin/biocircuits-explorer-backend
```

Then build the native macOS app:

```bash
xcodebuild -project frontend-swift/BiocircuitsExplorerMac.xcodeproj -scheme BiocircuitsExplorerMac -configuration Debug -destination 'platform=macOS' CODE_SIGNING_ALLOWED=NO build
```

The SwiftUI app launches the bundled backend locally when available, and can fall back to source-mode startup during development.

## Server Deployment

The `deploy/` directory contains a source-based Docker deployment:

- `deploy/Dockerfile` builds the Julia backend image
- `deploy/docker-compose.yml` runs the Julia app behind Nginx
- `deploy/nginx.conf` serves static assets and proxies API traffic

Build and start the server:

```bash
cd deploy
docker compose build
docker compose up -d
```

The backend runs on port `8088` inside the container, and Nginx exposes the service on ports `80` and `443`.

## Build Release Artifacts

### 1. Standalone Backend Bundle

Build the relocatable backend bundle:

```bash
julia --project=packaging packaging/build_backend_app.jl
```

Output:

```text
dist/BiocircuitsExplorerBackend/
```

This bundle contains the Julia runtime, the compiled backend executable, and the frontend assets.

### 2. macOS SwiftUI App

Build the macOS app from the repository root:

```bash
julia --project=packaging packaging/build_backend_app.jl
xcodebuild -project frontend-swift/BiocircuitsExplorerMac.xcodeproj -scheme BiocircuitsExplorerMac -configuration Debug -destination 'platform=macOS' CODE_SIGNING_ALLOWED=NO build
```

The build output is managed by Xcode in its build products / DerivedData location.


## How To Use A Published Release

### macOS app release

1. Download the packaged macOS app archive.
2. Unzip the app bundle.
3. Launch the app. It starts its local backend automatically and opens the UI.

### Backend-only release

1. Extract the backend bundle archive.
2. Start the executable:

```bash
cd BiocircuitsExplorerBackend
BIOCIRCUITS_EXPLORER_PORT=8088 ./bin/biocircuits-explorer-backend
```

3. Open `http://127.0.0.1:8088` in a browser.

## API Overview

The backend exposes JSON APIs under `/api/`, including:

- `POST /api/build_atlas`
- `POST /api/build_atlas_library`
- `POST /api/merge_atlas_library`
- `POST /api/query_atlas`
- `POST /api/run_inverse_design`
- `POST /api/build_model`
- `POST /api/find_vertices`
- `POST /api/build_graph`
- `POST /api/siso_paths`
- `POST /api/siso_polyhedra`
- `POST /api/siso_trajectory`
- `POST /api/rop_cloud`
- `POST /api/vertex_detail`
- `POST /api/fret_heatmap`

Sessions expire after one hour of inactivity.

## Atlas Batch Prototype

The repository now includes a first atlas-oriented batch builder that wraps the
existing single-network behavior classifier.

Build an atlas JSON from a network spec file:

```bash
julia --project=webapp webapp/build_atlas.jl path/to/spec.json path/to/atlas.json
```

The input spec should contain a `networks` array and may optionally override
the default `search_profile` and `behavior_config`. For example:

```json
{
  "behavior_config": {
    "path_scope": "feasible",
    "min_volume_mean": 0.0
  },
  "networks": [
    {
      "label": "monomer_dimer",
      "reactions": ["A + B <-> AB"],
      "input_symbols": ["tA"],
      "output_symbols": ["AB"]
    }
  ]
}
```

You can also let the backend enumerate a small v0 search space instead of
listing `networks` explicitly:

```json
{
  "behavior_config": {
    "path_scope": "feasible",
    "min_volume_mean": 0.0
  },
  "enumeration": {
    "mode": "pairwise_binding",
    "base_species_counts": [2, 3],
    "min_reactions": 1,
    "max_reactions": 2
  }
}
```

The resulting atlas is organized into:

- `network_entries`
- `input_graph_slices`
- `behavior_slices`
- `regime_records`
- `transition_records`
- `family_buckets`
- `path_records`

You may also include an existing atlas library as `library` together with
`skip_existing=true` to build only the delta atlas that is not already covered
by previously computed slices.

If you provide `sqlite_path`, the builder can also prune directly against a
persisted SQLite atlas store. Set `persist_sqlite=true` if you want the delta
atlas to be merged back into that store immediately after the build.

There is also a matching backend route:

- `POST /api/build_atlas`

## Atlas Library Prototype

Atlas JSON files can now be promoted into a reusable atlas library that supports
incremental imports and re-querying without rebuilding prior results.

Build a new atlas library from an atlas spec or atlas JSON:

```bash
julia --project=webapp webapp/build_atlas_library.jl path/to/input.json path/to/library.json
```

Merge a new atlas or atlas spec into an existing library:

```bash
julia --project=webapp webapp/merge_atlas_library.jl path/to/library.json path/to/input.json path/to/merged_library.json
```

When the merge input is an atlas spec rather than a precomputed atlas JSON, the
merge pipeline now prunes against the existing library before running behavior
classification. Any slice already present in the library under the same
`network_id + input + output + classifier_config` signature is skipped instead
of being recomputed.

The library keeps:

- `atlas_manifests` for import provenance
- merged `network_entries`
- merged `behavior_slices`
- merged `family_buckets`
- merged `path_records`
- `merge_events` for incremental import history

Repeated imports of the same atlas corpus are skipped at the manifest level.
Spec-driven merges that would add no new slices are recorded as
`skipped_all_existing` merge events rather than creating an empty atlas import.

Matching backend routes:

- `POST /api/build_atlas_library`
- `POST /api/merge_atlas_library`

Current limitation: the built-in enumerator only supports the first
`binding_small_v0`-style search mode, namely small reversible pairwise binding
networks with all base species required to appear in at least one reaction.

## SQLite Atlas Store

The same atlas library can now be persisted in SQLite. The database keeps a
full library snapshot plus indexed record tables for:

- `network_entries`
- `input_graph_slices`
- `behavior_slices`
- `regime_records`
- `transition_records`
- `family_buckets`
- `path_records`

This enables two things:

- persistent atlas-library reuse across runs
- direct `slice_id` lookups to skip already computed behavior slices before
  re-running classification

Build or merge an atlas spec directly into a SQLite store:

```bash
julia --project=webapp webapp/build_atlas_sqlite.jl path/to/input.json path/to/atlas.sqlite
```

Export the current SQLite store back to a JSON atlas library:

```bash
julia --project=webapp webapp/export_atlas_library_sqlite.jl path/to/atlas.sqlite path/to/library.json
```

Specs for `build_atlas`, `merge_atlas_library`, `query_atlas`, and
`run_inverse_design` may also include `sqlite_path`. When present, the backend
can load the persisted library, prune against existing `slice_id`s, and save
the merged library back to disk.

The Node Edition atlas workflow now exposes the same SQLite path directly in
the UI:

- `Atlas Spec` can reuse an existing SQLite store and optionally persist the
  newly built delta atlas back into it
- `Atlas Query Config` can query a SQLite store directly, or prefer the
  persisted store attached to an upstream `Atlas Builder`

## Atlas Query Prototype

Once an atlas JSON has been generated, you can query it for candidate behavior
slices:

```bash
julia --project=webapp webapp/query_atlas.jl path/to/atlas.json path/to/query.json path/to/result.json
```

The same command also accepts a SQLite atlas store as the first argument:

```bash
julia --project=webapp webapp/query_atlas.jl path/to/atlas.sqlite path/to/query.json path/to/result.json
```

Example query:

```json
{
  "motif_labels": ["activation_with_saturation"],
  "input_symbols": ["tA"],
  "output_symbols": ["AB"],
  "ranking_mode": "minimal_first",
  "limit": 10
}
```

There is also a compact goal-oriented form for common inverse-design queries:

```json
{
  "goal": {
    "io": "tA -> AB",
    "motif": "activation_with_saturation",
    "witness": "source:0 -> +1 -> sink:+1",
    "forbid_regimes": ["singular"],
    "robust": true,
    "min_volume": 0.02
  },
  "ranking_mode": "minimal_first",
  "collapse_by_network": true,
  "limit": 10
}
```

The backend expands this compact `goal` block into the lower-level
`required_regimes / required_transitions / required_path_sequences /
polytope_spec` fields used by the atlas query engine.

Current query filters support:

- motif-family labels
- exact-family labels
- input and output symbol constraints
- structural upper bounds such as base species count, reaction count, and support
- `minimal_first` or `robustness_first` ranking

There is also a matching backend route:

- `POST /api/query_atlas`

The same query script also works on atlas library JSON files, since the library
preserves the merged atlas object layers needed by the query engine.

## Inverse Design Controller Prototype

The repository now also includes a first inverse-design controller that chains:

- atlas-library reuse
- library-aware delta atlas construction
- library merge
- atlas query

Run it from a single JSON request:

```bash
julia --project=webapp webapp/run_inverse_design.jl path/to/request.json path/to/result.json
```

Example request:

```json
{
  "library": {
    "atlas_library_schema_version": "0.1.0",
    "atlas_schema_version": "0.1.0",
    "atlas_manifests": [],
    "merge_events": [],
    "network_entries": [],
    "behavior_slices": [],
    "family_buckets": [],
    "path_records": [],
    "duplicate_inputs": []
  },
  "inverse_design": {
    "source_label": "activation_search",
    "skip_existing": true,
    "build_library_if_missing": true
  },
  "enumeration": {
    "mode": "pairwise_binding",
    "base_species_counts": [2, 3],
    "min_reactions": 1,
    "max_reactions": 2
  },
  "behavior_config": {
    "path_scope": "robust",
    "min_volume_mean": 0.01
  },
  "query": {
    "motif_labels": ["activation_with_saturation"],
    "input_symbols": ["tA"],
    "ranking_mode": "minimal_first",
    "collapse_by_network": true,
    "limit": 5
  }
}
```

The controller returns:

- `delta_atlas_summary` for the newly computed atlas increment
- `library_summary` for the post-merge atlas library
- `query_result` for the requested behavior search

The inverse-design controller now defaults to a versioned
`support-first + summary-first + lazy witness` workflow:

- raw requests are first compiled into a stable hashed `Gamma_Q`
- candidate supports are screened before realized slice analysis
- atlas delta builds default to summary-only records (no eager `path_records`)
- witness paths are materialized only when a query or refinement step needs them
- hard negatives and soft failures are stored separately and remain profile-relative

You can also attach an optional `refinement` block to numerically post-process
the top atlas candidates. The current controller first tries polytope-guided
seeds from a materialized witness path and only falls back to random background
scans when no reliable witness polyhedron is available.

Example refinement block:

```json
{
  "refinement": {
    "enabled": true,
    "top_k": 3,
    "trials": 6,
    "param_min": -6.0,
    "param_max": 6.0,
    "n_points": 200,
    "background_min": -3.0,
    "background_max": 3.0,
    "include_traces": true,
    "rerank_by_refinement": true
  }
}
```

When enabled, `run_inverse_design` will return an additional
`refinement_result` block with per-candidate scan summaries, inferred numeric
motif labels, and the best trial found for each candidate.

If the request includes an existing library plus `skip_existing=true`, then any
slice already present in that library under the same
`network_id + input + output + classifier_config` signature is skipped before
behavior classification runs.

Matching backend route:

- `POST /api/run_inverse_design`

The Node Edition frontend now exposes the same workflow directly in the UI:

- `Atlas Spec`
- `Atlas Builder`
- `Atlas Query Config`
- `Atlas Query Result`
- `Atlas Inverse Design`

You can add them individually from `Add Node`, or use `Quick Add -> Atlas Workflow`
to create a connected atlas build/query chain. `Quick Add -> Atlas Inverse Design`
creates a spec/query/pipeline chain that runs `run_inverse_design` with the
same support-first, summary-first, lazy-witness backend.

## Acknowledgment

The core computational engine is [BindingAndCatalysis.jl](https://github.com/Qinguo25/BindingAndCatalysis.jl) by Qinguo Liu. The `Bnc_julia/` directory in this repository is a local copy of that package. Credit for the underlying ROP theory implementation belongs to the original authors.

## License

This repository uses the license defined in the root `LICENSE` file. The vendored `BindingAndCatalysis.jl` copy in `Bnc_julia/` remains separately licensed under MIT by its original author.
