# HTTP API contract

Verified against `f9c65a5`. The executable owners are
`webapp/src/routing.jl`, `webapp/src/BiocircuitsExplorerBackend.jl`, and
`webapp/src/jobs.jl`. This page is a compatibility map; handler source and tests
remain authoritative for complete request and response fields.

## Version boundary

The canonical current surface is `/api/v1`.

- `/api/v1` and `/api/v1/` are discovery endpoints and return the build/version
  payload.
- `/api/v1/<endpoint>` is internally mapped to the existing handler for
  `/api/<endpoint>`.
- Bare `/api/<endpoint>` remains a deprecated compatibility alias. Known legacy
  responses carry `X-API-Deprecation` with the declared sunset date 2027-05-25.
- Health and metrics routes are not under `/api` and do not carry the API
  deprecation header.
- The discovery payload declares `api_version`, `api_supported`, and
  `api_legacy_sunset`; application version and API protocol version are separate
  identities.

New clients must use `/api/v1`. Removing or changing a v1 field requires either a
compatible migration or a new API version; changing only the Julia function name
does not create a new wire contract.

## Methods and response basics

- Routes in `API_ROUTES` accept `POST`; another method returns 405.
- `/health`, `/ready`, and `/metrics` accept `GET` and `HEAD`.
- Version discovery and public auth configuration accept `GET` and `POST`.
- `/api/v1/local-image` accepts `GET` only.
- `OPTIONS` returns a CORS preflight response.
- JSON responses use `Content-Type: application/json`. The shared error shape is
  `{"error": "..."}`.
- Request-shape errors map to 400, quota exhaustion to 429, and unclassified
  handler failures to a generic 500 response. Internal exception details belong
  in logs, not the client body.
- Every routed response receives `X-Request-Id`. A client value is retained only
  when it is at most 128 characters and contains letters, digits, `-`, `_`, `:`,
  or `.`; otherwise the backend generates one.

The CORS policy currently permits any origin and exposes the deprecation header.
That is deployed behavior, not a security recommendation for every future host.

## Route groups

All paths below use their canonical `/api/v1` form.

### Atlas construction and inverse search

- `POST /api/v1/build_atlas`
- `POST /api/v1/query_atlas`
- `POST /api/v1/build_atlas_library`
- `POST /api/v1/merge_atlas_library`
- `POST /api/v1/run_inverse_design`

### Model, ROP, and phenotype computation

- `POST /api/v1/build_model`
- `POST /api/v1/find_vertices`
- `POST /api/v1/build_graph`
- `POST /api/v1/siso_paths`
- `POST /api/v1/siso_polyhedra`
- `POST /api/v1/siso_path_condition`
- `POST /api/v1/siso_trajectory`
- `POST /api/v1/behavior_families`
- `POST /api/v1/phenotype_classify`
- `POST /api/v1/rop_cloud`
- `POST /api/v1/vertex_detail`
- `POST /api/v1/fret_heatmap`
- `POST /api/v1/parameter_scan_1d`
- `POST /api/v1/parameter_scan_2d`

### Parameter placement and designability

- `POST /api/v1/place_parameters`
- `POST /api/v1/placer_menu`
- `POST /api/v1/placer_curve`
- `POST /api/v1/placer_threshold`
- `POST /api/v1/placer_realize_program`
- `POST /api/v1/placer_level`
- `POST /api/v1/design_search`
- `POST /api/v1/design_screen`
- `POST /api/v1/validate_designability_spec`
- `POST /api/v1/design_labels`
- `POST /api/v1/atlas_landscape_2d`
- `POST /api/v1/rop_polyhedron`

`design_screen` separates `screened_candidates` from
`verified_recommendations`. Clients must not promote proxy-only screened cards to
the verified list.

### IR and exchange

- `POST /api/v1/ir/network/validate`
- `POST /api/v1/ir/design/validate`
- `POST /api/v1/import/sbml`
- `POST /api/v1/export/sbml`

The structured contracts are indexed in `knowledge/contracts/schemas.md`.
Legacy `{reactions, kd}` inputs remain accepted by selected handlers but are a
bridge, not the preferred interchange form.

### Jobs, auth, and diagnostics

- `POST /api/v1/jobs` submits a job and returns 202 on accepted submission.
- `GET|POST /api/v1/jobs/<id>` reads job state.
- `GET|POST /api/v1/jobs/<id>/result` reads the result.
- `GET|POST /api/v1/jobs/<id>/result-url` requests a result URL when supported.
- `POST /api/v1/jobs/<id>/cancel` requests cancellation.
- `GET|POST /api/v1/auth/config` exposes only public auth bootstrap settings.
- `GET|POST /api/v1/version` returns build and protocol identity.
- `POST /api/v1/debug_logs` uses the normal routed handler contract.

With Cognito configured, job routes require a verified bearer token and derive
ownership from its subject. Development mode can use `X-User-Sub` and otherwise
falls back to the anonymous subject. This development fallback must not be
described as production authentication.

Job terminal states are monotonic. Cancellation is cooperative for local work;
the route returns the published job state rather than promising that arbitrary
foreign code was force-killed.

## Operations routes

- `GET|HEAD /health` is a cheap liveness response and reports version, revision,
  and process uptime.
- `GET|HEAD /ready` checks module initialization and the static asset directory;
  it returns 503 when either check fails.
- `GET|HEAD /metrics` emits Prometheus text. Path labels are deliberately
  low-cardinality; job IDs are collapsed to `/api/jobs/:id`.

The metrics route may reveal API shape and should be restricted at the deployment
edge when public metrics are not intended.

## Compatibility checks

The primary regression owner is `webapp/test/runtests.jl`, including version
mapping, deprecation headers, method rejection, request IDs, probes, metrics,
SBML, and error behavior. Job races and terminal-state rules are additionally
covered by:

- `webapp/test/jobs_cancellation_contract.jl`
- `webapp/test/cooperative_cancel_checkpoints_contract.jl`

This repository does not yet expose an OpenAPI document. Do not invent an exact
payload contract from a route name; cite a schema or handler and add a contract
test when a client begins to rely on a field.
