---
title: HTTP API contract
status: verified
verified_against: b91cf41
---

# HTTP API contract

The HTTP API has two execution lanes. Synchronous routes are for work that is
small enough to finish inside fixed, inspectable limits. Atlas construction,
large queries, persistence, and numerical refinement that can grow beyond those
limits belong in `/api/v1/jobs`. A route name alone does not authorize unbounded
work.

The exact path, method, handler, and legacy-alias inventory is generated from
executable route metadata in the
[contract reference](../generated/reference.md#api-routes). Do not maintain a
second route list here. The evidence owners are
[`api_contract.jl`](../../webapp/src/api_contract.jl),
[`routing.jl`](../../webapp/src/routing.jl), the budget modules under
[`webapp/src/`](../../webapp/src/), and the contract tests under
[`webapp/test/`](../../webapp/test/).

## Client-facing boundary

The canonical current surface is `/api/v1`.

- New clients use `/api/v1`; bare `/api` paths are deprecated compatibility
  aliases until the declared legacy sunset.
- `/api/v1` and `/api/v1/` return application and protocol identity.
- Application version and API protocol version are separate identities.
- Health, readiness, and metrics are operational routes outside `/api`.

Removing or changing a v1 field requires a compatible migration or a new API
version. Renaming a Julia function alone does not create a new wire contract.

## Shared request and response behavior

- `OPTIONS` is a global CORS preflight behavior, not an endpoint in the route
  inventory.
- JSON responses use `Content-Type: application/json`. Ordinary request errors
  retain the shared `{"error":"..."}` shape.
- Every routed response receives `X-Request-Id`. A supplied value is retained
  only when it is at most 128 characters and uses letters, digits, `-`, `_`,
  `:`, or `.`; otherwise the backend generates one.
- Known bare `/api` responses carry the deprecation header and declared sunset.
- CORS currently permits any origin and exposes `X-API-Deprecation` and
  `Retry-After`. This records deployed behavior; it is not a security
  recommendation for every host.

JSON request bodies have three independent application checks: at most
1,048,576 bytes, at most 64 levels of nesting, and at most 100,000 values. The
application byte check runs against `req.body`, after HTTP.jl has already
buffered the request. It is therefore a parsing contract, not a pre-buffer
memory defense. The supplied Nginx configuration enforces the same 1 MiB limit
before proxying the body and is the pre-buffer deployment boundary.

## Machine-readable overload and size failures

Clients should branch on `code`, not on the human-readable `error` text.

| Status | Condition | Required fields and headers |
|---|---|---|
| 413 | JSON body exceeds the 1 MiB limit | `code="request_body_too_large"`, `limit_bytes=1048576`, `retryable=false` |
| 422 | A synchronous work budget is exceeded | `code="sync_budget_exceeded"`, `retryable=false` |
| 429 | Both process-wide heavy synchronous slots are occupied | `code="sync_capacity_exhausted"`, `retry_after_seconds=1`, `retryable=true`, `Retry-After: 1` |

The Nginx 413 body uses the same code, limit, and retryability fields. Quota
exhaustion can also return 429, but it is a separate contract and must not be
mistaken for `sync_capacity_exhausted`.

## Exact synchronous limits

The process admits at most two heavy synchronous handlers at once. Admission is
fail-fast; the gate does not provide a queue, deadline, fairness, or
cancellation. Once admitted, the following hard limits apply.

### Model, scan, ROP, and design limits

| Quantity | Limit |
|---|---:|
| Model dimension `n` | 24 |
| Reactions per model | 5 |
| Regime-enumeration candidate product per ordinary model | 20,000 |
| Design cards for each bounded card list | 64 |
| Exact design placements | 8 |
| Output expressions in one scan | 16 |
| Bytes in one output expression | 1,024 |
| ROP cloud samples | 20,000 |
| ROP geometry points | 20,000 |
| Scan/heatmap solve cost | `points × n^3 <= 50,000,000` |
| qK-space ROP cloud cost | `samples × n^3 <= 20,000,000` |
| ROP geometry cost | `points × n^2 <= 5,000,000` |

Endpoint shape limits further bound the cost: 1D and placer dose-response scans
accept 10–1,000 points; 2D parameter scans accept a 20–200 grid; atlas landscape
accepts 20–160; FRET heatmaps accept 20–300; phenotype classification accepts
`K=1..64` and charges `K × (61 + 161) × n^3` to the scan budget. These shape
limits and the cost formula both apply.

### Atlas build limits

| Quantity | Limit |
|---|---:|
| Explicit networks | 8 |
| Input, output, or change selectors per network | 24 |
| Active change dimensions | 4 |
| Change-expansion candidates before a caller limit | 2,000 |
| Planned behavior slices across the request | 512 |
| Combined explicit-network regime candidates | 100,000 |
| `regime candidates × planned slices` analysis cost | 10,000,000 |

Reaction and species counts are not sufficient on their own: the preflight
builds each bounded model far enough to calculate its regime-candidate product,
then rejects the request before atlas enumeration when either the per-model,
aggregate, slice, or analysis-cost bound is exceeded.

### Query and referenced-corpus limits

| Quantity | Limit |
|---|---:|
| Query result `limit` | 1–100 |
| Behavior slices in an in-memory or SQLite corpus | 100 |
| Networks in a corpus | 100 |
| Indexed records across the declared corpus tables | 10,000 |
| SQLite file plus WAL size | 128 MiB |
| Combined query label/symbol items | 64 |
| Combined predicates | 64 |
| Predicates in one required path sequence | 16 |
| Query structural complexity | 128 |
| Query JSON bytes | 16 KiB |
| Query structural value nodes | 256 |
| Numeric components in one query token | 64 |
| Bytes in one query string/token | 1,024 |
| `corpus slices × query complexity` | 256,000 |
| `corpus slices × query bytes` | 32 MiB |

Support-count maps and required/forbidden/allowed species lists are also capped
at 64 entries each. Synchronous SQLite preflight opens an existing database in
read-only mode, counts its tables, validates network records, and refuses to
initialize or migrate an unknown schema.

Inverse-refinement request fields are shape-checked at `top_k <= 8`,
`trials <= 16`, and `n_points <= 512`, but `enabled=true` is jobs-only. These
numbers are validation bounds, not permission to run refinement synchronously.

## Work that is jobs-only

The synchronous Atlas-family services reject the following work with 422:

- fresh atlas enumeration;
- explicit builds outside the limits above, expanded search profiles beyond
  `binding_small_v0`, and higher-order or homomeric template search;
- behavior volume computation, path-record materialization, or a path scope
  other than `feasible`;
- witness or robustness queries that may trigger lazy materialization;
- enabled inverse refinement, unbounded query/refinement limits, and inverse
  responses requesting `return_library=true`;
- SQLite-backed build, merge, inverse design, creation, migration, or
  persistence, including a nested `atlas_spec.sqlite_path`.

Use `/api/v1/jobs` for those Atlas/library/query/inverse workflows. Other
numeric endpoints do not automatically gain an async equivalent: a scan or ROP
request beyond its limit must be reduced or split unless a documented job type
exists.

Job submission returns 202 when accepted. Job terminal states are monotonic,
and cancellation is cooperative for local work: a response reports the
published state, not a promise that arbitrary foreign code was force-killed.

## HTTP SQLite path policy

Raw `sqlite_path` values are disabled on HTTP APIs by default. In-memory Atlas
payloads remain the normal browser contract. The two backend controls are
`BIOCIRCUITS_EXPLORER_ALLOW_HTTP_SQLITE_PATHS=1` and
`BIOCIRCUITS_EXPLORER_ATLAS_STORE_ROOT=<trusted directory>`; production
operators should set both explicitly. If the root variable is empty, the
implementation uses its built-in Atlas store, never an unrestricted filesystem
root.

The backend resolves relative paths under that root and rejects the root itself,
directories, `..` escapes, paths outside the root, and escapes through existing
symlinks. Even after opt-in, synchronous HTTP access is read-only query access to
an existing bounded database; writes remain jobs-only.

The web UI adds a second, independent opt-in:
`window.BIOCIRCUITS_EXPLORER_ALLOW_HTTP_SQLITE_PATHS=true` must be set before the
app loads. Enabling only the UI does not bypass backend policy, and enabling only
the backend does not make the stock UI send a path.

This mode is for trusted, authenticated, operator-managed single-store
deployments. A raw server filesystem path is not a safe multi-tenant capability;
root containment does not provide per-user database authorization.

## Scientific result validity

Numerical endpoints do not turn a failed equilibrium solve into a number.

- 1D scans and qK-space ROP clouds return a Boolean `valid` vector and
  `partial=true` when any point is invalid.
- 2D scans, atlas landscapes, and FRET heatmaps return `validity_grid` plus
  `partial`.
- Invalid samples are represented as gaps/`NaN` in the computation and are
  serialized safely; clients must mask them and must not interpolate them into
  evidence.

Placement and refinement consumers require complete valid evidence before a
candidate can pass or become the selected best design. A partial refinement is
not eligible as `best_candidate`; when refinement is not requested to rerank, or
when no valid refined candidate exists, final selection falls back to the
original query ranking.

## Boundaries that route names do not prove

Structured network and design payloads are described by the
[schema contract](schemas.md). Selected handlers still accept legacy
`{reactions, kd}` payloads as a bridge, not as the preferred interchange form.

`design_screen` keeps proxy-screened candidates separate from verified
recommendations. In `bne-design-screen/v0.3.0`, `eligible_count` is the
deduplicated catalogue population before the synchronous card limit,
`evaluated_count` is the number actually evaluated, and `truncated` states
whether the former exceeds the latter. `screened_count` is the compatibility
count of evaluated cards. A client must not promote an unevaluated or proxy-only
candidate to the verified list.

With Cognito configured, job routes require a verified bearer token and derive
ownership from its subject. Development mode may use `X-User-Sub` and otherwise
falls back to an anonymous subject; that fallback is not production
authentication.

## Operations boundary

- `/health` is a cheap liveness response with version, revision, and uptime.
- `/ready` checks module initialization and the static asset directory; it
  returns 503 when either check fails.
- `/metrics` emits Prometheus text. Dynamic job IDs are collapsed to a
  low-cardinality path label.

Metrics may reveal API shape and should be restricted at the deployment edge
when public metrics are not intended.

The bounded counter `bcx_http_legacy_requests_total` records actual calls to
declared bare `/api` compatibility aliases with method, canonicalized route
label, and status. Canonical `/api/v1` traffic, unknown paths, and v1-only routes
do not increment it. This counter supplies removal evidence for the declared
`2027-05-25` sunset; it does not by itself authorize deletion, because every
deployed client and rollback version must also be accounted for.

## Verification and change rule

The unified read-only gate is:

```bash
python3 scripts/verify_repository.py --check
```

It exports executable route facts, rejects route/catalog/reference drift,
checks schemas and artifact fixtures, and verifies that generated files are
current. `webapp/test/runtests.jl`, `concurrency_and_budget_contract.jl`, and
`input_validation_contract.jl` exercise route behavior, numerical limits,
capacity admission, body/error payloads, SQLite policy, and validity reporting.

This repository does not expose an OpenAPI document. Do not infer an exact
payload from a route name; cite a schema or handler and add a contract test when
a client begins to depend on a field.

## Verified against

- Current source commit: `b91cf41`; canonical first-party callers and bounded
  legacy-alias metrics were verified locally on 2026-07-15.
- Earlier bounded-runtime anchor: `1177a3d`.
- Historical baseline: the route/version/provenance contract was previously
  audited at `f9c65a5`; that evidence remains historical and does not cover the
  synchronous budgets, SQLite HTTP policy, Design Screen v0.3, or numerical
  validity changes documented here.
