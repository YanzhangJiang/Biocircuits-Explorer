# HTTP API contract

This contract tells a client what it may rely on. The exact path, method, handler,
and legacy-alias inventory is generated from executable route metadata in the
[contract reference](../generated/reference.md#api-routes); do not maintain a
second route list here.

The evidence owners are `webapp/src/api_contract.jl`,
`webapp/src/routing.jl`, `webapp/src/jobs.jl`, and the route contract tests in
`webapp/test/runtests.jl`. Handler source and schemas remain authoritative for
request and response fields.

## Client-facing boundary

The canonical current surface is `/api/v1`.

- New clients use `/api/v1`; bare `/api` paths are deprecated compatibility
  aliases until the declared legacy sunset.
- `/api/v1` and `/api/v1/` return application and protocol identity.
- Application version and API protocol version are separate identities.
- Health, readiness, and metrics are operational routes outside `/api`.

Removing or changing a v1 field requires a compatible migration or a new API
version. Renaming a Julia function alone does not create a new wire contract.

## Shared behavior

- `OPTIONS` is a global CORS preflight behavior, not an endpoint in the route
  inventory.
- JSON responses use `Content-Type: application/json`; the shared error shape is
  `{"error": "..."}`.
- Request-shape errors map to 400, quota exhaustion to 429, and unclassified
  failures to a generic 500 response. Internal exception details stay in logs.
- Every routed response receives `X-Request-Id`. A supplied value is retained
  only when it is at most 128 characters and uses letters, digits, `-`, `_`,
  `:`, or `.`; otherwise the backend generates one.
- Known bare `/api` responses carry the deprecation header and declared sunset.

The CORS policy currently permits any origin and exposes the deprecation header.
That records deployed behavior; it is not a security recommendation for every
future host.

## Boundaries that route names do not prove

Structured network and design payloads are described by the
[schema contract](schemas.md). Selected handlers still accept legacy
`{reactions, kd}` payloads as a bridge, not as the preferred interchange form.

`design_screen` keeps proxy-screened candidates separate from verified
recommendations. A client must not promote a proxy-only candidate to the
verified list.

Job submission returns 202 when accepted. Job terminal states are monotonic,
and cancellation is cooperative for local work: a response reports the
published state, not a promise that arbitrary foreign code was force-killed.

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

## Verification and change rule

The unified read-only gate is:

```bash
python3 scripts/verify_repository.py --check
```

It exports executable route facts, rejects route/catalog/reference drift, checks
the schemas and artifact fixtures, and verifies that generated files are
current. `webapp/test/runtests.jl` additionally exercises version mapping,
deprecation, method rejection, request IDs, probes, metrics, SBML, and error
behavior. Job races and cancellation checkpoints have dedicated Julia contract
tests.

This repository does not expose an OpenAPI document. Do not infer an exact
payload from a route name; cite a schema or handler and add a contract test when
a client begins to depend on a field.
