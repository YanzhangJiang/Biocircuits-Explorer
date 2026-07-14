# Design Agent

`module_id: design-agent`

## Purpose

Provide the optional conversational inverse-design surface. The Python chat
service accepts a natural-language turn, lets a configured LLM call narrowly
scoped tools, sends proposed networks to the live Julia backend, and returns
renderable candidate cards plus an optional rerunnable `DesignabilitySpec`.
For a pinned exact finite-window design, it can also pass one typed shape edit
to the canonical fixed-topology optimizer; Python does not lower or solve the
geometry itself.

## Non-goals

- It is not the numerical or scientific authority; Julia engine responses and
  downstream designability checks own those decisions.
- Atlas lookup, corpus prevalence, retrieval rank, and proxy metrics are priors
  or screening signals, not verified designs.
- It does not certify external LLM quality, provider availability, or the
  completeness/currentness of any optional local corpus.
- It does not build shape-optimization matrices, calculate margins, identify
  peaks, or reinterpret backend duals and replay metrics.

## Owner paths

- Runtime: `webapp/scripts/design_agent.py`, `webapp/scripts/chat_api.py`,
  `webapp/scripts/engine_client.py`, `webapp/scripts/llm_compile.py`
- Seed/search helpers: `webapp/scripts/design_search.py`,
  `webapp/scripts/cards.py`
- Browser surface: `webapp/public/js/agent-view.js`,
  `webapp/public/js/llm-settings.js`, `webapp/public/js/agent-node.js`
- Persisted trace shape: `schemas/design-agent-trace.schema.json`

## Inputs

- `POST /design-chat` with `message`, client-held `state`, optional `llm`
  configuration, and a display limit.
- Optional `BNE_CHAT_MAX_CONCURRENT_TURNS`; it is a strict positive integer,
  defaults to two, and is capped at 32.
- Optional bounded trace-store settings: `BNE_TRACE_MAX_BYTES` (16 MiB per
  segment by default), `BNE_TRACE_ARCHIVE_COUNT` (three rotated segments),
  `BNE_TRACE_ARTIFACT_MAX_BYTES` (256 MiB), and
  `BNE_TRACE_ARTIFACT_MAX_FILES` (4,096). Values use canonical decimal syntax
  and hard implementation maxima.
- A configured OpenAI-compatible or Anthropic-compatible LLM endpoint.
- The Julia backend endpoints used by `engine_client.py`.
- A complete `bne-rop-shape-optimize-request/v1.0.0` object for the
  allow-listed `optimize_rop_shape` tool. Extra arguments and endpoint
  overrides are rejected.
- Optional ignored datasets used only to find candidate seeds.

The HTTP service itself can start without an LLM key. In that state a
`/design-chat` turn returns `kind=need_key`; the Julia workspace remains
available independently.

## Outputs

- A response containing `kind`, `reply`, `family`, `cards`, `info`, updated
  client state, and `trace_id`; a valid tool result may also contribute a
  `designability_spec`.
- For one-input reaction-order requests, `design_from_behavior` performs a
  two-stage handoff: an unbound target discovers candidate I/O roles, then a
  candidate-bound `DesignabilitySpec` is validated and screened. Only an exact
  recommendation that also passes a fresh `/api/v1/placer_curve` replay enters
  the returned-card path. Both Design Screen responses must identify the current
  screen schema, and every replay validity marker must be the literal Boolean
  `true` with `partial=false`.
- Best-effort local `traces/traces.jsonl` records, bounded rotated segments,
  and atomically published content-addressed card artifacts. Trace write
  failure is intentionally non-fatal, so a `trace_id` alone does not prove that
  a trace file exists. Artifact byte/file retention is independent of the
  compact log; an older trace summary may outlive its retired full-card file.
- A shape-optimization card is admitted only from the current v1 backend
  response when the topology/reference identities match, the parameter margin
  uses the declared parameter-only basis, the population label is consistent
  with truncation, and replay is complete evidence from exactly
  `POST /api/v1/placer_curve`. Otherwise the tool returns withheld evidence
  rather than a plausible card.

## Contract sources

- Request/response and health handling: `webapp/scripts/chat_api.py`
- Tool allow-list, card admission, abstention, and trace production:
  `webapp/scripts/design_agent.py`
- Engine error typing and endpoint payloads: `webapp/scripts/engine_client.py`
- Fixed-topology optimizer semantics:
  `knowledge/modules/rop-shape-optimization.md`
- Agent-to-`DesignabilitySpec` lowering and rejection rules:
  `webapp/scripts/design_agent.py` and
  `webapp/public/js/node-types/design-target.js`
- Trace fields: `schemas/design-agent-trace.schema.json`

## Tests

- `webapp/scripts/test_design_agent_contract.py` covers provider configuration,
  spec lowering, invalid-spec rejection, solver prerequisites, source identity,
  the exact-design tool contract, strict integer budgets, screen-version and
  curve-validity fail-closed behavior, prevention of stale or cross-card spec
  attachment, strict shape-tool arguments, canonical optimizer/replay routes,
  identity checks, refusal to recompute backend geometry in Python, rotating
  trace retention, atomic artifact publication, pruning, and concurrent JSONL
  append integrity.
- `webapp/scripts/test_design_agent_live_engine.py` starts the real Julia HTTP
  server and drives the production Agent dispatch with a deterministic provider
  tool call. It asserts the live Design Screen validation/screening and Placer
  replay path; it does not contact an external model provider.
- `webapp/scripts/test_chat_api.py` covers helper-process lifecycle edge cases,
  strict turn-capacity configuration, fail-fast overload, and permit release
  after success or failure.
- `webapp/test/design-spec-node-contract.test.mjs` covers browser-side export
  and rejection of non-rerunnable agent payloads.
- `webapp/test/design-agent-conversation-owner-contract.test.mjs` uses delayed
  fetches to cover single-flight turns, workspace/conversation ownership,
  aborted and late responses, and result reconstruction during restore.
- The ordinary Python contracts mock tool results. The separate live-engine
  contract uses the real HTTP engine and stubs only the external provider response.

## CI

`.github/workflows/ci.yml` runs the Python contract tests through
`npm run test:py`, browser contracts through `npm run test:js`, and the
process-level Agent-to-engine handoff in the Julia lane. CI does not contact an
external model provider or validate optional retrieval corpora.

## Invariants

- A retrieved network remains an explicitly unverified seed until a fresh
  compute tool evaluates it in the current session.
- Proxy scores, atlas labels, and retrieval support must never be promoted to a
  verified card or exact certificate.
- Display cards are admitted only from fresh `simulate`, `simulate_2d`, or
  `design_from_behavior` tool results under their local admission contracts; an
  explicitly present but invalid spec payload withholds that card.
- `design_from_behavior` rejects lossy integer coercion, stale or missing Design
  Screen versions, non-Boolean curve validity markers, and partial curve replies.
- `optimize_rop_shape` accepts only the canonical v1 request object and calls
  only `/api/v1/rop_shape_optimize`. Its successful card keeps exact geometry,
  population coverage, parameter-only margin, and sampled replay as separate
  evidence fields.
- Engine connection failures remain typed as `engine_offline`; the agent must
  abstain from inventing curves, labels, or metrics.
- API keys are not written into Design Agent traces, and the HTTP service logs
  request paths rather than request bodies.
- Missing LLM credentials produce the explicit `need_key` response; they do not
  fabricate a deterministic answer or make the Julia workspace unavailable.
- Expensive helper turns use a process-local bounded semaphore. Capacity
  exhaustion fails immediately with HTTP 429,
  `code=chat_capacity_exhausted`, `retryable=true`, and `Retry-After: 1`;
  requests are not hidden in an unbounded turn queue. The permit is released in
  `finally` after either a successful or failed Agent turn.
- Browser turns are single-flight and carry the workspace conversation epoch
  that owned their predecessor state. Replacing the conversation retires and
  aborts its pending turn; a late success or failure cannot mutate the new
  conversation log, chat state, active card, result panes, or thread DOM.
- Conversation restore clears the previous workspace's active candidate and
  result panes before rendering. The latest restored agent response owns the
  result: its first valid card rebuilds the panes, while no valid card leaves
  them cleared.
- Trace appends and artifact publication are serialized within one helper
  process. Trace logs rotate before the configured segment target, with one
  oversized record allowed to occupy its own segment; archive count is hard
  bounded even after a lower archive-count reconfiguration. Artifact retention
  runs independently after a card spill even if the compact-log append fails,
  and enforces byte/file ceilings by retiring least-recently-used files while
  preserving compact summaries. Reuse of a valid content-addressed card
  refreshes its retention recency; a truncated or mismatched predecessor is
  atomically replaced.

## Known gaps

- No CI job exercises a real LLM provider, optional corpus loading, multi-turn
  convergence, or replay from a stored trace. The live-engine contract uses a
  deterministic provider tool call so it proves wiring, not provider language quality.
- The generic artifact validator does not validate generated Design Agent trace
  instances against `schemas/design-agent-trace.schema.json`.
- Retrieval quality and corpus provenance are not release-locked; their mere
  presence on disk is not evidence of scientific validity.
- Some adjacent legacy comments and helpers describe older no-key behavior;
  current behavior is defined by `design_agent.run_turn`, which requires a
  configured key before entering the tool-calling loop.
- No test contacts a live model provider or establishes that natural-language
  requests will reliably choose the intended edit indices. The deterministic
  tool contract proves bounded wiring and fail-closed admission only.
- Turn admission is process-local and does not coordinate multiple helper
  processes. The HTTP server can still create short-lived request threads; the
  bounded contract applies to expensive `run_turn` execution.
- Trace-store serialization is also process-local. Pointing multiple helper
  processes at the same `BNE_TRACE_DIR` is unsupported; retention is a local
  diagnostic policy, not a durable replay archive.

## Change protocol

1. Change the request/response or trace shape together with its schema and both
   Python and browser contract tests.
2. When adding a tool, define its failure type, evidence strength, allowed card
   contribution, and no-fabrication behavior before exposing it to the LLM.
3. Keep retrieval results out of the verified-card path. Add a fresh engine
   recomputation and a contract test before upgrading any evidence label.
4. Re-run `npm run test:py` and `npm run test:js` from `webapp/`; for engine
   payload changes also run the Julia suite.

## Verified against

- Shape-optimization extension: uncommitted working tree on 2026-07-11, with
  Python and repository contract results recorded in the goal completion
  report. No external model provider or remote CI run is claimed.
- Historical source baseline: `f9c65a5`.
- Evidence inspected: owner paths, Python contract tests, browser agent export
  tests, and CI workflow wiring.
- Boundary: source-and-contract verification only; no external provider,
  optional corpus, or live multi-process agent run was treated as verified.
