# Design Agent

`module_id: design-agent`

## Purpose

Provide the optional conversational inverse-design surface. The Python chat
service accepts a natural-language turn, lets a configured LLM call narrowly
scoped tools, sends proposed networks to the live Julia backend, and returns
renderable candidate cards plus an optional rerunnable `DesignabilitySpec`.

## Non-goals

- It is not the numerical or scientific authority; Julia engine responses and
  downstream designability checks own those decisions.
- Atlas lookup, corpus prevalence, retrieval rank, and proxy metrics are priors
  or screening signals, not verified designs.
- It does not certify external LLM quality, provider availability, or the
  completeness/currentness of any optional local corpus.

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
- A configured OpenAI-compatible or Anthropic-compatible LLM endpoint.
- The Julia backend endpoints used by `engine_client.py`.
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
- Best-effort local `traces/traces.jsonl` records and content-addressed card
  artifacts. Trace write failure is intentionally non-fatal, so a `trace_id`
  alone does not prove that a trace file exists.

## Contract sources

- Request/response and health handling: `webapp/scripts/chat_api.py`
- Tool allow-list, card admission, abstention, and trace production:
  `webapp/scripts/design_agent.py`
- Engine error typing and endpoint payloads: `webapp/scripts/engine_client.py`
- Agent-to-`DesignabilitySpec` lowering and rejection rules:
  `webapp/scripts/design_agent.py` and
  `webapp/public/js/node-types/design-target.js`
- Trace fields: `schemas/design-agent-trace.schema.json`

## Tests

- `webapp/scripts/test_design_agent_contract.py` covers provider configuration,
  spec lowering, invalid-spec rejection, solver prerequisites, source identity,
  the exact-design tool contract, strict integer budgets, screen-version and
  curve-validity fail-closed behavior, and prevention of stale or cross-card
  spec attachment.
- `webapp/scripts/test_design_agent_live_engine.py` starts the real Julia HTTP
  server and drives the production Agent dispatch with a deterministic provider
  tool call. It asserts the live Design Screen validation/screening and Placer
  replay path; it does not contact an external model provider.
- `webapp/scripts/test_chat_api.py` covers helper-process lifecycle edge cases.
- `webapp/test/design-spec-node-contract.test.mjs` covers browser-side export
  and rejection of non-rerunnable agent payloads.
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
- Engine connection failures remain typed as `engine_offline`; the agent must
  abstain from inventing curves, labels, or metrics.
- API keys are not written into Design Agent traces, and the HTTP service logs
  request paths rather than request bodies.
- Missing LLM credentials produce the explicit `need_key` response; they do not
  fabricate a deterministic answer or make the Julia workspace unavailable.

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

- Source commit: `f9c65a5`
- Evidence inspected: owner paths, Python contract tests, browser agent export
  tests, and CI workflow wiring.
- Boundary: source-and-contract verification only; no external provider,
  optional corpus, or live multi-process agent run was treated as verified.
