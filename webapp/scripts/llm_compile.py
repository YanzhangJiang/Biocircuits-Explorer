#!/usr/bin/env python3
# LLM-backed NL -> behavior_spec compiler (the production swap for design_chat's rule-based
# first cut). Provider-agnostic: OpenAI-compatible (chat/completions, Bearer key + base_url —
# works with a local reverse proxy) OR Anthropic (/v1/messages, x-api-key). Output is
# schema-validated against behavior-spec.schema.json with a self-correction retry loop, so a
# malformed/invalid spec is fed back to the model rather than crashing the caller. Stdlib only.
#
# Config via env (the UI key field sets these): BNE_LLM_PROVIDER (openai|anthropic),
# BNE_LLM_API_KEY or BNE_LLM_KEY_FILE, BNE_LLM_BASE_URL, BNE_LLM_MODEL.
import os, sys, json, urllib.request, urllib.error
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import validate_artifacts as va

ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), "..", ".."))
SCHEMA_PATH = os.path.join(ROOT, "schemas", "behavior-spec.schema.json")

SYSTEM = """You compile a user's natural-language biocircuit behaviour request into ONE JSON
object — a behavior_spec for an equilibrium protein-binding design tool. Output ONLY the JSON
object: no prose, no markdown fences.

Shape (nested, NL-facing):
{
 "schema_version": "bne-behavior/v0.1.0",
 "goal": {
   "behavior_family": "dose_shape" | "logic" | "analog_surface" | "contextual_versatility",
   // dose_shape (one swept input -> a dose-response CURVE shape):
   "behavior_class": "monotone_activation"|"activation_with_saturation"|"monotone_repression"|
                     "repression_with_floor"|"thresholded_activation"|"biphasic_peak"|
                     "bandpass_with_plateau"|"biphasic_valley"|"window_repression",
   "input_axis": "log10_total_input",
   // multi-input (logic / analog):
   "arity": "two_input", "inputs": ["tA","tB"], "output_semantics": "booleanized",
   "logic": {"logic_type": "AND"|"OR"|"NAND"|"NOR"|"XOR"|"XNOR"|"NIMPLY"|"IMPLY"|"NOT_A"|"NOT_B"},
   "analog": {"target": "ratio_sensor"|"equality_detector"|"two_input_bump"|"diagonal_ridge"|"synergy"|"antagonism"},
   "output": {"kind":"species","symbol":"A"}
 },
 "shape_preferences": {  // dose_shape only; each term {preference, and min|max|range}
   "rise_slope":{"preference":"gentle","max":0.6}, "fall_slope":{"preference":"sharp","min":1.0},
   "plateau_width_log10_input":{"preference":"wide","min":0.5}, "peak_prominence":{"min":0.2},
   "baseline_return":{"max":0.3}, "dynamic_range_log10":{"range":[2.0,3.0]}
 },
 "network_constraints": {"max_reactions": 4, "max_base_species": 3, "kd_profile": {"mode":"mostly_weak"}},
 "verification_policy": {"min_robustness_score": 0.5}
}

Rules:
- Exactly one behavior_family. dose_shape = a 1-input curve shape; logic = a 2-input Boolean gate;
  analog_surface = a 2-input CONTINUOUS response surface; contextual_versatility = one network
  reprogrammed by an accessory-expression context.
- bandpass / band-pass / "narrow high band with a cutoff" / "hump that rises slowly then drops" =
  bandpass_with_plateau. A peak with NO plateau constraint = biphasic_peak. "saturating activation" /
  "rise to a broad high plateau, no peak" = activation_with_saturation. ultrasensitive / threshold =
  thresholded_activation. "repressed to a floor" = repression_with_floor.
- "high only when BOTH A and B" = logic AND; "either" = OR; "exclusive / one but not both" = XOR;
  "A unless B" = NIMPLY. "responds to the A/B ratio" = analog ratio_sensor; "high when A≈B" =
  equality_detector; "high only in a middle window of both" = two_input_bump.
- Map words to gates: "wide plateau"->plateau_width_log10_input.min; "sharp fall/cutoff"->fall_slope.min;
  "gentle/slow rise"->rise_slope.max; "robust"->verification_policy.min_robustness_score>=0.5;
  "at most N reactions"->network_constraints.max_reactions=N; "weak Kd"->kd_profile.mode="mostly_weak";
  "fold-change between X and Y"->dynamic_range_log10.range=[X,Y].
- REFINEMENT: if given a current spec, return it with ONLY the requested change applied.
- Out of scope (temporal/oscillation/memory/noise/spatial/gene circuits): pick the closest family and
  add "compilation_notes": ["<phrase> is out of scope for the equilibrium-binding backend"].
"""

def _call_openai(user, api_key, base_url, model):
    body = json.dumps({"model": model,
                       "messages": [{"role": "system", "content": SYSTEM}, {"role": "user", "content": user}],
                       "temperature": 0}).encode()
    req = urllib.request.Request(base_url.rstrip("/") + "/chat/completions", data=body,
                                 headers={"Authorization": "Bearer " + api_key, "Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=90) as r:
        return json.load(r)["choices"][0]["message"]["content"]

def _call_anthropic(user, api_key, base_url, model):
    body = json.dumps({"model": model, "max_tokens": 1024, "system": SYSTEM,
                       "messages": [{"role": "user", "content": user}]}).encode()
    req = urllib.request.Request((base_url or "https://api.anthropic.com").rstrip("/") + "/v1/messages", data=body,
                                 headers={"x-api-key": api_key, "anthropic-version": "2023-06-01", "Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=90) as r:
        return "".join(b.get("text", "") for b in json.load(r)["content"])

# ── tool-calling chat (for the design AGENT, design_agent.py) ────────────────────
# Thin provider HTTP helpers; the agent loop (message accumulation, tool dispatch) lives
# in design_agent.py. OpenAI returns the native assistant message; Anthropic the full body.
def _urlopen_json_retry(req, timeout, tries=6):
    """POST with retry on transient upstream failures (429/5xx, connection resets). Rate-limited
    relays (e.g. shared anyrouter keys) 429 readily, and one agent turn fires several LLM calls, so
    we honour Retry-After and back off harder on 429/503. Re-raises the last error if it never clears."""
    import time
    last = None
    for i in range(tries):
        try:
            with urllib.request.urlopen(req, timeout=timeout) as r:
                raw = r.read()
                enc = (r.headers.get("Content-Encoding") or "").lower()
                if "gzip" in enc or raw[:2] == b"\x1f\x8b":          # some providers gzip regardless
                    import gzip; raw = gzip.decompress(raw)
                elif "deflate" in enc:
                    import zlib; raw = zlib.decompress(raw)
                ctype = r.headers.get("Content-Type") or ""
                text = raw.decode("utf-8", "replace").strip()
                if "text/event-stream" in ctype or text.startswith("data:"):
                    raise RuntimeError("endpoint returned a streaming (SSE) response but this client expects a "
                                       "single JSON object — use DeepSeek's OpenAI-compatible endpoint "
                                       "(deepseek-chat), which returns JSON, or turn streaming off "
                                       f"(content-type {ctype!r})")
                if not text:
                    raise RuntimeError(f"empty response body (content-type {ctype!r}) — check the Base URL / model")
                try:
                    return json.loads(text)
                except json.JSONDecodeError:
                    raise RuntimeError(f"non-JSON response (content-type {ctype!r}): {text[:240]}")
        except urllib.error.HTTPError as e:
            last = e
            if e.code in (408, 429, 500, 502, 503, 504) and i < tries - 1:
                ra = e.headers.get("Retry-After") if getattr(e, "headers", None) else None
                try:
                    wait = float(ra)                                   # server told us how long
                except (TypeError, ValueError):
                    wait = (2.0 + 2.0 * i) if e.code in (429, 503) else (1.0 + 1.5 * i)
                time.sleep(min(wait, 12)); continue
            raise
        except (urllib.error.URLError, ConnectionError, OSError, TimeoutError) as e:
            last = e
            if i < tries - 1:
                time.sleep(1.0 + 1.5 * i); continue
            raise
    raise last

def openai_chat_tools(messages, tools, *, api_key, base_url, model, timeout=120, effort=None, **_):
    if not base_url:
        raise RuntimeError("openai provider needs a base_url (your proxy's /v1)")
    payload = {"model": model, "messages": messages, "tools": tools, "tool_choice": "auto", "temperature": 0}
    if effort:                                  # minimal|low|medium|high — reasoning models honour it
        payload["reasoning_effort"] = effort
    req = urllib.request.Request(base_url.rstrip("/") + "/chat/completions", data=json.dumps(payload).encode(),
                                 headers={"Authorization": "Bearer " + api_key, "Content-Type": "application/json",
                                          "Accept-Encoding": "identity", "User-Agent": "Mozilla/5.0 (compatible; BiocircuitsExplorer/1.0)"})
    return _urlopen_json_retry(req, timeout)["choices"][0]["message"]

# effort → Anthropic extended-thinking token budget (Anthropic has no reasoning_effort enum; this is a
# thinking-budget scale, low…max ≈ ultrathink). OpenAI-compatible providers get reasoning_effort verbatim
# (GPT-5.x/codex accept low|medium|high|xhigh).
_ANTHROPIC_THINK = {"low": 2048, "medium": 8192, "high": 16384, "max": 32768, "xhigh": 32768}
def anthropic_chat_tools(system, messages, tools, *, api_key, base_url, model, timeout=120, effort=None, **_):
    payload = {"model": model, "max_tokens": 2048, "system": system, "messages": messages, "tools": tools}
    budget = _ANTHROPIC_THINK.get(effort or "")
    if budget:                                  # max_tokens must exceed the thinking budget
        payload["thinking"] = {"type": "enabled", "budget_tokens": budget}
        payload["max_tokens"] = budget + 4096
    req = urllib.request.Request((base_url or "https://api.anthropic.com").rstrip("/") + "/v1/messages", data=json.dumps(payload).encode(),
                                 headers={"x-api-key": api_key, "anthropic-version": "2023-06-01", "Content-Type": "application/json",
                                          "Accept-Encoding": "identity", "User-Agent": "Mozilla/5.0 (compatible; BiocircuitsExplorer/1.0)"})
    return _urlopen_json_retry(req, timeout)

def _extract_json(text):
    text = text.strip()
    if "```" in text:
        seg = text.split("```")[1]
        text = seg[4:] if seg.startswith("json") else seg
    i, j = text.find("{"), text.rfind("}")
    if i < 0 or j < 0:
        raise ValueError("no JSON object in model output")
    return json.loads(text[i:j + 1])

def llm_config_from_env():
    prov = os.environ.get("BNE_LLM_PROVIDER", "openai")
    key = os.environ.get("BNE_LLM_API_KEY")
    kf = os.environ.get("BNE_LLM_KEY_FILE")
    if not key and kf and os.path.isfile(kf):
        key = open(kf).read().strip()
    return {"provider": prov, "api_key": key, "base_url": os.environ.get("BNE_LLM_BASE_URL"),
            "model": os.environ.get("BNE_LLM_MODEL", "gpt-5.4-mini" if prov == "openai" else "claude-sonnet-4-6")}

def compile_with_llm(nl, prior=None, *, provider="openai", api_key=None, base_url=None,
                     model="gpt-5.4-mini", max_retries=2):
    if not api_key:
        raise RuntimeError("no LLM api_key configured")
    schema = json.load(open(SCHEMA_PATH))
    user = f"Request: {nl}"
    if prior:
        user += "\n\nThis is a REFINEMENT of the current spec; edit it minimally:\n" + json.dumps(prior)
    last = None
    for _ in range(max_retries + 1):
        raw = (_call_anthropic if provider == "anthropic" else _call_openai)(user, api_key, base_url, model)
        try:
            spec = _extract_json(raw)
            errs = []
            va.check(spec, schema, schema, "behavior_spec", errs)
            if not errs:
                return spec
            last = "; ".join(errs[:4])
        except Exception as e:
            last = str(e)
        user += f"\n\nThe previous output was rejected ({last}). Output ONLY a corrected JSON behavior_spec."
    raise RuntimeError(f"LLM compile failed after {max_retries + 1} tries: {last}")

if __name__ == "__main__":  # quick CLI: compile one NL string from argv, using env config
    cfg = llm_config_from_env()
    print(json.dumps(compile_with_llm(" ".join(sys.argv[1:]), **cfg), indent=2))
