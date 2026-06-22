"""nl_target_compiler.py — Step 2: compile a natural-language behaviour request into a
TARGET OBJECT spec (not just a label).

Output (bne-target-spec/v0.1.0) ALWAYS carries: intent_type (existential/typical/robust),
target_object_type (reference_curve/constraint_curve/refinement_delta), a renderable
target, plus constraints / preferences / ambiguities / unsupported_parts. It MUST NOT
reduce to a bare label. LLM path (provider-agnostic, BNE_LLM_* env) with a deterministic
rule-based fallback; `render_target` turns the spec into a Reader target object.
"""
from __future__ import annotations
import json
import os
import sys
import numpy as np

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from compare import ReferenceCurveTarget, ConstraintCurveTarget
from reader import prototypes

SPEC_SCHEMA = "bne-target-spec/v0.1.0"
VOCAB = ["monotone_activation", "monotone_repression", "thresholded_activation",
         "biphasic_peak", "bandpass_with_plateau", "biphasic_valley", "multimodal"]

# label → control points (u in [-6,6], level 0..1) for reference_curve rendering
_CTRL = {
    "monotone_activation": [[-6, 0], [6, 1]], "monotone_repression": [[-6, 1], [6, 0]],
    "thresholded_activation": [[-6, 0], [1, 0], [3, 1], [6, 1]],
    "biphasic_peak": [[-6, 0], [0, 1], [6, 0]],
    "bandpass_with_plateau": [[-6, 0], [-2.5, 1], [2.5, 1], [6, 0]],
    "biphasic_valley": [[-6, 1], [0, 0], [6, 1]], "multimodal": [[-6, 0], [-2, 1], [0, 0.2], [2, 1], [6, 0]],
}
_LABEL_PROTO = {"biphasic_peak": "bump", "bandpass_with_plateau": "broad_bump", "biphasic_valley": "valley",
                "monotone_activation": "switch_on", "monotone_repression": "switch_off",
                "thresholded_activation": "shoulder", "multimodal": "double_bump"}

SYSTEM = (
    "You compile a natural-language protein-circuit DOSE-RESPONSE behaviour request into a JSON "
    "target-spec. The input axis is log10 total input (−6..6); output level is normalized 0..1. "
    "Return ONLY JSON with: schema_version='bne-target-spec/v0.1.0'; intent_type one of "
    "existential (find ANY realisation / 'is it possible' / discovery), typical (the usual behaviour), "
    "robust (a reliable/reproducible realisation); target_object_type one of reference_curve (give "
    "reference_curve.control_points=[[u,level],...]), constraint_curve (give constraint_curve.regions="
    "[{u:[lo,hi],want:low|high|mid|rise|fall,weight}]), refinement_delta (give refinement_delta.{base_label,"
    "delta in longer_plateau|steeper_fall|higher_peak|narrower|wider|shift_right|shift_left}); a "
    "behavior_label_hint from " + str(VOCAB) + " (a HINT, never the only output); constraints.{max_reactions,"
    "min_shape_support}; preferences (e.g. weak_kd, small_network); ambiguities (what is underspecified); "
    "unsupported_parts (anything outside equilibrium single-input dose-response, e.g. time-domain "
    "oscillation, multi-input/contextual, stochastic, spatial). Never output only a label.")


# ── rule-based fallback ────────────────────────────────────────────────────────
def _label_from_text(s):
    if any(k in s for k in ["notch", "valley", "dip in the middle", "window low"]):
        return "biphasic_valley"
    if any(k in s for k in ["bandpass", "broad", "plateau", "wide high", "broad high"]):
        return "bandpass_with_plateau"
    if any(k in s for k in ["bump", "peak", "hump", "biphasic", "rise then fall", "up then down"]):
        return "biphasic_peak"
    if any(k in s for k in ["threshold", "shoulder", "flat then", "switch-like", "ultrasensit"]):
        return "thresholded_activation"
    if any(k in s for k in ["oscillat", "multimodal", "back and forth", "来回", "先增后减再增", "wiggl", "two peaks", "double"]):
        return "multimodal"
    if any(k in s for k in ["repress", "decrease", "switch off", "falls", "goes down", "inhibit"]):
        return "monotone_repression"
    return "monotone_activation"


def _rule_compile(nl):
    s = nl.lower()
    # rare / out-of-vocab shape descriptions imply DISCOVERY (find ANY realisation), even without "find any"
    rare = any(k in s for k in ["two peak", "three peak", "separate peak", "multiple peak", "different height",
                                "wiggl", "spike", "oscillat", "back and forth", "two separate", "multimodal", "peaks of"])
    intent = ("existential" if (rare or any(k in s for k in ["find any", "is it possible", "can a", "can any", "does any", "exists", "at least one", "discover"]))
              else "robust" if any(k in s for k in ["robust", "reliable", "reproducible", "consistent", "always", "most networks"])
              else "typical")
    label = _label_from_text(s)
    delta = next((d for d, ks in {"longer_plateau": ["longer plateau", "wider plateau", "broader top"],
                                  "steeper_fall": ["steeper fall", "sharper fall", "sharper drop"],
                                  "higher_peak": ["higher peak", "taller", "stronger peak"],
                                  "narrower": ["narrower", "sharper peak", "thinner"],
                                  "wider": ["wider", "broader"], "shift_right": ["shift right", "toward high", "later"],
                                  "shift_left": ["shift left", "toward low", "earlier"]}.items()
                 if any(k in s for k in ks)), None)
    if delta or " than " in s or "more " in s or "less " in s:
        tot = "refinement_delta"; spec = {"refinement_delta": {"base_label": label, "delta": delta or "narrower"}}
    elif any(k in s for k in ["low", "high", "then", "window", "region", "broad", "shoulder", "back to"]) and any(k in s for k in ["then", "again", "window", "region", "middle"]):
        tot = "constraint_curve"; spec = {"constraint_curve": {"regions": _regions_from_label(label)}}
    else:
        tot = "reference_curve"; spec = {"reference_curve": {"control_points": _CTRL.get(label, _CTRL["biphasic_peak"])}}
    prefs = [p for p, ks in {"weak_kd": ["weak kd", "weak binding", "low affinity"],
                             "small_network": ["small network", "few reactions", "fewer reactions", "simple"]}.items()
             if any(k in s for k in ks)]
    unsup = []
    if any(k in s for k in ["time", "kinetic", "temporal", "oscillation over time", "dynamics"]):
        unsup.append("time-domain / kinetics — out of equilibrium scope")
    if any(k in s for k in ["two input", "2-input", "two-input", "context", "accessory", "cell type", "and gate", "logic"]):
        unsup.append("multi-input / contextual — single-input dose-response only here")
    amb = []
    if label == "biphasic_peak" and "plateau" not in s and "broad" not in s:
        amb.append("biphasic_peak vs bandpass_with_plateau — plateau width unstated")
    return {"schema_version": SPEC_SCHEMA, "intent_type": intent, "target_object_type": tot,
            "behavior_label_hint": label, "constraints": {"max_reactions": _max_rx(s), "min_shape_support": None},
            "preferences": prefs, "ambiguities": amb, "unsupported_parts": unsup, **spec}


def _regions_from_label(label):
    if label == "biphasic_valley":
        return [{"u": [-6, -2.5], "want": "high", "weight": 1}, {"u": [-1, 1], "want": "low", "weight": 1.5}, {"u": [2.5, 6], "want": "high", "weight": 1}]
    return [{"u": [-6, -2.5], "want": "low", "weight": 1}, {"u": [-2.5, -0.5], "want": "rise", "weight": 1},
            {"u": [-0.5, 0.5], "want": "high", "weight": 1.5}, {"u": [0.5, 2.5], "want": "fall", "weight": 1.5},
            {"u": [2.5, 6], "want": "low", "weight": 1}]


def _max_rx(s):
    import re
    m = re.search(r"(\d+)\s*(?:reaction|rxn|step)", s)
    return int(m.group(1)) if m else None


# ── LLM path (provider-agnostic; falls back to rules on any error) ─────────────
def _llm_compile(nl, cfg):
    import urllib.request
    key = cfg.get("api_key");
    if not key:
        return None
    prov = cfg.get("provider", "openai"); base = cfg.get("base_url"); model = cfg.get("model")
    if prov == "anthropic":
        url = (base or "https://api.anthropic.com") + "/v1/messages"
        body = {"model": model, "max_tokens": 1024, "system": SYSTEM, "messages": [{"role": "user", "content": nl}]}
        headers = {"x-api-key": key, "anthropic-version": "2023-06-01", "content-type": "application/json"}
    else:
        url = (base or "https://api.openai.com/v1").rstrip("/") + "/chat/completions"
        body = {"model": model, "temperature": 0, "messages": [{"role": "system", "content": SYSTEM}, {"role": "user", "content": nl}]}
        headers = {"Authorization": "Bearer " + key, "content-type": "application/json"}
    req = urllib.request.Request(url, data=json.dumps(body).encode(), headers=headers)
    r = json.loads(urllib.request.urlopen(req, timeout=60).read())
    text = r["content"][0]["text"] if prov == "anthropic" else r["choices"][0]["message"]["content"]
    try:
        from llm_compile import _extract_json
        spec = _extract_json(text)
    except Exception:
        spec = json.loads(text[text.find("{"):text.rfind("}") + 1])
    spec.setdefault("schema_version", SPEC_SCHEMA)
    return spec


def compile_target(nl, llm_cfg=None):
    """NL → target-spec dict. Tries the LLM (if configured), else rule-based; always a full spec."""
    if llm_cfg and llm_cfg.get("api_key"):
        try:
            spec = _llm_compile(nl, llm_cfg)
            if spec and spec.get("intent_type") and spec.get("target_object_type"):
                spec["_source"] = "llm"
                return spec
        except Exception as e:
            sys.stderr.write(f"[nl_target_compiler] LLM failed ({e}); rule-based fallback\n")
    spec = _rule_compile(nl); spec["_source"] = "rule"
    return spec


# ── render the spec into a Reader target object ────────────────────────────────
def _curve_from_label(label, u):
    cps = _CTRL.get(label)
    if cps:
        return np.interp(u, [p[0] for p in cps], [p[1] for p in cps])
    proto = prototypes(u).get(_LABEL_PROTO.get(label, "bump"))
    return proto[0] if proto else prototypes(u)["bump"][0]


def _delta_curve(base, delta, u):
    """Encode a refinement delta INTO the target curve (so it drives retrieval, not just the base)."""
    cps = [list(p) for p in _CTRL.get(base, _CTRL["biphasic_peak"])]
    us = [p[0] for p in cps]; ctr = sum(us) / len(us)
    if delta == "shift_right":
        cps = [[p[0] + 1.8, p[1]] for p in cps]
    elif delta == "shift_left":
        cps = [[p[0] - 1.8, p[1]] for p in cps]
    elif delta == "narrower":
        cps = [[ctr + (p[0] - ctr) * 0.55, p[1]] for p in cps]
    elif delta in ("wider", "longer_plateau"):
        cps = [[ctr + (p[0] - ctr) * 1.5, p[1]] for p in cps]
    elif delta == "steeper_fall":
        cps = [[p[0] if p[0] <= ctr else ctr + (p[0] - ctr) * 0.5, p[1]] for p in cps]
    # higher_peak: curve already normalised 0..1 — no geometry change
    return np.interp(u, [p[0] for p in cps], [p[1] for p in cps])


def render_target(spec, u):
    """→ (target_object, intent_type, behavior_label_hint, max_reactions). Used by reader_panel."""
    tot = spec["target_object_type"]; intent = spec["intent_type"]; label = spec.get("behavior_label_hint")
    mx = (spec.get("constraints") or {}).get("max_reactions")
    if tot == "constraint_curve" and (spec.get("constraint_curve") or {}).get("regions"):
        regs = [{"u": (r["u"][0], r["u"][1]), "want": r["want"], "w": r.get("weight", 1.0)}
                for r in spec["constraint_curve"]["regions"]]
        return ConstraintCurveTarget(regs), intent, label, mx
    if tot == "refinement_delta":
        rd_ = spec.get("refinement_delta") or {}
        base = rd_.get("base_label") or label or "biphasic_peak"
        return ReferenceCurveTarget(_delta_curve(base, rd_.get("delta"), u)), intent, label, mx
    rc = (spec.get("reference_curve") or {}).get("control_points")
    if rc and len(rc) >= 2:
        curve = np.interp(u, [p[0] for p in rc], [p[1] for p in rc])
    else:
        curve = _curve_from_label(label or "biphasic_peak", u)
    return ReferenceCurveTarget(curve), intent, label, mx


if __name__ == "__main__":
    cases = [
        "I want a response that's low, then a broad high window, then low again with a sharper fall than rise",
        "find any network that can produce a notch — high, dips in the middle, high again",
        "give me a reliable monotone activation with few reactions",
        "like a bump but with a longer plateau",
        "a switch-on but make it oscillate over time",
        "an AND gate on two inputs",
    ]
    u = np.linspace(-6, 6, 64)
    for nl in cases:
        spec = compile_target(nl)
        tgt, intent, label, mx = render_target(spec, u)
        print(f"\nNL: {nl}")
        print(f"  intent={spec['intent_type']} type={spec['target_object_type']} label_hint={spec['behavior_label_hint']} "
              f"max_rx={mx} src={spec['_source']}")
        print(f"  prefs={spec['preferences']} ambig={spec['ambiguities']} unsupported={spec['unsupported_parts']}")
        print(f"  rendered → {type(tgt).__name__}")
