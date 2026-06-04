#!/usr/bin/env python3
# Minimal usable closed loop: natural-language MULTI-TURN inverse design over the labelled
# corpora. Each turn: NL -> behavior_spec (compile or refine the held spec) -> lower ->
# retrieve candidates -> render cards (+ relaxation if nothing passes). Holds spec state
# across turns so "make the plateau wider" / "relax to 6 reactions" refine the prior request.
#
# The NL->spec compiler here is a RULE-BASED first cut (keyword/phrase) so the loop is
# self-testable without an API; the production path swaps compile_nl() for an LLM call that
# emits the same nested behavior_spec (validated by validate_artifacts.py). Dose-shape and
# logic families are wired (their label corpora are ready); analog/contextual slot in the
# same way once their corpora land.
#
#   echo -e "bandpass with a wide plateau, <=4 reactions\nmake the plateau wider\nrelax to 5 reactions" \
#     | python3 webapp/scripts/design_chat.py            # REPL reads stdin
#   python3 webapp/scripts/design_chat.py --script conversation.txt
import sys, os, json, argparse, re
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import design_search as ds
import cards as C

ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), "..", ".."))
DOSE_DS = os.path.join(ROOT, "datasets", "latent-atlas-v0")
LOGIC_LABELS = os.path.join(ROOT, "datasets", "latent-atlas-logic-v0", "logic_labels.jsonl")
ANALOG_LABELS = os.path.join(ROOT, "datasets", "latent-atlas-analog-v0", "analog_labels.jsonl")
CONTEXTUAL_LABELS = os.path.join(ROOT, "datasets", "latent-atlas-contextual-v0", "contextual_labels.jsonl")

def _dose_class(t):
    """Priority-ordered dose behavior_class classifier over general shape vocabulary.
    First-cut rule-based; the production compiler is an LLM emitting the same field."""
    if "ultrasensit" in t or "threshold" in t: return "thresholded_activation"
    if "biphasic" in t: return "biphasic_peak"
    if "bandpass" in t or "band-pass" in t or "band pass" in t: return "bandpass_with_plateau"
    if "plateau" in t and any(w in t for w in ("fall", "cutoff", "cut off", "cuts off", "crash", "drop")):
        return "bandpass_with_plateau"             # a plateau that then falls = bandpass
    if any(w in t for w in ("cuts off", "cut off", "cutoff", "hump", "bump", "middle", "notch", "crash")):
        return "biphasic_peak"
    if "floor" in t: return "repression_with_floor"
    if "repress" in t: return "monotone_repression"
    if "saturat" in t or "broad high" in t or "plateau" in t: return "activation_with_saturation"
    if "activation" in t or "rises" in t or "monotone" in t: return "monotone_activation"
    return "bandpass_with_plateau"                 # default
# Explicit gate names as WHOLE WORDS (so "for"/"android"/"nand" don't match "or"/"and"),
# specific gates (XNOR/NAND/NOR/XOR/NIMPLY/IMPLY) before bare AND/OR so an explicit gate
# wins over the " and "/" or " in an operand list — "XOR gate on A and B" -> XOR, not AND.
_GATE_RE = re.compile(r"\b(xnor|nand|nimply|imply|xor|nor|and|or)\b")
_GATE_CANON = {"xnor": "XNOR", "nand": "NAND", "nor": "NOR", "xor": "XOR",
               "nimply": "NIMPLY", "imply": "IMPLY", "and": "AND", "or": "OR"}

def _detect_gates(t):
    """Logic gates named in t, in positional (left-to-right) order, de-duplicated.
    The leftmost explicit gate word wins, so the connective in an operand list no
    longer shadows it: 'XOR gate on A and B' -> [XOR]; 'AND here, OR there' -> [AND, OR]."""
    t = t.lower()
    out = []
    def add(g):
        if g not in out: out.append(g)
    for m in _GATE_RE.finditer(t):
        add(_GATE_CANON[m.group(1)])
    if "exclusive" in t: add("XOR")
    if "coincidence" in t or "both" in t: add("AND")
    if "either" in t: add("OR")
    if "veto" in t or "unless" in t: add("NIMPLY")
    return out

def _detect_gate(t):
    """The single intended gate (first named, else AND)."""
    g = _detect_gates(t)
    return g[0] if g else "AND"

def _int_after(t, *kw):
    import re
    for k in kw:
        m = re.search(rf"(\d+)\s*{k}", t) or re.search(rf"{k}\D*(\d+)", t)
        if m: return int(m.group(1))
    return None

_LLM = None
def _llm_cfg():
    global _LLM
    if _LLM is None:
        try:
            import llm_compile
            c = llm_compile.llm_config_from_env()
            _LLM = c if c.get("api_key") else False
        except Exception:
            _LLM = False
    return _LLM or None

def compile_nl(text, prior=None, llm_cfg=None):
    """NL -> nested behavior_spec. Uses the LLM compiler when configured — either a per-call
    llm_cfg (snake_case {provider,api_key,base_url,model}, e.g. from the UI key panel via the
    HTTP endpoint) or env BNE_LLM_* — falling back to the rule-based compiler on any error."""
    cfg = llm_cfg if (llm_cfg and llm_cfg.get("api_key")) else _llm_cfg()
    if cfg:
        try:
            import llm_compile
            spec = llm_compile.compile_with_llm(text, prior, **cfg)
            g = spec.get("goal", {})
            tag = (g.get("behavior_class") or (g.get("logic") or {}).get("logic_type")
                   or (g.get("analog") or {}).get("target") or g.get("behavior_family"))
            return spec, f"llm {g.get('behavior_family')}:{tag}"
        except Exception as e:
            spec, note = _compile_rule(text, prior)
            return spec, f"{note}  [LLM unavailable: {str(e)[:70]}; used rule-based]"
    return _compile_rule(text, prior)

def _compile_rule(text, prior=None):
    """Rule-based first-cut compiler. If a prior spec exists and the text reads as a
    refinement, edit it; else compile a fresh spec. Returns (spec, note)."""
    import re
    t = " " + re.sub(r"\[[^\]]*\]", " ", text.lower()).strip() + " "   # drop bracketed editorial notes
    refine_words = ("wider", "narrower", "sharper", "gentler", "relax", "loosen", "tighten",
                    "broaden", "more robust", "make it", "now ", "instead", "also ")
    if prior and any(w in t for w in refine_words):
        return _refine(prior, t)
    # Route to logic only on a STRONG logic signal — never a bare "and"/"or" (those are
    # all over dose descriptions, e.g. "gentle rise and sharp fall"). logic_type is then
    # disambiguated inside _fresh_logic (where "both A and B" -> AND is safe).
    ctx_triggers = ("reprogram", "context", "cell type", "cell-type", "expression context",
                    "versatil", "different function", "different gate", "depending on", "accessory")
    if any(p in t for p in ctx_triggers):
        return _fresh_contextual(t)
    analog_triggers = ("ratio sensor", "ratio-sensing", "ratiometric", "ratio ", "response surface",
                       "analog", "two-input bump", "two input bump", "equality", "diagonal ridge",
                       "synergy", "antagonism")
    if any(p in t for p in analog_triggers):
        return _fresh_analog(t)
    logic_triggers = ("gate", "logic", "nand", "nor", "xor", "veto", "coincidence",
                      "exclusive", "truth table", "both ", "present", "imply")
    if any(p in t for p in logic_triggers) or ("either" in t and " or " in t):
        return _fresh_logic(t)
    return _fresh_dose(t)

def _fresh_dose(t):
    cls = _dose_class(t)
    sp = {}
    if "wide" in t and "plateau" in t: sp["plateau_width_log10_input"] = {"preference": "wide", "min": 0.5}
    if "sharp" in t and ("fall" in t or "cutoff" in t): sp["fall_slope"] = {"preference": "sharp", "min": 1.0}
    if "gentle" in t and "rise" in t: sp["rise_slope"] = {"preference": "gentle", "max": 0.6}
    goal = {"behavior_family": "dose_shape", "behavior_class": cls,
            "input_axis": "log10_total_input", "output": {"kind": "species", "symbol": "A"}}
    nc = {}
    mr = _int_after(t, "reactions?", "reaction")
    if mr: nc["max_reactions"] = mr
    vp = {"min_robustness_score": 0.5 if "robust" in t else 0.2}
    spec = {"schema_version": "bne-behavior/v0.1.0", "goal": goal,
            "shape_preferences": sp, "network_constraints": nc, "verification_policy": vp}
    return spec, f"dose_shape / {cls}" + (f", <= {mr} reactions" if mr else "")

def _fresh_logic(t):
    lt = _detect_gate(t)
    goal = {"behavior_family": "logic", "arity": "two_input", "input_domain": "dose_grid",
            "inputs": ["tA", "tB"], "output": {"kind": "species", "symbol": "C_A_B"},
            "output_semantics": "booleanized", "logic": {"logic_type": lt}}
    vp = {"min_robustness_score": 0.5}
    spec = {"schema_version": "bne-behavior/v0.1.0", "goal": goal, "verification_policy": vp}
    return spec, f"logic / {lt} gate"

def _fresh_analog(t):
    tgt = "ratio_sensor"
    if "bump" in t: tgt = "two_input_bump"
    elif "equality" in t: tgt = "equality_detector"
    elif "synergy" in t: tgt = "synergy"
    elif "antagonism" in t: tgt = "antagonism"
    elif "diagonal" in t: tgt = "diagonal_ridge"
    goal = {"behavior_family": "analog_surface", "arity": "two_input", "input_domain": "dose_grid",
            "inputs": ["tA", "tB"], "output": {"kind": "species", "symbol": "C_A_B"},
            "analog": {"target": tgt}}
    return {"schema_version": "bne-behavior/v0.1.0", "goal": goal, "verification_policy": {}}, f"analog_surface / {tgt}"

def _fresh_contextual(t):
    # detect the two target gates if phrased like "AND in one context, NOR in another"
    gates = _detect_gates(t)
    goal = {"behavior_family": "contextual_versatility", "arity": "context_indexed",
            "inputs": ["tA", "tB"], "context_sym": "tC",
            "output": {"kind": "species", "symbol": "C_A_B"},
            "targets": list(dict.fromkeys(gates))[:2]}
    return {"schema_version": "bne-behavior/v0.1.0", "goal": goal, "verification_policy": {}}, \
           f"contextual_versatility (reprogrammable network; targets={goal['targets'] or 'any'})"

def _refine(prior, t):
    import re
    spec = json.loads(json.dumps(prior)); goal = spec["goal"]; note = []
    if goal.get("behavior_family") == "dose_shape":
        sp = spec.setdefault("shape_preferences", {}); nc = spec.setdefault("network_constraints", {})
        if "wider" in t:
            pw = sp.setdefault("plateau_width_log10_input", {"preference": "wide"})
            pw["min"] = round(pw.get("min", 0.5) + 0.3, 3); note.append(f"plateau wider (min {pw['min']})")
        if "narrower" in t:
            pw = sp.setdefault("plateau_width_log10_input", {}); pw["min"] = round(max(0.0, pw.get("min", 0.5) - 0.3), 3); note.append("plateau narrower")
        if "sharper" in t:
            fs = sp.setdefault("fall_slope", {"preference": "sharp"}); fs["min"] = round(fs.get("min", 1.0) + 0.5, 3); note.append(f"fall sharper (min {fs['min']})")
        mr = _int_after(t, "reactions?", "reaction")
        if mr: nc["max_reactions"] = mr; note.append(f"<= {mr} reactions")
        if "relax" in t or "loosen" in t:
            for term in ("fall_slope", "plateau_width_log10_input", "rise_slope"):
                if term in sp:
                    d = sp[term]
                    if "min" in d: d["min"] = round(d["min"] * 0.7, 3)
                    if "max" in d: d["max"] = round(d["max"] * 1.3, 3)
            note.append("gates relaxed 30%")
    if "more robust" in t or ("robust" in t and "reaction" not in t):
        vp = spec.setdefault("verification_policy", {}); vp["min_robustness_score"] = round(min(0.95, vp.get("min_robustness_score", 0.5) + 0.2), 3)
        note.append(f"min_robustness {vp['min_robustness_score']}")
    return spec, "refine: " + ("; ".join(note) if note else "(no-op)")

# ---- per-family search ----
def _search_dose(spec, top=3):
    flat = ds.lower_spec(spec); cls = ds.CLASS_MAP.get(flat.get("behavior_class"))
    flat.setdefault("min_robustness", 0.2); flat.setdefault("max_reactions", 99)
    pool = [r for r in ds.iter_rows(DOSE_DS) if r.get("n_reactions", 99) <= flat["max_reactions"]]
    scored = [(ss, len(rs), r, rs) for r in pool for (ss, rs) in [ds.eval_row(r, flat, cls)]]
    scored.sort(key=lambda x: (-x[0], x[1]))
    passing = [s for s in scored if s[1] == 0]
    out = [f"# {len(passing)} hard-pass / {len(pool)} in-scope (class={cls})"]
    for ss, nm, r, rs in (passing[:top] or scored[:top]):
        c = ds.build_card(ss, nm, r, rs); mm = c["metrics_median"]
        out.append(f"  [{'PASS' if nm==0 else 'near'}] ss={c['shape_support']:.2f} r={c['n_reactions']} "
                   f"out={c['output_symbol']} rise={mm['rise_slope']} fall={mm['fall_slope']} plateau={mm['plateau_width_log10_input']}  {c['network_id'][:48]}…")
    if not passing:
        relax, absent = ds.compute_relaxation(flat, scored)
        for s in relax: out.append(f"  ↪ relax: {s['direction']} {s['gate']} {s['from']}→{s['to']} admits {s['admits']}")
        if absent: out.append("  ⚠ ABSTAIN: this shape is essentially absent in μ≤5 — none of the corpus realises it "
                              "robustly. Relax a gate, or pick a different behavior (this may need an out-of-scope backend).")
    return "\n".join(out)

def _search_logic(spec, top=3):
    lt = spec["goal"]["logic"]["logic_type"]
    if not os.path.isfile(LOGIC_LABELS):
        return f"# (logic corpus not present yet: {LOGIC_LABELS})"
    rows = [json.loads(l) for l in open(LOGIC_LABELS) if l.strip()]
    hits = [r for r in rows if r.get("realized_gate") == lt]
    hits.sort(key=lambda r: (-(r.get("gate_support") or 0), -(r.get("median_margin_decades") or 0)))
    out = [f"# {len(hits)} networks realise {lt} / {len(rows)} logic slices"]
    if not hits:
        out.append(f"  ⚠ ABSTAIN: {lt} is essentially not realisable in the μ≤5 equilibrium family "
                   f"(it needs engineered antagonistic competition). Options: accept a near gate, or defer to a future backend.")
        return "\n".join(out)
    best = hits[0].get("gate_support") or 0
    if best < 0.6:
        out.append(f"  ⚠ weak: best gate_support is only {best} (<0.6) — {lt} is marginal in this family; "
                   f"out-of-scope unless you accept low robustness, or relax to a near gate.")
    for r in hits[:top]:
        c, _ = C.logic_card(r)
        out.append(f"  [PASS] gate={c['realized_gate']} support={c['gate_support']} margin={c['margin_decades']}dec "
                   f"{r['inputs'][0]},{r['inputs'][1]}→{r['output']}  {r['network_id'][:44]}…")
    return "\n".join(out)

def _search_analog(spec, top=3):
    tgt = spec["goal"]["analog"]["target"]
    if not os.path.isfile(ANALOG_LABELS):
        return f"# (analog corpus not present yet: {ANALOG_LABELS})"
    rows = [json.loads(l) for l in open(ANALOG_LABELS) if l.strip()]
    keyfn = {"ratio_sensor": lambda r: abs(r.get("ratio_corr") or 0),
             "two_input_bump": lambda r: r.get("bump_fraction") or 0,
             "synergy": lambda r: r.get("coactivation_corr") or 0,
             "antagonism": lambda r: -(r.get("coactivation_corr") or 0),
             "equality_detector": lambda r: r.get("coactivation_corr") or 0,
             "diagonal_ridge": lambda r: r.get("coactivation_corr") or 0,
             }.get(tgt, lambda r: r.get("dynamic_range_decades") or 0)
    ranked = sorted(rows, key=lambda r: -keyfn(r))
    out = [f"# analog target={tgt}; {len(rows)} surfaces ranked by descriptor"]
    for r in ranked[:top]:
        c, _ = C.analog_card(r)
        out.append(f"  [{c['kind']}] coact={r.get('coactivation_corr')} ratio={r.get('ratio_corr')} "
                   f"bump={r.get('bump_fraction')} dyn={r.get('dynamic_range_decades')}dec "
                   f"{r['inputs'][0]},{r['inputs'][1]}→{r['output']}  {r['network_id'][:40]}…")
    return "\n".join(out)

PEAK_FAMILY = {"bandpass_with_plateau", "biphasic_peak"}
def _disambig(spec, text):
    """Ask a clarifying question when the compiled spec sits on a known taxonomy ambiguity
    the request didn't resolve (the NL-fidelity finding: bandpass = biphasic + a plateau)."""
    t = text.lower(); g = spec.get("goal", {})
    if g.get("behavior_family") == "dose_shape" and g.get("behavior_class") in PEAK_FAMILY:
        said_plateau = any(w in t for w in ("plateau", "flat top", "broad high", "wide high", "band", "shelf"))
        said_peak_only = any(w in t for w in ("no plateau", "just a peak", "sharp peak", "single peak", "pure peak"))
        if not said_plateau and not said_peak_only:
            cls = g["behavior_class"]; other = (PEAK_FAMILY - {cls}).pop()
            return (f"ambiguous: {cls} vs {other} (differ only by whether the peak has a flat "
                    f"plateau on top). Proceeding as {cls}; reply 'with/without plateau' to switch.")
    return None

def _search_contextual(spec, top=3):
    if not os.path.isfile(CONTEXTUAL_LABELS):
        return ("# contextual: does one fixed network compute different gates under different "
                "accessory-expression contexts (reprogrammable)?\n"
                "  ⚠ no contextual label corpus yet (it needs a context-sweep pass) — a bounded "
                "contextual corpus is being generated; retrieval lights up once it lands.")
    rows = [json.loads(l) for l in open(CONTEXTUAL_LABELS) if l.strip()]
    repro = [r for r in rows if r.get("reprogrammable")]
    tgt = spec["goal"].get("targets") or []
    repro.sort(key=lambda r: (-sum(1 for g in tgt if g in (r.get("distinct_robust_gates") or [])),
                              -(r.get("n_distinct_robust_gates") or 0)))
    out = [f"# {len(repro)} reprogrammable / {len(rows)} contextual slices" + (f"; want {tgt}" if tgt else "")]
    if not repro:
        out.append("  ⚠ none reprogrammable in this batch — contextual versatility is rare in the bare μ≤5 family (expected).")
    for r in repro[:top]:
        gates = "/".join(r.get("distinct_robust_gates") or [])
        out.append(f"  [REPROGRAMMABLE] context={r.get('context_sym')} gates={gates} "
                   f"{r['inputs'][0]},{r['inputs'][1]}→{r['output']}  {str(r.get('network_id'))[:40]}…")
    return "\n".join(out)

# ── intent gate ────────────────────────────────────────────────────────────────
# Only compile + search when the message actually describes a behaviour (or refines a
# prior spec). WITHOUT this gate the rule compiler's bandpass DEFAULT (_dose_class's
# catch-all) turns "你好"/"hello"/"thanks"/gibberish into a confident, misleading design
# result. Bare connectives (and/or) are excluded on purpose — logic intent must be NAMED
# ("gate"/"nand"/…), not implied by the "and" in "A and B".
_DESIGN_KW = (
    "bandpass", "band-pass", "band pass", "biphasic", "monotone", "activation", "activate",
    "repress", "threshold", "ultrasensit", "saturat", "plateau", "peak", "hump", "bump",
    "notch", "floor", "sigmoid", "hill", "cooperat", "adapt", "dose", "response curve",
    "rises", "rise then", "falls", "switch", "window",
    "reaction", "network", "circuit", "binding", "dimer", "complex", "species", "motif",
    "gate", "logic", "boolean", "truth table", "nand", "nor", "xor", "xnor", "imply",
    "veto", "coincidence",
    "ratio", "sensor", "surface", "synergy", "antagonism", "equality", "ridge",
    "coactivation", "ratiometric",
    "reprogram", "context", "cell type", "cell-type", "accessory", "expression context", "versatil",
    "behavior", "behaviour", "design a", "design an", "build me", "i want", "i need", "looking for",
)
_REFINE_HINT = ("wider", "narrower", "relax", "tighten", "sharper", "gentler", "steeper",
                "more reaction", "fewer reaction", "make it", "instead", "robust")
_CHAT_REPLY = (
    "I’m the Biocircuits design agent — I don’t do small talk. Describe a behaviour you want "
    "from a binding network and I’ll compile it to a spec, search the verified atlas, and return "
    "candidate reaction networks with their evidence. Try: “a bandpass with a wide plateau, "
    "≤4 reactions”, “an AND gate on inputs A and B”, “a ratio sensor for A vs B”, or “a network "
    "whose logic gate reprograms with context”."
)

def _is_design_request(text, prior=None):
    """True if text describes a behaviour to design, or refines a held spec. Greetings,
    thanks, capability questions and gibberish return False → a conversational reply."""
    t = (text or "").lower().strip()
    if not t:
        return False
    if prior and any(w in t for w in _REFINE_HINT):
        return True
    return any(k in t for k in _DESIGN_KW)

def run_turn(state, text, top=3):
    if not _is_design_request(text, (state or {}).get("spec")):
        return f"\nUSER: {text}\n  ⇒ {_CHAT_REPLY}"
    spec, note = compile_nl(text, state.get("spec"))
    state["spec"] = spec
    fam = spec["goal"].get("behavior_family", "dose_shape")
    q = _disambig(spec, text)
    body = (_search_logic(spec, top) if fam == "logic"
            else _search_analog(spec, top) if fam == "analog_surface"
            else _search_contextual(spec, top) if fam == "contextual_versatility"
            else _search_dose(spec, top))
    head = f"\nUSER: {text}\n  ⇒ compiled [{note}]"
    if q:
        head += f"\n  ❓ {q}"
    return head + "\n" + body

def api_search(spec, top=3):
    """Structured (dict) retrieval for the HTTP endpoint. Returns (cards: list[dict], info: dict)."""
    fam = spec["goal"].get("behavior_family", "dose_shape")
    if fam == "logic":
        lt = spec["goal"]["logic"]["logic_type"]
        rows = [json.loads(l) for l in open(LOGIC_LABELS)] if os.path.isfile(LOGIC_LABELS) else []
        hits = sorted([r for r in rows if r.get("realized_gate") == lt],
                      key=lambda r: (-(r.get("gate_support") or 0), -(r.get("median_margin_decades") or 0)))
        info = {"family": "logic", "logic_type": lt, "matches": len(hits), "total": len(rows)}
        if not hits:
            info["abstain"] = f"{lt} is essentially not realisable in the μ≤5 family (needs engineered antagonistic competition)."
        elif (hits[0].get("gate_support") or 0) < 0.6:
            info["weak"] = f"best gate_support {hits[0].get('gate_support')} < 0.6 — {lt} is marginal/edge in this family."
        return [C.logic_card(r)[0] for r in hits[:top]], info
    if fam == "analog_surface":
        tgt = spec["goal"]["analog"]["target"]
        rows = [json.loads(l) for l in open(ANALOG_LABELS)] if os.path.isfile(ANALOG_LABELS) else []
        keyfn = {"ratio_sensor": lambda r: abs(r.get("ratio_corr") or 0),
                 "two_input_bump": lambda r: r.get("bump_fraction") or 0,
                 "synergy": lambda r: r.get("coactivation_corr") or 0}.get(tgt, lambda r: r.get("dynamic_range_decades") or 0)
        ranked = sorted(rows, key=lambda r: -keyfn(r))[:top]
        return [C.analog_card(r)[0] for r in ranked], {"family": "analog_surface", "target": tgt, "total": len(rows)}
    if fam == "contextual_versatility":
        rows = [json.loads(l) for l in open(CONTEXTUAL_LABELS)] if os.path.isfile(CONTEXTUAL_LABELS) else []
        tgt = spec["goal"].get("targets") or []
        repro = sorted([r for r in rows if r.get("reprogrammable")],
                       key=lambda r: (-sum(1 for g in tgt if g in (r.get("distinct_robust_gates") or [])),
                                      -(r.get("n_distinct_robust_gates") or 0)))
        info = {"family": "contextual_versatility", "reprogrammable": len(repro), "total": len(rows), "targets": tgt}
        if not rows:
            info["abstain"] = "no contextual corpus yet (needs a context-sweep pass)."
        return [C.contextual_card(r)[0] for r in repro[:top]], info
    # dose_shape
    flat = ds.lower_spec(spec); cls = ds.CLASS_MAP.get(flat.get("behavior_class"))
    flat.setdefault("min_robustness", 0.2); flat.setdefault("max_reactions", 99)
    pool = [r for r in ds.iter_rows(DOSE_DS) if r.get("n_reactions", 99) <= flat["max_reactions"]]
    scored = [(ss, len(rs), r, rs) for r in pool for (ss, rs) in [ds.eval_row(r, flat, cls)]]
    scored.sort(key=lambda x: (-x[0], x[1]))
    passing = [s for s in scored if s[1] == 0]
    cards = [ds.build_card(*t) for t in (passing[:top] or scored[:top])]
    info = {"family": "dose_shape", "behavior_class": flat.get("behavior_class"),
            "corpus": len(pool), "hard_pass": len(passing)}
    if not passing:
        relax, absent = ds.compute_relaxation(flat, scored)
        info["relaxation"] = relax
        if absent:
            info["abstain"] = "this shape is essentially absent in μ≤5 — relax a gate or pick a different behaviour."
    return cards, info

def run_turn_api(state, text, llm_cfg=None, top=3):
    """One turn for the HTTP endpoint. state carries {'spec': <prior nested spec>} across
    turns (client-held). Returns a structured dict the frontend renders directly."""
    state = state or {}
    # Intent gate: a non-design message (greeting / thanks / capability question / gibberish)
    # gets a conversational reply with NO compiled spec and NO cards — never a defaulted design.
    if not _is_design_request(text, state.get("spec")):
        return {"message": text, "family": None, "kind": "chat", "note": None, "reply": _CHAT_REPLY,
                "disambiguation": None, "cards": [], "info": {}, "compiled_spec": None, "state": state}
    spec, note = compile_nl(text, state.get("spec"), llm_cfg=llm_cfg)
    fam = spec["goal"].get("behavior_family", "dose_shape")
    q = _disambig(spec, text)
    cards, info = api_search(spec, top)
    return {"message": text, "family": fam, "note": note, "disambiguation": q,
            "cards": cards, "info": info, "compiled_spec": spec, "state": {"spec": spec}}

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--script"); ap.add_argument("--top", type=int, default=3)
    a = ap.parse_args()
    state = {}
    lines = open(a.script).read().splitlines() if a.script else sys.stdin.read().splitlines()
    for ln in lines:
        ln = ln.strip()
        if ln and not ln.startswith("#"):
            print(run_turn(state, ln, a.top))

if __name__ == "__main__":
    main()
