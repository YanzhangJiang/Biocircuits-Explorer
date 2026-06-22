#!/usr/bin/env python3
# Tool-calling DESIGN AGENT — engine-in-the-loop (the architecture the roadmap.tex always
# specified: "conversational agent ... calling atlas, surrogate, scan, and ROP tools", with
# "the exact verifier in the loop" and "Model as Compass, Verifier as Truth").
# ---------------------------------------------------------------------------
# The LLM is the planner/reasoner. For a design request it: (optionally) pulls a SEED from the
# atlas (a PRIOR — "what's known to exist", never the final answer), proposes a network, and
# CALLS THE LIVE JULIA ENGINE to actually solve the equilibrium dose-response and phenotype it
# (build_model → parameter_scan_1d → behavior_families). It reads the COMPUTED shape, checks it
# against the request, and refines (mutate reactions/kd, re-sweep) across turns. Every candidate
# it presents carries a fresh in-session engine result.
#
# It NEVER fabricates: if the engine is offline, the simulate tool returns {engine_offline} and
# the system prompt forbids inventing a curve/label/metric — the agent says compute is offline.
# (This is the fix for the old lookup agent that "背答案" from precomputed .jsonl and would have
# returned identical canned cards with the engine down.)
#
# Requires an LLM key (provider-agnostic: OpenAI-compatible incl. a local proxy, or Anthropic).
import os, sys, json, collections, hashlib, time, uuid
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import design_search as ds
import cards as C
import llm_compile as L
import engine_client as E

ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), "..", ".."))
DOSE_DS = os.path.join(ROOT, "datasets", "latent-atlas-v0")
# Richer d≤4 full-parity dose corpus (2467 networks); unioned into the dose prior when present.
DOSE_DS_D4 = os.path.join(ROOT, "datasets", "latent-atlas-d4-v0")
# Curated multimodal (RO sign-oscillation) seed pack extracted from the d4 atlas — networks the
# phenotyper found robustly multi-sign-change. Read by retrieve_multimodal_seed.
MULTIMODAL_SEEDS = os.path.join(ROOT, "datasets", "multimodal_seeds_d4.json")
LOGIC_LABELS = os.path.join(ROOT, "datasets", "latent-atlas-logic-v0", "logic_labels.jsonl")
ANALOG_LABELS = os.path.join(ROOT, "datasets", "latent-atlas-analog-v0", "analog_labels.jsonl")
CONTEXTUAL_LABELS = os.path.join(ROOT, "datasets", "latent-atlas-contextual-v0", "contextual_labels.jsonl")

# ── DesignAgentTrace: one replayable record per turn (large arrays → artifacts/<hash>.json). ──
TRACE_SCHEMA_VERSION = "design-agent-trace/v0.1.0"
COMPILER_PROMPT_VERSION = "design-agent-prompt/v0.4.0"   # v0.4.0: multimodal (RO sign-oscillation) target
TRACE_DIR = os.environ.get("BNE_TRACE_DIR", os.path.join(ROOT, "traces"))

def _hash(obj):
    return hashlib.sha256(json.dumps(obj, sort_keys=True, default=str).encode()).hexdigest()[:16]

def _evidence(card):
    """Honest evidence tier for an engine result (no laundering, per the harness evidence policy).
    A dose shape with shape_support over the Kd prior Π (phenotype_classify, K=8) = Tier 3b
    (sampled verification under a prior); a single-kd 2-input corner scan = Tier 1 (fresh scan).
    We do NOT emit Tier 3a (geometric/volume certificate) — the live agent doesn't compute one."""
    if card.get("family") == "logic" or card.get("surface"):
        return {"tier": "1", "label": "Tier 1 · fresh 2-input scan (single kd)",
                "basis": f"realized gate from a fresh equilibrium corner scan at the given kd; on/off margin {card.get('margin_decades')} dec — single kd, not Π-sampled"}
    if card.get("shape_support") is not None:
        return {"tier": "3b", "label": f"Tier 3b · support {card.get('shape_support')} over Π (K=8)",
                "basis": f"shape_support over the Kd prior Π=LogUniform(-3,3), totals pinned, K=8; phenotyper {card.get('phenotyper_version', 'v0.4.0')}"}
    return {"tier": "1", "label": "Tier 1 · fresh dose scan", "basis": "single-kd dose-response scan"}

_HEAVY_KEYS = {"_card", "surface", "candidates", "seeds", "curve_sample_log10", "phenotype_families",
               "gate_corners_00_01_10_11", "shape_fractions", "dose_classes", "logic_gate_counts"}
def _tool_summary(res):
    """Compact, array-free summary of a tool result for the trace (scalars only)."""
    if not isinstance(res, dict):
        return {}
    return {k: v for k, v in res.items() if k not in _HEAVY_KEYS and not isinstance(v, (list, dict))}

def _spill_card(card):
    """Write a candidate card's FULL artifact (incl. computed_series / surface) keyed by content hash,
    so the trace can stay compact yet the heavy result is replayable. Returns the hash."""
    rh = _hash(card)
    try:
        adir = os.path.join(TRACE_DIR, "artifacts")
        os.makedirs(adir, exist_ok=True)
        p = os.path.join(adir, rh + ".json")
        if not os.path.exists(p):
            with open(p, "w") as fh:
                json.dump(card, fh, default=str)
    except Exception:
        pass
    return rh

def _card_summary(card, result_hash):
    keep = ("family", "realized_gate", "dominant_shape", "verdict", "shape_support", "margin_decades",
            "n_reactions", "output", "output_symbol", "inputs", "input_symbol", "kd", "rules", "evidence_tier")
    s = {k: card[k] for k in keep if k in card}
    s["result_hash"] = result_hash
    return s

def _write_trace(trace):
    """Append the compact trace to traces/traces.jsonl. Best-effort — never breaks a turn.
    The LLM api_key is never part of the trace (only provider/model/effort)."""
    try:
        os.makedirs(TRACE_DIR, exist_ok=True)
        with open(os.path.join(TRACE_DIR, "traces.jsonl"), "a") as fh:
            fh.write(json.dumps(trace, default=str) + "\n")
    except Exception as e:
        sys.stderr.write(f"[trace] write failed (non-fatal): {e}\n")

# ── atlas corpora (the PRIOR / seed source — cached once per process) ──
_CACHE = {}
def _rows_file(path):
    if path not in _CACHE:
        _CACHE[path] = [json.loads(l) for l in open(path)] if os.path.isfile(path) else []
    return _CACHE[path]
def _json_file(path):                          # whole-file JSON (e.g. the multimodal seed pack array)
    if path not in _CACHE:
        try: _CACHE[path] = json.load(open(path)) if os.path.isfile(path) else []
        except Exception: _CACHE[path] = []
    return _CACHE[path]
def _dose_rows():
    if "dose" not in _CACHE:
        rows = list(ds.iter_rows(DOSE_DS))
        if os.path.isdir(DOSE_DS_D4):          # union the richer d≤4 corpus when present
            rows += list(ds.iter_rows(DOSE_DS_D4))
        _CACHE["dose"] = rows
    return _CACHE["dose"]

# ── tools ────────────────────────────────────────────────────────────────────
def corpus_overview():
    """Prevalence of each behaviour in the atlas PRIOR — so the agent can judge feasibility
    (e.g. valley shapes are rarer than peaks) before proposing. NOT an answer; a map."""
    if "overview" in _CACHE:
        return _CACHE["overview"]
    dose = _dose_rows()
    classes = {}
    for cls in sorted(set(ds.CLASS_MAP.values())):
        vals = [(r.get("shape_fractions") or {}).get(cls) or 0.0 for r in dose]
        classes[cls] = {"n_robust_ge_0.5": sum(1 for v in vals if v >= 0.5), "max_support": round(max(vals), 3) if vals else 0.0}
    logic = _rows_file(LOGIC_LABELS)
    ov = {"note": "atlas PRIOR (precomputed survey); use it to pick seeds & gauge feasibility, NOT as the answer.",
          "dose_corpus": len(dose), "dose_classes": classes,
          "logic_total": len(logic),
          "logic_gate_counts": dict(collections.Counter(r.get("realized_gate") for r in logic).most_common()),
          "analog_total": len(_rows_file(ANALOG_LABELS)),
          "contextual_total": len(_rows_file(CONTEXTUAL_LABELS)),
          "contextual_reprogrammable": sum(1 for r in _rows_file(CONTEXTUAL_LABELS) if r.get("reprogrammable"))}
    _CACHE["overview"] = ov
    return ov

def retrieve_atlas_seed(behavior_class, max_reactions=99, min_robustness=0.4, top=3):
    """SEED networks (reactions+kd topology) the atlas suggests for a 1-input dose shape — a
    PRIOR to feed into `simulate`, NOT a verified answer. Tagged unverified-this-session."""
    cls = ds.CLASS_MAP.get(behavior_class)
    if not cls:
        return {"error": f"unknown behavior_class '{behavior_class}'", "valid_classes": sorted(ds.CLASS_MAP)}
    flat = {"behavior_class": behavior_class, "min_robustness": min_robustness, "max_reactions": max_reactions}
    pool = [r for r in _dose_rows() if (r.get("n_reactions") or 99) <= max_reactions]
    scored = [(ss, len(rs), r, rs) for r in pool for (ss, rs) in [ds.eval_row(r, flat, cls)]]
    scored.sort(key=lambda x: (-x[0], x[1]))
    seeds = []
    for (ss, _n, r, _rs) in scored[:top]:
        # carry the SAME axes the atlas used to assign this label, so `simulate` can reproduce it:
        seeds.append({"reactions": r.get("rules") or [], "kd": r.get("kd"),
                      "input_symbol": r.get("input_symbol"), "observe_species": r.get("output_symbol"),
                      "atlas_label": r.get("dominant_shape"), "atlas_shape_support": round(ss, 3),
                      "n_reactions": r.get("n_reactions"),
                      "note": "atlas prior — NOT re-verified this session; pass reactions+input_symbol+observe_species to simulate"})
    return {"behavior_class": behavior_class, "corpus": len(pool),
            "best_atlas_support": round(scored[0][0], 3) if scored else 0.0, "seeds": seeds}

def retrieve_logic_seed(gate, top=3):
    """SEED 2-input networks the atlas realised as a Boolean gate (PRIOR; live 2-input
    re-verification is not yet wired, so these are clearly unverified-this-session)."""
    rows = _rows_file(LOGIC_LABELS)
    hits = sorted([r for r in rows if r.get("realized_gate") == gate],
                  key=lambda r: (-(r.get("gate_support") or 0), -(r.get("median_margin_decades") or 0)))
    return {"gate": gate, "matches": len(hits), "total": len(rows),
            "best_gate_support": round(hits[0].get("gate_support") or 0, 3) if hits else 0.0,
            "note": "atlas PRIOR for a 2-input gate — pass each seed's reactions to simulate_2d to compute its real surface and verify the realized gate this session.",
            "seeds": [{"reactions": C.logic_card(r)[0].get("rules") or [], "realized_gate": r.get("realized_gate"),
                       "gate_support": r.get("gate_support"), "inputs": r.get("inputs"), "output": r.get("output")}
                      for r in hits[:top]]}

_ANALOG_KEYFN = {
    "two_input_bump": lambda r: r.get("bump_fraction") or 0,
    "interior_bump": lambda r: r.get("bump_fraction") or 0,
    "ratio_sensor": lambda r: abs(r.get("ratio_corr") or 0),
    "synergy": lambda r: r.get("coactivation_corr") or 0,
    "coactivation": lambda r: r.get("coactivation_corr") or 0,
}
def retrieve_analog_seed(target, top=3):
    """SEED 2-input networks for an analog SURFACE target (PRIOR). target='two_input_bump' ranks by
    interior-bump strength, 'ratio_sensor' by |ratio_corr|, 'synergy' by coactivation. Pass each
    seed's reactions+input1+input2+observe_species to simulate_2d to compute & verify the surface."""
    rows = _rows_file(ANALOG_LABELS)
    keyfn = _ANALOG_KEYFN.get(target, lambda r: r.get("dynamic_range_decades") or 0)
    ranked = sorted(rows, key=lambda r: -keyfn(r))
    seeds = [{"reactions": r.get("rules") or [],
              "input1": (r.get("inputs") or [None, None])[0], "input2": (r.get("inputs") or [None, None])[1],
              "observe_species": r.get("output"), "bump_fraction": r.get("bump_fraction"),
              "ratio_corr": r.get("ratio_corr"), "coactivation_corr": r.get("coactivation_corr"),
              "note": "atlas prior — pass reactions+input1+input2+observe_species to simulate_2d to verify"}
             for r in ranked[:top]]
    return {"target": target, "total": len(rows),
            "best_score": round(keyfn(ranked[0]), 3) if ranked else 0.0, "seeds": seeds}

def retrieve_multimodal_seed(top=6):
    """SEED 1-input networks the atlas found to produce a MULTIMODAL dose-response — the response
    order reverses sign ≥2 times (up-down-up / down-up-down). A PRIOR / starting point: pass each
    seed's reactions + input_symbol + observe_species to `simulate` to recompute and VERIFY it this
    session. Ranked by the phenotyper's multimodal fraction over the Kd prior Π (highest-confidence
    multimodal topologies first). Use this when the user asks for an oscillating / 来回穿梭 / 先增后减再增
    response — it is far more likely to yield a real multimodal than a hand-guessed topology."""
    pack = _json_file(MULTIMODAL_SEEDS)
    if not pack:
        return {"target": "multimodal", "total": 0, "seeds": [],
                "note": "no multimodal seed pack on disk — propose a topology and verify with simulate / ro_behavior instead"}
    seeds = [{"reactions": s.get("rules") or [], "input_symbol": s.get("input_symbol"),
              "observe_species": s.get("output_symbol"),
              "atlas_multimodal_fraction": s.get("complex_fraction"),
              "atlas_label": s.get("dominant_shape"), "n_reactions": s.get("n_reactions"),
              "note": "atlas prior (multimodal) — NOT re-verified this session; pass reactions+input_symbol+observe_species to simulate"}
             for s in pack[:top]]
    return {"target": "multimodal", "total": len(pack),
            "best_atlas_fraction": pack[0].get("complex_fraction") if pack else 0.0, "seeds": seeds}

def _interior_peak(xs, ys, grid):
    """Detect an interior peak (a 'bump'): is the global max strictly inside the grid and above the
    best edge value? Returns is_interior_peak + prominence (decades) + where the peak sits."""
    nx = len(grid); ny = len(grid[0]) if nx else 0
    pts = [(grid[i][j], i, j) for i in range(nx) for j in range(ny) if isinstance(grid[i][j], (int, float))]
    if nx < 3 or ny < 3 or not pts:
        return None
    gmax, gi, gj = max(pts, key=lambda t: t[0])
    border = [v for (v, i, j) in pts if i in (0, nx - 1) or j in (0, ny - 1)]
    bmax = max(border) if border else gmax
    is_int = 0 < gi < nx - 1 and 0 < gj < ny - 1
    return {"is_interior_peak": bool(is_int and gmax > bmax),
            "prominence_decades": round(gmax - bmax, 3),
            "peak_at_log10": [round(xs[gi], 2), round(ys[gj], 2)]}

def _active_window(xs, ys, margin=1.0):
    """Pick the informative x-interval from a recon scan: where the curve actually moves (slope
    magnitude above a fraction of the max), padded by `margin` decades. Falls back to the full
    scan when the curve is essentially flat. Used to focus the displayed dose-response window."""
    n = len(xs)
    if n < 3 or (max(ys) - min(ys)) < 0.05:
        return (xs[0], xs[-1]) if xs else (-6.0, 6.0)
    slopes = [abs((ys[i + 1] - ys[i]) / ((xs[i + 1] - xs[i]) or 1e-9)) for i in range(n - 1)]
    smax = max(slopes) or 1e-9
    active = [i for i, s in enumerate(slopes) if s >= 0.08 * smax]
    if not active:
        return xs[0], xs[-1]
    lo, hi = xs[active[0]] - margin, xs[active[-1] + 1] + margin
    if hi - lo < 2.0:                       # keep a readable minimum width
        c = 0.5 * (lo + hi); lo, hi = c - 1.0, c + 1.0
    return max(-12.0, lo), min(12.0, hi)

def simulate(reactions, kd=None, input_symbol=None, observe_species=None,
             param_min=-6.0, param_max=6.0, n_points=121, **_):
    # The sweep is over a standard LOG10 window; clamp to a sane range so a mis-supplied
    # value can never push the solve off the feasible map (the cause of an all-floor curve).
    param_min = max(-12.0, min(11.0, float(param_min)))
    param_max = min(12.0, max(param_min + 1.0, float(param_max)))
    n_points = int(max(21, min(401, n_points)))
    """THE compute tool. Builds the network on the live Julia engine and ACTUALLY SOLVES its
    1-input dose-response + phenotypes it. Returns the COMPUTED curve sample + the engine's
    motif/exact label + robustness volume. Use this to verify any candidate (atlas seed OR
    your own design, incl. networks not in the atlas). Returns {engine_offline:true} if the
    compute engine is down — in that case DO NOT fabricate; tell the user."""
    reactions = list(reactions or [])
    if not reactions:
        return {"error": "simulate needs a non-empty reactions list, e.g. ['A + B <-> AB']"}
    kd = [float(x) for x in kd] if kd else [1.0] * len(reactions)
    m = E.build_model(reactions=reactions, kd=kd)
    if m.get("engine_offline"):
        return {"engine_offline": True, "error": m.get("error")}
    if m.get("error"):
        return {"error": f"build_model rejected the network: {m.get('error')}"}
    sid, q_sym, species, prod = m.get("session_id"), m.get("q_sym") or [], m.get("x_sym") or [], m.get("product_species") or []
    inp = input_symbol or (q_sym[0] if q_sym else None)
    if inp not in q_sym:
        # tolerate a species name for the input axis ("A" -> its total "tA"), a common slip.
        for alt in (("t" + inp) if inp else None, (inp[1:] if inp and inp.startswith("t") else None)):
            if alt in q_sym:
                inp = alt; break
    if inp not in q_sym:
        return {"error": f"input_symbol '{input_symbol}' is not a swept input for this network",
                "valid_inputs": q_sym, "species": species, "hint": "the input axis is a total-concentration symbol like 'tA' (one of valid_inputs)"}
    obs = observe_species or (prod[-1] if prod else (species[-1] if species else None))
    if obs not in species:
        return {"error": f"observe_species '{obs}' is not a species", "valid_species": species}
    # CLOSED LOOP for the display window: a wide reconnaissance solve → find where the curve is
    # actually active (the regime-transition region) → a focused solve over that interval, so the
    # plotted curve shows the informative range with correct bounds, not flat tails. Both real solves.
    def _scan(lo, hi, n):
        return E.dose_response(sid, param_symbol=inp, output_exprs=[obs], param_min=lo, param_max=hi, n_points=n)
    recon = _scan(-8.0, 8.0, 81)
    if recon.get("engine_offline"):
        return {"engine_offline": True, "error": recon.get("error")}
    if recon.get("error"):
        return {"error": f"dose_response failed: {recon.get('error')}"}
    rxs = recon.get("param_values") or []
    rys = [(row[0] if isinstance(row, list) else row) for row in (recon.get("output_traj") or [])]
    lo, hi = _active_window(rxs, rys, margin=1.0)
    dr = _scan(lo, hi, int(n_points))
    if dr.get("engine_offline"):
        return {"engine_offline": True, "error": dr.get("error")}
    if dr.get("error"):
        dr = recon                          # focused scan failed → keep the recon curve
    xs = dr.get("param_values") or []
    ys = [(row[0] if isinstance(row, list) else row) for row in (dr.get("output_traj") or [])]
    series = [{"x": round(float(x), 4), "y": round(float(y), 4)} for x, y in zip(xs, ys)]
    # faithful verification: the SAME SISO phenotyper that labelled the atlas — shape_support over
    # the Kd prior Π (NOT behavior_families' ROP-volume view, which can disagree with the atlas).
    ph = E.phenotype_classify(sid, input_symbol=inp, output_expr=obs, K=8)
    shape = support = None; fracs = {}; sign_seq = None; n_sign_changes = None
    pheno_offline = bool(ph.get("engine_offline"))
    pheno_err = ph.get("error") if isinstance(ph, dict) else None
    if isinstance(ph, dict) and not pheno_offline and not pheno_err:
        shape = ph.get("dominant_shape")
        support = round(ph.get("shape_support") or 0, 3)
        fracs = {k: round(v, 3) for k, v in (ph.get("shape_fractions") or {}).items() if (v or 0) > 0}
        # the actual ±-pattern of the verified shape (e.g. [1,-1,1] = rise-fall-rise),
        # so the agent can cite RO sign oscillation and the UI can show n_sign_changes
        sign_seq = ph.get("sign_seq"); n_sign_changes = ph.get("n_sign_changes")
    # compact view for the LLM (downsampled curve so it can SEE+reason about the shape)
    step = max(1, len(series) // 16)
    sample = [[s["x"], s["y"]] for s in series[::step]]
    ymax_i = max(range(len(series)), key=lambda i: series[i]["y"]) if series else 0
    ymin_i = min(range(len(series)), key=lambda i: series[i]["y"]) if series else 0
    result = {"family": "dose_shape", "reactions": reactions, "kd": kd, "input_symbol": inp,
              "observe_species": obs, "n_points": len(series), "curve_sample_log10": sample,
              "y_min": round(min(ys), 4) if ys else None, "y_max": round(max(ys), 4) if ys else None,
              "x_at_ymax": round(xs[ymax_i], 3) if xs else None, "x_at_ymin": round(xs[ymin_i], 3) if xs else None,
              "phenotype_shape": shape, "shape_support": support, "shape_fractions": fracs,
              "sign_seq": sign_seq, "n_sign_changes": n_sign_changes,
              "phenotype_note": ("phenotyper offline" if pheno_offline else pheno_err),
              "evidence_tier": "engine-verified (this session)"}
    # full-resolution UI card (computed_series drives the real plotted curve in agent-view.js)
    card = {"family": "dose_shape", "verdict": shape or "computed", "shape_support": support,
            "n_reactions": len(reactions), "output_symbol": obs, "rules": reactions, "kd": kd,
            "dominant_shape": shape, "shape_fractions": fracs, "input_symbol": inp,
            "sign_seq": sign_seq, "n_sign_changes": n_sign_changes,
            "phenotyper_version": (ph or {}).get("phenotyper_version"),
            "metrics_median": {}, "computed_series": series}
    ev = _evidence(card); card["evidence"] = ev; card["evidence_tier"] = ev["label"]
    result["evidence_tier"] = ev["label"]; result["_card"] = card
    return result

# (A,B) corner order (00,01,10,11), high=1 — mirrors evaluators.jl / cards.py / agent-view.js.
_GATE_TABLES = {
    "AND": (0, 0, 0, 1), "OR": (0, 1, 1, 1), "NAND": (1, 1, 1, 0), "NOR": (1, 0, 0, 0),
    "XOR": (0, 1, 1, 0), "XNOR": (1, 0, 0, 1), "NIMPLY": (0, 0, 1, 0), "IMPLY": (1, 1, 0, 1),
    "NOT_A": (1, 1, 0, 0), "NOT_B": (1, 0, 1, 0), "A": (0, 0, 1, 1), "B": (0, 1, 0, 1),
    "CIMPLY": (1, 0, 1, 1), "BNIMPLY": (0, 1, 0, 0), "TRUE": (1, 1, 1, 1), "FALSE": (0, 0, 0, 0),
}
def _booleanize_corners(grid):
    """grid[i][j] over input1(rows A) × input2(cols B), log-space. Threshold at the surface midpoint;
    read the 4 corners (00,01,10,11) → realized gate + on/off margin (decades)."""
    flat = [v for row in grid for v in row if isinstance(v, (int, float))]
    if not flat:
        return None, None, None
    lo, hi = min(flat), max(flat); thr = 0.5 * (lo + hi)
    c = [grid[0][0], grid[0][-1], grid[-1][0], grid[-1][-1]]
    if any(not isinstance(v, (int, float)) for v in c):
        return None, None, None
    bits = tuple(1 if v >= thr else 0 for v in c)
    name = next((g for g, t in _GATE_TABLES.items() if t == bits), "custom" + "".join(map(str, bits)))
    return name, list(bits), round(min(abs(v - thr) for v in c), 3)

def _downsample_grid(xs, ys, grid, n=40):
    si = max(1, len(xs) // n); sj = max(1, len(ys) // n)
    gz = [[row[j] for j in range(0, len(row), sj)] for row in grid[::si]]
    return xs[::si], ys[::sj], gz

def simulate_2d(reactions, kd=None, input1=None, input2=None, observe_species=None, n_grid=48, **_):
    """THE 2-input compute tool. Builds the network and ACTUALLY SOLVES its 2-input response SURFACE
    over input1×input2 (a real heatmap), then reads the realized Boolean gate from the four corners.
    Use to design/verify logic gates and analog surfaces (incl. networks not in the atlas). Returns
    the computed surface + realized_gate + on/off margin, or {engine_offline} if the engine is down."""
    reactions = list(reactions or [])
    if not reactions:
        return {"error": "simulate_2d needs a non-empty reactions list"}
    kd = [float(x) for x in kd] if kd else [1.0] * len(reactions)
    m = E.build_model(reactions=reactions, kd=kd)
    if m.get("engine_offline"):
        return {"engine_offline": True, "error": m.get("error")}
    if m.get("error"):
        return {"error": f"build_model rejected the network: {m.get('error')}"}
    sid, q, sp, prod = m.get("session_id"), m.get("q_sym") or [], m.get("x_sym") or [], m.get("product_species") or []
    def _res(x):
        if x in q: return x
        if x and ("t" + x) in q: return "t" + x
        return None
    i1 = _res(input1) or (q[0] if q else None)
    i2 = _res(input2) or next((s for s in q if s != i1), None)
    if i1 not in q or i2 not in q or i1 == i2:
        return {"error": "need two distinct input totals", "valid_inputs": q, "species": sp}
    obs = observe_species or (prod[-1] if prod else (sp[-1] if sp else None))
    if obs not in sp:
        return {"error": f"observe_species '{obs}' is not a species", "valid_species": sp}
    s = E.scan_2d(sid, param1_symbol=i1, param2_symbol=i2, output_expr=obs, n_grid=int(n_grid))
    if s.get("engine_offline"):
        return {"engine_offline": True, "error": s.get("error")}
    if s.get("error"):
        return {"error": f"scan_2d failed: {s.get('error')}"}
    xs, ys = s.get("param1_values") or [], s.get("param2_values") or []
    grid = [[v for v in row] for row in (s.get("output_grid") or [])]
    gate, bits, margin = _booleanize_corners(grid)
    peak = _interior_peak(xs, ys, grid)          # analog "bump": interior max above the edges?
    gx, gy, gz = _downsample_grid(xs, ys, grid, 40)
    flat = [v for r in grid for v in r if isinstance(v, (int, float))]
    result = {"family": "logic", "reactions": reactions, "kd": kd, "input1": i1, "input2": i2,
              "observe_species": obs, "realized_gate": gate, "gate_corners_00_01_10_11": bits,
              "margin_decades": margin, "interior_peak": peak,
              "surface_min": round(min(flat), 3) if flat else None,
              "surface_max": round(max(flat), 3) if flat else None, "n_grid": len(xs),
              "evidence_tier": "engine-verified (this session)"}
    card = {"family": "logic", "realized_gate": gate or "surface", "inputs": [i1, i2],
            "output": obs, "margin_decades": margin, "rules": reactions, "kd": kd,
            "interior_peak": peak,
            "surface": {"x": gx, "y": gy, "z": gz, "input1": i1, "input2": i2, "observe": obs}}
    ev = _evidence(card); card["evidence"] = ev; card["evidence_tier"] = ev["label"]
    result["evidence_tier"] = ev["label"]; result["_card"] = card
    return result

def _ro_sign_seq(profile):
    """Collapse an analytic RO profile into a compressed sign sequence (+1/-1; flats and singular
    NaN/±Inf transitions dropped; repeats collapsed) — comparable to the phenotyper's sign_seq.
    Accepts the per-regime sign/RO list (motif_profile ['+','0','-'] or exact_profile [1.0,'NaN',-1.0])
    or the arrow-joined exact_label string ('1 → 0 → -1'). Returns [] for a multi-input regime token."""
    if profile is None:
        return []
    if isinstance(profile, str):
        toks = profile.replace("->", "→").split("→")        # exact_label uses the unicode arrow
    elif isinstance(profile, (list, tuple)):
        toks = list(profile)
    else:
        return []
    out = []
    for tok in toks:
        t = str(tok).strip().strip("[]").strip()
        if "," in t:                       # multi-input regime token → not a scalar dose word
            return []
        if t in ("NaN", "nan", "missing", "", "?"):
            continue                       # singular transition — no defined sign
        if t == "+": s = 1
        elif t == "-": s = -1
        elif t == "0": s = 0
        elif t in ("+Inf", "Inf", "inf"): s = 1
        elif t == "-Inf": s = -1
        else:
            try: v = float(t)
            except ValueError: continue
            s = 1 if v > 1e-9 else (-1 if v < -1e-9 else 0)
        if s == 0:
            continue
        if not out or out[-1] != s:
            out.append(s)
    return out

def ro_behavior(reactions, kd=None, input_symbol=None, observe_species=None, **_):
    """ANALYTIC RO (reaction-order) behavior of a 1-input network — the parameter-free SISO
    enumeration of which dose-response sign patterns this TOPOLOGY can produce over ALL kd. Use it
    BEFORE hunting kd to check whether a target is even structurally possible (esp. multimodal: does
    any feasible family reverse sign ≥2 times?), and to cross-check a numeric `simulate` verdict.
    Complements `simulate` (numeric, one kd) with structure. Returns the feasible exact families with
    their RO sign sequences + n_sign_changes + robustness volume, or {engine_offline}."""
    reactions = list(reactions or [])
    if not reactions:
        return {"error": "ro_behavior needs a non-empty reactions list"}
    kd = [float(x) for x in kd] if kd else [1.0] * len(reactions)
    m = E.build_model(reactions=reactions, kd=kd)
    if m.get("engine_offline"):
        return {"engine_offline": True, "error": m.get("error")}
    if m.get("error"):
        return {"error": f"build_model rejected the network: {m.get('error')}"}
    sid, q, sp, prod = m.get("session_id"), m.get("q_sym") or [], m.get("x_sym") or [], m.get("product_species") or []
    inp = input_symbol if input_symbol in q else \
          (("t" + input_symbol) if input_symbol and ("t" + input_symbol) in q else (q[0] if q else None))
    if inp not in q:
        return {"error": f"input_symbol '{input_symbol}' is not a swept input", "valid_inputs": q}
    obs = observe_species or (prod[-1] if prod else (sp[-1] if sp else None))
    if obs not in sp:
        return {"error": f"observe_species '{obs}' is not a species", "valid_species": sp}
    bf = E.phenotype(sid, change_qK=inp, observe_x=obs, path_scope="feasible", compute_volume=True)
    if bf.get("engine_offline"):
        return {"engine_offline": True, "error": bf.get("error")}
    if bf.get("error"):
        return {"error": f"behavior_families failed: {bf.get('error')}"}
    fams = []
    for fam in (bf.get("exact_families") or []):
        seq = _ro_sign_seq(fam.get("motif_profile") or fam.get("exact_label"))
        vol = fam.get("total_volume") or {}
        fams.append({"family_idx": fam.get("family_idx"), "ro_profile": fam.get("exact_label"),
                     "motif": fam.get("motif_label"),
                     "sign_seq": seq, "n_sign_changes": max(0, len(seq) - 1),
                     "n_paths": fam.get("n_paths"),
                     "volume_mean": (round(vol["mean"], 4) if isinstance(vol, dict) and vol.get("mean") is not None else None)})
    fams.sort(key=lambda f: (-f["n_sign_changes"], -(f["volume_mean"] or 0)))
    max_nsc = max((f["n_sign_changes"] for f in fams), default=0)
    return {"family": "ro_behavior", "input_symbol": inp, "observe_species": obs,
            "feasible_paths": bf.get("feasible_paths"), "total_paths": bf.get("total_paths"),
            "n_exact_families": len(fams), "max_sign_changes": max_nsc,
            "n_multimodal_families": sum(1 for f in fams if f["n_sign_changes"] >= 2),
            "multimodal_feasible": max_nsc >= 2, "families": fams[:12],
            "note": ("analytic parameter-free SISO enumeration: which RO sign sequences this TOPOLOGY "
                     "admits over all kd. max_sign_changes>=2 ⇒ a multimodal dose-response is "
                     "STRUCTURALLY POSSIBLE here (then find a kd with simulate). max_sign_changes<2 ⇒ "
                     "multimodal is impossible for this topology, change the network. This is the "
                     "ROP-volume view; the numeric phenotype at a specific kd is the per-design arbiter.")}

_READER_PROTOS = ["bump", "broad_bump", "right_peak", "left_peak", "valley",
                  "switch_on", "switch_off", "late_dip", "double_bump", "shoulder"]

def reader_panel(prototype="bump", intent="typical", behavior_class=None, nl=None, k=6, max_reactions=None, **_):
    """Function-Space Reader: an INTENT-ROUTED, evidence-backed candidate PANEL from the frozen
    industrial atlas_root (robust→label+support primary; typical/refinement→label recall+function
    rerank; existential/discovery→function primary). `nl` (the user's behaviour description) is
    compiled to target+intent; else `prototype`+`intent`. Returns route + coverage_status. A
    PRIOR/seed list — VERIFY a pick with `simulate` before presenting, never fabricate."""
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "reader"))
    try:
        import reader_service as RS
    except Exception as e:
        return {"error": f"reader service unavailable: {e}"}
    res = RS.panel(nl=nl, prototype=None if nl else prototype, intent=None if nl else intent,
                   behavior_label=behavior_class, k=k, max_reactions=max_reactions)
    if isinstance(res, dict) and res.get("error"):
        return res
    cands = [{"record_id": c["record_id"], "reactions": c["reactions"],
              "input_symbol": (c.get("io") or {}).get("input_symbol"),
              "observe_species": (c.get("io") or {}).get("output_symbol"),
              "dominant_shape": c["dominant_shape"], "match_score": c["match_score"],
              "evidence_tier": c["evidence"]["evidence_tier"], "shape_support": c["evidence"]["shape_support"],
              "rop_volume": c["evidence"]["volume_mean"], "robust_path_count": c["evidence"]["robust_path_count"]}
             for c in res.get("candidates", [])]
    out = {"family": "reader_panel", "intent": res.get("query", {}).get("intent_type", intent),
           "route": res.get("route"), "coverage_status": res.get("coverage_status"),
           "pipeline": res.get("pipeline"), "panel_diversity": res.get("panel_diversity"), "candidates": cands,
           "note": "intent-routed function-space atlas panel (PRIOR ranked by curve-match + ROP evidence; "
                   "see coverage_status). VERIFY each pick with `simulate` (its reactions+input_symbol+"
                   "observe_species) before presenting; if coverage_status is rare-or-absent, say so honestly."}
    if res.get("compiled_spec"):
        out["compiled_spec"] = res["compiled_spec"]
    return out

TOOLS_DISPATCH = {"corpus_overview": corpus_overview, "retrieve_atlas_seed": retrieve_atlas_seed,
                  "retrieve_logic_seed": retrieve_logic_seed, "retrieve_analog_seed": retrieve_analog_seed,
                  "retrieve_multimodal_seed": retrieve_multimodal_seed, "reader_panel": reader_panel,
                  "simulate": simulate, "simulate_2d": simulate_2d, "ro_behavior": ro_behavior}

_DOSE_CLASSES = sorted(set(ds.CLASS_MAP))
TOOLSPEC = [
    {"name": "corpus_overview",
     "description": "Atlas PRIOR: how prevalent each behaviour is in the precomputed μ≤5 survey (robust networks per dose class, per logic gate). Use to gauge feasibility before proposing. Not an answer.",
     "parameters": {"type": "object", "properties": {}, "additionalProperties": False}},
    {"name": "retrieve_atlas_seed",
     "description": "Get SEED 1-input networks for one of the standard dose classes — a PRIOR / starting point to feed into `simulate`, never a verified answer. Each seed includes the reactions plus the input_symbol and observe_species the atlas used for that label; pass those to `simulate` to reproduce and verify the shape.",
     "parameters": {"type": "object", "properties": {
         "behavior_class": {"type": "string", "enum": _DOSE_CLASSES, "description": "one of the system's standard dose class labels"},
         "max_reactions": {"type": "integer"}, "min_robustness": {"type": "number"}},
         "required": ["behavior_class"], "additionalProperties": False}},
    {"name": "retrieve_logic_seed",
     "description": "Get SEED 2-input networks the atlas recorded as realising a Boolean gate — a PRIOR. Pass each seed's reactions to simulate_2d to compute its real surface and verify the gate this session.",
     "parameters": {"type": "object", "properties": {
         "gate": {"type": "string", "enum": ["AND", "OR", "NAND", "NOR", "XOR", "XNOR", "NIMPLY", "IMPLY", "NOT_A", "NOT_B", "A", "B"]}},
         "required": ["gate"], "additionalProperties": False}},
    {"name": "retrieve_analog_seed",
     "description": "Get SEED 2-input networks for an analog SURFACE target — a PRIOR. 'two_input_bump' = an interior peak (output highest at a middle spot of BOTH inputs, flat elsewhere); 'ratio_sensor' = responds to the A/B ratio; 'synergy' = AND-like coactivation. Pass each seed's reactions+input1+input2+observe_species to simulate_2d to compute and verify (simulate_2d reports interior_peak for bumps).",
     "parameters": {"type": "object", "properties": {
         "target": {"type": "string", "enum": ["two_input_bump", "ratio_sensor", "synergy", "coactivation"]}},
         "required": ["target"], "additionalProperties": False}},
    {"name": "simulate",
     "description": "RUN THE LIVE ENGINE on a network: build it, solve its 1-input dose-response over a standard log10 input window, and phenotype it. Returns the COMPUTED curve + the engine's motif/exact label + robustness volume. Use for EVERY candidate you present (atlas seed or your own design, including networks not in the atlas). When verifying a seed, pass that seed's input_symbol and observe_species. If it returns engine_offline, DO NOT fabricate — tell the user the compute engine is offline.",
     "parameters": {"type": "object", "properties": {
         "reactions": {"type": "array", "items": {"type": "string"}, "description": "reaction strings, e.g. ['A + B <-> C_A_B','A + C_A_B <-> C_A_A_B']"},
         "kd": {"type": "array", "items": {"type": "number"}, "description": "dissociation constant per reaction (default all 1.0)"},
         "input_symbol": {"type": "string", "description": "the swept input: a total-concentration symbol like 'tA' (omit to use the first total)"},
         "observe_species": {"type": "string", "description": "the observed output species, e.g. 'C_A_B' (omit to use the main product)"}},
         "required": ["reactions"], "additionalProperties": False}},
    {"name": "simulate_2d",
     "description": "RUN THE LIVE ENGINE on a TWO-INPUT network: build it and actually solve its 2-input response SURFACE over input1×input2 (a real computed heatmap), then read the realized Boolean gate from the four corners. Use this to design/verify logic gates and analog surfaces (incl. networks not in the atlas). Returns the computed surface + realized_gate + on/off margin. engine_offline ⇒ do not fabricate.",
     "parameters": {"type": "object", "properties": {
         "reactions": {"type": "array", "items": {"type": "string"}},
         "kd": {"type": "array", "items": {"type": "number"}, "description": "dissociation constant per reaction (default all 1.0)"},
         "input1": {"type": "string", "description": "first swept input total, e.g. 'tA' (omit to use the first two totals)"},
         "input2": {"type": "string", "description": "second swept input total, e.g. 'tB'"},
         "observe_species": {"type": "string", "description": "observed output species, e.g. 'C_A_B'"}},
         "required": ["reactions"], "additionalProperties": False}},
    {"name": "retrieve_multimodal_seed",
     "description": "Get SEED 1-input networks the atlas found to produce a MULTIMODAL dose-response (response order reverses sign ≥2 times: up-down-up / down-up-down) — a PRIOR ranked by the phenotyper's multimodal fraction over the Kd prior. Call this FIRST for any oscillating / 来回穿梭 / 先增后减再增 request: these topologies are known to realise multimodal, so they are a far better starting point than a hand-guessed network. Then pass a seed's reactions+input_symbol+observe_species to `simulate` to recompute and verify it this session.",
     "parameters": {"type": "object", "properties": {}, "additionalProperties": False}},
    {"name": "ro_behavior",
     "description": "ANALYTIC, parameter-free RO (reaction-order) behavior of a 1-input network: the SISO enumeration of which dose-response sign patterns the TOPOLOGY can produce over ALL kd. Use it to decide whether a target is structurally possible BEFORE hunting kd — especially multimodal: it returns whether any feasible family reverses sign ≥2 times (multimodal_feasible). max_sign_changes<2 ⇒ multimodal is impossible for this network, change the topology; ≥2 ⇒ possible, then realise it with `simulate` and cross-check the sign_seq. Complements simulate (numeric, one kd) with structure.",
     "parameters": {"type": "object", "properties": {
         "reactions": {"type": "array", "items": {"type": "string"}},
         "kd": {"type": "array", "items": {"type": "number"}, "description": "dissociation constant per reaction (does not affect the analytic enumeration; default all 1.0)"},
         "input_symbol": {"type": "string", "description": "the swept input total, e.g. 'tA' (omit to use the first total)"},
         "observe_species": {"type": "string", "description": "observed output species (omit to use the main product)"}},
         "required": ["reactions"], "additionalProperties": False}},
    {"name": "reader_panel",
     "description": "FUNCTION-SPACE READER: an INTENT-ROUTED, evidence-backed candidate PANEL from the frozen industrial atlas — robust→label+support primary; typical/refinement→label recall+function rerank; existential/discovery→function primary; plus a coverage_status (well-covered / sparse / rare-or-absent). Pass EITHER `nl` (the user's behaviour description, compiled to target+intent) OR `prototype`+`intent`. Returns candidates with match_score, evidence_tier (T1/T2/T3a/T3b), shape_support, ROP volume + robust_path_count, panel diversity, route, and coverage_status. A PRIOR/seed list (like retrieve_*): VERIFY each pick with `simulate` before presenting; if coverage_status is rare-or-absent, report that honestly. Needs the atlas (env BNE_ATLAS_ROOT / BNE_PACKET_CORPUS); returns {error} if unavailable.",
     "parameters": {"type": "object", "properties": {
         "nl": {"type": "string", "description": "the user's behaviour request in natural language (preferred; compiled to target+intent+coverage)"},
         "prototype": {"type": "string", "enum": _READER_PROTOS, "description": "target dose shape (if not using nl)"},
         "intent": {"type": "string", "enum": ["existential", "typical", "robust"], "description": "existential=find any realisation (discovery/off-label), typical=medoid, robust=many θ realise it"},
         "behavior_class": {"type": "string", "description": "optional nearest vocab label to seed label-recall"},
         "k": {"type": "integer", "description": "panel size (default 6)"},
         "max_reactions": {"type": "integer", "description": "optional network-size cap"}},
         "additionalProperties": False}},
]
OPENAI_TOOLS = [{"type": "function", "function": t} for t in TOOLSPEC]
ANTHROPIC_TOOLS = [{"name": t["name"], "description": t["description"], "input_schema": t["parameters"]} for t in TOOLSPEC]

SYSTEM = """You are the Biocircuits Design Agent. You design equilibrium protein-binding reaction
networks by REASONING and by CALLING A LIVE COMPUTE ENGINE — not by reciting a database. The atlas
is a PRIOR (a map of what's known to exist and good starting points); the ENGINE (which solves the
binding equilibrium / ODE and phenotypes the result) is the source of truth. Every candidate you
present to the user MUST be backed by a fresh `simulate` result from THIS session.

THE LOOP for any design request:
1. (optional) corpus_overview / retrieve_atlas_seed (1-input) / retrieve_logic_seed (2-input) to get a
   starting topology and gauge feasibility. For a multimodal / oscillating target use
   `retrieve_multimodal_seed` (known-good topologies); `ro_behavior` checks whether a given topology
   can structurally produce a sign pattern before sweeping kd.
2. Propose a concrete network (reactions + kd) — a seed, or your own design. kd is a DESIGN VARIABLE,
   not a constant: do NOT just leave every kd at 1.0. Choose and VARY kd (strong↔weak binding, roughly
   1e-3…1e3 per reaction) to place and shape the requested feature — e.g. weaker/stronger Kd to shift a
   threshold, sharpen a peak/valley, set where a 2-input bump sits, or widen a plateau — and re-simulate
   to compare. Try at least one non-trivial kd set when the request asks for a specific/sharp feature.
3. COMPUTE it on the engine:
   - 1-INPUT dose-response → `simulate` (returns the curve + the phenotype shape + robustness). When
     verifying a seed, pass its input_symbol and observe_species. You do not set the sweep window.
   - 2-INPUT gate / response surface → `simulate_2d` (returns the real input1×input2 surface heatmap +
     the realized Boolean gate read from the corners + the on/off margin).
4. VERIFY against the request. If it doesn't match (wrong shape/gate, low robustness/margin), REFINE —
   change kd, add/alter a reaction, pick a different input/observable — and re-simulate. Iterate.
5. Present only engine-simulated candidates, citing their computed shape/gate + robustness/margin, and
   reply in the user's language. KD MATTERS: state the kd you used (every candidate carries its kd).

The system's STANDARD dose (1-input) class labels are exactly: monotone_activation,
activation_with_saturation, monotone_repression, repression_with_floor, thresholded_activation,
biphasic_peak, bandpass_with_plateau, biphasic_valley, multimodal. Map the user's request — in ANY
language and ANY phrasing — to the right label using YOUR OWN language understanding (the code does
not translate natural language; do not expect it to). The engine's phenotype label is the final
arbiter of which class a candidate actually realises, so if you are unsure which label fits, propose
one, simulate, and read back the engine's verdict.

`multimodal` = the output's response order changes sign ≥2 times as the single input sweeps — it goes
up then down then up (or down-up-down, or more) — i.e. ≥2 turning points, distinct from biphasic_peak
/biphasic_valley which have exactly one. Map requests like "RO 正负来回穿梭 / 先增后减再增 / oscillating
/ wiggly / multiple peaks" to `multimodal`. `simulate` returns the engine's sign_seq (e.g. [1,-1,1] =
rise-fall-rise) and n_sign_changes — cite them. Multimodal is RARE in this μ≤5 equilibrium-binding
family and usually needs extra competing complexes / binding sites — so DO NOT hand-guess a topology.
For a multimodal request, START with `retrieve_multimodal_seed`: it returns networks the atlas already
found to realise a multimodal dose-response (ranked by robustness over the Kd prior). Pick a seed,
pass its reactions+input_symbol+observe_species to `simulate`, and read back the verdict. Use
`ro_behavior` when you want the analytic, parameter-free check of which sign sequences a topology can
produce over ALL kd. If `multimodal_feasible` is false (max_sign_changes < 2) the shape
is STRUCTURALLY impossible for that network — change the topology, don't waste kd sweeps. If true,
then find a kd that realises it with `simulate` and confirm the numeric sign_seq matches one of the
analytic families (a real cross-check, not a guess). If after several genuine attempts (varying
topology AND kd) the engine never returns multimodal — and `ro_behavior` shows it is infeasible for
the topologies you tried — SAY SO PLAINLY: "this appears rare/absent in the in-scope networks I tried;
analytically max sign changes was N." DO NOT relabel a single-peak result as multimodal to please the
user. An honest "I could not realise it in scope" is a correct answer; a fabricated match is not.

HARD RULES:
- NEVER invent a network, curve, label, or robustness number. If `simulate` returns engine_offline:true,
  STOP and tell the user the compute engine is offline (and that the node Workspace server must be running);
  return no candidates.
- The atlas seeds and corpus_overview are PRIORS, explicitly unverified — never present them as the answer
  without simulating. Verify 1-input seeds with `simulate` and 2-input seeds with `simulate_2d`.
- `reader_panel` returns a FUNCTION-SPACE PRIOR panel (candidates ranked by curve-match + ROP evidence from
  the PRECOMPUTED atlas) — it is NOT verification. Treat every reader_panel candidate exactly like an atlas
  seed: its match_score, evidence_tier, and shape_support describe the precomputed phenotype, not a fresh
  result. You MUST re-verify a reader_panel pick with `simulate` (its reactions + input_symbol +
  observe_species) before presenting it; never show a reader_panel candidate or its match_score/tier as a
  verified answer. If reader_panel returns {error} (corpus unavailable), do NOT fabricate a panel — say so
  and fall back to retrieve_* or your own design.
- A 3-input / context-indexed (contextual-versatility) request: simulate_2d computes the 2-input
  surface holding the other totals at their default level. Sweeping a third (context) total across
  levels is NOT yet wired, so present the default-context surface and say plainly that the per-context
  sweep is a current limitation — never claim a context-dependent result you have not simulated.
- For greetings, thanks, or non-design chatter: reply briefly and naturally; do NOT call tools.
- Be concise and concrete. Show your reasoning about why a network produces the requested shape.
"""

NEED_KEY_MSG = ("This design agent runs on an LLM that reasons and drives the compute engine. Add an "
                "API key in the ⚙ panel (OpenAI-compatible incl. a local proxy, or Anthropic). "
                "需要在 ⚙ 面板填入一个 LLM key 才能使用。")

def _norm_cfg(llm_cfg):
    c = dict(llm_cfg or {})
    out = {"provider": c.get("provider") or "openai", "api_key": c.get("api_key") or c.get("apiKey"),
           "base_url": c.get("base_url") or c.get("baseUrl"), "model": c.get("model"),
           "effort": c.get("effort") or c.get("reasoning_effort")}
    if not out["api_key"] and c.get("key_file") and os.path.isfile(c["key_file"]):
        out["api_key"] = open(c["key_file"]).read().strip()
    if not out["model"]:
        out["model"] = "gpt-5.4-mini" if out["provider"] != "anthropic" else "claude-sonnet-4-6"
    return out

def _run_openai(history, message, dispatch, cfg, max_iters=12):
    msgs = [{"role": "system", "content": SYSTEM}] + history + [{"role": "user", "content": message}]
    final = ""
    for _ in range(max_iters):
        am = L.openai_chat_tools(msgs, OPENAI_TOOLS, **cfg)
        msgs.append(am)
        tcs = am.get("tool_calls") or []
        if not tcs:
            final = am.get("content") or ""
            break
        for tc in tcs:
            fn = tc.get("function") or {}
            try: args = json.loads(fn.get("arguments") or "{}")
            except Exception: args = {}
            result = dispatch(fn.get("name"), args)
            msgs.append({"role": "tool", "tool_call_id": tc.get("id"), "content": json.dumps(result, default=str)[:9000]})
    if not final:
        final = "I kept needing tools without converging — please restate or narrow the request."
    return final, history + [{"role": "user", "content": message}, {"role": "assistant", "content": final}]

def _run_anthropic(history, message, dispatch, cfg, max_iters=12):
    msgs = history + [{"role": "user", "content": message}]
    final = ""
    for _ in range(max_iters):
        resp = L.anthropic_chat_tools(SYSTEM, msgs, ANTHROPIC_TOOLS, **cfg)
        blocks = resp.get("content") or []
        msgs.append({"role": "assistant", "content": blocks})
        tool_uses = [b for b in blocks if b.get("type") == "tool_use"]
        text = "".join(b.get("text", "") for b in blocks if b.get("type") == "text")
        if not tool_uses:
            final = text; break
        results = [{"type": "tool_result", "tool_use_id": tu.get("id"),
                    "content": json.dumps(dispatch(tu.get("name"), tu.get("input") or {}), default=str)[:9000]}
                   for tu in tool_uses]
        msgs.append({"role": "user", "content": results})
    if not final:
        final = "I kept needing tools without converging — please restate or narrow the request."
    return final, history + [{"role": "user", "content": message}, {"role": "assistant", "content": final}]

def run_turn(state, message, llm_cfg=None, top=3):
    """One conversational turn. state = {'history': [...]}. Returns {kind, reply, family, cards, info,
    state, trace_id}. Emits a replayable DesignAgentTrace (traces/traces.jsonl) every turn; cards shown
    are engine-verified results (atlas seeds are priors, not shown alone)."""
    state = state or {}
    cfg = _norm_cfg(llm_cfg)
    turn_no = sum(1 for m in (state.get("history") or []) if isinstance(m, dict) and m.get("role") == "user") + 1
    trace = {"trace_schema_version": TRACE_SCHEMA_VERSION, "trace_id": "trace_" + uuid.uuid4().hex[:12],
             "created_at": time.strftime("%Y-%m-%dT%H:%M:%S"), "conversation_turn": turn_no,
             "raw_user_message": message,
             "compiler": {"provider": cfg.get("provider"), "model": cfg.get("model"),
                          "effort": cfg.get("effort"), "prompt_version": COMPILER_PROMPT_VERSION},
             "family": None, "tool_calls": [], "final_response": {}, "failure_tags": [], "user_feedback": None}
    if not cfg.get("api_key"):
        trace["final_response"] = {"kind": "need_key", "n_cards": 0, "abstained": True, "engine_offline": False}
        trace["failure_tags"] = ["no_api_key"]; _write_trace(trace)
        return {"kind": "need_key", "reply": NEED_KEY_MSG, "family": None, "cards": [], "info": {},
                "state": state, "trace_id": trace["trace_id"]}
    history = [m for m in (state.get("history") or []) if isinstance(m, dict) and m.get("content")]
    holder = {"verified": [], "family": None, "engine_offline": False, "info": {}}
    seen = set()
    debug = os.environ.get("BNE_AGENT_DEBUG")
    def dispatch(name, args):
        fn = TOOLS_DISPATCH.get(name)
        if not fn:
            return {"error": f"unknown tool '{name}'"}
        call_args = dict(args or {})
        if name in ("retrieve_atlas_seed", "retrieve_logic_seed", "retrieve_analog_seed"):
            call_args.setdefault("top", top)
        try:
            res = fn(**call_args)
        except Exception as e:
            res = {"error": f"{type(e).__name__}: {e}"}
        if debug:
            shown = {k: v for k, v in res.items() if k != "_card"} if isinstance(res, dict) else res
            sys.stderr.write(f"[tool] {name}({json.dumps(call_args, default=str)[:180]})\n      -> {json.dumps(shown, default=str)[:500]}\n")
        status = ("engine_offline" if isinstance(res, dict) and res.get("engine_offline")
                  else "error" if isinstance(res, dict) and res.get("error") else "ok")
        trace["tool_calls"].append({"call_id": "call_%03d" % (len(trace["tool_calls"]) + 1), "tool": name,
                                    "args": {k: v for k, v in call_args.items() if k != "top"},
                                    "status": status, "summary": _tool_summary(res),
                                    "result_hash": _hash(res) if isinstance(res, dict) else None})
        if isinstance(res, dict):
            if res.get("engine_offline"):
                holder["engine_offline"] = True
            card = res.pop("_card", None) if name in ("simulate", "simulate_2d") else None
            if card:
                key = tuple(card.get("rules") or []) + (card.get("input_symbol") or "", card.get("realized_gate") or "")
                if key not in seen:
                    seen.add(key); holder["verified"].append(card)
                    holder["family"] = card.get("family") or holder["family"] or "dose_shape"
        return res
    runner = _run_anthropic if cfg["provider"] == "anthropic" else _run_openai
    try:
        final, history = runner(history, message, dispatch, cfg)
    except Exception as e:
        trace["final_response"] = {"kind": "error", "n_cards": 0, "abstained": True, "engine_offline": holder["engine_offline"]}
        trace["failure_tags"].append("llm_backend_error"); _write_trace(trace)
        return {"kind": "error", "reply": f"LLM backend error: {type(e).__name__}: {e}",
                "family": None, "cards": [], "info": {}, "state": state, "trace_id": trace["trace_id"]}
    info = dict(holder["info"])
    if holder["engine_offline"]:
        info["engine_offline"] = True
    # Curate the cards: kd-exploration can simulate a dozen candidates; show the BEST, shape-diverse
    # few (≤6, ≤2 per shape, ranked by support/margin) rather than every exploration step.
    cards = holder["verified"]
    if len(cards) > 6:
        info["explored"] = len(cards)
        def _qual(c):
            return c.get("shape_support") if c.get("shape_support") is not None else (c.get("margin_decades") or 0)
        kept, per_shape = [], {}
        for c in sorted(cards, key=lambda c: -(_qual(c) or 0)):
            sh = c.get("dominant_shape") or c.get("realized_gate") or "?"
            if per_shape.get(sh, 0) >= 2:
                continue
            per_shape[sh] = per_shape.get(sh, 0) + 1
            kept.append(c)
            if len(kept) >= 6:
                break
        cards = kept
    # Finalize the trace: spill each shown card's full artifact, record compact summaries + failure tags.
    trace["family"] = holder["family"]
    trace["final_response"] = {"kind": "agent", "n_cards": len(cards),
                               "abstained": (not cards and not holder["engine_offline"]),
                               "engine_offline": holder["engine_offline"],
                               "cards": [_card_summary(c, _spill_card(c)) for c in cards]}
    if holder["engine_offline"]:
        trace["failure_tags"].append("engine_offline")
    elif holder["family"] and not cards:
        trace["failure_tags"].append("no_candidate")
    best = max((c.get("shape_support") for c in cards if c.get("shape_support") is not None), default=None)
    if best is not None and best < 0.5:
        trace["failure_tags"].append("weak_evidence")
    if info.get("explored"):
        trace["explored"] = info["explored"]
    _write_trace(trace)
    return {"kind": "agent", "reply": final, "family": holder["family"], "cards": cards,
            "info": info, "state": {"history": history[-12:]}, "trace_id": trace["trace_id"]}

if __name__ == "__main__":   # CLI smoke: BNE_LLM_* env configures the key; reads stdin lines
    cfg = L.llm_config_from_env()
    st = {}
    for ln in sys.stdin:
        ln = ln.strip()
        if not ln or ln.startswith("#"):
            continue
        r = run_turn(st, ln, cfg)
        st = r.get("state") or st
        print(f"\n> {ln}\n[{r.get('kind')}] {r.get('reply')}")
        for c in (r.get("cards") or [])[:3]:
            print("   card:", c.get("verdict"), "support", c.get("shape_support"), "rules", c.get("rules"),
                  "pts", len(c.get("computed_series") or []))
