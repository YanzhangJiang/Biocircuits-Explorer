#!/usr/bin/env python3
# Track-A (Phase 5) deterministic MVP core: a behavior request -> ranked candidate
# cards over the phenotyped corpus, WITH evidence tiers and a relaxation/tradeoff
# dialogue when nothing passes. Accepts either the NL-facing NESTED behavior_spec
# (schemas/behavior-spec.schema.json) or the FLAT verifier query (benchmark-task
# form); a NESTED spec is LOWERED to the flat query here (the Phase-5 lowering step).
# The NL->nested compile is done by the LLM agent upstream; this is the deterministic
# tail (lower -> retrieve -> rank -> cards -> relaxation).
#
#   python3 design_search.py --dataset datasets/latent-atlas-v0 --spec my_spec.json
#   python3 design_search.py --dataset ... --behavior-class bandpass_with_plateau \
#       --max-reactions 4 --rise-slope-max 0.8 --fall-slope-min 1 --top 5
import sys, os, json, glob, argparse, hashlib, datetime

CLASS_MAP = {
    "monotone_activation":"monotone_activation","activation_with_saturation":"monotone_activation",
    "monotone_repression":"monotone_repression","repression_with_floor":"monotone_repression",
    "thresholded_activation":"thresholded_activation","biphasic_peak":"biphasic_peak",
    "bandpass_with_plateau":"bandpass_with_plateau","bandpass_like":"bandpass_with_plateau",
    "biphasic_valley":"biphasic_valley","window_repression":"biphasic_valley",
}
# flat gate key -> (label-metric, which label stat, direction)  [mirrors run_benchmark gates]
MIN_GATES = {"fall_slope_min":("fall_slope","q"), "plateau_width_log10_input_min":("plateau_width_log10_input","q"),
             "peak_prominence_min":("peak_prominence","q")}
MAX_GATES = {"rise_slope_max":("rise_slope","m"), "baseline_return_max":("baseline_return","m")}

def lower_spec(spec):
    """Nested behavior_spec -> flat verifier query. If already flat, pass through."""
    if "goal" not in spec:           # already flat (benchmark-task form)
        return dict(spec)
    g = spec.get("goal", {}); sp = spec.get("shape_preferences", {})
    nc = spec.get("network_constraints", {}); vp = spec.get("verification_policy", {})
    flat = {"behavior_class": g.get("behavior_class")}
    if "max_reactions" in nc: flat["max_reactions"] = nc["max_reactions"]
    if "kd_profile" in nc: flat["kd_profile"] = nc["kd_profile"]
    def term(name): return sp.get(name, {}) or {}
    if "max" in term("rise_slope"): flat["rise_slope_max"] = term("rise_slope")["max"]
    if "min" in term("fall_slope"): flat["fall_slope_min"] = term("fall_slope")["min"]
    if "min" in term("plateau_width_log10_input"): flat["plateau_width_log10_input_min"] = term("plateau_width_log10_input")["min"]
    if "min" in term("peak_prominence"): flat["peak_prominence_min"] = term("peak_prominence")["min"]
    if "max" in term("baseline_return"): flat["baseline_return_max"] = term("baseline_return")["max"]
    dr = sp.get("dynamic_range_log10") or sp.get("output_fold_change_log10")
    if isinstance(dr, dict) and "range" in dr: flat["dynamic_range_log10"] = dr["range"]
    if "min_robustness_score" in vp: flat["min_robustness"] = vp["min_robustness_score"]
    return flat

def iter_rows(path):
    ds = os.path.join(path, "dataset.jsonl")
    files = [ds] if os.path.isfile(ds) else sorted(glob.glob(os.path.join(path, "shard_*.jsonl")))
    for f in (files or [path]):
        with open(f) as fh:
            for line in fh:
                line = line.strip()
                if line:
                    try: yield json.loads(line)
                    except Exception: pass

def num(x): return float(x) if isinstance(x,(int,float)) else None

def label_stat(row, metric, kind):  # kind: 'q' (q_alpha) or 'm' (median)
    d = (row.get("metrics_q_alpha") if kind=="q" else row.get("metrics_median")) or {}
    return num(d.get(metric))

def eval_row(row, flat, cls_key):
    ss = num((row.get("shape_fractions") or {}).get(cls_key)) or 0.0
    reasons = []
    if ss < flat.get("min_robustness", 0.2): reasons.append(("shape_support", ss, flat.get("min_robustness",0.2), "min"))
    for k,(metric,kind) in MIN_GATES.items():
        if k in flat:
            v = label_stat(row, metric, kind)
            if not (v is not None and v >= flat[k]): reasons.append((metric, v, flat[k], "min"))
    for k,(metric,kind) in MAX_GATES.items():
        if k in flat:
            v = label_stat(row, metric, kind)
            if not (v is not None and v <= flat[k]): reasons.append((metric, v, flat[k], "max"))
    if "dynamic_range_log10" in flat:
        lo,hi = flat["dynamic_range_log10"]; v = label_stat(row,"output_fold_change_log10","m")
        if not (v is not None and lo<=v<=hi): reasons.append(("output_fold_change_log10", v, (lo,hi), "range"))
    return ss, reasons

EVIDENCE_TIER = "tier-1-scan-observed"  # label retrieval; exact ROP verify = tier-2/3, run separately

def reason_to_dict(m, v, t, d):
    return {"metric": m, "observed": v, "threshold": t,
            "relation": {"min": ">=", "max": "<=", "range": "in"}[d]}

def build_card(ss, nmiss, r, reasons):
    mm = r.get("metrics_median") or {}
    return {
        "verdict": "pass" if nmiss == 0 else "near_miss",
        "evidence_tier": EVIDENCE_TIER,
        "shape_support": round(ss, 4),
        "n_reactions": r.get("n_reactions"),
        "input_symbol": r.get("input_symbol"), "output_symbol": r.get("output_symbol"),
        "atlas_volume_mean": r.get("atlas_volume_mean"),
        "metrics_median": {k: mm.get(k) for k in
                           ("rise_slope", "fall_slope", "plateau_width_log10_input", "output_fold_change_log10")},
        "network_id": r.get("network_id"), "slice_id": r.get("slice_id"),
        "rules": r.get("rules"), "dominant_shape": r.get("dominant_shape"),
        "misses": [reason_to_dict(*x) for x in reasons],
    }

def compute_relaxation(flat, scored):
    """Single-blocker relaxation suggestions + an 'absent shape' flag. Shared by text/JSON."""
    out = []
    gate_keys = [k for k in flat if k in MIN_GATES or k in MAX_GATES or k == "dynamic_range_log10"]
    for gk in gate_keys:
        metric = (MIN_GATES.get(gk, MAX_GATES.get(gk, ("output_fold_change_log10",))))[0]
        single = [(ss, r, fails[0]) for ss, nmiss, r, reasons in scored[:200]
                  for fails in [[x for x in reasons if x[0] != "shape_support"]]
                  if len(fails) == 1 and fails[0][0] == metric]
        vals = [s[2][1] for s in single if s[2][1] is not None]
        if single and vals:
            if gk in MIN_GATES: relaxed = round(max(vals), 3); direction = "lower"
            elif gk in MAX_GATES: relaxed = round(min(vals), 3); direction = "raise"
            else: relaxed = None; direction = "widen"
            out.append({"gate": gk, "direction": direction, "from": flat.get(gk),
                        "to": relaxed, "admits": len(single), "sole_blocker": True})
    absent = all(t[0] < flat["min_robustness"] for t in scored[:5])
    return out, absent

def dataset_input_hashes(dataset_path):
    mf = os.path.join(dataset_path, "manifest.json")
    if os.path.isfile(mf):
        try:
            d = json.load(open(mf))
            sa = d.get("source_atlas") or {}
            return {"dataset_id": d.get("dataset_id"),
                    "source_atlas_sha256": sa.get("sqlite_snapshot_hash") or sa.get("sha256")}
        except Exception:
            pass
    return {}

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dataset", default="datasets/latent-atlas-v0")
    ap.add_argument("--spec"); ap.add_argument("--behavior-class"); ap.add_argument("--max-reactions", type=int)
    ap.add_argument("--rise-slope-max", type=float); ap.add_argument("--fall-slope-min", type=float)
    ap.add_argument("--plateau-width-log10-input-min", type=float); ap.add_argument("--peak-prominence-min", type=float)
    ap.add_argument("--baseline-return-max", type=float); ap.add_argument("--dynamic-range-log10", type=float, nargs=2)
    ap.add_argument("--min-robustness", type=float, default=0.2); ap.add_argument("--top", type=int, default=5)
    ap.add_argument("--json", action="store_true", help="emit a bne-result/v1.0.0 candidate-card envelope")
    a = ap.parse_args()
    if a.spec:
        flat = lower_spec(json.load(open(a.spec)))
    else:
        flat = {"behavior_class": a.behavior_class, "min_robustness": a.min_robustness}
        for k,v in (("max_reactions",a.max_reactions),("rise_slope_max",a.rise_slope_max),("fall_slope_min",a.fall_slope_min),
                    ("plateau_width_log10_input_min",a.plateau_width_log10_input_min),("peak_prominence_min",a.peak_prominence_min),
                    ("baseline_return_max",a.baseline_return_max),("dynamic_range_log10",a.dynamic_range_log10)):
            if v is not None: flat[k]=v
    flat.setdefault("min_robustness",0.2); flat.setdefault("max_reactions",99)
    cls_key = CLASS_MAP.get(flat.get("behavior_class"))
    if not cls_key: sys.exit(f"unknown/absent behavior_class: {flat.get('behavior_class')}")
    pool = [r for r in iter_rows(a.dataset) if r.get("n_reactions", 99) <= flat["max_reactions"]]
    scored = [(ss, len(reasons), r, reasons)
              for r in pool for (ss, reasons) in [eval_row(r, flat, cls_key)]]
    scored.sort(key=lambda t: (-t[0], t[1]))
    passing = [t for t in scored if t[1] == 0]
    cards = [build_card(*t) for t in (passing[:a.top] if passing else scored[:a.top])]
    relax, absent = compute_relaxation(flat, scored) if not passing else ([], False)

    if a.json:
        payload = {
            "query": flat, "corpus_size": len(pool), "hard_pass": len(passing),
            "behavior_class": flat.get("behavior_class"), "shape_key": cls_key,
            "status": "verified_candidates" if passing else "no_hard_pass",
            "candidates": cards,
            "relaxation": (None if passing else {"suggestions": relax, "shape_absent_in_scope": absent}),
        }
        cfg = hashlib.sha256(json.dumps(flat, sort_keys=True).encode()).hexdigest()
        env = {
            "artifact_schema_version": "bne-result/v1.0.0", "kind": "design_search",
            "input_hashes": dataset_input_hashes(a.dataset),
            "algorithm": {"name": "design_search", "version": "0.1.0", "config_hash": "sha256:" + cfg},
            "created_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "result": payload,
        }
        print(json.dumps(env, indent=2))
        return

    # ── human-readable ──
    print(f"# lowered query: {json.dumps(flat)}")
    print(f"# corpus={len(pool)} candidates (class={cls_key}, r<={flat['max_reactions']}); hard_pass={len(passing)}\n")
    print(f"## {'VERIFIED candidates' if passing else 'CLOSEST candidates (none fully pass — see relaxation)'} [{EVIDENCE_TIER}]")
    for c in cards:
        mm = c["metrics_median"]
        print(f"\n[{'PASS' if c['verdict']=='pass' else 'near'}] shape_support={c['shape_support']:.3f} r={c['n_reactions']} "
              f"in={c['input_symbol']} out={c['output_symbol']} atlas_vol={c['atlas_volume_mean']}")
        print(f"   rise={mm['rise_slope']} fall={mm['fall_slope']} plateau={mm['plateau_width_log10_input']} fold={mm['output_fold_change_log10']}")
        if c["misses"]:
            print("   misses: " + "; ".join(f"{x['metric']}{x['relation']}{x['threshold']} (obs {x['observed']})" for x in c["misses"]))
        print(f"   network: {c['network_id']}")
    if not passing:
        print("\n## relaxation / tradeoff (no candidate meets all hard gates):")
        for s in relax:
            print(f"   - {s['direction']} {s['gate']} {s['from']} -> {s['to']}  => admits {s['admits']} sole-blocker candidate(s)")
        if absent:
            print(f"   - NOTE: even the most robust candidate has shape_support < {flat['min_robustness']} "
                  f"-> this behavior class is essentially ABSENT in the in-scope (mu<=5) family.")

if __name__ == "__main__":
    main()
