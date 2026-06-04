#!/usr/bin/env python3
# Track-A (deterministic MVP) corpus retrieval: given a FLAT behavior-spec query
# (the lowered verifier-query form used by benchmarks/tasks/*.json), search the
# phenotyped dataset (dataset.jsonl labels) and return ranked candidate cards.
#
# This is the retrieval/prefilter tier: it scores against the PRECOMPUTED labels
# (default-prior Π), instantly, with no solver. Gate logic mirrors
# run_benchmark.jl:evaluate_gates (q_alpha for *_min, median for *_max/range, the
# shape_support gate). Final Tier-2/3 verification of the top-k still goes through
# the phenotyper; this tier just finds the candidates fast.
#
#   python3 behavior_search.py --dataset datasets/latent-atlas-v0 \
#       --behavior-class bandpass_with_plateau --max-reactions 4 \
#       --rise-slope-max 0.8 --fall-slope-min 1 --min-robustness 0.2 --top 10
import sys, os, json, glob, argparse

# user-language aliases -> vocabulary key (mirrors run_benchmark.jl CLASS_MAP)
CLASS_MAP = {
    "monotone_activation": "monotone_activation",
    "activation_with_saturation": "monotone_activation",
    "monotone_repression": "monotone_repression",
    "repression_with_floor": "monotone_repression",
    "thresholded_activation": "thresholded_activation",
    "biphasic_peak": "biphasic_peak",
    "bandpass_with_plateau": "bandpass_with_plateau",
    "bandpass_like": "bandpass_with_plateau",
    "biphasic_valley": "biphasic_valley",
    "window_repression": "biphasic_valley",
}

def iter_rows(path):
    if os.path.isdir(path):
        ds = os.path.join(path, "dataset.jsonl")
        files = [ds] if os.path.isfile(ds) else sorted(glob.glob(os.path.join(path, "shard_*.jsonl")))
    else:
        files = [path]
    for f in files:
        with open(f) as fh:
            for line in fh:
                line = line.strip()
                if line:
                    try: yield json.loads(line)
                    except Exception: pass

def num(x):
    return x if isinstance(x, (int, float)) else None

def evaluate(row, q, cls_key):
    """Return (shape_support, hard_pass, design_score, objective, reasons)."""
    sf = row.get("shape_fractions") or {}
    med = row.get("metrics_median") or {}
    qa = row.get("metrics_q_alpha") or {}
    ss = num(sf.get(cls_key))
    ss = 0.0 if ss is None else ss
    reasons = []; ngates = 0; obj = 0.0
    # shape_support gate
    ngates += 1
    if ss < q["min_robustness"]:
        reasons.append(f"shape_support {ss:.3f} < {q['min_robustness']}")
    def gate_min(key, metric):
        nonlocal ngates, obj
        if q.get(key) is not None:
            ngates += 1; v = num(qa.get(metric)); m = q[key]
            obj += (v - m) if v is not None else -10.0
            if not (v is not None and v >= m):
                reasons.append(f"{metric}.q_alpha {v} < {m}")
    def gate_max(key, metric):
        nonlocal ngates, obj
        if q.get(key) is not None:
            ngates += 1; v = num(med.get(metric)); m = q[key]
            obj += (m - v) if v is not None else -10.0
            if not (v is not None and v <= m):
                reasons.append(f"{metric}.median {v} > {m}")
    gate_min("fall_slope_min", "fall_slope")
    gate_min("plateau_width_log10_input_min", "plateau_width_log10_input")
    gate_min("peak_prominence_min", "peak_prominence")
    gate_max("rise_slope_max", "rise_slope")
    gate_max("baseline_return_max", "baseline_return")
    if q.get("dynamic_range_log10") is not None:
        ngates += 1; lo, hi = q["dynamic_range_log10"]; v = num(med.get("output_fold_change_log10"))
        obj += -abs(v - (lo+hi)/2) if v is not None else -10.0
        if not (v is not None and lo <= v <= hi):
            reasons.append(f"output_fold_change_log10.median {v} not in [{lo},{hi}]")
    design = 1.0 if ngates == 0 else 1.0 - len(reasons)/ngates
    return ss, (len(reasons) == 0), design, obj, reasons

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dataset", default="datasets/latent-atlas-v0")
    ap.add_argument("--behavior-class", required=True)
    ap.add_argument("--max-reactions", type=int, default=99)
    ap.add_argument("--rise-slope-max", type=float)
    ap.add_argument("--fall-slope-min", type=float)
    ap.add_argument("--plateau-width-log10-input-min", type=float)
    ap.add_argument("--peak-prominence-min", type=float)
    ap.add_argument("--baseline-return-max", type=float)
    ap.add_argument("--dynamic-range-log10", type=float, nargs=2)
    ap.add_argument("--min-robustness", type=float, default=0.2)
    ap.add_argument("--top", type=int, default=10)
    a = ap.parse_args()
    cls_key = CLASS_MAP.get(a.behavior_class)
    if cls_key is None:
        sys.exit(f"unknown behavior_class '{a.behavior_class}'; known: {sorted(set(CLASS_MAP))}")
    q = {"min_robustness": a.min_robustness,
         "fall_slope_min": a.fall_slope_min,
         "plateau_width_log10_input_min": a.plateau_width_log10_input_min,
         "peak_prominence_min": a.peak_prominence_min,
         "rise_slope_max": a.rise_slope_max,
         "baseline_return_max": a.baseline_return_max,
         "dynamic_range_log10": a.dynamic_range_log10}
    results = []
    for r in iter_rows(a.dataset):
        if r.get("n_reactions", 99) > a.max_reactions: continue
        ss, hard, design, obj, reasons = evaluate(r, q, cls_key)
        results.append((ss, obj, hard, design, r, reasons))
    # robust-first: shape_support, then objective
    results.sort(key=lambda t: (-t[0], -t[1]))
    n_pass = sum(1 for t in results if t[2])
    print(f"# behavior_class={a.behavior_class}->{cls_key}  candidates_considered={len(results)}  "
          f"hard_pass={n_pass}  (retrieval over labels; verify top-k with phenotyper)")
    for ss, obj, hard, design, r, reasons in results[:a.top]:
        mm = r.get("metrics_median") or {}
        print(f"\n[{'PASS' if hard else 'near'}] ss={ss:.3f} design={design:.2f} "
              f"r={r.get('n_reactions')} in={r.get('input_symbol')} out={r.get('output_symbol')}")
        print(f"   metrics(median): rise={mm.get('rise_slope')} fall={mm.get('fall_slope')} "
              f"plateau={mm.get('plateau_width_log10_input')} fold={mm.get('output_fold_change_log10')}")
        print(f"   atlas_vol={r.get('atlas_volume_mean')} robust_paths={r.get('atlas_robust_path_count')}")
        if reasons: print(f"   misses: {reasons}")
        print(f"   network: {r.get('network_id')}")

if __name__ == "__main__":
    main()
