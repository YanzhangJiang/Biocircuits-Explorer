"""analyze_topo_capacity.py — Phase-1 gate analysis over the in-process C_S sweep.

Reads a JSONL (rows with P_S, P_S_atlas, by_impl{impl->{C_S,mean_r,frac_robust}}, dominant, volume_mean)
and answers: does local robust-capacity C_S carry signal beyond global support P_S, and at what
implementation-error level does C_S start to discriminate topologies (the precision knee)?

    python3 analyze_topo_capacity.py /tmp/topo_cs_sweep.jsonl [--out benchmarks/reports/topo_capacity_gate.json]
"""
import sys, json, argparse
import numpy as np

try:
    from scipy.stats import spearmanr, pearsonr
    def sp(a, b): return float(spearmanr(a, b).correlation)
    def pe(a, b): return float(pearsonr(a, b)[0])
except Exception:
    def sp(a, b): return float("nan")
    def pe(a, b):
        a, b = np.asarray(a, float), np.asarray(b, float)
        return float(np.corrcoef(a, b)[0, 1])


def load(path):
    rows = []
    for l in open(path):
        l = l.strip()
        if not l: continue
        r = json.loads(l)
        if "by_impl" in r: rows.append(r)
    return rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("jsonl")
    ap.add_argument("--out", default="benchmarks/reports/topo_capacity_gate.json")
    a = ap.parse_args()
    rows = load(a.jsonl)
    impls = sorted({float(k) for r in rows for k in r["by_impl"]}, )
    P = np.array([r["P_S"] for r in rows])
    Pa = np.array([r["P_S_atlas"] for r in rows], float)
    db = [r for r in rows if r.get("dominant") == "bandpass_with_plateau" and r.get("volume_mean") is not None]
    print(f"n={len(rows)}  impls={impls}  dominant-bandpass={len(db)}")
    print(f"recomputed-P_S vs atlas-P_S: pearson={pe(P,Pa):.3f} spearman={sp(P,Pa):.3f}  (sanity)")
    print(f"\n{'impl':>5} {'sp(C,P)':>8} {'pe(C,P)':>8} {'lowP&hiC':>9} {'medianC':>8} {'meanC':>7} "
          f"{'sat>=1':>7} {'C=0':>5} {'ROPvol~C(dbp)':>13}")
    report = {"schema": "bne-topo-capacity-gate/v0.1.0", "n": len(rows), "impls": impls,
              "pipeline_consistency_pearson": round(pe(P, Pa), 4), "per_impl": {}, "decoupling_cases": []}
    for im in impls:
        k = str(im)
        C = np.array([r["by_impl"][k]["C_S"] for r in rows])
        lowhi = int(np.sum((P <= 0.40) & (C >= 0.60)))
        sat = int(np.sum(C >= 0.999)); zero = int(np.sum(C <= 1e-9))
        Cb = np.array([r["by_impl"][k]["C_S"] for r in db]); Vb = np.array([r["volume_mean"] for r in db], float)
        ropc = sp(Vb, Cb) if len(set(Cb.tolist())) > 1 else float("nan")
        spc = sp(P, C) if len(set(C.tolist())) > 1 else float("nan")
        print(f"{im:>5} {spc:>8.3f} {pe(P,C):>8.3f} {lowhi:>6}/{len(rows)} {np.median(C):>8.3f} "
              f"{C.mean():>7.3f} {sat:>4}/{len(rows)} {zero:>5} {ropc:>13.3f}")
        report["per_impl"][k] = {"spearman_C_P": round(spc, 4), "pearson_C_P": round(pe(P, C), 4),
                                 "lowP_highC": lowhi, "median_C": round(float(np.median(C)), 4),
                                 "mean_C": round(float(C.mean()), 4), "saturated_ge1": sat, "C_zero": zero,
                                 "ROPvol_spearman_C_dombp": None if ropc != ropc else round(ropc, 4)}
    # decisive decoupling: globally-rare (P_S<=0.125) yet locally-robust (C_S>=0.8 at the tightest impl)
    tight = str(impls[0])
    dec = sorted([r for r in rows if r["P_S"] <= 0.125 and r["by_impl"][tight]["C_S"] >= 0.8],
                 key=lambda r: (r["P_S"], -r["by_impl"][tight]["C_S"]))
    print(f"\nDECOUPLING (P_S<=0.125 & C_S>=0.8 @impl={tight}): {len(dec)} topologies"
          f"  <-- shape_support ranking would FALSELY demote these")
    for r in dec[:12]:
        cs_by = {k: r["by_impl"][k]["C_S"] for k in sorted(r["by_impl"], key=float)}
        print(f"   P_S={r['P_S']:.3f} dom={r['dominant']:<22} out={r['observe_species']:<14} C_S(impl)={cs_by}")
        report["decoupling_cases"].append({"P_S": r["P_S"], "P_S_atlas": r["P_S_atlas"], "dominant": r["dominant"],
                                           "input_symbol": r["input_symbol"], "observe_species": r["observe_species"],
                                           "C_S_by_impl": cs_by, "reactions": r.get("reactions")})
    # precision knee = first impl where median C_S drops below 0.5 (robust pocket "mostly broken")
    knee = next((im for im in impls if np.median([r["by_impl"][str(im)]["C_S"] for r in rows]) < 0.5), None)
    report["precision_knee_median_below_0.5"] = knee
    print(f"\nprecision knee (median C_S < 0.5) at impl = {knee} decades")
    import os
    os.makedirs(os.path.dirname(a.out), exist_ok=True)
    json.dump(report, open(a.out, "w"), indent=2)
    print(f"\nwrote {a.out}")


if __name__ == "__main__":
    main()
