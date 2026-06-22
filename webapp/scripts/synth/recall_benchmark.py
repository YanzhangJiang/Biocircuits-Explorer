"""recall_benchmark.py — Atlas false-negative / recall benchmark.

Quantifies how much a coarse label/shape_support retriever MISSES vs. a label+recall-probe retriever, on
the engine-verified ground truth of *robust realizability*.

Ground truth Y(G,o) = the in-process probe finds a robust pocket: C_S(impl=realize_impl) >= tau_real
(a kd-centre whose +/-impl neighbourhood is >= tau_real the target). Label retrieval returns (G,o) when the
atlas shape_support P_S_atlas >= s. The recall gap = realizable designs the label index misses; the
recall-probe recovers them (all engine-verified).

Two populations:
  --low  : probe results on topos the atlas scored bandpass_support>0 but LOW (the decoupling regime)
  --zero : probe results on a RANDOM sample of atlas-says-zero topos (shape_support==0; label returns none)
The --zero set + --zero-pop (count of shape_support==0 topos atlas-wide) give an atlas-wide projection.

    python3 recall_benchmark.py --low benchmarks/reports/topo_capacity_bandpass.jsonl \
        --zero /tmp/topo_cs_zero.jsonl --zero-pop 4538 --realize-impl 0.5 --tau-real 0.6
"""
import argparse, json
import numpy as np


def load(path):
    return [json.loads(l) for l in open(path) if l.strip() and "by_impl" in json.loads(l)]


def realize(r, impl, tau):
    return r["by_impl"][str(impl)]["C_S"] >= tau


def qs(r, impl):
    return r["by_impl"][str(impl)]["frac_robust"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--low", required=True)
    ap.add_argument("--zero", default=None)
    ap.add_argument("--zero-pop", type=int, default=0, help="atlas-wide count of shape_support==0 topos")
    ap.add_argument("--realize-impl", type=float, default=0.5)
    ap.add_argument("--tau-real", type=float, default=0.6)
    ap.add_argument("--out", default="benchmarks/reports/recall_benchmark.json")
    a = ap.parse_args()
    imp, tau = a.realize_impl, a.tau_real
    low = load(a.low)
    zero = load(a.zero) if a.zero else []
    rep = {"schema": "bne-recall-benchmark/v0.1.0", "realize_impl": imp, "tau_real": tau,
           "note": "Y = robust realizability (C_S(impl)>=tau via the in-process probe). Label retrieval = P_S_atlas>=s."}

    # --- LOW-support regime (decoupling sample; over-represents low P_S by design) ---
    Ylow = [r for r in low if realize(r, imp, tau)]
    print(f"=== LOW-support regime (n={len(low)}, realizable Y={len(Ylow)}) ===")
    print(f"{'s (label thresh)':>16} {'label recall':>13} {'missed (FN)':>12} {'+probe recall':>13}")
    grid = [1e-9, 0.0625, 0.125, 0.1875, 0.375]
    rep["low"] = {"n": len(low), "realizable": len(Ylow), "by_threshold": {}}
    for s in grid:
        ret = [r for r in Ylow if r["P_S_atlas"] >= s]
        rec = len(ret) / len(Ylow) if Ylow else float("nan")
        fn = len(Ylow) - len(ret)
        print(f"{s:>16.4g} {rec:>13.2%} {fn:>12} {'100.00%':>13}")
        rep["low"]["by_threshold"][f"{s:.4g}"] = {"label_recall": round(rec, 4), "missed": fn, "probe_recall": 1.0}

    # --- ZERO-support population (atlas says shape_support==0; label returns NOTHING) ---
    if zero:
        Yz = [r for r in zero if realize(r, imp, tau)]
        rate = len(Yz) / len(zero)
        # tighten the realizability call with Q_S too (robustness-aware, not C_S-saturated)
        Yz_q = [r for r in zero if qs(r, imp) >= 0.1]
        print(f"\n=== ZERO-support population (atlas says NEVER bandpass; label retrieves 0) ===")
        print(f"  probed n={len(zero)};  realizable (C_S(impl={imp})>={tau}): {len(Yz)} ({rate:.0%})"
              f";  stricter (Q_S>=0.1): {len(Yz_q)}")
        print(f"  => label retrieval recall on this population = 0%; recall-probe recovers {len(Yz)} "
              f"verified designs the atlas labelled impossible.")
        rep["zero"] = {"probed": len(zero), "realizable": len(Yz), "realizable_rate": round(rate, 4),
                       "realizable_strict_Qge0.1": len(Yz_q),
                       "recovered_cases": [{"input_symbol": r["input_symbol"], "observe_species": r["observe_species"],
                                            "dominant": r.get("dominant"), "C_S": r["by_impl"][str(imp)]["C_S"],
                                            "Q_S": qs(r, imp), "reactions": r.get("reactions")} for r in Yz]}
        if a.zero_pop:
            proj = int(round(a.zero_pop * rate))
            print(f"  PROJECTION: atlas has {a.zero_pop} shape_support==0 (G,o) for bandpass; at rate {rate:.0%} "
                  f"=> ~{proj} are robustly realizable but invisible to label retrieval.")
            rep["zero"]["atlas_zero_population"] = a.zero_pop
            rep["zero"]["projected_false_negatives"] = proj

    # --- combined headline ---
    allr = Ylow + ([r for r in zero if realize(r, imp, tau)] if zero else [])
    if allr:
        s0 = 0.0625
        lab = sum(1 for r in allr if r["P_S_atlas"] >= s0)
        print(f"\nHEADLINE: of {len(allr)} engine-verified robustly-realizable bandpass designs in the union, "
              f"label retrieval @P_S>= {s0} returns {lab} ({lab/len(allr):.0%}); label+probe returns all "
              f"({len(allr)}, 100%).")
        rep["headline"] = {"realizable_union": len(allr), "label_at_0.0625": lab,
                           "label_recall": round(lab / len(allr), 4), "probe_recall": 1.0}
    import os
    os.makedirs(os.path.dirname(a.out), exist_ok=True)
    json.dump(rep, open(a.out, "w"), indent=2)
    print(f"\nwrote {a.out}")


if __name__ == "__main__":
    main()
