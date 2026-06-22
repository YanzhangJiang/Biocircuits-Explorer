"""fnrisk_model.py — Atlas false-negative RISK model.

Given the atlas's K=16 observation (k hits for a behaviour), what is P(the (G,o) is actually robustly
realizable)? Reads a probe run stratified by k (k recovered from P_S_atlas = k/16), and per k reports the
realizable rate (C_S(0.7)>=0.6) with a Wilson 95% CI and the robust-pocket prevalence Q_S — the calibrated
false-negative risk. Also overlays the Jeffreys posterior on the true support p given k/16 (the
sampling-uncertainty the atlas itself carries).

    python3 fnrisk_model.py --probe /tmp/topo_fnrisk_all.jsonl --out benchmarks/reports/fnrisk_model.json \
        --fig doc/figures/fnrisk_model.pdf
"""
import argparse, json, math, os
import numpy as np


def wilson(k, n, z=1.96):
    if n == 0:
        return (0.0, 0.0, 1.0)
    p = k / n; d = 1 + z * z / n
    c = (p + z * z / (2 * n)) / d
    h = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / d
    return p, max(0.0, c - h), min(1.0, c + h)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--probe", required=True)
    ap.add_argument("--out", default="benchmarks/reports/fnrisk_model.json")
    ap.add_argument("--fig", default="doc/figures/fnrisk_model.pdf")
    a = ap.parse_args()
    rows = [json.loads(l) for l in open(a.probe) if l.strip()]
    rows = [r for r in rows if "by_impl" in r and "0.7" in r["by_impl"]]
    for r in rows:
        r["k"] = round(16 * (r.get("P_S_atlas") or 0))
        r["C"] = r["by_impl"]["0.7"]["C_S"]; r["Q"] = r["by_impl"]["0.7"]["frac_robust"]
    ks = sorted({r["k"] for r in rows})
    print(f"n={len(rows)}  k-buckets={ks}")
    print(f"{'k':>2} {'support':>8} {'n':>4} {'realizable':>10} {'Wilson95':>16} {'meanQ_S':>8} {'medQ_S':>7}")
    rep = {"schema": "bne-fnrisk-model/v0.1.0", "n": len(rows), "realize_impl": 0.7, "tau": 0.6,
           "note": "P(robustly realizable | atlas K=16 hit-count k). realizable=C_S(0.7)>=0.6 at n_kd=64 "
                   "(detection-limited: a lower bound for the rarest pockets). Q_S=robust-pocket prevalence.",
           "by_k": {}}
    xs, ys, los, his = [], [], [], []
    for k in ks:
        b = [r for r in rows if r["k"] == k]
        nre = sum(1 for r in b if r["C"] >= 0.6)
        p, lo, hi = wilson(nre, len(b))
        qs = [r["Q"] for r in b]
        print(f"{k:>2} {k/16:>8.4f} {len(b):>4} {nre:>4}/{len(b):>3} ={p:>5.1%} [{lo:>5.1%},{hi:>5.1%}] "
              f"{np.mean(qs):>8.3f} {np.median(qs):>7.3f}")
        rep["by_k"][str(k)] = {"support": round(k / 16, 4), "n": len(b), "realizable": nre,
                               "realizable_rate": round(p, 4), "wilson95": [round(lo, 4), round(hi, 4)],
                               "mean_Q_S": round(float(np.mean(qs)), 4), "median_Q_S": round(float(np.median(qs)), 4)}
        xs.append(k); ys.append(p); los.append(p - lo); his.append(hi - p)
    # headline: the false-negative risk at k=0 (atlas "never")
    if "0" in rep["by_k"]:
        z = rep["by_k"]["0"]
        print(f"\nFALSE-NEGATIVE RISK at k=0 (atlas says never): P(realizable)={z['realizable_rate']:.1%} "
              f"CI{z['wilson95']} (n_kd=64 lower bound). Risk -> negligible once k>=~{ks[min(2,len(ks)-1)]}.")
    os.makedirs(os.path.dirname(a.out), exist_ok=True)
    json.dump(rep, open(a.out, "w"), indent=2)
    print(f"wrote {a.out}")
    try:
        import matplotlib; matplotlib.use("Agg"); import matplotlib.pyplot as plt
        fig, ax = plt.subplots(figsize=(5.2, 3.4))
        ax.errorbar(xs, ys, yerr=[np.clip(los, 0, None), np.clip(his, 0, None)], fmt="o-",
                    color="#1f77b4", capsize=3, label="P(realizable) (n_kd=64)")
        # Jeffreys posterior mean of true support given k/16 (the atlas's own sampling uncertainty)
        from scipy.stats import beta
        jm = [beta.mean(k + 0.5, 16 - k + 0.5) for k in xs]
        ax.plot(xs, jm, "s--", color="#888", label="Jeffreys E[true support | k/16]")
        ax.set_xlabel("atlas K=16 hit-count k  (observed support = k/16)")
        ax.set_ylabel("probability"); ax.set_ylim(-0.03, 1.03)
        ax.set_title("Atlas false-negative risk model: P(robustly realizable | k)")
        ax.legend(fontsize=8); ax.grid(alpha=0.3); fig.tight_layout()
        os.makedirs(os.path.dirname(a.fig), exist_ok=True); fig.savefig(a.fig, dpi=150)
        print(f"wrote {a.fig}")
    except Exception as e:
        print(f"(figure skipped: {e})")


if __name__ == "__main__":
    main()
