"""topo_capacity.py — Topology Robust-Capacity Sprint, Phase 1 GATE.

For each (topology, output) it estimates C_S = best LOCAL implementation robustness (max over sampled kd
of the local-perturbation bandpass fraction) and compares it to P_S = global prior support (the atlas
bandpass fraction) and to ROP evidence (volume_mean, robust_path_count).

DECISIVE QUESTION: is there a LOW-P_S / HIGH-C_S regime (=> local robustness carries signal beyond the
atlas's shape_support ranking, so the topology-capacity phase is worth it), or does C_S just track P_S
(=> P_S is a sufficient proxy and the phase reduces to existing label+support retrieval)?

Resumable: appends one JSONL row per topology to <out>; rerun the SAME command to resume after an engine
restart (it skips topologies already present).
    python3 topo_capacity.py --topos topo_capacity_input.json --n-kd 24 --K 5 --impl-noise 0.2 --out cap.jsonl
"""
from __future__ import annotations
import argparse, json, os, sys
import numpy as np

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "reader"))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
from verifier import EngineVerifier            # noqa: E402
import engine_client as E                       # noqa: E402


def _key(t):
    return json.dumps([t["reactions"], t["input_symbol"], t["observe_species"]], sort_keys=True)


def estimate_CS(V, topo, n_kd, K, lo, hi, rng):
    nrx = len(topo["reactions"]); rs = []; fails = 0
    for _ in range(n_kd):
        theta = lo + (hi - lo) * rng.random(nrx)
        ph = V.phenotype(topo, theta, K=K)
        if not isinstance(ph, dict) or ph.get("offline") or ph.get("shape_support") is None:
            return None      # engine down -> bail immediately so the caller restarts the engine + resumes
        rs.append(float(ph["shape_support"]))
    if not rs:
        return None
    a = np.array(rs)
    return {"C_S": round(float(a.max()), 3), "mean_r": round(float(a.mean()), 3),
            "frac_kd_robust": round(float((a >= 0.5).mean()), 3), "n_eval": len(rs), "n_fail": fails}


def analyze(out):
    rows = [json.loads(l) for l in open(out)] if os.path.exists(out) else []
    rows = [r for r in rows if r.get("C_S") is not None]
    if len(rows) < 4:
        print(f"(only {len(rows)} rows — not enough to analyze)"); return
    P = np.array([r["P_S"] for r in rows]); C = np.array([r["C_S"] for r in rows])
    try:
        from scipy.stats import spearmanr, pearsonr
        sp = spearmanr(P, C).correlation; pe = pearsonr(P, C)[0]
    except Exception:
        sp = pe = float("nan")
    print(f"\n=== Phase-1 GATE analysis (n={len(rows)}) ===")
    print(f"C_S vs P_S:  spearman={sp:.3f}  pearson={pe:.3f}")
    # the decisive cases: low global support but high local robustness
    low_hi = [r for r in rows if r["P_S"] <= 0.40 and r["C_S"] >= 0.60]
    print(f"LOW-P_S(<=0.40) & HIGH-C_S(>=0.60): {len(low_hi)} topologies "
          f"{'<-- local robustness ADDS signal; gate PASSES' if low_hi else '<-- none; P_S may suffice'}")
    for r in sorted(low_hi, key=lambda r: -r["C_S"])[:8]:
        print(f"   P_S={r['P_S']:.3f} C_S={r['C_S']:.3f} dom={r.get('dominant')} vol={r.get('volume_mean')} out={r['observe_species']}")
    # ROP volume vs C_S, restricted to dominant-bandpass rows (where atlas volume IS the bandpass family)
    db = [r for r in rows if r.get("dominant") == "bandpass_with_plateau" and r.get("volume_mean") is not None]
    if len(db) >= 4:
        V = np.array([r["volume_mean"] for r in db]); Cb = np.array([r["C_S"] for r in db])
        try:
            from scipy.stats import spearmanr
            print(f"ROP volume vs C_S (dominant-bandpass, n={len(db)}): spearman={spearmanr(V, Cb).correlation:.3f}")
        except Exception:
            pass
    print(f"C_S quantiles [min,p25,p50,p75,max]: {[round(float(x),3) for x in np.quantile(C,[0,.25,.5,.75,1])]}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--topos", required=True)
    ap.add_argument("--n-kd", type=int, default=24)
    ap.add_argument("--K", type=int, default=5)
    ap.add_argument("--impl-noise", type=float, default=0.2)
    ap.add_argument("--kd-lo", type=float, default=-3.0)
    ap.add_argument("--kd-hi", type=float, default=3.0)
    ap.add_argument("--out", default="/tmp/topo_capacity.jsonl")
    ap.add_argument("--seed", type=int, default=11)
    ap.add_argument("--analyze-only", action="store_true")
    a = ap.parse_args()
    if a.analyze_only:
        analyze(a.out); return 0

    _p = E.build_model(reactions=["A + B <-> C_A_B"], kd=[1.0])
    if not (isinstance(_p, dict) and _p.get("session_id")):
        print(f"engine NOT reachable: {_p}"); return 2

    topos = json.load(open(a.topos))
    done = set()
    if os.path.exists(a.out):
        for l in open(a.out):
            try: done.add(_key(json.loads(l)))
            except Exception: pass
    print(f"topos={len(topos)} already_done={len(done)} n_kd={a.n_kd} K={a.K} impl_noise={a.impl_noise}", flush=True)
    V = EngineVerifier(target_shape="bandpass_with_plateau", tau=0.5, impl_noise=a.impl_noise)
    fout = open(a.out, "a")
    for i, t in enumerate(topos):
        if _key(t) in done:
            continue
        V.cost.reset()
        est = estimate_CS(V, t, a.n_kd, a.K, a.kd_lo, a.kd_hi, np.random.default_rng(a.seed + i))
        if est is None:   # engine down -> do NOT record (so it retries on resume); stop for a restart
            print(f"  [{i+1}/{len(topos)}] BAILED (engine down) — restart engine + rerun to resume.", flush=True)
            break
        row = {"reactions": t["reactions"], "input_symbol": t["input_symbol"], "observe_species": t["observe_species"],
               "P_S": t.get("P_S"), "dominant": t.get("dominant"), "volume_mean": t.get("volume_mean"),
               "robust_path_count": t.get("robust_path_count"), "n_reactions": t.get("n_reactions"),
               "C_S": est["C_S"], "mean_r": est["mean_r"], "frac_kd_robust": est["frac_kd_robust"],
               "n_eval": est["n_eval"], "n_fail": est["n_fail"]}
        fout.write(json.dumps(row) + "\n"); fout.flush()
        print(f"  [{i+1}/{len(topos)}] P_S={t.get('P_S')} dom={t.get('dominant')} -> C_S={est['C_S']} (mean_r={est['mean_r']})", flush=True)
    fout.close()
    analyze(a.out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
