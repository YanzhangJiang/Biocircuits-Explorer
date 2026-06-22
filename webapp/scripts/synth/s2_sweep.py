"""s2_sweep.py — S2 sample-vs-optimize sweep on the LIVE engine across MANY topologies.

Answers the open question from the single-topology run: at a HARD robustness bar (high τ, high
implementation-noise), for robust kd-design, does `random` (sample-then-exact-filter) and `diverse`
(diverse exact-checking, every=1) beat the `ratchet` (exact-check only cheap-ρ leaders)? Reads a JSON
list of topologies ({reactions, input_symbol, observe_species}). Engine-agnostic via EngineVerifier.

    python3 s2_sweep.py --topos topos.json [--trials 4 --tau 0.7 --impl_noise 0.6]
"""
from __future__ import annotations
import argparse
import datetime
import json
import os
import statistics
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "reader"))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
from functional_spec import broad_window          # noqa: E402
from verifier import EngineVerifier               # noqa: E402
from synthesize import run_synthesis, run_random_search  # noqa: E402
import engine_client as E                          # noqa: E402

ARMS = ["random", "ratchet", "diverse"]


def _med(xs):
    xs = [x for x in xs if x is not None]
    return round(statistics.median(xs), 1) if xs else None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--topos", required=True)
    ap.add_argument("--trials", type=int, default=4)
    ap.add_argument("--tau", type=float, default=0.7)
    ap.add_argument("--impl_noise", type=float, default=0.6)
    ap.add_argument("--maxiter", type=int, default=12)
    ap.add_argument("--popsize", type=int, default=8)
    ap.add_argument("--K", type=int, default=8)
    ap.add_argument("--out", default="benchmarks/reports/s2_sweep_engine.json")
    a = ap.parse_args()

    # NB: older backends lack a /ready route (engine_ready would 404->False); probe the REAL API.
    _p = E.build_model(reactions=["A + B <-> C_A_B"], kd=[1.0])
    if not isinstance(_p, dict) or _p.get("engine_offline") or _p.get("error") or not _p.get("session_id"):
        print(f"engine NOT reachable at {E.engine_base_url()}: {_p.get('error') if isinstance(_p, dict) else _p}")
        return 2
    topos = json.load(open(a.topos))
    spec = broad_window()
    rows = {x: [] for x in ARMS}
    for ti, topo in enumerate(topos):
        V = EngineVerifier(target_shape="bandpass_with_plateau", tau=a.tau, impl_noise=a.impl_noise)
        budget = a.maxiter * a.popsize * len(topo["reactions"])
        for seed in range(a.trials):
            for arm in ARMS:
                V.cost.reset()
                if arm == "random":
                    r = run_random_search(spec, V, topo, budget=budget, K=a.K, de_seed=seed)
                elif arm == "ratchet":
                    r = run_synthesis(spec, V, topo, verify_strategy="ratchet", K=a.K,
                                      maxiter=a.maxiter, popsize=a.popsize, de_seed=seed)
                else:
                    r = run_synthesis(spec, V, topo, verify_strategy="diverse", diverse_every=1, K=a.K,
                                      maxiter=a.maxiter, popsize=a.popsize, de_seed=seed)
                r["topo"] = ti
                rows[arm].append(r)
            print(f"  topo{ti}(supp={topo.get('support')}) seed{seed}: "
                  + "  ".join(f"{x}={'Y' if rows[x][-1]['verified'] else 'n'}" for x in ARMS), flush=True)

    def agg(arm):
        rs = rows[arm]; succ = [r for r in rs if r["verified"]]
        return {"n": len(rs), "success_rate": round(len(succ) / len(rs), 3) if rs else 0.0,
                "median_solves_to_verified": _med([r["solves_to_verified"][0] + a.K * r["solves_to_verified"][1]
                                                   for r in succ if r.get("solves_to_verified")]),
                "median_total_solves": _med([r["n_cheap"] + r.get("n_draws", 0) for r in rs])}

    rep = {"report_schema": "bne-s2-sweep-engine/v0.1.0",
           "created_at": datetime.datetime.now().isoformat(timespec="seconds"),
           "engine_base_url": E.engine_base_url(), "n_topos": len(topos),
           "config": {k: getattr(a, k) for k in ("trials", "tau", "impl_noise", "maxiter", "popsize", "K")},
           "arms": {x: agg(x) for x in ARMS}}
    os.makedirs(os.path.dirname(a.out) or ".", exist_ok=True)
    json.dump(rep, open(a.out, "w"), indent=2)

    print(f"\nS2 sample-vs-optimize sweep (HARD bar τ={a.tau} impl_noise={a.impl_noise}) "
          f"across {len(topos)} topos × {a.trials} seeds, on {E.engine_base_url()}")
    print(f"  {'arm':<9} {'success':>8} {'solves→v':>9} {'tot_solves':>11}")
    for x in ARMS:
        m = rep["arms"][x]
        print(f"  {x:<9} {m['success_rate']:>8} {str(m['median_solves_to_verified']):>9} {str(m['median_total_solves']):>11}")
    print(f"\n  question: does random / diverse(every=1) beat ratchet for robust kd-design? JSON -> {a.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
