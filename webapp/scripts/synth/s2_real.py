"""s2_real.py — S2-REAL: fixed-topology kd-synthesis on the LIVE Julia engine (EngineVerifier).

Picks a bandpass-capable topology from the atlas, then runs the synthesis arms (optimizing log10 kd)
against the REAL equilibrium solver + phenotyper — the actual engine-in-the-loop loop the whole plan is
about. Uses verify_strategy='diverse' (the stress-test fix: the engine's cheap single-draw rho_Spec and
exact K-draw robustness diverge near the boundary, so the ratchet would chase a misleading leader).

    python3 s2_real.py --smoke                 # 1 build+dose+phenotype on the chosen topology
    python3 s2_real.py [--trials 3] [--maxiter 10] [--popsize 6] [--tau 0.4] [--K 8]

Requires the engine on BIOCIRCUITS_EXPLORER_PORT (default 8088). Refuses to fabricate if offline.
"""
from __future__ import annotations
import argparse
import datetime
import json
import os
import statistics
import sys

import numpy as np

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "reader"))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
from functional_spec import broad_window          # noqa: E402
from verifier import EngineVerifier               # noqa: E402
from synthesize import run_synthesis, run_random_search  # noqa: E402
from rho_spec import rho_spec                       # noqa: E402
from atlas_store import open_store                  # noqa: E402
import engine_client as E                           # noqa: E402

REPO = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", ".."))


def pick_topology(atlas_root="/tmp/atlas-fresh"):
    st = open_store(atlas_root)
    bp = [r for r in st.records if r["dominant_shape"] == "bandpass_with_plateau"]
    if not bp:
        raise SystemExit("no bandpass_with_plateau topology in the atlas")
    # pick the LOWEST-support bandpass topology (hardest available -> kd actually matters for the search;
    # high-support ones are robustly bandpass for almost any kd, i.e. a trivial search).
    bp.sort(key=lambda r: ((r["shape_fractions"].get("bandpass_with_plateau") or 0),
                           len((r["io_assignment"] or {}).get("output_symbol") or "")))
    r = bp[0]; io = r["io_assignment"]
    return {"reactions": list(r["rules"]), "input_symbol": io["input_symbol"],
            "observe_species": io["output_symbol"],
            "atlas_support": r["shape_fractions"].get("bandpass_with_plateau")}


def smoke(V, topo, K):
    nrx = len(topo["reactions"])
    th = [0.0] * nrx          # kd = 1.0 everywhere (theta = log10 kd)
    y = V.curve(topo, th)
    finite = int(np.isfinite(y).sum())
    print(f"topology: {nrx} reactions  input={topo['input_symbol']} output={topo['observe_species']} "
          f"atlas_support={topo.get('atlas_support')}")
    print(f"  rules: {topo['reactions']}")
    if finite < 3:
        print("  ENGINE returned no usable curve (offline or build rejected). Check /tmp/bne_engine.log.")
        return False
    r = rho_spec(broad_window(), y, V.u_grid)
    ph = V.phenotype(topo, th, K)
    print(f"  curve @kd=1: finite_pts={finite}/{len(y)}  rho_Spec={r['rho']:.3f} "
          f"margins={{{', '.join(f'{k}={v:.2f}' for k,v in r['margins'].items())}}}")
    print(f"  phenotype @kd=1: shape_support={ph.get('shape_support')} dominant={ph.get('dominant_shape')} "
          f"accept={ph.get('accept')}")
    print(f"  engine cost so far: {V.cost.snapshot()}")
    return True


def _med(xs):
    xs = [x for x in xs if x is not None]
    return round(statistics.median(xs), 1) if xs else None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--smoke", action="store_true")
    ap.add_argument("--trials", type=int, default=3)
    ap.add_argument("--maxiter", type=int, default=10)
    ap.add_argument("--popsize", type=int, default=6)
    ap.add_argument("--tau", type=float, default=0.4)
    ap.add_argument("--impl_noise", type=float, default=0.3)   # log10(kd) implementation tolerance
    ap.add_argument("--K", type=int, default=8)
    ap.add_argument("--atlas", default="/tmp/atlas-fresh")
    ap.add_argument("--out", default=os.path.join(REPO, "benchmarks", "reports", "s2_real_engine.json"))
    args = ap.parse_args()

    if not E.engine_ready():
        print(f"compute engine NOT ready at {E.engine_base_url()} — start it "
              f"(webapp/server.jl) and retry. No results fabricated.")
        return 2

    topo = pick_topology(args.atlas)
    V = EngineVerifier(target_shape="bandpass_with_plateau", tau=args.tau, impl_noise=args.impl_noise)

    if args.smoke:
        smoke(V, topo, args.K)
        return 0

    spec = broad_window()
    # verify the engine path once before spending the budget
    if not smoke(V, topo, args.K):
        return 3
    print()

    ARMS = ["random_search", "plain_opt", "diverse"]
    cheap_budget = args.maxiter * args.popsize * len(topo["reactions"])
    rows = {a: [] for a in ARMS}
    for arm in ARMS:
        for t in range(args.trials):
            V.cost.reset()
            if arm == "random_search":
                r = run_random_search(spec, V, topo, budget=cheap_budget, K=args.K, de_seed=t)
            else:
                r = run_synthesis(spec, V, topo, seed_theta=None, use_cegis=False,
                                  verify_strategy=("diverse" if arm == "diverse" else "ratchet"),
                                  K=args.K, maxiter=args.maxiter, popsize=args.popsize, de_seed=t)
            r["arm"] = arm
            rows[arm].append(r)
            print(f"  [{arm:<13} seed={t}] verified={r['verified']} "
                  f"solves_to_v={r.get('solves_to_verified')} n_cheap={r['n_cheap']} n_exact={r['n_exact']} "
                  f"support={r.get('shape_support')}")

    def agg(a):
        rs = rows[a]; succ = [r for r in rs if r["verified"]]
        return {"success_rate": round(len(succ) / len(rs), 3) if rs else 0.0,
                "median_n_cheap": _med([r["n_cheap"] for r in rs]),
                "median_n_exact": _med([r["n_exact"] for r in rs]),
                "median_n_draws": _med([r.get("n_draws") for r in rs]),
                "median_n_builds": _med([r.get("n_builds") for r in rs]),
                "median_solves_to_verified": _med([r["solves_to_verified"][0] + args.K * r["solves_to_verified"][1]
                                                   for r in succ if r.get("solves_to_verified")]),
                "median_shape_support": _med([r.get("shape_support") for r in succ])}

    report = {"report_schema": "bne-s2-real-engine/v0.1.0",
              "created_at": datetime.datetime.now().isoformat(timespec="seconds"),
              "engine_base_url": E.engine_base_url(), "topology": topo,
              "config": {k: getattr(args, k) for k in ("trials", "maxiter", "popsize", "tau", "K")},
              "arms": {a: agg(a) for a in ARMS}}
    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    json.dump(report, open(args.out, "w"), indent=2)

    print(f"\nS2-REAL — fixed-topology kd-synthesis on the LIVE engine ({E.engine_base_url()})")
    print(f"  topology: {len(topo['reactions'])} rx, input={topo['input_symbol']} output={topo['observe_species']}\n")
    print(f"  {'arm':<14} {'success':>7} {'solves→v':>9} {'n_cheap':>8} {'n_exact':>8} {'support':>8}")
    for a in ARMS:
        m = report["arms"][a]
        print(f"  {a:<14} {m['success_rate']:>7} {str(m['median_solves_to_verified']):>9} "
              f"{str(m['median_n_cheap']):>8} {str(m['median_n_exact']):>8} {str(m['median_shape_support']):>8}")
    print(f"\nJSON report -> {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
