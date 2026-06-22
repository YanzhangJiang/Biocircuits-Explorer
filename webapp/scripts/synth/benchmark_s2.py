"""benchmark_s2.py — S2 five-arm benchmark on the MOCK verifier (engine-independent HARNESS VALIDATION).

PURPOSE: validate the synthesis harness plumbing and show the arms differentiate — NOT to produce
biology. All numbers are on a transparent numpy surrogate; the real comparison comes from swapping
EngineVerifier (arms/metrics/this script unchanged). The adversarial audit (2026-06-07) verified the
cost-accounting and gating primitives; this revision fixes its findings: a real random-search baseline,
the K-weighted `draws` cost axis, the reported cheap-feasibility fraction, a non-pre-verified seed, and
honest labelling.

Arms : random_search (base rate) / nn_retrieval / plain_opt / rop_opt / rop_cegis
Regimes:
  * covered — the prior library contains broad-window members (retrieval should win on EXACT count)
  * sparse  — the library lacks broad-window members (a rare phenotype the atlas never enumerated);
              the optimizer still searches the full theta-space. Honest signal = does guided synthesis
              find verified designs (a) that retrieval misses AND (b) more efficiently than random?
Cost is reported on BOTH axes: cheap curve solves and K-weighted exact `draws` (1 exact = K solves),
so 'cheapest' is never claimed on a single axis silently.
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
from functional_spec import broad_window          # noqa: E402
from verifier import MockVerifier                  # noqa: E402
from synthesize import run_synthesis, run_nn_retrieval, run_random_search  # noqa: E402
from rho_spec import rho_spec                       # noqa: E402
from compare import bump                            # noqa: E402
from benchmark import f_broad_plateau              # noqa: E402

REPO = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", ".."))
ARMS = ["random_search", "nn_retrieval", "plain_opt", "rop_opt", "rop_cegis"]
TOPO = {"name": "mock_net"}


def _starve(library, u):
    """sparse regime: drop library members that are themselves broad plateaus, modelling a rare
    phenotype the precomputed atlas never enumerated. (The optimizer is unaffected — it searches the
    full theta-space — so this tests retrieval-coverage, with a random-search baseline alongside to
    show the optimizer isn't just getting the answer for free.)"""
    return [(th, y) for (th, y) in library if not f_broad_plateau(y, u)]


def feasibility(V, spec, n=20000, seed=0):
    """cheap-feasible fraction (rho>=0) and broad-plateau fraction over the theta box — so the
    multi-fidelity 'cheap filter rejects most' claim is auditable, not assumed."""
    b = V.theta_bounds(); lo = np.array([x[0] for x in b]); hi = np.array([x[1] for x in b])
    rng = np.random.default_rng(seed); u = V.u_grid
    ths = lo + (hi - lo) * rng.random((n, len(b)))
    rho = np.array([rho_spec(spec, V._raw_curve(t), u)["rho"] for t in ths])
    fbp = np.array([bool(f_broad_plateau(V._raw_curve(t), u)) for t in ths])
    return {"cheap_feasible_frac": round(float(np.mean(rho >= 0)), 4),
            "broad_plateau_frac": round(float(np.mean(fbp)), 4),
            "gradient_ramp_frac": round(float(np.mean((rho > -0.99) & (rho < 0))), 4)}


def one_trial(arm, regime, trial, *, tau, noise, maxiter, popsize, k, K, lib_n):
    V = MockVerifier(tau=tau, noise=noise, lib_seed=1000 + trial)
    spec = broad_window(); u = V.u_grid
    V.cost.reset()
    cheap_budget = maxiter * popsize * len(V.theta_bounds())     # match DE's cheap-eval budget
    if arm == "random_search":
        return run_random_search(spec, V, TOPO, budget=cheap_budget, K=K, de_seed=trial)
    if arm == "nn_retrieval":
        lib = V.library(spec, lib_n)
        if regime == "sparse":
            lib = _starve(lib, u)
        return run_nn_retrieval(spec, V, TOPO, bump(u, 0.0, 3.5), library=lib, k=k, K=K)
    seed = V.rop_seed(TOPO, spec) if arm in ("rop_opt", "rop_cegis") else None
    return run_synthesis(spec, V, TOPO, seed_theta=seed, use_cegis=(arm == "rop_cegis"),
                         K=K, maxiter=maxiter, popsize=popsize, de_seed=trial)


def _med(xs):
    xs = [x for x in xs if x is not None]
    return round(statistics.median(xs), 1) if xs else None


def aggregate(rows, K):
    out = {}
    for arm in ARMS:
        a = [r for r in rows if r["arm"] == arm]
        succ = [r for r in a if r["verified"]]
        c2v = [r["solves_to_verified"][0] for r in succ if r.get("solves_to_verified")]
        e2v = [r["solves_to_verified"][1] for r in succ if r.get("solves_to_verified")]
        tot_cheap = [r["n_cheap"] for r in a]
        tot_draws = [r.get("n_draws", r["n_exact"] * K) for r in a]
        tot_solves = [r["n_cheap"] + r.get("n_draws", r["n_exact"] * K) for r in a]
        out[arm] = {
            "n": len(a), "success_rate": round(len(succ) / len(a), 3) if a else 0.0,
            "median_cheap_to_verified": _med(c2v),
            "median_draws_to_verified": _med([K * e for e in e2v]),
            "median_total_solves_to_verified": _med([c2v[i] + K * e2v[i] for i in range(len(c2v))]) if c2v else None,
            "median_total_cheap": _med(tot_cheap), "median_total_draws": _med(tot_draws),
            "median_total_solves": _med(tot_solves),
            "median_best_rho": _med([r["best_rho"] for r in a if "best_rho" in r]),
            "median_cex": _med([r.get("n_cex") for r in a if r.get("n_cex") is not None]),
        }
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--trials", type=int, default=16)
    ap.add_argument("--tau", type=float, default=0.8)        # robustness threshold (forces real search + CEGIS)
    ap.add_argument("--noise", type=float, default=0.6)      # center-jitter for the exact phenotyper's K draws
    ap.add_argument("--maxiter", type=int, default=60)
    ap.add_argument("--popsize", type=int, default=8)
    ap.add_argument("--k", type=int, default=8)              # NN top-k to exact-verify
    ap.add_argument("--K", type=int, default=8)              # phenotype draws (1 exact = K solves)
    ap.add_argument("--lib_n", type=int, default=80)
    ap.add_argument("--out", default=os.path.join(REPO, "benchmarks", "reports", "s2_benchmark_mock.json"))
    args = ap.parse_args()
    noise = [args.noise, args.noise, args.noise, args.noise, 0.05]

    Vc = MockVerifier(tau=args.tau, noise=args.noise); spec = broad_window()
    feas = feasibility(Vc, spec)
    seed = Vc.rop_seed(TOPO, spec)
    seed_rho = float(rho_spec(spec, Vc._raw_curve(seed), Vc.u_grid)["rho"])
    seed_sup = Vc.phenotype(TOPO, seed, args.K)["shape_support"]
    seed_preverified = bool(seed_rho >= 0 and seed_sup >= args.tau)

    report = {"report_schema": "bne-s2-benchmark-mock/v0.2.0",
              "created_at": datetime.datetime.now().isoformat(timespec="seconds"),
              "verifier": "MockVerifier (numpy surrogate — NOT the equilibrium engine; harness validation only)",
              "config": {"trials": args.trials, "tau": args.tau, "noise": args.noise, "maxiter": args.maxiter,
                         "popsize": args.popsize, "k": args.k, "K": args.K, "lib_n": args.lib_n},
              "feasibility": feas,
              "seed_check": {"rho": round(seed_rho, 4), "shape_support": round(seed_sup, 4),
                             "pre_verified": seed_preverified,
                             "note": "rop_seed must NOT be pre-verified, else rop arms measure the seed not the optimizer"},
              "regimes": {}}
    for regime in ["covered", "sparse"]:
        rows = []
        for arm in ARMS:
            for t in range(args.trials):
                r = one_trial(arm, regime, t, tau=args.tau, noise=noise, maxiter=args.maxiter,
                              popsize=args.popsize, k=args.k, K=args.K, lib_n=args.lib_n)
                rows.append(r)
        report["regimes"][regime] = aggregate(rows, args.K)

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    json.dump(report, open(args.out, "w"), indent=2)

    print(f"S2 five-arm benchmark on the MOCK verifier (HARNESS VALIDATION; numbers are not biology)")
    print(f"  trials={args.trials} tau={args.tau} noise={args.noise} budget(maxiter={args.maxiter},popsize={args.popsize}) K={args.K}")
    print(f"  feasibility: cheap rho>=0 = {feas['cheap_feasible_frac']}  broad_plateau = {feas['broad_plateau_frac']}  "
          f"gradient_ramp = {feas['gradient_ramp_frac']}")
    print(f"  rop_seed: rho={report['seed_check']['rho']} support={report['seed_check']['shape_support']} "
          f"pre_verified={seed_preverified}  (1 exact = K={args.K} solves)\n")
    for regime in ["covered", "sparse"]:
        print(f"── regime: {regime} ──")
        print(f"  {'arm':<14} {'succ':>5} {'cheap→v':>8} {'draws→v':>8} {'solves→v':>9} {'tot_solves':>10} {'best_rho':>9} {'cex':>4}")
        for arm in ARMS:
            m = report["regimes"][regime][arm]
            print(f"  {arm:<14} {m['success_rate']:>5} {str(m['median_cheap_to_verified']):>8} "
                  f"{str(m['median_draws_to_verified']):>8} {str(m['median_total_solves_to_verified']):>9} "
                  f"{str(m['median_total_solves']):>10} {str(m['median_best_rho']):>9} {str(m['median_cex']):>4}")
        print()
    print("honest reading: 'solves→v' = cheap_to_verified + K*exact_to_verified (one comparable cost axis).")
    print("  covered: retrieval should be competitive (atlas has the shape). sparse: NN should fail (atlas")
    print("  lacks it); the real signal is whether guided synthesis (rop_opt/rop_cegis) verifies, and does so")
    print("  in FEWER solves than random_search (else the optimizer adds nothing over luck + the cheap filter).")
    print(f"\nNOTE: MOCK numbers (plumbing validation). Swap EngineVerifier for the real run. JSON -> {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
