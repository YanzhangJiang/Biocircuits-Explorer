"""synthesize.py — S2 synthesis loop: continuous theta-optimization with a cheap/exact multi-fidelity
split and counterexample-guided refinement (CEGIS). Verifier-agnostic (see verifier.py).

THE LOOP (run_synthesis):
  * inner: a derivative-free optimizer (scipy differential_evolution; cma is a drop-in if installed)
    maximizes the CHEAP rho_Spec(curve(topology, theta)) — the soft-min score calibrated in S1.
  * multi-fidelity: only when cheap rho_Spec crosses >=0 (and improves on the last exact-checked rho)
    is the EXACT phenotyper invoked to confirm. N_exact << N_cheap by construction.
  * CEGIS: an exact REJECT of a cheap-positive candidate is a counterexample — the exact verifier
    teaching the cheap surrogate where it is wrong. Each counterexample carves a Gaussian penalty into
    the objective at that theta, steering the optimizer out of false-positive basins.

S1 finding wired in: WIDTH is the discriminating axis; rho_prom is backwards (only a monotone/flat
gate). Use rho_Spec as the objective (its soft-min already weights the binding margin), and rank by the
per-draw robust fraction (shape_support), never the categorical label.

NN retrieval (run_nn_retrieval) is the baseline arm: rank a PRECOMPUTED prior library by curve distance
to the target and exact-verify the top-k. Retrieval pays no per-eval solves (the library is precomputed),
only the exact confirmations — so it is sample-efficient WHEN the library covers the target.
"""
from __future__ import annotations
import os
import sys

import numpy as np
from scipy.optimize import differential_evolution

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "reader"))
from rho_spec import rho_spec                # noqa: E402
from compare import curve_distance           # noqa: E402


def _counterexample(theta, spec, u, y, ph):
    """Structured, machine-actionable counterexample (decision-record schema): a cheap-positive theta
    the exact verifier rejected, tagged with the weakest predicate margin (diagnosed on its curve y)."""
    margins = rho_spec(spec, y, u).get("margins", {})
    weakest = min(margins, key=margins.get) if margins else None
    return {"counterexample_type": "theta_draw", "theta": np.asarray(theta, float).tolist(),
            "expected": {"predicate": "robust_accept", "threshold": getattr(spec, "tau", 0.5)},
            "observed": {"shape_support": ph.get("shape_support")},
            "weakest_margin": weakest, "margins": {k: round(float(v), 4) for k, v in margins.items()},
            "suggested_repair": (f"increase {weakest}" if weakest else "widen plateau")}


def run_synthesis(spec, verifier, topology, *, seed_theta=None, use_cegis=False,
                  K=8, maxiter=40, popsize=15, de_seed=0,
                  cex_amp=0.6, cex_scale=0.5, cex_lambda=1.0, cex_cap=1.0, exact_gap=0.03,
                  verify_strategy="ratchet", exact_budget=60, diverse_every=2):
    """Optimize theta for `topology` to satisfy `spec`. Returns verified flag, solves-to-verified
    (cheap, exact), total costs, best rho, and the counterexamples gathered.

    `best_theta`/`best_rho` track the best CHEAP candidate (may be unverified on a failed run); the
    verified design, when found, is recorded separately and drives solves_to_verified."""
    u = verifier.u_grid
    bounds = verifier.theta_bounds(topology)
    ranges = np.array([b[1] - b[0] for b in bounds], float)
    st = {"verified": None, "best_rho": -np.inf, "best_theta": None,
          "exact_max_score": -np.inf, "n_cheap_pos": 0, "n_exact_used": 0,
          "cex": [], "cex_objs": [], "stop": False,
          "n_cheap_at_verify": None, "n_exact_at_verify": None}
    c0 = verifier.cost.snapshot()

    def penalty(theta):
        if not st["cex"]:
            return 0.0
        th = np.asarray(theta, float)
        tot = 0.0
        for tc in st["cex"]:
            d2 = float(np.sum(((th - tc) / ranges) ** 2))
            tot += cex_amp * np.exp(-d2 / (2.0 * cex_scale ** 2))
        return min(cex_cap, tot)   # cap so a cluster of counterexamples cannot starve the search

    def objective(theta):
        if st["stop"]:
            return 0.0
        y = verifier.curve(topology, theta)
        r = float(rho_spec(spec, y, u)["rho"])
        if r > st["best_rho"]:
            st["best_rho"] = r; st["best_theta"] = np.asarray(theta, float)
        score = r - (cex_lambda * penalty(theta) if use_cegis else 0.0)
        # multi-fidelity verification of cheap-feasible (r>=0) candidates. Two strategies:
        #   ratchet — exact-check only candidates improving the PENALIZED score (cheap; correct when the
        #     cheap surrogate is well-calibrated). BUT if cheap rho is robustness-MISLEADING (rewards a
        #     globally non-robust direction), the ratchet chases the misleading leader and STALLS
        #     (demonstrated in stress_cegis.py: 0.44 success on the width-fragility trap).
        #   diverse — exact-check a sampled, budget-capped set of cheap-feasibles regardless of their
        #     cheap-rho rank; robust to a misleading surrogate at the cost of more exact calls (the same
        #     fixture: 1.0 success, ~2 exact checks). Prefer this when cheap/exact may diverge near the
        #     robustness boundary (the real engine).
        do_check = False
        if st["verified"] is None and r >= 0.0:
            if verify_strategy == "diverse":
                st["n_cheap_pos"] += 1
                if st["n_cheap_pos"] % diverse_every == 0 and st["n_exact_used"] < exact_budget:
                    do_check = True
            elif score > st["exact_max_score"] + exact_gap:
                st["exact_max_score"] = score
                do_check = True
        if do_check:
            st["n_exact_used"] += 1
            ph = verifier.phenotype(topology, theta, K)
            if ph.get("accept"):
                snap = verifier.cost.snapshot()
                st["verified"] = {"theta": np.asarray(theta, float), "phenotype": ph, "rho": r}
                st["n_cheap_at_verify"] = snap["cheap_calls"] - c0["cheap_calls"]
                st["n_exact_at_verify"] = snap["exact_calls"] - c0["exact_calls"]
                st["stop"] = True
            elif use_cegis:
                st["cex"].append(np.asarray(theta, float))
                st["cex_objs"].append(_counterexample(theta, spec, u, y, ph))
        return -score

    x0 = np.asarray(seed_theta, float) if seed_theta is not None else None

    def cb(*_a, **_k):
        return st["stop"]

    differential_evolution(objective, bounds, maxiter=maxiter, popsize=popsize, polish=False,
                           seed=de_seed, x0=x0, callback=cb, tol=1e-7,
                           mutation=(0.5, 1.0), recombination=0.7)

    snap = verifier.cost.snapshot()
    res = {"arm": ("rop_cegis" if (use_cegis and seed_theta is not None)
                   else "rop_opt" if seed_theta is not None else "plain_opt"),
           "verified": st["verified"] is not None,
           "best_rho": round(float(st["best_rho"]), 4),
           "n_cheap": snap["cheap_calls"] - c0["cheap_calls"],
           "n_exact": snap["exact_calls"] - c0["exact_calls"],
           "n_draws": snap["draws"] - c0["draws"],
           "n_builds": snap["builds"] - c0["builds"],
           "n_cex": len(st["cex"]),
           "solves_to_verified": ([st["n_cheap_at_verify"], st["n_exact_at_verify"]]
                                  if st["verified"] else None),
           "best_theta": st["best_theta"].tolist() if st["best_theta"] is not None else None,
           "counterexamples": st["cex_objs"][:5]}
    if st["verified"]:
        res["shape_support"] = st["verified"]["phenotype"].get("shape_support")
    return res


def run_nn_retrieval(spec, verifier, topology, target_curve, *, library, k=8, K=8):
    """Baseline: rank a PRECOMPUTED prior library by curve distance to target_curve, exact-verify the
    top-k. Retrieval is free (library precomputed); only the exact confirmations cost solves."""
    c0 = verifier.cost.snapshot()
    ranked = sorted(library, key=lambda ty: curve_distance(target_curve, ty[1]))
    verified, n_checked = None, 0
    for theta, _y in ranked[:k]:
        ph = verifier.phenotype(topology, theta, K); n_checked += 1
        if ph.get("accept"):
            verified = {"theta": np.asarray(theta, float).tolist(), "phenotype": ph}
            break
    snap = verifier.cost.snapshot()
    return {"arm": "nn_retrieval", "verified": verified is not None,
            "n_cheap": snap["cheap_calls"] - c0["cheap_calls"],
            "n_exact": snap["exact_calls"] - c0["exact_calls"],
            "n_draws": snap["draws"] - c0["draws"], "n_builds": snap["builds"] - c0["builds"],
            "n_checked": n_checked,
            "solves_to_verified": ([0, n_checked] if verified else None),
            "shape_support": verified["phenotype"].get("shape_support") if verified else None,
            "best_theta": verified["theta"] if verified else None}


def run_random_search(spec, verifier, topology, *, budget, K=8, de_seed=0, exact_gap=0.03):
    """Baseline that ISOLATES the optimizer's value: identical cheap/exact multi-fidelity machinery as
    run_synthesis but theta is sampled UNIFORMLY at random (no DE guidance). If guided synthesis does
    not beat this, the optimizer adds nothing over luck + the cheap filter."""
    u = verifier.u_grid
    bounds = verifier.theta_bounds(topology)
    lo = np.array([b[0] for b in bounds]); hi = np.array([b[1] for b in bounds])
    rng = np.random.default_rng(de_seed)
    c0 = verifier.cost.snapshot()
    exact_max, verified, stv = -np.inf, None, None
    for _ in range(int(budget)):
        th = lo + (hi - lo) * rng.random(len(bounds))
        r = float(rho_spec(spec, verifier.curve(topology, th), u)["rho"])
        if r >= 0.0 and r > exact_max + exact_gap:
            exact_max = r
            ph = verifier.phenotype(topology, th, K)
            if ph.get("accept"):
                snap = verifier.cost.snapshot()
                verified = {"theta": th.tolist(), "phenotype": ph}
                stv = [snap["cheap_calls"] - c0["cheap_calls"], snap["exact_calls"] - c0["exact_calls"]]
                break
    snap = verifier.cost.snapshot()
    return {"arm": "random_search", "verified": verified is not None,
            "n_cheap": snap["cheap_calls"] - c0["cheap_calls"], "n_exact": snap["exact_calls"] - c0["exact_calls"],
            "n_draws": snap["draws"] - c0["draws"], "n_builds": snap["builds"] - c0["builds"],
            "solves_to_verified": stv,
            "shape_support": verified["phenotype"].get("shape_support") if verified else None,
            "best_theta": verified["theta"] if verified else None}
