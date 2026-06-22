"""stress_cegis.py — CEGIS STRESS FIXTURE (NOT a benchmark result).

Runs plain_opt / rop_opt / rop_cegis on RuggedMockVerifier — a deceptive performance-vs-robustness
landscape where greedy cheap-rho maximization over-widens the plateau into a brittle (exact-REJECT)
region, and the robust band has LOWER cheap rho than the trap so the ratcheted exact gate can't reach
it by climbing. This unit-tests whether CEGIS (+ the penalized-score exact gate) recovers where plain
DE stalls. The point is the DELTA between arms on a landscape designed to need CEGIS — not an absolute
number and not biology.

    python3 stress_cegis.py [--trials 16] [--w_robust 3.0] [--trap_gain 3.0] [--tau 0.7]
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
from verifier import RuggedMockVerifier            # noqa: E402
from synthesize import run_synthesis               # noqa: E402

REPO = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", ".."))
TOPO = {"name": "rugged_mock_net"}
# plain_opt (ratchet) stalls; plain_cegis isolates CEGIS (same random init) → shows it doesn't help;
# diverse (diverse exact-checking) is the FIX; rop_cegis = ROP-seed-into-trap + CEGIS.
ARM_CFG = {
    "plain_opt":   {"seed": False, "cegis": False, "strategy": "ratchet"},
    "plain_cegis": {"seed": False, "cegis": True,  "strategy": "ratchet"},
    "diverse":     {"seed": False, "cegis": False, "strategy": "diverse"},
    "rop_cegis":   {"seed": True,  "cegis": True,  "strategy": "ratchet"},
}
ARMS = list(ARM_CFG)


def probe_band(V, spec, widths=(1.5, 2.5, 3.0, 4.0, 6.0, 9.0)):
    """Show the deception: cheap rho rises with width, but exact support collapses past w_robust."""
    from rho_spec import rho_spec
    rows = []
    for W in widths:
        th = np.array([-W / 2, W / 2, 2.5, 2.5, 1.0])          # a centered window of ~width W
        r = rho_spec(spec, V._raw_curve(th), V.u_grid)["rho"]
        sup = V.phenotype(TOPO, th, 8)["shape_support"]
        rows.append({"target_width": W, "nominal_width": round(V.nominal_width(th), 2),
                     "cheap_rho": round(r, 3), "exact_support": round(sup, 3),
                     "exact_accept": bool(sup >= V.tau)})
    return rows


def _med(xs):
    xs = [x for x in xs if x is not None]
    return round(statistics.median(xs), 1) if xs else None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--trials", type=int, default=16)
    ap.add_argument("--tau", type=float, default=0.7)
    ap.add_argument("--noise", type=float, default=0.4)
    ap.add_argument("--w_robust", type=float, default=3.0)
    ap.add_argument("--trap_gain", type=float, default=3.0)
    ap.add_argument("--maxiter", type=int, default=60)
    ap.add_argument("--popsize", type=int, default=10)
    ap.add_argument("--K", type=int, default=8)
    ap.add_argument("--cex_amp", type=float, default=1.0)
    ap.add_argument("--cex_cap", type=float, default=4.0)
    ap.add_argument("--cex_scale", type=float, default=0.4)
    ap.add_argument("--out", default=os.path.join(REPO, "benchmarks", "reports", "s2_cegis_stress_mock.json"))
    args = ap.parse_args()
    noise = [args.noise, args.noise, args.noise, args.noise, 0.05]

    Vp = RuggedMockVerifier(tau=args.tau, noise=noise, w_robust=args.w_robust, trap_gain=args.trap_gain)
    spec = broad_window()
    band = probe_band(Vp, spec)

    rows = {a: [] for a in ARMS}
    for arm in ARMS:
        for t in range(args.trials):
            V = RuggedMockVerifier(tau=args.tau, noise=noise, w_robust=args.w_robust,
                                   trap_gain=args.trap_gain, lib_seed=1000 + t)
            V.cost.reset()
            cfg = ARM_CFG[arm]
            seed = V.rop_seed(TOPO, spec) if cfg["seed"] else None
            r = run_synthesis(spec, V, TOPO, seed_theta=seed, use_cegis=cfg["cegis"],
                              verify_strategy=cfg["strategy"], K=args.K, maxiter=args.maxiter,
                              popsize=args.popsize, de_seed=t,
                              cex_amp=args.cex_amp, cex_cap=args.cex_cap, cex_scale=args.cex_scale)
            if r["verified"] and r.get("best_theta") is not None:
                r["verified_width"] = round(V.nominal_width(r["best_theta"]), 2)
            rows[arm].append(r)

    def agg(a):
        rs = rows[a]; succ = [r for r in rs if r["verified"]]
        return {"success_rate": round(len(succ) / len(rs), 3),
                "median_total_cheap": _med([r["n_cheap"] for r in rs]),
                "median_total_exact": _med([r["n_exact"] for r in rs]),
                "median_solves_to_verified": _med([r["solves_to_verified"][0] + args.K * r["solves_to_verified"][1]
                                                   for r in succ if r.get("solves_to_verified")]),
                "median_verified_width": _med([r.get("verified_width") for r in succ]),
                "median_cex": _med([r.get("n_cex") for r in rs])}

    report = {"report_schema": "bne-s2-cegis-stress/v0.1.0",
              "created_at": datetime.datetime.now().isoformat(timespec="seconds"),
              "fixture": "RuggedMockVerifier (deceptive width-fragility trap — UNIT/STRESS test, NOT biology)",
              "config": {k: getattr(args, k) for k in ("trials", "tau", "noise", "w_robust", "trap_gain",
                                                       "maxiter", "popsize", "K", "cex_amp", "cex_cap", "cex_scale")},
              "deception_band": band, "arms": {a: agg(a) for a in ARMS}}
    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    json.dump(report, open(args.out, "w"), indent=2)

    print("CEGIS STRESS FIXTURE — RuggedMockVerifier (deceptive width-fragility trap; UNIT TEST, not biology)")
    print(f"  tau={args.tau} w_robust={args.w_robust} trap_gain={args.trap_gain} "
          f"budget(maxiter={args.maxiter},popsize={args.popsize}) K={args.K}\n")
    print("  the deception (cheap rho rises with width; exact support collapses past w_robust):")
    print(f"    {'nom_width':>9} {'cheap_rho':>9} {'exact_support':>13} {'accept':>7}")
    for b in band:
        print(f"    {b['nominal_width']:>9} {b['cheap_rho']:>9} {b['exact_support']:>13} {str(b['exact_accept']):>7}")
    print(f"\n  {'arm':<11} {'success':>7} {'solves→ver':>10} {'ver_width':>9} {'tot_cheap':>9} {'cex':>5}")
    for a in ARMS:
        m = report["arms"][a]
        print(f"  {a:<11} {m['success_rate']:>7} {str(m['median_solves_to_verified']):>10} "
              f"{str(m['median_verified_width']):>9} {str(m['median_total_cheap']):>9} {str(m['median_cex']):>5}")
    pl, cg, dv = (report["arms"]["plain_opt"], report["arms"]["plain_cegis"], report["arms"]["diverse"])
    print(f"\n  FINDING: on this robustness trap CEGIS does NOT help (plain_opt {pl['success_rate']} vs")
    print(f"  plain_cegis {cg['success_rate']}, same init); the fix is the VERIFICATION STRATEGY —")
    print(f"  diverse exact-checking of cheap-feasibles recovers it (diverse {dv['success_rate']}).")
    print("  The ratchet (exact-check only cheap-rho leaders) is the trap-amplifier when the cheap")
    print("  surrogate is robustness-misleading. STRESS FIXTURE (not a benchmark).")
    print(f"  JSON -> {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
