#!/usr/bin/env python3
"""profile_verifier.py — S1.0 of the Functional Synthesis plan.

Measure the REAL wall-clock cost of the verifier so the S1-S3 compute budget uses measured
numbers, not assumptions. It profiles, per representative network:
  - build_model              (the per-theta rebuild cost, if kd cannot be varied another way)
  - dose_response(n_points)  at several resolutions (the cheap rho_Spec curve)
  - phenotype_classify(K)    at several K (the exact verifier)
  - ro_behavior              (the analytic ROP skeleton gate)
and PROBES whether kd can be varied via fixed_qK WITHOUT rebuilding (the lever that decides whether
S2 runs on a laptop or needs the workstation). It then projects the S2 four-arm budget at a chosen
parallelism.

Requires the live Julia engine (BIOCIRCUITS_EXPLORER_PORT, default 8088). If the engine is offline it
SAYS SO and writes {engine_offline: true} — it never fabricates a timing.

    python3 profile_verifier.py [--sample 6] [--reps 5] [--out PATH] \
        [--n-cheap 115000] [--n-exact 3500] [--parallel 100]
"""
from __future__ import annotations
import argparse
import json
import os
import statistics
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))  # webapp/scripts
import engine_client as E

REPO = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", ".."))
DATASET = os.path.join(REPO, "datasets", "latent-atlas-v0", "dataset.jsonl")

# Fallback networks if the dataset isn't present (input/observe left None -> the engine picks defaults).
_FALLBACK = [
    {"rules": ["A + B <-> C_A_B"]},
    {"rules": ["A + B <-> C_A_B", "A + C_A_B <-> C_A_A_B"]},
    {"rules": ["A + A <-> C_A_A", "A + B <-> C_A_B", "B + B <-> C_B_B"]},
]


def _sample_nets(path, n):
    """Sample n networks spread across network size from the dose dataset (real, known-to-build rules)."""
    if not os.path.isfile(path):
        return list(_FALLBACK)
    rows = []
    with open(path) as fh:
        for line in fh:
            try:
                r = json.loads(line)
            except Exception:
                continue
            if r.get("rules"):
                rows.append({"rules": r["rules"], "input_symbol": r.get("input_symbol"),
                             "output_symbol": r.get("output_symbol"), "n_reactions": r.get("n_reactions")})
    if not rows:
        return list(_FALLBACK)
    rows.sort(key=lambda r: (r.get("n_reactions") or 99))
    if len(rows) <= n:
        return rows
    step = len(rows) / n
    return [rows[int(i * step)] for i in range(n)]


def _med(fn, reps):
    """Median ms over `reps` timed calls after one discarded warmup. Returns (median_ms|None, last)."""
    last = fn()  # warmup: absorbs Julia JIT and validates the call
    if isinstance(last, dict) and (last.get("engine_offline") or last.get("error")):
        return None, last
    samples = []
    for _ in range(reps):
        t0 = time.perf_counter()
        last = fn()
        samples.append((time.perf_counter() - t0) * 1000.0)
    return statistics.median(samples), last


def _agg(per_net, key):
    vals = [d[key] for d in per_net if isinstance(d.get(key), (int, float))]
    return round(statistics.median(vals), 2) if vals else None


def profile(nets, reps, kpoints, kdraws):
    per_net = []
    for net in nets:
        rules = net["rules"]
        rec = {"rules": rules, "n_reactions": len(rules)}
        # build_model (timed) -> session + symbol surface
        t_build, m = _med(lambda: E.build_model(reactions=rules), reps)
        if t_build is None:
            rec["error"] = (m or {}).get("error") or "build failed"
            rec["engine_offline"] = bool((m or {}).get("engine_offline"))
            per_net.append(rec)
            if rec["engine_offline"]:
                return per_net, True
            continue
        sid = m.get("session_id")
        q = m.get("q_sym") or []
        prod = m.get("product_species") or m.get("x_sym") or []
        ksym = (m.get("K_sym") or m.get("k_sym") or [])
        q0 = q[0] if q else None
        obs = prod[-1] if prod else None
        rec.update({"build_ms": round(t_build, 2), "input": q0, "observe": obs,
                    "n_species": len(m.get("x_sym") or []), "n_K": len(ksym)})
        if not (sid and q0 and obs):
            rec["error"] = "no usable input/observe symbol"
            per_net.append(rec)
            continue
        # dose_response at several resolutions
        for n in kpoints:
            t, _ = _med(lambda n=n: E.dose_response(sid, param_symbol=q0, output_exprs=[obs], n_points=n), reps)
            rec[f"dose_ms_n{n}"] = round(t, 2) if t is not None else None
        # phenotype_classify (the exact verifier) at several K
        for K in kdraws:
            t, _ = _med(lambda K=K: E.phenotype_classify(sid, input_symbol=q0, output_expr=obs, K=K), reps)
            rec[f"phen_ms_K{K}"] = round(t, 2) if t is not None else None
        # ro_behavior (analytic ROP skeleton gate)
        t, _ = _med(lambda: E.phenotype(sid, change_qK=q0, observe_x=obs,
                                        path_scope="feasible", compute_volume=True), reps)
        rec["ro_ms"] = round(t, 2) if t is not None else None
        # fixed_qK probe: can we change a kd (a K symbol) WITHOUT rebuilding?
        if ksym:
            ks = ksym[0]
            r1 = E.dose_response(sid, param_symbol=q0, output_exprs=[obs], n_points=41, fixed_qK={ks: -2.0})
            r2 = E.dose_response(sid, param_symbol=q0, output_exprs=[obs], n_points=41, fixed_qK={ks: 2.0})
            def _traj(r):
                return [(row[0] if isinstance(row, list) else row) for row in (r.get("output_traj") or [])]
            ok1 = isinstance(r1, dict) and not r1.get("error") and not r1.get("engine_offline")
            ok2 = isinstance(r2, dict) and not r2.get("error") and not r2.get("engine_offline")
            changed = ok1 and ok2 and _traj(r1) != _traj(r2)
            rec["fixed_qK_accepted"] = bool(ok1 and ok2)
            rec["fixed_qK_changes_output"] = bool(changed)
        else:
            rec["fixed_qK_accepted"] = None
            rec["fixed_qK_changes_output"] = None
        per_net.append(rec)
    return per_net, False


def derive(per_net, n_cheap, n_exact, parallel):
    """Median per-call costs -> cheap/exact/full-simulate unit costs -> projected S2 budget."""
    g = lambda k: _agg(per_net, k)
    build = g("build_ms")
    dose_cheap = g("dose_ms_n41") or g("dose_ms_n81")
    dose81, dose121 = g("dose_ms_n81"), g("dose_ms_n121")
    phen8 = g("phen_ms_K8")
    ro = g("ro_ms")
    # does kd vary via fixed_qK on a reused model? (so the inner loop avoids per-theta rebuild)
    fq = [d.get("fixed_qK_changes_output") for d in per_net if d.get("fixed_qK_changes_output") is not None]
    fixed_qK_works = bool(fq) and all(fq)
    units = {
        "build_ms": build,
        "cheap_eval_ms": dose_cheap,                       # 1 low-res curve for rho_Spec (single draw)
        "exact_verify_ms": phen8,                          # phenotype_classify(K=8)
        "full_simulate_ms": (sum(x for x in [build, dose81, dose121, phen8] if x) or None),
        "ro_behavior_ms": ro,
        "fixed_qK_varies_kd_without_rebuild": fixed_qK_works,
    }
    rebuild_per_eval = 0.0 if fixed_qK_works else (build or 0.0)

    def project(label, cheap_ms, exact_ms):
        if cheap_ms is None or exact_ms is None:
            return {"label": label, "note": "insufficient timing"}
        total_ms = n_cheap * cheap_ms + n_exact * exact_ms
        sc_hr = total_ms / 1000.0 / 3600.0
        return {"label": label, "n_cheap": n_cheap, "n_exact": n_exact,
                "single_core_hours": round(sc_hr, 2),
                f"parallel_{parallel}way_minutes": round(sc_hr * 60.0 / max(1, parallel), 2)}

    budget = {
        "with_fixed_qK": project("inner loop reuses compiled model (fixed_qK)",
                                 dose_cheap, phen8),
        "with_rebuild_per_theta": project("inner loop rebuilds per theta (build + curve)",
                                          (dose_cheap + rebuild_per_eval) if dose_cheap is not None else None,
                                          (phen8 + rebuild_per_eval) if phen8 is not None else None),
    }
    return units, budget


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sample", type=int, default=6)
    ap.add_argument("--reps", type=int, default=5)
    ap.add_argument("--n-cheap", type=int, default=115000)
    ap.add_argument("--n-exact", type=int, default=3500)
    ap.add_argument("--parallel", type=int, default=100)
    ap.add_argument("--out", default=os.path.join(REPO, "benchmarks", "reports", "verifier_profile.json"))
    args = ap.parse_args()

    if not E.engine_ready():
        msg = (f"compute engine NOT ready at {E.engine_base_url()} — start the Julia engine "
               f"(node Workspace server / BIOCIRCUITS_EXPLORER_PORT) and re-run. No timings fabricated.")
        print(msg)
        os.makedirs(os.path.dirname(args.out), exist_ok=True)
        json.dump({"report_schema": "bne-verifier-profile/v0.1.0", "engine_offline": True, "note": msg},
                  open(args.out, "w"), indent=2)
        return 2

    nets = _sample_nets(DATASET, args.sample)
    kpoints, kdraws = [41, 81, 121, 161], [1, 4, 8]
    per_net, offline = profile(nets, args.reps, kpoints, kdraws)
    if offline:
        print("engine went offline mid-profile — no fabricated timings.")
        json.dump({"report_schema": "bne-verifier-profile/v0.1.0", "engine_offline": True},
                  open(args.out, "w"), indent=2)
        return 2

    units, budget = derive(per_net, args.n_cheap, args.n_exact, args.parallel)
    report = {
        "report_schema": "bne-verifier-profile/v0.1.0",
        "engine_base_url": E.engine_base_url(),
        "n_networks": len(nets), "reps": args.reps,
        "kpoints": kpoints, "kdraws": kdraws,
        "unit_costs_ms_median": units,
        "s2_budget_projection": budget,
        "per_network": per_net,
    }
    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    json.dump(report, open(args.out, "w"), indent=2)

    print(f"profiled {len(nets)} networks @ {E.engine_base_url()}  (median ms, reps={args.reps})\n")
    print("unit costs (ms, median):")
    for k, v in units.items():
        print(f"  {k:<42} {v}")
    print("\nS2 four-arm budget projection "
          f"(N_cheap={args.n_cheap:,}, N_exact={args.n_exact:,}, parallel={args.parallel}):")
    for k, b in budget.items():
        print(f"  {k:<22} {b}")
    print(f"\nJSON report -> {args.out}")
    if not units.get("fixed_qK_varies_kd_without_rebuild"):
        print("\nNOTE: fixed_qK did NOT vary kd without rebuild on these nets -> the inner loop will pay "
              "build_model per theta unless an engine-side fast-eval path is added (S2.0).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
