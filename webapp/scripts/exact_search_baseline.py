#!/usr/bin/env python3
# EXACT-SEARCH baseline (brute-force enumerate + verify, NO compass) over the
# fully-labelled in-scope atlas — the ceiling the model-as-compass must beat.
# For each benchmark task: the candidate set is every in-scope slice satisfying the
# task's hard structural constraint (n_reactions <= max_reactions); each oracle call
# = one phenotyper verification of the task's behavior gate. Because the μ≤5 atlas is
# exhaustively labelled, the phenotyper's verdict on every slice is already known
# (the labels ARE its output), so exact-search cost/coverage is computed in closed
# form instead of re-running thousands of solves:
#
#   - feasible        : does ANY in-scope candidate pass the full gate? (ground truth)
#   - P, N, base_rate : passers / candidate-pool size
#   - exact (random order, no ranking):
#       E[oracle calls to first pass]  = (N+1)/(P+1)
#       Pr[>=1 pass within budget B]   = 1 - prod_{i<B}(N-P-i)/(N-i)
#   - ranked (retrieval heuristic = sort by shape_support, the SAME signal used in
#       bench_retrieve_verify): oracle calls to first pass, pass@1/5/20.
#
# The comparison answers the Phase-3 gate: brute-force exact search needs E[X] oracle
# calls; the (label-)ranked heuristic needs ~1. A learned compass would have to beat a
# heuristic that already ranks on the ground-truth phenotype label — so in-scope there
# is no room. Out-of-scope (μ>5) there are no labels to rank on: that is the trigger.
#
#   python3 webapp/scripts/exact_search_baseline.py --dataset datasets/latent-atlas-v0 \
#       --tasks benchmarks/tasks --budget 50 --out benchmarks/reports/exact_search_baseline.json
import sys, os, json, glob, argparse
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from design_search import CLASS_MAP, eval_row, iter_rows  # reuse the identical gate logic

def task_to_flat(task):
    bs = dict(task.get("behavior_spec", {}))
    flat = {k: v for k, v in bs.items() if k not in ("paraphrases",)}
    flat["min_robustness"] = (task.get("success") or {}).get("min_robustness_score", 0.2)
    flat.setdefault("max_reactions", 99)
    return flat

def miss_prob(N, P, B):
    """Pr[no passer in B random draws without replacement] = prod_{i<B}(N-P-i)/(N-i)."""
    if P <= 0:
        return 1.0
    if (N - P) < B:
        return 0.0
    p = 1.0
    for i in range(B):
        p *= (N - P - i) / (N - i)
    return p

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dataset", default="datasets/latent-atlas-v0")
    ap.add_argument("--tasks", default="benchmarks/tasks")
    ap.add_argument("--budget", type=int, default=50)  # task max_exact_verifier_calls
    ap.add_argument("--out", default="")
    a = ap.parse_args()
    rows = list(iter_rows(a.dataset))
    out_tasks = []
    for tf in sorted(glob.glob(os.path.join(a.tasks, "*.json"))):
        task = json.load(open(tf))
        tid = task.get("task_id", os.path.basename(tf))
        flat = task_to_flat(task)
        cls_key = CLASS_MAP.get(flat.get("behavior_class"))
        if not cls_key:
            continue
        B = (task.get("budget") or {}).get("max_exact_verifier_calls", a.budget)
        pool = [r for r in rows if r.get("n_reactions", 99) <= flat["max_reactions"]]
        # evaluate the gate on every candidate (the oracle verdict = labels)
        evald = []
        for r in pool:
            ss, reasons = eval_row(r, flat, cls_key)
            evald.append((ss, len(reasons) == 0, r))
        N = len(evald)
        passers = [e for e in evald if e[1]]
        P = len(passers)
        # exact (no compass, random order)
        exact_E_first = (N + 1) / (P + 1) if P > 0 else None
        exact_hit_in_budget = round(1.0 - miss_prob(N, P, B), 4)
        # ranked (retrieval heuristic: sort by shape_support desc, stable)
        order = sorted(range(N), key=lambda i: -evald[i][0])
        ranked_first = None
        for rank, i in enumerate(order, start=1):
            if evald[i][1]:
                ranked_first = rank
                break
        ranked_pass = {f"pass@{k}": (ranked_first is not None and ranked_first <= k) for k in (1, 5, 20)}
        out_tasks.append({
            "task_id": tid, "behavior_class": flat.get("behavior_class"),
            "max_reactions": flat["max_reactions"], "budget_B": B,
            "N_candidates": N, "P_passers": P,
            "base_rate": round(P / N, 6) if N else 0.0,
            "feasible": P > 0,
            "exact_E_oracle_to_first": round(exact_E_first, 1) if exact_E_first else None,
            "exact_hit_within_budget": exact_hit_in_budget,
            "ranked_oracle_to_first": ranked_first,
            **{f"ranked_{k}": v for k, v in ranked_pass.items()},
        })
    # aggregate
    feas = [t for t in out_tasks if t["feasible"]]
    n = len(out_tasks)
    agg = {
        "n_tasks": n, "n_feasible": len(feas),
        "exact_hit_within_budget_rate": round(sum(t["exact_hit_within_budget"] for t in out_tasks) / max(n, 1), 3),
        "ranked_pass@1_rate": round(sum(t["ranked_pass@1"] for t in out_tasks) / max(n, 1), 3),
        "ranked_pass@5_rate": round(sum(t["ranked_pass@5"] for t in out_tasks) / max(n, 1), 3),
        "mean_exact_E_oracle_to_first_feasible": round(sum(t["exact_E_oracle_to_first"] for t in feas) / max(len(feas), 1), 1) if feas else None,
        "mean_ranked_oracle_to_first_feasible": round(sum(t["ranked_oracle_to_first"] for t in feas) / max(len(feas), 1), 2) if feas else None,
    }
    report = {"report_schema": "bne-exact-search-baseline/v0.1.0",
              "note": "oracle verdict = precomputed phenotype labels (exhaustive μ≤5 atlas); exact = random-order brute force; ranked = shape_support retrieval heuristic",
              "aggregate": agg, "tasks": out_tasks}
    print(f"AGGREGATE {json.dumps(agg)}")
    print(f"{'task':40s} {'feas':4s} {'N':>6s} {'P':>5s} {'base':>8s} {'exactE':>8s} {'hit<B':>6s} {'rankd1st':>8s} r@1 r@5")
    for t in out_tasks:
        print(f"{t['task_id']:40s} {str(t['feasible']):4s} {t['N_candidates']:6d} {t['P_passers']:5d} "
              f"{t['base_rate']:8.5f} {str(t['exact_E_oracle_to_first']):>8s} {t['exact_hit_within_budget']:6.3f} "
              f"{str(t['ranked_oracle_to_first']):>8s} {int(t['ranked_pass@1'])}   {int(t['ranked_pass@5'])}")
    if a.out:
        os.makedirs(os.path.dirname(a.out), exist_ok=True)
        json.dump(report, open(a.out, "w"), indent=2)
        print(f"\n-> {a.out}")

if __name__ == "__main__":
    main()
