"""recall_repair.py — Atlas false-negative repair (recall fix) for behavior retrieval.

A coarse label/shape_support index (K=16) systematically MISSES topology/output pairs that have
rare-but-robust parameter pockets: a K=16 zero/low support is consistent (Jeffreys 95%) with several-%
true support, and many such pairs have engine-verified robust pockets (see verify_cs.jl). This module is
the recall fix: given a target behavior and a candidate pool (each carrying the atlas's coarse
shape_support), it returns the label hits AND — by running the cheap in-process C_S/Q_S probe on the
EXCLUDED low-support candidates — the ones with a verified robust pocket.

Plug into the reader: when a query's label retrieval is thin / coverage is sparse, call recall_repair(...)
instead of returning an absence certificate.

probe_fn(topos) -> {key: {"C_S":float, "Q_S":float}}.  Two implementations:
  - precomputed_probe(rows): reads a finished probe run (offline benchmarking / the demo below).
  - julia_probe(...): shells out to topo_capacity_cs.jl for live candidates (warm a Julia worker in prod;
    cold start ~15 s of compile, then ~0.3 s/candidate at K=8).
"""
from __future__ import annotations
import json, os, subprocess, tempfile


def topo_key(t):
    return json.dumps([t.get("reactions") or t.get("rules"), t["input_symbol"],
                       t.get("observe_species") or t.get("output_symbol")], sort_keys=True)


def recall_repair(target, candidates, *, probe_fn, shape_support_key="shape_support",
                  s_thresh=0.1875, c_thresh=0.6, q_thresh=0.1, realize_impl=0.7):
    """Return label hits + probe-recovered realizable designs the label index would miss."""
    label_hits = [c for c in candidates if (c.get(shape_support_key) or 0.0) >= s_thresh]
    excluded = [c for c in candidates if (c.get(shape_support_key) or 0.0) < s_thresh]
    probe = probe_fn(excluded) if excluded else {}
    recovered = []
    for c in excluded:
        p = probe.get(topo_key(c))
        if p and p["C_S"] >= c_thresh and p["Q_S"] >= q_thresh:
            recovered.append({**c, "C_S": p["C_S"], "Q_S": p["Q_S"], "recovered_by": "C_S/Q_S probe"})
    return {"target": target, "n_candidates": len(candidates), "n_label_hits": len(label_hits),
            "n_excluded": len(excluded), "n_probed": len(excluded), "n_recovered": len(recovered),
            "label_hits": label_hits, "recovered": recovered}


def precomputed_probe(rows, realize_impl=0.7):
    """Build probe_fn from a finished topo_capacity_cs.jl run (rows with by_impl)."""
    idx = {topo_key(r): {"C_S": r["by_impl"][str(realize_impl)]["C_S"],
                         "Q_S": r["by_impl"][str(realize_impl)]["frac_robust"]}
           for r in rows if "by_impl" in r and str(realize_impl) in r["by_impl"]}
    return lambda topos: {topo_key(t): idx[topo_key(t)] for t in topos if topo_key(t) in idx}


def julia_probe(realize_impl=0.7, n_kd=32, K=8, repo=None):
    """Live probe_fn: shell out to the in-process Julia estimator for arbitrary candidates."""
    repo = repo or os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    def fn(topos):
        if not topos:
            return {}
        tin = tempfile.mktemp(suffix=".json"); tout = tempfile.mktemp(suffix=".jsonl")
        json.dump([{"reactions": t.get("reactions") or t.get("rules"), "input_symbol": t["input_symbol"],
                    "observe_species": t.get("observe_species") or t.get("output_symbol")} for t in topos],
                  open(tin, "w"))
        subprocess.run(["julia", "--project=webapp", "webapp/scripts/synth/topo_capacity_cs.jl",
                        "--topos", tin, "--impls", str(realize_impl), "--n-kd", str(n_kd), "--K", str(K),
                        "--serial", "--out", tout], cwd=repo, check=True,
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        out = {}
        for l in open(tout):
            r = json.loads(l)
            if "by_impl" in r and str(realize_impl) in r["by_impl"]:
                out[topo_key(r)] = {"C_S": r["by_impl"][str(realize_impl)]["C_S"],
                                    "Q_S": r["by_impl"][str(realize_impl)]["frac_robust"]}
        return out
    return fn


def _demo():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--pool", default="benchmarks/reports/topo_capacity_bandpass.jsonl")
    ap.add_argument("--s-thresh", type=float, default=0.375)
    ap.add_argument("--realize-impl", type=float, default=0.7)
    a = ap.parse_args()
    rows = [json.loads(l) for l in open(a.pool) if l.strip()]
    # candidate pool: each carries the atlas's coarse shape_support (P_S_atlas)
    cand = [{"reactions": r["reactions"], "input_symbol": r["input_symbol"],
             "observe_species": r["observe_species"], "shape_support": r["P_S_atlas"]} for r in rows]
    probe = precomputed_probe(rows, a.realize_impl)
    res = recall_repair("bandpass_with_plateau", cand, probe_fn=probe, s_thresh=a.s_thresh,
                        realize_impl=a.realize_impl)
    tot = res["n_label_hits"] + res["n_recovered"]
    print(f"Query: bandpass_with_plateau   pool={res['n_candidates']}   label threshold shape_support>={a.s_thresh}")
    print(f"  label-only retrieval:        {res['n_label_hits']} hits")
    print(f"  probe on {res['n_excluded']} excluded -> recovered: {res['n_recovered']} verified-realizable designs")
    print(f"  TOTAL with recall repair:    {tot}   (recall fix: +{res['n_recovered']} the label index would have missed)")
    print("  sample recovered (atlas said low/none, probe verified a robust pocket):")
    for c in sorted(res["recovered"], key=lambda c: c["shape_support"])[:5]:
        print(f"    in={c['input_symbol']:<4} out={c['observe_species']:<12} atlas_support={c['shape_support']:.3f} "
              f"-> C_S={c['C_S']:.2f} Q_S={c['Q_S']:.2f}")


if __name__ == "__main__":
    _demo()
