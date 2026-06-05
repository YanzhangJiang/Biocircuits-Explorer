"""S2.5 — Formal Concept Analysis lattice + redescription mining.

Derives view-tagged boolean PREDICATES per record (function / ROP / network / evidence),
builds an FCA concept lattice (objects=records, attributes=predicates) → data-derived
behaviour concepts (intent predicates + extent), and mines REDESCRIPTIONS: cross-view
predicate pairs that describe ~the same record set (high Jaccard) — i.e. "this function
shape ⇔ that ROP/network structure" mechanism hypotheses.

  indexes/concept_lattice.json   {n_concepts, top_concepts:[{intent,extent_size,views}]}
  indexes/redescriptions.json     [{function_pred, structural_pred, jaccard, support}]

    python3 build_concepts.py <atlas_root> [--max-records 8000]
"""
from __future__ import annotations
import argparse
import json
import os
import sys
import numpy as np

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from atlas_store import open_store
from benchmark import f_interior_peak, f_interior_valley, f_broad_plateau

# predicate name -> view
VIEWS = {"peak": "function", "valley": "function", "mono_up": "function", "mono_down": "function",
         "plateau": "function", "interior_peak": "function",
         "rop_supported": "rop", "high_volume": "rop",
         "robust": "evidence", "small_net": "network", "has_homodimer": "network"}


def _homodimer(rules):
    for s in rules:
        parts = [p.strip() for p in s.split("<->")[0].split("+")]
        if len(parts) == 2 and parts[0] == parts[1]:
            return True
    return False


def predicates(st):
    names = list(VIEWS)
    N = len(st); B = np.zeros((N, len(names)), bool)
    u = st.u_grid
    for i, r in enumerate(st.records):
        dom = r.get("dominant_shape"); rs = r.get("rop_summary") or {}
        supp = float((r.get("shape_fractions") or {}).get(dom, 0.0))
        med = st.medoid_curve(r)
        rules = r.get("rules") or []
        homod = _homodimer(rules)
        vals = {
            "peak": dom in ("biphasic_peak", "bandpass_with_plateau"),
            "valley": dom == "biphasic_valley",
            "mono_up": dom == "monotone_activation", "mono_down": dom == "monotone_repression",
            "plateau": dom == "bandpass_with_plateau",
            "interior_peak": bool(med is not None and f_interior_peak(med, u)),
            "rop_supported": float(rs.get("atlas_robust_path_count") or 0) > 0,
            "high_volume": float(rs.get("atlas_volume_mean") or 0) >= 0.05,
            "robust": supp >= 0.8,
            "small_net": (r.get("n_reactions") or 99) <= 4,
            "has_homodimer": homod,
        }
        for j, n in enumerate(names):
            B[i, j] = vals[n]
    return names, B


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("root")
    ap.add_argument("--max-records", type=int, default=8000)
    a = ap.parse_args()
    st = open_store(a.root)
    names, B = predicates(st)
    idxdir = os.path.join(a.root, "indexes")

    # ---- FCA concept lattice (on distinct predicate rows; sample objects if huge) ----
    import concepts
    N = len(st)
    obj_idx = list(range(N)) if N <= a.max_records else list(range(0, N, N // a.max_records + 1))
    objs = tuple(st.records[i]["record_id"] for i in obj_idx)
    bools = [tuple(bool(x) for x in B[i]) for i in obj_idx]
    ctx = concepts.Context(objs, tuple(names), bools)
    concs = []
    for c in ctx.lattice:
        ext, intent = c.extent, c.intent
        if intent and 3 <= len(ext) and len(ext) < len(objs):   # drop top/bottom + tiny
            concs.append({"intent": list(intent), "extent_size": len(ext),
                          "views": sorted({VIEWS[p] for p in intent})})
    concs.sort(key=lambda c: -c["extent_size"])
    json.dump({"schema": "function-atlas-concepts/v0.1.0", "n_predicates": len(names),
               "n_concepts": len(concs), "top_concepts": concs[:25]},
              open(os.path.join(idxdir, "concept_lattice.json"), "w"), indent=1)

    # ---- redescription mining: function-view ⇔ structural-view, high Jaccard ----
    ext = {n: set(np.where(B[:, j])[0]) for j, n in enumerate(names)}
    funcs = [n for n in names if VIEWS[n] == "function"]
    structs = [n for n in names if VIEWS[n] in ("rop", "network", "evidence")]
    red = []
    for f in funcs:
        for s in structs:
            A_, Bx = ext[f], ext[s]
            if len(A_) < 15 or len(Bx) < 15:
                continue
            j = len(A_ & Bx) / max(len(A_ | Bx), 1)
            red.append({"function_pred": f, "structural_pred": s, "jaccard": round(j, 3),
                        "support": len(A_ & Bx), "interesting": bool(j >= 0.4)})
    red.sort(key=lambda x: -x["jaccard"]); red = red[:15]
    n_int = sum(x["interesting"] for x in red)
    json.dump({"schema": "function-atlas-redescriptions/v0.1.0", "n_interesting": n_int, "redescriptions": red},
              open(os.path.join(idxdir, "redescriptions.json"), "w"), indent=1)
    print(f"S2.5: FCA {len(concs)} concepts over {len(names)} predicates ({len(objs)} objects); "
          f"top cross-view redescriptions {n_int} interesting (Jaccard≥0.4) → concept_lattice.json + redescriptions.json")


if __name__ == "__main__":
    main()
