"""S2.4 — Mapper graph of the function space (interpretable atlas map).

Lens = [diffusion-c0 (function-geometry axis), volume_mean (ROP robustness)] over an
overlapping cover, clustered within each cover element on the function embedding →
nodes (clusters of related response functions) + edges (shared members). Each node is
summarized by its dominant shape, size, mean ROP volume, mean shape_support — an
explorable behaviour-space map for agents and humans.

  indexes/mapper_graph.json   {nodes:[{id,size,dominant_shape,shape_mix,mean_vol,mean_support,members}], links:[[a,b]]}

    python3 build_mapper.py <atlas_root> [--cubes 9 --overlap 0.35]
"""
from __future__ import annotations
import argparse
import collections
import json
import os
import sys
import numpy as np
import pyarrow.parquet as pq
import kmapper as km
from sklearn.cluster import DBSCAN

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from atlas_store import open_store


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("root")
    ap.add_argument("--cubes", type=int, default=9)
    ap.add_argument("--overlap", type=float, default=0.35)
    ap.add_argument("--max-records", type=int, default=12000)
    a = ap.parse_args()
    st = open_store(a.root); idxdir = os.path.join(a.root, "indexes")
    E_all = np.load(os.path.join(idxdir, "func_embeddings.npy"))
    diff = pq.read_table(os.path.join(idxdir, "diffusion_coords.parquet")).to_pydict()
    c0_all = np.array(diff["c0"], float)
    vol_all = np.array([float((r.get("rop_summary") or {}).get("atlas_volume_mean") or 0.0) for r in st.records])
    N = len(st)
    # subsample for Mapper — DBSCAN/kmapper segfault on the full 59k/87k; a sample is a fine
    # navigation map (node members map back to full record_ids; others fall back to NN).
    sample = (np.arange(N) if N <= a.max_records
              else np.sort(np.random.RandomState(0).choice(N, a.max_records, replace=False)))
    E = E_all[sample]
    shapes = [st.records[i]["dominant_shape"] for i in sample]
    vol = vol_all[sample]
    lens = np.column_stack([c0_all[sample], vol]).astype(float)   # function-geometry × ROP-robustness

    mapper = km.KeplerMapper(verbose=0)
    graph = mapper.map(lens, X=E, cover=km.Cover(n_cubes=a.cubes, perc_overlap=a.overlap),
                       clusterer=DBSCAN(eps=0.9, min_samples=3))
    nodes = []
    for nid, members in graph["nodes"].items():
        sm = collections.Counter(shapes[m] for m in members)
        dom, _ = sm.most_common(1)[0]
        nodes.append({"id": nid, "size": len(members), "dominant_shape": dom,
                      "shape_mix": dict(sm.most_common(3)),
                      "mean_vol": round(float(vol[members].mean()), 4),
                      "mean_support": round(float(np.mean([(st.records[int(sample[m])].get("shape_fractions") or {}).get(shapes[m], 0) for m in members])), 3),
                      "members": [st.records[int(sample[m])]["record_id"] for m in members]})
    links = [[a_, b_] for a_, bs in graph["links"].items() for b_ in bs]
    purity = np.mean([max(n["shape_mix"].values()) / n["size"] for n in nodes]) if nodes else 0.0
    json.dump({"schema": "function-atlas-mapper/v0.1.0", "lens": ["diffusion_c0", "rop_volume_mean"],
               "n_nodes": len(nodes), "n_links": len(links), "node_purity": round(float(purity), 3),
               "nodes": nodes, "links": links},
              open(os.path.join(idxdir, "mapper_graph.json"), "w"), indent=1)
    print(f"S2.4: Mapper {len(nodes)} nodes / {len(links)} links, node shape-purity {purity:.3f} "
          f"→ mapper_graph.json")


if __name__ == "__main__":
    main()
