"""S2.2 — function-space kNN graph + HNSW ANN index over an atlas_root.

Embedding = each record's normalized medoid dose-response curve (n_points dims) — the
function-shape vector. Builds:
  indexes/func_embeddings.npy      (N, n_points) float32   — reused by S2.3/S2.4
  indexes/hnsw_function.bin         HNSW L2 index           — sub-ms NN over the corpus
  indexes/knn_graph.parquet         k-NN edges (src,dst,dist) — graph for diffusion/mapper

Turns the Reader's linear scan into ANN retrieval; the kNN graph is the substrate for
diffusion maps (S2.3) and Mapper (S2.4).

    python3 build_function_index.py <atlas_root> [--k 15]
"""
from __future__ import annotations
import argparse
import os
import sys
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import hnswlib

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from atlas_store import open_store
from compare import normalize


def func_embeddings(store):
    """(N, n_points) normalized-medoid embedding; NaN→0; missing medoid→zeros."""
    N, P = len(store), store.n_points
    E = np.zeros((N, P), np.float32)
    for i, r in enumerate(store.records):
        m = store.medoid_curve(r)
        if m is not None:
            E[i] = np.nan_to_num(normalize(m, "minmax"), nan=0.0)
    return E


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("root")
    ap.add_argument("--k", type=int, default=15)
    a = ap.parse_args()
    st = open_store(a.root)
    idxdir = os.path.join(a.root, "indexes"); os.makedirs(idxdir, exist_ok=True)
    N, P = len(st), st.n_points
    E = func_embeddings(st)
    np.save(os.path.join(idxdir, "func_embeddings.npy"), E)

    idx = hnswlib.Index(space="l2", dim=P)
    idx.init_index(max_elements=N, ef_construction=200, M=16)
    idx.add_items(E, np.arange(N))
    idx.set_ef(max(64, a.k * 4))
    idx.save_index(os.path.join(idxdir, "hnsw_function.bin"))

    labels, dists = idx.knn_query(E, k=min(a.k + 1, N))
    src, dst, dd = [], [], []
    for i in range(N):
        for nbr, d2 in zip(labels[i], dists[i]):
            if int(nbr) != i:
                src.append(i); dst.append(int(nbr)); dd.append(float(np.sqrt(max(d2, 0.0))))
    pq.write_table(pa.table({"src": pa.array(src, pa.int32()), "dst": pa.array(dst, pa.int32()),
                             "dist": pa.array(dd, pa.float32())}),
                   os.path.join(idxdir, "knn_graph.parquet"))
    print(f"S2.2: {N} nodes, embedding dim {P}, kNN graph {len(src)} edges (k={a.k}); "
          f"hnsw_function.bin + knn_graph.parquet + func_embeddings.npy in indexes/")


if __name__ == "__main__":
    main()
