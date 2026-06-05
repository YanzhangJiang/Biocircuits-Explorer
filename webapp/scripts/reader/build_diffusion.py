"""S2.3 — diffusion-map coordinates (Coifman–Lafon) over the S2.2 kNN graph.

Symmetric-normalized affinity M = D^-1/2 W D^-1/2 from the kNN edges (Gaussian kernel,
median bandwidth), top eigenvectors → low-dim diffusion coordinates that preserve
local→global function-space geometry. Used for neighborhood exploration and to tell a
local modification (small move) from a jump to another behavior stratum.

  indexes/diffusion_coords.parquet   record_id + c0..c{M-1}

    python3 build_diffusion.py <atlas_root> [--coords 8]
"""
from __future__ import annotations
import argparse
import os
import sys
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import scipy.sparse as sp
from scipy.sparse.linalg import eigsh, ArpackNoConvergence

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from atlas_store import open_store


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("root")
    ap.add_argument("--coords", type=int, default=8)
    a = ap.parse_args()
    st = open_store(a.root)
    idxdir = os.path.join(a.root, "indexes")
    g = pq.read_table(os.path.join(idxdir, "knn_graph.parquet")).to_pydict()
    N = len(st); M = min(a.coords, N - 2)
    rows = np.array(g["src"]); cols = np.array(g["dst"]); dist = np.array(g["dist"], float)
    # self-tuning bandwidth σ_i = distance to the κ-th neighbour (Zelnik-Manor–Perona),
    # so cross-cluster edges don't vanish; self-loops bound the degree (no D^-1/2 blow-up).
    kappa = 7
    per = [[] for _ in range(N)]
    for s_, d_ in zip(rows, dist):
        per[s_].append(d_)
    med = float(np.median(dist)) or 1.0
    sig = np.array([sorted(p)[min(kappa, len(p) - 1)] if p else med for p in per])
    sig = np.maximum(sig, 1e-6)
    w = np.exp(-(dist ** 2) / (sig[rows] * sig[cols]))
    W = sp.csr_matrix((w, (rows, cols)), shape=(N, N))
    W = W.maximum(W.T) + sp.identity(N, format="csr")    # symmetrize + self-loops
    d = np.maximum(np.asarray(W.sum(1)).ravel(), 1e-6)
    Dinv2 = sp.diags(1.0 / np.sqrt(d))
    sigma = float(np.median(sig))
    Ms = (Dinv2 @ W @ Dinv2).tocsr()
    # λ clusters near 1 when the function space splits into shape strata → bound the
    # Lanczos solve (maxiter/tol/ncv) so it can't hang; take partial vectors if it stops short.
    ncv = min(N - 1, max(4 * (M + 1) + 20, 40))
    try:
        vals, vecs = eigsh(Ms, k=M + 1, which="LM", tol=1e-3, maxiter=800, ncv=ncv)
    except ArpackNoConvergence as e:
        vals, vecs = e.eigenvalues, e.eigenvectors
        if vecs.shape[1] < 2:
            raise
        M = vecs.shape[1] - 1
    order = np.argsort(-vals); vals = vals[order]; vecs = vecs[:, order]
    psi = Dinv2 @ vecs                                   # (N, M+1)
    coords = (psi[:, 1:M + 1] * vals[1:M + 1]).astype(np.float32)   # drop trivial λ0

    tbl = {"record_id": pa.array([r["record_id"] for r in st.records], pa.string())}
    for c in range(coords.shape[1]):
        tbl[f"c{c}"] = pa.array(coords[:, c], pa.float32())
    pq.write_table(pa.table(tbl), os.path.join(idxdir, "diffusion_coords.parquet"))
    print(f"S2.3: diffusion coords {coords.shape} (σ={sigma:.3f}, λ1..λ3={np.round(vals[1:4],3)}) "
          f"→ diffusion_coords.parquet")


if __name__ == "__main__":
    main()
