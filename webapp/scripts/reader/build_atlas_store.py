"""build_atlas_store.py — Stage-2 S2.1: migrate a spike-curve-packet corpus to the
industrial atlas_root layout (proposal §5). Lossless re-encoding:

    atlas_root/
      manifests/atlas_manifest.json        versioned, content-addressed
      metadata/records.parquet             columnar record metadata (DuckDB-queryable)
      arrays/curves.zarr                    (N, K, n_points) float32, chunked
      rop/rop.parquet                       per-slice ROP evidence
      indexes/                              (scaffold: knn / mapper / diffusion / concepts)
      benchmarks/  logs/

`AtlasStore` (atlas_store.py) reads this back as a drop-in for `PacketStore`, so the
Reader/benchmark run unchanged. Metadata and arrays are separated; raw arrays stored
once and referenced by row.

    python3 build_atlas_store.py --in <packet-corpus> --out <atlas_root> [--chunk 4096]
"""
from __future__ import annotations
import argparse
import hashlib
import json
import os
import sys
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import zarr

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from packet_store import PacketStore

STORE_SCHEMA = "function-atlas-store/v0.1.0"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", required=True)
    ap.add_argument("--out", dest="out", required=True)
    ap.add_argument("--chunk", type=int, default=4096)
    a = ap.parse_args()

    st = PacketStore(a.inp)
    N, K, P = len(st), st.K, st.n_points
    for d in ("manifests", "metadata", "arrays", "rop", "indexes", "benchmarks", "logs"):
        os.makedirs(os.path.join(a.out, d), exist_ok=True)

    # curve arrays → Zarr (N, K, n_points), chunked over records
    z = zarr.open(os.path.join(a.out, "arrays", "curves.zarr"), mode="w",
                  shape=(N, K, P), chunks=(min(a.chunk, N), K, P), dtype="float32")

    cols = {k: [] for k in ("record_id", "network_id", "slice_id", "input_symbol", "output_symbol",
                            "n_reactions", "rules", "dominant_shape", "shape_support", "volume_mean",
                            "robust_path_count", "rop_family", "medoid_draw_id", "n_draws", "n_valid",
                            "n_failed", "valid_mask", "curve_row")}
    rop = {k: [] for k in ("slice_id", "atlas_family_label", "atlas_volume_mean", "atlas_robust_path_count")}
    buf = np.empty((min(a.chunk, N), K, P), np.float32); bi = 0; base = 0
    for i, r in enumerate(st.records):
        buf[bi] = st.curves(r); bi += 1
        if bi == buf.shape[0] or i == N - 1:
            z[base:base + bi] = buf[:bi]; base += bi; bi = 0
        io = r.get("io_assignment") or {}
        rs = r.get("rop_summary") or {}
        vs = r.get("validity_summary") or {}
        dom = r.get("dominant_shape")
        cols["record_id"].append(r.get("record_id")); cols["network_id"].append(r.get("network_id"))
        cols["slice_id"].append(r.get("slice_id")); cols["input_symbol"].append(io.get("input_symbol"))
        cols["output_symbol"].append(io.get("output_symbol")); cols["n_reactions"].append(r.get("n_reactions"))
        cols["rules"].append([str(x) for x in (r.get("rules") or [])]); cols["dominant_shape"].append(dom)
        cols["shape_support"].append(float((r.get("shape_fractions") or {}).get(dom, 0.0)))
        cols["volume_mean"].append(float(rs.get("atlas_volume_mean") or 0.0))
        cols["robust_path_count"].append(int(rs.get("atlas_robust_path_count") or 0))
        cols["rop_family"].append(rs.get("atlas_family_label"))
        cols["medoid_draw_id"].append(int(r.get("medoid_draw_id") or 0))
        cols["n_draws"].append(int(vs.get("n_draws") or K)); cols["n_valid"].append(int(vs.get("n_valid") or K))
        cols["n_failed"].append(int(vs.get("n_failed") or 0))
        cols["valid_mask"].append([bool(d.get("ok")) for d in (r.get("draws") or [])] or [True] * K)
        cols["curve_row"].append(i)
        rop["slice_id"].append(r.get("slice_id")); rop["atlas_family_label"].append(rs.get("atlas_family_label"))
        rop["atlas_volume_mean"].append(float(rs.get("atlas_volume_mean") or 0.0))
        rop["atlas_robust_path_count"].append(int(rs.get("atlas_robust_path_count") or 0))

    rec_tbl = pa.table({
        "record_id": pa.array(cols["record_id"], pa.string()), "network_id": pa.array(cols["network_id"], pa.string()),
        "slice_id": pa.array(cols["slice_id"], pa.string()), "input_symbol": pa.array(cols["input_symbol"], pa.string()),
        "output_symbol": pa.array(cols["output_symbol"], pa.string()), "n_reactions": pa.array(cols["n_reactions"], pa.int32()),
        "rules": pa.array(cols["rules"], pa.list_(pa.string())), "dominant_shape": pa.array(cols["dominant_shape"], pa.string()),
        "shape_support": pa.array(cols["shape_support"], pa.float32()), "volume_mean": pa.array(cols["volume_mean"], pa.float32()),
        "robust_path_count": pa.array(cols["robust_path_count"], pa.int32()), "rop_family": pa.array(cols["rop_family"], pa.string()),
        "medoid_draw_id": pa.array(cols["medoid_draw_id"], pa.int32()), "n_draws": pa.array(cols["n_draws"], pa.int32()),
        "n_valid": pa.array(cols["n_valid"], pa.int32()), "n_failed": pa.array(cols["n_failed"], pa.int32()),
        "valid_mask": pa.array(cols["valid_mask"], pa.list_(pa.bool_())), "curve_row": pa.array(cols["curve_row"], pa.int32()),
    })
    pq.write_table(rec_tbl, os.path.join(a.out, "metadata", "records.parquet"))
    pq.write_table(pa.table({
        "slice_id": pa.array(rop["slice_id"], pa.string()), "atlas_family_label": pa.array(rop["atlas_family_label"], pa.string()),
        "atlas_volume_mean": pa.array(rop["atlas_volume_mean"], pa.float32()),
        "atlas_robust_path_count": pa.array(rop["atlas_robust_path_count"], pa.int32()),
    }), os.path.join(a.out, "rop", "rop.parquet"))

    chash = hashlib.sha256(open(os.path.join(a.out, "metadata", "records.parquet"), "rb").read()).hexdigest()
    g = st._shards[0].manifest.get("grid", {})
    manifest = {"schema": STORE_SCHEMA, "source_corpus": a.inp, "n_records": N, "K": K, "n_points": P,
                "grid": g, "phenotyper_version": st._shards[0].manifest.get("phenotyper_version"),
                "content_hash": "sha256:" + chash, "layout": "metadata/records.parquet + arrays/curves.zarr",
                "arrays": {"curves": "arrays/curves.zarr", "shape": [N, K, P], "dtype": "float32"}}
    json.dump(manifest, open(os.path.join(a.out, "manifests", "atlas_manifest.json"), "w"), indent=2)
    print(f"migrated {N} records → {a.out}  (records.parquet {os.path.getsize(os.path.join(a.out,'metadata','records.parquet'))//1024} KB, "
          f"curves.zarr {N}×{K}×{P}, content {chash[:12]})")


if __name__ == "__main__":
    main()
