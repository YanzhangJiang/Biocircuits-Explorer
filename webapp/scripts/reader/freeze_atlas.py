"""freeze_atlas.py — Step-1 Freeze/Audit: pin an atlas_root's provenance + checksums
into manifests/freeze_audit.json so the store is a frozen, reproducible artifact.

Records: build command, schema/reader/phenotyper versions, content hashes of every
artifact (records.parquet, curves.zarr tree, all indexes), record counts/grid, and —
crucially — which artifacts are AGENT-FACING vs EXPLORATORY (Mapper/FCA/diffusion are
exploratory navigation aids, NOT part of any agent-facing scientific claim).

    python3 freeze_atlas.py <atlas_root> --build-command "build_indexes.py --packets ... --out ..."
"""
from __future__ import annotations
import argparse
import glob
import hashlib
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


def _sha_file(p):
    h = hashlib.sha256()
    with open(p, "rb") as f:
        for b in iter(lambda: f.read(1 << 20), b""):
            h.update(b)
    return h.hexdigest()


def _sha_tree(d):
    h = hashlib.sha256()
    for p in sorted(glob.glob(os.path.join(d, "**"), recursive=True)):
        if os.path.isfile(p):
            h.update(os.path.relpath(p, d).encode())
            with open(p, "rb") as f:
                for b in iter(lambda: f.read(1 << 20), b""):
                    h.update(b)
    return h.hexdigest()


AGENT_FACING = ["metadata/records.parquet", "arrays/curves.zarr", "rop/rop.parquet",
                "indexes/hnsw_function.bin", "indexes/knn_graph.parquet", "indexes/func_embeddings.npy"]
EXPLORATORY = ["indexes/diffusion_coords.parquet", "indexes/mapper_graph.json",
               "indexes/concept_lattice.json", "indexes/redescriptions.json"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("root")
    ap.add_argument("--build-command", default="")
    ap.add_argument("--built-at", default="")
    a = ap.parse_args()
    from reader import READER_VERSION, RESULT_SCHEMA
    man = json.load(open(os.path.join(a.root, "manifests", "atlas_manifest.json")))

    checks = {}
    for rel in AGENT_FACING + EXPLORATORY:
        p = os.path.join(a.root, rel)
        if rel.endswith(".zarr"):
            checks[rel] = "sha256-tree:" + _sha_tree(p) if os.path.isdir(p) else None
        elif os.path.isfile(p):
            checks[rel] = "sha256:" + _sha_file(p)

    freeze = {
        "schema": "function-atlas-freeze/v0.1.0",
        "frozen": True,
        "atlas_root": os.path.basename(a.root.rstrip("/")),
        "built_at": a.built_at,
        "build_command": a.build_command,
        "versions": {
            "atlas_store_schema": man.get("schema"),
            "reader_version": READER_VERSION,
            "reader_result_schema": RESULT_SCHEMA,
            "phenotyper_version": man.get("phenotyper_version"),
        },
        "n_records": man.get("n_records"), "K": man.get("K"), "n_points": man.get("n_points"),
        "grid": man.get("grid"), "records_content_hash": man.get("content_hash"),
        "agent_facing_artifacts": AGENT_FACING,
        "exploratory_artifacts": EXPLORATORY,
        "exploratory_note": ("Mapper graph, FCA concept lattice, redescriptions, and diffusion coords are "
                             "EXPLORATORY navigation/analysis aids ONLY — they are NOT part of any agent-facing "
                             "scientific claim and MUST NOT back a candidate shown to a user. Agent-facing "
                             "retrieval uses only: label recall (dominant_shape + shape_support) + function "
                             "search (HNSW over normalized-medoid curves) + ROP evidence (volume / "
                             "robust_path_count). The phenotyper-verified shape labels and the live engine "
                             "remain the sole truth source; every shown candidate is simulate-verified."),
        "checksums": checks,
    }
    out = os.path.join(a.root, "manifests", "freeze_audit.json")
    json.dump(freeze, open(out, "w"), indent=2)
    print(f"froze {freeze['atlas_root']}: {freeze['n_records']} records, "
          f"reader {READER_VERSION}, phenotyper {man.get('phenotyper_version')}, "
          f"{len([c for c in checks.values() if c])} artifacts checksummed → {out}")


if __name__ == "__main__":
    main()
