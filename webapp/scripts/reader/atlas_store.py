"""atlas_store.py — Stage-2 S2.1: read the industrial atlas_root (Parquet metadata +
Zarr curve arrays) as a DROP-IN for PacketStore, so reader.py / benchmark.py run
unchanged on the migrated store. Metadata is also DuckDB-queryable directly
(`duckdb.sql("select ... from 'atlas_root/metadata/records.parquet'")`).
"""
from __future__ import annotations
import json
import os
import numpy as np


class AtlasStore:
    def __init__(self, root: str):
        # Keep the packet-corpus fallback usable with only NumPy.  Parquet and
        # Zarr are optional, industrial-atlas dependencies and must not be
        # imported merely to dispatch ``open_store`` to ``PacketStore``.
        try:
            import pyarrow.parquet as pq
            import zarr
        except ModuleNotFoundError as exc:
            raise ModuleNotFoundError(
                "opening an atlas_root requires the optional Reader dependencies "
                "'pyarrow' and 'zarr'"
            ) from exc

        self.root = root
        self.manifest = json.load(open(os.path.join(root, "manifests", "atlas_manifest.json")))
        self.K = int(self.manifest["K"]); self.n_points = int(self.manifest["n_points"])
        g = self.manifest["grid"]
        self.u_grid = np.linspace(float(g["lo"]), float(g["hi"]), self.n_points)
        # eager-load the (N,K,P) curve array into RAM (~N·K·P·4 bytes, e.g. 121 MB at 59k×8×64).
        # zarr v3 re-decompresses the whole chunk on each single-row access, so per-record reads
        # are pathologically slow at scale — read it once, index in memory (like PacketStore's memmap).
        self._curves = np.asarray(zarr.open(os.path.join(root, "arrays", "curves.zarr"), mode="r")[:], dtype=np.float32)
        self.records = []
        for row in pq.read_table(os.path.join(root, "metadata", "records.parquet")).to_pylist():
            self.records.append({
                "record_id": row["record_id"], "network_id": row["network_id"], "slice_id": row["slice_id"],
                "io_assignment": {"input_symbol": row["input_symbol"], "output_symbol": row["output_symbol"]},
                "rules": list(row["rules"] or []), "n_reactions": row["n_reactions"],
                "dominant_shape": row["dominant_shape"],
                "shape_fractions": {row["dominant_shape"]: row["shape_support"]},
                "medoid_draw_id": row["medoid_draw_id"],
                "validity_summary": {"n_draws": row["n_draws"], "n_valid": row["n_valid"], "n_failed": row["n_failed"]},
                "rop_summary": {"atlas_family_label": row["rop_family"], "atlas_volume_mean": row["volume_mean"],
                                "atlas_robust_path_count": row["robust_path_count"]},
                "_curve_row": int(row["curve_row"]), "_valid_mask": list(row["valid_mask"] or []),
            })
        self._by_id = {r["record_id"]: r for r in self.records}

    def __len__(self):
        return len(self.records)

    @property
    def n_shards(self):
        return 1

    def _rec(self, rec):
        return self._by_id[rec] if isinstance(rec, str) else rec

    def curves(self, rec) -> np.ndarray:
        return self._curves[self._rec(rec)["_curve_row"]]

    def valid_mask(self, rec) -> np.ndarray:
        vm = self._rec(rec)["_valid_mask"]
        return np.array(vm if len(vm) == self.K else [True] * self.K, dtype=bool)

    def valid_curves(self, rec) -> np.ndarray:
        return self.curves(rec)[self.valid_mask(rec)]

    def medoid_curve(self, rec):
        mid = int(self._rec(rec)["medoid_draw_id"])
        return None if mid == 0 else self.curves(rec)[mid - 1]

    def envelope(self, rec, qs=(0.1, 0.5, 0.9)):
        C = self.valid_curves(rec)
        return None if C.shape[0] == 0 else {q: np.nanquantile(C, q, axis=0) for q in qs}


def open_store(path: str):
    """Open an atlas_root (Parquet+Zarr) if present, else a spike-curve-packet corpus."""
    if os.path.isfile(os.path.join(path, "manifests", "atlas_manifest.json")):
        return AtlasStore(path)
    import sys
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from packet_store import PacketStore
    return PacketStore(path)
