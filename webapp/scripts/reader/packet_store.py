"""packet_store.py — Function-Space Atlas spike, Reader layer (S2 foundation).

Loads spike-curve-packet/v0 data and exposes the K-draw curve PACKET per slice as
numpy arrays. Phenotype is prior-conditioned, so the unit of retrieval is the
*distribution* of K curves on a shared log-input grid — never a single curve. The
medoid + per-point quantile envelope are derived here; the three score modes
(existential / typical / robust) build on top of this.

Accepts EITHER a single packet dir (manifest.json + curves.f32 + packets_index.jsonl)
OR a sharded corpus — a parent dir containing `shards/shard_*/` (the bmac full-corpus
layout) or `shard_*/` directly. Each shard keeps its own curves.f32 (offsets are
per-shard); reads are routed to the owning shard's buffer.

    python3 webapp/scripts/reader/packet_store.py /tmp/curve-packets-600
    python3 webapp/scripts/reader/packet_store.py /path/to/curve-packets-v0   # sharded
"""
from __future__ import annotations
import glob
import json
import os
import numpy as np


def _is_packet_dir(d):
    return (os.path.isfile(os.path.join(d, "manifest.json"))
            and os.path.isfile(os.path.join(d, "packets_index.jsonl"))
            and os.path.isfile(os.path.join(d, "curves.f32")))


def _discover_shards(root):
    """Return the list of packet dirs under `root` (itself, or its shard_* children)."""
    if _is_packet_dir(root):
        return [root]
    for pat in ("shards/shard_*", "shard_*", "shards/*"):
        dirs = sorted(d for d in glob.glob(os.path.join(root, pat)) if _is_packet_dir(d))
        if dirs:
            return dirs
    raise FileNotFoundError(f"no spike-curve-packet dir(s) found under {root}")


class _Shard:
    __slots__ = ("dir", "manifest", "buf")

    def __init__(self, d):
        self.dir = d
        self.manifest = json.load(open(os.path.join(d, "manifest.json")))
        self.buf = np.memmap(os.path.join(d, self.manifest["curves_file"]), dtype="<f4", mode="r")


class PacketStore:
    def __init__(self, root: str):
        self.root = root
        self.shard_dirs = _discover_shards(root)
        self._shards = [_Shard(d) for d in self.shard_dirs]
        self.manifest = self._shards[0].manifest
        g = self._shards[0].manifest["grid"]
        self.n_points = int(g["n_points"])
        self.K = int(self._shards[0].manifest["K"])
        self.u_grid = np.linspace(float(g["lo"]), float(g["hi"]), self.n_points)
        # unified record list; each record tagged with its owning shard index
        self.records = []
        for si, sh in enumerate(self._shards):
            for r in (json.loads(l) for l in open(os.path.join(sh.dir, "packets_index.jsonl")) if l.strip()):
                r["_shard"] = si
                self.records.append(r)
        self._by_id = {r["record_id"]: r for r in self.records}
        # optional ROP-evidence side file (slice_id -> rop_summary) at the corpus root —
        # for corpora whose packets were built WITHOUT family_buckets (e.g. d4); attaches/
        # fills rop_summary so the ROP arms work without rebuilding curves.
        self.rop_attached = 0
        rop_path = os.path.join(root, "rop_evidence.jsonl")
        if os.path.isfile(rop_path):
            rop = {}
            for l in open(rop_path):
                if l.strip():
                    e = json.loads(l); rop[e["slice_id"]] = e
            for rec in self.records:
                e = rop.get(rec.get("slice_id"))
                if e:
                    rec["rop_summary"] = {"atlas_family_label": e.get("atlas_family_label"),
                                          "atlas_volume_mean": e.get("atlas_volume_mean"),
                                          "atlas_robust_path_count": e.get("atlas_robust_path_count")}
                    self.rop_attached += 1

    def __len__(self):
        return len(self.records)

    @property
    def n_shards(self):
        return len(self._shards)

    def _rec(self, rec):
        return self._by_id[rec] if isinstance(rec, str) else rec

    def curves(self, rec) -> np.ndarray:
        """The (K, n_points) packet of log10-observable curves for a record."""
        rec = self._rec(rec)
        p = rec["curve_array_pointer"]
        buf = self._shards[rec["_shard"]].buf
        off = p["offset_bytes"] // 4
        n = p["n_draws"] * p["n_points"]
        return np.asarray(buf[off:off + n], dtype=np.float32).reshape(p["n_draws"], p["n_points"])

    def valid_mask(self, rec) -> np.ndarray:
        return np.array([bool(d["ok"]) for d in self._rec(rec)["draws"]], dtype=bool)

    def valid_curves(self, rec) -> np.ndarray:
        return self.curves(rec)[self.valid_mask(rec)]

    def medoid_curve(self, rec):
        rec = self._rec(rec)
        mid = int(rec["medoid_draw_id"])
        return None if mid == 0 else self.curves(rec)[mid - 1]

    def envelope(self, rec, qs=(0.1, 0.5, 0.9)):
        """Per-point quantile envelope across the valid draws (derived, not stored)."""
        C = self.valid_curves(rec)
        if C.shape[0] == 0:
            return None
        return {q: np.nanquantile(C, q, axis=0) for q in qs}


# ── self-check ────────────────────────────────────────────────────────────────
def _net_peak(y, u):
    fin = np.isfinite(y)
    yv = y[fin]
    if yv.size < 3:
        return None
    imax = int(np.nanargmax(y)); imin = int(np.nanargmin(y))
    return dict(net=float(yv[-1] - yv[0]), rng=float(np.nanmax(y) - np.nanmin(y)),
                u_max=float(u[imax]), interior_peak=(0 < imax < len(y) - 1),
                interior_trough=(0 < imin < len(y) - 1))


def _consistent(dom, s):
    if s is None:
        return dom in ("flat", "none", "complex")
    if dom in ("monotone_activation", "thresholded_activation"):
        return s["net"] > 0.3 and not s["interior_peak"]
    if dom == "monotone_repression":
        return s["net"] < -0.3 and not s["interior_trough"]
    if dom in ("biphasic_peak", "bandpass_with_plateau"):
        return s["interior_peak"]
    if dom == "biphasic_valley":
        return s["interior_trough"]
    if dom == "flat":
        return s["rng"] < 0.3
    return True


if __name__ == "__main__":
    import sys
    root = sys.argv[1] if len(sys.argv) > 1 else "/tmp/curve-packets-600"
    st = PacketStore(root)
    print(f"loaded {len(st)} records across {st.n_shards} shard(s)  K={st.K}  n_points={st.n_points}")
    print(f"shard dirs: {', '.join(os.path.basename(d) for d in st.shard_dirs[:6])}"
          f"{' …' if st.n_shards > 6 else ''}")
    # spot-check label/curve consistency on a sample spread across shards
    sample = st.records[:: max(1, len(st) // 200)]
    ncons = ntot = 0
    for r in sample:
        med = st.medoid_curve(r)
        s = _net_peak(med, st.u_grid) if med is not None else None
        ncons += int(_consistent(r["dominant_shape"], s)); ntot += 1
    print(f"label/curve consistency on {ntot}-record sample: {ncons}/{ntot}")
    import collections
    hist = collections.Counter(r["dominant_shape"] for r in st.records)
    print("dominant-shape histogram:", dict(hist.most_common()))
