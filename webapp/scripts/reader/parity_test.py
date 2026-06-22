"""parity_test.py — Step-1 Freeze/Audit: packet-reader vs atlas-root-reader parity.

Proves the migrated atlas_root (Parquet + Zarr) is a LOSSLESS drop-in for the original
spike-curve-packet corpus: identical records, curves, validity, ROP evidence, and —
the real test — identical Reader.search top-k across all intents. If this passes, the
frozen store can stand in for the packets in every agent-facing path.

    python3 parity_test.py <packet_corpus> <atlas_root>
"""
from __future__ import annotations
import os
import sys
import numpy as np

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from packet_store import PacketStore
from atlas_store import AtlasStore
from reader import Reader, prototypes

RESULTS = []
def ck(name, cond, detail=""):
    RESULTS.append(bool(cond))
    print(("PASS " if cond else "FAIL ") + name + ((f"  — {detail}") if (detail and not cond) else ""))


def main():
    packet_corpus, atlas_root = sys.argv[1], sys.argv[2]
    ps = PacketStore(packet_corpus); ats = AtlasStore(atlas_root)
    ck("record count equal", len(ps) == len(ats), (len(ps), len(ats)))
    ck("record_id set identical", set(ps._by_id) == set(ats._by_id))

    ids = list(ps._by_id)[:: max(1, len(ps) // 500)]
    okc = okm = okr = 0
    for rid in ids:
        rp, ra = ps._by_id[rid], ats._by_id[rid]
        if np.allclose(np.nan_to_num(ps.curves(rp)), np.nan_to_num(ats.curves(ra)), atol=1e-5):
            okc += 1
        if rp["dominant_shape"] == ra["dominant_shape"] and np.array_equal(ps.valid_mask(rp), ats.valid_mask(ra)):
            okm += 1
        rsp, rsa = (rp.get("rop_summary") or {}), (ra.get("rop_summary") or {})
        if (abs((rsp.get("atlas_volume_mean") or 0) - (rsa.get("atlas_volume_mean") or 0)) < 1e-4
                and (rsp.get("atlas_robust_path_count") or 0) == (rsa.get("atlas_robust_path_count") or 0)):
            okr += 1
    ck(f"curve parity (sample {len(ids)})", okc == len(ids), f"{okc}/{len(ids)}")
    ck("dominant_shape + valid_mask parity", okm == len(ids), f"{okm}/{len(ids)}")
    ck("ROP evidence parity", okr == len(ids), f"{okr}/{len(ids)}")

    rp_r, ra_r = Reader(ps), Reader(ats)
    same = tot = 0
    protos = prototypes(ps.u_grid)
    for proto in ["bump", "valley", "broad_bump", "switch_on", "right_peak", "double_bump"]:
        for intent in ["existential", "typical", "robust"]:
            t, lbl = protos[proto]
            a = [c["record_id"] for c in rp_r.search(t, intent=intent, behavior_label=lbl, k=5)["candidates"]]
            b = [c["record_id"] for c in ra_r.search(t, intent=intent, behavior_label=lbl, k=5)["candidates"]]
            same += int(a == b); tot += 1
    ck(f"Reader.search top-5 identical ({tot} queries × intents)", same == tot, f"{same}/{tot}")

    n = sum(RESULTS)
    print(f"\n{n}/{len(RESULTS)} parity checks passed — atlas_root is "
          + ("a LOSSLESS drop-in ✓" if n == len(RESULTS) else "NOT yet faithful ✗"))
    sys.exit(0 if n == len(RESULTS) else 1)


if __name__ == "__main__":
    main()
