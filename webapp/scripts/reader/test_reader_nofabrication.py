"""test_reader_nofabrication.py — locks in the Function-Space Reader's agent usage
boundary + no-fabrication discipline (Stage-2-lite regression).

Self-contained: builds a tiny synthetic spike-curve-packet/v0 corpus in a temp dir,
so it runs in CI without the big atlas. Asserts:
  1. corpus-unavailable guard -> {error}, NO fabricated candidates (reader_panel & service)
  2. reader_panel output is a PRIOR: family=reader_panel, a "verify with simulate / do not
     fabricate" note, and NO candidate is flagged verified
  3. evidence_tier never over-claims (edge cases + every returned candidate's tier matches
     what its evidence actually supports)
  4. ReaderResult is schema-valid
  5. rerank returns ONLY candidates from the given set (no invented networks)
  6. match_score is a similarity prior in [0,1]; provenance is verifier-grounded but the
     pipeline is retrieval (not verification)

    python3 test_reader_nofabrication.py
"""
import json
import os
import sys
import tempfile
import numpy as np

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _HERE)                       # reader package
sys.path.insert(0, os.path.join(_HERE, ".."))   # webapp/scripts (design_agent + deps)


# ── synthetic corpus (4 records spanning tiers T3a/T3b/T2/T1) ──────────────────
def build_synthetic(root):
    n_points, K = 16, 4
    u = np.linspace(-6, 6, n_points)
    peak = lambda c: np.exp(-((u - c) / 2.0) ** 2)
    mono = lambda: 1.0 / (1.0 + np.exp(-1.5 * u))
    # (name, dominant, draw-curves, support, vol, paths, family, n_failed)
    specs = [
        ("r_peak", "biphasic_peak",       [peak(0.2 * j) for j in range(K)], 1.0, 0.08, 5, "famA", 0),  # -> T3a
        ("r_mono", "monotone_activation", [mono() for _ in range(K)],        0.9, 0.02, 4, "famB", 0),  # -> T3b
        ("r_pk2",  "biphasic_peak",       [peak(1.0) for _ in range(K)],     0.4, 0.03, 2, "famC", 1),  # -> T2
        ("r_weak", "monotone_activation", [mono() for _ in range(K)],        0.3, 0.0,  0, None,  0),   # -> T1
    ]
    idx, off = [], 0
    with open(os.path.join(root, "curves.f32"), "wb") as cf:
        for ri, (name, dom, draws, supp, vol, paths, fam, nfail) in enumerate(specs):
            for dr in draws:
                cf.write(np.asarray(dr, np.float32).tobytes())
            ok = [True] * K
            for j in range(nfail):
                ok[K - 1 - j] = False
            idx.append({
                "schema": "spike-curve-packet/v0", "record_id": f"rid_{name}",
                "network_id": f"net_{name}", "slice_id": f"sl_{name}",
                "io_assignment": {"input_symbol": "tA", "output_symbol": "C_A_B"},
                "n_reactions": 3, "rules": ["A + B <-> C_A_B", "A + C_A_B <-> C_A_A_B", "B + C_A_B <-> C_A_B_B"],
                "phenotyper_version": "bne-phenotyper/v0.4.0",
                "parameter_policy_id": "test", "u_grid_id": "test_grid",
                "curve_array_pointer": {"file": "curves.f32", "offset_bytes": off, "n_draws": K,
                                        "n_points": n_points, "dtype": "float32", "layout": "draw_major"},
                "draw_count": K,
                "validity_summary": {"n_draws": K, "n_valid": K - nfail, "n_failed": nfail},
                "dominant_shape": dom, "shape_fractions": {dom: supp},
                "medoid_draw_id": 1,
                "rop_summary": {"atlas_family_label": fam, "atlas_volume_mean": vol, "atlas_robust_path_count": paths},
                "draws": [{"di": j + 1, "ok": ok[j]} for j in range(K)],
            })
            off += K * n_points * 4
    with open(os.path.join(root, "packets_index.jsonl"), "w") as f:
        for r in idx:
            f.write(json.dumps(r) + "\n")
    with open(os.path.join(root, "manifest.json"), "w") as f:
        json.dump({"schema": "spike-curve-packet-manifest/v0", "curves_file": "curves.f32",
                   "grid": {"id": "test_grid", "lo": -6, "hi": 6, "n_points": n_points},
                   "K": K, "seed": 1, "sampler": "halton", "phenotyper_version": "bne-phenotyper/v0.4.0",
                   "source_in": "synthetic"}, f)


# ── tests ──────────────────────────────────────────────────────────────────────
RESULTS = []
def check(name, cond, detail=""):
    RESULTS.append((name, bool(cond), detail))
    print(("PASS " if cond else "FAIL ") + name + (f"  — {detail}" if (detail and not cond) else ""))


def main():
    tmp = tempfile.mkdtemp(prefix="reader-nofab-")
    build_synthetic(tmp)

    from packet_store import PacketStore
    from reader import Reader, evidence_tier, prototypes
    import reader_cli
    import reader_service as RS

    st = PacketStore(tmp); rd = Reader(st)
    u = st.u_grid; bump_target = prototypes(u)["bump"][0]

    # 1. corpus-unavailable guard — no fabrication
    err = RS.panel(prototype="bump", corpus="/no/such/corpus")
    check("service.panel missing-corpus -> error", err.get("error") and not err.get("candidates"), err)
    os.environ.pop("BNE_PACKET_CORPUS", None)
    import design_agent as DA
    g = DA.reader_panel(prototype="bump")
    check("reader_panel no-corpus guard -> error, no candidates", g.get("error") and not g.get("candidates"), g)

    # 2. reader_panel output is a PRIOR (must be simulate-verified), never flagged verified
    os.environ["BNE_PACKET_CORPUS"] = tmp
    RS._CACHE.clear()
    p = DA.reader_panel(prototype="bump", intent="typical", k=4)
    note = (p.get("note") or "").lower()
    check("reader_panel family=reader_panel", p.get("family") == "reader_panel", p.get("family"))
    check("reader_panel note demands simulate-verify + no-fabricate",
          ("verify" in note and "simulate" in note and "fabricate" in note), note)
    no_verified_flag = all(not (("verified" in c) or c.get("engine_verified") or c.get("verified"))
                           for c in p.get("candidates", []))
    check("no reader_panel candidate is flagged verified", no_verified_flag)

    # 3. evidence_tier never over-claims (edge cases)
    check("tier T3a (support>=.8 & vol>=.05)", evidence_tier(0.9, 0.08, 5) == "T3a")
    check("tier T3b (support>=.8, vol<.05)", evidence_tier(0.9, 0.02, 4) == "T3b")
    check("tier T2 (paths>0/vol>0, support<.8)", evidence_tier(0.4, 0.03, 2) == "T2")
    check("tier T1 (no ROP, low support)", evidence_tier(0.3, 0.0, 0) == "T1")
    res = rd.search(bump_target, intent="typical", behavior_label="biphasic_peak", k=4)
    over = []
    for c in res["candidates"]:
        e = c["evidence"]
        if evidence_tier(e["shape_support"], e["volume_mean"], e["robust_path_count"]) != e["evidence_tier"]:
            over.append(c["record_id"])
    check("no candidate over-claims its evidence_tier", not over, f"mismatched={over}")

    # 4. ReaderResult schema-valid
    schema = json.load(open(reader_cli._find_schema())) if reader_cli._find_schema() else None
    verrs = reader_cli._validate(res, schema) if schema else ["schema not found"]
    check("ReaderResult is schema-valid", not verrs, verrs[:3])

    # 5. rerank returns ONLY candidates from the given set (no invented networks)
    ids = [c["record_id"] for c in res["candidates"]][:3]
    rr = rd.rerank(ids, bump_target, intent="existential")
    returned = {c["record_id"] for c in rr["candidates"]}
    check("rerank invents no candidates", returned.issubset(set(ids)), returned - set(ids))

    # 6. match_score is a similarity prior in [0,1]; pipeline is retrieval not verification
    ms_ok = all(0.0 <= c["match_score"] <= 1.0 for c in res["candidates"])
    check("match_score in [0,1]", ms_ok)
    check("pipeline is retrieval (label_recall first), provenance verifier-grounded",
          res["pipeline"][0] == "label_recall" and res["provenance"].get("verifier_grounded") is True, res["pipeline"])

    npass = sum(1 for _, ok, _ in RESULTS if ok)
    print(f"\n{npass}/{len(RESULTS)} checks passed")
    sys.exit(0 if npass == len(RESULTS) else 1)


if __name__ == "__main__":
    main()
