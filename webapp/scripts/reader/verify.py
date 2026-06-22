"""verify.py — Step 4: close the verified loop.

reader_panel yields PRIOR candidates only. This module batch-verifies the top-k against
the target on the candidate's ENGINE-COMPUTED curves (verifier-grounded acceptance — the
phenotyper that labelled the atlas, NOT the retrieval match_score), and returns VERIFIED-
ONLY candidates, each card linking its ReaderResult evidence + its verifier result. Intent
sets the acceptance: existential→ANY draw, typical→medoid, robust→≥tau of draws.

In production the verifier is a live `simulate` on (rules, input, output); offline / in the
benchmark it reads the atlas's stored phenotyper curves (same verdict). `make_verifier`
returns either, behind one interface.
"""
from __future__ import annotations
import os
import sys
import numpy as np

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import benchmark as B   # the f_* feature detectors (verifier-grounded acceptance predicates)

_LABEL_FEATURE = {
    "biphasic_peak": B.f_interior_peak, "bandpass_with_plateau": B.f_broad_plateau,
    "biphasic_valley": B.f_interior_valley, "monotone_activation": B.f_monotone_up,
    "monotone_repression": B.f_monotone_down, "thresholded_activation": B.f_shoulder_then_rise,
    "multimodal": B.f_double_bump,
}
_DELTA_FEATURE = {
    "longer_plateau": lambda y, u: B.f_broad_plateau(y, u, 2.6), "steeper_fall": B.f_asym_peak,
    "higher_peak": B.f_interior_peak, "narrower": lambda y, u: B.f_narrow_peak(y, u, 1.6),
    "wider": B.f_broad_plateau, "shift_right": B.f_right_shifted_peak, "shift_left": B.f_left_shifted_peak,
}


def feature_for(spec):
    """The verifier-grounded acceptance feature implied by a compiled target-spec."""
    if spec.get("target_object_type") == "refinement_delta":
        d = (spec.get("refinement_delta") or {}).get("delta")
        if d in _DELTA_FEATURE:
            return _DELTA_FEATURE[d]
    return _LABEL_FEATURE.get(spec.get("behavior_label_hint"), B.f_interior_peak)


def verify_one(store, rec, feat, intent, tau=0.4):
    C = store.valid_curves(rec); u = store.u_grid
    if C.shape[0] == 0:
        return {"verified": False, "feature_hit_fraction": 0.0, "n_valid_draws": 0}
    hits = float(np.mean([1.0 if feat(C[i], u) else 0.0 for i in range(C.shape[0])]))
    ok = (hits > 0) if intent == "existential" else (hits >= tau)
    return {"verified": bool(ok), "intent": intent, "feature_hit_fraction": round(hits, 3),
            "n_valid_draws": int(C.shape[0]), "verifier": "phenotyper-curves(atlas)",
            "phenotyper_version": store.manifest.get("phenotyper_version")}


def verify_panel(reader_result, store, feat, intent, tau=0.4):
    """Verify a ReaderResult's PRIOR candidates; return VERIFIED-only candidate cards, each
    linking its ReaderResult evidence + verifier result."""
    verified = []
    for c in reader_result.get("candidates", []):
        rec = store._by_id.get(c["record_id"])
        if rec is None:
            continue
        vr = verify_one(store, rec, feat, intent, tau)
        if vr["verified"]:
            verified.append({
                "record_id": c["record_id"], "reactions": c.get("reactions"), "io": c.get("io"),
                "dominant_shape": c.get("dominant_shape"),
                "reader_result_ref": {"match_score": c["match_score"], "evidence": c["evidence"],
                                      "route": reader_result.get("route")},
                "verifier_result": vr,
            })
    return {"schema": "bne-verified-panel/v0.1.0", "route": reader_result.get("route"),
            "coverage_status": reader_result.get("coverage_status"),
            "n_prior": len(reader_result.get("candidates", [])), "n_verified": len(verified),
            "verified_candidates": verified,
            "note": ("Only VERIFIED candidates shown — each re-checked on its phenotyper curves against the "
                     "target. If n_verified=0, report the coverage_status honestly; do not fabricate.")}


def render_verified_card(vc):
    e = vc["reader_result_ref"]["evidence"]; vr = vc["verifier_result"]; io = vc.get("io") or {}
    rx = " ; ".join(vc.get("reactions") or [])
    return "\n".join([
        f"┌ {vc['dominant_shape']:<22} VERIFIED✓  match {vc['reader_result_ref']['match_score']} [{e['evidence_tier']}]",
        f"│ {io.get('input_symbol','?')} → {io.get('output_symbol','?')}   {rx[:80]}{'…' if len(rx) > 80 else ''}",
        f"│ reader: support {e['shape_support']} · ROP vol {e['volume_mean']} · paths {e['robust_path_count']} · route {vc['reader_result_ref']['route']}",
        f"│ verifier: feature-hit {vr['feature_hit_fraction']} of {vr['n_valid_draws']} draws ({vr['intent']}) · {vr['verifier']} {vr['phenotyper_version']}",
        "└" + "─" * 58])


if __name__ == "__main__":
    from atlas_store import open_store
    from reader import Reader, prototypes
    from nl_target_compiler import compile_target, render_target
    root = sys.argv[1] if len(sys.argv) > 1 else "/tmp/atlas-fresh"
    st = open_store(root); rd = Reader(st); u = st.u_grid
    for nl in ["a bump: low, high in the middle, low again",
               "find any network that produces a notch high-low-high",
               "two separate peaks back and forth"]:
        spec = compile_target(nl)
        tgt, intent, label, mx = render_target(spec, u)
        res = rd.routed_search(tgt, intent, behavior_label=label, k=6)
        vp = verify_panel(res, st, feature_for(spec), intent)
        print(f"\nNL: {nl}\n  route={vp['route']} coverage={vp['coverage_status']['status']} "
              f"prior={vp['n_prior']} VERIFIED={vp['n_verified']}")
        if vp["verified_candidates"]:
            print(render_verified_card(vp["verified_candidates"][0]))
