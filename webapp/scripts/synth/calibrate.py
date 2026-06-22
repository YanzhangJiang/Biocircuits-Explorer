"""calibrate.py — Sprint S1 calibration GATE.

Measures whether sign(rho_Spec) agrees with the phenotyper's accept on a target family, over a real
curve store. This is THE gate that decides whether functional synthesis proceeds to optimization:
if rho_Spec does not agree with the phenotyper, optimizing it is meaningless.

Reports (offline, on stored curves — no engine needed):
  * sign_consistency_all  = mean( (rho>=0) == POS_draw )           POS_draw = f_broad_plateau (phenotyper proxy)
  * sign_consistency_eps  outside an abstention band |rho|>=eps, with abstain_frac, swept over eps
  * fp_rate, fn_rate (per draw)
  * record_level_agreement vs the dominant_shape label + the plateau-vs-peak CONTAMINATION the gate
    inherits from the phenotyper (a deliberate, documented limitation: clean separation is S2 ranking)
  * gate_pass = sign_consistency_eps >= GATE_THRESHOLD at abstain_frac <= MAX_ABSTAIN

    python3 calibrate.py [atlas_root=/tmp/atlas-fresh] [--family broad_window] [--out PATH]
"""
from __future__ import annotations
import argparse
import datetime
import json
import os
import sys

import numpy as np

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "reader"))
from functional_spec import get_spec                       # noqa: E402
from rho_spec import rho_spec, record_accept               # noqa: E402
from atlas_store import open_store                          # noqa: E402
from benchmark import f_broad_plateau                       # noqa: E402  (per-draw phenotyper ground truth)

REPO = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", ".."))

GATE_THRESHOLD = 0.95      # required sign_consistency_eps
MAX_ABSTAIN = 0.15         # at an abstain_frac no larger than this
EPS_SWEEP = [0.0, 0.02, 0.05, 0.1, 0.15, 0.2]

# per-family: the per-draw phenotyper ground-truth detector + the positive record label
_FAMILY_GT = {
    "broad_window": {"detector": f_broad_plateau, "pos_label": "bandpass_with_plateau",
                     "hard_negative": "biphasic_peak"},
}


def calibrate(root, family):
    spec = get_spec(family)
    gt = _FAMILY_GT[family]
    st = open_store(root)
    u = np.asarray(st.u_grid, float)

    rho, pos = [], []                       # per-draw
    rec_rows = []                           # per-record
    for r in st.records:
        C = st.valid_curves(r)
        if C.shape[0] == 0:
            continue
        curves = [np.asarray(C[i], float) for i in range(C.shape[0])]
        rs = [rho_spec(spec, y, u)["rho"] for y in curves]
        ps = [bool(gt["detector"](y, u)) for y in curves]
        rho.extend(rs); pos.extend(ps)
        acc_rec, frac = record_accept(spec, curves, u)
        rec_rows.append({"label": r["dominant_shape"], "accept": bool(acc_rec), "frac": frac})

    rho = np.asarray(rho, float); pos = np.asarray(pos, bool); acc = rho >= 0.0
    n = rho.size
    sign_all = float(np.mean(acc == pos))
    fp = float(np.mean(acc[~pos])) if (~pos).any() else 0.0          # P(acc=1 | POS=0)
    fn = float(np.mean(~acc[pos])) if pos.any() else 0.0            # P(acc=0 | POS=1)

    eps_sweep = {}
    best = (-1.0, None)
    for e in EPS_SWEEP:
        keep = np.abs(rho) >= e
        sc = float(np.mean(acc[keep] == pos[keep])) if keep.any() else 1.0
        ab = float(np.mean(~keep))
        eps_sweep[str(e)] = {"sign_consistency_eps": round(sc, 4), "abstain_frac": round(ab, 4)}
        if ab <= MAX_ABSTAIN and sc > best[0]:
            best = (sc, e)
    best_sc, best_eps = best

    # record level
    labels = [x["label"] for x in rec_rows]
    pos_lab = np.array([x == gt["pos_label"] for x in labels])
    acc_rec = np.array([x["accept"] for x in rec_rows])
    rec_agree = float(np.mean(acc_rec == pos_lab)) if rec_rows else 0.0
    n_pos_rec = int(pos_lab.sum())
    n_pos_acc = int((acc_rec & pos_lab).sum())
    n_hardneg_acc = int(sum(1 for x in rec_rows if x["accept"] and x["label"] == gt["hard_negative"]))
    n_hardneg = int(sum(1 for x in rec_rows if x["label"] == gt["hard_negative"]))

    gate_pass = bool(best_sc >= GATE_THRESHOLD)
    return {
        "report_schema": "bne-rho-spec-calibration/v0.1.0",
        "created_at": datetime.datetime.now().isoformat(timespec="seconds"),
        "atlas_root": root, "store": type(st).__name__, "family": family,
        "spec": spec.to_dict(),
        "ground_truth": {"per_draw_detector": "f_broad_plateau (phenotyper proxy)",
                         "per_record_label": gt["pos_label"], "hard_negative": gt["hard_negative"]},
        "n_records": len(st), "n_draws": int(n), "n_pos_draw": int(pos.sum()), "n_pos_rec": n_pos_rec,
        "rho_range": [round(float(rho.min()), 4), round(float(rho.max()), 4)] if n else None,
        "sign_consistency_all": round(sign_all, 4), "fp_rate": round(fp, 4), "fn_rate": round(fn, 4),
        "eps_sweep": eps_sweep, "best_eps": best_eps, "best_sign_consistency_eps": round(best_sc, 4),
        "gate_threshold": GATE_THRESHOLD, "max_abstain": MAX_ABSTAIN, "gate_pass": gate_pass,
        "record_level": {
            "agreement_vs_label": round(rec_agree, 4),
            "pos_records_accepted": f"{n_pos_acc}/{n_pos_rec}",
            "contamination_hard_negative_accepted": f"{n_hardneg_acc}/{n_hardneg}",
            "caveat": ("the gate is SIGN-EXACT with the per-draw phenotyper, which itself fires on many "
                       f"{gt['hard_negative']} draws; record-level label agreement is carried by easy "
                       "negatives. Use the per-draw robust fraction for S2 RANKING, not the record label."),
        },
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("root", nargs="?", default="/tmp/atlas-fresh")
    ap.add_argument("--family", default="broad_window")
    ap.add_argument("--out", default=os.path.join(REPO, "benchmarks", "reports", "rho_spec_calibration.json"))
    args = ap.parse_args()

    rep = calibrate(args.root, args.family)
    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    json.dump(rep, open(args.out, "w"), indent=2)

    print(f"family={rep['family']}  store={rep['store']}  root={rep['atlas_root']}")
    print(f"records={rep['n_records']}  draws={rep['n_draws']}  pos_draw={rep['n_pos_draw']}  "
          f"pos_rec={rep['n_pos_rec']}  rho_range={rep['rho_range']}\n")
    print(f"sign_consistency_all = {rep['sign_consistency_all']}   fp={rep['fp_rate']}  fn={rep['fn_rate']}")
    print("eps sweep (sign_consistency_eps @ abstain_frac):")
    for e, d in rep["eps_sweep"].items():
        print(f"  eps={e:<5} {d['sign_consistency_eps']:>6}  @ abstain {d['abstain_frac']}")
    print(f"\nbest: sign_consistency_eps={rep['best_sign_consistency_eps']} @ eps={rep['best_eps']} "
          f"(threshold {rep['gate_threshold']}, max_abstain {rep['max_abstain']})")
    rl = rep["record_level"]
    print(f"record-level: agreement_vs_label={rl['agreement_vs_label']}  "
          f"pos_accepted={rl['pos_records_accepted']}  contamination={rl['contamination_hard_negative_accepted']}")
    print(f"\nGATE: {'PASS ✓ — proceed to S2 (CEGIS)' if rep['gate_pass'] else 'FAIL ✗ — do NOT optimize; ρ_Spec disagrees with the phenotyper'}")
    print(f"JSON report -> {args.out}")
    return 0 if rep["gate_pass"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
