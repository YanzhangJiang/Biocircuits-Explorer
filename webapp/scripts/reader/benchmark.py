"""benchmark.py — Function-Space Atlas spike: S3 (ROP roles) + S4 (label-free
benchmark, ~100 tasks in 4 groups) + S5 (multi-arm ablation) + 4-layer GATE.

Each task explicitly declares:
  * group        ∈ {paraphrase, refinement, discovery, stress}
  * intent_type  ∈ {existential, typical, robust}   — and is evaluated under THAT intent
  * target_object∈ {reference_curve, constraint_curve, refinement_delta}
  * acceptance_predicate — a verifier-grounded FEATURE test on the engine-computed
    K-draw curves (never the L2 retrieval distance), evaluated per the task intent:
        existential → ANY valid draw shows the feature
        typical     → the MEDOID curve shows the feature
        robust      → >= tau of valid draws show the feature

Arms (label_only / function_only / function_rop_{rerank,prefilter,panel}) rank under the
task's intent. Reports the full evidence panel: pass@k, calls-to-first, shape_support,
volume_mean, robust_path_count, Tier-2/3 fraction, mechanism & network diversity,
invalid/non-converged rate.
"""
from __future__ import annotations
import os
import sys
import collections
import numpy as np

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from packet_store import PacketStore
from compare import ReferenceCurveTarget, ConstraintCurveTarget, normalize, bump, sigmoid


# ── feature detectors ──────────────────────────────────────────────────────────
def _n(y):
    return normalize(y, "minmax")


def _maxima(yn, prom=0.12):
    out = []
    for i in range(1, len(yn) - 1):
        if yn[i] >= yn[i - 1] and yn[i] >= yn[i + 1]:
            if (yn[i] - max(np.nanmin(yn[:i]), np.nanmin(yn[i + 1:]))) >= prom:
                out.append(i)
    return out


def f_interior_peak(y, u, prom=0.18):
    yn = _n(y); i = int(np.nanargmax(yn))
    return 0 < i < len(yn) - 1 and (yn[i] - max(yn[0], yn[-1])) >= prom


def f_interior_valley(y, u, depth=0.18):
    yn = _n(y); i = int(np.nanargmin(yn))
    return 0 < i < len(yn) - 1 and (min(yn[0], yn[-1]) - yn[i]) >= depth


def f_monotone_up(y, u):
    return (_n(y)[-1] - _n(y)[0]) > 0.5 and not f_interior_peak(y, u, 0.15)


def f_monotone_down(y, u):
    return (_n(y)[0] - _n(y)[-1]) > 0.5 and not f_interior_valley(y, u, 0.15)


def f_broad_plateau(y, u, min_w=2.0):
    yn = _n(y); hi = np.where(yn >= 0.9)[0]
    return hi.size >= 2 and (u[hi[-1]] - u[hi[0]]) >= min_w and f_interior_peak(y, u, 0.12)


def f_narrow_peak(y, u, max_w=2.0):
    yn = _n(y); hi = np.where(yn >= 0.9)[0]
    w = (u[hi[-1]] - u[hi[0]]) if hi.size >= 2 else 0.0
    return f_interior_peak(y, u, 0.18) and w <= max_w


def f_late_dip(y, u, dip=0.12):
    yn = _n(y); i = int(np.nanargmax(yn))
    return i < len(yn) - 2 and (yn[i] - yn[0]) >= 0.4 and dip <= (yn[i] - yn[-1]) <= 0.7


def f_right_shifted_peak(y, u):
    yn = _n(y); i = int(np.nanargmax(yn))
    return f_interior_peak(y, u, 0.15) and u[i] > u[0] + 0.6 * (u[-1] - u[0])


def f_left_shifted_peak(y, u):
    yn = _n(y); i = int(np.nanargmax(yn))
    return f_interior_peak(y, u, 0.15) and u[i] < u[0] + 0.4 * (u[-1] - u[0])


def f_partial_return_high(y, u):
    yn = _n(y); i = int(np.nanargmax(yn))
    return 0 < i < len(yn) - 1 and 0.25 <= yn[-1] <= 0.7 and (yn[i] - yn[-1]) >= 0.2


def f_asym_peak(y, u):
    yn = _n(y); i = int(np.nanargmax(yn))
    if not (1 < i < len(yn) - 2):
        return False
    rise = (yn[i] - yn[0]) / max(u[i] - u[0], 1e-6)
    fall = (yn[i] - yn[-1]) / max(u[-1] - u[i], 1e-6)
    return f_interior_peak(y, u, 0.15) and fall > 1.4 * rise


def f_shoulder_then_rise(y, u, flat_frac=0.25):
    yn = _n(y); k = int(flat_frac * len(yn))
    if k < 2:
        return False
    lead = (np.nanmax(yn[:k]) - np.nanmin(yn[:k])) < 0.12 and np.nanmean(yn[:k]) < 0.3
    return lead and yn[-1] > 0.7 and not f_interior_peak(y, u, 0.12)


def f_double_bump(y, u):
    return len(_maxima(_n(y), 0.12)) >= 2


def f_triple_bump(y, u):
    return len(_maxima(_n(y), 0.10)) >= 3


def f_shoulder_bump(y, u):
    yn = _n(y); k = max(2, len(yn) // 4)
    return np.nanmean(yn[:k]) < 0.35 and f_interior_peak(y, u, 0.15)


def f_valley_then_high(y, u):
    yn = _n(y); i = int(np.nanargmin(yn))
    return 0 < i < len(yn) - 1 and yn[-1] > 0.6 and (min(yn[0], yn[-1]) - yn[i]) >= 0.15


# ── acceptance (verifier-grounded, intent-aware) ───────────────────────────────
def accept(store, rec, feat, intent, tau=0.4):
    C = store.valid_curves(rec); u = store.u_grid
    if C.shape[0] == 0:
        return False
    if intent == "typical":
        med = store.medoid_curve(rec)
        return med is not None and bool(feat(med, u))
    hits = np.mean([1.0 if feat(C[i], u) else 0.0 for i in range(C.shape[0])])
    return (hits > 0) if intent == "existential" else (hits >= tau)


# ── prototype targets ──────────────────────────────────────────────────────────
def _bump(u, c=0.0, w=2.5):
    return bump(u, c, w)


def _latedip(u):
    return sigmoid(u, -0.5, 1.6) - 0.30 * np.clip((u - 2.5) / 3.5, 0, 1)


def _partial(u):
    return sigmoid(u, -1.0, 1.8) - 0.45 * np.clip((u - 1.0) / 4.0, 0, 1)


def _asym(u):
    return sigmoid(u, -1.5, 1.2) * (1 - sigmoid(u, 0.5, 3.5))


def _double(u, c1=-2.2, c2=2.2, w=1.1):
    return np.maximum(bump(u, c1, w), bump(u, c2, w))


def _triple(u):
    return np.maximum.reduce([bump(u, -3.0, 0.9), bump(u, 0.0, 0.9), bump(u, 3.0, 0.9)])


def _shoulder_bump(u):
    return 0.45 * sigmoid(u, -2.0, 2.5) + bump(u, 1.5, 1.2)


def _valley_high(u):
    return 1 - bump(u, -1.0, 1.6) * (u < 1.5)


_REGIONS_BUMP = [{"u": (-6, -2.5), "want": "low", "w": 1}, {"u": (-2.5, -0.5), "want": "rise", "w": 1},
                 {"u": (-0.5, 0.5), "want": "high", "w": 1.5}, {"u": (0.5, 2.5), "want": "fall", "w": 1.5},
                 {"u": (2.5, 6), "want": "low", "w": 1}]
_REGIONS_VALLEY = [{"u": (-6, -2.5), "want": "high", "w": 1}, {"u": (-1, 1), "want": "low", "w": 1.5},
                   {"u": (2.5, 6), "want": "high", "w": 1}]


# ── task set: ~100 tasks across 4 groups ───────────────────────────────────────
def build_tasks(store):
    u = store.u_grid
    T = []
    def add(group, intent, tobj, tid, target, feat, label):
        T.append(dict(group=group, intent=intent, tobj=tobj, id=tid, target=target, feat=feat, label=label))
    RC, CC = ReferenceCurveTarget, ConstraintCurveTarget

    # ---- Group 1: vocab-paraphrase (intent=robust; want a reliable named-class realization) ----
    for j, s in enumerate((0.9, 1.4, 2.0, 2.6)):
        add("paraphrase", "robust", "reference_curve", f"par_actv{j}", RC(sigmoid(u, 0, s)), f_monotone_up, "monotone_activation")
        add("paraphrase", "robust", "reference_curve", f"par_repr{j}", RC(sigmoid(u, 0, s, falling=True)), f_monotone_down, "monotone_repression")
    for j, (c, w) in enumerate(((-0.6, 2.0), (0.0, 2.4), (0.6, 2.2), (0.0, 1.8))):
        add("paraphrase", "robust", "reference_curve", f"par_peak{j}", RC(_bump(u, c, w)), f_interior_peak, "biphasic_peak")
    for j, w in enumerate((3.2, 3.8, 4.4)):
        add("paraphrase", "robust", "reference_curve", f"par_band{j}", RC(_bump(u, 0, w)), f_broad_plateau, "bandpass_with_plateau")
    for j, c in enumerate((-0.6, 0.0, 0.6)):
        add("paraphrase", "robust", "reference_curve", f"par_val{j}", RC(1 - _bump(u, c, 2.0)), f_interior_valley, "biphasic_valley")
    for j, thr in enumerate((1.0, 1.8)):
        add("paraphrase", "robust", "reference_curve", f"par_thr{j}", RC(sigmoid(u, thr, 2.4)), f_shoulder_then_rise, "thresholded_activation")
    add("paraphrase", "robust", "constraint_curve", "par_con_bump", CC(_REGIONS_BUMP), f_interior_peak, "biphasic_peak")
    add("paraphrase", "robust", "constraint_curve", "par_con_valley", CC(_REGIONS_VALLEY), f_interior_valley, "biphasic_valley")

    # ---- Group 2: within-label refinement (intent=typical) ----
    for j, c in enumerate((1.6, 2.2, 2.8)):
        add("refinement", "typical", "reference_curve", f"ref_right{j}", RC(_bump(u, c, 1.6)), f_right_shifted_peak, "biphasic_peak")
    for j, c in enumerate((-1.6, -2.2, -2.8)):
        add("refinement", "typical", "reference_curve", f"ref_left{j}", RC(_bump(u, c, 1.6)), f_left_shifted_peak, "biphasic_peak")
    for j, w in enumerate((0.9, 1.3, 1.7)):
        add("refinement", "typical", "reference_curve", f"ref_narrow{j}", RC(_bump(u, 0, w)), f_narrow_peak, "biphasic_peak")
    add("refinement", "typical", "reference_curve", "ref_asym", RC(_asym(u)), f_asym_peak, "biphasic_peak")
    add("refinement", "typical", "reference_curve", "ref_partial", RC(_partial(u)), f_partial_return_high, "biphasic_peak")
    add("refinement", "typical", "reference_curve", "ref_latedip", RC(_latedip(u)), f_late_dip, "monotone_activation")
    add("refinement", "typical", "reference_curve", "ref_shoulder", RC(sigmoid(u, 1.6, 2.6)), f_shoulder_then_rise, "thresholded_activation")
    add("refinement", "typical", "refinement_delta", "rd_longplateau", RC(_bump(u, 0, 4.2)), lambda y, uu: f_broad_plateau(y, uu, 2.8), "bandpass_with_plateau")
    add("refinement", "typical", "refinement_delta", "rd_sharpfall", RC(_asym(u)), f_asym_peak, "biphasic_peak")
    add("refinement", "typical", "refinement_delta", "rd_narrow", RC(_bump(u, 0, 0.9)), lambda y, uu: f_narrow_peak(y, uu, 1.4), "biphasic_peak")

    # ---- Group 3: existential / discovery (intent=existential; "find ANY net that can do X") ----
    for j, (c, w) in enumerate(((-1.0, 2.2), (0.0, 2.2), (1.0, 2.2), (1.8, 1.6), (-1.8, 1.6))):
        add("discovery", "existential", "reference_curve", f"disc_peak{j}", RC(_bump(u, c, w)), f_interior_peak, "biphasic_peak")
    for j, c in enumerate((-0.6, 0.0, 0.6)):
        add("discovery", "existential", "reference_curve", f"disc_val{j}", RC(1 - _bump(u, c, 2.0)), f_interior_valley, "biphasic_valley")
    add("discovery", "existential", "reference_curve", "disc_band", RC(_bump(u, 0, 3.8)), f_broad_plateau, "bandpass_with_plateau")
    add("discovery", "existential", "reference_curve", "disc_thresh", RC(sigmoid(u, 1.5, 2.6)), f_shoulder_then_rise, "thresholded_activation")
    add("discovery", "existential", "reference_curve", "disc_shbump", RC(_shoulder_bump(u)), f_shoulder_bump, "biphasic_peak")
    add("discovery", "existential", "reference_curve", "disc_partial", RC(_partial(u)), f_partial_return_high, "biphasic_peak")
    add("discovery", "existential", "constraint_curve", "disc_con_bump", CC(_REGIONS_BUMP), f_interior_peak, "biphasic_peak")
    add("discovery", "existential", "constraint_curve", "disc_con_valley", CC(_REGIONS_VALLEY), f_interior_valley, "biphasic_valley")
    for j, c in enumerate((1.5, 2.5)):
        add("discovery", "existential", "reference_curve", f"disc_right{j}", RC(_bump(u, c, 1.6)), f_right_shifted_peak, "biphasic_peak")

    # ---- Group 4: coverage-limited / out-of-vocab stress (intent=existential) ----
    add("stress", "existential", "reference_curve", "str_double0", RC(_double(u)), f_double_bump, "biphasic_peak")
    add("stress", "existential", "reference_curve", "str_double1", RC(_double(u, -1.5, 1.5, 1.0)), f_double_bump, "biphasic_peak")
    add("stress", "existential", "reference_curve", "str_double2", RC(_double(u, -3.0, 1.0, 1.2)), f_double_bump, "biphasic_peak")
    add("stress", "existential", "reference_curve", "str_triple", RC(_triple(u)), f_triple_bump, "biphasic_peak")
    add("stress", "existential", "reference_curve", "str_valhigh", RC(_valley_high(u)), f_valley_then_high, "biphasic_valley")
    add("stress", "existential", "reference_curve", "str_latedip", RC(_latedip(u)), f_late_dip, "monotone_activation")
    add("stress", "existential", "reference_curve", "str_spike0", RC(_bump(u, 0, 0.55)), f_narrow_peak, "biphasic_peak")
    add("stress", "existential", "reference_curve", "str_spike1", RC(_bump(u, 2.0, 0.55)), f_narrow_peak, "biphasic_peak")
    add("stress", "existential", "reference_curve", "str_shbump", RC(_shoulder_bump(u)), f_shoulder_bump, "biphasic_peak")
    add("stress", "existential", "reference_curve", "str_asym2", RC(_asym(u)), f_asym_peak, "biphasic_peak")
    add("stress", "existential", "reference_curve", "str_spike2", RC(_bump(u, -2.0, 0.55)), f_narrow_peak, "biphasic_peak")
    add("stress", "existential", "reference_curve", "str_double3", RC(_double(u, -2.5, 2.5, 0.8)), f_double_bump, "biphasic_peak")
    add("stress", "existential", "reference_curve", "str_triple2", RC(_triple(u)), f_triple_bump, "biphasic_peak")
    add("stress", "existential", "constraint_curve", "str_con_double",
        CC([{"u": (-6, -3.5), "want": "low", "w": 1}, {"u": (-3, -2), "want": "high", "w": 1},
            {"u": (-0.5, 0.5), "want": "low", "w": 1}, {"u": (2, 3), "want": "high", "w": 1},
            {"u": (4.5, 6), "want": "low", "w": 1}]), f_double_bump, "biphasic_peak")
    # extra refinement
    for j, c in enumerate((1.9, 2.5)):
        add("refinement", "typical", "reference_curve", f"ref_right_b{j}", RC(_bump(u, c, 1.3)), f_right_shifted_peak, "biphasic_peak")
    for j, r2 in enumerate((2.0, 3.0)):
        add("refinement", "typical", "refinement_delta", f"rd_asym{j}", RC(sigmoid(u, -1.5, 1.0) * (1 - sigmoid(u, 0.5, r2))), f_asym_peak, "biphasic_peak")
    add("refinement", "typical", "reference_curve", "ref_partial2", RC(sigmoid(u, -1.5, 1.6) - 0.35 * np.clip((u - 0.5) / 4.0, 0, 1)), f_partial_return_high, "biphasic_peak")
    add("refinement", "typical", "reference_curve", "ref_narrow_r", RC(_bump(u, 1.5, 1.0)), f_narrow_peak, "biphasic_peak")
    # extra discovery
    for j, (c, w) in enumerate(((-2.0, 1.8), (0.5, 2.4), (2.2, 1.4))):
        add("discovery", "existential", "reference_curve", f"disc_peak_b{j}", RC(_bump(u, c, w)), f_interior_peak, "biphasic_peak")
    add("discovery", "existential", "reference_curve", "disc_valbroad", RC(1 - _bump(u, 0, 3.2)), f_interior_valley, "biphasic_valley")
    add("discovery", "existential", "reference_curve", "disc_latedip", RC(_latedip(u)), f_late_dip, "monotone_activation")
    # extra paraphrase
    for j, w in enumerate((3.0, 4.0)):
        add("paraphrase", "robust", "reference_curve", f"par_band_b{j}", RC(_bump(u, 0, w)), f_broad_plateau, "bandpass_with_plateau")
    add("paraphrase", "robust", "reference_curve", "par_actv_steep", RC(sigmoid(u, 0, 3.0)), f_monotone_up, "monotone_activation")
    add("paraphrase", "robust", "reference_curve", "par_repr_steep", RC(sigmoid(u, 0, 3.0, falling=True)), f_monotone_down, "monotone_repression")
    return T


# ── arms ───────────────────────────────────────────────────────────────────────
class Arms:
    def __init__(self, store):
        self.store = store
        N, K, P = len(store), store.K, store.n_points
        self.D = np.full((N, K, P), np.nan, np.float32)
        self.MED = np.full((N, P), np.nan, np.float32)
        for i, r in enumerate(store.records):
            C = store.curves(r); vm = store.valid_mask(r)
            for k in range(K):
                if vm[k]:
                    self.D[i, k] = normalize(C[k], "minmax")
            m = store.medoid_curve(r)
            if m is not None:
                self.MED[i] = normalize(m, "minmax")
        self.vol = np.array([float((r.get("rop_summary") or {}).get("atlas_volume_mean") or 0.0) for r in store.records])
        self.paths = np.array([float((r.get("rop_summary") or {}).get("atlas_robust_path_count") or 0.0) for r in store.records])
        self.by_label = {}
        for i, r in enumerate(store.records):
            self.by_label.setdefault(r["dominant_shape"], []).append(i)
        for s in self.by_label:
            self.by_label[s].sort(key=lambda i: -(store.records[i].get("shape_fractions", {}).get(s, 0)))

    def _target_vec(self, task):
        tgt = task["target"]
        if isinstance(tgt, ReferenceCurveTarget):
            return tgt.y
        u = self.store.u_grid; y = np.zeros_like(u)
        for rg in tgt.regions:
            lo, hi = rg["u"]; y[(u >= lo) & (u <= hi)] = {"low": 0.0, "high": 1.0}.get(rg["want"], 0.5)
        return y

    def _score(self, task, intent, thr=0.22):
        tn = normalize(self._target_vec(task), "minmax")
        if intent == "typical":
            return np.sqrt(np.nanmean((self.MED - tn) ** 2, axis=1))
        d = np.sqrt(np.nanmean((self.D - tn) ** 2, axis=2))               # (N,K)
        if intent == "existential":
            return np.nan_to_num(np.nanmin(d, axis=1), nan=9.9)
        valid = np.sum(~np.isnan(d), axis=1)
        return -(np.nansum(d <= thr, axis=1) / np.maximum(valid, 1))

    def label_only(self, task, k, intent):
        return self.by_label.get(task["label"], [])[:k]

    def function_only(self, task, k, intent):
        return list(np.argsort(self._score(task, intent))[:k])

    def function_rop_rerank(self, task, k, intent, pool=300, w=0.15):
        s = self._score(task, intent); cand = np.argsort(s)[:pool]
        vn = self.vol[cand] / (self.vol[cand].max() + 1e-9)
        return list(cand[np.argsort(s[cand] - w * vn)][:k])

    def function_rop_prefilter(self, task, k, intent, pool=600):
        s = self._score(task, intent); cand = np.argsort(s)[:pool]
        keep = cand[self.paths[cand] > 0]
        if keep.size < k:
            keep = cand
        return list(keep[np.argsort(s[keep])][:k])

    def function_rop_panel(self, task, k, intent, pool=300):
        s = self._score(task, intent); cand = list(np.argsort(s)[:pool])
        chosen, fams = [], set()
        for i in cand:
            fam = (self.store.records[i].get("rop_summary") or {}).get("atlas_family_label")
            if fam not in fams:
                chosen.append(i); fams.add(fam)
            if len(chosen) >= k:
                break
        for i in cand:
            if len(chosen) >= k:
                break
            if i not in chosen:
                chosen.append(i)
        return chosen[:k]


ARMS = ["label_only", "function_only", "function_rop_rerank", "function_rop_prefilter", "function_rop_panel"]
GROUPS = ["paraphrase", "refinement", "discovery", "stress"]


def _supp(rec):
    return float((rec.get("shape_fractions") or {}).get(rec.get("dominant_shape"), 0.0))


def _inval(rec):
    vs = rec.get("validity_summary") or {}
    return float(vs.get("n_failed", 0)) / max(vs.get("n_draws", 1) or 1, 1)


def run(store, tasks, ks=(1, 5, 20), tau=0.4, evk=5):
    A = Arms(store); rows = []; recs = store.records
    for task in tasks:
        intent = task["intent"]
        for arm in ARMS:
            ranked = getattr(A, arm)(task, max(ks), intent)
            passes = [accept(store, recs[i], task["feat"], intent, tau) for i in ranked]
            first = next((j + 1 for j, p in enumerate(passes) if p), 0)
            topk = ranked[:evk]; rk = [recs[i] for i in topk]
            vol = [A.vol[i] for i in topk]; paths = [A.paths[i] for i in topk]
            supp = [_supp(r) for r in rk]; inval = [_inval(r) for r in rk]
            fams = [(r.get("rop_summary") or {}).get("atlas_family_label") for r in rk]
            nets = [r.get("network_id") for r in rk]; n = max(len(topk), 1)
            rows.append(dict(group=task["group"], intent=intent, tobj=task["tobj"], task=task["id"], arm=arm,
                **{f"p{k}": int(any(passes[:k])) for k in ks}, first=first,
                vol=float(np.mean(vol)) if vol else 0.0, paths=float(np.mean(paths)) if paths else 0.0,
                supp=float(np.mean(supp)) if supp else 0.0,
                tier2=float(np.mean([p > 0 for p in paths])) if paths else 0.0,
                tier3=float(np.mean([s >= 0.8 for s in supp])) if supp else 0.0,
                mechdiv=len({f for f in fams if f is not None}) / n, netdiv=len(set(nets)) / n,
                inval=float(np.mean(inval)) if inval else 0.0))
    return rows, A


_COLS = [("p@1", "p1"), ("p@5", "p5"), ("p@20", "p20"), ("→1st", "first"), ("supp", "supp"),
         ("vol", "vol"), ("paths", "paths"), ("T2%", "tier2"), ("T3%", "tier3"),
         ("mechD", "mechdiv"), ("netD", "netdiv"), ("inv%", "inval")]


def gate(rows, tasks):
    def m(sel, key):
        v = [r[key] for r in rows if sel(r)]
        return round(float(np.mean(v)), 3) if v else 0.0
    gi = {t["group"]: t["intent"] for t in tasks}
    counts = collections.Counter(t["group"] for t in tasks)
    for g in GROUPS:
        G = lambda r, g=g: r["group"] == g
        print(f"\n========  {g.upper()}  (intent={gi.get(g,'?')}, {counts[g]} tasks, evidence over top-5)  ========")
        print(f"{'arm':>22} " + " ".join(f"{c[0]:>6}" for c in _COLS))
        for a in ARMS:
            cells = []
            for label, key in _COLS:
                sel = (lambda r, a=a, key=key, g=g: G(r) and r['arm'] == a and (r['first'] > 0 if key == 'first' else True))
                cells.append(f"{m(sel, key):>6}")
            print(f"{a:>22} " + " ".join(cells))
    # overall pass@5 by arm × group (the headline matrix)
    print("\n========  pass@5 matrix (arm × group)  ========")
    print(f"{'arm':>22} " + " ".join(f"{g[:5]:>7}" for g in GROUPS) + f"{'ALL':>7}")
    for a in ARMS:
        cells = [f"{m(lambda r, a=a, g=g: r['group']==g and r['arm']==a, 'p5'):>7}" for g in GROUPS]
        allv = m(lambda r, a=a: r['arm'] == a, 'p5')
        print(f"{a:>22} " + " ".join(cells) + f"{allv:>7}")


if __name__ == "__main__":
    root = sys.argv[1] if len(sys.argv) > 1 else (
        "/tmp/curve-packets-600" if os.path.isdir("/tmp/curve-packets-600") else "/tmp/curve-packets-spike")
    st = PacketStore(root)
    tasks = build_tasks(st)
    cc = collections.Counter(t["group"] for t in tasks)
    nrop = sum(1 for r in st.records if (r.get("rop_summary") or {}).get("atlas_volume_mean") is not None)
    print(f"corpus={len(st)}  tasks={len(tasks)} {dict(cc)}  records_with_rop={nrop}")
    rows, A = run(st, tasks)
    gate(rows, tasks)
