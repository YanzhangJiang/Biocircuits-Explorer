"""compare.py — Function-Space Atlas spike, Reader comparator (S2).

Deterministic function-space comparison over the K-draw curve packet. Two design
rules the owner fixed:

  (1) Target TYPE is explicit from the start — a fuzzy request must not be silently
      collapsed into one over-specific curve:
        * ReferenceCurveTarget   — an explicit / NL-compiled prototype curve
        * ConstraintCurveTarget  — qualitative region constraints (low/high/rise/fall/mid)
        * RefinementDeltaTarget  — "steeper fall / longer plateau / …" vs a base candidate
  (2) Phenotype is prior-conditioned, so every record is scored under THREE distinct
      modes that answer different questions and are never conflated:
        * existential — does SOME θ-draw realise the target?      (best single draw)
        * typical     — is the medoid (typical) behaviour close?   (medoid curve)
        * robust      — what FRACTION of θ-draws satisfy it?       (robustness)

ROP evidence (atlas volume / robust_path_count, carried in each record) is exposed
as an optional rerank weight here — the seed of the S3 ROP roles.
"""
from __future__ import annotations
import os
import sys
import numpy as np

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from packet_store import PacketStore


# ── normalization + curve distances (NaN-masked) ──────────────────────────────
def normalize(y, mode="minmax"):
    y = np.asarray(y, float)
    finite = np.isfinite(y)
    if mode == "none" or not finite.any():
        return y
    if mode == "minmax":
        lo, hi = np.nanmin(y), np.nanmax(y)
        return np.where(finite, 0.0, np.nan) if hi - lo < 1e-9 else (y - lo) / (hi - lo)
    if mode == "zscore":
        mu, sd = np.nanmean(y), np.nanstd(y)
        return np.where(finite, 0.0, np.nan) if sd < 1e-9 else (y - mu) / sd
    raise ValueError(f"unknown normalize mode {mode}")


def curve_distance(a, b, metric="l2", normalize_mode="minmax", deriv_weight=0.3):
    """Distance between two curves on the SAME grid. Shape-first (normalized), with
    an optional derivative term so edge sharpness is felt, not just pointwise level."""
    an, bn = normalize(a, normalize_mode), normalize(b, normalize_mode)
    m = np.isfinite(an) & np.isfinite(bn)
    if m.sum() < 3:
        return np.inf
    av, bv = an[m], bn[m]
    if metric == "l2":
        d = float(np.sqrt(np.mean((av - bv) ** 2)))
    elif metric == "chebyshev":
        d = float(np.max(np.abs(av - bv)))
    elif metric == "correlation":
        d = 1.0 if av.std() < 1e-9 or bv.std() < 1e-9 else float(1 - np.corrcoef(av, bv)[0, 1]) / 2
    else:
        raise ValueError(f"unknown metric {metric}")
    if deriv_weight > 0:
        da, db = np.gradient(an), np.gradient(bn)
        md = np.isfinite(da) & np.isfinite(db)
        if md.sum() >= 3:
            dd = float(np.sqrt(np.mean((da[md] - db[md]) ** 2)))
            d = (1 - deriv_weight) * d + deriv_weight * dd
    return d


# ── target types ──────────────────────────────────────────────────────────────
class ReferenceCurveTarget:
    kind = "reference_curve"

    def __init__(self, y, metric="l2", normalize_mode="minmax", deriv_weight=0.3, accept=0.15):
        self.y = np.asarray(y, float)
        self.metric, self.norm, self.deriv_weight, self.accept = metric, normalize_mode, deriv_weight, accept

    def distance(self, curve, u=None):
        return curve_distance(self.y, curve, self.metric, self.norm, self.deriv_weight)

    def satisfied(self, curve, u=None):
        return self.distance(curve) <= self.accept


class ConstraintCurveTarget:
    """Qualitative region constraints instead of a specific curve. `regions` is a list
    of {"u": (lo, hi), "want": "low"|"high"|"mid"|"rise"|"fall", "w": weight}."""
    kind = "constraint_curve"

    def __init__(self, regions, normalize_mode="minmax", accept=0.2):
        self.regions, self.norm, self.accept = regions, normalize_mode, accept

    def _penalty(self, curve, u):
        yn = normalize(curve, self.norm)
        g = np.gradient(yn)
        tot = wsum = 0.0
        for r in self.regions:
            lo, hi = r["u"]; w = r.get("w", 1.0); wsum += w
            sel = (u >= lo) & (u <= hi) & np.isfinite(yn)
            if sel.sum() < 1:
                tot += w; continue
            seg, want = yn[sel], r["want"]
            if want == "low":
                pen = float(np.mean(seg))
            elif want == "high":
                pen = float(np.mean(1 - seg))
            elif want == "mid":
                pen = float(np.mean(np.abs(seg - 0.5)) * 2)
            elif want in ("rise", "fall"):
                gm = float(np.nanmean(g[sel]))
                pen = min(1.0, max(0.0, (-gm if want == "rise" else gm)) * 5)
            else:
                pen = 0.0
            tot += w * pen
        return tot / max(wsum, 1e-9)

    def distance(self, curve, u):
        return self._penalty(curve, u)

    def satisfied(self, curve, u):
        return self._penalty(curve, u) <= self.accept


class RefinementDeltaTarget:
    """Spike stub: rank by a scalar objective relative to a base candidate. Operates on
    the per-draw metrics carried in the index (not the raw curve)."""
    kind = "refinement_delta"
    _METRIC = {"longer_plateau": "pw", "steeper_fall": "fs", "higher_peak": "pp", "wider_range": "fc"}

    def __init__(self, delta):
        if delta not in self._METRIC:
            raise ValueError(f"unknown delta {delta}")
        self.delta = delta


# ── scoring under the three modes ─────────────────────────────────────────────
def score_record(store: PacketStore, rec, target, u=None):
    u = store.u_grid if u is None else u
    C = store.curves(rec)
    valid = np.where(store.valid_mask(rec))[0]
    if valid.size == 0:
        return None
    dists = np.array([target.distance(C[i], u) for i in valid], float)
    sats = np.array([1.0 if target.satisfied(C[i], u) else 0.0 for i in valid])
    med = store.medoid_curve(rec)
    typ = float(target.distance(med, u)) if med is not None else float(np.nanmin(dists))
    bi = int(valid[int(np.nanargmin(dists))]) + 1
    return dict(existential=float(np.nanmin(dists)), typical=typ, robust=float(np.mean(sats)),
                n_valid=int(valid.size), best_draw=bi)


def search_by_reference_curve(store: PacketStore, target, k=8, mode="typical", rop_weight=0.0):
    """Search the packet corpus by function similarity. `mode` ∈ {existential, typical,
    robust}; `rop_weight` > 0 adds an atlas-volume (robustness) rerank — an S3 seed."""
    out = []
    for rec in store.records:
        sc = score_record(store, rec, target)
        if sc is None:
            continue
        key = -sc["robust"] if mode == "robust" else sc[mode]   # robust: higher better
        vol = (rec.get("rop_summary") or {}).get("atlas_volume_mean") or 0.0
        key -= rop_weight * float(vol)
        out.append((key, sc, rec))
    out.sort(key=lambda t: t[0])
    return out[:k]


# ── prototype-curve builders (stand-ins for the NL→target compiler) ───────────
def bump(u, center=0.0, width=2.0):
    return np.exp(-((u - center) / width) ** 2)


def sigmoid(u, center=0.0, slope=1.5, falling=False):
    s = 1.0 / (1.0 + np.exp(-slope * (u - center)))
    return 1.0 - s if falling else s


if __name__ == "__main__":
    root = sys.argv[1] if len(sys.argv) > 1 else (
        "/tmp/curve-packets-600" if os.path.isdir("/tmp/curve-packets-600") else "/tmp/curve-packets-spike")
    st = PacketStore(root)
    u = st.u_grid
    print(f"corpus: {len(st)} records @ {root}\n")

    def show(title, target, mode):
        print(f"### {title}   [mode={mode}]")
        for key, sc, rec in search_by_reference_curve(st, target, k=6, mode=mode):
            vol = (rec.get("rop_summary") or {}).get("atlas_volume_mean")
            vols = f"{vol:.3f}" if isinstance(vol, (int, float)) else "  -  "
            print(f"  {rec['dominant_shape']:22s} exist={sc['existential']:.3f} "
                  f"typ={sc['typical']:.3f} robust={sc['robust']:.2f} "
                  f"nv={sc['n_valid']} vol={vols}  {rec['io_assignment']['input_symbol']}→{rec['io_assignment']['output_symbol']}")
        print()

    # T1: explicit reference curve — a broad bump (no label named)
    show("broad bump (reference_curve)", ReferenceCurveTarget(bump(u, 0.0, 2.5)), "typical")
    show("broad bump (reference_curve)", ReferenceCurveTarget(bump(u, 0.0, 2.5)), "existential")

    # T2: constraint curve — low / high-broad-middle / low, with a sharper fall than rise
    regions = [{"u": (-6, -3), "want": "low", "w": 1.0},
               {"u": (-3, -1), "want": "rise", "w": 1.0},
               {"u": (-1, 1), "want": "high", "w": 1.5},
               {"u": (1, 3), "want": "fall", "w": 1.5},
               {"u": (3, 6), "want": "low", "w": 1.0}]
    show("low→broad-high→low, sharp fall (constraint_curve)", ConstraintCurveTarget(regions), "typical")
    show("low→broad-high→low, sharp fall (constraint_curve)", ConstraintCurveTarget(regions), "robust")
