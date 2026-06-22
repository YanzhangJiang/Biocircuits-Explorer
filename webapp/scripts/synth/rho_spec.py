"""rho_spec.py — calibrated robust-satisfaction score rho_Spec (Sprint S1).

Per-predicate continuous margins (dimensionless fraction-of-threshold units) combined with a smooth
soft-min (a differentiable conjunction). Calibrated so sign(rho_Spec) == the phenotyper's accept on
the broad-window family (sign_consistency 1.0 on /tmp/atlas-fresh; reproduce with calibrate.py).
Because soft-min is differentiable (unlike hard min()), rho_Spec doubles as the S2 optimizer objective.

Design facts established by the S1 bake-off (do not relearn the hard way):
  * WIDTH is the discriminating axis. interior_prominence is BACKWARDS as a broad-window discriminator
    (a sharp peak is more prominent than a plateau); it exists only to reject monotone/flat curves
    (argmax at an edge => prom<=0). An S2 optimizer must be steered toward plateau_width, NOT prominence.
  * Soft-min is one-sided (always <= min, bias <= log(k)/beta at a tie), so any beta-induced error is a
    near-boundary FALSE NEGATIVE, removed by the symmetric |rho|>=eps abstention band. Keep beta>=~1.5.
  * The score is sign-AGREEMENT with the phenotyper, not biological truth; it inherits the phenotyper's
    plateau-vs-peak contamination. Use the per-draw robust fraction (not the record label) for ranking.
"""
from __future__ import annotations
import os
import sys

import numpy as np

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "reader"))
from functional_spec import FunctionalSpec, broad_window   # noqa: E402
from benchmark import _n                                   # noqa: E402  (the phenotyper's EXACT minmax-normalize)


# ── per-predicate margins (operate on the phenotyper-normalized curve yn) ───────────────────────
def _m_plateau_width(yn, u, threshold, scale, hi_level=0.9):
    hi = np.where(yn >= hi_level)[0]
    W = float(u[hi[-1]] - u[hi[0]]) if hi.size >= 2 else 0.0
    return (W - threshold) / scale, {"width_decades": W, "n_high": int(hi.size)}


def _m_interior_prominence(yn, u, threshold, scale):
    i = int(np.nanargmax(yn))
    interior = 0 < i < len(yn) - 1
    P = float(yn[i] - max(yn[0], yn[-1]))
    m = (P - threshold) / scale if interior else -1.0      # non-interior => hard reject (folds in f_interior_peak)
    return m, {"prominence": P, "interior": bool(interior), "argmax_idx": i}


_MARGINS = {"plateau_width": _m_plateau_width, "interior_prominence": _m_interior_prominence}


def softmin(margins, beta):
    """Smooth conjunction: -(1/beta) logsumexp(-beta*m). -> min(margins) as beta->inf. Stable."""
    a = -beta * np.asarray(margins, float)
    amax = np.max(a)
    return float(-(1.0 / beta) * (amax + np.log(np.sum(np.exp(a - amax)))))


def predicate_margins(spec: FunctionalSpec, y, u):
    """Return {predicate_name: (margin, detail)}. Flat / all-NaN curves -> all margins reject (-1)."""
    yn = _n(np.asarray(y, float))
    u = np.asarray(u, float)
    if not np.isfinite(yn).any():
        return {p.name: (-1.0, {"degenerate": True}) for p in spec.predicates}
    out = {}
    for p in spec.predicates:
        fn = _MARGINS.get(p.name)
        if fn is None:
            raise KeyError(f"no margin implementation for predicate '{p.name}'")
        m, info = fn(yn, u, p.threshold, p.scale, **(p.params or {}))
        out[p.name] = (float(m), info)
    return out


def rho_spec(spec: FunctionalSpec, y, u):
    """Evaluate rho_Spec on one curve. Returns rho, accept (rho>=0), abstain (|rho|<eps), and margins."""
    pm = predicate_margins(spec, y, u)
    rho = softmin([v[0] for v in pm.values()], spec.beta) - spec.delta
    return {"rho": float(rho), "accept": bool(rho >= 0.0), "abstain": bool(abs(rho) < spec.eps),
            "margins": {k: v[0] for k, v in pm.items()},
            "detail": {k: v[1] for k, v in pm.items()}}


def rho_spec_broad_window(y, u, intent="robust"):
    """Convenience: evaluate the canonical S1 broad-window spec on one curve."""
    return rho_spec(broad_window(intent=intent), y, u)


def robust_fraction(spec: FunctionalSpec, curves, u):
    """Record-level robust satisfaction = fraction of (valid) draws with rho>=0. Use this for ranking,
    NOT the categorical record label (the gate inherits the phenotyper's plateau-vs-peak contamination)."""
    accs = [rho_spec(spec, y, u)["accept"] for y in curves]
    return float(np.mean(accs)) if accs else 0.0


def record_accept(spec: FunctionalSpec, curves, u):
    """Record-level accept under the spec's intent: robust => fraction>=tau; existential => any draw."""
    frac = robust_fraction(spec, curves, u)
    if spec.intent == "existential":
        return frac > 0.0, frac
    return frac >= spec.tau, frac


if __name__ == "__main__":
    # smoke: a broad plateau accepts, a narrow peak and a monotone rise reject
    u = np.linspace(-6, 6, 64)
    def bump(c, w):
        return np.exp(-((u - c) / w) ** 2)
    sp = broad_window()
    for name, y in [("broad_plateau", bump(0, 4.0)), ("narrow_peak", bump(0, 0.8)),
                    ("monotone", 1 / (1 + np.exp(-1.5 * u)))]:
        r = rho_spec(sp, y, u)
        print(f"{name:16s} rho={r['rho']:+.3f} accept={r['accept']} abstain={r['abstain']} "
              f"margins={ {k: round(v,3) for k,v in r['margins'].items()} }")
