"""verifier.py — S2 verifier interface + a numpy MockVerifier (engine-independent dev) and an
EngineVerifier (production, wraps engine_client). The synthesis harness (synthesize.py /
benchmark_s2.py) is verifier-AGNOSTIC: swap MockVerifier -> EngineVerifier when the live Julia
engine is up, with no change to the optimizer / CEGIS / benchmark code.

A Verifier maps a fixed topology + theta (the design variables) to:
  * curve(topology, theta)        CHEAP: one dose-response curve over u_grid              (1 cheap solve)
  * phenotype(topology, theta, K) EXACT: K-draw phenotype -> shape_support + accept        (K exact solves)
plus ROP-skeleton hooks (rop_feasible / rop_seed) and a prior library() for NN retrieval.
All solve costs accrue in `.cost` (CostCounter) so the benchmark can report solves-to-verified.

IMPORTANT: MockVerifier is a transparent numpy SURROGATE for harness validation only — NOT the
equilibrium solver. Numbers produced against it are "does the harness work / do the arms differ",
not biology. Real numbers come from EngineVerifier once the engine is up.
"""
from __future__ import annotations
import hashlib
import os
import sys
from abc import ABC, abstractmethod
from dataclasses import dataclass

import numpy as np

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "reader"))
from benchmark import _n, f_broad_plateau   # noqa: E402  (phenotyper normalize + per-draw broad-plateau proxy)

DEFAULT_U = np.linspace(-6.0, 6.0, 64)


class EngineDown(Exception):
    """Raised when the engine is offline/500s, so callers can distinguish an ENGINE failure from a
    genuinely-NaN/non-target curve (the latter is a real 'reject', the former must bail + resume)."""


@dataclass
class CostCounter:
    cheap_calls: int = 0     # cheap curve evaluations (rho_Spec inner loop)
    exact_calls: int = 0     # exact phenotype verifications
    draws: int = 0           # total K-draw equilibrium curves under exact verification
    builds: int = 0          # model builds (the per-theta rebuild cost, unless fixed_qK is used)

    def cheap(self, n=1):
        self.cheap_calls += n

    def exact(self, K):
        self.exact_calls += 1
        self.draws += K

    def build(self, n=1):
        self.builds += n

    def snapshot(self):
        return {"cheap_calls": self.cheap_calls, "exact_calls": self.exact_calls,
                "draws": self.draws, "builds": self.builds}

    def reset(self):
        self.cheap_calls = self.exact_calls = self.draws = self.builds = 0


class Verifier(ABC):
    name = "abstract"
    u_grid: np.ndarray
    cost: CostCounter

    @abstractmethod
    def theta_bounds(self, topology): ...
    @abstractmethod
    def curve(self, topology, theta): ...
    @abstractmethod
    def phenotype(self, topology, theta, K=8): ...

    # ROP-skeleton hooks (engine: wrap ro_behavior; mock: heuristic)
    def rop_feasible(self, topology, spec):
        return True

    def rop_seed(self, topology, spec):
        return None

    # prior library for NN retrieval (engine: the precomputed atlas; mock: sampled theta)
    def library(self, spec, n=60):
        return []


# ── MockVerifier: a transparent numpy surrogate "engine" ────────────────────────────────────────
class MockVerifier(Verifier):
    """theta = [c1, c2, s1, s2, h]: curve(u) = h * sigmoid(s1*(u-c1)) * (1 - sigmoid(s2*(u-c2))),
    a window from c1..c2. Broad-window (bandpass) needs c2-c1 wide AND c2 inside the grid (so it
    falls back down -> interior peak). c2 beyond the grid -> monotone-to-plateau (argmax at edge ->
    f_broad_plateau False), exactly like the real phenotyper's interior condition. Exact phenotype =
    f_broad_plateau over K theta-jittered draws (models robustness over the Kd prior)."""
    name = "mock"
    DIM = 5

    def __init__(self, u_grid=None, noise=None, tau=0.5, lib_seed=12345):
        self.u_grid = np.asarray(u_grid, float) if u_grid is not None else DEFAULT_U.copy()
        self.noise = np.asarray(noise, float) if noise is not None else np.array([0.3, 0.3, 0.3, 0.3, 0.05])
        self.tau = tau
        self.cost = CostCounter()
        self._lib_rng = np.random.default_rng(lib_seed)

    def theta_bounds(self, topology=None):
        # Wide box where broad-window is RARE: c2 often beyond the grid (no return -> not interior) and
        # slopes often too shallow (no flat >=0.9 top), so success requires real search, not luck.
        return [(-6.0, 3.0), (-3.0, 13.0), (0.4, 5.0), (0.4, 5.0), (0.15, 1.0)]

    def _raw_curve(self, theta):
        c1, c2, s1, s2, h = (float(x) for x in theta)
        u = self.u_grid
        rise = 1.0 / (1.0 + np.exp(-s1 * (u - c1)))
        fall = 1.0 / (1.0 + np.exp(-s2 * (u - c2)))
        return h * rise * (1.0 - fall)

    def curve(self, topology, theta):
        self.cost.cheap(); self.cost.build()           # rebuild-per-theta default (the safe assumption)
        return self._raw_curve(np.asarray(theta, float))

    def phenotype(self, topology, theta, K=8):
        self.cost.exact(K); self.cost.build()
        th = np.asarray(theta, float)
        # deterministic per-theta jitter (a real verifier is a function of theta, not of call order)
        seed = int.from_bytes(hashlib.sha1(th.tobytes()).digest()[:4], "little")
        rng = np.random.default_rng(seed)
        draws = [self._raw_curve(th + rng.normal(0.0, self.noise)) for _ in range(K)]
        hits = [bool(f_broad_plateau(y, self.u_grid)) for y in draws]
        support = float(np.mean(hits))
        return {"accept": bool(support >= self.tau), "shape_support": support,
                "dominant_shape": "bandpass_with_plateau" if support >= self.tau else "other",
                "curves": draws}

    def rop_feasible(self, topology, spec):
        return True                                     # the window topology can structurally make low-high-low

    def rop_seed(self, topology, spec):
        # ROP-feasible skeleton (a low-high-low window) but a GENERIC, borderline parameterization, NOT a
        # pre-verified answer: a narrow-ish window the optimizer must still widen to pass the exact gate.
        return np.array([-1.0, 1.3, 1.2, 1.2, 0.8])

    def library(self, spec, n=60):
        b = self.theta_bounds()
        lo = np.array([x[0] for x in b]); hi = np.array([x[1] for x in b])
        ths = lo + (hi - lo) * self._lib_rng.random((n, len(b)))
        return [(th, self._raw_curve(th)) for th in ths]


# ── RuggedMockVerifier: a DECEPTIVE stress fixture for CEGIS (NOT a benchmark) ───────────────────
class RuggedMockVerifier(MockVerifier):
    """STRESS FIXTURE — not a benchmark result. A deceptive performance-vs-robustness landscape:
    cheap rho_Spec rises monotonically with plateau width, but EXACT robustness holds only in a width
    BAND [2, w_robust]; beyond w_robust the design is fragile (the wider the nominal plateau, the more
    the kd prior destroys it). A greedy optimizer maximizing cheap rho over-widens into the brittle
    region (HIGH cheap rho, exact-REJECT) and stalls — and because the robust band has LOWER cheap rho
    than the trap, the ratcheted exact gate can never reach it once a wide candidate is checked. CEGIS
    must inject the exact verifier's robustness knowledge as counterexamples (and the gate must key on
    the PENALIZED score) to pull the search back to the robust band. Purpose: UNIT-TEST that CEGIS
    recovers where plain DE stalls."""
    name = "rugged_mock"

    def __init__(self, u_grid=None, noise=None, tau=0.7, lib_seed=12345, w_robust=3.0, trap_gain=3.0):
        super().__init__(u_grid=u_grid, noise=noise, tau=tau, lib_seed=lib_seed)
        self.w_robust = w_robust
        self.trap_gain = trap_gain

    def nominal_width(self, theta):
        yn = _n(self._raw_curve(np.asarray(theta, float)))
        hi = np.where(yn >= 0.9)[0]
        return float(self.u_grid[hi[-1]] - self.u_grid[hi[0]]) if hi.size >= 2 else 0.0

    def phenotype(self, topology, theta, K=8):
        self.cost.exact(K); self.cost.build()
        th = np.asarray(theta, float)
        frag = 1.0 + self.trap_gain * max(0.0, self.nominal_width(th) - self.w_robust)
        noise_eff = np.array(self.noise, float).copy()
        noise_eff[2:4] *= frag            # slope dims: fragility flattens the >=0.9 top under the prior
        noise_eff[0:2] *= np.sqrt(frag)   # centers wobble too
        seed = int.from_bytes(hashlib.sha1(th.tobytes()).digest()[:4], "little")
        rng = np.random.default_rng(seed)
        draws = [self._raw_curve(th + rng.normal(0.0, noise_eff)) for _ in range(K)]
        support = float(np.mean([bool(f_broad_plateau(y, self.u_grid)) for y in draws]))
        return {"accept": bool(support >= self.tau), "shape_support": support,
                "dominant_shape": "bandpass_with_plateau" if support >= self.tau else "other",
                "curves": draws}

    def rop_seed(self, topology, spec):
        # ROP is robustness-BLIND: it returns a broad-feasible window — here a WIDE one that sits in the
        # fragile trap, so the deception bites and CEGIS must rescue the search.
        return np.array([-4.0, 4.0, 2.5, 2.5, 0.95])


# ── EngineVerifier: production wrapper over the live Julia engine (runs when the engine is up) ────
class EngineVerifier(Verifier):
    """topology = {"reactions": [...], "input_symbol": "tA"|None, "observe_species": "C_A_B"|None};
    theta = log10(kd) per reaction. curve() = build_model + dose_response; phenotype() =
    phenotype_classify -> shape_fractions[target] -> accept if support >= tau. NOT exercised offline;
    correct-by-construction against engine_client. fixed_qK fast-path (avoid per-theta rebuild) is a
    profiled-then-wired S2.0 optimization; default here rebuilds per theta (the safe assumption)."""
    name = "engine"

    def __init__(self, u_grid=None, kd_lo=-3.0, kd_hi=3.0, target_shape="bandpass_with_plateau",
                 tau=0.4, impl_noise=0.3):
        import engine_client as E
        self.E = E
        self.u_grid = np.asarray(u_grid, float) if u_grid is not None else DEFAULT_U.copy()
        self.kd_lo, self.kd_hi = kd_lo, kd_hi
        self.target_shape, self.tau = target_shape, tau
        self.impl_noise = impl_noise        # log10(kd) implementation tolerance for the robust verdict
        self.cost = CostCounter()

    def theta_bounds(self, topology):
        return [(self.kd_lo, self.kd_hi)] * len(topology["reactions"])

    def _build(self, topology, theta):
        self.cost.build()
        kd = [10.0 ** float(x) for x in theta]
        return self.E.build_model(reactions=topology["reactions"], kd=kd)

    def _io(self, topology, m):
        inp = topology.get("input_symbol") or (m.get("q_sym") or [None])[0]
        prod = m.get("product_species") or m.get("x_sym") or [None]
        obs = topology.get("observe_species") or prod[-1]
        return inp, obs

    def _dose(self, topology, theta):
        """build at kd=10^theta + dose-response -> curve on u_grid. Raises EngineDown on engine error
        (so callers don't mistake an engine 500 for a non-target curve). Increments builds (via _build)."""
        m = self._build(topology, theta)
        if m.get("engine_offline") or m.get("error"):
            raise EngineDown(m.get("error") or "engine offline")
        inp, obs = self._io(topology, m)
        dr = self.E.dose_response(m["session_id"], param_symbol=inp, output_exprs=[obs],
                                  param_min=float(self.u_grid[0]), param_max=float(self.u_grid[-1]),
                                  n_points=len(self.u_grid))
        if dr.get("engine_offline") or dr.get("error"):
            raise EngineDown(dr.get("error") or "engine offline")
        xs = dr.get("param_values") or list(self.u_grid)
        ys = [(row[0] if isinstance(row, list) else row) for row in (dr.get("output_traj") or [])]
        if len(ys) != len(self.u_grid):
            ys = np.interp(self.u_grid, xs, ys) if ys else np.full(len(self.u_grid), np.nan)
        return np.asarray(ys, float)

    def curve(self, topology, theta):
        self.cost.cheap()
        try:
            return self._dose(topology, theta)
        except EngineDown:
            return np.full(len(self.u_grid), np.nan)   # optimizer-graceful: an engine blip reads as reject

    def phenotype(self, topology, theta, K=8):
        """Robust-to-IMPLEMENTATION verdict at THIS kd: perturb log10(kd) by impl_noise, K draws, the
        fraction whose engine dose-response is a broad plateau (f_broad_plateau). kd-DEPENDENT — unlike
        phenotype_classify, which samples the GLOBAL Kd prior and is a topology property (use that for
        topology-level robustness, not kd-design). Returns {offline:True, shape_support:None} on an
        engine failure (so callers bail+resume rather than recording a fake 0)."""
        self.cost.exact(K)
        th = np.asarray(theta, float)
        rng = np.random.default_rng(int.from_bytes(hashlib.sha1(th.tobytes()).digest()[:4], "little"))
        hits, n_ok = 0, 0
        try:
            for _ in range(K):
                y = self._dose(topology, th + rng.normal(0.0, self.impl_noise, size=th.shape))
                if int(np.isfinite(y).sum()) >= 3:
                    n_ok += 1
                    if f_broad_plateau(y, self.u_grid):
                        hits += 1
        except EngineDown as e:
            return {"offline": True, "shape_support": None, "error": str(e)}
        support = float(hits / K)
        return {"accept": bool(support >= self.tau), "shape_support": round(support, 3),
                "n_valid": n_ok, "dominant_shape": ("bandpass_with_plateau" if support >= self.tau else "other")}

    def rop_feasible(self, topology, spec):
        """Broad-window skeleton (rise then fall) is feasible iff some family reverses sign >=1 time."""
        m = self._build(topology, [0.0] * len(topology["reactions"]))
        if m.get("engine_offline") or m.get("error"):
            return True
        inp, obs = self._io(topology, m)
        bf = self.E.phenotype(m["session_id"], change_qK=inp, observe_x=obs,
                              path_scope="feasible", compute_volume=False)
        fams = bf.get("exact_families") or bf.get("families") or []
        return any((f.get("n_sign_changes") or 0) >= 1 for f in fams) if fams else True

    def rop_seed(self, topology, spec):
        return None   # TODO(S2): seed from an atlas broad-window record's kd; for now -> random init
