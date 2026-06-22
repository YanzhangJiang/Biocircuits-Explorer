"""functional_spec.py — FunctionalSpec v0.1 (Sprint S1 of the Functional Synthesis plan).

A FunctionalSpec is a structured, intent-tagged set of predicates over a one-input dose-response
curve f: U -> R. Each predicate carries a threshold and a fraction-of-threshold scale so the
continuous margins in rho_spec.py share ONE comparable scale (so the abstention band eps means the
same thing on every axis). It replaces the fragile reference-curve / bare-label target with a spec
whose robust-satisfaction score rho_Spec is CALIBRATED to agree in sign with the phenotyper
(see calibrate.py: sign_consistency 1.0 on the broad-window family).
"""
from __future__ import annotations
from dataclasses import asdict, dataclass, field

SPEC_VERSION = "functional-spec/v0.1.0"


@dataclass
class Predicate:
    """One AND-combined predicate. `name` selects the margin implementation in rho_spec.py.
    margin = (feature - threshold) / scale  (dimensionless fraction-of-threshold units)."""
    name: str
    threshold: float
    scale: float
    params: dict = field(default_factory=dict)
    discriminating: bool = True   # False => a gate term the S2 optimizer must NOT be steered to maximize


@dataclass
class FunctionalSpec:
    family: str
    intent: str                       # existential | typical | robust
    predicates: list                  # list[Predicate], AND-combined via soft-min
    beta: float = 2.0                 # soft-min sharpness; sign-exact vs the phenotyper at beta>=~1.5
    delta: float = 0.0                # global centering offset (0 shipped; not needed at beta>=2)
    eps: float = 0.10                 # abstention band: |rho_Spec| < eps -> route to the exact verifier
    tau: float = 0.5                  # robust record-level accept = fraction(rho>=0 over draws) >= tau
    spec_version: str = SPEC_VERSION

    def to_dict(self):
        d = asdict(self)
        return d


def broad_window(intent: str = "robust") -> FunctionalSpec:
    """The S1 broad-window / bandpass-with-plateau spec.

    Calibrated sign-exact vs the phenotyper's f_broad_plateau = (decade-width of the {yn>=0.9}
    region >= 2.0) AND (interior prominence >= 0.12). WIDTH is the discriminating axis;
    interior_prominence is NOT a broad-window discriminator (a sharp peak is MORE prominent than a
    plateau) and is flagged discriminating=False — it only rejects monotone/flat curves. An S2
    optimizer maximizing rho_Spec must be steered toward plateau_width, never prominence.
    """
    return FunctionalSpec(
        family="broad_window",
        intent=intent,
        predicates=[
            Predicate("plateau_width", threshold=2.0, scale=2.0, params={"hi_level": 0.9}, discriminating=True),
            Predicate("interior_prominence", threshold=0.12, scale=0.12, discriminating=False),
        ],
        beta=2.0, delta=0.0, eps=0.10, tau=0.5,
    )


# registry of named specs (extend per family as S1 expands beyond broad-window)
FAMILIES = {"broad_window": broad_window}


def get_spec(family: str, intent: str = "robust") -> FunctionalSpec:
    if family not in FAMILIES:
        raise KeyError(f"unknown FunctionalSpec family '{family}'; have {sorted(FAMILIES)}")
    return FAMILIES[family](intent=intent)
