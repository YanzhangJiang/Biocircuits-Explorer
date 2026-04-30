"""Small Tier 0 property observations.

These helpers operate on already-computed program summaries. Full ROP execution
is intentionally outside this module for Tier 0.
"""

from __future__ import annotations

import math
from typing import Sequence

from .complete_definition import DEFAULT_SIGN_EPS
from .sign_programs import matrix_sign_pattern, sign_program_summary


def observe_sign_switch(program: Sequence[object], eps: float = DEFAULT_SIGN_EPS) -> dict[str, object]:
    summary = sign_program_summary(program, eps)
    return {
        "hit": int(summary["max_sign_switch_count"]) > 0,
        "strength": summary,
        "program_summary": summary,
    }


def observe_ultrasensitivity(program: Sequence[object], mu: int) -> dict[str, object]:
    finite_values = []
    singular = []
    for idx, value in enumerate(program):
        try:
            number = float(value)
        except Exception:
            continue
        if math.isfinite(number):
            finite_values.append(number)
        else:
            singular.append({"index": idx, "value": str(value)})
    max_ro = max(finite_values) if finite_values else None
    max_abs = max((abs(value) for value in finite_values), default=None)
    return {
        "hit": max_ro is not None and max_ro > mu,
        "strength": {
            "definition": "finite_RO_greater_than_mu",
            "mu": mu,
            "max_finite_ro": max_ro,
            "max_abs_finite_ro": max_abs,
            "singular": singular,
        },
        "program_summary": {"program": list(program)},
    }


def observe_mimo_gain(matrix: Sequence[Sequence[object]], eps: float = DEFAULT_SIGN_EPS) -> dict[str, object]:
    pattern = matrix_sign_pattern(matrix, eps)
    return {
        "hit": True,
        "strength": {
            "sign_pattern": pattern["pattern"],
            "singular": pattern["singular"],
        },
        "program_summary": pattern,
    }
