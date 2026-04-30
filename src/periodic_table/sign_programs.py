"""Three-state sign program utilities."""

from __future__ import annotations

import math
from typing import Iterable, Sequence

from .complete_definition import DEFAULT_SIGN_EPS


def _to_float(value: object) -> float:
    if isinstance(value, str):
        text = value.strip().lower()
        if text in {"nan", "+nan", "-nan"}:
            return math.nan
        if text in {"inf", "+inf", "infinity", "+infinity"}:
            return math.inf
        if text in {"-inf", "-infinity"}:
            return -math.inf
    return float(value)  # type: ignore[arg-type]


def sign3(rho: object, eps: float = DEFAULT_SIGN_EPS) -> dict[str, object]:
    value = _to_float(rho)
    if math.isnan(value):
        return {"finite": False, "singular": "nan"}
    if math.isinf(value):
        return {"finite": False, "singular": "neg_inf" if value < 0 else "pos_inf"}
    if value > eps:
        return {"finite": True, "sign": 1, "singular": "none"}
    if value < -eps:
        return {"finite": True, "sign": -1, "singular": "none"}
    return {"finite": True, "sign": 0, "singular": "none"}


def rle_keep_zero(signs: Iterable[int]) -> list[int]:
    out: list[int] = []
    for sign in signs:
        if sign not in (-1, 0, 1):
            raise ValueError(f"invalid finite sign: {sign}")
        if not out or out[-1] != sign:
            out.append(sign)
    return out


def format_sign(sign: int) -> str:
    if sign == 1:
        return "+"
    if sign == -1:
        return "-"
    if sign == 0:
        return "0"
    raise ValueError(f"invalid finite sign: {sign}")


def transition_label(left: int, right: int) -> str:
    return f"{format_sign(left)}->{format_sign(right)}"


def finite_signs(values: Iterable[object], eps: float = DEFAULT_SIGN_EPS) -> tuple[list[int], list[dict[str, object]]]:
    signs: list[int] = []
    singular: list[dict[str, object]] = []
    for idx, value in enumerate(values):
        token = sign3(value, eps)
        if token["finite"]:
            signs.append(int(token["sign"]))
        else:
            singular.append({"index": idx, "singular": token["singular"]})
    return signs, singular


def sign_program_summary(values: Sequence[object], eps: float = DEFAULT_SIGN_EPS) -> dict[str, object]:
    signs, singular = finite_signs(values, eps)
    rle = rle_keep_zero(signs)
    transitions = [transition_label(a, b) for a, b in zip(rle, rle[1:])]
    transition_pairs = list(zip(rle, rle[1:]))
    state_set = sorted({format_sign(sign) for sign in rle})
    via_zero = any(window in ([1, 0, -1], [-1, 0, 1]) for window in _windows(rle, 3))
    direct = any(pair in ((1, -1), (-1, 1)) for pair in transition_pairs)
    opposite = 1 in rle and -1 in rle
    settle_to_zero = len(rle) > 1 and rle[-1] == 0 and any(sign != 0 for sign in rle[:-1])
    return {
        "eps": eps,
        "program_signs": signs,
        "program_sign_rle": rle,
        "singular": singular,
        "sign_state_set": state_set,
        "sign_transitions": transitions,
        "three_state": set(rle) == {-1, 0, 1},
        "opposite_sign_program": opposite,
        "via_zero_opposite_switch": via_zero,
        "direct_opposite_switch": direct,
        "settle_to_zero": settle_to_zero,
        "max_sign_switch_count": max(0, len(rle) - 1),
    }


def matrix_sign_pattern(matrix: Sequence[Sequence[object]], eps: float = DEFAULT_SIGN_EPS) -> dict[str, object]:
    pattern: list[list[int | None]] = []
    singular: list[dict[str, object]] = []
    for row_idx, row in enumerate(matrix):
        out_row: list[int | None] = []
        for col_idx, value in enumerate(row):
            token = sign3(value, eps)
            if token["finite"]:
                out_row.append(int(token["sign"]))
            else:
                out_row.append(None)
                singular.append({"row": row_idx, "col": col_idx, "singular": token["singular"]})
        pattern.append(out_row)
    return {"eps": eps, "pattern": pattern, "singular": singular}


def _windows(values: Sequence[int], width: int) -> list[list[int]]:
    if width <= 0:
        return []
    return [list(values[idx : idx + width]) for idx in range(0, max(0, len(values) - width + 1))]
