"""Upper-bound cell witness inheritance for periodic-table summaries."""

from __future__ import annotations

from copy import deepcopy
from typing import Any

from .complete_definition import STATUS_INHERITED_WITNESS, STATUS_UNKNOWN, STATUS_WITNESS_ONLY

POSITIVE_STATUSES = {STATUS_WITNESS_ONLY, STATUS_INHERITED_WITNESS}


def apply_inherited_witnesses(cells: list[dict[str, Any]], witnesses: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Fill UNKNOWN upper-bound cells with admissible smaller-cell witnesses.

    The periodic-table coordinates are upper bounds. If a witness is found in
    `(d0, mu0)`, it is admissible in every `(d, mu)` with `d >= d0` and
    `mu >= mu0`. This postprocess never turns a certified negative into a
    positive result and never overrides a direct witness.
    """
    witness_by_id = {witness.get("witness_id"): witness for witness in witnesses}
    positives = [
        cell
        for cell in cells
        if cell.get("status") in POSITIVE_STATUSES and cell.get("witness_id") in witness_by_id
    ]
    out: list[dict[str, Any]] = []
    for cell in cells:
        if cell.get("status") != STATUS_UNKNOWN:
            out.append(cell)
            continue
        inherited = _best_inheritable(cell, positives, witness_by_id)
        if inherited is None:
            out.append(cell)
            continue
        source_cell, source_witness = inherited
        updated = deepcopy(cell)
        updated["status"] = STATUS_INHERITED_WITNESS
        updated["witness_id"] = source_cell.get("witness_id")
        updated["min_r"] = source_cell.get("min_r")
        updated["min_assembly_depth"] = source_cell.get("min_assembly_depth")
        updated["strength"] = deepcopy(source_cell.get("strength", {}))
        updated["strength"]["inherited_from_cell"] = {
            "d": source_cell.get("d"),
            "mu": source_cell.get("mu"),
            "property_id": source_cell.get("property_id"),
            "witness_id": source_cell.get("witness_id"),
        }
        updated["strength"]["inheritance_basis"] = "upper_bound_design_space_monotonicity"
        updated["notes"] = (
            "inherited witness from smaller upper-bound cell "
            f"(d={source_cell.get('d')}, mu={source_cell.get('mu')}); "
            "not a new local minimality certificate"
        )
        features = _witness_features(source_witness)
        updated["min_r"] = features.get("reaction_count", updated.get("min_r"))
        updated["min_assembly_depth"] = features.get("assembly_depth", updated.get("min_assembly_depth"))
        out.append(updated)
        positives.append(updated)
    return out


def _best_inheritable(
    target: dict[str, Any],
    positives: list[dict[str, Any]],
    witness_by_id: dict[object, dict[str, Any]],
) -> tuple[dict[str, Any], dict[str, Any]] | None:
    candidates: list[tuple[tuple[Any, ...], dict[str, Any], dict[str, Any]]] = []
    for cell in positives:
        if cell.get("property_id") != target.get("property_id"):
            continue
        if int(cell.get("d", 0)) > int(target.get("d", 0)):
            continue
        if int(cell.get("mu", 0)) > int(target.get("mu", 0)):
            continue
        if cell.get("d") == target.get("d") and cell.get("mu") == target.get("mu"):
            continue
        witness = witness_by_id.get(cell.get("witness_id"))
        if witness is None:
            continue
        features = _witness_features(witness)
        key = (
            features.get("reaction_count", cell.get("min_r") or 999999),
            features.get("assembly_depth", cell.get("min_assembly_depth") or 999999),
            features.get("complex_count", 999999),
            cell.get("d", 999999),
            cell.get("mu", 999999),
            cell.get("witness_id", ""),
        )
        candidates.append((key, cell, witness))
    if not candidates:
        return None
    _, cell, witness = min(candidates, key=lambda item: item[0])
    return cell, witness


def _witness_features(witness: dict[str, Any]) -> dict[str, Any]:
    source_meta = witness.get("source_metadata", {})
    if not isinstance(source_meta, dict):
        return {}
    features = source_meta.get("features", {})
    return features if isinstance(features, dict) else {}
