"""Small structural helpers for periodic table witnesses."""

from __future__ import annotations

from itertools import permutations
from typing import Iterable

from .complex_generator import ComplexVector, Reaction, reaction_strings


def complexes_in_network(reactions: Iterable[Reaction]) -> set[ComplexVector]:
    complexes: set[ComplexVector] = set()
    for reaction in reactions:
        complexes.update(reaction.reactants)
        complexes.add(reaction.product)
    return complexes


def canonical_network_string(reactions: Iterable[Reaction], d: int) -> str:
    reaction_list = list(reactions)
    if not reaction_list:
        return "empty"
    candidates: list[str] = []
    for permutation in permutations(range(d)):
        permuted = [reaction.permute(tuple(permutation)) for reaction in reaction_list]
        candidates.append("|".join(reaction_strings(permuted)))
    return min(candidates)


def assembly_depths(reactions: Iterable[Reaction], d: int) -> dict[ComplexVector, int | None]:
    reaction_list = list(reactions)
    depths: dict[ComplexVector, int | None] = {}
    for idx in range(d):
        counts = tuple(1 if idx == j else 0 for j in range(d))
        depths[ComplexVector(counts)] = 0

    for complex_vector in complexes_in_network(reaction_list):
        depths.setdefault(complex_vector, None)

    changed = True
    while changed:
        changed = False
        for reaction in reaction_list:
            left_depth = depths.get(reaction.reactants[0])
            right_depth = depths.get(reaction.reactants[1])
            if left_depth is None or right_depth is None:
                continue
            candidate = 1 + max(left_depth, right_depth)
            current = depths.get(reaction.product)
            if current is None or candidate < current:
                depths[reaction.product] = candidate
                changed = True
    return depths


def structural_features(reactions: Iterable[Reaction], d: int) -> dict[str, object]:
    reaction_list = list(reactions)
    complexes = complexes_in_network(reaction_list)
    depths = assembly_depths(reaction_list, d)
    resolved_depths = [depth for complex_vector, depth in depths.items() if complex_vector in complexes and depth is not None]
    unresolved = sorted(
        complex_vector.symbol()
        for complex_vector, depth in depths.items()
        if complex_vector in complexes and depth is None
    )
    return {
        "reaction_count": len(reaction_list),
        "complex_count": len(complexes),
        "max_complex_size": max((complex_vector.size for complex_vector in complexes), default=0),
        "assembly_depth": max(resolved_depths) if resolved_depths else 0,
        "unresolved_complexes": unresolved,
    }
