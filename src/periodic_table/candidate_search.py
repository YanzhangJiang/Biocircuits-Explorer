"""Deterministic bounded candidate generation for periodic-table searches."""

from __future__ import annotations

from dataclasses import dataclass
from itertools import combinations
from typing import Iterable

from .complex_generator import BASE_SYMBOLS, ComplexVector, Reaction, allowed_binding_reactions, make_reaction
from .network_generator import canonical_network_string, structural_features


@dataclass(frozen=True)
class CandidateNetwork:
    label: str
    d: int
    mu: int
    reactions: tuple[Reaction, ...]
    source_kind: str
    source_metadata: dict[str, object]

    @property
    def canonical(self) -> str:
        return canonical_network_string(self.reactions, self.d)

    @property
    def reaction_strings(self) -> list[str]:
        return [reaction.canonical_string() for reaction in self.reactions]

    def features(self) -> dict[str, object]:
        return structural_features(self.reactions, self.d)

    def output_symbols(self) -> list[str]:
        species = set()
        for reaction in self.reactions:
            species.update(reactant.symbol() for reactant in reaction.reactants)
            species.add(reaction.product.symbol())
        return sorted(species, key=lambda item: (len(item), item))

    def input_symbols(self) -> list[str]:
        return [f"t{BASE_SYMBOLS[idx]}" for idx in range(self.d)]

    def to_julia_spec(self) -> dict[str, object]:
        features = self.features()
        metadata = {
            "d": self.d,
            "mu": self.mu,
            "canonical_network": self.canonical,
            "features": features,
            **self.source_metadata,
        }
        return {
            "label": self.label,
            "reactions": self.reaction_strings,
            "input_symbols": self.input_symbols(),
            "output_symbols": self.output_symbols(),
            "source_kind": self.source_kind,
            "source_metadata": metadata,
        }


def generate_candidate_networks(
    d: int,
    mu: int,
    *,
    max_reactions: int = 3,
    limit: int = 0,
    pair_limit: int = 400,
    require_multiple_base_species: bool = True,
) -> list[CandidateNetwork]:
    """Generate a deterministic, increasing-complexity search frontier.

    This is intentionally a bounded search layer. It is useful for witness
    discovery but is not an exhaustive certificate for non-trivial cells.
    """
    if d <= 0 or mu <= 0:
        raise ValueError("d and mu must be positive")
    if max_reactions <= 0:
        return []

    allowed = allowed_binding_reactions(d, mu)
    candidates: list[CandidateNetwork] = []
    seen: set[str] = set()

    def add(label: str, reactions: Iterable[Reaction], source_kind: str, **metadata: object) -> None:
        reaction_tuple = tuple(sorted(set(reactions), key=lambda reaction: reaction.canonical_string()))
        if not reaction_tuple or len(reaction_tuple) > max_reactions:
            return
        if require_multiple_base_species and d > 1 and len(_used_base_indices(reaction_tuple)) < 2:
            return
        if require_multiple_base_species and d == 1:
            return
        canonical = canonical_network_string(reaction_tuple, d)
        if canonical in seen:
            return
        seen.add(canonical)
        candidates.append(
            CandidateNetwork(
                label=label,
                d=d,
                mu=mu,
                reactions=reaction_tuple,
                source_kind=source_kind,
                source_metadata=metadata,
            )
        )

    for reaction in allowed:
        add(f"single_{reaction.product.symbol()}", [reaction], "single_reaction", generator="single_reaction")

    for idx, reactions in enumerate(_chain_networks(d, mu)):
        add(
            f"chain_{idx + 1}",
            reactions,
            "assembly_chain",
            generator="assembly_chain",
            target_complex=reactions[-1].product.symbol() if reactions else None,
        )

    for idx, reactions in enumerate(_seed_networks(d, mu)):
        add(f"seed_{idx + 1}", reactions, "seed_motif", generator="seed_motif")

    if max_reactions >= 2 and pair_limit > 0:
        emitted = 0
        for left, right in combinations(allowed, 2):
            if not _connected_pair(left, right):
                continue
            add(
                f"pair_{emitted + 1}",
                [left, right],
                "connected_pair",
                generator="connected_pair",
            )
            emitted += 1
            if emitted >= pair_limit:
                break

    candidates.sort(key=_candidate_sort_key)
    if limit > 0:
        candidates = candidates[:limit]
    return candidates


def _candidate_sort_key(candidate: CandidateNetwork) -> tuple[object, ...]:
    features = candidate.features()
    return (
        features["reaction_count"],
        features["assembly_depth"],
        features["complex_count"],
        candidate.canonical,
    )


def _unit(d: int, idx: int) -> ComplexVector:
    return ComplexVector(tuple(1 if pos == idx else 0 for pos in range(d)))


def _vector_from_indices(d: int, indices: Iterable[int]) -> ComplexVector:
    counts = [0] * d
    for idx in indices:
        counts[idx] += 1
    return ComplexVector(tuple(counts))


def _chain_networks(d: int, mu: int) -> list[tuple[Reaction, ...]]:
    out: list[tuple[Reaction, ...]] = []
    for size in range(2, mu + 1):
        for target_counts in _bounded_targets(d, size):
            sequence: list[int] = []
            for idx, count in enumerate(target_counts):
                sequence.extend([idx] * count)
            if not sequence:
                continue
            current = _unit(d, sequence[0])
            reactions: list[Reaction] = []
            for idx in sequence[1:]:
                nxt = _unit(d, idx)
                reaction = make_reaction(current, nxt, mu)
                reactions.append(reaction)
                current = reaction.product
            out.append(tuple(reactions))
    return out


def _bounded_targets(d: int, size: int) -> list[tuple[int, ...]]:
    targets: list[tuple[int, ...]] = []

    def rec(remaining: int, parts: int, prefix: tuple[int, ...]) -> None:
        if parts == 1:
            targets.append((*prefix, remaining))
            return
        for value in range(remaining + 1):
            rec(remaining - value, parts - 1, (*prefix, value))

    rec(size, d, tuple())
    return [target for target in targets if sum(target) == size]


def _seed_networks(d: int, mu: int) -> list[tuple[Reaction, ...]]:
    seeds: list[tuple[Reaction, ...]] = []
    if d >= 2 and mu >= 2:
        a = _unit(d, 0)
        b = _unit(d, 1)
        seeds.append((make_reaction(a, b, mu),))
    if d >= 3 and mu >= 3:
        a = _unit(d, 0)
        b = _unit(d, 1)
        c = _unit(d, 2)
        ab = make_reaction(a, b, mu)
        seeds.append((ab, make_reaction(ab.product, c, mu)))
    if d >= 2 and mu >= 3:
        a = _unit(d, 0)
        b = _unit(d, 1)
        aa = make_reaction(a, a, mu)
        seeds.append((aa, make_reaction(aa.product, b, mu)))
    if d >= 2 and mu >= 4:
        a = _unit(d, 0)
        b = _unit(d, 1)
        aa = make_reaction(a, a, mu)
        bb = make_reaction(b, b, mu)
        seeds.append((aa, bb, make_reaction(aa.product, bb.product, mu)))
    if d >= 4 and mu >= 4:
        a = _unit(d, 0)
        b = _unit(d, 1)
        c = _unit(d, 2)
        dd = _unit(d, 3)
        ab = make_reaction(a, b, mu)
        cd = make_reaction(c, dd, mu)
        seeds.append((ab, cd, make_reaction(ab.product, cd.product, mu)))
    return seeds


def _connected_pair(left: Reaction, right: Reaction) -> bool:
    left_species = set(left.reactants) | {left.product}
    right_species = set(right.reactants) | {right.product}
    return bool(left_species & right_species)


def _used_base_indices(reactions: Iterable[Reaction]) -> set[int]:
    used: set[int] = set()
    for reaction in reactions:
        for complex_vector in (*reaction.reactants, reaction.product):
            used.update(idx for idx, count in enumerate(complex_vector.counts) if count)
    return used
