"""Complex and reaction generation for `periodic_d_mu_v0`."""

from __future__ import annotations

from dataclasses import dataclass
from itertools import combinations_with_replacement
from typing import Iterable, Iterator

BASE_SYMBOLS = tuple("ABCDEFGHIJKLMNOPQRSTUVWXYZ")


@dataclass(frozen=True, order=True)
class ComplexVector:
    counts: tuple[int, ...]

    def __post_init__(self) -> None:
        if not self.counts:
            raise ValueError("complex vector must have at least one coordinate")
        if any(c < 0 for c in self.counts):
            raise ValueError("complex vector counts must be nonnegative")
        if sum(self.counts) <= 0:
            raise ValueError("complex vector size must be positive")

    @property
    def d(self) -> int:
        return len(self.counts)

    @property
    def size(self) -> int:
        return sum(self.counts)

    def permute(self, permutation: tuple[int, ...]) -> "ComplexVector":
        if len(permutation) != self.d:
            raise ValueError("permutation length must match complex dimension")
        return ComplexVector(tuple(self.counts[i] for i in permutation))

    def symbol(self, base_symbols: tuple[str, ...] = BASE_SYMBOLS) -> str:
        if self.d > len(base_symbols):
            raise ValueError("not enough base symbols for complex rendering")
        parts: list[str] = []
        for idx, count in enumerate(self.counts):
            parts.extend([base_symbols[idx]] * count)
        return "".join(parts)


@dataclass(frozen=True, order=True)
class Reaction:
    reactants: tuple[ComplexVector, ComplexVector]
    product: ComplexVector

    def __post_init__(self) -> None:
        if len(self.reactants) != 2:
            raise ValueError("binding reactions require exactly two reactants")
        left = tuple(sorted(self.reactants, key=lambda complex_vector: complex_vector.symbol()))
        object.__setattr__(self, "reactants", left)
        summed = tuple(a + b for a, b in zip(left[0].counts, left[1].counts))
        if summed != self.product.counts:
            raise ValueError("reaction product must equal the reactant sum")

    @property
    def d(self) -> int:
        return self.product.d

    @property
    def product_size(self) -> int:
        return self.product.size

    def permute(self, permutation: tuple[int, ...]) -> "Reaction":
        return Reaction(
            tuple(reactant.permute(permutation) for reactant in self.reactants),
            self.product.permute(permutation),
        )

    def canonical_string(self) -> str:
        return (
            f"{self.reactants[0].symbol()} + "
            f"{self.reactants[1].symbol()} <-> {self.product.symbol()}"
        )


def _compositions(total: int, parts: int) -> Iterator[tuple[int, ...]]:
    if parts == 1:
        yield (total,)
        return
    for first in range(total + 1):
        for rest in _compositions(total - first, parts - 1):
            yield (first, *rest)


def complex_space(d: int, mu: int) -> list[ComplexVector]:
    if d <= 0:
        raise ValueError("d must be positive")
    if mu <= 0:
        raise ValueError("mu must be positive")
    complexes: list[ComplexVector] = []
    for total in range(1, mu + 1):
        complexes.extend(ComplexVector(counts) for counts in _compositions(total, d))
    return sorted(complexes, key=lambda c: (c.size, c.counts))


def make_reaction(a: ComplexVector, b: ComplexVector, mu: int) -> Reaction:
    if a.d != b.d:
        raise ValueError("reactants must have the same dimension")
    product_counts = tuple(x + y for x, y in zip(a.counts, b.counts))
    product = ComplexVector(product_counts)
    if product.size > mu:
        raise ValueError("reaction product exceeds mu")
    return Reaction((a, b), product)


def allowed_binding_reactions(d: int, mu: int) -> list[Reaction]:
    complexes = complex_space(d, mu)
    reactions = set()
    for a, b in combinations_with_replacement(complexes, 2):
        if a.size + b.size <= mu:
            reactions.add(make_reaction(a, b, mu))
    return sorted(reactions, key=lambda reaction: (reaction.product_size, reaction.canonical_string()))


def reaction_strings(reactions: Iterable[Reaction]) -> list[str]:
    return [reaction.canonical_string() for reaction in sorted(reactions)]
