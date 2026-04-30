#!/usr/bin/env python3

from __future__ import annotations

import argparse
import csv
import json
import math
import sqlite3
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any


FIELDS = [
    "network_pk",
    "network_id",
    "rendered_id",
    "d",
    "r",
    "assembly_depth",
    "program_richness_slice",
    "program_richness_path",
    "hill_q0_slice",
    "hill_q1_slice",
    "hill_q2_slice",
    "hill_q0_path",
    "hill_q1_path",
    "hill_q2_path",
    "mean_program_length_slice",
    "mean_program_length_path",
    "mean_total_variation_slice",
    "mean_total_variation_path",
    "upper_tail_program_complexity",
    "fragility_gap_placeholder",
]


def hill_diversity(weights: list[float], q: float) -> float:
    positive = [weight for weight in weights if weight > 0]
    if not positive:
        return 0.0
    total = sum(positive)
    p = [weight / total for weight in positive]
    if q == 0:
        return float(len(p))
    if q == 1:
        entropy = -sum(pi * math.log(pi) for pi in p if pi > 0)
        return math.exp(entropy)
    if q == 2:
        return 1.0 / sum(pi * pi for pi in p)
    return sum(pi**q for pi in p) ** (1.0 / (1.0 - q))


def weighted_mean(values: list[float], weights: list[float]) -> float:
    total = sum(weight for weight in weights if weight > 0)
    if total <= 0:
        return 0.0
    return sum(value * weight for value, weight in zip(values, weights) if weight > 0) / total


def weighted_quantile(values: list[float], weights: list[float], quantile: float) -> float:
    pairs = sorted((value, weight) for value, weight in zip(values, weights) if weight > 0)
    if not pairs:
        return 0.0
    total = sum(weight for _, weight in pairs)
    threshold = total * quantile
    acc = 0.0
    for value, weight in pairs:
        acc += weight
        if acc >= threshold:
            return value
    return pairs[-1][0]


def load_rows(conn: sqlite3.Connection) -> dict[str, list[sqlite3.Row]]:
    grouped: dict[str, list[sqlite3.Row]] = defaultdict(list)
    for row in conn.execute(
        """
        SELECT
            nps.np AS network_id,
            nps.pid,
            nps.slice_count,
            nps.path_count,
            ne.canonical_code,
            ne.base_species_count,
            ne.reaction_count,
            nf.d,
            nf.r,
            nf.assembly_depth,
            pf.c_len,
            pf.c_total_variation
        FROM network_program_support AS nps
        JOIN program_features AS pf ON pf.pid = nps.pid
        LEFT JOIN network_entries AS ne ON ne.network_id = nps.np
        LEFT JOIN network_features AS nf ON nf.network_id = nps.np
        ORDER BY nps.np, nps.pid
        """
    ):
        grouped[str(row["network_id"])].append(row)
    return grouped


def compute_bci(db_path: Path) -> list[dict[str, Any]]:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        grouped = load_rows(conn)
        output: list[dict[str, Any]] = []
        for network_id, rows in grouped.items():
            slice_weights = [float(row["slice_count"] or 0) for row in rows]
            path_weights = [float(row["path_count"] or 0) for row in rows]
            lengths = [float(row["c_len"] or 0.0) for row in rows]
            variation = [float(row["c_total_variation"] or 0.0) for row in rows]
            complexity = [length + tv for length, tv in zip(lengths, variation)]
            first = rows[0]
            d = first["d"] if first["d"] is not None else first["base_species_count"]
            r = first["r"] if first["r"] is not None else first["reaction_count"]
            output.append({
                "network_pk": network_id,
                "network_id": network_id,
                "rendered_id": first["canonical_code"] or network_id,
                "d": d,
                "r": r,
                "assembly_depth": first["assembly_depth"],
                "program_richness_slice": sum(1 for weight in slice_weights if weight > 0),
                "program_richness_path": sum(1 for weight in path_weights if weight > 0),
                "hill_q0_slice": hill_diversity(slice_weights, 0),
                "hill_q1_slice": hill_diversity(slice_weights, 1),
                "hill_q2_slice": hill_diversity(slice_weights, 2),
                "hill_q0_path": hill_diversity(path_weights, 0),
                "hill_q1_path": hill_diversity(path_weights, 1),
                "hill_q2_path": hill_diversity(path_weights, 2),
                "mean_program_length_slice": weighted_mean(lengths, slice_weights),
                "mean_program_length_path": weighted_mean(lengths, path_weights),
                "mean_total_variation_slice": weighted_mean(variation, slice_weights),
                "mean_total_variation_path": weighted_mean(variation, path_weights),
                "upper_tail_program_complexity": weighted_quantile(complexity, path_weights, 0.95),
                "fragility_gap_placeholder": "",
            })
        output.sort(key=lambda row: (row["d"] if row["d"] is not None else 0, row["r"] if row["r"] is not None else 0, row["network_id"]))
        return output
    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Compute first-pass behavior complexity indices from a behavior_aggregate SQLite DB.")
    parser.add_argument("db_path", type=Path)
    parser.add_argument("--output", type=Path, default=None, help="CSV output path. Defaults to stdout.")
    parser.add_argument("--json", action="store_true", help="Emit JSON instead of CSV.")
    args = parser.parse_args()

    rows = compute_bci(args.db_path)
    if args.json:
        payload = json.dumps(rows, indent=2, ensure_ascii=False)
        if args.output:
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(payload + "\n", encoding="utf-8")
        else:
            print(payload)
        return

    handle = args.output.open("w", newline="", encoding="utf-8") if args.output else sys.stdout
    try:
        writer = csv.DictWriter(handle, fieldnames=FIELDS)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    finally:
        if args.output:
            handle.close()


if __name__ == "__main__":
    main()
