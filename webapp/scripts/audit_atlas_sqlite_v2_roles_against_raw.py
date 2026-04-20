#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
import time
from pathlib import Path
from typing import Iterable


TARGET_ROLE_CODES = {
    "source_sink": 6,
    "branch_merge": 7,
}


def _batched(values: list[int], size: int) -> Iterable[list[int]]:
    for idx in range(0, len(values), size):
        yield values[idx:idx + size]


def _load_slice_mapping(dst: sqlite3.Connection) -> dict[str, int]:
    mapping: dict[str, int] = {}
    for sp, slice_id in dst.execute("SELECT sp, slice_id FROM v_bs"):
        mapping[str(slice_id)] = int(sp)
    return mapping


def _parse_regime_vertex_idx(regime_record_id: str) -> int:
    return int(str(regime_record_id).rsplit("::regime::", 1)[1])


def _iter_regime_batches(src: sqlite3.Connection, batch_rows: int) -> Iterable[list[tuple[str, int, int]]]:
    cur = src.execute(
        """
        SELECT slice_id, regime_record_id, role
        FROM regime_records
        WHERE role IN ('source_sink', 'branch_merge')
        """
    )
    batch: list[tuple[str, int, int]] = []
    for slice_id, regime_record_id, role in cur:
        batch.append((str(slice_id), _parse_regime_vertex_idx(str(regime_record_id)), TARGET_ROLE_CODES[str(role)]))
        if len(batch) >= batch_rows:
            yield batch
            batch = []
    if batch:
        yield batch


def _iter_transition_batches(src: sqlite3.Connection, batch_rows: int) -> Iterable[list[tuple[str, int, int, int | None, int | None]]]:
    cur = src.execute(
        """
        SELECT slice_id, from_vertex_idx, to_vertex_idx, from_role, to_role
        FROM transition_records
        WHERE from_role IN ('source_sink', 'branch_merge')
           OR to_role IN ('source_sink', 'branch_merge')
        """
    )
    batch: list[tuple[str, int, int, int | None, int | None]] = []
    for slice_id, from_vertex_idx, to_vertex_idx, from_role, to_role in cur:
        batch.append(
            (
                str(slice_id),
                int(from_vertex_idx),
                int(to_vertex_idx),
                TARGET_ROLE_CODES.get(None if from_role is None else str(from_role)),
                TARGET_ROLE_CODES.get(None if to_role is None else str(to_role)),
            )
        )
        if len(batch) >= batch_rows:
            yield batch
            batch = []
    if batch:
        yield batch


def audit_roles(src_db: Path, dst_db: Path, batch_rows: int, sp_query_chunk: int) -> dict[str, object]:
    src = sqlite3.connect(f"file:{src_db}?mode=ro", uri=True)
    dst = sqlite3.connect(f"file:{dst_db}?mode=ro", uri=True)
    try:
        src.row_factory = sqlite3.Row
        dst.row_factory = sqlite3.Row
        sp_by_slice = _load_slice_mapping(dst)

        report: dict[str, object] = {
            "src_db": str(src_db),
            "dst_db": str(dst_db),
            "batch_rows": batch_rows,
            "sp_query_chunk": sp_query_chunk,
            "status": "passed",
            "regime": {
                "scanned_rows": 0,
                "missing_slice_rows": 0,
                "missing_target_rows": 0,
                "mismatch_rows": 0,
                "sample_mismatches": [],
            },
            "transition": {
                "scanned_rows": 0,
                "missing_slice_rows": 0,
                "missing_target_rows": 0,
                "mismatch_rows": 0,
                "sample_mismatches": [],
            },
        }

        started = time.time()

        for raw_batch in _iter_regime_batches(src, batch_rows):
            regime = report["regime"]
            assert isinstance(regime, dict)
            regime["scanned_rows"] += len(raw_batch)
            keyed_rows: dict[int, dict[int, int]] = {}
            for slice_id, vertex_idx, expected_code in raw_batch:
                sp = sp_by_slice.get(slice_id)
                if sp is None:
                    regime["missing_slice_rows"] += 1
                    continue
                keyed_rows.setdefault(sp, {})[vertex_idx] = expected_code
            for sp_chunk in _batched(list(keyed_rows.keys()), sp_query_chunk):
                if not sp_chunk:
                    continue
                placeholders = ",".join("?" for _ in sp_chunk)
                existing = {
                    (int(sp), int(v)): int(rc)
                    for sp, v, rc in dst.execute(
                        f"SELECT sp, v, rc FROM rr WHERE sp IN ({placeholders})",
                        sp_chunk,
                    )
                }
                for sp in sp_chunk:
                    for vertex_idx, expected_code in keyed_rows[sp].items():
                        actual_code = existing.get((sp, vertex_idx))
                        if actual_code is None:
                            regime["missing_target_rows"] += 1
                            continue
                        if actual_code != expected_code:
                            regime["mismatch_rows"] += 1
                            samples = regime["sample_mismatches"]
                            assert isinstance(samples, list)
                            if len(samples) < 20:
                                samples.append(
                                    {
                                        "sp": sp,
                                        "vertex_idx": vertex_idx,
                                        "expected_code": expected_code,
                                        "actual_code": actual_code,
                                    }
                                )
            elapsed = time.time() - started
            print(
                f"[audit rr] scanned={regime['scanned_rows']} missing_target={regime['missing_target_rows']} "
                f"mismatch={regime['mismatch_rows']} elapsed_sec={elapsed:.1f}",
                file=sys.stderr,
                flush=True,
            )

        for raw_batch in _iter_transition_batches(src, batch_rows):
            transition = report["transition"]
            assert isinstance(transition, dict)
            transition["scanned_rows"] += len(raw_batch)
            keyed_rows: dict[int, dict[tuple[int, int], tuple[int | None, int | None]]] = {}
            for slice_id, from_vertex_idx, to_vertex_idx, expected_fr, expected_tor in raw_batch:
                sp = sp_by_slice.get(slice_id)
                if sp is None:
                    transition["missing_slice_rows"] += 1
                    continue
                keyed_rows.setdefault(sp, {})[(from_vertex_idx, to_vertex_idx)] = (expected_fr, expected_tor)
            for sp_chunk in _batched(list(keyed_rows.keys()), sp_query_chunk):
                if not sp_chunk:
                    continue
                placeholders = ",".join("?" for _ in sp_chunk)
                existing = {
                    (int(sp), int(fv), int(tv)): (fr, tor)
                    for sp, fv, tv, fr, tor in dst.execute(
                        f"SELECT sp, fv, tv, fr, tor FROM tr WHERE sp IN ({placeholders})",
                        sp_chunk,
                    )
                }
                for sp in sp_chunk:
                    for (from_vertex_idx, to_vertex_idx), (expected_fr, expected_tor) in keyed_rows[sp].items():
                        actual = existing.get((sp, from_vertex_idx, to_vertex_idx))
                        if actual is None:
                            transition["missing_target_rows"] += 1
                            continue
                        actual_fr, actual_tor = actual
                        if ((expected_fr is not None and actual_fr != expected_fr) or
                            (expected_tor is not None and actual_tor != expected_tor)):
                            transition["mismatch_rows"] += 1
                            samples = transition["sample_mismatches"]
                            assert isinstance(samples, list)
                            if len(samples) < 20:
                                samples.append(
                                    {
                                        "sp": sp,
                                        "from_vertex_idx": from_vertex_idx,
                                        "to_vertex_idx": to_vertex_idx,
                                        "expected_fr": expected_fr,
                                        "actual_fr": actual_fr,
                                        "expected_tor": expected_tor,
                                        "actual_tor": actual_tor,
                                    }
                                )
            elapsed = time.time() - started
            print(
                f"[audit tr] scanned={transition['scanned_rows']} missing_target={transition['missing_target_rows']} "
                f"mismatch={transition['mismatch_rows']} elapsed_sec={elapsed:.1f}",
                file=sys.stderr,
                flush=True,
            )

        regime = report["regime"]
        transition = report["transition"]
        assert isinstance(regime, dict)
        assert isinstance(transition, dict)
        if any(
            int(section[key]) > 0
            for section in (regime, transition)
            for key in ("missing_slice_rows", "missing_target_rows", "mismatch_rows")
        ):
            report["status"] = "failed"
        return report
    finally:
        src.close()
        dst.close()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Audit all raw compound-role rows against an existing v2 sqlite to verify role codes exactly, not by sampling."
    )
    parser.add_argument("--src-db", required=True)
    parser.add_argument("--dst-db", required=True)
    parser.add_argument("--batch-rows", type=int, default=250000)
    parser.add_argument("--sp-query-chunk", type=int, default=1000)
    return parser


def main() -> None:
    args = _build_parser().parse_args()
    report = audit_roles(
        Path(args.src_db),
        Path(args.dst_db),
        batch_rows=int(args.batch_rows),
        sp_query_chunk=int(args.sp_query_chunk),
    )
    print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
    if report["status"] != "passed":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
