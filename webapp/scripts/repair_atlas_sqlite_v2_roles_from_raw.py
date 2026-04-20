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


def _parse_regime_vertex_idx(regime_record_id: str) -> int:
    return int(str(regime_record_id).rsplit("::regime::", 1)[1])


def _configure_dst(dst: sqlite3.Connection) -> None:
    dst.execute("PRAGMA journal_mode=WAL")
    dst.execute("PRAGMA synchronous=NORMAL")
    dst.execute("PRAGMA temp_store=MEMORY")
    dst.execute("PRAGMA cache_size=-262144")
    dst.execute("PRAGMA mmap_size=30000000000")


def _load_slice_mapping(dst: sqlite3.Connection) -> dict[str, int]:
    mapping: dict[str, int] = {}
    for sp, slice_id in dst.execute("SELECT sp, slice_id FROM v_bs"):
        mapping[str(slice_id)] = int(sp)
    return mapping


def _iter_regime_batches(src: sqlite3.Connection, batch_rows: int) -> Iterable[list[tuple[str, str, str]]]:
    cur = src.execute(
        """
        SELECT slice_id, regime_record_id, role
        FROM regime_records
        WHERE role IN ('source_sink', 'branch_merge')
        """
    )
    batch: list[tuple[str, str, str]] = []
    for slice_id, regime_record_id, role in cur:
        batch.append((str(slice_id), str(regime_record_id), str(role)))
        if len(batch) >= batch_rows:
            yield batch
            batch = []
    if batch:
        yield batch


def _iter_transition_batches(
    src: sqlite3.Connection,
    batch_rows: int,
) -> Iterable[list[tuple[str, int, int, str | None, str | None]]]:
    cur = src.execute(
        """
        SELECT slice_id, from_vertex_idx, to_vertex_idx, from_role, to_role
        FROM transition_records
        WHERE from_role IN ('source_sink', 'branch_merge')
           OR to_role IN ('source_sink', 'branch_merge')
        """
    )
    batch: list[tuple[str, int, int, str | None, str | None]] = []
    for slice_id, from_vertex_idx, to_vertex_idx, from_role, to_role in cur:
        batch.append(
            (
                str(slice_id),
                int(from_vertex_idx),
                int(to_vertex_idx),
                None if from_role is None else str(from_role),
                None if to_role is None else str(to_role),
            )
        )
        if len(batch) >= batch_rows:
            yield batch
            batch = []
    if batch:
        yield batch


def _repair_regime_codes(
    src: sqlite3.Connection,
    dst: sqlite3.Connection,
    sp_by_slice: dict[str, int],
    batch_rows: int,
    sp_query_chunk: int,
) -> dict[str, int]:
    stats = {
        "scanned_rows": 0,
        "updated_rows": 0,
        "missing_slice_rows": 0,
        "missing_target_rows": 0,
        "batches": 0,
    }
    started = time.time()
    for raw_batch in _iter_regime_batches(src, batch_rows):
        stats["batches"] += 1
        stats["scanned_rows"] += len(raw_batch)
        keyed_rows: dict[int, list[tuple[int, int]]] = {}
        for slice_id, regime_record_id, role in raw_batch:
            sp = sp_by_slice.get(slice_id)
            if sp is None:
                stats["missing_slice_rows"] += 1
                continue
            keyed_rows.setdefault(sp, []).append((_parse_regime_vertex_idx(regime_record_id), TARGET_ROLE_CODES[role]))

        updated_this_batch = 0
        missing_this_batch = 0
        with dst:
            for sp_chunk in _batched(list(keyed_rows.keys()), sp_query_chunk):
                if not sp_chunk:
                    continue
                placeholders = ",".join("?" for _ in sp_chunk)
                existing = {
                    (int(sp), int(vertex_idx)): int(rp)
                    for rp, sp, vertex_idx in dst.execute(
                        f"SELECT rp, sp, v FROM rr WHERE rc = 99 AND sp IN ({placeholders})",
                        sp_chunk,
                    )
                }
                updates: list[tuple[int, int]] = []
                for sp in sp_chunk:
                    for vertex_idx, role_code in keyed_rows[sp]:
                        rp = existing.get((sp, vertex_idx))
                        if rp is None:
                            missing_this_batch += 1
                            continue
                        updates.append((role_code, rp))
                if updates:
                    dst.executemany("UPDATE rr SET rc = ? WHERE rp = ?", updates)
                    updated_this_batch += len(updates)

        stats["updated_rows"] += updated_this_batch
        stats["missing_target_rows"] += missing_this_batch
        elapsed = time.time() - started
        print(
            f"[rr] batch={stats['batches']} scanned={stats['scanned_rows']} updated={stats['updated_rows']} "
            f"missing={stats['missing_target_rows']} elapsed_sec={elapsed:.1f}",
            file=sys.stderr,
            flush=True,
        )
    return stats


def _repair_transition_codes(
    src: sqlite3.Connection,
    dst: sqlite3.Connection,
    sp_by_slice: dict[str, int],
    batch_rows: int,
    sp_query_chunk: int,
) -> dict[str, int]:
    stats = {
        "scanned_rows": 0,
        "updated_rows": 0,
        "missing_slice_rows": 0,
        "missing_target_rows": 0,
        "batches": 0,
    }
    started = time.time()
    for raw_batch in _iter_transition_batches(src, batch_rows):
        stats["batches"] += 1
        stats["scanned_rows"] += len(raw_batch)
        keyed_rows: dict[int, dict[tuple[int, int], tuple[int | None, int | None]]] = {}
        for slice_id, from_vertex_idx, to_vertex_idx, from_role, to_role in raw_batch:
            sp = sp_by_slice.get(slice_id)
            if sp is None:
                stats["missing_slice_rows"] += 1
                continue
            keyed_rows.setdefault(sp, {})[(from_vertex_idx, to_vertex_idx)] = (
                TARGET_ROLE_CODES.get(from_role),
                TARGET_ROLE_CODES.get(to_role),
            )

        updated_this_batch = 0
        missing_this_batch = 0
        with dst:
            for sp_chunk in _batched(list(keyed_rows.keys()), sp_query_chunk):
                if not sp_chunk:
                    continue
                placeholders = ",".join("?" for _ in sp_chunk)
                existing = {
                    (int(sp), int(from_vertex_idx), int(to_vertex_idx)): (int(tp), fr, tor)
                    for tp, sp, from_vertex_idx, to_vertex_idx, fr, tor in dst.execute(
                        f"SELECT tp, sp, fv, tv, fr, tor FROM tr WHERE (fr = 99 OR tor = 99) AND sp IN ({placeholders})",
                        sp_chunk,
                    )
                }
                updates: list[tuple[int | None, int | None, int]] = []
                for sp in sp_chunk:
                    for (from_vertex_idx, to_vertex_idx), (new_fr, new_tor) in keyed_rows[sp].items():
                        row = existing.get((sp, from_vertex_idx, to_vertex_idx))
                        if row is None:
                            missing_this_batch += 1
                            continue
                        tp, cur_fr, cur_tor = row
                        final_fr = new_fr if new_fr is not None else cur_fr
                        final_tor = new_tor if new_tor is not None else cur_tor
                        if final_fr == cur_fr and final_tor == cur_tor:
                            continue
                        updates.append((final_fr, final_tor, tp))
                if updates:
                    dst.executemany("UPDATE tr SET fr = ?, tor = ? WHERE tp = ?", updates)
                    updated_this_batch += len(updates)

        stats["updated_rows"] += updated_this_batch
        stats["missing_target_rows"] += missing_this_batch
        elapsed = time.time() - started
        print(
            f"[tr] batch={stats['batches']} scanned={stats['scanned_rows']} updated={stats['updated_rows']} "
            f"missing={stats['missing_target_rows']} elapsed_sec={elapsed:.1f}",
            file=sys.stderr,
            flush=True,
        )
    return stats


def repair_v2_roles(
    src_db: Path,
    dst_db: Path,
    batch_rows: int,
    sp_query_chunk: int,
) -> dict[str, object]:
    src = sqlite3.connect(f"file:{src_db}?mode=ro", uri=True)
    dst = sqlite3.connect(dst_db)
    try:
        src.row_factory = sqlite3.Row
        dst.row_factory = sqlite3.Row
        _configure_dst(dst)
        sp_by_slice = _load_slice_mapping(dst)

        before = {
            "rr_rc_99": int(dst.execute("SELECT COUNT(*) FROM rr WHERE rc = 99").fetchone()[0]),
            "tr_fr_99": int(dst.execute("SELECT COUNT(*) FROM tr WHERE fr = 99").fetchone()[0]),
            "tr_tor_99": int(dst.execute("SELECT COUNT(*) FROM tr WHERE tor = 99").fetchone()[0]),
        }

        rr_stats = _repair_regime_codes(src, dst, sp_by_slice, batch_rows, sp_query_chunk)
        tr_stats = _repair_transition_codes(src, dst, sp_by_slice, batch_rows, sp_query_chunk)

        checkpoint_error = None
        with dst:
            dst.execute(
                "INSERT OR REPLACE INTO meta (k, v) VALUES (?, ?)",
                ("role_code_repair_from_raw", json.dumps({"rr": rr_stats, "tr": tr_stats}, separators=(",", ":"))),
            )
            dst.execute("INSERT OR REPLACE INTO meta (k, v) VALUES (?, ?)", ("schema", "atlas_sqlite_v2_lossless_alpha"))
        try:
            dst.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        except sqlite3.OperationalError as exc:
            checkpoint_error = str(exc)

        after = {
            "rr_rc_99": int(dst.execute("SELECT COUNT(*) FROM rr WHERE rc = 99").fetchone()[0]),
            "tr_fr_99": int(dst.execute("SELECT COUNT(*) FROM tr WHERE fr = 99").fetchone()[0]),
            "tr_tor_99": int(dst.execute("SELECT COUNT(*) FROM tr WHERE tor = 99").fetchone()[0]),
        }
        return {
            "src_db": str(src_db),
            "dst_db": str(dst_db),
            "batch_rows": batch_rows,
            "sp_query_chunk": sp_query_chunk,
            "slice_count": len(sp_by_slice),
            "before": before,
            "after": after,
            "rr": rr_stats,
            "tr": tr_stats,
            "checkpoint_error": checkpoint_error,
        }
    finally:
        src.close()
        dst.close()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Repair existing atlas v2 role codes in-place from the raw source sqlite without rebuilding the full v2 database."
    )
    parser.add_argument("--src-db", required=True)
    parser.add_argument("--dst-db", required=True)
    parser.add_argument("--batch-rows", type=int, default=250000)
    parser.add_argument("--sp-query-chunk", type=int, default=1000)
    return parser


def main() -> None:
    args = _build_parser().parse_args()
    report = repair_v2_roles(
        Path(args.src_db),
        Path(args.dst_db),
        batch_rows=int(args.batch_rows),
        sp_query_chunk=int(args.sp_query_chunk),
    )
    print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
