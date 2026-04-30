#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import math
import re
import sqlite3
from pathlib import Path
from typing import Any


MAGIC = b"RPB1"


def _read_varuint(data: bytes, pos: int) -> tuple[int, int]:
    shift = 0
    value = 0
    while True:
        if pos >= len(data):
            raise ValueError("unexpected end of varuint")
        byte = data[pos]
        pos += 1
        value |= (byte & 0x7F) << shift
        if byte & 0x80 == 0:
            return value, pos
        shift += 7
        if shift > 63:
            raise ValueError("varuint is too large")


def _read_zigzag(data: bytes, pos: int) -> tuple[int, int]:
    encoded, pos = _read_varuint(data, pos)
    if encoded & 1:
        return -((encoded + 1) >> 1), pos
    return encoded >> 1, pos


def _read_text(data: bytes, pos: int) -> tuple[str, int]:
    length, pos = _read_varuint(data, pos)
    stop = pos + length
    if stop > len(data):
        raise ValueError("unexpected end of text atom")
    return data[pos:stop].decode("utf-8"), stop


def _token_from_scaled(value: int, scale: int) -> str:
    if value == 0:
        return "0"
    sign = "+" if value > 0 else "-"
    value = abs(value)
    whole, frac = divmod(value, scale)
    if frac == 0:
        return f"{sign}{whole}"
    width = max(1, len(str(scale - 1)))
    frac_text = str(frac).zfill(width).rstrip("0")
    return f"{sign}{whole}.{frac_text}"


def decode_program_blob(blob: bytes, scale: int = 1000) -> list[list[str]]:
    data = bytes(blob)
    if not data.startswith(MAGIC):
        raise ValueError("bad magic")
    pos = len(MAGIC)
    version, pos = _read_varuint(data, pos)
    if version != 1:
        raise ValueError(f"unsupported codec version {version}")
    length, pos = _read_varuint(data, pos)
    dim, pos = _read_varuint(data, pos)
    out: list[list[str]] = []
    for _ in range(length):
        state: list[str] = []
        for _ in range(dim):
            if pos >= len(data):
                raise ValueError("unexpected end of atom")
            tag = data[pos]
            pos += 1
            if tag == 0x00:
                scaled, pos = _read_zigzag(data, pos)
                state.append(_token_from_scaled(scaled, scale))
            elif tag == 0x01:
                state.append("NaN")
            elif tag == 0x02:
                state.append("+Inf")
            elif tag == 0x03:
                state.append("-Inf")
            elif tag == 0x04:
                state.append("missing")
            elif tag == 0x05:
                text, pos = _read_text(data, pos)
                state.append(text)
            else:
                raise ValueError(f"unknown atom tag {tag}")
        out.append(state)
    if pos != len(data):
        raise ValueError("trailing bytes")
    return out


def _normalize_atom(raw: str, scale: int = 1000) -> str:
    text = raw.strip()
    low = text.lower()
    if low in {"nan", "+nan", "-nan"}:
        return "NaN"
    if low in {"inf", "+inf", "infinity", "+infinity"}:
        return "+Inf"
    if low in {"-inf", "-infinity"}:
        return "-Inf"
    if low in {"missing", "undef", "undefined", "nothing"}:
        return "missing"
    try:
        value = float(text)
    except ValueError:
        return text
    if math.isnan(value):
        return "NaN"
    if math.isinf(value):
        return "+Inf" if value > 0 else "-Inf"
    return _token_from_scaled(round(value * scale), scale)


def _split_state(raw: str) -> list[str]:
    text = raw.strip()
    if (text.startswith("[") and text.endswith("]")) or (text.startswith("(") and text.endswith(")")):
        inner = text[1:-1].strip()
        return [] if not inner else [part.strip() for part in inner.split(",")]
    return [text]


def normalize_label(label: str, scale: int = 1000) -> str:
    states = []
    for raw_state in re.split(r"\s*(?:->|→|=>)\s*", label.strip()):
        if not raw_state:
            continue
        atoms = [_normalize_atom(atom, scale) for atom in _split_state(raw_state)]
        states.append(atoms[0] if len(atoms) == 1 else "[" + ",".join(atoms) + "]")
    return " -> ".join(states)


def label_from_profile(profile: list[list[str]]) -> str:
    states = [state[0] if len(state) == 1 else "[" + ",".join(state) + "]" for state in profile]
    return " -> ".join(states)


def _table_count(conn: sqlite3.Connection, table: str) -> int:
    return int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])


def verify(db_path: Path, sample_limit: int = 20) -> dict[str, Any]:
    failures: list[dict[str, Any]] = []
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]
        if integrity != "ok":
            failures.append({"kind": "integrity_check", "value": integrity})

        metadata = {row["key"]: row["value_text"] for row in conn.execute("SELECT key, value_text FROM atlas_metadata")}
        persist_mode = metadata.get("persist_mode", "")
        if persist_mode != "behavior_aggregate":
            failures.append({"kind": "persist_mode", "value": persist_mode})

        required_tables = [
            "classifier_configs",
            "behavior_programs",
            "program_features",
            "slice_program_support",
            "network_program_support",
            "witness_paths",
        ]
        existing_tables = {
            row["name"]
            for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
        }
        for table in required_tables:
            if table not in existing_tables:
                failures.append({"kind": "missing_table", "table": table})

        for row in conn.execute(
            """
            SELECT bs.slice_id, bs.included_paths, COALESCE(SUM(sps.pc), 0) AS support_sum
            FROM behavior_slices AS bs
            LEFT JOIN slice_program_support AS sps ON sps.sp = bs.slice_id
            WHERE bs.analysis_status = 'ok'
            GROUP BY bs.slice_id, bs.included_paths
            """
        ):
            included = row["included_paths"]
            if included is not None and int(included) != int(row["support_sum"]):
                failures.append({
                    "kind": "slice_support_sum",
                    "slice_id": row["slice_id"],
                    "included_paths": int(included),
                    "support_sum": int(row["support_sum"]),
                })
                if len(failures) >= sample_limit:
                    break

        for row in conn.execute(
            """
            SELECT bs.network_id, sps.pid, SUM(sps.pc) AS expected_path_count,
                   SUM(sps.slice_incidence) AS expected_slice_count,
                   nps.path_count, nps.slice_count
            FROM slice_program_support AS sps
            JOIN behavior_slices AS bs ON bs.slice_id = sps.sp
            LEFT JOIN network_program_support AS nps
              ON nps.np = bs.network_id AND nps.pid = sps.pid
            GROUP BY bs.network_id, sps.pid
            """
        ):
            if row["path_count"] is None or int(row["path_count"]) != int(row["expected_path_count"]):
                failures.append({
                    "kind": "network_path_count",
                    "network_id": row["network_id"],
                    "pid": int(row["pid"]),
                    "expected": int(row["expected_path_count"]),
                    "actual": None if row["path_count"] is None else int(row["path_count"]),
                })
            if row["slice_count"] is None or int(row["slice_count"]) != int(row["expected_slice_count"]):
                failures.append({
                    "kind": "network_slice_count",
                    "network_id": row["network_id"],
                    "pid": int(row["pid"]),
                    "expected": int(row["expected_slice_count"]),
                    "actual": None if row["slice_count"] is None else int(row["slice_count"]),
                })
            if len(failures) >= sample_limit:
                break

        for row in conn.execute(
            """
            SELECT bp.pid, bp.blob, bp.exact_label, cc.config_json
            FROM behavior_programs AS bp
            JOIN classifier_configs AS cc ON cc.cfg = bp.cfg
            LIMIT ?
            """,
            (max(1, sample_limit),),
        ):
            cfg = json.loads(row["config_json"])
            scale = int(cfg.get("ro_quantization_scale", 1000))
            decoded_label = label_from_profile(decode_program_blob(row["blob"], scale))
            expected_label = normalize_label(row["exact_label"] or "", scale)
            if decoded_label != expected_label:
                failures.append({
                    "kind": "program_blob_roundtrip",
                    "pid": int(row["pid"]),
                    "decoded_label": decoded_label,
                    "exact_label": row["exact_label"],
                    "normalized_exact_label": expected_label,
                })

        stale_counts = {
            "path_records": _table_count(conn, "path_records"),
            "path_only_records": _table_count(conn, "path_only_records"),
            "family_buckets": _table_count(conn, "family_buckets"),
        }
        if persist_mode == "behavior_aggregate":
            for table, count in stale_counts.items():
                if count:
                    failures.append({"kind": "stale_legacy_rows", "table": table, "count": count})

        return {
            "db_path": str(db_path),
            "ok": not failures,
            "failure_count": len(failures),
            "failures": failures[:sample_limit],
            "counts": {
                table: _table_count(conn, table)
                for table in [
                    "network_entries",
                    "behavior_slices",
                    "classifier_configs",
                    "behavior_programs",
                    "slice_program_support",
                    "network_program_support",
                    "witness_paths",
                    "path_records",
                    "path_only_records",
                    "family_buckets",
                ]
            },
            "metadata": metadata,
        }
    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Verify B0 behavior_aggregate SQLite invariants.")
    parser.add_argument("db_path", type=Path)
    parser.add_argument("--sample-limit", type=int, default=20)
    args = parser.parse_args()
    summary = verify(args.db_path, sample_limit=args.sample_limit)
    print(json.dumps(summary, indent=2, ensure_ascii=False))
    raise SystemExit(0 if summary["ok"] else 1)


if __name__ == "__main__":
    main()
