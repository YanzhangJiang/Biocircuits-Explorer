#!/usr/bin/env python3

from __future__ import annotations

import argparse
import importlib.util
import json
import re
import sqlite3
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


SCRIPT_DIR = Path(__file__).resolve().parent
MIGRATE_PATH = SCRIPT_DIR / "migrate_atlas_sqlite_v2_lossless.py"
MIGRATE_SPEC = importlib.util.spec_from_file_location("migrate_atlas_sqlite_v2_lossless", MIGRATE_PATH)
if MIGRATE_SPEC is None or MIGRATE_SPEC.loader is None:
    raise RuntimeError(f"Unable to load migrate_atlas_sqlite_v2_lossless from {MIGRATE_PATH}")
migrate_atlas_sqlite_v2_lossless = importlib.util.module_from_spec(MIGRATE_SPEC)
sys.modules[MIGRATE_SPEC.name] = migrate_atlas_sqlite_v2_lossless
MIGRATE_SPEC.loader.exec_module(migrate_atlas_sqlite_v2_lossless)


DEGREE_RE = re.compile(r"(?:^|_)d(?P<degree>\d+)(?:_|$)")


NETWORK_COPY_TABLES = (
    "network_entries",
    "input_graph_slices",
)

SLICE_COPY_TABLES = (
    "behavior_slices",
    "regime_records",
    "transition_records",
    "family_buckets",
    "path_records",
)


@dataclass(frozen=True)
class DegreeSelection:
    degree: int
    network_ids: list[str]
    source_labels: list[str]


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _parse_degrees(raw: str | None) -> list[int] | None:
    if raw is None:
        return None
    values: list[int] = []
    for part in raw.split(","):
        text = part.strip()
        if not text:
            continue
        values.append(int(text))
    return sorted(set(values))


def _safe_suffix_label(raw: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", raw)


def _degree_from_source_label(source_label: str | None) -> int | None:
    if not source_label:
        return None
    match = DEGREE_RE.search(str(source_label))
    if match is None:
        return None
    return int(match.group("degree"))


def _connect_rw(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA temp_store = MEMORY")
    conn.execute("PRAGMA cache_size = -200000")
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = NORMAL")
    return conn


def _connect_ro(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def _schema_objects_from_source(src: sqlite3.Connection) -> tuple[list[str], list[str]]:
    rows = src.execute(
        """
        SELECT type, name, sql
        FROM sqlite_master
        WHERE sql IS NOT NULL
          AND name NOT LIKE 'sqlite_%'
          AND type IN ('table', 'index', 'view', 'trigger')
        ORDER BY
          CASE type
            WHEN 'table' THEN 1
            WHEN 'index' THEN 2
            WHEN 'view' THEN 3
            WHEN 'trigger' THEN 4
            ELSE 5
          END,
          name
        """
    ).fetchall()
    table_sql: list[str] = []
    post_sql: list[str] = []
    for row in rows:
        sql = str(row["sql"])
        if row["type"] == "table":
            table_sql.append(sql)
        else:
            post_sql.append(sql)
    return table_sql, post_sql


def _create_tables(dst: sqlite3.Connection, table_sql: list[str]) -> None:
    for sql in table_sql:
        dst.execute(sql)
    dst.commit()


def _create_post_objects(dst: sqlite3.Connection, post_sql: list[str]) -> None:
    for sql in post_sql:
        dst.execute(sql)
    dst.commit()


def _column_names(conn: sqlite3.Connection, table_name: str) -> list[str]:
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    return [str(row[1]) for row in rows]


def _copy_entire_table(dst: sqlite3.Connection, src_alias: str, table_name: str) -> int:
    columns = _column_names(dst, table_name)
    quoted_cols = ", ".join(columns)
    placeholders = ", ".join("?" for _ in columns)
    cursor = dst.execute(f"SELECT {quoted_cols} FROM {src_alias}.{table_name}")
    insert_sql = f"INSERT INTO {table_name} ({quoted_cols}) VALUES ({placeholders})"
    count = 0
    batch: list[tuple[Any, ...]] = []
    for row in cursor:
        batch.append(tuple(row))
        if len(batch) >= 5000:
            dst.executemany(insert_sql, batch)
            count += len(batch)
            batch.clear()
    if batch:
        dst.executemany(insert_sql, batch)
        count += len(batch)
    return count


def _copy_filtered_table_by_selected_ids(
    dst: sqlite3.Connection,
    src_alias: str,
    table_name: str,
    join_column: str,
    selected_table: str,
) -> int:
    columns = _column_names(dst, table_name)
    quoted_cols = ", ".join(f"src.{col}" for col in columns)
    dest_cols = ", ".join(columns)
    sql = f"""
        INSERT INTO {table_name} ({dest_cols})
        SELECT {quoted_cols}
        FROM {src_alias}.{table_name} AS src
        JOIN {selected_table} AS sel ON sel.id = src.{join_column}
    """
    before = dst.total_changes
    dst.execute(sql)
    return dst.total_changes - before


def _copy_filtered_atlas_manifests(dst: sqlite3.Connection, src_alias: str) -> int:
    before = dst.total_changes
    dst.execute(
        f"""
        INSERT INTO atlas_manifests
        SELECT src.*
        FROM {src_alias}.atlas_manifests AS src
        JOIN selected_source_labels AS ssl
          ON ssl.label = src.source_label
        """
    )
    return dst.total_changes - before


def _copy_filtered_merge_events(dst: sqlite3.Connection, src_alias: str) -> int:
    before = dst.total_changes
    dst.execute(
        f"""
        INSERT INTO merge_events
        SELECT src.*
        FROM {src_alias}.merge_events AS src
        LEFT JOIN selected_source_labels AS ssl
          ON ssl.label = src.source_label
        LEFT JOIN atlas_manifests AS am
          ON am.atlas_id = src.atlas_id
        WHERE ssl.label IS NOT NULL OR am.atlas_id IS NOT NULL
        """
    )
    return dst.total_changes - before


def _copy_filtered_duplicate_inputs(dst: sqlite3.Connection, src_alias: str) -> int:
    before = dst.total_changes
    dst.execute(
        f"""
        INSERT INTO duplicate_inputs
        SELECT src.*
        FROM {src_alias}.duplicate_inputs AS src
        LEFT JOIN selected_network_ids AS sn
          ON sn.id = src.duplicate_of_network_id
        LEFT JOIN selected_source_labels AS ssl
          ON ssl.label = src.source_label
        WHERE sn.id IS NOT NULL OR ssl.label IS NOT NULL
        """
    )
    return dst.total_changes - before


def _rewrite_metadata_for_split(
    dst: sqlite3.Connection,
    src_db: Path,
    degree: int,
    network_count: int,
    source_label_count: int,
) -> None:
    dst.execute("DELETE FROM library_state")
    dst.execute(
        """
        INSERT INTO atlas_metadata (key, value_text)
        VALUES (?, ?)
        ON CONFLICT(key) DO UPDATE SET value_text = excluded.value_text
        """,
        ("updated_at", _now_iso()),
    )
    metadata_updates = [
        ("split_source_db", str(src_db)),
        ("split_degree", str(degree)),
        ("split_network_count", str(network_count)),
        ("split_source_label_count", str(source_label_count)),
        ("split_generated_at", _now_iso()),
        ("split_strategy", "source_label_degree_partition"),
    ]
    dst.executemany(
        """
        INSERT INTO atlas_metadata (key, value_text)
        VALUES (?, ?)
        ON CONFLICT(key) DO UPDATE SET value_text = excluded.value_text
        """,
        metadata_updates,
    )


def _degree_selections(src: sqlite3.Connection, degrees_filter: list[int] | None) -> list[DegreeSelection]:
    degree_to_networks: dict[int, list[str]] = {}
    degree_to_labels: dict[int, set[str]] = {}
    for network_id, source_label in src.execute(
        "SELECT network_id, source_label FROM network_entries ORDER BY network_id"
    ):
        degree = _degree_from_source_label(source_label)
        if degree is None:
            continue
        if degrees_filter is not None and degree not in degrees_filter:
            continue
        degree_to_networks.setdefault(degree, []).append(str(network_id))
        labels = degree_to_labels.setdefault(degree, set())
        if source_label is not None:
            labels.add(str(source_label))
    return [
        DegreeSelection(
            degree=degree,
            network_ids=network_ids,
            source_labels=sorted(degree_to_labels.get(degree, set())),
        )
        for degree, network_ids in sorted(degree_to_networks.items())
        if network_ids
    ]


def _write_network_id_file(path: Path, network_ids: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        for network_id in network_ids:
            fh.write(network_id)
            fh.write("\n")


def split_raw_db(src_db: Path, dst_db: Path, selection: DegreeSelection) -> dict[str, Any]:
    if dst_db.exists():
        dst_db.unlink()
    dst_db.parent.mkdir(parents=True, exist_ok=True)

    src = _connect_ro(src_db)
    dst = _connect_rw(dst_db)
    try:
        table_sql, post_sql = _schema_objects_from_source(src)
        _create_tables(dst, table_sql)
        dst.execute(f"ATTACH DATABASE '{src_db}' AS src")
        dst.execute("CREATE TEMP TABLE selected_network_ids(id TEXT PRIMARY KEY)")
        dst.executemany(
            "INSERT INTO selected_network_ids(id) VALUES (?)",
            [(network_id,) for network_id in selection.network_ids],
        )
        dst.execute("CREATE TEMP TABLE selected_source_labels(label TEXT PRIMARY KEY)")
        dst.executemany(
            "INSERT INTO selected_source_labels(label) VALUES (?)",
            [(label,) for label in selection.source_labels],
        )
        dst.execute("CREATE TEMP TABLE selected_slice_ids(id TEXT PRIMARY KEY)")
        dst.execute(
            """
            INSERT INTO selected_slice_ids(id)
            SELECT slice_id
            FROM src.behavior_slices
            WHERE network_id IN (SELECT id FROM selected_network_ids)
            """
        )

        atlas_metadata_rows = _copy_entire_table(dst, "src", "atlas_metadata")
        _copy_filtered_atlas_manifests(dst, "src")
        _copy_filtered_merge_events(dst, "src")

        network_rows: dict[str, int] = {}
        for table_name in NETWORK_COPY_TABLES:
            network_rows[table_name] = _copy_filtered_table_by_selected_ids(
                dst, "src", table_name, "network_id", "selected_network_ids"
            )

        slice_rows: dict[str, int] = {}
        for table_name in SLICE_COPY_TABLES:
            slice_rows[table_name] = _copy_filtered_table_by_selected_ids(
                dst, "src", table_name, "slice_id", "selected_slice_ids"
            )

        duplicate_rows = _copy_filtered_duplicate_inputs(dst, "src")
        _rewrite_metadata_for_split(
            dst,
            src_db=src_db,
            degree=selection.degree,
            network_count=len(selection.network_ids),
            source_label_count=len(selection.source_labels),
        )
        _create_post_objects(dst, post_sql)
        dst.commit()
        dst.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        return {
            "degree": selection.degree,
            "raw_db": str(dst_db),
            "atlas_metadata_rows": atlas_metadata_rows,
            "network_count": len(selection.network_ids),
            "source_label_count": len(selection.source_labels),
            "table_counts": {
                **network_rows,
                **slice_rows,
                "duplicate_inputs": duplicate_rows,
                "atlas_manifests": dst.execute("SELECT COUNT(*) FROM atlas_manifests").fetchone()[0],
                "merge_events": dst.execute("SELECT COUNT(*) FROM merge_events").fetchone()[0],
                "library_state": dst.execute("SELECT COUNT(*) FROM library_state").fetchone()[0],
            },
        }
    finally:
        dst.close()
        src.close()


def build_v2_db(src_db: Path, dst_db: Path, selection: DegreeSelection, stats_path: Path) -> dict[str, Any]:
    dst_db.parent.mkdir(parents=True, exist_ok=True)
    stats = migrate_atlas_sqlite_v2_lossless.migrate(
        src_db=src_db,
        dst_db=dst_db,
        where_slice_like=None,
        limit_slices=None,
        network_ids=selection.network_ids,
    )
    stats_payload = {
        "degree": selection.degree,
        "source_db": str(src_db),
        "dst_db": str(dst_db),
        **stats,
    }
    stats_path.write_text(json.dumps(stats_payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    return stats_payload


def _default_out_dir(src_db: Path) -> Path:
    return src_db.with_suffix("").parent / f"{src_db.stem}_split_by_degree"


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Split an atlas raw sqlite into per-degree raw databases based on "
            "network_entries.source_label, and optionally regenerate per-degree v2 databases."
        )
    )
    parser.add_argument("--src-db", required=True, help="Source raw atlas sqlite path.")
    parser.add_argument(
        "--out-dir",
        default=None,
        help="Output directory. Defaults to <src-stem>_split_by_degree next to the source db.",
    )
    parser.add_argument(
        "--degrees",
        default=None,
        help="Comma-separated degree list to emit. Defaults to every degree detected from source_label.",
    )
    parser.add_argument(
        "--build-v2",
        action="store_true",
        help="Also build per-degree v2 sqlite outputs from the raw source using the same degree partition.",
    )
    parser.add_argument(
        "--network-id-file-dir",
        default=None,
        help="Optional directory to write per-degree newline-delimited network_id lists.",
    )
    return parser


def main() -> None:
    args = _build_parser().parse_args()
    src_db = Path(args.src_db).expanduser().resolve()
    out_dir = Path(args.out_dir).expanduser().resolve() if args.out_dir else _default_out_dir(src_db)
    network_id_file_dir = (
        Path(args.network_id_file_dir).expanduser().resolve()
        if args.network_id_file_dir
        else out_dir / "network_id_lists"
    )
    requested_degrees = _parse_degrees(args.degrees)

    if not src_db.exists():
        raise FileNotFoundError(src_db)

    src = _connect_ro(src_db)
    try:
        selections = _degree_selections(src, requested_degrees)
    finally:
        src.close()

    if not selections:
        raise SystemExit("No degree-tagged networks found in network_entries.source_label for the requested filter.")

    out_dir.mkdir(parents=True, exist_ok=True)
    summary: dict[str, Any] = {
        "source_db": str(src_db),
        "out_dir": str(out_dir),
        "build_v2": bool(args.build_v2),
        "degrees": [],
    }

    for selection in selections:
        degree_tag = f"d{selection.degree}"
        raw_db = out_dir / f"{src_db.stem}.{degree_tag}.sqlite"
        network_id_file = network_id_file_dir / f"{src_db.stem}.{degree_tag}.network_ids.txt"
        _write_network_id_file(network_id_file, selection.network_ids)

        degree_result: dict[str, Any] = {
            "degree": selection.degree,
            "network_id_file": str(network_id_file),
            "network_count": len(selection.network_ids),
            "source_label_count": len(selection.source_labels),
        }
        degree_result["raw"] = split_raw_db(src_db, raw_db, selection)

        if args.build_v2:
            v2_db = out_dir / f"{src_db.stem}.{degree_tag}.v2.sqlite"
            v2_stats = out_dir / f"{src_db.stem}.{degree_tag}.v2.stats.json"
            degree_result["v2"] = build_v2_db(src_db, v2_db, selection, v2_stats)

        summary["degrees"].append(degree_result)

    summary_path = out_dir / f"{src_db.stem}.split_by_degree.summary.json"
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
