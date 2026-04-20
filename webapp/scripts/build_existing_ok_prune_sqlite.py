#!/usr/bin/env python3

from __future__ import annotations

import argparse
import os
import sqlite3
from pathlib import Path


SCHEMA_STATEMENTS = [
    "PRAGMA journal_mode = DELETE",
    "PRAGMA synchronous = NORMAL",
    "CREATE TABLE IF NOT EXISTS atlas_metadata (key TEXT PRIMARY KEY, value_text TEXT NOT NULL)",
    """
    CREATE TABLE IF NOT EXISTS library_state (
        snapshot_name TEXT PRIMARY KEY,
        updated_at TEXT NOT NULL,
        summary_json TEXT NOT NULL,
        library_json TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS atlas_manifests (
        atlas_id TEXT PRIMARY KEY,
        source_label TEXT,
        imported_at TEXT,
        generated_at TEXT,
        behavior_slice_count INTEGER,
        manifest_json TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS merge_events (
        event_id INTEGER PRIMARY KEY AUTOINCREMENT,
        merged_at TEXT,
        status TEXT,
        atlas_id TEXT,
        source_label TEXT,
        event_json TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS network_entries (
        network_id TEXT PRIMARY KEY,
        canonical_code TEXT,
        analysis_status TEXT,
        base_species_count INTEGER,
        reaction_count INTEGER,
        total_species_count INTEGER,
        max_support INTEGER,
        support_mass INTEGER,
        source_label TEXT,
        source_kind TEXT,
        motif_union_json TEXT,
        exact_union_json TEXT,
        slice_ids_json TEXT,
        record_json TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS input_graph_slices (
        graph_slice_id TEXT PRIMARY KEY,
        network_id TEXT,
        input_symbol TEXT,
        change_signature TEXT,
        vertex_count INTEGER,
        edge_count INTEGER,
        path_count INTEGER,
        record_json TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS behavior_slices (
        slice_id TEXT PRIMARY KEY,
        network_id TEXT,
        graph_slice_id TEXT,
        input_symbol TEXT,
        change_signature TEXT,
        output_symbol TEXT,
        analysis_status TEXT,
        path_scope TEXT,
        min_volume_mean REAL,
        total_paths INTEGER,
        feasible_paths INTEGER,
        included_paths INTEGER,
        excluded_paths INTEGER,
        motif_union_json TEXT,
        exact_union_json TEXT,
        classifier_config_json TEXT,
        record_json TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS regime_records (
        regime_record_id TEXT PRIMARY KEY,
        slice_id TEXT,
        graph_slice_id TEXT,
        network_id TEXT,
        input_symbol TEXT,
        change_signature TEXT,
        output_symbol TEXT,
        vertex_idx INTEGER,
        role TEXT,
        singular INTEGER,
        nullity INTEGER,
        asymptotic INTEGER,
        output_order_token TEXT,
        record_json TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS transition_records (
        transition_record_id TEXT PRIMARY KEY,
        slice_id TEXT,
        graph_slice_id TEXT,
        input_symbol TEXT,
        change_signature TEXT,
        output_symbol TEXT,
        from_vertex_idx INTEGER,
        to_vertex_idx INTEGER,
        from_role TEXT,
        to_role TEXT,
        from_output_order_token TEXT,
        to_output_order_token TEXT,
        transition_token TEXT,
        record_json TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS family_buckets (
        bucket_id TEXT PRIMARY KEY,
        slice_id TEXT,
        graph_slice_id TEXT,
        network_id TEXT,
        family_kind TEXT,
        family_label TEXT,
        parent_motif TEXT,
        path_count INTEGER,
        robust_path_count INTEGER,
        volume_mean REAL,
        representative_path_idx INTEGER,
        record_json TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS path_records (
        path_record_id TEXT PRIMARY KEY,
        slice_id TEXT,
        graph_slice_id TEXT,
        network_id TEXT,
        input_symbol TEXT,
        change_signature TEXT,
        output_symbol TEXT,
        path_idx INTEGER,
        path_length INTEGER,
        exact_label TEXT,
        motif_label TEXT,
        feasible INTEGER,
        robust INTEGER,
        volume_mean REAL,
        output_order_tokens_json TEXT,
        transition_tokens_json TEXT,
        record_json TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS duplicate_inputs (
        duplicate_key TEXT PRIMARY KEY,
        source_label TEXT,
        duplicate_of_network_id TEXT,
        record_json TEXT NOT NULL
    )
    """,
]


FINAL_INDEX_STATEMENTS = [
    "CREATE INDEX IF NOT EXISTS idx_slice_network ON behavior_slices (network_id)",
    "CREATE INDEX IF NOT EXISTS idx_slice_status ON behavior_slices (analysis_status)",
    "CREATE INDEX IF NOT EXISTS idx_regime_slice ON regime_records (slice_id)",
    "CREATE INDEX IF NOT EXISTS idx_bucket_slice ON family_buckets (slice_id)",
]


def _create_schema(conn: sqlite3.Connection) -> None:
    for statement in SCHEMA_STATEMENTS:
        conn.execute(statement)


def _commit_stage(conn: sqlite3.Connection, label: str) -> None:
    conn.commit()
    try:
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    except sqlite3.OperationalError:
        # DELETE-mode databases do not use WAL; keep this best-effort so the
        # helper also works if journal_mode changes in the future.
        pass
    print(f"Committed stage: {label}", flush=True)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build a compact sqlite that preserves only existing successful slices "
            "plus a single supporting regime/family record per slice, suitable for "
            "skip_existing pruning on another machine."
        )
    )
    parser.add_argument("source", help="Source atlas sqlite path")
    parser.add_argument("dest", help="Destination compact sqlite path")
    parser.add_argument(
        "--source-label-glob",
        default=None,
        help="Optional SQLite GLOB applied to network_entries.source_label",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    source = Path(args.source).expanduser().resolve()
    dest = Path(args.dest).expanduser().resolve()
    if not source.exists():
        raise FileNotFoundError(source)

    if dest.exists():
        dest.unlink()
    dest.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(dest))
    conn.execute("PRAGMA temp_store = MEMORY")
    conn.execute("PRAGMA cache_size = -100000")
    _create_schema(conn)
    _commit_stage(conn, "schema_init")
    conn.execute(f"ATTACH DATABASE '{source}' AS src")

    filter_sql = ""
    params: tuple[str, ...] = ()
    if args.source_label_glob:
        filter_sql = "WHERE source_label GLOB ?"
        params = (args.source_label_glob,)

    print("Selecting networks...", flush=True)
    conn.execute("CREATE TEMP TABLE selected_network_ids(network_id TEXT PRIMARY KEY)")
    conn.execute(
        f"INSERT INTO selected_network_ids(network_id) "
        f"SELECT network_id FROM src.network_entries {filter_sql}",
        params,
    )
    _commit_stage(conn, "selected_network_ids")

    print("Copying network entries (minimal payload)...", flush=True)
    conn.execute(
        """
        INSERT INTO network_entries (
            network_id,
            canonical_code,
            analysis_status,
            base_species_count,
            reaction_count,
            total_species_count,
            max_support,
            support_mass,
            source_label,
            source_kind,
            motif_union_json,
            exact_union_json,
            slice_ids_json,
            record_json
        )
        SELECT
            ne.network_id,
            ne.canonical_code,
            ne.analysis_status,
            ne.base_species_count,
            ne.reaction_count,
            ne.total_species_count,
            ne.max_support,
            ne.support_mass,
            ne.source_label,
            ne.source_kind,
            ne.motif_union_json,
            ne.exact_union_json,
            ne.slice_ids_json,
            '{}'
        FROM src.network_entries AS ne
        JOIN selected_network_ids AS sel USING(network_id)
        """
    )
    _commit_stage(conn, "network_entries")

    print("Copying input graph slices (minimal payload)...", flush=True)
    conn.execute(
        """
        INSERT INTO input_graph_slices (
            graph_slice_id,
            network_id,
            input_symbol,
            change_signature,
            vertex_count,
            edge_count,
            path_count,
            record_json
        )
        SELECT
            igs.graph_slice_id,
            igs.network_id,
            igs.input_symbol,
            igs.change_signature,
            igs.vertex_count,
            igs.edge_count,
            igs.path_count,
            '{}'
        FROM src.input_graph_slices AS igs
        JOIN selected_network_ids AS sel USING(network_id)
        """
    )
    _commit_stage(conn, "input_graph_slices")

    print("Selecting successful slices...", flush=True)
    conn.execute("CREATE TEMP TABLE selected_slice_ids(slice_id TEXT PRIMARY KEY)")
    conn.execute(
        """
        INSERT INTO selected_slice_ids(slice_id)
        SELECT bs.slice_id
        FROM src.behavior_slices AS bs
        JOIN selected_network_ids AS sel USING(network_id)
        WHERE bs.analysis_status = 'ok'
        """
    )
    _commit_stage(conn, "selected_slice_ids")

    print("Copying successful behavior slices (minimal payload)...", flush=True)
    conn.execute(
        """
        INSERT INTO behavior_slices (
            slice_id,
            network_id,
            graph_slice_id,
            input_symbol,
            change_signature,
            output_symbol,
            analysis_status,
            path_scope,
            min_volume_mean,
            total_paths,
            feasible_paths,
            included_paths,
            excluded_paths,
            motif_union_json,
            exact_union_json,
            classifier_config_json,
            record_json
        )
        SELECT
            bs.slice_id,
            bs.network_id,
            bs.graph_slice_id,
            bs.input_symbol,
            bs.change_signature,
            bs.output_symbol,
            bs.analysis_status,
            bs.path_scope,
            bs.min_volume_mean,
            bs.total_paths,
            bs.feasible_paths,
            bs.included_paths,
            bs.excluded_paths,
            bs.motif_union_json,
            bs.exact_union_json,
            bs.classifier_config_json,
            '{}'
        FROM src.behavior_slices AS bs
        JOIN selected_slice_ids AS sel USING(slice_id)
        """
    )
    _commit_stage(conn, "behavior_slices")

    print("Selecting one regime rowid per successful slice...", flush=True)
    conn.execute("CREATE TEMP TABLE keep_regime_rowids(rowid INTEGER PRIMARY KEY)")
    conn.execute(
        """
        INSERT INTO keep_regime_rowids(rowid)
        SELECT picked.rowid
        FROM (
            SELECT (
                SELECT rr.rowid
                FROM src.regime_records AS rr
                WHERE rr.slice_id = sel.slice_id
                LIMIT 1
            ) AS rowid
            FROM selected_slice_ids AS sel
        ) AS picked
        WHERE picked.rowid IS NOT NULL
        """
    )
    _commit_stage(conn, "keep_regime_rowids")

    print("Copying one regime record per successful slice (minimal payload)...", flush=True)
    conn.execute(
        """
        INSERT INTO regime_records (
            regime_record_id,
            slice_id,
            graph_slice_id,
            network_id,
            input_symbol,
            change_signature,
            output_symbol,
            vertex_idx,
            role,
            singular,
            nullity,
            asymptotic,
            output_order_token,
            record_json
        )
        SELECT
            rr.regime_record_id,
            rr.slice_id,
            rr.graph_slice_id,
            rr.network_id,
            rr.input_symbol,
            rr.change_signature,
            rr.output_symbol,
            rr.vertex_idx,
            rr.role,
            rr.singular,
            rr.nullity,
            rr.asymptotic,
            rr.output_order_token,
            '{}'
        FROM src.regime_records AS rr
        JOIN keep_regime_rowids AS keep ON rr.rowid = keep.rowid
        """
    )
    _commit_stage(conn, "regime_records")

    print("Selecting one family-bucket rowid per successful slice...", flush=True)
    conn.execute("CREATE TEMP TABLE keep_bucket_rowids(rowid INTEGER PRIMARY KEY)")
    conn.execute(
        """
        INSERT INTO keep_bucket_rowids(rowid)
        SELECT picked.rowid
        FROM (
            SELECT (
                SELECT fb.rowid
                FROM src.family_buckets AS fb
                WHERE fb.slice_id = sel.slice_id
                LIMIT 1
            ) AS rowid
            FROM selected_slice_ids AS sel
        ) AS picked
        WHERE picked.rowid IS NOT NULL
        """
    )
    _commit_stage(conn, "keep_bucket_rowids")

    print("Copying one family bucket per successful slice (minimal payload)...", flush=True)
    conn.execute(
        """
        INSERT INTO family_buckets (
            bucket_id,
            slice_id,
            graph_slice_id,
            network_id,
            family_kind,
            family_label,
            parent_motif,
            path_count,
            robust_path_count,
            volume_mean,
            representative_path_idx,
            record_json
        )
        SELECT
            fb.bucket_id,
            fb.slice_id,
            fb.graph_slice_id,
            fb.network_id,
            fb.family_kind,
            fb.family_label,
            fb.parent_motif,
            fb.path_count,
            fb.robust_path_count,
            fb.volume_mean,
            fb.representative_path_idx,
            '{}'
        FROM src.family_buckets AS fb
        JOIN keep_bucket_rowids AS keep ON fb.rowid = keep.rowid
        """
    )
    _commit_stage(conn, "family_buckets")

    print("Recording prune metadata...", flush=True)
    conn.execute(
        "INSERT INTO atlas_metadata(key, value_text) VALUES (?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value_text=excluded.value_text",
        ("prune_only_sqlite", "true"),
    )
    conn.execute(
        "INSERT INTO atlas_metadata(key, value_text) VALUES (?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value_text=excluded.value_text",
        ("persist_mode", "lightweight"),
    )
    conn.execute(
        "INSERT INTO atlas_metadata(key, value_text) VALUES (?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value_text=excluded.value_text",
        ("prune_source_sqlite", str(source)),
    )
    if args.source_label_glob:
        conn.execute(
            "INSERT INTO atlas_metadata(key, value_text) VALUES (?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value_text=excluded.value_text",
            ("prune_source_label_glob", args.source_label_glob),
        )

    print("Creating minimal indexes...", flush=True)
    for statement in FINAL_INDEX_STATEMENTS:
        conn.execute(statement)
    _commit_stage(conn, "final_indexes")

    conn.commit()
    _commit_stage(conn, "atlas_metadata")

    counts = {
        "network_entries": conn.execute("SELECT COUNT(*) FROM network_entries").fetchone()[0],
        "input_graph_slices": conn.execute("SELECT COUNT(*) FROM input_graph_slices").fetchone()[0],
        "behavior_slices": conn.execute("SELECT COUNT(*) FROM behavior_slices").fetchone()[0],
        "regime_records": conn.execute("SELECT COUNT(*) FROM regime_records").fetchone()[0],
        "family_buckets": conn.execute("SELECT COUNT(*) FROM family_buckets").fetchone()[0],
    }
    conn.close()

    print("Done.", flush=True)
    print(f"Destination: {dest}", flush=True)
    print(f"Size bytes: {os.path.getsize(dest)}", flush=True)
    for key, value in counts.items():
        print(f"{key}: {value}", flush=True)


if __name__ == "__main__":
    main()
