#!/usr/bin/env python3

from __future__ import annotations

import json
import sqlite3
import subprocess
import tempfile
import unittest
from pathlib import Path


ROOT = Path("/Users/yanzhang/git/Biocircuits-Explorer")
SRC_DB = ROOT / "webapp/atlas_store/legacy/sqlite/atlas.sqlite"
TRANSITION_SRC_DB = ROOT / "webapp/atlas_store/legacy/sqlite/atlas.sqlite"
SCRIPT = ROOT / "webapp/scripts/migrate_atlas_sqlite_v2_lossless.py"
VERIFY_SCRIPT = ROOT / "webapp/scripts/verify_atlas_sqlite_v2_sample_lossless.py"


class V2MigrationSmokeTests(unittest.TestCase):
    def test_subset_migration_creates_readable_views(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            dst = Path(td) / "atlas_v2_subset.sqlite"
            out = subprocess.check_output(
                [
                    "python3",
                    str(SCRIPT),
                    "--src-db",
                    str(SRC_DB),
                    "--dst-db",
                    str(dst),
                    "--where-slice-like",
                    "%change=orthant(+tA,+tB)%",
                    "--limit-slices",
                    "3",
                ],
                text=True,
            )
            stats = json.loads(out)
            self.assertEqual(stats["behavior_slices"], 3)
            self.assertGreater(stats["regime_records"], 0)
            self.assertGreater(stats["family_buckets"], 0)

            conn = sqlite3.connect(dst)
            try:
                slice_row = conn.execute("SELECT slice_id, network_id, graph_slice_id FROM v_bs LIMIT 1").fetchone()
                regime_row = conn.execute("SELECT regime_record_id FROM v_rr LIMIT 1").fetchone()
                family_row = conn.execute("SELECT bucket_id FROM v_fb LIMIT 1").fetchone()
                self.assertIsNotNone(slice_row)
                self.assertIsNotNone(regime_row)
                self.assertIsNotNone(family_row)
                self.assertIn("::output=", slice_row[0])
                self.assertIn("::graph_", slice_row[2])
                self.assertIn("::regime::", regime_row[0])
                self.assertTrue(("::exact::" in family_row[0]) or ("::motif::" in family_row[0]))
            finally:
                conn.close()

    def test_real_transition_records_migrate_into_readable_view(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            dst = Path(td) / "atlas_v2_transition.sqlite"
            out = subprocess.check_output(
                [
                    "python3",
                    str(SCRIPT),
                    "--src-db",
                    str(TRANSITION_SRC_DB),
                    "--dst-db",
                    str(dst),
                    "--where-slice-like",
                    "%change=orthant(+tA,+tB)%",
                    "--limit-slices",
                    "3",
                ],
                text=True,
            )
            stats = json.loads(out)
            self.assertEqual(stats["behavior_slices"], 3)
            self.assertGreater(stats["transition_records"], 0)

            conn = sqlite3.connect(dst)
            try:
                row = conn.execute(
                    "SELECT transition_record_id, from_output_order_token, to_output_order_token, transition_token FROM v_tr LIMIT 1"
                ).fetchone()
                self.assertIsNotNone(row)
                self.assertIn("::transition::", row[0])
                self.assertIsInstance(row[1], str)
                self.assertIsInstance(row[2], str)
                self.assertIsInstance(row[3], str)
            finally:
                conn.close()

    def test_synthetic_path_records_migrate_into_readable_view(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            src = Path(td) / "atlas_src_with_path.sqlite"
            dst = Path(td) / "atlas_v2_path.sqlite"
            self._build_synthetic_path_source(src)
            out = subprocess.check_output(
                [
                    "python3",
                    str(SCRIPT),
                    "--src-db",
                    str(src),
                    "--dst-db",
                    str(dst),
                ],
                text=True,
            )
            stats = json.loads(out)
            self.assertEqual(stats["behavior_slices"], 1)
            self.assertEqual(stats["path_records"], 1)

            conn = sqlite3.connect(dst)
            try:
                row = conn.execute(
                    """
                    SELECT path_record_id, exact_label, motif_label, feasible, robust,
                           output_order_tokens_json, transition_tokens_json
                    FROM v_pr
                    LIMIT 1
                    """
                ).fetchone()
                self.assertIsNotNone(row)
                self.assertIn("::path::7", row[0])
                self.assertEqual(row[1], "1 → 0")
                self.assertEqual(row[2], "thresholded_activation")
                self.assertEqual(row[3], 1)
                self.assertEqual(row[4], 0)
                self.assertEqual(json.loads(row[5]), ["+1", "0"])
                self.assertEqual(json.loads(row[6]), ["+1->0"])
            finally:
                conn.close()

    def test_compound_roles_round_trip_through_verifier(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            src = Path(td) / "atlas_src_with_compound_roles.sqlite"
            dst = Path(td) / "atlas_v2_compound_roles.sqlite"
            self._build_synthetic_compound_role_source(src)
            subprocess.check_output(
                [
                    "python3",
                    str(SCRIPT),
                    "--src-db",
                    str(src),
                    "--dst-db",
                    str(dst),
                ],
                text=True,
            )

            conn = sqlite3.connect(dst)
            try:
                regime_row = conn.execute(
                    "SELECT role_code FROM v_rr WHERE regime_record_id LIKE '%::regime::2'"
                ).fetchone()
                transition_row = conn.execute(
                    "SELECT from_role_code, to_role_code FROM v_tr WHERE transition_record_id LIKE '%::transition::1->2'"
                ).fetchone()
                self.assertEqual(regime_row[0], 7)
                self.assertEqual(transition_row[0], 6)
                self.assertEqual(transition_row[1], 7)
            finally:
                conn.close()

            verify_out = subprocess.check_output(
                [
                    "python3",
                    str(VERIFY_SCRIPT),
                    "--src-db",
                    str(src),
                    "--dst-db",
                    str(dst),
                    "--sample-size",
                    "3",
                    "--seed",
                    "123",
                ],
                text=True,
            )
            report = json.loads(verify_out)
            self.assertEqual(report["status"], "passed")
            self.assertEqual(report["checked_count"], 3)

    def _build_synthetic_path_source(self, db_path: Path) -> None:
        conn = sqlite3.connect(db_path)
        try:
            conn.executescript(
                """
                CREATE TABLE behavior_slices (
                    slice_id TEXT PRIMARY KEY,
                    network_id TEXT,
                    graph_slice_id TEXT,
                    input_symbol TEXT,
                    change_signature TEXT,
                    output_symbol TEXT,
                    analysis_status TEXT,
                    total_paths INTEGER,
                    feasible_paths INTEGER,
                    included_paths INTEGER,
                    excluded_paths INTEGER,
                    motif_union_json TEXT,
                    exact_union_json TEXT,
                    record_json TEXT
                );
                CREATE TABLE regime_records (
                    regime_record_id TEXT PRIMARY KEY,
                    slice_id TEXT,
                    role TEXT,
                    singular INTEGER,
                    nullity INTEGER,
                    asymptotic INTEGER,
                    output_order_token TEXT,
                    record_json TEXT
                );
                CREATE TABLE family_buckets (
                    bucket_id TEXT PRIMARY KEY,
                    slice_id TEXT,
                    family_kind TEXT,
                    family_label TEXT,
                    parent_motif TEXT,
                    path_count INTEGER,
                    robust_path_count INTEGER,
                    volume_mean REAL,
                    representative_path_idx INTEGER,
                    record_json TEXT
                );
                CREATE TABLE transition_records (
                    transition_record_id TEXT PRIMARY KEY,
                    slice_id TEXT,
                    from_vertex_idx INTEGER,
                    to_vertex_idx INTEGER,
                    from_role TEXT,
                    to_role TEXT,
                    from_output_order_token TEXT,
                    to_output_order_token TEXT,
                    transition_token TEXT,
                    record_json TEXT
                );
                CREATE TABLE path_records (
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
                    record_json TEXT
                );
                """
            )

            network_id = "[1]+[2]<->[1,2]"
            graph_slice_id = network_id + "::graph_change=orthant(+tA,+tB)::graphcfg=orthant_v0"
            slice_id = network_id + "::change=orthant(+tA,+tB)::output=A::cfg=scope=feasible;min_volume_mean=0.0;deduplicate=true;keep_singular=true;keep_nonasymptotic=false;compute_volume=false;motif_zero_tol=1.0e-6"

            conn.execute(
                """
                INSERT INTO behavior_slices (
                    slice_id, network_id, graph_slice_id, input_symbol, change_signature, output_symbol,
                    analysis_status, total_paths, feasible_paths, included_paths, excluded_paths,
                    motif_union_json, exact_union_json, record_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    slice_id,
                    network_id,
                    graph_slice_id,
                    "+tA,+tB",
                    "orthant(+tA,+tB)",
                    "A",
                    "ok",
                    1,
                    1,
                    1,
                    0,
                    json.dumps(["thresholded_activation"]),
                    json.dumps(["1 → 0"]),
                    json.dumps({"slice_id": slice_id, "analysis_status": "ok", "motif_union_json": ["thresholded_activation"]}),
                ),
            )

            conn.execute(
                """
                INSERT INTO path_records (
                    path_record_id, slice_id, graph_slice_id, network_id, input_symbol, change_signature,
                    output_symbol, path_idx, path_length, exact_label, motif_label, feasible, robust, volume_mean,
                    output_order_tokens_json, transition_tokens_json, record_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    slice_id + "::path::7",
                    slice_id,
                    graph_slice_id,
                    network_id,
                    "+tA,+tB",
                    "orthant(+tA,+tB)",
                    "A",
                    7,
                    2,
                    "1 → 0",
                    "thresholded_activation",
                    1,
                    0,
                    0.5,
                    json.dumps(["+1", "0"]),
                    json.dumps(["+1->0"]),
                    json.dumps({
                        "path_record_id": slice_id + "::path::7",
                        "slice_id": slice_id,
                        "graph_slice_id": graph_slice_id,
                        "network_id": network_id,
                        "input_symbol": "+tA,+tB",
                        "change_signature": "orthant(+tA,+tB)",
                        "output_symbol": "A",
                        "path_idx": 7,
                        "path_length": 2,
                        "exact_label": "1 → 0",
                        "motif_label": "thresholded_activation",
                        "feasible": True,
                        "robust": False,
                        "volume_mean": 0.5,
                        "output_order_tokens_json": ["+1", "0"],
                        "transition_tokens_json": ["+1->0"],
                        "vertex_indices": [1, 2],
                    }),
                ),
            )
            conn.commit()
        finally:
            conn.close()

    def _build_synthetic_compound_role_source(self, db_path: Path) -> None:
        conn = sqlite3.connect(db_path)
        try:
            conn.executescript(
                """
                CREATE TABLE behavior_slices (
                    slice_id TEXT PRIMARY KEY,
                    network_id TEXT,
                    graph_slice_id TEXT,
                    input_symbol TEXT,
                    change_signature TEXT,
                    output_symbol TEXT,
                    analysis_status TEXT,
                    total_paths INTEGER,
                    feasible_paths INTEGER,
                    included_paths INTEGER,
                    excluded_paths INTEGER,
                    motif_union_json TEXT,
                    exact_union_json TEXT,
                    record_json TEXT
                );
                CREATE TABLE regime_records (
                    regime_record_id TEXT PRIMARY KEY,
                    slice_id TEXT,
                    role TEXT,
                    singular INTEGER,
                    nullity INTEGER,
                    asymptotic INTEGER,
                    output_order_token TEXT,
                    record_json TEXT
                );
                CREATE TABLE family_buckets (
                    bucket_id TEXT PRIMARY KEY,
                    slice_id TEXT,
                    family_kind TEXT,
                    family_label TEXT,
                    parent_motif TEXT,
                    path_count INTEGER,
                    robust_path_count INTEGER,
                    volume_mean REAL,
                    representative_path_idx INTEGER,
                    record_json TEXT
                );
                CREATE TABLE transition_records (
                    transition_record_id TEXT PRIMARY KEY,
                    slice_id TEXT,
                    from_vertex_idx INTEGER,
                    to_vertex_idx INTEGER,
                    from_role TEXT,
                    to_role TEXT,
                    from_output_order_token TEXT,
                    to_output_order_token TEXT,
                    transition_token TEXT,
                    record_json TEXT
                );
                CREATE TABLE path_records (
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
                    record_json TEXT
                );
                """
            )

            network_id = "[1]+[2]<->[1,2]|[3]+[4]<->[3,4]"
            graph_slice_id = network_id + "::graph_change=orthant(+tA,+tB)::graphcfg=orthant_v0"
            slice_id = network_id + "::change=orthant(+tA,+tB)::output=A::cfg=scope=feasible;min_volume_mean=0.0;deduplicate=true;keep_singular=true;keep_nonasymptotic=false;compute_volume=false;motif_zero_tol=1.0e-6"

            conn.execute(
                """
                INSERT INTO behavior_slices (
                    slice_id, network_id, graph_slice_id, input_symbol, change_signature, output_symbol,
                    analysis_status, total_paths, feasible_paths, included_paths, excluded_paths,
                    motif_union_json, exact_union_json, record_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    slice_id,
                    network_id,
                    graph_slice_id,
                    "+tA,+tB",
                    "orthant(+tA,+tB)",
                    "A",
                    "ok",
                    2,
                    2,
                    2,
                    0,
                    json.dumps([]),
                    json.dumps([]),
                    json.dumps({
                        "slice_id": slice_id,
                        "network_id": network_id,
                        "graph_slice_id": graph_slice_id,
                        "input_symbol": "+tA,+tB",
                        "change_signature": "orthant(+tA,+tB)",
                        "output_symbol": "A",
                        "analysis_status": "ok",
                        "path_scope": "feasible",
                        "min_volume_mean": 0.0,
                        "total_paths": 2,
                        "feasible_paths": 2,
                        "included_paths": 2,
                        "excluded_paths": 0,
                        "motif_union_json": [],
                        "exact_union_json": [],
                    }),
                ),
            )

            conn.execute(
                """
                INSERT INTO regime_records (
                    regime_record_id, slice_id, role, singular, nullity, asymptotic, output_order_token, record_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    slice_id + "::regime::2",
                    slice_id,
                    "branch_merge",
                    0,
                    1,
                    0,
                    "+1",
                    json.dumps({
                        "regime_record_id": slice_id + "::regime::2",
                        "slice_id": slice_id,
                        "graph_slice_id": graph_slice_id,
                        "network_id": network_id,
                        "input_symbol": "+tA,+tB",
                        "change_signature": "orthant(+tA,+tB)",
                        "output_symbol": "A",
                        "vertex_idx": 2,
                        "role": "branch_merge",
                        "singular": 0,
                        "nullity": 1,
                        "asymptotic": 0,
                        "output_order_token": "+1",
                    }),
                ),
            )

            conn.execute(
                """
                INSERT INTO transition_records (
                    transition_record_id, slice_id, from_vertex_idx, to_vertex_idx, from_role, to_role,
                    from_output_order_token, to_output_order_token, transition_token, record_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    slice_id + "::transition::1->2",
                    slice_id,
                    1,
                    2,
                    "source_sink",
                    "branch_merge",
                    "+1",
                    "0",
                    "+1->0",
                    json.dumps({
                        "transition_record_id": slice_id + "::transition::1->2",
                        "slice_id": slice_id,
                        "graph_slice_id": graph_slice_id,
                        "input_symbol": "+tA,+tB",
                        "change_signature": "orthant(+tA,+tB)",
                        "output_symbol": "A",
                        "from_vertex_idx": 1,
                        "to_vertex_idx": 2,
                        "from_role": "source_sink",
                        "to_role": "branch_merge",
                        "from_output_order_token": "+1",
                        "to_output_order_token": "0",
                        "transition_token": "+1->0",
                    }),
                ),
            )
            conn.commit()
        finally:
            conn.close()


if __name__ == "__main__":
    unittest.main()
