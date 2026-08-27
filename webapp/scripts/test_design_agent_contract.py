#!/usr/bin/env python3
import copy
import io
import json
import os
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest import mock

import chat_api
import design_agent
import llm_compile


EXECUTABLE_BEHAVIOR_SPEC = {
    "schema_version": "bne-behavior/v0.1.0",
    "behavior_spec": {
        "feature_space": "reaction_order",
        "input": "tA",
        "output": "C_A_A",
        "program": [
            {"kind": "reaction_order", "operator": "=", "value": 1.0},
            {"kind": "reaction_order", "operator": "=", "value": 0.0},
        ],
    },
    "network_constraints": {
        "max_reactions": 4,
        "kd_profile": {
            "log10_kd_min": -2,
            "log10_kd_max": 2,
        },
    },
}

ROP_SHAPE_SPEC = {
    "schema_version": "bne-designability/v1.0.0",
    "source": {"kind": "agent_design"},
    "target": {
        "behavior_spec": {
            "feature_space": "reaction_order",
            "input": "tA",
            "output": "C_A_B",
            "program": [
                {"kind": "reaction_order", "operator": "=", "value": 1.0},
                {"kind": "reaction_order", "operator": "=", "value": 0.0},
                {"kind": "reaction_order", "operator": "=", "value": -1.0},
            ],
            "input_window": {"input_log10": [-5.0, 5.0], "hard": True},
        },
    },
    "constraints": {
        "parameter_bounds": {
            "basis": "log10_qK",
            "by_class": {"kd": [-10.0, 10.0], "total": [-5.0, 5.0]},
        },
    },
    "candidate_budget": {
        "max_screened": 8,
        "max_verified_recommendations": 2,
        "max_exact_placements": 2,
    },
    "ranking_policy": {"verified_only": True},
    "audit_policy": {
        "unsupported": "block_if_hard",
        "path_format": "json_pointer",
        "include_supported": True,
    },
}

ROP_SHAPE_REFERENCE = {
    "reference_hash": "a" * 64,
    "artifact_ref": "sha256:" + "a" * 64,
    "network_ir_hash": "b" * 64,
    "operating_points_log10": [-3.0, 0.0, 3.0],
    "kd": [1.0],
    "totals": {"tA": 1.0, "tB": 1.0},
}

ROP_SHAPE_NETWORK = {
    "rules": ["A + B <-> C_A_B"],
    "input_symbols": ["tA"],
    "output_symbols": ["C_A_B"],
}

ROP_SHAPE_WORK_BUDGET = {
    "max_paths": 32,
    "max_cells": 128,
    "max_replays": 4,
    "require_exhaustive": True,
}

ROP_SHAPE_REPLAY = {
    "input_window_log10": [-5.0, 5.0],
    "sample_points": 21,
    "require_complete": True,
    "store_curve": True,
    "metrics": [{"kind": "two_peak", "min_prominence_log10": 0.2}],
}

ROP_SHAPE_INTENT = {
    "id": "separate-outer-witnesses",
    "kind": "separate",
    "steps": [0, 2],
    "preserve_midpoint_tolerance_log10": 0.1,
}


class TraceRetentionContractTests(unittest.TestCase):
    @staticmethod
    def _trace(trace_id, message_size=120, cards=None):
        return {
            "trace_schema_version": design_agent.TRACE_SCHEMA_VERSION,
            "trace_id": trace_id,
            "raw_user_message": "x" * message_size,
            "compiler": {"provider": "test", "model": "test"},
            "tool_calls": [],
            "final_response": {"kind": "agent", "cards": cards or []},
        }

    def test_trace_segments_rotate_and_drop_only_the_oldest_archive(self):
        with tempfile.TemporaryDirectory() as tmpdir, \
             mock.patch.multiple(
                 design_agent,
                 TRACE_DIR=tmpdir,
                 TRACE_MAX_BYTES=256,
                 TRACE_ARCHIVE_COUNT=2,
                 TRACE_ARTIFACT_MAX_BYTES=1024,
                 TRACE_ARTIFACT_MAX_FILES=8,
             ):
            for index in range(5):
                design_agent._write_trace(self._trace(f"trace-{index}"))

            paths = [os.path.join(tmpdir, "traces.jsonl")]
            paths.extend(os.path.join(tmpdir, f"traces.jsonl.{index}") for index in (1, 2))
            self.assertTrue(all(os.path.exists(path) for path in paths))
            self.assertFalse(os.path.exists(os.path.join(tmpdir, "traces.jsonl.3")))
            retained = []
            for path in paths:
                with open(path, encoding="utf-8") as fh:
                    retained.extend(json.loads(line)["trace_id"] for line in fh if line.strip())
            self.assertEqual(set(retained), {"trace-2", "trace-3", "trace-4"})

    def test_reduced_or_zero_archive_count_prunes_stale_higher_segments(self):
        for archive_count in (1, 0):
            with self.subTest(archive_count=archive_count), \
                 tempfile.TemporaryDirectory() as tmpdir, \
                 mock.patch.multiple(
                     design_agent,
                     TRACE_DIR=tmpdir,
                     TRACE_MAX_BYTES=1024 * 1024,
                     TRACE_ARCHIVE_COUNT=archive_count,
                     TRACE_ARTIFACT_MAX_BYTES=1024,
                     TRACE_ARTIFACT_MAX_FILES=8,
                 ):
                for index in (1, 2, 3):
                    with open(
                        os.path.join(tmpdir, f"traces.jsonl.{index}"),
                        "w",
                        encoding="utf-8",
                    ) as fh:
                        fh.write("{}\n")

                design_agent._write_trace(self._trace("trace-after-reconfigure", 8))

                self.assertEqual(
                    sorted(
                        name for name in os.listdir(tmpdir)
                        if name.startswith("traces.jsonl.")
                    ),
                    ["traces.jsonl.1"] if archive_count == 1 else [],
                )

    def test_trace_append_is_thread_serialized_and_jsonl_stays_parseable(self):
        with tempfile.TemporaryDirectory() as tmpdir, \
             mock.patch.multiple(
                 design_agent,
                 TRACE_DIR=tmpdir,
                 TRACE_MAX_BYTES=1024 * 1024,
                 TRACE_ARCHIVE_COUNT=1,
                 TRACE_ARTIFACT_MAX_BYTES=1024,
                 TRACE_ARTIFACT_MAX_FILES=8,
             ):
            with ThreadPoolExecutor(max_workers=8) as pool:
                list(pool.map(
                    lambda index: design_agent._write_trace(self._trace(f"trace-{index}", 8)),
                    range(64),
                ))
            with open(os.path.join(tmpdir, "traces.jsonl"), encoding="utf-8") as fh:
                records = [json.loads(line) for line in fh if line.strip()]
            self.assertEqual(len(records), 64)
            self.assertEqual(len({record["trace_id"] for record in records}), 64)

    def test_card_artifacts_publish_atomically_and_oldest_files_are_pruned(self):
        with tempfile.TemporaryDirectory() as tmpdir, \
             mock.patch.multiple(
                 design_agent,
                 TRACE_DIR=tmpdir,
                 TRACE_MAX_BYTES=1024 * 1024,
                 TRACE_ARCHIVE_COUNT=1,
                 TRACE_ARTIFACT_MAX_BYTES=9,
                 TRACE_ARTIFACT_MAX_FILES=2,
             ):
            artifact_dir = os.path.join(tmpdir, "artifacts")
            os.makedirs(artifact_dir)
            for index, name in enumerate(("old.json", "middle.json", "new.json")):
                path = os.path.join(artifact_dir, name)
                with open(path, "wb") as fh:
                    fh.write(b"data")
                os.utime(path, (100 + index, 100 + index))
            stale_tmp = os.path.join(artifact_dir, ".orphan.tmp")
            with open(stale_tmp, "wb") as fh:
                fh.write(b"orphan")
            os.utime(stale_tmp, (0, 0))

            design_agent._write_trace(self._trace(
                "trace-with-card", cards=[{"result_hash": "new"}],
            ))

            self.assertFalse(os.path.exists(os.path.join(artifact_dir, "old.json")))
            self.assertTrue(os.path.exists(os.path.join(artifact_dir, "middle.json")))
            self.assertTrue(os.path.exists(os.path.join(artifact_dir, "new.json")))
            self.assertFalse(os.path.exists(stale_tmp))
            self.assertLessEqual(sum(
                os.path.getsize(os.path.join(artifact_dir, name))
                for name in os.listdir(artifact_dir)
            ), 9)

            card = {"family": "dose_shape", "computed_series": [{"x": 0.0, "y": 1.0}]}
            result_hash = design_agent._spill_card(card)
            stored_path = os.path.join(artifact_dir, result_hash + ".json")
            with open(stored_path, encoding="utf-8") as fh:
                self.assertEqual(json.load(fh), card)
            self.assertFalse(any(name.endswith(".tmp") for name in os.listdir(artifact_dir)))

    def test_artifact_retention_runs_even_when_trace_append_fails(self):
        with tempfile.TemporaryDirectory() as tmpdir, \
             mock.patch.multiple(
                 design_agent,
                 TRACE_DIR=tmpdir,
                 TRACE_MAX_BYTES=1024 * 1024,
                 TRACE_ARCHIVE_COUNT=1,
                 TRACE_ARTIFACT_MAX_BYTES=1024 * 1024,
                 TRACE_ARTIFACT_MAX_FILES=1,
             ), \
             mock.patch.object(design_agent.sys, "stderr", io.StringIO()):
            for index in range(3):
                card = {
                    "family": "dose_shape",
                    "computed_series": [{"x": index, "y": index + 1}],
                }
                result_hash = design_agent._spill_card(card)
                with mock.patch.object(
                    design_agent,
                    "_trace_log_path",
                    side_effect=OSError("injected trace append failure"),
                ):
                    design_agent._write_trace(self._trace(
                        f"trace-failure-{index}",
                        cards=[{"result_hash": result_hash}],
                    ))

            artifact_dir = os.path.join(tmpdir, "artifacts")
            self.assertEqual(
                [name for name in os.listdir(artifact_dir) if name.endswith(".json")],
                [result_hash + ".json"],
            )

    def test_spill_repairs_a_truncated_content_addressed_artifact(self):
        with tempfile.TemporaryDirectory() as tmpdir, \
             mock.patch.object(design_agent, "TRACE_DIR", tmpdir):
            card = {
                "family": "dose_shape",
                "computed_series": [{"x": 0.0, "y": 1.0}],
            }
            result_hash = design_agent._hash(card)
            artifact_dir = os.path.join(tmpdir, "artifacts")
            os.makedirs(artifact_dir)
            artifact_path = os.path.join(artifact_dir, result_hash + ".json")
            with open(artifact_path, "w", encoding="utf-8") as fh:
                fh.write("{broken")

            self.assertEqual(design_agent._spill_card(card), result_hash)

            with open(artifact_path, encoding="utf-8") as fh:
                self.assertEqual(json.load(fh), card)
            self.assertFalse(any(name.endswith(".tmp") for name in os.listdir(artifact_dir)))

    def test_reused_card_artifact_becomes_recent_before_retention(self):
        with tempfile.TemporaryDirectory() as tmpdir, \
             mock.patch.multiple(
                 design_agent,
                 TRACE_DIR=tmpdir,
                 TRACE_MAX_BYTES=1024 * 1024,
                 TRACE_ARCHIVE_COUNT=1,
                 TRACE_ARTIFACT_MAX_BYTES=1024 * 1024,
                 TRACE_ARTIFACT_MAX_FILES=1,
             ):
            artifact_dir = os.path.join(tmpdir, "artifacts")
            os.makedirs(artifact_dir)
            card = {"family": "dose_shape", "computed_series": [{"x": 0.0, "y": 1.0}]}
            result_hash = design_agent._hash(card)
            reused_path = os.path.join(artifact_dir, result_hash + ".json")
            with open(reused_path, "w", encoding="utf-8") as fh:
                json.dump(card, fh)
            os.utime(reused_path, (100, 100))
            other_path = os.path.join(artifact_dir, "other.json")
            with open(other_path, "w", encoding="utf-8") as fh:
                json.dump({"other": True}, fh)
            os.utime(other_path, (200, 200))

            self.assertEqual(design_agent._spill_card(card), result_hash)
            design_agent._write_trace(self._trace(
                "trace-reuses-card", cards=[{"result_hash": result_hash}],
            ))

            self.assertTrue(os.path.exists(reused_path))
            self.assertFalse(os.path.exists(other_path))

    def test_trace_integer_config_rejects_noncanonical_or_out_of_range_values(self):
        for raw in ("", "+1", "01", " 1", "1.0", "-1"):
            with self.subTest(raw=raw), mock.patch.dict(os.environ, {"TRACE_TEST_VALUE": raw}):
                with self.assertRaises(ValueError):
                    design_agent._trace_integer_config("TRACE_TEST_VALUE", 2, 0, 4)
        with mock.patch.dict(os.environ, {"TRACE_TEST_VALUE": "5"}):
            with self.assertRaises(ValueError):
                design_agent._trace_integer_config("TRACE_TEST_VALUE", 2, 0, 4)
        with mock.patch.dict(os.environ, {"TRACE_TEST_VALUE": "0"}):
            self.assertEqual(design_agent._trace_integer_config("TRACE_TEST_VALUE", 2, 0, 4), 0)


def rop_shape_backend_response():
    xs = [-5.0 + 0.5 * index for index in range(21)]
    ys = [-2.0 + 0.1 * index for index in range(21)]
    return {
        "schema_version": "bne-rop-shape-optimization/v1.0.0",
        "request_hash": "c" * 64,
        "normalized_request": {"schema_version": "bne-rop-shape-optimize-request/v1.0.0"},
        "geometric_status": "global_optimal_over_declared_cells",
        "feasible": True,
        "coverage": {
            "eligible_path_count": 2,
            "evaluated_path_count": 2,
            "eligible_cell_count": 4,
            "evaluated_cell_count": 4,
            "feasible_cell_count": 2,
            "replay_candidate_count": 1,
            "replayed_count": 1,
            "truncated": False,
            "truncation_reasons": [],
        },
        "compiled_edit": {
            "compiler_version": "rop-shape-edit/v1",
            "kind": "separate",
            "constraints": [{"id": "ear_separation", "terms": []}],
        },
        "selected": {
            "path_identity": "path-1",
            "witness_identity": [1, 2, 3],
            "objective_values": {"effect": 1.25, "parameter_margin": 0.3},
        },
        "fixed_topology": {
            "normalized_network": {
                "ir_schema_version": "bne-ir/v1.0.0",
                "species": [],
                "reactions": [],
                "observables": [],
            },
            "network_ir_hash": "b" * 64,
            "network_canonical_code": "[1]+[2]<->[1,2]",
            "network_identity_semantics": "canonical_code_available",
            "input": "tA",
            "output": "C_A_B",
            "topology_preserved": True,
        },
        "replay": {
            "status": "pass",
            "complete": True,
            "pass": True,
            "request": {
                "endpoint": "/api/v1/placer_curve",
                "method": "POST",
                "body": {
                    "rules": ["A + B <-> C_A_B"],
                    "input_sym": "tA",
                    "output_sym": "C_A_B",
                    "kd": [0.5],
                    "totals": {"tA": 1.0, "tB": 2.0},
                    "param_min": -5.0,
                    "param_max": 5.0,
                    "n_points": 21,
                },
            },
            "curve": {
                "param_values": xs,
                "output_traj": [[value] for value in ys],
                "valid": [True] * len(xs),
                "partial": False,
            },
            "request_hash": "d" * 64,
            "result_hash": "e" * 64,
            "metrics": {
                "schema_version": "bne-rop-shape-replay/v1.0.0",
                "status": "pass",
                "sample_points": 21,
                "complete": True,
                "pass": True,
            },
        },
        "certificate_grade": "exact-window-siso-rop-path-optimization",
        "artifact": {
            "artifact_schema_version": "bne-result/v1.0.0",
            "kind": "rop_shape_optimize",
            "input_hashes": {"network_ir_hash": "b" * 64},
            "algorithm": {"name": "rop_shape_optimization", "version": "0.1.0", "config_hash": "f" * 64},
            "created_at": "2026-07-11T00:00:00Z",
        },
        "warnings": [],
    }


class DesignAgentContractTests(unittest.TestCase):
    @staticmethod
    def _simulate_2d_model():
        return {
            "session_id": "session-2d",
            "q_sym": ["tA", "tB"],
            "K_sym": ["Kd1"],
            "x_sym": ["A", "B", "Y"],
            "species": ["A", "B", "Y"],
            "free_species": ["A", "B"],
            "product_species": ["Y"],
            "kd": [2.0],
            "network_ir_hash": "a" * 64,
            "network_ir": {
                "ir_schema_version": "bne-ir/v1.0.0",
                "label": "",
                "species": [
                    {"name": "A", "role": "free", "initial_total": None,
                     "unit": "concentration", "metadata": {}},
                    {"name": "B", "role": "free", "initial_total": None,
                     "unit": "concentration", "metadata": {}},
                    {"name": "Y", "role": "bound", "initial_total": None,
                     "unit": "concentration", "metadata": {}},
                ],
                "reactions": [{
                    "formula": "A + B <-> Y", "kind": "binding", "kd": 2.0,
                    "kd_distribution": None, "reversible": True, "metadata": {},
                }],
                "observables": [],
                "parameter_distributions": [],
                "compartments": [],
                "provenance": {
                    "created_at": "2026-07-17T00:00:00", "created_by": "",
                    "source": "legacy_reactions_kd", "parent_ir_hash": None,
                    "notes": "",
                },
                "extensions": {},
            },
            "artifact": {
                "artifact_schema_version": "bne-result/v1.0.0",
                "kind": "build_model",
                "input_hashes": {"network_ir_hash": "a" * 64},
            },
        }

    @classmethod
    def _simulate_2d_canonical_identity(cls, model=None):
        model = copy.deepcopy(model if model is not None else cls._simulate_2d_model())
        canonical = copy.deepcopy(model["network_ir"])
        canonical["provenance"]["created_at"] = "2026-07-17T00:00:01"
        return {
            "network_ir": canonical,
            "network_ir_hash": model["network_ir_hash"],
        }

    @classmethod
    def _patch_simulate_2d_canonical_identity(cls, model=None):
        return mock.patch.object(
            design_agent,
            "_canonical_network_ir_identity",
            return_value=cls._simulate_2d_canonical_identity(model),
        )

    @staticmethod
    def _simulate_2d_scan(output_grid, validity_grid, partial):
        n_rows = len(output_grid)
        n_cols = len(output_grid[0]) if output_grid else 0
        def axis(count):
            if count <= 1:
                return [-6.0] * count
            return [-6.0 + (12.0 * index / (count - 1)) for index in range(count)]
        return {
            "network_ir_hash": "a" * 64,
            "param1_symbol": "tA",
            "param2_symbol": "tB",
            "param1_values": axis(n_rows),
            "param2_values": axis(n_cols),
            "output_expr": "Y",
            "output_grid": copy.deepcopy(output_grid),
            "validity_grid": copy.deepcopy(validity_grid),
            "partial": partial,
            "fixed_qK": [0.0, 0.0, 0.3010299956639812],
        }

    def test_simulate_2d_complete_surface_preserves_gate_and_full_identity(self):
        grid = [
            [0.0, 0.0, 0.0],
            [0.0, 0.5, 0.8],
            [0.0, 0.8, 2.0],
        ]
        scan = self._simulate_2d_scan(grid, [[True] * 3 for _ in range(3)], False)

        with mock.patch.object(
                design_agent.E, "build_model", return_value=self._simulate_2d_model()), \
             mock.patch.object(design_agent.E, "scan_2d", return_value=scan) as scan_2d, \
             self._patch_simulate_2d_canonical_identity():
            result = design_agent.simulate_2d(
                ["A + B <-> Y"], kd=[2.0], input1="tA", input2="tB",
                observe_species="Y", n_grid=3,
            )

        self.assertEqual(scan_2d.call_args.kwargs["network_ir_hash"], "a" * 64)

        self.assertIs(result["partial"], False)
        self.assertEqual(result["realized_gate"], "AND")
        self.assertEqual(result["gate_corners_00_01_10_11"], [0, 0, 0, 1])
        self.assertEqual(result["verification_status"], "complete")
        self.assertEqual(result["validity_grid"], [[True] * 3 for _ in range(3)])
        card = result["_card"]
        self.assertEqual(card["inputs"], ["tA", "tB"])
        self.assertEqual(card["output"], "Y")
        self.assertEqual(card["model_identity"]["kd"], [2.0])
        self.assertEqual(card["network_ir_hash"], "a" * 64)
        self.assertEqual(card["network_ir"], self._simulate_2d_model()["network_ir"])
        self.assertEqual(card["fixed_context"]["by_symbol"], {
            "Kd1": 0.3010299956639812,
        })
        self.assertEqual(len(card["request_fingerprint"]), 64)
        self.assertEqual(
            card["card_identity"]["request_fingerprint"],
            card["request_fingerprint"],
        )
        self.assertEqual(
            card["request_identity"]["model"]["network_ir_hash"], "a" * 64,
        )
        self.assertEqual(card["surface"]["validity_grid"], [[True] * 3 for _ in range(3)])
        summary = design_agent._card_summary(card, "artifact-hash")
        self.assertEqual(summary["request_fingerprint"], card["request_fingerprint"])
        self.assertEqual(summary["network_ir_hash"], "a" * 64)
        self.assertIs(summary["partial"], False)

        model = self._simulate_2d_model()
        changed_model = copy.deepcopy(model)
        changed_model["kd"] = [3.0]
        changed_model["network_ir_hash"] = "b" * 64
        variants = [
            design_agent._simulate_2d_identity(
                model, ["A + B <-> Y"], [2.0], "tB", "tA", "Y", 3,
                scan["fixed_qK"],
            )[1],
            design_agent._simulate_2d_identity(
                model, ["A + B <-> Y"], [2.0], "tA", "tB", "Z", 3,
                scan["fixed_qK"],
            )[1],
            design_agent._simulate_2d_identity(
                changed_model, ["A + B <-> Y"], [3.0], "tA", "tB", "Y", 3,
                scan["fixed_qK"],
            )[1],
            design_agent._simulate_2d_identity(
                model, ["A + B <-> Y"], [2.0], "tA", "tB", "Y", 3,
                [0.0, 0.0, 2.25],
            )[1],
        ]
        self.assertEqual(len({card["request_fingerprint"], *variants}), 5)

    def test_simulate_2d_invalid_corner_is_masked_and_withholds_verified_card(self):
        scan = self._simulate_2d_scan(
            [[0.0, 0.0], [1.0, 99.0]],
            [[True, True], [True, False]],
            True,
        )

        with mock.patch.object(
                design_agent.E, "build_model", return_value=self._simulate_2d_model()), \
             mock.patch.object(design_agent.E, "scan_2d", return_value=scan), \
             self._patch_simulate_2d_canonical_identity():
            result = design_agent.simulate_2d(
                ["A + B <-> Y"], kd=[2.0], input1="tA", input2="tB",
                observe_species="Y", n_grid=2,
            )

        self.assertIs(result["partial"], True)
        self.assertEqual(result["verification_status"], "diagnostic_partial")
        self.assertEqual(result["evidence_grade"], "current-computation-partial-diagnostic")
        self.assertIsNone(result["realized_gate"])
        self.assertIsNone(result["gate_corners_00_01_10_11"])
        self.assertIsNone(result["margin_decades"])
        self.assertIsNone(result["interior_peak"])
        self.assertIs(result["required_corners_valid"], False)
        self.assertEqual(result["invalid_point_count"], 1)
        self.assertEqual(result["surface_max"], 1.0)
        self.assertIsNone(result["surface"]["z"][-1][-1])
        self.assertEqual(result["surface"]["validity_grid"], [[True, True], [True, False]])
        self.assertNotIn("_card", result)

    def test_simulate_2d_any_partial_surface_cannot_certify_gate_or_feature(self):
        validity = [[True] * 3 for _ in range(3)]
        validity[1][1] = False
        scan = self._simulate_2d_scan(
            [[0.0, 0.0, 0.0], [0.0, 5.0, 0.0], [0.0, 0.0, 2.0]],
            validity,
            True,
        )

        with mock.patch.object(
                design_agent.E, "build_model", return_value=self._simulate_2d_model()), \
             mock.patch.object(design_agent.E, "scan_2d", return_value=scan), \
             self._patch_simulate_2d_canonical_identity():
            result = design_agent.simulate_2d(
                ["A + B <-> Y"], kd=[2.0], input1="tA", input2="tB",
                observe_species="Y", n_grid=3,
            )

        self.assertIs(result["required_corners_valid"], True)
        self.assertIs(result["partial"], True)
        self.assertIsNone(result["realized_gate"])
        self.assertIsNone(result["interior_peak"])
        self.assertNotIn("_card", result)
        self.assertIn("invalid_or_nonfinite_samples", result["incomplete_reasons"])

    def test_simulate_2d_missing_validity_fails_closed(self):
        scan = self._simulate_2d_scan(
            [[0.0, 0.0], [0.0, 2.0]], [[True, True], [True, True]], False,
        )
        scan.pop("validity_grid")

        with mock.patch.object(
                design_agent.E, "build_model", return_value=self._simulate_2d_model()), \
             mock.patch.object(design_agent.E, "scan_2d", return_value=scan), \
             self._patch_simulate_2d_canonical_identity():
            result = design_agent.simulate_2d(
                ["A + B <-> Y"], kd=[2.0], input1="tA", input2="tB",
                observe_species="Y", n_grid=2,
            )

        self.assertIs(result["partial"], True)
        self.assertEqual(result["validity_grid"], [])
        self.assertIsNone(result["realized_gate"])
        self.assertNotIn("_card", result)
        self.assertIn("missing_or_malformed_validity_grid", result["incomplete_reasons"])

    def test_simulate_2d_extra_malformed_output_row_fails_closed(self):
        scan = self._simulate_2d_scan(
            [[0.0, 0.0], [0.0, 2.0]], [[True, True], [True, True]], False,
        )
        scan["output_grid"].insert(0, None)

        with mock.patch.object(
                design_agent.E, "build_model", return_value=self._simulate_2d_model()), \
             mock.patch.object(design_agent.E, "scan_2d", return_value=scan), \
             self._patch_simulate_2d_canonical_identity():
            result = design_agent.simulate_2d(
                ["A + B <-> Y"], kd=[2.0], input1="tA", input2="tB",
                observe_species="Y", n_grid=2,
            )

        self.assertIs(result["partial"], True)
        self.assertNotIn("_card", result)
        self.assertIn("missing_or_malformed_validity_grid", result["incomplete_reasons"])

    def test_simulate_2d_malformed_axis_population_fails_closed(self):
        scan = self._simulate_2d_scan(
            [[0.0, 0.0], [0.0, 2.0]], [[True, True], [True, True]], False,
        )
        scan["param1_values"] = {0: -6.0, 1: 6.0}

        with mock.patch.object(
                design_agent.E, "build_model", return_value=self._simulate_2d_model()), \
             mock.patch.object(design_agent.E, "scan_2d", return_value=scan), \
             self._patch_simulate_2d_canonical_identity():
            result = design_agent.simulate_2d(
                ["A + B <-> Y"], kd=[2.0], input1="tA", input2="tB",
                observe_species="Y", n_grid=2,
            )

        self.assertIs(result["partial"], True)
        self.assertNotIn("_card", result)
        self.assertEqual(result["surface"]["x"], [])
        self.assertIn("missing_or_malformed_validity_grid",
                      result["incomplete_reasons"])

    def test_simulate_2d_downsampled_response_cannot_become_a_verified_card(self):
        scan = self._simulate_2d_scan(
            [[0.0, 0.0], [0.0, 2.0]], [[True, True], [True, True]], False,
        )

        with mock.patch.object(
                design_agent.E, "build_model", return_value=self._simulate_2d_model()), \
             mock.patch.object(design_agent.E, "scan_2d", return_value=scan), \
             self._patch_simulate_2d_canonical_identity():
            result = design_agent.simulate_2d(
                ["A + B <-> Y"], kd=[2.0], input1="tA", input2="tB",
                observe_species="Y", n_grid=48,
            )

        self.assertIs(result["partial"], True)
        self.assertNotIn("_card", result)
        self.assertIn("response_grid_does_not_match_request", result["incomplete_reasons"])

    def test_simulate_2d_missing_canonical_model_identity_cannot_be_verified(self):
        model = self._simulate_2d_model()
        model.pop("network_ir_hash")
        scan = self._simulate_2d_scan(
            [[0.0, 0.0], [0.0, 2.0]], [[True, True], [True, True]], False,
        )

        with mock.patch.object(design_agent.E, "build_model", return_value=model), \
             mock.patch.object(design_agent.E, "scan_2d", return_value=scan), \
             self._patch_simulate_2d_canonical_identity():
            result = design_agent.simulate_2d(
                ["A + B <-> Y"], kd=[2.0], input1="tA", input2="tB",
                observe_species="Y", n_grid=2,
            )

        self.assertIs(result["partial"], True)
        self.assertNotIn("_card", result)
        self.assertIn("missing_or_invalid_canonical_model_identity",
                      result["incomplete_reasons"])

    def test_simulate_2d_build_model_ir_must_match_caller_network_semantics(self):
        model = self._simulate_2d_model()
        model["network_ir"]["species"][0]["name"] = "C"
        model["network_ir"]["species"][1]["name"] = "D"
        model["network_ir"]["reactions"][0]["formula"] = "C + D <-> Y"
        scan = self._simulate_2d_scan(
            [[0.0, 0.0], [0.0, 2.0]], [[True, True], [True, True]], False,
        )

        def canonical_identity(network):
            if isinstance(network["reactions"][0], str):
                return self._simulate_2d_canonical_identity()
            return {
                "network_ir": copy.deepcopy(model["network_ir"]),
                "network_ir_hash": "b" * 64,
            }

        with mock.patch.object(design_agent.E, "build_model", return_value=model), \
             mock.patch.object(design_agent.E, "scan_2d", return_value=scan), \
             mock.patch.object(
                 design_agent, "_canonical_network_ir_identity",
                 side_effect=canonical_identity,
             ):
            result = design_agent.simulate_2d(
                ["A + B <-> Y"], kd=[2.0], input1="tA", input2="tB",
                observe_species="Y", n_grid=2,
            )

        self.assertIs(result["partial"], True)
        self.assertNotIn("_card", result)
        self.assertIn("missing_or_invalid_canonical_model_identity",
                      result["incomplete_reasons"])

    def test_simulate_2d_model_symbols_must_match_canonical_network_species(self):
        model = self._simulate_2d_model()
        model["q_sym"] = ["tC", "tD"]
        scan = self._simulate_2d_scan(
            [[0.0, 0.0], [0.0, 2.0]], [[True, True], [True, True]], False,
        )
        scan["param1_symbol"] = "tC"
        scan["param2_symbol"] = "tD"

        with mock.patch.object(design_agent.E, "build_model", return_value=model), \
             mock.patch.object(design_agent.E, "scan_2d", return_value=scan), \
             self._patch_simulate_2d_canonical_identity():
            result = design_agent.simulate_2d(
                ["A + B <-> Y"], kd=[2.0], input1="tC", input2="tD",
                observe_species="Y", n_grid=2,
            )

        self.assertIs(result["partial"], True)
        self.assertNotIn("_card", result)
        self.assertIn("missing_or_invalid_canonical_model_identity",
                      result["incomplete_reasons"])

    def test_simulate_2d_all_returned_symbol_populations_are_ir_bound(self):
        base = self._simulate_2d_model()
        variants = []
        for field, value in (
                ("K_sym", ["K1"]),
                ("x_sym", ["A", "B", "Z"]),
                ("species", ["A", "B", "Z"]),
                ("free_species", ["A", "C"]),
                ("product_species", ["Z"])):
            model = copy.deepcopy(base)
            model[field] = value
            variants.append((field, model))

        for field, model in variants:
            with self.subTest(field=field), mock.patch.object(
                    design_agent, "_canonical_network_ir_identity",
                    return_value=self._simulate_2d_canonical_identity()):
                accepted = design_agent._canonical_simulate_2d_model_identity(
                    model, [2.0], self._simulate_2d_canonical_identity(),
                )
            self.assertIs(accepted, False)

    def test_simulate_2d_accepts_canonical_ir_with_different_presentation_order(self):
        model = self._simulate_2d_model()
        model["network_ir"]["species"].reverse()
        scan = self._simulate_2d_scan(
            [[0.0, 0.0], [0.0, 2.0]], [[True, True], [True, True]], False,
        )

        with mock.patch.object(design_agent.E, "build_model", return_value=model), \
             mock.patch.object(design_agent.E, "scan_2d", return_value=scan), \
             self._patch_simulate_2d_canonical_identity():
            result = design_agent.simulate_2d(
                ["A + B <-> Y"], kd=[2.0], input1="tA", input2="tB",
                observe_species="Y", n_grid=2,
            )

        self.assertIs(result["partial"], False)
        self.assertIn("_card", result)

    def test_simulate_2d_model_identity_accepts_cached_reaction_kd_order(self):
        model = self._simulate_2d_model()
        model["network_ir"]["species"].extend([
            {"name": "C", "role": "free", "initial_total": None,
             "unit": "concentration", "metadata": {}},
            {"name": "Z", "role": "bound", "initial_total": None,
             "unit": "concentration", "metadata": {}},
        ])
        model["network_ir"]["reactions"].insert(0, {
            "formula": "B + C <-> Z", "kind": "binding", "kd": 3.0,
            "kd_distribution": None, "reversible": True, "metadata": {},
        })
        model["kd"] = [3.0, 2.0]
        model["K_sym"] = ["Kd1", "Kd2"]
        model["q_sym"] = ["tA", "tB", "tC"]
        model["x_sym"] = ["A", "B", "C", "Y", "Z"]
        model["species"] = ["A", "B", "C", "Y", "Z"]
        model["free_species"] = ["A", "B", "C"]
        model["product_species"] = ["Y", "Z"]
        canonical_request = {
            "network_ir": copy.deepcopy(model["network_ir"]),
            "network_ir_hash": "a" * 64,
        }
        canonical_request["network_ir"]["reactions"].reverse()

        with mock.patch.object(
                design_agent, "_canonical_network_ir_identity",
                return_value={
                    "network_ir": copy.deepcopy(model["network_ir"]),
                    "network_ir_hash": "a" * 64,
                }):
            accepted = design_agent._canonical_simulate_2d_model_identity(
                model, [2.0, 3.0], canonical_request,
            )

        self.assertIs(accepted, True)

    def test_simulate_2d_model_kd_order_must_match_returned_network_ir(self):
        model = self._simulate_2d_model()
        model["network_ir"]["species"].extend([
            {"name": "C", "role": "free", "initial_total": None,
             "unit": "concentration", "metadata": {}},
            {"name": "Z", "role": "bound", "initial_total": None,
             "unit": "concentration", "metadata": {}},
        ])
        model["network_ir"]["reactions"].insert(0, {
            "formula": "B + C <-> Z", "kind": "binding", "kd": 3.0,
            "kd_distribution": None, "reversible": True, "metadata": {},
        })
        model["kd"] = [2.0, 3.0]
        model["K_sym"] = ["Kd1", "Kd2"]
        model["q_sym"] = ["tA", "tB", "tC"]
        model["x_sym"] = ["A", "B", "C", "Y", "Z"]
        model["species"] = ["A", "B", "C", "Y", "Z"]
        model["free_species"] = ["A", "B", "C"]
        model["product_species"] = ["Y", "Z"]
        canonical_request = {
            "network_ir": copy.deepcopy(model["network_ir"]),
            "network_ir_hash": "a" * 64,
        }
        canonical_request["network_ir"]["reactions"].reverse()

        with mock.patch.object(
                design_agent, "_canonical_network_ir_identity",
                return_value={
                    "network_ir": copy.deepcopy(model["network_ir"]),
                    "network_ir_hash": "a" * 64,
                }):
            accepted = design_agent._canonical_simulate_2d_model_identity(
                model, [2.0, 3.0], canonical_request,
            )

        self.assertIs(accepted, False)

    def test_simulate_2d_kd_identity_does_not_use_absolute_numeric_tolerance(self):
        model = self._simulate_2d_model()
        model["network_ir"]["reactions"][0]["kd"] = 1e-12
        model["kd"] = [1e-15]
        scan = self._simulate_2d_scan(
            [[0.0, 0.0], [0.0, 2.0]], [[True, True], [True, True]], False,
        )
        scan["fixed_qK"] = [0.0, 0.0, -15.0]
        canonical_identity = {
            "network_ir": copy.deepcopy(model["network_ir"]),
            "network_ir_hash": "a" * 64,
        }

        with mock.patch.object(design_agent.E, "build_model", return_value=model), \
             mock.patch.object(design_agent.E, "scan_2d", return_value=scan), \
             mock.patch.object(
                 design_agent, "_canonical_network_ir_identity",
                 return_value=canonical_identity,
             ):
            result = design_agent.simulate_2d(
                ["A + B <-> Y"], kd=[1e-12], input1="tA", input2="tB",
                observe_species="Y", n_grid=2,
            )

        self.assertIs(result["partial"], True)
        self.assertNotIn("_card", result)
        self.assertIn("missing_or_invalid_canonical_model_identity",
                      result["incomplete_reasons"])

    def test_simulate_2d_model_hash_must_match_authoritative_caller_hash(self):
        model = self._simulate_2d_model()
        model["network_ir_hash"] = "b" * 64
        model["artifact"]["input_hashes"]["network_ir_hash"] = "b" * 64
        scan = self._simulate_2d_scan(
            [[0.0, 0.0], [0.0, 2.0]], [[True, True], [True, True]], False,
        )

        with mock.patch.object(design_agent.E, "build_model", return_value=model), \
             mock.patch.object(design_agent.E, "scan_2d", return_value=scan), \
             self._patch_simulate_2d_canonical_identity():
            result = design_agent.simulate_2d(
                ["A + B <-> Y"], kd=[2.0], input1="tA", input2="tB",
                observe_species="Y", n_grid=2,
            )

        self.assertIs(result["partial"], True)
        self.assertNotIn("_card", result)
        self.assertIn("missing_or_invalid_canonical_model_identity",
                      result["incomplete_reasons"])

    def test_simulate_2d_request_identity_uses_canonical_ir_owner(self):
        validated_network = copy.deepcopy(self._simulate_2d_model()["network_ir"])
        validated_network["provenance"]["created_at"] = "2026-07-17T00:00:02"
        canonical_response = {
            "valid": True,
            "ir_schema_version": "bne-ir/v1.0.0",
            "network": validated_network,
            "hash": "a" * 64,
        }

        with mock.patch.object(
                design_agent.E, "_post", return_value=canonical_response) as post:
            identity = design_agent._canonical_simulate_2d_request_identity(
                ["A + B <-> Y"], [2.0],
            )

        post.assert_called_once_with(
            "/api/v1/ir/network/validate",
            {"network": {"reactions": ["A + B <-> Y"], "kd": [2.0]}},
            30,
        )
        self.assertEqual(identity, {
            "network_ir": validated_network,
            "network_ir_hash": "a" * 64,
        })

        invalid_responses = [
            {**canonical_response, "valid": False},
            {**canonical_response, "ir_schema_version": "bne-ir/v2.0.0"},
            {**canonical_response, "hash": "not-a-sha256"},
        ]
        for response in invalid_responses:
            with self.subTest(response=response), mock.patch.object(
                    design_agent.E, "_post", return_value=response):
                self.assertIsNone(
                    design_agent._canonical_simulate_2d_request_identity(
                        ["A + B <-> Y"], [2.0],
                    )
                )

    def test_simulate_2d_response_identity_range_and_fixed_context_fail_closed(self):
        base = self._simulate_2d_scan(
            [[0.0, 0.0], [0.0, 2.0]], [[True, True], [True, True]], False,
        )
        variants = []
        wrong_axis = copy.deepcopy(base)
        wrong_axis["param1_symbol"] = "tB"
        variants.append(wrong_axis)
        wrong_range = copy.deepcopy(base)
        wrong_range["param1_values"] = [-5.0, 6.0]
        variants.append(wrong_range)
        wrong_output = copy.deepcopy(base)
        wrong_output["output_expr"] = "Z"
        variants.append(wrong_output)
        wrong_fixed = copy.deepcopy(base)
        wrong_fixed["fixed_qK"][2] = 1.25
        variants.append(wrong_fixed)
        missing_model_hash = copy.deepcopy(base)
        missing_model_hash.pop("network_ir_hash")
        variants.append(missing_model_hash)

        for scan in variants:
            with self.subTest(scan=scan), mock.patch.object(
                    design_agent.E, "build_model", return_value=self._simulate_2d_model()), \
                 mock.patch.object(design_agent.E, "scan_2d", return_value=scan), \
                 self._patch_simulate_2d_canonical_identity():
                result = design_agent.simulate_2d(
                    ["A + B <-> Y"], kd=[2.0], input1="tA", input2="tB",
                    observe_species="Y", n_grid=2,
                )
            self.assertIs(result["partial"], True)
            self.assertNotIn("_card", result)
            self.assertIn("response_identity_does_not_match_request",
                          result["incomplete_reasons"])

    def test_simulate_2d_stale_scan_from_another_model_hash_fails_closed(self):
        scan = self._simulate_2d_scan(
            [[0.0, 0.0], [0.0, 2.0]], [[True, True], [True, True]], False,
        )
        scan["network_ir_hash"] = "b" * 64

        with mock.patch.object(
                design_agent.E, "build_model", return_value=self._simulate_2d_model()), \
             mock.patch.object(design_agent.E, "scan_2d", return_value=scan), \
             self._patch_simulate_2d_canonical_identity():
            result = design_agent.simulate_2d(
                ["A + B <-> Y"], kd=[2.0], input1="tA", input2="tB",
                observe_species="Y", n_grid=2,
            )

        self.assertIs(result["partial"], True)
        self.assertNotIn("_card", result)
        self.assertIn("response_identity_does_not_match_request",
                      result["incomplete_reasons"])

    def test_simulate_2d_request_fingerprint_prevents_cross_request_deduplication(self):
        base_card = {
            "family": "logic",
            "rules": ["A + B <-> Y"],
            "kd": [2.0],
            "realized_gate": "AND",
            "inputs": ["tA", "tB"],
            "output": "Y",
        }
        results = [
            {"_card": {**base_card, "request_fingerprint": "1" * 64}},
            {"_card": {**base_card, "inputs": ["tB", "tA"],
                       "request_fingerprint": "2" * 64}},
        ]

        def fake_simulate_2d(**_kwargs):
            return results.pop(0)

        def fake_runner(_history, _message, dispatch, _cfg, max_iters=12):
            dispatch("simulate_2d", {})
            dispatch("simulate_2d", {})
            return "done", [{"role": "user", "content": "two scans"}]

        with mock.patch.dict(
                design_agent.TOOLS_DISPATCH, {"simulate_2d": fake_simulate_2d}, clear=False), \
             mock.patch.object(design_agent, "_run_anthropic", fake_runner), \
             mock.patch.object(design_agent, "_write_trace", lambda _trace: None), \
             mock.patch.object(design_agent, "_spill_card", lambda _card: "cardhash"):
            response = design_agent.run_turn(
                {}, "two scans",
                {"provider": "anthropic", "api_key": "test-key",
                 "base_url": "https://example.invalid", "model": "test"},
                top=1,
            )

        self.assertEqual(len(response["cards"]), 2)
        self.assertEqual(
            [card["request_fingerprint"] for card in response["cards"]],
            ["1" * 64, "2" * 64],
        )

    def test_engine_client_uses_canonical_routes_and_materializes_reactions_once(self):
        calls = []

        def record(path, payload, timeout):
            calls.append((path, copy.deepcopy(payload), timeout))
            return {"ok": True}

        reactions = (rule for rule in ("A + B <-> AB", "AB + C <-> ABC"))
        with mock.patch.object(design_agent.E, "_post", side_effect=record):
            design_agent.E.build_model(reactions=reactions, timeout=11)
            design_agent.E.dose_response(
                "session", param_symbol="tA", output_exprs=["AB"], timeout=12)
            design_agent.E.scan_2d(
                "session", param1_symbol="tA", param2_symbol="tB",
                output_expr="AB", network_ir_hash="a" * 64, timeout=13)
            design_agent.E.phenotype_classify(
                "session", input_symbol="tA", output_expr="AB", timeout=14)
            design_agent.E.phenotype(
                "session", change_qK="tA", observe_x="AB", timeout=15)

        self.assertEqual([call[0] for call in calls], [
            "/api/v1/build_model",
            "/api/v1/parameter_scan_1d",
            "/api/v1/parameter_scan_2d",
            "/api/v1/phenotype_classify",
            "/api/v1/behavior_families",
        ])
        self.assertEqual(calls[0][1]["reactions"], [
            "A + B <-> AB", "AB + C <-> ABC",
        ])
        self.assertEqual(calls[0][1]["kd"], [1.0, 1.0])
        self.assertEqual(calls[2][1]["network_ir_hash"], "a" * 64)

    def test_engine_ready_requires_exact_ready_payload(self):
        class Response(io.BytesIO):
            status = 200

            def __enter__(self):
                return self

            def __exit__(self, *_args):
                self.close()

            def getcode(self):
                return self.status

        for payload, expected in (
            ({"status": "ready"}, True),
            ({"status": "starting"}, False),
            ({"ok": True}, False),
        ):
            response = Response(json.dumps(payload).encode("utf-8"))
            with self.subTest(payload=payload), mock.patch.object(
                    design_agent.E.urllib.request, "urlopen", return_value=response):
                self.assertEqual(design_agent.E.engine_ready(), expected)

    def test_engine_post_returns_typed_error_for_invalid_success_json(self):
        class Response(io.BytesIO):
            status = 200

            def __enter__(self):
                return self

            def __exit__(self, *_args):
                self.close()

            def getcode(self):
                return self.status

        response = Response(b"not-json")
        with mock.patch.object(
                design_agent.E.urllib.request, "urlopen", return_value=response):
            result = design_agent.E._post("/api/v1/build_model", {}, 1)

        self.assertEqual(result["_status"], 200)
        self.assertIn("returned invalid JSON", result["error"])

    def test_end_to_end_design_tool_is_exposed_to_both_provider_protocols(self):
        self.assertIn("design_from_behavior", design_agent.TOOLS_DISPATCH)
        self.assertIn("design_from_behavior", [tool["name"] for tool in design_agent.TOOLSPEC])
        self.assertIn("design_from_behavior", [tool["function"]["name"] for tool in design_agent.OPENAI_TOOLS])
        self.assertIn("design_from_behavior", [tool["name"] for tool in design_agent.ANTHROPIC_TOOLS])

    def test_rop_shape_optimizer_tool_is_exposed_and_client_uses_canonical_endpoint(self):
        self.assertIn("optimize_rop_shape", design_agent.TOOLS_DISPATCH)
        self.assertIn("optimize_rop_shape", [tool["name"] for tool in design_agent.TOOLSPEC])
        self.assertIn("optimize_rop_shape", [tool["function"]["name"] for tool in design_agent.OPENAI_TOOLS])
        self.assertIn("optimize_rop_shape", [tool["name"] for tool in design_agent.ANTHROPIC_TOOLS])

        request = {"schema_version": "bne-rop-shape-optimize-request/v1.0.0"}
        with mock.patch.object(design_agent.E, "_post", return_value={"ok": True}) as post:
            response = design_agent.E.rop_shape_optimize(request, timeout=123)

        self.assertEqual(response, {"ok": True})
        post.assert_called_once_with("/api/v1/rop_shape_optimize", request, 123)

    def test_optimize_rop_shape_forwards_typed_request_and_admits_only_backend_evidence(self):
        seen = {}

        def fake_optimize(request):
            seen["request"] = copy.deepcopy(request)
            return rop_shape_backend_response()

        with mock.patch.object(design_agent.E, "rop_shape_optimize", fake_optimize), \
             mock.patch.object(design_agent, "_design_finite_slopes", side_effect=AssertionError("Python slope verifier must not run")):
            result = design_agent.optimize_rop_shape(
                copy.deepcopy(ROP_SHAPE_NETWORK),
                copy.deepcopy(ROP_SHAPE_SPEC),
                copy.deepcopy(ROP_SHAPE_REFERENCE),
                copy.deepcopy(ROP_SHAPE_INTENT),
                0.1,
                0.02,
                copy.deepcopy(ROP_SHAPE_WORK_BUDGET),
                copy.deepcopy(ROP_SHAPE_REPLAY),
            )

        request = seen["request"]
        self.assertEqual(request["schema_version"], "bne-rop-shape-optimize-request/v1.0.0")
        self.assertEqual(request["network"], {
            "reactions": ["A + B <-> C_A_B"],
            "kd": [1.0],
            "input_symbols": ["tA"],
            "output_symbols": ["C_A_B"],
        })
        self.assertIsNone(request["expected_network_ir_hash"])
        self.assertEqual(request["designability_spec"], ROP_SHAPE_SPEC)
        self.assertEqual(request["reference"], ROP_SHAPE_REFERENCE)
        self.assertEqual(request["edit_intent"], ROP_SHAPE_INTENT)
        self.assertNotIn("matrix", request["edit_intent"])
        self.assertEqual(request["optimization"], {
            "minimum_parameter_margin": 0.1,
            "effect_tolerance": 0.02,
        })
        self.assertEqual(request["work_budget"], ROP_SHAPE_WORK_BUDGET)
        self.assertEqual(request["replay"], ROP_SHAPE_REPLAY)

        self.assertEqual(result["admission_status"], "verified_card")
        self.assertEqual(result["compiled_edit"], rop_shape_backend_response()["compiled_edit"])
        self.assertEqual(result["artifact"], rop_shape_backend_response()["artifact"])
        card = result["_card"]
        self.assertEqual(card["verdict"], "verified_rop_shape_optimization")
        self.assertEqual(card["rules"], ["A + B <-> C_A_B"])
        self.assertEqual(card["kd"], [0.5])
        self.assertEqual(card["input_symbol"], "tA")
        self.assertEqual(card["output_symbol"], "C_A_B")
        self.assertEqual(len(card["computed_series"]), 21)
        self.assertEqual(card["compiled_edit"], result["compiled_edit"])
        self.assertEqual(card["artifact"], result["artifact"])

    def test_optimize_rop_shape_accepts_schema_valid_positional_network_identity(self):
        response = rop_shape_backend_response()
        response["fixed_topology"]["network_canonical_code"] = None
        response["fixed_topology"]["network_identity_semantics"] = "positional_content_hash_only"

        with mock.patch.object(design_agent.E, "rop_shape_optimize", return_value=response):
            result = design_agent.optimize_rop_shape(
                copy.deepcopy(ROP_SHAPE_NETWORK),
                copy.deepcopy(ROP_SHAPE_SPEC),
                copy.deepcopy(ROP_SHAPE_REFERENCE),
                copy.deepcopy(ROP_SHAPE_INTENT),
                0.1,
                0.02,
                copy.deepcopy(ROP_SHAPE_WORK_BUDGET),
                copy.deepcopy(ROP_SHAPE_REPLAY),
            )

        self.assertEqual(result["admission_status"], "verified_card")
        self.assertIsNone(result["_card"]["network_canonical_code"])
        self.assertEqual(result["_card"]["network_identity_semantics"], "positional_content_hash_only")

    def test_optimize_rop_shape_forwards_full_network_ir_without_lowering_it(self):
        network_ir = {
            "ir_schema_version": "bne-ir/v1.0.0",
            "label": "caller-owned-network",
            "species": [{"id": "A"}],
            "reactions": [{"id": "r1"}],
            "observables": [{"id": "C_A_B"}],
            "parameter_distributions": [{"id": "kd1"}],
            "compartments": [],
            "provenance": {},
            "extensions": {},
        }
        wrapped_network = {
            "network_ir": network_ir,
            "expected_network_ir_hash": "b" * 64,
        }
        seen = {}

        def fake_optimize(request):
            seen["request"] = copy.deepcopy(request)
            return rop_shape_backend_response()

        with mock.patch.object(design_agent.E, "rop_shape_optimize", fake_optimize):
            result = design_agent.optimize_rop_shape(
                copy.deepcopy(wrapped_network),
                copy.deepcopy(ROP_SHAPE_SPEC),
                copy.deepcopy(ROP_SHAPE_REFERENCE),
                copy.deepcopy(ROP_SHAPE_INTENT),
                0.1,
                0.02,
                copy.deepcopy(ROP_SHAPE_WORK_BUDGET),
                copy.deepcopy(ROP_SHAPE_REPLAY),
            )

        self.assertEqual(seen["request"]["network"], network_ir)
        self.assertEqual(seen["request"]["expected_network_ir_hash"], "b" * 64)
        self.assertNotIn("rules", seen["request"]["network"])
        self.assertEqual(result["admission_status"], "verified_card")

    def test_optimize_rop_shape_withholds_cards_for_noncanonical_or_incomplete_evidence(self):
        base = rop_shape_backend_response()
        cases = []

        stale = copy.deepcopy(base)
        stale["schema_version"] = "bne-rop-shape-optimization/v0.9.0"
        cases.append(("stale_schema", stale))

        local_only = copy.deepcopy(base)
        local_only["geometric_status"] = "optimal_over_evaluated_cells"
        cases.append(("not_global", local_only))

        truncated = copy.deepcopy(base)
        truncated["coverage"]["truncated"] = True
        cases.append(("truncated", truncated))

        no_selected = copy.deepcopy(base)
        no_selected["selected"] = None
        cases.append(("missing_selected", no_selected))

        incomplete = copy.deepcopy(base)
        incomplete["replay"]["complete"] = False
        cases.append(("incomplete_replay", incomplete))

        failed = copy.deepcopy(base)
        failed["replay"]["pass"] = False
        cases.append(("failed_replay", failed))

        loose_validity = copy.deepcopy(base)
        loose_validity["replay"]["curve"]["valid"][-1] = 1
        cases.append(("nonliteral_validity", loose_validity))

        partial = copy.deepcopy(base)
        partial["replay"]["curve"]["partial"] = True
        cases.append(("partial_curve", partial))

        legacy_replay_request = copy.deepcopy(base)
        legacy_replay_request["replay"]["request"] = {
            "endpoint": "/api/v1/placer_curve",
            **legacy_replay_request["replay"]["request"]["body"],
        }
        cases.append(("legacy_flat_replay_request", legacy_replay_request))

        noncanonical_replay_endpoint = copy.deepcopy(base)
        noncanonical_replay_endpoint["replay"]["request"]["endpoint"] = "/api/placer_curve"
        cases.append(("noncanonical_replay_endpoint", noncanonical_replay_endpoint))

        legacy_fixed_topology = copy.deepcopy(base)
        legacy_fixed_topology["fixed_topology"]["network"] = \
            legacy_fixed_topology["fixed_topology"].pop("normalized_network")
        legacy_fixed_topology["fixed_topology"].pop("topology_preserved")
        cases.append(("legacy_fixed_topology", legacy_fixed_topology))

        for name, response in cases:
            with self.subTest(name=name), \
                 mock.patch.object(design_agent.E, "rop_shape_optimize", return_value=response):
                result = design_agent.optimize_rop_shape(
                    copy.deepcopy(ROP_SHAPE_NETWORK),
                    copy.deepcopy(ROP_SHAPE_SPEC),
                    copy.deepcopy(ROP_SHAPE_REFERENCE),
                    copy.deepcopy(ROP_SHAPE_INTENT),
                    0.1,
                    0.02,
                    copy.deepcopy(ROP_SHAPE_WORK_BUDGET),
                    copy.deepcopy(ROP_SHAPE_REPLAY),
                )

            self.assertEqual(result["admission_status"], "withheld")
            self.assertIn("error", result)
            self.assertNotIn("_card", result)
            self.assertEqual(result.get("compiled_edit"), response.get("compiled_edit"))
            self.assertEqual(result.get("artifact"), response.get("artifact"))

    def test_optimize_rop_shape_rejects_unpinned_or_precompiled_requests_before_backend(self):
        invalid_cases = [
            {
                "name": "unknown_edit",
                "network": ROP_SHAPE_NETWORK,
                "reference": ROP_SHAPE_REFERENCE,
                "edit_intent": {"kind": "make_pretty"},
            },
            {
                "name": "precompiled_matrix",
                "network": ROP_SHAPE_NETWORK,
                "reference": ROP_SHAPE_REFERENCE,
                "edit_intent": {"kind": "linear_witness", "Aeq": [[1, 0]]},
            },
            {
                "name": "incomplete_typed_intent",
                "network": ROP_SHAPE_NETWORK,
                "reference": ROP_SHAPE_REFERENCE,
                "edit_intent": {
                    "id": "missing-preservation-contract",
                    "kind": "separate",
                    "steps": [0, 2],
                },
            },
            {
                "name": "unpinned_reference",
                "network": ROP_SHAPE_NETWORK,
                "reference": {
                    "operating_points_log10": [-3.0, 0.0, 3.0],
                    "kd": [1.0],
                    "totals": {"tA": 1.0, "tB": 1.0},
                },
                "edit_intent": ROP_SHAPE_INTENT,
            },
            {
                "name": "rules_without_io_identity",
                "network": {"rules": ["A + B <-> C_A_B"]},
                "reference": ROP_SHAPE_REFERENCE,
                "edit_intent": ROP_SHAPE_INTENT,
            },
            {
                "name": "empty_reference_totals",
                "network": ROP_SHAPE_NETWORK,
                "reference": {**ROP_SHAPE_REFERENCE, "totals": {}},
                "edit_intent": ROP_SHAPE_INTENT,
            },
            {
                "name": "job_cell_cap_exceeded",
                "network": ROP_SHAPE_NETWORK,
                "reference": ROP_SHAPE_REFERENCE,
                "edit_intent": ROP_SHAPE_INTENT,
                "work_budget": {**ROP_SHAPE_WORK_BUDGET, "max_cells": 10001},
            },
            {
                "name": "job_replay_cap_exceeded",
                "network": ROP_SHAPE_NETWORK,
                "reference": ROP_SHAPE_REFERENCE,
                "edit_intent": ROP_SHAPE_INTENT,
                "work_budget": {**ROP_SHAPE_WORK_BUDGET, "max_replays": 17},
            },
            {
                "name": "replay_window_outside_supported_domain",
                "network": ROP_SHAPE_NETWORK,
                "reference": ROP_SHAPE_REFERENCE,
                "edit_intent": ROP_SHAPE_INTENT,
                "replay": {**ROP_SHAPE_REPLAY, "input_window_log10": [-20.1, 5.0]},
            },
        ]
        for case in invalid_cases:
            with self.subTest(name=case["name"]), \
                 mock.patch.object(design_agent.E, "rop_shape_optimize") as optimize:
                result = design_agent.optimize_rop_shape(
                    copy.deepcopy(case["network"]),
                    copy.deepcopy(ROP_SHAPE_SPEC),
                    copy.deepcopy(case["reference"]),
                    copy.deepcopy(case["edit_intent"]),
                    0.1,
                    0.02,
                    copy.deepcopy(case.get("work_budget", ROP_SHAPE_WORK_BUDGET)),
                    copy.deepcopy(case.get("replay", ROP_SHAPE_REPLAY)),
                )

            self.assertIn("error", result)
            self.assertNotIn("_card", result)
            optimize.assert_not_called()

        with mock.patch.object(design_agent.E, "rop_shape_optimize") as optimize:
            result = design_agent.optimize_rop_shape(
                copy.deepcopy(ROP_SHAPE_NETWORK),
                copy.deepcopy(ROP_SHAPE_SPEC),
                copy.deepcopy(ROP_SHAPE_REFERENCE),
                copy.deepcopy(ROP_SHAPE_INTENT),
                0.1,
                0.02,
                copy.deepcopy(ROP_SHAPE_WORK_BUDGET),
                copy.deepcopy(ROP_SHAPE_REPLAY),
                endpoint="/api/rop_shape_optimize",
            )

        self.assertIn("unsupported fields", result["error"])
        self.assertNotIn("_card", result)
        optimize.assert_not_called()

    def test_run_turn_admits_verified_rop_shape_card_from_allowlisted_tool(self):
        card = {
            "family": "dose_shape",
            "verdict": "verified_rop_shape_optimization",
            "rules": ["A + B <-> C_A_B"],
            "kd": [0.5],
            "totals": {"tA": 1.0, "tB": 2.0},
            "input_symbol": "tA",
            "output_symbol": "C_A_B",
            "computed_series": [{"x": -5.0, "y": -2.0}, {"x": 5.0, "y": 0.0}],
            "compiled_edit": {"compiler_version": "rop-shape-edit/v1"},
            "artifact": {"kind": "rop_shape_optimize"},
            "designability_spec": copy.deepcopy(ROP_SHAPE_SPEC),
        }

        def fake_optimize(**_kwargs):
            return {
                "designability_spec": copy.deepcopy(ROP_SHAPE_SPEC),
                "_card": copy.deepcopy(card),
            }

        def fake_runner(_history, _message, dispatch, _cfg, max_iters=12):
            dispatch("optimize_rop_shape", {})
            return "优化完成", [{"role": "user", "content": "拉开两个峰"}]

        with mock.patch.dict(design_agent.TOOLS_DISPATCH, {"optimize_rop_shape": fake_optimize}, clear=False), \
             mock.patch.object(design_agent, "_run_anthropic", fake_runner), \
             mock.patch.object(design_agent, "_write_trace", lambda _trace: None), \
             mock.patch.object(design_agent, "_spill_card", lambda _card: "cardhash"):
            result = design_agent.run_turn(
                {}, "拉开两个峰",
                {"provider": "anthropic", "api_key": "test-key", "base_url": "https://example.invalid", "model": "test"},
                top=1,
            )

        self.assertEqual(result["reply"], "优化完成")
        self.assertEqual(len(result["cards"]), 1)
        self.assertEqual(result["cards"][0]["verdict"], "verified_rop_shape_optimization")
        self.assertEqual(result["cards"][0]["compiled_edit"], card["compiled_edit"])
        self.assertEqual(result["cards"][0]["artifact"], card["artifact"])

    def test_anthropic_compatible_env_aliases_configure_glm(self):
        with mock.patch.dict(os.environ, {
            "ANTHROPIC_BASE_URL": "https://api.z.ai/api/anthropic",
            "ANTHROPIC_AUTH_TOKEN": "redacted-test-token",
            "ANTHROPIC_MODEL": "glm-5.2",
        }, clear=True):
            cfg = llm_compile.llm_config_from_env()

        self.assertEqual(cfg["provider"], "anthropic")
        self.assertEqual(cfg["base_url"], "https://api.z.ai/api/anthropic")
        self.assertEqual(cfg["api_key"], "redacted-test-token")
        self.assertEqual(cfg["model"], "glm-5.2")

    def test_design_chat_uses_anthropic_env_aliases_when_ui_llm_config_is_blank(self):
        seen_cfg = {}

        def fake_runner(_history, _message, _dispatch, cfg, max_iters=12):
            seen_cfg.update(cfg)
            return "env configured", [{"role": "user", "content": "hello"}]

        blank_ui_llm = {
            "provider": "openai",
            "apiKey": "",
            "baseUrl": "",
            "model": "",
        }

        with mock.patch.dict(os.environ, {
            "ANTHROPIC_BASE_URL": "https://api.z.ai/api/anthropic",
            "ANTHROPIC_AUTH_TOKEN": "redacted-test-token",
            "ANTHROPIC_MODEL": "glm-5.2",
        }, clear=True), \
             mock.patch.object(design_agent, "_run_anthropic", fake_runner), \
             mock.patch.object(design_agent, "_run_openai", mock.Mock(side_effect=AssertionError("openai runner should not be used"))), \
             mock.patch.object(design_agent, "_write_trace", lambda _trace: None):
            res = design_agent.run_turn({}, "hello", chat_api._norm_llm(blank_ui_llm), top=1)

        self.assertEqual(res["kind"], "agent")
        self.assertEqual(res["reply"], "env configured")
        self.assertEqual(seen_cfg["provider"], "anthropic")
        self.assertEqual(seen_cfg["api_key"], "redacted-test-token")
        self.assertEqual(seen_cfg["base_url"], "https://api.z.ai/api/anthropic")
        self.assertEqual(seen_cfg["model"], "glm-5.2")

    def test_agent_compiled_behavior_spec_becomes_shared_designability_spec(self):
        card = {
            "family": "dose_shape",
            "rules": ["A + A <-> C_A_A"],
            "kd": [1.0],
            "input_symbol": "tA",
            "output_symbol": "C_A_A",
            "shape_support": 1.0,
        }

        def fake_simulate(**_kwargs):
            return {
                "compiled_spec": EXECUTABLE_BEHAVIOR_SPEC,
                "_card": dict(card),
            }

        def fake_runner(_history, _message, dispatch, _cfg, max_iters=12):
            dispatch("simulate", {})
            return "done", [{"role": "user", "content": "design"}]

        with mock.patch.dict(design_agent.TOOLS_DISPATCH, {"simulate": fake_simulate}, clear=False), \
             mock.patch.object(design_agent, "_run_anthropic", fake_runner), \
             mock.patch.object(design_agent, "_write_trace", lambda _trace: None), \
             mock.patch.object(design_agent, "_spill_card", lambda _card: "cardhash"):
            res = design_agent.run_turn(
                {},
                "design",
                {
                    "provider": "anthropic",
                    "api_key": "test-key",
                    "base_url": "https://example.invalid",
                    "model": "glm-5.2",
                },
                top=1,
            )

        spec = res.get("designability_spec")
        self.assertIsInstance(spec, dict)
        self.assertEqual(spec["schema_version"], "bne-designability/v1.0.0")
        self.assertEqual(spec["source"]["kind"], "agent_design")
        self.assertEqual(spec["target"]["behavior_spec"]["input"], "tA")
        self.assertEqual(spec["target"]["behavior_spec"]["output"], "C_A_A")
        self.assertEqual(spec["target"]["behavior_spec"]["program"][0]["value"], 1.0)
        self.assertEqual(spec["constraints"]["parameter_bounds"]["kd_log10"], [-2, 2])
        self.assertEqual(spec["constraints"]["network"]["max_reactions"], 4)
        self.assertEqual(res["cards"][0]["designability_spec"], spec)

    def test_agent_refuses_candidate_when_compiled_spec_has_invalid_sampled_constraint(self):
        invalid_spec = copy.deepcopy(EXECUTABLE_BEHAVIOR_SPEC)
        invalid_spec["shape_preferences"] = {"dynamic_range_log10": {"min": 1.2}}
        card = {
            "family": "dose_shape",
            "rules": ["A + A <-> C_A_A"],
            "kd": [1.0],
            "input_symbol": "tA",
            "output_symbol": "C_A_A",
            "shape_support": 1.0,
        }

        def fake_simulate(**_kwargs):
            return {
                "compiled_spec": invalid_spec,
                "_card": dict(card),
            }

        def fake_runner(_history, _message, dispatch, _cfg, max_iters=12):
            dispatch("simulate", {})
            return "done", [{"role": "user", "content": "design"}]

        with mock.patch.dict(design_agent.TOOLS_DISPATCH, {"simulate": fake_simulate}, clear=False), \
             mock.patch.object(design_agent, "_run_anthropic", fake_runner), \
             mock.patch.object(design_agent, "_write_trace", lambda _trace: None), \
             mock.patch.object(design_agent, "_spill_card", lambda _card: "cardhash"):
            res = design_agent.run_turn(
                {},
                "design",
                {
                    "provider": "anthropic",
                    "api_key": "test-key",
                    "base_url": "https://example.invalid",
                    "model": "glm-5.2",
                },
                top=1,
            )

        self.assertEqual(res["cards"], [])
        self.assertNotIn("designability_spec", res)

    def test_agent_refuses_to_export_behavior_class_only_spec(self):
        raw = {
            "schema_version": "bne-behavior/v0.1.0",
            "goal": {
                "behavior_family": "dose_shape",
                "behavior_class": "bandpass_with_plateau",
            },
        }

        self.assertIsNone(design_agent._agent_designability_spec_from_payload(raw, None))

    def test_agent_refuses_to_export_behavior_spec_with_bad_constraint_types(self):
        cases = [
            {"network_constraints": {"max_reactions": "4"}},
            {"network_constraints": {"max_base_species": True}},
            {"network_constraints": {"kd_profile": {"log10_kd_min": "-2", "log10_kd_max": 2}}},
            {"shape_preferences": {"dynamic_range_log10": {"min": False}}},
            {"shape_preferences": {"dynamic_range_log10": {"min": 1.2}}},
            {"shape_preferences": {"dynamic_range_log10": {"min": 1.2, "sample_points": 81}}},
            {"shape_preferences": {"dynamic_range_log10": {"min": 1.0e308, "sample_points": 81}}},
            {"shape_preferences": {"dynamic_range_log10": {"max": 2}}},
            {"shape_preferences": {"dynamic_range_log10": {"range": [0, 2]}}},
        ]
        for extra in cases:
            raw = copy.deepcopy(EXECUTABLE_BEHAVIOR_SPEC)
            for key, value in extra.items():
                raw[key] = value

            self.assertIsNone(design_agent._agent_designability_spec_from_payload(raw, None))

    def test_wrapped_designability_spec_accepts_json_integer_floats(self):
        spec = {
            "schema_version": "bne-designability/v1.0.0",
            "source": {"kind": "agent_design", "node_id": "agent-node"},
            "target": {
                "behavior_spec": {
                    "feature_space": "reaction_order",
                    "input": "tA",
                    "output": "C_A_A",
                    "program": [
                        {"kind": "reaction_order", "value": 1.0},
                        {"kind": "reaction_order", "value": 0.0},
                    ],
                    "input_window": {"input_log10": [-2, 2]},
                },
            },
            "constraints": {
                "parameter_bounds": {"kd_log10": [-2, 2]},
                "dynamic_range": {"min_fold_change": 2.0, "sample_points": 51.0},
                "transitions": {"min_spacing_decades": 0.5, "order": [0.0, 1.0]},
            },
            "candidate_budget": {
                "max_exact_placements": 3.0,
                "max_verified_recommendations": 2.0,
            },
            "ranking_policy": {"prefer": ["dynamic_range", "transition_spacing"]},
            "audit_policy": {"unsupported": "block_if_hard", "path_format": "json_pointer"},
        }

        out = design_agent._agent_designability_spec_from_payload(spec, None)

        self.assertIsInstance(out, dict)
        self.assertEqual(out["source"]["kind"], "agent_design")

    def test_agent_refuses_lossy_behavior_spec_lowering(self):
        cases = []

        null_operator = copy.deepcopy(EXECUTABLE_BEHAVIOR_SPEC)
        null_operator["behavior_spec"]["program"][0]["operator"] = None
        cases.append(null_operator)

        empty_operator = copy.deepcopy(EXECUTABLE_BEHAVIOR_SPEC)
        empty_operator["behavior_spec"]["program"][0]["operator"] = ""
        cases.append(empty_operator)

        unknown_step_field = copy.deepcopy(EXECUTABLE_BEHAVIOR_SPEC)
        unknown_step_field["behavior_spec"]["program"][0]["at"] = 0.5
        cases.append(unknown_step_field)

        unknown_behavior_field = copy.deepcopy(EXECUTABLE_BEHAVIOR_SPEC)
        unknown_behavior_field["behavior_spec"]["at"] = 0.5
        cases.append(unknown_behavior_field)

        null_input_window = copy.deepcopy(EXECUTABLE_BEHAVIOR_SPEC)
        null_input_window["behavior_spec"]["input_window"] = None
        cases.append(null_input_window)

        bad_input_window = copy.deepcopy(EXECUTABLE_BEHAVIOR_SPEC)
        bad_input_window["behavior_spec"]["input_window"] = {"input_log10": [-2]}
        cases.append(bad_input_window)

        unknown_shape_preference = copy.deepcopy(EXECUTABLE_BEHAVIOR_SPEC)
        unknown_shape_preference["shape_preferences"] = {"rise_slope": {"min": 0.5}}
        cases.append(unknown_shape_preference)

        for raw in cases:
            self.assertIsNone(design_agent._agent_designability_spec_from_payload(raw, None))

    def test_existing_designability_spec_from_agent_forces_agent_source_kind(self):
        raw = {
            "schema_version": "bne-designability/v1.0.0",
            "source": {"kind": "manual_config", "node_id": "design-spec-config-1"},
            "target": {
                "behavior_spec": {
                    "input": "tA",
                    "output": "C_A_A",
                    "program": [
                        {"kind": "reaction_order", "operator": "=", "value": 1.0},
                    ],
                },
            },
        }

        spec = design_agent._agent_designability_spec_from_payload(raw, None)

        self.assertEqual(spec["source"]["kind"], "agent_design")
        self.assertEqual(spec["source"]["node_id"], "design-spec-config-1")

    def test_existing_designability_spec_with_invalid_sampled_clause_is_rejected(self):
        raw = {
            "schema_version": "bne-designability/v1.0.0",
            "source": {"kind": "agent_design"},
            "target": {
                "behavior_spec": {
                    "input": "tA",
                    "output": "C_A_A",
                    "program": [{"kind": "reaction_order", "value": 1.0}],
                    "input_window": {"input_log10": [-2, 2]},
                },
            },
            "constraints": {
                "dynamic_range": {"min_fold_change": 10.0},
            },
        }

        self.assertIsNone(design_agent._agent_designability_spec_from_payload(raw, None))

    def test_existing_designability_spec_with_unsatisfied_sampled_solver_prerequisites_is_rejected(self):
        valid = {
            "schema_version": "bne-designability/v1.0.0",
            "source": {"kind": "agent_design"},
            "target": {
                "behavior_spec": {
                    "input": "tA",
                    "output": "C_A_A",
                    "program": [
                        {"kind": "reaction_order", "value": 1.0},
                        {"kind": "reaction_order", "value": 0.0},
                    ],
                    "input_window": {"input_log10": [-2, 2]},
                },
            },
            "constraints": {
                "parameter_bounds": {"kd_log10": [-2, 2]},
            },
        }
        cases = []

        bad_program = copy.deepcopy(valid)
        bad_program["target"]["behavior_spec"] = {
            "input": "tA",
            "output": "C_A_A",
            "program": ["bad"],
            "input_window": {"input_log10": [-2, 2]},
        }
        bad_program["constraints"]["dynamic_range"] = {
            "min_fold_change": 2.0,
            "sample_points": 51,
        }
        cases.append(bad_program)

        missing_input = copy.deepcopy(valid)
        missing_input["target"]["behavior_spec"] = {
            "output": "C_A_A",
            "program": [{"kind": "reaction_order", "value": 1.0}],
            "input_window": {"input_log10": [-2, 2]},
        }
        missing_input["constraints"]["dynamic_range"] = {
            "min_fold_change": 2.0,
            "sample_points": 51,
        }
        cases.append(missing_input)

        bad_feature_space = copy.deepcopy(valid)
        bad_feature_space["target"]["behavior_spec"] = {
            "feature_space": "concentration",
            "input": "tA",
            "output": "C_A_A",
            "program": [{"kind": "reaction_order", "value": 1.0}],
            "input_window": {"input_log10": [-2, 2]},
        }
        bad_feature_space["constraints"]["dynamic_range"] = {
            "min_fold_change": 2.0,
            "sample_points": 51,
        }
        cases.append(bad_feature_space)

        bad_operator = copy.deepcopy(valid)
        bad_operator["target"]["output_feature"] = {
            "feature": "threshold",
            "operator": "between",
            "value": 0.5,
            "sample_points": 51,
            "tolerance_log10": 0.01,
        }
        cases.append(bad_operator)

        bad_fold_change = copy.deepcopy(valid)
        bad_fold_change["target"]["output_feature"] = {
            "feature": "fold_change",
            "operator": "=",
            "value": 0.0,
            "sample_points": 51,
            "tolerance_log10": 0.01,
        }
        cases.append(bad_fold_change)

        missing_output_window = copy.deepcopy(valid)
        del missing_output_window["target"]["behavior_spec"]["input_window"]
        missing_output_window["target"]["output_feature"] = {
            "feature": "threshold",
            "operator": ">=",
            "value": 0.5,
            "sample_points": 51,
            "tolerance_log10": 0.01,
        }
        cases.append(missing_output_window)

        missing_dynamic_window = copy.deepcopy(valid)
        del missing_dynamic_window["target"]["behavior_spec"]["input_window"]
        missing_dynamic_window["constraints"]["dynamic_range"] = {
            "min_fold_change": 2.0,
            "sample_points": 51,
        }
        cases.append(missing_dynamic_window)

        bad_operating_points = copy.deepcopy(valid)
        bad_operating_points["target"]["behavior_spec"] = {
            "input": "tA",
            "output": "C_A_A",
            "program": [{"kind": "reaction_order", "value": 1.0}],
            "input_window": {
                "input_log10": [-2, 2],
                "operating_points_log10": [-1, 1],
            },
        }
        cases.append(bad_operating_points)

        bad_operating_point_value = copy.deepcopy(valid)
        bad_operating_point_value["target"]["behavior_spec"] = {
            "input": "tA",
            "output": "C_A_A",
            "program": [{"kind": "reaction_order", "value": 1.0}],
            "input_window": {
                "input_log10": [-2, 2],
                "operating_points_log10": ["bad"],
            },
        }
        cases.append(bad_operating_point_value)

        out_of_bounds_operating_point = copy.deepcopy(valid)
        out_of_bounds_operating_point["target"]["behavior_spec"]["input_window"] = {
            "input_log10": [-2, 2],
            "operating_points_log10": [-3, 0],
        }
        cases.append(out_of_bounds_operating_point)

        underspaced_operating_points = copy.deepcopy(valid)
        underspaced_operating_points["target"]["behavior_spec"]["input_window"] = {
            "input_log10": [-2, 2],
            "operating_points_log10": [-1, 0],
        }
        underspaced_operating_points["constraints"]["transitions"] = {
            "min_spacing_decades": 1.5,
        }
        cases.append(underspaced_operating_points)

        missing_operating_window = copy.deepcopy(valid)
        missing_operating_window["target"]["behavior_spec"] = {
            "input": "tA",
            "output": "C_A_A",
            "program": [{"kind": "reaction_order", "value": 1.0}],
            "input_window": {
                "operating_points_log10": [0.0],
            },
        }
        cases.append(missing_operating_window)

        bad_input_spacing = copy.deepcopy(valid)
        bad_input_spacing["target"]["behavior_spec"]["input_window"] = {
            "input_log10": [-2, 2],
            "min_spacing_decades": "bad",
        }
        cases.append(bad_input_spacing)

        bad_transition_order = copy.deepcopy(valid)
        bad_transition_order["target"]["behavior_spec"] = {
            "input": "tA",
            "output": "C_A_A",
            "program": [{"kind": "reaction_order", "value": 1.0}],
            "input_window": {"input_log10": [-2, 2]},
        }
        bad_transition_order["constraints"]["transitions"] = {"order": [0, 1]}
        cases.append(bad_transition_order)

        bad_transition_spacing = copy.deepcopy(valid)
        bad_transition_spacing["target"]["behavior_spec"] = {
            "input": "tA",
            "output": "C_A_A",
            "program": [{"kind": "reaction_order", "value": 1.0}],
            "input_window": {"input_log10": [-2, 2]},
        }
        bad_transition_spacing["constraints"]["transitions"] = {"min_spacing_decades": 0.5}
        cases.append(bad_transition_spacing)

        missing_transition_window = copy.deepcopy(valid)
        del missing_transition_window["target"]["behavior_spec"]["input_window"]
        missing_transition_window["constraints"]["transitions"] = {"order": [0, 1]}
        cases.append(missing_transition_window)

        bad_wrapped_input_window = copy.deepcopy(valid)
        bad_wrapped_input_window["target"]["behavior_spec"]["input_window"] = None
        cases.append(bad_wrapped_input_window)

        bad_wrapped_input_window_text = copy.deepcopy(valid)
        bad_wrapped_input_window_text["target"]["behavior_spec"]["input_window"] = "bad"
        cases.append(bad_wrapped_input_window_text)

        for spec in cases:
            self.assertIsNone(design_agent._agent_designability_spec_from_payload(spec, None))

    def test_existing_designability_spec_with_invalid_wrapper_schema_clauses_is_rejected(self):
        valid = {
            "schema_version": "bne-designability/v1.0.0",
            "source": {"kind": "agent_design", "node_id": "agent-node"},
            "target": {
                "behavior_spec": {
                    "feature_space": "reaction_order",
                    "input": "tA",
                    "output": "C_A_A",
                    "program": [{"kind": "reaction_order", "value": 1.0}],
                    "input_window": {"input_log10": [-2, 2]},
                },
            },
            "constraints": {
                "parameter_bounds": {"kd_log10": [-2, 2]},
            },
            "candidate_budget": {"max_exact_placements": 3},
            "ranking_policy": {"verified_only": True},
            "audit_policy": {"unsupported": "block_if_hard", "path_format": "json_pointer"},
        }
        cases = []

        unknown_root = copy.deepcopy(valid)
        unknown_root["unexpected_root"] = True
        cases.append(unknown_root)

        missing_source = copy.deepcopy(valid)
        del missing_source["source"]
        cases.append(missing_source)

        null_source = copy.deepcopy(valid)
        null_source["source"] = None
        cases.append(null_source)

        empty_source = copy.deepcopy(valid)
        empty_source["source"] = {}
        cases.append(empty_source)

        unknown_source = copy.deepcopy(valid)
        unknown_source["source"]["unknown"] = True
        cases.append(unknown_source)

        bad_source_node_id = copy.deepcopy(valid)
        bad_source_node_id["source"]["node_id"] = 12
        cases.append(bad_source_node_id)

        conflicting_top_level_input_window = copy.deepcopy(valid)
        conflicting_top_level_input_window["target"]["input_window"] = {
            "input_log10": [-10, -9],
            "hard": False,
        }
        cases.append(conflicting_top_level_input_window)

        legacy_target_hidden_shape = copy.deepcopy(valid)
        legacy_target_hidden_shape["target"] = {
            "legacy_target": {
                "target_kind": "exact",
                "target": [1.0],
                "shape": {
                    "class": "bell_shaped",
                    "min_prominence_log10": 0.2,
                    "sample_points": 51,
                    "tolerance_log10": 0.01,
                    "hard": True,
                },
            },
        }
        cases.append(legacy_target_hidden_shape)

        exact_target_with_bool = copy.deepcopy(valid)
        exact_target_with_bool["target"] = {
            "legacy_target": {
                "target_kind": "exact",
                "target": [True],
            },
        }
        cases.append(exact_target_with_bool)

        exact_target_empty = copy.deepcopy(valid)
        exact_target_empty["target"] = {
            "legacy_target": {
                "target_kind": "exact",
                "target": [],
            },
        }
        cases.append(exact_target_empty)

        exact_target_string = copy.deepcopy(valid)
        exact_target_string["target"] = {
            "legacy_target": {
                "target_kind": "exact",
                "target": "1, 0",
            },
        }
        cases.append(exact_target_string)

        hard_top_level_input_window = copy.deepcopy(valid)
        hard_top_level_input_window["target"] = {
            "legacy_target": {
                "target_kind": "exact",
                "target": [1.0],
            },
            "input_window": {"input_log10": [-2, 2]},
        }
        cases.append(hard_top_level_input_window)

        hard_temporal_dynamics = copy.deepcopy(valid)
        hard_temporal_dynamics["target"] = {
            "legacy_target": {
                "target_kind": "exact",
                "target": [1.0],
            },
            "temporal_dynamics": {
                "peak_width_seconds": {"min": 1.0},
            },
        }
        cases.append(hard_temporal_dynamics)

        bad_parameter_bounds = copy.deepcopy(valid)
        bad_parameter_bounds["constraints"]["parameter_bounds"] = {"kd_log10": [-2]}
        cases.append(bad_parameter_bounds)

        duplicate_kd_bounds = copy.deepcopy(valid)
        duplicate_kd_bounds["constraints"]["parameter_bounds"] = {
            "by_class": {"kd": [-1, 1]},
            "kd_log10": [-2, 2],
        }
        cases.append(duplicate_kd_bounds)

        duplicate_total_bounds = copy.deepcopy(valid)
        duplicate_total_bounds["constraints"]["parameter_bounds"] = {
            "by_class": {"total": [-1, 1]},
            "total_log10": [-2, 2],
        }
        cases.append(duplicate_total_bounds)

        bad_robustness = copy.deepcopy(valid)
        bad_robustness["constraints"]["robustness"] = {"min_chebyshev_radius": -0.1}
        cases.append(bad_robustness)

        ambiguous_tunable_volume = copy.deepcopy(valid)
        ambiguous_tunable_volume["constraints"]["robustness"] = {
            "min_tunable_volume_lower_bound": 0.1,
            "min_tunable_volume": 0.1,
        }
        cases.append(ambiguous_tunable_volume)

        hard_condition_number = copy.deepcopy(valid)
        hard_condition_number["constraints"]["robustness"] = {
            "condition_number_max": 100,
        }
        cases.append(hard_condition_number)

        hard_sampled_pass_fraction = copy.deepcopy(valid)
        hard_sampled_pass_fraction["constraints"]["robustness"] = {
            "min_sampled_pass_fraction": 0.8,
        }
        cases.append(hard_sampled_pass_fraction)

        bad_network = copy.deepcopy(valid)
        bad_network["constraints"]["network"] = {"max_species": 1.5}
        cases.append(bad_network)

        bad_budget = copy.deepcopy(valid)
        bad_budget["candidate_budget"] = {"max_exact_placements": -1}
        cases.append(bad_budget)

        exact_without_parameter_bounds = copy.deepcopy(valid)
        exact_without_parameter_bounds["constraints"] = {}
        exact_without_parameter_bounds["candidate_budget"] = {"max_exact_placements": 3}
        cases.append(exact_without_parameter_bounds)

        chebyshev_ranking_without_parameter_bounds = copy.deepcopy(valid)
        chebyshev_ranking_without_parameter_bounds["constraints"] = {}
        chebyshev_ranking_without_parameter_bounds["candidate_budget"] = {"max_exact_placements": 0}
        chebyshev_ranking_without_parameter_bounds["ranking_policy"] = {
            "prefer": ["chebyshev_radius"],
        }
        cases.append(chebyshev_ranking_without_parameter_bounds)

        bad_verified_only = copy.deepcopy(valid)
        bad_verified_only["ranking_policy"] = {"verified_only": False}
        cases.append(bad_verified_only)

        bad_prefer = copy.deepcopy(valid)
        bad_prefer["ranking_policy"] = {"prefer": ["unknown_metric"]}
        cases.append(bad_prefer)

        dynamic_range_prefer_without_metric = copy.deepcopy(valid)
        dynamic_range_prefer_without_metric["ranking_policy"] = {"prefer": ["dynamic_range"]}
        cases.append(dynamic_range_prefer_without_metric)

        transition_prefer_without_metric = copy.deepcopy(valid)
        transition_prefer_without_metric["ranking_policy"] = {"prefer": ["transition_spacing"]}
        cases.append(transition_prefer_without_metric)

        condition_number_prefer = copy.deepcopy(valid)
        condition_number_prefer["ranking_policy"] = {"prefer": ["condition_number"]}
        cases.append(condition_number_prefer)

        sampled_robustness_prefer = copy.deepcopy(valid)
        sampled_robustness_prefer["ranking_policy"] = {"prefer": ["sampled_robustness"]}
        cases.append(sampled_robustness_prefer)

        bad_audit = copy.deepcopy(valid)
        bad_audit["audit_policy"] = {"unsupported": "warn"}
        cases.append(bad_audit)

        empty_transition_shell = copy.deepcopy(valid)
        empty_transition_shell["constraints"]["transitions"] = {"hard": True}
        cases.append(empty_transition_shell)

        input_window_spacing_shell = copy.deepcopy(valid)
        input_window_spacing_shell["target"]["behavior_spec"]["input_window"] = {
            "min_spacing_decades": 0.5,
        }
        cases.append(input_window_spacing_shell)

        monotonic_with_prominence = copy.deepcopy(valid)
        monotonic_with_prominence["target"]["shape"] = {
            "class": "monotonic",
            "monotonicity": "any",
            "min_prominence_log10": 0.5,
            "sample_points": 51,
            "tolerance_log10": 0.01,
        }
        cases.append(monotonic_with_prominence)

        ambiguous_bell_prominence = copy.deepcopy(valid)
        ambiguous_bell_prominence["target"]["shape"] = {
            "class": "bell_shaped",
            "min_prominence_log10": 0.5,
            "min_prominence_decades": 1.0,
            "sample_points": 51,
            "tolerance_log10": 0.01,
        }
        cases.append(ambiguous_bell_prominence)

        bad_temporal_peak_range = copy.deepcopy(valid)
        bad_temporal_peak_range["target"]["temporal_dynamics"] = {
            "peak_width_seconds": {"min": 3, "max": 1},
            "hard": False,
        }
        cases.append(bad_temporal_peak_range)

        for spec in cases:
            self.assertIsNone(design_agent._agent_designability_spec_from_payload(spec, None))

        soft_unsupported = copy.deepcopy(valid)
        soft_unsupported["constraints"]["robustness"] = {
            "condition_number_max": 100,
            "min_sampled_pass_fraction": 0.8,
            "hard": False,
        }
        soft_out = design_agent._agent_designability_spec_from_payload(soft_unsupported, None)
        self.assertIsInstance(soft_out, dict)
        self.assertEqual(
            soft_out["constraints"]["robustness"],
            {
                "condition_number_max": 100,
                "min_sampled_pass_fraction": 0.8,
                "hard": False,
            },
        )

        soft_unsupported_target = copy.deepcopy(valid)
        soft_unsupported_target["target"] = {
            "legacy_target": {
                "target_kind": "exact",
                "target": [1.0],
            },
            "input_window": {"input_log10": [-2, 2], "hard": False},
            "temporal_dynamics": {
                "peak_width_seconds": {"min": 1.0},
                "hard": False,
            },
        }
        soft_target_out = design_agent._agent_designability_spec_from_payload(
            soft_unsupported_target,
            None,
        )
        self.assertIsInstance(soft_target_out, dict)
        self.assertEqual(soft_target_out["target"], soft_unsupported_target["target"])

        minimal_only = copy.deepcopy(valid)
        minimal_only["constraints"] = {"network": {"max_reactions": 4}}
        minimal_only["candidate_budget"] = {"max_exact_placements": 0}
        minimal_only["ranking_policy"] = {"prefer": ["complexity"]}

        out = design_agent._agent_designability_spec_from_payload(minimal_only, None)
        self.assertIsInstance(out, dict)
        self.assertEqual(out["constraints"], {"network": {"max_reactions": 4}})

    def test_nested_behavior_spec_lowering_rejects_explicit_empty_kd_profile(self):
        raw = copy.deepcopy(EXECUTABLE_BEHAVIOR_SPEC)
        raw["network_constraints"]["kd_profile"] = {}

        self.assertIsNone(design_agent._agent_designability_spec_from_payload(raw, None))

    def test_agent_refuses_candidate_with_explicit_empty_or_null_spec_payload(self):
        card = {
            "family": "dose_shape",
            "rules": ["A + A <-> C_A_A"],
            "kd": [1.0],
            "input_symbol": "tA",
            "output_symbol": "C_A_A",
            "shape_support": 1.0,
        }

        for payload in ({}, None):
            def fake_simulate(**_kwargs):
                return {
                    "compiled_spec": payload,
                    "_card": dict(card),
                }

            def fake_runner(_history, _message, dispatch, _cfg, max_iters=12):
                dispatch("simulate", {})
                return "done", [{"role": "user", "content": "design"}]

            with mock.patch.dict(design_agent.TOOLS_DISPATCH, {"simulate": fake_simulate}, clear=False), \
                 mock.patch.object(design_agent, "_run_anthropic", fake_runner), \
                 mock.patch.object(design_agent, "_write_trace", lambda _trace: None), \
                 mock.patch.object(design_agent, "_spill_card", lambda _card: "cardhash"):
                res = design_agent.run_turn(
                    {},
                    "design",
                    {
                        "provider": "anthropic",
                        "api_key": "test-key",
                        "base_url": "https://example.invalid",
                        "model": "glm-5.2",
                    },
                    top=1,
                )

            self.assertEqual(res["cards"], [])

    def test_agent_does_not_attach_first_spec_to_later_card_without_local_spec(self):
        first_card = {
            "family": "dose_shape",
            "rules": ["A + A <-> C_A_A"],
            "kd": [1.0],
            "input_symbol": "tA",
            "output_symbol": "C_A_A",
            "shape_support": 1.0,
        }
        second_card = {
            "family": "dose_shape",
            "rules": ["A + B <-> C_A_B"],
            "kd": [1.0],
            "input_symbol": "tA",
            "output_symbol": "C_A_B",
            "shape_support": 0.9,
        }
        results = [
            {"compiled_spec": copy.deepcopy(EXECUTABLE_BEHAVIOR_SPEC), "_card": dict(first_card)},
            {"_card": dict(second_card)},
        ]

        def fake_simulate(**_kwargs):
            return results.pop(0)

        def fake_runner(_history, _message, dispatch, _cfg, max_iters=12):
            dispatch("simulate", {})
            dispatch("simulate", {})
            return "done", [{"role": "user", "content": "design"}]

        with mock.patch.dict(design_agent.TOOLS_DISPATCH, {"simulate": fake_simulate}, clear=False), \
             mock.patch.object(design_agent, "_run_anthropic", fake_runner), \
             mock.patch.object(design_agent, "_write_trace", lambda _trace: None), \
             mock.patch.object(design_agent, "_spill_card", lambda _card: "cardhash"):
            res = design_agent.run_turn(
                {},
                "design",
                {
                    "provider": "anthropic",
                    "api_key": "test-key",
                    "base_url": "https://example.invalid",
                    "model": "glm-5.2",
                },
                top=1,
            )

        self.assertIn("designability_spec", res["cards"][0])
        self.assertNotIn("designability_spec", res["cards"][1])

    def test_design_from_behavior_binds_io_after_screen_and_replays_selected_parameters(self):
        seen = {}

        def fake_validate(spec):
            seen["validated_spec"] = copy.deepcopy(spec)
            return {"ok": True, "blocked_by_unsupported_hard_clause": False}

        def fake_screen(spec):
            if "legacy_target" in spec["target"]:
                seen["discovery_spec"] = copy.deepcopy(spec)
                return {
                    "schema_version": "bne-design-screen/v0.3.0",
                    "eligible_count": 7,
                    "evaluated_count": 7,
                    "truncated": False,
                    "screened_candidates": [{
                        "card_id": "discovery-card",
                        "nid": "[1]+[2]<->[1,2]",
                        "inp": "tA",
                        "out": "C_A_B",
                    }],
                }
            seen["screened_spec"] = copy.deepcopy(spec)
            return {
                "schema_version": "bne-design-screen/v0.3.0",
                "eligible_count": 4,
                "evaluated_count": 4,
                "truncated": False,
                "verified_recommendations": [{
                    "nid": "[1]+[2]<->[1,2]",
                    "inp": "tA",
                    "out": "C_A_B",
                    "pass": True,
                    "screen_status": "verified_exact",
                    "certificate_grade": "exact-window-siso-rop-path",
                    "evidence_grade": "enforced_exact",
                    "metrics": {"chebyshev_radius": 0.42},
                    "parameter_recommendation": {"theta_star": {
                        "status": "computed",
                        "source_type": "exact_solver",
                        "bounds_verified": True,
                        "log_qK": [0.0, 0.2, -0.3],
                        "kd": [10 ** -0.3],
                        "totals": {"tA": 1.0, "tB": 10 ** 0.2},
                    }},
                    "agent_handoff": {"next_request": {
                        "endpoint": "/api/v1/placer_curve",
                        "method": "POST",
                        "body": {
                            "rules": ["A + B <-> C_A_B"],
                            "input_sym": "tA",
                            "output_sym": "C_A_B",
                            "kd": [10 ** -0.3],
                            "totals": {"tA": 1.0, "tB": 10 ** 0.2},
                        },
                    }},
                }],
            }

        def fake_curve(**kwargs):
            seen["curve_kwargs"] = copy.deepcopy(kwargs)
            xs = [-5.0 + 0.5 * i for i in range(21)]
            return {
                "param_values": xs,
                "output_traj": [[-1.0 - 0.1 * x] for x in xs],
                "valid": [True] * 21,
                "partial": False,
            }

        with mock.patch.object(design_agent.E, "validate_designability_spec", fake_validate), \
             mock.patch.object(design_agent.E, "design_screen", fake_screen), \
             mock.patch.object(design_agent.E, "placer_curve", fake_curve), \
             mock.patch.object(design_agent, "_design_finite_slopes", lambda _x, _y, _w: [1.0, 0.0, -1.0]):
            result = design_agent.design_from_behavior(
                [1, 0, -1],
                input_window_log10=[-5, 5],
                operating_points_log10=[-3, 0, 3],
                n_points=21,
            )

        spec = seen["validated_spec"]
        self.assertEqual(spec, seen["screened_spec"])
        self.assertEqual(seen["discovery_spec"]["target"]["legacy_target"]["target"], [1.0, 0.0, -1.0])
        behavior = spec["target"]["behavior_spec"]
        self.assertEqual(behavior["input"], "tA")
        self.assertEqual(behavior["output"], "C_A_B")
        self.assertEqual([step["value"] for step in behavior["program"]], [1.0, 0.0, -1.0])
        self.assertEqual(seen["curve_kwargs"]["rules"], ["A + B <-> C_A_B"])
        self.assertEqual(seen["curve_kwargs"]["input_sym"], "tA")
        self.assertEqual(seen["curve_kwargs"]["output_sym"], "C_A_B")
        self.assertEqual(seen["curve_kwargs"]["totals"], {"tA": 1.0, "tB": 10 ** 0.2})
        self.assertEqual(seen["curve_kwargs"]["param_min"], -5.0)
        self.assertEqual(seen["curve_kwargs"]["param_max"], 5.0)
        self.assertEqual(seen["curve_kwargs"]["n_points"], 21)
        self.assertEqual(result["forward_points"], 21)
        self.assertEqual(result["_card"]["input_symbol"], "tA")
        self.assertEqual(result["_card"]["output_symbol"], "C_A_B")
        self.assertEqual(result["_card"]["certificate_grade"], "exact-window-siso-rop-path")

    def test_design_from_behavior_rejects_non_integral_or_boolean_budgets(self):
        invalid_cases = [
            {"max_reactions": True},
            {"max_reactions": 1.5},
            {"max_reactions": "4"},
            {"max_screened": True},
            {"max_screened": 2.9},
            {"max_verified_recommendations": False},
            {"max_exact_placements": 2.5},
            {"n_points": 21.5},
        ]
        for kwargs in invalid_cases:
            with self.subTest(kwargs=kwargs), \
                 mock.patch.object(design_agent.E, "design_screen") as screen:
                result = design_agent.design_from_behavior([1, 0, -1], **kwargs)

            self.assertEqual(result["error"], "design budgets and n_points must be integers")
            screen.assert_not_called()

    def test_design_from_behavior_rejects_stale_or_missing_screen_schema(self):
        for schema_version in (None, "bne-design-screen/v0.2.0"):
            discovery = {
                "schema_version": schema_version,
                "eligible_count": 1,
                "evaluated_count": 1,
                "truncated": False,
                "screened_candidates": [{
                    "card_id": "discovery-card",
                    "nid": "[1]+[2]<->[1,2]",
                    "inp": "tA",
                    "out": "C_A_B",
                }],
            }
            with self.subTest(stage="discovery", schema_version=schema_version), \
                 mock.patch.object(design_agent.E, "design_screen", return_value=discovery), \
                 mock.patch.object(design_agent.E, "validate_designability_spec") as validate:
                result = design_agent.design_from_behavior([1, 0, -1])

            self.assertIn("unsupported Design Screen schema_version", result["error"])
            validate.assert_not_called()

        discovery = {
            "schema_version": "bne-design-screen/v0.3.0",
            "eligible_count": 1,
            "evaluated_count": 1,
            "truncated": False,
            "screened_candidates": [{
                "card_id": "discovery-card",
                "nid": "[1]+[2]<->[1,2]",
                "inp": "tA",
                "out": "C_A_B",
            }],
        }
        stale_final = {
            "schema_version": "bne-design-screen/v0.2.0",
            "eligible_count": 1,
            "evaluated_count": 1,
            "truncated": False,
            "verified_recommendations": [{"pass": True}],
        }
        with mock.patch.object(design_agent.E, "design_screen", side_effect=[discovery, stale_final]), \
             mock.patch.object(design_agent.E, "validate_designability_spec", return_value={
                 "ok": True,
                 "blocked_by_unsupported_hard_clause": False,
             }), \
             mock.patch.object(design_agent.E, "placer_curve") as placer:
            result = design_agent.design_from_behavior([1, 0, -1])

        self.assertEqual(result["error"], "final Design Screen returned an unsupported schema_version")
        placer.assert_not_called()

    def test_design_from_behavior_requires_literal_true_curve_validity(self):
        discovery = {
            "schema_version": "bne-design-screen/v0.3.0",
            "eligible_count": 1,
            "evaluated_count": 1,
            "truncated": False,
            "screened_candidates": [{
                "card_id": "discovery-card",
                "nid": "[1]+[2]<->[1,2]",
                "inp": "tA",
                "out": "C_A_B",
            }],
        }
        exact_card = {
            "schema_version": "bne-design-screen/v0.3.0",
            "eligible_count": 1,
            "evaluated_count": 1,
            "truncated": False,
            "verified_recommendations": [{
                "nid": "[1]+[2]<->[1,2]",
                "inp": "tA",
                "out": "C_A_B",
                "pass": True,
                "screen_status": "verified_exact",
                "certificate_grade": "exact-window-siso-rop-path",
                "evidence_grade": "enforced_exact",
                "metrics": {"chebyshev_radius": 0.42},
                "parameter_recommendation": {"theta_star": {
                    "status": "computed",
                    "source_type": "exact_solver",
                    "bounds_verified": True,
                    "kd": [1.0],
                    "totals": {"tA": 1.0, "tB": 0.1},
                    "witness_input_log10": [-3.0, 0.0, 3.0],
                }},
                "agent_handoff": {"next_request": {
                    "endpoint": "/api/v1/placer_curve",
                    "method": "POST",
                    "body": {
                        "rules": ["A + B <-> C_A_B"],
                        "input_sym": "tA",
                        "output_sym": "C_A_B",
                        "kd": [1.0],
                        "totals": {"tA": 1.0, "tB": 0.1},
                    },
                }},
            }],
        }
        xs = [-5.0 + 0.5 * i for i in range(21)]
        curve = {
            "param_values": xs,
            "output_traj": [[-x] for x in xs],
            "valid": [True] * 20 + ["false"],
            "partial": False,
        }
        with mock.patch.object(design_agent.E, "design_screen", side_effect=[discovery, exact_card]), \
             mock.patch.object(design_agent.E, "validate_designability_spec", return_value={
                 "ok": True,
                 "blocked_by_unsupported_hard_clause": False,
             }), \
             mock.patch.object(design_agent.E, "placer_curve", return_value=curve), \
             mock.patch.object(design_agent, "_design_finite_slopes", return_value=[1.0, 0.0, -1.0]):
            result = design_agent.design_from_behavior(
                [1, 0, -1], operating_points_log10=[-3, 0, 3], n_points=21,
            )

        self.assertIn("no exact recommendation passed", result["error"])
        self.assertEqual(result["replay_failures"][0]["reason"], "incomplete/non-finite curve")

    def test_run_turn_admits_end_to_end_design_tool_card(self):
        spec = {
            "schema_version": "bne-designability/v1.0.0",
            "source": {"kind": "agent_design"},
            "target": {"legacy_target": {"target_kind": "exact", "target": [1, 0, -1]}},
        }
        card = {
            "family": "dose_shape",
            "rules": ["A + B <-> C_A_B"],
            "kd": [1.0],
            "input_symbol": "tA",
            "output_symbol": "C_A_B",
            "computed_series": [{"x": -1.0, "y": -2.0}, {"x": 1.0, "y": -3.0}],
            "evidence_tier": "exact Design Screen + fresh finite forward scan",
        }

        def fake_design(**_kwargs):
            return {"designability_spec": copy.deepcopy(spec), "_card": copy.deepcopy(card)}

        def fake_runner(_history, _message, dispatch, _cfg, max_iters=12):
            dispatch("design_from_behavior", {"target_program": [1, 0, -1]})
            return "设计完成", [{"role": "user", "content": "猫猫曲线"}]

        with mock.patch.dict(design_agent.TOOLS_DISPATCH, {"design_from_behavior": fake_design}, clear=False), \
             mock.patch.object(design_agent, "_run_anthropic", fake_runner), \
             mock.patch.object(design_agent, "_write_trace", lambda _trace: None), \
             mock.patch.object(design_agent, "_spill_card", lambda _card: "cardhash"):
            result = design_agent.run_turn(
                {}, "猫猫曲线",
                {"provider": "anthropic", "api_key": "test-key", "base_url": "https://example.invalid", "model": "test"},
                top=1,
            )

        self.assertEqual(result["reply"], "设计完成")
        self.assertEqual(len(result["cards"]), 1)
        self.assertEqual(result["cards"][0]["rules"], ["A + B <-> C_A_B"])
        self.assertEqual(result["designability_spec"]["target"]["legacy_target"]["target"], [1, 0, -1])


if __name__ == "__main__":
    unittest.main()
