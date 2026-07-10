#!/usr/bin/env python3
import copy
import os
import unittest
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


class DesignAgentContractTests(unittest.TestCase):
    def test_end_to_end_design_tool_is_exposed_to_both_provider_protocols(self):
        self.assertIn("design_from_behavior", design_agent.TOOLS_DISPATCH)
        self.assertIn("design_from_behavior", [tool["name"] for tool in design_agent.TOOLSPEC])
        self.assertIn("design_from_behavior", [tool["function"]["name"] for tool in design_agent.OPENAI_TOOLS])
        self.assertIn("design_from_behavior", [tool["name"] for tool in design_agent.ANTHROPIC_TOOLS])

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
