#!/usr/bin/env python3
import copy
import json
import math
import unittest
from unittest import mock

import chat_api
import design_target_compile as compiler
from test_chat_api import _request, _running_server, TEST_ORIGIN, TEST_TOKEN


class TargetCompilerTests(unittest.TestCase):
    def setUp(self):
        self.no_env = mock.patch.object(compiler.llm_transport, "llm_config_from_env", return_value={})
        self.no_env.start()
        self.addCleanup(self.no_env.stop)

    def test_chinese_description_controls_range_samples_direction_and_constraint(self):
        result = compiler.compile_target("单调上升，输入范围 0.1 到 100，输出范围 0.2 到 2，13 个采样点，最多 12 个反应")
        target = result["target"]
        self.assertEqual(target["inputs"][0]["min"], .1)
        self.assertEqual(target["inputs"][0]["max"], 100)
        self.assertEqual(len(target["samples"]), 13)
        self.assertEqual(target["samples"][0]["outputs"], [.2])
        self.assertEqual(target["samples"][-1]["outputs"], [2.])
        self.assertEqual(result["chemistry"], {"max_reactions": 12})
        down = compiler.compile_target("monotone decreasing")
        self.assertGreater(down["target"]["samples"][0]["outputs"][0], down["target"]["samples"][-1]["outputs"][0])
        self.assertTrue(result["warnings"])

    def test_bandpass_and_threshold_are_different_sampled_targets(self):
        band = compiler.compile_target("bandpass with a wide plateau")["target"]["samples"]
        self.assertGreater(band[len(band) // 2]["outputs"][0], band[0]["outputs"][0])
        self.assertGreater(band[len(band) // 2]["outputs"][0], band[-1]["outputs"][0])
        switch = compiler.compile_target("threshold switch input range 1 to 9 linear threshold at 3, 9 points")
        self.assertEqual(switch["target"]["inputs"][0]["name"], "X")
        self.assertEqual(switch["target"]["samples"][2]["inputs"], [3])
        self.assertAlmostEqual(switch["target"]["samples"][2]["outputs"][0], .525)
        self.assertIn("0.25", switch["interpretation"])

    def test_trajectory_is_single_input_double_output_with_explicit_order(self):
        result = compiler.compile_target("a circular trajectory, 9 points")
        target = result["target"]
        self.assertEqual(len(target["inputs"]), 1)
        self.assertEqual([output["species"] for output in target["outputs"]], ["A", "B"])
        first, quarter = target["samples"][0], target["samples"][2]
        self.assertAlmostEqual(first["outputs"][0], 1.)
        self.assertAlmostEqual(first["outputs"][1], .525)
        self.assertAlmostEqual(quarter["outputs"][0], .525)
        self.assertAlmostEqual(quarter["outputs"][1], 1.)
        self.assertIn("counterclockwise", result["interpretation"])

    def test_unknown_dynamic_or_conflicting_request_does_not_become_preset(self):
        for message in ("make a fox logo", "I need a robust network", "随时间周期振荡", "bandpass and monotone decreasing"):
            with self.subTest(message=message), self.assertRaises(compiler.TargetCompileError):
                compiler.compile_target(message)

    def test_prior_readout_and_input_identity_survive_refinement(self):
        prior = compiler.compile_target("monotone increasing")["target"]
        prior["inputs"][0]["name"] = "U"
        prior["outputs"][0].update(species="AB", transform="log10", offset=2, optimize_offset=True)
        refined = compiler.compile_target("monotone decreasing", target=prior)["target"]
        self.assertEqual(refined["outputs"], prior["outputs"])
        self.assertEqual(refined["inputs"], prior["inputs"])
        self.assertNotEqual(refined["samples"], prior["samples"])
        self.assertEqual(prior["samples"][0]["outputs"], [.05])

    def test_editable_output_labels_and_viewport_ranges_match_engine_contract(self):
        prior = compiler.compile_target("monotone increasing")["target"]
        prior["outputs"][0].update(name="target response", min=-2, max=3)
        result = compiler.compile_target("monotone decreasing", target=prior)
        self.assertEqual(result["target"]["outputs"][0]["name"], "target response")
        self.assertEqual(result["target"]["samples"][0]["outputs"], [3])
        self.assertEqual(result["target"]["samples"][-1]["outputs"], [-2])
        self.assertIn("+ 0", result["interpretation"])
        for sample in result["target"]["samples"]:
            self.assertLessEqual(sample["inputs"][0], prior["inputs"][0]["max"])
            self.assertGreaterEqual(sample["inputs"][0], prior["inputs"][0]["min"])

    def test_strict_validation_rejects_malformed_model_targets(self):
        valid = compiler.compile_target("monotone increasing")["target"]
        changes = (
            lambda t: t.update(extra="untrusted"),
            lambda t: t["samples"][0].update(outputs=[1, 2]),
            lambda t: t["samples"][0].update(inputs=[0]),
            lambda t: t["samples"][0].update(inputs=[999]),
            lambda t: t["samples"][0].update(outputs=[math.nan]),
            lambda t: t["samples"][0].update(weight=True),
            lambda t: t["outputs"][0].update(species="A + B"),
            lambda t: t["outputs"][0].update(offset="eval(1)"),
            lambda t: t["outputs"][0].update(optimize_offset=1),
        )
        for change in changes:
            target = copy.deepcopy(valid)
            change(target)
            with self.subTest(target=target), self.assertRaises(compiler.TargetCompileError):
                compiler.validate_target(target)

    def test_openai_tool_reply_uses_existing_transport_and_original_description(self):
        compiled = compiler.compile_target("monotone increasing")
        response = {"tool_calls": [{"function": {"name": "submit_target", "arguments": json.dumps(compiled)}}]}
        cfg = {"provider": "openai", "api_key": "test-only-key", "base_url": "http://test.invalid/v1", "model": "test"}
        with mock.patch.object(compiler.llm_transport, "openai_chat_tools", return_value=response) as call:
            result = compiler.compile_target("a more specific goal", cfg)
        self.assertEqual(result["target"]["description"], "a more specific goal")
        self.assertEqual(call.call_args.kwargs["api_key"], "test-only-key")
        self.assertNotIn("test-only-key", json.dumps(result))

    def test_malformed_llm_gets_one_correction_then_explicit_error(self):
        with mock.patch.object(compiler.llm_transport, "openai_chat_tools", return_value={"content": "not a target"}) as call:
            with self.assertRaises(compiler.TargetCompileError) as caught:
                compiler.compile_target("specific unknown goal", {"api_key": "test", "base_url": "http://test.invalid", "model": "test"})
        self.assertEqual(call.call_count, 2)
        self.assertEqual(caught.exception.code, "invalid_target")

    def test_llm_clarification_is_not_replaced_and_provider_secrets_are_not_echoed(self):
        config = {"api_key": "secret-do-not-echo", "base_url": "http://test.invalid", "model": "test"}
        with mock.patch.object(compiler.llm_transport, "openai_chat_tools", return_value={"content": '{"clarification":"Specify whether x and y are inputs or outputs."}'}):
            with self.assertRaisesRegex(compiler.TargetCompileError, "inputs or outputs"):
                compiler.compile_target("an xy image", config)
        with mock.patch.object(compiler.llm_transport, "openai_chat_tools", side_effect=RuntimeError("secret-do-not-echo")):
            with self.assertRaises(compiler.TargetCompileError) as caught:
                compiler.compile_target("a target", config)
            self.assertNotIn("secret-do-not-echo", str(caught.exception))

    def test_anthropic_compiler_accepts_validated_tool_output(self):
        reply = compiler.compile_target("bandpass")
        with mock.patch.object(compiler.llm_transport, "anthropic_chat_tools", return_value={"content": [{"type": "tool_use", "name": "submit_target", "input": reply}]}) as call:
            result = compiler.compile_target("custom bandpass", {"provider": "anthropic", "api_key": "test", "model": "test"})
        self.assertEqual(result["target"]["source"], "agent")
        call.assert_called_once()


class TargetCompilerRouteTests(unittest.TestCase):
    def test_compile_route_auth_origin_and_preflight_share_existing_security(self):
        with mock.patch.object(chat_api.target_compiler, "compile_target") as compile_target:
            with _running_server() as port:
                for headers, expected in (({"Origin": TEST_ORIGIN}, 401), ({"Origin": "https://evil.example", "Authorization": f"Bearer {TEST_TOKEN}"}, 403), ({}, 403)):
                    status, _, _ = _request(port, "POST", "/compile-target", headers=headers, payload={"message": "monotone increasing"})
                    self.assertEqual(status, expected)
                status, headers, _ = _request(port, "OPTIONS", "/compile-target", headers={"Origin": TEST_ORIGIN, "Access-Control-Request-Method": "POST", "Access-Control-Request-Headers": "authorization,content-type"})
                self.assertEqual(status, 204)
                self.assertEqual(headers["access-control-allow-origin"], TEST_ORIGIN)
                status, headers, _ = _request(port, "OPTIONS", "/compile-target", headers={"Origin": "https://evil.example", "Access-Control-Request-Method": "POST"})
                self.assertEqual(status, 403)
                self.assertNotIn("access-control-allow-origin", headers)
            compile_target.assert_not_called()

    def test_real_no_key_compile_route_and_unknown_intent(self):
        with mock.patch.object(compiler.llm_transport, "llm_config_from_env", return_value={}):
            with _running_server() as port:
                headers = {"Origin": TEST_ORIGIN, "Authorization": f"Bearer {TEST_TOKEN}", "Content-Type": "application/json"}
                status, response_headers, body = _request(port, "POST", "/compile-target", headers=headers, payload={"message": "单调下降"})
                self.assertEqual(status, 200)
                self.assertEqual(response_headers["cache-control"], "no-store")
                self.assertEqual(json.loads(body)["target"]["description"], "单调下降")
                status, _, body = _request(port, "POST", "/compile-target", headers=headers, payload={"message": "surprise me"})
                self.assertEqual(status, 422)
                self.assertEqual(json.loads(body)["code"], "cannot_compile_target")
                for malformed in ([], {"message": 123}, {"message": "hello", "llm": []}):
                    status, _, _ = _request(port, "POST", "/compile-target", headers=headers, payload=malformed)
                    self.assertEqual(status, 400)

    def test_compile_capacity_is_bounded_and_slot_released_after_failure(self):
        with _running_server(max_concurrent_turns=1) as port:
            headers = {"Origin": TEST_ORIGIN, "Authorization": f"Bearer {TEST_TOKEN}"}
            chat_api.CHAT_TURN_SEMAPHORE.acquire()
            status, response_headers, _ = _request(port, "POST", "/compile-target", headers=headers, payload={"message": "monotone increasing"})
            self.assertEqual(status, 429)
            self.assertEqual(response_headers["retry-after"], "1")
            chat_api.CHAT_TURN_SEMAPHORE.release()
            with mock.patch.object(chat_api.target_compiler, "compile_target", side_effect=compiler.TargetCompileError("clarify")):
                status, _, _ = _request(port, "POST", "/compile-target", headers=headers, payload={"message": "unknown"})
            self.assertEqual(status, 422)
            self.assertTrue(chat_api.CHAT_TURN_SEMAPHORE.acquire(blocking=False))
            chat_api.CHAT_TURN_SEMAPHORE.release()


if __name__ == "__main__":
    unittest.main()
