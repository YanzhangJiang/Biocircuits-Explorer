#!/usr/bin/env python3
"""Process-level Agent -> Design Screen -> Placer integration contract.

The model-provider response is deterministic, but every design/solver call goes
through the real HTTP server and current production dispatch table.
"""

import contextlib
import math
import os
from pathlib import Path
import shutil
import socket
import subprocess
import sys
import time
import unittest
import urllib.request
from unittest import mock


HERE = Path(__file__).resolve().parent
ROOT = HERE.parent.parent
sys.path.insert(0, str(HERE))

import design_agent


def _free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class DesignAgentLiveEngineTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        julia = shutil.which("julia")
        if not julia:
            raise unittest.SkipTest("Julia executable is required for the live-engine contract")
        cls.port = _free_port()
        cls.old_host = os.environ.get("BIOCIRCUITS_EXPLORER_HOST")
        cls.old_port = os.environ.get("BIOCIRCUITS_EXPLORER_PORT")
        os.environ["BIOCIRCUITS_EXPLORER_HOST"] = "127.0.0.1"
        os.environ["BIOCIRCUITS_EXPLORER_PORT"] = str(cls.port)
        env = os.environ.copy()
        env["JULIA_NUM_THREADS"] = "auto"
        cls.server = subprocess.Popen(
            [julia, "--project=webapp", "--startup-file=no", "webapp/server.jl"],
            cwd=ROOT,
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        ready = f"http://127.0.0.1:{cls.port}/ready"
        deadline = time.time() + 180
        while time.time() < deadline:
            if cls.server.poll() is not None:
                raise RuntimeError(f"Julia server exited with {cls.server.returncode}")
            try:
                with urllib.request.urlopen(ready, timeout=1) as response:
                    if 200 <= response.status < 300:
                        return
            except Exception:
                time.sleep(0.25)
        raise RuntimeError("Julia server did not become ready within 180 seconds")

    @classmethod
    def tearDownClass(cls):
        server = getattr(cls, "server", None)
        if server and server.poll() is None:
            server.terminate()
            try:
                server.wait(timeout=15)
            except subprocess.TimeoutExpired:
                server.kill()
                server.wait(timeout=5)
        if cls.old_host is None:
            os.environ.pop("BIOCIRCUITS_EXPLORER_HOST", None)
        else:
            os.environ["BIOCIRCUITS_EXPLORER_HOST"] = cls.old_host
        if cls.old_port is None:
            os.environ.pop("BIOCIRCUITS_EXPLORER_PORT", None)
        else:
            os.environ["BIOCIRCUITS_EXPLORER_PORT"] = cls.old_port

    def test_provider_tool_call_reaches_real_screen_and_placer(self):
        provider_responses = [
            {"content": [{
                "type": "tool_use",
                "id": "tool-live-1",
                "name": "design_from_behavior",
                "input": {
                    "target_program": [0, -1],
                    "input_window_log10": [-5, 5],
                    "operating_points_log10": [-2, 2],
                    "max_reactions": 1,
                    "max_screened": 16,
                    "max_verified_recommendations": 3,
                    "max_exact_placements": 4,
                    "n_points": 41,
                },
            }]},
            {"content": [{"type": "text", "text": "已返回经过精确筛选和新曲线验证的设计。"}]},
        ]

        def fake_provider(*_args, **_kwargs):
            return provider_responses.pop(0)

        real_post = design_agent.E._post
        paths = []

        def recording_post(path, payload, timeout):
            paths.append(path)
            return real_post(path, payload, timeout)

        with contextlib.ExitStack() as stack:
            stack.enter_context(mock.patch.object(design_agent.L, "anthropic_chat_tools", fake_provider))
            stack.enter_context(mock.patch.object(design_agent.E, "_post", recording_post))
            stack.enter_context(mock.patch.object(design_agent, "_write_trace", lambda _trace: None))
            stack.enter_context(mock.patch.object(design_agent, "_spill_card", lambda _card: "live-card"))
            result = design_agent.run_turn(
                {},
                "设计一个先平台、后下降的响应",
                {
                    "provider": "anthropic",
                    "api_key": "deterministic-test-provider",
                    "base_url": "https://provider.invalid",
                    "model": "deterministic-tool-caller",
                },
                top=1,
            )

        self.assertEqual(paths, [
            "/api/v1/design_screen",
            "/api/v1/validate_designability_spec",
            "/api/v1/design_screen",
            "/api/v1/placer_curve",
        ])
        self.assertEqual(result["kind"], "agent")
        self.assertEqual(len(result["cards"]), 1)
        card = result["cards"][0]
        self.assertNotIn("compiled_spec", card)
        self.assertEqual(card["certificate_grade"], "exact-window-siso-rop-path")
        self.assertEqual(card["evidence_grade"], "enforced_exact")
        self.assertTrue(card["finite_slope_pass"])
        self.assertEqual(len(card["observed_finite_slopes"]), 2)
        self.assertEqual(len(card["computed_series"]), 41)
        self.assertTrue(all(math.isfinite(point["x"]) and math.isfinite(point["y"])
                            for point in card["computed_series"]))
        exact = card["designability_card"]
        theta = exact["parameter_recommendation"]["theta_star"]
        self.assertTrue(exact["pass"])
        self.assertEqual(exact["screen_status"], "verified_exact")
        self.assertEqual(theta["status"], "computed")
        self.assertEqual(theta["source_type"], "exact_solver")
        self.assertTrue(theta["bounds_verified"])
        self.assertTrue(theta["kd"])
        spec = result["designability_spec"]
        behavior = spec["target"]["behavior_spec"]
        self.assertEqual(behavior["program"][0]["value"], 0.0)
        self.assertEqual(behavior["program"][1]["value"], -1.0)
        self.assertEqual(behavior["input"], card["input_symbol"])
        self.assertEqual(behavior["output"], card["output_symbol"])


if __name__ == "__main__":
    unittest.main()
