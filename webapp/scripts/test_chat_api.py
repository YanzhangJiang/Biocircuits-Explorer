#!/usr/bin/env python3
import json
import sys
import threading
import time
import unittest
from contextlib import contextmanager
from http.client import HTTPConnection
from http.server import ThreadingHTTPServer
from unittest import mock

import chat_api


class _BrokenStderr:
    def write(self, _message):
        raise BrokenPipeError("stderr pipe is closed")


TEST_ORIGIN = "http://127.0.0.1:18088"
TEST_TOKEN = "a" * 64
TEST_NONCE = "b" * 64


@contextmanager
def _running_server(
    *,
    token=TEST_TOKEN,
    allow_unauthenticated=False,
    max_concurrent_turns=2,
):
    with (
        mock.patch.object(chat_api, "ALLOWED_ORIGIN", TEST_ORIGIN),
        mock.patch.object(chat_api, "BEARER_TOKEN", token),
        mock.patch.object(chat_api, "INSTANCE_NONCE", TEST_NONCE),
        mock.patch.object(
            chat_api,
            "ALLOW_UNAUTHENTICATED_LOOPBACK",
            allow_unauthenticated,
        ),
        mock.patch.object(chat_api.engine, "engine_ready", return_value=True),
        mock.patch.object(chat_api.engine, "engine_base_url", return_value=TEST_ORIGIN),
        mock.patch.object(
            chat_api,
            "CHAT_TURN_MAX_CONCURRENCY",
            max_concurrent_turns,
        ),
        mock.patch.object(
            chat_api,
            "CHAT_TURN_SEMAPHORE",
            threading.BoundedSemaphore(max_concurrent_turns),
        ),
    ):
        server = ThreadingHTTPServer(("127.0.0.1", 0), chat_api.Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            yield server.server_port
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)


def _request(port, method, path, *, headers=None, payload=None):
    connection = HTTPConnection("127.0.0.1", port, timeout=3)
    body = None if payload is None else json.dumps(payload)
    connection.request(method, path, body=body, headers=headers or {})
    response = connection.getresponse()
    response_body = response.read()
    result = (
        response.status,
        {name.lower(): value for name, value in response.getheaders()},
        response_body,
    )
    connection.close()
    return result


class ChatApiProcessLifecycleTests(unittest.TestCase):
    def test_log_message_ignores_closed_stderr_pipe(self):
        handler = object.__new__(chat_api.Handler)
        handler.command = "GET"
        handler.path = "/health"

        original_stderr = sys.stderr
        sys.stderr = _BrokenStderr()
        try:
            chat_api.Handler.log_message(handler, "ignored")
        finally:
            sys.stderr = original_stderr

    def test_parent_is_gone_when_helper_was_reparented_to_launchd(self):
        def parent_pid_is_still_alive(_pid, _signal):
            return None

        self.assertTrue(
            chat_api._parent_is_gone(
                12345,
                getppid=lambda: 1,
                kill=parent_pid_is_still_alive,
            )
        )


class ChatApiCapacityContractTests(unittest.TestCase):
    def _authorized_post(self, port, message="hello"):
        return _request(
            port,
            "POST",
            "/design-chat",
            headers={
                "Origin": TEST_ORIGIN,
                "Authorization": f"Bearer {TEST_TOKEN}",
                "Content-Type": "application/json",
            },
            payload={"message": message, "state": {}, "top": 1},
        )

    def test_concurrency_setting_is_strict_and_bounded(self):
        self.assertEqual(chat_api._chat_turn_max_concurrency(""), 2)
        self.assertEqual(chat_api._chat_turn_max_concurrency("1"), 1)
        self.assertEqual(chat_api._chat_turn_max_concurrency("32"), 32)
        for invalid in ("0", "-1", "+1", "01", "1.0", "many", "33"):
            with self.subTest(invalid=invalid):
                with self.assertRaises(ValueError):
                    chat_api._chat_turn_max_concurrency(invalid)

    def test_expensive_turns_fail_fast_at_capacity_and_permit_is_reusable(self):
        entered = threading.Event()
        release = threading.Event()
        active = 0
        peak = 0
        counter_lock = threading.Lock()

        def blocking_turn(*_args, **_kwargs):
            nonlocal active, peak
            with counter_lock:
                active += 1
                peak = max(peak, active)
            entered.set()
            try:
                self.assertTrue(release.wait(timeout=2), "blocked turn was not released")
                return {"kind": "chat", "reply": "ok", "cards": []}
            finally:
                with counter_lock:
                    active -= 1

        with mock.patch.object(chat_api.agent, "run_turn", side_effect=blocking_turn):
            with _running_server(max_concurrent_turns=1) as port:
                first_result = []
                first = threading.Thread(
                    target=lambda: first_result.append(
                        self._authorized_post(port, "first")
                    ),
                    daemon=True,
                )
                first.start()
                self.assertTrue(entered.wait(timeout=1), "first turn did not enter")

                started_at = time.monotonic()
                status, headers, body = self._authorized_post(port, "over-capacity")
                elapsed = time.monotonic() - started_at
                self.assertEqual(status, 429)
                self.assertLess(elapsed, 0.75, "capacity failure must not queue")
                self.assertEqual(headers.get("retry-after"), "1")
                payload = json.loads(body)
                self.assertEqual(payload["code"], "chat_capacity_exhausted")
                self.assertTrue(payload["retryable"])
                self.assertEqual(peak, 1)

                release.set()
                first.join(timeout=2)
                self.assertFalse(first.is_alive())
                self.assertEqual(first_result[0][0], 200)

                # The first request's finally block must release the sole permit.
                status, _, _ = self._authorized_post(port, "after-release")
                self.assertEqual(status, 200)
                self.assertEqual(peak, 1)

    def test_agent_failure_also_releases_the_permit(self):
        responses = [RuntimeError("injected failure"), {"kind": "chat", "reply": "ok", "cards": []}]

        def next_turn(*_args, **_kwargs):
            outcome = responses.pop(0)
            if isinstance(outcome, Exception):
                raise outcome
            return outcome

        with mock.patch.object(chat_api.agent, "run_turn", side_effect=next_turn):
            with _running_server(max_concurrent_turns=1) as port:
                failed, _, body = self._authorized_post(port, "fails")
                recovered, _, _ = self._authorized_post(port, "recovers")
        self.assertEqual(failed, 500)
        self.assertIn("injected failure", json.loads(body)["error"])
        self.assertEqual(recovered, 200)


class ChatApiSecurityContractTests(unittest.TestCase):
    def test_runtime_contract_requires_loopback_origin_and_native_token(self):
        chat_api._validate_runtime_contract(
            TEST_ORIGIN, TEST_TOKEN, False, instance_nonce=TEST_NONCE
        )
        chat_api._validate_runtime_contract(TEST_ORIGIN, "", True)

        for origin in (
            "",
            "https://evil.example",
            TEST_ORIGIN + "/",
            "http://127.0.0.1:bad",
            "http://127.0.0.1:0",
        ):
            with self.subTest(origin=origin):
                with self.assertRaises(ValueError):
                    chat_api._validate_runtime_contract(origin, TEST_TOKEN, False)
        with self.assertRaises(ValueError):
            chat_api._validate_runtime_contract(TEST_ORIGIN, "short", False)
        with self.assertRaises(ValueError):
            chat_api._validate_runtime_contract(
                TEST_ORIGIN, TEST_TOKEN, False, instance_nonce=""
            )
        with self.assertRaises(ValueError):
            chat_api._validate_runtime_contract(TEST_ORIGIN, TEST_TOKEN, False, "0.0.0.0")
        with self.assertRaises(ValueError):
            chat_api._validate_runtime_contract(TEST_ORIGIN, "", True, "0.0.0.0")
        with self.assertRaises(ValueError):
            chat_api._validate_runtime_contract(
                TEST_ORIGIN, TEST_TOKEN, False, instance_nonce="short"
            )

    def test_identity_handshake_requires_no_bearer_and_returns_only_launch_identity(self):
        with _running_server() as port:
            status, headers, body = _request(port, "GET", "/identity")
        self.assertEqual(status, 200)
        self.assertNotIn("access-control-allow-origin", headers)
        self.assertEqual(
            json.loads(body),
            {
                "service": chat_api.SERVICE_IDENTITY,
                "instance_nonce": TEST_NONCE,
            },
        )

    def test_preflight_only_echoes_the_exact_allowed_origin(self):
        with _running_server() as port:
            status, headers, _ = _request(
                port,
                "OPTIONS",
                "/design-chat",
                headers={
                    "Origin": TEST_ORIGIN,
                    "Access-Control-Request-Method": "POST",
                    "Access-Control-Request-Headers": "Content-Type, Authorization",
                },
            )
            self.assertEqual(status, 204)
            self.assertEqual(headers.get("access-control-allow-origin"), TEST_ORIGIN)
            self.assertIn("Authorization", headers.get("access-control-allow-headers", ""))

            status, headers, _ = _request(
                port,
                "OPTIONS",
                "/design-chat",
                headers={
                    "Origin": "https://evil.example",
                    "Access-Control-Request-Method": "POST",
                },
            )
            self.assertEqual(status, 403)
            self.assertNotIn("access-control-allow-origin", headers)

    def test_post_rejects_evil_origin_before_agent_even_with_valid_token(self):
        with mock.patch.object(chat_api.agent, "run_turn") as run_turn:
            with _running_server() as port:
                status, headers, _ = _request(
                    port,
                    "POST",
                    "/design-chat",
                    headers={
                        "Origin": "https://evil.example",
                        "Authorization": f"Bearer {TEST_TOKEN}",
                        # A simple/no-cors request must still be rejected server-side.
                        "Content-Type": "text/plain",
                    },
                    payload={"message": "spend the user's key", "state": {}},
                )
            self.assertEqual(status, 403)
            self.assertNotIn("access-control-allow-origin", headers)
            run_turn.assert_not_called()

    def test_native_mode_requires_bearer_and_accepts_authorized_probe(self):
        with _running_server() as port:
            for authorization in (None, "Bearer wrong"):
                headers = {"Origin": TEST_ORIGIN}
                if authorization is not None:
                    headers["Authorization"] = authorization
                status, response_headers, _ = _request(
                    port, "GET", "/health", headers=headers
                )
                self.assertEqual(status, 401)
                self.assertEqual(
                    response_headers.get("access-control-allow-origin"),
                    TEST_ORIGIN,
                )

            # URLSession/curl probes are safe without Origin when they know the secret.
            status, _, body = _request(
                port,
                "GET",
                "/health",
                headers={"Authorization": f"Bearer {TEST_TOKEN}"},
            )
            self.assertEqual(status, 200)
            payload = json.loads(body)
            self.assertTrue(payload["ok"])
            self.assertEqual(payload["service"], chat_api.SERVICE_IDENTITY)
            self.assertEqual(payload["instance_nonce"], TEST_NONCE)

    def test_authorized_native_post_reaches_agent_once(self):
        response = {"kind": "chat", "reply": "ok", "cards": []}
        with mock.patch.object(chat_api.agent, "run_turn", return_value=response) as run_turn:
            with _running_server() as port:
                status, headers, body = _request(
                    port,
                    "POST",
                    "/design-chat",
                    headers={
                        "Origin": TEST_ORIGIN,
                        "Authorization": f"Bearer {TEST_TOKEN}",
                        "Content-Type": "application/json",
                    },
                    payload={"message": "hello", "state": {}, "top": 1},
                )
            self.assertEqual(status, 200)
            self.assertEqual(headers.get("access-control-allow-origin"), TEST_ORIGIN)
            self.assertEqual(json.loads(body), response)
            run_turn.assert_called_once()

    def test_explicit_local_dev_mode_still_requires_the_exact_origin(self):
        response = {"kind": "chat", "reply": "dev", "cards": []}
        with mock.patch.object(chat_api.agent, "run_turn", return_value=response) as run_turn:
            with _running_server(token="", allow_unauthenticated=True) as port:
                status, _, _ = _request(
                    port,
                    "POST",
                    "/design-chat",
                    headers={"Origin": TEST_ORIGIN, "Content-Type": "application/json"},
                    payload={"message": "hello"},
                )
                self.assertEqual(status, 200)

                status, _, _ = _request(
                    port,
                    "POST",
                    "/design-chat",
                    headers={"Content-Type": "application/json"},
                    payload={"message": "no origin"},
                )
                self.assertEqual(status, 403)
            run_turn.assert_called_once()


if __name__ == "__main__":
    unittest.main()
