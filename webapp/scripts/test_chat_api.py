#!/usr/bin/env python3
import sys
import unittest

import chat_api


class _BrokenStderr:
    def write(self, _message):
        raise BrokenPipeError("stderr pipe is closed")


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


if __name__ == "__main__":
    unittest.main()
