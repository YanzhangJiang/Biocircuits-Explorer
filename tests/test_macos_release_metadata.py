from __future__ import annotations

import platform
import subprocess
from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[1]
HELPER = ROOT / "packaging" / "macos_release_metadata.sh"


def _call(function: str, *arguments: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            "bash",
            "-c",
            'source "$1"; shift; "$@"',
            "metadata-test",
            str(HELPER),
            function,
            *arguments,
        ],
        text=True,
        capture_output=True,
        check=False,
    )


class MacOSReleaseMetadataTests(unittest.TestCase):
    def test_apple_marketing_version_uses_numeric_semver_core(self) -> None:
        result = _call("apple_marketing_version", "1.2.3-rc.4+build.9")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), "1.2.3")

    def test_apple_marketing_version_rejects_non_apple_core(self) -> None:
        for version in ("", "1.2", "1.2.3.4", "1.2.x", "v1.2.3"):
            with self.subTest(version=version):
                result = _call("apple_marketing_version", version)
                self.assertNotEqual(result.returncode, 0)

    def test_macos_target_arch_rejects_cross_architecture_labels(self) -> None:
        arm = _call("macos_target_arch", "arm64", "arm64")
        intel = _call("macos_target_arch", "x86_64", "x86_64")
        mismatch = _call("macos_target_arch", "x86_64", "arm64")
        unsupported = _call("macos_target_arch", "i386", "i386")

        self.assertEqual(arm.stdout.strip(), "arm64")
        self.assertEqual(intel.stdout.strip(), "x86_64")
        self.assertNotEqual(mismatch.returncode, 0)
        self.assertNotEqual(unsupported.returncode, 0)

        host = _call("macos_target_arch", "")
        self.assertEqual(host.returncode, 0, host.stderr)
        self.assertEqual(host.stdout.strip(), platform.machine())


if __name__ == "__main__":
    unittest.main()
