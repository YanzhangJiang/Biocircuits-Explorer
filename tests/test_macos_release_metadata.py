from __future__ import annotations

import os
import platform
import re
import subprocess
import tempfile
from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[1]
HELPER = ROOT / "packaging" / "macos_release_metadata.sh"
BUILD_SCRIPT = ROOT / "scripts" / "build_macos_dmg.sh"
COPY_SCRIPT = ROOT / "frontend-swift" / "scripts" / "copy_backend_into_app.sh"
XCODE_PROJECT = (
    ROOT / "frontend-swift" / "BiocircuitsExplorerMac.xcodeproj" / "project.pbxproj"
)


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


def _call_build(script: str, *arguments: str, **settings: str) -> subprocess.CompletedProcess[str]:
    env = dict(os.environ, RELEASE_MODE="local", SIGN_IDENTITY="-",
               NOTARY_PROFILE="", DESIGN_PYTHON_SOURCE="", JULIA_CHANNEL="",
               SKIP_BACKEND="0", PREBUILT_BACKEND_SHA256="", APPLE_BUILD_NUMBER="",
               TARGET_ARCH="", BACKEND_MODE="portable")
    env.update(settings)
    return subprocess.run(
        ["bash", "-c", 'source "$1"; shift\n' + script,
         "build-test", str(BUILD_SCRIPT), *arguments],
        env=env, text=True, capture_output=True, check=False,
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

    def test_apple_build_number_is_derived_from_semver_core(self) -> None:
        cases = {
            "0.0.0": "1001.0.0",
            "0.1.0": "1001.1.0",
            "1.0.0": "1002.0.0",
            "12.34.56-rc.4+build.9": "1013.34.56",
            "8998.99.99": "9999.99.99",
        }
        for version, expected in cases.items():
            with self.subTest(version=version):
                result = _call("apple_build_number", version)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(result.stdout.strip(), expected)

        for version in (
            "",
            "1.2",
            "01.2.3",
            "1.100.3",
            "1.2.100",
            "8999.0.0",
            "10000.0.0",
            "v1.2.3",
        ):
            with self.subTest(version=version):
                self.assertNotEqual(_call("apple_build_number", version).returncode, 0)

    def test_derived_build_is_newer_than_the_xcode_project_baseline(self) -> None:
        project = XCODE_PROJECT.read_text(encoding="utf-8")
        baselines = set(re.findall(r"CURRENT_PROJECT_VERSION = ([^;]+);", project))
        self.assertEqual(baselines, {"1000"})

        version = (ROOT / "VERSION").read_text(encoding="utf-8").strip()
        derived = _call("apple_build_number", version)
        self.assertEqual(derived.returncode, 0, derived.stderr)
        comparison = _call(
            "apple_build_version_is_strictly_greater",
            derived.stdout.strip(),
            "1000",
        )
        self.assertEqual(comparison.returncode, 0, comparison.stderr)

    def test_apple_build_number_override_accepts_only_one_to_three_numeric_parts(self) -> None:
        default = _call("apple_bundle_build_version", "12.34.56-rc.4+build.9", "")
        self.assertEqual(default.returncode, 0, default.stderr)
        self.assertEqual(default.stdout.strip(), "1013.34.56")

        for build_number in (
            "1014",
            "1013.35",
            "1013.34.57",
            "9999.99.99",
        ):
            with self.subTest(build_number=build_number):
                result = _call(
                    "apple_bundle_build_version",
                    "12.34.56-rc.4+build.9",
                    build_number,
                )
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(result.stdout.strip(), build_number)

        for build_number in (
            "",
            "0",
            "0001",
            "10000",
            "1.00",
            "1.01.0",
            "1.100",
            "1.1.100",
            "1.2.3.4",
            "1..2",
            ".1",
            "1.",
            "1a",
            "-1",
            " 1",
            "1",
            "1012.99.99",
            "1013.33.99",
            "1013.34.55",
            "1013.34.56",
        ):
            if not build_number:
                continue  # Empty deliberately selects the SemVer-derived default.
            with self.subTest(build_number=build_number):
                self.assertNotEqual(
                    _call(
                        "apple_bundle_build_version",
                        "12.34.56",
                        build_number,
                    ).returncode,
                    0,
                )

    def test_backend_payload_hash_is_deterministic_and_excludes_injected_python(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            executable = root / "bin" / "backend"
            executable.parent.mkdir()
            executable.write_text("backend-v1\n", encoding="utf-8")
            executable.chmod(0o755)
            (root / "share").mkdir()
            (root / "share" / "VERSION").write_text("0.1.0\n", encoding="utf-8")

            first = _call("backend_payload_sha256", str(root))
            second = _call("backend_payload_sha256", str(root))
            self.assertEqual(first.returncode, 0, first.stderr)
            self.assertEqual(first.stdout, second.stdout)
            # Golden identity from the previous shell implementation.
            self.assertEqual(first.stdout.strip(),
                             "9bdab3c41fa396940d57745f2d2633aa11db59e6857d04008555fb2f40808b6d")

            (root / "macos-release-metadata.txt").write_text(
                "metadata changes are excluded\n", encoding="utf-8"
            )
            (root / "python" / "bin").mkdir(parents=True)
            (root / "python" / "bin" / "python3").write_text(
                "injected runtime changes are excluded\n", encoding="utf-8"
            )
            excluded_change = _call("backend_payload_sha256", str(root))
            self.assertEqual(excluded_change.returncode, 0, excluded_change.stderr)
            self.assertEqual(first.stdout, excluded_change.stdout)

            executable.write_text("backend-v2\n", encoding="utf-8")
            payload_change = _call("backend_payload_sha256", str(root))
            self.assertEqual(payload_change.returncode, 0, payload_change.stderr)
            self.assertNotEqual(first.stdout, payload_change.stdout)

            executable.write_text("backend-v1\n", encoding="utf-8")
            executable.chmod(0o644)
            mode_change = _call("backend_payload_sha256", str(root))
            self.assertEqual(mode_change.returncode, 0, mode_change.stderr)
            self.assertNotEqual(first.stdout, mode_change.stdout)

            executable.chmod(0o755)
            (root / "bin" / "alias").symlink_to("backend")
            (root / "dangling").symlink_to("missing")
            linked = _call("backend_payload_sha256", str(root))
            self.assertEqual(linked.returncode, 0, linked.stderr)
            self.assertEqual(linked.stdout.strip(),
                             "37694d00a801409ab8a40f40022e758d60a3afbf4b89245ff183fa496c504cf8")

    def test_packaging_uses_direct_julia_by_default_and_validates_1_12(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            julia = Path(directory) / "julia"
            julia.write_text('#!/bin/sh\nprintf "%s\\n" "$@"\n', encoding="utf-8")
            julia.chmod(0o755)
            invoke = 'julia_command\n"${julia_cmd[@]}" --version'
            direct = _call_build(invoke, JULIA_BIN=str(julia))
            channel = _call_build(invoke, JULIA_BIN=str(julia), JULIA_CHANNEL="1.12")
            self.assertEqual(direct.returncode, 0, direct.stderr)
            self.assertEqual(direct.stdout.splitlines(), ["--startup-file=no", "--version"])
            self.assertEqual(channel.stdout.splitlines(),
                             ["+1.12", "--startup-file=no", "--version"])
            julia.write_text('#!/bin/sh\nprintf "%s\\n" "$TEST_JULIA_VERSION"\n', encoding="utf-8")
            for version, accepted in (("1.12.6", True), ("1.11.9", False), ("", False)):
                result = _call_build("validate_julia_1_12", JULIA_BIN=str(julia),
                                     TEST_JULIA_VERSION=version)
                self.assertEqual(result.returncode == 0, accepted, result.stderr)

    def test_fresh_digest_is_reused_and_prebuilt_payload_is_verified(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            payload = Path(directory) / "backend"
            payload.write_text("original", encoding="utf-8")
            fresh = _call_build('''
BACKEND_ROOT="$1"
DETECTED_JULIA_VERSION=1.12.6
write_backend_metadata
backend_payload_sha256() { echo "unexpected second hash" >&2; return 1; }
validate_backend_metadata
''', directory)
            self.assertEqual(fresh.returncode, 0, fresh.stderr)
            # Additional provenance fields do not invalidate required ones.
            with (Path(directory) / "macos-release-metadata.txt").open("a") as stream:
                stream.write("builder=contract-test\n")
            verify = 'BACKEND_ROOT="$1"\nvalidate_backend_metadata'
            prebuilt = _call_build(verify, directory)
            self.assertEqual(prebuilt.returncode, 0, prebuilt.stderr)
            pinned = _call_build(verify, directory, PREBUILT_BACKEND_SHA256="0" * 64)
            self.assertNotEqual(pinned.returncode, 0)
            payload.write_text("changed", encoding="utf-8")
            tampered = _call_build(verify, directory)
            self.assertNotEqual(tampered.returncode, 0)

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

    @unittest.skipUnless(platform.system() == "Darwin", "embedding uses macOS ditto")
    def test_backend_is_staged_in_the_standard_helpers_location(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "backend"
            source.mkdir()
            (source / "payload").write_text("fixture", encoding="utf-8")
            env = dict(os.environ, SRCROOT=str(ROOT / "frontend-swift"),
                       TARGET_BUILD_DIR=str(root / "app"),
                       CONTENTS_FOLDER_PATH="Test.app/Contents",
                       BIOCIRCUITS_EXPLORER_BACKEND_BUNDLE_SOURCE=str(source),
                       CONFIGURATION="Release")
            result = subprocess.run(["sh", str(COPY_SCRIPT)], env=env,
                                    text=True, capture_output=True, check=False)
            self.assertEqual(result.returncode, 0, result.stderr)
            destination = root / "app/Test.app/Contents/Helpers/BiocircuitsExplorerBackend"
            self.assertEqual((destination / "payload").read_text(), "fixture")

    def test_release_configuration_requires_its_external_inputs(self) -> None:
        settings = dict(RELEASE_MODE="release")
        cases = [
            ({}, "SIGN_IDENTITY"),
            ({"SIGN_IDENTITY": "Developer ID Application: test"}, "NOTARY_PROFILE"),
            ({"NOTARY_PROFILE": "test-profile"}, "DESIGN_PYTHON_SOURCE"),
            ({"DESIGN_PYTHON_SOURCE": "/example/runtime", "SKIP_BACKEND": "1"},
             "PREBUILT_BACKEND_SHA256"),
        ]
        for changes, missing in cases:
            settings.update(changes)
            result = _call_build("VERSION=1.2.3\nvalidate_release_configuration", **settings)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn(missing, result.stderr)
        settings["PREBUILT_BACKEND_SHA256"] = "a" * 64
        accepted = _call_build(
            "VERSION=1.2.3\nrequire_tool() { :; }\nvalidate_release_configuration", **settings)
        self.assertEqual(accepted.returncode, 0, accepted.stderr)

    def test_python_runtime_links_must_remain_relocatable(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "runtime"
            root.mkdir()
            (root / "python3").write_text("fixture", encoding="utf-8")
            link = root / "python"
            link.symlink_to("python3")
            valid = _call_build('validate_design_python_symlinks "$1"', str(root))
            self.assertEqual(valid.returncode, 0, valid.stderr)
            (root.parent / "external").mkdir()
            (root.parent / "external/python3").write_text("external", encoding="utf-8")
            for target in (str(root / "python3"), "../external/python3", "missing"):
                link.unlink()
                link.symlink_to(target)
                invalid = _call_build('validate_design_python_symlinks "$1"', str(root))
                self.assertNotEqual(invalid.returncode, 0)


if __name__ == "__main__":
    unittest.main()
