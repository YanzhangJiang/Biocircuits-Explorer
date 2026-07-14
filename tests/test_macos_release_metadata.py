from __future__ import annotations

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
            self.assertRegex(first.stdout.strip(), r"^[0-9a-f]{64}$")

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

    def test_packaging_uses_direct_julia_by_default_and_validates_1_12(self) -> None:
        script = BUILD_SCRIPT.read_text(encoding="utf-8")
        self.assertIn('JULIA_CHANNEL="${JULIA_CHANNEL-}"', script)
        self.assertNotIn('JULIA_CHANNEL="${JULIA_CHANNEL:-1.12}"', script)
        self.assertIn('julia_cmd+=("+${JULIA_CHANNEL}")', script)
        self.assertIn("julia_cmd+=(--startup-file=no)", script)
        self.assertEqual(script.count("julia_cmd+=(--startup-file=no)"), 1)
        self.assertNotIn(
            '"${julia_cmd[@]}" --startup-file=no',
            script,
            "startup isolation must be centralized in julia_command()",
        )
        self.assertIn("validate_julia_1_12()", script)
        self.assertIn("macOS packaging requires Julia 1.12", script)
        self.assertIn("validate_julia_1_12\n", script)

    def test_release_skip_requires_pinned_julia_1_12_backend_provenance(self) -> None:
        script = BUILD_SCRIPT.read_text(encoding="utf-8")
        for required in (
            "RELEASE_MODE=release with SKIP_BACKEND=1 requires a pinned lowercase PREBUILT_BACKEND_SHA256",
            "backend_payload_sha256=",
            'julia_version="$(/usr/bin/sed',
            "Prebuilt backend provenance does not declare Julia 1.12",
            'actual_sha256="$(backend_payload_sha256',
            'recorded_sha256}" != "${PREBUILT_BACKEND_SHA256}',
        ):
            self.assertIn(required, script)

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

    def test_backend_is_staged_in_the_standard_helpers_location(self) -> None:
        script = COPY_SCRIPT.read_text(encoding="utf-8")
        self.assertIn('${APP_CONTENTS_DIR}/Helpers', script)
        self.assertIn('BiocircuitsExplorerBackend', script)
        self.assertNotIn('${APP_RESOURCES_DIR}/backend', script)

    def test_release_mode_requires_notarization_and_avoids_deep_signing(self) -> None:
        script = BUILD_SCRIPT.read_text(encoding="utf-8")
        for required in (
            'RELEASE_MODE=release requires SIGN_IDENTITY',
            'RELEASE_MODE=release requires a NOTARY_PROFILE',
            'A formal prerelease/build-metadata VERSION requires a strictly increasing APPLE_BUILD_NUMBER override',
            'sign_args=(--force --options runtime',
            'notarytool submit',
            'stapler staple',
            'spctl --assess',
            'validate_macho_architectures "${APP_DEST}"',
        ):
            self.assertIn(required, script)
        self.assertNotIn('codesign --force --deep --sign', script)

    def test_portable_release_does_not_copy_the_user_depot_or_scratchspaces(self) -> None:
        script = BUILD_SCRIPT.read_text(encoding="utf-8")
        self.assertNotIn('EXTRA_SOURCE_DEPOT', script)
        self.assertNotIn('packages artifacts scratchspaces', script)
        self.assertIn('JULIA_DEPOT_PATH="${LOCAL_DEPOT}"', script)

    def test_release_bundles_and_probes_a_relocatable_design_python(self) -> None:
        script = BUILD_SCRIPT.read_text(encoding="utf-8")
        for required in (
            "RELEASE_MODE=release requires DESIGN_PYTHON_SOURCE",
            'DESIGN_PYTHON_ROOT="${BACKEND_ROOT}/python"',
            '"${python_executable}" -I -B -X utf8',
            "import chat_api",
            'validate_macho_architectures "${runtime_root}"',
            'validate_macho_load_paths "${runtime_root}"',
            "/usr/bin/otool -L",
            '"cmd" && $2 == "LC_RPATH"',
            "validate_design_python_symlinks",
            "design-python-runtime-metadata.txt",
            '[[ "$path" != */Contents/Helpers/BiocircuitsExplorerBackend/python/* ]]',
            "require_within(sys.executable, runtime_root",
            "require_within(sys.prefix, runtime_root",
            "require_within(sys.base_prefix, runtime_root",
            "require_within(chat_api.__file__, script_directory",
            "for index, entry in enumerate(sys.path)",
            "Probing signed Design Chat Python",
        ):
            self.assertIn(required, script)
        self.assertNotIn("curl ", script)
        self.assertNotIn("wget ", script)

        swift = (
            ROOT
            / "frontend-swift"
            / "BiocircuitsExplorerMac"
            / "DesignChatBackendController.swift"
        ).read_text(encoding="utf-8")
        resolver = swift.index("private func resolvePythonExecutable()")
        bundled_lookup = swift.index(
            "Self.bundledPythonExecutableCandidates(resourceURL: resourceURL)",
            resolver,
        )
        path_lookup = swift.index('executableSearchCandidates(named: "python3")', resolver)
        self.assertLess(bundled_lookup, path_lookup)
        self.assertIn('appendingPathComponent("python", isDirectory: true)', swift)
        self.assertIn('arguments: ["-I", "-B", "-X", "utf8", scriptURL.path]', swift)


if __name__ == "__main__":
    unittest.main()
