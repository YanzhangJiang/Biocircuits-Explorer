from __future__ import annotations

import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import tempfile
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
SET_VERSION = REPO_ROOT / "scripts" / "set_version.sh"
REQUIRED_FILES = (
    "VERSION",
    "webapp/Project.toml",
    "webapp/Manifest.toml",
    "packaging/Project.toml",
    "packaging/Manifest.toml",
    "webapp_hpc/Project.toml",
    "webapp_hpc/Manifest.toml",
    "webapp/package.json",
    "webapp/package-lock.json",
)
MANIFEST_TARGETS = {
    "webapp/Manifest.toml": "BiocircuitsExplorerBackend",
    "packaging/Manifest.toml": "BiocircuitsExplorerPackaging",
    "webapp_hpc/Manifest.toml": "BiocircuitsExplorerBackendHPC",
}
NON_OWNED_MANIFEST = "webapp/Manifest-v1.10.toml"


class SetVersionTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        for relative in REQUIRED_FILES:
            destination = self.root / relative
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(REPO_ROOT / relative, destination)
        legacy_manifest = self.root / NON_OWNED_MANIFEST
        shutil.copy2(REPO_ROOT / NON_OWNED_MANIFEST, legacy_manifest)

    def tearDown(self):
        self.temporary.cleanup()

    def run_script(self, *arguments: str) -> subprocess.CompletedProcess[str]:
        environment = os.environ.copy()
        environment["BCX_ROOT_DIR"] = str(self.root)
        return subprocess.run(
            [str(SET_VERSION), *arguments],
            cwd=self.root,
            env=environment,
            text=True,
            capture_output=True,
            check=False,
        )

    def snapshot(self, relatives=REQUIRED_FILES):
        return {
            relative: (
                (self.root / relative).read_bytes(),
                (self.root / relative).stat().st_mode,
                (self.root / relative).stat().st_ino,
                (self.root / relative).stat().st_mtime_ns,
            )
            for relative in relatives
        }

    def assert_all_versions(self, expected: str):
        self.assertEqual((self.root / "VERSION").read_text(encoding="utf-8").strip(), expected)
        for relative in (
            "webapp/Project.toml",
            "packaging/Project.toml",
            "webapp_hpc/Project.toml",
        ):
            text = (self.root / relative).read_text(encoding="utf-8")
            match = re.search(r'(?m)^version[ \t]*=[ \t]*["\']([^"\']+)["\']', text)
            self.assertIsNotNone(match)
            self.assertEqual(match.group(1), expected)

        for relative, package_name in MANIFEST_TARGETS.items():
            text = (self.root / relative).read_text(encoding="utf-8")
            section = re.search(
                rf"(?ms)^\[\[deps\.{re.escape(package_name)}\]\].*?(?=^\[\[deps\.|\Z)",
                text,
            )
            self.assertIsNotNone(section)
            versions = re.findall(
                r'(?m)^version[ \t]*=[ \t]*["\']([^"\']+)["\']',
                section.group(0),
            )
            self.assertEqual(versions, [expected])

        package = json.loads((self.root / "webapp/package.json").read_text(encoding="utf-8"))
        package_lock = json.loads(
            (self.root / "webapp/package-lock.json").read_text(encoding="utf-8")
        )
        self.assertEqual(package["version"], expected)
        self.assertEqual(package_lock["version"], expected)
        self.assertEqual(package_lock["packages"][""]["version"], expected)

    def test_script_remains_executable(self):
        self.assertTrue(os.access(SET_VERSION, os.X_OK))

    def test_dry_run_lists_every_target_without_mutation(self):
        before = self.snapshot()

        result = self.run_script("--dry-run", "2.4.0-rc.1")

        self.assertEqual(result.returncode, 0, result.stderr)
        for relative in REQUIRED_FILES:
            self.assertIn(f"[dry-run] would update {relative}", result.stdout)
        self.assertEqual(self.snapshot(), before)

    def test_valid_prerelease_updates_all_fields_and_preserves_modes_and_newlines(self):
        package_path = self.root / "webapp/package.json"
        package_path.write_bytes(package_path.read_bytes().replace(b"\n", b"\r\n"))
        package_path.chmod(0o640)
        modes_before = {relative: (self.root / relative).stat().st_mode for relative in REQUIRED_FILES}
        inodes_before = {relative: (self.root / relative).stat().st_ino for relative in REQUIRED_FILES}
        legacy_before = (self.root / NON_OWNED_MANIFEST).read_bytes()

        result = self.run_script("1.7.3-rc.2+build.5")

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assert_all_versions("1.7.3-rc.2+build.5")
        self.assertEqual(
            {relative: (self.root / relative).stat().st_mode for relative in REQUIRED_FILES},
            modes_before,
        )
        self.assertTrue(
            all(
                (self.root / relative).stat().st_ino != inodes_before[relative]
                for relative in REQUIRED_FILES
            )
        )
        package_bytes = package_path.read_bytes()
        self.assertIn(b"\r\n", package_bytes)
        self.assertNotIn(b"\n", package_bytes.replace(b"\r\n", b""))
        self.assertTrue(package_bytes.endswith(b"\r\n"))
        self.assertEqual((self.root / NON_OWNED_MANIFEST).read_bytes(), legacy_before)

    def test_invalid_semver_never_mutates_files(self):
        invalid_versions = ("1.2", "01.2.3", "1.2.3-01", "1.2.3-")
        for invalid in invalid_versions:
            with self.subTest(version=invalid):
                before = self.snapshot()
                result = self.run_script(invalid)
                self.assertEqual(result.returncode, 2)
                self.assertIn("Semantic Versioning", result.stderr)
                self.assertEqual(self.snapshot(), before)

    def test_missing_required_file_causes_no_partial_update(self):
        missing = "webapp_hpc/Project.toml"
        (self.root / missing).unlink()
        remaining = tuple(relative for relative in REQUIRED_FILES if relative != missing)
        before = self.snapshot(remaining)

        result = self.run_script("2.0.0")

        self.assertEqual(result.returncode, 1)
        self.assertIn(missing, result.stderr)
        self.assertEqual(self.snapshot(remaining), before)

    def test_malformed_version_field_causes_no_partial_update(self):
        lock_path = self.root / "webapp/package-lock.json"
        lock = json.loads(lock_path.read_text(encoding="utf-8"))
        del lock["packages"][""]["version"]
        lock_path.write_text(json.dumps(lock, indent=2) + "\n", encoding="utf-8")
        before = self.snapshot()

        result = self.run_script("2.0.0")

        self.assertEqual(result.returncode, 1)
        self.assertIn("packages[''].version", result.stderr)
        self.assertEqual(self.snapshot(), before)

    def test_malformed_self_manifest_causes_no_partial_update(self):
        manifest_path = self.root / "webapp_hpc/Manifest.toml"
        manifest = manifest_path.read_text(encoding="utf-8")
        manifest_path.write_text(
            manifest.replace(
                '[[deps.BiocircuitsExplorerBackendHPC]]',
                '[[deps.BiocircuitsExplorerBackendHPC_BROKEN]]',
                1,
            ),
            encoding="utf-8",
        )
        before = self.snapshot()

        result = self.run_script("2.0.0")

        self.assertEqual(result.returncode, 1)
        self.assertIn("BiocircuitsExplorerBackendHPC", result.stderr)
        self.assertEqual(self.snapshot(), before)

    def test_duplicate_self_manifest_entry_causes_no_partial_update(self):
        manifest_path = self.root / "packaging/Manifest.toml"
        manifest = manifest_path.read_text(encoding="utf-8")
        marker = "[[deps.BiocircuitsExplorerPackaging]]"
        start = manifest.index(marker)
        next_section = manifest.find("[[deps.", start + len(marker))
        block = manifest[start : next_section if next_section >= 0 else len(manifest)]
        manifest_path.write_text(manifest + "\n" + block, encoding="utf-8")
        before = self.snapshot()

        result = self.run_script("2.0.0")

        self.assertEqual(result.returncode, 1)
        self.assertIn("found 2", result.stderr)
        self.assertEqual(self.snapshot(), before)

    def test_repeating_the_same_version_is_byte_and_metadata_idempotent(self):
        first = self.run_script("3.1.4-beta.1")
        self.assertEqual(first.returncode, 0, first.stderr)
        before = self.snapshot()

        second = self.run_script("3.1.4-beta.1")

        self.assertEqual(second.returncode, 0, second.stderr)
        self.assertIn("no files changed", second.stdout)
        self.assertEqual(self.snapshot(), before)
        self.assert_all_versions("3.1.4-beta.1")


if __name__ == "__main__":
    unittest.main()
