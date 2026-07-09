from __future__ import annotations

import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
BUILD_IMAGE = REPO_ROOT / "deploy" / "build_image.sh"
FULL_REVISION = "0123456789abcdef0123456789abcdef01234567"
SHORT_REVISION = FULL_REVISION[:12]


class BuildImageContractTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        (self.root / "deploy").mkdir()
        (self.root / "fake-bin").mkdir()
        (self.root / "scripts").mkdir()
        shutil.copy2(BUILD_IMAGE, self.root / "deploy" / "build_image.sh")
        shutil.copy2(REPO_ROOT / "deploy" / "Dockerfile", self.root / "deploy" / "Dockerfile")
        shutil.copy2(REPO_ROOT / "VERSION", self.root / "VERSION")
        docker = self.root / "fake-bin" / "docker"
        docker.write_text(
            "#!/bin/sh\n"
            "if [ \"${1:-}\" = buildx ] && [ \"${2:-}\" = version ]; then\n"
            "  exit \"${FAKE_BUILDX_STATUS:-0}\"\n"
            "fi\n"
            "exit 0\n",
            encoding="utf-8",
        )
        docker.chmod(0o755)
        git = self.root / "fake-bin" / "git"
        git.write_text(
            "#!/bin/sh\n"
            "if [ \"${1:-}\" = -C ]; then shift 2; fi\n"
            "case \"${1:-}:${2:-}:${3:-}\" in\n"
            "  rev-parse:--is-inside-work-tree:) [ \"${FAKE_GIT_NOT_REPO:-0}\" = 1 ] && exit 1; echo true; exit 0 ;;\n"
            f"  rev-parse:HEAD:) echo {FULL_REVISION}; exit 0 ;;\n"
            f"  rev-parse:--short=12:HEAD) echo {SHORT_REVISION}; exit 0 ;;\n"
            "  diff:--cached:*) exit \"${FAKE_GIT_INDEX_DIRTY:-0}\" ;;\n"
            "  diff:*) exit \"${FAKE_GIT_WORKTREE_DIRTY:-0}\" ;;\n"
            "  ls-files:--others:*) [ \"${FAKE_GIT_UNTRACKED:-0}\" = 1 ] && echo untracked.txt; exit 0 ;;\n"
            "esac\n"
            "exit 1\n",
            encoding="utf-8",
        )
        git.chmod(0o755)
        version_gate = self.root / "scripts" / "set_version.sh"
        version_gate.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
        version_gate.chmod(0o755)

    def tearDown(self):
        self.temporary.cleanup()

    def run_script(
        self,
        *arguments: str,
        environment_overrides: dict[str, str] | None = None,
    ) -> subprocess.CompletedProcess[str]:
        environment = os.environ.copy()
        environment["PATH"] = str(self.root / "fake-bin") + os.pathsep + environment["PATH"]
        environment.update(environment_overrides or {})
        return subprocess.run(
            [str(self.root / "deploy" / "build_image.sh"), *arguments],
            cwd=self.root,
            env=environment,
            text=True,
            capture_output=True,
            check=False,
        )

    def set_version(self, value: str) -> None:
        (self.root / "VERSION").write_text(value, encoding="utf-8")

    def test_valid_build_metadata_is_preserved_but_projected_for_docker_tags(self):
        self.set_version("1.7.3-rc.2+build.5\n")

        result = self.run_script("--dry-run", "--repo", "example/image")

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("Version:    1.7.3-rc.2+build.5", result.stdout)
        self.assertIn("Version tag: 1.7.3-rc.2_build.5", result.stdout)
        self.assertIn("example/image:1.7.3-rc.2_build.5", result.stdout)
        self.assertIn("BIOCIRCUITS_EXPLORER_VERSION=1.7.3-rc.2+build.5", result.stdout)
        self.assertIn("org.opencontainers.image.version=1.7.3-rc.2+build.5", result.stdout)

    def test_explicit_version_must_match_the_authoritative_version_file(self):
        result = self.run_script("--dry-run", "--version", "9.9.9")

        self.assertEqual(result.returncode, 2)
        self.assertIn("does not match authoritative VERSION", result.stderr)
        self.assertNotIn("docker build", result.stdout)

    def test_full_revision_is_used_in_oci_metadata_and_short_revision_in_tag(self):
        result = self.run_script("--dry-run", "--repo", "example/image")

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn(f"Revision:   {FULL_REVISION}", result.stdout)
        self.assertIn(f"BIOCIRCUITS_EXPLORER_REVISION={FULL_REVISION}", result.stdout)
        self.assertIn(f"org.opencontainers.image.revision={FULL_REVISION}", result.stdout)
        self.assertIn(f"example/image:0.1.0-{SHORT_REVISION}", result.stdout)
        self.assertNotIn(f"example/image:0.1.0-{FULL_REVISION}", result.stdout)

    def test_push_rejects_every_kind_of_dirty_git_state(self):
        dirty_states = (
            {"FAKE_GIT_WORKTREE_DIRTY": "1"},
            {"FAKE_GIT_INDEX_DIRTY": "1"},
            {"FAKE_GIT_UNTRACKED": "1"},
        )
        for dirty in dirty_states:
            with self.subTest(dirty=dirty):
                result = self.run_script(
                    "--dry-run",
                    "--push",
                    "--repo",
                    "example/image",
                    environment_overrides=dirty,
                )
                self.assertEqual(result.returncode, 1)
                self.assertIn("Refusing to push", result.stderr)
                self.assertNotIn("docker build", result.stdout)

    def test_push_rejects_source_tree_without_git_provenance(self):
        result = self.run_script(
            "--dry-run",
            "--push",
            "--repo",
            "example/image",
            environment_overrides={"FAKE_GIT_NOT_REPO": "1"},
        )

        self.assertEqual(result.returncode, 1)
        self.assertIn("without a Git worktree", result.stderr)
        self.assertNotIn("docker build", result.stdout)

    def test_push_rejects_latest_and_ecr_creation_enforces_immutable_tags(self):
        latest = self.run_script("--dry-run", "--push", "--latest")
        disguised_latest = self.run_script("--dry-run", "--push", "--tag", "latest")
        ecr = self.run_script(
            "--dry-run",
            "--push",
            "--create-ecr-repo",
            "--repo",
            "123456789012.dkr.ecr.us-west-2.amazonaws.com/biocircuits-explorer",
        )

        self.assertEqual(latest.returncode, 2)
        self.assertIn("Refusing to push the mutable :latest", latest.stderr)
        self.assertEqual(disguised_latest.returncode, 2)
        self.assertIn("Refusing to push the mutable :latest", disguised_latest.stderr)
        self.assertEqual(ecr.returncode, 0, ecr.stderr)
        self.assertIn("--image-tag-mutability IMMUTABLE", ecr.stdout)
        self.assertIn("scanOnPush=true", ecr.stdout)

    def test_local_dirty_build_is_labeled_and_require_clean_still_rejects_it(self):
        dirty = {"FAKE_GIT_UNTRACKED": "1"}
        local = self.run_script("--dry-run", environment_overrides=dirty)
        strict = self.run_script("--dry-run", "--require-clean", environment_overrides=dirty)

        self.assertEqual(local.returncode, 0, local.stderr)
        self.assertIn(f"Revision:   {FULL_REVISION}-dirty", local.stdout)
        self.assertIn(f"biocircuits-explorer:0.1.0-{SHORT_REVISION}-dirty", local.stdout)
        self.assertEqual(strict.returncode, 1)
        self.assertIn("Git worktree is dirty", strict.stderr)

    def test_invalid_semver_is_rejected_before_building(self):
        invalid_versions = (
            "01.2.3\n",
            "1.02.3\n",
            "1.2.03\n",
            "1.2.3-01\n",
            "1.2.3-alpha..1\n",
            "1.2.3+\n",
            "1. 2.3\n",
            "1.2.3\n4.5.6\n",
            "1.2.3\n\n",
        )
        for invalid in invalid_versions:
            with self.subTest(version=invalid):
                self.set_version(invalid)
                result = self.run_script("--dry-run")
                self.assertEqual(result.returncode, 2)
                self.assertRegex(result.stderr, r"Semantic Versioning 2\.0\.0|exactly one")
                self.assertNotIn("docker build", result.stdout)

    def test_single_crlf_version_line_is_accepted(self):
        (self.root / "VERSION").write_bytes(b"2.4.6-rc.1\r\n")

        result = self.run_script("--dry-run")

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("Version:    2.4.6-rc.1", result.stdout)

    def test_missing_or_option_shaped_values_are_usage_errors(self):
        for arguments in (("--version",), ("--repo", "--push"), ("--tag", "")):
            with self.subTest(arguments=arguments):
                result = self.run_script(*arguments)
                self.assertEqual(result.returncode, 2)
                self.assertIn("requires a value", result.stderr)
                self.assertNotIn("unbound variable", result.stderr)

    def test_invalid_and_overlong_extra_tags_are_rejected(self):
        for tag in ("bad+tag", "-leading-dash", "x" * 129):
            with self.subTest(tag=tag):
                result = self.run_script("--dry-run", "--tag", tag)
                self.assertEqual(result.returncode, 2)
                self.assertIn("Invalid Docker tag", result.stderr)

    def test_duplicate_tags_are_emitted_once(self):
        result = self.run_script("--dry-run", "--latest", "--tag", "latest")

        self.assertEqual(result.returncode, 0, result.stderr)
        tag_lines = [line.strip() for line in result.stdout.splitlines() if line.strip().endswith(":latest")]
        self.assertEqual(tag_lines, ["- biocircuits-explorer:latest"])


if __name__ == "__main__":
    unittest.main()
