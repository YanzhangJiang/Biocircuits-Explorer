from __future__ import annotations

import contextlib
import importlib.util
import io
import json
import re
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


REPO_ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "verify_repository", REPO_ROOT / "scripts" / "verify_repository.py"
)
assert SPEC is not None and SPEC.loader is not None
verify_repository = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = verify_repository
SPEC.loader.exec_module(verify_repository)


class PathAndMarkdownTests(unittest.TestCase):
    def test_balanced_parentheses_in_markdown_destinations(self):
        text = "[one](docs/a_(draft).md) and ![two](images/b.png \"caption\")"
        self.assertEqual(
            verify_repository.markdown_inline_destinations(text),
            ["docs/a_(draft).md", "images/b.png"],
        )

    def test_existing_markdown_link_cannot_escape_repository(self):
        with tempfile.TemporaryDirectory() as temporary:
            parent = Path(temporary)
            root = parent / "repo"
            docs = root / "docs"
            docs.mkdir(parents=True)
            (parent / "outside.md").write_text("outside\n", encoding="utf-8")
            relative = Path("docs/page.md")
            (root / relative).write_text("[escape](../../outside.md)\n", encoding="utf-8")
            audit = verify_repository.Audit()

            verify_repository.check_markdown_file(root, relative, audit)

            self.assertTrue(any("escapes repository" in error for error in audit.errors))

    def test_markdown_heading_fragment_must_exist(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            docs = root / "docs"
            docs.mkdir()
            (docs / "target.md").write_text(
                "# Existing heading\n\n## Repeated\n\n## Repeated\n",
                encoding="utf-8",
            )
            good = Path("docs/good.md")
            bad = Path("docs/bad.md")
            (root / good).write_text(
                "[one](target.md#existing-heading) [two](target.md#repeated-1)\n",
                encoding="utf-8",
            )
            (root / bad).write_text("[missing](target.md#not-there)\n", encoding="utf-8")

            good_audit = verify_repository.Audit()
            verify_repository.check_markdown_file(root, good, good_audit)
            bad_audit = verify_repository.Audit()
            verify_repository.check_markdown_file(root, bad, bad_audit)

            self.assertEqual(good_audit.errors, [])
            self.assertTrue(any("broken Markdown heading fragment" in error for error in bad_audit.errors))

    def test_maintained_scope_covers_entrypoints_and_knowledge_tree(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "README.md").write_text("maintained\n", encoding="utf-8")
            (root / "PROJECT_SUMMARY.md").write_text("maintained\n", encoding="utf-8")
            nested = root / "knowledge" / "contracts"
            nested.mkdir(parents=True)
            (nested / "api.md").write_text("# API\n", encoding="utf-8")
            (root / "knowledge" / "notes.txt").write_text("not markdown\n", encoding="utf-8")

            relatives = verify_repository.maintained_markdown_paths(root)

            self.assertEqual(
                relatives,
                [
                    Path("README.md"),
                    Path("PROJECT_SUMMARY.md"),
                    Path("knowledge/contracts/api.md"),
                ],
            )

    def test_private_key_and_major_token_shapes_are_detected(self):
        markers = verify_repository.find_private_markers(
            "-----BEGIN RSA " + "PRIVATE KEY-----\n"
            + "ghp_" + "A" * 36 + "\n"
            + "sk-" + "proj-" + "b" * 32
        )
        self.assertIn("PEM private-key header", markers)
        self.assertIn("GitHub token pattern", markers)
        self.assertIn("OpenAI token pattern", markers)
        self.assertEqual(verify_repository.find_private_markers("a sketch-project note"), [])

    def test_ai_provider_token_shapes_are_detected(self):
        markers = verify_repository.find_private_markers(
            "sk-ant-" + "a" * 24 + "\n"
            + "AIza" + "b" * 35 + "\n"
            + "hf_" + "c" * 24 + "\n"
            + "xai-" + "d" * 24 + "\n"
            + "xoxb-" + "e" * 24
        )
        self.assertIn("Anthropic token pattern", markers)
        self.assertIn("Google API key pattern", markers)
        self.assertIn("Hugging Face token pattern", markers)
        self.assertIn("xAI token pattern", markers)
        self.assertIn("Slack token pattern", markers)

    def test_public_path_policy_rejects_private_research_material(self):
        violation = verify_repository.public_repository_path_violation
        self.assertIsNone(violation(Path("src/periodic_table/result_schema.py")))
        self.assertIsNotNone(violation(Path("paper_rop_periodic_table/data/slices.jsonl.gz")))
        self.assertIsNotNone(violation(Path("manuscripts/draft.tex")))
        self.assertIsNotNone(violation(Path("notes/reviewer_response.docx")))

    def test_manuscript_file_types_are_rejected_even_when_force_added(self):
        violation = verify_repository.public_repository_path_violation
        for path in (
            "docs/guide.pdf",
            "notes/derivation.tex",
            "references.bib",
            "submissions/paper.docx",
            "webapp/public/media/附录.pdf",
        ):
            self.assertIsNotNone(violation(Path(path)), path)
        for path in ("webapp/public/media/main.png", "notebooks/example.ipynb", "README.md"):
            self.assertIsNone(violation(Path(path)), path)
        self.assertEqual(verify_repository.find_private_markers(
            "git clone git@github.com:public/example.git; use /tmp/example or file://example"
        ), [])

    def test_notebook_policy_requires_no_outputs_or_execution_counts(self):
        audit = verify_repository.Audit()
        verify_repository.check_notebook_is_clear(
            Path("notebooks/example.ipynb"),
            json.dumps({"cells": [{"cell_type": "code", "execution_count": 1, "outputs": [{"output_type": "stream"}]}]}),
            audit,
        )
        self.assertTrue(any("contains output" in error for error in audit.errors))
        self.assertTrue(any("execution_count" in error for error in audit.errors))


class VersionInventoryTests(unittest.TestCase):
    def test_version_lines_compare_components_not_prefixes(self):
        self.assertEqual(verify_repository.major_minor_line("1.12"), (1, 12))
        self.assertEqual(verify_repository.major_minor_line("1.12.6-alpine"), (1, 12))
        self.assertNotEqual(
            verify_repository.major_minor_line("1.1"),
            verify_repository.major_minor_line("1.12"),
        )

    def test_project_toml_reader_separates_root_and_compatibility_fields(self):
        text = (
            'name = "Example"\n'
            'version = "1.2.3"\n\n'
            '[deps]\n'
            'version = "dependency-value"\n\n'
            '[compat]\n'
            'julia = "1.12"\n'
        )
        self.assertEqual(verify_repository.project_toml_string(text, "version"), "1.2.3")
        self.assertEqual(
            verify_repository.project_toml_string(text, "julia", section="compat"),
            "1.12",
        )

    def test_manifest_self_version_requires_exact_package_identity(self):
        package = "ExampleApplication"
        uuid = "11111111-2222-3333-4444-555555555555"
        text = (
            "julia_version = \"1.12.6\"\n\n"
            "[[deps.Dependency]]\n"
            "uuid = \"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee\"\n"
            "version = \"9.9.9\"\n\n"
            "[[deps.ExampleApplication]]\n"
            "path = \".\"\n"
            f"uuid = \"{uuid}\"\n"
            "version = \"1.2.3-rc.1+build.4\"\n"
        )

        self.assertEqual(
            verify_repository.manifest_self_version(text, package, uuid),
            "1.2.3-rc.1+build.4",
        )
        with self.assertRaisesRegex(ValueError, "uuid"):
            verify_repository.manifest_self_version(text, package, "wrong")
        with self.assertRaisesRegex(ValueError, "found 2"):
            verify_repository.manifest_self_version(text + text[text.index("[[deps.Example") :], package, uuid)

    def test_version_file_requires_one_strict_semver_line(self):
        self.assertEqual(verify_repository.version_file_value("1.2.3+build.4\n"), "1.2.3+build.4")
        for invalid in ("01.2.3\n", "1.2.3-01\n", "1.2.3\nextra\n", " 1.2.3\n"):
            with self.subTest(invalid=invalid):
                with self.assertRaises(ValueError):
                    verify_repository.version_file_value(invalid)

    def test_version_json_rejects_duplicate_keys_and_non_objects(self):
        self.assertEqual(
            verify_repository.unique_json_object('{"version":"1.2.3"}', "package"),
            {"version": "1.2.3"},
        )
        with self.assertRaisesRegex(ValueError, "duplicate JSON key"):
            verify_repository.unique_json_object(
                '{"version":"1.2.3","version":"9.9.9"}',
                "package",
            )
        with self.assertRaisesRegex(ValueError, "top-level JSON object"):
            verify_repository.unique_json_object('["1.2.3"]', "package")

    def test_manifest_version_drift_is_rejected_by_inventory(self):
        owned_files = (
            "VERSION",
            "webapp/Project.toml",
            "webapp/Manifest.toml",
            "packaging/Project.toml",
            "packaging/Manifest.toml",
            "webapp_hpc/Project.toml",
            "webapp_hpc/Manifest.toml",
            "webapp/package.json",
            "webapp/package-lock.json",
            "deploy/Dockerfile",
            "frontend-swift/BiocircuitsExplorerMac.xcodeproj/project.pbxproj",
        )
        ci_document = {
            "jobs": {
                "lint-js": {
                    "steps": [
                        {"uses": "actions/setup-node@v4", "with": {"node-version": "20"}},
                        {"uses": "actions/setup-python@v5", "with": {"python-version": "3.13"}},
                    ]
                },
                "test-julia": {"strategy": {"matrix": {"julia": ["1.12"]}}},
                "test-hpc-environment": {
                    "strategy": {"matrix": {"julia": ["1.10", "1.12"]}}
                },
            }
        }
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            for relative in owned_files:
                destination = root / relative
                destination.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(REPO_ROOT / relative, destination)

            manifest_path = root / "webapp/Manifest.toml"
            text = manifest_path.read_text(encoding="utf-8")
            package = "BiocircuitsExplorerBackend"
            section = re.search(
                rf"(?ms)^\[\[deps\.{package}\]\].*?(?=^\[\[deps\.|\Z)",
                text,
            )
            self.assertIsNotNone(section)
            mutated = section.group(0).replace('version = "0.1.0"', 'version = "9.9.9"', 1)
            manifest_path.write_text(
                text[: section.start()] + mutated + text[section.end() :],
                encoding="utf-8",
            )

            audit = verify_repository.Audit()
            with mock.patch.object(verify_repository, "load_yaml", return_value=ci_document):
                verify_repository.version_inventory(root, audit)

        self.assertTrue(any("application version drift" in error for error in audit.errors))
        self.assertTrue(any("webapp/Manifest.toml" in error for error in audit.errors))

    def test_check_ignores_unrelated_worktree_activity(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            subprocess.run(["git", "init", "-q"], cwd=root, check=True)
            (root / "baseline.txt").write_text("baseline\n", encoding="utf-8")
            subprocess.run(["git", "add", "baseline.txt"], cwd=root, check=True)

            def run_command(command_root, _command, _audit, label):
                if label == "generated schema check":
                    (command_root / "side-effect.tmp").write_text("unexpected\n", encoding="utf-8")
                return ""

            output = io.StringIO()
            with mock.patch.object(verify_repository, "version_inventory"), \
                 mock.patch.object(verify_repository, "check_public_repository_safety"), \
                 mock.patch.object(verify_repository, "run_command", side_effect=run_command), \
                 contextlib.redirect_stdout(output):
                status = verify_repository.verify(root, external=True)

            self.assertEqual(status, 0, output.getvalue())
            self.assertEqual((root / "baseline.txt").read_text(), "baseline\n")


if __name__ == "__main__":
    unittest.main()
