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
    def test_repository_paths_reject_absolute_parent_and_glob_without_opt_in(self):
        safe = verify_repository.is_safe_repo_path
        self.assertTrue(safe("knowledge/contracts/api.md"))
        self.assertFalse(safe("/etc/passwd"))
        self.assertFalse(safe("knowledge/../README.md"))
        self.assertFalse(safe("schemas/*.json"))
        self.assertTrue(safe("schemas/*.json", allow_glob=True))

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

    def test_manifest_document_symlink_cannot_escape_repository(self):
        with tempfile.TemporaryDirectory() as temporary:
            parent = Path(temporary)
            root = parent / "repo"
            (root / "knowledge" / "catalogs").mkdir(parents=True)
            for name in ("README.md", "PROJECT_SUMMARY.md", "AGENTS.md", "CLAUDE.md"):
                (root / name).write_text("maintained\n", encoding="utf-8")
            outside = parent / "outside.md"
            outside.write_text("outside\n", encoding="utf-8")
            (root / "knowledge" / "out.md").symlink_to(outside)
            for name in ("modules.yaml", "contracts.yaml", "artifacts.yaml"):
                (root / "knowledge" / "catalogs" / name).write_text("{}\n", encoding="utf-8")
            common = {
                "baseline_evidence_revision": "baseline",
                "verification_command": "python3 scripts/verify_repository.py --check",
            }
            manifest = {
                **common,
                "schema_version": "bcx-knowledge-manifest/v1.0.0",
                "entrypoints": [],
                "documents": [{"id": "outside", "path": "knowledge/out.md"}],
                "catalogs": {
                    "modules": "knowledge/catalogs/modules.yaml",
                    "contracts": "knowledge/catalogs/contracts.yaml",
                    "artifacts": "knowledge/catalogs/artifacts.yaml",
                },
                "active_context_packs": [],
            }
            modules = {**common, "schema_version": "bcx-module-catalog/v1.0.0", "modules": []}
            contracts = {**common, "schema_version": "bcx-contract-catalog/v1.0.0", "contracts": []}
            artifacts = {**common, "schema_version": "bcx-artifact-catalog/v1.0.0", "artifacts": []}
            audit = verify_repository.Audit()

            verify_repository.validate_knowledge(
                root, manifest, modules, contracts, artifacts, audit
            )

            self.assertTrue(any("path escapes repository in manifest document" in error for error in audit.errors))

    def test_private_key_and_major_token_shapes_are_detected(self):
        markers = verify_repository.find_private_markers(
            "-----BEGIN TEST PRIVATE KEY-----\n"
            + "ghp_" + "A" * 36 + "\n"
            + "sk-" + "proj-" + "b" * 32
        )
        self.assertIn("PEM private-key header", markers)
        self.assertIn("GitHub token pattern", markers)
        self.assertIn("OpenAI token pattern", markers)
        self.assertEqual(verify_repository.find_private_markers("a sketch-project note"), [])

    def test_manifest_document_paths_must_be_unique(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "knowledge" / "catalogs").mkdir(parents=True)
            for name in ("README.md", "PROJECT_SUMMARY.md", "AGENTS.md", "CLAUDE.md"):
                (root / name).write_text("maintained\n", encoding="utf-8")
            (root / "knowledge" / "same.md").write_text("# Same\n", encoding="utf-8")
            for name in ("modules.yaml", "contracts.yaml", "artifacts.yaml"):
                (root / "knowledge" / "catalogs" / name).write_text("{}\n", encoding="utf-8")
            common = {
                "baseline_evidence_revision": "baseline",
                "verification_command": "python3 scripts/verify_repository.py --check",
            }
            manifest = {
                **common,
                "schema_version": "bcx-knowledge-manifest/v1.0.0",
                "entrypoints": [],
                "documents": [
                    {"id": "first", "path": "knowledge/same.md"},
                    {"id": "second", "path": "knowledge/same.md"},
                ],
                "catalogs": {
                    "modules": "knowledge/catalogs/modules.yaml",
                    "contracts": "knowledge/catalogs/contracts.yaml",
                    "artifacts": "knowledge/catalogs/artifacts.yaml",
                },
                "active_context_packs": [],
            }
            modules = {**common, "schema_version": "bcx-module-catalog/v1.0.0", "modules": []}
            contracts = {**common, "schema_version": "bcx-contract-catalog/v1.0.0", "contracts": []}
            artifacts = {**common, "schema_version": "bcx-artifact-catalog/v1.0.0", "artifacts": []}
            audit = verify_repository.Audit()

            verify_repository.validate_knowledge(
                root, manifest, modules, contracts, artifacts, audit
            )

            self.assertTrue(any("same path" in error for error in audit.errors))


class SchemaInventoryTests(unittest.TestCase):
    def _fixture(self, root: Path) -> tuple[dict, dict]:
        schema_path = root / "schemas" / "example.schema.json"
        schema_path.parent.mkdir(parents=True)
        schema_path.write_text(
            json.dumps(
                {
                    "$schema": "http://json-schema.org/draft-07/schema#",
                    "$id": "https://example.test/example.schema.json",
                    "type": "object",
                    "properties": {"schema_version": {"const": "example/v1"}},
                }
            ),
            encoding="utf-8",
        )
        contracts = {
            "contracts": [
                {
                    "id": "example",
                    "owner": "example-module",
                    "coverage": "direct",
                    "version_source": "schema-const",
                    "schemas": ["schemas/example.schema.json"],
                    "sources": ["schemas/example.schema.json"],
                }
            ]
        }
        artifacts = {
            "artifacts": [
                {"id": "example-schemas", "paths": ["schemas/example.schema.json"]}
            ]
        }
        return contracts, artifacts

    def test_schema_version_has_one_derived_owner(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            contracts, artifacts = self._fixture(root)
            audit = verify_repository.Audit()

            rows = verify_repository.schema_inventory(root, contracts, artifacts, audit)

            self.assertEqual(audit.errors, [])
            self.assertEqual(rows[0]["version"], "example/v1")
            self.assertEqual(rows[0]["contract"], "example")

    def test_two_version_identity_fields_are_rejected(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            contracts, artifacts = self._fixture(root)
            path = root / "schemas" / "example.schema.json"
            document = json.loads(path.read_text(encoding="utf-8"))
            document["properties"]["trace_schema_version"] = {"const": "trace/v1"}
            path.write_text(json.dumps(document), encoding="utf-8")
            audit = verify_repository.Audit()

            verify_repository.schema_inventory(root, contracts, artifacts, audit)

            self.assertTrue(any("exactly one version identity" in error for error in audit.errors))

    def test_schema_contract_cannot_copy_or_empty_the_owned_version(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            contracts, artifacts = self._fixture(root)
            contracts["contracts"][0]["version"] = "WRONG"
            path = root / "schemas" / "example.schema.json"
            document = json.loads(path.read_text(encoding="utf-8"))
            document["properties"]["schema_version"]["const"] = ""
            path.write_text(json.dumps(document), encoding="utf-8")
            audit = verify_repository.Audit()

            verify_repository.schema_inventory(root, contracts, artifacts, audit)

            self.assertTrue(any("duplicates its schema-owned version" in error for error in audit.errors))
            self.assertTrue(any("empty version identity" in error for error in audit.errors))


class GeneratedReferenceTests(unittest.TestCase):
    def test_reference_order_is_deterministic(self):
        routes = [
            {
                "canonical_path": path,
                "methods": ["POST"],
                "handler": "handle_" + path[-1],
                "legacy_alias": None,
                "match_kind": "exact",
            }
            for path in ("/z", "/a")
        ]
        schemas = [
            {
                "path": path,
                "schema_id": "https://example.test/" + path,
                "identity_field": "schema_version",
                "version": "v1",
                "contract": "contract",
                "owner": "owner",
                "coverage": "direct",
                "artifact": "artifact",
            }
            for path in ("schemas/z.schema.json", "schemas/a.schema.json")
        ]
        rendered = verify_repository.render_reference({"routes": routes}, schemas, [])

        self.assertLess(rendered.index("`/a`"), rendered.index("`/z`"))
        self.assertLess(
            rendered.index("`schemas/a.schema.json`"),
            rendered.index("`schemas/z.schema.json`"),
        )
        self.assertNotIn("generated_at", rendered)

    def test_atomic_writer_and_read_only_comparison(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            audit = verify_repository.Audit()
            expected = "generated\n"

            verify_repository.compare_or_write_generated(root, expected, True, audit)
            path = root / verify_repository.GENERATED_REFERENCE
            before = (path.read_bytes(), path.stat().st_ino, path.stat().st_mtime_ns)
            verify_repository.compare_or_write_generated(root, expected, False, audit)
            after = (path.read_bytes(), path.stat().st_ino, path.stat().st_mtime_ns)

            self.assertEqual(audit.errors, [])
            self.assertEqual(before, after)

    def test_api_fact_mutation_is_rejected(self):
        facts = {
            "schema_version": "1",
            "api_version": "v1",
            "legacy_sunset": "2027-05-25",
            "route_count": 2,
            "routes": [
                {
                    "canonical_path": "/api/v1/a",
                    "internal_path": "/api/a",
                    "methods": ["POST"],
                    "handler": "handle_a",
                    "legacy_alias": "/api/a",
                    "match_kind": "exact",
                },
                {
                    "canonical_path": "/api/v1/b",
                    "internal_path": "/api/b",
                    "methods": ["post"],
                    "handler": "handle_b",
                    "legacy_alias": "/api/a",
                    "match_kind": "exact",
                },
            ],
        }
        audit = verify_repository.Audit()
        with mock.patch.object(
            verify_repository, "run_command", return_value=json.dumps(facts)
        ):
            verify_repository.load_api_facts(Path("."), audit)

        self.assertTrue(any("invalid methods" in error for error in audit.errors))
        self.assertTrue(any("duplicate legacy API aliases" in error for error in audit.errors))

    def test_api_version_and_sunset_cross_fields_are_rejected(self):
        facts = {
            "schema_version": "1",
            "api_version": "v2",
            "legacy_sunset": "2027-99-99",
            "route_count": 1,
            "routes": [
                {
                    "canonical_path": "/api/v1/a",
                    "internal_path": "/api/a",
                    "methods": ["POST"],
                    "handler": "handle_a",
                    "legacy_alias": "/api/a",
                    "match_kind": "exact",
                }
            ],
        }
        audit = verify_repository.Audit()
        with mock.patch.object(
            verify_repository, "run_command", return_value=json.dumps(facts)
        ):
            verify_repository.load_api_facts(Path("."), audit)

        self.assertTrue(any("real ISO date" in error for error in audit.errors))
        self.assertTrue(any("disagrees with api_version" in error for error in audit.errors))

    def test_api_catalog_cannot_duplicate_executable_version_or_sunset(self):
        facts = {"api_version": "v1", "legacy_sunset": "2027-05-25"}
        contracts = {
            "contracts": [
                {
                    "id": "http-api-v1",
                    "version": "v999",
                    "compatibility": "Canonical /api/v999 until 2099-01-01",
                }
            ]
        }
        audit = verify_repository.Audit()

        verify_repository.validate_api_catalog(facts, contracts, audit)

        self.assertTrue(any("owned by executable metadata" in error for error in audit.errors))
        self.assertTrue(any("duplicates its executable version" in error for error in audit.errors))
        self.assertTrue(any("duplicates the executable legacy sunset" in error for error in audit.errors))
        self.assertTrue(any("duplicates the executable versioned prefix" in error for error in audit.errors))

    def test_maintained_api_projections_are_cross_checked(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            relative = Path("knowledge/current.md")
            (root / relative).parent.mkdir(parents=True)
            (root / relative).write_text(
                "Use /api/v2; the legacy sunset is 2099-01-01.\n",
                encoding="utf-8",
            )
            audit = verify_repository.Audit()

            verify_repository.validate_api_projections(
                root,
                [relative],
                {"api_version": "v1", "legacy_sunset": "2027-05-25"},
                audit,
            )

            self.assertTrue(any("stale API version projection" in error for error in audit.errors))
            self.assertTrue(any("stale legacy sunset projection" in error for error in audit.errors))

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
            "scripts/build_macos_dmg.sh",
            "packaging/macos_release_metadata.sh",
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
                verify_repository.version_inventory(
                    root,
                    {"api_version": "v1", "legacy_sunset": "2027-05-25"},
                    audit,
                )

        self.assertTrue(any("application version drift" in error for error in audit.errors))
        self.assertTrue(any("webapp/Manifest.toml" in error for error in audit.errors))

    def test_write_inventories_schemas_after_generation(self):
        events: list[str] = []

        def run_command(_root, _command, _audit, label):
            events.append(label)
            return ""

        def schema_inventory(_root, _contracts, _artifacts, _audit):
            events.append("schema inventory")
            return []

        def compare(_root, _expected, write, _audit):
            events.append("write reference" if write else "check reference")

        with tempfile.TemporaryDirectory() as temporary, \
             mock.patch.object(verify_repository, "load_yaml", return_value={}), \
             mock.patch.object(verify_repository, "validate_knowledge", return_value=[]), \
             mock.patch.object(verify_repository, "schema_inventory", side_effect=schema_inventory), \
             mock.patch.object(verify_repository, "load_api_facts", return_value={"routes": []}), \
             mock.patch.object(verify_repository, "version_inventory", return_value=[]), \
             mock.patch.object(verify_repository, "run_command", side_effect=run_command), \
             mock.patch.object(verify_repository, "compare_or_write_generated", side_effect=compare), \
             contextlib.redirect_stdout(io.StringIO()):
            status = verify_repository.verify(Path(temporary), write=True, external=True)

        self.assertEqual(status, 0)
        self.assertLess(events.index("generated schema check"), events.index("schema inventory"))
        self.assertLess(events.index("schema inventory"), events.index("write reference"))
        self.assertLess(events.index("write reference"), events.index("post-write generated schema check"))

    def test_read_only_gate_detects_external_command_side_effect(self):
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
            with mock.patch.object(verify_repository, "load_yaml", return_value={}), \
                 mock.patch.object(verify_repository, "validate_knowledge", return_value=[]), \
                 mock.patch.object(verify_repository, "schema_inventory", return_value=[]), \
                 mock.patch.object(verify_repository, "load_api_facts", return_value={"routes": []}), \
                 mock.patch.object(verify_repository, "version_inventory", return_value=[]), \
                 mock.patch.object(verify_repository, "run_command", side_effect=run_command), \
                 mock.patch.object(verify_repository, "compare_or_write_generated"), \
                 contextlib.redirect_stdout(output):
                status = verify_repository.verify(root, write=False, external=True)

            self.assertEqual(status, 1)
            self.assertIn("read-only verification modified", output.getvalue())
            self.assertIn("added:side-effect.tmp", output.getvalue())


if __name__ == "__main__":
    unittest.main()
