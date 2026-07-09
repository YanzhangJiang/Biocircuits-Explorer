#!/usr/bin/env python3
"""Verify repository contracts and render the deterministic reference page.

Usage:
  python3 scripts/verify_repository.py --check
  python3 scripts/verify_repository.py --write

`--check` is read-only. `--write` atomically regenerates derived schemas and the
single generated knowledge reference, then runs the same checks.
"""

from __future__ import annotations

import argparse
import datetime as dt
import glob
import hashlib
import json
import os
import re
import subprocess
import sys
import tempfile
import urllib.parse
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath
from typing import Any, Iterable

try:
    import yaml
except ModuleNotFoundError:  # pragma: no cover - gives a useful local error
    yaml = None  # type: ignore[assignment]


ROOT = Path(__file__).resolve().parents[1]
MANIFEST_PATH = Path("knowledge/manifest.yaml")
MODULES_PATH = Path("knowledge/catalogs/modules.yaml")
CONTRACTS_PATH = Path("knowledge/catalogs/contracts.yaml")
ARTIFACTS_PATH = Path("knowledge/catalogs/artifacts.yaml")
GENERATED_REFERENCE = Path("knowledge/generated/reference.md")
IDENTITY_FIELDS = (
    "ir_schema_version",
    "schema_version",
    "artifact_schema_version",
    "manifest_schema_version",
    "trace_schema_version",
)
SEMVER_IDENTIFIER = r"(?:0|[1-9][0-9]*|[0-9]*[A-Za-z-][0-9A-Za-z-]*)"
SEMVER = re.compile(
    rf"^(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)"
    rf"(?:-{SEMVER_IDENTIFIER}(?:\.{SEMVER_IDENTIFIER})*)?"
    r"(?:\+[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?$"
)
APPLICATION_MANIFESTS = {
    Path("webapp/Manifest.toml"): (
        "BiocircuitsExplorerBackend",
        "67d10611-6cfe-4cce-80b3-3428f29739d0",
    ),
    Path("packaging/Manifest.toml"): (
        "BiocircuitsExplorerPackaging",
        "2611948b-0538-4b60-b4c0-66cc43878c3b",
    ),
    Path("webapp_hpc/Manifest.toml"): (
        "BiocircuitsExplorerBackendHPC",
        "67d10611-6cfe-4cce-80b3-3428f29739d0",
    ),
}


@dataclass
class Audit:
    errors: list[str] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)

    def require(self, condition: bool, message: str) -> None:
        if not condition:
            self.errors.append(message)

    def extend(self, messages: Iterable[str]) -> None:
        self.errors.extend(messages)

    def report(self) -> int:
        for note in self.notes:
            print(f"NOTE: {note}")
        for error in self.errors:
            print(f"ERROR: {error}")
        if self.errors:
            print(f"FAIL: {len(self.errors)} repository contract error(s)")
            return 1
        print("PASS: repository contracts and generated reference are current")
        return 0


@dataclass(frozen=True)
class WorktreeFileState:
    mode: int
    size: int
    mtime_ns: int
    inode: int
    digest: str


def snapshot_git_visible_worktree(root: Path, audit: Audit) -> dict[str, WorktreeFileState]:
    """Fingerprint tracked and unignored-untracked files without changing them."""
    try:
        result = subprocess.run(
            ["git", "ls-files", "--cached", "--others", "--exclude-standard", "-z"],
            cwd=root,
            capture_output=True,
            check=False,
        )
    except OSError as exc:
        audit.errors.append(f"cannot inventory Git-visible worktree: {exc}")
        return {}
    if result.returncode != 0:
        detail = result.stderr.decode("utf-8", errors="replace").strip()
        audit.errors.append(f"cannot inventory Git-visible worktree: {detail}")
        return {}

    states: dict[str, WorktreeFileState] = {}
    for raw in result.stdout.split(b"\0"):
        if not raw:
            continue
        path_text = raw.decode("utf-8", errors="surrogateescape")
        if not is_safe_repo_path(path_text):
            audit.errors.append(f"unsafe Git-visible path: {path_text!r}")
            continue
        path = root / path_text
        try:
            info = path.lstat()
            if path.is_symlink():
                payload = os.readlink(path).encode("utf-8", errors="surrogateescape")
            elif path.is_file():
                payload = path.read_bytes()
            else:
                payload = b""
        except OSError as exc:
            audit.errors.append(f"cannot fingerprint Git-visible path {path_text}: {exc}")
            continue
        states[path_text] = WorktreeFileState(
            mode=info.st_mode,
            size=info.st_size,
            mtime_ns=info.st_mtime_ns,
            inode=info.st_ino,
            digest=hashlib.sha256(payload).hexdigest(),
        )
    return states


def describe_snapshot_change(
    before: dict[str, WorktreeFileState], after: dict[str, WorktreeFileState]
) -> str:
    added = sorted(set(after) - set(before))
    removed = sorted(set(before) - set(after))
    changed = sorted(path for path in set(before) & set(after) if before[path] != after[path])
    details: list[str] = []
    details.extend(f"added:{path}" for path in added)
    details.extend(f"removed:{path}" for path in removed)
    details.extend(f"changed:{path}" for path in changed)
    shown = details[:20]
    if len(details) > len(shown):
        shown.append(f"... and {len(details) - len(shown)} more")
    return ", ".join(shown)


def load_yaml(root: Path, relative: Path, audit: Audit) -> dict[str, Any]:
    if yaml is None:
        audit.errors.append(
            "PyYAML is required; install with: python3 -m pip install -r scripts/requirements-verify.txt"
        )
        return {}
    path = root / relative
    try:
        value = yaml.safe_load(path.read_text(encoding="utf-8"))
    except Exception as exc:
        audit.errors.append(f"cannot parse {relative.as_posix()}: {exc}")
        return {}
    audit.require(isinstance(value, dict), f"{relative.as_posix()} root must be a mapping")
    return value if isinstance(value, dict) else {}


def load_json(root: Path, relative: Path, audit: Audit) -> dict[str, Any]:
    try:
        value = json.loads((root / relative).read_text(encoding="utf-8"))
    except Exception as exc:
        audit.errors.append(f"cannot parse {relative.as_posix()}: {exc}")
        return {}
    audit.require(isinstance(value, dict), f"{relative.as_posix()} root must be an object")
    return value if isinstance(value, dict) else {}


def is_safe_repo_path(value: Any, *, allow_glob: bool = False) -> bool:
    if not isinstance(value, str) or not value or "\\" in value or "://" in value:
        return False
    path = PurePosixPath(value)
    if path.is_absolute() or ".." in path.parts or value != path.as_posix():
        return False
    if not allow_glob and any(char in value for char in "*?["):
        return False
    return True


def resolves_within(root: Path, path: Path) -> bool:
    """Return whether `path` resolves inside `root`, including through symlinks."""
    try:
        path.resolve().relative_to(root.resolve())
    except (OSError, ValueError):
        return False
    return True


def mapping_list(value: Any, owner: str, audit: Audit) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        audit.errors.append(f"{owner} must be a list")
        return []
    entries: list[dict[str, Any]] = []
    for index, item in enumerate(value):
        if not isinstance(item, dict):
            audit.errors.append(f"{owner}[{index}] must be a mapping")
            continue
        entries.append(item)
    return entries


def string_list(value: Any, owner: str, audit: Audit) -> list[str]:
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        audit.errors.append(f"{owner} must be a list of strings")
        return []
    return value


def find_private_markers(text: str) -> list[str]:
    needles = [
        "/" + "Users/",
        "/" + "home/",
        "/" + "raid/",
        "/" + "tmp/",
        "file" + "://",
        "git" + "@",
        "ssh" + "://",
        "BEGIN " + "PRIVATE KEY",
        "BEGIN " + "OPENSSH PRIVATE KEY",
    ]
    found = [needle for needle in needles if needle in text]
    if re.search(r"AKIA[0-9A-Z]{16}", text):
        found.append("AWS access-key pattern")
    if re.search(r"-----BEGIN [A-Z0-9 ]*PRIVATE KEY-----", text):
        found.append("PEM private-key header")
    if re.search(r"\bghp_[A-Za-z0-9]{30,}\b", text) or re.search(
        r"\bgithub_pat_[A-Za-z0-9_]{20,}\b", text
    ):
        found.append("GitHub token pattern")
    if re.search(r"\bsk-(?:proj-)?[A-Za-z0-9_-]{20,}\b", text):
        found.append("OpenAI token pattern")
    if re.search(r"https?://[^/\s:@]+:[^/\s@]+@", text):
        found.append("credential-bearing URL")
    return found


def markdown_inline_destinations(text: str) -> list[str]:
    """Return inline link/image destinations, supporting balanced parentheses."""
    destinations: list[str] = []
    cursor = 0
    while True:
        start = text.find("](", cursor)
        if start < 0:
            break
        index = start + 2
        depth = 1
        escaped = False
        while index < len(text) and depth:
            char = text[index]
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == "(":
                depth += 1
            elif char == ")":
                depth -= 1
            index += 1
        if depth:
            raise ValueError(f"unterminated Markdown link near offset {start}")
        raw = text[start + 2 : index - 1].strip()
        if raw.startswith("<") and ">" in raw:
            destination = raw[1 : raw.index(">")]
        else:
            destination = raw.split(maxsplit=1)[0] if raw else ""
        destinations.append(destination)
        cursor = index
    return destinations


def markdown_heading_anchors(text: str) -> set[str]:
    """Return GitHub-style anchors for the ATX headings used by maintained docs."""
    anchors: set[str] = set()
    occurrences: dict[str, int] = {}
    for line in text.splitlines():
        match = re.match(r"^#{1,6}[ \t]+(.+?)[ \t]*#*[ \t]*$", line)
        if match is None:
            continue
        heading = re.sub(r"[`*_~]", "", match.group(1)).strip().lower()
        base = re.sub(r"[^\w\- ]", "", heading, flags=re.UNICODE)
        base = re.sub(r"[ \t]+", "-", base)
        number = occurrences.get(base, 0)
        occurrences[base] = number + 1
        anchors.add(base if number == 0 else f"{base}-{number}")
    return anchors


def check_markdown_file(root: Path, relative: Path, audit: Audit) -> None:
    path = root / relative
    text = path.read_text(encoding="utf-8")
    audit.require(not re.search(r"(?m)^\[[^]]+\]:\s*", text), f"reference-style links are unsupported in {relative}")
    try:
        destinations = markdown_inline_destinations(text)
    except ValueError as exc:
        audit.errors.append(f"{relative.as_posix()}: {exc}")
        return
    for destination in destinations:
        base, separator, fragment = destination.partition("#")
        if re.match(r"^[a-zA-Z][a-zA-Z0-9+.-]*:", base):
            continue
        if base:
            audit.require(not base.startswith("/"), f"absolute Markdown link in {relative}: {destination}")
            candidate = (path.parent / base).resolve()
            audit.require(resolves_within(root, candidate), f"Markdown link escapes repository in {relative}: {destination}")
            audit.require(candidate.exists(), f"broken Markdown link in {relative}: {destination}")
        else:
            candidate = path
        if (
            separator
            and fragment
            and candidate.is_file()
            and resolves_within(root, candidate)
            and candidate.suffix.lower() == ".md"
        ):
            anchors = markdown_heading_anchors(candidate.read_text(encoding="utf-8"))
            decoded = urllib.parse.unquote(fragment).lower()
            audit.require(decoded in anchors, f"broken Markdown heading fragment in {relative}: {destination}")
    for line_number, line in enumerate(text.splitlines(), 1):
        audit.require(not line.endswith((" ", "\t")), f"trailing whitespace in {relative}:{line_number}")


def unique_ids(items: Any, kind: str, audit: Audit) -> set[str]:
    if not isinstance(items, list):
        audit.errors.append(f"{kind} collection must be a list")
        return set()
    identifiers: list[str] = []
    for item in items:
        if not isinstance(item, dict) or not isinstance(item.get("id"), str):
            audit.errors.append(f"{kind} entry lacks a string id")
            continue
        identifiers.append(item["id"])
    audit.require(len(identifiers) == len(set(identifiers)), f"duplicate {kind} IDs")
    return set(identifiers)


def require_local_path(root: Path, value: Any, owner: str, audit: Audit) -> None:
    audit.require(is_safe_repo_path(value), f"unsafe path in {owner}: {value!r}")
    if is_safe_repo_path(value):
        candidate = root / value
        audit.require(resolves_within(root, candidate), f"path escapes repository in {owner}: {value}")
        audit.require(candidate.exists(), f"missing path in {owner}: {value}")


def validate_knowledge(
    root: Path,
    manifest: dict[str, Any],
    modules_doc: dict[str, Any],
    contracts_doc: dict[str, Any],
    artifacts_doc: dict[str, Any],
    audit: Audit,
    *,
    allow_missing_generated: bool = False,
) -> list[Path]:
    audit.require(manifest.get("schema_version") == "bcx-knowledge-manifest/v1.0.0", "knowledge manifest schema drift")
    audit.require(modules_doc.get("schema_version") == "bcx-module-catalog/v1.0.0", "module catalog schema drift")
    audit.require(contracts_doc.get("schema_version") == "bcx-contract-catalog/v1.0.0", "contract catalog schema drift")
    audit.require(artifacts_doc.get("schema_version") == "bcx-artifact-catalog/v1.0.0", "artifact catalog schema drift")
    verification_command = "python3 scripts/verify_repository.py --check"
    for name, document in (
        ("knowledge manifest", manifest),
        ("module catalog", modules_doc),
        ("contract catalog", contracts_doc),
        ("artifact catalog", artifacts_doc),
    ):
        audit.require("verified_against" not in document, f"{name} uses stale snapshot-style verified_against")
        audit.require(
            isinstance(document.get("baseline_evidence_revision"), str)
            and bool(document["baseline_evidence_revision"]),
            f"{name} lacks a baseline evidence revision",
        )
        audit.require(
            document.get("verification_command") == verification_command,
            f"{name} does not declare the unified current-tree gate",
        )

    maintained: list[Path] = [Path("README.md"), Path("PROJECT_SUMMARY.md"), Path("AGENTS.md"), Path("CLAUDE.md")]
    for relative in maintained:
        require_local_path(root, relative.as_posix(), "maintained entrypoint", audit)
        audit.require((root / relative).is_file(), f"maintained entrypoint is not a file: {relative}")
    manifest_items = mapping_list(manifest.get("entrypoints", []), "manifest entrypoints", audit)
    manifest_items += mapping_list(manifest.get("documents", []), "manifest documents", audit)
    manifest_ids = unique_ids(manifest_items, "manifest document", audit)
    manifest_paths: set[str] = set()
    manifest_path_list: list[str] = []
    for item in manifest_items:
        if not isinstance(item, dict):
            continue
        path_text = item.get("path")
        audit.require(is_safe_repo_path(path_text), f"unsafe manifest path: {path_text!r}")
        if isinstance(path_text, str) and is_safe_repo_path(path_text):
            manifest_path_list.append(path_text)
            manifest_paths.add(path_text)
            maintained.append(Path(path_text))
            if not (allow_missing_generated and path_text == GENERATED_REFERENCE.as_posix()):
                require_local_path(root, path_text, "manifest document", audit)
                audit.require((root / path_text).is_file(), f"manifest document is not a file: {path_text}")
    audit.require(
        len(manifest_path_list) == len(manifest_paths),
        "multiple manifest document IDs register the same path",
    )

    catalogs = manifest.get("catalogs", {})
    if not isinstance(catalogs, dict):
        audit.errors.append("manifest catalogs must be a mapping")
        catalogs = {}
    for name, path_text in catalogs.items():
        require_local_path(root, path_text, f"manifest catalog {name}", audit)
        if isinstance(path_text, str):
            maintained.append(Path(path_text))

    module_items = mapping_list(modules_doc.get("modules", []), "modules", audit)
    module_ids = unique_ids(module_items, "module", audit)
    module_docs: set[str] = set()
    for item in module_items:
        module_id = item.get("id", "unknown")
        doc = item.get("doc")
        require_local_path(root, doc, f"module {module_id} doc", audit)
        if isinstance(doc, str):
            module_docs.add(doc)
        for key in ("owner_paths", "entrypoints", "tests"):
            for path_text in string_list(item.get(key, []), f"module {module_id} {key}", audit):
                require_local_path(root, path_text, f"module {module_id} {key}", audit)
        for dependency in string_list(item.get("depends_on", []), f"module {module_id} depends_on", audit):
            audit.require(dependency in module_ids, f"module {module_id} has unknown dependency {dependency}")

    contract_items = mapping_list(contracts_doc.get("contracts", []), "contracts", audit)
    contract_ids = unique_ids(contract_items, "contract", audit)
    contract_docs: set[str] = set()
    for item in contract_items:
        contract_id = item.get("id", "unknown")
        owner = item.get("owner")
        audit.require(owner in module_ids or str(owner).startswith("cross-"), f"contract {contract_id} has unknown owner {owner}")
        audit.require(isinstance(item.get("coverage"), str) and bool(item["coverage"]), f"contract {contract_id} lacks coverage")
        for key in ("docs", "sources", "tests", "schemas"):
            for path_text in string_list(item.get(key, []), f"contract {contract_id} {key}", audit):
                require_local_path(root, path_text, f"contract {contract_id} {key}", audit)
                if key == "docs" and isinstance(path_text, str):
                    contract_docs.add(path_text)

    artifact_items = mapping_list(artifacts_doc.get("artifacts", []), "artifacts", audit)
    unique_ids(artifact_items, "artifact", audit)
    for item in artifact_items:
        artifact_id = item.get("id", "unknown")
        for path_text in string_list(item.get("paths", []), f"artifact {artifact_id} paths", audit):
            require_local_path(root, path_text, f"artifact {artifact_id}", audit)
        if "schema" in item:
            require_local_path(root, item["schema"], f"artifact {artifact_id} schema", audit)
        if "path_pattern" in item:
            pattern = item["path_pattern"]
            audit.require(is_safe_repo_path(pattern, allow_glob=True), f"unsafe artifact glob {artifact_id}: {pattern!r}")
            if is_safe_repo_path(pattern, allow_glob=True):
                matches = glob.glob(str(root / pattern))
                audit.require(bool(matches) or item.get("allow_empty") is True, f"artifact glob matches nothing: {pattern}")

    audit.require(module_docs <= manifest_paths, f"module docs absent from manifest: {sorted(module_docs - manifest_paths)}")
    audit.require(contract_docs <= manifest_paths, f"contract docs absent from manifest: {sorted(contract_docs - manifest_paths)}")
    active = set(string_list(manifest.get("active_context_packs", []), "active_context_packs", audit))
    audit.require(active <= manifest_ids, f"active context packs are not manifest document IDs: {sorted(active - manifest_ids)}")

    registered_md = {path for path in manifest_paths if path.endswith(".md")}
    actual_md = {path.relative_to(root).as_posix() for path in (root / "knowledge").rglob("*.md")}
    if allow_missing_generated:
        actual_md.add(GENERATED_REFERENCE.as_posix())
    audit.require(actual_md <= registered_md, f"knowledge Markdown absent from manifest: {sorted(actual_md - registered_md)}")

    return sorted(set(maintained))


def schema_inventory(
    root: Path,
    contracts_doc: dict[str, Any],
    artifacts_doc: dict[str, Any],
    audit: Audit,
) -> list[dict[str, str]]:
    contract_owners: dict[str, list[dict[str, Any]]] = {}
    contracts = mapping_list(contracts_doc.get("contracts", []), "contracts", audit)
    for contract in contracts:
        contract_id = str(contract.get("id", "unknown"))
        for path_text in string_list(contract.get("schemas", []), f"contract {contract_id} schemas", audit):
            contract_owners.setdefault(path_text, []).append(contract)

    artifact_owners: dict[str, list[str]] = {}
    artifacts = mapping_list(artifacts_doc.get("artifacts", []), "artifacts", audit)
    for artifact in artifacts:
        artifact_id = str(artifact.get("id", "unknown"))
        for path_text in string_list(artifact.get("paths", []), f"artifact {artifact_id} paths", audit):
            if isinstance(path_text, str) and path_text.startswith("schemas/") and path_text.endswith(".schema.json"):
                artifact_owners.setdefault(path_text, []).append(artifact_id)

    rows: list[dict[str, str]] = []
    ids: set[str] = set()
    schema_paths = sorted(path.relative_to(root).as_posix() for path in (root / "schemas").glob("*.schema.json"))
    for path_text in schema_paths:
        document = load_json(root, Path(path_text), audit)
        schema_uri = document.get("$schema")
        schema_id = document.get("$id")
        audit.require(isinstance(schema_uri, str) and bool(schema_uri), f"{path_text} lacks $schema")
        audit.require(isinstance(schema_id, str) and bool(schema_id), f"{path_text} lacks $id")
        if isinstance(schema_id, str):
            audit.require(schema_id not in ids, f"duplicate schema $id: {schema_id}")
            ids.add(schema_id)
        identities: list[tuple[str, str]] = []
        properties = document.get("properties", {})
        for field_name in IDENTITY_FIELDS:
            candidate = properties.get(field_name, {}) if isinstance(properties, dict) else {}
            if isinstance(candidate, dict) and isinstance(candidate.get("const"), str):
                value = candidate["const"]
                audit.require(bool(value.strip()), f"{path_text} has an empty version identity const")
                identities.append((field_name, value))
        audit.require(len(identities) == 1, f"{path_text} must contain exactly one version identity const")
        contracts = contract_owners.get(path_text, [])
        audit.require(len(contracts) == 1, f"{path_text} must have exactly one contract owner")
        artifacts = artifact_owners.get(path_text, [])
        audit.require(len(artifacts) == 1, f"{path_text} must have exactly one artifact-catalog owner")
        if len(contracts) == 1:
            contract = contracts[0]
            contract_id = str(contract.get("id", "unknown"))
            audit.require(contract.get("version_source") == "schema-const", f"{contract_id} duplicates or lacks schema version ownership")
            audit.require("version" not in contract, f"{contract_id} duplicates its schema-owned version")
            sources = contract.get("sources", [])
            audit.require(isinstance(sources, list) and path_text in sources, f"{contract_id} schemas must also be cited as sources")
        field_name, version = identities[0] if len(identities) == 1 else ("unknown", "unknown")
        rows.append(
            {
                "path": path_text,
                "schema_id": str(schema_id or "unknown"),
                "identity_field": field_name,
                "version": version,
                "contract": str(contracts[0].get("id", "unknown") if len(contracts) == 1 else "unknown"),
                "owner": str(contracts[0].get("owner", "unknown") if len(contracts) == 1 else "unknown"),
                "coverage": str(contracts[0].get("coverage", "unknown") if len(contracts) == 1 else "unknown"),
                "artifact": str(artifacts[0] if len(artifacts) == 1 else "unknown"),
            }
        )

    audit.require(set(contract_owners) == set(schema_paths), f"contract schema mapping drift: {sorted(set(contract_owners) ^ set(schema_paths))}")
    audit.require(set(artifact_owners) == set(schema_paths), f"artifact schema mapping drift: {sorted(set(artifact_owners) ^ set(schema_paths))}")
    return rows


def extract_ci_toolchains(document: dict[str, Any]) -> dict[str, list[str]]:
    jobs = document.get("jobs")
    if not isinstance(jobs, dict):
        raise ValueError("CI workflow has no jobs mapping")
    node: set[str] = set()
    python: set[str] = set()
    for job in jobs.values():
        if not isinstance(job, dict):
            continue
        for step in job.get("steps", []):
            if not isinstance(step, dict):
                continue
            uses = str(step.get("uses", ""))
            config = step.get("with", {})
            if not isinstance(config, dict):
                continue
            if uses.startswith("actions/setup-node@") and "node-version" in config:
                node.add(str(config["node-version"]))
            if uses.startswith("actions/setup-python@") and "python-version" in config:
                python.add(str(config["python-version"]))
    try:
        julia_values = jobs["test-julia"]["strategy"]["matrix"]["julia"]
        hpc_julia_values = jobs["test-hpc-environment"]["strategy"]["matrix"]["julia"]
    except (KeyError, TypeError) as exc:
        raise ValueError("CI webapp/HPC Julia matrix shape is unsupported") from exc
    if not isinstance(julia_values, list) or not julia_values:
        raise ValueError("CI webapp Julia matrix must be a nonempty list")
    if not isinstance(hpc_julia_values, list) or not hpc_julia_values:
        raise ValueError("CI HPC Julia matrix must be a nonempty list")
    if not node or not python:
        raise ValueError("CI Node/Python setup shape is unsupported")
    return {
        "node": sorted(node),
        "python": sorted(python),
        "julia": sorted(map(str, julia_values)),
        "julia_hpc": sorted(map(str, hpc_julia_values)),
    }


def major_minor_line(value: str) -> tuple[int, int] | None:
    match = re.fullmatch(r"(\d+)\.(\d+)(?:\D.*)?", value)
    return (int(match.group(1)), int(match.group(2))) if match else None


def project_toml_string(text: str, key: str, *, section: str | None = None) -> str:
    """Read one quoted string from the known project TOML surface.

    This intentionally is not a general TOML parser. It accepts exactly one
    simple quoted assignment in the top-level document or named table, which is
    the complete shape used by the version and Julia-compatibility owners here.
    """
    header = re.compile(r"(?m)^[ \t]*\[([^]\r\n]+)\][ \t]*(?:#.*)?$")
    headers = list(header.finditer(text))
    if section is None:
        start = 0
        end = headers[0].start() if headers else len(text)
        owner = "top level"
    else:
        matches = [match for match in headers if match.group(1).strip() == section]
        if len(matches) != 1:
            raise ValueError(f"expected one [{section}] table; found {len(matches)}")
        selected = matches[0]
        start = selected.end()
        end = next(
            (match.start() for match in headers if match.start() > selected.start()),
            len(text),
        )
        owner = f"[{section}]"
    body = text[start:end]
    assignment = re.compile(
        rf"(?m)^[ \t]*{re.escape(key)}[ \t]*=[ \t]*(['\"])([^'\"\r\n]+)\1[ \t]*(?:#.*)?$"
    )
    values = [match.group(2) for match in assignment.finditer(body)]
    if len(values) != 1:
        raise ValueError(f"expected one quoted {key} in {owner}; found {len(values)}")
    return values[0]


def manifest_self_version(text: str, package_name: str, expected_uuid: str) -> str:
    section_pattern = re.compile(
        r"(?m)^[ \t]*\[\[deps\.(?P<name>[^\]\r\n]+)\]\][ \t]*(?:#.*)?$"
    )
    sections = list(section_pattern.finditer(text))
    matches = [match for match in sections if match.group("name").strip() == package_name]
    if len(matches) != 1:
        raise ValueError(
            f"expected one [[deps.{package_name}]] self-package entry; found {len(matches)}"
        )
    selected = matches[0]
    block_end = next(
        (section.start() for section in sections if section.start() > selected.start()),
        len(text),
    )
    block = text[selected.end() : block_end]

    def field(name: str) -> str:
        assignment = re.compile(
            rf"(?m)^[ \t]*{re.escape(name)}[ \t]*=[ \t]*(['\"])([^'\"\r\n]+)\1[ \t]*(?:#.*)?$"
        )
        values = [match.group(2) for match in assignment.finditer(block)]
        if len(values) != 1:
            raise ValueError(f"self-package entry needs one quoted {name}; found {len(values)}")
        return values[0]

    path_value = field("path")
    uuid_value = field("uuid")
    version = field("version")
    if path_value != ".":
        raise ValueError(f"self-package path is {path_value!r}, expected '.'")
    if uuid_value != expected_uuid:
        raise ValueError(f"self-package uuid is {uuid_value!r}, expected {expected_uuid!r}")
    return version


def version_file_value(text: str) -> str:
    match = re.fullmatch(r"([^\r\n]+)(?:\r\n|\n|\r)?", text)
    if match is None:
        raise ValueError("VERSION must contain exactly one semantic-version line")
    value = match.group(1)
    if SEMVER.fullmatch(value) is None:
        raise ValueError(f"VERSION is not Semantic Versioning 2.0.0: {value!r}")
    return value


def unique_json_object(text: str, label: str) -> dict[str, Any]:
    def reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError(f"{label} has duplicate JSON key {key!r}")
            result[key] = value
        return result

    try:
        document = json.loads(text, object_pairs_hook=reject_duplicate_keys)
    except json.JSONDecodeError as exc:
        raise ValueError(f"{label} is malformed JSON: {exc}") from exc
    if not isinstance(document, dict):
        raise ValueError(f"{label} must contain a top-level JSON object")
    return document


def version_inventory(root: Path, api_facts: dict[str, Any], audit: Audit) -> list[dict[str, str]]:
    try:
        application = version_file_value((root / "VERSION").read_text(encoding="utf-8"))
    except ValueError as exc:
        audit.errors.append(str(exc))
        application = "unknown"
    project_paths = [Path("webapp/Project.toml"), Path("packaging/Project.toml"), Path("webapp_hpc/Project.toml")]
    project_text = {
        path.as_posix(): (root / path).read_text(encoding="utf-8")
        for path in project_paths
    }
    project_versions: dict[str, str] = {}
    for path, text in project_text.items():
        try:
            project_versions[path] = project_toml_string(text, "version")
        except ValueError as exc:
            audit.errors.append(f"cannot read application version from {path}: {exc}")
            project_versions[path] = "unknown"
    manifest_versions: dict[str, str] = {}
    for path, (package_name, expected_uuid) in APPLICATION_MANIFESTS.items():
        path_text = path.as_posix()
        try:
            manifest_versions[path_text] = manifest_self_version(
                (root / path).read_text(encoding="utf-8"),
                package_name,
                expected_uuid,
            )
        except ValueError as exc:
            audit.errors.append(f"cannot read application version from {path_text}: {exc}")
            manifest_versions[path_text] = "unknown"
    try:
        package = unique_json_object(
            (root / "webapp/package.json").read_text(encoding="utf-8"),
            "webapp/package.json",
        )
    except ValueError as exc:
        audit.errors.append(str(exc))
        package = {}
    try:
        package_lock = unique_json_object(
            (root / "webapp/package-lock.json").read_text(encoding="utf-8"),
            "webapp/package-lock.json",
        )
    except ValueError as exc:
        audit.errors.append(str(exc))
        package_lock = {}
    lock_packages = package_lock.get("packages")
    lock_root = lock_packages.get("") if isinstance(lock_packages, dict) else None
    versions = {
        "VERSION": application,
        **project_versions,
        **manifest_versions,
        "webapp/package.json": str(package.get("version")),
        "webapp/package-lock.json (top-level)": str(package_lock.get("version")),
        "webapp/package-lock.json (root package)": str(
            lock_root.get("version") if isinstance(lock_root, dict) else None
        ),
    }
    invalid_semver = {
        owner: value for owner, value in versions.items() if SEMVER.fullmatch(value) is None
    }
    audit.require(not invalid_semver, f"application version is not valid SemVer: {invalid_semver}")
    audit.require(len(set(versions.values())) == 1, f"application version drift: {versions}")

    ci_document = load_yaml(root, Path(".github/workflows/ci.yml"), audit)
    try:
        ci = extract_ci_toolchains(ci_document)
    except ValueError as exc:
        audit.errors.append(str(exc))
        ci = {"node": [], "python": [], "julia": [], "julia_hpc": []}

    docker = (root / "deploy/Dockerfile").read_text(encoding="utf-8")
    docker_match = re.search(r"(?m)^FROM\s+julia:([^\s]+)\s*$", docker)
    audit.require(docker_match is not None, "Dockerfile Julia base shape is unsupported")
    docker_julia = docker_match.group(1) if docker_match else "unknown"
    docker_line = major_minor_line(docker_julia)
    ci_lines = [major_minor_line(value) for value in ci["julia"]]
    audit.require(docker_line is not None, f"Docker Julia tag has no major.minor line: {docker_julia}")
    audit.require(all(match is not None for match in ci_lines), f"CI Julia versions need major.minor lines: {ci['julia']}")
    audit.require(
        docker_line is not None and docker_line in ci_lines,
        "Docker Julia base is outside the CI Julia line",
    )

    swift = (root / "frontend-swift/BiocircuitsExplorerMac.xcodeproj/project.pbxproj").read_text(encoding="utf-8")
    macos_build = (root / "scripts/build_macos_dmg.sh").read_text(encoding="utf-8")
    macos_metadata = (root / "packaging/macos_release_metadata.sh").read_text(encoding="utf-8")
    swift_marketing = sorted(set(re.findall(r"MARKETING_VERSION = ([^;]+);", swift)))
    swift_build = sorted(set(re.findall(r"CURRENT_PROJECT_VERSION = ([^;]+);", swift)))
    swift_macos_target = sorted(set(re.findall(r"MACOSX_DEPLOYMENT_TARGET = ([^;]+);", swift)))
    audit.require(len(swift_marketing) == 1, f"Swift marketing versions disagree: {swift_marketing}")
    audit.require(len(swift_build) == 1, f"Swift build versions disagree: {swift_build}")
    audit.require(len(swift_macos_target) == 1, f"Swift macOS deployment targets disagree: {swift_macos_target}")
    audit.require(
        'MARKETING_VERSION="${APPLE_MARKETING_VERSION}"' in macos_build
        and 'apple_marketing_version "${VERSION}"' in macos_build
        and 'numeric_core="${full_version%%[-+]*}"' in macos_metadata,
        "macOS release marketing version is not derived from the application SemVer numeric core",
    )

    try:
        julia_compat = project_toml_string(
            project_text["webapp/Project.toml"], "julia", section="compat"
        )
    except ValueError as exc:
        audit.errors.append(f"cannot read Julia compatibility from webapp/Project.toml: {exc}")
        julia_compat = "unknown"
    return [
        {"fact": "Application version", "value": application, "evidence": ", ".join(versions)},
        {"fact": "API version", "value": str(api_facts.get("api_version", "unknown")), "evidence": "webapp/src/api_contract.jl"},
        {"fact": "Legacy API sunset", "value": str(api_facts.get("legacy_sunset", "unknown")), "evidence": "webapp/src/api_contract.jl"},
        {"fact": "Julia compatibility (declared)", "value": julia_compat, "evidence": "webapp/Project.toml"},
        {"fact": "Julia webapp configured in CI", "value": ", ".join(ci["julia"]), "evidence": ".github/workflows/ci.yml"},
        {"fact": "Julia HPC configured in CI", "value": ", ".join(ci["julia_hpc"]), "evidence": ".github/workflows/ci.yml"},
        {"fact": "Julia container base", "value": docker_julia, "evidence": "deploy/Dockerfile"},
        {"fact": "Node configured in CI", "value": ", ".join(ci["node"]), "evidence": ".github/workflows/ci.yml"},
        {"fact": "Python configured in CI", "value": ", ".join(ci["python"]), "evidence": ".github/workflows/ci.yml"},
        {"fact": "Swift project marketing default", "value": ", ".join(swift_marketing), "evidence": "frontend-swift/BiocircuitsExplorerMac.xcodeproj/project.pbxproj"},
        {"fact": "Swift build version (separate identity)", "value": ", ".join(swift_build), "evidence": "frontend-swift/BiocircuitsExplorerMac.xcodeproj/project.pbxproj"},
        {"fact": "macOS deployment target", "value": ", ".join(swift_macos_target), "evidence": "frontend-swift/BiocircuitsExplorerMac.xcodeproj/project.pbxproj"},
    ]


def run_command(root: Path, command: list[str], audit: Audit, label: str) -> str:
    try:
        result = subprocess.run(command, cwd=root, text=True, capture_output=True, check=False)
    except OSError as exc:
        audit.errors.append(f"cannot run {label}: {exc}")
        return ""
    if result.returncode != 0:
        detail = (result.stdout + "\n" + result.stderr).strip()
        audit.errors.append(f"{label} failed ({result.returncode}):\n{detail}")
        return ""
    return result.stdout


def load_api_facts(root: Path, audit: Audit) -> dict[str, Any]:
    output = run_command(
        root,
        ["julia", "--project=webapp", "webapp/scripts/export_reference_facts.jl"],
        audit,
        "API reference exporter",
    )
    if not output:
        return {"routes": []}
    try:
        facts = json.loads(output)
    except json.JSONDecodeError as exc:
        audit.errors.append(f"API reference exporter did not emit one JSON document: {exc}")
        return {"routes": []}
    audit.require(isinstance(facts, dict), "API exporter root must be an object")
    if not isinstance(facts, dict):
        return {"routes": []}
    audit.require(facts.get("schema_version") == "1", "API reference fact schema drift")
    api_version = facts.get("api_version")
    audit.require(
        isinstance(api_version, str) and re.fullmatch(r"v[1-9][0-9]*", api_version) is not None,
        "API exporter lacks a valid api_version",
    )
    legacy_sunset = facts.get("legacy_sunset")
    valid_sunset = False
    if isinstance(legacy_sunset, str) and re.fullmatch(r"\d{4}-\d{2}-\d{2}", legacy_sunset):
        try:
            dt.date.fromisoformat(legacy_sunset)
            valid_sunset = True
        except ValueError:
            pass
    audit.require(valid_sunset, "API exporter legacy_sunset must be a real ISO date")
    routes = facts.get("routes", [])
    audit.require(isinstance(routes, list) and bool(routes), "API exporter emitted no routes")
    canonical: list[str] = []
    internal: list[str] = []
    aliases: list[str] = []
    for route in routes if isinstance(routes, list) else []:
        audit.require(isinstance(route, dict), "API route fact must be an object")
        if not isinstance(route, dict):
            continue
        path = route.get("canonical_path")
        internal_path = route.get("internal_path")
        methods = route.get("methods")
        audit.require(isinstance(path, str) and path.startswith("/"), f"invalid canonical route: {path!r}")
        audit.require(isinstance(internal_path, str) and internal_path.startswith("/"), f"invalid internal route: {internal_path!r}")
        audit.require(
            isinstance(methods, list)
            and bool(methods)
            and all(isinstance(method, str) and re.fullmatch(r"[A-Z]+", method) for method in methods),
            f"route {path} has invalid methods",
        )
        audit.require(isinstance(route.get("handler"), str) and bool(route["handler"]), f"route {path} has no handler")
        audit.require(route.get("match_kind") in ("exact", "template"), f"route {path} has invalid match_kind")
        alias = route.get("legacy_alias")
        audit.require(alias is None or (isinstance(alias, str) and alias.startswith("/api/")), f"route {path} has invalid legacy alias")
        canonical.append(str(path))
        internal.append(str(internal_path))
        if isinstance(alias, str):
            aliases.append(alias)
        if isinstance(path, str) and path.startswith("/api/") and isinstance(api_version, str):
            version_prefix = f"/api/{api_version}"
            audit.require(
                path == version_prefix or path.startswith(version_prefix + "/"),
                f"canonical route {path} disagrees with api_version {api_version}",
            )
    audit.require(len(canonical) == len(set(canonical)), "duplicate canonical API route facts")
    audit.require(len(internal) == len(set(internal)), "duplicate internal API route facts")
    audit.require(len(aliases) == len(set(aliases)), "duplicate legacy API aliases")
    audit.require(facts.get("route_count") == len(routes), "API exporter route_count does not match routes")
    return facts


def validate_api_catalog(
    facts: dict[str, Any], contracts_doc: dict[str, Any], audit: Audit
) -> None:
    api_version = facts.get("api_version")
    if not isinstance(api_version, str):
        return
    contracts = mapping_list(contracts_doc.get("contracts", []), "contracts", audit)
    expected_id = f"http-api-{api_version}"
    matches = [contract for contract in contracts if contract.get("id") == expected_id]
    audit.require(len(matches) == 1, f"contract catalog must contain exactly one {expected_id} contract")
    if len(matches) != 1:
        return
    contract = matches[0]
    audit.require(contract.get("version_source") == "api-contract", "HTTP API version must be owned by executable metadata")
    audit.require("version" not in contract, "HTTP API contract duplicates its executable version")
    compatibility = str(contract.get("compatibility", ""))
    audit.require(re.search(r"\d{4}-\d{2}-\d{2}", compatibility) is None, "HTTP API catalog duplicates the executable legacy sunset")
    audit.require(re.search(r"/api/v[0-9]+", compatibility) is None, "HTTP API catalog duplicates the executable versioned prefix")


def validate_api_projections(
    root: Path,
    maintained: Iterable[Path],
    facts: dict[str, Any],
    audit: Audit,
) -> None:
    """Cross-check useful current API literals in maintained Markdown."""
    api_version = facts.get("api_version")
    sunset = facts.get("legacy_sunset")
    if not isinstance(api_version, str) or not isinstance(sunset, str):
        return
    for relative in maintained:
        if relative.suffix != ".md":
            continue
        path = root / relative
        if not path.is_file() or not resolves_within(root, path):
            continue
        text = path.read_text(encoding="utf-8")
        for projected in re.findall(r"/api/(v[0-9]+)(?=/|\b)", text):
            audit.require(
                projected == api_version,
                f"stale API version projection in {relative}: {projected} != {api_version}",
            )
        for match in re.finditer(r"(?is)\bsunset\b.{0,160}?(\d{4}-\d{2}-\d{2})", text):
            audit.require(
                match.group(1) == sunset,
                f"stale legacy sunset projection in {relative}: {match.group(1)} != {sunset}",
            )


def markdown_cell(value: Any) -> str:
    return str(value).replace("|", "\\|").replace("\n", " ")


def render_reference(
    api_facts: dict[str, Any],
    schemas: list[dict[str, str]],
    versions: list[dict[str, str]],
) -> str:
    lines = [
        "<!-- Generated by scripts/verify_repository.py; do not edit by hand. -->",
        "# Generated contract reference",
        "",
        "This page is a deterministic projection of executable route metadata, JSON Schemas,",
        "version owners, and CI configuration. Regenerate with",
        "`python3 scripts/verify_repository.py --write`.",
        "",
        "## API routes",
        "",
        "| Canonical path | Methods | Handler | Legacy alias | Match |",
        "|---|---|---|---|---|",
    ]
    for route in sorted(api_facts.get("routes", []), key=lambda item: (item["canonical_path"], item.get("match_kind", ""))):
        methods = ", ".join(route.get("methods", []))
        legacy = route.get("legacy_alias")
        if isinstance(legacy, bool):
            legacy = "yes" if legacy else "no"
        lines.append(
            "| `{}` | {} | `{}` | {} | {} |".format(
                markdown_cell(route.get("canonical_path")),
                markdown_cell(methods),
                markdown_cell(route.get("handler")),
                markdown_cell(legacy if legacy not in (None, "") else "none"),
                markdown_cell(route.get("match_kind", "exact")),
            )
        )
    lines.extend(
        [
            "",
            "`OPTIONS` is a global CORS behavior rather than a route entry. Payload shapes remain",
            "owned by handlers and schemas; this table documents paths and methods only.",
            "",
            "## JSON Schemas",
            "",
            "| Schema | `$id` | Identity | Version | Contract owner | Coverage |",
            "|---|---|---|---|---|---|",
        ]
    )
    for row in sorted(schemas, key=lambda item: item["path"]):
        lines.append(
            "| `{path}` | `{schema_id}` | `{identity_field}` | `{version}` | `{contract}` / `{owner}` | {coverage} |".format(
                **{key: markdown_cell(value) for key, value in row.items()}
            )
        )
    lines.extend(
        [
            "",
            "Schema versions are derived from each schema's identity-field `const`; catalogs do not",
            "own a second copy of those values.",
            "",
            "## Versions and configured toolchains",
            "",
            "| Fact | Value | Evidence |",
            "|---|---|---|",
        ]
    )
    for row in versions:
        lines.append(
            "| {} | `{}` | `{}` |".format(
                markdown_cell(row["fact"]), markdown_cell(row["value"]), markdown_cell(row["evidence"])
            )
        )
    lines.extend(
        [
            "",
            "A version configured in CI is not, by itself, evidence that an external CI run passed.",
            "The Xcode project defaults are reported separately from the application version. The DMG",
            "release script overrides MARKETING_VERSION with the application's three-part numeric core;",
            "prerelease/build metadata remains in VERSION, backend responses, and artifact filenames.",
            "",
        ]
    )
    return "\n".join(lines)


def compare_or_write_generated(root: Path, expected: str, write: bool, audit: Audit) -> None:
    path = root / GENERATED_REFERENCE
    if write:
        path.parent.mkdir(parents=True, exist_ok=True)
        file_descriptor, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
        try:
            mode = path.stat().st_mode & 0o777 if path.exists() else 0o644
            os.fchmod(file_descriptor, mode)
            with os.fdopen(file_descriptor, "w", encoding="utf-8", newline="\n") as handle:
                handle.write(expected)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, path)
        finally:
            if os.path.exists(temporary):
                os.unlink(temporary)
    else:
        if not path.is_file():
            audit.errors.append(f"generated reference missing: {GENERATED_REFERENCE}")
            return
        actual = path.read_text(encoding="utf-8")
        audit.require(actual == expected, f"generated reference is stale; run: python3 scripts/verify_repository.py --write")


def verify(root: Path, *, write: bool, external: bool = True) -> int:
    audit = Audit()
    if sys.version_info < (3, 9):
        audit.errors.append("Python 3.9+ is required; CI uses Python 3.13")
        return audit.report()

    manifest = load_yaml(root, MANIFEST_PATH, audit)
    modules_doc = load_yaml(root, MODULES_PATH, audit)
    contracts_doc = load_yaml(root, CONTRACTS_PATH, audit)
    artifacts_doc = load_yaml(root, ARTIFACTS_PATH, audit)

    maintained = validate_knowledge(
        root,
        manifest,
        modules_doc,
        contracts_doc,
        artifacts_doc,
        audit,
        allow_missing_generated=write,
    )

    read_only_before = (
        snapshot_git_visible_worktree(root, audit) if external and not write else None
    )

    if external:
        schema_mode = "--write" if write else "--check"
        run_command(
            root,
            ["julia", "--project=webapp", "webapp/scripts/gen_schemas.jl", schema_mode],
            audit,
            "generated schema check",
        )
        api_facts = load_api_facts(root, audit)
    else:
        api_facts = {"api_version": "test", "legacy_sunset": "test", "routes": []}

    # In write mode, inventory must be read after the generated schemas are
    # updated; otherwise the reference can accidentally preserve stale bytes.
    validate_api_catalog(api_facts, contracts_doc, audit)
    api_projection_files = [
        relative
        for relative in maintained
        if not (write and relative == GENERATED_REFERENCE)
    ]
    validate_api_projections(root, api_projection_files, api_facts, audit)
    schemas = schema_inventory(root, contracts_doc, artifacts_doc, audit)
    versions = version_inventory(root, api_facts, audit)
    expected = render_reference(api_facts, schemas, versions)
    if write:
        if audit.errors:
            audit.notes.append("generated reference was not written because source validation failed")
        else:
            compare_or_write_generated(root, expected, True, audit)
            if external:
                run_command(
                    root,
                    ["julia", "--project=webapp", "webapp/scripts/gen_schemas.jl", "--check"],
                    audit,
                    "post-write generated schema check",
                )
            compare_or_write_generated(root, expected, False, audit)
    else:
        compare_or_write_generated(root, expected, False, audit)

    public_files = sorted(set(maintained + [GENERATED_REFERENCE, Path("scripts/verify_repository.py")]))
    for relative in public_files:
        path = root / relative
        if not path.exists():
            continue
        if not resolves_within(root, path):
            # The owning manifest/path check already reports the precise error;
            # do not follow an unsafe symlink merely to scan its target.
            continue
        text = path.read_text(encoding="utf-8")
        markers = find_private_markers(text)
        audit.require(not markers, f"private/public-safety markers in {relative}: {markers}")
        if relative.suffix == ".md":
            check_markdown_file(root, relative, audit)

    if external:
        run_command(root, [sys.executable, "webapp/scripts/validate_artifacts.py"], audit, "artifact validation")
        run_command(root, ["git", "diff", "--check"], audit, "git whitespace check")
        if read_only_before is not None:
            read_only_after = snapshot_git_visible_worktree(root, audit)
            audit.require(
                read_only_before == read_only_after,
                "read-only verification modified the Git-visible worktree: "
                + describe_snapshot_change(read_only_before, read_only_after),
            )

    audit.notes.append(
        f"checked {len(public_files)} maintained files, {len(schemas)} schemas, "
        f"{len(api_facts.get('routes', []))} routes"
    )
    return audit.report()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--check", action="store_true", help="read-only drift and integrity check")
    mode.add_argument("--write", action="store_true", help="atomically regenerate derived files, then check")
    args = parser.parse_args()
    return verify(ROOT, write=args.write, external=True)


if __name__ == "__main__":
    raise SystemExit(main())
