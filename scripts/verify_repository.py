#!/usr/bin/env python3
"""Verify repository contracts.

Usage:
  python3 scripts/verify_repository.py --check

`--check` is read-only. It verifies generated-schema drift, that every
application-version owner carries one SemVer, the public repository boundary
(private directories, manuscript file types, credential patterns), and the
worktree whitespace diff.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable


ROOT = Path(__file__).resolve().parents[1]
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
}
FORBIDDEN_TRACKED_PREFIXES = (
    "paper_rop_periodic_table/",
    "webapp/scripts/rop_periodic_table/",
    "webapp/scripts/_archive/",
    "workstation/",
)
FORBIDDEN_RESEARCH_ROOTS = {
    "manuscript",
    "manuscripts",
    "unpublished",
    "private",
    "confidential",
    "submissions",
}
# Manuscript and bibliography file types are rejected wherever they are tracked, so a
# force-add of a paper draft or a published PDF cannot slip past the directory rules.
# Product media (images, video, fonts) is unaffected; keep it in the product tree.
FORBIDDEN_RESEARCH_SUFFIXES = {
    ".tex",
    ".doc",
    ".docx",
    ".pdf",
    ".bib",
    ".ris",
    ".nbib",
    ".enw",
}
REQUIRED_PRIVACY_IGNORES = (
    "/paper_rop_periodic_table/",
    "/webapp/scripts/rop_periodic_table/",
    "/webapp/scripts/_archive/",
    "/workstation/",
)


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
        print("PASS: repository contracts are current")
        return 0


def resolves_within(root: Path, path: Path) -> bool:
    """Return whether `path` resolves inside `root`, including through symlinks."""
    try:
        path.resolve().relative_to(root.resolve())
    except (OSError, ValueError):
        return False
    return True


def find_private_markers(text: str) -> list[str]:
    needles = [
        "BEGIN " + "PRIVATE KEY",
        "BEGIN " + "OPENSSH PRIVATE KEY",
    ]
    found = [needle for needle in needles if needle in text]
    if re.search(r"(?:AKIA|ASIA)[0-9A-Z]{16}", text):
        found.append("AWS access-key pattern")
    if re.search(r"-----BEGIN [A-Z0-9 ]*PRIVATE KEY-----", text):
        found.append("PEM private-key header")
    if re.search(r"\bghp_[A-Za-z0-9]{30,}\b", text) or re.search(
        r"\bgithub_pat_[A-Za-z0-9_]{20,}\b", text
    ):
        found.append("GitHub token pattern")
    if re.search(r"\bsk-(?:proj-)?[A-Za-z0-9_-]{20,}\b", text):
        found.append("OpenAI token pattern")
    if re.search(r"\bsk-ant-[A-Za-z0-9_-]{20,}\b", text):
        found.append("Anthropic token pattern")
    if re.search(r"\bAIza[0-9A-Za-z_-]{35}\b", text):
        found.append("Google API key pattern")
    if re.search(r"\bhf_[A-Za-z0-9]{20,}\b", text):
        found.append("Hugging Face token pattern")
    if re.search(r"\bxai-[A-Za-z0-9_-]{20,}\b", text):
        found.append("xAI token pattern")
    if re.search(r"\bxox[baprs]-[A-Za-z0-9-]{10,}\b", text):
        found.append("Slack token pattern")
    if re.search(r"https?://[^/\s:@]+:[^/\s@]+@", text):
        found.append("credential-bearing URL")
    return found


def public_repository_path_violation(relative: Path) -> str | None:
    """Return the public-boundary violation for a tracked path, if any."""
    value = relative.as_posix()
    if value.startswith(FORBIDDEN_TRACKED_PREFIXES):
        return "forbidden private or historical directory"
    parts = relative.parts
    if parts and (parts[0] in FORBIDDEN_RESEARCH_ROOTS or parts[0].startswith("paper_")):
        return "forbidden manuscript or private-research root"
    if relative.suffix.lower() in FORBIDDEN_RESEARCH_SUFFIXES:
        return f"forbidden manuscript or bibliography suffix {relative.suffix}"
    stem = relative.stem.lower()
    if "rebuttal" in stem or ("reviewer" in stem and ("response" in stem or "reply" in stem)):
        return "forbidden peer-review response filename"
    return None


def _tracked_repository_paths(root: Path, audit: Audit) -> list[Path]:
    try:
        result = subprocess.run(
            ["git", "ls-files", "-z"], cwd=root, capture_output=True, check=False
        )
    except OSError as exc:
        audit.errors.append(f"cannot inventory tracked repository files: {exc}")
        return []
    if result.returncode != 0:
        detail = result.stderr.decode("utf-8", errors="replace").strip()
        audit.errors.append(f"cannot inventory tracked repository files: {detail}")
        return []
    return [Path(raw.decode("utf-8", errors="surrogateescape")) for raw in result.stdout.split(b"\0") if raw]


def check_public_repository_safety(root: Path, audit: Audit) -> None:
    """Check explicit private directories and credential patterns."""
    # Unit tests exercise generation order with a bare temporary directory. The
    # public-boundary policy applies only when there is a Git index to inspect.
    if not (root / ".git").exists():
        return
    ignore_path = root / ".gitignore"
    try:
        ignore_lines = set(ignore_path.read_text(encoding="utf-8").splitlines())
    except OSError as exc:
        audit.errors.append(f"cannot read .gitignore for privacy policy: {exc}")
        ignore_lines = set()
    for required in REQUIRED_PRIVACY_IGNORES:
        audit.require(required in ignore_lines, f".gitignore must contain {required}")

    for relative in _tracked_repository_paths(root, audit):
        violation = public_repository_path_violation(relative)
        audit.require(violation is None, f"tracked path {relative} violates public boundary: {violation}")
        path = root / relative
        if not path.is_file() or path.is_symlink():
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue
        except OSError as exc:
            audit.errors.append(f"cannot read tracked file {relative}: {exc}")
            continue
        markers = find_private_markers(text)
        audit.require(not markers, f"private or credential marker in tracked file {relative}: {markers}")


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


def version_inventory(root: Path, audit: Audit) -> None:
    """Require every application-version owner to carry one identical SemVer."""
    try:
        application = version_file_value((root / "VERSION").read_text(encoding="utf-8"))
    except ValueError as exc:
        audit.errors.append(str(exc))
        application = "unknown"
    project_paths = [Path("webapp/Project.toml"), Path("packaging/Project.toml")]
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

    swift = (root / "frontend-swift/BiocircuitsExplorerMac.xcodeproj/project.pbxproj").read_text(encoding="utf-8")
    swift_marketing = sorted(set(re.findall(r"MARKETING_VERSION = ([^;]+);", swift)))
    swift_build = sorted(set(re.findall(r"CURRENT_PROJECT_VERSION = ([^;]+);", swift)))
    swift_macos_target = sorted(set(re.findall(r"MACOSX_DEPLOYMENT_TARGET = ([^;]+);", swift)))
    audit.require(len(swift_marketing) == 1, f"Swift marketing versions disagree: {swift_marketing}")
    audit.require(len(swift_build) == 1, f"Swift build versions disagree: {swift_build}")
    audit.require(len(swift_macos_target) == 1, f"Swift macOS deployment targets disagree: {swift_macos_target}")


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


def verify(root: Path, *, external: bool = True) -> int:
    audit = Audit()
    if sys.version_info < (3, 9):
        audit.errors.append("Python 3.9+ is required; CI uses Python 3.13")
        return audit.report()

    if external:
        run_command(
            root,
            ["julia", "--project=webapp", "webapp/scripts/gen_schemas.jl", "--check"],
            audit,
            "generated schema check",
        )

    version_inventory(root, audit)
    check_public_repository_safety(root, audit)

    if external:
        run_command(root, ["git", "diff", "--check"], audit, "git whitespace check")

    return audit.report()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check",
        action="store_true",
        required=True,
        help="read-only drift and integrity check",
    )
    parser.parse_args()
    return verify(ROOT, external=True)


if __name__ == "__main__":
    raise SystemExit(main())
