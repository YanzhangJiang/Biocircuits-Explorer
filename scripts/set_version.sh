#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="${BCX_ROOT_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
DRY_RUN=0

usage() {
    cat <<'USAGE'
Usage:
  scripts/set_version.sh [--dry-run] <semver>

Examples:
  scripts/set_version.sh 0.1.1
  scripts/set_version.sh --dry-run 0.2.0-rc.1

Set BCX_ROOT_DIR to operate on an isolated repository-shaped directory.
USAGE
}

while [ "$#" -gt 0 ]; do
    case "$1" in
        --dry-run)
            DRY_RUN=1
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        --*)
            echo "Unknown option: $1" >&2
            usage >&2
            exit 2
            ;;
        *)
            break
            ;;
    esac
done

if [ "$#" -ne 1 ]; then
    usage >&2
    exit 2
fi

python3 - "$ROOT_DIR" "$DRY_RUN" "$1" <<'PY'
from __future__ import annotations

import json
import os
from pathlib import Path
import re
import stat
import sys
import tempfile


REQUIRED_FILES = (
    "VERSION",
    "webapp/Project.toml",
    "packaging/Project.toml",
    "webapp_hpc/Project.toml",
    "webapp/package.json",
    "webapp/package-lock.json",
)

# Semantic Versioning 2.0.0: numeric core and prerelease identifiers reject
# leading zeroes; build metadata permits them.
IDENTIFIER = r"(?:0|[1-9][0-9]*|[0-9]*[A-Za-z-][0-9A-Za-z-]*)"
SEMVER = re.compile(
    rf"^(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)"
    rf"(?:-{IDENTIFIER}(?:\.{IDENTIFIER})*)?"
    r"(?:\+[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?$"
)


class VersionWorkflowError(Exception):
    pass


class UsageError(VersionWorkflowError):
    pass


def require_semver(value: object, label: str) -> str:
    if not isinstance(value, str) or SEMVER.fullmatch(value) is None:
        raise VersionWorkflowError(f"{label} must contain a valid semantic version, got {value!r}")
    return value


def decode_utf8(relative: str, content: bytes) -> str:
    try:
        return content.decode("utf-8")
    except UnicodeDecodeError as error:
        raise VersionWorkflowError(f"{relative} is not valid UTF-8: {error}") from error


def newline_style(relative: str, text: str) -> str:
    without_crlf = text.replace("\r\n", "")
    if "\r\n" in text:
        if "\n" in without_crlf or "\r" in without_crlf:
            raise VersionWorkflowError(f"{relative} uses mixed newline styles")
        return "\r\n"
    if "\n" in text:
        if "\r" in text:
            raise VersionWorkflowError(f"{relative} uses mixed newline styles")
        return "\n"
    if "\r" in text:
        return "\r"
    return "\n"


def update_version_file(relative: str, content: bytes, new_version: str) -> bytes:
    text = decode_utf8(relative, content)
    match = re.fullmatch(r"([^\r\n]+)(\r\n|\n|\r)?", text)
    if match is None:
        raise VersionWorkflowError(f"{relative} must contain exactly one semantic-version line")
    require_semver(match.group(1), relative)
    return f"{new_version}{match.group(2) or ''}".encode("utf-8")


TOML_VERSION = re.compile(
    r"(?m)^(?P<prefix>[ \t]*version[ \t]*=[ \t]*)(?P<quote>['\"])(?P<value>[^'\"\r\n]*)(?P=quote)"
)


def update_project_toml(relative: str, content: bytes, new_version: str) -> bytes:
    text = decode_utf8(relative, content)
    first_table = re.search(r"(?m)^[ \t]*\[", text)
    root_end = first_table.start() if first_table else len(text)
    matches = list(TOML_VERSION.finditer(text, 0, root_end))
    if len(matches) != 1:
        raise VersionWorkflowError(
            f"{relative} must have exactly one quoted top-level version field; found {len(matches)}"
        )
    match = matches[0]
    require_semver(match.group("value"), f"{relative} top-level version")
    updated = text[: match.start("value")] + new_version + text[match.end("value") :]
    return updated.encode("utf-8")


def reject_duplicate_keys(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise VersionWorkflowError(f"duplicate JSON key {key!r}")
        result[key] = value
    return result


def load_json(relative: str, content: bytes):
    text = decode_utf8(relative, content)
    try:
        document = json.loads(text, object_pairs_hook=reject_duplicate_keys)
    except json.JSONDecodeError as error:
        raise VersionWorkflowError(f"{relative} is malformed JSON: {error}") from error
    except VersionWorkflowError as error:
        raise VersionWorkflowError(f"{relative} is malformed JSON: {error}") from error
    if not isinstance(document, dict):
        raise VersionWorkflowError(f"{relative} must contain a top-level JSON object")
    return text, document


def render_json(relative: str, original: str, document: dict) -> bytes:
    newline = newline_style(relative, original)
    had_final_newline = original.endswith(("\r\n", "\n", "\r"))
    rendered = json.dumps(document, ensure_ascii=False, indent=2)
    if newline != "\n":
        rendered = rendered.replace("\n", newline)
    if had_final_newline:
        rendered += newline
    return rendered.encode("utf-8")


def update_package_json(relative: str, content: bytes, new_version: str) -> bytes:
    original, document = load_json(relative, content)
    require_semver(document.get("version"), f"{relative} top-level version")
    document["version"] = new_version
    return render_json(relative, original, document)


def update_package_lock(relative: str, content: bytes, new_version: str) -> bytes:
    original, document = load_json(relative, content)
    require_semver(document.get("version"), f"{relative} top-level version")
    packages = document.get("packages")
    if not isinstance(packages, dict):
        raise VersionWorkflowError(f"{relative} packages must be a JSON object")
    root_package = packages.get("")
    if not isinstance(root_package, dict):
        raise VersionWorkflowError(f"{relative} packages[''] must be a JSON object")
    require_semver(root_package.get("version"), f"{relative} packages[''].version")
    document["version"] = new_version
    root_package["version"] = new_version
    return render_json(relative, original, document)


UPDATERS = {
    "VERSION": update_version_file,
    "webapp/Project.toml": update_project_toml,
    "packaging/Project.toml": update_project_toml,
    "webapp_hpc/Project.toml": update_project_toml,
    "webapp/package.json": update_package_json,
    "webapp/package-lock.json": update_package_lock,
}


def fingerprint(path: Path):
    info = path.stat()
    return (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_mode)


def stage_atomic(path: Path, content: bytes, mode: int) -> Path:
    descriptor, temporary_name = tempfile.mkstemp(
        dir=path.parent, prefix=f".{path.name}.", suffix=".tmp"
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temporary, mode)
        return temporary
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise


def main(root_argument: str, dry_run_argument: str, new_version: str) -> int:
    if SEMVER.fullmatch(new_version) is None:
        raise UsageError(
            f"version must be Semantic Versioning 2.0.0, for example 0.1.1 or 0.2.0-rc.1; got {new_version!r}"
        )

    root = Path(root_argument).expanduser().resolve()
    paths = {relative: root / relative for relative in REQUIRED_FILES}
    missing = [relative for relative, path in paths.items() if not path.is_file()]
    if missing:
        raise VersionWorkflowError("missing required files: " + ", ".join(missing))

    prepared = {}
    for relative in REQUIRED_FILES:
        path = paths[relative]
        content = path.read_bytes()
        prepared[relative] = {
            "path": path,
            "original": content,
            "updated": UPDATERS[relative](relative, content, new_version),
            "mode": stat.S_IMODE(path.stat().st_mode),
            "fingerprint": fingerprint(path),
        }

    print(f"Setting Biocircuits Explorer version to {new_version}")
    if dry_run_argument == "1":
        for relative in REQUIRED_FILES:
            print(f"[dry-run] would update {relative}")
        return 0

    changed = [relative for relative in REQUIRED_FILES if prepared[relative]["original"] != prepared[relative]["updated"]]
    if not changed:
        print("Version already synchronized; no files changed.")
        return 0

    staged = {}
    try:
        for relative in changed:
            item = prepared[relative]
            staged[relative] = stage_atomic(item["path"], item["updated"], item["mode"])

        # Refuse to overwrite a file changed concurrently after validation.
        for relative in REQUIRED_FILES:
            item = prepared[relative]
            if fingerprint(item["path"]) != item["fingerprint"]:
                raise VersionWorkflowError(f"{relative} changed while the update was being prepared")

        for relative in changed:
            item = prepared[relative]
            os.replace(staged.pop(relative), item["path"])
            print(f"updated {relative}")
    finally:
        for temporary in staged.values():
            temporary.unlink(missing_ok=True)

    print(f"Version updated in {len(changed)} file(s).")
    return 0


try:
    raise SystemExit(main(sys.argv[1], sys.argv[2], sys.argv[3]))
except UsageError as error:
    print(f"set_version: {error}", file=sys.stderr)
    raise SystemExit(2)
except (OSError, VersionWorkflowError) as error:
    print(f"set_version: {error}", file=sys.stderr)
    raise SystemExit(1)
PY
