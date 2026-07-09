#!/usr/bin/env python3
"""Rewrite only explicitly snapshotted paths in a rendered Compose config."""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import sys
import tempfile


def rewrite_config(path: Path, replacements: list[tuple[str, str]]) -> None:
    text = path.read_text(encoding="utf-8")
    for old, new in replacements:
        if not old or not new:
            raise ValueError("rollback path replacements must be nonempty")
        occurrences = text.count(old)
        if occurrences == 0:
            raise ValueError(f"rendered rollback config does not reference {old}")
        text = text.replace(old, new)

    descriptor, temporary_name = tempfile.mkstemp(
        dir=path.parent,
        prefix=f".{path.name}.",
        suffix=".tmp",
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temporary, path.stat().st_mode & 0o777)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser()
    result.add_argument("config", type=Path)
    result.add_argument(
        "--replace",
        nargs=2,
        action="append",
        default=[],
        metavar=("OLD", "SNAPSHOT"),
        required=True,
    )
    return result


def main() -> int:
    arguments = parser().parse_args()
    try:
        rewrite_config(arguments.config, [tuple(item) for item in arguments.replace])
    except (OSError, ValueError) as error:
        print(f"Cannot prepare rollback configuration: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
