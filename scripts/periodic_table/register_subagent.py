#!/usr/bin/env python3
"""Append a subagent registry row for a periodic table run."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--subagent-id", required=True)
    parser.add_argument("--task-scope", required=True)
    parser.add_argument("--status", default="completed")
    parser.add_argument("--notes", default="")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    path = Path(args.run_dir) / "logs" / "subagents_registry.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    record = {
        "time_utc": now,
        "supervisor": "Codex",
        "subagent_id": args.subagent_id,
        "codex_mode": "xhigh/fast",
        "task_scope": args.task_scope,
        "worktree": str(Path.cwd()),
        "tmux_session": None,
        "pid": None,
        "status": args.status,
        "start_time_utc": None,
        "end_time_utc": now if args.status in {"completed", "failed", "closed"} else None,
        "notes": args.notes,
    }
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, sort_keys=True, ensure_ascii=True) + "\n")
    print(path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
