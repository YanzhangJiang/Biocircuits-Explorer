"""JSONL schemas and IO for periodic table runs."""

from __future__ import annotations

import gzip
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from .complete_definition import (
    PROFILE_ID,
    STATUS_NO_COMPLETE,
    STATUS_UNKNOWN,
    code_commit,
    default_profile,
    profile_hash,
)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def atomic_write_text(path: str | Path, text: str) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_name(target.name + ".tmp")
    with tmp.open("w", encoding="utf-8") as handle:
        handle.write(text)
        handle.flush()
        os.fsync(handle.fileno())
    tmp.replace(target)


def write_json(path: str | Path, payload: Any) -> None:
    atomic_write_text(path, json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=True) + "\n")


def write_jsonl(path: str | Path, records: Iterable[dict[str, Any]]) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    opener = gzip.open if target.suffix == ".gz" else open
    mode = "wt"
    tmp = target.with_name(target.name + ".tmp")
    with opener(tmp, mode, encoding="utf-8") as handle:  # type: ignore[arg-type]
        for record in records:
            handle.write(json.dumps(record, sort_keys=True, ensure_ascii=True) + "\n")
    tmp.replace(target)


def append_event(path: str | Path, record: dict[str, Any]) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    payload = {"time_utc": utc_now(), **record}
    with target.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, sort_keys=True, ensure_ascii=True) + "\n")


def read_jsonl(path: str | Path) -> list[dict[str, Any]]:
    target = Path(path)
    opener = gzip.open if target.suffix == ".gz" else open
    with opener(target, "rt", encoding="utf-8") as handle:  # type: ignore[arg-type]
        return [json.loads(line) for line in handle if line.strip()]


def run_config(
    *,
    run_id: str,
    d_values: list[int],
    mu_values: list[int],
    property_ids: list[str],
    repo_root: str | Path,
    dry_run: bool,
) -> dict[str, Any]:
    profile = default_profile()
    return {
        "run_id": run_id,
        "created_at_utc": utc_now(),
        "profile_id": PROFILE_ID,
        "profile": profile,
        "profile_hash": profile_hash(profile),
        "code_commit": code_commit(repo_root),
        "d_values": d_values,
        "mu_values": mu_values,
        "property_ids": property_ids,
        "dry_run": dry_run,
        "storage_policy": profile["storage_policy"],
    }


def cell_result(
    *,
    run_id: str,
    d: int,
    mu: int,
    property_id: str,
    status: str,
    strength: dict[str, Any],
    repo_root: str | Path,
    witness_id: str | None = None,
    certificate_id: str | None = None,
    min_r: int | None = None,
    min_assembly_depth: int | None = None,
    robustness_id: str | None = None,
    notes: str = "",
    runtime_sec: float = 0.0,
) -> dict[str, Any]:
    return {
        "run_id": run_id,
        "profile_id": PROFILE_ID,
        "profile_hash": profile_hash(),
        "code_commit": code_commit(repo_root),
        "d": d,
        "mu": mu,
        "property_id": property_id,
        "status": status,
        "strength": strength,
        "min_r": min_r,
        "min_assembly_depth": min_assembly_depth,
        "witness_id": witness_id,
        "certificate_id": certificate_id,
        "robustness_id": robustness_id,
        "runtime_sec": runtime_sec,
        "notes": notes,
    }


def trivial_certificate(
    *,
    run_id: str,
    d: int,
    mu: int,
    property_id: str,
    certificate_id: str,
    reason: str,
) -> dict[str, Any]:
    return {
        "certificate_id": certificate_id,
        "property_id": property_id,
        "d": d,
        "mu": mu,
        "certificate_type": "trivial_no_allowed_binding_reactions",
        "profile_hash": profile_hash(),
        "candidate_count": 0,
        "canonical_network_count": 0,
        "slice_count": 0,
        "all_smaller_than_witness_checked": True,
        "negative_statement": reason,
        "verification_command": "python scripts/periodic_table/verify_results.py --run-dir <run_dir>",
        "run_id": run_id,
    }


def dry_status_for_cell(mu: int) -> str:
    return STATUS_NO_COMPLETE if mu < 2 else STATUS_UNKNOWN
