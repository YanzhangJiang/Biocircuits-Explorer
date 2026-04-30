#!/usr/bin/env python3
"""Verify conclusion-only periodic table result integrity."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

from src.periodic_table.complete_definition import STATUS_NO_COMPLETE, STATUS_UNKNOWN, STATUS_YES_EXISTENCE, STATUS_YES_MINIMAL
from src.periodic_table.result_schema import read_jsonl
from scripts.periodic_table.reproduce_witness import verify_witness_payload


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--require-witness-reproduction", action="store_true")
    parser.add_argument("--require-certificates-for-no-complete", action="store_true")
    return parser.parse_args()


def verify_run_dir(run_dir: str | Path, *, require_witness_reproduction: bool = False, require_certificates_for_no_complete: bool = False) -> list[str]:
    target = Path(run_dir)
    errors: list[str] = []
    config_path = target / "config.json"
    if not config_path.exists():
        return [f"missing config: {config_path}"]
    config = json.loads(config_path.read_text(encoding="utf-8"))
    cells = read_jsonl(target / "cell_results.jsonl.gz")
    witnesses = read_jsonl(target / "witnesses.jsonl.gz")
    certificates = read_jsonl(target / "certificates.jsonl.gz")
    witness_ids = {record.get("witness_id") for record in witnesses}
    certificate_ids = {record.get("certificate_id") for record in certificates}

    expected = {
        (d, mu, property_id)
        for d in config.get("d_values", [])
        for mu in config.get("mu_values", [])
        for property_id in config.get("property_ids", [])
    }
    observed = {(record.get("d"), record.get("mu"), record.get("property_id")) for record in cells}
    missing = sorted(expected - observed)
    extra = sorted(observed - expected)
    if missing:
        errors.append(f"missing cell result rows: {missing[:10]}{'...' if len(missing) > 10 else ''}")
    if extra:
        errors.append(f"unexpected cell result rows: {extra[:10]}{'...' if len(extra) > 10 else ''}")

    profile_hash = config.get("profile_hash")
    for idx, record in enumerate(cells):
        status = record.get("status")
        if record.get("profile_hash") != profile_hash:
            errors.append(f"cell row {idx} profile hash mismatch")
        if status in {STATUS_YES_EXISTENCE, STATUS_YES_MINIMAL} and not record.get("witness_id"):
            errors.append(f"positive row lacks witness_id: {record}")
        if record.get("witness_id") and record.get("witness_id") not in witness_ids:
            errors.append(f"cell row references missing witness: {record.get('witness_id')}")
        if status == STATUS_NO_COMPLETE:
            if not record.get("certificate_id"):
                errors.append(f"NO_COMPLETE row lacks certificate_id: {record}")
            elif record.get("certificate_id") not in certificate_ids:
                errors.append(f"NO_COMPLETE row references missing certificate: {record.get('certificate_id')}")
        if status == STATUS_UNKNOWN and record.get("certificate_id"):
            errors.append(f"UNKNOWN row must not carry negative certificate: {record}")

    if require_certificates_for_no_complete:
        no_complete_count = sum(1 for record in cells if record.get("status") == STATUS_NO_COMPLETE)
        if no_complete_count and not certificates:
            errors.append("NO_COMPLETE rows exist but certificates file is empty")
    if require_witness_reproduction:
        for witness in witnesses:
            witness_errors = verify_witness_payload(witness)
            for error in witness_errors:
                errors.append(f"witness {witness.get('witness_id')} reproduction failed: {error}")

    suspicious_names = ("path_records", "path_only", "route_blob", "full_atlas")
    for path in target.rglob("*"):
        if path.is_file() and path.stat().st_size > 1_000_000_000:
            errors.append(f"file exceeds 1GB: {path}")
        lowered = path.name.lower()
        if any(token in lowered for token in suspicious_names) and path.stat().st_size > 10_000_000:
            errors.append(f"suspicious large atlas/path artifact: {path}")
    return errors


def main() -> int:
    args = parse_args()
    errors = verify_run_dir(
        args.run_dir,
        require_witness_reproduction=args.require_witness_reproduction,
        require_certificates_for_no_complete=args.require_certificates_for_no_complete,
    )
    if errors:
        for error in errors:
            print(f"ERROR: {error}", file=sys.stderr)
        return 1
    print("verification ok")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
