#!/usr/bin/env python3
"""Re-check a stored witness program against its periodic-table oracle."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

from src.periodic_table.property_oracles import observe_mimo_gain, observe_sign_switch, observe_ultrasensitivity
from src.periodic_table.result_schema import read_jsonl
from src.periodic_table.sign_programs import sign_program_summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--witness-id", required=True)
    return parser.parse_args()


def verify_witness_payload(witness: dict[str, Any]) -> list[str]:
    property_id = witness.get("property_id")
    program = witness.get("program", [])
    mu = int(witness.get("mu", 0))
    errors: list[str] = []
    try:
        if property_id == "sign_switch.v1":
            obs = observe_sign_switch(program)
            if not obs["hit"]:
                errors.append("stored program does not satisfy sign_switch.v1")
        elif property_id == "settle_to_zero.v1":
            summary = sign_program_summary(program)
            if not summary.get("settle_to_zero"):
                errors.append("stored program does not satisfy settle_to_zero.v1")
        elif property_id == "ultrasensitivity.v1":
            obs = observe_ultrasensitivity(program, mu)
            if not obs["hit"]:
                errors.append("stored program does not satisfy ultrasensitivity.v1")
        elif property_id == "mimo_gain.v1":
            if not isinstance(program, list) or not program or not all(isinstance(row, list) for row in program):
                errors.append("mimo witness program is not a matrix")
            else:
                obs = observe_mimo_gain(program)
                if not obs["hit"]:
                    errors.append("stored matrix does not satisfy mimo_gain.v1")
                rank = witness.get("strength", {}).get("max_rank_G")
                if rank is not None and int(rank) < 2:
                    errors.append("mimo witness rank is below 2")
        else:
            errors.append(f"reproduction oracle not implemented for {property_id}")
    except Exception as exc:  # pragma: no cover - defensive CLI guard
        errors.append(f"witness reproduction raised {type(exc).__name__}: {exc}")
    return errors


def main() -> int:
    args = parse_args()
    witnesses = read_jsonl(Path(args.run_dir) / "witnesses.jsonl.gz")
    match = next((record for record in witnesses if record.get("witness_id") == args.witness_id), None)
    if match is None:
        print(f"ERROR: witness not found: {args.witness_id}", file=sys.stderr)
        return 1
    errors = verify_witness_payload(match)
    if errors:
        for error in errors:
            print(f"ERROR: {error}", file=sys.stderr)
        return 1
    print("witness reproduction ok")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
