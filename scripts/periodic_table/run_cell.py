#!/usr/bin/env python3
"""Run one conclusion-only periodic table cell in Tier 0 dry-run mode."""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

from src.periodic_table.dry_run import build_dry_run_records, normalize_properties, write_run_outputs
from src.periodic_table.result_schema import run_config


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--d", type=int, required=True)
    parser.add_argument("--mu", type=int, required=True)
    parser.add_argument("--properties", default="all")
    parser.add_argument("--profile", default="periodic_d_mu_v0")
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--output-dir", default=None)
    parser.add_argument("--checkpoint-dir", default=None)
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.profile != "periodic_d_mu_v0":
        raise SystemExit("Tier 0 only supports --profile periodic_d_mu_v0")
    if not args.dry_run:
        raise SystemExit("Only --dry-run is implemented; full ROP execution is intentionally disabled in Tier 0.")
    run_id = args.run_id or datetime.now(timezone.utc).strftime("periodic_tier0_%Y%m%dT%H%M%SZ")
    run_dir = Path(args.output_dir) if args.output_dir else REPO_ROOT / "results" / "periodic_table" / "runs" / run_id
    property_ids = normalize_properties(args.properties)
    config = run_config(
        run_id=run_id,
        d_values=[args.d],
        mu_values=[args.mu],
        property_ids=property_ids,
        repo_root=REPO_ROOT,
        dry_run=True,
    )
    if args.checkpoint_dir:
        config["external_checkpoint_dir"] = str(Path(args.checkpoint_dir))
    cells, witnesses, certificates = build_dry_run_records(
        run_id=run_id,
        d_values=[args.d],
        mu_values=[args.mu],
        property_ids=property_ids,
        repo_root=REPO_ROOT,
    )
    write_run_outputs(run_dir, config, cells, witnesses, certificates)
    print(run_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
