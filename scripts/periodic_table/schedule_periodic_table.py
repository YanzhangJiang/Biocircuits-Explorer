#!/usr/bin/env python3
"""Schedule Tier 0 periodic table dry-run cells without full atlas storage."""

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
    parser.add_argument("--d-max", type=int, default=2)
    parser.add_argument("--mu-max", type=int, default=3)
    parser.add_argument("--properties", default="all")
    parser.add_argument("--profile", default="periodic_d_mu_v0")
    parser.add_argument("--tier", default="tier0")
    parser.add_argument("--max-workers", type=int, default=1)
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--output-dir", default=None)
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.profile != "periodic_d_mu_v0":
        raise SystemExit("Tier 0 only supports --profile periodic_d_mu_v0")
    if not args.dry_run:
        raise SystemExit("Only --dry-run is implemented; full ROP execution is intentionally disabled in Tier 0.")
    if args.d_max < 1 or args.mu_max < 1:
        raise SystemExit("--d-max and --mu-max must be positive")
    run_id = args.run_id or datetime.now(timezone.utc).strftime("periodic_tier0_%Y%m%dT%H%M%SZ")
    run_dir = Path(args.output_dir) if args.output_dir else REPO_ROOT / "results" / "periodic_table" / "runs" / run_id
    property_ids = normalize_properties(args.properties)
    d_values = list(range(1, args.d_max + 1))
    mu_values = list(range(1, args.mu_max + 1))
    config = run_config(
        run_id=run_id,
        d_values=d_values,
        mu_values=mu_values,
        property_ids=property_ids,
        repo_root=REPO_ROOT,
        dry_run=True,
    )
    config["tier"] = args.tier
    config["max_workers_requested"] = args.max_workers
    cells, witnesses, certificates = build_dry_run_records(
        run_id=run_id,
        d_values=d_values,
        mu_values=mu_values,
        property_ids=property_ids,
        repo_root=REPO_ROOT,
    )
    write_run_outputs(run_dir, config, cells, witnesses, certificates)
    latest = REPO_ROOT / "results" / "periodic_table" / "latest"
    try:
        if latest.exists() or latest.is_symlink():
            latest.unlink()
        latest.symlink_to(run_dir)
    except OSError:
        pass
    print(run_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
