#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import os
import shlex
import subprocess
import sys
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[2]
WEBAPP_ROOT = REPO_ROOT / "webapp"
ATLAS_STORE = WEBAPP_ROOT / "atlas_store"
CAMPAIGN_ROOT = ATLAS_STORE / "complex_growth_campaign"
RUN_SCAN_CHUNKED = WEBAPP_ROOT / "scripts" / "run_atlas_scan_chunked.jl"
DEFAULT_CAMPAIGN_TAG = "order8_orthant"
DEFAULT_DEGREES = tuple(range(2, 9))
SUMMARY_FILENAME = "complex_growth_campaign_summary.json"
SETTINGS_FILENAME = "campaign_settings.json"
DEFAULT_JULIA = "/Users/yanzhang/.julia/juliaup/julia-1.12.5+0.aarch64.apple.darwin14/Julia-1.12.app/Contents/Resources/julia/bin/julia"


def _parse_degrees(raw: str) -> list[int]:
    values: set[int] = set()
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        if "-" in part:
            start_raw, end_raw = part.split("-", 1)
            start = int(start_raw)
            end = int(end_raw)
            step = 1 if start <= end else -1
            values.update(range(start, end + step, step))
        else:
            values.add(int(part))

    degrees = sorted(values)
    if not degrees:
        raise argparse.ArgumentTypeError("Expected at least one degree in 2..8.")
    invalid = [degree for degree in degrees if degree < 2 or degree > 8]
    if invalid:
        raise argparse.ArgumentTypeError(
            f"Degrees must stay within 2..8; received {invalid}."
        )
    return degrees


def _load_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)
        handle.write("\n")


def _write_json_if_changed(path: Path, payload: dict[str, Any]) -> bool:
    rendered = json.dumps(payload, indent=2) + "\n"
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and path.read_text(encoding="utf-8") == rendered:
        return False
    path.write_text(rendered, encoding="utf-8")
    return True


def _run(cmd: list[str], env: dict[str, str] | None = None) -> None:
    print("RUN", " ".join(shlex.quote(part) for part in cmd), flush=True)
    subprocess.run(cmd, cwd=REPO_ROOT, env=env, check=True)


def _default_network_parallelism(degree: int, override: int | None) -> int:
    if override is not None:
        return max(1, override)
    return 6 if degree <= 5 else 9


def _change_expansion_dims(degree: int, override: int | None) -> int:
    if override is None:
        return degree
    return max(2, min(degree, override))


def _campaign_paths(args: argparse.Namespace) -> tuple[Path, Path, Path]:
    campaign_dir = CAMPAIGN_ROOT / args.campaign_tag
    sqlite_path = (
        campaign_dir / "atlas_complex_growth_campaign.sqlite"
        if args.sqlite_path is None
        else Path(args.sqlite_path).expanduser().resolve()
    )
    summary_path = campaign_dir / SUMMARY_FILENAME
    return campaign_dir, sqlite_path, summary_path


def _scan_summary_path(spec_path: Path) -> Path:
    return spec_path.with_suffix(".final.summary.json")


def _scan_status(summary_path: Path) -> str:
    if not summary_path.exists():
        return "missing"
    try:
        summary = _load_json(summary_path)
    except Exception:
        return "invalid"
    return str(summary.get("status", "missing"))


def _summary_is_fresh(summary_path: Path, spec_path: Path) -> bool:
    return summary_path.exists() and summary_path.stat().st_mtime >= spec_path.stat().st_mtime


def _collect_scan_totals(summary: dict[str, Any]) -> dict[str, int]:
    delta = summary.get("delta_totals")
    if isinstance(delta, dict) and delta:
        return {
            "successful_network_count": int(delta.get("successful_network_count", 0)),
            "behavior_slice_count": int(delta.get("behavior_slice_count", 0)),
            "family_bucket_count": int(delta.get("family_bucket_count", 0)),
            "input_graph_slice_count": int(delta.get("input_graph_slice_count", 0)),
        }

    chunks = summary.get("chunks", [])
    return {
        "successful_network_count": sum(
            int(chunk.get("atlas_summary", {}).get("successful_network_count", 0))
            for chunk in chunks
        ),
        "behavior_slice_count": sum(
            int(chunk.get("atlas_summary", {}).get("behavior_slice_count", 0))
            for chunk in chunks
        ),
        "family_bucket_count": sum(
            int(chunk.get("atlas_summary", {}).get("family_bucket_count", 0))
            for chunk in chunks
        ),
        "input_graph_slice_count": sum(
            int(chunk.get("atlas_summary", {}).get("input_graph_slice_count", 0))
            for chunk in chunks
        ),
    }


def _campaign_settings(args: argparse.Namespace) -> dict[str, Any]:
    return {
        "campaign_type": "complex_growth_binding",
        "campaign_tag": args.campaign_tag,
        "max_support": args.max_support,
        "max_homomer_order": args.max_homomer_order,
        "max_reactions": args.max_reactions,
        "min_reactions": args.min_reactions,
        "min_product_support": args.min_product_support,
        "enumeration_limit": args.enumeration_limit,
        "require_homomeric_template": args.require_homomeric_template,
        "max_active_dims_override": args.max_active_dims,
        "include_negative_directions": args.include_negative_directions,
        "slice_mode": "change",
        "input_mode": "totals_only",
        "change_mode": "orthant",
        "persist_sqlite": args.persist_sqlite,
    }


def _validate_args(args: argparse.Namespace) -> None:
    if args.min_reactions < 1:
        raise ValueError("--min-reactions must be at least 1.")
    if args.max_reactions < args.min_reactions:
        raise ValueError("--max-reactions must be >= --min-reactions.")
    if args.max_support < 2:
        raise ValueError("--max-support must be at least 2.")
    if args.max_homomer_order < 2:
        raise ValueError("--max-homomer-order must be at least 2.")
    if args.max_homomer_order > args.max_support:
        raise ValueError("--max-homomer-order cannot exceed --max-support.")
    if args.min_product_support < 2:
        raise ValueError("--min-product-support must be at least 2.")
    if args.min_product_support > args.max_support:
        raise ValueError("--min-product-support cannot exceed --max-support.")
    if args.max_active_dims is not None and args.max_active_dims < 2:
        raise ValueError("--max-active-dims must be at least 2 for multi-input orthants.")
    if not RUN_SCAN_CHUNKED.exists():
        raise FileNotFoundError(f"Missing chunked scan runner: {RUN_SCAN_CHUNKED}")


def _build_spec(
    degree: int,
    args: argparse.Namespace,
    sqlite_path: Path,
) -> dict[str, Any]:
    source_label = f"report_d{degree}_complex_growth_scan"
    spec: dict[str, Any] = {
        "source_label": source_label,
        "source_metadata": {
            "campaign": "complex_growth_campaign",
            "campaign_tag": args.campaign_tag,
            "generated_by": Path(__file__).name,
            "grammar": "complex_growth_binding",
            "degree": degree,
            "max_support": args.max_support,
            "max_homomer_order": args.max_homomer_order,
            "min_product_support": args.min_product_support,
            "require_homomeric_template": args.require_homomeric_template,
        },
        "chunk_size": max(1, args.chunk_size),
        "network_parallelism": _default_network_parallelism(
            degree, args.network_parallelism
        ),
        "skip_existing": args.skip_existing,
        "persist_sqlite": args.persist_sqlite,
        "search_profile": {
            "name": "binding_complex_growth_order8_v0",
            "max_base_species": degree,
            "max_reactions": args.max_reactions,
            "max_support": args.max_support,
            "slice_mode": "change",
            "input_mode": "totals_only",
            "allow_homomeric_templates": True,
            "allow_higher_order_templates": True,
            "max_homomer_order": args.max_homomer_order,
        },
        "behavior_config": {
            "path_scope": "feasible",
            "compute_volume": False,
            "include_path_records": False,
            "min_volume_mean": 0.0,
            "deduplicate": True,
            "keep_singular": True,
            "keep_nonasymptotic": False,
        },
        "change_expansion": {
            "mode": "orthant",
            "max_active_dims": _change_expansion_dims(degree, args.max_active_dims),
            "include_axis_slices": True,
            "include_negative_directions": args.include_negative_directions,
            "limit_per_network": 0,
        },
        "enumeration": {
            "mode": "complex_growth_binding",
            "base_species_counts": [degree],
            "min_reactions": args.min_reactions,
            "max_reactions": args.max_reactions,
            "min_template_order": 2,
            "max_template_order": args.max_support,
            "require_homomeric_template": args.require_homomeric_template,
            "require_complex_growth_template": True,
            "require_product_support_at_least": args.min_product_support,
            "limit": args.enumeration_limit,
        },
    }
    if args.persist_sqlite:
        spec["sqlite_path"] = str(sqlite_path)
    return spec


def _ensure_sqlite(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        return
    print(f"Initializing empty campaign sqlite placeholder at {path}", flush=True)
    path.touch()


def _generate_specs(
    degrees: list[int],
    args: argparse.Namespace,
    sqlite_path: Path,
    campaign_dir: Path,
) -> list[Path]:
    spec_paths: list[Path] = []
    for degree in degrees:
        spec = _build_spec(degree, args, sqlite_path)
        spec_path = campaign_dir / f"report_d{degree}_complex_growth_scan.json"
        changed = _write_json_if_changed(spec_path, spec)
        verb = "Updated" if changed else "Reused"
        print(f"{verb} spec {spec_path}", flush=True)
        spec_paths.append(spec_path)
    return spec_paths


def _read_previous_settings(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        return _load_json(path)
    except Exception:
        return None


def _run_scans(
    degrees: list[int],
    spec_paths: list[Path],
    args: argparse.Namespace,
    settings_changed: bool,
) -> list[dict[str, Any]]:
    env = os.environ.copy()
    if args.julia_threads is not None:
        env["JULIA_NUM_THREADS"] = str(args.julia_threads)

    results: list[dict[str, Any]] = []
    for degree, spec_path in zip(degrees, spec_paths):
        summary_path = _scan_summary_path(spec_path)
        reused_completed_summary = False

        if (
            args.reuse_completed
            and not settings_changed
            and _scan_status(summary_path) == "completed"
            and _summary_is_fresh(summary_path, spec_path)
        ):
            print(f"Skipping completed scan {summary_path}", flush=True)
            summary = _load_json(summary_path)
            reused_completed_summary = True
        else:
            _run(
                [
                    args.julia,
                    f"--project={WEBAPP_ROOT}",
                    str(RUN_SCAN_CHUNKED),
                    str(spec_path),
                    str(summary_path),
                ],
                env=env,
            )
            summary = _load_json(summary_path)

        totals = _collect_scan_totals(summary)
        results.append(
            {
                "degree": degree,
                "spec_path": str(spec_path),
                "summary_path": str(summary_path),
                "status": summary.get("status", "missing"),
                "reused_completed_summary": reused_completed_summary,
                "elapsed_seconds": summary.get("elapsed_seconds"),
                "generated_network_count": summary.get("enumeration", {}).get(
                    "generated_network_count", 0
                ),
                **totals,
            }
        )
    return results


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Generate and optionally run complex_growth_binding atlas campaign specs "
            "for d=2..8 with multi-input orthant slices."
        )
    )
    parser.add_argument(
        "--degrees",
        default="2-8",
        help="Comma-separated degrees and ranges to generate, e.g. 2-8 or 2,4,6-8.",
    )
    parser.add_argument(
        "--campaign-tag",
        default=DEFAULT_CAMPAIGN_TAG,
        help=(
            "Output subdirectory under webapp/atlas_store/complex_growth_campaign/. "
            "Use a new tag if you change structural scan settings."
        ),
    )
    parser.add_argument(
        "--sqlite-path",
        default=None,
        help="Override the sqlite output path. Defaults inside the campaign-tag directory.",
    )
    parser.add_argument("--julia", default=DEFAULT_JULIA, help="Julia executable to use.")
    parser.add_argument(
        "--julia-threads",
        type=int,
        default=None,
        help="Set JULIA_NUM_THREADS for scan execution.",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=32,
        help="Chunk size passed to run_atlas_scan_chunked.jl.",
    )
    parser.add_argument(
        "--network-parallelism",
        type=int,
        default=None,
        help="Override per-degree network parallelism. Defaults to 6 for d<=5 and 9 for d>=6.",
    )
    parser.add_argument(
        "--max-active-dims",
        type=int,
        default=None,
        help="Clamp orthant slice dimensionality; defaults to each degree, never below 2.",
    )
    parser.add_argument(
        "--min-reactions",
        type=int,
        default=2,
        help="Minimum reaction count in the enumerator.",
    )
    parser.add_argument(
        "--max-reactions",
        type=int,
        default=8,
        help="Maximum reaction count in the search profile and enumerator.",
    )
    parser.add_argument(
        "--max-support",
        type=int,
        default=8,
        help="Maximum support/order allowed in the search profile and templates.",
    )
    parser.add_argument(
        "--max-homomer-order",
        type=int,
        default=8,
        help="Maximum homomer order allowed by the search profile.",
    )
    parser.add_argument(
        "--min-product-support",
        type=int,
        default=4,
        help="Require at least one enumerated product species of this support/order or larger.",
    )
    parser.add_argument(
        "--enumeration-limit",
        type=int,
        default=0,
        help="Optional cap on generated networks per degree; 0 means uncapped.",
    )
    parser.add_argument(
        "--include-negative-directions",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Include negative orthant directions in generated change specs.",
    )
    parser.add_argument(
        "--require-homomeric-template",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Restrict enumeration to networks that include at least one homomeric template.",
    )
    parser.add_argument(
        "--persist-sqlite",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Persist scan output into the campaign sqlite.",
    )
    parser.add_argument(
        "--skip-existing",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Ask the backend to skip slices already present in sqlite.",
    )
    parser.add_argument(
        "--reuse-completed",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Skip rerunning a degree when its completed summary is fresher than the spec.",
    )
    parser.add_argument(
        "--generate-only",
        action="store_true",
        help="Only write spec/config files; do not launch Julia scans.",
    )
    return parser


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()
    args.degrees = _parse_degrees(args.degrees)
    _validate_args(args)

    campaign_dir, sqlite_path, campaign_summary_path = _campaign_paths(args)
    settings_path = campaign_dir / SETTINGS_FILENAME
    current_settings = _campaign_settings(args)
    previous_settings = _read_previous_settings(settings_path)
    settings_changed = previous_settings not in (None, current_settings)

    if (
        settings_changed
        and args.persist_sqlite
        and sqlite_path.exists()
        and sqlite_path.stat().st_size > 0
    ):
        raise RuntimeError(
            "Structural campaign settings changed for an existing sqlite. "
            "Choose a new --campaign-tag or --sqlite-path before reusing persisted data."
        )

    campaign_dir.mkdir(parents=True, exist_ok=True)
    if args.persist_sqlite and not args.generate_only:
        _ensure_sqlite(sqlite_path)

    spec_paths = _generate_specs(args.degrees, args, sqlite_path, campaign_dir)
    _write_json_if_changed(settings_path, current_settings)

    scan_results: list[dict[str, Any]] = []
    if not args.generate_only:
        scan_results = _run_scans(args.degrees, spec_paths, args, settings_changed)

    campaign_summary = {
        "campaign_dir": str(campaign_dir),
        "campaign_tag": args.campaign_tag,
        "campaign_settings_path": str(settings_path),
        "campaign_settings": current_settings,
        "sqlite_path": str(sqlite_path) if args.persist_sqlite else None,
        "degrees": args.degrees,
        "generated_spec_paths": [str(path) for path in spec_paths],
        "scan_results": scan_results,
        "generate_only": args.generate_only,
    }
    _write_json(campaign_summary_path, campaign_summary)
    print(json.dumps(campaign_summary, indent=2), flush=True)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:  # pragma: no cover
        print(f"ERROR: {exc}", file=sys.stderr)
        raise
