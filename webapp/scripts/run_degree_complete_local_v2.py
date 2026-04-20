#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
WEBAPP_DIR = REPO_ROOT / "webapp"
ATLAS_STORE_DIR = WEBAPP_DIR / "atlas_store"
RUN_SCAN_CHUNKED = WEBAPP_DIR / "scripts" / "run_atlas_scan_chunked.jl"
RUN_SCAN_STREAMING = WEBAPP_DIR / "scripts" / "run_atlas_scan_streaming.jl"


@dataclass(frozen=True)
class FamilySpec:
    slug: str
    mode: str
    search_profile_name: str
    min_template_order: int
    max_template_order: int
    require_homomeric_template: bool = False
    require_complex_growth_template: bool = False
    require_product_support_at_least: int = 0
    allow_higher_order_templates: bool = False
    allow_homomeric_templates: bool = False
    max_support: int = 3
    max_homomer_order: int = 3
    min_degree: int = 2


FAMILY_ORDER: tuple[FamilySpec, ...] = (
    FamilySpec(
        slug="orthant",
        mode="pairwise_binding",
        search_profile_name="binding_pairwise_orthant_v0",
        min_template_order=2,
        max_template_order=2,
        max_support=2,
        max_homomer_order=2,
    ),
    FamilySpec(
        slug="higher_order",
        mode="subset_binding",
        search_profile_name="binding_higher_order_v0",
        min_template_order=3,
        max_template_order=8,
        allow_higher_order_templates=True,
        max_support=8,
        max_homomer_order=8,
        min_degree=3,
    ),
    FamilySpec(
        slug="homomer",
        mode="pairwise_plus_homomeric",
        search_profile_name="binding_homomer_v0",
        min_template_order=2,
        max_template_order=3,
        require_homomeric_template=True,
        allow_homomeric_templates=True,
        max_support=3,
        max_homomer_order=3,
    ),
    FamilySpec(
        slug="homomer4plus",
        mode="pairwise_plus_homomeric",
        search_profile_name="binding_homomer4plus_v0",
        min_template_order=2,
        max_template_order=8,
        require_homomeric_template=True,
        require_product_support_at_least=4,
        allow_homomeric_templates=True,
        max_support=8,
        max_homomer_order=8,
    ),
    FamilySpec(
        slug="complex_growth",
        mode="complex_growth_binding",
        search_profile_name="binding_complex_growth_v0",
        min_template_order=2,
        max_template_order=8,
        require_complex_growth_template=True,
        require_product_support_at_least=3,
        allow_higher_order_templates=True,
        allow_homomeric_templates=True,
        max_support=8,
        max_homomer_order=8,
    ),
)


def _now_iso() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")


def _detect_julia() -> str:
    override = os.environ.get("BIOCIRCUITS_JULIA_BIN", "").strip()
    if override and Path(override).exists():
        return override
    juliaup_root = Path.home() / ".julia" / "juliaup"
    candidates: list[Path] = []
    if juliaup_root.exists():
        candidates.extend(juliaup_root.glob("julia-*/Julia-*.app/Contents/Resources/julia/bin/julia"))
    if candidates:
        def _candidate_key(path: Path) -> tuple[tuple[int, ...], str]:
            match = re.search(r"julia-(\\d+)\\.(\\d+)\\.(\\d+)", str(path))
            version = tuple(int(part) for part in match.groups()) if match else (0, 0, 0)
            return version, str(path)

        return str(max(candidates, key=_candidate_key))
    env = shutil.which("julia")
    if env:
        return env
    raise RuntimeError("Unable to find Julia in PATH.")


def _run_root(degree: int, root: Path | None = None) -> Path:
    if root is not None:
        return root.expanduser().resolve()
    return (ATLAS_STORE_DIR / "by_degree" / f"d{degree}").resolve()


def _families_for_degree(degree: int) -> list[FamilySpec]:
    return [family for family in FAMILY_ORDER if degree >= family.min_degree]


def _search_profile_for_family(family: FamilySpec, degree: int) -> dict[str, object]:
    return {
        "name": family.search_profile_name,
        "max_base_species": degree,
        "max_reactions": 5,
        "max_support": min(family.max_support, 8),
        "slice_mode": "change",
        "input_mode": "totals_only",
        "allow_higher_order_templates": family.allow_higher_order_templates,
        "allow_homomeric_templates": family.allow_homomeric_templates,
        "max_homomer_order": family.max_homomer_order,
    }


def _enumeration_for_family(family: FamilySpec, degree: int) -> dict[str, object]:
    return {
        "mode": family.mode,
        "base_species_counts": [degree],
        "min_reactions": 1,
        "max_reactions": 5,
        "min_template_order": family.min_template_order,
        "max_template_order": family.max_template_order,
        "require_homomeric_template": family.require_homomeric_template,
        "require_complex_growth_template": family.require_complex_growth_template,
        "require_product_support_at_least": family.require_product_support_at_least,
        "limit": 0,
    }


def _behavior_config() -> dict[str, object]:
    return {
        "path_scope": "feasible",
        "compute_volume": False,
        "include_path_records": True,
        "min_volume_mean": 0.0,
        "deduplicate": True,
        "keep_singular": True,
        "keep_nonasymptotic": False,
    }


def _change_expansion(degree: int) -> dict[str, object]:
    return {
        "mode": "orthant",
        "max_active_dims": degree,
        "include_axis_slices": True,
        "include_negative_directions": False,
        "limit_per_network": 0,
    }


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def _load_json(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))


def _is_completed_summary(summary: dict[str, object]) -> bool:
    if summary.get("status") != "completed":
        return False
    total_chunk_count = summary.get("total_chunk_count")
    completed_chunk_count = summary.get("completed_chunk_count")
    if total_chunk_count is None or completed_chunk_count is None:
        return True
    return completed_chunk_count == total_chunk_count


def _summary_execution_mode(summary: dict[str, object]) -> str:
    raw = summary.get("execution_mode") or summary.get("scan_mode") or "chunked"
    return str(raw)


def _family_spec_payload(
    degree: int,
    family: FamilySpec,
    raw_db_path: Path,
    network_parallelism: int,
    chunk_size: int,
    scan_mode: str,
    flush_network_count: int,
    discover_only: bool,
) -> dict[str, object]:
    payload: dict[str, object] = {
        "source_label": f"report_d{degree}_{family.slug}_complete_local",
        "network_parallelism": network_parallelism,
        "skip_existing": False,
        "persist_sqlite": not discover_only,
        "sqlite_path": str(raw_db_path),
        "sqlite_persist_mode": "path_only",
        "search_profile": _search_profile_for_family(family, degree),
        "behavior_config": _behavior_config(),
        "change_expansion": _change_expansion(degree),
        "enumeration": _enumeration_for_family(family, degree),
        "source_metadata": {
            "campaign": "degree_complete_path_only_v1",
            "degree": degree,
            "family": family.slug,
            "prepared_by": "run_degree_complete_local_v2.py",
            "scan_mode": scan_mode,
            "complete_families_for_degree": [item.slug for item in _families_for_degree(degree)],
        },
    }
    if scan_mode == "chunked":
        payload["chunk_size"] = chunk_size
    else:
        payload["stream_flush_network_count"] = flush_network_count
    if discover_only:
        payload["discover_only"] = True
        payload["persist_sqlite"] = False
    return payload


def _build_paths(run_root: Path, degree: int) -> dict[str, Path]:
    return {
        "run_root": run_root,
        "path_only_dir": run_root / "path_only",
        "meta_dir": run_root / "meta",
        "specs_dir": run_root / "meta" / "specs",
        "summaries_dir": run_root / "meta" / "summaries",
        "plan_path": run_root / "meta" / f"report_d{degree}_complete.plan.json",
        "path_only_db": run_root / "path_only" / f"report_d{degree}_complete_path_only.sqlite",
        "run_meta": run_root / "meta" / f"report_d{degree}_complete_local.run.meta.json",
    }


def _write_plan(
    degree: int,
    run_root: Path,
    network_parallelism: int,
    julia_threads: int,
    chunk_size: int,
    scan_mode: str,
    flush_network_count: int,
    families: list[FamilySpec],
    paths: dict[str, Path],
) -> None:
    plan = {
        "degree": degree,
        "created_at": _now_iso(),
        "run_root": str(run_root),
        "path_only_db": str(paths["path_only_db"]),
        "network_parallelism": network_parallelism,
        "julia_threads": julia_threads,
        "scan_mode": scan_mode,
        "chunk_size": chunk_size,
        "flush_network_count": flush_network_count,
        "complete_condition": {
            "family_slugs": [family.slug for family in families],
            "rule": "Each listed family summary must finish with status=completed. Chunked runs additionally report completed_chunk_count=total_chunk_count. The per-degree path-only database is complete when every family has completed against the same per-degree sqlite. Because families overlap, final path counts are not expected to equal the sum of per-family generated_network_count.",
        },
        "storage_format": {
            "path_only_db": str(paths["path_only_db"]),
            "meta_dir": str(paths["meta_dir"]),
            "notes": "The path_only sqlite is the authoritative artifact. It stores only path_record_id and behavior_code in the narrow path_only_records table. Per-family specs and summaries live under meta/ for recovery and audit.",
        },
        "execution_notes": {
            "streaming_resume": "Streaming runs are treated as fresh-run only. If a streaming summary exists and is not completed, reuse the completed output or rerun into a fresh run_root instead of resuming against the same sqlite.",
        },
    }
    _write_json(paths["plan_path"], plan)


def _run_scan(
    julia_bin: str,
    julia_threads: int,
    spec_path: Path,
    summary_path: Path,
    scan_mode: str,
) -> dict[str, object]:
    env = dict(os.environ)
    env["JULIA_NUM_THREADS"] = str(julia_threads)
    runner = RUN_SCAN_CHUNKED if scan_mode == "chunked" else RUN_SCAN_STREAMING
    cmd = [
        julia_bin,
        f"--project={WEBAPP_DIR}",
        str(runner),
        str(spec_path),
        str(summary_path),
    ]
    subprocess.run(cmd, cwd=REPO_ROOT, env=env, check=True)
    return _load_json(summary_path)


def _record_run_meta(paths: dict[str, Path], payload: dict[str, object]) -> None:
    _write_json(paths["run_meta"], payload)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run one degree of the complete atlas family set into a per-degree path-only sqlite output.")
    parser.add_argument("--degree", type=int, required=True, choices=[2, 3, 4], help="Degree to build.")
    parser.add_argument("--phase", choices=["discover", "run"], default="discover", help="Whether to only enumerate/planning or to execute and migrate.")
    parser.add_argument("--run-root", type=Path, default=None, help="Override the degree output root.")
    parser.add_argument("--network-parallelism", type=int, default=16, help="Network build parallelism.")
    parser.add_argument("--julia-threads", type=int, default=16, help="Julia thread count.")
    parser.add_argument("--scan-mode", choices=["chunked", "streaming"], default="chunked", help="Chunked keeps the historical static chunk runner; streaming uses a global worker pool and flushes completed networks to sqlite in small batches.")
    parser.add_argument("--chunk-size", type=int, default=128, help="Networks per chunk.")
    parser.add_argument("--flush-network-count", type=int, default=8, help="When scan-mode=streaming, flush completed network batches to sqlite after this many finished networks.")
    parser.add_argument("--julia-bin", default=None, help="Optional Julia executable.")
    args = parser.parse_args()

    if args.network_parallelism < 1 or args.julia_threads < 1 or args.chunk_size < 1 or args.flush_network_count < 1:
        raise SystemExit("network-parallelism, julia-threads, chunk-size, and flush-network-count must all be positive.")

    degree = args.degree
    run_root = _run_root(degree, args.run_root)
    families = _families_for_degree(degree)
    paths = _build_paths(run_root, degree)
    paths["specs_dir"].mkdir(parents=True, exist_ok=True)
    paths["summaries_dir"].mkdir(parents=True, exist_ok=True)
    paths["path_only_dir"].mkdir(parents=True, exist_ok=True)

    _write_plan(
        degree,
        run_root,
        args.network_parallelism,
        args.julia_threads,
        args.chunk_size,
        args.scan_mode,
        args.flush_network_count,
        families,
        paths,
    )

    julia_bin = args.julia_bin or _detect_julia()
    run_meta = {
        "phase": args.phase,
        "degree": degree,
        "started_at": _now_iso(),
        "run_root": str(run_root),
        "julia_bin": julia_bin,
        "julia_threads": args.julia_threads,
        "network_parallelism": args.network_parallelism,
        "scan_mode": args.scan_mode,
        "chunk_size": args.chunk_size,
        "flush_network_count": args.flush_network_count,
        "families": [family.slug for family in families],
        "path_only_db": str(paths["path_only_db"]),
        "storage_mode": "path_only_narrow",
        "plan_path": str(paths["plan_path"]),
    }
    _record_run_meta(paths, run_meta)

    family_results: list[dict[str, object]] = []
    discover_only = args.phase == "discover"
    for family in families:
        spec_path = paths["specs_dir"] / f"report_d{degree}_{family.slug}_complete_local.spec.json"
        summary_suffix = "discover.summary.json" if discover_only else "run.summary.json"
        summary_path = paths["summaries_dir"] / f"report_d{degree}_{family.slug}_complete_local.{summary_suffix}"
        spec_payload = _family_spec_payload(
            degree,
            family,
            paths["path_only_db"],
            args.network_parallelism,
            args.chunk_size,
            args.scan_mode,
            args.flush_network_count,
            discover_only=discover_only,
        )
        _write_json(spec_path, spec_payload)
        if summary_path.exists():
            existing_summary = _load_json(summary_path)
            if _is_completed_summary(existing_summary):
                summary = existing_summary
            elif args.scan_mode == "streaming" or _summary_execution_mode(existing_summary) == "streaming":
                raise RuntimeError(
                    f"Found incomplete streaming summary at {summary_path}. "
                    "Streaming mode is fresh-run only; rerun into a fresh run_root or remove the stale summary and partial sqlite first."
                )
            else:
                summary = _run_scan(julia_bin, args.julia_threads, spec_path, summary_path, args.scan_mode)
        else:
            summary = _run_scan(julia_bin, args.julia_threads, spec_path, summary_path, args.scan_mode)
        family_results.append({
            "family": family.slug,
            "spec_path": str(spec_path),
            "summary_path": str(summary_path),
            "status": summary.get("status"),
            "execution_mode": summary.get("execution_mode", args.scan_mode),
            "generated_network_count": summary.get("enumeration", {}).get("generated_network_count"),
            "total_network_count": summary.get("total_network_count"),
            "total_chunk_count": summary.get("total_chunk_count"),
            "completed_chunk_count": summary.get("completed_chunk_count"),
            "finished_at": summary.get("finished_at"),
        })

    run_meta["family_results"] = family_results
    run_meta["finished_at"] = _now_iso()
    _record_run_meta(paths, run_meta)

    print(json.dumps(run_meta, indent=2))


if __name__ == "__main__":
    main()
