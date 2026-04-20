#!/usr/bin/env python3

from __future__ import annotations

import argparse
import queue
import json
import os
import shlex
import shutil
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[2]
WEBAPP = REPO_ROOT / "webapp"
ATLAS_STORE = WEBAPP / "atlas_store"
RUN_SCAN_CHUNKED = WEBAPP / "scripts" / "run_atlas_scan_chunked.jl"
MERGE_SQLITE_SHARDS = WEBAPP / "scripts" / "merge_atlas_sqlite_shards.jl"
DEFAULT_OUTPUT_ROOT = ATLAS_STORE / "extended_assembly_sharded_campaign"
DEFAULT_DEGREES = tuple(range(2, 9))


@dataclass(frozen=True)
class CampaignFamily:
    slug: str
    search_profile_name: str
    enumeration_mode: str
    require_homomeric_template: bool
    require_complex_growth_template: bool
    require_product_support_at_least: int
    allow_higher_order_templates: bool
    allow_homomeric_templates: bool
    max_support: int
    max_homomer_order: int
    min_template_order: int = 2
    max_template_order: int = 8
    chunk_size: int = 64
    network_parallelism: int = 9


FAMILIES = {
    "homomer4plus": CampaignFamily(
        slug="homomer4plus",
        search_profile_name="binding_homomer4plus_v0",
        enumeration_mode="pairwise_plus_homomeric",
        require_homomeric_template=True,
        require_complex_growth_template=False,
        require_product_support_at_least=4,
        allow_higher_order_templates=False,
        allow_homomeric_templates=True,
        max_support=8,
        max_homomer_order=8,
        chunk_size=32,
        network_parallelism=6,
    ),
    "complex_growth": CampaignFamily(
        slug="complex_growth",
        search_profile_name="binding_complex_growth_v0",
        enumeration_mode="complex_growth_binding",
        require_homomeric_template=False,
        require_complex_growth_template=True,
        require_product_support_at_least=3,
        allow_higher_order_templates=True,
        allow_homomeric_templates=True,
        max_support=8,
        max_homomer_order=8,
        chunk_size=64,
        network_parallelism=9,
    ),
}


def _detect_julia() -> str:
    env = os.environ.get("JULIA")
    if env:
        return env
    discovered = shutil.which("julia")
    if discovered:
        return discovered
    fallback = Path("/Users/yanzhang/.julia/juliaup/julia-1.12.5+0.aarch64.apple.darwin14/Julia-1.12.app/Contents/Resources/julia/bin/julia")
    if fallback.exists():
        return str(fallback)
    return "julia"


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
    invalid = [degree for degree in degrees if degree < 2 or degree > 8]
    if invalid or not degrees:
        raise argparse.ArgumentTypeError(f"Degrees must lie in 2..8; received {raw!r}.")
    return degrees


def _parse_families(raw: str) -> list[CampaignFamily]:
    names = [part.strip() for part in raw.split(",") if part.strip()]
    if not names:
        raise argparse.ArgumentTypeError("Expected at least one family slug.")
    missing = [name for name in names if name not in FAMILIES]
    if missing:
        raise argparse.ArgumentTypeError(
            f"Unsupported families {missing}; choose from {sorted(FAMILIES)}."
        )
    return [FAMILIES[name] for name in names]


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


def _popen(cmd: list[str], env: dict[str, str] | None = None) -> subprocess.Popen[str]:
    print("RUN", " ".join(shlex.quote(part) for part in cmd), flush=True)
    return subprocess.Popen(cmd, cwd=REPO_ROOT, env=env)


def _campaign_settings(args: argparse.Namespace) -> dict[str, Any]:
    return {
        "runner": Path(__file__).name,
        "families": [family.slug for family in args.families],
        "degrees": args.degrees,
        "scheduler": args.scheduler,
        "shard_count": args.shard_count,
        "shard_mode": args.shard_mode,
        "julia_threads": args.julia_threads,
        "persist_shard_sqlite": args.persist_shard_sqlite,
        "merge_campaign_sqlite": args.merge_campaign_sqlite,
        "network_parallelism_override": args.network_parallelism,
        "chunk_size_override": args.chunk_size,
    }


def _campaign_dir(args: argparse.Namespace) -> Path:
    return Path(args.output_root).expanduser().resolve() / args.campaign_tag


def _campaign_sqlite_path(args: argparse.Namespace, campaign_dir: Path) -> Path:
    if args.sqlite_path:
        return Path(args.sqlite_path).expanduser().resolve()
    return campaign_dir / "atlas_extended_assembly_campaign.sqlite"


def _family_degree_dir(campaign_dir: Path, family: CampaignFamily, degree: int) -> Path:
    return campaign_dir / family.slug / f"d{degree}"


def _base_spec_path(campaign_dir: Path, family: CampaignFamily, degree: int) -> Path:
    return _family_degree_dir(campaign_dir, family, degree) / f"report_d{degree}_{family.slug}_scan.json"


def _scan_summary_path(campaign_dir: Path, family: CampaignFamily, degree: int) -> Path:
    return _family_degree_dir(campaign_dir, family, degree) / f"report_d{degree}_{family.slug}_scan.final.summary.json"


def _scan_sqlite_path(campaign_dir: Path, family: CampaignFamily, degree: int) -> Path:
    return _family_degree_dir(campaign_dir, family, degree) / f"report_d{degree}_{family.slug}_scan.sqlite"


def _scan_merge_summary_path(campaign_dir: Path, family: CampaignFamily, degree: int) -> Path:
    return _family_degree_dir(campaign_dir, family, degree) / f"report_d{degree}_{family.slug}_scan.merge.summary.json"


def _shard_dir(campaign_dir: Path, family: CampaignFamily, degree: int, shard_index: int, shard_count: int) -> Path:
    return _family_degree_dir(campaign_dir, family, degree) / "shards" / f"shard_{shard_index:02d}_of_{shard_count:02d}"


def _shard_summary_path(campaign_dir: Path, family: CampaignFamily, degree: int, shard_index: int, shard_count: int) -> Path:
    return _shard_dir(campaign_dir, family, degree, shard_index, shard_count) / "scan.summary.json"


def _shard_sqlite_path(campaign_dir: Path, family: CampaignFamily, degree: int, shard_index: int, shard_count: int) -> Path:
    return _shard_dir(campaign_dir, family, degree, shard_index, shard_count) / "scan.sqlite"


def _plan_summary_path(campaign_dir: Path, family: CampaignFamily, degree: int) -> Path:
    return _family_degree_dir(campaign_dir, family, degree) / "plan.summary.json"


def _worker_dir(campaign_dir: Path, family: CampaignFamily, degree: int, worker_index: int, worker_count: int) -> Path:
    return _family_degree_dir(campaign_dir, family, degree) / "workers" / f"worker_{worker_index:02d}_of_{worker_count:02d}"


def _worker_sqlite_path(campaign_dir: Path, family: CampaignFamily, degree: int, worker_index: int, worker_count: int) -> Path:
    return _worker_dir(campaign_dir, family, degree, worker_index, worker_count) / "scan.sqlite"


def _worker_chunk_summary_path(
    campaign_dir: Path,
    family: CampaignFamily,
    degree: int,
    worker_index: int,
    worker_count: int,
    chunk_index: int,
) -> Path:
    return _worker_dir(campaign_dir, family, degree, worker_index, worker_count) / "chunks" / f"chunk_{chunk_index:04d}.summary.json"


def _chunk_specs_dir(campaign_dir: Path, family: CampaignFamily, degree: int) -> Path:
    return _family_degree_dir(campaign_dir, family, degree) / "chunk_specs"


def _default_network_parallelism(family: CampaignFamily, override: int | None) -> int:
    if override is not None:
        return max(1, override)
    return family.network_parallelism


def _default_chunk_size(family: CampaignFamily, override: int | None) -> int:
    if override is not None:
        return max(1, override)
    return family.chunk_size


def _build_spec(family: CampaignFamily, degree: int, args: argparse.Namespace) -> dict[str, Any]:
    return {
        "source_label": f"report_d{degree}_{family.slug}_scan",
        "source_metadata": {
            "campaign": "extended_assembly_sharded",
            "campaign_tag": args.campaign_tag,
            "family": family.slug,
            "degree": degree,
            "runner": Path(__file__).name,
        },
        "chunk_size": _default_chunk_size(family, args.chunk_size),
        "network_parallelism": _default_network_parallelism(family, args.network_parallelism),
        "skip_existing": True,
        "persist_sqlite": args.persist_shard_sqlite,
        "search_profile": {
            "name": family.search_profile_name,
            "max_base_species": degree,
            "max_reactions": 5,
            "max_support": family.max_support,
            "slice_mode": "change",
            "input_mode": "totals_only",
            "allow_higher_order_templates": family.allow_higher_order_templates,
            "allow_homomeric_templates": family.allow_homomeric_templates,
            "max_homomer_order": family.max_homomer_order,
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
            "max_active_dims": degree,
            "include_axis_slices": True,
            "include_negative_directions": False,
            "limit_per_network": 0,
        },
        "enumeration": {
            "mode": family.enumeration_mode,
            "base_species_counts": [degree],
            "min_reactions": 1,
            "max_reactions": 5,
            "min_template_order": family.min_template_order,
            "max_template_order": family.max_template_order,
            "require_homomeric_template": family.require_homomeric_template,
            "require_complex_growth_template": family.require_complex_growth_template,
            "require_product_support_at_least": family.require_product_support_at_least,
            "limit": 0,
        },
    }


def _prepare_specs(args: argparse.Namespace, campaign_dir: Path) -> list[Path]:
    spec_paths: list[Path] = []
    for family in args.families:
        for degree in args.degrees:
            spec_path = _base_spec_path(campaign_dir, family, degree)
            _write_json_if_changed(spec_path, _build_spec(family, degree, args))
            spec_paths.append(spec_path)
    return spec_paths


def _summary_is_completed(path: Path) -> bool:
    if not path.exists():
        return False
    try:
        summary = _load_json(path)
    except Exception:
        return False
    return str(summary.get("status", "")) == "completed"


def _summary_has_status(path: Path, *statuses: str) -> bool:
    if not path.exists():
        return False
    try:
        summary = _load_json(path)
    except Exception:
        return False
    return str(summary.get("status", "")) in set(statuses)


def _discover_scan_plan(
    family: CampaignFamily,
    degree: int,
    args: argparse.Namespace,
    campaign_dir: Path,
) -> dict[str, Any]:
    plan_summary = _plan_summary_path(campaign_dir, family, degree)
    spec_path = _base_spec_path(campaign_dir, family, degree)
    chunk_specs_dir = _chunk_specs_dir(campaign_dir, family, degree)
    if (
        args.reuse_completed
        and _summary_has_status(plan_summary, "planned", "completed")
        and plan_summary.exists()
        and plan_summary.stat().st_mtime >= spec_path.stat().st_mtime
        and chunk_specs_dir.exists()
    ):
        plan = _load_json(plan_summary)
        chunk_spec_paths = plan.get("chunk_spec_paths", [])
        if chunk_spec_paths and all(Path(path).exists() for path in chunk_spec_paths):
            return plan

    plan_summary.parent.mkdir(parents=True, exist_ok=True)
    chunk_specs_dir.mkdir(parents=True, exist_ok=True)
    env = os.environ.copy()
    env["ATLAS_EMIT_CHUNK_SPECS_ONLY"] = "1"
    env["ATLAS_WRITE_CHUNK_SPECS_DIR"] = str(chunk_specs_dir)
    cmd = [
        args.julia,
        f"--project={WEBAPP}",
        str(RUN_SCAN_CHUNKED),
        str(spec_path),
        str(plan_summary),
    ]
    _run(cmd, env=env)
    return _load_json(plan_summary)


def _collect_existing_chunk_summaries(
    campaign_dir: Path,
    family: CampaignFamily,
    degree: int,
    expected_total_chunk_count: int | None = None,
) -> dict[int, Path]:
    workers_root = _family_degree_dir(campaign_dir, family, degree) / "workers"
    if not workers_root.exists():
        return {}
    discovered: dict[int, tuple[float, Path]] = {}
    for path in workers_root.rglob("chunk_*.summary.json"):
        if not _summary_is_completed(path):
            continue
        try:
            summary = _load_json(path)
            if expected_total_chunk_count is not None and int(summary.get("total_chunk_count", 0)) != expected_total_chunk_count:
                continue
            chunks = summary.get("chunks", [])
            if len(chunks) != 1:
                continue
            chunk_index = int(chunks[0].get("chunk_index", 0))
        except Exception:
            continue
        if chunk_index < 1:
            continue
        mtime = path.stat().st_mtime
        existing = discovered.get(chunk_index)
        if existing is None or mtime > existing[0]:
            discovered[chunk_index] = (mtime, path)
    return {chunk_index: entry[1] for chunk_index, entry in discovered.items()}


def _collect_existing_worker_sqlites(
    campaign_dir: Path,
    family: CampaignFamily,
    degree: int,
) -> list[Path]:
    workers_root = _family_degree_dir(campaign_dir, family, degree) / "workers"
    if not workers_root.exists():
        return []
    return sorted(path for path in workers_root.rglob("scan.sqlite") if path.exists())


def _launch_single_chunk(
    family: CampaignFamily,
    degree: int,
    args: argparse.Namespace,
    chunk_spec_path: Path,
    chunk_summary: Path,
    worker_sqlite: Path,
    worker_index: int,
    worker_count: int,
    chunk_index: int,
) -> Path:
    chunk_summary.parent.mkdir(parents=True, exist_ok=True)
    worker_sqlite.parent.mkdir(parents=True, exist_ok=True)

    env = os.environ.copy()
    if args.julia_threads is not None:
        env["JULIA_NUM_THREADS"] = str(args.julia_threads)
    env["ATLAS_SQLITE_PATH"] = str(worker_sqlite)
    env["ATLAS_SOURCE_METADATA_JSON"] = json.dumps(
        {
            "campaign": "extended_assembly_sharded",
            "campaign_tag": args.campaign_tag,
            "family": family.slug,
            "degree": degree,
            "scheduler": "dynamic",
            "worker_index": worker_index,
            "worker_count": worker_count,
            "chunk_index": chunk_index,
        }
    )
    _run(
        [
            args.julia,
            f"--project={WEBAPP}",
            str(RUN_SCAN_CHUNKED),
            str(chunk_spec_path),
            str(chunk_summary),
        ],
        env=env,
    )
    return chunk_summary


def _run_dynamic_worker(
    family: CampaignFamily,
    degree: int,
    args: argparse.Namespace,
    chunk_specs_by_index: dict[int, Path],
    worker_dir: Path,
    worker_sqlite: Path,
    worker_index: int,
    worker_count: int,
    pending_chunks: queue.Queue[int],
) -> dict[str, Any]:
    completed: list[Path] = []
    while True:
        try:
            chunk_index = pending_chunks.get_nowait()
        except queue.Empty:
            break
        try:
            completed.append(
                _launch_single_chunk(
                    family,
                    degree,
                    args,
                    chunk_specs_by_index[chunk_index],
                    worker_dir / "chunks" / f"chunk_{chunk_index:04d}.summary.json",
                    worker_sqlite,
                    worker_index,
                    worker_count,
                    chunk_index,
                )
            )
        finally:
            pending_chunks.task_done()
    return {
        "worker_index": worker_index,
        "worker_count": worker_count,
        "chunk_summary_paths": [str(path) for path in completed],
        "worker_sqlite_path": str(worker_sqlite),
    }


def _launch_dynamic_queue(
    family: CampaignFamily,
    degree: int,
    args: argparse.Namespace,
    campaign_dir: Path,
) -> tuple[list[Path], list[Path], float, dict[str, Any]]:
    plan = _discover_scan_plan(family, degree, args, campaign_dir)
    total_chunk_count = int(plan.get("total_chunk_count", 0))
    if total_chunk_count < 1:
        raise RuntimeError(f"Discovery produced no chunks for family={family.slug} degree={degree}.")

    existing_chunks = _collect_existing_chunk_summaries(
        campaign_dir,
        family,
        degree,
        expected_total_chunk_count=total_chunk_count,
    )
    chunk_spec_paths = [Path(path) for path in plan.get("chunk_spec_paths", [])]
    if len(chunk_spec_paths) != total_chunk_count:
        raise RuntimeError(
            f"Expected {total_chunk_count} chunk specs for family={family.slug} degree={degree}, found {len(chunk_spec_paths)}."
        )
    chunk_specs_by_index = {
        int(path.name.split(".")[0].split("_")[1]): path
        for path in chunk_spec_paths
    }
    pending_indices = [
        chunk_index
        for chunk_index in range(1, total_chunk_count + 1)
        if not (args.reuse_completed and chunk_index in existing_chunks)
    ]

    pending_chunks: queue.Queue[int] = queue.Queue()
    for chunk_index in pending_indices:
        pending_chunks.put(chunk_index)

    launched_t0 = time.time()
    worker_reports: list[dict[str, Any]] = []
    if pending_indices:
        with ThreadPoolExecutor(max_workers=args.shard_count) as executor:
            futures = [
                executor.submit(
                    _run_dynamic_worker,
                    family,
                    degree,
                    args,
                    chunk_specs_by_index,
                    _worker_dir(campaign_dir, family, degree, worker_index, args.shard_count),
                    _worker_sqlite_path(campaign_dir, family, degree, worker_index, args.shard_count),
                    worker_index,
                    args.shard_count,
                    pending_chunks,
                )
                for worker_index in range(1, args.shard_count + 1)
            ]
            for future in as_completed(futures):
                worker_reports.append(future.result())

    combined_chunks = _collect_existing_chunk_summaries(
        campaign_dir,
        family,
        degree,
        expected_total_chunk_count=total_chunk_count,
    )
    missing = [
        chunk_index
        for chunk_index in range(1, total_chunk_count + 1)
        if chunk_index not in combined_chunks
    ]
    if missing:
        raise RuntimeError(
            f"Dynamic queue did not complete all chunks for family={family.slug} degree={degree}; missing {missing[:10]}"
            + ("..." if len(missing) > 10 else "")
        )

    chunk_summary_paths = [combined_chunks[idx] for idx in sorted(combined_chunks)]
    worker_sqlites = sorted(
        {
            path.parent.parent / "scan.sqlite"
            for path in chunk_summary_paths
            if (path.parent.parent / "scan.sqlite").exists()
        }
    )
    return chunk_summary_paths, worker_sqlites, launched_t0, {
        "plan_summary_path": str(_plan_summary_path(campaign_dir, family, degree)),
        "pending_chunk_count": len(pending_indices),
        "reused_chunk_count": total_chunk_count - len(pending_indices),
        "worker_reports": worker_reports,
    }


def _launch_shards(
    family: CampaignFamily,
    degree: int,
    args: argparse.Namespace,
    campaign_dir: Path,
) -> tuple[list[Path], list[Path], float]:
    spec_path = _base_spec_path(campaign_dir, family, degree)
    shard_summary_paths: list[Path] = []
    shard_sqlite_paths: list[Path] = []
    processes: list[tuple[int, subprocess.Popen[str]]] = []
    launched_t0 = time.time()

    for shard_index in range(1, args.shard_count + 1):
        shard_summary = _shard_summary_path(campaign_dir, family, degree, shard_index, args.shard_count)
        shard_sqlite = _shard_sqlite_path(campaign_dir, family, degree, shard_index, args.shard_count)
        shard_summary_paths.append(shard_summary)
        shard_sqlite_paths.append(shard_sqlite)

        if args.reuse_completed and _summary_is_completed(shard_summary) and shard_sqlite.exists():
            print(f"Reusing completed shard summary {shard_summary}", flush=True)
            continue

        shard_summary.parent.mkdir(parents=True, exist_ok=True)
        env = os.environ.copy()
        if args.julia_threads is not None:
            env["JULIA_NUM_THREADS"] = str(args.julia_threads)
        env["ATLAS_SHARD_COUNT"] = str(args.shard_count)
        env["ATLAS_SHARD_INDEX"] = str(shard_index)
        env["ATLAS_SHARD_MODE"] = args.shard_mode
        if args.persist_shard_sqlite:
            env["ATLAS_SQLITE_PATH"] = str(shard_sqlite)
        env["ATLAS_SOURCE_LABEL"] = f"report_d{degree}_{family.slug}_scan.shard_{shard_index:02d}_of_{args.shard_count:02d}"
        env["ATLAS_SOURCE_METADATA_JSON"] = json.dumps(
            {
                "campaign": "extended_assembly_sharded",
                "campaign_tag": args.campaign_tag,
                "family": family.slug,
                "degree": degree,
                "shard_index": shard_index,
                "shard_count": args.shard_count,
                "shard_mode": args.shard_mode,
            }
        )
        proc = _popen(
            [
                args.julia,
                f"--project={WEBAPP}",
                str(RUN_SCAN_CHUNKED),
                str(spec_path),
                str(shard_summary),
            ],
            env=env,
        )
        processes.append((shard_index, proc))

    failed: list[int] = []
    for shard_index, proc in processes:
        exit_code = proc.wait()
        if exit_code != 0:
            failed.append(shard_index)

    if failed:
        raise RuntimeError(f"Shards failed for family={family.slug} degree={degree}: {failed}")

    return shard_summary_paths, shard_sqlite_paths, launched_t0


def _merge_scan_sqlites(
    family: CampaignFamily,
    degree: int,
    args: argparse.Namespace,
    campaign_dir: Path,
    input_sqlite_paths: list[Path],
) -> dict[str, Any]:
    input_sqlites = [path for path in input_sqlite_paths if path.exists()]
    if not input_sqlites:
        raise RuntimeError(f"No sqlite outputs found for family={family.slug} degree={degree}.")
    output_sqlite = _scan_sqlite_path(campaign_dir, family, degree)
    merge_summary = _scan_merge_summary_path(campaign_dir, family, degree)
    _run(
        [
            args.julia,
            f"--project={WEBAPP}",
            str(MERGE_SQLITE_SHARDS),
            str(output_sqlite),
            *[str(path) for path in input_sqlites],
            str(merge_summary),
        ]
    )
    return _load_json(merge_summary)


def _aggregate_scan_summary(
    family: CampaignFamily,
    degree: int,
    args: argparse.Namespace,
    campaign_dir: Path,
    component_summary_paths: list[Path],
    merge_summary: dict[str, Any],
    launched_t0: float,
    launch_metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    component_summaries = [_load_json(path) for path in component_summary_paths if path.exists()]
    if not component_summaries:
        raise RuntimeError(f"No component summaries found for family={family.slug} degree={degree}.")
    base = component_summaries[0]
    chunks = []
    for summary in component_summaries:
        chunks.extend(summary.get("chunks", []))
    chunks.sort(key=lambda chunk: int(chunk.get("chunk_index", 0)))

    elapsed_values = [float(chunk.get("elapsed_seconds", 0.0)) for chunk in chunks]
    median = 0.0
    if elapsed_values:
        sorted_elapsed = sorted(elapsed_values)
        mid = len(sorted_elapsed) // 2
        if len(sorted_elapsed) % 2 == 0:
            median = (sorted_elapsed[mid - 1] + sorted_elapsed[mid]) / 2.0
        else:
            median = sorted_elapsed[mid]
    delta_totals = {
        "successful_network_count": sum(int(chunk.get("atlas_summary", {}).get("successful_network_count", 0)) for chunk in chunks),
        "behavior_slice_count": sum(int(chunk.get("atlas_summary", {}).get("behavior_slice_count", 0)) for chunk in chunks),
        "family_bucket_count": sum(int(chunk.get("atlas_summary", {}).get("family_bucket_count", 0)) for chunk in chunks),
        "input_graph_slice_count": sum(int(chunk.get("atlas_summary", {}).get("input_graph_slice_count", 0)) for chunk in chunks),
        "regime_record_count": sum(int(chunk.get("atlas_summary", {}).get("regime_record_count", 0)) for chunk in chunks),
        "transition_record_count": sum(int(chunk.get("atlas_summary", {}).get("transition_record_count", 0)) for chunk in chunks),
        "path_record_count": sum(int(chunk.get("atlas_summary", {}).get("path_record_count", 0)) for chunk in chunks),
    }

    summary = {
        "status": "completed",
        "summary_kind": "dynamic_chunk_queue_scan" if args.scheduler == "dynamic" else "sharded_chunk_scan",
        "spec_path": str(_base_spec_path(campaign_dir, family, degree)),
        "summary_path": str(_scan_summary_path(campaign_dir, family, degree)),
        "started_at": min(str(item.get("started_at", "")) for item in component_summaries),
        "finished_at": merge_summary.get("finished_at"),
        "updated_at": merge_summary.get("finished_at"),
        "elapsed_seconds": time.time() - launched_t0,
        "family": family.slug,
        "degree": degree,
        "scheduler": args.scheduler,
        "shard_count": args.shard_count,
        "shard_mode": args.shard_mode,
        "julia_threads_per_shard": args.julia_threads,
        "network_parallelism_requested": _default_network_parallelism(family, args.network_parallelism),
        "enumeration": base.get("enumeration"),
        "total_network_count": base.get("total_network_count"),
        "total_chunk_count": base.get("total_chunk_count"),
        "completed_chunk_count": len(chunks),
        "assigned_chunk_count": len(chunks),
        "assigned_chunk_indices": [int(chunk.get("chunk_index", 0)) for chunk in chunks],
        "chunk_size": base.get("chunk_size"),
        "chunk_elapsed_seconds": {
            "sum": sum(elapsed_values),
            "mean": (sum(elapsed_values) / len(elapsed_values)) if elapsed_values else 0.0,
            "median": median,
            "min": min(elapsed_values) if elapsed_values else 0.0,
            "max": max(elapsed_values) if elapsed_values else 0.0,
        },
        "delta_totals": delta_totals,
        "chunks": chunks,
        "component_summary_paths": [str(path) for path in component_summary_paths],
        "merged_scan_sqlite_path": merge_summary.get("output_sqlite_path"),
        "sqlite_library_summary": merge_summary.get("output_summary"),
    }
    if launch_metadata:
        summary["launch_metadata"] = launch_metadata
    _write_json(_scan_summary_path(campaign_dir, family, degree), summary)
    return summary


def _merge_campaign_sqlite(args: argparse.Namespace, campaign_dir: Path, scan_sqlites: list[Path], campaign_sqlite: Path) -> dict[str, Any] | None:
    if not args.merge_campaign_sqlite or not scan_sqlites:
        return None
    merge_summary_path = campaign_dir / "campaign.merge.summary.json"
    _run(
        [
            args.julia,
            f"--project={WEBAPP}",
            str(MERGE_SQLITE_SHARDS),
            str(campaign_sqlite),
            *[str(path) for path in scan_sqlites],
            str(merge_summary_path),
        ]
    )
    return _load_json(merge_summary_path)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run the extended assembly atlas campaign with either static sharding "
            "or a dynamic chunk queue, writing isolated sqlite shards before merging."
        )
    )
    parser.add_argument("--campaign-tag", default="default", help="Subdirectory name under the output root.")
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT), help="Root directory for sharded campaign outputs.")
    parser.add_argument("--sqlite-path", default=None, help="Optional final merged campaign sqlite path.")
    parser.add_argument("--families", type=_parse_families, default=list(FAMILIES.values()), help="Comma-separated family list: complex_growth,homomer4plus.")
    parser.add_argument("--degrees", type=_parse_degrees, default=list(DEFAULT_DEGREES), help="Comma-separated degree list/ranges, e.g. 2-8 or 5,6-8.")
    parser.add_argument("--julia", default=_detect_julia(), help="Julia executable to use.")
    parser.add_argument("--julia-threads", type=int, default=None, help="JULIA_NUM_THREADS for each shard process.")
    parser.add_argument("--scheduler", choices=("dynamic", "sharded"), default="dynamic", help="Execution mode: dynamic queue or static shard assignment.")
    parser.add_argument("--shard-count", type=int, default=1, help="Number of worker processes per family/degree scan; in sharded mode this is the shard count.")
    parser.add_argument("--shard-mode", choices=("stride", "block"), default="stride", help="Chunk assignment mode across shards.")
    parser.add_argument("--network-parallelism", type=int, default=None, help="Override per-shard network_parallelism.")
    parser.add_argument("--chunk-size", type=int, default=None, help="Override per-shard chunk size.")
    parser.add_argument("--persist-shard-sqlite", action=argparse.BooleanOptionalAction, default=True, help="Write each shard into its own sqlite.")
    parser.add_argument("--merge-campaign-sqlite", action=argparse.BooleanOptionalAction, default=True, help="Merge per-scan sqlite libraries into one campaign sqlite.")
    parser.add_argument("--reuse-completed", action=argparse.BooleanOptionalAction, default=True, help="Reuse completed shard summaries when shard sqlite already exists.")
    parser.add_argument("--generate-only", action="store_true", help="Write specs/settings only, without launching shards.")
    return parser


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()
    if args.shard_count < 1:
        raise ValueError("--shard-count must be at least 1.")
    if args.julia_threads is not None and args.julia_threads < 1:
        raise ValueError("--julia-threads must be at least 1.")
    if args.chunk_size is not None and args.chunk_size < 1:
        raise ValueError("--chunk-size must be at least 1.")
    if not args.persist_shard_sqlite and not args.generate_only:
        raise ValueError("Sharded execution currently requires --persist-shard-sqlite.")
    if not RUN_SCAN_CHUNKED.exists():
        raise FileNotFoundError(f"Missing chunked scan runner: {RUN_SCAN_CHUNKED}")
    if not MERGE_SQLITE_SHARDS.exists():
        raise FileNotFoundError(f"Missing sqlite shard merge script: {MERGE_SQLITE_SHARDS}")

    campaign_dir = _campaign_dir(args)
    campaign_sqlite = _campaign_sqlite_path(args, campaign_dir)
    settings = _campaign_settings(args)
    settings_path = campaign_dir / "campaign_settings.json"
    campaign_dir.mkdir(parents=True, exist_ok=True)
    _write_json_if_changed(settings_path, settings)
    spec_paths = _prepare_specs(args, campaign_dir)

    if args.generate_only:
        _write_json(campaign_dir / "campaign_summary.json", {
            "campaign_dir": str(campaign_dir),
            "spec_paths": [str(path) for path in spec_paths],
            "generate_only": True,
        })
        print(json.dumps({"campaign_dir": str(campaign_dir), "spec_paths": [str(path) for path in spec_paths]}, indent=2), flush=True)
        return

    scan_results: list[dict[str, Any]] = []
    scan_sqlites: list[Path] = []

    for family in args.families:
        for degree in args.degrees:
            if args.scheduler == "dynamic":
                component_summary_paths, sqlite_paths, launched_t0, launch_metadata = _launch_dynamic_queue(
                    family, degree, args, campaign_dir
                )
            else:
                component_summary_paths, sqlite_paths, launched_t0 = _launch_shards(
                    family, degree, args, campaign_dir
                )
                launch_metadata = None
            merge_summary = _merge_scan_sqlites(
                family, degree, args, campaign_dir, sqlite_paths
            )
            scan_summary = _aggregate_scan_summary(
                family,
                degree,
                args,
                campaign_dir,
                component_summary_paths,
                merge_summary,
                launched_t0,
                launch_metadata=launch_metadata,
            )
            scan_results.append(
                {
                    "family": family.slug,
                    "degree": degree,
                    "scheduler": args.scheduler,
                    "summary_path": str(_scan_summary_path(campaign_dir, family, degree)),
                    "merged_scan_sqlite_path": scan_summary["merged_scan_sqlite_path"],
                    "behavior_slice_count": scan_summary["delta_totals"]["behavior_slice_count"],
                    "family_bucket_count": scan_summary["delta_totals"]["family_bucket_count"],
                    "successful_network_count": scan_summary["delta_totals"]["successful_network_count"],
                    "elapsed_seconds": scan_summary["elapsed_seconds"],
                }
            )
            scan_sqlites.append(_scan_sqlite_path(campaign_dir, family, degree))

    campaign_merge = _merge_campaign_sqlite(args, campaign_dir, scan_sqlites, campaign_sqlite)

    campaign_summary = {
        "campaign_dir": str(campaign_dir),
        "campaign_tag": args.campaign_tag,
        "campaign_settings_path": str(settings_path),
        "campaign_settings": settings,
        "generated_spec_paths": [str(path) for path in spec_paths],
        "scan_results": scan_results,
        "campaign_sqlite_path": str(campaign_sqlite) if args.merge_campaign_sqlite else None,
        "campaign_merge_summary_path": str(campaign_dir / "campaign.merge.summary.json") if campaign_merge is not None else None,
        "campaign_sqlite_summary": None if campaign_merge is None else campaign_merge.get("output_summary"),
    }
    _write_json(campaign_dir / "campaign_summary.json", campaign_summary)
    print(json.dumps(campaign_summary, indent=2), flush=True)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:  # pragma: no cover
        print(f"ERROR: {exc}", file=sys.stderr)
        raise
