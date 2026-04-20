#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
WEBAPP = REPO_ROOT / "webapp"
ATLAS_STORE = WEBAPP / "atlas_store"
ATLAS_SPECS_DIR = ATLAS_STORE / "specs"
ATLAS_SUMMARIES_DIR = ATLAS_STORE / "summaries"
ATLAS_BENCHMARKS_DIR = ATLAS_STORE / "benchmarks"
ATLAS_LIBRARIES_DIR = ATLAS_STORE / "libraries"
DOC_DIR = REPO_ROOT / "doc"
RUN_SCAN = WEBAPP / "scripts" / "run_atlas_scan.jl"
RUN_SCAN_CHUNKED = WEBAPP / "scripts" / "run_atlas_scan_chunked.jl"
CAMPAIGN_SQLITE = ATLAS_LIBRARIES_DIR / "atlas_extended_assembly_campaign.sqlite"
JULIA_BIN = Path("/Users/yanzhang/.julia/juliaup/julia-1.12.5+0.aarch64.apple.darwin14/Julia-1.12.app/Contents/Resources/julia/bin/julia")
DEGREES = list(range(2, 9))
BENCHMARK_DEGREE = 5
BENCHMARK_LIMIT = 150
BENCHMARK_PARALLELISM = [1, 3, 6, 9]


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


FAMILIES = [
    CampaignFamily(
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
    CampaignFamily(
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
    ),
]


def _run(cmd: list[str]) -> None:
    print("RUN", " ".join(cmd), flush=True)
    subprocess.run(cmd, cwd=REPO_ROOT, check=True)


def _load_json(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as fh:
        return json.load(fh)


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2)
        fh.write("\n")


def _ensure_campaign_sqlite() -> None:
    if CAMPAIGN_SQLITE.exists():
        return
    CAMPAIGN_SQLITE.parent.mkdir(parents=True, exist_ok=True)
    print(f"Initializing sqlite placeholder at {CAMPAIGN_SQLITE}", flush=True)
    CAMPAIGN_SQLITE.touch()


def _spec_path(family: CampaignFamily, degree: int) -> Path:
    return ATLAS_SPECS_DIR / f"report_d{degree}_{family.slug}_scan.json"


def _summary_path(family: CampaignFamily, degree: int) -> Path:
    return ATLAS_SUMMARIES_DIR / f"report_d{degree}_{family.slug}_scan.final.summary.json"


def _build_spec(family: CampaignFamily, degree: int) -> dict:
    return {
        "source_label": f"report_d{degree}_{family.slug}_scan",
        "chunk_size": family.chunk_size,
        "network_parallelism": family.network_parallelism,
        "skip_existing": True,
        "persist_sqlite": True,
        "sqlite_path": str(CAMPAIGN_SQLITE),
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


def _write_specs() -> list[Path]:
    spec_paths: list[Path] = []
    for family in FAMILIES:
        for degree in DEGREES:
            path = _spec_path(family, degree)
            _write_json(path, _build_spec(family, degree))
            spec_paths.append(path)
    return spec_paths


def _run_benchmark() -> list[dict]:
    benchmark_results: list[dict] = []
    for family in FAMILIES:
        base_spec = _build_spec(family, BENCHMARK_DEGREE)
        base_spec["skip_existing"] = False
        base_spec["persist_sqlite"] = False
        base_spec.pop("sqlite_path", None)
        base_spec["source_label"] = f"benchmark_d{BENCHMARK_DEGREE}_{family.slug}_sample"
        base_spec["enumeration"]["limit"] = BENCHMARK_LIMIT

        for parallelism in BENCHMARK_PARALLELISM:
            spec = json.loads(json.dumps(base_spec))
            spec["network_parallelism"] = parallelism
            spec_path = ATLAS_BENCHMARKS_DIR / f"benchmark_d{BENCHMARK_DEGREE}_{family.slug}_sample_np{parallelism}.json"
            summary_path = ATLAS_BENCHMARKS_DIR / f"benchmark_d{BENCHMARK_DEGREE}_{family.slug}_sample_np{parallelism}.summary.json"
            _write_json(spec_path, spec)
            _run([
                str(JULIA_BIN),
                f"--project={WEBAPP}",
                str(RUN_SCAN),
                str(spec_path),
                str(summary_path),
            ])
            summary = _load_json(summary_path)
            benchmark_results.append({
                "family": family.slug,
                "parallelism": parallelism,
                "summary_path": str(summary_path),
                "elapsed_seconds": summary["elapsed_seconds"],
                "generated_network_count": summary["enumeration"]["generated_network_count"],
                "successful_network_count": summary["atlas_summary"]["successful_network_count"],
                "behavior_slice_count": summary["atlas_summary"]["behavior_slice_count"],
            })
    return benchmark_results


def _delta_totals(summary: dict) -> dict:
    delta = summary.get("delta_totals", {})
    if delta:
        return delta
    totals = {
        "successful_network_count": 0,
        "behavior_slice_count": 0,
        "family_bucket_count": 0,
    }
    for chunk in summary.get("chunks", []):
        atlas_summary = chunk.get("atlas_summary", {})
        for key in totals:
            totals[key] += int(atlas_summary.get(key, 0))
    return totals


def _run_scans() -> list[dict]:
    scan_results: list[dict] = []
    for family in FAMILIES:
        for degree in DEGREES:
            spec_path = _spec_path(family, degree)
            summary_path = _summary_path(family, degree)
            _run([
                str(JULIA_BIN),
                f"--project={WEBAPP}",
                str(RUN_SCAN_CHUNKED),
                str(spec_path),
                str(summary_path),
            ])
            summary = _load_json(summary_path)
            delta = _delta_totals(summary)
            scan_results.append({
                "family": family.slug,
                "degree": degree,
                "spec_path": str(spec_path),
                "summary_path": str(summary_path),
                "elapsed_seconds": summary["elapsed_seconds"],
                "generated_network_count": summary["enumeration"]["generated_network_count"],
                "successful_network_count": delta["successful_network_count"],
                "behavior_slice_count": delta["behavior_slice_count"],
                "family_bucket_count": delta["family_bucket_count"],
            })
    return scan_results


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the extended assembly atlas campaign.")
    parser.add_argument("--write-specs-only", action="store_true", help="Only materialize scan spec JSON files.")
    parser.add_argument("--skip-benchmark", action="store_true", help="Skip the benchmark phase.")
    parser.add_argument("--skip-scans", action="store_true", help="Skip the chunked scan phase.")
    args = parser.parse_args()

    _ensure_campaign_sqlite()
    spec_paths = _write_specs()

    if args.write_specs_only:
        print(json.dumps({"spec_paths": [str(path) for path in spec_paths]}, indent=2), flush=True)
        return

    benchmark = [] if args.skip_benchmark else _run_benchmark()
    scans = [] if args.skip_scans else _run_scans()
    campaign_summary = {
        "campaign_sqlite": str(CAMPAIGN_SQLITE),
        "spec_paths": [str(path) for path in spec_paths],
        "benchmark_limit": BENCHMARK_LIMIT,
        "benchmark_results": benchmark,
        "scan_results": scans,
    }
    summary_path = ATLAS_SUMMARIES_DIR / "extended_assembly_campaign_summary.json"
    _write_json(summary_path, campaign_summary)
    print(json.dumps(campaign_summary, indent=2), flush=True)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:  # pragma: no cover
        print(f"ERROR: {exc}", file=sys.stderr)
        raise
