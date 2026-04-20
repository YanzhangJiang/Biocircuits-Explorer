#!/usr/bin/env python3

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
ATLAS_STORE = REPO_ROOT / "webapp" / "atlas_store"
ATLAS_SPECS_DIR = ATLAS_STORE / "specs"
ATLAS_SUMMARIES_DIR = ATLAS_STORE / "summaries"
ATLAS_BENCHMARKS_DIR = ATLAS_STORE / "benchmarks"
ATLAS_LIBRARIES_DIR = ATLAS_STORE / "libraries"
RUN_SCAN = REPO_ROOT / "webapp" / "scripts" / "run_atlas_scan.jl"
RUN_SCAN_CHUNKED = REPO_ROOT / "webapp" / "scripts" / "run_atlas_scan_chunked.jl"
CAMPAIGN_SQLITE = ATLAS_LIBRARIES_DIR / "atlas_homomer_campaign.sqlite"
SCAN_SPECS = [
    ATLAS_SPECS_DIR / "report_d2_homomer_scan.json",
    ATLAS_SPECS_DIR / "report_d3_homomer_scan.json",
    ATLAS_SPECS_DIR / "report_d4_homomer_scan.json",
    ATLAS_SPECS_DIR / "report_d5_homomer_scan.json",
    ATLAS_SPECS_DIR / "report_d6_homomer_scan.json",
    ATLAS_SPECS_DIR / "report_d7_homomer_scan.json",
    ATLAS_SPECS_DIR / "report_d8_homomer_scan.json",
]
BENCHMARK_PARALLELISM = [1, 3, 6]
BENCHMARK_LIMIT = 200


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
    print(f"Initializing empty campaign sqlite placeholder at {CAMPAIGN_SQLITE}", flush=True)
    CAMPAIGN_SQLITE.touch()


def _run_benchmark() -> list[dict]:
    benchmark_results: list[dict] = []
    base_spec = _load_json(ATLAS_SPECS_DIR / "report_d5_homomer_scan.json")
    base_spec["skip_existing"] = False
    base_spec["persist_sqlite"] = False
    base_spec.pop("sqlite_path", None)
    base_spec["source_label"] = "benchmark_d5_homomer_sample"
    base_spec["enumeration"]["limit"] = BENCHMARK_LIMIT

    for parallelism in BENCHMARK_PARALLELISM:
        spec = json.loads(json.dumps(base_spec))
        spec["network_parallelism"] = parallelism
        spec_path = ATLAS_BENCHMARKS_DIR / f"benchmark_d5_homomer_sample_np{parallelism}.json"
        summary_path = ATLAS_BENCHMARKS_DIR / f"benchmark_d5_homomer_sample_np{parallelism}.summary.json"
        _write_json(spec_path, spec)
        _run([
            "julia",
            "--project=/Users/yanzhang/git/Biocircuits-Explorer/webapp",
            str(RUN_SCAN),
            str(spec_path),
            str(summary_path),
        ])
        summary = _load_json(summary_path)
        benchmark_results.append({
            "parallelism": parallelism,
            "summary_path": str(summary_path),
            "elapsed_seconds": summary["elapsed_seconds"],
            "generated_network_count": summary["enumeration"]["generated_network_count"],
            "successful_network_count": summary["atlas_summary"]["successful_network_count"],
            "behavior_slice_count": summary["atlas_summary"]["behavior_slice_count"],
        })

    return benchmark_results


def _run_scans() -> list[dict]:
    scan_results: list[dict] = []
    for spec_path in SCAN_SPECS:
        summary_path = ATLAS_SUMMARIES_DIR / spec_path.name.replace(".json", ".final.summary.json")
        _run([
            "julia",
            "--project=/Users/yanzhang/git/Biocircuits-Explorer/webapp",
            str(RUN_SCAN_CHUNKED),
            str(spec_path),
            str(summary_path),
        ])
        summary = _load_json(summary_path)
        delta = summary.get("delta_totals", {})
        if not delta:
            delta = {
                "successful_network_count": sum(
                    chunk.get("atlas_summary", {}).get("successful_network_count", 0)
                    for chunk in summary.get("chunks", [])
                ),
                "behavior_slice_count": sum(
                    chunk.get("atlas_summary", {}).get("behavior_slice_count", 0)
                    for chunk in summary.get("chunks", [])
                ),
                "family_bucket_count": sum(
                    chunk.get("atlas_summary", {}).get("family_bucket_count", 0)
                    for chunk in summary.get("chunks", [])
                ),
            }
        scan_results.append({
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
    _ensure_campaign_sqlite()
    benchmark = _run_benchmark()
    scans = _run_scans()
    campaign_summary = {
        "campaign_sqlite": str(CAMPAIGN_SQLITE),
        "benchmark_limit": BENCHMARK_LIMIT,
        "benchmark_results": benchmark,
        "scan_results": scans,
    }
    summary_path = ATLAS_SUMMARIES_DIR / "homomer_campaign_summary.json"
    _write_json(summary_path, campaign_summary)
    print(json.dumps(campaign_summary, indent=2), flush=True)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:  # pragma: no cover
        print(f"ERROR: {exc}", file=sys.stderr)
        raise
