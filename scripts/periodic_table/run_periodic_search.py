#!/usr/bin/env python3
"""Run bounded ROP property search and write conclusion-only table outputs."""

from __future__ import annotations

import argparse
import csv
import gzip
import json
import os
import shutil
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

from src.periodic_table.candidate_search import CandidateNetwork, generate_candidate_networks
from src.periodic_table.complete_definition import (
    PROPERTY_IDS,
    STATUS_NO_COMPLETE,
    STATUS_UNKNOWN,
    STATUS_WITNESS_ONLY,
    code_commit,
    profile_hash,
)
from src.periodic_table.dry_run import SUMMARY_NAMES, _strength_template, normalize_properties
from src.periodic_table.result_schema import (
    append_event,
    cell_result,
    run_config,
    trivial_certificate,
    utc_now,
    write_json,
    write_jsonl,
)
from src.periodic_table.witness_codec import make_certificate_id, make_witness_id


SUPPORTED_WITNESS_PROPERTIES = {
    "sign_switch.v1",
    "ultrasensitivity.v1",
    "settle_to_zero.v1",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--d-max", type=int, default=4)
    parser.add_argument("--mu-max", type=int, default=5)
    parser.add_argument("--d-min", type=int, default=1)
    parser.add_argument("--mu-min", type=int, default=1)
    parser.add_argument("--properties", default="all")
    parser.add_argument("--profile", default="periodic_d_mu_v0")
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--output-dir", default=None)
    parser.add_argument("--candidate-limit-per-cell", type=int, default=600)
    parser.add_argument("--pair-limit-per-cell", type=int, default=250)
    parser.add_argument("--max-reactions", type=int, default=3)
    parser.add_argument("--batch-size", type=int, default=40)
    parser.add_argument("--network-parallelism", type=int, default=4)
    parser.add_argument("--batch-timeout-sec", type=int, default=1800)
    parser.add_argument("--julia", default=os.environ.get("JULIA", "julia"))
    parser.add_argument("--keep-batch-json", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.profile != "periodic_d_mu_v0":
        raise SystemExit("only --profile periodic_d_mu_v0 is supported")
    if args.d_min < 1 or args.mu_min < 1 or args.d_max < args.d_min or args.mu_max < args.mu_min:
        raise SystemExit("invalid d/mu range")

    run_id = args.run_id or datetime.now(timezone.utc).strftime("periodic_search_%Y%m%dT%H%M%SZ")
    run_dir = Path(args.output_dir) if args.output_dir else REPO_ROOT / "results" / "periodic_table" / "runs" / run_id
    property_ids = normalize_properties(args.properties)
    d_values = list(range(args.d_min, args.d_max + 1))
    mu_values = list(range(args.mu_min, args.mu_max + 1))

    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "logs").mkdir(exist_ok=True)
    (run_dir / "checkpoints").mkdir(exist_ok=True)
    (run_dir / "heartbeat").mkdir(exist_ok=True)
    (run_dir / "summary").mkdir(exist_ok=True)
    scratch_dir = run_dir / "scratch_batches"
    scratch_dir.mkdir(exist_ok=True)

    config = run_config(
        run_id=run_id,
        d_values=d_values,
        mu_values=mu_values,
        property_ids=property_ids,
        repo_root=REPO_ROOT,
        dry_run=False,
    )
    config.update(
        {
            "search_kind": "bounded_property_mode",
            "candidate_limit_per_cell": args.candidate_limit_per_cell,
            "pair_limit_per_cell": args.pair_limit_per_cell,
            "max_reactions": args.max_reactions,
            "batch_size": args.batch_size,
            "network_parallelism": args.network_parallelism,
            "batch_timeout_sec": args.batch_timeout_sec,
            "julia": args.julia,
            "supported_witness_properties": sorted(SUPPORTED_WITNESS_PROPERTIES),
            "unsupported_properties_marked_unknown": sorted(set(PROPERTY_IDS) - SUPPORTED_WITNESS_PROPERTIES),
        }
    )
    write_json(run_dir / "config.json", config)
    _write_heartbeat(run_dir, "running", {"stage": "initializing"})
    append_event(run_dir / "logs" / "events.jsonl", {
        "run_id": run_id,
        "agent_id": "codex-main",
        "stage": "search_start",
        "status": "running",
        "message": "bounded periodic-table property search started",
        "config": {k: config[k] for k in ("d_values", "mu_values", "property_ids", "candidate_limit_per_cell", "max_reactions")},
    })

    all_cells: list[dict[str, Any]] = []
    all_witnesses: list[dict[str, Any]] = []
    all_certificates: list[dict[str, Any]] = []
    cell_summaries: list[dict[str, Any]] = []
    batch_outputs: list[Path] = []
    started = time.time()

    for d in d_values:
        for mu in mu_values:
            _write_heartbeat(run_dir, "running", {"stage": "cell", "d": d, "mu": mu})
            append_event(run_dir / "logs" / "events.jsonl", {
                "run_id": run_id,
                "agent_id": "codex-main",
                "stage": "cell_start",
                "status": "running",
                "d": d,
                "mu": mu,
            })
            if mu < 2:
                cells, certs = _trivial_cell_records(run_id, d, mu, property_ids)
                all_cells.extend(cells)
                all_certificates.extend(certs)
                cell_summaries.append({"d": d, "mu": mu, "candidate_count": 0, "batch_count": 0, "status": "trivial_complete"})
                _write_checkpoint(run_dir, all_cells, all_witnesses, all_certificates, cell_summaries)
                continue

            candidates = generate_candidate_networks(
                d,
                mu,
                max_reactions=args.max_reactions,
                limit=args.candidate_limit_per_cell,
                pair_limit=args.pair_limit_per_cell,
            )
            cell_best: dict[str, dict[str, Any]] = {}
            batch_count = 0
            errors: list[dict[str, Any]] = []
            for batch_idx, batch in enumerate(_chunks(candidates, args.batch_size), start=1):
                batch_count += 1
                batch_json = scratch_dir / f"d{d}_mu{mu}_batch{batch_idx:04d}_candidates.json"
                batch_out = scratch_dir / f"d{d}_mu{mu}_batch{batch_idx:04d}_result.json"
                _write_batch_json(batch_json, run_id, d, mu, batch)
                command = [
                    args.julia,
                    "--project=webapp",
                    str(REPO_ROOT / "scripts" / "periodic_table" / "run_julia_property_batch.jl"),
                    "--run-id",
                    run_id,
                    "--d",
                    str(d),
                    "--mu",
                    str(mu),
                    "--candidates-json",
                    str(batch_json),
                    "--output-json",
                    str(batch_out),
                    "--network-parallelism",
                    str(args.network_parallelism),
                ]
                _write_heartbeat(run_dir, "running", {"stage": "batch", "d": d, "mu": mu, "batch": batch_idx, "candidates": len(batch)})
                rc = _run_command(command, run_dir / "logs" / "julia_batches.log", timeout_sec=args.batch_timeout_sec)
                if rc != 0:
                    errors.append({"batch": batch_idx, "returncode": rc, "output": str(batch_out)})
                    append_event(run_dir / "logs" / "events.jsonl", {
                        "run_id": run_id,
                        "agent_id": "codex-main",
                        "stage": "batch_error",
                        "status": "error",
                        "d": d,
                        "mu": mu,
                        "batch": batch_idx,
                        "returncode": rc,
                    })
                    if batch_out.exists():
                        _merge_batch_hits(batch_out, cell_best)
                    continue
                _merge_batch_hits(batch_out, cell_best)
                batch_outputs.append(batch_out)
                if not args.keep_batch_json:
                    batch_json.unlink(missing_ok=True)

            cells, witnesses = _cell_records_from_best(
                run_id=run_id,
                d=d,
                mu=mu,
                property_ids=property_ids,
                best=cell_best,
                candidate_count=len(candidates),
                batch_count=batch_count,
                errors=errors,
            )
            all_cells.extend(cells)
            all_witnesses.extend(witnesses)
            cell_summaries.append(
                {
                    "d": d,
                    "mu": mu,
                    "candidate_count": len(candidates),
                    "batch_count": batch_count,
                    "witness_properties": sorted(cell_best),
                    "error_count": len(errors),
                    "status": "searched",
                }
            )
            _write_checkpoint(run_dir, all_cells, all_witnesses, all_certificates, cell_summaries)
            append_event(run_dir / "logs" / "events.jsonl", {
                "run_id": run_id,
                "agent_id": "codex-main",
                "stage": "cell_done",
                "status": "completed",
                "d": d,
                "mu": mu,
                "candidate_count": len(candidates),
                "witness_properties": sorted(cell_best),
                "error_count": len(errors),
            })

    write_jsonl(run_dir / "cell_results.jsonl.gz", all_cells)
    write_jsonl(run_dir / "witnesses.jsonl.gz", all_witnesses)
    write_jsonl(run_dir / "certificates.jsonl.gz", all_certificates)
    write_jsonl(run_dir / "robustness_estimates.jsonl.gz", [])
    _write_summary_tables(run_dir / "summary", config, all_cells, all_witnesses, cell_summaries, runtime_sec=time.time() - started)
    _write_heartbeat(run_dir, "completed", {"stage": "done"})
    append_event(run_dir / "logs" / "events.jsonl", {
        "run_id": run_id,
        "agent_id": "codex-main",
        "stage": "search_done",
        "status": "completed",
        "message": "bounded periodic-table property search completed",
        "counts": {
            "cell_results": len(all_cells),
            "witnesses": len(all_witnesses),
            "certificates": len(all_certificates),
            "runtime_sec": round(time.time() - started, 3),
        },
    })
    latest = REPO_ROOT / "results" / "periodic_table" / "latest"
    try:
        if latest.exists() or latest.is_symlink():
            latest.unlink()
        latest.symlink_to(run_dir)
    except OSError:
        pass
    if not args.keep_batch_json:
        _compress_batch_outputs(batch_outputs)
    print(run_dir)
    return 0


def _chunks(values: list[CandidateNetwork], size: int) -> list[list[CandidateNetwork]]:
    return [values[idx : idx + size] for idx in range(0, len(values), max(1, size))]


def _write_batch_json(path: Path, run_id: str, d: int, mu: int, batch: list[CandidateNetwork]) -> None:
    payload = {
        "run_id": run_id,
        "d": d,
        "mu": mu,
        "candidates": [candidate.to_julia_spec() for candidate in batch],
    }
    path.write_text(json.dumps(payload, sort_keys=True, ensure_ascii=True) + "\n", encoding="utf-8")


def _run_command(command: list[str], log_path: Path, *, timeout_sec: int) -> int:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("a", encoding="utf-8") as log:
        log.write(f"\n$ {' '.join(command)}\n")
        log.flush()
        try:
            proc = subprocess.run(
                command,
                cwd=REPO_ROOT,
                stdout=log,
                stderr=subprocess.STDOUT,
                text=True,
                timeout=timeout_sec,
            )
            log.write(f"[returncode {proc.returncode}]\n")
            return proc.returncode
        except subprocess.TimeoutExpired:
            log.write(f"[timeout after {timeout_sec} sec]\n")
            return 124


def _merge_batch_hits(batch_out: Path, cell_best: dict[str, dict[str, Any]]) -> None:
    if not batch_out.exists():
        return
    payload = json.loads(batch_out.read_text(encoding="utf-8"))
    for hit in payload.get("hits", []):
        property_id = hit.get("property_id")
        if not property_id:
            continue
        current = cell_best.get(property_id)
        if current is None or _witness_sort_key(hit) < _witness_sort_key(current):
            cell_best[property_id] = hit


def _witness_sort_key(hit: dict[str, Any]) -> tuple[Any, ...]:
    features = hit.get("source_metadata", {}).get("features", {})
    return (
        features.get("reaction_count", 999999),
        features.get("assembly_depth", 999999),
        features.get("complex_count", 999999),
        hit.get("representative_path_length", 999999) or 999999,
        hit.get("network_canonical", ""),
        hit.get("program_label", ""),
    )


def _trivial_cell_records(run_id: str, d: int, mu: int, property_ids: list[str]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    cells: list[dict[str, Any]] = []
    certificates: list[dict[str, Any]] = []
    for property_id in property_ids:
        reason = (
            f"No binding reactions exist for mu={mu} under periodic_d_mu_v0; "
            "association products require size at least 2."
        )
        certificate_id = make_certificate_id(property_id, d, mu, {"reason": reason})
        certificates.append(
            trivial_certificate(
                run_id=run_id,
                d=d,
                mu=mu,
                property_id=property_id,
                certificate_id=certificate_id,
                reason=reason,
            )
        )
        cells.append(
            cell_result(
                run_id=run_id,
                d=d,
                mu=mu,
                property_id=property_id,
                status=STATUS_NO_COMPLETE,
                strength=_strength_template(property_id, dry_run=False, mu=mu),
                repo_root=REPO_ROOT,
                certificate_id=certificate_id,
                notes="trivial complete: no legal association product for mu < 2",
            )
        )
    return cells, certificates


def _cell_records_from_best(
    *,
    run_id: str,
    d: int,
    mu: int,
    property_ids: list[str],
    best: dict[str, dict[str, Any]],
    candidate_count: int,
    batch_count: int,
    errors: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    cells: list[dict[str, Any]] = []
    witnesses: list[dict[str, Any]] = []
    witness_by_property: dict[str, str] = {}
    for property_id, hit in sorted(best.items()):
        witness_payload = {
            **hit,
            "d": d,
            "mu": mu,
            "run_id": run_id,
            "profile_hash": profile_hash(),
            "status": STATUS_WITNESS_ONLY,
            "candidate_count_checked": candidate_count,
            "batch_count": batch_count,
            "minimality_statement": "bounded increasing-complexity search only; not an exhaustive minimality certificate",
            "reproduce_command": f"python scripts/periodic_table/reproduce_witness.py --run-dir results/periodic_table/runs/{run_id} --witness-id <witness_id>",
        }
        witness_id = make_witness_id(property_id, d, mu, witness_payload)
        witness_payload["witness_id"] = witness_id
        witness_payload["reproduce_command"] = witness_payload["reproduce_command"].replace("<witness_id>", witness_id)
        witnesses.append(witness_payload)
        witness_by_property[property_id] = witness_id

    for property_id in property_ids:
        if property_id in best:
            hit = best[property_id]
            features = hit.get("source_metadata", {}).get("features", {})
            cells.append(
                cell_result(
                    run_id=run_id,
                    d=d,
                    mu=mu,
                    property_id=property_id,
                    status=STATUS_WITNESS_ONLY,
                    strength=hit.get("strength", {}),
                    repo_root=REPO_ROOT,
                    witness_id=witness_by_property[property_id],
                    min_r=features.get("reaction_count"),
                    min_assembly_depth=features.get("assembly_depth"),
                    notes="witness found by bounded property-mode search; not a negative or minimality certificate",
                )
            )
        else:
            notes = "bounded property-mode search found no witness; no exhaustive negative certificate"
            if property_id not in SUPPORTED_WITNESS_PROPERTIES:
                notes = "property oracle not yet available in backend summary mode; marked UNKNOWN"
            if errors:
                notes += f"; {len(errors)} batch error(s) logged"
            cells.append(
                cell_result(
                    run_id=run_id,
                    d=d,
                    mu=mu,
                    property_id=property_id,
                    status=STATUS_UNKNOWN,
                    strength=_strength_template(property_id, dry_run=False, mu=mu),
                    repo_root=REPO_ROOT,
                    notes=notes,
                )
            )
    return cells, witnesses


def _write_checkpoint(
    run_dir: Path,
    cells: list[dict[str, Any]],
    witnesses: list[dict[str, Any]],
    certificates: list[dict[str, Any]],
    cell_summaries: list[dict[str, Any]],
) -> None:
    write_json(
        run_dir / "checkpoints" / "state.json",
        {
            "updated_at_utc": utc_now(),
            "completed_cell_result_count": len(cells),
            "witness_count": len(witnesses),
            "certificate_count": len(certificates),
            "cell_summaries": cell_summaries,
        },
    )


def _write_heartbeat(run_dir: Path, status: str, task: dict[str, Any]) -> None:
    write_json(
        run_dir / "heartbeat" / "supervisor.json",
        {
            "run_id": run_dir.name,
            "worker_id": "supervisor",
            "pid": os.getpid(),
            "hostname": os.uname().nodename,
            "current_task": task,
            "last_update_utc": utc_now(),
            "rss_gb": None,
            "status": status,
        },
    )


def _write_summary_tables(
    summary_dir: Path,
    config: dict[str, Any],
    cells: list[dict[str, Any]],
    witnesses: list[dict[str, Any]],
    cell_summaries: list[dict[str, Any]],
    *,
    runtime_sec: float,
) -> None:
    fieldnames = [
        "run_id",
        "profile_id",
        "profile_hash",
        "d",
        "mu",
        "property_id",
        "status",
        "min_r",
        "min_assembly_depth",
        "witness_id",
        "certificate_id",
        "notes",
    ]
    for property_id in sorted(set(record["property_id"] for record in cells)):
        name = SUMMARY_NAMES.get(property_id, f"periodic_table_{property_id.replace('.', '_')}.csv")
        _write_csv(summary_dir / name, [record for record in cells if record["property_id"] == property_id], fieldnames)
    _write_csv(summary_dir / "periodic_table_all.csv", cells, fieldnames)
    _write_csv(
        summary_dir / "minimal_witnesses.csv",
        [
            {
                "witness_id": witness.get("witness_id"),
                "property_id": witness.get("property_id"),
                "d": witness.get("d"),
                "mu": witness.get("mu"),
                "reaction_count": witness.get("source_metadata", {}).get("features", {}).get("reaction_count"),
                "assembly_depth": witness.get("source_metadata", {}).get("features", {}).get("assembly_depth"),
                "network_canonical": witness.get("network_canonical"),
            }
            for witness in witnesses
        ],
        ["witness_id", "property_id", "d", "mu", "reaction_count", "assembly_depth", "network_canonical"],
    )
    _write_csv(
        summary_dir / "cell_search_summary.csv",
        cell_summaries,
        ["d", "mu", "candidate_count", "batch_count", "witness_properties", "error_count", "status"],
    )
    status_counts: dict[str, int] = {}
    for record in cells:
        status_counts[record["status"]] = status_counts.get(record["status"], 0) + 1
    write_json(
        summary_dir / "final_summary.json",
        {
            "run_id": config["run_id"],
            "profile_id": config["profile_id"],
            "profile_hash": config["profile_hash"],
            "code_commit": code_commit(REPO_ROOT),
            "cell_result_count": len(cells),
            "witness_count": len(witnesses),
            "status_counts": status_counts,
            "runtime_sec": round(runtime_sec, 3),
            "search_kind": config["search_kind"],
        },
    )


def _write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _compress_batch_outputs(paths: list[Path]) -> None:
    for path in paths:
        if not path.exists():
            continue
        gz_path = path.with_suffix(path.suffix + ".gz")
        with path.open("rb") as src, gzip.open(gz_path, "wb") as dst:
            shutil.copyfileobj(src, dst)
        path.unlink()


if __name__ == "__main__":
    raise SystemExit(main())
