"""Dry-run dataset generation for Tier 0."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any

from .complete_definition import PROPERTY_IDS, STATUS_NO_COMPLETE, STATUS_UNKNOWN
from .result_schema import (
    append_event,
    cell_result,
    dry_status_for_cell,
    trivial_certificate,
    utc_now,
    write_json,
    write_jsonl,
)
from .witness_codec import make_certificate_id

SUMMARY_NAMES = {
    "sign_switch.v1": "periodic_table_sign_switch.csv",
    "ultrasensitivity.v1": "periodic_table_ultrasensitivity.csv",
    "upper_bound_reachable.v1": "periodic_table_ubr.csv",
    "mimo_gain.v1": "periodic_table_mimo.csv",
}


def normalize_properties(raw: str | list[str] | tuple[str, ...] | None) -> list[str]:
    if raw is None or raw == "" or raw == "all":
        return list(PROPERTY_IDS)
    if isinstance(raw, str):
        values = [part.strip() for part in raw.split(",") if part.strip()]
    else:
        values = [str(part).strip() for part in raw if str(part).strip()]
    unknown = sorted(set(values) - set(PROPERTY_IDS))
    if unknown:
        raise ValueError(f"unknown property ids: {', '.join(unknown)}")
    return values


def _strength_template(property_id: str, *, dry_run: bool, mu: int) -> dict[str, Any]:
    if property_id == "sign_switch.v1":
        return {
            "max_sign_switch_count": 0,
            "transition_types": [],
            "three_state": False,
            "dry_run": dry_run,
        }
    if property_id == "ultrasensitivity.v1":
        return {
            "definition": "finite_RO_greater_than_mu",
            "mu": mu,
            "max_finite_ro": None,
            "max_abs_finite_ro": None,
            "dry_run": dry_run,
        }
    if property_id == "mimo_gain.v1":
        return {
            "max_rank_G": None,
            "max_spectral_norm": None,
            "max_abs_entry": None,
            "sign_pattern_matrices_observed": [],
            "dry_run": dry_run,
        }
    return {"dry_run": dry_run}


def build_dry_run_records(
    *,
    run_id: str,
    d_values: list[int],
    mu_values: list[int],
    property_ids: list[str],
    repo_root: str | Path,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    cells: list[dict[str, Any]] = []
    certificates: list[dict[str, Any]] = []
    witnesses: list[dict[str, Any]] = []
    for d in d_values:
        for mu in mu_values:
            for property_id in property_ids:
                status = dry_status_for_cell(mu)
                certificate_id = None
                notes = "dry-run placeholder; no candidate search executed"
                if status == STATUS_NO_COMPLETE:
                    reason = (
                        f"No binding reactions exist for mu={mu} under periodic_d_mu_v0; "
                        "association products require size at least 2."
                    )
                    cert_payload = {"reason": reason, "status": status}
                    certificate_id = make_certificate_id(property_id, d, mu, cert_payload)
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
                    notes = "trivial complete: no legal association product for mu < 2"
                elif status != STATUS_UNKNOWN:
                    raise AssertionError(f"unexpected dry-run status: {status}")

                cells.append(
                    cell_result(
                        run_id=run_id,
                        d=d,
                        mu=mu,
                        property_id=property_id,
                        status=status,
                        strength=_strength_template(property_id, dry_run=True, mu=mu),
                        repo_root=repo_root,
                        certificate_id=certificate_id,
                        notes=notes,
                    )
                )
    return cells, witnesses, certificates


def write_run_outputs(run_dir: str | Path, config: dict[str, Any], cells: list[dict[str, Any]], witnesses: list[dict[str, Any]], certificates: list[dict[str, Any]]) -> None:
    target = Path(run_dir)
    (target / "logs").mkdir(parents=True, exist_ok=True)
    (target / "checkpoints").mkdir(parents=True, exist_ok=True)
    (target / "heartbeat").mkdir(parents=True, exist_ok=True)
    (target / "summary").mkdir(parents=True, exist_ok=True)

    write_json(target / "config.json", config)
    write_jsonl(target / "cell_results.jsonl.gz", cells)
    write_jsonl(target / "witnesses.jsonl.gz", witnesses)
    write_jsonl(target / "certificates.jsonl.gz", certificates)
    write_jsonl(target / "robustness_estimates.jsonl.gz", [])
    write_json(target / "checkpoints" / "state.json", {
        "run_id": config["run_id"],
        "completed_count": len(cells),
        "current_best": {},
        "updated_at_utc": utc_now(),
        "dry_run": config["dry_run"],
    })
    write_json(target / "heartbeat" / "supervisor.json", {
        "run_id": config["run_id"],
        "worker_id": "supervisor",
        "pid": None,
        "hostname": "local",
        "current_task": {"stage": "dry_run"},
        "last_update_utc": utc_now(),
        "rss_gb": None,
        "status": "completed",
    })
    (target / "logs" / "subagents_registry.jsonl").touch()
    append_event(target / "logs" / "events.jsonl", {
        "run_id": config["run_id"],
        "agent_id": "codex-main",
        "stage": "dry_run",
        "status": "completed",
        "message": "wrote conclusion-only dry-run records",
        "counts": {"cell_results": len(cells), "witnesses": len(witnesses), "certificates": len(certificates)},
    })
    _write_summary_tables(target / "summary", config, cells)


def _write_summary_tables(summary_dir: Path, config: dict[str, Any], cells: list[dict[str, Any]]) -> None:
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
    _write_csv(summary_dir / "minimal_witnesses.csv", [], [
        "witness_id",
        "property_id",
        "d",
        "mu",
        "reaction_count",
        "assembly_depth",
        "network_canonical",
    ])
    status_counts: dict[str, int] = {}
    for record in cells:
        status_counts[record["status"]] = status_counts.get(record["status"], 0) + 1
    (summary_dir / "final_summary.json").write_text(
        json.dumps(
            {
                "run_id": config["run_id"],
                "profile_id": config["profile_id"],
                "profile_hash": config["profile_hash"],
                "cell_result_count": len(cells),
                "status_counts": status_counts,
                "dry_run": config["dry_run"],
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )


def _write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
