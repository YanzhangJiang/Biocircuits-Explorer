#!/usr/bin/env python3
"""Generate Markdown and CSV reports from a periodic-table run directory."""

from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

from src.periodic_table.result_schema import read_jsonl


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--report-md", default=str(REPO_ROOT / "reports" / "periodic_table_final_report.md"))
    parser.add_argument("--tables-csv", default=str(REPO_ROOT / "reports" / "periodic_table_final_report_tables.csv"))
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    run_dir = Path(args.run_dir)
    config = json.loads((run_dir / "config.json").read_text(encoding="utf-8"))
    final_summary = json.loads((run_dir / "summary" / "final_summary.json").read_text(encoding="utf-8"))
    cells = read_jsonl(run_dir / "cell_results.jsonl.gz")
    witnesses = read_jsonl(run_dir / "witnesses.jsonl.gz")
    certificates = read_jsonl(run_dir / "certificates.jsonl.gz")

    report_md = Path(args.report_md)
    tables_csv = Path(args.tables_csv)
    report_md.parent.mkdir(parents=True, exist_ok=True)
    tables_csv.parent.mkdir(parents=True, exist_ok=True)

    _write_tables_csv(tables_csv, cells)
    report_md.write_text(
        _render_report(run_dir, config, final_summary, cells, witnesses, certificates, tables_csv),
        encoding="utf-8",
    )
    print(report_md)
    print(tables_csv)
    return 0


def _write_tables_csv(path: Path, cells: list[dict[str, Any]]) -> None:
    fieldnames = [
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
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in sorted(cells, key=lambda item: (item["property_id"], item["d"], item["mu"])):
            writer.writerow(row)


def _render_report(
    run_dir: Path,
    config: dict[str, Any],
    final_summary: dict[str, Any],
    cells: list[dict[str, Any]],
    witnesses: list[dict[str, Any]],
    certificates: list[dict[str, Any]],
    tables_csv: Path,
) -> str:
    status_counts = final_summary.get("status_counts", {})
    property_ids = config.get("property_ids", [])
    lines: list[str] = []
    lines.append("# ROP Capability Periodic Table Report")
    lines.append("")
    lines.append("## 1. Executive summary")
    lines.append("")
    lines.append(f"- Run directory: `{run_dir}`")
    lines.append(f"- Run id: `{config.get('run_id')}`")
    lines.append(f"- Profile: `{config.get('profile_id')}`")
    lines.append(f"- Profile hash: `{config.get('profile_hash')}`")
    lines.append(f"- Search kind: `{config.get('search_kind', 'unknown')}`")
    lines.append(f"- Scope: `d={config.get('d_values')}`, `mu={config.get('mu_values')}`")
    lines.append(f"- Cell rows: {len(cells)}; witnesses: {len(witnesses)}; certificates: {len(certificates)}")
    lines.append(f"- Status counts: `{json.dumps(status_counts, sort_keys=True)}`")
    lines.append("")
    lines.append("This run preserves zero as a third sign state. Non-trivial negative results are not claimed unless a certificate is present; bounded searches without a witness are reported as `UNKNOWN`.")
    lines.append("")
    lines.append("## 2. Complete definition and profile hash")
    lines.append("")
    lines.append("The table uses `periodic_d_mu_v0`: complexes are nonnegative integer vectors over base species with total size at most `mu`, and legal reactions are reversible associations `C_a + C_b <-> C_{a+b}`. Homomers, complex-plus-base, and complex-plus-complex associations are included.")
    lines.append("")
    lines.append("## 3. Mathematical definitions")
    lines.append("")
    lines.append("A cell `(d, mu)` is a design space, not a single network. Status values are profile-relative. `WITNESS_ONLY` means a witness was found by bounded search but no global minimality or negative certificate is asserted.")
    lines.append("")
    for property_id in property_ids:
        rows = [row for row in cells if row.get("property_id") == property_id]
        lines.append(f"## {property_id}")
        lines.append("")
        lines.append(_markdown_status_grid(rows))
        lines.append("")
    lines.append("## Minimal witnesses")
    lines.append("")
    if witnesses:
        lines.append("| property | d | mu | min_r | network | witness |")
        lines.append("|---|---:|---:|---:|---|---|")
        for witness in sorted(witnesses, key=lambda item: (item.get("property_id"), item.get("d"), item.get("mu"))):
            features = witness.get("source_metadata", {}).get("features", {})
            network = str(witness.get("network_canonical", "")).replace("|", "<br>")
            lines.append(
                f"| `{witness.get('property_id')}` | {witness.get('d')} | {witness.get('mu')} | "
                f"{features.get('reaction_count')} | `{network}` | `{witness.get('witness_id')}` |"
            )
    else:
        lines.append("No non-trivial witnesses were found in this run.")
    lines.append("")
    lines.append("## Negative certificates and unknown cells")
    lines.append("")
    lines.append(f"- Certificates: {len(certificates)}. In this run, certificates are trivial `mu < 2` no-binding certificates unless otherwise noted.")
    unknown_count = sum(1 for row in cells if row.get("status") == "UNKNOWN")
    lines.append(f"- UNKNOWN rows: {unknown_count}. These are not negative conclusions.")
    lines.append("")
    lines.append("## Storage/runtime/resource summary")
    lines.append("")
    lines.append(f"- Runtime seconds: {final_summary.get('runtime_sec')}")
    lines.append(f"- Consolidated CSV: `{tables_csv}`")
    lines.append("- Storage policy: no full atlas and no persisted path records.")
    lines.append("")
    return "\n".join(lines) + "\n"


def _markdown_status_grid(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return "_No rows._"
    d_values = sorted({row["d"] for row in rows})
    mu_values = sorted({row["mu"] for row in rows})
    lookup = {(row["d"], row["mu"]): row for row in rows}
    lines = ["| d \\ mu | " + " | ".join(str(mu) for mu in mu_values) + " |"]
    lines.append("|---|" + "|".join("---" for _ in mu_values) + "|")
    for d in d_values:
        values = []
        for mu in mu_values:
            row = lookup.get((d, mu))
            if row is None:
                values.append("")
            else:
                cell = row.get("status", "")
                if row.get("witness_id"):
                    cell += f"<br>`r={row.get('min_r')}`"
                values.append(cell)
        lines.append(f"| {d} | " + " | ".join(values) + " |")
    return "\n".join(lines)


if __name__ == "__main__":
    raise SystemExit(main())
