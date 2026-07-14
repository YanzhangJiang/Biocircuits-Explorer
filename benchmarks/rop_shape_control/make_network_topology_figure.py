#!/usr/bin/env python3
"""Render the frozen shape-control topology with the sibling rop-network-viz package."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import importlib
import json
import os
from pathlib import Path
import subprocess
import sys
from typing import Any

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt


ROOT = Path(__file__).resolve().parents[2]
FIXTURE_PATH = ROOT / "benchmarks" / "rop_shape_control" / "cat_fixed_topology.json"
RESULT_PATH = ROOT / "benchmarks" / "rop_shape_control" / "cat_fixed_topology_results.json"
DEFAULT_VIZ_ROOT = ROOT.parent / "rop-network-viz"
DEFAULT_REPORT_ROOT = ROOT / "reports" / "2026-07-11"
STEM = "figure_fixed_topology_network"
FIXED_ARTIFACT_DATE = datetime(2026, 7, 11, tzinfo=timezone.utc)

BLUE = "#0F4D92"
RED = "#B64342"
INK = "#232323"
MID = "#626A73"
WHITE = "#FFFFFF"


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def load_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def write_json(path: Path, value: Any) -> None:
    with path.open("w", encoding="utf-8") as handle:
        json.dump(value, handle, indent=2, sort_keys=True, allow_nan=False)
        handle.write("\n")


def repo_relative(path: Path) -> str:
    return Path(os.path.relpath(path.resolve(), ROOT)).as_posix()


def git_value(viz_root: Path, *args: str) -> str | None:
    completed = subprocess.run(
        ["git", "-C", str(viz_root), *args],
        check=False,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    return completed.stdout.strip() if completed.returncode == 0 else None


def import_renderer(viz_root: Path):
    source_root = viz_root / "src"
    binding_path = source_root / "rop_network_viz" / "binding.py"
    if not binding_path.is_file():
        raise RuntimeError(f"rop-network-viz binding renderer not found under {viz_root}")
    sys.path.insert(0, str(source_root))
    package = importlib.import_module("rop_network_viz")
    binding = importlib.import_module("rop_network_viz.binding")
    imported_path = Path(binding.__file__).resolve()
    if imported_path != binding_path.resolve():
        raise RuntimeError(f"imported renderer {imported_path} instead of requested {binding_path}")
    return package, binding, binding_path


def configure_style() -> None:
    plt.rcParams.update(
        {
            "font.family": "sans-serif",
            "font.sans-serif": ["Arial", "Helvetica", "DejaVu Sans"],
            "font.size": 8.0,
            "pdf.fonttype": 42,
            "svg.fonttype": "none",
            "svg.hashsalt": "bne-fixed-topology-network-v1",
            "figure.facecolor": WHITE,
            "savefig.facecolor": WHITE,
        }
    )


def save_outputs(fig: plt.Figure, report_root: Path) -> list[Path]:
    paths: list[Path] = []
    for extension in ("pdf", "svg", "png"):
        path = report_root / f"{STEM}.{extension}"
        kwargs: dict[str, Any] = {"facecolor": WHITE}
        if extension == "png":
            kwargs["dpi"] = 300
        elif extension == "pdf":
            kwargs["metadata"] = {
                "Title": STEM,
                "Author": "Biocircuits Explorer",
                "Subject": "Frozen fixed-topology shape-control network rendered by rop-network-viz",
                "Creator": "benchmarks/rop_shape_control/make_network_topology_figure.py",
                "CreationDate": FIXED_ARTIFACT_DATE,
                "ModDate": FIXED_ARTIFACT_DATE,
            }
        else:
            kwargs["metadata"] = {
                "Title": STEM,
                "Date": "2026-07-11",
                "Creator": "Biocircuits Explorer with rop-network-viz",
                "Description": "Frozen fixed-topology shape-control network",
            }
        fig.savefig(path, **kwargs)
        paths.append(path)
    return paths


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--viz-root",
        type=Path,
        default=DEFAULT_VIZ_ROOT,
        help="rop-network-viz checkout (default: sibling ../rop-network-viz)",
    )
    parser.add_argument(
        "--report-root",
        type=Path,
        default=DEFAULT_REPORT_ROOT,
        help="output directory (default: reports/2026-07-11)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    viz_root = args.viz_root.resolve()
    report_root = args.report_root.resolve()
    report_root.mkdir(parents=True, exist_ok=True)
    data_dir = report_root / "shape_control_figure_data"
    data_dir.mkdir(parents=True, exist_ok=True)

    fixture = load_json(FIXTURE_PATH)
    result = load_json(RESULT_PATH)
    package, binding, binding_path = import_renderer(viz_root)

    network_id = str(fixture["source"]["network_id"])
    input_symbol = str(fixture["network"]["input"])
    output_symbol = str(fixture["network"]["output"])
    parsed = binding.parse_nid(network_id)
    names = {binding.species_name(species) for species in parsed.species}
    if parsed.d != 3 or len(parsed.reactions) != 4:
        raise RuntimeError("frozen topology did not parse as three monomers and four reactions")
    if input_symbol != "tB" or output_symbol != "C_A_A_B_C" or output_symbol not in names:
        raise RuntimeError("frozen input/readout identity drifted")
    if len(fixture["network"]["rules"]) != len(parsed.reactions):
        raise RuntimeError("fixture rule count and renderer reaction count disagree")

    configure_style()
    fig = plt.figure(figsize=(4.60, 3.50))
    ax = fig.add_axes([0.04, 0.035, 0.92, 0.76])
    style = {
        "input": BLUE,
        "readout": RED,
        "ink": INK,
        "label": INK,
        "muted": MID,
        "arrow": "#555B61",
        "reactant": "#B8C0CC",
    }
    returned = binding.draw_binding_network(
        ax,
        parsed,
        input_symbol=input_symbol,
        output_symbol=output_symbol,
        compact=False,
        label_nodes=True,
        style=style,
    )
    # The package defaults target dense manuscript insets.  This is a
    # standalone topology panel, so increase only the package-authored text;
    # glyph geometry and reaction layout remain owned by rop-network-viz.
    for text_artist in ax.texts:
        text_artist.set_fontsize(7.0)
    if returned.nid != network_id:
        raise RuntimeError("renderer returned a different network identity")

    fig.text(
        0.5,
        0.965,
        "Fixed-topology network used for shape control",
        ha="center",
        va="top",
        fontsize=10.5,
        fontweight="bold",
        color=INK,
    )
    fig.text(
        0.5,
        0.905,
        r"4 reversible binding steps  |  swept input $t_B$  |  readout AABC",
        ha="center",
        va="top",
        fontsize=7.2,
        color=MID,
    )
    fig.text(
        0.5,
        0.845,
        "Assembly arrows show binding construction; every modeled step is reversible.",
        ha="center",
        va="top",
        fontsize=5.9,
        color=MID,
    )

    outputs = save_outputs(fig, report_root)
    plt.close(fig)

    branch = git_value(viz_root, "branch", "--show-current")
    head = git_value(viz_root, "rev-parse", "--verify", "HEAD")
    status = git_value(viz_root, "status", "--porcelain") or ""
    manifest = {
        "schema_version": "bne-fixed-topology-network-figure/v1.0.0",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "renderer": {
            "package": "rop-network-viz",
            "version": str(package.__version__),
            "api": "rop_network_viz.binding.draw_binding_network",
            "checkout": repo_relative(viz_root),
            "binding_source": repo_relative(binding_path),
            "binding_sha256": sha256_file(binding_path),
            "git_branch": branch,
            "git_head": head,
            "git_dirty": bool(status),
            "git_status_path_count": len(status.splitlines()) if status else 0,
            "provenance_note": "The renderer checkout has no commit yet; the source-file hash is the executable provenance lock.",
        },
        "network": {
            "benchmark_id": fixture["id"],
            "network_id": network_id,
            "network_ir_hash": result["network_ir_hash"],
            "rules": fixture["network"]["rules"],
            "reaction_count": len(parsed.reactions),
            "base_monomer_count": parsed.d,
            "input_symbol": input_symbol,
            "output_symbol": output_symbol,
            "topology_preserved_across_parameter_sets": True,
        },
        "sources": {
            "fixture": {"path": repo_relative(FIXTURE_PATH), "sha256": sha256_file(FIXTURE_PATH)},
            "result": {"path": repo_relative(RESULT_PATH), "sha256": sha256_file(RESULT_PATH)},
            "script": {"path": repo_relative(Path(__file__)), "sha256": sha256_file(Path(__file__))},
        },
        "render": {
            "canvas_inches": [4.60, 3.50],
            "png_dpi": 300,
            "compact": False,
            "label_nodes": True,
            "package_text_size_pt": 7.0,
            "input_color": BLUE,
            "readout_color": RED,
            "reaction_arrow_semantics": "assembly-direction visual convention for reversible equilibrium binding",
        },
        "outputs": {repo_relative(path): sha256_file(path) for path in outputs},
        "command": "python3 benchmarks/rop_shape_control/make_network_topology_figure.py --viz-root ../rop-network-viz",
    }
    manifest_path = data_dir / f"{STEM}_manifest.json"
    write_json(manifest_path, manifest)

    for path in outputs:
        print(repo_relative(path))
    print(repo_relative(manifest_path))


if __name__ == "__main__":
    main()
