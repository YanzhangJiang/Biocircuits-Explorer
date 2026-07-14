#!/usr/bin/env python3
"""Regenerate the frozen fixed-topology shape-control figure packet.

The optimizer is not rerun here.  Direct and three-candidate curves come from
the hash-locked benchmark result.  The one curve absent from that artifact—the
pinned reference—is replayed through the same Julia engine function and checked
against the fixture's stored summary before any figure is written.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile
from datetime import datetime, timezone
from typing import Any

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
from matplotlib.patches import FancyArrowPatch
import numpy as np


ROOT = Path(__file__).resolve().parents[2]
BENCHMARK_DIR = ROOT / "benchmarks" / "rop_shape_control"
FIXTURE_PATH = BENCHMARK_DIR / "cat_fixed_topology.json"
RESULT_PATH = BENCHMARK_DIR / "cat_fixed_topology_results.json"
PRODUCER_PATH = BENCHMARK_DIR / "run_benchmark.jl"
REFERENCE_REPLAY_HELPER = BENCHMARK_DIR / "export_reference_replay.jl"
DEFAULT_REPORT_ROOT = ROOT / "reports" / "2026-07-11"

EXPECTED_SOURCE_SHA256 = {
    "fixture": "a37a3f8562c7d82a4d08d6032c6b22a875c4e3e8f460ca1fba2232ee820dab5c",
    "result": "f91a319c1e8a9200503560daa02c40c07cf05c8d11e38f844e19b4bfee558d1b",
    "producer": "7f920ab23f4a8981b2e2de1710e6109471650670598a8c769795ead866253dca",
}
EXPECTED_RESULT_HASH = "6eda30d093ea44cf841e1eefa87f51659b39e025719e70e109fdca6ddebd177f"
EXPECTED_NETWORK_IR_HASH = "0e8ae6398f18fc8cc04c96a5f539ac704e48fb7593b5be7bc65e3c52ccc7ace9"
EXPECTED_REFERENCE_HASH = "28ebf6f0dff7034cd2fe909816330353081024fec9262bbdb1e551b3e3e685fa"
EXPECTED_ATLAS_SHA256 = "38d05fc7a219e3b0ef85c15777311713fd6916da6a9e8b592660e88a9824708f"

EXPECTED_EDITS = {
    "broaden_both_ears": {
        "baseline": 0.75,
        "closure": 2.44448319755561,
        "selected": 2.424483197733962,
        "margin": 0.0599999994998195,
        "pass": True,
    },
    "separate_ear_tops": {
        "baseline": 0.75,
        "closure": 2.805910267580442,
        "selected": 2.7859102707943446,
        "margin": 0.014999997118308477,
        "pass": True,
    },
    "widen_center": {
        "baseline": 0.75,
        "closure": 8.664381601549245,
        "selected": 8.644381603753722,
        "margin": 0.014721357265004259,
        "pass": False,
    },
    "translate_right_ear": {
        "baseline": 0.50,
        "closure": 0.5065646031939393,
        "selected": 0.49624046248733933,
        "margin": 0.5549137193503341,
        "pass": True,
    },
}

# House style shared with the response-first weekly-report figures.
BLUE = "#0F4D92"
GREEN = "#4C8B5D"
RED = "#B64342"
GOLD = "#C58B2B"
INK = "#232323"
MID = "#626A73"
GRID = "#D9E0E7"
PALE_GREEN = "#EAF3EC"
PALE_RED = "#F8EAEA"
WHITE = "#FFFFFF"
FIXED_ARTIFACT_DATE = datetime(2026, 7, 11, tzinfo=timezone.utc)

EDIT_ORDER = [
    "broaden_both_ears",
    "separate_ear_tops",
    "widen_center",
    "translate_right_ear",
]
EDIT_TITLES = {
    "broaden_both_ears": "Broaden both ears",
    "separate_ear_tops": "Separate ear tops",
    "widen_center": "Widen center",
    "translate_right_ear": "Translate right ear",
}


def configure_style() -> None:
    plt.rcParams.update(
        {
            "font.family": "sans-serif",
            "font.sans-serif": ["Arial", "Helvetica", "DejaVu Sans"],
            "font.size": 8.0,
            "axes.titlesize": 8.2,
            "axes.labelsize": 8.0,
            "xtick.labelsize": 7.0,
            "ytick.labelsize": 7.0,
            "legend.fontsize": 6.5,
            "axes.linewidth": 1.1,
            "xtick.major.width": 0.9,
            "ytick.major.width": 0.9,
            "xtick.major.size": 3.2,
            "ytick.major.size": 3.2,
            "pdf.fonttype": 42,
            "ps.fonttype": 42,
            "svg.fonttype": "none",
            "svg.hashsalt": "bne-shape-control-v1",
            "figure.facecolor": WHITE,
            "axes.facecolor": WHITE,
            "savefig.facecolor": WHITE,
        }
    )


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def load_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def write_json(path: Path, payload: Any) -> None:
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True, allow_nan=False)
        handle.write("\n")


def relpath(path: Path) -> str:
    return path.resolve().relative_to(ROOT).as_posix()


def run_text(command: list[str]) -> str:
    return subprocess.run(
        command,
        cwd=ROOT,
        check=True,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    ).stdout.strip()


def git_metadata() -> dict[str, Any]:
    head = run_text(["git", "rev-parse", "HEAD"])
    branch = run_text(["git", "branch", "--show-current"])
    status = run_text(["git", "status", "--porcelain"])
    return {
        "head": head,
        "branch": branch,
        "dirty": bool(status),
        "dirty_path_count": 0 if not status else len(status.splitlines()),
    }


def assert_close(actual: float, expected: float, label: str, atol: float = 1e-10) -> None:
    if not np.isclose(actual, expected, atol=atol, rtol=0.0):
        raise RuntimeError(f"{label} drifted: {actual!r} != {expected!r}")


def curve_arrays(curve: dict[str, Any], label: str) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    x = np.asarray(curve["param_values"], dtype=float)
    if "output_log10" in curve:
        y = np.asarray(curve["output_log10"], dtype=float)
    else:
        output = np.asarray(curve["output_traj"], dtype=float)
        if output.ndim != 2 or output.shape[1] != 1:
            raise RuntimeError(f"{label}: expected one stored output trajectory")
        y = output[:, 0]
    valid = np.asarray(curve["valid"], dtype=bool)
    if len(x) != 281 or len(y) != 281 or len(valid) != 281:
        raise RuntimeError(f"{label}: expected 281 replay samples")
    if bool(curve["partial"]) or not bool(valid.all()):
        raise RuntimeError(f"{label}: plotted replay must be complete and finite")
    if not np.all(np.isfinite(x)) or not np.all(np.isfinite(y)):
        raise RuntimeError(f"{label}: non-finite plotted coordinate")
    if not np.all(np.diff(x) > 0):
        raise RuntimeError(f"{label}: input samples are not strictly increasing")
    return x, y, valid


def validate_frozen_sources(
    fixture: dict[str, Any], result: dict[str, Any]
) -> dict[str, str]:
    hashes = {
        "fixture": sha256_file(FIXTURE_PATH),
        "result": sha256_file(RESULT_PATH),
        "producer": sha256_file(PRODUCER_PATH),
    }
    for key, expected in EXPECTED_SOURCE_SHA256.items():
        if hashes[key] != expected:
            raise RuntimeError(f"frozen {key} SHA-256 drifted")
    if result["result_hash"] != EXPECTED_RESULT_HASH:
        raise RuntimeError("canonical benchmark result hash drifted")
    if result["network_ir_hash"] != EXPECTED_NETWORK_IR_HASH:
        raise RuntimeError("NetworkIR hash drifted")
    if fixture["id"] != "cat-fixed-topology-v1" or result["benchmark_id"] != fixture["id"]:
        raise RuntimeError("benchmark identity drifted")
    if fixture["reference"]["artifact_id"] != result["fixture_artifact_id"]:
        raise RuntimeError("fixture artifact linkage drifted")

    by_id = {edit["id"]: edit for edit in result["edits"]}
    if list(by_id) != EDIT_ORDER:
        raise RuntimeError("edit order or population drifted")
    reference_hashes = set()
    for edit_id in EDIT_ORDER:
        edit = by_id[edit_id]
        expected = EXPECTED_EDITS[edit_id]
        baseline = edit["three_candidate_baseline"]["selected"]
        direct = edit["direct_lp"]
        response = direct["result"]
        replay = response["replay"]
        coverage = response["coverage"]
        assert_close(float(baseline["magnitude_log10"]), expected["baseline"], f"{edit_id} baseline")
        assert_close(float(direct["closure_support_improvement"]), expected["closure"], f"{edit_id} closure")
        assert_close(float(direct["selected_realized_improvement"]), expected["selected"], f"{edit_id} selected")
        assert_close(float(direct["parameter_chebyshev_radius"]), expected["margin"], f"{edit_id} margin")
        if bool(replay["pass"]) is not expected["pass"]:
            raise RuntimeError(f"{edit_id} replay status drifted")
        expected_coverage = {
            "eligible_path_count": 18,
            "evaluated_path_count": 18,
            "eligible_cell_count": 24,
            "evaluated_cell_count": 24,
            "feasible_cell_count": 24,
            "replay_candidate_count": 1,
            "replayed_count": 1,
        }
        for key, value in expected_coverage.items():
            if int(coverage[key]) != value:
                raise RuntimeError(f"{edit_id} coverage field {key} drifted")
        if bool(coverage["truncated"]) or coverage["truncation_reasons"]:
            raise RuntimeError(f"{edit_id} unexpectedly truncated")
        if response["geometric_status"] != "global_optimal_over_declared_cells":
            raise RuntimeError(f"{edit_id} geometric status drifted")
        if response["fixed_topology"]["network_ir_hash"] != EXPECTED_NETWORK_IR_HASH:
            raise RuntimeError(f"{edit_id} topology identity drifted")
        reference_hashes.add(response["normalized_request"]["reference"]["reference_hash"])
        if response["selected"]["kd"] != replay["request"]["body"]["kd"]:
            raise RuntimeError(f"{edit_id} selected Kd/replay Kd mismatch")
        if response["selected"]["totals"] != replay["request"]["body"]["totals"]:
            raise RuntimeError(f"{edit_id} selected totals/replay totals mismatch")
        curve_arrays(replay["curve"], f"{edit_id} direct")
        curve_arrays(baseline["replay"]["curve"], f"{edit_id} baseline")

    if reference_hashes != {EXPECTED_REFERENCE_HASH}:
        raise RuntimeError("normalized request reference hash drifted")
    widen_metrics = by_id["widen_center"]["direct_lp"]["result"]["replay"]["metrics"]
    if widen_metrics["status"] != "prominence_below_minimum":
        raise RuntimeError("widen-center failure status drifted")
    if widen_metrics["reason"] != "the weaker sampled peak missed min_prominence_log10":
        raise RuntimeError("widen-center failure reason drifted")
    assert_close(float(widen_metrics["right_prominence_log10"]), 0.003909274923750594, "widen right prominence")
    assert_close(float(widen_metrics["min_prominence_log10"]), 0.5, "widen minimum prominence")
    return hashes


def replay_reference() -> dict[str, Any]:
    with tempfile.TemporaryDirectory(prefix="bcx-shape-reference-") as tmp_dir:
        output_path = Path(tmp_dir) / "reference_replay.json"
        env = dict(os.environ)
        env.setdefault("JULIA_NUM_THREADS", "auto")
        command = [
            "julia",
            "--project=webapp",
            relpath(REFERENCE_REPLAY_HELPER),
            str(output_path),
        ]
        completed = subprocess.run(
            command,
            cwd=ROOT,
            env=env,
            check=False,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        if completed.returncode != 0:
            raise RuntimeError(
                "reference replay failed\n" + completed.stdout + "\n" + completed.stderr
            )
        replay = load_json(output_path)
    match = replay["fixture_match"]
    if not bool(match["matches_fixture_summary"]):
        raise RuntimeError("reference replay did not match fixture summary")
    if int(match["sample_count"]) != 281 or int(match["valid_count"]) != 281:
        raise RuntimeError("reference replay validity count drifted")
    if bool(match["partial"]):
        raise RuntimeError("reference replay unexpectedly partial")
    if replay["metrics"]["status"] != "pass":
        raise RuntimeError("pinned reference no longer passes replay metric")
    curve_arrays(replay["curve"], "reference")
    return replay


def write_curve_csv(path: Path, curve: dict[str, Any], source: str) -> None:
    x, y, valid = curve_arrays(curve, source)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerow(["sample_index", "input_log10", "output_log10", "output_linear", "valid", "source"])
        for index, (x_value, y_value, is_valid) in enumerate(zip(x, y, valid, strict=True)):
            writer.writerow(
                [
                    index,
                    format(float(x_value), ".17g"),
                    format(float(y_value), ".17g"),
                    format(float(10.0 ** y_value), ".17g"),
                    str(bool(is_valid)).lower(),
                    source,
                ]
            )


def extract_packet(
    fixture: dict[str, Any], result: dict[str, Any], reference_replay: dict[str, Any], data_dir: Path
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, tuple[np.ndarray, np.ndarray, np.ndarray]]]:
    by_id = {edit["id"]: edit for edit in result["edits"]}
    reference_curve_path = data_dir / "reference_curve.csv"
    write_curve_csv(reference_curve_path, reference_replay["curve"], "pinned_reference_exact_replay")

    baseline_curve_files = {
        "broaden_both_ears": "broaden_baseline_grid_curve.csv",
        "separate_ear_tops": "separate_baseline_grid_curve.csv",
        "widen_center": "widen_center_baseline_grid_curve.csv",
        "translate_right_ear": "translate_right_baseline_grid_curve.csv",
    }
    for edit_id, filename in baseline_curve_files.items():
        selected_baseline = by_id[edit_id]["three_candidate_baseline"]["selected"]
        write_curve_csv(
            data_dir / filename,
            selected_baseline["replay"]["curve"],
            f"frozen_three_candidate_baseline_{edit_id}_selected_{selected_baseline['magnitude_log10']}",
        )
    broaden_baseline = by_id["broaden_both_ears"]["three_candidate_baseline"]["selected"]

    curve_files = {
        "broaden_both_ears": "broaden_direct_curve.csv",
        "separate_ear_tops": "separate_direct_curve.csv",
        "widen_center": "widen_center_direct_curve.csv",
        "translate_right_ear": "translate_right_direct_curve.csv",
    }
    for edit_id, filename in curve_files.items():
        write_curve_csv(
            data_dir / filename,
            by_id[edit_id]["direct_lp"]["result"]["replay"]["curve"],
            f"frozen_direct_{edit_id}",
        )

    reference = fixture["reference"]
    witness_positions: dict[str, Any] = {
        "schema_version": "bne-shape-control-witness-positions/v1.0.0",
        "coordinate": "log10 input total tB",
        "index_basis": "zero_based_reaction_order_program_step",
        "meaning": "ROP path landmarks; these are not nonlinear replay peak locations",
        "reference": {
            "path_idx": reference["path_idx"],
            "positions_log10": reference["operating_points_log10"],
        },
        "edits": {},
    }
    replay_metrics: dict[str, Any] = {
        "schema_version": "bne-shape-control-replay-metrics/v1.0.0",
        "coordinate": "log10 input and log10 output",
        "meaning": "sampled nonlinear curve features; separate from ROP witness geometry",
        "reference": reference_replay["metrics"],
        "edits": {},
    }
    parameter_sets: dict[str, Any] = {
        "schema_version": "bne-shape-control-parameter-sets/v1.0.0",
        "network": fixture["network"],
        "network_ir_hash": result["network_ir_hash"],
        "reference": {
            "curve_file": "reference_curve.csv",
            "path_idx": reference["path_idx"],
            "kd": reference["kd"],
            "totals": reference["totals"],
            "operating_points_log10": reference["operating_points_log10"],
            "reference_hash": EXPECTED_REFERENCE_HASH,
        },
        "baseline_grid_edits": {},
        "direct_edits": {},
    }

    plotted_curves: dict[str, tuple[np.ndarray, np.ndarray, np.ndarray]] = {
        "reference": curve_arrays(reference_replay["curve"], "reference"),
        "broaden_baseline": curve_arrays(broaden_baseline["replay"]["curve"], "broaden baseline"),
    }
    for edit_id in EDIT_ORDER:
        edit = by_id[edit_id]
        baseline = edit["three_candidate_baseline"]["selected"]
        direct = edit["direct_lp"]
        response = direct["result"]
        selected = response["selected"]
        replay = response["replay"]
        witness_positions["edits"][edit_id] = {
            "kind": edit["kind"],
            "reference_positions_log10": reference["operating_points_log10"],
            "baseline_selected_positions_log10": baseline["operating_points_log10"],
            "direct_selected_positions_log10": selected["witness_input_log10"],
            "direct_path_idx": selected["path_idx"],
            "direct_cell_id": selected["cell_id"],
        }
        replay_metrics["edits"][edit_id] = {
            "baseline_selected": baseline["replay"]["metrics"],
            "direct": replay["metrics"],
            "direct_complete": replay["complete"],
            "direct_pass": replay["pass"],
            "direct_status": replay["status"],
        }
        parameter_sets["baseline_grid_edits"][edit_id] = {
            "curve_file": baseline_curve_files[edit_id],
            "selection_method": "largest passing member of the frozen [0.25, 0.5, 0.75] candidate grid",
            "magnitude_log10": baseline["magnitude_log10"],
            "path_idx": baseline["path_idx"],
            "kd": baseline["replay"]["request"]["kd"],
            "totals": baseline["replay"]["request"]["totals"],
            "operating_points_log10": baseline["operating_points_log10"],
            "parameter_only_margin": baseline["parameter_chebyshev_radius"],
            "augmented_chebyshev_radius": baseline["augmented_chebyshev_radius"],
            "replay_pass": baseline["replay"]["pass"],
        }
        parameter_sets["direct_edits"][edit_id] = {
            "curve_file": curve_files[edit_id],
            "kind": edit["kind"],
            "path_idx": selected["path_idx"],
            "cell_id": selected["cell_id"],
            "kd": selected["kd"],
            "totals": selected["totals"],
            "witness_input_log10": selected["witness_input_log10"],
            "closure_support_improvement": direct["closure_support_improvement"],
            "selected_realized_improvement": direct["selected_realized_improvement"],
            "parameter_only_margin": direct["parameter_chebyshev_radius"],
            "request_hash": response["request_hash"],
            "result_hash": response["result_hash"],
            "replay_request_hash": replay["request_hash"],
            "replay_result_hash": replay["result_hash"],
        }
        plotted_curves[edit_id] = curve_arrays(replay["curve"], f"{edit_id} direct")

    write_json(data_dir / "witness_positions.json", witness_positions)
    write_json(data_dir / "replay_metrics.json", replay_metrics)
    write_json(data_dir / "parameter_sets.json", parameter_sets)
    return witness_positions, replay_metrics, parameter_sets, plotted_curves


def style_axis(ax: plt.Axes, *, xlabel: bool = True, ylabel: bool = True) -> None:
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["left"].set_color(INK)
    ax.spines["bottom"].set_color(INK)
    ax.tick_params(colors=INK, direction="out")
    ax.grid(axis="y", color=GRID, linewidth=0.55, alpha=0.65, zorder=0)
    if xlabel:
        ax.set_xlabel(r"input total $\log_{10}(t_B)$")
    if ylabel:
        ax.set_ylabel(r"readout $\log_{10}([AABC])$")


def plot_curve(ax: plt.Axes, curve: tuple[np.ndarray, np.ndarray, np.ndarray], *, color: str, lw: float, ls: str = "-", label: str | None = None, zorder: float = 2.0) -> None:
    x, y, valid = curve
    ax.plot(x, np.where(valid, y, np.nan), color=color, lw=lw, ls=ls, label=label, zorder=zorder)


def add_witness_rugs(
    ax: plt.Axes,
    reference_positions: list[float],
    selected_positions: list[float] | None = None,
) -> None:
    y_min, y_max = ax.get_ylim()
    span = y_max - y_min
    y_reference = y_min + 0.045 * span
    ax.scatter(
        reference_positions,
        np.full(len(reference_positions), y_reference),
        s=18,
        marker="o",
        facecolors=WHITE,
        edgecolors=GREEN,
        linewidths=1.0,
        zorder=7,
        clip_on=False,
    )
    if selected_positions is not None:
        y_selected = y_min + 0.095 * span
        ax.scatter(
            selected_positions,
            np.full(len(selected_positions), y_selected),
            s=19,
            marker="D",
            facecolors=WHITE,
            edgecolors=GREEN,
            linewidths=1.0,
            zorder=7,
            clip_on=False,
        )


def add_replay_features(ax: plt.Axes, metrics: dict[str, Any], *, include_half_prominence: bool = True) -> None:
    peak_x = np.asarray(metrics["peak_input_log10"], dtype=float)
    peak_y = np.asarray(metrics["peak_output_log10"], dtype=float)
    ax.scatter(
        peak_x,
        peak_y,
        s=19,
        marker="o",
        color=INK,
        edgecolor=WHITE,
        linewidth=0.35,
        zorder=8,
        clip_on=False,
    )
    if not include_half_prominence:
        return
    crossings = np.asarray(metrics["half_prominence_crossings_log10"], dtype=float)
    prominences = [float(metrics["left_prominence_log10"]), float(metrics["right_prominence_log10"])]
    for side in range(2):
        level = peak_y[side] - prominences[side] / 2.0
        left, right = crossings[2 * side : 2 * side + 2]
        ax.hlines(level, left, right, color=INK, lw=0.75, zorder=6)
        tick = max(0.035 * (ax.get_ylim()[1] - ax.get_ylim()[0]), 0.04)
        ax.vlines([left, right], level - tick, level + tick, color=INK, lw=0.75, zorder=6)


def add_badge(ax: plt.Axes, text: str, *, passed: bool, xy: tuple[float, float] = (0.97, 0.04)) -> None:
    color = GREEN if passed else RED
    face = PALE_GREEN if passed else PALE_RED
    ax.text(
        xy[0],
        xy[1],
        text,
        transform=ax.transAxes,
        ha="right",
        va="bottom",
        fontsize=6.4,
        fontweight="bold",
        color=color,
        bbox={"boxstyle": "round,pad=0.28", "facecolor": face, "edgecolor": color, "linewidth": 0.7},
        zorder=12,
    )


def panel_letter(ax: plt.Axes, letter: str) -> None:
    ax.text(-0.16, 1.055, letter, transform=ax.transAxes, fontsize=10.0, fontweight="bold", color=INK, va="bottom")


def draw_topology_inset(ax: plt.Axes) -> None:
    inset = ax.inset_axes([0.035, 0.055, 0.43, 0.29], zorder=10)
    inset.set_xlim(0, 1)
    inset.set_ylim(0, 1)
    inset.set_xticks([])
    inset.set_yticks([])
    inset.set_facecolor((1, 1, 1, 0.94))
    for spine in inset.spines.values():
        spine.set_color(GRID)
        spine.set_linewidth(0.7)

    nodes = {
        "C": (0.08, 0.28),
        "BC": (0.36, 0.28),
        "BBC": (0.70, 0.16),
        "ABC": (0.64, 0.60),
        "AABC": (0.91, 0.79),
    }
    for label, (x, y) in nodes.items():
        is_output = label == "AABC"
        inset.text(
            x,
            y,
            label,
            ha="center",
            va="center",
            fontsize=5.5,
            fontweight="bold" if is_output else "normal",
            color=RED if is_output else INK,
            bbox={"boxstyle": "round,pad=0.16", "facecolor": WHITE, "edgecolor": RED if is_output else MID, "linewidth": 0.6},
        )

    def reversible(start: tuple[float, float], end: tuple[float, float], label: str, offset: tuple[float, float]) -> None:
        arrow = FancyArrowPatch(start, end, arrowstyle="<->", mutation_scale=6, color=MID, linewidth=0.7)
        inset.add_patch(arrow)
        mid_x = (start[0] + end[0]) / 2 + offset[0]
        mid_y = (start[1] + end[1]) / 2 + offset[1]
        inset.text(mid_x, mid_y, label, fontsize=4.8, color=BLUE if label == "+B" else MID, ha="center", va="center")

    reversible((0.15, 0.28), (0.28, 0.28), "+B", (0, 0.09))
    reversible((0.44, 0.27), (0.62, 0.18), "+B", (0, 0.09))
    reversible((0.42, 0.35), (0.58, 0.54), "+A", (-0.03, 0.01))
    reversible((0.70, 0.65), (0.85, 0.76), "+A", (0.0, 0.07))
    inset.text(0.03, 0.94, "same 4-step topology", fontsize=5.5, fontweight="bold", color=INK, ha="left", va="top")
    inset.text(0.03, 0.82, "retune parameters only", fontsize=5.0, color=MID, ha="left", va="top")
    inset.text(0.03, 0.06, r"input $t_B$", fontsize=4.8, color=BLUE, ha="left", va="bottom")


def draw_edit_ruler(ax: plt.Axes, direct: dict[str, Any]) -> None:
    # Place this ruler over the empty valley rather than over either peak.
    x_left, x_right = -1.75, 1.35
    y_line = -5.73
    ruler_max = 2.50
    map_x = lambda value: x_left + float(value) / ruler_max * (x_right - x_left)
    baseline = 0.75
    selected = float(direct["selected_realized_improvement"])
    closure = float(direct["closure_support_improvement"])
    ax.plot([x_left, x_right], [y_line, y_line], color=MID, lw=0.8, zorder=9)
    ax.vlines(map_x(baseline), y_line - 0.13, y_line + 0.13, color=GOLD, lw=1.25, zorder=10)
    ax.scatter([map_x(selected)], [y_line], s=18, color=BLUE, zorder=11)
    ax.vlines(map_x(closure), y_line - 0.17, y_line + 0.17, color=RED, lw=1.0, zorder=10)
    ax.text(x_left, y_line + 0.13, "edit size", fontsize=5.5, color=INK, ha="left", va="bottom", fontweight="bold")
    ax.text(map_x(baseline), y_line - 0.18, "grid 0.75", fontsize=5.1, color=GOLD, ha="center", va="top")
    ax.text(x_right, y_line + 0.47, "direct 2.424", fontsize=5.1, color=BLUE, ha="right", va="bottom")
    ax.text(x_right, y_line + 0.31, "limit 2.444", fontsize=5.1, color=RED, ha="right", va="bottom")
    ax.text(x_left, y_line - 0.39, "parameter margin 0.060", fontsize=5.2, color=GREEN, ha="left", va="top")


def draw_reference_handles(ax: plt.Axes, reference_positions: list[float]) -> None:
    positions = np.asarray(reference_positions, dtype=float)
    arrow = {"arrowstyle": "<->", "color": GOLD, "lw": 1.0, "shrinkA": 0, "shrinkB": 0}
    ax.annotate("", xy=(positions[5], -4.56), xytext=(positions[1], -4.56), arrowprops=arrow)
    ax.text((positions[1] + positions[5]) / 2, -4.49, "separate tops", color=GOLD, fontsize=5.5, ha="center", va="bottom")
    ax.annotate("", xy=(positions[2], -5.40), xytext=(positions[0], -5.40), arrowprops=arrow)
    ax.annotate("", xy=(positions[6], -5.40), xytext=(positions[4], -5.40), arrowprops=arrow)
    ax.text(0.45, -5.33, "broaden ears", color=GOLD, fontsize=5.5, ha="center", va="bottom")
    ax.annotate("", xy=(positions[4], -6.47), xytext=(positions[2], -6.47), arrowprops=arrow)
    ax.text((positions[2] + positions[4]) / 2, -6.55, "widen center", color=GOLD, fontsize=5.5, ha="center", va="top")
    ax.annotate("", xy=(positions[4] + 1.15, -5.82), xytext=(positions[4], -5.82), arrowprops={"arrowstyle": "->", "color": GOLD, "lw": 1.0})
    ax.text(positions[4] + 0.58, -5.74, "shift right", color=GOLD, fontsize=5.5, ha="center", va="bottom")


def save_figure(fig: plt.Figure, stem: Path) -> list[Path]:
    outputs = []
    for extension in ("pdf", "svg", "png"):
        path = stem.with_suffix(f".{extension}")
        kwargs: dict[str, Any] = {"facecolor": WHITE}
        if extension == "png":
            kwargs["dpi"] = 300
        elif extension == "pdf":
            kwargs["metadata"] = {
                "Title": stem.name,
                "Author": "Biocircuits Explorer",
                "Subject": "Frozen fixed-topology ROP shape-control benchmark",
                "Creator": "benchmarks/rop_shape_control/make_weekly_report_figures.py",
                "CreationDate": FIXED_ARTIFACT_DATE,
                "ModDate": FIXED_ARTIFACT_DATE,
            }
        elif extension == "svg":
            kwargs["metadata"] = {
                "Title": stem.name,
                "Date": "2026-07-11",
                "Creator": "Biocircuits Explorer",
                "Description": "Frozen fixed-topology ROP shape-control benchmark",
            }
        fig.savefig(path, **kwargs)
        outputs.append(path)
    return outputs


def make_key_figure(
    report_root: Path,
    fixture: dict[str, Any],
    result: dict[str, Any],
    witnesses: dict[str, Any],
    metrics: dict[str, Any],
    curves: dict[str, tuple[np.ndarray, np.ndarray, np.ndarray]],
) -> list[Path]:
    by_id = {edit["id"]: edit for edit in result["edits"]}
    reference_positions = fixture["reference"]["operating_points_log10"]
    fig, axes = plt.subplots(
        1,
        3,
        figsize=(7.10, 3.90),
        gridspec_kw={"width_ratios": [0.95, 1.35, 1.15]},
    )
    fig.subplots_adjust(left=0.072, right=0.985, bottom=0.15, top=0.80, wspace=0.29)
    fig.text(
        0.5,
        0.975,
        "Direct fixed-topology edits can outgrow a grid—but replay still decides",
        ha="center",
        va="top",
        fontsize=9.4,
        fontweight="bold",
        color=INK,
    )

    handles = [
        Line2D([0], [0], color=MID, lw=1.45, label="reference"),
        Line2D([0], [0], color=GOLD, lw=1.6, ls="--", label="3-point grid"),
        Line2D([0], [0], color=BLUE, lw=2.25, label="direct"),
        Line2D([0], [0], marker="o", color="none", markeredgecolor=GREEN, markerfacecolor=WHITE, markersize=4.2, label="reference witness"),
        Line2D([0], [0], marker="D", color="none", markeredgecolor=GREEN, markerfacecolor=WHITE, markersize=3.8, label="selected witness"),
        Line2D([0], [0], marker="o", color=INK, lw=0, markersize=3.8, label="replay peak"),
    ]
    fig.legend(handles=handles, loc="upper center", bbox_to_anchor=(0.5, 0.925), ncol=6, frameon=False, columnspacing=0.9, handlelength=2.0)

    # a — actual pinned reference and the four allowed edit handles.
    ax = axes[0]
    plot_curve(ax, curves["reference"], color=RED, lw=2.25)
    ax.set_xlim(-7, 7)
    ax.set_ylim(-7.42, -4.30)
    style_axis(ax)
    panel_letter(ax, "a")
    ax.set_title("The “cat” calibration curve", loc="left", pad=7, fontweight="bold")
    ref_x, ref_y, _ = curves["reference"]
    witness_y = np.interp(reference_positions, ref_x, ref_y)
    ax.scatter(reference_positions, witness_y, s=22, marker="o", facecolors=WHITE, edgecolors=GREEN, linewidths=1.0, zorder=8)
    draw_reference_handles(ax, reference_positions)
    ax.text(0.03, 0.04, "4-step benchmark\ninput tB → readout AABC", transform=ax.transAxes, fontsize=5.8, color=MID, ha="left", va="bottom")

    # b — key success: direct broadening versus the old grid.
    ax = axes[1]
    broaden = by_id["broaden_both_ears"]
    plot_curve(ax, curves["reference"], color=MID, lw=1.45)
    plot_curve(ax, curves["broaden_baseline"], color=GOLD, lw=1.65, ls="--")
    plot_curve(ax, curves["broaden_both_ears"], color=BLUE, lw=2.3)
    ax.set_xlim(-7, 7)
    ax.set_ylim(-7.48, -4.30)
    style_axis(ax, ylabel=False)
    panel_letter(ax, "b")
    ax.set_title("Broaden: direct search exceeds the grid", loc="left", pad=7, fontweight="bold")
    add_witness_rugs(ax, reference_positions, witnesses["edits"]["broaden_both_ears"]["direct_selected_positions_log10"])
    add_replay_features(ax, metrics["edits"]["broaden_both_ears"]["direct"])
    add_badge(ax, "finite replay PASS", passed=True)
    draw_topology_inset(ax)
    draw_edit_ruler(ax, broaden["direct_lp"])

    # c — counterexample: large witness-space edit, failed sampled prominence.
    ax = axes[2]
    widen = by_id["widen_center"]
    widen_selected = witnesses["edits"]["widen_center"]["direct_selected_positions_log10"]
    widen_metrics = metrics["edits"]["widen_center"]["direct"]
    plot_curve(ax, curves["reference"], color=MID, lw=1.45)
    plot_curve(ax, curves["widen_center"], color=BLUE, lw=2.3)
    ax.set_xlim(-7, 7)
    ax.set_ylim(-13.75, -4.30)
    style_axis(ax, ylabel=False)
    panel_letter(ax, "c")
    ax.set_title("Widen center: geometry ≠ replay", loc="left", pad=7, fontweight="bold")
    add_witness_rugs(ax, reference_positions, widen_selected)
    add_replay_features(ax, widen_metrics)
    add_badge(ax, "finite replay FAIL", passed=False)
    ax.annotate("", xy=(widen_selected[4], -4.78), xytext=(widen_selected[2], -4.78), arrowprops={"arrowstyle": "<->", "color": GREEN, "lw": 1.05})
    ax.text((widen_selected[2] + widen_selected[4]) / 2, -4.62, "witness span 11.738\nedit +8.644", color=GREEN, fontsize=5.6, ha="center", va="bottom")
    crossings = widen_metrics["half_prominence_crossings_log10"]
    ax.annotate("", xy=(crossings[2], -12.52), xytext=(crossings[1], -12.52), arrowprops={"arrowstyle": "<->", "color": INK, "lw": 0.8})
    ax.text((crossings[1] + crossings[2]) / 2, -12.72, "replay central interval 11.843", color=INK, fontsize=5.4, ha="center", va="top")
    ax.text(
        0.04,
        0.45,
        "weaker sampled peak\nprominence 0.00391 < 0.5",
        transform=ax.transAxes,
        fontsize=6.0,
        color=RED,
        fontweight="bold",
        ha="left",
        va="center",
        bbox={"boxstyle": "round,pad=0.28", "facecolor": PALE_RED, "edgecolor": RED, "linewidth": 0.7},
        zorder=12,
    )
    ax.text(
        0.97,
        0.72,
        "expanded y-range",
        transform=ax.transAxes,
        fontsize=5.2,
        color=MID,
        ha="right",
        va="top",
        bbox={"facecolor": WHITE, "edgecolor": "none", "pad": 0.8, "alpha": 0.88},
    )

    outputs = save_figure(fig, report_root / "figure_shape_control_key_result")
    plt.close(fig)
    return outputs


def make_all_edits_figure(
    report_root: Path,
    fixture: dict[str, Any],
    result: dict[str, Any],
    witnesses: dict[str, Any],
    metrics: dict[str, Any],
    curves: dict[str, tuple[np.ndarray, np.ndarray, np.ndarray]],
) -> list[Path]:
    by_id = {edit["id"]: edit for edit in result["edits"]}
    reference_positions = fixture["reference"]["operating_points_log10"]
    fig, axes = plt.subplots(2, 2, figsize=(7.10, 5.20), sharex=True)
    fig.subplots_adjust(left=0.086, right=0.985, bottom=0.125, top=0.82, wspace=0.23, hspace=0.36)
    fig.text(
        0.5,
        0.975,
        "One fixed topology, four edits: three replay passes and one informative failure",
        ha="center",
        va="top",
        fontsize=9.4,
        fontweight="bold",
        color=INK,
    )
    handles = [
        Line2D([0], [0], color=MID, lw=1.45, label="reference"),
        Line2D([0], [0], color=BLUE, lw=2.25, label="direct realization"),
        Line2D([0], [0], marker="o", color="none", markeredgecolor=GREEN, markerfacecolor=WHITE, markersize=4.2, label="reference witness"),
        Line2D([0], [0], marker="D", color="none", markeredgecolor=GREEN, markerfacecolor=WHITE, markersize=3.8, label="selected witness"),
        Line2D([0], [0], marker="o", color=INK, lw=0, markersize=3.8, label="sampled replay peak"),
    ]
    fig.legend(handles=handles, loc="upper center", bbox_to_anchor=(0.5, 0.915), ncol=5, frameon=False, columnspacing=1.15, handlelength=2.2)

    for index, (ax, edit_id) in enumerate(zip(axes.flat, EDIT_ORDER, strict=True)):
        edit = by_id[edit_id]
        direct = edit["direct_lp"]
        replay = edit["direct_lp"]["result"]["replay"]
        selected_positions = witnesses["edits"][edit_id]["direct_selected_positions_log10"]
        direct_metrics = metrics["edits"][edit_id]["direct"]
        plot_curve(ax, curves["reference"], color=MID, lw=1.45)
        plot_curve(ax, curves[edit_id], color=BLUE, lw=2.25)
        ax.set_xlim(-7, 7)
        if edit_id == "widen_center":
            ax.set_ylim(-13.75, -4.30)
        else:
            ax.set_ylim(-7.80, -4.30)
        style_axis(ax, xlabel=index >= 2, ylabel=index % 2 == 0)
        panel_letter(ax, chr(ord("a") + index))
        ax.set_title(EDIT_TITLES[edit_id], loc="left", pad=6, fontweight="bold")
        add_witness_rugs(ax, reference_positions, selected_positions)
        add_replay_features(ax, direct_metrics)
        passed = bool(replay["pass"])
        add_badge(ax, "replay PASS" if passed else "replay FAIL", passed=passed, xy=(0.97, 0.045))
        selected_value = float(direct["selected_realized_improvement"])
        noun = "shift" if edit_id == "translate_right_ear" else ("witness edit" if edit_id == "widen_center" else "edit")
        ax.text(
            0.03,
            0.96,
            f"{noun} {selected_value:.3f}  ·  margin {float(direct['parameter_chebyshev_radius']):.3f}",
            transform=ax.transAxes,
            fontsize=6.1,
            color=INK,
            ha="left",
            va="top",
        )
        if edit_id == "widen_center":
            ax.text(0.97, 0.96, "expanded y-range", transform=ax.transAxes, fontsize=5.4, color=MID, ha="right", va="top")
            ax.text(
                0.03,
                0.37,
                "weak peak prominence\n0.00391 < 0.5",
                transform=ax.transAxes,
                fontsize=6.0,
                color=RED,
                fontweight="bold",
                ha="left",
                va="center",
                bbox={"boxstyle": "round,pad=0.25", "facecolor": PALE_RED, "edgecolor": RED, "linewidth": 0.65},
            )

    fig.text(
        0.5,
        0.026,
        "Each edit evaluated 18/18 eligible paths and 24/24 cells without truncation.  Curves show actual log10 output; no per-curve normalization.",
        ha="center",
        va="bottom",
        fontsize=6.4,
        color=MID,
    )
    outputs = save_figure(fig, report_root / "figure_shape_control_all_edits")
    plt.close(fig)
    return outputs


def write_captions(report_root: Path) -> Path:
    caption_path = report_root / "shape_control_figure_captions.tex"
    content = r"""% Standalone captions for the frozen fixed-topology shape-control figures.
% This file is generated by benchmarks/rop_shape_control/make_weekly_report_figures.py.
\providecommand{\ShapeControlKeyResultCaption}{%
Direct fixed-topology optimization found a larger broadening edit than the former three-candidate grid, while an independent finite replay exposed a counterexample for center widening.  The frozen benchmark is a four-step reversible binding circuit with total B as input and AABC as readout; it is a separate circuit from the earlier response-first Figure~1, which used five steps, total A, and ABCC.  The two circuits share the same seven-trend calibration program but not topology or molecular roles.  Grey/red curves are the pinned reference, the gold dashed curve is the selected three-point-grid baseline, and blue curves use the direct optimizer's returned parameters; curves are actual $\log_{10}$ output and are not normalized separately.  Green open circles and diamonds mark reaction-order witnesses (path landmarks), whereas black points and ticks mark peaks and half-prominence features measured from the nonlinear replay.  For broadening, the grid selected $0.75$ log-input units, while direct geometry reached a closure limit of $2.444$ and selected $2.424$ with parameter-only margin $0.060$; its complete replay passed.  Center widening selected a witness-space improvement of $8.644$, not a nonlinear peak width; its complete replay failed because the weaker sampled peak prominence was $0.00391<0.5$.  Every edit evaluated all 18 eligible paths and all 24 eligible cells for this declared topology and bounds, with no truncation.  These results establish fixed-topology, finite-window computational behavior only, not optimality over other circuits or experimental validation.%
}

\providecommand{\ShapeControlAllEditsCaption}{%
All four edits of the same frozen four-step benchmark.  Each panel compares the exact replay of the pinned reference (grey) with the direct optimizer's returned realization (blue), using green open witness markers and black sampled replay-feature markers.  Broadening ($2.424$), ear-top separation ($2.786$), and right-ear translation ($0.496$) passed complete 281-point replay; center widening ($8.644$ in witness space) failed the declared prominence test even though its path geometry was feasible.  The center-widening panel uses an expanded, explicitly labelled y-range to retain its deep valley; all panels share the same input range and use actual, unnormalized $\log_{10}$ output.  All four searches covered 18/18 eligible paths and 24/24 eligible cells without truncation.  Witness geometry and nonlinear replay features are distinct evidence layers, and the scope remains one pinned topology, program, parameter box, and finite replay grid.%
}
"""
    caption_path.write_text(content, encoding="utf-8")
    return caption_path


def write_readme(data_dir: Path) -> Path:
    path = data_dir / "README.md"
    content = """# Shape-control figure data

This packet supports the 2026-07-11 fixed-topology shape-control figures.  The central result is simple: direct geometric optimization moves the requested response farther than the former three-candidate grid, but a separate nonlinear replay still decides whether the resulting sampled curve passes.

## Regenerate

Run from the Biocircuits Explorer repository root:

```bash
JULIA_NUM_THREADS=auto python3 benchmarks/rop_shape_control/make_weekly_report_figures.py
```

The command verifies the frozen fixture, producer, and result SHA-256 locks; replays only the missing pinned reference curve through `BiocircuitsExplorerBackend.placer_dose_response`; checks that the replay matches the fixture summary; extracts all plotted data; and writes both figures in PDF, SVG, and 300-dpi PNG.

## Files

- `reference_curve.csv`: exact 281-point replay of the pinned reference parameters.
- `*_baseline_grid_curve.csv`: stored replays of the old grid's selected candidate for every edit; broadening selected 0.75.
- `*_direct_curve.csv`: stored replay curves for the four direct edits.
- `witness_positions.json`: reference, baseline, and selected reaction-order path landmarks.
- `replay_metrics.json`: sampled nonlinear peak, prominence, width, and central-interval measurements.
- `parameter_sets.json`: all parameters used by every plotted curve.
- `figure_manifest.json`: source locks, repository state, commands, coverage, output hashes, and disclosed gaps.

`output_log10` is already the engine's log10 output coordinate.  No curve is independently normalized.  `output_linear` is included only as the deterministic inverse transform `10**output_log10`.

## Evidence boundary

Witness positions describe exact path geometry inside the declared fixed-topology cells; they are not nonlinear peak locations or widths.  Replay features come from a complete 281-point finite equilibrium scan.  All four edits covered 18/18 eligible paths and 24/24 eligible cells without truncation.  Three direct realizations passed replay.  `widen_center` did not: its weaker sampled peak prominence was 0.0039092749, below the declared minimum 0.5.

This benchmark is not the circuit shown in the earlier response-first Figure 1.  It has four reversible binding steps, total B as input, and AABC as readout; Figure 1 has five steps, total A, and ABCC.  They share a seven-trend calibration program, not circuit identity.

The fixture pins the reference parameters and describes their selection, but the original pre-optimizer cell-selection artifact is not present in this repository.  This packet therefore verifies the pinned parameters and their exact replay, not the historical reason that cell was selected.
"""
    path.write_text(content, encoding="utf-8")
    return path


def output_hashes(paths: list[Path]) -> dict[str, str]:
    return {relpath(path): sha256_file(path) for path in sorted(paths)}


def build_manifest(
    report_root: Path,
    data_dir: Path,
    fixture: dict[str, Any],
    result: dict[str, Any],
    reference_replay: dict[str, Any],
    source_hashes: dict[str, str],
    parameter_sets: dict[str, Any],
    generated_paths: list[Path],
) -> dict[str, Any]:
    atlas_path = ROOT / fixture["source"]["atlas_snapshot"]
    atlas = {
        "path": fixture["source"]["atlas_snapshot"],
        "declared_sha256": fixture["source"]["atlas_sha256"],
        "present": atlas_path.is_file(),
        "observed_sha256": sha256_file(atlas_path) if atlas_path.is_file() else None,
    }
    if atlas["present"] and atlas["observed_sha256"] != EXPECTED_ATLAS_SHA256:
        raise RuntimeError("source atlas SHA-256 drifted")
    return {
        "schema_version": "bne-shape-control-figure-manifest/v1.0.0",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "repository": git_metadata(),
        "benchmark": {
            "id": fixture["id"],
            "evidence_scope": fixture["evidence_scope"],
            "network_ir_hash": result["network_ir_hash"],
            "fixture_artifact_id": fixture["reference"]["artifact_id"],
            "canonical_result_hash": result["result_hash"],
            "artifact_config_hash": result["artifact"]["algorithm"]["config_hash"],
            "application_version": result["application_version"],
        },
        "frozen_sources": {
            "fixture": {"path": relpath(FIXTURE_PATH), "sha256": source_hashes["fixture"]},
            "result": {"path": relpath(RESULT_PATH), "sha256": source_hashes["result"]},
            "producer": {"path": relpath(PRODUCER_PATH), "sha256": source_hashes["producer"]},
            "reference_replay_helper": {"path": relpath(REFERENCE_REPLAY_HELPER), "sha256": sha256_file(REFERENCE_REPLAY_HELPER)},
            "figure_script": {"path": relpath(Path(__file__)), "sha256": sha256_file(Path(__file__))},
            "atlas": atlas,
        },
        "reference_replay": {
            "endpoint": "/api/v1/placer_curve",
            "function": reference_replay["replay_function"],
            "metric_function": reference_replay["metric_function"],
            "julia_version": reference_replay["julia_version"],
            "application_version": reference_replay["application_version"],
            "input_window_log10": fixture["reference"]["replay"]["input_window_log10"],
            "sample_points": reference_replay["fixture_match"]["sample_count"],
            "valid_points": reference_replay["fixture_match"]["valid_count"],
            "partial": reference_replay["fixture_match"]["partial"],
            "matches_fixture_summary": reference_replay["fixture_match"]["matches_fixture_summary"],
            "sampled_peak_input_log10": reference_replay["fixture_match"]["sampled_peak_input_log10"],
            "sampled_output_log10_range": reference_replay["fixture_match"]["sampled_output_log10_range"],
            "discrepancy": None,
        },
        "population": {
            "scope": "one pinned fixed topology, finite-window seven-step SISO ROP program, declared parameter bounds",
            "per_edit": {
                "eligible_paths": 18,
                "evaluated_paths": 18,
                "eligible_cells": 24,
                "evaluated_cells": 24,
                "feasible_cells": 24,
                "truncated": False,
            },
            "edit_count": 4,
            "geometric_success_count": 4,
            "finite_replay_pass_count": 3,
        },
        "solver_validity": {
            "reference": "281/281 valid; partial=false",
            "direct_edits": "each 281/281 valid; partial=false",
            "baseline_selected_curves": "each 281/281 valid; partial=false",
            "invalid_samples_plotted": 0,
        },
        "curve_coordinate": {
            "x": "log10 input total tB",
            "y": "engine output_logspace=true coordinate: log10 AABC concentration",
            "normalization": "none; no per-curve normalization",
        },
        "parameters_per_curve": parameter_sets,
        "relationship_to_prior_figure1": {
            "identity_status": "contradicted",
            "relationship": "different_circuit",
            "basis": "canonical topology codes and reaction counts differ; input/output molecular roles also differ",
            "shared_property": "same seven-trend reaction-order calibration program",
            "benchmark": "four reversible binding steps; input total B; readout AABC",
            "prior_figure1": "five reversible binding steps; input total A; readout ABCC",
            "must_not_imply_same_circuit": True,
            "external_source_note": "prior Figure 1 source is in the separate weekly-report repository and was dirty/untracked at audit time",
        },
        "disclosures": [
            "The frozen result omitted the full pinned-reference curve; this packet reruns only that curve and hard-checks its 281/281 validity, peak locations, and output range against the fixture summary.",
            "The fixture describes the reference as the highest-augmented-radius pre-optimizer cell, but the original cell-selection artifact is absent; the pinned parameters and replay are verified, not the historical selection lineage.",
            "A full benchmark producer rerun contains timestamps and elapsed times, and its outer config hash includes the producer checkout path; byte-identical full-result regeneration is not claimed. The plotting packet locks the frozen result file instead.",
            "The widen-center value 8.644 is a witness-space improvement, not a nonlinear peak width. Its finite replay fails because the weaker sampled peak prominence is 0.0039092749 < 0.5.",
        ],
        "software": {
            "python": os.sys.version.split()[0],
            "numpy": np.__version__,
            "matplotlib": matplotlib.__version__,
        },
        "commands": {
            "regenerate_all": "JULIA_NUM_THREADS=auto python3 benchmarks/rop_shape_control/make_weekly_report_figures.py",
            "reference_only": "JULIA_NUM_THREADS=auto julia --project=webapp benchmarks/rop_shape_control/export_reference_replay.jl OUTPUT.json",
            "frozen_contract": "JULIA_NUM_THREADS=auto julia --project=webapp webapp/test/rop_shape_cat_benchmark.jl",
            "repository_gate": "python3 scripts/verify_repository.py --check",
            "render_pdf": "pdftoppm -png -r 180 INPUT.pdf OUTPUT_PREFIX",
        },
        "outputs": output_hashes(generated_paths),
        "figure_size_inches": {
            "figure_shape_control_key_result": [7.10, 3.90],
            "figure_shape_control_all_edits": [7.10, 5.20],
        },
        "caption_file": relpath(report_root / "shape_control_figure_captions.tex"),
        "data_directory": relpath(data_dir),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--report-root",
        type=Path,
        default=DEFAULT_REPORT_ROOT,
        help="output directory (default: reports/2026-07-11)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    report_root = args.report_root.resolve()
    report_root.mkdir(parents=True, exist_ok=True)
    data_dir = report_root / "shape_control_figure_data"
    data_dir.mkdir(parents=True, exist_ok=True)

    configure_style()
    fixture = load_json(FIXTURE_PATH)
    result = load_json(RESULT_PATH)
    source_hashes = validate_frozen_sources(fixture, result)
    reference_replay = replay_reference()
    witnesses, metrics, parameter_sets, curves = extract_packet(
        fixture, result, reference_replay, data_dir
    )
    figure_paths = make_key_figure(report_root, fixture, result, witnesses, metrics, curves)
    figure_paths += make_all_edits_figure(report_root, fixture, result, witnesses, metrics, curves)
    caption_path = write_captions(report_root)
    readme_path = write_readme(data_dir)

    generated_paths = [
        data_dir / "reference_curve.csv",
        data_dir / "broaden_baseline_grid_curve.csv",
        data_dir / "separate_baseline_grid_curve.csv",
        data_dir / "widen_center_baseline_grid_curve.csv",
        data_dir / "translate_right_baseline_grid_curve.csv",
        data_dir / "broaden_direct_curve.csv",
        data_dir / "separate_direct_curve.csv",
        data_dir / "widen_center_direct_curve.csv",
        data_dir / "translate_right_direct_curve.csv",
        data_dir / "witness_positions.json",
        data_dir / "replay_metrics.json",
        data_dir / "parameter_sets.json",
        readme_path,
        caption_path,
        *figure_paths,
    ]
    manifest = build_manifest(
        report_root,
        data_dir,
        fixture,
        result,
        reference_replay,
        source_hashes,
        parameter_sets,
        generated_paths,
    )
    manifest_path = data_dir / "figure_manifest.json"
    write_json(manifest_path, manifest)

    print("Verified frozen sources and exact pinned-reference replay.")
    print(f"Wrote {relpath(report_root / 'figure_shape_control_key_result.pdf')}")
    print(f"Wrote {relpath(report_root / 'figure_shape_control_all_edits.pdf')}")
    print(f"Wrote {relpath(data_dir)}")


if __name__ == "__main__":
    main()
