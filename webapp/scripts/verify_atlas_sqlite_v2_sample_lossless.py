#!/usr/bin/env python3

from __future__ import annotations

import argparse
import importlib.util
import json
import re
import random
import sqlite3
import sys
import zlib
from dataclasses import dataclass
from pathlib import Path
from typing import Any


SCRIPT_DIR = Path(__file__).resolve().parent
CODEC_PATH = SCRIPT_DIR / "atlas_id_codec.py"
SPEC = importlib.util.spec_from_file_location("atlas_id_codec", CODEC_PATH)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError(f"Unable to load atlas_id_codec from {CODEC_PATH}")
atlas_id_codec = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = atlas_id_codec
SPEC.loader.exec_module(atlas_id_codec)


ROLE_CODE_TO_TEXT = {
    1: "source",
    2: "sink",
    3: "interior",
    4: "branch",
    5: "merge",
    6: "source_sink",
    7: "branch_merge",
}

FAMILY_KIND_CODE_TO_TEXT = {
    1: "exact",
    2: "motif",
}


@dataclass(frozen=True)
class TableSpec:
    table: str
    id_col: str
    view: str


TABLE_SPECS = [
    TableSpec("behavior_slices", "slice_id", "v_bs"),
    TableSpec("regime_records", "regime_record_id", "v_rr"),
    TableSpec("family_buckets", "bucket_id", "v_fb"),
    TableSpec("transition_records", "transition_record_id", "v_tr"),
    TableSpec("path_records", "path_record_id", "v_pr"),
]


def _json_load(raw: str | None) -> Any:
    if raw in (None, ""):
        return None
    return json.loads(raw)


def _decompress_residual(blob: bytes | None) -> dict[str, Any]:
    if blob in (None, b""):
        return {}
    return json.loads(zlib.decompress(blob))


def _normalize(value: Any) -> Any:
    if isinstance(value, sqlite3.Row):
        return {k: _normalize(value[k]) for k in value.keys()}
    if isinstance(value, dict):
        return {str(k): _normalize(v) for k, v in sorted(value.items(), key=lambda item: str(item[0]))}
    if isinstance(value, (list, tuple)):
        return [_normalize(v) for v in value]
    if isinstance(value, str):
        return re.sub(r"\s*(?:->|=>|→)\s*", " → ", value.strip())
    return value


def _compare_jsonish(lhs: Any, rhs: Any) -> bool:
    return _normalize(lhs) == _normalize(rhs)


def _value_or_json(text: Any) -> Any:
    if text is None:
        return None
    if isinstance(text, (int, float)):
        return text
    if not isinstance(text, str):
        return text
    stripped = text.strip()
    if stripped.startswith("{") or stripped.startswith("["):
        return json.loads(stripped)
    return text


def _input_symbol_from_value(mode: str, value: str) -> str:
    if mode == "input":
        return value
    if value.startswith("orthant(") and value.endswith(")"):
        return value[len("orthant("):-1]
    if value.startswith("axis(") and value.endswith(")"):
        return value[len("axis("):-1]
    return value


def _classifier_config_json(cfg: atlas_id_codec.ClassifierConfig) -> dict[str, Any]:
    typed = dict(cfg.typed_fields)
    return {
        "compute_volume": bool(typed["compute_volume"]),
        "deduplicate": bool(typed["deduplicate"]),
        "include_path_records": False,
        "keep_nonasymptotic": bool(typed["keep_nonasymptotic"]),
        "keep_singular": bool(typed["keep_singular"]),
        "min_volume_mean": typed["min_volume_mean"],
        "motif_zero_tol": typed["motif_zero_tol"],
        "path_scope": typed["scope"],
    }


def _output_order_details(token: str | None) -> tuple[str | None, Any, Any]:
    if token is None:
        return None, None, None
    token = str(token).strip()
    if token.startswith("(") and token.endswith(")"):
        inner = token[1:-1].strip()
        atoms = [] if not inner else [part.strip() for part in inner.split(",")]
        values = [_scalar_from_output_atom(atom) for atom in atoms]
        return "vector", values, None
    return "scalar", _scalar_from_output_atom(token), None


def _scalar_from_output_atom(atom: str) -> float | int:
    atom = atom.strip()
    if atom.startswith("+"):
        atom = atom[1:]
    if atom == "Inf":
        return float("inf")
    if atom == "-Inf":
        return float("-inf")
    if "." in atom or "e" in atom.lower():
        return float(atom)
    return int(atom)


def _parse_change_metadata(change_signature: str) -> dict[str, Any]:
    if change_signature.startswith("orthant(") and change_signature.endswith(")"):
        inner = change_signature[len("orthant("):-1]
        tokens = [part.strip() for part in inner.split(",") if part.strip()]
        return {
            "change_kind": "orthant",
            "change_label": inner,
            "change_qk_symbols": [token[1:] if token[:1] in "+-" else token for token in tokens],
            "change_qk_indices": list(range(1, len(tokens) + 1)),
            "change_qk_signs": [-1 if token.startswith("-") else 1 for token in tokens],
        }
    if change_signature.startswith("axis(") and change_signature.endswith(")"):
        inner = change_signature[len("axis("):-1].strip()
        symbol = inner[1:] if inner[:1] in "+-" else inner
        sign = -1 if inner.startswith("-") else 1
        return {
            "change_kind": "axis",
            "change_label": inner,
            "change_qk_symbols": [symbol],
            "change_qk_indices": [1],
            "change_qk_signs": [sign],
            "change_qK": symbol,
            "change_qK_idx": 1,
        }
    return {
        "change_kind": "axis",
        "change_label": change_signature,
        "change_qk_symbols": [change_signature],
        "change_qk_indices": [1],
        "change_qk_signs": [1],
        "change_qK": change_signature,
        "change_qK_idx": 1,
    }


def _fetch_row(conn: sqlite3.Connection, sql: str, params: tuple[Any, ...]) -> sqlite3.Row:
    row = conn.execute(sql, params).fetchone()
    if row is None:
        raise AssertionError(f"Missing row for query: {sql} params={params!r}")
    return row


def _recover_role_text(
    dst: sqlite3.Connection,
    table: str,
    pk_col: str,
    pk_value: int,
    code: int | None,
    residual_key: str,
) -> str | None:
    if code is None:
        return None
    text = ROLE_CODE_TO_TEXT.get(code)
    if text is not None:
        return text
    row = _fetch_row(dst, f"SELECT rj FROM {table} WHERE {pk_col} = ?", (pk_value,))
    residual = _decompress_residual(row["rj"])
    text = residual.get(residual_key)
    if text is None:
        raise KeyError(code)
    return str(text)


def _require_views(conn: sqlite3.Connection) -> None:
    names = {
        row[0]
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='view' AND name IN ('v_bs','v_rr','v_fb','v_tr','v_pr')"
        )
    }
    missing = sorted({spec.view for spec in TABLE_SPECS} - names)
    if missing:
        raise RuntimeError(f"Destination v2 database is not ready yet; missing views: {', '.join(missing)}")


def _sample_ids(conn: sqlite3.Connection, table: str, id_col: str, sample_count: int, seed: int) -> list[str]:
    if sample_count <= 0:
        return []
    min_rowid, max_rowid = conn.execute(f"SELECT MIN(rowid), MAX(rowid) FROM {table}").fetchone()
    if min_rowid is None or max_rowid is None:
        return []
    rng = random.Random(seed)
    sampled: list[str] = []
    seen: set[str] = set()
    attempts = 0
    max_attempts = max(sample_count * 20, 100)
    while len(sampled) < sample_count and attempts < max_attempts:
        attempts += 1
        target = rng.randint(int(min_rowid), int(max_rowid))
        row = conn.execute(
            f"SELECT {id_col} FROM {table} WHERE rowid >= ? ORDER BY rowid LIMIT 1",
            (target,),
        ).fetchone()
        if row is None:
            row = conn.execute(
                f"SELECT {id_col} FROM {table} WHERE rowid < ? ORDER BY rowid DESC LIMIT 1",
                (target,),
            ).fetchone()
        if row is None:
            continue
        value = str(row[0])
        if value in seen:
            continue
        seen.add(value)
        sampled.append(value)
    if len(sampled) < sample_count:
        for row in conn.execute(f"SELECT {id_col} FROM {table} ORDER BY rowid"):
            value = str(row[0])
            if value in seen:
                continue
            seen.add(value)
            sampled.append(value)
            if len(sampled) >= sample_count:
                break
    if len(sampled) < sample_count:
        raise RuntimeError(f"Unable to sample {sample_count} unique ids from {table}; only got {len(sampled)}")
    return sampled


def _table_has_rows(conn: sqlite3.Connection, table: str) -> bool:
    return conn.execute(f"SELECT 1 FROM {table} LIMIT 1").fetchone() is not None


def _table_sample_capacity(conn: sqlite3.Connection, table: str, cap: int) -> int:
    row = conn.execute(f"SELECT COUNT(*) FROM (SELECT 1 FROM {table} LIMIT ?)", (cap,)).fetchone()
    return int(row[0]) if row is not None else 0


def _sample_plan(src: sqlite3.Connection, sample_size: int) -> list[tuple[TableSpec, int]]:
    capacities = [
        (spec, _table_sample_capacity(src, spec.table, sample_size))
        for spec in TABLE_SPECS
    ]
    nonempty = [(spec, capacity) for spec, capacity in capacities if capacity > 0]
    if not nonempty:
        raise RuntimeError("Source database has no sampleable tables")
    remaining = min(sample_size, sum(capacity for _, capacity in nonempty))
    plan: list[tuple[TableSpec, int]] = []
    pending = nonempty[:]
    while remaining > 0 and pending:
        base = remaining // len(pending)
        remainder = remaining % len(pending)
        next_pending: list[tuple[TableSpec, int]] = []
        allocated_this_round = 0
        for idx, (spec, capacity) in enumerate(pending):
            requested = base + (1 if idx < remainder else 0)
            requested = min(requested, capacity)
            if requested > 0:
                plan.append((spec, requested))
                allocated_this_round += requested
            leftover = capacity - requested
            if leftover > 0:
                next_pending.append((spec, leftover))
        if allocated_this_round == 0:
            break
        remaining -= allocated_this_round
        pending = next_pending
    return plan


def _behavior_expected_from_view(view_row: sqlite3.Row) -> dict[str, Any]:
    parsed = atlas_id_codec.BehaviorSliceId.parse(view_row["slice_id"])
    input_symbol = _input_symbol_from_value(parsed.mode, parsed.value)
    return {
        "slice_id": view_row["slice_id"],
        "network_id": view_row["network_id"],
        "graph_slice_id": view_row["graph_slice_id"],
        "input_symbol": input_symbol,
        "change_signature": parsed.value,
        "output_symbol": view_row["output_symbol"],
        "analysis_status": view_row["analysis_status"],
        "path_scope": parsed.cfg.typed_fields["scope"],
        "min_volume_mean": parsed.cfg.typed_fields["min_volume_mean"],
        "total_paths": view_row["total_paths"],
        "feasible_paths": view_row["feasible_paths"],
        "included_paths": view_row["included_paths"],
        "excluded_paths": view_row["excluded_paths"],
        "motif_union_json": _value_or_json(view_row["motif_union_json"]) or [],
        "exact_union_json": _value_or_json(view_row["exact_union_json"]) or [],
        "classifier_config_json": _classifier_config_json(parsed.cfg),
    }


def _verify_behavior_slice(src: sqlite3.Connection, dst: sqlite3.Connection, slice_id: str) -> None:
    raw = _fetch_row(src, "SELECT * FROM behavior_slices WHERE slice_id = ?", (slice_id,))
    view = _fetch_row(dst, "SELECT * FROM v_bs WHERE slice_id = ?", (slice_id,))
    expected = _behavior_expected_from_view(view)
    raw_keys = set(raw.keys())
    for key in [
        "slice_id",
        "network_id",
        "graph_slice_id",
        "input_symbol",
        "change_signature",
        "output_symbol",
        "analysis_status",
        "path_scope",
        "min_volume_mean",
        "total_paths",
        "feasible_paths",
        "included_paths",
        "excluded_paths",
    ]:
        if key in raw_keys:
            assert raw[key] == expected[key]
    if "motif_union_json" in raw_keys:
        assert _compare_jsonish(_json_load(raw["motif_union_json"]) or [], expected["motif_union_json"])
    if "exact_union_json" in raw_keys:
        assert _compare_jsonish(_json_load(raw["exact_union_json"]) or [], expected["exact_union_json"])
    if "classifier_config_json" in raw_keys:
        assert _compare_jsonish(_json_load(raw["classifier_config_json"]), expected["classifier_config_json"])


def _verify_regime_record(src: sqlite3.Connection, dst: sqlite3.Connection, record_id: str) -> None:
    raw = _fetch_row(src, "SELECT * FROM regime_records WHERE regime_record_id = ?", (record_id,))
    view = _fetch_row(dst, "SELECT * FROM v_rr WHERE regime_record_id = ?", (record_id,))
    parsed = atlas_id_codec.RegimeRecordId.parse(record_id)
    input_symbol = _input_symbol_from_value(parsed.slice.mode, parsed.slice.value)
    raw_keys = set(raw.keys())
    expected = {
        "regime_record_id": view["regime_record_id"],
        "slice_id": view["slice_id"],
        "graph_slice_id": view["graph_slice_id"],
        "network_id": view["network_id"],
        "input_symbol": input_symbol,
        "change_signature": parsed.slice.value,
        "output_symbol": view["output_symbol"],
        "vertex_idx": view["vertex_idx"],
        "role": _recover_role_text(dst, "rr", "rp", view["rp"], view["role_code"], "role"),
        "singular": view["singular"],
        "nullity": view["nullity"],
        "asymptotic": view["asymptotic"],
        "output_order_token": view["output_order_token"],
    }
    for key, value in expected.items():
        if key in raw_keys:
            assert raw[key] == value


def _verify_family_bucket(src: sqlite3.Connection, dst: sqlite3.Connection, bucket_id: str) -> None:
    raw = _fetch_row(src, "SELECT * FROM family_buckets WHERE bucket_id = ?", (bucket_id,))
    view = _fetch_row(dst, "SELECT * FROM v_fb WHERE bucket_id = ?", (bucket_id,))
    raw_keys = set(raw.keys())
    expected = {
        "bucket_id": view["bucket_id"],
        "slice_id": view["slice_id"],
        "graph_slice_id": view["graph_slice_id"],
        "network_id": view["network_id"],
        "family_kind": FAMILY_KIND_CODE_TO_TEXT[view["family_kind_code"]],
        "family_label": view["family_label"],
        "parent_motif": view["parent_motif"],
        "path_count": view["path_count"],
        "robust_path_count": view["robust_path_count"],
        "volume_mean": view["volume_mean"],
        "representative_path_idx": view["representative_path_idx"],
    }
    for key, value in expected.items():
        if key in raw_keys:
            assert raw[key] == value


def _verify_transition_record(src: sqlite3.Connection, dst: sqlite3.Connection, record_id: str) -> None:
    raw = _fetch_row(src, "SELECT * FROM transition_records WHERE transition_record_id = ?", (record_id,))
    view = _fetch_row(dst, "SELECT * FROM v_tr WHERE transition_record_id = ?", (record_id,))
    parsed = atlas_id_codec.TransitionRecordId.parse(record_id)
    input_symbol = _input_symbol_from_value(parsed.slice.mode, parsed.slice.value)
    raw_keys = set(raw.keys())
    expected = {
        "transition_record_id": view["transition_record_id"],
        "slice_id": view["slice_id"],
        "graph_slice_id": view["graph_slice_id"],
        "input_symbol": input_symbol,
        "change_signature": parsed.slice.value,
        "output_symbol": view["output_symbol"],
        "from_vertex_idx": view["from_vertex_idx"],
        "to_vertex_idx": view["to_vertex_idx"],
        "from_role": _recover_role_text(dst, "tr", "tp", view["tp"], view["from_role_code"], "from_role"),
        "to_role": _recover_role_text(dst, "tr", "tp", view["tp"], view["to_role_code"], "to_role"),
        "from_output_order_token": view["from_output_order_token"],
        "to_output_order_token": view["to_output_order_token"],
        "transition_token": view["transition_token"],
    }
    for key, value in expected.items():
        if key in raw_keys:
            assert raw[key] == value


def _verify_path_record(src: sqlite3.Connection, dst: sqlite3.Connection, record_id: str) -> None:
    raw = _fetch_row(src, "SELECT * FROM path_records WHERE path_record_id = ?", (record_id,))
    view = _fetch_row(dst, "SELECT * FROM v_pr WHERE path_record_id = ?", (record_id,))
    parsed = atlas_id_codec.PathRecordId.parse(record_id)
    input_symbol = _input_symbol_from_value(parsed.slice.mode, parsed.slice.value)
    raw_keys = set(raw.keys())
    expected = {
        "path_record_id": view["path_record_id"],
        "slice_id": view["slice_id"],
        "graph_slice_id": view["graph_slice_id"],
        "network_id": view["network_id"],
        "input_symbol": input_symbol,
        "change_signature": parsed.slice.value,
        "output_symbol": parsed.slice.output_symbol,
        "path_idx": view["path_idx"],
        "path_length": view["path_length"],
        "exact_label": view["exact_label"],
        "motif_label": view["motif_label"],
        "feasible": view["feasible"],
        "robust": view["robust"],
        "volume_mean": view["volume_mean"],
    }
    for key, value in expected.items():
        if key in raw_keys:
            assert raw[key] == value
    if "output_order_tokens_json" in raw_keys:
        assert _compare_jsonish(_json_load(raw["output_order_tokens_json"]) or [], _value_or_json(view["output_order_tokens_json"]) or [])
    if "transition_tokens_json" in raw_keys:
        assert _compare_jsonish(_json_load(raw["transition_tokens_json"]) or [], _value_or_json(view["transition_tokens_json"]) or [])


VERIFY_DISPATCH = {
    "behavior_slices": _verify_behavior_slice,
    "regime_records": _verify_regime_record,
    "family_buckets": _verify_family_bucket,
    "transition_records": _verify_transition_record,
    "path_records": _verify_path_record,
}


def verify_sample(src_db: Path, dst_db: Path, sample_size: int, seed: int) -> dict[str, Any]:
    src = sqlite3.connect(f"file:{src_db}?mode=ro", uri=True)
    dst = sqlite3.connect(dst_db)
    src.row_factory = sqlite3.Row
    dst.row_factory = sqlite3.Row
    report: dict[str, Any] = {
        "src_db": str(src_db),
        "dst_db": str(dst_db),
        "sample_size": sample_size,
        "seed": seed,
        "status": "passed",
        "sampled_tables": [],
        "checked_count": 0,
        "failures": [],
    }
    try:
        _require_views(dst)
        plan = _sample_plan(src, sample_size)
        for plan_idx, (spec, count) in enumerate(plan):
            sampled_ids = _sample_ids(src, spec.table, spec.id_col, count, seed + 1000 * (plan_idx + 1))
            checked_ids: list[str] = []
            verifier = VERIFY_DISPATCH[spec.table]
            for item_id in sampled_ids:
                try:
                    verifier(src, dst, item_id)
                except Exception as exc:  # noqa: BLE001
                    report["status"] = "failed"
                    report["failures"].append(
                        {
                            "table": spec.table,
                            "id": item_id,
                            "error": f"{type(exc).__name__}: {exc}",
                        }
                    )
                    break
                checked_ids.append(item_id)
                report["checked_count"] += 1
            report["sampled_tables"].append(
                {
                    "table": spec.table,
                    "view": spec.view,
                    "requested_count": count,
                    "checked_count": len(checked_ids),
                    "sampled_ids": checked_ids,
                }
            )
            if report["status"] != "passed":
                break
        return report
    finally:
        src.close()
        dst.close()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Verify that sampled raw atlas rows are losslessly recoverable from the v2 readable views."
    )
    parser.add_argument("--src-db", required=True)
    parser.add_argument("--dst-db", required=True)
    parser.add_argument("--sample-size", type=int, default=100)
    parser.add_argument("--seed", type=int, default=20260419)
    parser.add_argument("--report-out", default=None)
    return parser


def main() -> None:
    args = _build_parser().parse_args()
    report = verify_sample(Path(args.src_db), Path(args.dst_db), args.sample_size, args.seed)
    rendered = json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True)
    if args.report_out:
        Path(args.report_out).write_text(rendered + "\n", encoding="utf-8")
    print(rendered)
    if report["status"] != "passed":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
