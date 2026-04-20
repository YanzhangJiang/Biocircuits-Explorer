#!/usr/bin/env python3

from __future__ import annotations

import argparse
import importlib.util
import json
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


ROLE_CODE = {
    "source": 1,
    "sink": 2,
    "interior": 3,
    "branch": 4,
    "merge": 5,
    "source_sink": 6,
    "branch_merge": 7,
}

FAMILY_KIND_CODE = {
    "exact": 1,
    "motif": 2,
}

PATH_SCOPE_CODE = {
    "all": 1,
    "feasible": 2,
    "robust": 3,
}

GRAPH_KIND_CODE = {
    "input": 1,
    "axis": 2,
    "orthant": 3,
    "other": 99,
}

OUTPUT_KIND_CODE = {
    "symbol": 1,
    "complex": 2,
    "other": 99,
}

OUTPUT_TOKEN_KIND_CODE = {
    "scalar": 1,
    "vector": 2,
}

LABEL_KIND_CODE = {
    "named": 1,
    "exact_seq": 2,
    "vector_seq": 3,
}


def _parse_scalar(value: str) -> Any:
    lowered = value.lower()
    if lowered == "true":
        return True
    if lowered == "false":
        return False
    try:
        if any(ch in value for ch in (".", "e", "E")):
            return float(value)
        return int(value)
    except ValueError:
        return value


def _compress_residual(payload: Any) -> bytes | None:
    if payload is None:
        return None
    if isinstance(payload, dict) and not payload:
        return None
    encoded = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode("utf-8")
    if not encoded or encoded == b"{}":
        return None
    return zlib.compress(encoded, level=9)


def _json_load(raw: str | None) -> dict[str, Any]:
    if raw is None or raw == "":
        return {}
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _residual_from_record_json(raw_json: str | None, drop_keys: set[str]) -> bytes | None:
    payload = _json_load(raw_json)
    for key in drop_keys:
        payload.pop(key, None)
    return _compress_residual(payload)


def _parse_change_signature(raw: str) -> tuple[int, list[str], int]:
    raw = str(raw)
    if raw.startswith("orthant(") and raw.endswith(")"):
        inner = raw[len("orthant("):-1]
        parts = [part.strip() for part in inner.split(",") if part.strip()]
        symbols: list[str] = []
        sign_mask = 0
        for idx, part in enumerate(parts):
            sign = part[0] if part and part[0] in "+-" else "+"
            symbol = part[1:] if part and part[0] in "+-" else part
            symbols.append(symbol)
            if sign == "-":
                sign_mask |= (1 << idx)
        return GRAPH_KIND_CODE["orthant"], symbols, sign_mask

    if raw.startswith("axis(") and raw.endswith(")"):
        inner = raw[len("axis("):-1].strip()
        sign = inner[0] if inner and inner[0] in "+-" else "+"
        symbol = inner[1:] if inner and inner[0] in "+-" else inner
        sign_mask = 1 if sign == "-" else 0
        return GRAPH_KIND_CODE["axis"], [symbol], sign_mask

    return GRAPH_KIND_CODE["input"], [raw], 0


def _parse_output_symbol(raw: str) -> tuple[int, list[str]]:
    raw = str(raw)
    if raw.startswith("C_"):
        return OUTPUT_KIND_CODE["complex"], [part for part in raw.split("_")[1:] if part]
    if raw:
        return OUTPUT_KIND_CODE["symbol"], [raw]
    return OUTPUT_KIND_CODE["other"], [raw]


def _split_arrow_seq(raw: str) -> list[str]:
    import re
    return [part.strip() for part in re.split(r"\s*(?:->|=>|→)\s*", str(raw)) if part.strip()]


def _parse_output_order_token(raw: str) -> tuple[int, list[str]]:
    text = str(raw).strip()
    if text.startswith("(") and text.endswith(")"):
        inner = text[1:-1].strip()
        if not inner:
            return OUTPUT_TOKEN_KIND_CODE["vector"], []
        return OUTPUT_TOKEN_KIND_CODE["vector"], [part.strip() for part in inner.split(",")]
    return OUTPUT_TOKEN_KIND_CODE["scalar"], [text]


@dataclass
class V2State:
    np_by_raw: dict[str, int]
    sy_by_text: dict[str, int]
    gc_by_raw: dict[str, int]
    gv_by_raw: dict[str, int]
    ou_by_raw: dict[str, int]
    c_by_sig: dict[str, int]
    gp_by_raw: dict[str, int]
    sp_by_raw: dict[str, int]
    nm_by_raw: dict[str, int]
    fl_by_raw: dict[str, int]
    oa_by_raw: dict[str, int]
    ot_by_raw: dict[str, int]
    next_np: int = 1
    next_sy: int = 1
    next_gc: int = 1
    next_gv: int = 1
    next_ou: int = 1
    next_c: int = 1
    next_gp: int = 1
    next_sp: int = 1
    next_nm: int = 1
    next_fl: int = 1
    next_oa: int = 1
    next_ot: int = 1


def _new_state() -> V2State:
    return V2State({}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {})


def _intern_symbol(cur: sqlite3.Cursor, state: V2State, text: str) -> int:
    existing = state.sy_by_text.get(text)
    if existing is not None:
        return existing
    sy = state.next_sy
    state.next_sy += 1
    state.sy_by_text[text] = sy
    cur.execute("INSERT INTO sy (sy, t) VALUES (?, ?)", (sy, text))
    return sy


def _intern_network(cur: sqlite3.Cursor, state: V2State, raw: str) -> int:
    existing = state.np_by_raw.get(raw)
    if existing is not None:
        return existing
    np = state.next_np
    state.next_np += 1
    state.np_by_raw[raw] = np
    cur.execute("INSERT INTO nw (np, raw) VALUES (?, ?)", (np, raw))
    return np


def _intern_graph_cfg(cur: sqlite3.Cursor, state: V2State, raw: str) -> int:
    existing = state.gc_by_raw.get(raw)
    if existing is not None:
        return existing
    gc = state.next_gc
    state.next_gc += 1
    state.gc_by_raw[raw] = gc
    cur.execute("INSERT INTO gcfg (gc, raw) VALUES (?, ?)", (gc, raw))
    return gc


def _intern_graph_value(cur: sqlite3.Cursor, state: V2State, raw: str) -> int:
    existing = state.gv_by_raw.get(raw)
    if existing is not None:
        return existing
    gk, symbols, sign_mask = _parse_change_signature(raw)
    sy_ids = [_intern_symbol(cur, state, sym) for sym in symbols]
    gv = state.next_gv
    state.next_gv += 1
    state.gv_by_raw[raw] = gv
    cur.execute(
        "INSERT INTO gv (gv, gk, syj, sg) VALUES (?, ?, ?, ?)",
        (gv, gk, json.dumps(sy_ids, separators=(",", ":")), sign_mask),
    )
    return gv


def _intern_output(cur: sqlite3.Cursor, state: V2State, raw: str) -> int:
    existing = state.ou_by_raw.get(raw)
    if existing is not None:
        return existing
    ok, symbols = _parse_output_symbol(raw)
    sy_ids = [_intern_symbol(cur, state, sym) for sym in symbols]
    o = state.next_ou
    state.next_ou += 1
    state.ou_by_raw[raw] = o
    cur.execute(
        "INSERT INTO ou (o, ok, syj) VALUES (?, ?, ?)",
        (o, ok, json.dumps(sy_ids, separators=(",", ":"))),
    )
    return o


def _intern_cfg(cur: sqlite3.Cursor, state: V2State, signature: str) -> int:
    existing = state.c_by_sig.get(signature)
    if existing is not None:
        return existing
    parsed = atlas_id_codec.ClassifierConfig.parse(signature)
    raw_fields: dict[str, str] = {}
    for part in signature.split(";"):
        key, value = part.split("=", 1)
        raw_fields[key] = value
    c = state.next_c
    state.next_c += 1
    state.c_by_sig[signature] = c
    cur.execute(
        "INSERT INTO cf (c, ps, mv, dd, ks, kn, cv, mz, mvs, mzs) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            c,
            PATH_SCOPE_CODE.get(str(parsed.typed_fields["scope"]), 99),
            float(parsed.typed_fields["min_volume_mean"]),
            int(bool(parsed.typed_fields["deduplicate"])),
            int(bool(parsed.typed_fields["keep_singular"])),
            int(bool(parsed.typed_fields["keep_nonasymptotic"])),
            int(bool(parsed.typed_fields["compute_volume"])),
            float(parsed.typed_fields["motif_zero_tol"]),
            raw_fields["min_volume_mean"],
            raw_fields["motif_zero_tol"],
        ),
    )
    return c


def _intern_graph_slice(cur: sqlite3.Cursor, state: V2State, raw: str) -> int:
    existing = state.gp_by_raw.get(raw)
    if existing is not None:
        return existing
    parsed = atlas_id_codec.GraphSliceId.parse(raw)
    np = _intern_network(cur, state, parsed.network_id)
    gv = _intern_graph_value(cur, state, parsed.value)
    gc = _intern_graph_cfg(cur, state, parsed.graph_cfg)
    gm = 1 if parsed.mode == "input" else 2
    gp = state.next_gp
    state.next_gp += 1
    state.gp_by_raw[raw] = gp
    cur.execute(
        "INSERT INTO gs (gp, np, gm, gv, gc) VALUES (?, ?, ?, ?, ?)",
        (gp, np, gm, gv, gc),
    )
    return gp


def _intern_named_label(cur: sqlite3.Cursor, state: V2State, raw: str) -> int:
    existing = state.nm_by_raw.get(raw)
    if existing is not None:
        return existing
    nm = state.next_nm
    state.next_nm += 1
    state.nm_by_raw[raw] = nm
    cur.execute("INSERT INTO nm (nm, raw) VALUES (?, ?)", (nm, raw))
    return nm


def _intern_output_atom(cur: sqlite3.Cursor, state: V2State, raw: str) -> int:
    existing = state.oa_by_raw.get(raw)
    if existing is not None:
        return existing
    oa = state.next_oa
    state.next_oa += 1
    state.oa_by_raw[raw] = oa
    cur.execute("INSERT INTO oa (oa, raw) VALUES (?, ?)", (oa, raw))
    return oa


def _intern_family_label(cur: sqlite3.Cursor, state: V2State, raw: str) -> int:
    existing = state.fl_by_raw.get(raw)
    if existing is not None:
        return existing
    fl = state.next_fl
    state.next_fl += 1
    state.fl_by_raw[raw] = fl
    if raw.startswith("vector_motif::"):
        seq = _split_arrow_seq(raw.split("::", 1)[1])
        tok_ids = [_intern_output_token(cur, state, tok) for tok in seq]
        cur.execute(
            "INSERT INTO fl (fl, lk, nm, sj) VALUES (?, ?, ?, ?)",
            (fl, LABEL_KIND_CODE["vector_seq"], None, json.dumps(tok_ids, separators=(",", ":"))),
        )
    elif any(sep in raw for sep in ("→", "->", "=>")):
        seq = _split_arrow_seq(raw)
        tok_ids = [_intern_output_token(cur, state, tok) for tok in seq]
        cur.execute(
            "INSERT INTO fl (fl, lk, nm, sj) VALUES (?, ?, ?, ?)",
            (fl, LABEL_KIND_CODE["exact_seq"], None, json.dumps(tok_ids, separators=(",", ":"))),
        )
    else:
        nm = _intern_named_label(cur, state, raw)
        cur.execute(
            "INSERT INTO fl (fl, lk, nm, sj) VALUES (?, ?, ?, ?)",
            (fl, LABEL_KIND_CODE["named"], nm, None),
        )
    return fl


def _intern_output_token(cur: sqlite3.Cursor, state: V2State, raw: str) -> int:
    existing = state.ot_by_raw.get(raw)
    if existing is not None:
        return existing
    tk, atoms = _parse_output_order_token(raw)
    atom_ids = [_intern_output_atom(cur, state, atom) for atom in atoms]
    ot = state.next_ot
    state.next_ot += 1
    state.ot_by_raw[raw] = ot
    cur.execute(
        "INSERT INTO ot (ot, tk, aj) VALUES (?, ?, ?)",
        (ot, tk, json.dumps(atom_ids, separators=(",", ":"))),
    )
    return ot


def _create_schema(cur: sqlite3.Cursor) -> None:
    cur.executescript(
        """
        PRAGMA journal_mode=WAL;
        PRAGMA synchronous=NORMAL;
        PRAGMA temp_store=MEMORY;

        CREATE TABLE meta (
            k TEXT PRIMARY KEY,
            v TEXT NOT NULL
        );

        CREATE TABLE nw (
            np INTEGER PRIMARY KEY,
            raw TEXT NOT NULL UNIQUE
        );

        CREATE TABLE sy (
            sy INTEGER PRIMARY KEY,
            t TEXT NOT NULL UNIQUE
        );

        CREATE TABLE gcfg (
            gc INTEGER PRIMARY KEY,
            raw TEXT NOT NULL UNIQUE
        );

        CREATE TABLE gv (
            gv INTEGER PRIMARY KEY,
            gk INTEGER NOT NULL,
            syj TEXT NOT NULL,
            sg INTEGER NOT NULL
        );

        CREATE TABLE ou (
            o INTEGER PRIMARY KEY,
            ok INTEGER NOT NULL,
            syj TEXT NOT NULL
        );

        CREATE TABLE cf (
            c INTEGER PRIMARY KEY,
            ps INTEGER NOT NULL,
            mv REAL NOT NULL,
            dd INTEGER NOT NULL,
            ks INTEGER NOT NULL,
            kn INTEGER NOT NULL,
            cv INTEGER NOT NULL,
            mz REAL NOT NULL,
            mvs TEXT NOT NULL,
            mzs TEXT NOT NULL
        );

        CREATE TABLE nm (
            nm INTEGER PRIMARY KEY,
            raw TEXT NOT NULL UNIQUE
        );

        CREATE TABLE oa (
            oa INTEGER PRIMARY KEY,
            raw TEXT NOT NULL UNIQUE
        );

        CREATE TABLE fl (
            fl INTEGER PRIMARY KEY,
            lk INTEGER NOT NULL,
            nm INTEGER,
            sj TEXT
        );

        CREATE TABLE ot (
            ot INTEGER PRIMARY KEY,
            tk INTEGER NOT NULL,
            aj TEXT NOT NULL
        );

        CREATE TABLE gs (
            gp INTEGER PRIMARY KEY,
            np INTEGER NOT NULL,
            gm INTEGER NOT NULL,
            gv INTEGER NOT NULL,
            gc INTEGER NOT NULL
        );

        CREATE TABLE bs (
            sp INTEGER PRIMARY KEY,
            np INTEGER NOT NULL,
            gp INTEGER NOT NULL,
            sm INTEGER NOT NULL,
            gv INTEGER NOT NULL,
            o INTEGER NOT NULL,
            c INTEGER NOT NULL,
            st TEXT NOT NULL,
            tp INTEGER NOT NULL,
            fp INTEGER NOT NULL,
            ip INTEGER NOT NULL,
            xp INTEGER NOT NULL,
            mu TEXT,
            eu TEXT,
            rj BLOB
        );

        CREATE TABLE rr (
            rp INTEGER PRIMARY KEY,
            sp INTEGER NOT NULL,
            v INTEGER NOT NULL,
            rc INTEGER NOT NULL,
            sg INTEGER NOT NULL,
            nl INTEGER NOT NULL,
            ay INTEGER NOT NULL,
            ot INTEGER NOT NULL,
            rj BLOB
        );

        CREATE TABLE fb (
            bp INTEGER PRIMARY KEY,
            sp INTEGER NOT NULL,
            fk INTEGER NOT NULL,
            fi INTEGER NOT NULL,
            fl INTEGER NOT NULL,
            pml INTEGER,
            pc INTEGER NOT NULL,
            rc INTEGER NOT NULL,
            vm REAL,
            rpi INTEGER,
            rj BLOB
        );

        CREATE TABLE tr (
            tp INTEGER PRIMARY KEY,
            sp INTEGER NOT NULL,
            fv INTEGER NOT NULL,
            tv INTEGER NOT NULL,
            fr INTEGER,
            tor INTEGER,
            fo INTEGER,
            uo INTEGER,
            rj BLOB
        );

        CREATE TABLE pr (
            pp INTEGER PRIMARY KEY,
            sp INTEGER NOT NULL,
            pi INTEGER NOT NULL,
            pl INTEGER NOT NULL,
            el INTEGER,
            ml INTEGER,
            fe INTEGER NOT NULL,
            rb INTEGER NOT NULL,
            vm REAL,
            oj TEXT,
            rj BLOB
        );

        CREATE INDEX idx_bs_np ON bs (np);
        CREATE INDEX idx_bs_o ON bs (o);
        CREATE INDEX idx_bs_c ON bs (c);
        CREATE INDEX idx_rr_sp ON rr (sp);
        CREATE INDEX idx_rr_ot ON rr (ot);
        CREATE INDEX idx_fb_sp ON fb (sp);
        CREATE INDEX idx_fb_fk ON fb (fk);
        CREATE INDEX idx_fb_fl ON fb (fl);
        CREATE INDEX idx_tr_sp ON tr (sp);
        CREATE INDEX idx_pr_sp ON pr (sp);
        """
    )


def _create_views(cur: sqlite3.Cursor) -> None:
    cur.executescript(
        """
        CREATE VIEW v_gv AS
        SELECT
            gv.gv,
            CASE gv.gk
                WHEN 1 THEN (
                    SELECT sy.t
                    FROM json_each(gv.syj) je
                    JOIN sy ON sy.sy = je.value
                    ORDER BY CAST(je.key AS INTEGER)
                    LIMIT 1
                )
                WHEN 2 THEN 'axis(' || (
                    SELECT
                        CASE
                            WHEN ((gv.sg >> CAST(je.key AS INTEGER)) & 1) = 1 THEN '-'
                            ELSE '+'
                        END || sy.t
                    FROM json_each(gv.syj) je
                    JOIN sy ON sy.sy = je.value
                    ORDER BY CAST(je.key AS INTEGER)
                    LIMIT 1
                ) || ')'
                WHEN 3 THEN 'orthant(' || (
                    SELECT group_concat(part, ',')
                    FROM (
                        SELECT
                            CASE
                                WHEN ((gv.sg >> CAST(je.key AS INTEGER)) & 1) = 1 THEN '-'
                                ELSE '+'
                            END || sy.t AS part
                        FROM json_each(gv.syj) je
                        JOIN sy ON sy.sy = je.value
                        ORDER BY CAST(je.key AS INTEGER)
                    )
                ) || ')'
                ELSE (
                    SELECT group_concat(part, ',')
                    FROM (
                        SELECT sy.t AS part
                        FROM json_each(gv.syj) je
                        JOIN sy ON sy.sy = je.value
                        ORDER BY CAST(je.key AS INTEGER)
                    )
                )
            END AS txt
        FROM gv;

        CREATE VIEW v_ou AS
        SELECT
            ou.o,
            CASE ou.ok
                WHEN 1 THEN (
                    SELECT sy.t
                    FROM json_each(ou.syj) je
                    JOIN sy ON sy.sy = je.value
                    ORDER BY CAST(je.key AS INTEGER)
                    LIMIT 1
                )
                WHEN 2 THEN 'C_' || (
                    SELECT group_concat(part, '_')
                    FROM (
                        SELECT sy.t AS part
                        FROM json_each(ou.syj) je
                        JOIN sy ON sy.sy = je.value
                        ORDER BY CAST(je.key AS INTEGER)
                    )
                )
                ELSE (
                    SELECT group_concat(part, ',')
                    FROM (
                        SELECT sy.t AS part
                        FROM json_each(ou.syj) je
                        JOIN sy ON sy.sy = je.value
                        ORDER BY CAST(je.key AS INTEGER)
                    )
                )
            END AS txt
        FROM ou;

        CREATE VIEW v_cf AS
        SELECT
            cf.c,
            'scope=' ||
                CASE cf.ps
                    WHEN 1 THEN 'all'
                    WHEN 2 THEN 'feasible'
                    WHEN 3 THEN 'robust'
                    ELSE 'unknown'
                END ||
                ';min_volume_mean=' || cf.mvs ||
                ';deduplicate=' || CASE cf.dd WHEN 1 THEN 'true' ELSE 'false' END ||
                ';keep_singular=' || CASE cf.ks WHEN 1 THEN 'true' ELSE 'false' END ||
                ';keep_nonasymptotic=' || CASE cf.kn WHEN 1 THEN 'true' ELSE 'false' END ||
                ';compute_volume=' || CASE cf.cv WHEN 1 THEN 'true' ELSE 'false' END ||
                ';motif_zero_tol=' || cf.mzs AS sig
        FROM cf;

        CREATE VIEW v_ot AS
        SELECT
            ot.ot,
            CASE ot.tk
                WHEN 1 THEN (
                    SELECT oa.raw
                    FROM json_each(ot.aj) je
                    JOIN oa ON oa.oa = je.value
                    ORDER BY CAST(je.key AS INTEGER)
                    LIMIT 1
                )
                WHEN 2 THEN '(' || (
                    SELECT group_concat(part, ',')
                    FROM (
                        SELECT oa.raw AS part
                        FROM json_each(ot.aj) je
                        JOIN oa ON oa.oa = je.value
                        ORDER BY CAST(je.key AS INTEGER)
                    )
                ) || ')'
                ELSE NULL
            END AS txt
        FROM ot;

        CREATE VIEW v_fl AS
        SELECT
            fl.fl,
            CASE fl.lk
                WHEN 1 THEN nm.raw
                WHEN 2 THEN (
                    SELECT group_concat(tok, ' → ')
                    FROM (
                        SELECT v_ot.txt AS tok
                        FROM json_each(fl.sj) je
                        JOIN v_ot ON v_ot.ot = je.value
                        ORDER BY CAST(je.key AS INTEGER)
                    )
                )
                WHEN 3 THEN 'vector_motif::' || (
                    SELECT group_concat(tok, ' -> ')
                    FROM (
                        SELECT v_ot.txt AS tok
                        FROM json_each(fl.sj) je
                        JOIN v_ot ON v_ot.ot = je.value
                        ORDER BY CAST(je.key AS INTEGER)
                    )
                )
                ELSE NULL
            END AS txt
        FROM fl
        LEFT JOIN nm ON nm.nm = fl.nm;

        CREATE VIEW v_gs AS
        SELECT
            gs.gp,
            nw.raw || '::' ||
                CASE gs.gm
                    WHEN 1 THEN 'graph_input=' || v_gv.txt
                    ELSE 'graph_change=' || v_gv.txt
                END ||
                '::graphcfg=' || gcfg.raw AS graph_slice_id
        FROM gs
        JOIN nw ON nw.np = gs.np
        JOIN v_gv ON v_gv.gv = gs.gv
        JOIN gcfg ON gcfg.gc = gs.gc;

        CREATE VIEW v_bs AS
        SELECT
            bs.sp,
            nw.raw || '::' ||
                CASE bs.sm
                    WHEN 1 THEN 'input=' || v_gv.txt
                    ELSE 'change=' || v_gv.txt
                END ||
                '::output=' || v_ou.txt ||
                '::cfg=' || v_cf.sig AS slice_id,
            nw.raw AS network_id,
            v_gs.graph_slice_id,
            v_gv.txt AS change_signature,
            v_ou.txt AS output_symbol,
            bs.st AS analysis_status,
            bs.tp AS total_paths,
            bs.fp AS feasible_paths,
            bs.ip AS included_paths,
            bs.xp AS excluded_paths,
            (
                SELECT json_group_array(lbl)
                FROM (
                    SELECT v_fl.txt AS lbl
                    FROM json_each(bs.mu) je
                    JOIN v_fl ON v_fl.fl = je.value
                    ORDER BY CAST(je.key AS INTEGER)
                )
            ) AS motif_union_json,
            (
                SELECT json_group_array(lbl)
                FROM (
                    SELECT v_fl.txt AS lbl
                    FROM json_each(bs.eu) je
                    JOIN v_fl ON v_fl.fl = je.value
                    ORDER BY CAST(je.key AS INTEGER)
                )
            ) AS exact_union_json
        FROM bs
        JOIN nw ON nw.np = bs.np
        JOIN v_gs ON v_gs.gp = bs.gp
        JOIN v_gv ON v_gv.gv = bs.gv
        JOIN v_ou ON v_ou.o = bs.o
        JOIN v_cf ON v_cf.c = bs.c;

        CREATE VIEW v_rr AS
        SELECT
            rr.rp,
            v_bs.slice_id || '::regime::' || rr.v AS regime_record_id,
            v_bs.slice_id,
            v_bs.graph_slice_id,
            v_bs.network_id,
            v_bs.change_signature,
            v_bs.output_symbol,
            rr.v AS vertex_idx,
            rr.rc AS role_code,
            rr.sg AS singular,
            rr.nl AS nullity,
            rr.ay AS asymptotic,
            v_ot.txt AS output_order_token
        FROM rr
        JOIN bs ON bs.sp = rr.sp
        JOIN v_bs ON v_bs.sp = bs.sp
        JOIN v_ot ON v_ot.ot = rr.ot;

        CREATE VIEW v_fb AS
        SELECT
            fb.bp,
            v_bs.slice_id || '::' ||
                CASE fb.fk
                    WHEN 1 THEN 'exact'
                    WHEN 2 THEN 'motif'
                    ELSE 'family'
                END ||
                '::' || fb.fi AS bucket_id,
            v_bs.slice_id,
            v_bs.graph_slice_id,
            v_bs.network_id,
            fb.fk AS family_kind_code,
            v_fl.txt AS family_label,
            pml.txt AS parent_motif,
            fb.pc AS path_count,
            fb.rc AS robust_path_count,
            fb.vm AS volume_mean,
            fb.rpi AS representative_path_idx
        FROM fb
        JOIN bs ON bs.sp = fb.sp
        JOIN v_bs ON v_bs.sp = bs.sp
        JOIN v_fl ON v_fl.fl = fb.fl
        LEFT JOIN v_fl AS pml ON pml.fl = fb.pml;

        CREATE VIEW v_tr AS
        SELECT
            tr.tp,
            v_bs.slice_id || '::transition::' || tr.fv || '->' || tr.tv AS transition_record_id,
            v_bs.slice_id,
            v_bs.graph_slice_id,
            v_bs.network_id,
            v_bs.change_signature,
            v_bs.output_symbol,
            tr.fv AS from_vertex_idx,
            tr.tv AS to_vertex_idx,
            tr.fr AS from_role_code,
            tr.tor AS to_role_code,
            fot.txt AS from_output_order_token,
            uot.txt AS to_output_order_token,
            CASE
                WHEN fot.txt IS NOT NULL AND uot.txt IS NOT NULL THEN fot.txt || '->' || uot.txt
                ELSE NULL
            END AS transition_token
        FROM tr
        JOIN bs ON bs.sp = tr.sp
        JOIN v_bs ON v_bs.sp = bs.sp
        LEFT JOIN v_ot AS fot ON fot.ot = tr.fo
        LEFT JOIN v_ot AS uot ON uot.ot = tr.uo;

        CREATE VIEW v_pr AS
        SELECT
            pr.pp,
            v_bs.slice_id || '::path::' || pr.pi AS path_record_id,
            v_bs.slice_id,
            v_bs.graph_slice_id,
            v_bs.network_id,
            pr.pi AS path_idx,
            pr.pl AS path_length,
            efl.txt AS exact_label,
            mfl.txt AS motif_label,
            pr.fe AS feasible,
            pr.rb AS robust,
            pr.vm AS volume_mean,
            (
                SELECT json_group_array(tok)
                FROM (
                    SELECT v_ot.txt AS tok
                    FROM json_each(pr.oj) je
                    JOIN v_ot ON v_ot.ot = je.value
                    ORDER BY CAST(je.key AS INTEGER)
                )
            ) AS output_order_tokens_json,
            (
                SELECT json_group_array(tok)
                FROM (
                    SELECT fot.txt || '->' || tot.txt AS tok
                    FROM json_each(pr.oj) j1
                    JOIN json_each(pr.oj) j2
                      ON CAST(j2.key AS INTEGER) = CAST(j1.key AS INTEGER) + 1
                    JOIN v_ot AS fot ON fot.ot = j1.value
                    JOIN v_ot AS tot ON tot.ot = j2.value
                    ORDER BY CAST(j1.key AS INTEGER)
                )
            ) AS transition_tokens_json
        FROM pr
        JOIN bs ON bs.sp = pr.sp
        JOIN v_bs ON v_bs.sp = bs.sp
        LEFT JOIN v_fl AS efl ON efl.fl = pr.el
        LEFT JOIN v_fl AS mfl ON mfl.fl = pr.ml;
        """
    )


def _iter_behavior_rows(
    src_cur: sqlite3.Cursor,
    where_slice_like: str | None,
    limit_slices: int | None,
    network_ids: list[str] | None = None,
):
    sql = """
        SELECT
            slice_id, network_id, graph_slice_id, input_symbol, change_signature, output_symbol,
            analysis_status, total_paths, feasible_paths, included_paths, excluded_paths,
            motif_union_json, exact_union_json, record_json
        FROM behavior_slices
    """
    params: list[Any] = []
    clauses: list[str] = []
    if where_slice_like:
        clauses.append("slice_id LIKE ?")
        params.append(where_slice_like)
    if network_ids:
        clauses.append("network_id IN ({})".format(",".join("?" for _ in network_ids)))
        params.extend(network_ids)
    if clauses:
        sql += " WHERE " + " AND ".join(clauses)
    sql += " ORDER BY slice_id"
    if limit_slices is not None:
        sql += f" LIMIT {int(limit_slices)}"
    return src_cur.execute(sql, params)


def _chunked(values: list[str], size: int = 500) -> list[list[str]]:
    return [values[idx:idx + size] for idx in range(0, len(values), size)]


def _migrate_behavior_slices(
    src_cur: sqlite3.Cursor,
    dst_cur: sqlite3.Cursor,
    state: V2State,
    where_slice_like: str | None,
    limit_slices: int | None,
    network_ids: list[str] | None = None,
) -> int:
    count = 0
    for row in _iter_behavior_rows(src_cur, where_slice_like, limit_slices, network_ids):
        slice_id, network_id, graph_slice_id, _input_symbol, _change_signature, output_symbol, status, tp, fp, ip, xp, motif_union_json, exact_union_json, record_json = row
        parsed_slice = atlas_id_codec.BehaviorSliceId.parse(slice_id)

        np = _intern_network(dst_cur, state, network_id)
        gp = _intern_graph_slice(dst_cur, state, graph_slice_id)
        gv = _intern_graph_value(dst_cur, state, parsed_slice.value)
        o = _intern_output(dst_cur, state, output_symbol)
        c = _intern_cfg(dst_cur, state, parsed_slice.cfg.render())
        sm = 1 if parsed_slice.mode == "input" else 2
        sp = state.next_sp
        state.next_sp += 1
        state.sp_by_raw[slice_id] = sp
        motif_union_ids = [_intern_family_label(dst_cur, state, item) for item in json.loads(motif_union_json or "[]")]
        exact_union_ids = [_intern_family_label(dst_cur, state, item) for item in json.loads(exact_union_json or "[]")]

        residual = _residual_from_record_json(record_json, {
            "slice_id", "network_id", "graph_slice_id", "input_symbol", "change_signature", "output_symbol",
            "analysis_status", "path_scope", "min_volume_mean", "total_paths", "feasible_paths", "included_paths",
            "excluded_paths", "motif_union", "motif_union_json", "exact_union", "exact_union_json",
            "classifier_config", "classifier_config_json",
        })

        dst_cur.execute(
            "INSERT INTO bs (sp, np, gp, sm, gv, o, c, st, tp, fp, ip, xp, mu, eu, rj) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                sp,
                np,
                gp,
                sm,
                gv,
                o,
                c,
                status,
                tp,
                fp,
                ip,
                xp,
                json.dumps(motif_union_ids, separators=(",", ":")),
                json.dumps(exact_union_ids, separators=(",", ":")),
                residual,
            ),
        )
        count += 1
    return count


def _migrate_regime_records(src_cur: sqlite3.Cursor, dst_cur: sqlite3.Cursor, state: V2State) -> int:
    count = 0
    slice_ids = list(state.sp_by_raw.keys())
    for batch in _chunked(slice_ids):
        sql = """
            SELECT
                regime_record_id, slice_id, role, singular, nullity, asymptotic, output_order_token, record_json
            FROM regime_records
            WHERE slice_id IN ({})
            ORDER BY regime_record_id
        """.format(",".join("?" for _ in batch))
        for regime_record_id, slice_id, role, singular, nullity, asymptotic, output_order_token, record_json in src_cur.execute(sql, batch):
            sp = state.sp_by_raw[slice_id]
            parsed = atlas_id_codec.RegimeRecordId.parse(regime_record_id)
            ot = _intern_output_token(dst_cur, state, output_order_token)
            rc = ROLE_CODE.get(role, 99)
            drop_keys = {
                "regime_record_id", "slice_id", "graph_slice_id", "network_id", "input_symbol", "change_signature",
                "output_symbol", "vertex_idx", "role", "singular", "nullity", "asymptotic", "output_order_token",
                "output_order_value", "output_order_kind", "output_order_detail", "is_source", "is_sink", "is_branch",
                "is_merge", "indegree", "outdegree", "reachable_from_source", "can_reach_sink", "change_kind",
                "change_label", "change_qk_symbols", "change_qk_indices", "change_qk_signs", "change_qK", "change_qK_idx",
            }
            if rc == 99:
                drop_keys.discard("role")
            residual = _residual_from_record_json(record_json, drop_keys)
            dst_cur.execute(
                "INSERT INTO rr (rp, sp, v, rc, sg, nl, ay, ot, rj) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (count + 1, sp, parsed.vertex_idx, rc, singular, nullity, asymptotic, ot, residual),
            )
            count += 1
    return count


def _migrate_family_buckets(src_cur: sqlite3.Cursor, dst_cur: sqlite3.Cursor, state: V2State) -> int:
    count = 0
    slice_ids = list(state.sp_by_raw.keys())
    for batch in _chunked(slice_ids):
        sql = """
            SELECT
                bucket_id, slice_id, family_kind, family_label, parent_motif, path_count, robust_path_count, volume_mean, representative_path_idx, record_json
            FROM family_buckets
            WHERE slice_id IN ({})
            ORDER BY bucket_id
        """.format(",".join("?" for _ in batch))
        for bucket_id, slice_id, family_kind, family_label, parent_motif, path_count, robust_path_count, volume_mean, representative_path_idx, record_json in src_cur.execute(sql, batch):
            sp = state.sp_by_raw[slice_id]
            parsed = atlas_id_codec.FamilyBucketId.parse(bucket_id)
            fk = FAMILY_KIND_CODE.get(family_kind, 99)
            fl = _intern_family_label(dst_cur, state, family_label)
            pml = _intern_family_label(dst_cur, state, parent_motif) if parent_motif is not None else None
            residual = _residual_from_record_json(record_json, {
                "bucket_id", "slice_id", "graph_slice_id", "network_id", "family_kind", "family_label", "parent_motif",
                "path_count", "robust_path_count", "volume_mean", "representative_path_idx", "representative_path_signature",
                "exact_family_indices", "path_indices", "total_volume",
            })
            dst_cur.execute(
                "INSERT INTO fb (bp, sp, fk, fi, fl, pml, pc, rc, vm, rpi, rj) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (count + 1, sp, fk, parsed.family_idx, fl, pml, path_count, robust_path_count, volume_mean, representative_path_idx, residual),
            )
            count += 1
    return count


def _migrate_transition_records(src_cur: sqlite3.Cursor, dst_cur: sqlite3.Cursor, state: V2State) -> int:
    count = 0
    slice_ids = list(state.sp_by_raw.keys())
    for batch in _chunked(slice_ids):
        sql = """
            SELECT
                transition_record_id, slice_id, from_vertex_idx, to_vertex_idx, from_role, to_role,
                from_output_order_token, to_output_order_token, record_json
            FROM transition_records
            WHERE slice_id IN ({})
            ORDER BY transition_record_id
        """.format(",".join("?" for _ in batch))
        for (
            transition_record_id,
            slice_id,
            from_vertex_idx,
            to_vertex_idx,
            from_role,
            to_role,
            from_output_order_token,
            to_output_order_token,
            record_json,
        ) in src_cur.execute(sql, batch):
            sp = state.sp_by_raw[slice_id]
            parsed = atlas_id_codec.TransitionRecordId.parse(transition_record_id)
            fo = _intern_output_token(dst_cur, state, from_output_order_token) if from_output_order_token is not None else None
            uo = _intern_output_token(dst_cur, state, to_output_order_token) if to_output_order_token is not None else None
            fr = ROLE_CODE.get(from_role, 99) if from_role is not None else None
            tor = ROLE_CODE.get(to_role, 99) if to_role is not None else None
            drop_keys = {
                "transition_record_id", "slice_id", "graph_slice_id", "input_symbol", "change_signature",
                "output_symbol", "from_vertex_idx", "to_vertex_idx", "from_role", "to_role",
                "from_output_order_token", "to_output_order_token", "transition_token", "change_kind",
                "change_label", "change_qk_symbols", "change_qk_indices", "change_qk_signs", "atlas_source_ids",
                "from_nullity", "to_nullity", "from_singular", "to_singular",
            }
            if fr == 99:
                drop_keys.discard("from_role")
            if tor == 99:
                drop_keys.discard("to_role")
            residual = _residual_from_record_json(record_json, drop_keys)
            dst_cur.execute(
                "INSERT INTO tr (tp, sp, fv, tv, fr, tor, fo, uo, rj) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (count + 1, sp, parsed.from_vertex_idx, parsed.to_vertex_idx, fr, tor, fo, uo, residual),
            )
            count += 1
    return count


def _migrate_path_records(src_cur: sqlite3.Cursor, dst_cur: sqlite3.Cursor, state: V2State) -> int:
    count = 0
    slice_ids = list(state.sp_by_raw.keys())
    for batch in _chunked(slice_ids):
        sql = """
            SELECT
                path_record_id, slice_id, path_idx, path_length, exact_label, motif_label,
                feasible, robust, volume_mean, output_order_tokens_json, record_json
            FROM path_records
            WHERE slice_id IN ({})
            ORDER BY path_record_id
        """.format(",".join("?" for _ in batch))
        for (
            path_record_id,
            slice_id,
            path_idx,
            path_length,
            exact_label,
            motif_label,
            feasible,
            robust,
            volume_mean,
            output_order_tokens_json,
            record_json,
        ) in src_cur.execute(sql, batch):
            sp = state.sp_by_raw[slice_id]
            parsed = atlas_id_codec.PathRecordId.parse(path_record_id)
            el = _intern_family_label(dst_cur, state, exact_label) if exact_label is not None else None
            ml = _intern_family_label(dst_cur, state, motif_label) if motif_label is not None else None
            output_order_token_ids = None
            if output_order_tokens_json:
                output_order_tokens = json.loads(output_order_tokens_json)
                output_order_token_ids = [_intern_output_token(dst_cur, state, str(tok)) for tok in output_order_tokens]
            residual = _residual_from_record_json(record_json, {
                "path_record_id", "slice_id", "graph_slice_id", "network_id", "input_symbol", "change_signature",
                "output_symbol", "path_idx", "path_length", "exact_label", "motif_label", "feasible", "robust",
                "volume_mean", "output_order_tokens_json", "transition_tokens_json", "output_order_tokens",
                "transition_tokens", "exact_profile", "motif_profile", "exact_family_idx", "motif_family_idx",
                "vertex_indices", "regime_sequence", "transition_sequence", "included", "feasibility_checked",
                "exclusion_reason", "volume",
            })
            dst_cur.execute(
                "INSERT INTO pr (pp, sp, pi, pl, el, ml, fe, rb, vm, oj, rj) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    count + 1,
                    sp,
                    parsed.path_idx if path_idx is None else path_idx,
                    path_length,
                    el,
                    ml,
                    feasible,
                    robust,
                    volume_mean,
                    json.dumps(output_order_token_ids, separators=(",", ":")) if output_order_token_ids is not None else None,
                    residual,
                ),
            )
            count += 1
    return count


def migrate(
    src_db: Path,
    dst_db: Path,
    where_slice_like: str | None,
    limit_slices: int | None,
    network_ids: list[str] | None = None,
) -> dict[str, int]:
    if dst_db.exists():
        dst_db.unlink()

    src = sqlite3.connect(f"file:{src_db}?mode=ro", uri=True)
    dst = sqlite3.connect(dst_db)
    src.row_factory = sqlite3.Row
    state = _new_state()
    try:
        src_cur = src.cursor()
        dst_cur = dst.cursor()
        _create_schema(dst_cur)

        bs_count = _migrate_behavior_slices(src_cur, dst_cur, state, where_slice_like, limit_slices, network_ids)
        rr_count = _migrate_regime_records(src_cur, dst_cur, state)
        fb_count = _migrate_family_buckets(src_cur, dst_cur, state)
        tr_count = _migrate_transition_records(src_cur, dst_cur, state)
        pr_count = _migrate_path_records(src_cur, dst_cur, state)

        _create_views(dst_cur)
        dst_cur.executemany(
            "INSERT INTO meta (k, v) VALUES (?, ?)",
            [
                ("schema", "atlas_sqlite_v2_lossless_alpha"),
                ("source_db", str(src_db)),
                ("filtered_network_count", str(len(network_ids)) if network_ids is not None else ""),
                ("migrated_behavior_slices", str(bs_count)),
                ("migrated_regime_records", str(rr_count)),
                ("migrated_family_buckets", str(fb_count)),
                ("migrated_transition_records", str(tr_count)),
                ("migrated_path_records", str(pr_count)),
            ],
        )
        dst.commit()
        return {
            "behavior_slices": bs_count,
            "regime_records": rr_count,
            "family_buckets": fb_count,
            "transition_records": tr_count,
            "path_records": pr_count,
            "networks": len(state.np_by_raw),
            "symbols": len(state.sy_by_text),
            "graph_values": len(state.gv_by_raw),
            "outputs": len(state.ou_by_raw),
            "cfgs": len(state.c_by_sig),
            "named_labels": len(state.nm_by_raw),
            "output_atoms": len(state.oa_by_raw),
        }
    finally:
        src.close()
        dst.close()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Losslessly migrate atlas sqlite tables to a short-code v2 schema.")
    parser.add_argument("--src-db", required=True)
    parser.add_argument("--dst-db", required=True)
    parser.add_argument("--where-slice-like", default=None, help="Optional SQL LIKE filter for behavior_slices.slice_id.")
    parser.add_argument("--limit-slices", type=int, default=None, help="Optional limit for behavior_slices migration.")
    parser.add_argument("--network-id-file", default=None, help="Optional newline-delimited file of network_id values to include.")
    return parser


def main() -> None:
    args = _build_parser().parse_args()
    network_ids = None
    if args.network_id_file:
        with open(args.network_id_file, "r", encoding="utf-8") as fh:
            network_ids = [line.strip() for line in fh if line.strip()]
    stats = migrate(Path(args.src_db), Path(args.dst_db), args.where_slice_like, args.limit_slices, network_ids)
    print(json.dumps(stats, ensure_ascii=False, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
