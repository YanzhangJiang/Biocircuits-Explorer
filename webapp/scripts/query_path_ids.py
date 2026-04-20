#!/usr/bin/env python3

from __future__ import annotations

import argparse
import base64
import json
import sqlite3
from dataclasses import dataclass
from typing import Any


PATH_ID_PREFIX = "p3."
BEHAVIOR_CODE_PREFIX = "b2."
LEGACY_BEHAVIOR_CODE_PREFIX = "b1."


def _b64url_encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode("ascii").rstrip("=")


def _b64url_decode(text: str) -> bytes:
    padding = "=" * ((4 - (len(text) % 4)) % 4)
    return base64.urlsafe_b64decode(text + padding)


def _base36_encode(value: int) -> str:
    if value < 0:
        raise ValueError("base36 requires a non-negative integer")
    chars = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    if value == 0:
        return "0"
    out: list[str] = []
    current = value
    while current:
        current, rem = divmod(current, 36)
        out.append(chars[rem])
    return "".join(reversed(out))


def _base36_decode(text: str) -> int:
    return int(text.strip().lower(), 36)


def _write_varuint(out: bytearray, value: int) -> None:
    if value < 0:
        raise ValueError("varuint requires a non-negative integer")
    current = value
    while current >= 0x80:
        out.append((current & 0x7F) | 0x80)
        current >>= 7
    out.append(current)


def _write_bytes(out: bytearray, data: bytes) -> None:
    _write_varuint(out, len(data))
    out.extend(data)


def _write_text(out: bytearray, text: str) -> None:
    _write_bytes(out, text.encode("utf-8"))


def _encode_text_segment(text: str) -> str:
    out = bytearray()
    _write_text(out, text)
    return _b64url_encode(bytes(out))


def _decode_text_segment(segment: str) -> str:
    reader = _Reader(_b64url_decode(segment))
    value = reader.read_text()
    if reader.offset != len(reader.data):
        raise ValueError("unexpected trailing bytes in text segment")
    return value


@dataclass
class _Reader:
    data: bytes
    offset: int = 0

    def read_byte(self) -> int:
        if self.offset >= len(self.data):
            raise ValueError("unexpected end of payload")
        value = self.data[self.offset]
        self.offset += 1
        return value

    def read_varuint(self) -> int:
        shift = 0
        value = 0
        while True:
            byte = self.read_byte()
            value |= (byte & 0x7F) << shift
            if (byte & 0x80) == 0:
                return value
            shift += 7
            if shift > 63:
                raise ValueError("varuint is too large")

    def read_bytes(self) -> bytes:
        length = self.read_varuint()
        end = self.offset + length
        if end > len(self.data):
            raise ValueError("unexpected end of payload")
        value = self.data[self.offset:end]
        self.offset = end
        return value

    def read_text(self) -> str:
        return self.read_bytes().decode("utf-8")


def _parse_canonical_term(term: str) -> list[int]:
    text = term.strip()
    if not (text.startswith("[") and text.endswith("]")):
        raise ValueError(f"unrecognized canonical network term: {text}")
    inner = text[1:-1]
    if not inner:
        return []
    return [int(piece) for piece in inner.split(",")]


def _parse_canonical_side(side: str) -> list[list[int]]:
    text = side.strip()
    if not text:
        return []
    return [_parse_canonical_term(term) for term in text.split("+")]


_BEHAVIOR_TOKEN_CODES: dict[str, int] = {
    "+1": 0,
    "0": 1,
    "-1": 2,
    "+Inf": 3,
    "-Inf": 4,
    "NaN": 5,
}
_BEHAVIOR_TOKEN_CODES_REV = {value: key for key, value in _BEHAVIOR_TOKEN_CODES.items()}


def _zigzag_encode(value: int) -> int:
    return (value << 1) if value >= 0 else ((-value << 1) - 1)


def _zigzag_decode(value: int) -> int:
    return (value >> 1) if (value & 1) == 0 else -((value >> 1) + 1)


def _canonical_behavior_numeric_from_milli(value_milli: int) -> str:
    if value_milli == 0:
        return "0"
    abs_milli = abs(value_milli)
    whole = abs_milli // 1000
    frac = abs_milli % 1000
    if frac == 0:
        body = str(whole)
    else:
        body = f"{whole}.{frac:03d}".rstrip("0")
    return f"+{body}" if value_milli > 0 else f"-{body}"


def _behavior_numeric_milli(token: str) -> int | None:
    text = token.strip()
    try:
        value = float(text)
    except ValueError:
        return None
    if value != value or value in {float("inf"), float("-inf")}:
        return None
    return int(round(round(value, 3) * 1000))


def _encode_network_id(network_id: str) -> bytes:
    out = bytearray()
    reactions = network_id.split("|")
    _write_varuint(out, len(reactions))
    for reaction in reactions:
        parts = reaction.split("<->")
        if len(parts) != 2:
            raise ValueError(f"unrecognized canonical network reaction: {reaction}")
        for side in (_parse_canonical_side(parts[0]), _parse_canonical_side(parts[1])):
            _write_varuint(out, len(side))
            for term in side:
                _write_varuint(out, len(term))
                for atom_idx in term:
                    _write_varuint(out, atom_idx)
    return bytes(out)


def _decode_network_id(reader: _Reader) -> str:
    reactions: list[str] = []
    reaction_count = reader.read_varuint()
    for _ in range(reaction_count):
        sides: list[str] = []
        for _side_idx in range(2):
            terms: list[str] = []
            term_count = reader.read_varuint()
            for _ in range(term_count):
                atom_count = reader.read_varuint()
                atoms = [str(reader.read_varuint()) for _ in range(atom_count)]
                terms.append("[" + ",".join(atoms) + "]")
            sides.append("+".join(terms))
        reactions.append(f"{sides[0]}<->{sides[1]}")
    return "|".join(reactions)


def _parse_behavior_token(token: str) -> tuple[str, list[str]]:
    text = token.strip()
    if text.startswith("(") and text.endswith(")"):
        inner = text[1:-1]
        coords = [] if not inner else [piece.strip() for piece in inner.split(",")]
        return ("vector", coords)
    return ("scalar", [text])


def encode_behavior_code(tokens: list[str]) -> str:
    out = bytearray()
    _write_varuint(out, len(tokens))
    for token in tokens:
        kind, coords = _parse_behavior_token(token)
        if kind == "scalar":
            out.append(0)
            _encode_behavior_atom(out, coords[0])
        else:
            out.append(1)
            _write_varuint(out, len(coords))
            for coord in coords:
                _encode_behavior_atom(out, coord)
    return BEHAVIOR_CODE_PREFIX + _b64url_encode(bytes(out))


def decode_behavior_code(behavior_code: str) -> dict[str, Any]:
    if behavior_code.startswith(LEGACY_BEHAVIOR_CODE_PREFIX):
        return _decode_behavior_code_v1(behavior_code)
    if not behavior_code.startswith(BEHAVIOR_CODE_PREFIX):
        raise ValueError(f"unrecognized behavior code: {behavior_code}")
    reader = _Reader(_b64url_decode(behavior_code[len(BEHAVIOR_CODE_PREFIX) :]))
    token_count = reader.read_varuint()
    tokens: list[str] = []
    for _ in range(token_count):
        kind_code = reader.read_byte()
        if kind_code == 0:
            tokens.append(_decode_behavior_atom(reader))
        elif kind_code == 1:
            coord_count = reader.read_varuint()
            coords = [_decode_behavior_atom(reader) for _ in range(coord_count)]
            tokens.append("(" + ",".join(coords) + ")")
        else:
            raise ValueError(f"unknown behavior token kind: {kind_code}")
    if reader.offset != len(reader.data):
        raise ValueError("unexpected trailing bytes in behavior payload")
    return {
        "behavior_code": behavior_code,
        "output_order_tokens": tokens,
        "path_length": len(tokens),
        "exact_label": " -> ".join(tokens),
        "transition_tokens": [f"{tokens[idx]}->{tokens[idx + 1]}" for idx in range(max(0, len(tokens) - 1))],
        "motif_profile": _motif_profile_from_exact_tokens(tokens),
        "motif_label": _motif_label(_motif_profile_from_exact_tokens(tokens)),
    }


def _encode_behavior_atom(out: bytearray, token: str) -> None:
    text = token.strip()
    if text in _BEHAVIOR_TOKEN_CODES:
        out.append(0)
        out.append(_BEHAVIOR_TOKEN_CODES[text])
        return

    milli = _behavior_numeric_milli(text)
    if milli is not None:
        out.append(1)
        _write_varuint(out, _zigzag_encode(milli))
        return

    out.append(2)
    _write_text(out, text)


def _decode_behavior_atom(reader: _Reader) -> str:
    atom_kind = reader.read_byte()
    if atom_kind == 0:
        return _BEHAVIOR_TOKEN_CODES_REV[reader.read_byte()]
    if atom_kind == 1:
        return _canonical_behavior_numeric_from_milli(_zigzag_decode(reader.read_varuint()))
    if atom_kind == 2:
        return reader.read_text()
    raise ValueError(f"unknown behavior atom kind: {atom_kind}")


def _decode_behavior_code_v1(behavior_code: str) -> dict[str, Any]:
    reader = _Reader(_b64url_decode(behavior_code[len(LEGACY_BEHAVIOR_CODE_PREFIX) :]))
    token_count = reader.read_varuint()
    tokens: list[str] = []
    for _ in range(token_count):
        kind_code = reader.read_byte()
        if kind_code == 0:
            tokens.append(_BEHAVIOR_TOKEN_CODES_REV[reader.read_byte()])
        elif kind_code == 1:
            coord_count = reader.read_varuint()
            coords = [_BEHAVIOR_TOKEN_CODES_REV[reader.read_byte()] for _ in range(coord_count)]
            tokens.append("(" + ",".join(coords) + ")")
        else:
            raise ValueError(f"unknown legacy behavior token kind: {kind_code}")
    if reader.offset != len(reader.data):
        raise ValueError("unexpected trailing bytes in behavior payload")
    return {
        "behavior_code": behavior_code,
        "output_order_tokens": tokens,
        "path_length": len(tokens),
        "exact_label": " -> ".join(tokens),
        "transition_tokens": [f"{tokens[idx]}->{tokens[idx + 1]}" for idx in range(max(0, len(tokens) - 1))],
        "motif_profile": _motif_profile_from_exact_tokens(tokens),
        "motif_label": _motif_label(_motif_profile_from_exact_tokens(tokens)),
    }


def _parse_cfg_signature(raw_signature: str) -> dict[str, str]:
    fields: dict[str, str] = {}
    for segment in raw_signature.split(";"):
        if not segment:
            continue
        key, value = segment.split("=", 1)
        fields[key] = value
    return fields


def _encode_cfg_signature(raw_signature: str) -> bytes:
    fields = _parse_cfg_signature(raw_signature)
    out = bytearray()
    scope = fields.get("scope", "feasible")
    scope_codes = {
        "feasible": 0,
        "all": 1,
        "included": 2,
        "robust": 3,
    }
    if scope in scope_codes:
        out.append(scope_codes[scope])
    else:
        out.append(255)
        _write_text(out, scope)

    flags = 0
    truthy = lambda value: value.strip().lower() in {"1", "true", "yes", "on", "y"}
    if truthy(fields.get("deduplicate", "true")):
        flags |= 0x01
    if truthy(fields.get("keep_singular", "true")):
        flags |= 0x02
    if truthy(fields.get("keep_nonasymptotic", "false")):
        flags |= 0x04
    if truthy(fields.get("compute_volume", "false")):
        flags |= 0x08
    out.append(flags)

    min_volume_mean = fields.get("min_volume_mean", "0.0")
    if min_volume_mean in {"0", "0.0"}:
        out.append(0)
    else:
        out.append(1)
        _write_text(out, min_volume_mean)

    motif_zero_tol = fields.get("motif_zero_tol", "1.0e-6")
    if motif_zero_tol.strip().lower() in {"1e-6", "1.0e-6"}:
        out.append(0)
    else:
        out.append(1)
        _write_text(out, motif_zero_tol)

    return bytes(out)


def _decode_cfg_signature(reader: _Reader) -> str:
    scope_code = reader.read_byte()
    scope = {
        0: "feasible",
        1: "all",
        2: "included",
        3: "robust",
    }.get(scope_code)
    if scope is None:
        if scope_code != 255:
            raise ValueError(f"unknown scope code: {scope_code}")
        scope = reader.read_text()

    flags = reader.read_byte()
    deduplicate = "true" if (flags & 0x01) else "false"
    keep_singular = "true" if (flags & 0x02) else "false"
    keep_nonasymptotic = "true" if (flags & 0x04) else "false"
    compute_volume = "true" if (flags & 0x08) else "false"

    min_volume_mean = "0.0" if reader.read_byte() == 0 else reader.read_text()
    motif_zero_tol = "1.0e-6" if reader.read_byte() == 0 else reader.read_text()

    return ";".join(
        [
            f"scope={scope}",
            f"min_volume_mean={min_volume_mean}",
            f"deduplicate={deduplicate}",
            f"keep_singular={keep_singular}",
            f"keep_nonasymptotic={keep_nonasymptotic}",
            f"compute_volume={compute_volume}",
            f"motif_zero_tol={motif_zero_tol}",
        ]
    )


def _coarse_scalar_token(token: str) -> str:
    if token in {"+Inf", "-Inf", "NaN", "0"}:
        return token
    milli = _behavior_numeric_milli(token)
    if milli is not None:
        if milli > 0:
            return "+"
        if milli < 0:
            return "-"
        return "0"
    return token


def _motif_profile_from_exact_tokens(tokens: list[str]) -> list[str]:
    motif: list[str] = []
    for token in tokens:
        kind, coords = _parse_behavior_token(token)
        if kind == "scalar":
            motif.append(_coarse_scalar_token(coords[0]))
        else:
            motif.append("(" + ",".join(_coarse_scalar_token(coord) for coord in coords) + ")")
    return motif


def _motif_label(motif_profile: list[str]) -> str:
    if not motif_profile:
        return "empty"
    if any(token.startswith("(") for token in motif_profile):
        return "vector_motif::" + " -> ".join(motif_profile)

    has_singular = any(token in {"NaN", "+Inf", "-Inf"} for token in motif_profile)
    coarse = [
        "+" if token == "+Inf" else "-" if token == "-Inf" else "0" if token == "NaN" else token
        for token in motif_profile
    ]

    if all(token == "0" for token in coarse):
        label = "flat"
    elif all(token == "+" for token in coarse):
        label = "monotone_activation" if len(coarse) == 1 else "multistage_activation"
    elif all(token == "-" for token in coarse):
        label = "monotone_repression" if len(coarse) == 1 else "multistage_repression"
    else:
        nz = [token for token in coarse if token != "0"]
        first_nz = next((idx for idx, token in enumerate(coarse, start=1) if token != "0"), None)
        last_nz = next((idx for idx, token in enumerate(reversed(coarse), start=1) if token != "0"), None)
        last_nz = None if last_nz is None else len(coarse) - last_nz + 1
        sign_changes = 0 if len(nz) <= 1 else sum(1 for idx in range(len(nz) - 1) if nz[idx] != nz[idx + 1])

        if nz and all(token in {"+", "0"} for token in coarse):
            if first_nz > 1 and last_nz < len(coarse):
                label = "band_pass_like"
            elif first_nz > 1:
                label = "thresholded_activation"
            elif last_nz < len(coarse):
                label = "activation_with_saturation"
            else:
                label = "positive_motif"
        elif nz and all(token in {"-", "0"} for token in coarse):
            if first_nz > 1 and last_nz < len(coarse):
                label = "window_repression"
            elif first_nz > 1:
                label = "thresholded_repression"
            elif last_nz < len(coarse):
                label = "repression_with_floor"
            else:
                label = "negative_motif"
        elif sign_changes == 1 and nz[0] == "+" and nz[-1] == "-":
            label = "biphasic_peak"
        elif sign_changes == 1 and nz[0] == "-" and nz[-1] == "+":
            label = "biphasic_valley"
        else:
            label = "complex_motif"

    return label + "_with_singular_transition" if has_singular else label


def encode_path_id(
    *,
    network_id: str,
    selector_kind: str,
    selector_value: str,
    output_symbol: str,
    cfg_signature: str,
    path_idx: int,
) -> str:
    selector_prefix = {"input": "i", "change": "c"}.get(selector_kind)
    if selector_prefix is None:
        raise ValueError(f"unsupported selector_kind: {selector_kind}")
    network_segment = _b64url_encode(_encode_network_id(network_id))
    selector_segment = selector_prefix + _encode_text_segment(selector_value)
    output_segment = _encode_text_segment(output_symbol)
    cfg_segment = _b64url_encode(_encode_cfg_signature(cfg_signature))
    path_segment = _base36_encode(path_idx)
    return ".".join([PATH_ID_PREFIX[:-1], network_segment, selector_segment, output_segment, cfg_segment, path_segment])


def decode_path_id(path_id: str) -> dict[str, Any]:
    if not path_id.startswith(PATH_ID_PREFIX):
        raise ValueError(f"unrecognized stable path id: {path_id}")
    parts = path_id.split(".")
    if len(parts) != 6 or parts[0] != PATH_ID_PREFIX[:-1]:
        raise ValueError(f"unrecognized stable path id: {path_id}")
    network_reader = _Reader(_b64url_decode(parts[1]))
    network_id = _decode_network_id(network_reader)
    if network_reader.offset != len(network_reader.data):
        raise ValueError("unexpected trailing bytes in network segment")
    selector_kind = {"i": "input", "c": "change"}.get(parts[2][:1])
    if selector_kind is None:
        raise ValueError(f"unknown selector segment: {parts[2]}")
    selector_value = _decode_text_segment(parts[2][1:])
    output_symbol = _decode_text_segment(parts[3])
    cfg_reader = _Reader(_b64url_decode(parts[4]))
    cfg_signature = _decode_cfg_signature(cfg_reader)
    if cfg_reader.offset != len(cfg_reader.data):
        raise ValueError("unexpected trailing bytes in cfg segment")
    path_idx = _base36_decode(parts[5])
    return {
        "path_id": path_id,
        "network_id": network_id,
        "selector_kind": selector_kind,
        "selector_value": selector_value,
        "output_symbol": output_symbol,
        "cfg_signature": cfg_signature,
        "path_idx": path_idx,
    }


def _connect(db_path: str) -> sqlite3.Connection:
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    return con


def _path_table_name(con: sqlite3.Connection) -> str:
    row = con.execute(
        """
        SELECT value_text
        FROM atlas_metadata
        WHERE key = 'persist_mode'
        """
    ).fetchone()
    if row is not None and row["value_text"] == "path_only":
        return "path_only_records"
    return "path_records"


def _attach_behavior(con: sqlite3.Connection, payload: dict[str, Any]) -> dict[str, Any]:
    table_name = _path_table_name(con)
    row = con.execute(
        f"SELECT behavior_code FROM {table_name} WHERE path_record_id = ?",
        (payload["path_id"],),
    ).fetchone()
    if row is None:
        return payload
    payload["behavior"] = decode_behavior_code(row["behavior_code"])
    return payload


def _cmd_decode(args: argparse.Namespace) -> None:
    payload = decode_path_id(args.path_id)
    if args.db is not None:
        con = _connect(args.db)
        try:
            payload = _attach_behavior(con, payload)
        finally:
            con.close()
    print(json.dumps(payload, indent=2, ensure_ascii=False))


def _cmd_encode(args: argparse.Namespace) -> None:
    if args.input_symbol is not None:
        selector_kind = "input"
        selector_value = args.input_symbol
    else:
        selector_kind = "change"
        selector_value = args.change_signature
    path_id = encode_path_id(
        network_id=args.network_id,
        selector_kind=selector_kind,
        selector_value=selector_value,
        output_symbol=args.output_symbol,
        cfg_signature=args.cfg_signature,
        path_idx=args.path_idx,
    )
    print(json.dumps({"path_id": path_id}, ensure_ascii=False))


def _cmd_behavior_decode(args: argparse.Namespace) -> None:
    print(json.dumps(decode_behavior_code(args.behavior_code), indent=2, ensure_ascii=False))


def _cmd_find(args: argparse.Namespace) -> None:
    con = _connect(args.db)
    try:
        table_name = _path_table_name(con)
        clauses: list[str] = []
        params: list[Any] = []
        if args.network_id is not None:
            network_segment = _b64url_encode(_encode_network_id(args.network_id))
            clauses.append("path_record_id GLOB ?")
            params.append(f"{PATH_ID_PREFIX[:-1]}.{network_segment}.*")
        if args.behavior_code is not None:
            clauses.append("behavior_code = ?")
            params.append(args.behavior_code)

        sql = f"SELECT path_record_id, behavior_code FROM {table_name}"
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY path_record_id"

        rows = con.execute(sql, params).fetchall()
        out = []
        for row in rows:
            decoded = decode_path_id(row["path_record_id"])
            behavior = decode_behavior_code(row["behavior_code"])
            if args.exact_label is not None and behavior["exact_label"] != args.exact_label:
                continue
            if args.motif_label is not None and behavior["motif_label"] != args.motif_label:
                continue
            if args.path_length is not None and behavior["path_length"] != args.path_length:
                continue
            decoded["behavior"] = behavior
            out.append(decoded)
            if len(out) >= args.limit:
                break
        print(json.dumps(out, indent=2, ensure_ascii=False))
    finally:
        con.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Encode, decode, and query stable path IDs in path-only atlas SQLite databases.")
    sub = parser.add_subparsers(dest="cmd", required=True)

    decode = sub.add_parser("decode", help="Decode one stable path_id into recompute conditions.")
    decode.add_argument("--path-id", required=True)
    decode.add_argument("--db", help="Optional SQLite database to also fetch behavior columns.")
    decode.set_defaults(func=_cmd_decode)

    encode = sub.add_parser("encode", help="Encode one condition tuple into a stable path_id.")
    encode.add_argument("--network-id", required=True)
    selector = encode.add_mutually_exclusive_group(required=True)
    selector.add_argument("--input-symbol")
    selector.add_argument("--change-signature")
    encode.add_argument("--output-symbol", required=True)
    encode.add_argument("--cfg-signature", required=True)
    encode.add_argument("--path-idx", required=True, type=int)
    encode.set_defaults(func=_cmd_encode)

    behavior_decode = sub.add_parser("behavior-decode", help="Decode one standardized behavior_code.")
    behavior_decode.add_argument("--behavior-code", required=True)
    behavior_decode.set_defaults(func=_cmd_behavior_decode)

    find = sub.add_parser("find", help="Find path IDs by behavior labels and decode their conditions.")
    find.add_argument("--db", required=True)
    find.add_argument("--network-id")
    find.add_argument("--behavior-code")
    find.add_argument("--exact-label")
    find.add_argument("--motif-label")
    find.add_argument("--path-length", type=int)
    find.add_argument("--limit", type=int, default=20)
    find.set_defaults(func=_cmd_find)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
