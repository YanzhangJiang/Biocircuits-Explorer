#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import sqlite3
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any


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


@dataclass(frozen=True)
class ClassifierConfig:
    raw_signature: str
    fields: dict[str, str] = field(default_factory=dict)
    typed_fields: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def parse(cls, raw_signature: str) -> "ClassifierConfig":
        parts = raw_signature.split(";") if raw_signature else []
        fields: dict[str, str] = {}
        typed: dict[str, Any] = {}
        for part in parts:
            key, value = part.split("=", 1)
            fields[key] = value
            typed[key] = _parse_scalar(value)
        return cls(raw_signature=raw_signature, fields=fields, typed_fields=typed)

    def render(self) -> str:
        return self.raw_signature


@dataclass(frozen=True)
class GraphSliceId:
    network_id: str
    mode: str  # input or change
    value: str
    graph_cfg: str

    @classmethod
    def parse(cls, raw_id: str) -> "GraphSliceId":
        parts = raw_id.split("::")
        if len(parts) != 3:
            raise ValueError(f"Unrecognized graph_slice_id: {raw_id}")
        network_id = parts[0]
        if parts[1].startswith("graph_input="):
            mode = "input"
            value = parts[1].split("=", 1)[1]
        elif parts[1].startswith("graph_change="):
            mode = "change"
            value = parts[1].split("=", 1)[1]
        else:
            raise ValueError(f"Unrecognized graph slice middle segment: {parts[1]}")
        if not parts[2].startswith("graphcfg="):
            raise ValueError(f"Unrecognized graph slice config segment: {parts[2]}")
        graph_cfg = parts[2].split("=", 1)[1]
        return cls(network_id=network_id, mode=mode, value=value, graph_cfg=graph_cfg)

    def render(self) -> str:
        middle = "graph_input=" + self.value if self.mode == "input" else "graph_change=" + self.value
        return "::".join([self.network_id, middle, "graphcfg=" + self.graph_cfg])


@dataclass(frozen=True)
class BehaviorSliceId:
    network_id: str
    mode: str  # input or change
    value: str
    output_symbol: str
    cfg: ClassifierConfig

    @classmethod
    def parse(cls, raw_id: str) -> "BehaviorSliceId":
        parts = raw_id.split("::")
        if len(parts) != 4:
            raise ValueError(f"Unrecognized slice_id: {raw_id}")
        network_id = parts[0]
        if parts[1].startswith("input="):
            mode = "input"
            value = parts[1].split("=", 1)[1]
        elif parts[1].startswith("change="):
            mode = "change"
            value = parts[1].split("=", 1)[1]
        else:
            raise ValueError(f"Unrecognized slice mode segment: {parts[1]}")
        if not parts[2].startswith("output="):
            raise ValueError(f"Unrecognized slice output segment: {parts[2]}")
        output_symbol = parts[2].split("=", 1)[1]
        if not parts[3].startswith("cfg="):
            raise ValueError(f"Unrecognized slice cfg segment: {parts[3]}")
        cfg = ClassifierConfig.parse(parts[3].split("=", 1)[1])
        return cls(network_id=network_id, mode=mode, value=value, output_symbol=output_symbol, cfg=cfg)

    def render(self) -> str:
        middle = "input=" + self.value if self.mode == "input" else "change=" + self.value
        return "::".join([self.network_id, middle, "output=" + self.output_symbol, "cfg=" + self.cfg.render()])


@dataclass(frozen=True)
class RegimeRecordId:
    slice: BehaviorSliceId
    vertex_idx: int

    @classmethod
    def parse(cls, raw_id: str) -> "RegimeRecordId":
        head, marker, tail = raw_id.rpartition("::regime::")
        if not marker:
            raise ValueError(f"Unrecognized regime_record_id: {raw_id}")
        return cls(slice=BehaviorSliceId.parse(head), vertex_idx=int(tail))

    def render(self) -> str:
        return f"{self.slice.render()}::regime::{self.vertex_idx}"


@dataclass(frozen=True)
class TransitionRecordId:
    slice: BehaviorSliceId
    from_vertex_idx: int
    to_vertex_idx: int

    @classmethod
    def parse(cls, raw_id: str) -> "TransitionRecordId":
        head, marker, tail = raw_id.rpartition("::transition::")
        if not marker:
            raise ValueError(f"Unrecognized transition_record_id: {raw_id}")
        from_v, to_v = tail.split("->", 1)
        return cls(slice=BehaviorSliceId.parse(head), from_vertex_idx=int(from_v), to_vertex_idx=int(to_v))

    def render(self) -> str:
        return f"{self.slice.render()}::transition::{self.from_vertex_idx}->{self.to_vertex_idx}"


@dataclass(frozen=True)
class FamilyBucketId:
    slice: BehaviorSliceId
    family_kind: str
    family_idx: int

    @classmethod
    def parse(cls, raw_id: str) -> "FamilyBucketId":
        if "::exact::" in raw_id:
            head, _, tail = raw_id.rpartition("::exact::")
            family_kind = "exact"
        elif "::motif::" in raw_id:
            head, _, tail = raw_id.rpartition("::motif::")
            family_kind = "motif"
        else:
            raise ValueError(f"Unrecognized family bucket id: {raw_id}")
        return cls(slice=BehaviorSliceId.parse(head), family_kind=family_kind, family_idx=int(tail))

    def render(self) -> str:
        return f"{self.slice.render()}::{self.family_kind}::{self.family_idx}"


@dataclass(frozen=True)
class PathRecordId:
    slice: BehaviorSliceId
    path_idx: int

    @classmethod
    def parse(cls, raw_id: str) -> "PathRecordId":
        head, marker, tail = raw_id.rpartition("::path::")
        if not marker:
            raise ValueError(f"Unrecognized path_record_id: {raw_id}")
        return cls(slice=BehaviorSliceId.parse(head), path_idx=int(tail))

    def render(self) -> str:
        return f"{self.slice.render()}::path::{self.path_idx}"


@dataclass
class AtlasDictionaries:
    networks: dict[str, int] = field(default_factory=dict)
    graph_values: dict[str, int] = field(default_factory=dict)
    graph_cfgs: dict[str, int] = field(default_factory=dict)
    outputs: dict[str, int] = field(default_factory=dict)
    cfgs: dict[str, int] = field(default_factory=dict)
    family_kinds: dict[str, int] = field(default_factory=dict)

    @staticmethod
    def _intern(store: dict[str, int], value: str) -> int:
        existing = store.get(value)
        if existing is not None:
            return existing
        new_id = len(store) + 1
        store[value] = new_id
        return new_id

    def network_id(self, value: str) -> int:
        return self._intern(self.networks, value)

    def graph_value_id(self, value: str) -> int:
        return self._intern(self.graph_values, value)

    def graph_cfg_id(self, value: str) -> int:
        return self._intern(self.graph_cfgs, value)

    def output_id(self, value: str) -> int:
        return self._intern(self.outputs, value)

    def cfg_id(self, value: str) -> int:
        return self._intern(self.cfgs, value)

    def family_kind_id(self, value: str) -> int:
        return self._intern(self.family_kinds, value)

    @staticmethod
    def _lookup(store: dict[str, int], value: int, label: str) -> str:
        for text, idx in store.items():
            if idx == value:
                return text
        raise KeyError(f"Unknown {label} id: {value}")

    def network_text(self, value: int) -> str:
        return self._lookup(self.networks, value, "network")

    def graph_value_text(self, value: int) -> str:
        return self._lookup(self.graph_values, value, "graph_value")

    def graph_cfg_text(self, value: int) -> str:
        return self._lookup(self.graph_cfgs, value, "graph_cfg")

    def output_text(self, value: int) -> str:
        return self._lookup(self.outputs, value, "output")

    def cfg_text(self, value: int) -> str:
        return self._lookup(self.cfgs, value, "cfg")

    def family_kind_text(self, value: int) -> str:
        return self._lookup(self.family_kinds, value, "family_kind")

    def to_json(self) -> dict[str, Any]:
        return {
            "networks": self.networks,
            "graph_values": self.graph_values,
            "graph_cfgs": self.graph_cfgs,
            "outputs": self.outputs,
            "cfgs": self.cfgs,
            "family_kinds": self.family_kinds,
        }

    @classmethod
    def from_json(cls, payload: dict[str, Any]) -> "AtlasDictionaries":
        return cls(
            networks={str(k): int(v) for k, v in payload.get("networks", {}).items()},
            graph_values={str(k): int(v) for k, v in payload.get("graph_values", {}).items()},
            graph_cfgs={str(k): int(v) for k, v in payload.get("graph_cfgs", {}).items()},
            outputs={str(k): int(v) for k, v in payload.get("outputs", {}).items()},
            cfgs={str(k): int(v) for k, v in payload.get("cfgs", {}).items()},
            family_kinds={str(k): int(v) for k, v in payload.get("family_kinds", {}).items()},
        )


def _compact_graph_slice(obj: GraphSliceId, dictionaries: AtlasDictionaries) -> dict[str, Any]:
    return {
        "np": dictionaries.network_id(obj.network_id),
        "gm": 1 if obj.mode == "input" else 2,
        "gv": dictionaries.graph_value_id(obj.value),
        "gc": dictionaries.graph_cfg_id(obj.graph_cfg),
    }


def _compact_slice(obj: BehaviorSliceId, dictionaries: AtlasDictionaries) -> dict[str, Any]:
    return {
        "np": dictionaries.network_id(obj.network_id),
        "sm": 1 if obj.mode == "input" else 2,
        "gv": dictionaries.graph_value_id(obj.value),
        "o": dictionaries.output_id(obj.output_symbol),
        "c": dictionaries.cfg_id(obj.cfg.render()),
    }


def _compact_regime(obj: RegimeRecordId, dictionaries: AtlasDictionaries) -> dict[str, Any]:
    payload = _compact_slice(obj.slice, dictionaries)
    payload["v"] = obj.vertex_idx
    return payload


def _compact_transition(obj: TransitionRecordId, dictionaries: AtlasDictionaries) -> dict[str, Any]:
    payload = _compact_slice(obj.slice, dictionaries)
    payload["fv"] = obj.from_vertex_idx
    payload["tv"] = obj.to_vertex_idx
    return payload


def _compact_family(obj: FamilyBucketId, dictionaries: AtlasDictionaries) -> dict[str, Any]:
    payload = _compact_slice(obj.slice, dictionaries)
    payload["fk"] = dictionaries.family_kind_id(obj.family_kind)
    payload["fi"] = obj.family_idx
    return payload


def _compact_path(obj: PathRecordId, dictionaries: AtlasDictionaries) -> dict[str, Any]:
    payload = _compact_slice(obj.slice, dictionaries)
    payload["pi"] = obj.path_idx
    return payload


def _payload_get(payload: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in payload:
            return payload[key]
    raise KeyError(f"Missing keys {keys} in compact payload")


def _expand_graph_slice(payload: dict[str, Any], dictionaries: AtlasDictionaries) -> GraphSliceId:
    return GraphSliceId(
        network_id=dictionaries.network_text(int(_payload_get(payload, "np", "network_pk"))),
        mode="input" if int(_payload_get(payload, "gm", "graph_mode")) == 1 else "change",
        value=dictionaries.graph_value_text(int(_payload_get(payload, "gv", "graph_value_id"))),
        graph_cfg=dictionaries.graph_cfg_text(int(_payload_get(payload, "gc", "graph_cfg_id"))),
    )


def _expand_slice(payload: dict[str, Any], dictionaries: AtlasDictionaries) -> BehaviorSliceId:
    return BehaviorSliceId(
        network_id=dictionaries.network_text(int(_payload_get(payload, "np", "network_pk"))),
        mode="input" if int(_payload_get(payload, "sm", "slice_mode")) == 1 else "change",
        value=dictionaries.graph_value_text(int(_payload_get(payload, "gv", "change_ref_id"))),
        output_symbol=dictionaries.output_text(int(_payload_get(payload, "o", "output_symbol_id"))),
        cfg=ClassifierConfig.parse(dictionaries.cfg_text(int(_payload_get(payload, "c", "cfg_id")))),
    )


def _expand_regime(payload: dict[str, Any], dictionaries: AtlasDictionaries) -> RegimeRecordId:
    return RegimeRecordId(
        slice=_expand_slice(payload, dictionaries),
        vertex_idx=int(_payload_get(payload, "v", "vertex_idx")),
    )


def _expand_transition(payload: dict[str, Any], dictionaries: AtlasDictionaries) -> TransitionRecordId:
    return TransitionRecordId(
        slice=_expand_slice(payload, dictionaries),
        from_vertex_idx=int(_payload_get(payload, "fv", "from_vertex_idx")),
        to_vertex_idx=int(_payload_get(payload, "tv", "to_vertex_idx")),
    )


def _expand_family(payload: dict[str, Any], dictionaries: AtlasDictionaries) -> FamilyBucketId:
    return FamilyBucketId(
        slice=_expand_slice(payload, dictionaries),
        family_kind=dictionaries.family_kind_text(int(_payload_get(payload, "fk", "family_kind_id"))),
        family_idx=int(_payload_get(payload, "fi", "family_idx")),
    )


def _expand_path(payload: dict[str, Any], dictionaries: AtlasDictionaries) -> PathRecordId:
    return PathRecordId(
        slice=_expand_slice(payload, dictionaries),
        path_idx=int(_payload_get(payload, "pi", "path_idx")),
    )


KIND_TO_CLASS = {
    "graph_slice": GraphSliceId,
    "slice": BehaviorSliceId,
    "regime": RegimeRecordId,
    "transition": TransitionRecordId,
    "family": FamilyBucketId,
    "path": PathRecordId,
}

KIND_TO_COMPACT = {
    "graph_slice": _compact_graph_slice,
    "slice": _compact_slice,
    "regime": _compact_regime,
    "transition": _compact_transition,
    "family": _compact_family,
    "path": _compact_path,
}

KIND_TO_EXPAND = {
    "graph_slice": _expand_graph_slice,
    "slice": _expand_slice,
    "regime": _expand_regime,
    "transition": _expand_transition,
    "family": _expand_family,
    "path": _expand_path,
}


def parse_identifier(kind: str, raw_id: str) -> Any:
    if kind == "cfg":
        return ClassifierConfig.parse(raw_id)
    return KIND_TO_CLASS[kind].parse(raw_id)


def render_identifier(kind: str, payload: Any) -> str:
    if kind == "cfg":
        if isinstance(payload, ClassifierConfig):
            return payload.render()
        return ClassifierConfig.parse(str(payload)).render()
    return payload.render()


def expand_compact_identifier(kind: str, payload: Any, dictionaries: AtlasDictionaries) -> Any:
    if kind == "cfg":
        if isinstance(payload, dict):
            cfg_id = int(_payload_get(payload, "c", "cfg_id"))
        else:
            cfg_id = int(payload)
        return ClassifierConfig.parse(dictionaries.cfg_text(cfg_id))
    return KIND_TO_EXPAND[kind](payload, dictionaries)


def as_jsonable(obj: Any) -> Any:
    if isinstance(obj, ClassifierConfig):
        return {
            "raw_signature": obj.raw_signature,
            "fields": obj.fields,
            "typed_fields": obj.typed_fields,
        }
    if hasattr(obj, "__dataclass_fields__"):
        out = {}
        for key, value in asdict(obj).items():
            out[key] = value
        return out
    return obj


def _read_jsonish(raw_value: str) -> Any:
    candidate = Path(raw_value)
    if candidate.exists():
        return json.loads(candidate.read_text(encoding="utf-8"))
    return json.loads(raw_value)


def build_dictionaries_from_db(db_path: Path, limit: int | None = None) -> AtlasDictionaries:
    dictionaries = AtlasDictionaries()
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        plans = [
            ("SELECT network_id FROM network_entries", "networks"),
            ("SELECT graph_slice_id FROM input_graph_slices", "graph"),
            ("SELECT slice_id FROM behavior_slices", "slice"),
            ("SELECT regime_record_id FROM regime_records", "regime"),
            ("SELECT transition_record_id FROM transition_records", "transition"),
            ("SELECT bucket_id FROM family_buckets", "family"),
            ("SELECT path_record_id FROM path_records", "path"),
        ]
        for sql, kind in plans:
            if limit is not None:
                sql += f" LIMIT {int(limit)}"
            for (raw_id,) in conn.execute(sql):
                if raw_id is None:
                    continue
                if kind == "networks":
                    dictionaries.network_id(str(raw_id))
                    continue
                parsed = parse_identifier("graph_slice" if kind == "graph" else kind, str(raw_id))
                if kind == "graph":
                    _compact_graph_slice(parsed, dictionaries)
                elif kind == "slice":
                    _compact_slice(parsed, dictionaries)
                elif kind == "regime":
                    _compact_regime(parsed, dictionaries)
                elif kind == "transition":
                    _compact_transition(parsed, dictionaries)
                elif kind == "family":
                    _compact_family(parsed, dictionaries)
                elif kind == "path":
                    _compact_path(parsed, dictionaries)
        return dictionaries
    finally:
        conn.close()


def _cmd_parse(args: argparse.Namespace) -> None:
    parsed = parse_identifier(args.kind, args.id_text)
    print(json.dumps(as_jsonable(parsed), indent=2, sort_keys=True, ensure_ascii=False))


def _cmd_roundtrip(args: argparse.Namespace) -> None:
    parsed = parse_identifier(args.kind, args.id_text)
    rendered = render_identifier(args.kind, parsed)
    print(json.dumps({
        "input": args.id_text,
        "rendered": rendered,
        "matches": rendered == args.id_text,
    }, indent=2, ensure_ascii=False))


def _cmd_compact(args: argparse.Namespace) -> None:
    dictionaries = AtlasDictionaries()
    parsed = parse_identifier(args.kind, args.id_text)
    if args.kind == "cfg":
        payload = {"c": dictionaries.cfg_id(parsed.render())}
    else:
        payload = KIND_TO_COMPACT[args.kind](parsed, dictionaries)
    print(json.dumps({
        "compact": payload,
        "dictionaries": dictionaries.to_json(),
    }, indent=2, ensure_ascii=False, sort_keys=True))


def _cmd_build_dicts(args: argparse.Namespace) -> None:
    dictionaries = build_dictionaries_from_db(Path(args.db_path), limit=args.limit)
    print(json.dumps(dictionaries.to_json(), indent=2, ensure_ascii=False, sort_keys=True))


def _cmd_expand(args: argparse.Namespace) -> None:
    payload = _read_jsonish(args.compact_json)
    dictionaries = AtlasDictionaries.from_json(_read_jsonish(args.dicts_json))
    expanded = expand_compact_identifier(args.kind, payload, dictionaries)
    print(json.dumps({
        "expanded": as_jsonable(expanded),
        "rendered": render_identifier(args.kind, expanded),
    }, indent=2, ensure_ascii=False, sort_keys=True))


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Parse and round-trip atlas IDs for lossless v2 compression.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    parse_p = subparsers.add_parser("parse", help="Parse a textual atlas id into structured components.")
    parse_p.add_argument("--kind", choices=["cfg", *KIND_TO_CLASS.keys()], required=True)
    parse_p.add_argument("id_text")
    parse_p.set_defaults(func=_cmd_parse)

    rt_p = subparsers.add_parser("roundtrip", help="Parse then re-render an atlas id.")
    rt_p.add_argument("--kind", choices=["cfg", *KIND_TO_CLASS.keys()], required=True)
    rt_p.add_argument("id_text")
    rt_p.set_defaults(func=_cmd_roundtrip)

    compact_p = subparsers.add_parser("compact", help="Show a compact integer-key representation for an atlas id.")
    compact_p.add_argument("--kind", choices=["cfg", *KIND_TO_CLASS.keys()], required=True)
    compact_p.add_argument("id_text")
    compact_p.set_defaults(func=_cmd_compact)

    expand_p = subparsers.add_parser("expand", help="Expand a compact atlas payload back into a readable id.")
    expand_p.add_argument("--kind", choices=["cfg", *KIND_TO_CLASS.keys()], required=True)
    expand_p.add_argument("--compact-json", required=True, help="Inline JSON or a path to a JSON file.")
    expand_p.add_argument("--dicts-json", required=True, help="Inline JSON or a path to a JSON file.")
    expand_p.set_defaults(func=_cmd_expand)

    dicts_p = subparsers.add_parser("build-dicts", help="Build string dictionaries from an existing sqlite db.")
    dicts_p.add_argument("--db-path", required=True)
    dicts_p.add_argument("--limit", type=int, default=None)
    dicts_p.set_defaults(func=_cmd_build_dicts)

    return parser


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
