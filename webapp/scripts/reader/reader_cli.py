"""reader_cli.py — Stage-2-lite Reader CLI / demo.

    python3 reader_cli.py <corpus> --prototype bump --intent typical --label biphasic_peak [--k 6] [--json]
    python3 reader_cli.py <corpus> --rerank-ids id1,id2,... --prototype bump --intent typical
    python3 reader_cli.py <corpus> --demo

Emits a schema-valid ReaderResult (validated in-process against
schemas/reader-result.schema.json by a tiny draft-07-subset checker — no deps).
"""
from __future__ import annotations
import argparse
import json
import os
import sys
import numpy as np

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from packet_store import PacketStore
from reader import Reader, render_result, RESULT_SCHEMA
from compare import bump, sigmoid

def _find_schema():
    here = os.path.dirname(os.path.abspath(__file__))
    for d in (os.environ.get("BNE_SCHEMA_DIR", ""),
              os.path.join(here, "..", "..", "..", "schemas"),   # repo layout
              os.path.join(here, "..", "..", "schemas"),         # bundled resource layout
              here):                                             # deployed alongside the reader
        p = os.path.join(d, "reader-result.schema.json") if d else ""
        if p and os.path.isfile(p):
            return p
    return None


_SCHEMA_PATH = _find_schema()


# named prototype targets (stand-ins for the NL->target compiler) + nearest vocab label
def _protos(u):
    return {
        "bump":        (bump(u, 0, 2.5),                         "biphasic_peak"),
        "broad_bump":  (bump(u, 0, 3.8),                         "bandpass_with_plateau"),
        "right_peak":  (bump(u, 2.2, 1.6),                       "biphasic_peak"),
        "valley":      (1 - bump(u, 0, 2.2),                     "biphasic_valley"),
        "switch_on":   (sigmoid(u, 0, 1.6),                      "monotone_activation"),
        "switch_off":  (sigmoid(u, 0, 1.6, falling=True),        "monotone_repression"),
        "late_dip":    (sigmoid(u, -0.5, 1.6) - 0.3 * np.clip((u - 2.5) / 3.5, 0, 1), "monotone_activation"),
        "double_bump": (np.maximum(bump(u, -2.2, 1.1), bump(u, 2.2, 1.1)), "biphasic_peak"),
        "shoulder":    (sigmoid(u, 1.6, 2.4),                    "thresholded_activation"),
    }


# ── tiny draft-07-subset validator (type/required/const/enum/properties/items) ──
def _validate(inst, schema, path="$"):
    errs = []
    t = schema.get("type")
    if t:
        ok = {"object": dict, "array": list, "string": str, "number": (int, float),
              "integer": int, "boolean": bool, "null": type(None)}
        types = t if isinstance(t, list) else [t]
        if not any(isinstance(inst, ok[x]) and not (x == "number" and isinstance(inst, bool)) for x in types):
            errs.append(f"{path}: expected {t}, got {type(inst).__name__}")
            return errs
    if "const" in schema and inst != schema["const"]:
        errs.append(f"{path}: const != {schema['const']}")
    if "enum" in schema and inst not in schema["enum"]:
        errs.append(f"{path}: {inst!r} not in {schema['enum']}")
    if isinstance(inst, dict):
        for r in schema.get("required", []):
            if r not in inst:
                errs.append(f"{path}: missing required '{r}'")
        for k, sub in schema.get("properties", {}).items():
            if k in inst:
                errs += _validate(inst[k], sub, f"{path}.{k}")
    if isinstance(inst, list) and "items" in schema:
        for i, el in enumerate(inst):
            errs += _validate(el, schema["items"], f"{path}[{i}]")
    return errs


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("corpus")
    ap.add_argument("--prototype", default="bump")
    ap.add_argument("--intent", default="typical", choices=["existential", "typical", "robust"])
    ap.add_argument("--label", default=None)
    ap.add_argument("--k", type=int, default=6)
    ap.add_argument("--max-reactions", type=int, default=None)
    ap.add_argument("--rerank-ids", default=None, help="comma-separated record_ids to rerank (agent candidate rerank)")
    ap.add_argument("--json", action="store_true")
    ap.add_argument("--demo", action="store_true")
    a = ap.parse_args()

    st = PacketStore(a.corpus); rd = Reader(st)
    protos = _protos(st.u_grid)
    schema = json.load(open(_SCHEMA_PATH)) if os.path.isfile(_SCHEMA_PATH) else None

    def run_one(proto, intent, label, k, ids=None):
        curve, nearest = protos[proto]
        lbl = label if label is not None else nearest
        if ids:
            res = rd.rerank(ids, curve, intent=intent, k=k)
        else:
            res = rd.search(curve, intent=intent, behavior_label=lbl, k=k, max_reactions=a.max_reactions)
        verrs = _validate(res, schema) if schema else ["(schema not found)"]
        return res, verrs

    if a.demo:
        cases = [("bump", "typical", None), ("right_peak", "typical", None),
                 ("double_bump", "existential", None), ("broad_bump", "robust", None)]
        for proto, intent, label in cases:
            res, verrs = run_one(proto, intent, label, a.k)
            print(f"\n###### query: prototype={proto} intent={intent} ######")
            print(render_result(res))
            print(f"[schema {RESULT_SCHEMA}] " + ("VALID ✓" if not verrs else "INVALID: " + "; ".join(verrs[:4])))
        return

    ids = a.rerank_ids.split(",") if a.rerank_ids else None
    res, verrs = run_one(a.prototype, a.intent, a.label, a.k, ids)
    if a.json:
        print(json.dumps(res, indent=2))
    else:
        print(render_result(res))
    print(f"\n[schema {RESULT_SCHEMA}] " + ("VALID ✓" if not verrs else "INVALID: " + "; ".join(verrs[:4])), file=sys.stderr)


if __name__ == "__main__":
    main()
