"""reader_service.py — Stage-2-lite agent-callable Reader service.

Loads + caches a Reader per packet-corpus path (the DRAWS tensor is built once),
so an agent tool can call it cheaply per turn. Two functions the agent uses:
  panel(...)  — function-space hybrid panel for a target behavior
  rerank(...) — reorder a GIVEN candidate set by function + ROP evidence

Corpus path: explicit arg or env BNE_PACKET_CORPUS. Returns a ReaderResult dict
(or {"error": ...} when the corpus is unavailable) — never fabricates.
"""
from __future__ import annotations
import os
import sys
import numpy as np

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from packet_store import PacketStore
from reader import Reader, prototypes, render_result

_CACHE = {}


def _reader(corpus):
    # prefer the frozen industrial atlas_root (HNSW etc.), fall back to a raw packet corpus
    corpus = corpus or os.environ.get("BNE_ATLAS_ROOT") or os.environ.get("BNE_PACKET_CORPUS")
    if not corpus or not os.path.isdir(corpus):
        raise FileNotFoundError(f"atlas/corpus not found: {corpus!r} (set BNE_ATLAS_ROOT or BNE_PACKET_CORPUS)")
    if corpus not in _CACHE:
        from atlas_store import open_store
        rd = Reader(open_store(corpus))
        try:
            rd.attach_indexes(corpus)              # HNSW + diffusion + Mapper if this is an atlas_root
        except Exception:
            pass
        _CACHE[corpus] = rd
    return _CACHE[corpus]


def _target(rd, prototype=None, curve=None):
    if curve is not None:
        return np.asarray(curve, float), None
    protos = prototypes(rd.store.u_grid)
    if prototype not in protos:
        raise ValueError(f"unknown prototype {prototype!r}; known: {sorted(protos)}")
    c, nearest = protos[prototype]
    return c, nearest


def panel(nl=None, prototype=None, curve=None, intent=None, behavior_label=None, k=6,
          max_reactions=None, corpus=None, llm_cfg=None):
    """Intent-ROUTED function-space panel. If `nl` is given it is compiled (NL→target-spec) into
    target + intent + label + constraints; else use prototype/curve + intent. Returns a ReaderResult
    with route + coverage_status (+ compiled_spec when NL). PRIOR only — verify before showing."""
    try:
        rd = _reader(corpus)
    except FileNotFoundError as e:
        return {"error": str(e)}
    u = rd.store.u_grid
    spec = None
    if nl:
        from nl_target_compiler import compile_target, render_target
        spec = compile_target(nl, llm_cfg)
        tgt, intent, behavior_label, mx = render_target(spec, u)
        max_reactions = max_reactions or mx
        tot = spec["target_object_type"]
    else:
        tgt, nearest = _target(rd, prototype, curve)
        behavior_label = behavior_label or nearest; intent = intent or "typical"; tot = "reference_curve"
    res = rd.routed_search(tgt, intent, behavior_label=behavior_label, k=k,
                           max_reactions=max_reactions, target_object_type=tot)
    if spec:
        res["compiled_spec"] = {x: spec.get(x) for x in
                                ("intent_type", "target_object_type", "behavior_label_hint", "constraints",
                                 "preferences", "ambiguities", "unsupported_parts", "_source")}
    return res


def rerank(record_ids, prototype=None, curve=None, intent="typical", k=None, corpus=None):
    """Agent candidate rerank: reorder a GIVEN set of record_ids by function + ROP."""
    try:
        rd = _reader(corpus)
    except FileNotFoundError as e:
        return {"error": str(e)}
    tv, _ = _target(rd, prototype, curve)
    return rd.rerank(record_ids, tv, intent=intent, k=k)


if __name__ == "__main__":
    corpus = sys.argv[1] if len(sys.argv) > 1 else "/tmp/curve-packets-600"
    res = panel(prototype="bump", intent="typical", k=4, corpus=corpus)
    print(render_result(res) if "error" not in res else res)
    # agent candidate rerank demo: take the panel's own ids, rerank under existential
    ids = [c["record_id"] for c in res.get("candidates", [])]
    if ids:
        rr = rerank(ids, prototype="bump", intent="existential", corpus=corpus)
        print("\n[rerank demo] reordered", len(rr.get("candidates", [])), "candidates under intent=existential; "
              "top match=", rr["candidates"][0]["match_score"] if rr.get("candidates") else None)
