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
    corpus = corpus or os.environ.get("BNE_PACKET_CORPUS")
    if not corpus or not os.path.isdir(corpus):
        raise FileNotFoundError(f"packet corpus not found: {corpus!r} (set BNE_PACKET_CORPUS)")
    if corpus not in _CACHE:
        _CACHE[corpus] = Reader(PacketStore(corpus))
    return _CACHE[corpus]


def _target(rd, prototype=None, curve=None):
    if curve is not None:
        return np.asarray(curve, float), None
    protos = prototypes(rd.store.u_grid)
    if prototype not in protos:
        raise ValueError(f"unknown prototype {prototype!r}; known: {sorted(protos)}")
    c, nearest = protos[prototype]
    return c, nearest


def panel(prototype=None, curve=None, intent="typical", behavior_label=None, k=6,
          max_reactions=None, corpus=None):
    """Hybrid function-space panel for a target behavior. Returns a ReaderResult."""
    try:
        rd = _reader(corpus)
    except FileNotFoundError as e:
        return {"error": str(e)}
    tv, nearest = _target(rd, prototype, curve)
    return rd.search(tv, intent=intent, behavior_label=behavior_label or nearest, k=k,
                     max_reactions=max_reactions,
                     target_object_type="reference_curve")


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
