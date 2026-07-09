"""reader.py — Stage-2-lite Function-Space Atlas Reader.

Productizes the spike-validated HYBRID pipeline (no full DB migration):
    label recall  ->  function rerank (by intent)  ->  ROP-evidence rerank  ->
    diverse, evidence-backed candidate panel  ->  ReaderResult.

Honest annotations throughout: each candidate carries intent-matched match_score,
an evidence tier (T1/T2/T3a/T3b per the roadmap), shape_support, ROP volume/
robust_path_count, family, and invalid/non-converged rate. Two entry points:
  Reader.search(...)  — full hybrid retrieval over the corpus
  Reader.rerank(ids)  — reorder a GIVEN candidate set (agent candidate rerank)
"""
from __future__ import annotations
import json
import os
import sys
import numpy as np

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from packet_store import PacketStore
from compare import normalize, ReferenceCurveTarget, ConstraintCurveTarget, bump, sigmoid  # noqa: F401

READER_VERSION = "bne-reader/v0.1.0"
RESULT_SCHEMA = "bne-reader-result/v0.1.0"
QUERY_SCHEMA = "bne-reader-query/v0.1.0"


def evidence_tier(support, vol, paths):
    """Highest applicable tier (roadmap T0..T3b), honest about what's actually verified."""
    s = support or 0.0
    if s >= 0.8 and (vol or 0) >= 0.05:
        return "T3a"   # robust phenotype + geometric (ROP-volume) robustness
    if s >= 0.8:
        return "T3b"   # high-budget sampled robustness
    if (paths or 0) > 0 or (vol or 0) > 0:
        return "T2"    # scan + ROP supported (a robust path / volume exists)
    return "T1"        # fresh scan only


class Reader:
    def __init__(self, store: PacketStore):
        self.store = store
        N, K, P = len(store), store.K, store.n_points
        self.D = np.full((N, K, P), np.nan, np.float32)
        self.MED = np.full((N, P), np.nan, np.float32)
        for i, r in enumerate(store.records):
            C = store.curves(r); vm = store.valid_mask(r)
            for k in range(K):
                if vm[k]:
                    self.D[i, k] = normalize(C[k], "minmax")
            m = store.medoid_curve(r)
            if m is not None:
                self.MED[i] = normalize(m, "minmax")
        self.vol = np.array([float((r.get("rop_summary") or {}).get("atlas_volume_mean") or 0.0) for r in store.records])
        self.paths = np.array([float((r.get("rop_summary") or {}).get("atlas_robust_path_count") or 0.0) for r in store.records])
        self.by_label = {}
        for i, r in enumerate(store.records):
            self.by_label.setdefault(r["dominant_shape"], []).append(i)
        self.meta = getattr(store, "manifest", None) or store._shards[0].manifest
        self._row = {r["record_id"]: i for i, r in enumerate(store.records)}
        self.idx = {}; self.has_index = False

    # ── target lowering ────────────────────────────────────────────────────────
    def _target_vec(self, target):
        if isinstance(target, ReferenceCurveTarget):
            return np.asarray(target.y, float)
        if isinstance(target, ConstraintCurveTarget):
            u = self.store.u_grid; y = np.zeros_like(u)
            for rg in target.regions:
                lo, hi = rg["u"]; y[(u >= lo) & (u <= hi)] = {"low": 0.0, "high": 1.0}.get(rg["want"], 0.5)
            return y
        return np.asarray(target, float)            # raw curve on u_grid

    @staticmethod
    def _nan_rmse(values, target, axis):
        """RMSE over finite samples, leaving wholly invalid rows as NaN."""
        squared = (values - target) ** 2
        finite = np.isfinite(squared)
        count = np.sum(finite, axis=axis)
        total = np.sum(np.where(finite, squared, 0.0), axis=axis)
        mean = np.divide(
            total,
            count,
            out=np.full(count.shape, np.nan, dtype=float),
            where=count > 0,
        )
        return np.sqrt(mean)

    def _scores(self, target_vec, intent, thr=0.22):
        tn = normalize(target_vec, "minmax")
        if intent == "typical":
            return self._nan_rmse(self.MED, tn, axis=1)                         # lower=better (distance)
        d = self._nan_rmse(self.D, tn, axis=2)                                  # (N,K)
        if intent == "existential":
            # ``nanmin`` warns on a legal all-invalid record.  Treat missing
            # draws as +Inf while reducing, then map an entirely missing row
            # to the established worst-distance sentinel.
            best = np.min(np.where(np.isfinite(d), d, np.inf), axis=1)
            return np.where(np.isfinite(best), best, 9.9)
        valid = np.sum(~np.isnan(d), axis=1)
        return -(np.nansum(d <= thr, axis=1) / np.maximum(valid, 1))            # lower=better (-fraction)

    @staticmethod
    def _match(score, intent):
        return round(float(-score if score < 0 else 0.0), 3) if intent == "robust" \
            else round(float(max(0.0, 1.0 - score)), 3)

    # ── full hybrid search ──────────────────────────────────────────────────────
    def search(self, target, intent="typical", behavior_label=None, k=6,
               max_reactions=None, rop_weight=0.12, diverse=True, target_object_type="reference_curve"):
        tv = self._target_vec(target)
        scores = self._scores(tv, intent)
        # 1. label recall: nominal-label members first, then the rest (hybrid recall so
        #    function search can still surface off-label candidates)
        if behavior_label and behavior_label in self.by_label:
            seed = self.by_label[behavior_label]; seen = set(seed)
            pool = np.array(seed + [i for i in range(len(self.store)) if i not in seen])
        else:
            pool = np.arange(len(self.store))
        if max_reactions:
            pool = pool[[(self.store.records[i].get("n_reactions") or 99) <= max_reactions for i in pool]]
        # 2. function rerank, then 3. ROP-evidence rerank on the function-top pool
        order = pool[np.argsort(scores[pool])]
        topN = order[:300]
        vn = self.vol[topN] / (self.vol[topN].max() + 1e-9)
        topN = topN[np.argsort(scores[topN] - rop_weight * vn)]
        # 4. diverse panel: dedup by ROP family
        chosen = []
        if diverse:
            fams = set()
            for i in topN:
                fam = (self.store.records[i].get("rop_summary") or {}).get("atlas_family_label")
                if fam in fams:
                    continue
                chosen.append(int(i)); fams.add(fam)
                if len(chosen) >= k:
                    break
            for i in topN:
                if len(chosen) >= k:
                    break
                if int(i) not in chosen:
                    chosen.append(int(i))
        else:
            chosen = [int(i) for i in topN[:k]]
        cands = [self._candidate(i, float(scores[i]), intent) for i in chosen[:k]]
        return self._result(cands, intent, target_object_type, behavior_label, k,
                            ["label_recall", "function_rerank", "rop_evidence_rerank"] + (["diverse_panel"] if diverse else []))

    # ── agent candidate rerank: reorder a GIVEN set of record_ids ────────────────
    def rerank(self, record_ids, target, intent="typical", k=None, rop_weight=0.12, target_object_type="reference_curve"):
        idx = [self.store.records.index(self.store._by_id[rid]) for rid in record_ids if rid in self.store._by_id]
        tv = self._target_vec(target); scores = self._scores(tv, intent)
        idx = np.array(idx)
        vn = self.vol[idx] / (self.vol[idx].max() + 1e-9) if idx.size else np.array([])
        order = idx[np.argsort(scores[idx] - rop_weight * vn)] if idx.size else idx
        k = k or len(order)
        cands = [self._candidate(int(i), float(scores[i]), intent) for i in order[:k]]
        return self._result(cands, intent, target_object_type, None, k, ["agent_candidate_rerank", "rop_evidence_rerank"])

    # ── Step 3: intent-routed search (spike A/B verdict) + coverage status ────────
    def _diverse(self, ordered, k):
        chosen, fams = [], set()
        for i in ordered:
            fam = (self.store.records[int(i)].get("rop_summary") or {}).get("atlas_family_label")
            if fam in fams:
                continue
            chosen.append(int(i)); fams.add(fam)
            if len(chosen) >= k:
                break
        for i in ordered:
            if len(chosen) >= k:
                break
            if int(i) not in chosen:
                chosen.append(int(i))
        return chosen[:k]

    def _coverage(self, label, cands):
        best = max((c["match_score"] for c in cands), default=0.0)
        n_strong = sum(1 for c in cands if c["match_score"] >= 0.7)
        status = ("well-covered" if best >= 0.72 and n_strong >= 2
                  else "sparse" if best >= 0.55 else "rare-or-absent")
        note = {"well-covered": "strong matches exist",
                "sparse": "few/weak matches — may be rare in this μ≤5 family; verify or relax constraints",
                "rare-or-absent": "no strong match — this behaviour appears rare/absent in scope; report honestly, do NOT fabricate"}[status]
        return {"best_match": round(best, 3), "n_strong_in_panel": n_strong,
                "label_prevalence": len(self.by_label.get(label, [])), "status": status, "note": note}

    def routed_search(self, target, intent, behavior_label=None, k=6, max_reactions=None,
                      rop_weight=0.12, target_object_type="reference_curve"):
        """Route by intent (spike A/B): robust→label+support primary; typical/refinement→label
        recall+function rerank; existential/discovery→function primary. Always returns route +
        coverage_status (stress/out-of-vocab targets surface as rare-or-absent, not fake answers)."""
        tv = self._target_vec(target); scores = self._scores(tv, intent)
        recs = self.store.records; N = len(recs)
        if intent == "robust" and behavior_label in self.by_label:
            order = sorted(self.by_label[behavior_label],
                           key=lambda i: (-(recs[i].get("shape_fractions") or {}).get(behavior_label, 0), scores[i]))
            route = "label+support primary"
        elif intent == "existential":
            order = list(np.argsort(scores)); route = "function primary"
        else:
            if behavior_label in self.by_label:
                seed = self.by_label[behavior_label]; seen = set(seed)
                order = sorted(seed + [i for i in range(N) if i not in seen], key=lambda i: scores[i])
            else:
                order = list(np.argsort(scores))
            route = "label recall + function rerank"
        if max_reactions:
            order = [i for i in order if (recs[i].get("n_reactions") or 99) <= max_reactions]
        top = list(order[:300])
        if top:
            v = np.array([self.vol[i] for i in top]); vn = v / (v.max() + 1e-9)
            top = [top[j] for j in np.argsort(np.array([scores[i] for i in top]) - rop_weight * vn)]
        chosen = self._diverse(top, k)
        cands = [self._candidate(i, float(scores[i]), intent) for i in chosen]
        res = self._result(cands, intent, target_object_type, behavior_label, k,
                           [route, "rop_evidence_rerank", "diverse_panel"])
        res["route"] = route
        res["coverage_status"] = self._coverage(behavior_label, cands)
        return res

    # ── candidate + result assembly ──────────────────────────────────────────────
    def _candidate(self, i, score, intent):
        r = self.store.records[i]
        supp = float((r.get("shape_fractions") or {}).get(r.get("dominant_shape"), 0.0))
        vol = float(self.vol[i]); paths = int(self.paths[i])
        vs = r.get("validity_summary") or {}
        cand = {
            "record_id": r.get("record_id"), "network_id": r.get("network_id"), "slice_id": r.get("slice_id"),
            "io": r.get("io_assignment"), "reactions": r.get("rules"), "n_reactions": r.get("n_reactions"),
            "dominant_shape": r.get("dominant_shape"), "match_score": self._match(score, intent),
            "evidence": {
                "evidence_tier": evidence_tier(supp, vol, paths),
                "shape_support": round(supp, 3), "volume_mean": round(vol, 4), "robust_path_count": paths,
                "rop_family": (r.get("rop_summary") or {}).get("atlas_family_label"),
                "invalid_rate": round(float(vs.get("n_failed", 0)) / max(vs.get("n_draws", 1) or 1, 1), 3),
            },
            "follow_ups": [],
        }
        return cand

    def _result(self, cands, intent, tobj, label, k, pipeline):
        for c in cands:                          # honest, candidate-specific follow-ups
            fu = c["follow_ups"]
            if c["match_score"] < 0.6:
                fu.append("weak match — behavior may be rare/out-of-vocab in this family; consider relaxing constraints")
            if intent == "existential" and c["evidence"]["shape_support"] < 0.5:
                fu.append("realized by some θ but not robust — re-query intent=robust for a reliable design")
            if c["evidence"]["evidence_tier"] in ("T3a", "T3b"):
                fu.append("robust over the Kd prior; good export candidate")
        fams = {c["evidence"]["rop_family"] for c in cands if c["evidence"]["rop_family"] is not None}
        nets = {c["network_id"] for c in cands}
        nk = max(len(cands), 1)
        return {
            "schema_version": RESULT_SCHEMA,
            "query": {"intent_type": intent, "target_object_type": tobj, "behavior_label": label, "k": k},
            "corpus": {"id": self.meta.get("source_in"), "n_records": len(self.store),
                       "u_grid_id": self.meta.get("grid", {}).get("id"),
                       "phenotyper_version": self.meta.get("phenotyper_version")},
            "pipeline": pipeline,
            "candidates": cands,
            "panel_diversity": {"mechanism_diversity": round(len(fams) / nk, 3),
                                "network_diversity": round(len(nets) / nk, 3)},
            "provenance": {"reader_version": READER_VERSION, "verifier_grounded": True,
                           "note": "evidence from the same phenotyper that labeled the atlas; no fabrication"},
        }

    # ── S2.6: index integration (optional — loaded from atlas_root/indexes) ───────
    def attach_indexes(self, root):
        ix = os.path.join(root, "indexes")
        try:
            import hnswlib
            h = hnswlib.Index(space="l2", dim=self.store.n_points)
            h.load_index(os.path.join(ix, "hnsw_function.bin"), max_elements=len(self.store)); h.set_ef(80)
            self.idx["hnsw"] = h
        except Exception:
            pass
        try:
            import pyarrow.parquet as pq
            d = pq.read_table(os.path.join(ix, "diffusion_coords.parquet")).to_pydict()
            nc = sum(1 for k in d if k.startswith("c"))
            self.idx["diff"] = {d["record_id"][i]: np.array([d[f"c{c}"][i] for c in range(nc)])
                                for i in range(len(d["record_id"]))}
        except Exception:
            pass
        try:
            mg = json.load(open(os.path.join(ix, "mapper_graph.json")))
            node_of = {}
            for n in mg["nodes"]:
                for rid in n["members"]:
                    node_of.setdefault(rid, n["id"])
            self.idx["mapper"] = {"nodes": {n["id"]: n for n in mg["nodes"]}, "node_of": node_of}
        except Exception:
            pass
        self.has_index = bool(self.idx)
        return self

    def neighbors(self, target, k=8):
        """Fast function-space NN via HNSW (target = record_id or a curve on u_grid)."""
        h = self.idx.get("hnsw")
        if h is None:
            return None
        vec = self.MED[self._row[target]] if isinstance(target, str) else normalize(np.asarray(target, float), "minmax")
        vec = np.nan_to_num(vec, nan=0.0).astype("float32")[None, :]
        lab, dist = h.knn_query(vec, k=min(k + 1, len(self.store)))
        out = []
        for nbr, d2 in zip(lab[0], dist[0]):
            r = self.store.records[int(nbr)]
            if isinstance(target, str) and r["record_id"] == target:
                continue
            out.append({"record_id": r["record_id"], "dominant_shape": r["dominant_shape"],
                        "dist": round(float(np.sqrt(max(d2, 0.0))), 3)})
        return out[:k]

    def explain_neighborhood(self, record_id, k=6):
        """Neighborhood map: the record's Mapper node + its k function-NN, with a local-move
        vs near-a-stratum-boundary signal from diffusion distance."""
        nb = self.neighbors(record_id, k) or []
        node_id = self.idx.get("mapper", {}).get("node_of", {}).get(record_id)
        node = self.idx.get("mapper", {}).get("nodes", {}).get(node_id) if node_id else None
        move, diff = None, self.idx.get("diff")
        if diff and nb and record_id in diff:
            dd = [float(np.linalg.norm(diff[record_id] - diff[n["record_id"]])) for n in nb if n["record_id"] in diff]
            if dd:
                move = "local-move" if np.median(dd) < 0.02 else "near-a-stratum-boundary"
        return {"record_id": record_id,
                "mapper_node": (node and {"id": node["id"], "dominant_shape": node["dominant_shape"],
                                          "size": node["size"], "shape_mix": node["shape_mix"]}),
                "function_neighbors": nb, "neighborhood_geometry": move}

    def concept_search(self, intent, k=10):
        """Records matching ALL given FCA predicates (a concept intent)."""
        if not hasattr(self, "_pred"):
            from build_concepts import predicates
            self._pred_names, self._pred = predicates(self.store)
        cols = [self._pred_names.index(p) for p in intent if p in self._pred_names]
        if not cols:
            return []
        mask = np.all(self._pred[:, cols], axis=1)
        return [{"record_id": self.store.records[i]["record_id"],
                 "dominant_shape": self.store.records[i]["dominant_shape"]}
                for i in np.where(mask)[0][:k]]


# ── candidate-card renderer (ASCII, agent/human readable) ──────────────────────
def render_card(c):
    e = c["evidence"]
    io = c.get("io") or {}
    rx = " ; ".join(c.get("reactions") or [])
    head = f"┌ {c['dominant_shape']:<22} match {c['match_score']:<5} [{e['evidence_tier']}]"
    lines = [head,
             f"│ {io.get('input_symbol','?')} → {io.get('output_symbol','?')}   ({c.get('n_reactions','?')} reactions)",
             f"│ {rx[:90]}{'…' if len(rx) > 90 else ''}",
             f"│ support {e['shape_support']} · ROP vol {e['volume_mean']} · robust_paths {e['robust_path_count']}"
             f" · family {e.get('rop_family')} · invalid {e['invalid_rate']}"]
    for f in c.get("follow_ups", [])[:2]:
        lines.append(f"│ ↳ {f}")
    lines.append("└" + "─" * 60)
    return "\n".join(lines)


# named prototype targets (stand-ins for the NL→target compiler) → (curve, nearest vocab label)
def prototypes(u):
    return {
        "bump":        (bump(u, 0, 2.5),                  "biphasic_peak"),
        "broad_bump":  (bump(u, 0, 3.8),                  "bandpass_with_plateau"),
        "right_peak":  (bump(u, 2.2, 1.6),                "biphasic_peak"),
        "left_peak":   (bump(u, -2.2, 1.6),               "biphasic_peak"),
        "valley":      (1 - bump(u, 0, 2.2),              "biphasic_valley"),
        "switch_on":   (sigmoid(u, 0, 1.6),               "monotone_activation"),
        "switch_off":  (sigmoid(u, 0, 1.6, falling=True), "monotone_repression"),
        "late_dip":    (sigmoid(u, -0.5, 1.6) - 0.3 * np.clip((u - 2.5) / 3.5, 0, 1), "monotone_activation"),
        "double_bump": (np.maximum(bump(u, -2.2, 1.1), bump(u, 2.2, 1.1)), "biphasic_peak"),
        "shoulder":    (sigmoid(u, 1.6, 2.4),             "thresholded_activation"),
    }


def render_result(res):
    q = res["query"]
    out = [f"ReaderResult · intent={q['intent_type']} · target={q['target_object_type']}"
           f" · label={q['behavior_label']} · pipeline={'→'.join(res['pipeline'])}",
           f"corpus={res['corpus']['n_records']} records · {res['corpus']['phenotyper_version']}"
           f" · panel mechDiv={res['panel_diversity']['mechanism_diversity']} netDiv={res['panel_diversity']['network_diversity']}",
           ""]
    out += [render_card(c) for c in res["candidates"]]
    return "\n".join(out)
