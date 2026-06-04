#!/usr/bin/env python3
# Step 7: candidate-card renderers for the behavior-language families. Turns a label row
# (logic / analog / contextual; dose-shape is design_search.py's existing card) into a
# structured card dict + an ASCII rendering for the terminal/agent. The frontend
# (agent-view.js) can consume the dict; the ASCII view is for the CLI + tests/fixtures.
#   python3 webapp/scripts/cards.py --logic  datasets/latent-atlas-logic-v0/logic_labels.jsonl  --top 6
#   python3 webapp/scripts/cards.py --analog datasets/latent-atlas-analog-v0/analog_labels.jsonl --top 6
import sys, os, json, argparse

# (A,B) corner order (00,01,10,11); output high=1 — mirrors evaluators.jl LOGIC_TABLES.
LOGIC_TABLES = {
    "AND": (0,0,0,1), "OR": (0,1,1,1), "NAND": (1,1,1,0), "NOR": (1,0,0,0),
    "XOR": (0,1,1,0), "XNOR": (1,0,0,1), "NIMPLY": (0,0,1,0), "IMPLY": (1,1,0,1),
    "NOT_A": (1,1,0,0), "NOT_B": (1,0,1,0), "A": (0,0,1,1), "B": (0,1,0,1),
    "CIMPLY": (1,0,1,1), "BNIMPLY": (0,1,0,0), "TRUE": (1,1,1,1), "FALSE": (0,0,0,0),
}

def _truth_grid(gate):
    """ASCII 2x2 truth table for a gate name (rows A=lo/hi, cols B=lo/hi)."""
    t = LOGIC_TABLES.get(gate)
    if t is None and isinstance(gate, str) and gate.startswith("none("):
        # legacy label generated before the gate got a name: parse the tuple
        try: t = tuple(int(x) for x in gate[gate.index("(")+1:gate.index(")")].replace(" ", "").split(","))
        except Exception: t = None
    if t is None:
        return f"   (table {gate})"
    c00, c01, c10, c11 = t  # (A,B): 00,01,10,11
    return ("        B=lo  B=hi\n"
            f"   A=lo   {c00}     {c01}\n"
            f"   A=hi   {c10}     {c11}")

def _bar(x, lo=0.0, hi=1.0, w=12):
    x = max(lo, min(hi, x)); n = int(round((x - lo) / (hi - lo) * w))
    return "█" * n + "·" * (w - n)

def logic_card(row):
    g = row.get("realized_gate"); ins = row.get("inputs"); out = row.get("output")
    card = {"family": "logic", "evidence_tier": "tier-1-scan-observed",
            "realized_gate": g, "inputs": ins, "output": out,
            "gate_support": row.get("gate_support"), "margin_decades": row.get("median_margin_decades"),
            "network_id": row.get("network_id"), "rules": row.get("rules")}
    ascii = (f"[LOGIC] {ins[0]},{ins[1]} -> {out}   gate={g}   "
             f"support={card['gate_support']}  margin={card['margin_decades']} dec\n"
             f"{_truth_grid(g)}\n   network: {card['network_id']}")
    return card, ascii

def _analog_kind(row):
    co = row.get("coactivation_corr") or 0; ra = row.get("ratio_corr") or 0; bp = row.get("bump_fraction") or 0
    if bp >= 0.5: return "two-input bump (interior peak)"
    if co >= 0.8: return "coactivation / AND-like surface"
    if abs(ra) >= 0.5: return "ratio-sensing surface"
    return "mixed / monotone surface"

def analog_card(row):
    ins = row.get("inputs"); out = row.get("output")
    card = {"family": "analog_surface", "evidence_tier": "tier-1-scan-observed",
            "kind": _analog_kind(row), "inputs": ins, "output": out,
            "bump_fraction": row.get("bump_fraction"), "dynamic_range_decades": row.get("dynamic_range_decades"),
            "ratio_corr": row.get("ratio_corr"), "coactivation_corr": row.get("coactivation_corr"),
            "network_id": row.get("network_id"), "rules": row.get("rules")}
    ascii = (f"[ANALOG] {ins[0]},{ins[1]} -> {out}   {card['kind']}\n"
             f"   coactivation {_bar(row.get('coactivation_corr') or 0)} {row.get('coactivation_corr')}\n"
             f"   ratio        {_bar(abs(row.get('ratio_corr') or 0))} {row.get('ratio_corr')}\n"
             f"   bump_frac    {_bar(row.get('bump_fraction') or 0)} {row.get('bump_fraction')}   "
             f"dyn_range={row.get('dynamic_range_decades')} dec\n   network: {card['network_id']}")
    return card, ascii

def contextual_card(result):
    """result = evaluate_contextual output dict."""
    rows = result.get("per_context", [])
    card = {"family": "contextual_versatility", "context_sym": result.get("context_sym"),
            "reprogrammable": result.get("reprogrammable"), "network_id": result.get("network_id"),
            "inputs": result.get("inputs"), "output": result.get("output"), "rules": result.get("rules"),
            "distinct_gates": result.get("distinct_robust_gates"), "per_context": rows}
    lines = [f"[CONTEXT] vary {result.get('context_sym')}  reprogrammable={result.get('reprogrammable')}  "
             f"distinct={result.get('distinct_robust_gates')}"]
    for p in rows:
        lines.append(f"   {result.get('context_sym')}={p.get('context'):<5} -> {p.get('gate'):<10} support={p.get('support')}")
    return card, "\n".join(lines)

def _iter(path):
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line: yield json.loads(line)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--logic"); ap.add_argument("--analog"); ap.add_argument("--top", type=int, default=6)
    ap.add_argument("--json", action="store_true")
    a = ap.parse_args()
    if a.logic:
        rows = sorted(_iter(a.logic), key=lambda r: (-(r.get("gate_support") or 0), -(r.get("median_margin_decades") or 0)))
        # show a spread of distinct gates rather than 6 ANDs
        seen = {}; picked = []
        for r in rows:
            g = r.get("realized_gate")
            if seen.get(g, 0) < 1: picked.append(r); seen[g] = seen.get(g, 0) + 1
            if len(picked) >= a.top: break
        for r in picked:
            c, s = logic_card(r); print(json.dumps(c) if a.json else s); print()
    if a.analog:
        rows = sorted(_iter(a.analog), key=lambda r: -(r.get("dynamic_range_decades") or 0))[:a.top]
        for r in rows:
            c, s = analog_card(r); print(json.dumps(c) if a.json else s); print()

if __name__ == "__main__":
    main()
