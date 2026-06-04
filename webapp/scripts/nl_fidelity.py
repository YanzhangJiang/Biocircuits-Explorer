#!/usr/bin/env python3
# NL->spec fidelity benchmark (the "serious harness" check on the conversational layer).
# For each benchmark task: compile its natural_language with the design_chat compiler and
# compare the compiled behavior_class to the task's ground-truth behavior_spec.behavior_class.
# Also measures PARAPHRASE ROBUSTNESS on task 18 (5 paraphrases of one bandpass spec) — the
# honest stress test, where a keyword compiler is expected to break and an LLM compiler should
# not. This quantifies exactly how much the rule-based first cut needs the LLM swap.
#   python3 webapp/scripts/nl_fidelity.py
import sys, os, json, glob
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import design_chat as dc

ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), "..", ".."))
TASKS = os.path.join(ROOT, "benchmarks", "tasks")

# Near-class families: bandpass_with_plateau vs biphasic_peak differ only by a plateau the
# NL often does not state, so exact-match under-credits a defensible compile. We also score
# whether the compiler got the right FAMILY.
FAMILY = {
    "bandpass_with_plateau": "peak", "biphasic_peak": "peak", "window_repression": "peak",
    "biphasic_valley": "peak", "monotone_activation": "mono_up", "activation_with_saturation": "mono_up",
    "monotone_repression": "mono_down", "repression_with_floor": "mono_down",
    "thresholded_activation": "threshold",
}
def _fam(c): return FAMILY.get(c, c)   # logic:* / None keep their own identity

def compiled_class(nl):
    spec, _ = dc.compile_nl(nl)
    g = spec["goal"]
    if g.get("behavior_family") == "logic":
        return "logic:" + g["logic"]["logic_type"]
    return g.get("behavior_class")

def main():
    rows = []
    for f in sorted(glob.glob(os.path.join(TASKS, "*.json"))):
        d = json.load(open(f)); bs = d.get("behavior_spec", {})
        nl = d.get("natural_language"); exp = bs.get("behavior_class")
        if not nl or not exp:
            continue
        got = compiled_class(nl)
        rows.append((d["task_id"], exp, got, got == exp))
    n = len(rows); ok = sum(r[3] for r in rows)
    near = sum(1 for _, exp, got, _ in rows if _fam(got) == _fam(exp))
    print(f"=== NL->spec class fidelity (canonical natural_language) ===")
    print(f"exact class  = {ok}/{n} = {ok/n:.2f}")
    print(f"near (family)= {near}/{n} = {near/n:.2f}\n")
    for tid, exp, got, hit in rows:
        if not hit:
            tag = "near" if _fam(got) == _fam(exp) else "MISS"
            print(f"  {tag} {tid:34s} expected={exp:26s} got={got}")

    # paraphrase robustness on task 18
    p = os.path.join(TASKS, "18_paraphrase_bandpass.json")
    if os.path.isfile(p):
        d = json.load(open(p)); bs = d.get("behavior_spec", {})
        exp = bs.get("behavior_class")
        phr = [d.get("natural_language")] + list(bs.get("paraphrases") or d.get("paraphrases") or [])
        got = [compiled_class(x) for x in phr]
        hit = sum(1 for g in got if g == exp)
        nearhit = sum(1 for g in got if _fam(g) == _fam(exp))
        print(f"\n=== paraphrase robustness (task 18, expected {exp}) ===")
        print(f"exact class  = {hit}/{len(phr)}")
        print(f"near (family)= {nearhit}/{len(phr)}")
        for x, g in zip(phr, got):
            mark = "ok  " if g == exp else ("near" if _fam(g) == _fam(exp) else "MISS")
            print(f"  [{mark}] {g:26s} <- \"{x}\"")
        print("\n(The exact bandpass-vs-biphasic split is a taxonomy ambiguity — they differ only "
              "by a plateau the paraphrase rarely states; near=family is the fair read. The fix is a "
              "one-turn disambiguation in chat, not a better compiler.)")

if __name__ == "__main__":
    main()
