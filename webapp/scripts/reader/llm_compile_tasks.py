"""Precompile the e2e_benchmark TASKS' NL with the LLM compiler → a specs JSON, so the E2E
can be re-run on bmac with LLM specs (isolating LLM-compiler vs rule-compiler; everything
downstream — routing, verify, corpus — identical)."""
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
from e2e_benchmark import TASKS
import nl_target_compiler as C
from llm_compile import llm_config_from_env

cfg = llm_config_from_env()
out = sys.argv[1] if len(sys.argv) > 1 else "/tmp/llm_specs.json"
specs = []
for i, (group, nl, gt_intent, gt_label, feat) in enumerate(TASKS):
    s = C.compile_target(nl, cfg)
    specs.append(s)
    print(f"[{i+1:2d}/{len(TASKS)}] {s.get('_source'):4s} intent={s.get('intent_type'):<11} "
          f"type={s.get('target_object_type'):<16} label={s.get('behavior_label_hint')}  :: {nl[:46]}",
          file=sys.stderr)
json.dump(specs, open(out, "w"))
n_llm = sum(1 for s in specs if s.get("_source") == "llm")
print(f"\nwrote {len(specs)} specs ({n_llm} from LLM, {len(specs)-n_llm} rule-fallback) → {out}")
