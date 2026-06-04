#!/usr/bin/env python3
# Replay a DesignAgentTrace's ENGINE tool calls against the live engine (no LLM) and check the engine
# still produces the recorded verdicts — a lightweight regression for engine drift / reproducibility.
# This is the minimal "replay from trace" capability (not a full failure-inbox/CI): given a trace, the
# tool args are recorded, so re-running them deterministically must reproduce the same shape/gate/support.
#
#   python3 webapp/scripts/replay_trace.py            # replay the most recent trace
#   python3 webapp/scripts/replay_trace.py <trace_id> # replay a specific trace
#   python3 webapp/scripts/replay_trace.py --all       # replay every trace
import os, sys, json
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import design_agent as A
import engine_client as E

TRACES = os.path.join(A.TRACE_DIR, "traces.jsonl")
_ENGINE_TOOLS = ("simulate", "simulate_2d")              # the deterministic engine surface to re-check
_VERDICT_KEYS = ("phenotype_shape", "shape_support", "realized_gate", "margin_decades", "interior_peak")

def _reexec(tc):
    name, args = tc.get("tool"), dict(tc.get("args") or {})
    fn = A.TOOLS_DISPATCH.get(name)
    if not fn:
        return None, "unknown tool"
    try:
        return fn(**args), None
    except Exception as e:
        return None, f"{type(e).__name__}: {e}"

def replay(trace):
    if not E.engine_ready():
        print("  engine offline — cannot replay live tool calls"); return False
    ok = True; checked = 0
    for tc in trace.get("tool_calls", []):
        if tc.get("tool") not in _ENGINE_TOOLS:
            continue
        checked += 1
        res, err = _reexec(tc)
        if err:
            print(f"  [{tc['tool']}] RE-EXEC ERROR: {err}"); ok = False; continue
        old, new = tc.get("summary") or {}, A._tool_summary(res)
        drift = {k: [old.get(k), new.get(k)] for k in _VERDICT_KEYS
                 if (k in old or k in new) and old.get(k) != new.get(k)}
        hash_ok = (A._hash(res) == tc.get("result_hash"))
        print(f"  [{tc['tool']}] {'MATCH' if not drift else 'DRIFT'}  hash{'=' if hash_ok else '≠'}"
              + (f"  drift={json.dumps(drift, default=str)}" if drift else ""))
        if drift:
            ok = False
    if not checked:
        print("  (no engine tool calls in this trace — nothing to replay)")
    return ok

def main():
    if not os.path.isfile(TRACES):
        print("no traces at", TRACES); return
    traces = [json.loads(l) for l in open(TRACES) if l.strip()]
    args = sys.argv[1:]
    sel = traces if (args and args[0] == "--all") else \
          ([t for t in traces if t.get("trace_id") == args[0]] if args else traces[-1:])
    if not sel:
        print("no matching trace"); return
    all_ok = True
    for t in sel:
        print(f"\nreplay {t.get('trace_id')}  msg={t.get('raw_user_message','')[:60]!r}  "
              f"family={t.get('family')}  tags={t.get('failure_tags')}")
        all_ok = replay(t) and all_ok
    print("\n" + ("ALL MATCH ✓" if all_ok else "DRIFT/ERRORS DETECTED ✗"))

if __name__ == "__main__":
    main()
