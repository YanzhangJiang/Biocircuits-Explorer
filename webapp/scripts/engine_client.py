#!/usr/bin/env python3
# HTTP client from the Python design agent to the LIVE Julia analysis engine — the real
# "compiler kernel": the equilibrium/ODE solver (parameter_scan_1d) + the phenotyper
# (behavior_families) the node Workspace already uses. Stdlib only (mirrors llm_compile.py).
#
# CONTRACT THAT MATTERS: every call maps a connection-refused / timeout / unreachable host
# to a typed {"engine_offline": True, ...} result, and an engine-side rejection (4xx/5xx,
# e.g. 409 need_network, 400 bad param) to {"error": ..., "_status": code}. It NEVER returns
# a fabricated curve, label, or metric. The agent surfaces engine_offline to the LLM, whose
# system prompt forbids inventing a result when the engine is down — closing the hole where a
# pure-lookup agent would hand back canned answers with the engine offline.
import os, json, urllib.request, urllib.error

def engine_base_url():
    port = os.environ.get("BIOCIRCUITS_EXPLORER_PORT") or os.environ.get("ROP_PORT") or "8088"
    host = os.environ.get("BIOCIRCUITS_EXPLORER_HOST", "127.0.0.1")
    return f"http://{host}:{port}"

def _post(path, payload, timeout):
    url = engine_base_url().rstrip("/") + path
    req = urllib.request.Request(url, data=json.dumps(payload).encode(),
                                 headers={"Content-Type": "application/json"}, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.load(r)
    except urllib.error.HTTPError as e:                      # engine answered but rejected
        try: body = json.load(e)
        except Exception: body = {}
        if not isinstance(body, dict): body = {"error": str(body)}
        body.setdefault("error", f"engine HTTP {e.code}")
        body["_status"] = e.code
        return body
    except (urllib.error.URLError, ConnectionError, OSError, TimeoutError) as e:
        return {"engine_offline": True, "error": f"compute engine unreachable at {url}: {e}"}

def engine_ready(timeout=3):
    """True only if the Julia engine answers GET /ready with 2xx (module initialised)."""
    url = engine_base_url().rstrip("/") + "/ready"
    try:
        with urllib.request.urlopen(urllib.request.Request(url, method="GET"), timeout=timeout) as r:
            return 200 <= getattr(r, "status", r.getcode()) < 300
    except Exception:
        return False

def build_model(reactions=None, kd=None, network=None, session_id=None, timeout=180):
    """Compile a network (legacy {reactions, kd} so the agent can build networks NOT in the
    atlas, or a full NetworkIR). Returns the model description incl. session_id, species,
    q_sym (candidate swept inputs), x_sym, K_sym — or {engine_offline}/{error}."""
    payload = {}
    if network is not None:
        payload["network"] = network
    elif reactions is not None:
        payload["reactions"] = list(reactions)
        payload["kd"] = [float(x) for x in kd] if kd is not None else [1.0] * len(list(reactions))
    else:
        return {"error": "build_model needs reactions+kd or a network"}
    if session_id:
        payload["session_id"] = session_id
    return _post("/api/build_model", payload, timeout)

def dose_response(session_id=None, *, network=None, param_symbol, output_exprs,
                  param_min=-6.0, param_max=6.0, n_points=160, fixed_qK=None, timeout=180):
    """Real 1-input dose-response: solves the equilibrium across param_symbol (a q/K symbol)
    and evaluates output_exprs (species linear combos). Returns param_values + output_traj
    (log-space) — the genuine computed curve — or {engine_offline}/{error}."""
    payload = {"param_symbol": param_symbol,
               "output_exprs": output_exprs if isinstance(output_exprs, list) else [output_exprs],
               "param_min": param_min, "param_max": param_max, "n_points": n_points}
    if session_id: payload["session_id"] = session_id
    if network is not None: payload["network"] = network
    if fixed_qK is not None: payload["fixed_qK"] = fixed_qK
    return _post("/api/parameter_scan_1d", payload, timeout)

def scan_2d(session_id=None, *, network=None, param1_symbol, param2_symbol, output_expr,
            param1_min=-6.0, param1_max=6.0, param2_min=-6.0, param2_max=6.0, n_grid=48,
            fixed_qK=None, timeout=300):
    """Real 2-input response SURFACE: solves the equilibrium on a param1×param2 grid and evaluates
    output_expr. Returns param1_values, param2_values, output_grid (log-space) — the genuine 2D
    design surface — or {engine_offline}/{error}."""
    payload = {"param1_symbol": param1_symbol, "param2_symbol": param2_symbol, "output_expr": output_expr,
               "param1_min": param1_min, "param1_max": param1_max,
               "param2_min": param2_min, "param2_max": param2_max, "n_grid": n_grid}
    if session_id: payload["session_id"] = session_id
    if network is not None: payload["network"] = network
    if fixed_qK is not None: payload["fixed_qK"] = fixed_qK
    return _post("/api/parameter_scan_2d", payload, timeout)

def phenotype_classify(session_id=None, *, network=None, input_symbol, output_expr, K=8, timeout=300):
    """Faithful verification: classify the 1-input dose-response with the SAME SISO phenotyper that
    labelled the dose atlas (shape_support = fraction of the Kd prior Π that shows the shape).
    Returns {dominant_shape, shape_support, shape_fractions, ...} — or {engine_offline}/{error}."""
    payload = {"input_symbol": input_symbol, "output_expr": output_expr, "K": K}
    if session_id: payload["session_id"] = session_id
    if network is not None: payload["network"] = network
    return _post("/api/phenotype_classify", payload, timeout)

def phenotype(session_id=None, *, network=None, change_qK, observe_x, path_scope="feasible",
              compute_volume=True, timeout=240):
    """On-demand phenotyper: classifies what the network actually DOES along change_qK while
    observing observe_x (a species). Returns path_records with exact_label/motif_label +
    volume (robustness) — the live verification — or {engine_offline}/{error}."""
    payload = {"change_qK": change_qK, "observe_x": observe_x,
               "path_scope": path_scope, "compute_volume": compute_volume}
    if session_id: payload["session_id"] = session_id
    if network is not None: payload["network"] = network
    return _post("/api/behavior_families", payload, timeout)

if __name__ == "__main__":   # smoke: build a 1-reaction net and sweep it (engine must be up)
    print("engine_base_url:", engine_base_url(), "ready:", engine_ready())
    m = build_model(reactions=["A + B <-> AB"], kd=[1.0])
    print("build_model:", json.dumps(m, default=str)[:300])
    if m.get("session_id"):
        c = dose_response(m["session_id"], param_symbol=m["q_sym"][0], output_exprs=["AB"], n_points=21)
        print("dose_response keys:", list(c.keys()) if isinstance(c, dict) else c)
