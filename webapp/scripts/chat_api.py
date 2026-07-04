#!/usr/bin/env python3
# (iii) backend: a dependency-free HTTP endpoint for the engine-in-the-loop design AGENT.
#   POST /design-chat  {message, state, llm:{provider,apiKey,baseUrl,model}, top}
#     -> {kind, reply, family, cards:[...], info, state}   (design_agent.run_turn)
#   GET  /health  -> chat-backend readiness, label corpora present, AND live ENGINE readiness
# The frontend (agent-view.js) renders `reply` + `cards`; `state` is client-held and echoed
# back each turn (stateless server). The LLM key comes in the request body from the UI key
# panel (llm-settings.js); it is passed straight through and never logged or stored.
#   python3 webapp/scripts/chat_api.py            # binds 127.0.0.1:8765
import os, sys, json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import design_agent as agent
import engine_client as engine

HOST = os.environ.get("BNE_CHAT_HOST", "127.0.0.1")
PORT = int(os.environ.get("BNE_CHAT_PORT", "8765"))

def _norm_llm(llm):
    if not llm:
        return None
    prov = llm.get("provider", "openai")
    return {"provider": prov,
            "api_key": llm.get("apiKey") or llm.get("api_key"),
            "base_url": llm.get("baseUrl") or llm.get("base_url"),
            "model": llm.get("model") or ("gpt-5.4-mini" if prov == "openai" else "claude-sonnet-4-6"),
            "effort": llm.get("effort") or llm.get("reasoning_effort")}

class Handler(BaseHTTPRequestHandler):
    def _cors(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.send_header("Access-Control-Allow-Methods", "POST, GET, OPTIONS")
    def _json(self, code, obj):
        body = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self._cors()
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)
    def do_OPTIONS(self):
        self.send_response(204); self._cors(); self.end_headers()
    def do_GET(self):
        if self.path.split("?")[0] == "/health":
            corpora = {"dose": os.path.isdir(agent.DOSE_DS), "logic": os.path.isfile(agent.LOGIC_LABELS),
                       "analog": os.path.isfile(agent.ANALOG_LABELS), "contextual": os.path.isfile(agent.CONTEXTUAL_LABELS)}
            # Live compute engine: the agent can only return verified designs when this is up.
            eng = {"ready": engine.engine_ready(), "url": engine.engine_base_url()}
            return self._json(200, {"ok": True, "families": list(corpora.keys()), "corpora": corpora,
                                    "engine": eng})
        return self._json(404, {"error": "not found"})
    def do_POST(self):
        if self.path.split("?")[0] != "/design-chat":
            return self._json(404, {"error": "not found"})
        try:
            n = int(self.headers.get("Content-Length", 0) or 0)
            req = json.loads(self.rfile.read(n) or b"{}")
        except Exception as e:
            return self._json(400, {"error": f"bad request: {e}"})
        msg = (req.get("message") or "").strip()
        if not msg:
            return self._json(400, {"error": "empty message"})
        try:
            res = agent.run_turn(req.get("state") or {}, msg, _norm_llm(req.get("llm")), int(req.get("top", 3)))
            return self._json(200, res)
        except Exception as e:
            return self._json(500, {"error": f"chat failed: {e}"})
    def log_message(self, *a):   # never log request bodies (they carry the API key)
        try:
            sys.stderr.write(f"[chat_api] {self.command} {self.path.split('?')[0]}\n")
        except OSError:
            pass

def _parent_is_gone(pid, getppid=os.getppid, kill=os.kill):
    if getppid() == 1:
        return True
    try:
        kill(pid, 0)
        return False
    except OSError:
        return True

def _watch_parent(pid):
    # When launched by the macOS shell (BNE_CHAT_PARENT_PID set), exit once the
    # parent app is gone so this helper can never orphan. No-op for CLI/web use.
    import time
    while True:
        if _parent_is_gone(pid):
            sys.stderr.write("[chat_api] parent process gone — exiting\n")
            os._exit(0)
        time.sleep(2)

if __name__ == "__main__":
    parent = os.environ.get("BNE_CHAT_PARENT_PID")
    if parent and parent.isdigit():
        import threading
        threading.Thread(target=_watch_parent, args=(int(parent),), daemon=True).start()
    print(f"[chat_api] listening: POST http://{HOST}:{PORT}/design-chat   GET /health", flush=True)
    ThreadingHTTPServer((HOST, PORT), Handler).serve_forever()
