#!/usr/bin/env python3
# (iii) backend: a dependency-free HTTP endpoint for the engine-in-the-loop design AGENT.
#   POST /design-chat  {message, state, llm:{provider,apiKey,baseUrl,model}, top}
#     -> {kind, reply, family, cards:[...], info, state}   (design_agent.run_turn)
#   GET  /health  -> chat-backend readiness, label corpora present, AND live ENGINE readiness
# The frontend (agent-view.js) renders `reply` + `cards`; `state` is client-held and echoed
# back each turn (stateless server). The LLM key comes in the request body from the UI key
# panel (llm-settings.js); it is passed straight through and never logged or stored.
# Local-only contract (single-user tool): the helper binds to loopback and
# accepts browser requests only from one exact loopback Origin
# (BNE_CHAT_ALLOWED_ORIGIN, e.g. http://127.0.0.1:8088). An optional
# BNE_CHAT_INSTANCE_NONCE is echoed by /health so the native shell can tell its
# own helper apart from a stale process on the same port.
import hmac, os, sys, json, threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlsplit
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import design_agent as agent
import engine_client as engine
import design_target_compile as target_compiler

HOST = os.environ.get("BNE_CHAT_HOST", "127.0.0.1")
PORT = int(os.environ.get("BNE_CHAT_PORT", "8765"))
ALLOWED_ORIGIN = os.environ.get("BNE_CHAT_ALLOWED_ORIGIN", "").strip()
INSTANCE_NONCE = os.environ.get("BNE_CHAT_INSTANCE_NONCE", "").strip()
SERVICE_IDENTITY = "biocircuits-design-chat"
_LOOPBACK_ORIGIN_HOSTS = frozenset(("127.0.0.1", "localhost", "::1"))
CHAT_TURN_MAX_CONCURRENCY_HARD_LIMIT = 32


def _strict_positive_int(name, raw, default, *, maximum):
    text = str(raw or "").strip()
    if not text:
        return default
    try:
        value = int(text, 10)
    except (TypeError, ValueError) as error:
        raise ValueError(
            f"{name} must be an integer between 1 and {maximum}, got {text!r}"
        ) from error
    if value < 1 or value > maximum or str(value) != text:
        raise ValueError(
            f"{name} must be an integer between 1 and {maximum}, got {text!r}"
        )
    return value


def _chat_turn_max_concurrency(raw=None):
    if raw is None:
        raw = os.environ.get("BNE_CHAT_MAX_CONCURRENT_TURNS", "")
    return _strict_positive_int(
        "BNE_CHAT_MAX_CONCURRENT_TURNS",
        raw,
        2,
        maximum=CHAT_TURN_MAX_CONCURRENCY_HARD_LIMIT,
    )


CHAT_TURN_MAX_CONCURRENCY = _chat_turn_max_concurrency()
CHAT_TURN_SEMAPHORE = threading.BoundedSemaphore(CHAT_TURN_MAX_CONCURRENCY)


def _is_exact_loopback_origin(origin):
    """Accept only a canonical http(s) Origin with a literal loopback host."""
    if not origin or origin != origin.strip():
        return False
    try:
        parsed = urlsplit(origin)
        port = parsed.port  # forces validation of malformed/non-numeric ports
    except ValueError:
        return False
    if parsed.scheme not in ("http", "https"):
        return False
    if parsed.username is not None or parsed.password is not None:
        return False
    if parsed.path or parsed.query or parsed.fragment:
        return False
    host = (parsed.hostname or "").lower()
    if host not in _LOOPBACK_ORIGIN_HOSTS:
        return False
    if port is not None and not 1 <= port <= 65535:
        return False
    rendered_host = f"[{host}]" if ":" in host else host
    canonical = f"{parsed.scheme}://{rendered_host}"
    if port is not None:
        canonical += f":{port}"
    return hmac.compare_digest(origin, canonical)


def _validate_runtime_contract(allowed_origin=ALLOWED_ORIGIN, bind_host=HOST):
    if bind_host != bind_host.strip() or bind_host.lower() not in _LOOPBACK_ORIGIN_HOSTS:
        raise ValueError("BNE_CHAT_HOST must be a literal loopback host")
    if not _is_exact_loopback_origin(allowed_origin):
        raise ValueError(
            "BNE_CHAT_ALLOWED_ORIGIN must be one exact http(s) loopback origin "
            "without a path (for example http://127.0.0.1:18088)"
        )

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
        self.send_header("Access-Control-Allow-Origin", ALLOWED_ORIGIN)
        self.send_header("Vary", "Origin")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.send_header("Access-Control-Allow-Methods", "POST, GET, OPTIONS")
        self.send_header("Access-Control-Max-Age", "600")
    def _json(self, code, obj, *, cors=False, extra_headers=()):
        body = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Cache-Control", "no-store")
        if cors:
            self._cors()
        for name, value in extra_headers:
            self.send_header(name, value)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)
    def _origin_is_allowed(self):
        origin = (self.headers.get("Origin") or "").strip()
        return bool(origin) and hmac.compare_digest(origin, ALLOWED_ORIGIN)
    def _authorize(self, *, origin_required):
        # Browser requests always carry Origin and it must be the one loopback
        # workspace origin. A local non-browser probe (the native shell's
        # URLSession, curl) may omit Origin on GET /health only.
        origin = (self.headers.get("Origin") or "").strip()
        if origin and not self._origin_is_allowed():
            self._json(403, {"error": "origin forbidden"})
            return False
        if not origin and origin_required:
            self._json(403, {"error": "origin required"})
            return False
        return True
    def do_OPTIONS(self):
        if not self._origin_is_allowed():
            return self._json(403, {"error": "origin forbidden"})
        self.send_response(204)
        self._cors()
        self.send_header("Content-Length", "0")
        self.end_headers()
    def do_GET(self):
        if not self._authorize(origin_required=False):
            return
        if self.path.split("?")[0] == "/health":
            corpora = {"dose": os.path.isdir(agent.DOSE_DS), "logic": os.path.isfile(agent.LOGIC_LABELS),
                       "analog": os.path.isfile(agent.ANALOG_LABELS), "contextual": os.path.isfile(agent.CONTEXTUAL_LABELS)}
            # Live compute engine: the agent can only return verified designs when this is up.
            eng = {"ready": engine.engine_ready(), "url": engine.engine_base_url()}
            return self._json(200, {"ok": True, "service": SERVICE_IDENTITY,
                                    "instance_nonce": INSTANCE_NONCE,
                                    "families": list(corpora.keys()), "corpora": corpora,
                                    "engine": eng}, cors=self._origin_is_allowed())
        return self._json(404, {"error": "not found"}, cors=self._origin_is_allowed())
    def do_POST(self):
        if not self._authorize(origin_required=True):
            return
        path = self.path.split("?")[0]
        if path not in ("/design-chat", "/compile-target"):
            return self._json(404, {"error": "not found"}, cors=self._origin_is_allowed())
        try:
            n = int(self.headers.get("Content-Length", 0) or 0)
            if n < 0 or n > 2 * 1024 * 1024:
                return self._json(413, {"error": "request body too large"}, cors=self._origin_is_allowed())
            req = json.loads(self.rfile.read(n) or b"{}")
            if not isinstance(req, dict):
                raise ValueError("expected a JSON object")
            if req.get("llm") is not None and not isinstance(req["llm"], dict):
                raise ValueError("llm must be an object")
            if not isinstance(req.get("message", ""), str):
                raise ValueError("message must be text")
        except Exception as e:
            return self._json(400, {"error": f"bad request: {e}"}, cors=self._origin_is_allowed())
        msg = (req.get("message") or "").strip()
        if not msg:
            return self._json(400, {"error": "empty message"}, cors=self._origin_is_allowed())
        admitted = CHAT_TURN_SEMAPHORE.acquire(blocking=False)
        if not admitted:
            return self._json(
                429,
                {
                    "error": {
                        "code": "chat_capacity_exhausted",
                        "message": "Design Agent capacity is full. Retry later.",
                        "retryable": True,
                    },
                    "code": "chat_capacity_exhausted",
                    "retryable": True,
                },
                cors=self._origin_is_allowed(),
                extra_headers=(("Retry-After", "1"),),
            )
        try:
            if path == "/compile-target":
                res = target_compiler.compile_target(msg, _norm_llm(req.get("llm")), req.get("target"))
                return self._json(200, res, cors=self._origin_is_allowed())
            res = agent.run_turn(req.get("state") or {}, msg, _norm_llm(req.get("llm")), int(req.get("top", 3)))
            return self._json(200, res, cors=self._origin_is_allowed())
        except target_compiler.TargetCompileError as e:
            return self._json(422, {"error": str(e), "code": e.code}, cors=self._origin_is_allowed())
        except Exception as e:
            if path == "/compile-target":
                return self._json(500, {"error": "Target compilation failed.", "code": "target_compile_failed"}, cors=self._origin_is_allowed())
            return self._json(500, {"error": f"chat failed: {e}"}, cors=self._origin_is_allowed())
        finally:
            CHAT_TURN_SEMAPHORE.release()
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
    try:
        _validate_runtime_contract()
    except ValueError as error:
        sys.stderr.write(f"[chat_api] configuration error: {error}\n")
        raise SystemExit(2)
    parent = os.environ.get("BNE_CHAT_PARENT_PID")
    if parent and parent.isdigit():
        threading.Thread(target=_watch_parent, args=(int(parent),), daemon=True).start()
    print(
        f"[chat_api] listening: POST http://{HOST}:{PORT}/design-chat   "
        f"GET /health   max_turns={CHAT_TURN_MAX_CONCURRENCY}",
        flush=True,
    )
    ThreadingHTTPServer((HOST, PORT), Handler).serve_forever()
