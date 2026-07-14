#!/usr/bin/env python3
# (iii) backend: a dependency-free HTTP endpoint for the engine-in-the-loop design AGENT.
#   POST /design-chat  {message, state, llm:{provider,apiKey,baseUrl,model}, top}
#     -> {kind, reply, family, cards:[...], info, state}   (design_agent.run_turn)
#   GET  /health  -> chat-backend readiness, label corpora present, AND live ENGINE readiness
# The frontend (agent-view.js) renders `reply` + `cards`; `state` is client-held and echoed
# back each turn (stateless server). The LLM key comes in the request body from the UI key
# panel (llm-settings.js); it is passed straight through and never logged or stored.
# Security contract:
#   BNE_CHAT_ALLOWED_ORIGIN=<exact loopback origin>   required
#   BNE_CHAT_BEARER_TOKEN=<at least 32 characters>    required outside local dev
#   BNE_CHAT_INSTANCE_NONCE=<at least 32 characters>  required outside local dev
#   BNE_CHAT_ALLOW_UNAUTHENTICATED_LOOPBACK=1        explicit local-dev-only mode
# `webapp/start.sh` supplies the local-dev contract. The native macOS shell
# rotates a bearer token for every helper launch and injects it into its WKWebView.
import hmac, os, sys, json, threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlsplit
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import design_agent as agent
import engine_client as engine

HOST = os.environ.get("BNE_CHAT_HOST", "127.0.0.1")
PORT = int(os.environ.get("BNE_CHAT_PORT", "8765"))
ALLOWED_ORIGIN = os.environ.get("BNE_CHAT_ALLOWED_ORIGIN", "").strip()
BEARER_TOKEN = os.environ.get("BNE_CHAT_BEARER_TOKEN", "").strip()
INSTANCE_NONCE = os.environ.get("BNE_CHAT_INSTANCE_NONCE", "").strip()
SERVICE_IDENTITY = "biocircuits-design-chat"
ALLOW_UNAUTHENTICATED_LOOPBACK = os.environ.get(
    "BNE_CHAT_ALLOW_UNAUTHENTICATED_LOOPBACK", ""
).strip().lower() in ("1", "true", "yes", "on")
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


def _validate_runtime_contract(
    allowed_origin=ALLOWED_ORIGIN,
    bearer_token=BEARER_TOKEN,
    allow_unauthenticated_loopback=ALLOW_UNAUTHENTICATED_LOOPBACK,
    bind_host=HOST,
    instance_nonce=INSTANCE_NONCE,
):
    if bind_host != bind_host.strip() or bind_host.lower() not in _LOOPBACK_ORIGIN_HOSTS:
        raise ValueError("BNE_CHAT_HOST must be a literal loopback host")
    if not _is_exact_loopback_origin(allowed_origin):
        raise ValueError(
            "BNE_CHAT_ALLOWED_ORIGIN must be one exact http(s) loopback origin "
            "without a path (for example http://127.0.0.1:18088)"
        )
    if allow_unauthenticated_loopback:
        if instance_nonce and (len(instance_nonce) < 32 or instance_nonce != instance_nonce.strip()):
            raise ValueError(
                "BNE_CHAT_INSTANCE_NONCE must contain at least 32 non-whitespace characters"
            )
        return
    if len(bearer_token) < 32 or bearer_token != bearer_token.strip():
        raise ValueError(
            "BNE_CHAT_BEARER_TOKEN must contain at least 32 non-whitespace characters"
        )
    if len(instance_nonce) < 32 or instance_nonce != instance_nonce.strip():
        raise ValueError(
            "BNE_CHAT_INSTANCE_NONCE must contain at least 32 non-whitespace characters"
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
        self.send_header("Access-Control-Allow-Headers", "Content-Type, Authorization")
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
    def _bearer_is_valid(self):
        authorization = (self.headers.get("Authorization") or "").strip()
        scheme, separator, supplied = authorization.partition(" ")
        return (
            bool(separator) and
            scheme.lower() == "bearer" and
            bool(BEARER_TOKEN) and
            hmac.compare_digest(supplied, BEARER_TOKEN)
        )
    def _authorize(self):
        origin = (self.headers.get("Origin") or "").strip()
        token_valid = self._bearer_is_valid()

        # Browsers always have to present the one configured loopback Origin.
        # A token-authenticated non-browser probe (the native URLSession) may
        # omit Origin, but the explicit unauthenticated dev mode may not.
        if origin and not self._origin_is_allowed():
            self._json(403, {"error": "origin forbidden"})
            return False
        if not origin and not token_valid:
            self._json(403, {"error": "origin required"})
            return False
        if not token_valid and not (ALLOW_UNAUTHENTICATED_LOOPBACK and origin):
            self._json(
                401,
                {"error": "bearer token required"},
                cors=self._origin_is_allowed(),
                extra_headers=(("WWW-Authenticate", "Bearer"),),
            )
            return False
        return True
    def do_OPTIONS(self):
        path = self.path.split("?")[0]
        expected_method = {"/health": "GET", "/design-chat": "POST"}.get(path)
        if not self._origin_is_allowed():
            return self._json(403, {"error": "origin forbidden"})
        requested_method = (self.headers.get("Access-Control-Request-Method") or "").upper()
        if expected_method is None or requested_method != expected_method:
            return self._json(405, {"error": "preflight method forbidden"}, cors=True)
        requested_headers = {
            item.strip().lower()
            for item in (self.headers.get("Access-Control-Request-Headers") or "").split(",")
            if item.strip()
        }
        if not requested_headers.issubset({"authorization", "content-type"}):
            return self._json(403, {"error": "preflight headers forbidden"}, cors=True)
        self.send_response(204)
        self._cors()
        self.send_header("Content-Length", "0")
        self.end_headers()
    def do_GET(self):
        if self.path.split("?")[0] == "/identity":
            # This endpoint deliberately contains no bearer-protected data. It
            # lets the native shell prove process ownership before disclosing
            # the per-launch bearer to /health.
            return self._json(200, {
                "service": SERVICE_IDENTITY,
                "instance_nonce": INSTANCE_NONCE,
            })
        if not self._authorize():
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
        if not self._authorize():
            return
        if self.path.split("?")[0] != "/design-chat":
            return self._json(404, {"error": "not found"}, cors=self._origin_is_allowed())
        try:
            n = int(self.headers.get("Content-Length", 0) or 0)
            req = json.loads(self.rfile.read(n) or b"{}")
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
            res = agent.run_turn(req.get("state") or {}, msg, _norm_llm(req.get("llm")), int(req.get("top", 3)))
            return self._json(200, res, cors=self._origin_is_allowed())
        except Exception as e:
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
