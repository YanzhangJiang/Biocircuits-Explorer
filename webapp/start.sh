#!/bin/bash
# Biocircuits Explorer Startup Script
# Launches BOTH backends the frontend needs:
#   - Julia analysis server (node Workspace)     -> http://127.0.0.1:$PORT
#   - Python design-chat backend (Design Agent)  -> http://127.0.0.1:$CHAT_PORT
# The Python entrypoint is dependency-free, but design-chat requests need a
# configured LLM key. Set BNE_CHAT_DISABLE=1 to skip it.

cd "$(dirname "$0")"

PORT=${BIOCIRCUITS_EXPLORER_PORT:-${ROP_PORT:-8088}}
HOST=${BIOCIRCUITS_EXPLORER_HOST:-${ROP_HOST:-127.0.0.1}}
CHAT_PORT=${BNE_CHAT_PORT:-8765}
PARENT_PID=${BIOCIRCUITS_EXPLORER_PARENT_PID:-${ROP_PARENT_PID:-$$}}
DISPLAY_HOST="$HOST"
case "$HOST" in *:*) DISPLAY_HOST="[$HOST]" ;; esac
if [ -n "${BNE_CHAT_ALLOWED_ORIGIN:-}" ]; then
  CHAT_ALLOWED_ORIGIN="$BNE_CHAT_ALLOWED_ORIGIN"
else
  case "$HOST" in
    127.0.0.1|localhost|::1) CHAT_ALLOWED_ORIGIN="http://$DISPLAY_HOST:$PORT" ;;
    *) CHAT_ALLOWED_ORIGIN="http://127.0.0.1:$PORT" ;;
  esac
fi
export BIOCIRCUITS_EXPLORER_HOST="$HOST" ROP_HOST="$HOST"
export BIOCIRCUITS_EXPLORER_PORT="$PORT" ROP_PORT="$PORT"
export BIOCIRCUITS_EXPLORER_PARENT_PID="$PARENT_PID" ROP_PARENT_PID="$PARENT_PID"

CHAT_PID=""
if [ "${BNE_CHAT_DISABLE:-0}" != "1" ]; then
  if command -v python3 >/dev/null 2>&1; then
    echo "Starting Design Agent backend (chat_api.py) on http://127.0.0.1:$CHAT_PORT ..."
    echo "Design Agent local-dev origin: $CHAT_ALLOWED_ORIGIN"
    # BNE_CHAT_PARENT_PID lets chat_api.py self-terminate if this script is killed
    # hard (SIGKILL bypasses the trap), so it can't be left orphaned.
    # This launcher is the one explicit unauthenticated development path. Direct
    # and native launches must instead set BNE_CHAT_BEARER_TOKEN (>=32 chars).
    BNE_CHAT_HOST="127.0.0.1" \
      BNE_CHAT_PORT="$CHAT_PORT" \
      BNE_CHAT_PARENT_PID="$$" \
      BNE_CHAT_ALLOWED_ORIGIN="$CHAT_ALLOWED_ORIGIN" \
      BNE_CHAT_BEARER_TOKEN="" \
      BNE_CHAT_ALLOW_UNAUTHENTICATED_LOOPBACK="1" \
      python3 scripts/chat_api.py &
    CHAT_PID=$!
  else
    echo "python3 not found — the Design Agent backend will be offline (the Workspace still works)."
  fi
fi

# Stop the design-chat backend whenever this script exits (Ctrl+C, error, or normal exit).
cleanup() { [ -n "$CHAT_PID" ] && kill "$CHAT_PID" 2>/dev/null; }
trap cleanup EXIT INT TERM

echo "Starting Biocircuits Explorer Web Server..."
echo "Server will be available at: http://$DISPLAY_HOST:$PORT"
echo ""
echo "Press Ctrl+C to stop the server"
echo ""

julia -t auto --project=. server.jl
