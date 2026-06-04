#!/bin/bash
# Biocircuits Explorer Startup Script
# Launches BOTH backends the frontend needs:
#   - Julia analysis server (node Workspace)     -> http://localhost:$PORT
#   - Python design-chat backend (Design Agent)  -> http://127.0.0.1:$CHAT_PORT
# The Python backend is dependency-free and runs the rule-based compiler with no
# LLM key. Set BNE_CHAT_DISABLE=1 to skip it (e.g. if you run chat_api.py yourself).

cd "$(dirname "$0")"

PORT=${BIOCIRCUITS_EXPLORER_PORT:-${ROP_PORT:-8088}}
CHAT_PORT=${BNE_CHAT_PORT:-8765}

CHAT_PID=""
if [ "${BNE_CHAT_DISABLE:-0}" != "1" ]; then
  if command -v python3 >/dev/null 2>&1; then
    echo "Starting Design Agent backend (chat_api.py) on http://127.0.0.1:$CHAT_PORT ..."
    # BNE_CHAT_PARENT_PID lets chat_api.py self-terminate if this script is killed
    # hard (SIGKILL bypasses the trap), so it can't be left orphaned.
    BNE_CHAT_PORT="$CHAT_PORT" BNE_CHAT_PARENT_PID="$$" python3 scripts/chat_api.py &
    CHAT_PID=$!
  else
    echo "python3 not found — the Design Agent backend will be offline (the Workspace still works)."
  fi
fi

# Stop the design-chat backend whenever this script exits (Ctrl+C, error, or normal exit).
cleanup() { [ -n "$CHAT_PID" ] && kill "$CHAT_PID" 2>/dev/null; }
trap cleanup EXIT INT TERM

echo "Starting Biocircuits Explorer Web Server..."
echo "Server will be available at: http://localhost:$PORT"
echo ""
echo "Press Ctrl+C to stop the server"
echo ""

julia -t auto --project=. server.jl
