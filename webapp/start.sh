#!/bin/bash
# Biocircuits Explorer Startup Script

cd "$(dirname "$0")"

PORT=${BIOCIRCUITS_EXPLORER_PORT:-${ROP_PORT:-8088}}

echo "Starting Biocircuits Explorer Web Server..."
echo "Server will be available at: http://localhost:$PORT"
echo ""
echo "Press Ctrl+C to stop the server"
echo ""

julia -t auto --project=. server.jl
