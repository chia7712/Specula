#!/usr/bin/env bash
# Launch one of Specula's stdio MCP servers from wherever this repo is checked out.
# Usage: cloud/mcp.sh <tracedebugger|spec_analyzer|inv_checking_tool>
# Used by .mcp.json so the config needs no absolute paths.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export SPECULA_ROOT="$ROOT"

case "${1:-}" in
  tracedebugger)      dir="$ROOT/tools/trace_debugger" ;;
  spec_analyzer)      dir="$ROOT/tools/spec_analyzer" ;;
  inv_checking_tool)  dir="$ROOT/tools/inv_checking_tool" ;;
  *) echo "usage: $0 <tracedebugger|spec_analyzer|inv_checking_tool>" >&2; exit 2 ;;
esac

py="$dir/.venv/bin/python"
if [[ ! -x "$py" ]]; then
  echo "venv missing for $1; run cloud/setup.sh first" >&2
  exit 1
fi

exec "$py" "$dir/mcp_server.py"
