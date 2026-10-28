#!/usr/bin/env bash
# Non-interactive Specula setup for a Claude Code cloud environment.
#
# Paste the following into the environment's "Setup script" field:
#
#   bash cloud/setup.sh
#
# The cloud VM already has Python 3, uv, OpenJDK 21, Maven and Gradle.
# Everything this script writes to disk is cached by the environment snapshot,
# so later sessions skip it. Keep total runtime under ~5 minutes.
#
# Optional env vars (set them in the environment dialog):
#   KAFKA_REPO  git URL to clone            (default: https://github.com/apache/kafka.git)
#   KAFKA_REF   branch or tag to check out  (default: trunk)
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
export SPECULA_ROOT="$ROOT"

KAFKA_REPO="${KAFKA_REPO:-https://github.com/apache/kafka.git}"
KAFKA_REF="${KAFKA_REF:-trunk}"

log() { printf '\n[specula-setup] %s\n' "$*"; }

# ── 1. Python venvs for the MCP servers and helper tools (parallel) ─────────
make_venv() {
  local dir="$1" req="$2"
  python3 -I -m venv "$dir/.venv" || return 1
  "$dir/.venv/bin/python" -I -m pip install -q --upgrade pip || true
  if [[ -n "$req" && -f "$req" ]]; then
    "$dir/.venv/bin/python" -I -m pip install -q -r "$req"
  else
    "$dir/.venv/bin/python" -I -m pip install -q mcp
  fi
}

log "Creating Python environments"
make_venv tools/trace_debugger   tools/trace_debugger/requirements.txt   & p1=$!
make_venv tools/spec_analyzer    tools/spec_analyzer/requirements.txt    & p2=$!
make_venv tools/inv_checking_tool ""                                     & p3=$!
make_venv tools/tlc_tools        tools/tlc_tools/requirements.txt        & p4=$!
make_venv tools/context_control  tools/context_control/requirements.txt & p5=$!

# ── 2. CFA jar for spec_analyzer (Maven, parallel with the venvs) ───────────
log "Building CFA jar"
( cd tools/cfa && mvn -q -B package -DskipTests ) & p6=$!

# ── 3. Kafka source checkout (parallel) ─────────────────────────────────────
if [[ ! -d kafka/.git ]]; then
  log "Cloning Kafka ($KAFKA_REF)"
  ( git clone --depth 1 --branch "$KAFKA_REF" "$KAFKA_REPO" kafka ) & p7=$!
else
  log "Kafka checkout already present"
  p7=""
fi

# ── 4. specula CLI (small, quick) ───────────────────────────────────────────
log "Installing specula CLI"
uv tool install -q -e . || pip install -q -e . || true

# ── 5. TLA+ jars: committed in lib/, download only if missing ───────────────
mkdir -p lib
[[ -f lib/tla2tools.jar ]] || curl -sSL -o lib/tla2tools.jar \
  https://github.com/tlaplus/tlaplus/releases/download/v1.8.0/tla2tools.jar || true
[[ -f lib/CommunityModules-deps.jar ]] || curl -sSL -o lib/CommunityModules-deps.jar \
  https://github.com/tlaplus/CommunityModules/releases/download/202505152026/CommunityModules-deps.jar || true

# ── 6. gh CLI (optional; used by some confirmation steps) ───────────────────
command -v gh >/dev/null 2>&1 || { apt-get update -qq && apt-get install -y -qq gh; } || true

# ── Wait and report ─────────────────────────────────────────────────────────
rc=0
for p in $p1 $p2 $p3 $p4 $p5 $p6 $p7; do
  wait "$p" || { rc=1; echo "[specula-setup] a background step failed (pid $p)"; }
done

log "Verification"
if java -cp lib/tla2tools.jar tlc2.TLC -help 2>&1 | grep -q "TLA+"; then echo "TLC OK"; else echo "TLC check failed"; fi
ls tools/cfa/target/*.jar 2>/dev/null || echo "CFA jar missing"
[[ -x tools/trace_debugger/.venv/bin/python ]] && echo "trace_debugger venv OK"
[[ -d kafka ]] && echo "kafka source at $ROOT/kafka"

# Never block session start on a non-critical failure.
exit 0
