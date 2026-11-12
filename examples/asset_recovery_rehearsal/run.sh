#!/usr/bin/env bash
# One entry point for the rehearsal: create a local environment, run the drill,
# start/stop the Dagster UI, and run the automated checks.
#
# No cloud account, container runtime, or external service is required.

set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE="${REHEARSAL_WORKSPACE:-$HERE/.rehearsal}"
VENV="$HERE/.venv"

export REHEARSAL_WORKSPACE="$WORKSPACE"
export PYTHONPATH="$HERE/src"
export DAGSTER_HOME="$WORKSPACE/dagster_home"

ensure_env() {
  if [[ ! -x "$VENV/bin/python" ]]; then
    echo "creating local environment at $VENV"
    uv venv "$VENV" --python 3.11 >/dev/null
    if [[ "${REHEARSAL_USE_REPO:-1}" == "0" ]]; then
      echo "installing released dagster packages"
      uv pip install --python "$VENV/bin/python" "dagster" "dagster-webserver" \
        "asset-recovery-rehearsal @ file://$HERE" pytest >/dev/null
    else
      # Editable install of the local Dagster checkout, so the drill runs against
      # this repository's source rather than a released package.
      REPO_ROOT="$(cd "$HERE/../.." && pwd)"
      uv pip install --python "$VENV/bin/python" \
        -e "$REPO_ROOT/python_modules/dagster-pipes" \
        -e "$REPO_ROOT/python_modules/libraries/dagster-shared" \
        -e "$REPO_ROOT/python_modules/dagster" \
        -e "$REPO_ROOT/python_modules/dagster-graphql" \
        -e "$REPO_ROOT/python_modules/dagster-webserver" \
        -e "$HERE" pytest >/dev/null
    fi
  fi
}

run_cli() {
  "$VENV/bin/python" -m rehearsal.cli "$@"
}

cmd_bootstrap() {
  ensure_env
  echo "environment ready: $VENV"
  echo "workspace:         $WORKSPACE"
  run_cli status
}

cmd_reset() {
  echo "removing workspace $WORKSPACE"
  rm -rf "$WORKSPACE"
  cmd_bootstrap
}

cmd_drill() {
  ensure_env
  run_cli demonstrate
}

cmd_ui() {
  ensure_env
  run_cli up --port "${PORT:-3001}" --background
  echo "Dagster UI: http://localhost:${PORT:-3001}"
}

cmd_down() {
  ensure_env
  run_cli down
}

cmd_test() {
  ensure_env
  "$VENV/bin/python" -m pytest "$HERE/tests" -q
}

cmd_status() {
  ensure_env
  run_cli status
}

cmd_map() {
  ensure_env
  run_cli map
}

usage() {
  cat <<'EOF'
usage: run.sh <command> [args...]

convenience commands:
  bootstrap   create the local venv and workspace, install dependencies
  reset       delete the workspace and re-bootstrap (clean slate)
  drill       run the entire rehearsal end to end and write reports
  ui          start the Dagster UI in the background
  down        stop the background Dagster UI
  status      summarize sources and derived results in the workspace
  map         write the dependency map artifact
  test        run the automated verification suite

Any other command is forwarded to the rehearsal CLI, e.g.:
  run.sh generate | plan | run | fail ... | restore-gaps | recover
  run.sh correct | repair-scope | cancel | verify

environment:
  REHEARSAL_WORKSPACE   workspace directory (default: ./.rehearsal)
  PORT                  UI port for `ui` (default: 3001)
EOF
}

COMMAND="${1:-}"
shift || true
case "$COMMAND" in
  bootstrap) cmd_bootstrap ;;
  reset) cmd_reset ;;
  drill) cmd_drill ;;
  ui) cmd_ui ;;
  down) cmd_down ;;
  status) cmd_status ;;
  map) cmd_map ;;
  test) cmd_test ;;
  "")
    usage; exit 1 ;;
  *)
    ensure_env
    run_cli "$COMMAND" "$@" ;;
esac

