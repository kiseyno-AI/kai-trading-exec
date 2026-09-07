#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

if [ -f .env ]; then
  set -a
  source ./.env
  set +a
fi

PYTHON_BIN="${PYTHON_BIN:-python3.12}"
HOST="${HOST:-${KAI_EXEC_API_HOST:-127.0.0.1}}"
PORT="${PORT:-8010}"

is_loopback_host() {
  case "${1:-}" in
    127.*|localhost|::1|'[::1]'|0:0:0:0:0:0:0:1|'[0:0:0:0:0:0:0:1]') return 0 ;;
    *) return 1 ;;
  esac
}

if ! is_loopback_host "$HOST" && [ -z "${KAI_EXEC_API_BEARER_TOKEN//[[:space:]]/}" ]; then
  echo "KAI_EXEC_API_BEARER_TOKEN is required when KAI_EXEC_API_HOST is not loopback: $HOST" >&2
  exit 1
fi

export PATH="/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin:${PATH:-}"
export PYTHONPATH="apps/api${PYTHONPATH:+:$PYTHONPATH}"
export KAI_EXEC_API_HOST="$HOST"
export KAI_EXEC_API_PORT="$PORT"

if ! command -v "$PYTHON_BIN" >/dev/null 2>&1; then
  echo "Python not found: $PYTHON_BIN" >&2
  exit 1
fi

exec "$PYTHON_BIN" -m uvicorn app.main:app --host "$HOST" --port "$PORT"
