#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

if [ -f .env ]; then
  set -a
  source ./.env
  set +a
fi

# Paper-only credentials override the shared live settings without changing the
# 8010 runtime.  This file is git-ignored and should contain KAI_EXEC_* values.
if [ -f .env.paper ]; then
  set -a
  source ./.env.paper
  set +a
fi

PYTHON_BIN="${PYTHON_BIN:-/opt/homebrew/bin/python3.11}"
HOST="${HOST:-127.0.0.1}"
PORT="${PORT:-8012}"

export PATH="/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin:${PATH:-}"
export PYTHONPATH="apps/api${PYTHONPATH:+:$PYTHONPATH}"
export KAI_EXEC_API_HOST="$HOST"
export KAI_EXEC_API_PORT="$PORT"

# KIS US paper uses the KIS autonomous coordinator, whose monitoring state is
# not provider-scoped.  Keep it in a dedicated process and database.
export KAI_EXEC_RUNTIME_MODE="paper"
export KAI_EXEC_DATABASE_URL="sqlite:///$ROOT_DIR/var/kis_us_paper_exec.db"
export KAI_EXEC_SQLITE_WAL_AUTOCHECKPOINT_PAGES="1000"
export KAI_EXEC_AUTONOMOUS_RUNTIME_ENABLED="true"
export KAI_EXEC_KIS_US_TRADING_ENABLED="true"
export KAI_EXEC_KIS_US_LIVE_TRADING_ENABLED="false"
export KAI_EXEC_ALPACA_CRYPTO_MONITORING_ENABLED="false"
export KAI_EXEC_ALPACA_LIVE_TRADING_ENABLED="false"
export KAI_EXEC_ALPACA_CRYPTO_STRATEGY_ENABLED="false"
export KAI_EXEC_ALPACA_CRYPTO_STRATEGY_LIVE_ENABLED="false"
export KAI_EXEC_ALPACA_US_EQUITY_MONITORING_ENABLED="false"
export KAI_EXEC_ALPACA_US_EQUITY_STRATEGY_ENABLED="false"
export KAI_EXEC_ALPACA_US_EQUITY_STRATEGY_LIVE_ENABLED="false"

mkdir -p "$ROOT_DIR/var" "$ROOT_DIR/logs"

if ! command -v "$PYTHON_BIN" >/dev/null 2>&1; then
  echo "Python not found: $PYTHON_BIN" >&2
  exit 1
fi

"$PYTHON_BIN" -m alembic upgrade head
exec "$PYTHON_BIN" -m uvicorn app.main:app --host "$HOST" --port "$PORT"
