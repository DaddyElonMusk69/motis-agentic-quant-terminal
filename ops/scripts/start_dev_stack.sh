#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"

RUN_DIR="${RUN_DIR:-${ROOT_DIR}/.run}"
LOG_DIR="${LOG_DIR:-${RUN_DIR}/logs}"
API_HOST="${API_HOST:-127.0.0.1}"
API_PORT="${API_PORT:-8000}"
WEB_HOST="${WEB_HOST:-127.0.0.1}"
WEB_PORT="${WEB_PORT:-5174}"
CELERY_BROKER_URL="${CELERY_BROKER_URL:-redis://127.0.0.1:6379/0}"
CELERY_RESULT_BACKEND="${CELERY_RESULT_BACKEND:-${CELERY_BROKER_URL}}"
CELERY_CONCURRENCY="${CELERY_CONCURRENCY:-4}"
CELERY_QUEUES="${CELERY_QUEUES:-market_data,signal_generation,research,execution,default}"
DATABASE_URL="${DATABASE_URL:-postgresql+psycopg://motis:motis@127.0.0.1:5432/motis}"
PYTHONPATH_VALUE="${PYTHONPATH:-packages/strategy_sdk/src:packages/engine_sdk/src:packages/strategy_modules/src:apps/api/src:apps/worker/src}"
VITE_API_BASE_URL="${VITE_API_BASE_URL:-http://${API_HOST}:${API_PORT}}"

mkdir -p "${RUN_DIR}" "${LOG_DIR}"

pid_file_for() {
  printf "%s/%s.pid" "${RUN_DIR}" "$1"
}

is_running() {
  local pid_file="$1"
  if [[ ! -f "${pid_file}" ]]; then
    return 1
  fi
  local pid
  pid="$(cat "${pid_file}")"
  [[ -n "${pid}" ]] && kill -0 "${pid}" 2>/dev/null
}

ensure_not_running() {
  local name="$1"
  local pid_file
  pid_file="$(pid_file_for "${name}")"
  if is_running "${pid_file}"; then
    echo "${name} already running with pid $(cat "${pid_file}"). Run ops/scripts/stop_dev_stack.sh first." >&2
    exit 1
  fi
  rm -f "${pid_file}"
}

ensure_port_free() {
  local name="$1"
  local port="$2"
  if lsof -nP -iTCP:"${port}" -sTCP:LISTEN >/dev/null 2>&1; then
    echo "${name} port ${port} is already in use." >&2
    lsof -nP -iTCP:"${port}" -sTCP:LISTEN >&2 || true
    echo "Stop the existing process or set a different port before starting the stack." >&2
    exit 1
  fi
}

wait_for_http() {
  local name="$1"
  local url="$2"
  local attempts="${3:-30}"
  for _ in $(seq 1 "${attempts}"); do
    if curl -fsS "${url}" >/dev/null 2>&1; then
      echo "${name} ready: ${url}"
      return 0
    fi
    sleep 1
  done
  echo "${name} did not become ready at ${url}. Check ${LOG_DIR}/${name}.log" >&2
  return 1
}

start_service() {
  local name="$1"
  shift
  local pid_file
  pid_file="$(pid_file_for "${name}")"
  echo "Starting ${name}..."
  (
    cd "${ROOT_DIR}"
    exec "$@"
  ) >"${LOG_DIR}/${name}.log" 2>&1 &
  echo "$!" >"${pid_file}"
  sleep 0.5
  if ! is_running "${pid_file}"; then
    rm -f "${pid_file}"
    echo "${name} failed to start. Check ${LOG_DIR}/${name}.log" >&2
    exit 1
  fi
}

command -v redis-cli >/dev/null 2>&1 || {
  echo "redis-cli is required to check the Celery broker. Install Redis or start the stack with Docker Compose." >&2
  exit 1
}

if ! redis-cli -u "${CELERY_BROKER_URL}" ping >/dev/null 2>&1; then
  echo "Redis broker is not reachable at ${CELERY_BROKER_URL}. Start Redis first." >&2
  exit 1
fi

ensure_not_running api
ensure_not_running worker
ensure_not_running web-v2
ensure_port_free api "${API_PORT}"
ensure_port_free web-v2 "${WEB_PORT}"

start_service api env \
  PYTHONPATH="${PYTHONPATH_VALUE}" \
  DATABASE_URL="${DATABASE_URL}" \
  MOTIS_JOB_BACKEND=celery \
  CELERY_BROKER_URL="${CELERY_BROKER_URL}" \
  CELERY_RESULT_BACKEND="${CELERY_RESULT_BACKEND}" \
  uvicorn quant_terminal_api.main:app --reload --host "${API_HOST}" --port "${API_PORT}"

start_service worker env \
  PYTHONPATH="${PYTHONPATH_VALUE}" \
  DATABASE_URL="${DATABASE_URL}" \
  CELERY_BROKER_URL="${CELERY_BROKER_URL}" \
  CELERY_RESULT_BACKEND="${CELERY_RESULT_BACKEND}" \
  celery -A quant_terminal_worker.celery_app:celery_app worker --loglevel=INFO --concurrency="${CELERY_CONCURRENCY}" -Q "${CELERY_QUEUES}"

start_service web-v2 env \
  VITE_API_BASE_URL="${VITE_API_BASE_URL}" \
  npm --workspace apps/web-v2 run dev -- --host "${WEB_HOST}" --port "${WEB_PORT}" --strictPort

wait_for_http api "http://${API_HOST}:${API_PORT}/api/v1/health" 30
wait_for_http web-v2 "http://${WEB_HOST}:${WEB_PORT}/" 30

echo
echo "Motis dev stack started."
echo "API:      http://${API_HOST}:${API_PORT}"
echo "Web v2:   http://${WEB_HOST}:${WEB_PORT}"
echo "Run dir:  ${RUN_DIR}"
echo "Logs:     ${LOG_DIR}"
echo "Stop:     ops/scripts/stop_dev_stack.sh"
