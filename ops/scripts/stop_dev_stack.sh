#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"

RUN_DIR="${RUN_DIR:-${ROOT_DIR}/.run}"

stop_matching_processes() {
  local name="$1"
  local pattern="$2"
  local pids
  pids="$(pgrep -f "${pattern}" 2>/dev/null || true)"
  if [[ -z "${pids}" ]]; then
    return 0
  fi

  echo "${name}: stopping stale matching processes"
  for pid in ${pids}; do
    if [[ "${pid}" == "$$" ]]; then
      continue
    fi
    kill "${pid}" 2>/dev/null || true
  done

  sleep 1
  pids="$(pgrep -f "${pattern}" 2>/dev/null || true)"
  for pid in ${pids}; do
    if [[ "${pid}" == "$$" ]]; then
      continue
    fi
    kill -9 "${pid}" 2>/dev/null || true
  done
}

stop_service() {
  local name="$1"
  local pid_file="${RUN_DIR}/${name}.pid"
  if [[ ! -f "${pid_file}" ]]; then
    echo "${name}: not running"
    return 0
  fi

  local pid
  pid="$(cat "${pid_file}")"
  if [[ -z "${pid}" ]] || ! kill -0 "${pid}" 2>/dev/null; then
    echo "${name}: stale pid file removed"
    rm -f "${pid_file}"
    return 0
  fi

  echo "Stopping ${name} (${pid})..."
  kill "${pid}" 2>/dev/null || true
  for _ in $(seq 1 20); do
    if ! kill -0 "${pid}" 2>/dev/null; then
      rm -f "${pid_file}"
      echo "${name}: stopped"
      return 0
    fi
    sleep 0.5
  done

  echo "${name}: forcing stop"
  kill -9 "${pid}" 2>/dev/null || true
  rm -f "${pid_file}"
}

stop_service web-v2
stop_service worker
stop_service api
stop_matching_processes worker "celery -A quant_terminal_worker.celery_app:celery_app worker"
stop_matching_processes api "uvicorn quant_terminal_api.main:app"

echo "Motis dev stack stopped."
