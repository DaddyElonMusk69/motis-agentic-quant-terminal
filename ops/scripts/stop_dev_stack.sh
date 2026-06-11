#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"

RUN_DIR="${RUN_DIR:-${ROOT_DIR}/.run}"

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

echo "Motis dev stack stopped."
