from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_dev_stack_scripts_are_present_and_executable() -> None:
    start_script = ROOT / "ops" / "scripts" / "start_dev_stack.sh"
    stop_script = ROOT / "ops" / "scripts" / "stop_dev_stack.sh"

    assert start_script.exists()
    assert stop_script.exists()
    assert start_script.stat().st_mode & 0o111
    assert stop_script.stat().st_mode & 0o111


def test_start_dev_stack_launches_all_local_services() -> None:
    script = (ROOT / "ops" / "scripts" / "start_dev_stack.sh").read_text()

    assert 'RUN_DIR="${RUN_DIR:-${ROOT_DIR}/.run}"' in script
    assert 'LOG_DIR="${LOG_DIR:-${RUN_DIR}/logs}"' in script
    assert "redis-cli" in script
    assert "ensure_port_free api" in script
    assert "ensure_port_free web-v2" in script
    assert "uvicorn quant_terminal_api.main:app" in script
    assert "celery -A quant_terminal_worker.celery_app:celery_app worker" in script
    assert "npm --workspace apps/web-v2 run dev" in script
    assert '--port "${WEB_PORT}"' in script
    assert 'VITE_API_BASE_URL="${VITE_API_BASE_URL}"' in script
    assert 'rm -f "${pid_file}"' in script


def test_stop_dev_stack_uses_pid_files_and_does_not_grep_processes() -> None:
    script = (ROOT / "ops" / "scripts" / "stop_dev_stack.sh").read_text()

    assert 'RUN_DIR="${RUN_DIR:-${ROOT_DIR}/.run}"' in script
    assert 'local pid_file="${RUN_DIR}/${name}.pid"' in script
    assert "stop_service api" in script
    assert "stop_service worker" in script
    assert "stop_service web-v2" in script
    assert "pgrep" not in script
