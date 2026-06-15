from __future__ import annotations

import os
from datetime import UTC, datetime

from celery import Celery
from celery.signals import heartbeat_sent, worker_ready, worker_shutting_down

from quant_terminal_api.repositories.runtime import RuntimeRepository

def _broker_url() -> str:
    return os.environ.get("CELERY_BROKER_URL") or os.environ.get("MOTIS_CELERY_BROKER_URL") or "redis://127.0.0.1:6379/0"


def _result_backend_url() -> str:
    return (
        os.environ.get("CELERY_RESULT_BACKEND")
        or os.environ.get("MOTIS_CELERY_RESULT_BACKEND")
        or _broker_url()
    )


celery_app = Celery(
    "quant_terminal_worker",
    broker=_broker_url(),
    backend=_result_backend_url(),
    include=["quant_terminal_worker.celery_tasks"],
)

celery_app.conf.update(
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    task_ignore_result=True,
    worker_prefetch_multiplier=1,
    broker_connection_retry_on_startup=True,
    task_default_queue="default",
)


def _record_celery_worker_heartbeat(sender: object, *, status: str) -> None:
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        return
    hostname = getattr(sender, "hostname", None)
    if not hostname:
        eventer = getattr(sender, "eventer", None)
        hostname = getattr(eventer, "hostname", None)
    if not hostname:
        return
    worker_id = f"celery-{hostname}"
    try:
        RuntimeRepository(database_url).record_worker_heartbeat(
            worker_id,
            status=status,
            started_at=datetime.now(UTC),
        )
    except Exception:
        pass


@worker_ready.connect
def _on_worker_ready(sender: object, **_: object) -> None:
    _record_celery_worker_heartbeat(sender, status="idle")


@heartbeat_sent.connect
def _on_worker_heartbeat(sender: object, **_: object) -> None:
    _record_celery_worker_heartbeat(sender, status="idle")


@worker_shutting_down.connect
def _on_worker_shutdown(sender: object, **_: object) -> None:
    _record_celery_worker_heartbeat(sender, status="offline")
