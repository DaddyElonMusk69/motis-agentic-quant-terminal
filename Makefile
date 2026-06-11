.PHONY: dev-api dev-worker dev-worker-legacy dev-web dev-stack stop-stack test compose-up compose-down

PYTHONPATH := packages/strategy_sdk/src:packages/engine_sdk/src:packages/strategy_modules/src:apps/api/src:apps/worker/src
CELERY_BROKER_URL ?= redis://127.0.0.1:6379/0
CELERY_RESULT_BACKEND ?= $(CELERY_BROKER_URL)
CELERY_CONCURRENCY ?= 4
CELERY_QUEUES ?= market_data,signal_generation,research,execution,default

dev-api:
	PYTHONPATH=$(PYTHONPATH) MOTIS_JOB_BACKEND=celery CELERY_BROKER_URL=$(CELERY_BROKER_URL) CELERY_RESULT_BACKEND=$(CELERY_RESULT_BACKEND) uvicorn quant_terminal_api.main:app --reload --host 0.0.0.0 --port 8000

dev-worker:
	PYTHONPATH=$(PYTHONPATH) CELERY_BROKER_URL=$(CELERY_BROKER_URL) CELERY_RESULT_BACKEND=$(CELERY_RESULT_BACKEND) celery -A quant_terminal_worker.celery_app:celery_app worker --loglevel=INFO --concurrency=$(CELERY_CONCURRENCY) -Q $(CELERY_QUEUES)

dev-worker-legacy:
	PYTHONPATH=$(PYTHONPATH) python3 -m quant_terminal_worker.service

dev-web:
	npm --workspace apps/web run dev -- --host 0.0.0.0

dev-stack:
	ops/scripts/start_dev_stack.sh

stop-stack:
	ops/scripts/stop_dev_stack.sh

test:
	PYTHONPATH=$(PYTHONPATH) python3 -m pytest tests -q

compose-up:
	docker compose --env-file .env -f ops/docker-compose.yml up --build

compose-down:
	docker compose --env-file .env -f ops/docker-compose.yml down
