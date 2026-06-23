from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
import hashlib
import json
from pathlib import Path
from typing import Any
from uuid import uuid4

from sqlalchemy import Engine, create_engine, func, insert, select
from sqlalchemy.dialects.postgresql import insert as postgres_insert

from quant_terminal_api.db.models import (
    backtest_runs,
    decisions,
    deployment_routes,
    execution_bundles,
    jobs,
    live_signal_observations,
    market_data_refs,
    owner_states,
    score_summaries,
    signal_engine_versions,
    signal_engines,
    signal_sets,
    signals,
    stage_runs,
    stage1_research_sessions,
    stage0_universe_candidates,
    stage0_universe_runs,
    strategy_development_runs,
    strategy_modules,
    strategy_versions,
    wake_runs,
    worker_heartbeats,
)


class RuntimeRepository:
    def __init__(self, engine_or_database_url: Engine | str) -> None:
        self.engine = (
            create_engine(engine_or_database_url)
            if isinstance(engine_or_database_url, str)
            else engine_or_database_url
        )

    def enqueue_job(
        self,
        *,
        job_type: str,
        scope_key: str,
        payload: dict[str, Any],
        current_step: str | None = None,
        priority: int = 0,
    ) -> dict[str, Any]:
        now = datetime.now(UTC)
        with self.engine.begin() as connection:
            self._requeue_expired_running_jobs(connection, now=now)
            active = connection.execute(
                select(jobs)
                .where(jobs.c.scope_key == scope_key)
                .where(jobs.c.status.in_(("queued", "running")))
                .order_by(jobs.c.created_at)
                .limit(1)
            ).mappings().first()
            if active:
                return _normalize_job_row(dict(active))
            job_id = f"job-{uuid4().hex}"
            values = {
                "job_id": job_id,
                "job_type": job_type,
                "scope_key": scope_key,
                "status": "queued",
                "payload": _json_safe(payload),
                "result": {},
                "error": {},
                "current_step": current_step,
                "priority": priority,
                "created_at": now,
            }
            connection.execute(insert(jobs).values(values))
            row = connection.execute(select(jobs).where(jobs.c.job_id == job_id)).mappings().one()
            return _normalize_job_row(dict(row))

    def get_job(self, job_id: str) -> dict[str, Any] | None:
        with self.engine.connect() as connection:
            row = connection.execute(select(jobs).where(jobs.c.job_id == job_id)).mappings().first()
            return _normalize_job_row(dict(row)) if row else None

    def list_jobs(self, *, scope_key: str | None = None, limit: int = 50) -> list[dict[str, Any]]:
        statement = select(jobs).order_by(jobs.c.created_at.desc()).limit(limit)
        if scope_key:
            statement = statement.where(jobs.c.scope_key == scope_key)
        with self.engine.connect() as connection:
            return [_normalize_job_row(dict(row)) for row in connection.execute(statement).mappings()]

    def cancel_job(self, job_id: str) -> dict[str, Any] | None:
        now = datetime.now(UTC)
        with self.engine.begin() as connection:
            row = connection.execute(select(jobs).where(jobs.c.job_id == job_id)).mappings().first()
            if not row or row["status"] != "queued":
                return None
            connection.execute(
                jobs.update()
                .where(jobs.c.job_id == job_id)
                .values(status="cancelled", finished_at=now, error={"reason": "cancelled"})
            )
            updated = connection.execute(select(jobs).where(jobs.c.job_id == job_id)).mappings().one()
            return _normalize_job_row(dict(updated))

    def record_worker_heartbeat(
        self,
        worker_id: str,
        *,
        status: str = "idle",
        current_job_id: str | None = None,
        current_step: str | None = None,
        started_at: datetime | None = None,
    ) -> dict[str, Any]:
        now = datetime.now(UTC)
        values = {
            "worker_id": worker_id,
            "status": status,
            "current_job_id": current_job_id,
            "current_step": current_step,
            "started_at": started_at or now,
            "last_seen_at": now,
        }
        with self.engine.begin() as connection:
            connection.execute(self._upsert_worker_heartbeat(values))
            row = connection.execute(
                select(worker_heartbeats).where(worker_heartbeats.c.worker_id == worker_id)
            ).mappings().one()
            return _normalize_worker_heartbeat_row(dict(row))

    def get_worker_runtime_status(self, *, stale_after_seconds: int = 15) -> dict[str, Any]:
        now = datetime.now(UTC)
        stale_cutoff = now - timedelta(seconds=stale_after_seconds)
        with self.engine.connect() as connection:
            worker_rows = [
                _normalize_worker_heartbeat_row(dict(row))
                for row in connection.execute(
                    select(worker_heartbeats).order_by(worker_heartbeats.c.last_seen_at.desc())
                ).mappings()
                if not _is_ignored_worker_heartbeat(dict(row))
            ]
            queued_count = int(
                connection.execute(
                    select(func.count()).select_from(jobs).where(jobs.c.status == "queued")
                ).scalar_one()
            )
            running_count = int(
                connection.execute(
                    select(func.count()).select_from(jobs).where(jobs.c.status == "running")
                ).scalar_one()
            )
        active_workers = [
            row for row in worker_rows if _coerce_datetime(row["last_seen_at"]) >= stale_cutoff
        ]
        stale_workers = [
            row for row in worker_rows if _coerce_datetime(row["last_seen_at"]) < stale_cutoff
        ]
        if active_workers:
            status = "online"
        elif worker_rows:
            status = "stale"
        else:
            status = "offline"
        return {
            "status": status,
            "online": bool(active_workers),
            "active_worker_count": len(active_workers),
            "stale_worker_count": len(stale_workers),
            "queued_job_count": queued_count,
            "running_job_count": running_count,
            "stale_after_seconds": stale_after_seconds,
            "checked_at": now,
            "workers": worker_rows[:10],
        }

    def claim_next_job(self, *, worker_id: str, lock_seconds: int = 900) -> dict[str, Any] | None:
        now = datetime.now(UTC)
        lock_expires_at = now + timedelta(seconds=lock_seconds)
        with self.engine.begin() as connection:
            self._requeue_expired_running_jobs(connection, now=now)
            statement = (
                select(jobs)
                .where(jobs.c.status == "queued")
                .order_by(jobs.c.priority.desc(), jobs.c.created_at)
                .limit(1)
            )
            if self.engine.dialect.name == "postgresql":
                statement = statement.with_for_update(skip_locked=True)
            row = connection.execute(statement).mappings().first()
            if not row:
                return None
            connection.execute(
                jobs.update()
                .where(jobs.c.job_id == row["job_id"])
                .where(jobs.c.status == "queued")
                .values(
                    status="running",
                    started_at=now,
                    heartbeat_at=now,
                    locked_by=worker_id,
                    lock_expires_at=lock_expires_at,
                )
            )
            updated = connection.execute(select(jobs).where(jobs.c.job_id == row["job_id"])).mappings().one()
            return _normalize_job_row(dict(updated))

    def claim_job(self, *, job_id: str, worker_id: str, lock_seconds: int = 900) -> dict[str, Any] | None:
        now = datetime.now(UTC)
        lock_expires_at = now + timedelta(seconds=lock_seconds)
        with self.engine.begin() as connection:
            statement = select(jobs).where(jobs.c.job_id == job_id).limit(1)
            if self.engine.dialect.name == "postgresql":
                statement = statement.with_for_update(skip_locked=True)
            row = connection.execute(statement).mappings().first()
            if not row:
                return None
            if row["status"] == "running" and row["lock_expires_at"] and _coerce_datetime(row["lock_expires_at"]) < now:
                connection.execute(
                    jobs.update()
                    .where(jobs.c.job_id == job_id)
                    .where(jobs.c.status == "running")
                    .values(status="queued", locked_by=None, lock_expires_at=None)
                )
                row = connection.execute(statement).mappings().first()
            if not row or row["status"] != "queued":
                return None
            connection.execute(
                jobs.update()
                .where(jobs.c.job_id == job_id)
                .where(jobs.c.status == "queued")
                .values(
                    status="running",
                    started_at=now,
                    heartbeat_at=now,
                    locked_by=worker_id,
                    lock_expires_at=lock_expires_at,
                )
            )
            updated = connection.execute(select(jobs).where(jobs.c.job_id == job_id)).mappings().one()
            return _normalize_job_row(dict(updated))

    def heartbeat_job(self, job_id: str, *, current_step: str | None = None) -> dict[str, Any] | None:
        now = datetime.now(UTC)
        values: dict[str, Any] = {"heartbeat_at": now, "lock_expires_at": now + timedelta(seconds=900)}
        if current_step is not None:
            values["current_step"] = current_step
        with self.engine.begin() as connection:
            row = connection.execute(select(jobs).where(jobs.c.job_id == job_id)).mappings().first()
            if not row or row["status"] != "running":
                return None
            connection.execute(jobs.update().where(jobs.c.job_id == job_id).values(**values))
            if row["locked_by"]:
                connection.execute(
                    self._upsert_worker_heartbeat(
                        {
                            "worker_id": row["locked_by"],
                            "status": "running",
                            "current_job_id": job_id,
                            "current_step": current_step or row["current_step"],
                            "started_at": row["started_at"] or now,
                            "last_seen_at": now,
                        }
                    )
                )
            updated = connection.execute(select(jobs).where(jobs.c.job_id == job_id)).mappings().one()
            return _normalize_job_row(dict(updated))

    def _requeue_expired_running_jobs(self, connection: Any, *, now: datetime) -> None:
        connection.execute(
            jobs.update()
            .where(jobs.c.status == "running")
            .where(jobs.c.lock_expires_at.is_not(None))
            .where(jobs.c.lock_expires_at < now)
            .values(
                status="queued",
                locked_by=None,
                lock_expires_at=None,
                error={"reason": "expired_worker_lock_requeued"},
            )
        )

    def complete_job(self, job_id: str, *, result: dict[str, Any]) -> dict[str, Any] | None:
        now = datetime.now(UTC)
        with self.engine.begin() as connection:
            row = connection.execute(select(jobs).where(jobs.c.job_id == job_id)).mappings().first()
            if not row or row["status"] != "running":
                return None
            connection.execute(
                jobs.update()
                .where(jobs.c.job_id == job_id)
                .values(
                    status="completed",
                    result=_json_safe(result),
                    error={},
                    finished_at=now,
                    heartbeat_at=now,
                    lock_expires_at=None,
                )
            )
            updated = connection.execute(select(jobs).where(jobs.c.job_id == job_id)).mappings().one()
            return _normalize_job_row(dict(updated))

    def fail_job(self, job_id: str, *, error: dict[str, Any]) -> dict[str, Any] | None:
        now = datetime.now(UTC)
        with self.engine.begin() as connection:
            row = connection.execute(select(jobs).where(jobs.c.job_id == job_id)).mappings().first()
            if not row or row["status"] != "running":
                return None
            connection.execute(
                jobs.update()
                .where(jobs.c.job_id == job_id)
                .values(
                    status="failed",
                    error=_json_safe(error),
                    finished_at=now,
                    heartbeat_at=now,
                    lock_expires_at=None,
                )
            )
            updated = connection.execute(select(jobs).where(jobs.c.job_id == job_id)).mappings().one()
            return _normalize_job_row(dict(updated))

    def register_signal_engine(self, registration: dict[str, Any]) -> None:
        with self.engine.begin() as connection:
            connection.execute(
                self._insert_signal_engine_ignore_conflict(
                    {
                        "signal_engine_id": registration["signal_engine_id"],
                        "name": registration["name"],
                        "description": registration.get("description", ""),
                    }
                )
            )
            connection.execute(
                self._insert_signal_engine_version_ignore_conflict(
                    {
                        "signal_engine_id": registration["signal_engine_id"],
                        "version": registration["version"],
                        "code_ref": registration["code_ref"],
                        "supported_input_data_types": registration["supported_input_data_types"],
                        "required_data": registration.get("required_data", []),
                        "output_envelope_version": registration["output_envelope_version"],
                        "runtime_entrypoint": registration["runtime_entrypoint"],
                        "live_scanner_entrypoint": registration.get("live_scanner_entrypoint"),
                        "configuration_schema": registration.get("configuration_schema", {}),
                    }
                )
            )
            connection.execute(
                signal_engine_versions.update()
                .where(signal_engine_versions.c.signal_engine_id == registration["signal_engine_id"])
                .where(signal_engine_versions.c.version == registration["version"])
                .values(
                    code_ref=registration["code_ref"],
                    supported_input_data_types=registration["supported_input_data_types"],
                    required_data=registration.get("required_data", []),
                    output_envelope_version=registration["output_envelope_version"],
                    runtime_entrypoint=registration["runtime_entrypoint"],
                    live_scanner_entrypoint=registration.get("live_scanner_entrypoint"),
                    configuration_schema=registration.get("configuration_schema", {}),
                )
            )

    def update_signal_engine(self, signal_engine_id: str, **values: Any) -> dict[str, Any] | None:
        allowed = {key: value for key, value in values.items() if key in {"name", "description"}}
        if not allowed:
            return next((engine for engine in self.list_signal_engines() if engine["signal_engine_id"] == signal_engine_id), None)
        with self.engine.begin() as connection:
            connection.execute(
                signal_engines.update()
                .where(signal_engines.c.signal_engine_id == signal_engine_id)
                .values(**allowed)
            )
        return next((engine for engine in self.list_signal_engines() if engine["signal_engine_id"] == signal_engine_id), None)

    def register_strategy(self, registration: dict[str, Any]) -> None:
        with self.engine.begin() as connection:
            connection.execute(
                insert(strategy_modules)
                .values(
                    strategy_id=registration["strategy_id"],
                    name=registration["name"],
                    description=registration.get("description", ""),
                )
                .prefix_with("OR IGNORE")
            )
            connection.execute(
                insert(strategy_versions).values(
                    strategy_id=registration["strategy_id"],
                    version=registration["version"],
                    code_ref=registration["code_ref"],
                    supported_signal_engine_ids=registration["supported_signal_engine_ids"],
                    parameter_schema=registration.get("parameter_schema", {}),
                    decision_schema=registration.get("decision_schema", {}),
                    execution_profile_schema=registration.get("execution_profile_schema", {}),
                    test_suite_status=registration.get("test_suite_status", "unknown"),
                )
            )

    def persist_stage1_backtest(self, result: dict[str, Any]) -> None:
        stage_run_id = f"{result['run_id']}-stage1a"
        score_id = f"{result['run_id']}-stage1a-score"
        with self.engine.begin() as connection:
            connection.execute(
                insert(backtest_runs).values(
                    run_id=result["run_id"],
                    template_id=result.get("template_id", "ad_hoc"),
                    stage="stage1a",
                    strategy_id=result["strategy_id"],
                    strategy_version=result["strategy_version"],
                    signal_engine_id=result["signal_engine_id"],
                    signal_engine_version=result["signal_engine_version"],
                    asset=result["asset"],
                    instrument=result["instrument"],
                    dataset_refs=result.get("dataset_refs", []),
                    parameters_hash=result.get("parameters_hash", "unhashed"),
                    status="completed",
                    metrics=result["score_summary"]["metrics"],
                )
            )
            connection.execute(
                insert(stage_runs).values(
                    stage_run_id=stage_run_id,
                    walk_forward_run_id=result.get("walk_forward_run_id"),
                    stage="stage1a",
                    strategy_id=result["strategy_id"],
                    strategy_version=result["strategy_version"],
                    signal_engine_id=result["signal_engine_id"],
                    signal_engine_version=result["signal_engine_version"],
                    dataset_refs=result.get("dataset_refs", []),
                    status="completed",
                    metrics=result["score_summary"]["metrics"],
                )
            )
            for signal in result["signals"]:
                values = {
                    "signal_id": signal["signal_id"],
                    "signal_set_key": signal.get("signal_set_key"),
                    "signal_engine_id": signal["signal_engine_id"],
                    "signal_engine_version": signal["signal_engine_version"],
                    "asset": signal["asset"],
                    "instrument": signal["instrument"],
                    "timestamp": _coerce_datetime(signal["timestamp"]),
                    "data_refs": signal["data_refs"],
                    "payload_schema": signal["payload_schema"],
                    "payload": signal["payload"],
                }
                connection.execute(self._insert_signal_ignore_conflict(values))
            for decision in result["decisions"]:
                stored_decision_id = f"{result['run_id']}:{decision['decision_id']}"
                diagnostics = {
                    **decision.get("diagnostics", {}),
                    "strategy_decision_id": decision["decision_id"],
                }
                connection.execute(
                    insert(decisions).values(
                        decision_id=stored_decision_id,
                        stage_run_id=stage_run_id,
                        signal_id=decision["signal_id"],
                        strategy_id=decision["strategy_id"],
                        strategy_version=decision["strategy_version"],
                        action=decision["action"],
                        direction=decision["direction"],
                        confidence=decision["confidence"],
                        reason_code=decision["reason_code"],
                        execution_profile=decision.get("execution_profile", {}),
                        diagnostics=diagnostics,
                    )
                )
            connection.execute(
                insert(score_summaries).values(
                    score_id=score_id,
                    stage_run_id=stage_run_id,
                    scoring_method=result["score_summary"]["scoring_method"],
                    metrics=result["score_summary"]["metrics"],
                    records_uri=None,
                    summary="Stage 1A deterministic directional agreement complete.",
                )
            )

    def get_backtest_run(self, run_id: str) -> dict[str, Any] | None:
        statement = (
            select(
                backtest_runs,
                func.count(decisions.c.decision_id).label("decision_count"),
                func.count(signals.c.signal_id).label("signal_count"),
            )
            .select_from(backtest_runs)
            .join(stage_runs, stage_runs.c.stage_run_id == (backtest_runs.c.run_id + "-stage1a"))
            .outerjoin(decisions, decisions.c.stage_run_id == stage_runs.c.stage_run_id)
            .outerjoin(signals, signals.c.signal_id == decisions.c.signal_id)
            .where(backtest_runs.c.run_id == run_id)
            .group_by(backtest_runs.c.run_id)
        )
        with self.engine.connect() as connection:
            row = connection.execute(statement).mappings().first()
            return dict(row) if row else None

    def upsert_signal_set(self, registration: dict[str, Any]) -> None:
        values = {
            **registration,
            "start_ts": _coerce_optional_datetime(registration.get("start_ts")),
            "end_ts": _coerce_optional_datetime(registration.get("end_ts")),
        }
        with self.engine.begin() as connection:
            connection.execute(self._upsert_signal_set(values))

    def upsert_signal(self, signal: dict[str, Any]) -> None:
        values = {
            **signal,
            "timestamp": _coerce_datetime(signal["timestamp"]),
        }
        with self.engine.begin() as connection:
            connection.execute(self._insert_signal_ignore_conflict(values))

    def upsert_signals(self, signal_rows: list[dict[str, Any]]) -> None:
        values = [
            {
                **signal,
                "timestamp": _coerce_datetime(signal["timestamp"]),
            }
            for signal in signal_rows
        ]
        if not values:
            return
        with self.engine.begin() as connection:
            connection.execute(self._insert_signals_ignore_conflict(values))

    def replace_signals_for_set(self, signal_set_key: str, signal_rows: list[dict[str, Any]]) -> None:
        values = [
            {
                **signal,
                "timestamp": _coerce_datetime(signal["timestamp"]),
            }
            for signal in signal_rows
        ]
        with self.engine.begin() as connection:
            connection.execute(signals.delete().where(signals.c.signal_set_key == signal_set_key))
            if values:
                connection.execute(insert(signals), values)

    def refresh_signal_set_coverage(self, signal_set_key: str) -> None:
        coverage = (
            select(
                func.count(func.distinct(signals.c.timestamp)).label("packet_count"),
                func.min(signals.c.timestamp).label("start_ts"),
                func.max(signals.c.timestamp).label("end_ts"),
            )
            .where(signals.c.signal_set_key == signal_set_key)
        )
        with self.engine.begin() as connection:
            row = connection.execute(coverage).mappings().one()
            connection.execute(
                signal_sets.update()
                .where(signal_sets.c.signal_set_key == signal_set_key)
                .values(
                    packet_count=int(row["packet_count"] or 0),
                    start_ts=row["start_ts"],
                    end_ts=row["end_ts"],
                )
            )

    def canonicalize_signal_pools(self, *, dry_run: bool = False) -> dict[str, Any]:
        with self.engine.begin() as connection:
            pool_rows = [dict(row) for row in connection.execute(select(signal_sets)).mappings()]
            signal_rows = [
                dict(row)
                for row in connection.execute(
                    select(
                        signals.c.signal_set_key,
                        signals.c.signal_engine_id,
                        signals.c.signal_engine_version,
                        signals.c.asset,
                        signals.c.instrument,
                        signals.c.payload_schema,
                        func.count(signals.c.signal_id).label("packet_count"),
                        func.min(signals.c.timestamp).label("start_ts"),
                        func.max(signals.c.timestamp).label("end_ts"),
                    )
                    .where(signals.c.signal_set_key.is_not(None))
                    .group_by(
                        signals.c.signal_set_key,
                        signals.c.signal_engine_id,
                        signals.c.signal_engine_version,
                        signals.c.asset,
                        signals.c.instrument,
                        signals.c.payload_schema,
                    )
                ).mappings()
            ]
            groups: dict[tuple[str, str], list[dict[str, Any]]] = {}
            for row in signal_rows:
                groups.setdefault((row["signal_engine_id"], row["asset"]), []).append(row)

            mapping: dict[str, str] = {}
            canonical_rows: list[dict[str, Any]] = []
            for (signal_engine_id, asset), rows in sorted(groups.items()):
                canonical_id = f"{asset}-{signal_engine_id}-canonical"
                canonical_key = f"{signal_engine_id}:{asset}:{canonical_id}"
                for row in rows:
                    mapping[row["signal_set_key"]] = canonical_key
                source_keys = sorted(row["signal_set_key"] for row in rows)
                first_row = sorted(rows, key=lambda row: str(row["end_ts"] or ""))[-1]
                canonical_rows.append(
                    {
                        "signal_set_key": canonical_key,
                        "signal_set_id": canonical_id,
                        "signal_engine_id": signal_engine_id,
                        "signal_engine_version": first_row["signal_engine_version"],
                        "asset": asset,
                        "instrument": first_row["instrument"],
                        "start_ts": min(row["start_ts"] for row in rows if row["start_ts"] is not None),
                        "end_ts": max(row["end_ts"] for row in rows if row["end_ts"] is not None),
                        "packet_count": sum(int(row["packet_count"] or 0) for row in rows),
                        "payload_schema": first_row["payload_schema"],
                        "source_path": "canonicalized:signals",
                        "manifest": {
                            "schema_version": "0.1",
                            "canonical_signal_set_id": canonical_id,
                            "canonical_signal_set_key": canonical_key,
                            "source_signal_set_keys": source_keys,
                            "canonicalized_from_existing_db": True,
                        },
                    }
                )

            duplicate_groups_before = sum(
                1
                for row in connection.execute(
                    select(signal_sets.c.signal_engine_id, signal_sets.c.asset, func.count().label("pool_count"))
                    .group_by(signal_sets.c.signal_engine_id, signal_sets.c.asset)
                    .having(func.count() > 1)
                )
            )
            report = {
                "dry_run": dry_run,
                "canonical_pool_count": len(canonical_rows),
                "source_pool_count": len(pool_rows),
                "mapped_source_pool_count": len(mapping),
                "duplicate_engine_asset_groups_before": duplicate_groups_before,
            }
            if dry_run:
                return report

            for row in canonical_rows:
                connection.execute(self._upsert_signal_set(row))

            candidate_rows = [
                dict(row)
                for row in connection.execute(select(stage0_universe_candidates)).mappings()
            ]
            stage1_candidate_ids = {
                row["source_candidate_id"]
                for row in connection.execute(select(stage1_research_sessions.c.source_candidate_id)).mappings()
            }
            candidates_by_target: dict[tuple[str, str], list[dict[str, Any]]] = {}
            for candidate in candidate_rows:
                canonical_key = mapping.get(candidate["signal_set_key"])
                if canonical_key:
                    candidates_by_target.setdefault((candidate["universe_run_id"], canonical_key), []).append(candidate)

            deleted_candidate_ids: set[str] = set()
            source_candidate_rewrites: dict[str, str] = {}
            for (universe_run_id, canonical_key), candidates in candidates_by_target.items():
                if len(candidates) <= 1:
                    continue
                keep = sorted(
                    candidates,
                    key=lambda candidate: (
                        candidate["candidate_id"] in stage1_candidate_ids,
                        int(candidate.get("packet_count") or 0),
                        str(candidate.get("created_at") or ""),
                    ),
                    reverse=True,
                )[0]
                for candidate in candidates:
                    if candidate["candidate_id"] == keep["candidate_id"]:
                        continue
                    if candidate["candidate_id"] in stage1_candidate_ids:
                        connection.execute(
                            stage1_research_sessions.update()
                            .where(stage1_research_sessions.c.source_candidate_id == candidate["candidate_id"])
                            .values(source_candidate_id=keep["candidate_id"])
                        )
                        source_candidate_rewrites[candidate["candidate_id"]] = keep["candidate_id"]
                    connection.execute(
                        stage0_universe_candidates.delete().where(
                            stage0_universe_candidates.c.candidate_id == candidate["candidate_id"]
                        )
                    )
                    deleted_candidate_ids.add(candidate["candidate_id"])

            for old_key, canonical_key in mapping.items():
                if old_key == canonical_key:
                    continue
                canonical_id = canonical_key.split(":", 2)[2]
                connection.execute(
                    signals.update()
                    .where(signals.c.signal_set_key == old_key)
                    .values(signal_set_key=canonical_key)
                )
                connection.execute(
                    strategy_development_runs.update()
                    .where(strategy_development_runs.c.signal_set_key == old_key)
                    .values(signal_set_key=canonical_key)
                )
                connection.execute(
                    stage1_research_sessions.update()
                    .where(stage1_research_sessions.c.signal_set_key == old_key)
                    .values(signal_set_key=canonical_key, signal_set_id=canonical_id)
                )
                connection.execute(
                    stage0_universe_candidates.update()
                    .where(stage0_universe_candidates.c.signal_set_key == old_key)
                    .values(signal_set_key=canonical_key, signal_set_id=canonical_id)
                )

            self._repair_stage1_signal_pool_references(
                connection=connection,
                signal_set_rewrites=mapping,
                source_candidate_rewrites=source_candidate_rewrites,
            )

            canonical_key_set = {row["signal_set_key"] for row in canonical_rows}
            deduped_signal_row_count = 0
            updated_decision_reference_count = 0
            for canonical_key in canonical_key_set:
                signal_rows = [
                    dict(row)
                    for row in connection.execute(
                        select(signals)
                        .where(signals.c.signal_set_key == canonical_key)
                        .order_by(signals.c.timestamp, signals.c.signal_id)
                    ).mappings()
                ]
                deduped_rows, signal_rewrites = _dedupe_signal_rows(signal_rows)
                if signal_rewrites:
                    for old_signal_id, new_signal_id in signal_rewrites.items():
                        result = connection.execute(
                            decisions.update()
                            .where(decisions.c.signal_id == old_signal_id)
                            .values(signal_id=new_signal_id)
                        )
                        updated_decision_reference_count += int(result.rowcount or 0)
                    connection.execute(signals.delete().where(signals.c.signal_id.in_(tuple(signal_rewrites.keys()))))
                    deduped_signal_row_count += len(signal_rows) - len(deduped_rows)
            for old_key in sorted(mapping):
                if old_key in canonical_key_set:
                    continue
                connection.execute(signal_sets.delete().where(signal_sets.c.signal_set_key == old_key))

            for canonical_key in canonical_key_set:
                coverage = connection.execute(
                    select(
                        func.count(func.distinct(signals.c.timestamp)).label("packet_count"),
                        func.min(signals.c.timestamp).label("start_ts"),
                        func.max(signals.c.timestamp).label("end_ts"),
                    ).where(signals.c.signal_set_key == canonical_key)
                ).mappings().one()
                connection.execute(
                    signal_sets.update()
                    .where(signal_sets.c.signal_set_key == canonical_key)
                    .values(
                        packet_count=int(coverage["packet_count"] or 0),
                        start_ts=coverage["start_ts"],
                        end_ts=coverage["end_ts"],
                    )
                )

            duplicate_groups_after = sum(
                1
                for row in connection.execute(
                    select(signal_sets.c.signal_engine_id, signal_sets.c.asset, func.count().label("pool_count"))
                    .group_by(signal_sets.c.signal_engine_id, signal_sets.c.asset)
                    .having(func.count() > 1)
                )
            )
            remaining_metadata_mismatches = sum(
                1
                for row in connection.execute(
                    select(signal_sets.c.signal_set_key)
                    .outerjoin(signals, signals.c.signal_set_key == signal_sets.c.signal_set_key)
                    .group_by(
                        signal_sets.c.signal_set_key,
                        signal_sets.c.packet_count,
                        signal_sets.c.start_ts,
                        signal_sets.c.end_ts,
                    )
                    .having(
                        (signal_sets.c.packet_count != func.count(signals.c.signal_id))
                        | (signal_sets.c.start_ts.is_distinct_from(func.min(signals.c.timestamp)))
                        | (signal_sets.c.end_ts.is_distinct_from(func.max(signals.c.timestamp)))
                    )
                )
            )
            return {
                **report,
                "deleted_duplicate_stage0_candidate_count": len(deleted_candidate_ids),
                "deduped_signal_row_count": deduped_signal_row_count,
                "updated_decision_reference_count": updated_decision_reference_count,
                "duplicate_engine_asset_groups_after": duplicate_groups_after,
                "remaining_metadata_mismatch_count": remaining_metadata_mismatches,
            }

    def list_signal_engines(self) -> list[dict[str, Any]]:
        latest_version = (
            select(
                signal_engine_versions.c.signal_engine_id,
                func.max(signal_engine_versions.c.created_at).label("latest_created_at"),
            )
            .group_by(signal_engine_versions.c.signal_engine_id)
            .subquery()
        )
        signal_set_counts = (
            select(
                signal_sets.c.signal_engine_id,
                func.count(signal_sets.c.signal_set_key).label("signal_set_count"),
            )
            .group_by(signal_sets.c.signal_engine_id)
            .subquery()
        )
        signal_counts = (
            select(
                signals.c.signal_engine_id,
                func.count(signals.c.signal_id).label("packet_count"),
            )
            .group_by(signals.c.signal_engine_id)
            .subquery()
        )
        statement = (
            select(
                signal_engines.c.signal_engine_id,
                signal_engines.c.name,
                signal_engines.c.description,
                signal_engine_versions.c.version,
                signal_engine_versions.c.created_at,
                signal_engine_versions.c.code_ref,
                signal_engine_versions.c.required_data,
                signal_engine_versions.c.output_envelope_version,
                signal_engine_versions.c.runtime_entrypoint,
                signal_engine_versions.c.live_scanner_entrypoint,
                signal_engine_versions.c.configuration_schema,
                func.coalesce(signal_set_counts.c.signal_set_count, 0).label("signal_set_count"),
                func.coalesce(signal_counts.c.packet_count, 0).label("packet_count"),
            )
            .select_from(signal_engines)
            .outerjoin(latest_version, latest_version.c.signal_engine_id == signal_engines.c.signal_engine_id)
            .outerjoin(
                signal_engine_versions,
                (signal_engine_versions.c.signal_engine_id == signal_engines.c.signal_engine_id)
                & (signal_engine_versions.c.created_at == latest_version.c.latest_created_at),
            )
            .outerjoin(
                signal_set_counts,
                signal_set_counts.c.signal_engine_id == signal_engines.c.signal_engine_id,
            )
            .outerjoin(signal_counts, signal_counts.c.signal_engine_id == signal_engines.c.signal_engine_id)
            .group_by(
                signal_engines.c.signal_engine_id,
                signal_engines.c.name,
                signal_engines.c.description,
                signal_engine_versions.c.version,
                signal_engine_versions.c.created_at,
                signal_engine_versions.c.code_ref,
                signal_engine_versions.c.required_data,
                signal_engine_versions.c.output_envelope_version,
                signal_engine_versions.c.runtime_entrypoint,
                signal_engine_versions.c.live_scanner_entrypoint,
                signal_engine_versions.c.configuration_schema,
                signal_set_counts.c.signal_set_count,
                signal_counts.c.packet_count,
            )
            .order_by(signal_engines.c.signal_engine_id)
        )
        with self.engine.connect() as connection:
            return [dict(row._mapping) for row in connection.execute(statement)]

    def list_signal_sets(self, signal_engine_id: str | None = None) -> list[dict[str, Any]]:
        statement = select(signal_sets).order_by(
            signal_sets.c.asset,
            signal_sets.c.start_ts,
            signal_sets.c.signal_set_id,
        )
        if signal_engine_id:
            statement = statement.where(signal_sets.c.signal_engine_id == signal_engine_id)
        with self.engine.connect() as connection:
            return [_normalize_signal_set_row(dict(row._mapping)) for row in connection.execute(statement)]

    def get_signal_set(self, signal_set_key: str) -> dict[str, Any] | None:
        statement = select(signal_sets).where(signal_sets.c.signal_set_key == signal_set_key)
        with self.engine.connect() as connection:
            row = connection.execute(statement).mappings().first()
            return _normalize_signal_set_row(dict(row)) if row else None

    def get_candle_ref(
        self,
        *,
        asset: str,
        timeframe: str,
        origin: str,
        data_type: str = "candles",
    ) -> dict[str, Any] | None:
        statement = (
            select(market_data_refs)
            .where(market_data_refs.c.asset == asset.upper())
            .where(market_data_refs.c.data_type == data_type)
            .where(market_data_refs.c.timeframe == timeframe)
            .where(market_data_refs.c.data_origin == origin)
            .order_by(market_data_refs.c.end_ts.desc())
            .limit(1)
        )
        with self.engine.connect() as connection:
            row = connection.execute(statement).mappings().first()
            return dict(row) if row else None

    def get_data_ref(
        self,
        *,
        asset: str,
        timeframe: str,
        origin: str,
        data_type: str,
    ) -> dict[str, Any] | None:
        return self.get_candle_ref(asset=asset, timeframe=timeframe, origin=origin, data_type=data_type)

    def list_signals(
        self,
        *,
        signal_set_key: str | None = None,
        signal_engine_id: str | None = None,
        asset: str | None = None,
        limit: int = 25,
        descending: bool = False,
    ) -> list[dict[str, Any]]:
        order_by = signals.c.timestamp.desc() if descending else signals.c.timestamp
        statement = select(signals).order_by(order_by).limit(limit)
        if signal_set_key:
            statement = statement.where(signals.c.signal_set_key == signal_set_key)
        if signal_engine_id:
            statement = statement.where(signals.c.signal_engine_id == signal_engine_id)
        if asset:
            statement = statement.where(signals.c.asset == asset)
        with self.engine.connect() as connection:
            return [dict(row._mapping) for row in connection.execute(statement)]

    def record_live_signal_observation(self, observation: dict[str, Any]) -> dict[str, Any]:
        payload = _json_safe(observation.get("payload", {}))
        values = {
            "observation_id": observation.get("observation_id") or _live_signal_observation_id(observation),
            "signal_engine_id": observation["signal_engine_id"],
            "signal_engine_version": observation.get("signal_engine_version") or "unknown",
            "asset": str(observation["asset"]).upper(),
            "instrument": observation.get("instrument") or f"{str(observation['asset']).upper()}-USDT-SWAP",
            "signal_id": observation["signal_id"],
            "signal_timestamp": _coerce_datetime(observation.get("signal_timestamp") or observation["timestamp"]),
            "route_id": observation.get("route_id"),
            "bundle_id": observation.get("bundle_id"),
            "packet_hash": observation.get("packet_hash") or _stable_json_hash(payload),
            "payload_schema": observation.get("payload_schema") or payload.get("schema_version") or "signal_packet.v2",
            "payload": payload,
            "decision": _json_safe(observation.get("decision", {})),
            "scan_metadata": _json_safe(observation.get("scan_metadata", {})),
            "observed_at": _coerce_optional_datetime(observation.get("observed_at")) or datetime.now(UTC),
        }
        with self.engine.begin() as connection:
            connection.execute(self._upsert_live_signal_observation(values))
            row = connection.execute(
                select(live_signal_observations).where(live_signal_observations.c.observation_id == values["observation_id"])
            ).mappings().one()
            return _normalize_live_signal_observation_row(dict(row))

    def list_live_signal_observations(
        self,
        *,
        signal_engine_id: str,
        asset: str,
        limit: int = 100,
        offset: int = 0,
    ) -> dict[str, Any]:
        safe_limit = max(1, min(int(limit), 500))
        safe_offset = max(0, int(offset))
        filters = [
            live_signal_observations.c.signal_engine_id == signal_engine_id,
            live_signal_observations.c.asset == asset.upper(),
        ]
        statement = (
            select(live_signal_observations)
            .where(*filters)
            .order_by(live_signal_observations.c.signal_timestamp.desc(), live_signal_observations.c.observed_at.desc())
            .limit(safe_limit)
            .offset(safe_offset)
        )
        count_statement = select(func.count()).select_from(live_signal_observations).where(*filters)
        with self.engine.connect() as connection:
            rows = [_normalize_live_signal_observation_row(dict(row)) for row in connection.execute(statement).mappings()]
            total = int(connection.execute(count_statement).scalar_one())
        return {"observations": rows, "total": total, "limit": safe_limit, "offset": safe_offset}

    def create_strategy_development_run(self, run: dict[str, Any]) -> None:
        with self.engine.begin() as connection:
            connection.execute(self._insert_strategy_development_run_ignore_conflict(run))

    def list_strategy_development_runs(self) -> list[dict[str, Any]]:
        statement = select(strategy_development_runs).order_by(strategy_development_runs.c.created_at.desc())
        with self.engine.connect() as connection:
            return [dict(row._mapping) for row in connection.execute(statement)]

    def create_stage1_research_session(self, session: dict[str, Any]) -> None:
        values = {
            **session,
            "train_start": _coerce_date(session["train_start"]),
            "train_end": _coerce_date(session["train_end"]),
            "walk_forward_start": _coerce_date(session["walk_forward_start"]),
            "walk_forward_end": _coerce_date(session["walk_forward_end"]),
        }
        with self.engine.begin() as connection:
            connection.execute(self._insert_stage1_research_session_ignore_conflict(values))

    def list_stage1_research_sessions(self) -> list[dict[str, Any]]:
        statement = select(stage1_research_sessions).order_by(stage1_research_sessions.c.created_at.desc())
        with self.engine.connect() as connection:
            return [dict(row._mapping) for row in connection.execute(statement)]

    def get_stage1_research_session(self, session_id: str) -> dict[str, Any] | None:
        statement = select(stage1_research_sessions).where(
            stage1_research_sessions.c.session_id == session_id
        )
        with self.engine.connect() as connection:
            row = connection.execute(statement).mappings().first()
            return dict(row) if row else None

    def delete_stage1_research_session(self, session_id: str) -> None:
        with self.engine.begin() as connection:
            connection.execute(
                stage1_research_sessions.delete().where(stage1_research_sessions.c.session_id == session_id)
            )

    def latest_stage1_strategy_seed(
        self,
        *,
        asset: str,
        signal_engine_id: str,
        strategy_id: str,
    ) -> dict[str, Any] | None:
        statement = (
            select(stage1_research_sessions)
            .where(stage1_research_sessions.c.asset == asset)
            .where(stage1_research_sessions.c.signal_engine_id == signal_engine_id)
            .where(stage1_research_sessions.c.strategy_id == strategy_id)
            .order_by(stage1_research_sessions.c.created_at.desc())
        )
        with self.engine.connect() as connection:
            rows = [dict(row._mapping) for row in connection.execute(statement)]
        for session in rows:
            artifact_root = Path(session["artifact_root"])
            frozen_path = artifact_root / "promotion" / "frozen_stage1a_strategy_module" / "strategy.py"
            if frozen_path.is_file():
                return {
                    "source_type": "latest_pair_frozen",
                    "source_path": str(frozen_path),
                    "source_version": session.get("strategy_version"),
                    "source_session_id": session.get("session_id"),
                }
            draft_path = artifact_root / "strategy_module" / "strategy.py"
            if draft_path.is_file():
                return {
                    "source_type": "latest_pair_draft",
                    "source_path": str(draft_path),
                    "source_version": session.get("strategy_version"),
                    "source_session_id": session.get("session_id"),
                }
        return None

    def update_stage1_research_session_state(
        self,
        *,
        session_id: str,
        status: str,
        manifest: dict[str, Any],
    ) -> None:
        statement = (
            stage1_research_sessions.update()
            .where(stage1_research_sessions.c.session_id == session_id)
            .values(status=status, manifest=manifest)
        )
        with self.engine.begin() as connection:
            connection.execute(statement)

    def create_execution_bundle(self, bundle: dict[str, Any]) -> dict[str, Any]:
        with self.engine.begin() as connection:
            connection.execute(self._insert_execution_bundle_ignore_conflict(bundle))
        stored = self.get_execution_bundle(bundle["bundle_id"])
        if stored is None:
            raise RuntimeError("execution bundle was not persisted")
        return stored

    def get_execution_bundle(self, bundle_id: str) -> dict[str, Any] | None:
        statement = select(execution_bundles).where(execution_bundles.c.bundle_id == bundle_id)
        with self.engine.connect() as connection:
            row = connection.execute(statement).mappings().first()
            return dict(row) if row else None

    def list_execution_bundles(self) -> list[dict[str, Any]]:
        statement = select(execution_bundles).order_by(execution_bundles.c.created_at.desc())
        with self.engine.connect() as connection:
            return [dict(row._mapping) for row in connection.execute(statement)]

    def list_execution_bundles_for_stage1_session(self, session_id: str) -> list[dict[str, Any]]:
        statement = (
            select(execution_bundles)
            .where(execution_bundles.c.source_stage1_session_id == session_id)
            .order_by(execution_bundles.c.created_at.desc())
        )
        with self.engine.connect() as connection:
            return [dict(row._mapping) for row in connection.execute(statement)]

    def upsert_deployment_route_for_bundle(
        self,
        *,
        bundle: dict[str, Any],
        account_mode: str,
        execution_adapter: str,
        exchange_account: str = "default",
    ) -> dict[str, Any]:
        route_id = self._deployment_route_id(
            asset=bundle["asset"],
            account_mode=account_mode,
            execution_adapter=execution_adapter,
            exchange_account=exchange_account,
        )
        values = {
            "route_id": route_id,
            "active_bundle_id": bundle["bundle_id"],
            "strategy_id": bundle["strategy_id"],
            "strategy_version": bundle["strategy_version"],
            "signal_engine_id": bundle["signal_engine_id"],
            "signal_engine_version": bundle["signal_engine_version"],
            "asset": bundle["asset"],
            "instrument": bundle["instrument"],
            "account_mode": account_mode,
            "execution_adapter": execution_adapter,
            "exchange_account": exchange_account,
            "cron_interval_minutes": _execution_setup_cron_minutes(bundle.get("execution_setup")),
            "margin_allocation_pct": _execution_setup_margin_allocation_pct(bundle.get("execution_setup")),
            "leverage": _execution_setup_leverage(bundle.get("execution_setup")),
            "manual_sizing_enabled": False,
            "scheduler_status": "stopped",
            "auto_submit_enabled": True,
            "last_wake_at": None,
            "last_wake_id": None,
            "next_wake_at": None,
            "last_lifecycle_error": {},
            "risk_limits": bundle["risk_limits"],
            "promoted": True,
            "data_warmed": False,
            "manually_armed": False,
            "enabled": False,
            "archived": False,
            "archived_at": None,
        }
        with self.engine.begin() as connection:
            connection.execute(self._upsert_deployment_route(values))
        route = self.get_deployment_route_for_asset_account(
            asset=bundle["asset"],
            account_mode=account_mode,
            execution_adapter=execution_adapter,
            exchange_account=exchange_account,
        )
        if route is None:
            raise RuntimeError("deployment route was not persisted")
        return route

    def list_deployment_routes(self, *, include_archived: bool = False) -> list[dict[str, Any]]:
        statement = select(deployment_routes)
        if not include_archived:
            statement = statement.where(deployment_routes.c.archived.is_(False))
        statement = statement.order_by(
            deployment_routes.c.asset,
            deployment_routes.c.account_mode,
        )
        with self.engine.connect() as connection:
            routes = [dict(row._mapping) for row in connection.execute(statement)]
        bundles = {bundle["bundle_id"]: bundle for bundle in self.list_execution_bundles()}
        engines = {
            (engine.get("signal_engine_id"), engine.get("version")): engine
            for engine in self.list_signal_engines()
        }
        for route in routes:
            _normalize_route_datetimes(route)
            route["active_bundle"] = bundles.get(route.get("active_bundle_id"))
            route["data_freshness"] = self._route_data_freshness(
                route,
                engine=engines.get((route.get("signal_engine_id"), route.get("signal_engine_version"))),
            )
            route["blockers"] = _route_blockers(route)
        return routes

    def get_deployment_route(self, route_id: str) -> dict[str, Any] | None:
        statement = select(deployment_routes).where(deployment_routes.c.route_id == route_id)
        with self.engine.connect() as connection:
            row = connection.execute(statement).mappings().first()
        if row is None:
            return None
        route = dict(row)
        _normalize_route_datetimes(route)
        if route.get("active_bundle_id"):
            route["active_bundle"] = self.get_execution_bundle(route["active_bundle_id"])
        else:
            route["active_bundle"] = None
        route["data_freshness"] = self._route_data_freshness(route)
        route["blockers"] = _route_blockers(route)
        return route

    def _route_data_freshness(self, route: dict[str, Any], *, engine: dict[str, Any] | None = None) -> dict[str, Any]:
        checked_at = datetime.now(UTC)
        try:
            wake_interval_seconds = max(60, int(route.get("cron_interval_minutes") or 5) * 60)
        except (TypeError, ValueError):
            wake_interval_seconds = 300
        candle_interval_seconds = _timeframe_seconds("5m")
        grace_seconds = 90
        max_age_seconds = candle_interval_seconds + wake_interval_seconds + grace_seconds
        resolved_engine = engine or self._route_signal_engine(route)
        required_data = list((resolved_engine or {}).get("required_data") or [])
        raw_required = _route_requires_candle(required_data, origin="raw", timeframe="5m")
        derived_required = _route_requires_candle(required_data, origin="derived", timeframe="5m")
        raw_ref = self.get_candle_ref(
            asset=route["asset"],
            timeframe="5m",
            origin="raw",
            data_type="candles",
        ) if raw_required else None
        derived_ref = self.get_candle_ref(
            asset=route["asset"],
            timeframe="5m",
            origin="derived",
            data_type="candles",
        ) if derived_required else None
        raw_status = _market_ref_freshness(raw_ref, checked_at=checked_at, max_age_seconds=max_age_seconds)
        derived_status = _market_ref_freshness(derived_ref, checked_at=checked_at, max_age_seconds=max_age_seconds)
        status = "fresh"
        reason = None
        if raw_required and raw_status["status"] in {"missing", "stale"}:
            status = "stale"
            reason = "raw_5m_missing" if raw_status["status"] == "missing" else "raw_5m_stale"
        elif derived_required and derived_status["status"] in {"missing", "stale"}:
            status = "stale"
            reason = "derived_5m_missing" if derived_status["status"] == "missing" else "derived_5m_stale"
        elif derived_required and raw_status.get("timestamp") and derived_status.get("timestamp"):
            raw_ts = _coerce_optional_datetime(raw_status["timestamp"])
            derived_ts = _coerce_optional_datetime(derived_status["timestamp"])
            if raw_ts is not None and derived_ts is not None and derived_ts < raw_ts:
                status = "stale"
                reason = "derived_5m_lagging_raw"
        return {
            "status": status,
            "reason": reason,
            "checked_at": checked_at.replace(microsecond=0).isoformat().replace("+00:00", "Z"),
            "candle_interval_seconds": candle_interval_seconds,
            "wake_interval_seconds": wake_interval_seconds,
            "grace_seconds": grace_seconds,
            "max_age_seconds": max_age_seconds,
            "raw_5m": raw_status if raw_required else None,
            "derived_5m": derived_status if derived_required else None,
        }

    def _route_signal_engine(self, route: dict[str, Any]) -> dict[str, Any] | None:
        for engine in self.list_signal_engines():
            if engine.get("signal_engine_id") != route.get("signal_engine_id"):
                continue
            if route.get("signal_engine_version") is not None and engine.get("version") != route.get("signal_engine_version"):
                continue
            return engine
        return None

    def get_deployment_route_for_asset_account(
        self,
        *,
        asset: str,
        account_mode: str,
        execution_adapter: str | None = None,
        exchange_account: str | None = None,
    ) -> dict[str, Any] | None:
        statement = (
            select(deployment_routes)
            .where(deployment_routes.c.asset == asset)
            .where(deployment_routes.c.account_mode == account_mode)
        )
        if execution_adapter is not None:
            statement = statement.where(deployment_routes.c.execution_adapter == execution_adapter)
        if exchange_account is not None:
            statement = statement.where(deployment_routes.c.exchange_account == exchange_account)
        statement = statement.order_by(deployment_routes.c.created_at.desc()).limit(1)
        with self.engine.connect() as connection:
            row = connection.execute(statement).mappings().first()
        if row is None:
            return None
        return self.get_deployment_route(row["route_id"])

    def _deployment_route_id(
        self,
        *,
        asset: str,
        account_mode: str,
        execution_adapter: str,
        exchange_account: str,
    ) -> str:
        existing = self.get_deployment_route_for_asset_account(
            asset=asset,
            account_mode=account_mode,
            execution_adapter=execution_adapter,
            exchange_account=exchange_account,
        )
        if existing is not None:
            return str(existing["route_id"])
        base_route_id = _route_id(asset=asset, account_mode=account_mode)
        existing_base = self.get_deployment_route(base_route_id)
        if existing_base is None:
            return base_route_id
        if (
            existing_base.get("asset") == asset
            and existing_base.get("account_mode") == account_mode
            and existing_base.get("execution_adapter") == execution_adapter
            and existing_base.get("exchange_account") == exchange_account
        ):
            return base_route_id
        return _route_id(
            asset=asset,
            account_mode=account_mode,
            execution_adapter=execution_adapter,
            exchange_account=exchange_account,
        )

    def update_deployment_route_gate(self, route_id: str, **values: Any) -> dict[str, Any] | None:
        allowed = {
            "enabled",
            "manually_armed",
            "data_warmed",
            "execution_adapter",
            "exchange_account",
            "cron_interval_minutes",
            "margin_allocation_pct",
            "leverage",
            "manual_sizing_enabled",
            "scheduler_status",
            "auto_submit_enabled",
            "last_wake_at",
            "last_wake_id",
            "next_wake_at",
            "last_lifecycle_error",
            "archived",
            "archived_at",
        }
        updates = {key: value for key, value in values.items() if key in allowed}
        for key in ("last_wake_at", "next_wake_at", "archived_at"):
            if key in updates and updates[key] is not None:
                updates[key] = _coerce_datetime(updates[key])
        if "last_lifecycle_error" in updates:
            updates["last_lifecycle_error"] = _json_safe(updates["last_lifecycle_error"] or {})
        if updates:
            statement = deployment_routes.update().where(deployment_routes.c.route_id == route_id).values(**updates)
            with self.engine.begin() as connection:
                connection.execute(statement)
        return self.get_deployment_route(route_id)

    def archive_deployment_route(self, route_id: str, *, archived_at: str | datetime | None = None) -> dict[str, Any] | None:
        return self.update_deployment_route_gate(
            route_id,
            archived=True,
            archived_at=archived_at or datetime.now(UTC),
            enabled=False,
            manually_armed=False,
            scheduler_status="stopped",
            auto_submit_enabled=False,
            next_wake_at=None,
            last_lifecycle_error={},
        )

    def delete_archived_strategy_route(self, route_id: str) -> dict[str, Any] | None:
        route = self.get_deployment_route(route_id)
        if route is None:
            return None
        if not route.get("archived"):
            raise ValueError("deployment route is not archived")
        bundle_id = route.get("active_bundle_id")
        if not bundle_id:
            raise ValueError("archived route has no active bundle")
        bundle = self.get_execution_bundle(bundle_id)
        if bundle is None:
            raise ValueError("execution bundle not found for archived route")

        wake_filter = (wake_runs.c.route_id == route_id) | (wake_runs.c.bundle_id == bundle_id)
        owner_filter = (owner_states.c.route_id == route_id) | (owner_states.c.bundle_id == bundle_id)

        with self.engine.begin() as connection:
            other_route_ref = connection.execute(
                select(func.count())
                .select_from(deployment_routes)
                .where(deployment_routes.c.active_bundle_id == bundle_id)
                .where(deployment_routes.c.route_id != route_id)
            ).scalar_one()
            if other_route_ref:
                raise ValueError("execution bundle is still referenced by another route")

            deleted_wake_count = int(
                connection.execute(select(func.count()).select_from(wake_runs).where(wake_filter)).scalar_one() or 0
            )
            deleted_owner_state_count = int(
                connection.execute(select(func.count()).select_from(owner_states).where(owner_filter)).scalar_one() or 0
            )
            connection.execute(wake_runs.delete().where(wake_filter))
            connection.execute(owner_states.delete().where(owner_filter))
            connection.execute(deployment_routes.delete().where(deployment_routes.c.route_id == route_id))
            connection.execute(execution_bundles.delete().where(execution_bundles.c.bundle_id == bundle_id))

        return {
            "route_id": route_id,
            "bundle_id": bundle_id,
            "bundle_uri": bundle.get("bundle_uri"),
            "strategy_module_ref": bundle.get("strategy_module_ref"),
            "deleted_wake_count": deleted_wake_count,
            "deleted_owner_state_count": deleted_owner_state_count,
        }

    def record_wake_run(self, wake: dict[str, Any]) -> dict[str, Any]:
        values = {
            **wake,
            "blockers": _json_safe(wake.get("blockers", [])),
            "exchange_snapshot": _json_safe(wake.get("exchange_snapshot", {})),
            "signal_scan_result": _json_safe(wake.get("signal_scan_result", {})),
            "strategy_decision": _json_safe(wake.get("strategy_decision", {})),
            "order_intents": _json_safe(wake.get("order_intents", [])),
            "adapter_results": _json_safe(wake.get("adapter_results", [])),
            "error": _json_safe(wake.get("error", {})),
            "completed_at": _coerce_optional_datetime(wake.get("completed_at")),
        }
        with self.engine.begin() as connection:
            connection.execute(insert(wake_runs).values(**values))
        stored = self.get_wake_run(wake["wake_id"])
        if stored is None:
            raise RuntimeError("wake run was not persisted")
        return stored

    def get_wake_run(self, wake_id: str) -> dict[str, Any] | None:
        statement = select(wake_runs).where(wake_runs.c.wake_id == wake_id)
        with self.engine.connect() as connection:
            row = connection.execute(statement).mappings().first()
            return dict(row) if row else None

    def list_wake_runs(self, route_id: str, limit: int = 25, offset: int = 0) -> list[dict[str, Any]]:
        statement = (
            select(wake_runs)
            .where(wake_runs.c.route_id == route_id)
            .order_by(func.coalesce(wake_runs.c.completed_at, wake_runs.c.started_at).desc())
            .limit(limit)
            .offset(offset)
        )
        with self.engine.connect() as connection:
            return [dict(row._mapping) for row in connection.execute(statement)]

    def count_wake_runs(self, route_id: str) -> int:
        statement = select(func.count()).select_from(wake_runs).where(wake_runs.c.route_id == route_id)
        with self.engine.connect() as connection:
            return int(connection.execute(statement).scalar_one())

    def list_wake_run_page(self, route_id: str, *, limit: int = 25, offset: int = 0) -> dict[str, Any]:
        safe_limit = max(1, min(int(limit), 100))
        safe_offset = max(0, int(offset))
        return {
            "wakes": self.list_wake_runs(route_id, limit=safe_limit, offset=safe_offset),
            "total": self.count_wake_runs(route_id),
            "limit": safe_limit,
            "offset": safe_offset,
        }

    def update_wake_execution_results(
        self,
        *,
        wake_id: str,
        order_intents: list[dict[str, Any]],
        adapter_results: list[dict[str, Any]],
    ) -> dict[str, Any]:
        statement = (
            wake_runs.update()
            .where(wake_runs.c.wake_id == wake_id)
            .values(order_intents=_json_safe(order_intents), adapter_results=_json_safe(adapter_results))
        )
        with self.engine.begin() as connection:
            connection.execute(statement)
        stored = self.get_wake_run(wake_id)
        if stored is None:
            raise RuntimeError("wake run was not persisted")
        return stored

    def get_open_owner_state(self, route_id: str) -> dict[str, Any] | None:
        statement = (
            select(owner_states)
            .where(owner_states.c.route_id == route_id)
            .where(owner_states.c.status == "open")
            .order_by(owner_states.c.created_at.desc())
            .limit(1)
        )
        with self.engine.connect() as connection:
            row = connection.execute(statement).mappings().first()
            return dict(row) if row else None

    def create_owner_state(self, owner_state: dict[str, Any]) -> dict[str, Any]:
        owner_state = {**owner_state, "position_state": _json_safe(owner_state.get("position_state", {}))}
        with self.engine.begin() as connection:
            connection.execute(insert(owner_states).values(**owner_state))
        return owner_state

    def update_owner_state(
        self,
        owner_state_id: str,
        *,
        status: str | None = None,
        position_state: dict[str, Any] | None = None,
        closed_at: datetime | str | None = None,
    ) -> dict[str, Any]:
        values: dict[str, Any] = {}
        if status is not None:
            values["status"] = status
        if position_state is not None:
            values["position_state"] = _json_safe(position_state)
        if closed_at is not None:
            values["closed_at"] = _coerce_optional_datetime(closed_at)
        if not values:
            statement = select(owner_states).where(owner_states.c.owner_state_id == owner_state_id)
            with self.engine.connect() as connection:
                row = connection.execute(statement).mappings().first()
                if row is None:
                    raise ValueError(f"owner state not found: {owner_state_id}")
                return dict(row)
        with self.engine.begin() as connection:
            connection.execute(
                owner_states.update()
                .where(owner_states.c.owner_state_id == owner_state_id)
                .values(**values)
            )
        statement = select(owner_states).where(owner_states.c.owner_state_id == owner_state_id)
        with self.engine.connect() as connection:
            row = connection.execute(statement).mappings().first()
            if row is None:
                raise ValueError(f"owner state not found: {owner_state_id}")
            return dict(row)

    def append_owner_state_leg(self, owner_state_id: str, leg: dict[str, Any]) -> dict[str, Any]:
        statement = select(owner_states).where(owner_states.c.owner_state_id == owner_state_id)
        with self.engine.connect() as connection:
            row = connection.execute(statement).mappings().first()
        if row is None:
            raise ValueError(f"owner state not found: {owner_state_id}")
        owner_state = dict(row)
        position_state = dict(owner_state.get("position_state") or {})
        legs = list(position_state.get("legs") or [])
        legs.append(leg)
        position_state["legs"] = legs
        position_state["protection_refresh_required"] = True
        return self.update_owner_state(owner_state_id, position_state=position_state)

    def close_open_owner_state(self, route_id: str, reason: str) -> dict[str, Any] | None:
        owner_state = self.get_open_owner_state(route_id)
        if owner_state is None:
            return None
        position_state = dict(owner_state.get("position_state") or {})
        position_state["close_reason"] = reason
        return self.update_owner_state(
            owner_state["owner_state_id"],
            status="closed",
            position_state=position_state,
            closed_at=datetime.now(UTC),
        )

    def close_open_owner_states(self, route_id: str, *, instrument: str | None = None, reason: str) -> list[dict[str, Any]]:
        statement = (
            select(owner_states)
            .where(owner_states.c.route_id == route_id)
            .where(owner_states.c.status == "open")
        )
        if instrument:
            statement = statement.where(owner_states.c.instrument == instrument)
        with self.engine.connect() as connection:
            rows = [dict(row) for row in connection.execute(statement).mappings()]
        closed = []
        for owner_state in rows:
            position_state = dict(owner_state.get("position_state") or {})
            position_state["close_reason"] = reason
            closed.append(
                self.update_owner_state(
                    owner_state["owner_state_id"],
                    status="closed",
                    position_state=position_state,
                    closed_at=datetime.now(UTC),
                )
            )
        return closed

    def _repair_stage1_signal_pool_references(
        self,
        *,
        connection,
        signal_set_rewrites: dict[str, str],
        source_candidate_rewrites: dict[str, str],
    ) -> None:
        session_rows = [
            dict(row)
            for row in connection.execute(select(stage1_research_sessions)).mappings()
        ]
        for session in session_rows:
            values: dict[str, Any] = {}
            canonical_key = signal_set_rewrites.get(session["signal_set_key"])
            if canonical_key:
                values["signal_set_key"] = canonical_key
                values["signal_set_id"] = canonical_key.split(":", 2)[2]
            rewritten_candidate_id = source_candidate_rewrites.get(session["source_candidate_id"])
            if rewritten_candidate_id:
                values["source_candidate_id"] = rewritten_candidate_id

            manifest = session.get("manifest") or {}
            repaired_manifest = _repair_stage1_manifest_references(
                manifest=manifest,
                current_signal_set_key=values.get("signal_set_key", session["signal_set_key"]),
                signal_set_rewrites=signal_set_rewrites,
                source_candidate_rewrites=source_candidate_rewrites,
            )
            if repaired_manifest != manifest:
                values["manifest"] = repaired_manifest

            if values:
                connection.execute(
                    stage1_research_sessions.update()
                    .where(stage1_research_sessions.c.session_id == session["session_id"])
                    .values(**values)
                )

    def get_stage0_universe_run_by_config_hash(self, config_hash: str) -> dict[str, Any] | None:
        statement = select(stage0_universe_runs).where(stage0_universe_runs.c.config_hash == config_hash)
        with self.engine.connect() as connection:
            row = connection.execute(statement).mappings().first()
            return dict(row) if row else None

    def get_stage0_universe_run(self, universe_run_id: str) -> dict[str, Any] | None:
        statement = select(stage0_universe_runs).where(stage0_universe_runs.c.universe_run_id == universe_run_id)
        with self.engine.connect() as connection:
            row = connection.execute(statement).mappings().first()
            return dict(row) if row else None

    def create_stage0_universe(
        self,
        run: dict[str, Any],
        candidates: list[dict[str, Any]],
    ) -> None:
        run_values = {
            **run,
            "window_start": _coerce_datetime(run["window_start"]),
            "window_end": _coerce_datetime(run["window_end"]),
            "train_start": _coerce_optional_date(run.get("train_start")),
            "train_end": _coerce_optional_date(run.get("train_end")),
            "walk_forward_start": _coerce_optional_date(run.get("walk_forward_start")),
            "walk_forward_end": _coerce_optional_date(run.get("walk_forward_end")),
        }
        with self.engine.begin() as connection:
            connection.execute(self._insert_stage0_universe_run_ignore_conflict(run_values))
            for candidate in candidates:
                connection.execute(
                    self._insert_stage0_universe_candidate_ignore_conflict(
                        {
                            **candidate,
                            "last_error": candidate.get("last_error", {}),
                        }
                    )
                )

    def append_stage0_universe_candidates(self, universe_run_id: str, candidates: list[dict[str, Any]]) -> None:
        with self.engine.begin() as connection:
            for candidate in candidates:
                connection.execute(
                    self._insert_stage0_universe_candidate_ignore_conflict(
                        {
                            **candidate,
                            "universe_run_id": universe_run_id,
                            "last_error": candidate.get("last_error", {}),
                        }
                    )
                )

    def list_stage0_universe_runs(self) -> list[dict[str, Any]]:
        statement = select(stage0_universe_runs).order_by(stage0_universe_runs.c.created_at.desc())
        with self.engine.connect() as connection:
            return [dict(row._mapping) for row in connection.execute(statement)]

    def list_stage0_universe_candidates(self, universe_run_id: str) -> list[dict[str, Any]]:
        statement = (
            select(stage0_universe_candidates)
            .where(stage0_universe_candidates.c.universe_run_id == universe_run_id)
            .order_by(
                stage0_universe_candidates.c.acceptance_status,
                stage0_universe_candidates.c.asset,
                stage0_universe_candidates.c.signal_engine_id,
            )
        )
        with self.engine.connect() as connection:
            return [dict(row._mapping) for row in connection.execute(statement)]

    def get_stage0_universe_candidate(self, candidate_id: str) -> dict[str, Any] | None:
        statement = select(stage0_universe_candidates).where(
            stage0_universe_candidates.c.candidate_id == candidate_id
        )
        with self.engine.connect() as connection:
            row = connection.execute(statement).mappings().first()
            return dict(row) if row else None

    def update_stage0_universe_candidate(self, candidate: dict[str, Any]) -> None:
        values = {
            "trigger_rate_pct": candidate.get("trigger_rate_pct"),
            "branch_path": candidate["branch_path"],
            "acceptance_status": candidate["acceptance_status"],
            "duplicate_status": candidate.get("duplicate_status", "new"),
            "existing_strategy_id": candidate.get("existing_strategy_id"),
            "last_error": candidate.get("last_error", {}),
            "metrics": candidate.get("metrics", {}),
        }
        statement = (
            stage0_universe_candidates.update()
            .where(stage0_universe_candidates.c.candidate_id == candidate["candidate_id"])
            .values(**values)
        )
        with self.engine.begin() as connection:
            connection.execute(statement)

    def mark_stage0_universe_candidate_error(self, candidate_id: str, error: dict[str, Any]) -> None:
        statement = (
            stage0_universe_candidates.update()
            .where(stage0_universe_candidates.c.candidate_id == candidate_id)
            .values(last_error=error)
        )
        with self.engine.begin() as connection:
            connection.execute(statement)

    def refresh_stage0_universe_summary(self, universe_run_id: str) -> None:
        candidates = self.list_stage0_universe_candidates(universe_run_id)
        summary = {
            "total_candidates": len(candidates),
            "accepted": sum(1 for item in candidates if item["acceptance_status"] == "accepted"),
            "watchlist": sum(1 for item in candidates if item["acceptance_status"] == "watchlist"),
            "pending_stage0": sum(1 for item in candidates if item["acceptance_status"] == "pending_stage0"),
            "failed": sum(1 for item in candidates if item.get("last_error")),
        }
        status = "completed" if candidates and summary["pending_stage0"] == 0 else "created"
        statement = (
            stage0_universe_runs.update()
            .where(stage0_universe_runs.c.universe_run_id == universe_run_id)
            .where(stage0_universe_runs.c.status != "superseded")
            .values(summary=summary, status=status)
        )
        with self.engine.begin() as connection:
            connection.execute(statement)

    def supersede_stage0_universe_run(self, universe_run_id: str) -> None:
        statement = (
            stage0_universe_runs.update()
            .where(stage0_universe_runs.c.universe_run_id == universe_run_id)
            .values(status="superseded")
        )
        with self.engine.begin() as connection:
            connection.execute(statement)

    def delete_stage0_universe_run(self, universe_run_id: str) -> None:
        with self.engine.begin() as connection:
            connection.execute(
                stage1_research_sessions.delete().where(
                    stage1_research_sessions.c.source_universe_run_id == universe_run_id
                )
            )
            connection.execute(
                stage0_universe_candidates.delete().where(
                    stage0_universe_candidates.c.universe_run_id == universe_run_id
                )
            )
            connection.execute(
                stage0_universe_runs.delete().where(
                    stage0_universe_runs.c.universe_run_id == universe_run_id
                )
            )

    def stage0_metrics_by_signal_set(self) -> dict[str, dict[str, Any]]:
        statement = select(strategy_development_runs).where(strategy_development_runs.c.stage == "stage0")
        metrics: dict[str, dict[str, Any]] = {}
        with self.engine.connect() as connection:
            for row in connection.execute(statement).mappings():
                row_metrics = row["metrics"] or {}
                trigger_rate = row_metrics.get("trigger_rate_pct")
                if trigger_rate is not None:
                    metrics[row["signal_set_key"]] = row_metrics
        return metrics

    def existing_rnd_by_signal_set(self) -> dict[str, dict[str, Any]]:
        existing: dict[str, dict[str, Any]] = {}
        with self.engine.connect() as connection:
            statement = select(strategy_development_runs).where(strategy_development_runs.c.stage != "stage0")
            for row in connection.execute(statement).mappings():
                existing.setdefault(
                    row["signal_set_key"],
                    {
                        "strategy_id": row["strategy_id"],
                        "status": row["status"],
                        "run_id": row["run_id"],
                    },
                )
            session_statement = select(stage1_research_sessions).order_by(stage1_research_sessions.c.created_at.desc())
            for row in connection.execute(session_statement).mappings():
                existing.setdefault(
                    row["signal_set_key"],
                    {
                        "strategy_id": row["strategy_id"],
                        "status": row["status"],
                        "run_id": row["session_id"],
                    },
                )
        return existing

    def signal_counts_by_signal_set_window(
        self,
        *,
        window_start: str,
        window_end: str,
        engine_ids: list[str] | None = None,
    ) -> dict[str, int]:
        statement = (
            select(signals.c.signal_set_key, func.count(func.distinct(signals.c.timestamp)).label("signal_count"))
            .where(signals.c.timestamp >= _coerce_datetime(window_start))
            .where(signals.c.timestamp <= _coerce_datetime(window_end))
            .where(signals.c.signal_set_key.is_not(None))
            .group_by(signals.c.signal_set_key)
        )
        if engine_ids:
            statement = statement.where(signals.c.signal_engine_id.in_(engine_ids))
        with self.engine.connect() as connection:
            return {
                row["signal_set_key"]: int(row["signal_count"])
                for row in connection.execute(statement).mappings()
                if row["signal_set_key"]
            }

    def split_signal_counts_by_signal_set(
        self,
        *,
        train_start: str | None,
        train_end: str | None,
        walk_forward_start: str | None,
        walk_forward_end: str | None,
        engine_ids: list[str] | None = None,
    ) -> dict[str, dict[str, int]]:
        windows = {
            "train": (train_start, train_end),
            "walk_forward": (walk_forward_start, walk_forward_end),
        }
        counts: dict[str, dict[str, int]] = {}
        for split, (start, end) in windows.items():
            if not start or not end:
                continue
            for signal_set_key, signal_count in self.signal_counts_by_signal_set_window(
                window_start=f"{start}T00:00:00Z",
                window_end=f"{end}T23:59:59Z",
                engine_ids=engine_ids,
            ).items():
                counts.setdefault(signal_set_key, {})[split] = signal_count
        return counts

    def list_signals_for_signal_set_window(
        self,
        *,
        signal_set_key: str,
        window_start: str,
        window_end: str,
    ) -> list[dict[str, Any]]:
        statement = (
            select(signals)
            .where(signals.c.signal_set_key == signal_set_key)
            .where(signals.c.timestamp >= _coerce_datetime(window_start))
            .where(signals.c.timestamp <= _coerce_datetime(window_end))
            .order_by(signals.c.timestamp, signals.c.signal_id)
        )
        with self.engine.connect() as connection:
            rows = [dict(row._mapping) for row in connection.execute(statement)]
        deduped_rows, _ = _dedupe_signal_rows(rows)
        return deduped_rows

    def _insert_signal_ignore_conflict(self, values: dict[str, Any]):
        if self.engine.dialect.name == "postgresql":
            return postgres_insert(signals).values(**values).on_conflict_do_nothing(
                index_elements=["signal_id"]
            )
        return insert(signals).values(**values).prefix_with("OR IGNORE")

    def _insert_signals_ignore_conflict(self, values: list[dict[str, Any]]):
        if self.engine.dialect.name == "postgresql":
            return postgres_insert(signals).values(values).on_conflict_do_nothing(
                index_elements=["signal_id"]
            )
        return insert(signals).values(values).prefix_with("OR IGNORE")

    def _upsert_live_signal_observation(self, values: dict[str, Any]):
        if self.engine.dialect.name == "postgresql":
            return postgres_insert(live_signal_observations).values(**values).on_conflict_do_update(
                index_elements=["signal_engine_id", "asset", "signal_timestamp", "route_id"],
                set_={
                    "signal_engine_version": values["signal_engine_version"],
                    "instrument": values["instrument"],
                    "signal_id": values["signal_id"],
                    "bundle_id": values["bundle_id"],
                    "packet_hash": values["packet_hash"],
                    "payload_schema": values["payload_schema"],
                    "payload": values["payload"],
                    "decision": values["decision"],
                    "scan_metadata": values["scan_metadata"],
                    "observed_at": values["observed_at"],
                },
            )
        return insert(live_signal_observations).values(**values).prefix_with("OR REPLACE")

    def _insert_signal_engine_ignore_conflict(self, values: dict[str, Any]):
        if self.engine.dialect.name == "postgresql":
            return postgres_insert(signal_engines).values(**values).on_conflict_do_nothing(
                index_elements=["signal_engine_id"]
            )
        return insert(signal_engines).values(**values).prefix_with("OR IGNORE")

    def _insert_signal_engine_version_ignore_conflict(self, values: dict[str, Any]):
        if self.engine.dialect.name == "postgresql":
            return postgres_insert(signal_engine_versions).values(**values).on_conflict_do_nothing(
                index_elements=["signal_engine_id", "version"]
            )
        return insert(signal_engine_versions).values(**values).prefix_with("OR IGNORE")

    def _upsert_signal_set(self, values: dict[str, Any]):
        if self.engine.dialect.name == "postgresql":
            return postgres_insert(signal_sets).values(**values).on_conflict_do_update(
                index_elements=["signal_set_key"],
                set_={
                    "signal_engine_version": values["signal_engine_version"],
                    "instrument": values["instrument"],
                    "start_ts": values["start_ts"],
                    "end_ts": values["end_ts"],
                    "packet_count": values["packet_count"],
                    "payload_schema": values["payload_schema"],
                    "source_path": values["source_path"],
                    "manifest": values["manifest"],
                },
            )
        return insert(signal_sets).values(**values).prefix_with("OR REPLACE")

    def _upsert_worker_heartbeat(self, values: dict[str, Any]):
        if self.engine.dialect.name == "postgresql":
            return postgres_insert(worker_heartbeats).values(**values).on_conflict_do_update(
                index_elements=["worker_id"],
                set_={
                    "status": values["status"],
                    "current_job_id": values["current_job_id"],
                    "current_step": values["current_step"],
                    "last_seen_at": values["last_seen_at"],
                },
            )
        return insert(worker_heartbeats).values(**values).prefix_with("OR REPLACE")

    def _insert_strategy_development_run_ignore_conflict(self, values: dict[str, Any]):
        if self.engine.dialect.name == "postgresql":
            return postgres_insert(strategy_development_runs).values(**values).on_conflict_do_nothing(
                index_elements=["run_id"]
            )
        return insert(strategy_development_runs).values(**values).prefix_with("OR IGNORE")

    def _insert_stage0_universe_run_ignore_conflict(self, values: dict[str, Any]):
        if self.engine.dialect.name == "postgresql":
            return postgres_insert(stage0_universe_runs).values(**values).on_conflict_do_nothing(
                index_elements=["universe_run_id"]
            )
        return insert(stage0_universe_runs).values(**values).prefix_with("OR IGNORE")

    def _insert_stage0_universe_candidate_ignore_conflict(self, values: dict[str, Any]):
        if self.engine.dialect.name == "postgresql":
            return postgres_insert(stage0_universe_candidates).values(**values).on_conflict_do_nothing(
                index_elements=["candidate_id"]
            )
        return insert(stage0_universe_candidates).values(**values).prefix_with("OR IGNORE")

    def _insert_stage1_research_session_ignore_conflict(self, values: dict[str, Any]):
        if self.engine.dialect.name == "postgresql":
            return postgres_insert(stage1_research_sessions).values(**values).on_conflict_do_nothing(
                index_elements=["session_id"]
            )
        return insert(stage1_research_sessions).values(**values).prefix_with("OR IGNORE")

    def _insert_execution_bundle_ignore_conflict(self, values: dict[str, Any]):
        if self.engine.dialect.name == "postgresql":
            return postgres_insert(execution_bundles).values(**values).on_conflict_do_nothing(
                index_elements=["bundle_id"]
            )
        return insert(execution_bundles).values(**values).prefix_with("OR IGNORE")

    def _upsert_deployment_route(self, values: dict[str, Any]):
        if self.engine.dialect.name == "postgresql":
            return postgres_insert(deployment_routes).values(**values).on_conflict_do_update(
                index_elements=["asset", "account_mode", "execution_adapter", "exchange_account"],
                set_={
                    "active_bundle_id": values["active_bundle_id"],
                    "strategy_id": values["strategy_id"],
                    "strategy_version": values["strategy_version"],
                    "signal_engine_id": values["signal_engine_id"],
                    "signal_engine_version": values["signal_engine_version"],
                    "instrument": values["instrument"],
                    "execution_adapter": values["execution_adapter"],
                    "exchange_account": values["exchange_account"],
                    "cron_interval_minutes": values["cron_interval_minutes"],
                    "margin_allocation_pct": values["margin_allocation_pct"],
                    "leverage": values["leverage"],
                    "manual_sizing_enabled": values["manual_sizing_enabled"],
                    "scheduler_status": values["scheduler_status"],
                    "auto_submit_enabled": values["auto_submit_enabled"],
                    "last_wake_at": values["last_wake_at"],
                    "last_wake_id": values["last_wake_id"],
                    "next_wake_at": values["next_wake_at"],
                    "last_lifecycle_error": values["last_lifecycle_error"],
                    "risk_limits": values["risk_limits"],
                    "promoted": values["promoted"],
                    "data_warmed": values["data_warmed"],
                    "manually_armed": values["manually_armed"],
                    "enabled": values["enabled"],
                    "archived": values["archived"],
                    "archived_at": values["archived_at"],
                },
            )
        return insert(deployment_routes).values(**values).prefix_with("OR REPLACE")


def _coerce_datetime(value: str | datetime) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _normalize_route_datetimes(route: dict[str, Any]) -> None:
    for key in ("last_wake_at", "next_wake_at", "archived_at", "created_at"):
        value = route.get(key)
        if isinstance(value, datetime) and value.tzinfo is None:
            route[key] = value.replace(tzinfo=UTC)


def _route_id(
    *,
    asset: str,
    account_mode: str,
    execution_adapter: str | None = None,
    exchange_account: str | None = None,
) -> str:
    base = f"{asset.lower()}-{account_mode}"
    if execution_adapter is None and exchange_account is None:
        return base
    return f"{base}-{str(execution_adapter or 'exchange').lower()}-{str(exchange_account or 'default').lower()}"


def _execution_setup_cron_minutes(execution_setup: Any) -> int:
    value = None
    if isinstance(execution_setup, dict):
        value = execution_setup.get("cron_interval_minutes") or execution_setup.get("cron_interval")
        nested = execution_setup.get("setup")
        if value is None and isinstance(nested, dict):
            value = nested.get("cron_interval_minutes") or nested.get("cron_interval")
    try:
        minutes = int(value) if value is not None else 5
    except (TypeError, ValueError):
        minutes = 5
    return max(1, minutes)


def _execution_setup_leverage(execution_setup: Any) -> float:
    value = None
    if isinstance(execution_setup, dict):
        sizing = execution_setup.get("sizing")
        if isinstance(sizing, dict):
            value = sizing.get("leverage")
        if value is None:
            value = execution_setup.get("leverage")
        nested = execution_setup.get("setup")
        if value is None and isinstance(nested, dict):
            value = nested.get("leverage")
    try:
        leverage = float(value) if value is not None else 1.0
    except (TypeError, ValueError):
        leverage = 1.0
    return leverage if leverage > 0 else 1.0


def _execution_setup_margin_allocation_pct(execution_setup: Any) -> float:
    value = None
    if isinstance(execution_setup, dict):
        sizing = execution_setup.get("sizing")
        if isinstance(sizing, dict):
            value = sizing.get("margin_allocation_pct")
        if value is None:
            value = execution_setup.get("margin_allocation_pct")
        nested = execution_setup.get("setup")
        if value is None and isinstance(nested, dict):
            value = nested.get("margin_allocation_pct")
    try:
        margin = float(value) if value is not None else 10.0
    except (TypeError, ValueError):
        margin = 10.0
    return margin if margin > 0 else 10.0


def _route_blockers(route: dict[str, Any]) -> list[str]:
    blockers: list[str] = []
    if not route.get("enabled"):
        blockers.append("route_disabled")
    if not route.get("active_bundle_id"):
        blockers.append("missing_active_bundle")
    if not route.get("promoted"):
        blockers.append("route_not_promoted")
    if not route.get("data_warmed"):
        blockers.append("data_not_warmed")
    if route.get("account_mode") == "live" and not route.get("manually_armed"):
        blockers.append("route_not_manually_armed")
    return blockers


def _market_ref_freshness(
    ref: dict[str, Any] | None,
    *,
    checked_at: datetime,
    max_age_seconds: int,
) -> dict[str, Any]:
    if ref is None:
        return {"status": "missing"}
    end_ts = ref.get("end_ts")
    if end_ts is None:
        return {
            "status": "unknown",
            "dataset_id": ref.get("dataset_id"),
            "reason": "missing_end_ts",
        }
    timestamp = _coerce_datetime(end_ts)
    age_seconds = max(0, int((checked_at - timestamp).total_seconds()))
    return {
        "status": "fresh" if age_seconds <= max_age_seconds else "stale",
        "dataset_id": ref.get("dataset_id"),
        "timestamp": timestamp.replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "age_seconds": age_seconds,
    }


def _route_requires_candle(requirements: list[dict[str, Any]], *, origin: str, timeframe: str) -> bool:
    return any(
        requirement.get("data_type") == "candles"
        and requirement.get("origin") == origin
        and (requirement.get("timeframe") or "5m") == timeframe
        for requirement in requirements
    )


def _timeframe_seconds(timeframe: str) -> int:
    normalized = str(timeframe).strip().lower()
    if normalized.endswith("m"):
        return int(normalized[:-1]) * 60
    if normalized.endswith("h"):
        return int(normalized[:-1]) * 60 * 60
    if normalized.endswith("d"):
        return int(normalized[:-1]) * 24 * 60 * 60
    return 300


def _repair_stage1_manifest_references(
    *,
    manifest: dict[str, Any],
    current_signal_set_key: str,
    signal_set_rewrites: dict[str, str],
    source_candidate_rewrites: dict[str, str],
) -> dict[str, Any]:
    repaired = dict(manifest)
    manifest_signal_key = repaired.get("signal_set_key")
    canonical_key = signal_set_rewrites.get(manifest_signal_key)
    if canonical_key is None and _same_engine_asset_signal_pool(
        old_key=manifest_signal_key,
        canonical_key=current_signal_set_key,
    ):
        canonical_key = current_signal_set_key
    if canonical_key:
        repaired["signal_set_key"] = canonical_key
        repaired["signal_set_id"] = canonical_key.split(":", 2)[2]

    stage0_candidate_id = repaired.get("stage0_candidate_id")
    rewritten_candidate_id = source_candidate_rewrites.get(stage0_candidate_id)
    if rewritten_candidate_id:
        repaired["stage0_candidate_id"] = rewritten_candidate_id

    seed_strategy = repaired.get("seed_strategy")
    if isinstance(seed_strategy, dict):
        seed_candidate_id = seed_strategy.get("stage0_candidate_id")
        rewritten_seed_candidate_id = source_candidate_rewrites.get(seed_candidate_id)
        if rewritten_seed_candidate_id:
            repaired["seed_strategy"] = {
                **seed_strategy,
                "stage0_candidate_id": rewritten_seed_candidate_id,
            }

    return repaired


def _same_engine_asset_signal_pool(*, old_key: Any, canonical_key: str) -> bool:
    if not isinstance(old_key, str) or old_key.count(":") < 2 or canonical_key.count(":") < 2:
        return False
    old_engine, old_asset, old_set_id = old_key.split(":", 2)
    canonical_engine, canonical_asset, canonical_set_id = canonical_key.split(":", 2)
    return (
        old_engine == canonical_engine
        and old_asset == canonical_asset
        and old_set_id != canonical_set_id
        and canonical_set_id == f"{canonical_asset}-{canonical_engine}-canonical"
    )


def _coerce_optional_datetime(value: str | datetime | None) -> datetime | None:
    if value is None:
        return None
    return _coerce_datetime(value)


def _json_safe(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat().replace("+00:00", "Z")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, dict):
        return {key: _json_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    if isinstance(value, tuple):
        return [_json_safe(item) for item in value]
    return value


def _stable_json_hash(value: Any) -> str:
    encoded = json.dumps(_json_safe(value), sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _live_signal_observation_id(observation: dict[str, Any]) -> str:
    timestamp = _coerce_datetime(observation.get("signal_timestamp") or observation["timestamp"]).strftime("%Y%m%dT%H%M%SZ")
    route = observation.get("route_id") or "unrouted"
    return f"live-observation:{observation['signal_engine_id']}:{str(observation['asset']).upper()}:{route}:{timestamp}"


def _normalize_live_signal_observation_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        **row,
        "signal_timestamp": _coerce_datetime(row["signal_timestamp"]),
        "observed_at": _coerce_datetime(row["observed_at"]),
    }


def _normalize_signal_set_row(row: dict[str, Any]) -> dict[str, Any]:
    manifest = row.get("manifest") or {}
    scan_coverage = manifest.get("scan_coverage") if isinstance(manifest, dict) else None
    coverage_start = scan_coverage.get("start_ts") if isinstance(scan_coverage, dict) else None
    coverage_end = scan_coverage.get("end_ts") if isinstance(scan_coverage, dict) else None
    return {
        **row,
        "start_ts": _coerce_optional_datetime(row.get("start_ts")),
        "end_ts": _coerce_optional_datetime(row.get("end_ts")),
        "packet_start_ts": _coerce_optional_datetime(row.get("start_ts")),
        "packet_end_ts": _coerce_optional_datetime(row.get("end_ts")),
        "coverage_start_ts": _coerce_optional_datetime(coverage_start) or _coerce_optional_datetime(row.get("start_ts")),
        "coverage_end_ts": _coerce_optional_datetime(coverage_end) or _coerce_optional_datetime(row.get("end_ts")),
    }


def _normalize_job_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        **row,
        "payload": row.get("payload") or {},
        "result": row.get("result") or {},
        "error": row.get("error") or {},
    }


def _normalize_worker_heartbeat_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        **row,
        "started_at": _coerce_datetime(row["started_at"]),
        "last_seen_at": _coerce_datetime(row["last_seen_at"]),
    }


def _is_ignored_worker_heartbeat(row: dict[str, Any]) -> bool:
    return row.get("worker_id") == "celery-unknown"


def _coerce_date(value: str | date) -> date:
    if isinstance(value, date):
        return value
    return date.fromisoformat(value)


def _coerce_optional_date(value: str | date | None) -> date | None:
    if value is None:
        return None
    return _coerce_date(value)


def _dedupe_signal_rows(rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, str]]:
    selected: dict[tuple[str | None, datetime], dict[str, Any]] = {}
    rewrites: dict[str, str] = {}
    for row in rows:
        timestamp = _coerce_datetime(row["timestamp"])
        key = (row.get("signal_set_key"), timestamp)
        current = selected.get(key)
        if current is None:
            selected[key] = row
            continue
        if _signal_row_rank(row) > _signal_row_rank(current):
            rewrites[current["signal_id"]] = row["signal_id"]
            selected[key] = row
        else:
            rewrites[row["signal_id"]] = current["signal_id"]
    deduped = sorted(
        selected.values(),
        key=lambda row: (_coerce_datetime(row["timestamp"]), row["signal_id"]),
    )
    return deduped, rewrites


def _signal_row_rank(row: dict[str, Any]) -> tuple[int, int, int, str]:
    signal_id = str(row.get("signal_id") or "")
    canonical_signal_id = _canonical_signal_id(row)
    return (
        1 if signal_id == canonical_signal_id else 0,
        1 if _signal_id_matches_signal_set(row) else 0,
        len(row.get("data_refs") or []),
        signal_id,
    )


def _canonical_signal_id(row: dict[str, Any]) -> str:
    signal_set_key = str(row.get("signal_set_key") or "")
    if signal_set_key.count(":") < 2:
        return str(row.get("signal_id") or "")
    signal_engine_id, asset, signal_set_id = signal_set_key.split(":", 2)
    timestamp = _coerce_datetime(row["timestamp"]).strftime("%Y%m%dT%H%M%SZ")
    return f"{signal_engine_id}:{asset}:{signal_set_id}:{timestamp}"


def _signal_id_matches_signal_set(row: dict[str, Any]) -> bool:
    signal_id = str(row.get("signal_id") or "")
    signal_set_key = str(row.get("signal_set_key") or "")
    if signal_set_key.count(":") < 2:
        return False
    _, _, signal_set_id = signal_set_key.split(":", 2)
    return f":{signal_set_id}:" in signal_id
