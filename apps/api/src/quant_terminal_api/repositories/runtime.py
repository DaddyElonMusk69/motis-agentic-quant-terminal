from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from sqlalchemy import Engine, create_engine, func, insert, select
from sqlalchemy.dialects.postgresql import insert as postgres_insert

from quant_terminal_api.db.models import (
    backtest_runs,
    decisions,
    market_data_refs,
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
)


class RuntimeRepository:
    def __init__(self, engine_or_database_url: Engine | str) -> None:
        self.engine = (
            create_engine(engine_or_database_url)
            if isinstance(engine_or_database_url, str)
            else engine_or_database_url
        )

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
                        "output_envelope_version": registration["output_envelope_version"],
                        "runtime_entrypoint": registration["runtime_entrypoint"],
                        "live_scanner_entrypoint": registration.get("live_scanner_entrypoint"),
                        "configuration_schema": registration.get("configuration_schema", {}),
                    }
                )
            )

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
                signal_engine_versions.c.code_ref,
                signal_engine_versions.c.runtime_entrypoint,
                signal_engine_versions.c.live_scanner_entrypoint,
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
                signal_engine_versions.c.code_ref,
                signal_engine_versions.c.runtime_entrypoint,
                signal_engine_versions.c.live_scanner_entrypoint,
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

    def list_signals(
        self,
        *,
        signal_set_key: str | None = None,
        signal_engine_id: str | None = None,
        asset: str | None = None,
        limit: int = 25,
    ) -> list[dict[str, Any]]:
        statement = select(signals).order_by(signals.c.timestamp).limit(limit)
        if signal_set_key:
            statement = statement.where(signals.c.signal_set_key == signal_set_key)
        if signal_engine_id:
            statement = statement.where(signals.c.signal_engine_id == signal_engine_id)
        if asset:
            statement = statement.where(signals.c.asset == asset)
        with self.engine.connect() as connection:
            return [dict(row._mapping) for row in connection.execute(statement)]

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
            "validation_start": _coerce_date(session["validation_start"]),
            "validation_end": _coerce_date(session["validation_end"]),
            "locked_oos_start": _coerce_date(session["locked_oos_start"]),
            "locked_oos_end": _coerce_date(session["locked_oos_end"]),
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
            "validation_start": _coerce_optional_date(run.get("validation_start")),
            "validation_end": _coerce_optional_date(run.get("validation_end")),
            "locked_oos_start": _coerce_optional_date(run.get("locked_oos_start")),
            "locked_oos_end": _coerce_optional_date(run.get("locked_oos_end")),
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
        validation_start: str | None,
        validation_end: str | None,
        locked_oos_start: str | None,
        locked_oos_end: str | None,
        engine_ids: list[str] | None = None,
    ) -> dict[str, dict[str, int]]:
        windows = {
            "train": (train_start, train_end),
            "validation": (validation_start, validation_end),
            "locked_oos": (locked_oos_start, locked_oos_end),
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


def _coerce_datetime(value: str | datetime) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _coerce_optional_datetime(value: str | datetime | None) -> datetime | None:
    if value is None:
        return None
    return _coerce_datetime(value)


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
