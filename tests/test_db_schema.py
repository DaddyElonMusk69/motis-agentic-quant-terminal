from quant_terminal_api.db.models import metadata


def test_schema_declares_core_product_tables():
    expected = {
        "data_sources",
        "market_data_refs",
        "signal_engines",
        "signal_engine_versions",
        "signal_sets",
        "signals",
        "strategy_modules",
        "strategy_versions",
        "walk_forward_templates",
        "walk_forward_runs",
        "stage0_universe_runs",
        "stage0_universe_candidates",
        "stage1_research_sessions",
        "strategy_development_runs",
        "backtest_runs",
        "stage_runs",
        "decisions",
        "score_summaries",
        "agent_tasks",
        "agent_runs",
        "deployment_routes",
        "audit_log",
    }

    assert expected.issubset(set(metadata.tables))


def test_deployment_routes_enforce_one_live_route_per_strategy_asset_pair():
    route_table = metadata.tables["deployment_routes"]

    unique_constraints = {
        tuple(constraint.columns.keys())
        for constraint in route_table.constraints
        if constraint.__class__.__name__ == "UniqueConstraint"
    }

    assert ("strategy_id", "asset") in unique_constraints


def test_market_data_refs_unique_key_includes_data_origin():
    table = metadata.tables["market_data_refs"]

    unique_constraints = {
        tuple(constraint.columns.keys())
        for constraint in table.constraints
        if constraint.__class__.__name__ == "UniqueConstraint"
    }

    assert ("source_id", "instrument", "data_type", "timeframe", "data_origin", "ingestion_version") in unique_constraints


def test_signal_sets_enforce_one_set_per_engine_asset_and_name():
    table = metadata.tables["signal_sets"]

    unique_constraints = {
        tuple(constraint.columns.keys())
        for constraint in table.constraints
        if constraint.__class__.__name__ == "UniqueConstraint"
    }

    assert ("signal_engine_id", "asset", "signal_set_id") in unique_constraints


def test_strategy_development_runs_track_stage_and_signal_set():
    table = metadata.tables["strategy_development_runs"]

    assert {"run_id", "stage", "strategy_id", "signal_set_key", "artifact_root", "status"}.issubset(
        set(table.columns.keys())
    )


def test_stage0_universe_candidates_are_unique_per_run_and_signal_set():
    table = metadata.tables["stage0_universe_candidates"]

    unique_constraints = {
        tuple(constraint.columns.keys())
        for constraint in table.constraints
        if constraint.__class__.__name__ == "UniqueConstraint"
    }

    assert ("universe_run_id", "signal_set_key") in unique_constraints
    assert "last_error" in table.columns


def test_stage0_universe_runs_allow_repeat_configs():
    table = metadata.tables["stage0_universe_runs"]

    unique_constraints = {
        tuple(constraint.columns.keys())
        for constraint in table.constraints
        if constraint.__class__.__name__ == "UniqueConstraint"
    }

    assert ("config_hash",) not in unique_constraints


def test_stage1_research_sessions_link_to_stage0_candidate():
    table = metadata.tables["stage1_research_sessions"]

    assert {
        "session_id",
        "source_universe_run_id",
        "source_candidate_id",
        "strategy_id",
        "strategy_version",
        "train_start",
        "train_end",
        "walk_forward_start",
        "walk_forward_end",
        "artifact_root",
        "status",
        "seed_strategy_source_type",
        "seed_strategy_source_path",
        "seed_strategy_source_version",
        "seed_strategy_source_session_id",
    }.issubset(set(table.columns.keys()))
