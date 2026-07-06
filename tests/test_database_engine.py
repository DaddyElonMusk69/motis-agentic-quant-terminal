from sqlalchemy import create_engine

from quant_terminal_api.db import engine as database_engine
from quant_terminal_api.repositories.market_data import PostgresMarketDataRepository
from quant_terminal_api.repositories.runtime import RuntimeRepository


def test_url_based_repositories_reuse_engine_for_same_database_url(tmp_path):
    database_url = f"sqlite+pysqlite:///{tmp_path / 'shared.db'}"

    first_runtime = RuntimeRepository(database_url)
    second_runtime = RuntimeRepository(database_url)
    market_data = PostgresMarketDataRepository(database_url)

    assert first_runtime.engine is second_runtime.engine
    assert market_data.engine is first_runtime.engine


def test_explicit_runtime_repository_engine_is_preserved():
    engine = create_engine("sqlite+pysqlite:///:memory:")

    repository = RuntimeRepository(engine)

    assert repository.engine is engine


def test_database_engine_cache_is_pid_aware(monkeypatch, tmp_path):
    database_url = f"sqlite+pysqlite:///{tmp_path / 'pid-aware.db'}"
    database_engine.dispose_cached_engines()
    monkeypatch.setattr(database_engine.os, "getpid", lambda: 100)
    parent_engine = database_engine.get_database_engine(database_url)

    monkeypatch.setattr(database_engine.os, "getpid", lambda: 200)
    child_engine = database_engine.get_database_engine(database_url)

    assert child_engine is not parent_engine


def test_dispose_cached_engines_clears_cached_instances(tmp_path):
    database_url = f"sqlite+pysqlite:///{tmp_path / 'dispose.db'}"
    first_engine = database_engine.get_database_engine(database_url)

    database_engine.dispose_cached_engines()
    second_engine = database_engine.get_database_engine(database_url)

    assert second_engine is not first_engine
