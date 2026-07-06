from __future__ import annotations

import os
from threading import Lock

from sqlalchemy import Engine, create_engine

_DEFAULT_POOL_SIZE = 2
_DEFAULT_MAX_OVERFLOW = 0

_ENGINE_LOCK = Lock()
_ENGINE_CACHE: dict[tuple[int, str, int, int], Engine] = {}


def get_database_engine(database_url: str) -> Engine:
    pool_size = _read_pool_int("MOTIS_DB_POOL_SIZE", _DEFAULT_POOL_SIZE)
    max_overflow = _read_pool_int("MOTIS_DB_MAX_OVERFLOW", _DEFAULT_MAX_OVERFLOW)
    cache_key = (os.getpid(), database_url, pool_size, max_overflow)
    with _ENGINE_LOCK:
        engine = _ENGINE_CACHE.get(cache_key)
        if engine is None:
            engine = create_engine(
                database_url,
                pool_pre_ping=True,
                pool_size=pool_size,
                max_overflow=max_overflow,
            )
            _ENGINE_CACHE[cache_key] = engine
        return engine


def dispose_cached_engines() -> None:
    with _ENGINE_LOCK:
        engines = list(_ENGINE_CACHE.values())
        _ENGINE_CACHE.clear()
    for engine in engines:
        engine.dispose()


def _read_pool_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    return max(0, value)
