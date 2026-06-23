from sqlalchemy.dialects import postgresql

from quant_terminal_api.repositories.market_data import (
    build_data_source_upsert,
    build_market_data_ref_upsert,
    PostgresMarketDataRepository,
)


def test_build_market_data_ref_insert_targets_market_data_refs_table():
    registration = {
        "dataset_id": "okx-BTC-USDT-SWAP-candles-5m-2026-06-okx-cli-test",
        "source_id": "okx",
        "asset": "BTC",
        "instrument": "BTC-USDT-SWAP",
        "data_type": "candles",
        "timeframe": "5m",
        "data_origin": "raw",
        "start_ts": "2026-06-01T00:00:00Z",
        "end_ts": "2026-06-01T00:05:00Z",
        "storage_backend": "parquet",
        "storage_uri": "/tmp/data.parquet",
        "schema_descriptor": {"columns": ["timestamp"]},
        "quality_status": "ingested",
        "ingestion_version": "okx-cli-test",
    }

    statement = build_market_data_ref_upsert(registration)
    compiled = str(statement.compile(dialect=postgresql.dialect()))

    assert "INSERT INTO market_data_refs" in compiled
    assert "ON CONFLICT" in compiled
    assert "dataset_id" in compiled
    assert "storage_uri" in compiled


def test_build_data_source_upsert_targets_data_sources_table():
    statement = build_data_source_upsert(source_id="okx", name="OKX", source_type="exchange")
    compiled = str(statement.compile(dialect=postgresql.dialect()))

    assert "INSERT INTO data_sources" in compiled
    assert "ON CONFLICT" in compiled


def test_list_derived_refs_for_raw_filters_matching_candle_datasets():
    repository = PostgresMarketDataRepository.__new__(PostgresMarketDataRepository)
    statement = repository.build_derived_refs_for_raw_statement(
        {
            "source_id": "okx",
            "asset": "BTC",
            "instrument": "BTC-USDT-SWAP",
            "data_type": "candles",
        }
    )
    compiled = str(statement.compile(dialect=postgresql.dialect()))

    assert "market_data_refs.source_id = " in compiled
    assert "market_data_refs.asset = " in compiled
    assert "market_data_refs.instrument = " in compiled
    assert "market_data_refs.data_type = " in compiled
    assert "market_data_refs.data_origin = " in compiled


def test_get_candle_ref_statement_filters_by_origin_and_latest_end_ts():
    repository = PostgresMarketDataRepository.__new__(PostgresMarketDataRepository)
    statement = repository.build_candle_ref_statement(
        asset="arb",
        timeframe="5m",
        origin="derived",
        data_type="candles",
    )
    compiled = str(statement.compile(dialect=postgresql.dialect()))

    assert "market_data_refs.asset = " in compiled
    assert "market_data_refs.data_type = " in compiled
    assert "market_data_refs.timeframe = " in compiled
    assert "market_data_refs.data_origin = " in compiled
    assert "ORDER BY market_data_refs.end_ts DESC" in compiled
    assert "LIMIT " in compiled
