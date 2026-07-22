# Binance Futures Metrics Data Design

Status: implementation plan for the canonical BTC backfill and live refresh path.

## Objective

Create a dedicated `futures_metrics` market-data type for Binance USD-M five-minute
open-interest, positioning, and taker-flow observations. Historical archive import
and live REST refresh must produce the same point-in-time-safe row contract.

Legacy `open_interest` datasets remain unchanged for existing engines. New supervised
engines that consume the full Binance metrics surface must declare
`data_type=futures_metrics` explicitly.

## Canonical Raw Row

Dataset id: `btc-binance-futures_metrics-raw-5m`

Storage:

```text
.data/market-data/origin=raw/source=binance/type=futures_metrics/asset=BTC/timeframe=5m
```

Schema version: `binance-futures-metrics.v1`

```text
timestamp                                UTC interval start
interval_end                             timestamp + 5 minutes
available_at                             earliest causal availability; interval_end
symbol                                   Binance USD-M symbol
sum_open_interest                        contract-denominated open interest
sum_open_interest_value                  quote-denominated open-interest value
top_trader_account_long_short_ratio      top-trader long/short account-count ratio
top_trader_position_long_short_ratio     top-trader long/short position-size ratio
global_account_long_short_ratio          all-account long/short account-count ratio
taker_buy_sell_volume_ratio              taker buy volume / taker sell volume
complete                                 all six metric values are present
confirm                                  1 only for complete rows
ingest_source                            archive or rest
```

`CMCCirculatingSupply` is excluded because the live OI endpoint exposes it but the
archive does not. It cannot be a continuous supervised feature without another
historical source.

## Timestamp Contract

Binance archive `create_time` is the canonical interval start. For an interval
`[T, T+5m)`:

- Open-interest statistics are read from the REST record timestamped `T+5m`.
- Top-trader account ratios are read from the REST record timestamped `T+5m`.
- Top-trader position ratios are read from the REST record timestamped `T+5m`.
- Global account ratios are read from the REST record timestamped `T+5m`.
- Taker buy/sell volume is read from the REST record timestamped `T`.
- The joined canonical row is available at `T+5m`.

No row may be forward-filled. Live refresh appends only the contiguous complete prefix
after the registered end timestamp. A missing endpoint observation stops the append so
the next refresh retries that interval.

## Source Mapping

```text
Archive count_toptrader_long_short_ratio
  -> top-trader-long-short-ratio-accounts.longShortRatio

Archive sum_toptrader_long_short_ratio
  -> top-trader-long-short-ratio-positions.longShortRatio

Archive count_long_short_ratio
  -> long-short-ratio.longShortRatio

Archive sum_taker_long_short_vol_ratio
  -> taker-buy-sell-volume.buySellRatio
```

Archive ratios retain their source precision in raw Parquet. Model-facing transforms
round ratios to four decimal places so historical training and live inference see the
same precision supported by Binance REST.

## Backfill And Reconciliation

1. Download daily metrics archives from `2023-01-01` through the latest published UTC day.
2. Normalize, sort, and deduplicate by interval-start timestamp.
3. Write monthly Parquet shards and register the raw dataset.
4. Fetch the short unpublished tail from the five REST statistics endpoints.
5. Periodically re-import newly published daily archives; archive rows replace provisional
   REST rows at matching timestamps.
6. Record real Binance source gaps without imputation. Consumers use temporal coverage and
   completeness masks rather than synthetic values.

## Consumer Migration

`btc_multires_opportunity_v1` moves from `open_interest` to `futures_metrics`. Its model
channel names and weights remain stable; only the canonical row-to-model mapping changes.
Other engines continue to consume legacy `open_interest` until migrated deliberately.
