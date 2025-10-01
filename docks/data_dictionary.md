# Bronze: prices_eod
Columns: date, ticker, open, high, low, close, adjClose, volume, source, ingest_ts_utc, ingest_run_id
Partitioning: bronze/equities/tiingo/prices_eod/date=YYYY-MM-DD/part-<TICKER>.parquet