CREATE TABLE IF NOT EXISTS crypto_trades
(
    symbol LowCardinality(String),
    price Float64,
    volume Float64,
    side LowCardinality(String),
    event_ts DateTime64(3, 'UTC'),
    offset UInt64,
    kafka_created_at DateTime64(3, 'UTC'),
    created_at DateTime DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_ts)
ORDER BY (symbol, event_ts, offset)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS trades
(
    symbol String,
    price Float64,
    volume Float64,
    side String,
    ts UInt64,
    offset UInt64
)
ENGINE = Kafka
SETTINGS 
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list = 'nobitex.trades',
    kafka_group_name = 'trades_group',
    kafka_format = 'AvroConfluent',
    format_avro_schema_registry_url = 'http://schema-registry:8081',
    kafka_num_consumers = 1,
    stream_flush_interval_ms = 1000;

CREATE MATERIALIZED VIEW IF NOT EXISTS crypto_trades_mv 
TO crypto_trades AS
SELECT
    symbol,
    price,
    volume,
    side,
    toDateTime64(ts / 1000.0, 3, 'UTC') AS event_ts,
    offset,
    _timestamp AS kafka_created_at
FROM crypto_db.trades;

