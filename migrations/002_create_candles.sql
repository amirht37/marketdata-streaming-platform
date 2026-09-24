CREATE TABLE IF NOT EXISTS crypto_candles
(
    symbol LowCardinality(String),

    resolution LowCardinality(String),

    event_ts DateTime64(3, 'UTC'),

    open Float64,
    high Float64,
    low Float64,
    close Float64,

    volume Float64,

    offset UInt64,

    created_at DateTime DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_ts)
ORDER BY (symbol, resolution, event_ts, offset)
SETTINGS index_granularity = 8192;


CREATE TABLE IF NOT EXISTS candles
(
    symbol String,

    resolution String,

    ts UInt64,

    open Float64,
    high Float64,
    low Float64,
    close Float64,

    volume Float64,

    offset UInt64
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list = 'nobitex.candles',
    kafka_group_name = 'candles_group',
    kafka_format = 'AvroConfluent',
    format_avro_schema_registry_url = 'http://schema-registry:8081',
    kafka_num_consumers = 1,
    stream_flush_interval_ms = 1000;


CREATE MATERIALIZED VIEW IF NOT EXISTS crypto_candles_mv
TO crypto_candles AS

SELECT
    symbol,

    resolution,

    toDateTime64(ts, 3, 'UTC') AS event_ts,

    open,
    high,
    low,
    close,

    volume,

    offset,

    now() AS created_at

FROM candles;
