CREATE TABLE IF NOT EXISTS crypto_market_stats
(
    symbol LowCardinality(String),

    is_closed Bool,

    best_sell Float64,
    best_buy Float64,

    volume_src Float64,
    volume_dst Float64,

    latest Float64,
    mark Float64,

    day_low Float64,
    day_high Float64,

    day_open Float64,
    day_close Float64,

    day_change Float64,

    offset UInt64,

    created_at DateTime DEFAULT now()
)
ENGINE = MergeTree()
ORDER BY (symbol, created_at, offset)
SETTINGS index_granularity = 8192;


CREATE TABLE IF NOT EXISTS market_stats
(
    symbol String,

    isClosed Bool,

    bestSell Float64,
    bestBuy Float64,

    volumeSrc Float64,
    volumeDst Float64,

    latest Float64,
    mark Float64,

    dayLow Float64,
    dayHigh Float64,

    dayOpen Float64,
    dayClose Float64,

    dayChange Float64,

    offset UInt64
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list = 'nobitex.market-stats',
    kafka_group_name = 'market_stats_group',
    kafka_format = 'AvroConfluent',
    format_avro_schema_registry_url = 'http://schema-registry:8081',
    kafka_num_consumers = 1,
    stream_flush_interval_ms = 1000;


CREATE MATERIALIZED VIEW IF NOT EXISTS crypto_market_stats_mv
TO crypto_market_stats AS

SELECT
    symbol,

    isClosed AS is_closed,

    bestSell AS best_sell,
    bestBuy AS best_buy,

    volumeSrc AS volume_src,
    volumeDst AS volume_dst,

    latest,
    mark,

    dayLow AS day_low,
    dayHigh AS day_high,

    dayOpen AS day_open,
    dayClose AS day_close,

    dayChange AS day_change,

    offset,

    now() AS created_at

FROM market_stats;
