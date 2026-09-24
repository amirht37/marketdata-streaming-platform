CREATE TABLE IF NOT EXISTS crypto_db.orderbook
(
    `symbol` String,
    `lastUpdate` UInt64,
    `lastTradePrice` Float64,
    `bids` Array(
        Tuple(
            price Float64,
            amount Float64
        )
    ),
    `asks` Array(
        Tuple(
            price Float64,
            amount Float64
        )
    ),
    `offset` UInt64
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list = 'nobitex.orderbook',
    kafka_group_name = 'orderbook_group',
    kafka_format = 'AvroConfluent',
    format_avro_schema_registry_url = 'http://schema-registry:8081',
    kafka_num_consumers = 1;


CREATE TABLE IF NOT EXISTS crypto_db.crypto_orderbook
(
    `symbol` LowCardinality(String),
    `price` Float64,
    `amount` Float64,
    `side` LowCardinality(String),
    `event_ts` DateTime64(3, 'UTC'),
    `last_trade_price` Float64,
    `offset` UInt64,
    `kafka_created_at` DateTime64(3, 'UTC'),
    `created_at` DateTime DEFAULT now()
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_ts)
ORDER BY (symbol, event_ts, side, price, offset)
SETTINGS index_granularity = 8192;


CREATE MATERIALIZED VIEW IF NOT EXISTS crypto_db.crypto_orderbook_mv
TO crypto_db.crypto_orderbook
(
    `symbol` String,
    `price` Float64,
    `amount` Float64,
    `side` String,
    `event_ts` DateTime64(3, 'UTC'),
    `last_trade_price` Float64,
    `offset` UInt64,
    `kafka_created_at` DateTime64(3, 'UTC')
)
AS
SELECT
    symbol,
    level.1 AS price,
    level.2 AS amount,
    level.3 AS side,
    toDateTime64(lastUpdate / 1000., 3, 'UTC') AS event_ts,
    lastTradePrice AS last_trade_price,
    offset,
    toDateTime64(_timestamp, 3, 'UTC') AS kafka_created_at
FROM crypto_db.orderbook
ARRAY JOIN
    arrayConcat(
        arrayMap(x -> (x.1, x.2, 'bid'), bids),
        arrayMap(x -> (x.1, x.2, 'ask'), asks)
    ) AS level;