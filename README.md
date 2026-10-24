# Market Data Streaming Platform

A real-time crypto market data pipeline. It ingests live order books, trades, candles and market stats from the Nobitex WebSocket API, serialises them as Avro, streams them through Kafka, and stores/serves them via Redis and ClickHouse, with full monitoring (Prometheus, Grafana, Loki).

## Architecture

```
Nobitex WebSocket
      |
  producers/  (async Python: parse -> Avro serialise -> Kafka)
      |
   Kafka (KRaft) + Schema Registry
      |-------------------------------|
 consumers/orderbook            consumers/trades
      |                               |
    Redis (hash per symbol)       Redis (streams)

 Kafka Connect  ->  sinks (Redis sink config in connectors/)
 migrator/      ->  creates ClickHouse tables from migrations/*.sql

 Monitoring: Prometheus + Grafana + Loki/Promtail + cAdvisor + node_exporter
 Storage:    PostgreSQL, ClickHouse, MinIO (+ Iceberg REST catalog)
```

Kafka topics: `nobitex.orderbook`, `nobitex.trades`, `nobitex.candles`, `nobitex.market-stats`.
Avro schemas live in `schemas/`.

## Prerequisites

- Docker and Docker Compose v2
- ~8 GB free RAM (the stack runs ~19 containers)

## Quick start

```bash
git clone <your-repo-url>
cd marketdata-streaming-platform

# 1. Create your local config and set real values
cp .env.example .env
nano .env            # at minimum, replace every "change_me"

# 2. Start everything
docker compose up -d --build

# 3. Check status / logs
docker compose ps
docker compose logs -f crypto-producer
```

If a required variable is missing, `docker compose` stops immediately and tells you which one.

## Environment variables

All secrets and credentials come from `.env` (never committed). See `.env.example` for the full list.

| Variable | Used by | Notes |
|---|---|---|
| `POSTGRES_USER` / `POSTGRES_PASSWORD` / `POSTGRES_DB` | PostgreSQL, consumers, Alembic | password is **required** |
| `POSTGRES_HOST` / `POSTGRES_PORT` | consumers, Alembic | default `postgres` / `5432` |
| `CLICKHOUSE_DB` / `CLICKHOUSE_USER` / `CLICKHOUSE_PASSWORD` | ClickHouse, migrator | password is **required** |
| `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD` | MinIO, Kafka Connect, Iceberg | **required** |
| `AWS_REGION` | Kafka Connect, Iceberg | default `us-east-1` |
| `GRAFANA_ADMIN_USER` / `GRAFANA_ADMIN_PASSWORD` | Grafana | password is **required** |
| `REDIS_HOST` / `REDIS_PORT` | consumers | default `redis` / `6379` |

## Service endpoints (default ports)

| Service | URL |
|---|---|
| Kafka UI | http://localhost:8090 |
| Schema Registry | http://localhost:8081 |
| Grafana | http://localhost:3001 |
| Prometheus | http://localhost:9090 |
| MinIO console | http://localhost:9003 |
| Kafka Connect REST | http://localhost:8083 |
| ClickHouse HTTP | http://localhost:8123 |
| Loki | http://localhost:3100 |
| Producer metrics | http://localhost:9200 |

## Project layout

```
producers/     WebSocket -> Kafka producer (parser, serializer, sender, ws_manager)
consumers/     Kafka -> Redis consumers (orderbook, trades) + shared helpers
migrator/      Runs ClickHouse SQL migrations on start
migrations/    ClickHouse DDL (trades, candles, market stats, orderbook)
schemas/       Avro schemas
connectors/    Kafka Connect connector configs
prometheus/ loki/ promtail/ provisioning/ monitoring/   observability config
```

## Known issues / TODO

- `kafka-connect` has `build: ./connectors`, but `connectors/` has no `Dockerfile`. Add one (installing the Redis sink plugin and the JMX exporter jar) or remove the `build:` line to use the stock image.
- `tests/` currently only contains a scratch script; no automated tests yet.

## Security notes

- Never commit `.env`. Only `.env.example` (placeholders) belongs in git.
- Change all default/dev credentials before exposing any port beyond your own machine.
