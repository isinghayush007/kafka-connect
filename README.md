# kafka-connect

A local Kafka Connect playground: a 3-broker KRaft cluster in Docker plus JDBC connectors that
stream a MySQL table into Kafka and sink it into PostgreSQL.

## Layout

| Path | What it is |
| --- | --- |
| `docker-compose/` | 3-broker Kafka cluster only. Run Connect yourself against it (distributed mode). |
| `standalone/` | Same cluster **plus** a `cp-kafka-connect` container running both connectors in standalone mode. |
| `plugins/` | The Confluent JDBC connector (10.8.4) and its JDBC drivers. |
| `dockerfile-kafka-connect` | Builds a Connect image with the JDBC plugin and MySQL/PostgreSQL drivers baked in. |
| `environment.env` | Worker config for distributed mode (`CONNECT_*` vars). |
| `docs/kafka-connect-api.md` | REST API reference for managing the connectors. |

## Quick start

Standalone — brokers and connectors in one go:

```bash
cd standalone
docker compose --env-file environment.env up -d
curl http://localhost:8083/connectors
```

Brokers only:

```bash
cd docker-compose
docker compose --env-file environment.env up -d
```

Broker data is written to `volumes/` in each directory, which is gitignored — a fresh clone starts
with an empty cluster.

## Before you run it

The connector properties in `standalone/` ship with `password=<your-password>`. Replace it with
your actual local MySQL/PostgreSQL password in both files:

- `standalone/mysql-jdbc-connector-for-docker.properties`
- `standalone/postgresql-jdbc-sink-connector-for-docker.properties`

The source connector expects a `users.user_profiles` table with an `id` column and a
`last_updated` timestamp; the sink upserts into `user_profiles` on `id`.

## REST API

See [`docs/kafka-connect-api.md`](docs/kafka-connect-api.md) for all ten endpoints — deploy, list,
inspect config, check status, pause, resume, update, and delete — with request and response
examples.
