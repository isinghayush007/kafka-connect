# Kafka Connect API Reference

## Overview

Kafka Connect is a scalable and reliable streaming data integration framework built on top of
Apache Kafka. It simplifies moving data between Kafka and other systems (databases, key-value
stores, search indexes, file systems) using **connectors** — pre-built or custom plugins that
handle the data transfer logic.

- **Source connectors** read data from an external system and publish it to Kafka topics.
- **Sink connectors** consume data from Kafka topics and write it to an external system.

This reference documents the REST API endpoints used to manage the connectors in this workspace.

## Base URLs

| Role | Base URL |
| --- | --- |
| Source connector worker | `http://localhost:8083` |
| Sink connector worker | `http://localhost:8084` |

The source connector endpoints use port `8083`; the sink connector deployment uses port `8084`.
Every endpoint below other than *Deploy Sink Connector* is shown against `8083`, but the Connect
REST API is identical on both workers — swap the port to manage connectors on the other one.

## Conventions

- `{name}` is the connector name, e.g. `mysql_source_user_profiles`.
- Request bodies are `application/json`.
- Passwords are shown as `<your-password>`. Substitute your own local values.
- On **deploy**, the config is nested under a `"config"` key. On **config update**, the body is a
  flat object with no wrapper. This asymmetry is part of the Connect API, not a typo.

## Endpoints

| # | Name | Method | Path |
| --- | --- | --- | --- |
| 1 | Deploy source connector | `POST` | `:8083/connectors` |
| 2 | Deploy sink connector | `POST` | `:8084/connectors` |
| 3 | List connectors | `GET` | `:8083/connectors` |
| 4 | Connector details | `GET` | `:8083/connectors/{name}` |
| 5 | Connector config only | `GET` | `:8083/connectors/{name}/config` |
| 6 | Connector status | `GET` | `:8083/connectors/{name}/status` |
| 7 | Pause connector | `PUT` | `:8083/connectors/{name}/pause` |
| 8 | Resume connector | `PUT` | `:8083/connectors/{name}/resume` |
| 9 | Update connector config | `PUT` | `:8083/connectors/{name}/config` |
| 10 | Delete connector | `DELETE` | `:8083/connectors/{name}` |

---

### 1. Deploy source connector

```
POST http://localhost:8083/connectors
```

Deploys a JDBC source connector that reads from a MySQL table and publishes to Kafka topics.

**Request**

```json
{
  "name": "mysql_source_user_profiles",
  "config": {
    "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
    "connection.url": "jdbc:mysql://localhost:3306/users",
    "connection.user": "root",
    "connection.password": "<your-password>",
    "table.whitelist": "users.user_profiles",
    "mode": "timestamp+incrementing",
    "timestamp.column.name": "last_updated",
    "incrementing.column.name": "id",
    "topic.prefix": "users_"
  }
}
```

**Response** — `201 Created`

```json
{
  "name": "mysql_source_user_profiles",
  "config": {
    "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
    "connection.url": "jdbc:mysql://localhost:3306/users",
    "connection.user": "root",
    "table.whitelist": "users.user_profiles",
    "mode": "timestamp+incrementing",
    "timestamp.column.name": "last_updated",
    "incrementing.column.name": "id",
    "topic.prefix": "users_",
    "name": "mysql_source_user_profiles"
  },
  "tasks": [],
  "type": "source"
}
```

`tasks` is empty in the deploy response — tasks are assigned asynchronously. Poll
[`/status`](#6-connector-status) to confirm the connector is actually running.

---

### 2. Deploy sink connector

```
POST http://localhost:8084/connectors
```

Deploys a JDBC sink connector that consumes a Kafka topic and upserts into a PostgreSQL table.

**Request**

```json
{
  "name": "postgresql-jdbc-sink-connector",
  "config": {
    "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
    "tasks.max": "1",
    "topics": "users_user_profiles",
    "connection.url": "jdbc:postgresql://localhost:5432/users?user=postgres&password=<your-password>",
    "insert.mode": "upsert",
    "pk.mode": "record_value",
    "pk.fields": "id",
    "table.name.format": "user_profiles"
  }
}
```

**Response** — `201 Created`

```json
{
  "name": "postgresql-jdbc-sink-connector",
  "config": {
    "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
    "tasks.max": "1",
    "topics": "users_user_profiles",
    "insert.mode": "upsert",
    "pk.mode": "record_value",
    "pk.fields": "id",
    "table.name.format": "user_profiles",
    "name": "postgresql-jdbc-sink-connector"
  },
  "tasks": [],
  "type": "sink"
}
```

---

### 3. List connectors

```
GET http://localhost:8083/connectors
```

Returns the names of all registered connectors on the worker. No request body.

> Presence in this list means *registered*, not *running*. Use `/status` to check runtime state.

**Response** — `200 OK`

```json
[
  "mysql_source_user_profiles",
  "postgresql-jdbc-sink-connector"
]
```

---

### 4. Connector details

```
GET http://localhost:8083/connectors/{name}
```

Returns the full connector record: name, config map, assigned tasks, and type. No request body.

**Response** — `200 OK`

```json
{
  "name": "mysql_source_user_profiles",
  "config": {
    "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
    "connection.url": "jdbc:mysql://localhost:3306/users",
    "connection.user": "root",
    "table.whitelist": "users.user_profiles",
    "mode": "timestamp+incrementing",
    "timestamp.column.name": "last_updated",
    "incrementing.column.name": "id",
    "topic.prefix": "users_",
    "name": "mysql_source_user_profiles"
  },
  "tasks": [
    { "connector": "mysql_source_user_profiles", "task": 0 }
  ],
  "type": "source"
}
```

---

### 5. Connector config only

```
GET http://localhost:8083/connectors/{name}/config
```

Returns just the flat config map, without task or type metadata. Useful for inspecting or
diffing settings. No request body.

**Response** — `200 OK`

```json
{
  "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
  "connection.url": "jdbc:mysql://localhost:3306/users",
  "connection.user": "root",
  "table.whitelist": "users.user_profiles",
  "mode": "timestamp+incrementing",
  "timestamp.column.name": "last_updated",
  "incrementing.column.name": "id",
  "topic.prefix": "users_",
  "name": "mysql_source_user_profiles"
}
```

---

### 6. Connector status

```
GET http://localhost:8083/connectors/{name}/status
```

Returns the runtime state of the connector and each of its tasks. No request body.

**Response** — `200 OK`

```json
{
  "name": "mysql_source_user_profiles",
  "connector": {
    "state": "RUNNING",
    "worker_id": "localhost:8083"
  },
  "tasks": [
    { "id": 0, "state": "RUNNING", "worker_id": "localhost:8083" }
  ],
  "type": "source"
}
```

**States:** `RUNNING`, `PAUSED`, `FAILED`, `UNASSIGNED`.

A connector can report `RUNNING` while an individual task is `FAILED` — always check the `tasks`
array, not just the top-level `connector.state`. A failed task includes a `trace` field with the
stack trace.

---

### 7. Pause connector

```
PUT http://localhost:8083/connectors/{name}/pause
```

Pauses the connector and all its tasks. It stays registered but stops processing records.
No request body.

**Response** — `202 Accepted`, no body. Applied asynchronously; the connector transitions to
`PAUSED`.

---

### 8. Resume connector

```
PUT http://localhost:8083/connectors/{name}/resume
```

Resumes a paused connector and its tasks, continuing from the last committed offset.
No request body.

**Response** — `202 Accepted`, no body. Applied asynchronously; the connector transitions back to
`RUNNING`.

---

### 9. Update connector config

```
PUT http://localhost:8083/connectors/{name}/config
```

Updates an existing connector's configuration. The connector restarts with the new config on
success.

**Request** — a *flat* object, not wrapped in `"config"`. Send the complete config; omitted keys
are dropped, not merged. This example changes `topic.prefix` to `app_users_`:

```json
{
  "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
  "connection.url": "jdbc:mysql://localhost:3306/users",
  "connection.user": "root",
  "connection.password": "<your-password>",
  "table.whitelist": "users.user_profiles",
  "mode": "timestamp+incrementing",
  "timestamp.column.name": "last_updated",
  "incrementing.column.name": "id",
  "topic.prefix": "app_users_",
  "name": "mysql_source_user_profiles"
}
```

**Response** — `200 OK`

```json
{
  "name": "mysql_source_user_profiles",
  "config": {
    "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
    "connection.url": "jdbc:mysql://localhost:3306/users",
    "connection.user": "root",
    "table.whitelist": "users.user_profiles",
    "mode": "timestamp+incrementing",
    "timestamp.column.name": "last_updated",
    "incrementing.column.name": "id",
    "topic.prefix": "app_users_",
    "name": "mysql_source_user_profiles"
  },
  "tasks": [
    { "connector": "mysql_source_user_profiles", "task": 0 }
  ],
  "type": "source"
}
```

---

### 10. Delete connector

```
DELETE http://localhost:8083/connectors/{name}
```

Deletes the connector and permanently stops all its tasks. No request body.

**Response** — `204 No Content`, no body.

> **Irreversible.** The connector config is removed and cannot be recovered. Committed offsets in
> the Connect offset topic survive, so redeploying under the same name resumes from where it left
> off rather than re-reading from the start.

---

## Error responses

| Status | Meaning |
| --- | --- |
| `400 Bad Request` | Malformed JSON, or config failed validation (missing/invalid properties) |
| `404 Not Found` | No connector with the given name |
| `409 Conflict` | A connector with that name already exists (on deploy), or a rebalance is in progress |
| `500 Internal Server Error` | The Connect worker hit an unexpected error |
