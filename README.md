# webhook-relay

Pet project: a webhook delivery service written in Go. Accepts events via HTTP, stores them in PostgreSQL, publishes to Kafka through the **Transactional Outbox** pattern, and (WIP) delivers payloads to subscriber URLs.

**Flow:** `POST /events` → `events` + `outbox` (one transaction) → outbox worker → Kafka → delivery worker → `POST target_url`

---

## Status

| Part | Status |
|------|--------|
| HTTP API (subscriptions, events) | done |
| PostgreSQL + migrations | done |
| Outbox publisher → Kafka | done |
| Delivery consumer → HTTP webhook | **WIP** |
| `GET /events/{id}`, retries, delivery logs | planned |

---

## Stack

- **Go** — `net/http`, chi router, `database/sql`
- **PostgreSQL** — events, subscriptions, outbox, deliveries
- **Apache Kafka** — `segmentio/kafka-go`
- **Docker Compose** — Postgres, Kafka, Kafka UI

---

## Architecture

```text
Client
  │ POST /events
  ▼
API (cmd/api)
  ├─► PostgreSQL   events + outbox (transaction)
  └─► Outbox worker (goroutine)
        └─► Kafka: webhook-relay

Delivery worker (WIP)
  ├─ consume webhook-relay
  ├─ load payload from events
  ├─ POST subscription.target_url
  └─ update events.status, deliveries
```

### Project layout

```text
app/
├── cmd/api/              entrypoint
├── internal/
│   ├── config/           env config
│   ├── kernel/           composition root, lifecycle
│   ├── handlers/         HTTP controllers + DTO
│   ├── service/          business logic + workers
│   ├── repository/       database access
│   ├── integration/      Kafka producers
│   ├── server/           router, http.Server
│   └── model/            domain types
├── migrations/
docker-compose.yml
```

### Layers

```text
HTTP Request → Handler → Service → Repository → PostgreSQL
                              ↓
                         Integration (Kafka)
```

---

## Quick start

### 1. Infrastructure

```bash
make up
make topics   # optional: list Kafka topics
```

Services:

| Service | URL |
|---------|-----|
| API | http://localhost:8080 |
| Kafka UI | http://localhost:8090 |
| PostgreSQL | localhost:5432 |

### 2. Environment

Copy and fill env file from project root:

```bash
cp .env.example .env
```

Run API from `app/` directory (config loads `../.env`):

```bash
cd app
go run ./cmd/api
```

### 3. Example requests

Create a subscription:

```bash
curl -s -X POST http://localhost:8080/subscriptions \
  -H "Content-Type: application/json" \
  -d '{"name":"demo","targetUrl":"https://httpbin.org/post"}'
```

Send an event:

```bash
curl -s -X POST http://localhost:8080/events \
  -H "Content-Type: application/json" \
  -d '{"subscription_id":1,"event_type":"user.created","payload":{"user_id":10}}'
```

List subscriptions:

```bash
curl -s http://localhost:8080/subscriptions
```

---

## API

| Method | Path | Description |
|--------|------|-------------|
| POST | `/subscriptions` | Create subscriber `{name, targetUrl}` |
| GET | `/subscriptions` | List subscribers |
| POST | `/events` | Create event `{subscription_id, event_type, payload}` |

---

## Kafka topics

| Topic | Purpose |
|-------|---------|
| `webhook-relay` | Events ready for delivery |
| `events.pending` | Reserved for future use |

Created automatically by `kafka-init` service in Docker Compose.

---

## Makefile

```bash
make up            # start all services
make down          # stop all services
make logs          # follow logs
make topics        # list Kafka topics
make postgres-only # start only PostgreSQL
```

---

## Patterns used

- **Transactional Outbox** — event and outbox row in one DB transaction; background worker publishes to Kafka
- **Claim with lease** — `FOR UPDATE SKIP LOCKED` + `locked_until` for concurrent publishers
- **At-least-once delivery** — Kafka + idempotent consumer (planned)

---

## Roadmap

- [ ] `DeliveryService` — Kafka consumer + HTTP delivery
- [ ] `GET /events/{id}` — status and delivery history
- [ ] Retry + `deliveries` table
- [ ] Separate `cmd/worker` binary
- [ ] Tests, CI, health endpoint

---
