# 12 — Infrastructure & Docker Setup

This document covers the full local and production-equivalent Docker stack for the
AMPS Queue Concurrency system: C4 architecture diagrams, AMPS server setup, Docker
Compose bring-up, and Grafana/Loki observability.

---

## Table of Contents

1. [C4 Level 1 — System Context](#1-c4-level-1--system-context)
2. [C4 Level 2 — Container Diagram](#2-c4-level-2--container-diagram)
3. [C4 Level 3 — Component Diagram (Spring Boot)](#3-c4-level-3--component-diagram-spring-boot)
4. [Infrastructure Topology](#4-infrastructure-topology)
5. [AMPS Server Setup](#5-amps-server-setup)
6. [PostgreSQL Setup](#6-postgresql-setup)
7. [Docker Compose — Quick Start](#7-docker-compose--quick-start)
8. [Observability Stack](#8-observability-stack)
9. [Grafana Dashboards](#9-grafana-dashboards)
10. [Loki Log Queries](#10-loki-log-queries)
11. [Production Considerations](#11-production-considerations)

---

## 1. C4 Level 1 — System Context

```
┌────────────────────────────────────────────────────────────────────┐
│                        System Context                              │
└────────────────────────────────────────────────────────────────────┘

 ┌─────────────────┐        publishes trades        ┌──────────────────────────────────────┐
 │  Trading System │ ──────────────────────────────▶ │                                      │
 │  (external)     │                                  │   AMPS Queue Concurrency System      │
 └─────────────────┘                                  │                                      │
                                                       │  Ingests, deduplicates, and         │
 ┌─────────────────┐        admin / monitoring         │  processes high-throughput AMPS     │
 │  Operations     │ ◀──────────────────────────────▶  │  queue messages at scale            │
 │  Team           │                                   │  using Java 21 virtual threads.     │
 └─────────────────┘                                   │                                     │
                                                        └──────────────────────────────────────┘
 ┌─────────────────┐        reads processed data
 │  Downstream     │ ◀─────────────────────────────── (via PostgreSQL / H2)
 │  Analytics      │
 └─────────────────┘
```

**Actors:**
| Actor | Interaction |
|---|---|
| Trading System | Publishes JSON messages to AMPS `/queue/trades` via TCP |
| Operations Team | Monitors AMPS admin UI, Grafana dashboards, health endpoints |
| Downstream Analytics | Reads `processed_messages` table for reporting |

---

## 2. C4 Level 2 — Container Diagram

```
┌──────────────────────────────────────────────────────────────────────────────────┐
│                           AMPS Queue Concurrency System                          │
│                                                                                  │
│  ┌──────────────────┐   tcp:9004    ┌───────────────────────────────────────┐   │
│  │   AMPS Server    │◀─────────────▶│  Spring Boot: message-publisher       │   │
│  │  (external)      │               │  Profile: message-publisher            │   │
│  │  172.21.12.69    │               │  Port: 8082                            │   │
│  │                  │               │  Publishes N msg/s via HAClient        │   │
│  │  Queue topics:   │               └───────────────────────────────────────┘   │
│  │  /queue/trades   │                                                            │
│  │  /queue/orders   │   tcp:9004    ┌───────────────────────────────────────┐   │
│  │                  │◀─────────────▶│  Spring Boot: single-subscriber       │   │
│  │  Admin:  8085    │               │  Profile: single-subscriber            │   │
│  └──────────────────┘               │  Port: 8084                            │   │
│                                     │  1 HAClient, Semaphore(100), VTs       │   │
│                                     └───────────────────────┬───────────────┘   │
│                                                             │ writes             │
│  ┌──────────────────┐   tcp:9004    ┌──────────────────┐   │  ┌──────────────┐  │
│  │   PostgreSQL     │               │ Spring Boot:      │   │  │     H2       │  │
│  │  (postgres:16)   │◀─────────────▶│ multi-jvm-sub    │   └─▶│  (embedded)  │  │
│  │                  │   jdbc:5432   │ Port: 8083        │      └──────────────┘  │
│  │ processed_       │               │ N HAClients/JVM   │                        │
│  │ messages table   │               └───────────────────┘                        │
│  └──────────────────┘                                                            │
│                                                                                  │
│  ┌──────────────────┐  scrapes :8080/actuator/prometheus                        │
│  │   Prometheus     │◀─────────────────────────────────────                     │
│  │  (port 9090)     │                                                            │
│  └────────┬─────────┘                                                            │
│           │ datasource                                                            │
│  ┌────────▼─────────┐  ┌────────────────┐                                       │
│  │    Grafana        │  │   Loki         │  ◀── Promtail (reads Docker logs)    │
│  │  (port 3000)     │◀─│  (port 3100)   │                                       │
│  └──────────────────┘  └────────────────┘                                       │
└──────────────────────────────────────────────────────────────────────────────────┘
```

**Container inventory:**
| Container | Image | Port(s) | Purpose |
|---|---|---|---|
| *(external)* | AMPS at `172.21.12.69` | 9004 (client), 8085 (admin) | AMPS message broker — not containerised |
| `postgres` | `postgres:16-alpine` | host:5433→5432 | Shared DB (multi-JVM profile) |
| `app-publisher` | `./Dockerfile` | 8082 | Publishes simulated messages |
| `app-single` | `./Dockerfile` | 8084 | Single-subscriber consumer |
| `app-multi` | `./Dockerfile` | 8081 | Multi-subscriber consumer |
| `app-multi-jvm` | `./Dockerfile` | 8083 | Multi-JVM consumer (PostgreSQL) |
| `prometheus` | `prom/prometheus:v2.51.0` | 9090 | Metrics collection |
| `grafana` | `grafana/grafana:10.4.0` | 3000 | Dashboards |
| `loki` | `grafana/loki:2.9.5` | 3100 | Log aggregation |
| `promtail` | `grafana/promtail:2.9.5` | — | Docker log shipper → Loki |

---

## 3. C4 Level 3 — Component Diagram (Spring Boot)

```
┌──────────────────────────────────────────────────────────────────────┐
│               Spring Boot Application Container                      │
│                                                                      │
│  ┌──────────────────────────────────────────────────────────────┐   │
│  │  Publisher Module  (@Profile("message-publisher"))           │   │
│  │                                                              │   │
│  │  PublisherConfig ──▶ HAClient (publisher)                    │   │
│  │  AmpsMessagePublisher ──▶ PublisherRateLimiter               │   │
│  │                       ──▶ MessagePayloadFactory              │   │
│  │                       ──▶ HAClient.publish(topic, payload)   │   │
│  │  PublisherMetrics ──▶ Micrometer /actuator/prometheus        │   │
│  └──────────────────────────────────────────────────────────────┘   │
│                                                                      │
│  ┌──────────────────────────────────────────────────────────────┐   │
│  │  Subscriber Module (@Profile("single-subscriber" etc.))      │   │
│  │                                                              │   │
│  │  [Single]  SingleAmpsSubscriber                             │   │
│  │              │  semaphore.acquire()                          │   │
│  │              ▼                                               │   │
│  │  [Multi]   MultiAmpsSubscriberPool (N platform threads)     │   │
│  │              │  per-subscriber semaphore.acquire()           │   │
│  │              ▼                                               │   │
│  │           MessageDispatchService                             │   │
│  │              │  executor.submit() → Virtual Thread           │   │
│  │              ▼                                               │   │
│  │           MessageProcessor (@Transactional)                  │   │
│  │              │  idempotency check (messageId → DB)           │   │
│  │              │  executeBusinessLogic(payload)                │   │
│  │              │  INSERT/UPDATE ProcessedMessage               │   │
│  │              ▼                                               │   │
│  │           ProcessedMessageRepository ──▶ HikariCP ──▶ DB    │   │
│  └──────────────────────────────────────────────────────────────┘   │
│                                                                      │
│  ┌─────────────────────┐  ┌─────────────────────────────────────┐   │
│  │  CommonConfig       │  │  AmpsHealthIndicator                │   │
│  │  ampsVirtualThread  │  │  /actuator/health → AMPS connected? │   │
│  │  Executor (shared)  │  └─────────────────────────────────────┘   │
│  └─────────────────────┘                                            │
└──────────────────────────────────────────────────────────────────────┘
```

---

## 4. Infrastructure Topology

### Single-JVM simulation (all-in-one)

```
[JVM 1]
  AmpsMessagePublisher (VT workers) ──publish──▶ AMPS /queue/trades
                                                          │
  SingleAmpsSubscriber ◀──deliver────────────────────────┘
    │ semaphore.acquire()
    │ executor.submit()
    ▼
  MessageProcessor (VT) ──▶ H2 DB
```

### Multi-JVM production layout

```
AMPS Server
  /queue/trades   ◀──────────────────── External trading system
       │
       ├──deliver──▶ JVM A (multi-jvm-subscriber)
       │               ├─ HAClient-A-sub-1 → VT pool → PostgreSQL
       │               └─ HAClient-A-sub-2 → VT pool → PostgreSQL
       │
       └──deliver──▶ JVM B (multi-jvm-subscriber)
                       ├─ HAClient-B-sub-1 → VT pool → PostgreSQL
                       └─ HAClient-B-sub-2 → VT pool → PostgreSQL

AMPS lease TTL ensures no message is lost if a JVM crashes mid-processing.
PostgreSQL UNIQUE(message_id) ensures no double-processing across JVMs.
```

---

## 5. AMPS Server Setup

### 5.1 External AMPS server

AMPS runs externally at `172.21.12.69`. All Docker app containers connect directly
to that IP — no AMPS container is started by this Compose stack.

| Endpoint | Address |
|---|---|
| Client TCP (HAClient) | `tcp://172.21.12.69:9004/amps/json` |
| Admin HTTP UI | `http://172.21.12.69:8085/amps/admin` |

To switch to a different AMPS host, set the `AMPS_SERVER_URI` environment variable
when starting the stack, or update `amps.server.uri` in `application.yaml`:

```bash
AMPS_SERVER_URI=tcp://other-host:9004/amps/json \
  docker compose -f docker/docker-compose.yml up -d app-publisher app-multi-jvm
```

> **Note — Dockerised AMPS:** `docker/amps/Dockerfile` and `docker/amps/config.xml`
> are kept for reference should you need to run AMPS in Docker. Build with:
> ```bash
> docker login   # 60East Docker Hub credentials required
> docker compose -f docker/docker-compose.yml build amps
> ```

### 5.2 AMPS configuration file (`docker/amps/config.xml`)

Key sections:

| Section | Purpose |
|---|---|
| `<Transports>` | TCP listener on 9004 (client), 8085 (admin HTTP) |
| `<Modules><Module>queue</Module>` | Enables queue semantics on `/queue/*` topics |
| `<SOW><Topic>` with `<Type>queue</Type>` | Declares `/queue/trades` as a persistent queue |
| `<Persistence><Journal>` | Journal file path; survives AMPS restarts |

### 5.3 Verifying AMPS is running

```bash
# Health check — TCP connect on port 9004
nc -zv 172.21.12.69 9004

# Admin REST API
curl http://172.21.12.69:8085/amps/admin

# List active clients
curl http://172.21.12.69:8085/amps/admin/clients | python -m json.tool

# List topic statistics
curl http://172.21.12.69:8085/amps/admin/topics | python -m json.tool

# Queue depth for /queue/trades
curl "http://172.21.12.69:8085/amps/admin/topics?topic=/queue/trades"
```

### 5.4 AMPS Admin UI — monitoring queue operations

Open `http://172.21.12.69:8085/amps/admin` in a browser.

**Key metrics to watch:**

| Metric | Admin UI Path | What it means |
|---|---|---|
| Queue depth | Topics → `/queue/trades` → `queued` | Messages waiting for a subscriber lease |
| Active leases | Topics → `/queue/trades` → `leased` | Messages out to subscribers (not yet ACK'd) |
| Subscriber count | Clients | Number of connected HAClients |
| Messages/s in | Topics → `publish_rate` | Publisher throughput |
| Messages/s out | Topics → `subscribe_rate` | Consumer throughput |
| ACK rate | Topics → `ack_rate` | Successful processing rate |
| Expired leases | Topics → `lease_expire` | Messages returned to queue (retry path) |

### 5.5 AMPS `amps-cmd` CLI (optional)

```bash
# If amps-cmd is installed locally or accessible on the AMPS host:
amps-cmd --server tcp://172.21.12.69:9004 topic-stats /queue/trades

# Subscribe to watch live messages (diagnostic)
amps-cmd --server tcp://172.21.12.69:9004 subscribe /queue/trades
```

### 5.6 Queue configuration tuning

The running AMPS server at `172.21.12.69` is configured independently.
`docker/amps/config.xml` documents the recommended settings for reference:

```xml
<!-- Lease timeout: how long AMPS holds a message for one subscriber before re-delivery -->
<!-- Default: 5000ms. Match to amps.queue.lease-timeout-ms in application.yaml -->
<LeaseTimeout>5000</LeaseTimeout>

<!-- Max queue depth before producers are back-pressured -->
<MaxQueuedMessages>1000000</MaxQueuedMessages>
```

---

## 6. PostgreSQL Setup

### 6.1 Schema initialisation

`docker/postgres/init.sql` runs automatically on first container start via
`/docker-entrypoint-initdb.d/`. It creates:

- `processed_messages` table with all columns
- `UNIQUE INDEX` on `message_id` — the idempotency key
- Status indexes for monitoring queries
- `CHECK` constraint on `status` column

### 6.2 Manual schema application (non-Docker)

```bash
psql -U amps_user -d ampsdb -f docker/postgres/init.sql
```

### 6.3 Useful monitoring queries

```sql
-- Queue depth by status
SELECT status, COUNT(*) FROM processed_messages GROUP BY status;

-- Throughput last 60 seconds
SELECT COUNT(*) FROM processed_messages
WHERE received_at > NOW() - INTERVAL '60 seconds'
  AND status = 'PROCESSED';

-- Per-JVM throughput (multi-JVM profile)
SELECT processed_by, COUNT(*), MAX(processed_at)
FROM processed_messages
WHERE received_at > NOW() - INTERVAL '5 minutes'
GROUP BY processed_by ORDER BY COUNT(*) DESC;

-- Poison messages (high retry count)
SELECT message_id, retry_count, last_error, last_attempt_at
FROM processed_messages
WHERE retry_count >= 2
ORDER BY retry_count DESC LIMIT 20;

-- Messages currently FAILED (will be retried on next AMPS delivery)
SELECT COUNT(*) FROM processed_messages WHERE status = 'FAILED';
```

---

## 7. Docker Compose — Quick Start

### 7.1 Prerequisites

- Docker Engine 24+ and Docker Compose v2
- AMPS JAR installed in local Maven repo (for building the app image)
- 4 GB RAM, 10 GB disk

### 7.2 Build the application image

```bash
# From project root — AMPS JAR must be installed in local Maven repo
docker compose -f docker/docker-compose.yml build app-publisher app-multi-jvm
```

### 7.3 Start the infrastructure only (postgres + monitoring)

```bash
# AMPS is external — only postgres and the observability stack need to start
docker compose -f docker/docker-compose.yml up -d \
  postgres prometheus grafana loki promtail
```

### 7.4 Start a simulation (publisher + multi-JVM subscriber)

```bash
docker compose -f docker/docker-compose.yml up -d \
  postgres prometheus grafana loki promtail \
  app-publisher app-multi-jvm

# Watch logs
docker compose -f docker/docker-compose.yml logs -f app-publisher app-multi-jvm
```

### 7.5 Scale multi-JVM subscriber horizontally

```bash
# Start additional subscriber JVMs (all share the same PostgreSQL)
docker compose -f docker/docker-compose.yml up -d --scale app-multi-jvm=3
```

### 7.6 Common commands

```bash
# View running services
docker compose -f docker/docker-compose.yml ps

# Tail logs for a service
docker compose -f docker/docker-compose.yml logs -f app-multi-jvm

# Stop everything, keep volumes
docker compose -f docker/docker-compose.yml down

# Stop and wipe all data (including postgres)
docker compose -f docker/docker-compose.yml down -v

# Rebuild and restart the app after a code change
docker compose -f docker/docker-compose.yml build app-multi-jvm
docker compose -f docker/docker-compose.yml up -d app-multi-jvm

# Verify AMPS is reachable from your machine
curl -s http://172.21.12.69:8085/amps/admin | head -5
```

### 7.7 Environment variable overrides

All `amps.*` properties map to environment variables via Spring's relaxed binding:

| Spring Property | Docker Env Var |
|---|---|
| `amps.server.uri` | `AMPS_SERVER_URI` |
| `amps.publisher.messages-per-second` | `AMPS_PUBLISHER_MESSAGES_PER_SECOND` |
| `amps.consumer.max-concurrency` | `AMPS_CONSUMER_MAX_CONCURRENCY` |
| `amps.consumer.subscriber-count` | `AMPS_CONSUMER_SUBSCRIBER_COUNT` |
| `spring.datasource.url` | `SPRING_DATASOURCE_URL` |
| `spring.profiles.active` | `SPRING_PROFILES_ACTIVE` |

---

## 8. Observability Stack

### 8.1 Architecture

```
Spring Boot App
  /actuator/prometheus  ──scrape──▶ Prometheus ──datasource──▶ Grafana
  stdout (JSON logs)    ──tail──▶   Promtail   ──push──▶       Loki ──datasource──▶ Grafana
```

### 8.2 Structured logging (JSON)

The `docker-logging` Spring profile activates JSON log format via `logback-spring.xml`.
All log lines include these fields parseable by Promtail/Loki:

| JSON Field | Description |
|---|---|
| `@timestamp` | ISO-8601 UTC timestamp |
| `level` | INFO / WARN / ERROR / DEBUG |
| `message` | Log message text |
| `logger` | Logger class name |
| `thread` | Thread name (e.g., `amps-vt-42`, `amps-subscriber-1`) |
| `messageId` | AMPS bookmark — set in MDC for every virtual thread |
| `topic` | AMPS topic — set in MDC |
| `subscriberIndex` | Which subscriber (1..N) handled this message |
| `publisherWorker` | Which publisher VT (1..N) sent this message |
| `service` | `amps-queue-concurrency` (static label) |

Enable JSON logging:
```bash
# Via Spring profile
SPRING_PROFILES_ACTIVE=single-subscriber,docker-logging

# Or via env var
LOGGING_FORMAT=json  # only if you add env-based switching to logback-spring.xml
```

### 8.3 Tracing a message end-to-end

Every message carries its AMPS bookmark as `messageId` in MDC. To trace a single message:

```logql
# In Grafana Explore → Loki
{service="amps-queue-concurrency"} | json | messageId = "0000000000000000000000001|1|42"
```

This shows the full lifecycle:
1. `AmpsMessagePublisher` → `Published seq=42`
2. `MessageDispatchService` → `ACK result=OK messageId=...`
3. `MessageProcessor` → `OK messageId=... topic=/queue/trades`

---

## 9. Grafana Dashboards

Open `http://localhost:3000` (credentials: `admin` / `admin`).

The pre-provisioned **AMPS Queue Concurrency** dashboard (`docker/grafana/dashboards/amps-queue-dashboard.json`) includes:

| Panel | Metric | Query |
|---|---|---|
| Messages Published (total) | Cumulative counter | `sum(amps_publisher_messages_published_total)` |
| Publish Errors | Error counter | `sum(amps_publisher_messages_errors_total)` |
| Active Publisher Workers | Gauge | `amps_publisher_active_workers` |
| Publish Rate (msg/s) | Rate over 30s | `rate(amps_publisher_messages_published_total[30s])` |
| Publish Latency p50/p95/p99 | Histogram | `histogram_quantile(0.95, rate(...bucket[1m]))` |
| JVM Virtual Threads (live) | JVM threads | `jvm_threads_live_threads` |
| JVM Heap Used | Memory | `jvm_memory_used_bytes{area="heap"}` |
| Application Logs | Loki stream | `{service=~"app.*"} \| json \| level != "DEBUG"` |

### Additional useful Prometheus queries

```promql
# HikariCP active connections (DB pool pressure)
hikaricp_connections_active{pool="HikariPool-SingleSub"}

# JVM GC pause duration
rate(jvm_gc_pause_seconds_sum[1m])

# HTTP error rate on actuator endpoints
rate(http_server_requests_seconds_count{status=~"5.."}[1m])
```

---

## 10. Loki Log Queries

### Find all errors in last 15 minutes

```logql
{service="amps-queue-concurrency"} | json | level = "ERROR" | __error__ = ""
```

### Find all DISCARD events

```logql
{service="amps-queue-concurrency"} | json
  | message =~ "DISCARD.*"
```

### Find slow messages (long lease time implies retries)

```logql
{service="amps-queue-concurrency"} | json
  | message =~ "FAIL retry.*"
  | line_format "retry={{.message}} messageId={{.messageId}}"
```

### Per-subscriber message distribution

```logql
sum by (subscriberIndex) (
  count_over_time({service="amps-queue-concurrency"} | json | message =~ "OK.*" [5m])
)
```

### Publisher throughput from logs

```logql
{service="amps-queue-concurrency", spring_profile="message-publisher"} | json
  | message =~ "Published seq=.*"
  | __error__ = ""
```

---

## 11. Production Considerations

### AMPS server

| Concern | Recommendation |
|---|---|
| High availability | Deploy AMPS in HA pair with primary/replica replication |
| Persistence | Mount `/amps/sow` on a fast SSD; use XFS or ext4 |
| Lease timeout | Match to `(maxRetries + 1) × avg_processing_time × 2` |
| Client naming | `{app}-{hostname}-sub-{index}` — already implemented |
| Security | Enable AMPS file-based auth; use TLS transport (`tcps://`) |
| Capacity | 1M msg/s per AMPS node (single core); scale horizontally |

### Application

| Concern | Recommendation |
|---|---|
| JVM | Eclipse Temurin 21 + ZGC (`-XX:+UseZGC`) for low pause |
| VT count | Semaphore permits = DB pool size × 5 (rule of thumb) |
| HikariCP | `maximum-pool-size` = `max-concurrency / 5` minimum |
| Bookmark store | Mount `/home/amps/.amps/bookmarks` as a persistent Docker volume |
| Graceful shutdown | `spring.lifecycle.timeout-per-shutdown-phase: 30s` |

### PostgreSQL (multi-JVM)

| Concern | Recommendation |
|---|---|
| Idempotency | UNIQUE constraint on `message_id` is the only cross-JVM guard |
| Index maintenance | Run `VACUUM ANALYZE processed_messages` weekly |
| Archival | Move PROCESSED rows older than 30 days to a separate table |
| Connection pool | `max-pool-size` per JVM × JVM count ≤ PostgreSQL `max_connections` |

### Kubernetes

For K8s deployment of the multi-JVM profile, add:

```yaml
env:
  - name: AMPS_CLIENT_NAME
    value: "amps-queue-concurrency"   # base name; hostname suffix added automatically
  - name: AMPS_CONSUMER_BOOKMARK_DIR
    value: "/amps/bookmarks"

volumeMounts:
  - name: bookmark-store
    mountPath: /amps/bookmarks

volumes:
  - name: bookmark-store
    persistentVolumeClaim:
      claimName: amps-bookmark-pvc   # ReadWriteOnce per Pod
```

> Each Pod gets its own PVC for bookmark isolation — matching the hostname-qualified
> client naming in `MultiJvmSubscriberConfig`.
