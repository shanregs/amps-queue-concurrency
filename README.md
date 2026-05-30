# AMPS Queue Concurrency

Enterprise-grade AMPS queue consumer and simulation publisher built on **Spring Boot 3.5 + Java 21 virtual threads**.

Four deployment topologies — selectable via Spring profile at runtime with no code changes.

---

## Table of Contents

- [Overview](#overview)
- [C4 Architecture Diagrams](#c4-architecture-diagrams)
  - [Level 1 — System Context](#level-1--system-context)
  - [Level 2 — Container Diagram](#level-2--container-diagram)
  - [Level 3 — Component Diagram](#level-3--component-diagram)
- [Four Deployment Profiles](#four-deployment-profiles)
- [Concurrency Model](#concurrency-model)
- [Message Processing Pipeline](#message-processing-pipeline)
- [Publisher Module](#publisher-module)
- [Quick Start](#quick-start)
- [Configuration](#configuration)
- [Observability](#observability)
- [Testing](#testing)
- [Technology Stack](#technology-stack)
- [Key Design Decisions](#key-design-decisions)
- [Documentation](#documentation)

---

## Overview

This project solves the problem of high-throughput, at-least-once AMPS queue consumption with exactly-once processing guarantees — across one or many JVMs.

| Requirement | Implementation |
|---|---|
| At-least-once delivery | AMPS lease model — consumer must ACK within TTL |
| Exactly-once processing | `UNIQUE(message_id)` idempotency key in database |
| High-concurrency fan-out | Java 21 virtual threads — one VT per message |
| Backpressure | `Semaphore` acquired before VT submission; subscriber blocks |
| Retry without DLQ | DB tracks retry count; AMPS re-delivers on no-ACK |
| Graceful shutdown | `SmartLifecycle` drains all in-flight VTs before JVM exit |
| Observability | Micrometer + Prometheus + Grafana + Loki |
| Simulation | `message-publisher` profile generates configurable AMPS traffic |

---

## C4 Architecture Diagrams

### Level 1 — System Context

```
┌─────────────────────────────────────────────────────────────────────┐
│                           System Context                            │
└─────────────────────────────────────────────────────────────────────┘

 ┌─────────────────┐   publishes trades    ┌──────────────────────────┐
 │  Trading System │ ────────────────────▶ │                          │
 │  (external)     │                        │  AMPS Queue Concurrency  │
 └─────────────────┘                        │                          │
                                            │  Ingests, deduplicates,  │
 ┌─────────────────┐   admin / monitoring   │  and processes           │
 │  Operations     │ ◀──────────────────▶  │  high-throughput AMPS    │
 │  Team           │                        │  queue messages at scale │
 └─────────────────┘                        │  using Java 21 virtual   │
                                            │  threads.                │
 ┌─────────────────┐   reads processed data └──────────────────────────┘
 │  Downstream     │ ◀──────────────────── (via PostgreSQL / H2)
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

### Level 2 — Container Diagram

```
┌──────────────────────────────────────────────────────────────────────────────────┐
│                          AMPS Queue Concurrency System                           │
│                                                                                  │
│  ┌──────────────────┐  tcp:9004   ┌─────────────────────────────────────────┐   │
│  │   AMPS Server    │◀───────────▶│  Spring Boot: message-publisher         │   │
│  │  (external)      │             │  Profile: message-publisher             │   │
│  │  172.21.12.69    │             │  Port: 8082                             │   │
│  │  /queue/trades   │             │  Publishes N msg/s via HAClient         │   │
│  │  /queue/orders   │             └─────────────────────────────────────────┘   │
│  │                  │                                                            │
│  │  Admin:  8085    │  tcp:9004   ┌─────────────────────────────────────────┐   │
│  └──────────────────┘◀───────────▶│  Spring Boot: single-subscriber         │   │
│                                   │  Profile: single-subscriber             │   │
│                                   │  Port: 8084                             │   │
│                                   │  1 HAClient · Semaphore(100) · VTs      │   │
│                                   └──────────────────────┬──────────────────┘   │
│                                                          │ writes               │
│  ┌──────────────────┐  tcp:9004   ┌──────────────────┐  │  ┌────────────────┐  │
│  │   PostgreSQL     │             │ Spring Boot:      │  │  │  H2 (embedded) │  │
│  │  (postgres:16)   │◀───────────▶│ multi-jvm-sub    │  └─▶│  single-sub    │  │
│  │                  │  jdbc:5432  │ Port: 8083        │     │  multi-sub     │  │
│  │ processed_       │             │ N HAClients/JVM   │     └────────────────┘  │
│  │ messages table   │             └──────────────────┘                          │
│  └──────────────────┘                                                            │
│                                                                                  │
│  ┌──────────────────┐  scrapes :8080/actuator/prometheus                        │
│  │   Prometheus     │◀────────────────────────────────────────────              │
│  │  (port 9090)     │                                                            │
│  └────────┬─────────┘                                                            │
│           │                                                                      │
│  ┌────────▼─────────┐    ┌───────────────────────────────────────┐              │
│  │    Grafana       │◀───│  Loki (port 3100)                     │              │
│  │  (port 3000)     │    │  ◀── Promtail (reads Docker logs)     │              │
│  └──────────────────┘    └───────────────────────────────────────┘              │
└──────────────────────────────────────────────────────────────────────────────────┘
```

**Container inventory:**

| Container | Image | Port(s) | Purpose |
|---|---|---|---|
| *(external)* | AMPS at `172.21.12.69` | 9004 (client), 8085 (admin) | AMPS message broker — not containerised |
| `postgres` | `postgres:16-alpine` | host:5433 | Shared DB for multi-JVM profile |
| `app-publisher` | `./Dockerfile` | 8082 | Simulation publisher |
| `app-single` | `./Dockerfile` | 8084 | Single-subscriber consumer |
| `app-multi` | `./Dockerfile` | 8081 | Multi-subscriber consumer |
| `app-multi-jvm` | `./Dockerfile` | 8083 | Multi-JVM consumer (PostgreSQL) |
| `prometheus` | `prom/prometheus:v2.51.0` | 9090 | Metrics scraping |
| `grafana` | `grafana/grafana:10.4.0` | 3000 | Dashboards |
| `loki` | `grafana/loki:2.9.5` | 3100 | Log aggregation |
| `promtail` | `grafana/promtail:2.9.5` | — | Docker log shipper → Loki |

---

### Level 3 — Component Diagram

```
┌───────────────────────────────────────────────────────────────────────┐
│                   Spring Boot Application Container                   │
│                                                                       │
│  ┌─────────────────────────────────────────────────────────────────┐  │
│  │  Publisher Module  (@Profile("message-publisher"))              │  │
│  │                                                                 │  │
│  │  PublisherConfig ──▶ HAClient (publisher)                       │  │
│  │  AmpsMessagePublisher                                           │  │
│  │    ├── PublisherRateLimiter  (token-bucket Semaphore)           │  │
│  │    ├── MessagePayloadFactory (TRADE / ORDER / RISK / CUSTOM)    │  │
│  │    └── HAClient.publish(topic, payload)                         │  │
│  │  PublisherMetrics ──▶ Micrometer /actuator/prometheus           │  │
│  └─────────────────────────────────────────────────────────────────┘  │
│                                                                       │
│  ┌─────────────────────────────────────────────────────────────────┐  │
│  │  Subscriber Module (@Profile("single-subscriber" | "multi-*"))  │  │
│  │                                                                 │  │
│  │  [Single]  SingleAmpsSubscriber   (1 platform thread)          │  │
│  │  [Multi]   MultiAmpsSubscriberPool (N platform threads)         │  │
│  │               │                                                 │  │
│  │               │  semaphore.acquire()  ← backpressure gate       │  │
│  │               ▼                                                 │  │
│  │           MessageDispatchService                                │  │
│  │               │  executor.submit() → Virtual Thread             │  │
│  │               ▼                                                 │  │
│  │           MessageProcessor (@Transactional)                     │  │
│  │               │  1. idempotency check (messageId → DB)          │  │
│  │               │  2. executeBusinessLogic(payload)               │  │
│  │               │  3. INSERT / UPDATE ProcessedMessage            │  │
│  │               ▼                                                 │  │
│  │           ProcessedMessageRepository ──▶ HikariCP ──▶ DB       │  │
│  └─────────────────────────────────────────────────────────────────┘  │
│                                                                       │
│  ┌──────────────────────┐    ┌────────────────────────────────────┐   │
│  │  CommonConfig        │    │  AmpsHealthIndicator               │   │
│  │  Virtual Thread      │    │  /actuator/health                  │   │
│  │  Executor (shared)   │    │  AMPS connection status            │   │
│  └──────────────────────┘    └────────────────────────────────────┘   │
└───────────────────────────────────────────────────────────────────────┘
```

---

## Four Deployment Profiles

```
┌──────────────────────┬────────┬──────────────────────────────────────────────────┐
│ Profile              │ Role   │ Topology                                          │
├──────────────────────┼────────┼──────────────────────────────────────────────────┤
│ single-subscriber    │ CONSUME│ 1 JVM · 1 HAClient · 1 subscriber thread         │
│                      │        │ N virtual threads per message · H2 or PostgreSQL  │
├──────────────────────┼────────┼──────────────────────────────────────────────────┤
│ multi-subscriber     │ CONSUME│ 1 JVM · N HAClients · N subscriber threads        │
│                      │        │ M virtual threads per subscriber · H2 or Postgres │
├──────────────────────┼────────┼──────────────────────────────────────────────────┤
│ multi-jvm-subscriber │ CONSUME│ K JVMs · N HAClients each · N subscriber threads  │
│                      │        │ M virtual threads per subscriber                  │
│                      │        │ Shared PostgreSQL required (cross-JVM idempotency)│
├──────────────────────┼────────┼──────────────────────────────────────────────────┤
│ message-publisher    │ PUBLISH│ 1 HAClient · N virtual thread workers             │
│                      │        │ 4 modes · 4 payload templates · no DB             │
└──────────────────────┴────────┴──────────────────────────────────────────────────┘
```

Profiles are composable. The most common combination runs a full self-contained simulation:

```bash
# Publisher + subscriber in one JVM — no external services needed beyond AMPS
-Dspring.profiles.active=message-publisher,single-subscriber
```

```
Publisher VTs ──publish──▶ AMPS /queue/trades ──deliver──▶ Subscriber VTs
                                    │
                          (same JVM, two separate HAClients;
                           AMPS enforces lease model end-to-end)
```

### Profile Decision Tree

```
Need to generate test traffic for subscriber validation?
   └─ YES ──▶  message-publisher  (standalone or + subscriber profile)

Local dev or single-node low-volume workload?
   └─ YES ──▶  single-subscriber  (simplest, one TCP connection)

Ingest rate too high for one subscriber thread, still want one JVM?
   └─ YES ──▶  multi-subscriber   (N parallel AMPS connections in one JVM)

Need HA / fault tolerance or throughput beyond a single JVM?
   └─ YES ──▶  multi-jvm-subscriber  (K processes · shared PostgreSQL)
```

---

## Concurrency Model

### One message end-to-end

```
AMPS Queue       Subscriber Thread    Semaphore        VT-n          DB
    │                    │                │               │            │
    │── deliver msg ────▶│                │               │            │
    │   (leased, TTL)    │                │               │            │
    │                    │── acquire() ──▶│               │            │
    │                    │   blocks if    │               │            │
    │                    │   VT slots full│               │            │
    │                    │◀── permit ─────│               │            │
    │                    │                │               │            │
    │                    │── submit(task) ─────────────▶  │            │
    │                    │   (returns O(1))│              │            │
    │                    │                │  1. validate  │            │
    │── deliver next ───▶│                │  2. parse     │            │
    │   (subscriber      │                │  3. INSERT ──────────────▶ │
    │    unblocks after  │                │  4. ACK msg  │            │
    │    submit returns) │                │  5. release()│            │
    │                    │                │◀─ permit ────│            │
```

### Backpressure gate

```
                  Semaphore(maxConcurrency)
                  e.g. maxConcurrency = 200
                         │
        semaphore.acquire() — called by subscriber thread
        BEFORE submitting to VT executor

        IF permits > 0  → proceed immediately
        IF permits == 0 → BLOCK subscriber thread
            ↳ AMPS stops delivering on this connection
            ↳ Messages remain safely in AMPS server queue
            ↳ No data loss, no OOM from unbounded task queues

        semaphore.release() — called inside VT finally block
            ↳ frees one slot → subscriber thread unblocks
```

### Virtual threads and I/O parking

```
Platform Thread (OS-scheduled, ~1 MB stack)
  └── carries Virtual Thread (JVM-scheduled, ~1-2 KB heap)

When a VT blocks on JDBC getConnection or INSERT:
  VT parks its continuation on the heap
  Carrier platform thread is freed to run another VT
  DB responds → VT resumes on any available carrier

Result: 10 platform threads run 1,000+ VTs concurrently.
        20 HikariCP connections serve 200+ concurrent VTs
        (the extra 180 VTs park while waiting for a connection).
```

> **Thread pinning note:** AMPS client and some JDBC drivers use `synchronized` internally,
> which pins a VT to its carrier. Mitigation: add JVM flag `-Djdk.tracePinnedThreads=full`
> during development and size HikariCP conservatively. JDK 24+ resolves most driver-level
> pinning. See [`docs/11-virtual-threads.md`](docs/11-virtual-threads.md) for details.

---

## Message Processing Pipeline

### Consumer path

```
AMPS Server  ──deliver──▶  Subscriber Layer   (SmartLifecycle, platform threads)
                                 │  semaphore.acquire()  — backpressure gate
                                 │  executor.submit()    — O(1), spawns VT
                                 ▼
                            MessageDispatchService
                                 │  process(message)
                                 ▼
                            MessageProcessor   (@Transactional)
                                 │  1. idempotency check
                                 │  2. business logic
                                 │  3. INSERT / UPDATE ProcessedMessage
                                 │  4. return OK | DUPLICATE | DISCARD | FAIL
                                 ▼
                            ProcessedMessageRepository → HikariCP → H2 / PostgreSQL
```

### Idempotency and retry logic

```
Message arrives
      │
      ▼
MessageProcessor.process(message)
      │
      ├─ repo.findByMessageId(messageId)
      │
      │  Record EXISTS?
      │    status=PROCESSED  → return DUPLICATE  (ACK — safe to skip)
      │    status=DISCARDED  → return DISCARD    (ACK — already exhausted retries)
      │    status=FAILED     → check retryCount
      │        retryCount < maxRetries  → attempt processing again
      │        retryCount >= maxRetries → mark DISCARDED, return DISCARD (ACK)
      │
      │  Record NOT EXISTS → first delivery → process normally
      │
      ├─ Execute business logic
      │
      │  Success? YES → save status=PROCESSED → caller ACKs
      │  Success? NO  → save status=FAILED, retryCount++ → caller NO-ACKs
      │                  → AMPS re-delivers after lease TTL expires
```

**ACK rules:**
- `OK`, `DUPLICATE`, `DISCARD` → ACK the message
- `FAIL` → no ACK → AMPS re-delivers → natural retry
- No DLQ; after `maxRetries` failures the message is ACK'd and logged as DISCARDED

---

## Publisher Module

### Publishing modes

| Mode | Behaviour |
|---|---|
| `RATE_LIMITED` | Publish at exactly N msg/s via token-bucket limiter |
| `BURST` | Publish as fast as possible — no rate gate |
| `FIXED_COUNT` | Publish exactly `total-messages` messages then exit cleanly |
| `INFINITE` | Publish until SIGTERM / Ctrl+C |

### Payload templates

| Template | Domain | Key fields |
|---|---|---|
| `TRADE` | Equity trade | `tradeId`, `symbol`, `side`, `quantity`, `price`, `currency` |
| `ORDER` | FX order | `orderId`, `instrument`, `side`, `orderType`, `notional`, `limitPrice` |
| `RISK` | Risk snapshot | `riskId`, `book`, `pnl`, `delta`, `vega` |
| `CUSTOM` | Any JSON | Placeholders: `{seq}`, `{uuid}`, `{timestamp}`, `{random-int}`, `{symbol}` |

### Publisher architecture

```
AmpsMessagePublisher (SmartLifecycle, phase = MAX_VALUE - 1)
  │
  ├── spawn N virtual thread workers
  │       │
  │       │  loop until (count >= total OR !running):
  │       │    1. payload  = payloadFactory.generate(seq++)
  │       │    2. rateLimiter.acquire()     ← blocks if over rate (RATE_LIMITED)
  │       │    3. haClient.publish(topic, payload)
  │       │    4. metrics.recordPublished()
  │       │    5. if FIXED_COUNT && done → signal context.close()
  │
  └── Token-bucket rate limiter
          Semaphore (0 permits initially)
          ScheduledExecutor fires every 100ms:
              releases = messagesPerSecond × 100 / 1000 permits
          Each VT: semaphore.acquire() before publish
```

**Phase ordering:** publisher phase `MAX_VALUE - 1` means it stops *before* subscriber
when profiles are combined — the subscriber drains the queue before the publisher shuts down.

---

## Quick Start

### Prerequisites

- Java 21+
- Maven wrapper (`mvnw.cmd` / `./mvnw`)
- AMPS client JAR installed in local Maven repo
- Docker + Docker Compose (for full stack)

### Install the AMPS client JAR

```bash
mvn install:install-file \
  -Dfile=/path/to/amps-client-5.3.4.0.jar \
  -DgroupId=com.crankuptheamps \
  -DartifactId=amps-client \
  -Dversion=5.3.4.0 \
  -Dpackaging=jar \
  -DgeneratePom=true
```

### Build

```bash
mvnw.cmd clean package -DskipTests      # Windows
./mvnw clean package -DskipTests        # Linux / Mac
```

### Run subscriber profiles

```bash
# Simplest — one connection, H2 in-memory DB
mvnw.cmd spring-boot:run -Dspring-boot.run.profiles=single-subscriber

# Multiple AMPS connections in one JVM
mvnw.cmd spring-boot:run -Dspring-boot.run.profiles=multi-subscriber

# Horizontal scale — requires external PostgreSQL
mvnw.cmd spring-boot:run -Dspring-boot.run.profiles=multi-jvm-subscriber
```

### Run publisher (feeds external subscriber nodes)

```bash
mvnw.cmd spring-boot:run -Dspring-boot.run.profiles=message-publisher
```

### Self-contained simulation (no external services except AMPS)

```bash
# Publisher + single subscriber in one JVM
mvnw.cmd spring-boot:run "-Dspring-boot.run.profiles=message-publisher,single-subscriber"

# Publisher + multi subscriber in one JVM
mvnw.cmd spring-boot:run "-Dspring-boot.run.profiles=message-publisher,multi-subscriber"
```

### Docker — full stack

```bash
# Infrastructure only (AMPS + Postgres + observability)
docker compose -f docker/docker-compose.yml up -d \
  amps postgres prometheus grafana loki promtail

# Publisher + single subscriber + full observability
docker compose -f docker/docker-compose.yml up -d \
  amps app-publisher app-single prometheus grafana loki promtail

# Multi-JVM scenario scaled to 3 instances
docker compose -f docker/docker-compose.yml up -d \
  amps postgres app-publisher app-multi-jvm prometheus grafana loki promtail
docker compose -f docker/docker-compose.yml up -d --scale app-multi-jvm=3
```

---

## Configuration

| File | Used by |
|---|---|
| `application.yaml` | All profiles — base settings |
| `application-single-subscriber.yaml` | H2, 1 HAClient, `max-concurrency` |
| `application-multi-subscriber.yaml` | H2, `subscriber-count`, `max-concurrency-per-subscriber` |
| `application-multi-jvm-subscriber.yaml` | PostgreSQL, `subscriber-count`, `max-concurrency-per-subscriber` |
| `application-message-publisher.yaml` | Publisher mode, rate, payload template, worker count |

### Key properties (subscriber)

```yaml
amps:
  server:
    uri: tcp://172.21.12.69:9004/amps/json
  consumer:
    topic: /queue/trades
    max-concurrency: 200          # semaphore permits (= max in-flight VTs)
    max-retries: 3                # discard after N failures
    lease-timeout-ms: 5000        # match AMPS LeaseTimeout config
    subscriber-count: 4           # multi-subscriber profiles only
```

### Key properties (publisher)

```yaml
amps:
  publisher:
    topic: /queue/trades
    mode: RATE_LIMITED            # RATE_LIMITED | BURST | FIXED_COUNT | INFINITE
    messages-per-second: 500
    total-messages: 50000         # FIXED_COUNT only; 0 = ignore
    concurrent-publishers: 5
    payload-template: TRADE       # TRADE | ORDER | RISK | CUSTOM
    rate-replenish-interval-ms: 100
```

### Docker environment variable overrides

| Spring property | Docker env var |
|---|---|
| `amps.server.uri` | `AMPS_SERVER_URI` |
| `amps.publisher.messages-per-second` | `AMPS_PUBLISHER_MESSAGES_PER_SECOND` |
| `amps.consumer.max-concurrency` | `AMPS_CONSUMER_MAX_CONCURRENCY` |
| `spring.datasource.url` | `SPRING_DATASOURCE_URL` |
| `spring.profiles.active` | `SPRING_PROFILES_ACTIVE` |

---

## Observability

### Health endpoint

```
GET /actuator/health
{
  "status": "UP",
  "components": {
    "amps": {
      "status": "UP",
      "details": { "connection": "CONNECTED", "topic": "/queue/trades" }
    }
  }
}
```

### Metrics emitted (Micrometer / Prometheus)

| Metric | Type | Tags |
|---|---|---|
| `amps.messages.received` | counter | `profile`, `topic` |
| `amps.messages.processed` | counter | `profile`, `topic` |
| `amps.messages.failed` | counter | `profile`, `topic` |
| `amps.messages.duplicate` | counter | `profile` |
| `amps.messages.discarded` | counter | `profile` |
| `amps.processing.time` | timer (p50/p95/p99) | `profile`, `topic` |
| `amps.subscriber.active` | gauge | `profile` |
| `amps.semaphore.available` | gauge | `profile` |
| `amps.publisher.messages.sent` | counter | — |
| `amps.publisher.messages.failed` | counter | — |
| `amps.publisher.actual.rate` | gauge | — |
| `amps.publisher.send.time` | timer | — |

### Dashboards and log queries

Grafana: `http://localhost:3000` (admin / admin)
Pre-provisioned dashboard: **AMPS Queue Concurrency** — 8 panels covering publish rate,
processing latency, JVM heap, live thread count, and application logs.

Trace a single message end-to-end in Loki:
```logql
{service="amps-queue-concurrency"} | json | messageId = "<amps-bookmark>"
```

Structured JSON logging is activated by adding `docker-logging` to `SPRING_PROFILES_ACTIVE`.

---

## Testing

```bash
# All tests
mvnw.cmd test

# Single class
mvnw.cmd test -Dtest=MessageProcessorTest

# Unit tests only (no Spring context)
mvnw.cmd test -Dgroups=unit

# Integration tests only
mvnw.cmd test -Dgroups=integration
```

| Test class | Scope |
|---|---|
| `MessageProcessorTest` | Unit — all four result paths (OK, DUPLICATE, DISCARD, FAIL) |
| `MessageDispatchServiceTest` | Unit — VT dispatch, semaphore lifecycle, ACK logic |
| `MessagePayloadFactoryTest` | Unit — JSON schema validation for all four templates |
| `PublisherRateLimiterTest` | Unit — token-bucket refill, exhaustion, BURST no-op |
| `MessageProcessorIntegrationTest` | Integration — H2 + full Spring context, no AMPS |

---

## Technology Stack

| Layer | Technology |
|---|---|
| Framework | Spring Boot 3.5 |
| Language | Java 21 |
| AMPS Client | HAClient 5.3.x (60East Technologies) |
| Concurrency | `Executors.newVirtualThreadPerTaskExecutor()` + `Semaphore` |
| Persistence | Spring Data JPA + HikariCP |
| DB (dev / single-JVM) | H2 in-file mode |
| DB (production / multi-JVM) | PostgreSQL 16 |
| Metrics | Micrometer + Prometheus |
| Logging | Logback + LogstashEncoder (JSON) + Loki |
| Dashboards | Grafana 10 |
| Testing | JUnit 5 + Mockito + H2 |
| Build | Maven + Spring Boot Maven Plugin |
| Runtime | Eclipse Temurin 21 + ZGC |

---

## Key Design Decisions

| Decision | Choice | Rationale |
|---|---|---|
| Architecture style | Simple 5-layer — no hexagonal | Lower complexity; clear package boundaries |
| Profile = topology | Spring `@Profile` selects all beans | No runtime switch needed; config-driven topology |
| Retry mechanism | AMPS natural re-delivery (no ACK = lease expiry) | Releases semaphore and DB connection immediately |
| Dead-letter handling | None — discard after `maxRetries` | No DLQ infrastructure available |
| Idempotency key | AMPS bookmark (`message.getBookmark()`) | Globally unique per queue message; built-in |
| Backpressure | `Semaphore` acquired before `executor.submit()` | Blocks subscriber thread; AMPS pauses delivery |
| Semaphore scope | Per-subscriber (multi-sub profiles) | One slow subscriber does not starve others |
| ACK connection | Same `HAClient` that received the message | AMPS protocol requirement |
| Client naming | `{app}-{hostname}-sub-{index}` | Globally unique; stable across restarts |
| Publisher HAClient | Single shared — `publish()` is thread-safe | Avoids N TCP connections for N VT workers |
| Publisher DB | Excluded in standalone mode | Publisher is stateless; avoids wasted JPA bootstrap |
| Publisher phase | `MAX_VALUE - 1` | Publisher stops before subscriber when profiles combined |
| Rate limiting | Token-bucket Semaphore + ScheduledExecutorService | Accurate aggregate rate across N VTs; 100ms replenishment |

---

## Documentation

Full architecture and design documentation lives in [`docs/`](docs/):

| Document | Contents |
|---|---|
| [`docs/00-project-overview.md`](docs/00-project-overview.md) | Problem statement, profile decision tree, project layout |
| [`docs/01-amps-fundamentals.md`](docs/01-amps-fundamentals.md) | AMPS concepts, queue model, lease lifecycle, golden rules |
| [`docs/02-system-architecture.md`](docs/02-system-architecture.md) | All-profiles architecture, VT model, backpressure, data flow |
| [`docs/03-profile-single-subscriber.md`](docs/03-profile-single-subscriber.md) | Single-subscriber: config, components, resource counts, shutdown |
| [`docs/04-profile-multi-subscriber.md`](docs/04-profile-multi-subscriber.md) | Multi-subscriber: N connections in one JVM, per-subscriber semaphore |
| [`docs/05-profile-multi-jvm-subscriber.md`](docs/05-profile-multi-jvm-subscriber.md) | Multi-JVM: horizontal scaling, crash recovery, PostgreSQL |
| [`docs/06-cross-cutting-concerns.md`](docs/06-cross-cutting-concerns.md) | Idempotency, retry, graceful shutdown, backpressure, observability |
| [`docs/07-testing-strategy.md`](docs/07-testing-strategy.md) | Unit + integration test plan, MockHaClient, test profiles |
| [`docs/08-capacity-planning.md`](docs/08-capacity-planning.md) | Throughput formulas, sizing guide, JVM tuning |
| [`docs/10-publisher-module.md`](docs/10-publisher-module.md) | Publisher: 4 modes, 4 payload templates, rate limiter, combined profiles |
| [`docs/11-virtual-threads.md`](docs/11-virtual-threads.md) | VT theory, lifecycle, parking vs blocking, pinning detection |
| [`docs/12-infrastructure-docker.md`](docs/12-infrastructure-docker.md) | Docker Compose, AMPS setup, Grafana/Loki, production runbook |