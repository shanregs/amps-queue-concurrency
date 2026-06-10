# CLAUDE.md

This file provides guidance to Claude Code when working with this repository.
Full architecture and design documentation lives in [`docs/`](docs/).

---

## Project Overview

**Enterprise-grade AMPS queue consumer + simulation publisher** built on Spring Boot 3.5 + Java 21.

Three consumer topologies and one publisher — all selectable via **Spring profile**, no code changes needed.

| Profile | Role | Topology | Database |
|---|---|---|---|
| `single-subscriber` | CONSUME | 1 JVM · 1 subscriber · N virtual threads | H2 or PostgreSQL |
| `multi-subscriber` | CONSUME | 1 JVM · N subscribers · M VTs each | H2 or PostgreSQL |
| `multi-jvm-subscriber` | CONSUME | K JVMs · N subscribers · M VTs each | Shared PostgreSQL |
| `message-publisher` | PUBLISH | N VT workers · 4 modes · 4 payload templates | None (stateless) |

Profiles are composable: `message-publisher,single-subscriber` runs a full simulation in one JVM.

> See [`docs/00-project-overview.md`](docs/00-project-overview.md) for the full decision tree.

---

## Commands

```bash
# Build (skip tests)
mvnw.cmd clean package -DskipTests               # Windows
./mvnw clean package -DskipTests                 # Linux/Mac

# Subscriber profiles
mvnw.cmd spring-boot:run -Dspring-boot.run.profiles=single-subscriber
mvnw.cmd spring-boot:run -Dspring-boot.run.profiles=multi-subscriber
mvnw.cmd spring-boot:run -Dspring-boot.run.profiles=multi-jvm-subscriber

# Publisher profile (standalone — feeds external subscriber nodes)
mvnw.cmd spring-boot:run -Dspring-boot.run.profiles=message-publisher

# Combined: self-contained simulation (publisher + subscriber in one JVM)
mvnw.cmd spring-boot:run "-Dspring-boot.run.profiles=message-publisher,single-subscriber"
mvnw.cmd spring-boot:run "-Dspring-boot.run.profiles=message-publisher,multi-subscriber"

# Run all tests
mvnw.cmd test

# Run a single test class
mvnw.cmd test -Dtest=MessageProcessorTest

# Run only unit tests
mvnw.cmd test -Dgroups=unit

# Run only integration tests
mvnw.cmd test -Dgroups=integration
```

---

## Architecture in 30 Seconds

### Consumer path (subscriber profiles)

```text
AMPS Server  ──deliver──▶  Subscriber Layer  (SmartLifecycle, platform threads)
                                │  semaphore.acquire()  — backpressure gate
                                │  executor.submit()    — O(1), spawns VT
                                ▼
                           MessageDispatchService
                                │  process(message)
                                ▼
                           MessageProcessor  (@Transactional)
                                │  1. idempotency check
                                │  2. business logic
                                │  3. INSERT/UPDATE ProcessedMessage
                                │  4. return OK | DUPLICATE | DISCARD | FAIL
                                ▼
                           ProcessedMessageRepository → HikariCP → H2 / PostgreSQL
```

### Publisher path (message-publisher profile)

```text
MessagePayloadFactory  ──generate──▶  AmpsMessagePublisher VT workers
                                            │  rateLimiter.acquire()  — token bucket
                                            │  haClient.publish(topic, payload)
                                            ▼
                                       AMPS Server  /queue/trades
                                            │
                                       (any subscriber profile consumes)
```

**Key rules (consumer):**
- `semaphore.acquire()` before `executor.submit()` — backpressure blocks the subscriber thread
- `client.ack()` only on `OK`, `DUPLICATE`, `DISCARD` — never on `FAIL`
- `FAIL` → no ACK → AMPS re-delivers after lease TTL → retry path
- After `maxRetries` failures → status = `DISCARDED` → ACK (discard with severe log)
- No DLQ. No hexagonal architecture.

**Key rules (publisher):**
- Single shared `HAClient` for all VT workers — `publish()` is thread-safe
- No bookmark store, no DB, no retry obligation — simulation only
- Publisher phase = `MAX_VALUE - 1` → stops before subscriber when profiles are combined

---

## Package Structure

```text
src/main/java/com/shan/mq/amps/ampsqueueconcurrency/
│
├── AmpsQueueConcurrencyApplication.java
│
├── config/
│   ├── SingleSubscriberConfig.java      @Profile("single-subscriber")
│   ├── MultiSubscriberConfig.java       @Profile("multi-subscriber")
│   ├── MultiJvmSubscriberConfig.java    @Profile("multi-jvm-subscriber")
│   ├── PublisherConfig.java             @Profile("message-publisher")
│   └── CommonConfig.java               shared (VT executor, metrics)
│
├── model/
│   ├── ProcessedMessage.java           JPA @Entity — shared across subscriber profiles
│   └── ProcessingStatus.java          PROCESSED | FAILED | DISCARDED
│
├── repository/
│   └── ProcessedMessageRepository.java
│
├── subscriber/
│   ├── SingleAmpsSubscriber.java       @Profile("single-subscriber") SmartLifecycle
│   └── MultiAmpsSubscriberPool.java    @Profile("multi-subscriber|multi-jvm-subscriber")
│
├── publisher/                          ← publisher module
│   ├── AmpsMessagePublisher.java       @Profile("message-publisher") SmartLifecycle
│   ├── MessagePayloadFactory.java      TRADE | ORDER | RISK | CUSTOM payload generation
│   ├── PublisherRateLimiter.java       Token-bucket gate (Semaphore + scheduler)
│   ├── PublisherMode.java             enum: RATE_LIMITED | BURST | FIXED_COUNT | INFINITE
│   ├── PayloadTemplate.java           enum: TRADE | ORDER | RISK | CUSTOM
│   └── PublisherMetrics.java          Micrometer counters/timers/gauges
│
├── service/
│   └── MessageDispatchService.java     Semaphore + VT fan-out
│
├── processor/
│   └── MessageProcessor.java          @Transactional, idempotency, retry tracking
│
├── health/
│   └── AmpsHealthIndicator.java       Spring Actuator — AMPS connection status
│
└── exception/
    ├── AmpsConnectionException.java
    └── MessageProcessingException.java
```

---

## Configuration Files

| File | Purpose |
|---|---|
| `application.yaml` | Base config (no profile-specific settings) |
| `application-single-subscriber.yaml` | H2, 1 HAClient, `max-concurrency` |
| `application-multi-subscriber.yaml` | H2, `subscriber-count`, `max-concurrency-per-subscriber` |
| `application-multi-jvm-subscriber.yaml` | PostgreSQL, `subscriber-count`, `max-concurrency-per-subscriber` |
| `application-message-publisher.yaml` | Publisher mode, rate, payload template, concurrent workers |
| `application-test-single.yaml` | In-memory H2, mock AMPS, single profile |
| `application-test-multi.yaml` | In-memory H2, mock AMPS, multi profile |

---

## Key Design Decisions

| Decision | Choice | Rationale |
|---|---|---|
| Architecture style | Simple layered (no hexagonal) | Lower complexity, clear package boundaries |
| Retry mechanism | AMPS natural re-delivery (no ACK → lease expiry) | Releases semaphore and DB connection immediately |
| Retry count storage | `retry-db-tracking-enabled` flag (default `true`) | `true` → FAILED rows in DB (survives restarts, multi-JVM safe); `false` → in-memory `ConcurrentHashMap` (no FAILED rows written, JVM-local only) |
| Dead-letter handling | None — discard after `max-retries` exhausted; log ERROR + ACK | No DLQ infrastructure available |
| Idempotency key | AMPS bookmark (`message.getBookmark()`) | Globally unique per queue message, built-in |
| Backpressure | `Semaphore` acquired before `executor.submit()` | Blocks subscriber thread; AMPS pauses delivery naturally |
| Semaphore scope | Per-subscriber (multi-sub profiles) | One slow subscriber does not starve others |
| ACK connection | Must use same `HAClient` that received the message | AMPS protocol requirement |
| Client naming | `{app}-{hostname}-sub-{index}` | Globally unique; stable across restarts |
| Publisher HAClient | Single shared — `publish()` is thread-safe | Avoids N TCP connections for N publisher VTs |
| Publisher DB | None in standalone mode | Publisher is stateless; excludes JPA auto-config |
| Publisher phase | `MAX_VALUE - 1` | Stops before subscriber when profiles combined; subscriber drains queue first |
| Publisher rate limiting | Token-bucket (`Semaphore` + `ScheduledExecutorService`) | Accurate aggregate rate across N VTs; 100ms replenishment granularity |

---

## Documentation Index

| Document | Contents |
|---|---|
| [`docs/00-project-overview.md`](docs/00-project-overview.md) | Problem statement, profile decision tree, full project layout |
| [`docs/01-amps-fundamentals.md`](docs/01-amps-fundamentals.md) | AMPS concepts, queue model, lease lifecycle, golden rules |
| [`docs/02-system-architecture.md`](docs/02-system-architecture.md) | All-profiles architecture, VT model, backpressure, data flow |
| [`docs/03-profile-single-subscriber.md`](docs/03-profile-single-subscriber.md) | Single-subscriber: config, components, resource counts, shutdown |
| [`docs/04-profile-multi-subscriber.md`](docs/04-profile-multi-subscriber.md) | Multi-subscriber: N connections in one JVM, per-subscriber semaphore |
| [`docs/05-profile-multi-jvm-subscriber.md`](docs/05-profile-multi-jvm-subscriber.md) | Multi-JVM: horizontal scaling, crash recovery, PostgreSQL |
| [`docs/06-cross-cutting-concerns.md`](docs/06-cross-cutting-concerns.md) | Idempotency, retry (no DLQ), graceful shutdown, backpressure, observability |
| [`docs/07-testing-strategy.md`](docs/07-testing-strategy.md) | Unit + integration test plan, MockHaClient, test profiles |
| [`docs/08-capacity-planning.md`](docs/08-capacity-planning.md) | Throughput formulas, sizing guide, JVM tuning |
| [`docs/10-publisher-module.md`](docs/10-publisher-module.md) | Publisher design: 4 modes, 4 payload templates, rate limiter, combined profiles |
| [`docs/11-virtual-threads.md`](docs/11-virtual-threads.md) | VT theory, lifecycle, parking vs blocking, pinning detection and fixes |
| [`docs/12-infrastructure-docker.md`](docs/12-infrastructure-docker.md) | C4 diagrams (L1/L2/L3), Docker Compose, AMPS setup, Grafana/Loki observability |
| [`docs/13-docker-cheatsheet.md`](docs/13-docker-cheatsheet.md) | Docker & Compose quick-reference: build, run, logs, exec, volume, network commands |
| [`docs/14-observability-cheatsheet.md`](docs/14-observability-cheatsheet.md) | Prometheus PromQL, Grafana dashboards, Loki LogQL, Promtail — quick-reference |
| [`docs/15-smart-lifecycle.md`](docs/15-smart-lifecycle.md) | SmartLifecycle theory, phase ordering, stop callback, patterns, pitfalls, project usage |
| [`docs/16-rate-limiters.md`](docs/16-rate-limiters.md) | Five rate-limiter algorithms, C4 diagrams, message flows, enterprise use cases, library guide |
| [`docs/17-how-to-run.md`](docs/17-how-to-run.md) | Step-by-step run guide: publisher + 2 multi-JVM subscribers via Docker, Windows Terminal, IntelliJ |

---

## AMPS Concepts Quick Reference

| Concept | Meaning |
|---|---|
| Queue | Persistent channel — each message delivered to exactly ONE subscriber (competing consumers) |
| Lease | AMPS holds a message for one subscriber; subscriber must ACK within TTL or message returns |
| Bookmark | Globally unique cursor per message — used as idempotency key |
| HAClient | Java client class — handles reconnect, failover, bookmark replay automatically |
| LoggedBookmarkStore | Persists read position to disk — no missed messages after JVM restart |
