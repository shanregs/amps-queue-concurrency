# 00 — Project Overview

## Problem Statement

Build an **enterprise-grade AMPS queue consumer and simulation publisher** in Spring Boot 3.5 + Java 21 that supports
four distinct deployment modes — selectable at runtime via a Spring profile — without
changing a single line of application code.

| Requirement | Detail |
|---|---|
| **At-least-once delivery** | AMPS leases messages; consumer must ACK within TTL |
| **Exactly-once processing** | Idempotent writes via `UNIQUE(message_id)` in DB |
| **Virtual-thread fan-out** | Each arriving message spawns one Java 21 virtual thread |
| **Backpressure** | Semaphore limits in-flight VTs; subscriber thread blocks naturally |
| **Retry (no DLQ)** | DB tracks retry count; after `maxRetries` attempts, message is discarded with severe log |
| **Graceful shutdown** | `SmartLifecycle` drains all in-flight VTs before JVM exit |
| **Observability** | Micrometer counters + timers, Spring Actuator health per subscriber |
| **Testability** | Unit tests (Mockito), integration tests (Testcontainers or embedded H2) |
| **Simulation** | `message-publisher` profile generates configurable AMPS traffic for subscriber testing |

---

## Four Deployment Profiles

```text
┌──────────────────────┬────────┬─────────────────────────────────────────────────────┐
│ Profile Name         │ Role   │ Topology                                             │
├──────────────────────┼────────┼─────────────────────────────────────────────────────┤
│ single-subscriber    │ CONSUME│ 1 JVM · 1 HAClient · 1 subscriber thread            │
│                      │        │ N virtual threads per message · H2 or PostgreSQL    │
├──────────────────────┼────────┼─────────────────────────────────────────────────────┤
│ multi-subscriber     │ CONSUME│ 1 JVM · N HAClients · N subscriber threads          │
│                      │        │ M virtual threads per subscriber · H2 or PostgreSQL │
├──────────────────────┼────────┼─────────────────────────────────────────────────────┤
│ multi-jvm-subscriber │ CONSUME│ K JVMs · N HAClients per JVM · N subscriber threads │
│                      │        │ M virtual threads per subscriber                    │
│                      │        │ Shared PostgreSQL required (cross-JVM idempotency)  │
├──────────────────────┼────────┼─────────────────────────────────────────────────────┤
│ message-publisher    │ PUBLISH│ 1 HAClient · N virtual thread workers               │
│                      │        │ Configurable: rate, mode, payload template          │
│                      │        │ No database · combinable with any subscriber profile│
└──────────────────────┴────────┴─────────────────────────────────────────────────────┘
```

### Profile Decision Tree

```text
Do you need to generate test traffic for subscriber validation?
   └─ YES ──▶  message-publisher  (standalone or combined with a subscriber profile)
              (RATE_LIMITED / BURST / FIXED_COUNT / INFINITE modes)

Is this for local development or single-node low-volume workload?
   └─ YES ──▶  single-subscriber
              (simplest, easiest to debug, one TCP connection)

Is the ingest rate too high for one subscriber thread to keep up,
but you want to stay on a single JVM?
   └─ YES ──▶  multi-subscriber
              (N parallel AMPS connections within one JVM; scales vertically)

Do you need HA / fault tolerance, or does your throughput requirement
exceed what a single JVM can provide?
   └─ YES ──▶  multi-jvm-subscriber
              (K identical JVM processes each with N subscribers;
               scales horizontally; requires shared PostgreSQL)
```

### Combined Profile — Self-Contained Simulation

Profiles can be composed. The most common combination:

```bash
# Publisher + subscriber in one JVM — no external dependencies
-Dspring.profiles.active=message-publisher,single-subscriber
```

```text
Publisher VTs ──publish──▶ AMPS Server ──deliver──▶ Subscriber VTs
                                │
                           /queue/trades
                       (AMPS enforces lease model;
                        same JVM, two separate HAClients)
```

---

## Project Layout

```text
amps-queue-concurrency/
│
├── pom.xml                                  Spring Boot 3.5, Java 21, AMPS client
│
├── src/main/java/.../
│   ├── AmpsQueueConcurrencyApplication.java @SpringBootApplication
│   │
│   ├── config/
│   │   ├── SingleSubscriberConfig.java      @Profile("single-subscriber")
│   │   ├── MultiSubscriberConfig.java       @Profile("multi-subscriber")
│   │   ├── MultiJvmSubscriberConfig.java    @Profile("multi-jvm-subscriber")
│   │   ├── PublisherConfig.java             @Profile("message-publisher")
│   │   └── CommonConfig.java               shared executor, metrics, etc.
│   │
│   ├── model/
│   │   ├── ProcessedMessage.java            JPA @Entity — shared across subscriber profiles
│   │   └── ProcessingStatus.java           enum: PROCESSED | FAILED | DISCARDED
│   │
│   ├── repository/
│   │   └── ProcessedMessageRepository.java  Spring Data JPA
│   │
│   ├── subscriber/
│   │   ├── SingleAmpsSubscriber.java        @Profile("single-subscriber") SmartLifecycle
│   │   └── MultiAmpsSubscriberPool.java     @Profile("multi-subscriber|multi-jvm-subscriber")
│   │
│   ├── publisher/                           ← NEW: publisher module
│   │   ├── AmpsMessagePublisher.java        @Profile("message-publisher") SmartLifecycle
│   │   ├── MessagePayloadFactory.java       Template-based payload generation
│   │   ├── PublisherRateLimiter.java        Token-bucket rate gate
│   │   ├── PublisherMode.java              enum: RATE_LIMITED|BURST|FIXED_COUNT|INFINITE
│   │   ├── PayloadTemplate.java            enum: TRADE|ORDER|RISK|CUSTOM
│   │   └── PublisherMetrics.java           Micrometer counters/timers/gauges
│   │
│   ├── service/
│   │   └── MessageDispatchService.java      Backpressure gate + VT fan-out
│   │
│   ├── processor/
│   │   └── MessageProcessor.java           @Transactional, idempotency, retry tracking
│   │
│   ├── health/
│   │   └── AmpsHealthIndicator.java        Spring Actuator — AMPS connection status
│   │
│   └── exception/
│       ├── AmpsConnectionException.java
│       └── MessageProcessingException.java
│
├── src/main/resources/
│   ├── application.yaml                     base config (active profile must be set)
│   ├── application-single-subscriber.yaml   profile-specific overrides
│   ├── application-multi-subscriber.yaml
│   ├── application-multi-jvm-subscriber.yaml
│   └── application-message-publisher.yaml   ← NEW
│
├── src/test/java/.../
│   ├── unit/                                Mockito — no Spring context
│   └── integration/                         @SpringBootTest, Testcontainers
│
└── docs/
    ├── 00-project-overview.md               ← you are here
    ├── 01-amps-fundamentals.md
    ├── 02-system-architecture.md
    ├── 03-profile-single-subscriber.md
    ├── 04-profile-multi-subscriber.md
    ├── 05-profile-multi-jvm-subscriber.md
    ├── 06-cross-cutting-concerns.md
    ├── 07-testing-strategy.md
    ├── 08-capacity-planning.md
    ├── 09-faq.md
    ├── 10-publisher-module.md
    ├── 11-virtual-threads.md
    ├── 12-infrastructure-docker.md          ← Docker, C4 diagrams, AMPS setup, Grafana/Loki
    ├── 13-docker-cheatsheet.md              ← Docker & Compose quick-reference commands
    ├── 14-observability-cheatsheet.md       ← PromQL, LogQL, Grafana, Loki quick-reference
    ├── 15-smart-lifecycle.md                ← SmartLifecycle theory, phase ordering, patterns
    ├── 16-rate-limiters.md                  ← Rate-limiter algorithms, C4 diagrams, library guide
    └── 17-how-to-run.md                     ← Step-by-step: publisher + multi-JVM (Docker / Terminal / IntelliJ)
```

---

## Technology Stack

| Layer | Technology | Rationale |
|---|---|---|
| Framework | Spring Boot 3.5 | Auto-configuration, Actuator, SmartLifecycle |
| Language | Java 21 | Virtual threads, records, sealed classes |
| AMPS Client | HAClient 5.3.x | Auto-reconnect, LoggedBookmarkStore, ExponentialDelayStrategy |
| Concurrency | `Executors.newVirtualThreadPerTaskExecutor()` + `Semaphore` | Lightweight fan-out with bounded backpressure |
| Persistence | Spring Data JPA + HikariCP | Connection pooling; VTs park on pool exhaustion |
| DB (dev) | H2 in-file mode | Zero-ops for single-JVM profiles |
| DB (prod) | PostgreSQL | Shared state for multi-JVM idempotency |
| Metrics | Micrometer + Prometheus | Counters, timers, gauges per profile |
| Testing | JUnit 5, Mockito, Testcontainers | Unit + integration; mock AMPS for unit |

---

## Architectural Principles

1. **No Hexagonal / No Ports-and-Adapters** — Simple 5-layer design: `config → subscriber → service → processor → repository`.
2. **Profile = Topology** — The active Spring profile determines the entire concurrency topology. No runtime switch is needed.
3. **Shared domain model** — `ProcessedMessage` and `ProcessedMessageRepository` are profile-agnostic.
4. **Idempotency is mandatory** — at-least-once delivery means every write must be safe to repeat.
5. **Retry without DLQ** — failed messages are retried up to `amps.consumer.max-retries` times; after that, they are ACKed and logged as discarded.
6. **Semaphore-before-submit** — the semaphore permit is acquired by the subscriber thread *before* submitting to the VT executor, ensuring the subscriber thread blocks (and AMPS slows delivery) when all VTs are busy.
7. **ACK on the receiving connection** — AMPS requires the ACK to go back on the same `HAClient` that received the message.

---

## Running the Application

```bash
# Build (skip tests)
./mvnw clean package -DskipTests          # Linux/Mac
mvnw.cmd clean package -DskipTests        # Windows

# ── Subscriber profiles ───────────────────────────────────────────────────
./mvnw spring-boot:run -Dspring-boot.run.profiles=single-subscriber
./mvnw spring-boot:run -Dspring-boot.run.profiles=multi-subscriber
./mvnw spring-boot:run -Dspring-boot.run.profiles=multi-jvm-subscriber

# ── Publisher profile (standalone — feeds external subscriber nodes) ───────
./mvnw spring-boot:run -Dspring-boot.run.profiles=message-publisher

# ── Combined: self-contained simulation (publisher + subscriber in one JVM) 
./mvnw spring-boot:run -Dspring-boot.run.profiles=message-publisher,single-subscriber
./mvnw spring-boot:run -Dspring-boot.run.profiles=message-publisher,multi-subscriber

# ── Tests ─────────────────────────────────────────────────────────────────
./mvnw test
./mvnw test -Dtest=MessageProcessorTest
./mvnw test -Dgroups=integration
```

---

## Documents in This `docs/` Folder

| File | Contents |
|---|---|
| [01-amps-fundamentals.md](01-amps-fundamentals.md) | AMPS concepts, queue model, lease lifecycle, golden rules |
| [02-system-architecture.md](02-system-architecture.md) | All-profiles architecture diagrams, VT model, backpressure |
| [03-profile-single-subscriber.md](03-profile-single-subscriber.md) | Single-subscriber design: config, classes, resource counts |
| [04-profile-multi-subscriber.md](04-profile-multi-subscriber.md) | Multi-subscriber design: N connections in one JVM |
| [05-profile-multi-jvm-subscriber.md](05-profile-multi-jvm-subscriber.md) | Multi-JVM design: horizontal scaling, crash recovery |
| [06-cross-cutting-concerns.md](06-cross-cutting-concerns.md) | Idempotency, retry (no DLQ), shutdown, backpressure, observability |
| [07-testing-strategy.md](07-testing-strategy.md) | Unit + integration test plan, tooling, mock AMPS |
| [08-capacity-planning.md](08-capacity-planning.md) | Throughput formulas, sizing guide, profile comparison |
| [09-faq.md](09-faq.md) | Frequently asked questions: design choices, edge cases |
| [10-publisher-module.md](10-publisher-module.md) | Publisher design: modes, payload templates, rate limiter, combined usage |
| [11-virtual-threads.md](11-virtual-threads.md) | VT theory, lifecycle, parking vs blocking, pinning (detection + fixes), JDK notes |
| [12-infrastructure-docker.md](12-infrastructure-docker.md) | C4 diagrams, Docker Compose stack, AMPS setup, PostgreSQL, Grafana/Loki observability |
| [13-docker-cheatsheet.md](13-docker-cheatsheet.md) | Docker & Compose quick-reference: build, run, logs, exec, volume, network commands |
| [14-observability-cheatsheet.md](14-observability-cheatsheet.md) | Prometheus PromQL, Grafana dashboards, Loki LogQL, Promtail — quick-reference |
| [15-smart-lifecycle.md](15-smart-lifecycle.md) | Spring SmartLifecycle theory: phase ordering, stop callback, patterns, pitfalls, project usage |
| [16-rate-limiters.md](16-rate-limiters.md) | Five rate-limiter algorithms, C4 diagrams, message flows, enterprise use cases, library guide |
| [17-how-to-run.md](17-how-to-run.md) | **Step-by-step run guide:** publisher + 2 multi-JVM subscribers via Docker, Windows Terminal, IntelliJ |
