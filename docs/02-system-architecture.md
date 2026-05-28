# 02 — System Architecture

## All Three Profiles — Side-by-Side

```text
                              AMPS Server
                       /queue/trades  [messages]
                               │
              ┌────────────────┼─────────────────────────────────┐
              │                │                                   │
   ┌──────────▼──────────┐  ┌──▼─────────────────┐  ┌────────────▼──────────────────────┐
   │  PROFILE:            │  │  PROFILE:           │  │  PROFILE:                          │
   │  single-subscriber   │  │  multi-subscriber   │  │  multi-jvm-subscriber             │
   │                      │  │                     │  │                                   │
   │  1 JVM               │  │  1 JVM              │  │  K JVMs (identical processes)      │
   │  1 HAClient          │  │  N HAClients        │  │  Each JVM: N HAClients             │
   │  1 subscriber thread │  │  N subscriber thds  │  │  Each JVM: N subscriber threads   │
   │  N virtual threads   │  │  M VTs/subscriber   │  │  Each JVM: M VTs/subscriber       │
   │  H2 or Postgres      │  │  H2 or Postgres     │  │  Shared PostgreSQL (mandatory)    │
   └──────────────────────┘  └─────────────────────┘  └───────────────────────────────────┘
```

---

## Component Architecture — All Profiles

The component graph is the same for all profiles. What changes is how many instances
of each component exist, and which Spring configuration class wires them.

```text
┌──────────────────────────────────────────────────────────────────────────────┐
│                              AMPS Server                                     │
│                  /queue/trades  [m01][m02][m03]...[mN]                       │
└───────────────────────────────────┬──────────────────────────────────────────┘
                                    │  TCP / AMPS protocol
                                    │  One or more HAClient connections
                                    ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                         Spring Boot Application (JVM)                         │
│                                                                               │
│  ┌─────────────────────────────────────────────────────────────────────────┐ │
│  │  Subscriber Layer (SmartLifecycle)                                       │ │
│  │                                                                          │ │
│  │  Single profile:   SingleAmpsSubscriber   (1 platform thread)           │ │
│  │  Multi profiles:   MultiAmpsSubscriberPool (N platform threads)         │ │
│  │                                                                          │ │
│  │  for each arriving message:                                              │ │
│  │      1. semaphore.acquire()   ← blocks if VT slots full (backpressure)  │ │
│  │      2. executor.submit(task) ← O(1), non-blocking                      │ │
│  └──────────────────────────────┬───────────────────────────────────────── ┘ │
│                                  │ submit virtual thread task                 │
│                                  ▼                                            │
│  ┌─────────────────────────────────────────────────────────────────────────┐ │
│  │  Service Layer (MessageDispatchService)                                  │ │
│  │  ExecutorService: Executors.newVirtualThreadPerTaskExecutor()            │ │
│  │                                                                          │ │
│  │  VT-1: processAndAck(msg1, haClient)                                    │ │
│  │  VT-2: processAndAck(msg2, haClient)                                    │ │
│  │  VT-N: processAndAck(msgN, haClient)                                    │ │
│  └──────────────────────────────┬───────────────────────────────────────── ┘ │
│                                  │ process(message)                           │
│                                  ▼                                            │
│  ┌─────────────────────────────────────────────────────────────────────────┐ │
│  │  Processor Layer (MessageProcessor)                                      │ │
│  │  @Transactional                                                          │ │
│  │                                                                          │ │
│  │  1. Check idempotency (existing record?)                                 │ │
│  │  2. Validate payload                                                     │ │
│  │  3. Execute business logic                                               │ │
│  │  4. Persist ProcessedMessage (INSERT or UPDATE)                          │ │
│  │  5. If retry count exceeded → mark DISCARDED, return "discard" signal   │ │
│  └──────────────────────────────┬───────────────────────────────────────── ┘ │
│                                  │  JPA / HikariCP                            │
│                                  ▼                                            │
│  ┌─────────────────────────────────────────────────────────────────────────┐ │
│  │  Repository Layer (ProcessedMessageRepository)                           │ │
│  │  Spring Data JPA                                                         │ │
│  └──────────────────────────────┬───────────────────────────────────────── ┘ │
│                                  │  JDBC                                      │
│                                  ▼                                            │
│              ┌────────────────────────────────────┐                          │
│              │  HikariCP Connection Pool           │                          │
│              │  (bounded: 10–20 connections)       │                          │
│              └────────────────────┬───────────────┘                          │
│                                   │                                           │
│              ┌────────────────────▼───────────────┐                          │
│              │  Database                           │                          │
│              │  H2 (single-subscriber dev)        │                          │
│              │  PostgreSQL (multi-jvm prod)       │                          │
│              └─────────────────────────────────── ┘                          │
└──────────────────────────────────────────────────────────────────────────────┘
```

---

## Concurrency Model — One Message End-to-End

```text
AMPS Queue          Subscriber Thread        Semaphore        VT-n          DB
    │                       │                    │               │            │
    │── deliver msg ────────▶                    │               │            │
    │   (leased, TTL starts) │                   │               │            │
    │                        │                   │               │            │
    │                        │── acquire() ─────▶│               │            │
    │                        │   blocks if        │               │            │
    │                        │   VT slots full    │               │            │
    │                        │◀── permit ─────────│               │            │
    │                        │                   │               │            │
    │                        │── submit(task) ─────────────────▶ │            │
    │                        │   (returns O(1))   │               │            │
    │                        │                   │               │            │
    │── deliver next msg ────▶                   │               │            │
    │   (subscriber unblocks  │                   │  (VT runs)    │            │
    │    immediately after    │                   │  1. validate  │            │
    │    submit returns)      │                   │  2. parse     │            │
    │                        │                   │  3. INSERT ───────────────▶│
    │                        │                   │  4. ACK msg ──────────────▶│
    │                        │                   │  5. release() │            │
    │                        │                   │◀─ permit ─────│            │
    │                        │                   │  (slot freed) │            │
```

---

## Backpressure Model

The `Semaphore` is the backpressure gate. Its permits equal the maximum number of
messages that can be in-flight concurrently.

```text
                      ┌───────────────────────────────────┐
                      │      Semaphore(maxConcurrency)    │
                      │   e.g. maxConcurrency = 200       │
                      └──────────────┬────────────────────┘
                                     │
              ┌──────────────────────▼──────────────────────────┐
              │  semaphore.acquire()  — called by subscriber     │
              │  thread BEFORE submitting to VT executor         │
              │                                                   │
              │  IF permits > 0   → proceed immediately           │
              │  IF permits == 0  → BLOCK subscriber thread      │
              │    ↳ AMPS message delivery naturally slows down  │
              │    ↳ Messages remain in AMPS queue server-side   │
              │    ↳ No data loss, no OOM from task queue growth │
              └─────────────────────────────────────────────────┘

                            semaphore.release()
                  called inside the VT in the `finally` block
                  → frees one slot → subscriber thread unblocks
```

**Why this works:** When the subscriber thread is blocked on `semaphore.acquire()`,
it cannot call `stream.next()` on the AMPS `MessageStream`. AMPS sees no active read
on this connection and stops delivering new messages. Messages pile up safely in the
AMPS server queue, not in JVM heap.

---

## Virtual Thread Model

```text
Platform Thread (OS-scheduled, ~1 MB stack)
  └── carries Virtual Thread (JVM-scheduled, ~1-2 KB heap)

When a virtual thread does blocking I/O (JDBC getConnection, INSERT):
  VT parks itself on the heap (stores continuation)
  Carrier platform thread is FREED to run another VT
  (later) DB responds → VT is resumed on any available carrier

Effect: 10 platform threads can run 1,000+ VTs concurrently.
        20 HikariCP connections can serve 200+ concurrent VTs
        (the extra 180 VTs park while waiting for a connection).
```

### Thread Pinning (Important on JDK 21)

Virtual threads are **pinned** (cannot unmount from carrier) inside `synchronized` blocks.
AMPS client and older JDBC drivers use `synchronized` internally.

```text
Mitigation strategy:
  1. Use java.util.concurrent.locks.ReentrantLock in your own code (not synchronized)
  2. Add JVM flag: -Djdk.tracePinnedThreads=full  (detects pinning during dev)
  3. Keep HikariCP pool generous enough to absorb pinned carrier threads
     (e.g. if 5% of VTs pin, set pool-size = expected-pinned-VTs + headroom)
  4. JDK 24+ resolves most driver-level pinning; until then size pools conservatively
```

---

## Profile-Conditional Bean Wiring

```text
                    @SpringBootApplication
                           │
              ┌────────────┼────────────┐
              │            │            │
   @Profile   │  @Profile  │  @Profile  │
   ("single-  │  ("multi-  │  ("multi-  │
   subscriber")│  subscriber")│  jvm-   │
              │            │  subscriber")
              ▼            ▼            ▼
  SingleSub   MultiSub   MultiJvmSub
  Config.java Config.java Config.java
              │            │            │
              └────────────┼────────────┘
                           │
                   CommonConfig.java  ← no @Profile (always loaded)
                   (shared executor, metrics registry, health indicator)
```

**`CommonConfig.java`** always loads:
- `ExecutorService` — `newVirtualThreadPerTaskExecutor()`
- Micrometer `MeterRegistry` hooks
- `AmpsHealthIndicator`

**Profile-specific configs** create:
- `HAClient` instance(s)
- `Semaphore` instance(s) (with correct permit count)
- The appropriate subscriber bean (`SingleAmpsSubscriber` or `MultiAmpsSubscriberPool`)

---

## Data Flow — Idempotency and Retry

```text
Message arrives
      │
      ▼
MessageProcessor.process(message)
      │
      ├─ repo.findByMessageId(messageId)
      │         │
      │    ┌────▼──────────────────────────────────────────────────────┐
      │    │  Record EXISTS?                                            │
      │    │                                                            │
      │    │  status=PROCESSED  → return DUPLICATE (caller ACKs)       │
      │    │  status=DISCARDED  → return DISCARD   (caller ACKs)       │
      │    │  status=FAILED     → check retryCount                     │
      │    │      retryCount < maxRetries → attempt processing again   │
      │    │      retryCount >= maxRetries → mark DISCARDED, ACK       │
      │    │                                                            │
      │    │  Record NOT EXISTS → first delivery → process normally    │
      │    └─────────────────────────────────────────────────────────-─┘
      │
      ├─ Execute business logic
      │         │
      │    ┌────▼───────────────┐
      │    │ Success?           │
      │    │                    │
      │    │ YES → save         │
      │    │  ProcessedMessage  │
      │    │  status=PROCESSED  │
      │    │  → caller ACKs     │
      │    │                    │
      │    │ NO  → update/save  │
      │    │  ProcessedMessage  │
      │    │  status=FAILED     │
      │    │  retryCount++      │
      │    │  → caller NO-ACK   │
      │    │  → AMPS re-delivers│
      │    └────────────────────┘
```

---

## Observability Architecture

```text
Spring Boot Application
        │
        ├── /actuator/health
        │       └── AmpsHealthIndicator
        │               reports UP/DOWN per HAClient connection
        │
        ├── /actuator/metrics
        │       └── Micrometer counters, timers, gauges
        │
        └── /actuator/prometheus
                └── Scrape endpoint for Prometheus / Grafana

Metrics emitted:
  amps.messages.received   [counter]  tags: profile, topic
  amps.messages.processed  [counter]  tags: profile, topic
  amps.messages.failed     [counter]  tags: profile, topic
  amps.messages.duplicate  [counter]  tags: profile
  amps.messages.discarded  [counter]  tags: profile (max retries reached, ACK'd)
  amps.processing.time     [timer]    tags: profile, topic  (p50/p95/p99)
  amps.subscriber.active   [gauge]    tags: profile  (number of running subscriber threads)
  amps.semaphore.available [gauge]    tags: profile  (free semaphore permits)

  # Publisher-specific (message-publisher profile)
  amps.publisher.messages.published  [counter]  cumulative published count
  amps.publisher.messages.errors     [counter]  cumulative publish failures
  amps.publisher.active.workers      [gauge]    running VT worker count
  amps.publisher.publish.duration    [timer]    per-publish latency (p50/p95/p99)
```

---

## Observability Stack (Docker)

```text
┌─────────────────────────────────────────────────────────┐
│             Observability Data Flow                      │
│                                                         │
│  Spring Boot                                            │
│    /actuator/prometheus  ──scrape every 15s──▶ Prometheus │
│    stdout (JSON via                                      │
│    LogstashEncoder)      ──tail──▶ Promtail ──push──▶ Loki │
│                                                         │
│  Grafana  ◀──datasource──  Prometheus  (metrics)        │
│  Grafana  ◀──datasource──  Loki        (logs)           │
└─────────────────────────────────────────────────────────┘
```

### MDC fields propagated to every log line

| MDC Key | Set by | Example value |
|---|---|---|
| `messageId` | `MessageDispatchService` | `0000000000001\|1\|42` |
| `topic` | `MessageDispatchService` | `/queue/trades` |
| `subscriberIndex` | `SingleAmpsSubscriber` / `MultiAmpsSubscriberPool` | `2` |
| `publisherWorker` | `AmpsMessagePublisher` | `3` |

Use `messageId` to trace one message across publisher → subscriber → DB in Loki:
```logql
{service="amps-queue-concurrency"} | json | messageId = "<bookmark>"
```

### Structured JSON logging

Activated by adding `docker-logging` to `SPRING_PROFILES_ACTIVE`.
`logback-spring.xml` switches the appender from human-readable pattern to
`LogstashEncoder` (JSON), which Promtail parses and ships to Loki.

> Full setup: see [`docs/12-infrastructure-docker.md`](12-infrastructure-docker.md)
