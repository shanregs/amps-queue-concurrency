# 04 — Profile: `multi-subscriber`

## When to Use This Profile

| Condition | Recommendation |
|---|---|
| Single JVM but throughput exceeds single-subscriber capacity | ✅ Use this profile |
| Want vertical scaling (more CPU cores, higher message rate) | ✅ |
| Still single host — no HA requirement | ✅ |
| Development environment testing multi-subscriber concurrency | ✅ |
| Need fault tolerance across multiple hosts | ❌ Use `multi-jvm-subscriber` |
| Message volume is low (< 500 msg/s) | ❌ Use `single-subscriber` instead |

---

## Core Concept

Where `single-subscriber` has **one TCP connection → one receive loop → N VTs**,
`multi-subscriber` has **N TCP connections → N receive loops → N × M VTs**.

AMPS distributes messages across all N connections using the competing-consumer model.
Each connection is an independent competing consumer — AMPS leases each message
to exactly one connection, so no two subscriber threads receive the same message.

```text
Before (single-subscriber):         After (multi-subscriber):

  AMPS  ──▶ conn-1                   AMPS  ──▶ conn-1 → sub-1 thread → VTs
           sub-1 thread               │   ──▶ conn-2 → sub-2 thread → VTs
           VT pool                    └── ──▶ conn-3 → sub-3 thread → VTs

  Throughput: X msg/s                Throughput: up to N×X msg/s
```

---

## Architecture

```text
                          AMPS Server
                    /queue/trades  [m01 .. mM]
                              │
          ┌───────────────────┼──────────────────┐
          │                   │                  │
          │ TCP conn-1         │ TCP conn-2        │ TCP conn-3
          ▼                   ▼                  ▼
┌──────────────────────────────────────────────────────────────────────┐
│                 Single JVM — Spring Boot Application                  │
│                                                                       │
│  ┌────────────────┐  ┌────────────────┐  ┌────────────────┐         │
│  │  HAClient-1    │  │  HAClient-2    │  │  HAClient-3    │         │
│  │  name: ...-1   │  │  name: ...-2   │  │  name: ...-3   │         │
│  │  bookmark-1.log│  │  bookmark-2.log│  │  bookmark-3.log│         │
│  └───────┬────────┘  └───────┬────────┘  └───────┬────────┘         │
│          │                   │                   │                   │
│  ┌───────▼────────┐  ┌───────▼────────┐  ┌───────▼────────┐         │
│  │  Subscriber-1  │  │  Subscriber-2  │  │  Subscriber-3  │         │
│  │  platform thd  │  │  platform thd  │  │  platform thd  │         │
│  │  Semaphore(M)  │  │  Semaphore(M)  │  │  Semaphore(M)  │         │
│  │  (independent) │  │  (independent) │  │  (independent) │         │
│  └───────┬────────┘  └───────┬────────┘  └───────┬────────┘         │
│          │                   │                   │                   │
│          └───────────────────┼───────────────────┘                   │
│                              │ dispatch to shared VT executor         │
│                              ▼                                        │
│  ┌────────────────────────────────────────────────────────────────┐  │
│  │  Shared VirtualThreadExecutor                                   │  │
│  │  Executors.newVirtualThreadPerTaskExecutor()                    │  │
│  │                                                                 │  │
│  │  VTs from sub-1:  process(m01), process(m04), process(m07)...  │  │
│  │  VTs from sub-2:  process(m02), process(m05), process(m08)...  │  │
│  │  VTs from sub-3:  process(m03), process(m06), process(m09)...  │  │
│  └────────────────────────────────────────────────────────────────┘  │
│                              │                                        │
│               ┌──────────────▼──────────────┐                        │
│               │  HikariCP (pool-size = N×M÷10) │                     │
│               └──────────────┬──────────────┘                        │
│               ┌──────────────▼──────────────┐                        │
│               │  H2 (dev) or PostgreSQL      │                        │
│               │  UNIQUE(message_id)          │                        │
│               └─────────────────────────────┘                        │
└──────────────────────────────────────────────────────────────────────┘
```

---

## Configuration — `application-multi-subscriber.yaml`

```yaml
spring:
  application:
    name: amps-queue-multi-subscriber

  datasource:
    # H2 for development; switch to PostgreSQL for production
    url: jdbc:h2:file:./data/amps-multi;AUTO_SERVER=TRUE
    driver-class-name: org.h2.Driver
    username: sa
    password: ""
    hikari:
      pool-name: HikariPool-MultiSub
      # Rule: pool-size ≥ (subscriberCount × maxConcurrencyPerSubscriber) / 10
      # e.g. 3 subscribers × 50 VTs each = 150 max VTs; pool-size = 20 is safe
      # because VTs PARK (not block) while waiting for a connection
      maximum-pool-size: 20
      minimum-idle: 5
      connection-timeout: 3000

  jpa:
    hibernate:
      ddl-auto: update
    show-sql: false
    open-in-view: false

amps:
  server:
    uri: tcp://172.21.12.69:9004/amps/json
  queue:
    topic: /queue/trades
    filter: ""
    lease-timeout-ms: 5000
  consumer:
    subscriber-count: 3                     # N parallel HAClient connections
    max-concurrency-per-subscriber: 50      # semaphore permits per subscriber
    max-retries: 3

management:
  endpoints:
    web:
      exposure:
        include: health,info,metrics,prometheus
  endpoint:
    health:
      show-details: always
```

---

## Components

### `MultiSubscriberConfig.java` — `@Profile("multi-subscriber")`

**Responsibility:** Creates N `HAClient` instances + N `Semaphore` instances,
wrapped in `SubscriberContext` records.

```text
Beans created:

  List<SubscriberContext> subscriberContexts()
    - loop i = 1..subscriberCount:
        clientName = "{app}-{hostname}-sub-{i}"
        bookmarkFile = ~/.amps/{clientName}.log
        HAClient: DefaultServerChooser, LoggedBookmarkStore, ExponentialDelayStrategy
        Semaphore: maxConcurrencyPerSubscriber permits
        SubscriberContext: (haClient, semaphore, index=i)
    - destroyMethod = "" (pool lifecycle managed by MultiAmpsSubscriberPool)

  ExecutorService sharedVirtualThreadExecutor()
    - Executors.newVirtualThreadPerTaskExecutor()
    - Shared across all subscribers in this JVM
    - destroyMethod = "shutdown"
```

### `SubscriberContext.java` — Java Record

```text
public record SubscriberContext(
    HAClient  haClient,
    Semaphore semaphore,
    int       index
) {}

Purpose:
  - Bundles all state for one subscriber thread
  - Immutable record prevents accidental cross-subscriber state sharing
  - haClient: the connection this subscriber receives from AND ACKs on
  - semaphore: this subscriber's independent backpressure gate
  - index: for logging and platform thread naming ("amps-sub-{index}")
```

**Why per-subscriber semaphores?**

```text
Shared semaphore (wrong):
  Total permits = 150 (3 × 50)
  Subscriber-1 acquires 50 → subscriber-2 acquires 50 → subscriber-3 acquires 50
  → All 150 used → ALL three subscriber threads block simultaneously
  → AMPS effectively sees zero active consumers

Per-subscriber semaphore (correct):
  Each subscriber has its own 50-permit semaphore
  Subscriber-1 fills up → only sub-1 blocks → sub-2 and sub-3 keep processing
  → AMPS continues delivering to the other two connections
```

---

### `MultiAmpsSubscriberPool.java` — `@Profile({"multi-subscriber", "multi-jvm-subscriber"})`, `SmartLifecycle`

**Responsibility:** Manages N platform threads (one per `SubscriberContext`).

```text
start():
  for each ctx in contexts:
      Thread.ofPlatform()
          .name("amps-sub-" + ctx.index())
          .daemon(false)
          .start(() → receiveLoop(ctx))

receiveLoop(SubscriberContext ctx):
  try (MessageStream stream = ctx.haClient().bookmarkSubscribe(topic, filter)):
      for (Message msg : stream):
          if (!running) break
          dispatchService.dispatch(msg, ctx)
  catch (InterruptedException) → clean shutdown
  catch (Exception) → log, thread exits (HAClient reconnects automatically)

stop():
  running = false
  interrupt all platform threads
  for each ctx: ctx.haClient().close()     ← close after threads stopped

getPhase(): Integer.MAX_VALUE
```

---

### `MessageDispatchService.java` (multi-subscriber variant)

```text
dispatch(Message msg, SubscriberContext ctx):
  ctx.semaphore().acquire()   ← blocks THIS subscriber's platform thread only
  sharedExecutor.submit(() → {
      try:
          result = processor.process(msg)
          ctx.haClient().ack(msg)   ← always ACK on the receiving connection
                                    ← even for DISCARD / DUPLICATE
          if result == FAIL → throw (prevents ack call above — see note)
      catch (Exception) → log error, no ACK → AMPS re-delivers
      finally → ctx.semaphore().release()
  })
```

**Note on ACK strategy:**
- `PROCESSED` (new message, success) → ACK
- `DUPLICATE` (already processed) → ACK (idempotent; record exists)
- `DISCARD` (max retries exceeded) → ACK (log severe warning)
- `FAIL` (processing error, retry available) → NO ACK → AMPS re-delivers after TTL

---

## Message Distribution Across Subscribers

```text
AMPS server distributes messages round-robin (or load-balanced) across all
active connections. With 3 subscribers:

  Time t=0:  m01 → sub-1 (conn-1 is ready first)
             m02 → sub-2 (conn-2 is also available)
             m03 → sub-3 (conn-3 is also available)
  Time t=1:  m04 → sub-1 (m01 ACK'd, conn-1 ready again)
             m05 → sub-2
             m06 → sub-3

No two subscriber threads ever hold the same message.
AMPS enforces this at the server via the lease model.
```

---

## Thread and Resource Count (3 subscribers, 50 VTs each)

```text
Component                              Count    Notes
─────────────────────────────────────  ──────   ──────────────────────────────────────────
AMPS TCP connections                       3    One HAClient per subscriber
Platform threads (subscriber)              3    "amps-sub-1", "amps-sub-2", "amps-sub-3"
Semaphores                                 3    One per subscriber, 50 permits each
Max VTs per subscriber                    50    Controlled by per-subscriber semaphore
Max total in-flight VTs                  150    3 × 50 — share one VT executor
HikariCP DB connections                   20    VTs park waiting; 20 serves 150 safely
Bookmark log files                         3    ~/.amps/{app}-{host}-sub-{1,2,3}.log
```

---

## Throughput Estimate

```text
Formula: throughput = subscriberCount × (maxConcurrencyPerSub / avgProcessingTimeMs × 1000)

With subscriberCount=3, maxConcurrencyPerSub=50, avgProcessingTime=50ms:
  per subscriber = 50 / 50 × 1000 = 1,000 msg/s
  total          = 3 × 1,000       = 3,000 msg/s (theoretical max)
  practical (80%): ~2,400 msg/s

Bottleneck options:
  - AMPS delivery rate per connection (increase subscriber-count)
  - DB connection pool (VTs parking → increase maximum-pool-size)
  - Business logic complexity (reduce avgProcessingTime)
```

---

## Graceful Shutdown Sequence

```text
Step 1: SmartLifecycle.stop() called (phase MAX_VALUE → stops first)
Step 2: MultiAmpsSubscriberPool sets running=false
Step 3: All platform threads interrupted → exit their receiveLoops
Step 4: All MessageStreams closed → AMPS subscription cancelled on each connection
Step 5: In-flight VTs continue running:
          Each VT calls processor.process() → DB write → client.ack() → semaphore.release()
Step 6: MultiAmpsSubscriberPool.stop() calls ctx.haClient().close() for each context
          (called after threads are interrupted, not before)
Step 7: sharedVirtualThreadExecutor.shutdown() + awaitTermination() (CommonConfig)
Step 8: HikariCP pool drains → all DB connections closed
Step 9: JVM exits

Graceful drain guarantee:
  The semaphore tracks in-flight VTs.
  VTs that were submitted before stop() was called finish normally.
  No messages are lost — they either get ACK'd or their lease expires and
  AMPS re-delivers to another consumer.
```
