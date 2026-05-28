# 05 — Profile: `multi-jvm-subscriber`

## When to Use This Profile

| Condition | Recommendation |
|---|---|
| Production workload requiring high availability | ✅ Use this profile |
| Throughput exceeds single JVM capacity | ✅ |
| JVM crash must not halt message processing | ✅ |
| Need to scale out by adding more nodes | ✅ |
| Can afford shared PostgreSQL | ✅ (required) |
| Development / simple testing | ❌ Use `single-subscriber` |
| Single-node, no HA requirement | ❌ Use `multi-subscriber` |

---

## Core Concept

`multi-jvm-subscriber` is **horizontal scaling** of `multi-subscriber`.
Each JVM process runs the same code, pointed at the same AMPS server and the same
shared PostgreSQL database. AMPS treats every HAClient connection from every JVM
as a competing consumer.

**The only shared state is the database.**
All other state (JVM heap, subscriber threads, semaphores, VTs, bookmark files) is
local to each JVM and completely independent.

---

## Deployment Topology

```text
                              AMPS Server
                    /queue/trades  [m001 .. mN]
                              │
          ┌───────────────────┼──────────────────────┐
          │ 3 TCP conns from  │ 3 TCP conns from      │ 3 TCP conns from
          │ JVM-1 (node1)     │ JVM-2 (node2)         │ JVM-3 (node3)
          ▼                   ▼                        ▼
┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐
│    JVM-1         │  │    JVM-2         │  │    JVM-3         │
│    node1         │  │    node2         │  │    node3         │
│                  │  │                  │  │                  │
│  [sub-1] Sem(M)  │  │  [sub-1] Sem(M)  │  │  [sub-1] Sem(M)  │
│  [sub-2] Sem(M)  │  │  [sub-2] Sem(M)  │  │  [sub-2] Sem(M)  │
│  [sub-3] Sem(M)  │  │  [sub-3] Sem(M)  │  │  [sub-3] Sem(M)  │
│                  │  │                  │  │                  │
│  VT pool (local) │  │  VT pool (local) │  │  VT pool (local) │
│  HikariCP (local)│  │  HikariCP (local)│  │  HikariCP (local)│
│                  │  │                  │  │                  │
│  bookmarks:      │  │  bookmarks:      │  │  bookmarks:      │
│  node1-sub-1.log │  │  node2-sub-1.log │  │  node3-sub-1.log │
│  node1-sub-2.log │  │  node2-sub-2.log │  │  node3-sub-2.log │
│  node1-sub-3.log │  │  node2-sub-3.log │  │  node3-sub-3.log │
└────────┬─────────┘  └────────┬─────────┘  └────────┬─────────┘
         │                     │                      │
         └─────────────────────┼──────────────────────┘
                               │  JDBC / TCP
                    ┌──────────▼──────────┐
                    │  Shared PostgreSQL   │
                    │  TABLE: processed_messages
                    │  UNIQUE(message_id) ← cross-JVM idempotency key
                    └─────────────────────┘
```

---

## Configuration — `application-multi-jvm-subscriber.yaml`

```yaml
spring:
  application:
    name: amps-queue-multi-jvm-subscriber

  datasource:
    # Shared PostgreSQL — SAME URL on all JVM instances
    url: jdbc:postgresql://${DB_HOST:db-server}:5432/ampsdb
    username: ${DB_USER:amps_user}
    password: ${DB_PASSWORD}
    driver-class-name: org.postgresql.Driver
    hikari:
      pool-name: HikariPool-MultiJvm
      # 3 subscribers × 50 VTs each = 150 max concurrent DB ops per JVM
      # VTs park while waiting → 20 connections serve 150 VTs safely
      maximum-pool-size: 20
      minimum-idle: 5
      connection-timeout: 3000
      idle-timeout: 600000
      # Recommended for PostgreSQL with virtual threads:
      keepalive-time: 30000

  jpa:
    hibernate:
      # NEVER use update in production — schema managed externally (Flyway/Liquibase)
      ddl-auto: validate
    show-sql: false
    open-in-view: false
    properties:
      hibernate:
        dialect: org.hibernate.dialect.PostgreSQLDialect
        jdbc:
          batch_size: 50

amps:
  server:
    # Supports HA cluster: comma-separate multiple URIs
    uri: tcp://${AMPS_HOST:amps-server}:9004/amps/json
  queue:
    topic: /queue/trades
    filter: ""
    lease-timeout-ms: 5000
  consumer:
    subscriber-count: 3                       # N parallel connections per JVM
    max-concurrency-per-subscriber: 50        # semaphore permits per subscriber
    max-retries: 3
    bookmark-dir: ${user.home}/.amps          # directory for bookmark log files

management:
  endpoints:
    web:
      exposure:
        include: health,info,metrics,prometheus
  endpoint:
    health:
      show-details: always
      probes:
        enabled: true    # enables /actuator/health/liveness + /actuator/health/readiness

logging:
  level:
    com.shan.mq.amps: INFO
```

---

## How Multi-JVM Differs from Multi-Subscriber

The Java code is nearly identical. What differs:

| Aspect | multi-subscriber | multi-jvm-subscriber |
|---|---|---|
| Database | H2 (dev) or PostgreSQL | **PostgreSQL required** (shared state) |
| Client name | `{app}-{hostname}-sub-{i}` | Same pattern — hostname uniqueness is critical |
| Bookmark files | Local dir | Local dir (per host — each JVM has its own) |
| `ddl-auto` | `update` | `validate` (schema managed by migration tool) |
| Idempotency | Important | **Critical** — cross-JVM duplicate delivery is real |
| Crash recovery | JVM restart recovers | Other JVM(s) continue; crashed JVM replays on restart |

The `MultiAmpsSubscriberPool` component (annotated `@Profile({"multi-subscriber", "multi-jvm-subscriber"})`)
is **reused** — no code duplication.

---

## Components

### `MultiJvmSubscriberConfig.java` — `@Profile("multi-jvm-subscriber")`

**Responsibility:** Same as `MultiSubscriberConfig` but enforces hostname-based client naming
and uses the configured bookmark directory.

```text
Beans created:

  List<SubscriberContext> subscriberContexts()
    hostname = InetAddress.getLocalHost().getHostName().sanitize()
    for i = 1..subscriberCount:
        clientName    = "{app}-{hostname}-sub-{i}"
        bookmarkFile  = {bookmarkDir}/{clientName}.log
                        (creates directory if absent)
        HAClient      = new HAClient(clientName)
                        .setServerChooser(DefaultServerChooser([ampsUri]))
                        .setBookmarkStore(LoggedBookmarkStore(bookmarkFile))
                        .setReconnectDelay(ExponentialDelayStrategy(50, 5000))
                        .connect(ampsUri)
        Semaphore     = new Semaphore(maxConcurrencyPerSubscriber)
        contexts.add(new SubscriberContext(haClient, semaphore, i))

  Note: MultiAmpsSubscriberPool is activated by @Profile("multi-jvm-subscriber")
        — the same class as for multi-subscriber profile.
```

---

## Cross-JVM Message Distribution

```text
9 active AMPS connections (3 JVMs × 3 subscribers each):

AMPS dispatches 9 messages simultaneously:
  m01 → JVM-1 / conn node1-sub-1    m04 → JVM-1 / conn node1-sub-2
  m02 → JVM-2 / conn node2-sub-1    m05 → JVM-2 / conn node2-sub-2
  m03 → JVM-3 / conn node3-sub-1    m06 → JVM-3 / conn node3-sub-2
                                     m07 → JVM-1 / conn node1-sub-3
                                     m08 → JVM-2 / conn node2-sub-3
                                     m09 → JVM-3 / conn node3-sub-3

No JVM ever sees another JVM's leased message.
Each message is processed by exactly one virtual thread in exactly one JVM.
```

---

## Crash Recovery — Step by Step

```text
Scenario: JVM-2 processes m05, saves to PostgreSQL, crashes before ACK

Step 1: m05 leased to JVM-2 / node2-sub-2
Step 2: JVM-2 VT executes business logic → INSERT ProcessedMessage(m05) to PostgreSQL
Step 3: JVM-2 crashes (power failure, OOM, kill -9)
         → TCP connection drops → AMPS server detects disconnect
Step 4: AMPS lease TTL for m05 expires (~5s after last heartbeat)
Step 5: m05 transitions from LEASED → AVAILABLE
Step 6: AMPS re-delivers m05 to next available connection (say JVM-1 / node1-sub-1)
Step 7: JVM-1 VT calls processor.process(m05)
Step 8: processor.process() queries: repo.findByMessageId("m05-bookmark")
          → record EXISTS, status=PROCESSED
          → returns DUPLICATE
Step 9: MessageDispatchService receives DUPLICATE → calls haClient.ack(m05)
Step 10: AMPS marks m05 as ACKED → permanently removed from queue

Result: One record in DB (from Step 2). Correct outcome.
        No duplicate row. No lost message. No manual intervention.
```

---

## JVM-1 Restart Recovery

```text
Scenario: JVM-1 restarts after crash

Step 1: MultiJvmSubscriberConfig creates HAClients with LoggedBookmarkStore
Step 2: LoggedBookmarkStore reads bookmark log: ~/.amps/amps-queue-{host}-sub-{n}.log
Step 3: HAClient connects to AMPS and resumes from last ACK'd bookmark
Step 4: AMPS replays all messages that were delivered but not ACK'd by this client
          - m07, m08, m09 (if they were delivered but not ACK'd before crash)
Step 5: These messages are processed again → processor.process() finds them
          either already in PROCESSED state → DUPLICATE → ACK (safe)
          or not yet processed → process normally → INSERT → ACK

Result: Zero message loss. Automatic recovery without manual intervention.
```

---

## Thread and Resource Count (Per JVM, 3 subscribers, 50 VTs each)

```text
Component                          Per JVM   System (3 JVMs)   Notes
─────────────────────────────────  ───────   ───────────────   ──────────────────────────
AMPS TCP connections                     3              9       One per HAClient
Platform threads (subscriber)            3              9       One per subscriber context
Semaphores                               3              9       Independent per subscriber
Max VTs per subscriber                  50            150       Semaphore-controlled
Max total in-flight VTs                150            450       Per JVM × JVM count
HikariCP DB connections                 20             60       PostgreSQL connection pool
Bookmark log files                       3              9       Per client, per host
```

---

## Client Naming — Globally Unique

```text
3 JVMs × 3 subscribers = 9 unique client names:

JVM-1 (node1):  amps-queue-multi-jvm-node1-sub-1
                amps-queue-multi-jvm-node1-sub-2
                amps-queue-multi-jvm-node1-sub-3

JVM-2 (node2):  amps-queue-multi-jvm-node2-sub-1
                amps-queue-multi-jvm-node2-sub-2
                amps-queue-multi-jvm-node2-sub-3

JVM-3 (node3):  amps-queue-multi-jvm-node3-sub-1
                amps-queue-multi-jvm-node3-sub-2
                amps-queue-multi-jvm-node3-sub-3

Hostname sanitization:
  InetAddress.getLocalHost().getHostName()
      .toLowerCase()
      .replaceAll("[^a-z0-9-]", "-")   ← safe for file names and AMPS client names
```

---

## Throughput and Scaling

```text
Formula:
  throughput_total = JVMs × subscribers × (maxConcurrencyPerSub / avgProcessingMs × 1000)

Example (3 JVMs, 3 subscribers, 50 VTs, 50ms avg processing):
  per subscriber = 50 / 50 × 1000 = 1,000 msg/s
  per JVM        = 3 × 1,000       = 3,000 msg/s
  total          = 3 × 3,000       = 9,000 msg/s (theoretical)
  practical(80%) : ~7,200 msg/s

Horizontal scaling:
  Add a 4th JVM → total = 4 × 3,000 = 12,000 msg/s
  No code changes. No AMPS configuration changes.
  Just start another JVM with -Dspring.profiles.active=multi-jvm-subscriber

Vertical scaling (within a JVM):
  Increase subscriber-count from 3 to 5 → per JVM = 5,000 msg/s
  Increase max-concurrency-per-subscriber from 50 to 100
```

---

## Operational Notes

```text
Schema migration:
  Use Flyway or Liquibase — never ddl-auto=update in production.
  All JVMs share the same schema; migration must run before ANY JVM starts.

Connection pool sizing:
  HikariCP max-pool-size should be:
      (subscriberCount × maxConcurrencyPerSub) / parkingRatio
  Where parkingRatio ≈ 5–10 (VTs spend 80–90% of time waiting for DB)
  Safe formula: max-pool-size = subscriberCount × 5

Monitor PostgreSQL max_connections:
  total_connections = JVMs × maxPoolSize
  e.g. 3 JVMs × 20 = 60 connections used; ensure postgres max_connections > 60 + headroom

AMPS server capacity:
  AMPS can handle thousands of concurrent connections.
  Practical limit is network bandwidth and AMPS server CPU.
  Monitor AMPS admin console: connected clients, queue depth, lease expirations.

Kubernetes deployment:
  Each pod = one JVM with multi-jvm-subscriber profile
  Horizontal Pod Autoscaler (HPA) scales pods based on:
    - amps.queue.depth metric (custom metric via Prometheus)
    - CPU utilization
  Readiness probe: /actuator/health/readiness
  Liveness probe:  /actuator/health/liveness
```
