# 10 — Publisher Module: `message-publisher` Profile

## Purpose

The `message-publisher` profile turns the application into a **simulation publisher**:
it connects to the same AMPS server as the subscribers and publishes configurable
messages to a queue at a controlled rate. It exists purely to generate test traffic —
there is no persistence, no idempotency check, and no retry obligation on the publisher side.

Use it to:
- Load-test subscriber profiles against realistic traffic volumes
- Develop subscriber logic without an external upstream system
- Benchmark backpressure, retry, and idempotency behaviour
- Run a fully self-contained simulation in a single JVM (`message-publisher` + `single-subscriber`)

---

## System Context Diagram

```text
┌─────────────────────────────────────────────────────────────────────────────────┐
│                        Simulation Environment                                   │
│                                                                                 │
│  ┌─────────────────────────────────────────┐                                   │
│  │   Publisher JVM                          │                                   │
│  │   (profile: message-publisher)           │                                   │
│  │                                          │                                   │
│  │   AmpsMessagePublisher                   │                                   │
│  │   ├─ VT-Worker-1 ─▶ publish(msg)         │                                   │
│  │   ├─ VT-Worker-2 ─▶ publish(msg)         │                                   │
│  │   └─ VT-Worker-N ─▶ publish(msg)         │                                   │
│  │              │                            │                                   │
│  │   HAClient (publisher)                   │                                   │
│  │   (1 TCP connection, thread-safe publish) │                                   │
│  └─────────────────┬────────────────────────┘                                   │
│                    │  TCP / AMPS protocol                                        │
│                    ▼                                                             │
│  ┌─────────────────────────────────────────┐                                   │
│  │               AMPS Server               │                                   │
│  │                                          │                                   │
│  │   /queue/trades                          │                                   │
│  │   [msg-001][msg-002][msg-003]...[msg-N]  │                                   │
│  │   (durable queue — leases messages out)  │                                   │
│  └──────────────────┬───────────────────────┘                                   │
│                     │  competing consumers                                       │
│     ┌───────────────┼──────────────────────┐                                   │
│     │               │                      │                                    │
│     ▼               ▼                      ▼                                    │
│  ┌──────────┐  ┌──────────┐  ┌──────────────────────┐                          │
│  │ Sub JVM1 │  │ Sub JVM2 │  │  Same JVM (combined) │                          │
│  │ (multi-  │  │ (multi-  │  │  message-publisher   │                          │
│  │  jvm)    │  │  jvm)    │  │  + single-subscriber │                          │
│  └──────────┘  └──────────┘  └──────────────────────┘                          │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## Publishing Modes

Four modes cover every simulation scenario:

```text
┌────────────────┬─────────────────────────────────────────────────────────────────┐
│ Mode           │ Behaviour                                                        │
├────────────────┼─────────────────────────────────────────────────────────────────┤
│ RATE_LIMITED   │ Publish at exactly N msg/s using a token-bucket limiter.         │
│                │ Best for: steady-state subscriber validation, SLA testing.       │
├────────────────┼─────────────────────────────────────────────────────────────────┤
│ BURST          │ Publish as fast as possible — no rate gate. Limited only         │
│                │ by AMPS server throughput and HAClient TCP throughput.           │
│                │ Best for: peak-load / stress testing, backpressure validation.  │
├────────────────┼─────────────────────────────────────────────────────────────────┤
│ FIXED_COUNT    │ Publish exactly `total-messages` messages then stop.             │
│                │ Application exits cleanly after last message is published.       │
│                │ Best for: reproducible integration tests, exact corpus tests.   │
├────────────────┼─────────────────────────────────────────────────────────────────┤
│ INFINITE       │ Publish indefinitely until JVM is shut down (SIGTERM/Ctrl+C).   │
│                │ Best for: long-running soak tests, monitoring dashboard demos.  │
└────────────────┴─────────────────────────────────────────────────────────────────┘
```

---

## Payload Templates

```text
┌──────────────┬──────────────────────────────────────────────────────────────────┐
│ Template     │ Sample JSON                                                       │
├──────────────┼──────────────────────────────────────────────────────────────────┤
│ TRADE        │ {                                                                  │
│              │   "tradeId":    "TRD-0000001",                                   │
│              │   "symbol":     "AAPL",                                           │
│              │   "side":       "BUY",                                            │
│              │   "quantity":   100,                                              │
│              │   "price":      182.34,                                           │
│              │   "currency":   "USD",                                            │
│              │   "tradeDate":  "2026-05-27",                                     │
│              │   "timestamp":  "2026-05-27T17:00:01.234Z",                       │
│              │   "publisherId":"pub-node1-vt2"                                   │
│              │ }                                                                  │
├──────────────┼──────────────────────────────────────────────────────────────────┤
│ ORDER        │ {                                                                  │
│              │   "orderId":    "ORD-0000001",                                   │
│              │   "instrument": "EURUSD",                                         │
│              │   "side":       "SELL",                                           │
│              │   "orderType":  "LIMIT",                                          │
│              │   "notional":   1000000,                                          │
│              │   "limitPrice": 1.0845,                                           │
│              │   "status":     "NEW",                                            │
│              │   "timestamp":  "2026-05-27T17:00:01.234Z",                       │
│              │   "publisherId":"pub-node1-vt1"                                   │
│              │ }                                                                  │
├──────────────┼──────────────────────────────────────────────────────────────────┤
│ RISK         │ {                                                                  │
│              │   "riskId":     "RSK-0000001",                                   │
│              │   "book":       "EQUITY-DESK-1",                                  │
│              │   "pnl":        12345.67,                                         │
│              │   "delta":      0.75,                                             │
│              │   "vega":       -234.5,                                           │
│              │   "timestamp":  "2026-05-27T17:00:01.234Z",                       │
│              │   "publisherId":"pub-node1-vt3"                                   │
│              │ }                                                                  │
├──────────────┼──────────────────────────────────────────────────────────────────┤
│ CUSTOM       │ Any JSON string provided via `custom-payload-template` config.    │
│              │ Supported placeholders:                                            │
│              │   {seq}       → monotonically increasing sequence number          │
│              │   {uuid}      → random UUID                                       │
│              │   {timestamp} → ISO-8601 UTC instant                              │
│              │   {random-int}→ random positive integer                           │
│              │   {symbol}    → random symbol from configured symbols list        │
│              │   {publisher} → publisherId tag (VT name + host)                 │
└──────────────┴──────────────────────────────────────────────────────────────────┘
```

---

## Internal Architecture

```text
┌─────────────────────────────────────────────────────────────────────────────┐
│                Publisher JVM  (profile: message-publisher)                   │
│                                                                               │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │  AmpsMessagePublisher  (@Component, SmartLifecycle, phase=MAX-1)     │   │
│  │                                                                       │   │
│  │  start()                                                              │   │
│  │   └── spawn N virtual thread workers (via publisherExecutor)         │   │
│  │        ┌──────────────────────────────────────────────────────────┐  │   │
│  │        │  PublishWorker (runs inside each virtual thread)          │  │   │
│  │        │                                                           │  │   │
│  │        │  loop until (messagesPublished >= total OR !running):     │  │   │
│  │        │    1. payload  = payloadFactory.generate(seqNum++)        │  │   │
│  │        │    2. rateLimiter.acquire()    ← blocks if over rate      │  │   │
│  │        │    3. haClient.publish(topic, payload)                    │  │   │
│  │        │    4. metrics.recordPublished()                           │  │   │
│  │        │    5. if (mode==FIXED_COUNT && allDone) → signal shutdown │  │   │
│  │        └──────────────────────────────────────────────────────────┘  │   │
│  │                                                                       │   │
│  │  stop()                                                               │   │
│  │   └── running=false → all workers exit loop → executor shutdown      │   │
│  └───────────────────────────────────────────────────────────────────── ┘   │
│                                                                               │
│  ┌───────────────────────┐   ┌─────────────────────────┐                    │
│  │ MessagePayloadFactory │   │ PublisherRateLimiter     │                    │
│  │                       │   │                          │                    │
│  │ AtomicLong seqNum     │   │ Token-bucket Semaphore   │                    │
│  │ ThreadLocalRandom rng │   │ ScheduledExecutorService │                    │
│  │ template rendering    │   │ releases N tokens/sec    │                    │
│  └───────────────────────┘   └─────────────────────────┘                    │
│                                                                               │
│  ┌────────────────────────────────────┐                                      │
│  │  HAClient (publisherHaClient)      │                                      │
│  │  - 1 TCP connection to AMPS        │                                      │
│  │  - publish() is thread-safe        │                                      │
│  │  - ExponentialDelayStrategy        │                                      │
│  │  - No bookmark store (publish-only)│                                      │
│  └─────────────────────┬──────────────┘                                      │
└────────────────────────│────────────────────────────────────────────────────┘
                         │  TCP / AMPS protocol
                         ▼
                    AMPS Server  /queue/trades
```

---

## Rate Limiter Design

A **token-bucket rate limiter** shared across all publisher VTs:

```text
                  Target rate: R msg/s
                  Concurrent publishers: N

Token Bucket:
  ┌─────────────────────────────────────────────────────────────┐
  │  Semaphore (initially 0 permits)                            │
  │                                                             │
  │  ScheduledExecutorService fires every 100ms:               │
  │      releases = R × (100ms / 1000ms) = R/10 permits        │
  │      e.g. R=500 → releases 50 permits every 100ms          │
  │                                                             │
  │  Each publisher VT: semaphore.acquire() before each publish │
  │      → blocks if tokens exhausted                           │
  │      → unblocks within 100ms when next batch is released    │
  └─────────────────────────────────────────────────────────────┘

Why 100ms replenishment interval (not 1ms)?
  - 1ms interval = 1000 scheduler wakeups/s = high overhead
  - 100ms batched release = accurate enough for simulation (±10%)
  - Works identically for N concurrent VTs sharing the same semaphore

BURST mode: no rate limiter — VTs publish directly without acquire()

Special case: if R is very high (>10,000 msg/s) and N is small,
  the 100ms batch may be very large → use 10ms interval instead.
  Configurable via amps.publisher.rate-replenish-interval-ms.
```

---

## Components

### `PublisherConfig.java` — `@Profile("message-publisher")`

**Responsibility:** Create the publisher HAClient and executor; does NOT create any
subscriber beans or semaphore.

```text
Beans created:

  HAClient publisherHaClient()
    - clientName: "amps-publisher-{hostname}"
    - ServerChooser: DefaultServerChooser([${amps.server.uri}])
    - NO bookmark store (publisher does not need replay)
    - ReconnectDelay: ExponentialDelayStrategy(50, 5000)
    - connect(ampsUri)
    - destroyMethod: "close"

  ExecutorService publisherVirtualExecutor()
    - Executors.newVirtualThreadPerTaskExecutor()
    - destroyMethod: "shutdown"

  PublisherRateLimiter publisherRateLimiter()
    - Instantiated with messagesPerSecond from config
    - Only created when mode == RATE_LIMITED
    - BURST/FIXED_COUNT/INFINITE with no rate → a no-op limiter bean

  Note: Bean name "publisherHaClient" avoids collision with subscriber profiles'
        "ampsHaClient" bean when profiles are combined.
```

---

### `AmpsMessagePublisher.java` — `@Profile("message-publisher")`, `SmartLifecycle`

**Responsibility:** Top-level lifecycle manager. Spawns VT workers, tracks overall
progress, triggers JVM shutdown on FIXED_COUNT completion.

```text
Fields:
  HAClient              publisherHaClient
  ExecutorService       publisherVirtualExecutor
  MessagePayloadFactory payloadFactory
  PublisherRateLimiter  rateLimiter
  PublisherProperties   props         (mode, total, concurrency, topic, template)
  AtomicLong            globalSeqNum  = new AtomicLong(0)
  AtomicLong            publishedCount= new AtomicLong(0)
  CountDownLatch        workersDone   (size = concurrentPublishers)
  volatile boolean      running

start():
  running = true
  log "Publisher starting: mode={} rate={} total={} workers={} topic={}"
  for i in 1..concurrentPublishers:
      publisherVirtualExecutor.submit(() → runWorker(i))
  if mode == FIXED_COUNT:
      publisherVirtualExecutor.submit(() → {
          workersDone.await()           // wait for all workers to finish
          context.close()              // trigger graceful Spring shutdown
      })

runWorker(int workerId):
  while (running && !limitReached()):
      long seq      = globalSeqNum.incrementAndGet()
      String payload = payloadFactory.generate(seq, workerId)
      rateLimiter.acquire()           // no-op for BURST
      try:
          publisherHaClient.publish(props.topic, payload)
          long count = publishedCount.incrementAndGet()
          metrics.recordPublished()
          if count % props.logProgressEvery == 0:
              log "Published {} messages (target={})", count, props.totalMessages
      catch AmpsException:
          metrics.recordFailed()
          log.warn "Publish failed seq={}", seq
  workersDone.countDown()

limitReached():
  return props.mode == FIXED_COUNT && publishedCount.get() >= props.totalMessages

stop():
  running = false
  log "Publisher stopping — published {} messages total", publishedCount.get()

getPhase():  Integer.MAX_VALUE - 1
  (stops AFTER subscribers — publish stops before subscribe stops when combined)
```

---

### `MessagePayloadFactory.java` — `@Profile("message-publisher")`

**Responsibility:** Generate valid JSON payloads for a given sequence number and template.

```text
Fields:
  AtomicLong seqNum = new AtomicLong(0)      // shared with AmpsMessagePublisher
  List<String> symbols                         // e.g. ["AAPL","MSFT","GOOGL"]
  List<String> instruments                     // e.g. ["EURUSD","GBPUSD"]
  List<String> books                           // e.g. ["EQUITY-DESK-1","FX-DESK"]
  PayloadTemplate template
  String customTemplate                        // only when template == CUSTOM

generate(long seq, int workerId) → String:
  switch template:
    TRADE  → buildTradeJson(seq, workerId)
    ORDER  → buildOrderJson(seq, workerId)
    RISK   → buildRiskJson(seq, workerId)
    CUSTOM → renderTemplate(customTemplate, seq, workerId)

buildTradeJson(seq, workerId):
  String tradeId    = String.format("TRD-%07d", seq)
  String symbol     = symbols.get((int)(seq % symbols.size()))
  String side       = (seq % 2 == 0) ? "BUY" : "SELL"
  int    qty        = ThreadLocalRandom.current().nextInt(1, 10001)
  double price      = ThreadLocalRandom.current().nextDouble(10.0, 5000.0)
  return JSON string with all fields + publisherId + timestamp

renderTemplate(template, seq, workerId):
  Perform string replacement:
    {seq}       → String.format("%07d", seq)
    {uuid}      → UUID.randomUUID().toString()
    {timestamp} → Instant.now().toString()
    {random-int}→ ThreadLocalRandom.current().nextInt(1, 100001)
    {symbol}    → random from symbols list
    {publisher} → "pub-" + InetAddress.getLocalHost().getHostName() + "-vt" + workerId
```

---

### `PublisherRateLimiter.java`

**Responsibility:** Token-bucket rate gate shared across all publisher VTs.

```text
RATE_LIMITED implementation:
  Semaphore tokens = new Semaphore(0)
  ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor()
  int replenishIntervalMs = config.rateReplenishIntervalMs  (default 100)
  int tokensPerInterval   = messagesPerSecond × replenishIntervalMs / 1000

  start():
    scheduler.scheduleAtFixedRate(
        () → tokens.release(tokensPerInterval),
        0,
        replenishIntervalMs,
        MILLISECONDS
    )

  acquire():
    tokens.acquire()   // parks VT until token available

  close():
    scheduler.shutdown()

BURST / INFINITE / FIXED_COUNT with no rate:
  NoOpRateLimiter:
    acquire() → returns immediately (no-op)
```

---

### `PublisherMetrics.java`

```text
Micrometer meters (all tags include: profile=message-publisher, topic):

  Counter: amps.publisher.messages.sent      — total messages successfully published
  Counter: amps.publisher.messages.failed    — publish errors (HAClient exception)

  Timer:   amps.publisher.send.time          — time from acquire() to publish() return
                                               exposes p50/p95/p99

  Gauge:   amps.publisher.actual.rate        — rolling 1s window: sent / elapsed
  Gauge:   amps.publisher.workers.active     — how many VT workers are still running
```

---

## Configuration — `application-message-publisher.yaml`

```yaml
spring:
  application:
    name: amps-queue-message-publisher

  # Publisher is stateless — no database required in standalone mode
  autoconfigure:
    exclude:
      - org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration
      - org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration

amps:
  server:
    uri: tcp://172.21.12.69:9004/amps/json

  publisher:
    topic: /queue/trades             # must match subscriber topic

    # ── Mode ────────────────────────────────────────────────────────────
    mode: RATE_LIMITED               # RATE_LIMITED | BURST | FIXED_COUNT | INFINITE

    # ── Volume ──────────────────────────────────────────────────────────
    messages-per-second: 500         # target rate (RATE_LIMITED mode only)
    total-messages: 50000            # total to publish (FIXED_COUNT); 0 = ignore
    concurrent-publishers: 5         # number of parallel VT workers

    # ── Payload ─────────────────────────────────────────────────────────
    payload-template: TRADE          # TRADE | ORDER | RISK | CUSTOM
    custom-payload-template: ""      # e.g. '{"id":"{seq}","val":"{random-int}"}'

    symbols:                         # for TRADE template
      - AAPL
      - MSFT
      - GOOGL
      - AMZN
      - TSLA
      - META
      - NVDA
      - JPM

    # ── Rate limiter internals ──────────────────────────────────────────
    rate-replenish-interval-ms: 100  # token bucket replenishment interval

    # ── Observability ───────────────────────────────────────────────────
    log-progress-every: 1000         # log "Published N messages" every N msgs

management:
  endpoints:
    web:
      exposure:
        include: health,info,metrics,prometheus
  endpoint:
    health:
      show-details: always
```

### Combined Profile — Publisher + Subscriber in One JVM

When combining with a subscriber profile, re-enable JPA (the subscriber needs it):

```yaml
# application-message-publisher.yaml when combined with subscriber profile,
# remove the autoconfigure.exclude block. Spring will load JPA for the subscriber.
# Only exclude DataSource when publisher is running ALONE.
```

---

## Usage Scenarios

### Scenario 1 — Standalone Publisher (generate traffic for external subscriber nodes)

```bash
mvnw.cmd spring-boot:run -Dspring-boot.run.profiles=message-publisher
```

Publishes at 500 msg/s indefinitely to `/queue/trades`.
External subscriber JVMs (any profile) consume the messages.

---

### Scenario 2 — Publish a fixed corpus then stop

```yaml
# application-message-publisher.yaml
amps:
  publisher:
    mode: FIXED_COUNT
    total-messages: 10000
    messages-per-second: 1000
    concurrent-publishers: 10
```

```bash
mvnw.cmd spring-boot:run -Dspring-boot.run.profiles=message-publisher
# Application exits automatically after 10,000 messages published
```

---

### Scenario 3 — Burst test (max throughput)

```yaml
amps:
  publisher:
    mode: BURST
    concurrent-publishers: 20
    total-messages: 100000
```

```bash
mvnw.cmd spring-boot:run -Dspring-boot.run.profiles=message-publisher
# Publishes 100,000 messages as fast as possible, then exits
```

---

### Scenario 4 — Self-contained simulation (publisher + subscriber in one JVM)

```bash
mvnw.cmd spring-boot:run \
  -Dspring-boot.run.profiles=message-publisher,single-subscriber
```

What happens:
1. Spring loads both `PublisherConfig` and `SingleSubscriberConfig`
2. Two separate HAClients are created (publisher and subscriber)
3. Publisher VTs push messages to `/queue/trades`
4. AMPS queues them; subscriber receives and processes concurrently
5. The queue ensures each message is processed exactly once
6. All subscriber observability metrics are active
7. Graceful shutdown: publisher stops first (phase MAX-1), subscriber drains (phase MAX)

```text
One JVM:

  PublisherWorker-VT-1 ──▶ HAClient(publisher) ──▶ AMPS ──▶ HAClient(subscriber)
  PublisherWorker-VT-2 ──▶                          │       SingleAmpsSubscriber
  PublisherWorker-VT-N ──▶                          │           │
                                                    │       VT-1 process(m01)
                                                    │       VT-2 process(m02)
                                                    │       VT-N process(mN)
                                                    │           │
                                                    │       H2 (local DB)
```

---

### Scenario 5 — Custom payload with RISK template

```yaml
amps:
  publisher:
    payload-template: RISK
    topic: /queue/risk-events
    mode: RATE_LIMITED
    messages-per-second: 200
    symbols:
      - EQUITY-DESK-1
      - FX-DESK
      - RATES-DESK
```

---

## Package Structure

```text
src/main/java/.../
├── config/
│   └── PublisherConfig.java               @Profile("message-publisher")
│
├── publisher/
│   ├── AmpsMessagePublisher.java          SmartLifecycle, orchestrates workers
│   ├── MessagePayloadFactory.java         Template rendering, seq numbering
│   ├── PublisherRateLimiter.java          Token-bucket Semaphore + scheduler
│   ├── PublisherMode.java                 enum: RATE_LIMITED | BURST | FIXED_COUNT | INFINITE
│   ├── PayloadTemplate.java               enum: TRADE | ORDER | RISK | CUSTOM
│   └── PublisherMetrics.java             Micrometer wrappers
│
└── config/
    └── PublisherProperties.java           @ConfigurationProperties("amps.publisher")

src/main/resources/
└── application-message-publisher.yaml
```

---

## Thread and Resource Count

```text
Component                          Count    Notes
─────────────────────────────────  ──────   ──────────────────────────────────────────────
AMPS TCP connections                   1    One publisher HAClient (publish() is thread-safe)
Virtual thread workers               N    ${amps.publisher.concurrent-publishers}
Rate limiter scheduler thread          1    ScheduledExecutorService (only for RATE_LIMITED)
Semaphore permits (token bucket)     var  Replenished every replenishIntervalMs
Database connections                   0    Publisher is stateless — no DB in standalone mode
```

---

## Graceful Shutdown Sequence

```text
SmartLifecycle phases at shutdown:
  MAX_VALUE - 1 → AmpsMessagePublisher.stop()  (stops BEFORE subscriber drains)
  MAX_VALUE     → Subscriber stop (subscriber drains in-flight VTs first)

  In FIXED_COUNT mode:
    workers finish naturally → countdown latch → context.close() → graceful shutdown

  In all other modes:
    SIGTERM → Spring calls AmpsMessagePublisher.stop()
            → running = false
            → all publisher VTs exit their loops
            → publisherVirtualExecutor.shutdown()
            → publisherHaClient.close()

Phase ordering rationale:
  Publisher phase = MAX_VALUE - 1 (one below subscriber)
  → When shutting down, subscriber stops FIRST (phase MAX_VALUE)
  → Subscriber drains all messages already in AMPS queue
  → Then publisher stops
  → No race: publisher doesn't push new messages while subscriber is draining

  When running publisher alone (no subscriber):
    Publisher phase = MAX_VALUE — doesn't matter, it's the only lifecycle bean
```

---

## Observability

### Actuator Health

```json
GET /actuator/health

{
  "status": "UP",
  "components": {
    "ampsPublisher": {
      "status": "UP",
      "details": {
        "publisherClient":  "CONNECTED",
        "mode":             "RATE_LIMITED",
        "publishedCount":   15234,
        "targetTotal":      50000,
        "workersActive":    5,
        "actualRateMsgSec": 498
      }
    }
  }
}
```

### Key Metrics

```text
amps.publisher.messages.sent     → total sent (expect to grow steadily)
amps.publisher.messages.failed   → should be 0; if non-zero → AMPS connectivity problem
amps.publisher.actual.rate       → compare to configured messages-per-second
amps.publisher.workers.active    → should equal concurrent-publishers until FIXED_COUNT done
amps.publisher.send.time (p99)   → HAClient publish latency; spikes indicate AMPS server load
```

### Recommended Alerts

```text
Alert: amps_publisher_messages_failed_total > 0
  → Publisher can't reach AMPS server
  → Severity: HIGH

Alert: amps_publisher_actual_rate < (configured_rate × 0.8) for > 30s
  → Publisher is being throttled or AMPS is saturated
  → Severity: WARNING

Alert: amps_publisher_workers_active < configured_workers
  → A worker VT has died unexpectedly
  → Severity: WARNING
```

---

## Unit Test Plan

```text
MessagePayloadFactoryTest:
  test_generateTrade_hasAllRequiredFields()
  test_seqNumber_isMonotonicallyIncreasing()
  test_generateTrade_rotatesSymbolsCorrectly()
  test_customTemplate_replacesAllPlaceholders()
  test_customTemplate_sequentialSeqNumbers()

PublisherRateLimiterTest:
  test_rateLimited_doesNotExceedConfiguredRate()
  test_burst_noOpAcquire_returnsImmediately()
  test_tokenBucket_replenishesPeriodically()

AmpsMessagePublisherTest (Mockito):
  test_start_spawnsCorrectNumberOfWorkers()
  test_fixedCount_stopsAfterTotalMessages()
  test_stop_setsRunningFalse_workersExit()
  test_publishFailure_recordsMetric_continues()  ← publisher should NOT stop on single failure
  test_burst_noRateLimiterAcquire()
```

---

## Design Decisions

| Decision | Choice | Rationale |
|---|---|---|
| Single HAClient for all publisher VTs | Yes — `publish()` is thread-safe in HAClient | Avoids N TCP connections; HAClient docs confirm thread safety for publish |
| No bookmark store for publisher | Correct — publisher never needs replay | Bookmark is only for subscribers that need resume-after-restart |
| No DB dependency in standalone mode | `exclude DataSourceAutoConfiguration` | Publisher is stateless; JPA would waste resources when running alone |
| VT workers share one rate limiter semaphore | Yes | Simpler than per-VT rate; achieves aggregate rate target accurately |
| Phase = MAX_VALUE - 1 | Publisher stops before subscriber when combined | Avoids pushing new messages while subscriber is draining shutdown |
| FIXED_COUNT completion triggers `context.close()` | Yes | Allows clean JVM exit in automated test pipelines without SIGTERM |
| Publisher does NOT retry on publish failure | Log + continue | Simulation doesn't need at-least-once guarantees |
