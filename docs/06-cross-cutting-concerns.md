# 06 — Cross-Cutting Concerns

This document covers the five concerns that apply to **all three profiles**:
idempotency, retry handling, graceful shutdown, backpressure, and observability.

---

## 1. Idempotency

### Why It Is Mandatory

AMPS guarantees **at-least-once** delivery. A message can be re-delivered if:
- The subscriber's JVM crashed after processing but before ACKing
- The AMPS lease TTL expired before the ACK was sent (slow processing)
- The HAClient connection was lost mid-lease

In all these cases, AMPS re-delivers the same message (same bookmark). Without
idempotency, you get duplicate rows in the database.

### Implementation Strategy

```text
Database:
  processed_messages.message_id  VARCHAR(255) NOT NULL UNIQUE

  The AMPS bookmark is globally unique per queue message.
  Using it as the idempotency key gives zero-configuration uniqueness.

Application (MessageProcessor):

  Option A — Check-then-Insert (used in this project):
    1. SELECT * FROM processed_messages WHERE message_id = ?
    2. If found → return DUPLICATE / DISCARD based on status
    3. If not found → INSERT with status = PROCESSED

  Option B — Insert-then-catch (alternative, simpler):
    1. INSERT INTO processed_messages ...
    2. catch DataIntegrityViolationException → it is a duplicate; treat as success

  Option A is preferred because it gives us fine-grained control over the status
  (FAILED / DISCARDED handling during retry path).
```

### Idempotency Matrix

```text
Delivery scenario                        DB state        Action
─────────────────────────────────────── ────────────── ─────────────────────────────────
First delivery, success                  No row         INSERT status=PROCESSED → ACK
First delivery, failure                  No row or FAILED INSERT/UPDATE status=FAILED → NO ACK
Re-delivery after failure (retry < max)  FAILED row     Retry → update → ACK or NO ACK
Re-delivery after failure (retry = max)  FAILED row     Update to DISCARDED → ACK + log SEVERE
Re-delivery after success                PROCESSED row  Return DUPLICATE → ACK (no-op)
Re-delivery after discard                DISCARDED row  Return DISCARD → ACK (no-op)
```

---

## 2. Retry Handling (No Dead-Letter Queue)

### Design Rationale

There is no DLQ in this environment. Failed messages must be:
1. Retried a configurable number of times (AMPS re-delivers naturally via lease expiry)
2. Discarded after `max-retries` exhausted — ACK sent, message removed from AMPS queue,
   ERROR log emitted, metric counter incremented, next message processed

### Retry Tracking Modes

Two modes are available via `retry-db-tracking-enabled`:

```text
Mode A — DB tracking (retry-db-tracking-enabled: true, default)
  FAILED rows are written to the database on each failed attempt.
  Retry count and last error are persisted and survive JVM restarts.
  Correct for all profiles, including multi-jvm-subscriber.

Mode B — In-memory tracking (retry-db-tracking-enabled: false)
  No FAILED rows are written to DB.
  Retry count is tracked in a ConcurrentHashMap inside the JVM.
  PROCESSED rows are still written on success (idempotency intact).
  Counter is lost on JVM restart → retries reset to zero.
  Suitable for single-subscriber and multi-subscriber (same JVM).
  NOT suitable for multi-jvm-subscriber.

In both modes:
  - Retries exhausted → log ERROR + return DISCARD → caller ACKs → next message
  - In-flight failure (retries remaining) → return FAIL → no ACK → AMPS re-delivers
```

### Retry Flow

```text
First attempt fails:
  VT catches exception → processor.process() returns FAIL
  dispatchService does NOT call haClient.ack(msg)
  semaphore.release() is called (slot freed)
  AMPS lease expires (default 5s) → message returns to AVAILABLE
  AMPS re-delivers to the next available subscriber connection

On re-delivery (attempt 2, 3, ...):

  DB tracking mode:
    MessageProcessor checks repo.findByMessageId(messageId)
    found: status=FAILED, retryCount=N
        retryCount < maxRetries → attempt processing again
        retryCount >= maxRetries → mark DISCARDED, return DISCARD signal

  In-memory tracking mode:
    MessageProcessor checks repo.findByMessageId(messageId) for idempotency only
    (FAILED rows are never written, so re-delivery finds no existing record)
    inMemoryRetryCount.get(messageId) → attempt N
        attempt < maxRetries → return FAIL (no DB write)
        attempt >= maxRetries → remove from map, return DISCARD (no DB write)

  Caller receives DISCARD → calls haClient.ack(msg) → removes from AMPS queue
  Logs: ERROR "DISCARD messageId={} after {} attempts — moving to next message"
  Metric: amps.messages.discarded counter++
```

### ProcessedMessage State (DB Tracking Mode)

```text
Columns involved in retry:

  status          VARCHAR(20)   PROCESSED | FAILED | DISCARDED
  retry_count     INT DEFAULT 0  incremented on each failed attempt
  last_error      TEXT           last exception message (for debugging)
  last_attempt_at TIMESTAMP     when the last attempt (success or failure) occurred

State transitions:
  (new)         → FAILED     (retry_count=1)     on first failure
  FAILED(1)     → FAILED     (retry_count=2)     on second failure
  FAILED(n)     → FAILED     (retry_count=n+1)   if n+1 < maxRetries
  FAILED(max-1) → DISCARDED  (retry_count=max)   on final failure
  FAILED(any)   → PROCESSED  (retry_count stays) on eventual success
  DISCARDED     → DISCARDED  (no change)          re-delivery after discard
```

### ProcessedMessage State (In-Memory Tracking Mode)

```text
Only PROCESSED rows are written to DB.
FAILED and DISCARDED states exist only in the JVM's inMemoryRetryCount map.

  inMemoryRetryCount map:
    key   = messageId (AMPS bookmark)
    value = number of failed attempts so far

  Map entry lifecycle:
    (new) → map[messageId]=1          on first failure
    map[id]=N → map[id]=N+1           on each subsequent failure
    map[id]=maxRetries → removed      on discard (DISCARD returned)
    map[id]=N → removed               on success (OK returned)

  DB state:
    No row written on FAIL or DISCARD.
    PROCESSED row written on success (idempotency).
```

### Configuration

```yaml
amps:
  consumer:
    max-retries: 3                    # total failed attempts before discard
                                      # 1 = discard after first failure (no retries)
                                      # 3 = discard after third failure (2 retries after first)
    retry-db-tracking-enabled: true   # true  → persist FAILED/DISCARDED rows to DB
                                      # false → in-memory retry tracking only (no FAILED rows)
                                      # must be true for multi-jvm-subscriber
```

### Why Natural Retry via AMPS Lease Expiry Is Correct

```text
Spring Retry (@Retryable) retries WITHIN the same VT:
  - Holds the DB connection open during retry sleep
  - Holds the semaphore permit during retry sleep
  - Blocks the HikariCP connection during the sleep period
  → NOT recommended for AMPS queue consumers

AMPS natural retry (NO ACK → lease expires → re-delivery):
  - VT releases semaphore immediately after failure
  - VT releases HikariCP connection immediately after failure
  - AMPS server handles the retry delay (lease TTL)
  - Other messages are processed while this one waits for re-delivery
  → CORRECT approach for AMPS queue consumers
```

---

## 3. Graceful Shutdown

### Goal

When the JVM receives a shutdown signal (SIGTERM, `Ctrl+C`, or Spring context close):
1. Stop accepting new messages from AMPS
2. Allow all in-flight virtual threads to complete their current processing
3. Ensure all completed messages are ACKed before the HAClient connection closes
4. Close all connections cleanly

### SmartLifecycle Phase Ordering

```text
Spring lifecycle phases (higher phase = starts last / stops first):

  Phase Integer.MAX_VALUE:
    SingleAmpsSubscriber / MultiAmpsSubscriberPool
      stop() → interrupt subscriber threads → close MessageStreams → close HAClients

  Phase 0 (default):
    All other Spring beans (repositories, services, processors)

  Phase Integer.MIN_VALUE:
    HikariCP DataSource (managed by Spring Boot auto-config)

  Shutdown order:  MAX_VALUE → 0 → MIN_VALUE
  Startup order:   MIN_VALUE → 0 → MAX_VALUE

  This guarantees:
    - HAClient is the LAST thing to close (so in-flight VTs can still ACK)
    - Database remains open until all VTs are done
    - Subscriber stop happens before any downstream bean is torn down
```

### Drain Sequence Detail

```text
Time 0: JVM receives SIGTERM

Time 0ms:   SmartLifecycle.stop() called on subscriber
            running = false
            subscriber threads interrupted

Time 1-5ms: Platform threads exit receiveLoop
            MessageStream.close() signals subscription cancel to AMPS
            No new messages received

Time 0ms → ~T: In-flight VTs continue running (independently)
            Each VT: business logic → INSERT → ACK → semaphore.release()

Time T:     All VT tasks finish
            ExecutorService.awaitTermination(30s, SECONDS)
            (gives VTs up to 30 seconds to drain)

Time T+ε:   HAClient.close() called (destroyMethod)
Time T+ε:   HikariCP pool closes all connections
Time T+ε:   JVM exits

Maximum shutdown time = 30s (configurable via awaitTerminationSeconds)
```

### Shutdown Configuration

```yaml
# In application.yaml — add to spring.lifecycle:
spring:
  lifecycle:
    timeout-per-shutdown-phase: 30s    # Spring SmartLifecycle drain timeout
```

---

## 4. Backpressure

### The Problem Without Backpressure

```text
Without semaphore:
  AMPS delivers 1,000 msg/s
  DB can process 100 msg/s
  → 900 VTs per second accumulate in JVM heap
  → After 10 seconds: 9,000 live VTs
  → After 60 seconds: OOM / GC pressure / response latency explosion
```

### The Semaphore Solution

```text
With semaphore (maxConcurrency = 200):
  AMPS delivers 1,000 msg/s
  VT slot becomes available → semaphore.acquire() returns
  VT slot full (200 in-flight) → semaphore.acquire() BLOCKS

  Subscriber thread is blocked → cannot call stream.next()
  AMPS sees the subscriber is "slow" → stops delivering to this connection
  Messages pile up safely in AMPS server queue (server-side buffering)

  As VTs finish → semaphore.release() → subscriber unblocks → next message received
```

### Semaphore Sizing Rules

```text
Goal: maxConcurrency should be tuned so:
  (a) DB connection pool is never starved (VTs always get connections quickly)
  (b) Memory pressure from VTs is bounded
  (c) AMPS lease TTL is never exceeded (VT finishes before TTL expires)

Rule 1 — DB pool ratio:
  maxConcurrency ≤ hikari.maximum-pool-size × 10
  (VTs park while waiting for connections — 10× is a safe ratio)

Rule 2 — Lease TTL:
  avgProcessingTimeMs × 1 ≪ lease-timeout-ms
  (ideally avgProcessingTimeMs < lease-timeout-ms / 2)
  If processing is slow, EITHER reduce maxConcurrency OR increase lease TTL in AMPS config

Rule 3 — Memory:
  Each VT ≈ 1-2 KB stack + payload size
  maxConcurrency × (2KB + avgPayloadSize) should be < 10% of JVM heap

Example (hikari.max-pool-size=20, lease-timeout=5000ms, avgPayload=2KB):
  maxConcurrency ≤ 200  (rule 1: 20 × 10)
  avgProcessingTime < 2500ms (rule 2: 5000 / 2)
  memory = 200 × 4KB = 800KB (rule 3: negligible)
  → set maxConcurrency = 200 ✓
```

---

## 5. Observability

### Health Indicator — `AmpsHealthIndicator`

Implements `HealthIndicator` and reports UP/DOWN per HAClient.

```text
GET /actuator/health

{
  "status": "UP",
  "components": {
    "amps": {
      "status": "UP",
      "details": {
        "amps-queue-concurrency-node1-sub-1": "CONNECTED",
        "amps-queue-concurrency-node1-sub-2": "CONNECTED",
        "amps-queue-concurrency-node1-sub-3": "DISCONNECTED"   ← alerts ops
      }
    },
    "db": { "status": "UP" },
    "diskSpace": { "status": "UP" }
  }
}
```

Logic: try `haClient.getConnectionState()` → if CONNECTED → UP; else DOWN.
If ANY subscriber is disconnected, report overall AMPS status as DOWN.

### Micrometer Metrics

All metrics use the tag `profile` (e.g. `single-subscriber`, `multi-subscriber`, `multi-jvm-subscriber`)
and `topic` (e.g. `/queue/trades`).

```text
Counter metrics:
  amps.messages.received        — incremented when subscriber receives a message
  amps.messages.processed       — incremented on successful processing + ACK
  amps.messages.failed          — incremented when processing fails (no ACK, will retry)
  amps.messages.duplicate       — incremented when idempotency check finds existing record
  amps.messages.discarded       — incremented when max retries exceeded (ACK'd, removed)

Timer metrics:
  amps.processing.time          — records processing duration per message
                                   exposes p50, p95, p99 percentiles
                                   tag: topic

Gauge metrics:
  amps.subscriber.active        — number of currently running subscriber platform threads
  amps.semaphore.available      — available semaphore permits (per subscriber for multi-sub)
  amps.queue.inflight           — current in-flight VT count (= maxConcurrency - available)
```

### Prometheus Scrape Configuration

```yaml
# prometheus.yml
scrape_configs:
  - job_name: 'amps-queue-consumer'
    metrics_path: '/actuator/prometheus'
    static_configs:
      - targets:
          - 'node1:8080'
          - 'node2:8080'
          - 'node3:8080'
```

### Key Dashboards / Alerts

```text
Alert: amps_messages_discarded_total > 0
  → Messages are being silently dropped after max retries
  → Severity: HIGH — investigate root cause immediately

Alert: amps_subscriber_active < expected_subscriber_count
  → A subscriber thread has died
  → Severity: CRITICAL — application may need restart

Alert: amps_semaphore_available == 0 for > 30s
  → Backpressure is sustained; DB or business logic too slow
  → Severity: WARNING — scale up or tune maxConcurrency

Alert: amps_processing_time_p99 > (lease_timeout_ms × 0.8)
  → 1% of messages risk lease expiry (will be re-delivered)
  → Severity: WARNING — optimize slow path or increase lease TTL

Dashboard panels:
  - Messages received/s (rate of amps.messages.received)
  - Messages processed/s (rate of amps.messages.processed)
  - Processing time p50/p95/p99
  - In-flight VT count (amps.queue.inflight)
  - Failure rate (rate of amps.messages.failed / received)
  - Discard rate (rate of amps.messages.discarded)
```

### Structured Logging

Use MDC (Mapped Diagnostic Context) to enrich log lines with AMPS context:

```text
MDC fields to set in the subscriber thread before dispatch:
  messageId = msg.getBookmark()
  topic     = msg.getTopic()
  subscriberIndex = ctx.index()

Log pattern example:
  2026-05-27 17:00:01.234  INFO [amps-sub-2] [messageId=bm001,topic=/queue/trades,sub=2]
  MessageProcessor     : Processing completed in 47ms

This makes it trivial to grep all log lines for one message across JVMs:
  grep "messageId=bm001" app-node1.log app-node2.log
```
