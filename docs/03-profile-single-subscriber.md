# 03 — Profile: `single-subscriber`

## When to Use This Profile

| Condition | Recommendation |
|---|---|
| Local development / debugging | ✅ Use this profile |
| Low-to-moderate message volume (< ~500 msg/s) | ✅ |
| Single deployment node (no HA requirement) | ✅ |
| Simplest possible setup, easy log tracing | ✅ |
| Need > 1,000 msg/s sustained throughput | ❌ Use `multi-subscriber` |
| Need fault tolerance if the JVM dies | ❌ Use `multi-jvm-subscriber` |

---

## Architecture

```text
                          AMPS Server
                    /queue/trades  [m01 .. mN]
                              │
                       1 TCP connection
                              │
┌─────────────────────────────▼──────────────────────────────────────────┐
│                         Single JVM                                      │
│                                                                         │
│   ┌─────────────────────────────────────────────────────────────────┐  │
│   │  SingleAmpsSubscriber  (SmartLifecycle)                          │  │
│   │  ONE dedicated platform thread: "amps-subscriber"               │  │
│   │                                                                  │  │
│   │   for (Message msg : ampsStream) {                               │  │
│   │       semaphore.acquire();   ← blocks if N VTs all busy         │  │
│   │       executor.submit(                                           │  │
│   │           () -> dispatchService.processAndAck(msg, haClient)    │  │
│   │       );                                                         │  │
│   │   }                                                              │  │
│   └──────────────────────────┬──────────────────────────────────── ─┘  │
│                               │ virtual thread per message               │
│                               ▼                                          │
│   ┌─────────────────────────────────────────────────────────────────┐  │
│   │  VirtualThreadExecutor (newVirtualThreadPerTaskExecutor)         │  │
│   │  Semaphore: maxConcurrency permits                               │  │
│   │                                                                  │  │
│   │  VT-01 → processor.process(m01) → INSERT → ACK → release()     │  │
│   │  VT-02 → processor.process(m02) → INSERT → ACK → release()     │  │
│   │  VT-03 → processor.process(m03) → INSERT → ACK → release()     │  │
│   │  ...                                                             │  │
│   │  VT-N  → processor.process(mN)  → INSERT → ACK → release()     │  │
│   │                                                                  │  │
│   │  [m(N+1) waits — semaphore blocks subscriber until a VT done]   │  │
│   └──────────────────────────┬──────────────────────────────────── ─┘  │
│                               │ HikariCP                                 │
│               ┌───────────────▼─────────────────┐                       │
│               │  HikariCP  (pool-size = 10)      │                       │
│               └───────────────┬─────────────────┘                       │
│               ┌───────────────▼─────────────────┐                       │
│               │  H2 (dev) / PostgreSQL (prod)    │                       │
│               │  TABLE: processed_messages       │                       │
│               │  UNIQUE(message_id)              │                       │
│               └─────────────────────────────────┘                       │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Configuration — `application-single-subscriber.yaml`

```yaml
spring:
  application:
    name: amps-queue-single-subscriber

  datasource:
    url: jdbc:h2:file:./data/amps-single;AUTO_SERVER=TRUE
    driver-class-name: org.h2.Driver
    username: sa
    password: ""
    hikari:
      pool-name: HikariPool-SingleSub
      maximum-pool-size: 10     # 10 connections serve up to N VTs (VTs park when waiting)
      minimum-idle: 2
      connection-timeout: 3000
      idle-timeout: 600000

  jpa:
    hibernate:
      ddl-auto: update          # auto-create schema on first run
    show-sql: false
    open-in-view: false
    properties:
      hibernate:
        format_sql: false

amps:
  server:
    uri: tcp://172.21.12.69:9004/amps/json
  client:
    name: amps-queue-concurrency-sub-1      # fixed name — single subscriber
  queue:
    topic: /queue/trades
    filter: ""                              # optional: WHERE side='BUY'
    lease-timeout-ms: 5000                  # must ACK within 5s
  consumer:
    max-concurrency: 100                    # semaphore permits = max in-flight VTs
    max-retries: 3                          # retry attempts before discarding
    retry-backoff-ms: 0                     # AMPS handles backoff via lease expiry

management:
  endpoints:
    web:
      exposure:
        include: health,info,metrics,prometheus
  endpoint:
    health:
      show-details: always
```

**Profile activation:**
```yaml
# In application.yaml (base config):
spring:
  profiles:
    active: single-subscriber
```

Or at startup: `-Dspring.profiles.active=single-subscriber`

---

## Components

### `SingleSubscriberConfig.java` — `@Profile("single-subscriber")`

**Responsibility:** Creates exactly one `HAClient` bean and one `Semaphore` bean.

```text
Beans created:
  HAClient ampsHaClient()
    - client name: from ${amps.client.name}
    - bookmark store: ~/.amps/{clientName}.log
    - reconnect: ExponentialDelayStrategy(50ms, 5000ms)
    - server chooser: DefaultServerChooser([${amps.server.uri}])
    - destroyMethod: "close"

  Semaphore ampsSemaphore()
    - permits: ${amps.consumer.max-concurrency}  (e.g. 100)
```

---

### `SingleAmpsSubscriber.java` — `@Profile("single-subscriber")`, `SmartLifecycle`

**Responsibility:** Owns exactly one AMPS receive loop on one dedicated platform thread.

```text
Lifecycle:
  start()   → start() creates one platform thread (Thread.ofPlatform())
                named "amps-subscriber", daemon=false
                calls receiveLoop()

  stop()    → sets running=false, interrupts the platform thread
              waits for the platform thread to die (join with timeout)

receiveLoop():
  try (MessageStream stream = haClient.bookmarkSubscribe(topic, filter)) {
      for (Message msg : stream) {
          if (!running) break;
          dispatchService.dispatch(msg, haClient);  // blocks on semaphore if full
      }
  } catch (InterruptedException) → clean shutdown
    catch (Exception)            → log error, optionally reconnect

getPhase(): Integer.MAX_VALUE   ← starts last, stops first
```

---

### `MessageDispatchService.java`

**Responsibility:** Acquires the semaphore, submits the VT task, releases on completion.

```text
dispatch(Message msg, HAClient client):
  1. semaphore.acquire()            ← blocks subscriber thread if at capacity
  2. executor.submit(() → {
       try {
           ProcessingResult result = processor.process(msg);
           if (result == DISCARD)  client.ack(msg)   // max retries exceeded
           else if (result == OK)  client.ack(msg)   // normal success
           else if (result == DUP) client.ack(msg)   // already processed
           // else FAIL → no ack, AMPS re-delivers
       } catch (Exception e) → log, no ACK
       finally → semaphore.release()
     })
```

---

### `MessageProcessor.java`

**Responsibility:** Idempotency check, business logic, DB persistence, retry tracking.

```text
process(Message msg) → ProcessingResult:

  messageId = msg.getBookmark()   ← globally unique AMPS bookmark

  existing = repo.findByMessageId(messageId)

  if (existing.isPresent()):
      switch (existing.status):
          PROCESSED  → return DUPLICATE
          DISCARDED  → return DISCARD
          FAILED     → check retryCount:
                          if retryCount >= maxRetries → update to DISCARDED, return DISCARD
                          else → proceed with retry attempt

  // Execute business logic (parse payload, domain operations)
  try:
      doBusinessLogic(msg.getData())
      save ProcessedMessage(status=PROCESSED, retryCount=0)
      return OK

  catch Exception:
      upsert ProcessedMessage(status=FAILED, retryCount++, lastError=e.message)
      throw   ← caller does NOT ack → AMPS re-delivers after TTL
```

---

### `ProcessedMessage.java` — JPA Entity (shared across all profiles)

```text
Table: processed_messages

Columns:
  id             BIGINT IDENTITY (PK)
  message_id     VARCHAR(255) NOT NULL UNIQUE  ← AMPS bookmark (idempotency key)
  topic          VARCHAR(255)
  payload        TEXT
  status         VARCHAR(20)                   ← PROCESSED | FAILED | DISCARDED
  retry_count    INT DEFAULT 0
  last_error     TEXT                          ← last exception message on failure
  received_at    TIMESTAMP
  processed_at   TIMESTAMP
  last_attempt_at TIMESTAMP
  processed_by   VARCHAR(255)                  ← hostname (for multi-JVM debugging)

Indexes:
  idx_pm_message_id   (message_id) UNIQUE
  idx_pm_status       (status)
  idx_pm_received_at  (received_at)
  idx_pm_processed_by (processed_by)
```

---

## Thread and Resource Count

```text
Component                          Count    Notes
─────────────────────────────────  ──────   ─────────────────────────────────────────
AMPS TCP connections                   1    One HAClient, one LoggedBookmarkStore
Platform threads (subscriber)          1    "amps-subscriber" — blocks on MessageStream
Semaphore permits                    100    Max concurrent in-flight VTs
Virtual threads (max concurrent)     100    Created on-demand, heap-allocated ~1-2 KB
HikariCP DB connections               10    VTs park when all 10 in use (no starvation)
Bookmark log files                     1    ~/.amps/amps-queue-concurrency-sub-1.log
```

---

## Throughput Estimate

```text
Formula: throughput = maxConcurrency / avgProcessingTimeMs × 1000

With maxConcurrency=100, avgProcessingTime=50ms:
  throughput = 100 / 50 × 1000 = 2,000 msg/s (theoretical max)

Practical sustained rate (80%):  ~1,600 msg/s

Bottleneck: single TCP connection to AMPS limits ingestion rate.
If AMPS cannot push messages fast enough on one connection →
switch to multi-subscriber profile.
```

---

## Graceful Shutdown Sequence

```text
Step 1: Spring calls SmartLifecycle.stop() (highest phase = stops first)
Step 2: SingleAmpsSubscriber sets running=false, interrupts subscriber thread
Step 3: Subscriber thread exits receiveLoop (InterruptedException caught)
Step 4: MessageStream.close() sends subscription cancellation to AMPS
Step 5: In-flight VTs continue running (semaphore still tracking them)
Step 6: VTs complete their DB INSERTs and send ACKs to AMPS
Step 7: As each VT finishes, semaphore.release() is called
Step 8: ExecutorService.shutdown() + awaitTermination() (in CommonConfig destroy)
Step 9: HAClient.close() closes the TCP connection (destroyMethod = "close")
Step 10: HikariCP pool closes all DB connections
Step 11: JVM exits cleanly

Key guarantee: No message is lost — either the VT ACKs before shutdown,
or the lease expires and AMPS re-delivers to the next available subscriber.
```
