# Module 2 — Multiple JVMs, Multiple Subscribers per JVM, VT Pool per Subscriber

## What This Module Covers

Multiple JVM processes run simultaneously, each pointing at the same AMPS queue.
Inside each JVM there are **multiple subscriber threads** (one per HAClient connection).
Each subscriber thread manages its own independent pool of **20 virtual threads**.

This is horizontal + vertical scaling combined:
- **Horizontal**: add more JVM processes to increase throughput linearly
- **Vertical**: each JVM runs multiple parallel subscriber threads, each with 20 VTs

---

## Architecture

### Overview — 2 JVMs × 3 Subscribers × 20 VTs Each

```text
                        AMPS Server
              /queue/trades  [m01 .. m120]
                           │
          ┌────────────────┼───────────────┐
          │ 3 TCP conns    │ 3 TCP conns   │
          ▼                ▼               │
┌──────────────────┐  ┌──────────────────┐│
│     JVM-1        │  │     JVM-2        ││
│  (host: node-1)  │  │  (host: node-2)  ││
│                  │  │                  ││
│  [Subscriber-1]  │  │  [Subscriber-1]  ││  ← each is a dedicated
│  platform thread │  │  platform thread ││    platform thread
│  Semaphore(20)   │  │  Semaphore(20)   ││
│       │          │  │       │          ││
│   VT-01..VT-20   │  │   VT-01..VT-20   ││
│                  │  │                  ││
│  [Subscriber-2]  │  │  [Subscriber-2]  ││
│  Semaphore(20)   │  │  Semaphore(20)   ││
│       │          │  │       │          ││
│   VT-01..VT-20   │  │   VT-01..VT-20   ││
│                  │  │                  ││
│  [Subscriber-3]  │  │  [Subscriber-3]  ││
│  Semaphore(20)   │  │  Semaphore(20)   ││
│       │          │  │       │          ││
│   VT-01..VT-20   │  │   VT-01..VT-20   ││
│                  │  │                  ││
│  Shared Executor │  │  Shared Executor ││  ← one VT executor per JVM
│  HikariCP(20)    │  │  HikariCP(20)    ││  ← one pool per JVM
└────────┬─────────┘  └────────┬─────────┘│
         │                     │           │
         └──────────┬──────────┘           │
                    │  shared DB           │
         ┌──────────▼──────────┐           │
         │  PostgreSQL          │           │
         │  UNIQUE(message_id)  │           │
         └──────────────────────┘           │
```

### Message Distribution (2 JVMs × 3 Subscribers = 6 competing connections)

```text
AMPS distributes 6 messages to 6 connections simultaneously:

  m01 → JVM-1 / Subscriber-1  →  VT dispatched
  m02 → JVM-1 / Subscriber-2  →  VT dispatched
  m03 → JVM-1 / Subscriber-3  →  VT dispatched
  m04 → JVM-2 / Subscriber-1  →  VT dispatched
  m05 → JVM-2 / Subscriber-2  →  VT dispatched
  m06 → JVM-2 / Subscriber-3  →  VT dispatched

No two connections ever receive the same message (lease model).
Total max in-flight VTs: 2 JVMs × 3 subscribers × 20 VTs = 120 concurrent tasks.
```

### Per-Subscriber Isolation Diagram

```text
Each subscriber has its own platform thread + semaphore + bookmark store.
The virtual thread executor is shared within a JVM (cheap, stateless).

  Subscriber-1 (HAClient: node-1-sub-1)
  ┌────────────────────────────────────────┐
  │  Platform thread: "amps-sub-1"         │
  │  Semaphore: 20 permits                 │
  │  Bookmark: ~/.amps/node-1-sub-1.log    │
  │                                        │
  │  for (msg : stream) {                  │
  │      sem1.acquire();                   │
  │      executor.submit(                  │
  │          () -> processAndAck(msg, c1)  │
  │      );                                │
  │  }                                     │
  └────────────────────────────────────────┘

  Subscriber-2 (HAClient: node-1-sub-2)
  ┌────────────────────────────────────────┐
  │  Platform thread: "amps-sub-2"         │
  │  Semaphore: 20 permits  (independent!) │
  │  Bookmark: ~/.amps/node-1-sub-2.log    │
  │  ... same loop ...                     │
  └────────────────────────────────────────┘

  Subscriber-3 (HAClient: node-1-sub-3)
  ┌────────────────────────────────────────┐
  │  Platform thread: "amps-sub-3"         │
  │  Semaphore: 20 permits  (independent!) │
  │  Bookmark: ~/.amps/node-1-sub-3.log    │
  │  ... same loop ...                     │
  └────────────────────────────────────────┘

  Shared within JVM:
    ExecutorService: Executors.newVirtualThreadPerTaskExecutor()
    HikariCP pool:   20 connections to PostgreSQL
```

---

## Package Structure

```text
src/main/java/com/shan/mq/amps/ampsqueueconcurrency/
│
├── AmpsQueueConcurrencyApplication.java
│
├── config/
│   ├── MultiSubscriberAmpsConfig.java    ← creates List<SubscriberContext>
│   └── VirtualThreadConfig.java          ← shared executor bean
│
├── model/
│   ├── SubscriberContext.java            ← record: HAClient + Semaphore + index
│   ├── ProcessedMessage.java             ← JPA @Entity (with processedBy field)
│   └── ProcessingStatus.java             ← PROCESSED | FAILED enum
│
├── repository/
│   └── ProcessedMessageRepository.java
│
├── subscriber/
│   └── AmpsSubscriberPool.java           ← manages N platform threads
│
├── service/
│   └── MessageDispatchService.java       ← per-subscriber-semaphore dispatch
│
└── processor/
    └── MessageProcessor.java             ← idempotent @Transactional save
```

---

## Full Implementation

### application.yaml (per JVM — identical on all nodes)

```yaml
spring:
  application:
    name: amps-queue-concurrency-module2

  datasource:
    # Shared PostgreSQL — same URL on all JVM instances
    url: jdbc:postgresql://db-host:5432/ampsdb
    username: amps_user
    password: ${DB_PASSWORD}
    hikari:
      # 3 subscribers × 20 VTs = 60 possible concurrent DB ops per JVM.
      # VTs park while waiting — 20 connections serve 60 VTs safely.
      maximum-pool-size: 20
      minimum-idle: 5
      connection-timeout: 3000
      pool-name: HikariPool-Module2

  jpa:
    hibernate:
      ddl-auto: validate
    show-sql: false
    open-in-view: false

amps:
  server:
    uri: tcp://amps-host:9004/amps/json
  queue:
    topic: /queue/trades
  consumer:
    subscriber-count: 3              # platform threads per JVM
    virtual-threads-per-subscriber: 20  # semaphore permits per subscriber
```

---

### SubscriberContext.java

```java
package com.shan.mq.amps.ampsqueueconcurrency.model;

import com.crankuptheamps.client.HAClient;
import java.util.concurrent.Semaphore;

/**
 * Bundles everything needed by one subscriber thread.
 *
 * client    — the HAClient connection (unique client name, unique bookmark store)
 * semaphore — this subscriber's own backpressure gate (20 permits, independent)
 * index     — 1-based index for logging and naming
 *
 * Using a record ensures immutability — no subscriber thread can accidentally
 * swap another subscriber's client or semaphore.
 */
public record SubscriberContext(
    HAClient  client,
    Semaphore semaphore,
    int       index
) {}
```

---

### MultiSubscriberAmpsConfig.java

```java
package com.shan.mq.amps.ampsqueueconcurrency.config;

import com.crankuptheamps.client.HAClient;
import com.crankuptheamps.client.DefaultServerChooser;
import com.crankuptheamps.client.LoggedBookmarkStore;
import com.crankuptheamps.client.ExponentialDelayStrategy;
import com.shan.mq.amps.ampsqueueconcurrency.model.SubscriberContext;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.net.InetAddress;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

@Configuration
public class MultiSubscriberAmpsConfig {

    @Value("${amps.server.uri}")
    private String ampsUri;

    @Value("${amps.consumer.subscriber-count:3}")
    private int subscriberCount;

    @Value("${amps.consumer.virtual-threads-per-subscriber:20}")
    private int vtPerSubscriber;

    /**
     * Creates one SubscriberContext per subscriber.
     * Each context = one HAClient (unique TCP connection) + one Semaphore.
     *
     * Client naming: amps-queue-{hostname}-sub-{index}
     *   e.g. amps-queue-node1-sub-1, amps-queue-node1-sub-2, amps-queue-node1-sub-3
     *        amps-queue-node2-sub-1, amps-queue-node2-sub-2, amps-queue-node2-sub-3
     *
     * The hostname makes names globally unique across JVMs with zero configuration.
     * The index makes names unique within the same JVM.
     *
     * destroyMethod = "" — cleanup is handled manually in AmpsSubscriberPool.stop()
     * to ensure in-flight ACKs complete before connections close.
     */
    @Bean(destroyMethod = "")
    public List<SubscriberContext> subscriberContexts() throws Exception {
        String hostname = sanitize(InetAddress.getLocalHost().getHostName());
        List<SubscriberContext> contexts = new ArrayList<>();

        for (int i = 1; i <= subscriberCount; i++) {
            String clientName = "amps-queue-" + hostname + "-sub-" + i;

            // Bookmark file: unique per client, stored in ~/.amps/
            Path bookmarkPath = Paths.get(
                System.getProperty("user.home"), ".amps", clientName + ".log"
            );
            bookmarkPath.getParent().toFile().mkdirs();  // create ~/.amps if needed

            HAClient client = new HAClient(clientName);
            client.setServerChooser(new DefaultServerChooser(List.of(ampsUri)));
            client.setBookmarkStore(new LoggedBookmarkStore(bookmarkPath.toString()));
            client.setReconnectDelay(new ExponentialDelayStrategy(50, 5000));
            client.connect(ampsUri);

            // Each subscriber gets its own independent semaphore
            Semaphore semaphore = new Semaphore(vtPerSubscriber);

            contexts.add(new SubscriberContext(client, semaphore, i));
        }
        return contexts;
    }

    /**
     * ONE shared virtual-thread executor for all subscribers within this JVM.
     * newVirtualThreadPerTaskExecutor() creates a new VT per submit() call.
     * There is no thread pool to configure — VTs are created on demand.
     * The semaphore per subscriber controls how many are active at once.
     */
    @Bean(name = "sharedVirtualThreadExecutor", destroyMethod = "shutdown")
    public ExecutorService sharedVirtualThreadExecutor() {
        return Executors.newVirtualThreadPerTaskExecutor();
    }

    /** Sanitize hostname for use in file names and AMPS client names */
    private String sanitize(String hostname) {
        return hostname.toLowerCase().replaceAll("[^a-z0-9-]", "-");
    }
}
```

---

### AmpsSubscriberPool.java

```java
package com.shan.mq.amps.ampsqueueconcurrency.subscriber;

import com.crankuptheamps.client.Message;
import com.crankuptheamps.client.MessageStream;
import com.shan.mq.amps.ampsqueueconcurrency.model.SubscriberContext;
import com.shan.mq.amps.ampsqueueconcurrency.service.MessageDispatchService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

/**
 * Manages the lifecycle of N subscriber platform threads (one per SubscriberContext).
 *
 * On start(): spawns N platform threads, each running its own receiveLoop().
 * On stop():  interrupts all threads, then closes all HAClient connections.
 *
 * Each subscriber thread is completely independent:
 *   - Its own TCP connection (HAClient)
 *   - Its own backpressure gate (Semaphore)
 *   - Its own AMPS bookmark position
 *
 * All subscriber threads share:
 *   - The same virtual thread executor (stateless, no shared state)
 *   - The same MessageDispatchService bean
 */
@Component
@Slf4j
public class AmpsSubscriberPool implements SmartLifecycle {

    private final List<SubscriberContext> contexts;
    private final MessageDispatchService  dispatcher;
    private final String                  queueTopic;

    private volatile boolean     running = false;
    private final List<Thread>   subscriberThreads = new ArrayList<>();

    public AmpsSubscriberPool(
            List<SubscriberContext> contexts,
            MessageDispatchService dispatcher,
            @Value("${amps.queue.topic}") String queueTopic) {
        this.contexts    = contexts;
        this.dispatcher  = dispatcher;
        this.queueTopic  = queueTopic;
    }

    @Override
    public void start() {
        running = true;
        for (SubscriberContext ctx : contexts) {
            Thread t = Thread.ofPlatform()
                .name("amps-sub-" + ctx.index())
                .daemon(false)
                .start(() -> receiveLoop(ctx));
            subscriberThreads.add(t);
        }
        log.info("Started {} AMPS subscriber threads on topic={}",
            contexts.size(), queueTopic);
    }

    /**
     * Blocking receive loop for one subscriber.
     * Runs on its own platform thread for the lifetime of the application.
     *
     * @param ctx  the subscriber's HAClient + Semaphore + index
     */
    private void receiveLoop(SubscriberContext ctx) {
        log.info("Subscriber-{} ready, listening on {}", ctx.index(), queueTopic);
        try (MessageStream stream = ctx.client().bookmarkSubscribe(queueTopic)) {
            for (Message message : stream) {
                if (!running) break;
                /*
                 * dispatch() uses ctx.semaphore() — this subscriber's own gate.
                 * Subscriber-1 being busy (semaphore exhausted) does NOT affect
                 * Subscriber-2 or Subscriber-3 at all.
                 */
                dispatcher.dispatch(message, ctx);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.info("Subscriber-{} interrupted — shutting down", ctx.index());
        } catch (Exception e) {
            log.error("Subscriber-{} receive loop error", ctx.index(), e);
        }
    }

    @Override
    public void stop() {
        log.info("Stopping subscriber pool...");
        running = false;
        subscriberThreads.forEach(Thread::interrupt);

        // Close all HAClient connections after threads have stopped
        contexts.forEach(ctx -> {
            try {
                ctx.client().close();
                log.info("Subscriber-{} HAClient closed", ctx.index());
            } catch (Exception e) {
                log.warn("Error closing HAClient for subscriber-{}", ctx.index(), e);
            }
        });
    }

    @Override public boolean isRunning() { return running; }
    @Override public int getPhase()      { return Integer.MAX_VALUE; }
}
```

---

### MessageDispatchService.java

```java
package com.shan.mq.amps.ampsqueueconcurrency.service;

import com.crankuptheamps.client.Message;
import com.shan.mq.amps.ampsqueueconcurrency.model.SubscriberContext;
import com.shan.mq.amps.ampsqueueconcurrency.processor.MessageProcessor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.concurrent.ExecutorService;

@Service
@Slf4j
public class MessageDispatchService {

    private final ExecutorService  executor;
    private final MessageProcessor processor;

    public MessageDispatchService(
            @Qualifier("sharedVirtualThreadExecutor") ExecutorService executor,
            MessageProcessor processor) {
        this.executor  = executor;
        this.processor = processor;
    }

    /**
     * Called by a subscriber platform thread.
     *
     * Uses ctx.semaphore() — the semaphore that belongs to THIS subscriber only.
     * Subscriber-1 blocks here only if its own 20 VTs are full.
     * Subscriber-2 and Subscriber-3 are completely unaffected.
     *
     * ctx.client() is passed into the VT so the ACK goes back on the
     * correct TCP connection (AMPS requires ACK on the receiving connection).
     */
    public void dispatch(Message message, SubscriberContext ctx) {
        try {
            ctx.semaphore().acquire();   // blocks THIS subscriber thread only
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("Subscriber-{} interrupted during acquire", ctx.index());
            return;
        }
        executor.submit(() -> processAndAck(message, ctx));
    }

    private void processAndAck(Message message, SubscriberContext ctx) {
        String bookmark = message.getBookmark();
        try {
            processor.process(message);
            ctx.client().ack(message);           // ACK on the receiving connection
            log.debug("ACK  subscriber={} bookmark={}", ctx.index(), bookmark);
        } catch (Exception e) {
            log.error("FAIL subscriber={} bookmark={}", ctx.index(), bookmark, e);
            // No ACK — AMPS re-delivers after lease expiry
        } finally {
            ctx.semaphore().release();           // frees a slot for this subscriber
        }
    }
}
```

---

### MessageProcessor.java (idempotent — multi-JVM safe)

```java
package com.shan.mq.amps.ampsqueueconcurrency.processor;

import com.crankuptheamps.client.Message;
import com.shan.mq.amps.ampsqueueconcurrency.model.ProcessedMessage;
import com.shan.mq.amps.ampsqueueconcurrency.model.ProcessingStatus;
import com.shan.mq.amps.ampsqueueconcurrency.repository.ProcessedMessageRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.net.InetAddress;
import java.time.Instant;

@Service
@Slf4j
public class MessageProcessor {

    private final ProcessedMessageRepository repo;
    private final String hostname;

    public MessageProcessor(ProcessedMessageRepository repo) {
        this.repo = repo;
        this.hostname = resolveHostname();
    }

    @Transactional
    public void process(Message message) {
        String payload   = message.getData();
        String messageId = message.getBookmark();
        String topic     = message.getTopic();
        Instant now      = Instant.now();

        log.debug("Processing messageId={} topic={} on {}", messageId, topic, hostname);

        // Business logic goes here
        // MyDto dto = objectMapper.readValue(payload, MyDto.class);

        ProcessedMessage entity = ProcessedMessage.builder()
            .messageId(messageId)
            .topic(topic)
            .payload(payload)
            .status(ProcessingStatus.PROCESSED)
            .receivedAt(now)
            .processedAt(now)
            .processedBy(hostname)    // which JVM processed this message
            .build();

        try {
            repo.save(entity);
            log.debug("Saved messageId={}", messageId);

        } catch (DataIntegrityViolationException e) {
            /*
             * Multi-JVM duplicate scenario:
             *   JVM-A processed this message and saved it but crashed before ACK.
             *   AMPS re-delivered to JVM-B (this JVM).
             *   The record already exists in PostgreSQL.
             *   Catch, log, and let the caller ACK — correct outcome.
             */
            log.warn("Duplicate delivery (idempotent) messageId={} — record exists", messageId);
        }
    }

    private String resolveHostname() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            return "unknown-host";
        }
    }
}
```

---

### ProcessedMessage.java (with processedBy field)

```java
package com.shan.mq.amps.ampsqueueconcurrency.model;

import jakarta.persistence.*;
import lombok.*;
import java.time.Instant;

@Entity
@Table(
    name = "processed_messages",
    indexes = {
        @Index(name = "idx_pm_message_id",  columnList = "message_id", unique = true),
        @Index(name = "idx_pm_received_at", columnList = "received_at"),
        @Index(name = "idx_pm_status",      columnList = "status"),
        @Index(name = "idx_pm_processed_by",columnList = "processed_by")
    }
)
@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ProcessedMessage {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "message_id", unique = true, nullable = false, length = 255)
    private String messageId;

    @Column(name = "topic", length = 255)
    private String topic;

    @Lob
    @Column(name = "payload", columnDefinition = "TEXT")
    private String payload;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", length = 20)
    private ProcessingStatus status;

    @Column(name = "received_at")
    private Instant receivedAt;

    @Column(name = "processed_at")
    private Instant processedAt;

    /**
     * Which JVM (by hostname) processed this message.
     * Critical for debugging: if message_id X appears twice in logs
     * but only once in DB, processedBy tells you which JVM won the race.
     */
    @Column(name = "processed_by", length = 255)
    private String processedBy;
}
```

---

### ProcessedMessageRepository.java

```java
package com.shan.mq.amps.ampsqueueconcurrency.repository;

import com.shan.mq.amps.ampsqueueconcurrency.model.ProcessedMessage;
import com.shan.mq.amps.ampsqueueconcurrency.model.ProcessingStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

@Repository
public interface ProcessedMessageRepository extends JpaRepository<ProcessedMessage, Long> {

    boolean existsByMessageId(String messageId);

    Optional<ProcessedMessage> findByMessageId(String messageId);

    List<ProcessedMessage> findByStatus(ProcessingStatus status);

    List<ProcessedMessage> findByProcessedBy(String hostname);

    long countByReceivedAtAfter(Instant since);

    // Per-JVM throughput — compare which nodes are processing most messages
    @Query("""
        SELECT pm.processedBy, COUNT(pm)
        FROM ProcessedMessage pm
        WHERE pm.receivedAt >= :since
        GROUP BY pm.processedBy
        ORDER BY COUNT(pm) DESC
        """)
    List<Object[]> throughputByNode(@Param("since") Instant since);

    // All failed messages across all JVMs
    @Query("""
        SELECT pm FROM ProcessedMessage pm
        WHERE pm.status = 'FAILED'
        ORDER BY pm.receivedAt DESC
        """)
    List<ProcessedMessage> findAllFailed();

    // Check for duplicate delivery events (messageId processed by more than one JVM)
    @Query(value = """
        SELECT message_id, COUNT(DISTINCT processed_by) as jvm_count
        FROM processed_messages
        GROUP BY message_id
        HAVING COUNT(DISTINCT processed_by) > 1
        """, nativeQuery = true)
    List<Object[]> findMessagesProcessedByMultipleJvms();
}
```

---

## Thread & Resource Summary Per JVM

```text
Component                             Count     Notes
────────────────────────────────────  ────────  ────────────────────────────────────
Platform threads (subscriber)              3    "amps-sub-1/2/3"
Semaphores                                 3    One per subscriber, 20 permits each
Max VTs per subscriber                    20    Controlled by per-subscriber semaphore
Max total in-flight VTs per JVM           60    3 × 20 — but share one executor
HikariCP DB connections (per JVM)         20    VTs park waiting — 20 serves 60 safely
AMPS TCP connections (per JVM)             3    One per HAClient
Bookmark log files (per JVM)               3    ~/.amps/amps-queue-{host}-sub-{n}.log
```

## Thread & Resource Summary — Entire System (2 JVMs)

```text
Component                             Count     Calculation
────────────────────────────────────  ────────  ────────────────────────────────────
Total AMPS subscriber connections          6    2 JVMs × 3 subscribers
Max total in-flight VTs                  120    2 JVMs × 3 subs × 20 VTs
Total HikariCP DB connections             40    2 JVMs × 20 connections
Total AMPS TCP connections                 6    2 JVMs × 3 per JVM
```

---

## Crash Recovery — Step by Step

```text
Scenario: JVM-1 Subscriber-2 processes m04, saves to DB, crashes before ACK.

Step 1: m04 is leased to JVM-1/sub-2
Step 2: VT processes m04, saves row (message_id='bm-m04') to PostgreSQL
Step 3: JVM-1 crashes → TCP connection drops → HAClient disconnects
Step 4: AMPS lease TTL for m04 expires (default ~5s after last heartbeat)
Step 5: m04 returns to AVAILABLE state in the queue
Step 6: AMPS re-delivers m04 to next available connection — say JVM-2/sub-1
Step 7: JVM-2 VT calls processor.process(m04)
Step 8: repo.save() → PostgreSQL throws unique_violation (message_id already exists)
Step 9: DataIntegrityViolationException caught → logged as duplicate (not rethrown)
Step 10: Caller (processAndAck) proceeds to client.ack(m04)
Step 11: AMPS marks m04 as ACKED — removed from queue permanently

Result: One row in DB (from Step 2). Correct. No data loss. No duplicate record.
```

---

## Capacity Planning Formula

```text
Throughput per subscriber  = virtualThreadsPerSubscriber / avgProcessingTimeMs × 1000

Example (20 VTs, 50ms avg processing):
  = 20 / 50 × 1000 = 400 msg/s per subscriber

Total per JVM (3 subscribers):
  = 3 × 400 = 1,200 msg/s

Total across 2 JVMs:
  = 2 × 1,200 = 2,400 msg/s

To hit 5,000 msg/s:
  → subscribers needed = 5,000 / 400 = 12.5 → 13 subscribers total
  → options:
       2 JVMs × 7 subscribers each  = 2 × 7 × 400 = 5,600 msg/s
       3 JVMs × 5 subscribers each  = 3 × 5 × 400 = 6,000 msg/s
```
