# Module 1 — Single JVM, Single Subscriber, 20 Virtual Threads

## What This Module Covers

One JVM. One TCP connection to AMPS. One subscriber platform thread that blocks
on the queue. Every arriving message is handed off to one of **20 virtual threads**
for concurrent processing and DB persistence. The subscriber thread never does
any processing work — it only receives and dispatches.

---

## Architecture

```text
                         AMPS Server
                   /queue/trades  [m1..m20]
                           │
                    TCP connection (1)
                           │
┌──────────────────────────▼──────────────────────────────────────────┐
│                    Single JVM                                        │
│                                                                      │
│  ┌────────────────────────────────────────────────────────────┐     │
│  │  AmpsQueueSubscriber                                        │     │
│  │  (ONE dedicated platform thread — "amps-subscriber")        │     │
│  │                                                             │     │
│  │    for (Message msg : ampsStream) {                         │     │
│  │        semaphore.acquire();    ← blocks if 20 VTs busy      │     │
│  │        executor.submit(        ← O(1), never blocks         │     │
│  │            () -> processAndAck(msg)                         │     │
│  │        );                                                   │     │
│  │    }                                                         │     │
│  └───────────────────────────┬────────────────────────────────┘     │
│                               │  dispatch                            │
│                               ▼                                      │
│  ┌────────────────────────────────────────────────────────────┐     │
│  │  VirtualThreadExecutor                                      │     │
│  │  Executors.newVirtualThreadPerTaskExecutor()                │     │
│  │                                                             │     │
│  │  Semaphore: 20 permits  (max 20 concurrent in-flight)       │     │
│  │                                                             │     │
│  │   VT-01 → process(m01) → INSERT → ACK → semaphore.release  │     │
│  │   VT-02 → process(m02) → INSERT → ACK → semaphore.release  │     │
│  │   VT-03 → process(m03) → INSERT → ACK → semaphore.release  │     │
│  │   ...                                                        │     │
│  │   VT-20 → process(m20) → INSERT → ACK → semaphore.release  │     │
│  │                                                             │     │
│  │   [m21 waits — semaphore blocks subscriber until a VT done] │     │
│  └──────────────────────────┬─────────────────────────────────┘     │
│                              │                                        │
│             ┌────────────────▼────────────────┐                      │
│             │  HikariCP  (pool-size = 10)      │                      │
│             └────────────────┬────────────────┘                      │
│             ┌────────────────▼────────────────┐                      │
│             │  H2 (dev) / PostgreSQL (prod)    │                      │
│             │  TABLE: processed_messages       │                      │
│             └──────────────────────────────────┘                     │
└──────────────────────────────────────────────────────────────────────┘
```

### Concurrency Flow — One Message

```text
AMPS              Subscriber Thread       Semaphore        VT-n          DB
 │                       │                    │               │            │
 │── deliver m01 ────────▶                    │               │            │
 │                        │── acquire() ─────▶│               │            │
 │                        │   (permit -1=19)  │               │            │
 │                        │                   │               │            │
 │                        │── submit(m01) ──────────────────▶ │            │
 │                        │   (returns immediately)           │            │
 │                        │                                    │─ parse()  │
 │── deliver m02 ─────────▶                                    │─ INSERT ─▶│
 │                        │── acquire() ─────▶│               │◀─ commit  │
 │                        │   (permit -1=18)  │               │─ ACK()    │
 │                        │                   │               │─ release()▶│
 │                        │                   │  (permit +1)  │            │
```

---

## Package Structure

```text
src/main/java/com/shan/mq/amps/ampsqueueconcurrency/
│
├── AmpsQueueConcurrencyApplication.java
│
├── config/
│   ├── AmpsConfig.java               ← single HAClient bean
│   └── VirtualThreadConfig.java      ← executor + Semaphore(20)
│
├── model/
│   ├── ProcessedMessage.java         ← JPA @Entity
│   └── ProcessingStatus.java         ← PROCESSED | FAILED enum
│
├── repository/
│   └── ProcessedMessageRepository.java
│
├── subscriber/
│   └── AmpsQueueSubscriber.java      ← SmartLifecycle, single platform thread
│
├── service/
│   └── MessageDispatchService.java   ← semaphore + executor fan-out
│
└── processor/
    └── MessageProcessor.java         ← @Transactional parse + save
```

---

## Full Implementation

### application.yaml

```yaml
spring:
  application:
    name: amps-queue-concurrency-module1

  datasource:
    url: jdbc:h2:file:./data/module1;AUTO_SERVER=TRUE
    driver-class-name: org.h2.Driver
    username: sa
    password:
    hikari:
      maximum-pool-size: 10       # 20 VTs share 10 connections — VTs park when waiting
      minimum-idle: 2
      connection-timeout: 3000

  jpa:
    hibernate:
      ddl-auto: update
    show-sql: false
    open-in-view: false

amps:
  server:
    uri: tcp://localhost:9004/amps/json
  client:
    name: amps-module1-subscriber
  queue:
    topic: /queue/trades
  consumer:
    virtual-thread-count: 20     # semaphore permits = max concurrent VTs
```

---

### ProcessingStatus.java

```java
package com.shan.mq.amps.ampsqueueconcurrency.model;

public enum ProcessingStatus {
    PROCESSED,
    FAILED
}
```

---

### ProcessedMessage.java

```java
package com.shan.mq.amps.ampsqueueconcurrency.model;

import jakarta.persistence.*;
import lombok.*;
import java.time.Instant;

@Entity
@Table(
    name = "processed_messages",
    indexes = {
        @Index(name = "idx_pm_message_id",   columnList = "message_id", unique = true),
        @Index(name = "idx_pm_received_at",  columnList = "received_at"),
        @Index(name = "idx_pm_status",       columnList = "status")
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

    /**
     * AMPS bookmark — globally unique per queue message.
     * UNIQUE constraint prevents duplicate rows on re-delivery.
     */
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

    // Check if already processed (idempotency guard)
    boolean existsByMessageId(String messageId);

    // Fetch by AMPS bookmark
    Optional<ProcessedMessage> findByMessageId(String messageId);

    // Find all failed messages for retry
    List<ProcessedMessage> findByStatus(ProcessingStatus status);

    // Count messages processed after a given time
    long countByReceivedAtAfter(Instant since);

    // Find messages by topic
    List<ProcessedMessage> findByTopicOrderByReceivedAtDesc(String topic);

    // Throughput query: messages processed in last N minutes
    @Query("""
        SELECT COUNT(pm) FROM ProcessedMessage pm
        WHERE pm.receivedAt >= :since
        AND pm.status = 'PROCESSED'
        """)
    long countProcessedSince(@Param("since") Instant since);
}
```

---

### AmpsConfig.java

```java
package com.shan.mq.amps.ampsqueueconcurrency.config;

import com.crankuptheamps.client.HAClient;
import com.crankuptheamps.client.DefaultServerChooser;
import com.crankuptheamps.client.LoggedBookmarkStore;
import com.crankuptheamps.client.ExponentialDelayStrategy;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;

@Configuration
public class AmpsConfig {

    @Value("${amps.server.uri}")
    private String ampsUri;

    @Value("${amps.client.name:amps-module1-subscriber}")
    private String clientName;

    /**
     * Single HAClient — one TCP connection to the AMPS server.
     * HAClient auto-reconnects with exponential backoff on disconnect.
     * LoggedBookmarkStore persists the read position so no messages
     * are missed across application restarts.
     *
     * destroyMethod = "close" ensures the connection is cleanly shut
     * down when the Spring context closes.
     */
    @Bean(destroyMethod = "close")
    public HAClient ampsHaClient() throws Exception {
        HAClient client = new HAClient(clientName);
        client.setServerChooser(new DefaultServerChooser(List.of(ampsUri)));
        client.setBookmarkStore(
            new LoggedBookmarkStore("amps-bookmark-" + clientName + ".log")
        );
        client.setReconnectDelay(new ExponentialDelayStrategy(50, 5000));
        client.connect(ampsUri);
        return client;
    }
}
```

---

### VirtualThreadConfig.java

```java
package com.shan.mq.amps.ampsqueueconcurrency.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

@Configuration
public class VirtualThreadConfig {

    /**
     * Max number of messages being processed concurrently.
     * Defaults to 20 as specified in this module.
     *
     * When all 20 permits are held, the subscriber thread will block
     * on semaphore.acquire() — this is intentional backpressure.
     * AMPS keeps messages in the queue server-side; no data is lost.
     */
    @Value("${amps.consumer.virtual-thread-count:20}")
    private int virtualThreadCount;

    /**
     * Creates one new virtual thread per submitted task.
     * Virtual threads are JVM-managed, heap-allocated, ~1-2 KB each.
     * Blocking on JDBC parks the VT and frees the carrier platform thread.
     */
    @Bean(name = "ampsVirtualThreadExecutor", destroyMethod = "shutdown")
    public ExecutorService ampsVirtualThreadExecutor() {
        return Executors.newVirtualThreadPerTaskExecutor();
    }

    /**
     * Backpressure gate: limits how many virtual threads are active at once.
     * Without this, an unbounded number of VTs could be submitted faster
     * than the DB can handle them, causing memory pressure.
     *
     * Rule of thumb: virtualThreadCount should be ≤ HikariCP pool-size × 10
     * because VTs park (not block) while waiting for a DB connection.
     * At 20 VTs and 10 HikariCP connections the ratio is 2:1 — very safe.
     */
    @Bean(name = "ampsConcurrencySemaphore")
    public Semaphore ampsConcurrencySemaphore() {
        return new Semaphore(virtualThreadCount);
    }
}
```

---

### MessageDispatchService.java

```java
package com.shan.mq.amps.ampsqueueconcurrency.service;

import com.crankuptheamps.client.HAClient;
import com.crankuptheamps.client.Message;
import com.shan.mq.amps.ampsqueueconcurrency.processor.MessageProcessor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;

@Service
@Slf4j
public class MessageDispatchService {

    private final ExecutorService executor;
    private final Semaphore semaphore;
    private final MessageProcessor processor;
    private final HAClient client;

    public MessageDispatchService(
            @Qualifier("ampsVirtualThreadExecutor") ExecutorService executor,
            @Qualifier("ampsConcurrencySemaphore")  Semaphore semaphore,
            MessageProcessor processor,
            HAClient client) {
        this.executor  = executor;
        this.semaphore = semaphore;
        this.processor = processor;
        this.client    = client;
    }

    /**
     * Called by the subscriber platform thread for every arriving message.
     *
     * Step 1: acquire a semaphore permit.
     *         If 20 VTs are already running this BLOCKS the subscriber thread.
     *         That is intentional — AMPS pauses delivery naturally.
     *
     * Step 2: submit a virtual thread task.
     *         executor.submit() returns immediately; the VT is created lazily.
     *
     * The subscriber thread then loops back to receive the next message.
     */
    public void dispatch(Message message) {
        try {
            semaphore.acquire();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("Subscriber thread interrupted during semaphore.acquire()");
            return;
        }
        executor.submit(() -> processAndAck(message));
    }

    /**
     * Runs inside a virtual thread.
     * Processes the message, persists to DB, then ACKs AMPS.
     *
     * If processing fails: no ACK is sent.
     * AMPS will re-deliver the message when the lease expires (~5s default).
     */
    private void processAndAck(Message message) {
        String bookmark = message.getBookmark();
        try {
            processor.process(message);      // parse + INSERT
            client.ack(message);             // release AMPS lease
            log.debug("ACK  bookmark={}", bookmark);
        } catch (Exception e) {
            log.error("FAIL bookmark={} — lease will expire and AMPS will re-deliver",
                bookmark, e);
        } finally {
            semaphore.release();             // open slot for next message
        }
    }
}
```

---

### MessageProcessor.java

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

import java.time.Instant;

@Service
@Slf4j
public class MessageProcessor {

    private final ProcessedMessageRepository repo;

    public MessageProcessor(ProcessedMessageRepository repo) {
        this.repo = repo;
    }

    /**
     * Each call to process() runs inside its own virtual thread.
     * @Transactional opens a DB transaction per call — Spring binds it to
     * the current virtual thread via ThreadLocal, which is safe because
     * each virtual thread is independent.
     *
     * Virtual thread parks (not blocks) while waiting for a HikariCP
     * connection, freeing the carrier platform thread for other VTs.
     */
    @Transactional
    public void process(Message message) {
        String payload   = message.getData();
        String messageId = message.getBookmark();
        String topic     = message.getTopic();
        Instant now      = Instant.now();

        log.debug("Processing messageId={} topic={}", messageId, topic);

        // Business logic: parse payload here
        // MyDto dto = objectMapper.readValue(payload, MyDto.class);
        // ... business rules on dto ...

        ProcessedMessage entity = ProcessedMessage.builder()
            .messageId(messageId)
            .topic(topic)
            .payload(payload)
            .status(ProcessingStatus.PROCESSED)
            .receivedAt(now)
            .processedAt(now)
            .build();

        try {
            repo.save(entity);
            log.debug("Saved messageId={}", messageId);

        } catch (DataIntegrityViolationException e) {
            /*
             * Idempotency guard: this message was already saved in a
             * previous delivery attempt (lease expired, AMPS re-delivered).
             * The record exists; no action needed. We will ACK it.
             */
            log.warn("Duplicate delivery detected (idempotent) messageId={}", messageId);
        }
    }
}
```

---

### AmpsQueueSubscriber.java

```java
package com.shan.mq.amps.ampsqueueconcurrency.subscriber;

import com.crankuptheamps.client.HAClient;
import com.crankuptheamps.client.Message;
import com.crankuptheamps.client.MessageStream;
import com.shan.mq.amps.ampsqueueconcurrency.service.MessageDispatchService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

/**
 * Owns the single AMPS receive loop.
 *
 * Implements SmartLifecycle so Spring starts it after all beans are ready
 * and stops it cleanly before the JVM exits (allowing in-flight VTs to
 * finish their ACKs before the HAClient connection closes).
 *
 * This component runs on exactly ONE dedicated platform thread.
 * All concurrency is handled downstream in MessageDispatchService (VTs).
 */
@Component
@Slf4j
public class AmpsQueueSubscriber implements SmartLifecycle {

    private final HAClient client;
    private final MessageDispatchService dispatcher;
    private final String queueTopic;

    private volatile boolean running = false;
    private Thread subscriberThread;

    public AmpsQueueSubscriber(
            HAClient client,
            MessageDispatchService dispatcher,
            @Value("${amps.queue.topic}") String queueTopic) {
        this.client      = client;
        this.dispatcher  = dispatcher;
        this.queueTopic  = queueTopic;
    }

    @Override
    public void start() {
        running = true;
        subscriberThread = Thread.ofPlatform()
            .name("amps-subscriber")
            .daemon(false)
            .start(this::receiveLoop);
        log.info("AMPS subscriber started on topic={}", queueTopic);
    }

    private void receiveLoop() {
        try (MessageStream stream = client.bookmarkSubscribe(queueTopic)) {
            for (Message message : stream) {
                if (!running) break;
                /*
                 * dispatch() acquires a semaphore permit (blocks if 20 VTs busy)
                 * then submits to the virtual thread executor (non-blocking).
                 * This loop is the only place that calls dispatch().
                 */
                dispatcher.dispatch(message);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.info("Subscriber thread interrupted — shutting down");
        } catch (Exception e) {
            log.error("AMPS receive loop terminated with error", e);
        }
    }

    @Override
    public void stop() {
        log.info("Stopping AMPS subscriber...");
        running = false;
        if (subscriberThread != null) {
            subscriberThread.interrupt();
        }
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    /**
     * Phase MAX_VALUE = this bean starts last and stops first.
     * Ensures all Spring beans (DB, repo) are ready before subscribing,
     * and the subscription is torn down before the DB connection closes.
     */
    @Override
    public int getPhase() {
        return Integer.MAX_VALUE;
    }
}
```

---

## Thread & Resource Summary

```text
Component                          Count    Notes
─────────────────────────────────  ──────   ─────────────────────────────────────
Platform threads (subscriber)          1    "amps-subscriber" — blocks on stream
Virtual threads (max concurrent)      20    Created on demand, heap-allocated
Semaphore permits                     20    Gate: prevents > 20 in-flight tasks
HikariCP DB connections               10    VTs park when all 10 are in use
AMPS TCP connections                   1    One HAClient, one bookmark log file
```

## Scaling Limits of Module 1

Module 1 is bounded by a single TCP connection to AMPS. If the AMPS server
publishes messages faster than 20 VTs can process them, the semaphore will
permanently block the subscriber thread and the AMPS lease TTL will start
expiring — messages will be re-queued and re-delivered.

**When to move to Module 2:**

- Message arrival rate > 20 × (average processing time per message)^-1
- e.g. if processing takes 50 ms, Module 1 handles ~400 msg/s max
- For higher throughput → Module 2 (multiple subscribers per JVM across multiple JVMs)
