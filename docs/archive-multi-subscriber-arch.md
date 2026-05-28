# Multi-Subscriber Architecture — AMPS Queue Concurrency

---

## Understanding the Core Question

You asked three things:

1. **Within ONE JVM** — if multiple threads each subscribe to the same AMPS queue,
   does each thread get the same messages or different messages?
2. **What is AMPS's rule?** — how does the server decide which subscriber gets which message,
   and how is duplicate processing prevented?
3. **Across MULTIPLE JVMs** — if 2 or more JVM processes all subscribe to the same queue,
   how does AMPS distribute work, and what happens if two JVMs try to process the same message?

The answers depend entirely on understanding the difference between an
**AMPS Queue** and an **AMPS Topic**, and how AMPS implements its lease-based
competing-consumer model.

---

## Part 1 — AMPS Queue vs Topic: The Fundamental Rule

```text
┌─────────────────────────────────────────────────────────────────────────┐
│                    AMPS TOPIC  (pub/sub broadcast)                      │
│                                                                         │
│  Publisher → msg                                                        │
│                 ├──▶ Subscriber A  receives msg                         │
│                 ├──▶ Subscriber B  receives msg   ← SAME message        │
│                 └──▶ Subscriber C  receives msg   ← ALL get it          │
│                                                                         │
│  Rule: Every subscriber gets every message. Fan-out = N copies.        │
└─────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────┐
│                   AMPS QUEUE  (competing consumers)                     │
│                                                                         │
│  Publisher → [msg1][msg2][msg3][msg4][msg5]  (server-side queue)        │
│                                                                         │
│  Subscriber A  ←── msg1  (leased, no one else sees it)                 │
│  Subscriber B  ←── msg2  (leased, no one else sees it)                 │
│  Subscriber C  ←── msg3  (leased, no one else sees it)                 │
│  Subscriber A  ←── msg4  (A finished msg1, gets next)                  │
│  Subscriber B  ←── msg5                                                 │
│                                                                         │
│  Rule: Each message goes to EXACTLY ONE subscriber at a time.          │
│        No two subscribers ever receive the same active message.        │
└─────────────────────────────────────────────────────────────────────────┘
```

**This is the answer to your first question:**
Multiple subscribers on the same AMPS queue do NOT all get the same messages.
Each message is dispatched to exactly one subscriber. AMPS distributes the workload
across all connected subscribers.

---

## Part 2 — How AMPS Prevents Duplicate Processing: The Lease Model

```text
AMPS Queue Server State for /queue/trades
┌────────────────────────────────────────────────────────┐
│  msg1  │ state: LEASED   │ leased_to: client-A │ ttl: 4s │
│  msg2  │ state: LEASED   │ leased_to: client-B │ ttl: 3s │
│  msg3  │ state: AVAILABLE│ leased_to: (none)   │         │
│  msg4  │ state: AVAILABLE│ leased_to: (none)   │         │
│  msg5  │ state: ACKED    │ leased_to: client-A │ done ✓  │
└────────────────────────────────────────────────────────┘

Rules:
  - AVAILABLE   → any subscriber can receive it
  - LEASED      → invisible to all other subscribers until lease expires
  - ACKED       → permanently removed from queue
  - lease expiry → returns to AVAILABLE → re-delivered (possibly to different subscriber)
```

### The Lease Lifecycle

```text
  Message arrives in queue
          │
          ▼
    [AVAILABLE] ─────────────────────────────────────────┐
          │                                               │
   Subscriber receives it                                 │
          │                                               │
          ▼                                               │
     [LEASED to Subscriber X]                            │
          │                                               │
    ┌─────┴──────────────────────────────┐               │
    │                                    │               │
    ▼                                    ▼               │
Subscriber ACKs              Lease timeout expires       │
(within TTL)                 (no ACK received)           │
    │                                    │               │
    ▼                                    ▼               │
 [ACKED]                          [AVAILABLE] ───────────┘
 removed                          re-queued, given to
 from queue                       another subscriber
```

**At-most-one active delivery, but at-least-once overall:**
A message can only be processed twice if the first subscriber processed it
(saved to DB) but crashed BEFORE sending the ACK. This is the only window
for duplicate processing — and the reason idempotency is non-negotiable.

---

## Part 3 — Scenario A: Multiple Subscribers in a Single JVM

### What "multiple subscribers" means in a single JVM

There are two very different approaches inside a single JVM:

```text
╔══════════════════════════════════════════════════════════════════════╗
║  APPROACH 1 (recommended — used in CLAUDE.md)                       ║
║  ONE HAClient  →  ONE subscription  →  ONE subscriber thread        ║
║                                →  N virtual threads (fan-out)       ║
║                                                                      ║
║  Single connection to AMPS. Messages arrive on one thread.          ║
║  Processing is parallelized via virtual threads AFTER receipt.      ║
╚══════════════════════════════════════════════════════════════════════╝

╔══════════════════════════════════════════════════════════════════════╗
║  APPROACH 2 (this document)                                          ║
║  N HAClients  →  N subscriptions  →  N subscriber threads           ║
║               each → M virtual threads (fan-out per subscriber)     ║
║                                                                      ║
║  N parallel connections to AMPS. AMPS distributes messages across   ║
║  all N connections. Each connection also fans out to VTs.           ║
╚══════════════════════════════════════════════════════════════════════╝
```

### Single JVM — Multi-Subscriber Architecture Diagram

```text
┌─────────────────────────────────────────────────────────────────────────┐
│                        AMPS Server                                      │
│              /queue/trades  [m1][m2][m3][m4][m5][m6]                   │
│                                                                         │
│  AMPS distributes messages round-robin across ALL active connections    │
└──────────┬──────────────────┬──────────────────┬───────────────────────┘
           │                  │                  │
           │ TCP conn-1        │ TCP conn-2        │ TCP conn-3
           ▼                  ▼                  ▼
┌──────────────────────────────────────────────────────────────────────────┐
│                   Single JVM — Spring Boot Application                   │
│                                                                          │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐                  │
│  │ HAClient-1   │  │ HAClient-2   │  │ HAClient-3   │                  │
│  │ name:node-1-a│  │ name:node-1-b│  │ name:node-1-c│                  │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘                  │
│         │                 │                 │                            │
│  ┌──────▼───────┐  ┌──────▼───────┐  ┌──────▼───────┐                  │
│  │ Subscriber-1 │  │ Subscriber-2 │  │ Subscriber-3 │                  │
│  │ (platform    │  │ (platform    │  │ (platform    │  ← each a        │
│  │  thread)     │  │  thread)     │  │  thread)     │    dedicated     │
│  │  gets m1,m4  │  │  gets m2,m5  │  │  gets m3,m6  │    platform thd │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘                  │
│         │ dispatch         │ dispatch         │ dispatch                 │
│         ▼                  ▼                  ▼                          │
│  ┌─────────────────────────────────────────────────────────────────┐    │
│  │              Shared VirtualThreadExecutor                        │    │
│  │         Executors.newVirtualThreadPerTaskExecutor()              │    │
│  │                                                                  │    │
│  │  VT-1 process(m1)  VT-2 process(m2)  VT-3 process(m3)          │    │
│  │  VT-4 process(m4)  VT-5 process(m5)  VT-6 process(m6)          │    │
│  └──────────────────────────────┬──────────────────────────────────┘    │
│                                  │                                        │
│                  ┌───────────────▼────────────────┐                      │
│                  │   HikariCP (20 connections)     │                      │
│                  └───────────────┬────────────────┘                      │
│                  ┌───────────────▼────────────────┐                      │
│                  │   Local H2 / PostgreSQL DB      │                      │
│                  │   UNIQUE(message_id)            │                      │
│                  └─────────────────────────────────┘                     │
└──────────────────────────────────────────────────────────────────────────┘
```

### Message Distribution Table — Single JVM, 3 Subscribers, 6 Messages

```text
Message  │  AMPS assigns to  │  Subscriber thread  │  Virtual Thread
─────────┼───────────────────┼─────────────────────┼─────────────────
  m1     │  conn-1 (S1)      │  Subscriber-1       │  VT-1
  m2     │  conn-2 (S2)      │  Subscriber-2       │  VT-2
  m3     │  conn-3 (S3)      │  Subscriber-3       │  VT-3
  m4     │  conn-1 (S1)      │  Subscriber-1       │  VT-4
  m5     │  conn-2 (S2)      │  Subscriber-2       │  VT-5
  m6     │  conn-3 (S3)      │  Subscriber-3       │  VT-6

No two subscriber threads ever receive the same message simultaneously.
AMPS enforces this at the server; the lease prevents double delivery.
```

---

## Part 4 — Scenario B: Multiple JVM Processes, Each Multi-Threaded

This is horizontal scaling. Each JVM process is a separate physical process with
its own memory space, its own HAClient connections, and its own JVM resources.
AMPS treats each connection from each JVM as an independent competing consumer.

### Multi-JVM Architecture Diagram

```text
┌────────────────────────────────────────────────────────────────────────────┐
│                             AMPS Server                                    │
│      /queue/trades  [m01][m02][m03]...[m30]  (30 messages)                │
│                                                                            │
│   All HAClient connections compete for messages — AMPS picks one          │
│   per message, leases it, and no other connection can see it              │
└────┬──────────────────────┬──────────────────────┬─────────────────────────┘
     │  3 TCP connections    │  3 TCP connections    │  3 TCP connections
     │  from JVM-1           │  from JVM-2           │  from JVM-3
     ▼                       ▼                       ▼
┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐
│   JVM Process 1 │  │   JVM Process 2 │  │   JVM Process 3 │
│   (host: node1) │  │   (host: node2) │  │   (host: node3) │
│                 │  │                 │  │                 │
│  HAClient-1a    │  │  HAClient-2a    │  │  HAClient-3a    │
│  HAClient-1b    │  │  HAClient-2b    │  │  HAClient-3b    │
│  HAClient-1c    │  │  HAClient-2c    │  │  HAClient-3c    │
│                 │  │                 │  │                 │
│  Subscriber-1a  │  │  Subscriber-2a  │  │  Subscriber-3a  │
│  Subscriber-1b  │  │  Subscriber-2b  │  │  Subscriber-3b  │
│  Subscriber-1c  │  │  Subscriber-2c  │  │  Subscriber-3c  │
│                 │  │                 │  │                 │
│  VT pool (200)  │  │  VT pool (200)  │  │  VT pool (200)  │
│                 │  │                 │  │                 │
│  Gets ~10 msgs  │  │  Gets ~10 msgs  │  │  Gets ~10 msgs  │
└────────┬────────┘  └────────┬────────┘  └────────┬────────┘
         │                    │                    │
         └────────────────────┼────────────────────┘
                              │
              ┌───────────────▼───────────────┐
              │   Shared PostgreSQL Database   │
              │   UNIQUE constraint: message_id│
              │   Handles concurrent inserts   │
              │   from all 3 JVMs safely       │
              └───────────────────────────────┘
```

### How AMPS Distributes Across JVMs

```text
Timeline: 9 messages, 3 JVMs × 3 subscribers each = 9 active connections

AMPS queue state:
  [m01] → leased to JVM1/conn-a  (JVM1 processes m01)
  [m02] → leased to JVM2/conn-a  (JVM2 processes m02)
  [m03] → leased to JVM3/conn-a  (JVM3 processes m03)
  [m04] → leased to JVM1/conn-b
  [m05] → leased to JVM2/conn-b
  [m06] → leased to JVM3/conn-b
  [m07] → leased to JVM1/conn-c
  [m08] → leased to JVM2/conn-c
  [m09] → leased to JVM3/conn-c

No JVM sees another JVM's leased message.
Each message is processed by exactly one virtual thread in exactly one JVM.
```

### What Happens When JVM-2 Crashes Mid-Processing?

```text
  m05 is leased to JVM2/conn-b
  JVM2 saves m05 to its local DB  ← processing done
  JVM2 crashes before sending ACK ← ACK never reaches AMPS

  AMPS lease timer for m05 expires (e.g. after 5s)
  m05 returns to AVAILABLE state

  m05 is re-leased to JVM1/conn-a (next available subscriber)
  JVM1 tries to INSERT m05 into shared DB
  → UNIQUE constraint violation (m05 already exists from JVM2's save)
  → catch DataIntegrityViolationException
  → treat as success (idempotent)
  → send ACK to AMPS

Result: m05 processed twice at DB level but only one record exists.
        Idempotency ensures correctness.
```

---

## Part 5 — Client Naming Strategy (Critical)

AMPS identifies subscribers by their **client name**. Client names MUST be:

- **Unique per connection** — two connections with the same name will conflict
- **Stable across restarts** — so the `LoggedBookmarkStore` can resume from where it left off
- **Descriptive** — for debugging in the AMPS admin console

### Naming Strategy

```text
Single JVM, 3 subscribers:
  {app-name}-{hostname}-sub-1
  {app-name}-{hostname}-sub-2
  {app-name}-{hostname}-sub-3

Multi JVM (3 nodes, 3 subscribers each):
  JVM1:  amps-queue-node1-sub-1,  amps-queue-node1-sub-2,  amps-queue-node1-sub-3
  JVM2:  amps-queue-node2-sub-1,  amps-queue-node2-sub-2,  amps-queue-node2-sub-3
  JVM3:  amps-queue-node3-sub-1,  amps-queue-node3-sub-2,  amps-queue-node3-sub-3
```

**Why it matters:** If JVM1 and JVM2 both register a connection with the same
client name, AMPS will reject or kick out one of them — only one connection per
unique name is allowed at a time.

---

## Part 6 — Implementation: Single JVM, Multiple Subscribers

### application.yaml (single JVM)

```yaml
spring:
  application:
    name: amps-queue-concurrency
  datasource:
    url: jdbc:h2:file:./data/amps-messages;AUTO_SERVER=TRUE
    hikari:
      maximum-pool-size: 20
      minimum-idle: 5
  jpa:
    hibernate:
      ddl-auto: update
    open-in-view: false

amps:
  server:
    uri: tcp://localhost:9004/amps/json
  queue:
    topic: /queue/trades
  consumer:
    subscriber-count: 3          # number of parallel HAClient connections
    max-concurrency: 200         # total semaphore permits across all subscribers
```

### MultiSubscriberConfig.java

```java
package com.shan.mq.amps.ampsqueueconcurrency.config;

import com.crankuptheamps.client.HAClient;
import com.crankuptheamps.client.DefaultServerChooser;
import com.crankuptheamps.client.LoggedBookmarkStore;
import com.crankuptheamps.client.ExponentialDelayStrategy;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.net.InetAddress;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

@Configuration
public class MultiSubscriberConfig {

    @Value("${amps.server.uri}")
    private String ampsUri;

    @Value("${amps.consumer.subscriber-count:3}")
    private int subscriberCount;

    @Value("${amps.consumer.max-concurrency:200}")
    private int maxConcurrency;

    /**
     * Creates N independent HAClient beans, each with a unique client name.
     * AMPS treats each as a separate competing consumer on the queue.
     *
     * Returns a list so AmpsSubscriberPool can iterate over them.
     */
    @Bean(destroyMethod = "")   // destroy handled by AmpsSubscriberPool
    public List<HAClient> ampsClientPool() throws Exception {
        String hostname = InetAddress.getLocalHost().getHostName();
        List<HAClient> clients = new java.util.ArrayList<>();

        for (int i = 1; i <= subscriberCount; i++) {
            String clientName = "amps-queue-" + hostname + "-sub-" + i;
            HAClient client = new HAClient(clientName);
            client.setServerChooser(new DefaultServerChooser(List.of(ampsUri)));
            // Each subscriber gets its own bookmark log file
            client.setBookmarkStore(
                new LoggedBookmarkStore("amps-bookmark-" + clientName + ".log")
            );
            client.setReconnectDelay(new ExponentialDelayStrategy(50, 5000));
            client.connect(ampsUri);
            clients.add(client);
        }
        return clients;
    }

    /** One virtual thread per task — shared across all subscriber dispatchers */
    @Bean(name = "sharedVirtualThreadExecutor")
    public ExecutorService sharedVirtualThreadExecutor() {
        return Executors.newVirtualThreadPerTaskExecutor();
    }

    /**
     * Semaphore shared across all subscribers.
     * Total in-flight virtual threads = maxConcurrency, regardless of
     * how many subscriber connections are open.
     */
    @Bean
    public Semaphore sharedConcurrencySemaphore() {
        return new Semaphore(maxConcurrency);
    }
}
```

### AmpsSubscriberPool.java

```java
package com.shan.mq.amps.ampsqueueconcurrency.subscriber;

import com.crankuptheamps.client.HAClient;
import com.crankuptheamps.client.Message;
import com.crankuptheamps.client.MessageStream;
import com.shan.mq.amps.ampsqueueconcurrency.service.MessageDispatchService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

/**
 * Manages a pool of subscriber threads — one platform thread per HAClient.
 * Each thread blocks on its own MessageStream and dispatches to virtual threads.
 *
 * This gives AMPS N separate connections to distribute messages across,
 * effectively multiplying the ingest throughput by N within a single JVM.
 */
@Component
@Slf4j
public class AmpsSubscriberPool implements SmartLifecycle {

    private final List<HAClient> clientPool;
    private final MessageDispatchService dispatcher;
    private final String queueTopic;

    private volatile boolean running = false;
    private final List<Thread> subscriberThreads = new ArrayList<>();

    public AmpsSubscriberPool(
            List<HAClient> clientPool,
            MessageDispatchService dispatcher,
            @Value("${amps.queue.topic}") String queueTopic) {
        this.clientPool = clientPool;
        this.dispatcher = dispatcher;
        this.queueTopic = queueTopic;
    }

    @Override
    public void start() {
        running = true;
        for (int i = 0; i < clientPool.size(); i++) {
            HAClient client = clientPool.get(i);
            int idx = i + 1;
            Thread t = Thread.ofPlatform()
                .name("amps-subscriber-" + idx)
                .daemon(false)
                .start(() -> receiveLoop(client, idx));
            subscriberThreads.add(t);
        }
        log.info("Started {} AMPS subscriber threads on topic={}",
            clientPool.size(), queueTopic);
    }

    private void receiveLoop(HAClient client, int subscriberIndex) {
        log.info("Subscriber-{} connected, listening on {}", subscriberIndex, queueTopic);
        try (MessageStream stream = client.bookmarkSubscribe(queueTopic)) {
            for (Message message : stream) {
                if (!running) break;
                // dispatcher fans out to virtual threads with semaphore backpressure
                dispatcher.dispatch(message, client);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            log.error("Subscriber-{} receive loop error", subscriberIndex, e);
        }
    }

    @Override
    public void stop() {
        running = false;
        subscriberThreads.forEach(Thread::interrupt);
        // Close all HAClient connections
        clientPool.forEach(client -> {
            try { client.close(); } catch (Exception ignored) {}
        });
    }

    @Override public boolean isRunning() { return running; }
    @Override public int getPhase()      { return Integer.MAX_VALUE; }
}
```

### MessageDispatchService.java (updated for pool)

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

    private final ExecutorService virtualExecutor;
    private final Semaphore semaphore;
    private final MessageProcessor processor;

    public MessageDispatchService(
            @Qualifier("sharedVirtualThreadExecutor") ExecutorService virtualExecutor,
            Semaphore semaphore,
            MessageProcessor processor) {
        this.virtualExecutor = virtualExecutor;
        this.semaphore = semaphore;
        this.processor = processor;
    }

    /**
     * Called from any subscriber thread.
     * client reference passed in so the virtual thread can ACK on the
     * correct connection (AMPS requires ACK on the same connection that
     * received the message).
     */
    public void dispatch(Message message, HAClient client) {
        try {
            semaphore.acquire();    // blocks this subscriber thread if overloaded
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
        }
        virtualExecutor.submit(() -> processAndAck(message, client));
    }

    private void processAndAck(Message message, HAClient client) {
        String bookmark = message.getBookmark();
        try {
            processor.process(message);
            client.ack(message);    // ACK must go back on the receiving connection
            log.debug("ACK  bookmark={}", bookmark);
        } catch (Exception e) {
            log.error("FAIL bookmark={}", bookmark, e);
            // No ACK — lease expires, AMPS re-delivers
        } finally {
            semaphore.release();
        }
    }
}
```

---

## Part 7 — Implementation: Multi-JVM, Multi-Subscriber

In a multi-JVM setup, each JVM process is completely independent. The **only**
shared resource is the database. The critical difference from single-JVM is:

- Client names must embed the **hostname** (auto-detected) to ensure global uniqueness
- The bookmark store file path must be unique per JVM (hostname-scoped)
- The database must be PostgreSQL (shared across JVMs, not H2)
- All DB writes must be **idempotent** — a message re-delivered after JVM crash must
  not cause duplicate rows or exceptions that prevent the ACK

### application.yaml (multi-JVM / production)

```yaml
spring:
  application:
    name: amps-queue-concurrency
  datasource:
    # Shared PostgreSQL — same URL in all JVM instances
    url: jdbc:postgresql://db-host:5432/ampsdb
    username: amps_user
    password: ${DB_PASSWORD}
    hikari:
      maximum-pool-size: 20
      minimum-idle: 5
      connection-timeout: 3000
  jpa:
    hibernate:
      ddl-auto: validate      # schema managed externally in prod
    open-in-view: false

amps:
  server:
    uri: tcp://amps-host:9004/amps/json
  queue:
    topic: /queue/trades
  consumer:
    subscriber-count: 3       # per-JVM parallel connections
    max-concurrency: 200      # per-JVM semaphore limit
```

### MultiJvmAmpsConfig.java

```java
package com.shan.mq.amps.ampsqueueconcurrency.config;

import com.crankuptheamps.client.HAClient;
import com.crankuptheamps.client.DefaultServerChooser;
import com.crankuptheamps.client.LoggedBookmarkStore;
import com.crankuptheamps.client.ExponentialDelayStrategy;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.net.InetAddress;
import java.nio.file.Paths;
import java.util.List;

@Configuration
public class MultiJvmAmpsConfig {

    @Value("${amps.server.uri}")
    private String ampsUri;

    @Value("${amps.consumer.subscriber-count:3}")
    private int subscriberCount;

    /**
     * Each JVM instance auto-detects its hostname and creates client names like:
     *   amps-queue-node1-sub-1
     *   amps-queue-node1-sub-2
     *   amps-queue-node2-sub-1   (different JVM on node2)
     *   amps-queue-node2-sub-2
     *
     * This guarantees global uniqueness with zero configuration.
     */
    @Bean(destroyMethod = "")
    public List<HAClient> ampsClientPool() throws Exception {
        String hostname = InetAddress.getLocalHost().getHostName()
                            .replaceAll("[^a-zA-Z0-9-]", "-");   // safe for file names
        List<HAClient> clients = new java.util.ArrayList<>();

        for (int i = 1; i <= subscriberCount; i++) {
            String clientName = "amps-queue-" + hostname + "-sub-" + i;

            // Bookmark file is per-client, per-host — survives JVM restart
            String bookmarkFile = Paths.get(
                System.getProperty("user.home"),
                ".amps",
                "bookmark-" + clientName + ".log"
            ).toString();

            HAClient client = new HAClient(clientName);
            client.setServerChooser(new DefaultServerChooser(List.of(ampsUri)));
            client.setBookmarkStore(new LoggedBookmarkStore(bookmarkFile));
            client.setReconnectDelay(new ExponentialDelayStrategy(50, 5000));
            client.connect(ampsUri);

            clients.add(client);
        }
        return clients;
    }
}
```

### MessageProcessor.java — Idempotent Upsert (Multi-JVM critical)

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

    @Transactional
    public void process(Message message) {
        String payload   = message.getData();
        String messageId = message.getBookmark();   // globally unique AMPS bookmark
        String topic     = message.getTopic();

        log.debug("Processing messageId={} topic={}", messageId, topic);

        // Parse business payload
        // AmpsMessageDto dto = parsePayload(payload);

        ProcessedMessage entity = ProcessedMessage.builder()
            .messageId(messageId)
            .topic(topic)
            .payload(payload)
            .status(ProcessingStatus.PROCESSED)
            .receivedAt(Instant.now())
            .processedAt(Instant.now())
            .build();

        try {
            repo.save(entity);
            log.debug("Saved messageId={}", messageId);

        } catch (DataIntegrityViolationException e) {
            // Duplicate: this JVM received a re-delivery of a message that was
            // already saved by this JVM or another JVM before a crash.
            // Safe to ignore — the record already exists; we will ACK it.
            log.warn("Duplicate message detected (idempotent) messageId={}", messageId);

        } catch (Exception e) {
            // Unexpected failure — rethrow to prevent ACK; let AMPS re-deliver
            log.error("Processing failed messageId={}", messageId, e);
            throw e;
        }
    }
}
```

### ProcessedMessage Entity (shared schema)

```java
package com.shan.mq.amps.ampsqueueconcurrency.model;

import jakarta.persistence.*;
import lombok.*;
import java.time.Instant;

@Entity
@Table(
    name = "processed_messages",
    indexes = {
        @Index(name = "idx_pm_message_id", columnList = "message_id", unique = true),
        @Index(name = "idx_pm_received_at", columnList = "received_at")
    }
)
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Getter
public class ProcessedMessage {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    /**
     * AMPS bookmark — globally unique across all messages on this queue.
     * UNIQUE constraint here is what makes the multi-JVM pattern safe.
     */
    @Column(name = "message_id", unique = true, nullable = false)
    private String messageId;

    @Column(name = "topic")
    private String topic;

    @Lob
    @Column(name = "payload")
    private String payload;

    @Enumerated(EnumType.STRING)
    @Column(name = "status")
    private ProcessingStatus status;

    @Column(name = "received_at")
    private Instant receivedAt;

    @Column(name = "processed_at")
    private Instant processedAt;

    /**
     * Which JVM/subscriber processed this message — useful for debugging
     * re-delivery scenarios across JVMs.
     */
    @Column(name = "processed_by")
    private String processedBy;    // InetAddress.getLocalHost().getHostName()
}
```

---

## Part 8 — Comparison: Single JVM vs Multi-JVM

```text
┌────────────────────────────┬────────────────────────────┬────────────────────────────┐
│ Dimension                  │ Single JVM Multi-Subscriber │ Multi-JVM Multi-Subscriber │
├────────────────────────────┼────────────────────────────┼────────────────────────────┤
│ AMPS connections           │ N (within one process)      │ N × JVMs (across processes)│
│ Message distribution       │ Round-robin across N conns  │ Round-robin across all conns│
│ Throughput scaling         │ Limited by one JVM CPU/heap │ Linear: add more JVMs      │
│ Fault tolerance            │ Single point of failure     │ JVM crash = others continue│
│ DB                         │ H2 or shared Postgres       │ Shared Postgres required   │
│ Idempotency needed?        │ Yes (lease expiry)          │ Yes (lease expiry + crash) │
│ Client name uniqueness     │ hostname + sub-index        │ hostname + sub-index       │
│ Bookmark store             │ Per-client file (local)     │ Per-client file (per host) │
│ Operational complexity     │ Low — one process           │ Higher — orchestration     │
│ Use when                   │ Dev, moderate load          │ Prod, high throughput      │
└────────────────────────────┴────────────────────────────┴────────────────────────────┘
```

---

## Part 9 — Anti-Patterns to Avoid

```text
╔══════════════════════════════════════════════════════════════════════╗
║  ANTI-PATTERN 1: Same client name for multiple connections           ║
║                                                                      ║
║  HAClient c1 = new HAClient("my-app");  // node1                    ║
║  HAClient c2 = new HAClient("my-app");  // node2 — CONFLICT!        ║
║                                                                      ║
║  Result: AMPS will disconnect one of them. Intermittent failures.    ║
╚══════════════════════════════════════════════════════════════════════╝

╔══════════════════════════════════════════════════════════════════════╗
║  ANTI-PATTERN 2: Subscribing to a topic instead of a queue          ║
║                                                                      ║
║  client.subscribe("/trades")   // topic — ALL subscribers get ALL   ║
║                                // messages → massive duplicates      ║
║                                                                      ║
║  Correct: client.subscribe("/queue/trades")   // queue path         ║
╚══════════════════════════════════════════════════════════════════════╝

╔══════════════════════════════════════════════════════════════════════╗
║  ANTI-PATTERN 3: ACKing on a different connection than received on   ║
║                                                                      ║
║  Message received by HAClient-1                                      ║
║  client2.ack(message)   ← wrong connection — AMPS ignores this ACK  ║
║                                                                      ║
║  Always ACK on the same HAClient that received the message.         ║
╚══════════════════════════════════════════════════════════════════════╝

╔══════════════════════════════════════════════════════════════════════╗
║  ANTI-PATTERN 4: No idempotency guard in multi-JVM                  ║
║                                                                      ║
║  JVM1 saves msg, crashes before ACK                                  ║
║  AMPS re-delivers msg to JVM2                                        ║
║  JVM2 saves msg → duplicate row → data corruption                   ║
║                                                                      ║
║  Fix: UNIQUE(message_id) + catch DataIntegrityViolationException     ║
╚══════════════════════════════════════════════════════════════════════╝

╔══════════════════════════════════════════════════════════════════════╗
║  ANTI-PATTERN 5: Unbounded subscriber count without semaphore        ║
║                                                                      ║
║  3 subscribers × 200 VTs = 600 in-flight DB operations              ║
║  HikariCP pool = 20 connections                                      ║
║  → VTs park, but if semaphore not set correctly, subscriber threads  ║
║    buffer thousands of tasks → OOM on task queue objects             ║
║                                                                      ║
║  Fix: semaphore permits = (subscriberCount × msgRate) bounded by     ║
║       a reasonable multiple of HikariCP pool size                   ║
╚══════════════════════════════════════════════════════════════════════╝
```

---

## Part 10 — Summary: The Golden Rules

```text
┌────────────────────────────────────────────────────────────────────┐
│                 AMPS Queue — Golden Rules                           │
│                                                                     │
│  1. ONE message  →  ONE subscriber  (AMPS enforces via lease)      │
│                                                                     │
│  2. Multiple connections = work distribution, not duplication      │
│                                                                     │
│  3. Duplicate delivery ONLY occurs on lease expiry (crash path)   │
│     → Always guard with UNIQUE(message_id) + idempotent upsert    │
│                                                                     │
│  4. ACK MUST go back on the SAME connection that received          │
│                                                                     │
│  5. Client names MUST be unique across ALL connections globally    │
│     → Use hostname + index pattern                                  │
│                                                                     │
│  6. Bookmark store MUST be per-client-name                         │
│     → Enables replay after restart with no missed messages         │
│                                                                     │
│  7. For multi-JVM: use shared Postgres, not H2                     │
│     → H2 is in-process; cannot be shared across JVMs              │
└────────────────────────────────────────────────────────────────────┘
```
