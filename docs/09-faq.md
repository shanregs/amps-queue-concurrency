# AMPS Queue — Frequently Asked Questions

This document captures common questions and answers about AMPS queue
subscriptions, concurrency, virtual threads, and multi-JVM patterns.

---

## Q1 — Multiple threads subscribing to the same queue in one JVM: do they get the same messages?

**Answer: NO.**

AMPS Queue uses the **competing consumer model**. Each message is delivered
to exactly one subscriber. Multiple subscriber threads do NOT each receive a
copy of the same message — they compete for different messages.

```text
AMPS Queue: [m1][m2][m3][m4][m5][m6]

Subscriber-1 thread  gets  m1, m4
Subscriber-2 thread  gets  m2, m5
Subscriber-3 thread  gets  m3, m6

No subscriber receives a message that another subscriber already received.
```

This is the fundamental difference between an AMPS **Queue** (`/queue/trades`)
and an AMPS **Topic** (`/trades`). A topic broadcasts every message to every
subscriber. A queue distributes messages — each message goes to one subscriber only.

**Key rule:** Multiple subscribers = work distribution, not message duplication.

---

## Q2 — How does AMPS prevent duplicate processing?

**Answer: The Lease Model.**

When AMPS delivers a message to subscriber A, it applies a **lease** to that
message. While the lease is active, the message is **invisible** to all other
subscribers. No other subscriber can receive it, even if they ask for it.

```text
Message state machine:

  AVAILABLE ──────────────▶ LEASED (to subscriber A)
      ▲                          │
      │ lease expires             │ A calls client.ack()
      │ (no ACK received)         ▼
      └──────────────── ◀── ACKED (removed permanently)
```

Duplicate delivery can only happen in **one window**:
subscriber A processes the message and saves it to DB,
but crashes **before** sending the ACK.
The lease expires, and AMPS re-delivers the message to subscriber B.

This is "at-least-once" delivery — guaranteed no loss, rare possible duplicate.
The fix is an idempotent `UNIQUE(message_id)` constraint in the database.

---

## Q3 — Multiple JVMs, each with multiple subscriber threads: how does AMPS handle it?

**Answer: AMPS treats every HAClient connection as an independent competing consumer, regardless of which JVM it comes from.**

```text
JVM-1 subscriber-1  ─┐
JVM-1 subscriber-2  ─┤
JVM-1 subscriber-3  ─┤──▶ AMPS distributes one message per connection
JVM-2 subscriber-1  ─┤    round-robin or first-available
JVM-2 subscriber-2  ─┤
JVM-2 subscriber-3  ─┘
```

AMPS does not know or care whether connections come from the same JVM or
different physical machines. It sees connections — not JVMs.

**If JVM-2 crashes:**

- JVM-2's TCP connections drop
- Any messages leased to JVM-2 subscribers have their leases expire
- AMPS re-delivers those messages to surviving connections (JVM-1 subscribers)
- JVM-1 catches `DataIntegrityViolationException` if JVM-2 already saved the row
- Result: idempotent, no data loss, no duplicate records

---

## Q4 — What is an AMPS Bookmark and why does it matter?

**Answer:** A bookmark is a unique identifier assigned by the AMPS server to
each message in a queue. It is globally unique per queue.

```text
Message arrives →  AMPS assigns bookmark = "20240527T160000000000001"
Subscriber receives message with this bookmark
Subscriber calls client.ack(message)  → AMPS uses the bookmark to mark it done
```

**Why it matters for your application:**

1. **ACKing** — ACKs reference the bookmark, not the message content
2. **Idempotency** — Store `message.getBookmark()` as `message_id` in your DB;
   the `UNIQUE` constraint on this column prevents duplicate rows on re-delivery
3. **Bookmark Store** — The `LoggedBookmarkStore` persists your position in the
   queue to disk; on JVM restart, the AMPS server replays any unACKed messages
   so nothing is missed even if the JVM was down for hours

---

## Q5 — What happens to messages if the JVM crashes mid-processing?

**Three cases depending on when the crash occurs:**

```text
Case 1: Crash BEFORE processing
  Message leased, JVM crashes, no DB save, no ACK.
  → Lease expires → AMPS re-delivers → processed correctly by another subscriber.
  → No data loss. No duplicate.

Case 2: Crash AFTER processing, BEFORE ACK  (the dangerous window)
  Message leased, DB save succeeds, JVM crashes, no ACK.
  → Lease expires → AMPS re-delivers → another subscriber tries to save.
  → DataIntegrityViolationException (UNIQUE constraint) → caught and ignored.
  → ACK sent. Message removed from queue.
  → No data loss. No duplicate record (idempotency handled it).

Case 3: Crash AFTER ACK
  ACK received by AMPS before crash. Message permanently removed from queue.
  → No re-delivery. DB row exists. Correct state.
```

**The UNIQUE constraint + idempotent catch is what makes Case 2 safe.**
Without it, Case 2 would cause a duplicate DB row and potentially
corrupt business data.

---

## Q6 — Can I have one HAClient with multiple subscriptions to the same queue?

**Yes, technically. But it is NOT the recommended pattern for queues.**

```text
// Do NOT do this for queues:
HAClient client = new HAClient("my-app");
MessageStream stream1 = client.subscribe("/queue/trades");   // sub-id-1
MessageStream stream2 = client.subscribe("/queue/trades");   // sub-id-2
```

**Why this is problematic:**

- Both subscriptions share the same TCP connection
- AMPS may route messages to either sub-id, but ACKs and processing still
  go through one connection — no parallelism at the network level
- If one subscription stalls (e.g., blocked), it can affect the other
- The bookmark store becomes ambiguous — which sub-id's position do you track?

**Recommended pattern:** One `HAClient` per subscriber thread.
Each HAClient = one TCP connection = one bookmark store = one clear position.

---

## Q7 — What is the difference between `subscribe()` and `bookmarkSubscribe()`?

```text
subscribe()
  - Receives new messages arriving after the subscription starts
  - If your JVM was down for 1 hour, you MISS messages that arrived during that hour
  - Use for: low-stakes real-time feeds, canary testing, SOW-only queries

bookmarkSubscribe()
  - Persists your read position (bookmark) to disk via LoggedBookmarkStore
  - On restart, AMPS replays all messages you have not yet ACKed
  - If your JVM was down for 1 hour, you receive ALL messages from that hour on reconnect
  - Use for: queues, any at-least-once processing, financial message processing

Rule: Always use bookmarkSubscribe() for queue processing in production.
```

---

## Q8 — How do I tune the virtual thread count (semaphore permits)?

The semaphore permits control how many messages are being processed concurrently.
Setting them too low wastes throughput; too high risks overwhelming the DB.

```text
Recommended formula:
  permits = min(
    desired_max_concurrent,
    HikariCP_pool_size × 10
  )

Why ×10?
  Virtual threads PARK (not block) while waiting for a DB connection.
  One DB connection can serve ~10 parked VTs sequentially within the same
  time window — so 20 DB connections can support ~200 concurrent VTs safely.

Example:
  HikariCP pool = 20 connections
  → permits = min(200, 20 × 10) = 200 — safe upper bound
  → Start conservative: 20 permits. Increase while monitoring DB pool wait times.

Signals to increase permits:
  - semaphore.availablePermits() is frequently > 0 (headroom available)
  - HikariCP connection wait time is near 0 (pool not stressed)

Signals to decrease permits:
  - HikariCP connectionTimeout exceptions in logs
  - DB CPU at 100%
  - Transaction timeouts
```

---

## Q9 — What is the AMPS lease timeout and how do I configure it on the subscriber?

The lease timeout is configured **server-side** in the AMPS config file
(e.g., `AMPS.xml`) per queue. It is not set by the Java client.

```xml
<!-- AMPS server configuration (not Java code) -->
<Queue>
    <Name>trades</Name>
    <Topic>/queue/trades</Topic>
    <MessageType>json</MessageType>
    <LeaseTimeout>30000</LeaseTimeout>   <!-- milliseconds: 30 seconds -->
</Queue>
```

**Impact on your Java application:**

| Lease timeout | Effect |
| --- | --- |
| Too short (< 5s) | Messages re-queued before slow VTs finish → frequent duplicates |
| Too long (> 60s) | Crashed JVM's messages stuck for a long time before re-delivery |
| Recommended | 2 × p99 processing time — gives slow messages time to finish |

---

## Q10 — Why must the ACK go back on the same connection that received the message?

AMPS tracks which connection leased each message. The ACK is validated against
the connection that originally received it.

```text
Correct:
  HAClient-A  receives m01  →  HAClient-A.ack(m01)  ✓

Wrong:
  HAClient-A  receives m01  →  HAClient-B.ack(m01)  ✗ (AMPS ignores this ACK)
```

This is why in `MessageDispatchService` the `SubscriberContext` (containing the
correct `HAClient` reference) is passed all the way into the virtual thread.
The virtual thread does the processing AND calls `ctx.client().ack(message)`.

---

## Q11 — Will AMPS guarantee message order within the queue?

**Per-subscriber: yes. Across competing subscribers: no.**

```text
If you have ONE subscriber:
  Messages are delivered in publish order: m1, m2, m3, m4 ...
  Processing order depends on your VTs — may be out of order if m2 takes longer than m3.

If you have MULTIPLE subscribers (Module 2):
  AMPS delivers m1 to sub-1, m2 to sub-2, m3 to sub-3 simultaneously.
  No global order guarantee across subscribers.
```

If strict message ordering matters, you need exactly one subscriber with
a concurrency of 1 (semaphore permits = 1) — effectively single-threaded processing.
This sacrifices throughput for order. It is rarely needed in practice.

---

## Q12 — What is the difference between AMPS Queue and JMS Queue?

```text
Feature                AMPS Queue              JMS Queue (e.g. ActiveMQ)
─────────────────────  ─────────────────────   ─────────────────────────────
Protocol               AMPS (TCP binary)       AMQP / JMS / STOMP
Content filtering      Yes (SQL WHERE)         No (typically)
SOW integration        Yes (snapshot + live)   No
Message type           JSON/XML/NVFIX/binary   Any (serialized objects)
Lease mechanism        Server-managed TTL      Consumer ACK / NACK
Horizontal scaling     Multiple HAClients      Multiple consumers
Spring Boot client     HAClient (60East SDK)   spring-jms / spring-amqp
```

---

## Q13 — How do I monitor queue depth and subscriber lag?

AMPS provides a built-in admin topic you can query:

```java
// Subscribe to AMPS admin stats for your queue
client.subscribe(message -> {
    System.out.println(message.getData());  // JSON stats
}, "AMPS/Queue/Stats");
```

From your Spring Boot application, expose metrics via Actuator:

```java
@Component
public class AmpsMetrics {

    private final ProcessedMessageRepository repo;
    private final MeterRegistry meterRegistry;

    public AmpsMetrics(ProcessedMessageRepository repo, MeterRegistry meterRegistry) {
        this.repo = repo;
        this.meterRegistry = meterRegistry;

        Gauge.builder("amps.messages.processed.total",
                repo, r -> r.count())
            .description("Total messages processed")
            .register(meterRegistry);
    }
}
```

Accessible at: `GET /actuator/metrics/amps.messages.processed.total`
