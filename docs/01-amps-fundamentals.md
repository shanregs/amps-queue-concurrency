# 01 — AMPS Fundamentals

## What Is AMPS?

**AMPS (Advanced Message Processing System)** by 60East Technologies is a high-performance,
low-latency messaging server used extensively in financial services (trading desks, risk
systems, capital markets). It supports both pub/sub topics and durable persistent queues with
lease-based at-least-once delivery.

---

## Core Concepts

| Concept | Description |
|---|---|
| **Topic** | Pub/sub broadcast channel. Every subscriber receives every message. No persistence guarantee. |
| **Queue** | Persistent at-least-once channel. Each message is delivered to exactly one subscriber via lease. |
| **SOW (State of the World)** | In-memory snapshot of the latest value per key. Can be replayed on connect. |
| **Bookmark** | A durable, globally-unique cursor per message. Used to resume after reconnect. |
| **Lease** | When AMPS delivers a queue message, it is "leased" to one subscriber. The subscriber must ACK within the TTL or the message returns to the queue. |
| **HAClient** | The AMPS Java client class. Handles TCP reconnection, failover, and bookmark replay automatically. |
| **LoggedBookmarkStore** | Persists the bookmark position to disk. Ensures no messages are missed across JVM restarts. |
| **ExponentialDelayStrategy** | Reconnect backoff strategy: retries at 50ms, 100ms, 200ms … up to max (e.g. 5s). |
| **DefaultServerChooser** | Round-robin server selector for AMPS HA clusters. |
| **Content Filter** | SQL-style WHERE clause on a subscription: `/queue/orders WHERE side='BUY'` |

---

## Queue vs Topic — The Fundamental Difference

```text
┌──────────────────────────────────────────────────────────────────────────┐
│               AMPS TOPIC  (/trades)  — pub/sub broadcast                 │
│                                                                          │
│  Publisher ──▶ msg                                                       │
│                  ├──▶ Subscriber A  gets msg  ← all get the same        │
│                  ├──▶ Subscriber B  gets msg    message                  │
│                  └──▶ Subscriber C  gets msg                             │
│                                                                          │
│  Fan-out: N subscribers → N copies of every message                     │
└──────────────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────────────┐
│             AMPS QUEUE  (/queue/trades)  — competing consumers           │
│                                                                          │
│  Publisher ──▶ [m1][m2][m3][m4][m5][m6]  (server-side durable queue)    │
│                                                                          │
│  Subscriber A  ←── m1  (leased — invisible to all others)               │
│  Subscriber B  ←── m2  (leased — invisible to all others)               │
│  Subscriber C  ←── m3  (leased — invisible to all others)               │
│  Subscriber A  ←── m4  (A finished m1, gets next available)             │
│  Subscriber B  ←── m5                                                    │
│  Subscriber A  ←── m6                                                    │
│                                                                          │
│  Rule: each message goes to EXACTLY ONE subscriber at a time            │
└──────────────────────────────────────────────────────────────────────────┘
```

**Multiple subscribers on the same AMPS queue distribute work, they do NOT duplicate messages.**

---

## The Lease Model — Message Lifecycle

```text
AMPS Server — queue state for /queue/trades
┌──────────────────────────────────────────────────────────────────────┐
│  m1  │ LEASED    │ leased_to: client-A │ ttl: 4.2s remaining        │
│  m2  │ LEASED    │ leased_to: client-B │ ttl: 2.9s remaining        │
│  m3  │ AVAILABLE │ leased_to: (none)   │ ready for any subscriber   │
│  m4  │ AVAILABLE │ leased_to: (none)   │                            │
│  m5  │ ACKED     │ leased_to: client-A │ permanently removed ✓      │
└──────────────────────────────────────────────────────────────────────┘
```

### State Machine

```text
                 Publisher publishes message
                           │
                           ▼
                     [AVAILABLE] ─────────────────────────────────────┐
                           │                                           │
                  Subscriber receives it                               │
                  (message is leased)                                  │
                           │                                           │
                           ▼                                           │
                [LEASED to Subscriber X]                               │
                           │                                           │
           ┌───────────────┼───────────────────┐                      │
           │               │                   │                      │
           ▼               ▼                   ▼                      │
    Subscriber ACKs   Subscriber NACKs   Lease TTL expires            │
    (within TTL)       (explicit)       (no ACK received)             │
           │               │                   │                      │
           ▼               ▼                   ▼                      │
        [ACKED]       [AVAILABLE] ─────▶ [AVAILABLE] ────────────────┘
     permanently       re-queued          re-queued
      removed          immediately        after TTL
```

### Key delivery guarantee: **At-Most-Once Active, At-Least-Once Overall**

- A message is active (leased) on exactly ONE subscriber at a time.
- A message CAN be delivered more than once ONLY if the subscriber processed it but crashed before sending the ACK.
- This is the only window for duplicate delivery → which is why **idempotency is non-negotiable**.

---

## HAClient — What It Gives You for Free

```text
Feature                    How it works
─────────────────────────  ─────────────────────────────────────────────────────
Auto-reconnect             HAClient detects TCP disconnect, reconnects with backoff
Bookmark replay            After reconnect, LoggedBookmarkStore resumes from last
                           ACK'd position — no missed messages across restarts
Failover                   DefaultServerChooser tries servers in round-robin on failure
Exponential backoff        ExponentialDelayStrategy(initialMs, maxMs) prevents storm
Thread-safe send/receive   HAClient is safe to use from multiple threads for ACKs;
                           the receive loop must be on ONE dedicated thread per client
```

---

## Client Naming — Critical for Multi-Subscriber / Multi-JVM

AMPS identifies each connection by its **client name**. Client names must be:

- **Globally unique** — AMPS will disconnect any existing connection with the same name
- **Stable across restarts** — so `LoggedBookmarkStore` can resume from where it left off
- **Descriptive** — visible in AMPS admin console for monitoring

### Naming Convention Used in This Project

```text
Pattern:  {app}-{hostname}-sub-{index}

single-subscriber:
    amps-queue-concurrency-sub-1           (index always 1)

multi-subscriber (single JVM, node "app-server-01"):
    amps-queue-concurrency-app-server-01-sub-1
    amps-queue-concurrency-app-server-01-sub-2
    amps-queue-concurrency-app-server-01-sub-3

multi-jvm-subscriber (two nodes: node1, node2):
    node1:  amps-queue-concurrency-node1-sub-1
            amps-queue-concurrency-node1-sub-2
            amps-queue-concurrency-node1-sub-3
    node2:  amps-queue-concurrency-node2-sub-1
            amps-queue-concurrency-node2-sub-2
            amps-queue-concurrency-node2-sub-3
```

The hostname is auto-detected via `InetAddress.getLocalHost().getHostName()` — no manual
configuration required.

---

## Bookmark Store — Durable Cursor

```text
Without LoggedBookmarkStore:
  JVM restarts → subscriber starts from "now" → all messages published
  while the JVM was down are missed permanently.

With LoggedBookmarkStore:
  JVM restarts → HAClient reads the bookmark log file → replays all
  un-ACK'd messages from the server → zero message loss.

File naming:  ~/.amps/amps-queue-concurrency-{hostname}-sub-{index}.log
              (unique per client name = unique per subscriber)
```

---

## AMPS Queue — The Golden Rules

```text
┌───────────────────────────────────────────────────────────────────────┐
│                 Golden Rules for AMPS Queue Consumers                  │
│                                                                        │
│  1. ONE message → ONE subscriber at a time (AMPS enforces via lease)  │
│                                                                        │
│  2. Multiple connections = WORK DISTRIBUTION, not message duplication  │
│                                                                        │
│  3. Duplicate delivery ONLY occurs on lease expiry / crash            │
│     → ALWAYS guard with UNIQUE(message_id) + idempotent write         │
│                                                                        │
│  4. ACK MUST go back on the SAME HAClient that received the message   │
│     → Pass the receiving HAClient reference into the VT               │
│                                                                        │
│  5. Client names MUST be globally unique across ALL connections        │
│     → Embed hostname + index in every client name                     │
│                                                                        │
│  6. Bookmark store file is per-client-name (one file per HAClient)    │
│     → Do NOT share bookmark files between clients                     │
│                                                                        │
│  7. Multi-JVM → shared PostgreSQL. H2 is per-process, cannot be      │
│     shared across JVMs.                                                │
└───────────────────────────────────────────────────────────────────────┘
```

---

## Anti-Patterns

```text
╔═════════════════════════════════════════════════════════════════════╗
║ ANTI-PATTERN 1 — Same client name for two connections               ║
║                                                                     ║
║  new HAClient("my-app")   // on node1                              ║
║  new HAClient("my-app")   // on node2 — AMPS kicks out node1!      ║
╚═════════════════════════════════════════════════════════════════════╝

╔═════════════════════════════════════════════════════════════════════╗
║ ANTI-PATTERN 2 — Subscribe to a topic instead of a queue           ║
║                                                                     ║
║  client.subscribe("/trades")       // topic: all subscribers get   ║
║                                    // ALL messages → duplicates    ║
║  Correct: client.bookmarkSubscribe("/queue/trades")                ║
╚═════════════════════════════════════════════════════════════════════╝

╔═════════════════════════════════════════════════════════════════════╗
║ ANTI-PATTERN 3 — ACKing on a different HAClient                    ║
║                                                                     ║
║  Message received by HAClient-1                                    ║
║  haClient2.ack(message)   ← AMPS ignores this; lease never ends   ║
╚═════════════════════════════════════════════════════════════════════╝

╔═════════════════════════════════════════════════════════════════════╗
║ ANTI-PATTERN 4 — No idempotency in multi-JVM                       ║
║                                                                     ║
║  JVM-1 saves msg, crashes before ACK                               ║
║  AMPS re-delivers to JVM-2                                         ║
║  JVM-2 saves msg again → duplicate row OR exception                ║
║  Fix: UNIQUE(message_id) + catch DataIntegrityViolationException   ║
╚═════════════════════════════════════════════════════════════════════╝

╔═════════════════════════════════════════════════════════════════════╗
║ ANTI-PATTERN 5 — Shared bookmark file between subscribers          ║
║                                                                     ║
║  Both sub-1 and sub-2 write to the same .log file                 ║
║  → Corrupted bookmark state; incorrect replay position             ║
║  Fix: one bookmark log file per HAClient name                      ║
╚═════════════════════════════════════════════════════════════════════╝
```
