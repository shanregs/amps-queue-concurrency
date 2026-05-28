# 11 — Virtual Threads: Theory, Lifecycle, Pinning, and Usage

Java 21 (Project Loom, JEP 444) — the definitive reference for this project.

---

## 1. The Problem Virtual Threads Solve

### Classic Platform Thread Model (pre-Java 21)

```text
OS Kernel
  │
  │  schedules
  ▼
Platform Thread  (1:1 mapping to an OS thread)
  │
  │  ~1 MB stack, OS context switch cost (~1–10 µs)
  │  practical limit: ~10,000 threads per JVM before OOM / scheduler thrash
  │
  │  When it calls a blocking operation (JDBC, file I/O, Thread.sleep):
  │
  │      getConnection()  ──▶  [BLOCKED]  ──────────────────────────────▶ time
  │                            OS thread HELD, doing nothing, burning RAM
  │
  └── The OS thread is parked at the OS level — scheduler cannot use it
```

**Consequence:** to handle 10,000 concurrent JDBC operations you need 10,000 platform threads.
That is ~10 GB of stack RAM, thousands of OS context switches per second,
and a saturated scheduler — before a single line of business logic runs.

---

### Virtual Thread Model (Java 21+)

```text
OS Kernel
  │  schedules
  ▼
Carrier Thread  (platform thread from ForkJoinPool)
  │  runs virtual threads one at a time
  │  pool size = Runtime.availableProcessors()  (e.g. 8 on a 8-core machine)
  │
  │  When VT calls a blocking operation (JDBC, file I/O, Thread.sleep):
  │
  │      VT parks itself:
  │        1. Serialise VT stack (continuation) onto the JVM heap
  │        2. Unmount VT from carrier thread
  │        3. Carrier thread is now FREE — picks up another runnable VT
  │
  │      (later) blocking call completes:
  │        4. VT moves to RUNNABLE state
  │        5. Any available carrier thread mounts the VT
  │        6. VT resumes exactly where it left off
  │
  └── Carrier thread is never idle while there is work to do
```

**Result:** 8 carrier threads can run **millions** of VTs concurrently.
A VT blocked on JDBC is just a small heap object — not an OS thread.

---

## 2. Internal Architecture

### The Three Layers

```text
┌─────────────────────────────────────────────────────────────────────┐
│  Application Code                                                    │
│  Thread.ofVirtual().start(() → doWork())                            │
│  Executors.newVirtualThreadPerTaskExecutor()                         │
└──────────────────────────────┬──────────────────────────────────────┘
                               │  creates / schedules
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│  JVM Virtual Thread Scheduler                                        │
│  (a ForkJoinPool in FIFO mode, size = availableProcessors)          │
│                                                                      │
│  Maintains:                                                          │
│   • run queue of RUNNABLE virtual threads                           │
│   • per-carrier work-stealing deques                                 │
│   • mount / unmount logic (park ↔ resume)                           │
└──────────────────────────────┬──────────────────────────────────────┘
                               │  assigns VTs to
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│  Carrier Threads  (platform threads — 1 per CPU core by default)     │
│                                                                      │
│  Carrier-1  Carrier-2  Carrier-3  ...  Carrier-N                    │
│  ┌───────┐  ┌───────┐  ┌───────┐      ┌───────┐                    │
│  │ VT-A  │  │ VT-D  │  │ VT-G  │      │ VT-Z  │  ← currently       │
│  │running│  │running│  │running│      │running│     mounted         │
│  └───────┘  └───────┘  └───────┘      └───────┘                    │
│                                                                      │
│  While each carrier runs one VT, thousands of others sit on heap    │
│  as parked continuations, waiting for their I/O to complete.        │
└─────────────────────────────────────────────────────────────────────┘
```

### What Is a Continuation?

A **continuation** is the captured execution state of a virtual thread:

```text
Continuation = { stack frames, local variables, program counter }

When a VT parks:
  JVM serialises the continuation onto the JVM heap (~1–2 KB for simple stacks)
  Carrier thread is released

When the VT resumes:
  JVM deserialises the continuation onto a carrier thread's call stack
  Execution continues from exactly the point it parked
```

This is the same mechanism behind `yield` in coroutines and `async/await`
in other languages — Java 21 makes it fully transparent.

---

## 3. Virtual Thread Lifecycle

### State Machine

```text
                  Thread.ofVirtual().start(task)
                  executor.submit(task)
                           │
                           ▼
                       ┌──────────┐
                       │   NEW    │  VT object created, not yet scheduled
                       └────┬─────┘
                            │  scheduler picks it up
                            ▼
                       ┌──────────┐
              ┌────────│ RUNNABLE │◀─────────────────────────────┐
              │        └────┬─────┘                              │
              │             │  carrier thread available          │
              │             ▼                                    │
              │        ┌──────────┐                             │
              │        │ RUNNING  │  mounted on a carrier thread │
              │        └────┬─────┘                             │
              │             │                                    │
              │      ┌──────┴──────────────────┐                │
              │      │                         │                │
              │      ▼                         ▼                │
              │  Blocking I/O            Non-blocking        code finishes
              │  Thread.sleep()          return               │
              │  LockSupport.park()      │                    │
              │  Semaphore.acquire()     └────────────────────┘
              │      │
              │      ▼
              │  ┌──────────┐
              │  │  PARKED  │  continuation stored on heap
              │  │(WAITING) │  carrier thread FREED
              │  └────┬─────┘
              │       │  I/O completes / unpark() called
              └───────┘  VT re-queued as RUNNABLE
                         picked up by any carrier thread

At task completion:
                       ┌────────────┐
                       │ TERMINATED │  GC'd like any heap object
                       └────────────┘
```

### States in Code Terms

| JVM State | `Thread.getState()` | What's Happening |
|---|---|---|
| NEW | `NEW` | Created, `start()` not yet called |
| RUNNABLE (queued) | `RUNNABLE` | Waiting for a carrier thread slot |
| RUNNING | `RUNNABLE` | Mounted on a carrier; executing |
| PARKED on I/O | `WAITING` | Blocked on JDBC, sleep, semaphore, etc. Continuation on heap. |
| PARKED on lock | `WAITING` or `TIMED_WAITING` | Waiting for `ReentrantLock`, `Semaphore`, etc. |
| TERMINATED | `TERMINATED` | Task complete |

> **`RUNNABLE` covers both queued and running** — this is a known subtlety.
> Use JFR events (`jdk.VirtualThreadStart`, `jdk.VirtualThreadEnd`,
> `jdk.VirtualThreadPinned`) for precise state tracking.

---

## 4. Parking vs Blocking — The Critical Distinction

This is the single most important concept for writing correct virtual thread code.

### PARKING (good — what VTs are designed for)

```text
Virtual Thread calls a java.util.concurrent or java.io operation
  e.g.:  Semaphore.acquire()
         BlockingQueue.poll()
         Thread.sleep()
         Socket I/O (java.nio channels)
         JDBC connection from HikariCP (via LockSupport.park())

JVM detects the park point:
  1. Saves VT continuation to heap
  2. Unmounts VT from carrier thread
  3. Carrier thread is available for another VT
  4. (later) unpark() signal arrives
  5. VT is re-queued as RUNNABLE
  6. Any carrier mounts it and resumes

Effect: zero carrier threads wasted on waiting
```

### BLOCKING (bad — carrier thread is held)

```text
Virtual Thread calls synchronized { ... } while holding a monitor,
AND inside that block performs a blocking operation.

OR

Virtual Thread calls a JNI (native) method that blocks.

JVM cannot unmount the VT — the carrier thread is PINNED.
The carrier thread is held for the entire duration of the block.
  e.g. 20 VTs all pinned on synchronized JDBC → 20 carrier threads held
  → only (totalCarriers - 20) carriers left for all other VTs
  → severe throughput degradation, potential deadlock

Effect: carrier starvation
```

### Visual Comparison

```text
PARKING (correct):

  time ──────────────────────────────────────────────────▶
  Carrier-1:  [VT-A runs] [VT-B runs] [VT-C runs] [VT-D runs] ...
                    ↑            ↑           ↑
                VT-A parks,  VT-B parks, VT-C parks...
                VT-A goes    VT-B goes   VT-C goes
                to heap      to heap     to heap
                
  Carrier-1 never idles — always running something useful


PINNING (wrong):

  time ──────────────────────────────────────────────────▶
  Carrier-1:  [VT-A PINNED ——————————————————————————————]
  Carrier-2:  [VT-B PINNED ——————————————————]
  Carrier-3:  [VT-C runs...] [idle...idle...idle...]
  
  Carrier-1 is held hostage by VT-A's synchronized block
  Only Carrier-3 is free — severe bottleneck
```

---

## 5. Concrete Examples

### Example 1 — JDBC with HikariCP (parking correctly)

```java
// This is what happens inside your MessageProcessor.process():

@Transactional
public void process(Message message) {
    // Step A: HikariCP.getConnection()
    //   If pool is full → LockSupport.park() → VT parks, carrier freed
    //   If connection available → returns immediately
    
    repo.save(entity);   // ← inside: getConnection() + PreparedStatement.execute()
    
    // Step B: PreparedStatement.execute()  (socket write to DB)
    //   NIO channel write → if not ready → VT parks, carrier freed
    //   DB responds → VT unparked, carrier resumes it
}
```

```text
Timeline with 5 VTs and 2 HikariCP connections:

  VT-1: getConn() → conn available → execute() [I/O wait → PARKED] → resume → done
  VT-2: getConn() → conn available → execute() [I/O wait → PARKED] → resume → done
  VT-3: getConn() → POOL FULL    → PARKED (continuation on heap)
  VT-4: getConn() → POOL FULL    → PARKED (continuation on heap)
  VT-5: getConn() → POOL FULL    → PARKED (continuation on heap)

  Carrier threads during this:
    Carrier-1 runs VT-1 → VT-1 parks → Carrier-1 picks up VT-3's other work
    Carrier-2 runs VT-2 → VT-2 parks → Carrier-2 picks up VT-4's other work

  When VT-1's DB response arrives:
    HikariCP returns connection to pool
    VT-3 is unparked (was waiting for connection)
    VT-3 becomes RUNNABLE → carrier mounts it

  Result: 2 connections serve 5 VTs. No carrier thread ever idles.
```

---

### Example 2 — Semaphore (AMPS backpressure — parking correctly)

```java
// In MessageDispatchService:
public void dispatch(Message message, HAClient client) {
    semaphore.acquire();   // ← if permits exhausted: VT parks here
                           //    (this is the SUBSCRIBER's platform thread,
                           //     but the principle is the same for any thread)
    executor.submit(() → processAndAck(message, client));
}
```

```text
Semaphore.acquire() when permits = 0:
  1. VT calls LockSupport.park()
  2. JVM detects park → serialises VT continuation to heap
  3. Carrier thread freed — picks up next runnable VT
  4. (later) another VT calls semaphore.release() → LockSupport.unpark(waiter)
  5. Waiting VT is re-queued as RUNNABLE
  6. Carrier mounts it and continues from the line after acquire()

Note: In single-subscriber profile, the subscriber thread is a PLATFORM thread
(not a VT) that parks at the OS level. The VTs are the workers.
Platform threads blocking on semaphore = intentional backpressure design.
```

---

### Example 3 — Thread.sleep() (parking, not blocking)

```java
// Old understanding (wrong): Thread.sleep() holds a platform thread
// New reality (Java 21+): Thread.sleep() on a VT → PARKS the VT

Thread.ofVirtual().start(() → {
    System.out.println("Before sleep");
    Thread.sleep(5000);              // VT parks for 5 seconds
                                      // carrier thread is FREE during those 5 seconds
    System.out.println("After sleep");
});
```

```text
// You can have 1 million VTs all sleeping simultaneously:
for (int i = 0; i < 1_000_000; i++) {
    Thread.ofVirtual().start(() → {
        Thread.sleep(10_000);    // 1M VTs parked → ~1-2 GB heap for continuations
        doWork();                // all resume after 10s, runs on 8 carriers in bursts
    });
}
// On a platform thread pool: this would require 1M platform threads → OOM
```

---

### Example 4 — Creating and Running VTs (this project's pattern)

```java
// Pattern used in MessageDispatchService:
ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

// Each submit() creates ONE new virtual thread.
// There is no pool of idle VTs — each task gets a fresh VT.
// VT is GC'd when the task completes.

executor.submit(() → {
    // This entire lambda runs in its own virtual thread.
    // If it calls repo.save() → JDBC getConnection → if pool full → VT PARKS
    // Carrier thread freed, picks up next submitted VT
    processor.process(message);
    client.ack(message);
});

// Comparison: with a fixed thread pool
ExecutorService fixed = Executors.newFixedThreadPool(200);
// If all 200 threads are blocked on JDBC → NO NEW TASKS RUN → throughput = 0
// With VTs: all 200 VTs park on JDBC → carriers run other VTs → throughput maintained
```

---

## 6. Thread Pinning — Deep Dive

### What Is Pinning?

**Pinning** = a virtual thread cannot be unmounted from its carrier thread.
The carrier is held ("pinned") for the entire duration of the blocking operation.
This destroys the performance benefit of virtual threads.

### Root Causes

#### Cause 1 — `synchronized` blocks/methods (most common)

```java
// VT enters a synchronized block — acquires the object monitor
synchronized (someObject) {
    // VT is now PINNED to its carrier thread
    // If ANY blocking call happens here, the carrier is held:
    
    connection.prepareStatement(sql);  // ← socket I/O, but carrier is PINNED
    Thread.sleep(100);                 // ← sleep, but carrier is PINNED
    blockingQueue.take();              // ← blocks, but carrier is PINNED
    
    // Carrier cannot be freed until we EXIT the synchronized block
}
// Only here does the VT become unpin-able again
```

**Why?** The JVM monitor (object lock) is tied to the OS thread. If the VT unmounted,
the monitor association would be broken. JVM 21 cannot solve this without breaking
the Java Memory Model — so it pins instead.

#### Cause 2 — JNI (Java Native Interface) calls

```java
// Any native method call pins the VT:
System.loadLibrary("myNativeLib");
NativeClass.nativeMethod();   // ← VT pinned for duration of native call
```

**Why?** Native code has direct stack access (C pointers into the stack frame).
Moving the stack to another carrier thread would corrupt those pointers.

---

### Real Pinning Sources in This AMPS Project

```text
Component             Pinning Risk         Mitigation
────────────────────  ──────────────────   ──────────────────────────────────────────
AMPS HAClient SDK     MEDIUM               HAClient uses synchronized internally.
                                           Subscriber thread is a PLATFORM THREAD
                                           (not a VT), so pinning doesn't apply to
                                           the receive loop. VTs only call
                                           client.ack() which is a short operation.

H2 JDBC driver        MEDIUM (JDK 21)      H2 uses synchronized in its JDBC impl.
(dev only)            LOW (JDK 24+)        With JDK 21: keep HikariCP pool generous.
                                           With JDK 24+: H2 uses ReentrantLock.

PostgreSQL JDBC       LOW (JDK 21+)        pgjdbc ≥ 42.7.0 replaced most synchronized
driver (prod)         MINIMAL (JDK 24+)    blocks with ReentrantLock. Upgrade to
                                           pgjdbc ≥ 42.7.0.

HikariCP              VERY LOW             HikariCP uses ReentrantLock for its pool
                                           management. Safe for VTs.

Spring @Transactional NONE                 Spring's transaction interceptor is pure
                                           Java — no synchronized, no native.

Jackson ObjectMapper  NONE in normal use   ObjectMapper is thread-safe via immutable
                                           config. No synchronized in read path.

Your own code         DEPENDS ON YOU       Replace synchronized with ReentrantLock
                                           in any code that might be called from VTs.
```

---

### How to Detect Pinning

#### Method 1 — JVM Flag (Development)

```bash
# Add to JVM startup:
-Djdk.tracePinnedThreads=full

# Output when pinning occurs:
Thread[#42,ForkJoinPool-1-worker-1,5,CarrierThreads]
    com.h2database.jdbc.JdbcConnection.prepareStatement(JdbcConnection.java:847)  ← pinned here
    com.shan.mq.amps.ampsqueueconcurrency.processor.MessageProcessor.process(...)
    ...

# Use "short" for less verbose output:
-Djdk.tracePinnedThreads=short
```

In Spring Boot, add to `application.yaml`:
```yaml
spring:
  jvm:
    args: -Djdk.tracePinnedThreads=full
```
Or to `JAVA_OPTS` / `spring-boot-maven-plugin` configuration:
```xml
<plugin>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-maven-plugin</artifactId>
    <configuration>
        <jvmArguments>-Djdk.tracePinnedThreads=full</jvmArguments>
    </configuration>
</plugin>
```

#### Method 2 — Java Flight Recorder (JFR)

```bash
# Start with JFR:
-XX:StartFlightRecording=duration=60s,filename=vt-pinning.jfr

# Or at runtime:
jcmd <pid> JFR.start duration=60s filename=vt-pinning.jfr

# Analyse in JMC (Java Mission Control) → look for:
#   jdk.VirtualThreadPinned events
#   Carrier utilization chart (all carriers held = pinning storm)
```

#### Method 3 — Micrometer / Actuator Gauge

```java
// Register a gauge tracking pinned thread count via MBeans:
// MBeanServer → java.lang:type=Threading → VirtualThreadPinnedCount (JDK 21+)

// In CommonConfig:
MeterRegistry registry = ...;
MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
registry.gauge("jvm.virtual.threads.pinned",
    Tags.of("profile", activeProfile),
    mbs,
    server -> {
        try {
            return (Number) server.getAttribute(
                new ObjectName("java.lang:type=Threading"),
                "VirtualThreadPinnedCount"
            );
        } catch (Exception e) { return 0; }
    }
);
```

#### Method 4 — Thread Dump

```bash
jcmd <pid> Thread.print

# Look for lines like:
#   <virtual> ... state=PINNED
#   Mounted on carrier thread: ForkJoinPool-1-worker-3
```

---

### How to Fix Pinning

#### Fix 1 — Replace `synchronized` with `ReentrantLock` (your own code)

```java
// BEFORE — pins the VT if blocking I/O happens inside:
private final Object lock = new Object();

public void doWork() {
    synchronized (lock) {         // ← VT PINNED from here...
        callDatabase();           //   ...carrier held during JDBC wait
    }                             // ← ...to here
}


// AFTER — VT can park normally on ReentrantLock:
private final ReentrantLock lock = new ReentrantLock();

public void doWork() {
    lock.lock();                  // ← VT can park while waiting for lock
    try {
        callDatabase();           //   VT parks on JDBC, carrier freed
    } finally {
        lock.unlock();
    }
}
```

#### Fix 2 — Replace `synchronized` collections

```java
// BEFORE:
Map<String, Value> map = Collections.synchronizedMap(new HashMap<>());
// Uses synchronized — pins VTs on contention

// AFTER:
Map<String, Value> map = new ConcurrentHashMap<>();
// Uses CAS + lock striping, VT-friendly

// BEFORE:
Stack<Item> stack = new Stack<>();  // extends Vector — synchronized everywhere

// AFTER:
Deque<Item> stack = new ConcurrentLinkedDeque<>();   // lock-free
// or
Deque<Item> stack = new ArrayDeque<>();  // if single-threaded access
```

#### Fix 3 — Move blocking calls outside `synchronized`

```java
// Sometimes you cannot change the lock type. Restructure instead:

// BEFORE (blocking inside synchronized):
synchronized (this) {
    result = expensiveDbCall();   // ← blocks inside synchronized → pinned
    cache.put(key, result);
}

// AFTER (blocking outside synchronized):
result = expensiveDbCall();       // ← park here, no lock held → fine
synchronized (this) {
    cache.put(key, result);       // ← very short critical section, no blocking
}
```

#### Fix 4 — Increase carrier thread count (workaround for third-party code)

```bash
# If you can't fix the pinning source (e.g. legacy HAClient SDK):
-Djdk.virtualThreadScheduler.parallelism=32   # default = availableProcessors

# More carriers = more threads available even when some are pinned
# This is a band-aid, not a cure — but practical for SDK pinning you can't change
```

#### Fix 5 — Upgrade drivers (free fix)

```text
# pgjdbc (PostgreSQL JDBC):
# ≥ 42.7.0 → most synchronized blocks replaced with ReentrantLock
# ≥ 42.7.3 → further improvements

# H2:
# ≥ 2.3.x → some improvements
# JDK 24+ → synchronized on Object monitors no longer pins VTs (JEP 491)

# Spring Boot 3.5 uses pgjdbc 42.7.x by default → PostgreSQL pinning is minimal
```

---

### Pinning Severity Guide

```text
Pinning duration × frequency = impact

LOW IMPACT:
  Short synchronized blocks (< 1µs of wall time)
  Infrequent operations (once at startup)
  → Carrier held briefly, usually recovers immediately

MEDIUM IMPACT:
  synchronized around a non-I/O computation (< 1ms)
  Occasional JDBC in synchronized (H2 dev mode)
  → May cause brief carrier starvation under high load

HIGH IMPACT:
  synchronized around JDBC operations (ms-range latency)
  Under high concurrency (many VTs pinning simultaneously)
  → Carriers saturated → throughput collapses → looks like thread starvation
  → The symptom: VTs pile up as RUNNABLE but nothing is executing

CRITICAL:
  Deadlock: VT-A holds monitor, waits for VT-B
            VT-B holds monitor, waits for VT-A
            Both pinned → carriers held → other VTs starve → cascade
```

---

## 7. Memory Model

### Virtual Thread Stack vs Platform Thread Stack

```text
Platform Thread:
  ~1 MB stack (fixed, pre-allocated from native memory)
  OS controls the stack memory

Virtual Thread:
  Stack grows as a linked list of "stack chunks" on the JVM heap
  Starts at ~100 bytes, grows on demand up to default limit
  When parked: only live frames are retained (dead frames GC'd)

Practical heap usage per VT:
  Empty VT (just created, not yet run):    ~200 bytes
  VT parked mid-call stack (5 frames):    ~1–2 KB
  VT parked with large locals/arrays:     depends on locals in scope
```

### Scaling Example

```text
10,000 VTs all parked mid-JDBC-call:
  stack per VT ≈ 2 KB (a few frames: process() → save() → execute())
  total        ≈ 10,000 × 2 KB = 20 MB heap

Compare: 10,000 platform threads:
  stack per thread ≈ 1 MB
  total            ≈ 10,000 × 1 MB = 10 GB native memory → OOM on most servers

VTs: 20 MB. Platform threads: 10 GB. Factor of 500x.
```

---

## 8. Interaction with Spring Framework

### `@Transactional` and ThreadLocals

Spring binds transaction context to `ThreadLocal<TransactionStatus>`.
This is **safe for VTs** because:
- Each VT has its own `ThreadLocal` map (VTs are `Thread` subclasses)
- When a VT parks and a different VT runs on the same carrier, their `ThreadLocal`s are separate
- `ThreadLocal` is per-thread (per-VT), not per-carrier

```java
// This is safe — each VT gets its own transaction:
executor.submit(() → {
    // ThreadLocal in this VT = independent from all other VTs
    processor.process(message);   // @Transactional opens a tx in THIS VT's ThreadLocal
    client.ack(message);
    // @Transactional commits at method exit — still in THIS VT's context
});
```

**Exception:** `InheritableThreadLocal` inherits from the parent thread at VT creation time,
then diverges. Do not use `InheritableThreadLocal` for mutable state in VT-heavy code.

### Spring Boot 3.2+ Virtual Thread Executor

Spring Boot 3.2+ can configure Tomcat, task executor, and scheduler to use VTs automatically:

```yaml
spring:
  threads:
    virtual:
      enabled: true   # Spring Boot 3.2+ auto-configures VTs for @Async, HTTP, etc.
```

This project manages its own VT executor explicitly for the AMPS dispatch path,
so `spring.threads.virtual.enabled` is independent of the AMPS executor.

---

## 9. Structured Concurrency (Java 21 Preview → Java 23 Standard)

Structured concurrency groups VTs so failures propagate cleanly:

```java
// Available as standard in Java 23+ (preview in 21):
try (var scope = new StructuredTaskScope.ShutdownOnFailure()) {
    Future<String> task1 = scope.fork(() → processMessage(msg1));
    Future<String> task2 = scope.fork(() → processMessage(msg2));
    
    scope.join();          // wait for both
    scope.throwIfFailed(); // if either threw, rethrow here
    
    String result1 = task1.get();
    String result2 = task2.get();
}
// If task1 fails, task2 is cancelled automatically
// This is NOT used in this project (Java 21, scope is still preview)
// This project uses executor.submit() + semaphore, which is simpler
```

For **this project** (Java 21, non-preview), the `executor.submit()` + `semaphore` pattern
in `MessageDispatchService` is the correct and production-safe approach.

---

## 10. Virtual Thread Do's and Don'ts

### ✅ DO

```text
DO use virtual threads for I/O-bound work:
  JDBC calls, HTTP calls, file reads, queue operations

DO park on java.util.concurrent primitives:
  Semaphore, ReentrantLock, CountDownLatch, BlockingQueue, CompletableFuture

DO use newVirtualThreadPerTaskExecutor() for task-per-request workloads:
  One VT per AMPS message = correct and efficient

DO keep ThreadLocals short-lived:
  Transaction context, MDC values — scoped within the VT's task

DO use VTs for large fan-out:
  Dispatching 10,000 concurrent AMPS messages = fine
  Dispatching 1,000,000 = also fine (memory permitting)

DO set -Djdk.tracePinnedThreads=full in development:
  Catch pinning before it reaches production
```

### ❌ DO NOT

```text
DO NOT pool virtual threads:
  newFixedThreadPool(200) with VTs wastes VTs (they are cheap — just create new ones)
  Correct: newVirtualThreadPerTaskExecutor() creates a fresh VT per task

DO NOT use ThreadLocal for cross-VT shared state:
  ThreadLocal is per-VT — mutations in VT-1 are invisible to VT-2
  Use ConcurrentHashMap, AtomicReference, or a bean for shared state

DO NOT write synchronized blocks that contain blocking I/O in VTs:
  This pins the carrier. Use ReentrantLock instead.

DO NOT rely on VT thread identity for correctness:
  VTs can be rescheduled to any carrier at any park point
  Never assume VT-A always runs on Carrier-1

DO NOT use VTs for CPU-bound tight loops with no I/O:
  VTs won't magically parallelise CPU work — they park on I/O, not on CPU usage
  CPU-bound tasks: use platform thread pool sized to core count

DO NOT set thread priority on VTs (JDK 21):
  VT scheduler ignores Thread.setPriority() — all VTs are FIFO-scheduled

DO NOT call Thread.stop() / Thread.suspend() on VTs:
  These are deprecated on platform threads and broken on VTs
  Use cooperative cancellation: check a volatile boolean flag or Thread.isInterrupted()
```

---

## 11. How This Project Uses Virtual Threads

### Summary of VT usage per component

```text
Component                      Thread Type     Park Points
─────────────────────────────  ─────────────   ─────────────────────────────────────────
AmpsQueueSubscriber            PLATFORM        semaphore.acquire() (intentional backpressure)
  (subscriber loop thread)                     stream.next() (AMPS blocking receive)

MessageDispatchService         VIRTUAL         created per message via executor.submit()
  (each processAndAck task)                    processor.process() contains park points:

MessageProcessor.process()     VIRTUAL         HikariCP.getConnection() if pool full
  (called from VT)                             PreparedStatement.execute() (JDBC I/O)

PublishWorker (publisher)      VIRTUAL         rateLimiter.acquire() (token bucket semaphore)
  (each worker in publisher)                   haClient.publish() (AMPS socket write)
```

### Configuration tuning based on VT behaviour

```yaml
# HikariCP pool sizing for virtual threads:
spring:
  datasource:
    hikari:
      # VTs park while waiting — 10:1 ratio (VTs:connections) is safe
      # With maxConcurrency=200 VTs: pool-size=20 is sufficient
      maximum-pool-size: 20

# VT scheduler parallelism (increase if HAClient SDK causes pinning):
# -Djdk.virtualThreadScheduler.parallelism=16   (default = core count)
```

---

## 12. JDK Version Progression

```text
JDK 19  — Virtual threads: Preview (JEP 425)
JDK 20  — Virtual threads: Second preview (JEP 436)
JDK 21  — Virtual threads: FINAL / GA (JEP 444)  ← this project targets this
           • synchronized still pins
           • Most java.io.* and java.nio.* properly park

JDK 21+  — Structured Concurrency: preview (JEP 453 → 462 → 480)

JDK 24   — synchronized no longer pins VTs (JEP 491) ← MAJOR improvement
           • H2 synchronized blocks no longer cause pinning
           • Most legacy JDBC drivers safe without upgrade
           • Nearly eliminates the entire pinning problem class

Rule:
  On JDK 21: audit synchronized usage, upgrade JDBC drivers, monitor with JFR
  On JDK 24+: pinning is largely a non-issue; monitor anyway as a best practice
```

---

## 13. Quick Reference Card

```text
┌─────────────────────────────────────────────────────────────────────┐
│                   Virtual Thread Quick Reference                     │
│                                                                      │
│  Create:      Executors.newVirtualThreadPerTaskExecutor()            │
│               Thread.ofVirtual().start(runnable)                     │
│                                                                      │
│  Park safely: Semaphore, ReentrantLock, CountDownLatch,             │
│               BlockingQueue, CompletableFuture, Thread.sleep(),      │
│               java.nio socket I/O, JDBC (via LockSupport)           │
│                                                                      │
│  Pinning:     synchronized { blocking_op }  → BAD                   │
│               JNI native calls              → BAD                   │
│               Fix: ReentrantLock or restructure                      │
│                                                                      │
│  Detect:      -Djdk.tracePinnedThreads=full  (dev)                  │
│               JFR event: jdk.VirtualThreadPinned  (prod)            │
│                                                                      │
│  Memory:      ~1–2 KB per parked VT (vs ~1 MB platform thread)      │
│                                                                      │
│  ThreadLocal: safe — per-VT, not per-carrier                        │
│                                                                      │
│  DO NOT pool: use newVirtualThreadPerTaskExecutor() always           │
│  DO NOT sync+block: use ReentrantLock in VT-called code             │
│                                                                      │
│  JDK 21:  pinning exists — audit sync blocks, upgrade JDBC drivers  │
│  JDK 24+: JEP 491 — synchronized no longer pins                    │
└─────────────────────────────────────────────────────────────────────┘
```
