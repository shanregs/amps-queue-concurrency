# 08 — Capacity Planning

## Throughput Formula

```text
Base formula (per subscriber):
  throughput_per_subscriber = maxConcurrencyPerSubscriber
                              ─────────────────────────────  × 1000
                               avgProcessingTimeMs

Total throughput (multi-subscriber, single JVM):
  throughput_jvm = subscriberCount × throughput_per_subscriber

Total throughput (multi-jvm-subscriber):
  throughput_total = jvmCount × throughput_jvm

Units: messages per second (msg/s)
```

---

## Profile Comparison Table

```text
┌────────────────────────────┬────────────────────┬────────────────────┬──────────────────────┐
│ Metric                     │ single-subscriber  │ multi-subscriber   │ multi-jvm-subscriber │
├────────────────────────────┼────────────────────┼────────────────────┼──────────────────────┤
│ AMPS TCP connections       │ 1                  │ N                  │ K × N                │
│ Platform threads           │ 1                  │ N                  │ K × N                │
│ Max in-flight VTs          │ maxConcurrency     │ N × maxConcurrSub  │ K × N × maxConcurrSub│
│ Max throughput (50ms avg)  │ maxConc÷50×1000   │ N×maxConc÷50×1000 │ K×N×maxConc÷50×1000  │
│ Database                   │ H2 or PostgreSQL   │ H2 or PostgreSQL   │ Shared PostgreSQL    │
│ HA / fault tolerance       │ None               │ None               │ JVM-level HA         │
│ Horizontal scalability     │ No                 │ No                 │ Yes (add JVMs)       │
│ Operational complexity     │ Low                │ Medium             │ Higher               │
│ Idempotency requirement    │ Recommended        │ Recommended        │ Mandatory            │
└────────────────────────────┴────────────────────┴────────────────────┴──────────────────────┘
```

---

## Worked Examples

### Example A — `single-subscriber`, 100 VTs, 50ms avg

```text
Config:
  max-concurrency: 100
  hikari.maximum-pool-size: 10
  avgProcessingTime: 50ms

Throughput:
  = 100 / 50 × 1000 = 2,000 msg/s theoretical
  = 1,600 msg/s practical (80% efficiency)

Memory (VTs):
  100 VTs × 2KB = 200KB — negligible

HikariCP ratio:
  100 VTs / 10 connections = 10:1 — safe (VTs park while waiting)

Recommendation: Use for volumes up to ~1,500 msg/s sustained.
```

### Example B — `multi-subscriber`, 3 subscribers, 50 VTs each, 50ms avg

```text
Config:
  subscriber-count: 3
  max-concurrency-per-subscriber: 50
  hikari.maximum-pool-size: 20

Throughput:
  per subscriber = 50 / 50 × 1000 = 1,000 msg/s
  total          = 3 × 1,000       = 3,000 msg/s theoretical
  practical      = 2,400 msg/s (80%)

Memory (VTs):
  150 VTs × 2KB = 300KB — negligible

HikariCP ratio:
  150 VTs / 20 connections = 7.5:1 — safe

Recommendation: Use for 1,500–5,000 msg/s on a single JVM.
```

### Example C — `multi-jvm-subscriber`, 3 JVMs × 3 subscribers × 50 VTs, 50ms avg

```text
Config (per JVM):
  subscriber-count: 3
  max-concurrency-per-subscriber: 50
  hikari.maximum-pool-size: 20

Per JVM:     3,000 msg/s theoretical
Total:       3 × 3,000 = 9,000 msg/s theoretical
Practical:   ~7,200 msg/s

PostgreSQL load:
  Total DB connections = 3 JVMs × 20 = 60
  PostgreSQL max_connections must be > 60 + 10 headroom = 70+

Recommendation: Use for > 5,000 msg/s or when HA is required.
```

---

## Sizing Decision Matrix

```text
Target Throughput   avg Processing Time   Recommended Config
────────────────   ────────────────────  ───────────────────────────────────────────
< 500 msg/s        Any                   single-subscriber, maxConcurrency=50
500–2,000 msg/s    < 100ms              single-subscriber, maxConcurrency=100-200
2,000–5,000 msg/s  < 100ms              multi-subscriber, N=3-5, maxConc/sub=50-100
5,000–15,000 msg/s Any                   multi-jvm (3 JVMs × 3-5 sub × 50-100 VTs)
> 15,000 msg/s     Any                   multi-jvm, add JVMs until target reached
                                          (linear scaling: each JVM adds ~5,000 msg/s)
```

---

## When to Change Profile

### Signals to move from `single-subscriber` to `multi-subscriber`

```text
- amps.semaphore.available == 0 for > 30s continuously (sustained backpressure)
- amps.processing.time p99 approaching lease-timeout-ms
- AMPS admin console shows queue depth growing (not draining)
- CPU utilization on the application node < 50% (more subscribers would help)
```

### Signals to move from `multi-subscriber` to `multi-jvm-subscriber`

```text
- Single JVM GC pause > 200ms (heap pressure from payloads)
- Single JVM CPU saturated (> 85%) despite tuning
- HA requirement: "if this JVM dies, processing must continue"
- Queue depth grows even with maxed-out subscriber count in one JVM
```

---

## Configuration Sizing Reference

### HikariCP Pool Size

```text
Formula: maximum-pool-size ≥ ceil(totalConcurrentVTs / parkingRatio)

parkingRatio (typical values):
  Simple DB INSERT (no business logic)  → 10–15
  Moderate business logic (50ms)        → 5–10
  Complex queries / joins               → 3–5

Example:
  single-subscriber, maxConcurrency=200, parkingRatio=10:
  pool-size = 200 / 10 = 20

  multi-subscriber, 3×50=150 VTs, parkingRatio=7:
  pool-size = 150 / 7 = 22 → round to 25
```

### Semaphore Permits (maxConcurrencyPerSubscriber)

```text
Upper bound:  hikariMaxPoolSize × parkingRatio
Lower bound:  throughputTarget × avgProcessingTimeMs / 1000

Example targeting 1,000 msg/s, 50ms avg, 10 connections:
  lower bound = 1,000 × 0.050 = 50 permits
  upper bound = 10 × 10 = 100 permits
  → set to 60–80 permits (buffer above lower bound, below upper)
```

### AMPS Lease Timeout

```text
Recommendation: lease-timeout-ms ≥ 5 × avgProcessingTimeMs (p99)

If p99 processing time is 200ms:
  lease-timeout-ms ≥ 1,000ms

If p99 processing time is 1,000ms:
  lease-timeout-ms ≥ 5,000ms

Too low a lease TTL → messages re-delivered while still being processed
  → unnecessary duplicates → increased DB write load
Too high a lease TTL → if a JVM crashes, messages take longer to be re-delivered
  → increased end-to-end latency in crash scenarios
```

---

## JVM Tuning Notes

```text
Heap:
  Virtual threads are heap-allocated (~1-2 KB each).
  With maxConcurrency=500, that's only ~1 MB from VTs.
  Main heap consumers are message payloads and JPA object graph.
  Start with -Xmx512m; increase if GC pauses > 100ms.

GC:
  Java 21 defaults to G1GC — appropriate for this workload.
  If payload sizes are large (> 10 KB each) and sustained throughput is high,
  consider ZGC (-XX:+UseZGC) for lower p99 pauses.

Virtual thread tuning:
  -Djdk.tracePinnedThreads=full        detect VT pinning (dev only)
  -Djdk.virtualThreadScheduler.parallelism=N  override carrier thread count
                                              (default = Runtime.availableProcessors())
                                              increase if CPU count is high and VTs pin frequently

Thread monitoring:
  jcmd <pid> Thread.print    shows all platform + virtual threads
  JFR event: jdk.VirtualThreadPinned (log whenever VT pins > threshold)
```

---

## Scaling Cheat Sheet

```text
Add more throughput WITHOUT changing profile:
  ↑ max-concurrency-per-subscriber     (more VTs per subscriber)
  ↑ subscriber-count                   (more subscribers in this JVM)
  ↓ avgProcessingTime                  (optimize business logic or DB queries)
  ↑ hikari.maximum-pool-size           (reduce VT parking time)

Add more throughput BY changing profile:
  single → multi-subscriber            (N parallel AMPS connections in same JVM)
  multi  → multi-jvm-subscriber        (deploy K identical JVMs)

Add more throughput WITH multi-jvm-subscriber (no code change):
  Deploy an additional JVM node        (linear scaling: +N×M VTs per new JVM)
```
