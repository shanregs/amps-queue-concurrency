# Rate Limiters — Theory, Algorithms, Enterprise Patterns, and Implementation

> Covers: five core algorithms, C4 system diagrams, message-flow diagrams,
> library comparison, enterprise use cases, and production code examples
> including this project's token-bucket publisher rate limiter.

---

## 1. Why Rate Limiting Exists

Any system that produces or consumes resources has a finite capacity. Without a gate,
producers will saturate consumers, downstream APIs will return 429s, and databases will
fall over. Rate limiting is the **admission control** layer that converts an unbounded
burst into a sustainable flow.

Three distinct problems, three distinct gate placements:

```
┌──────────────────────────────────────────────────────────────┐
│                        Enterprise App                        │
│                                                              │
│  Inbound  ──[API Gateway rate limiter]──►  Service Layer     │
│                                              │               │
│  Internal ──[Worker rate limiter]──────►  Message Broker     │
│                                              │               │
│  Outbound ──[Client rate limiter]──────►  External API       │
└──────────────────────────────────────────────────────────────┘
```

| Problem | Gate | Goal |
|---|---|---|
| Inbound abuse / DDoS | API gateway | Protect service from overload |
| Internal producer speed | Worker rate limiter | Match consumer capacity |
| External API quota | Client rate limiter | Avoid 429 / billing overcharge |

---

## 2. The Five Core Algorithms

### 2.1 Token Bucket

**Mental model**: a bucket fills with tokens at a constant rate. Each request consumes
one token. If the bucket is empty, the request either waits (blocking) or is rejected.

```
                   Refill rate: R tokens/sec
                         │
                         ▼
              ┌─────────────────────┐
              │  ○ ○ ○ ○ ○ ○ ○ ○ ○  │  ← capacity B tokens
              └──────────┬──────────┘
                         │ consume 1 per request
                         ▼
                    Request passes
```

**Properties**:
- Allows **bursts** up to capacity B
- Long-term average rate is bounded by R
- Unused tokens accumulate (up to B)
- Implementation: single integer counter + refill scheduler

**This project's `PublisherRateLimiter`** is a token-bucket:

```java
// src/main/java/…/publisher/PublisherRateLimiter.java
public class PublisherRateLimiter {

    private final Semaphore tokens;
    private final ScheduledExecutorService scheduler;
    private final int tokensPerInterval;

    public PublisherRateLimiter(int ratePerSecond) {
        // burst capacity = ratePerSecond (1-second window)
        this.tokens = new Semaphore(ratePerSecond);
        this.tokensPerInterval = Math.max(1, ratePerSecond / 10);  // 100ms ticks

        scheduler = Executors.newSingleThreadScheduledExecutor(
            Thread.ofVirtual().factory());

        // Refill every 100ms for smoother distribution
        scheduler.scheduleAtFixedRate(this::refill, 100, 100, MILLISECONDS);
    }

    public void acquire() {
        tokens.acquireUninterruptibly(1);   // blocks publisher VT until token available
    }

    private void refill() {
        int current = tokens.availablePermits();
        int deficit = ratePerSecond - current;
        if (deficit > 0) tokens.release(Math.min(tokensPerInterval, deficit));
    }
}
```

**When to use**: producers publishing to a message broker, outbound HTTP calls,
any scenario where short bursts are acceptable but sustained rate must be bounded.

---

### 2.2 Leaky Bucket

**Mental model**: requests enter a bucket; the bucket leaks at a constant rate regardless
of how fast requests arrive. Overflow is discarded or rejected.

```
  Requests ──►  ┌──────────────┐  ──► processed at constant rate R
  (any rate)    │   ~ ~ ~ ~ ~  │
                │   ~ ~ ~ ~ ~  │  (excess discarded / 429)
                └──────────────┘
                    capacity Q
```

**Properties**:
- Output rate is **strictly constant** — no bursting
- Incoming traffic is smoothed; bursts are absorbed (up to Q) or dropped
- Simpler than token bucket when strict output smoothness is needed

**Difference from token bucket**:

| | Token Bucket | Leaky Bucket |
|---|---|---|
| Burst | Yes (up to capacity) | No — output is always smooth |
| Unused capacity | Accumulates as tokens | No rollover |
| Use case | Clients with occasional bursts | Strict output rate (e.g., audio/video streaming) |

**Implementation sketch** (queue-based):

```java
public class LeakyBucketRateLimiter {

    private final BlockingQueue<Runnable> bucket;
    private final ScheduledExecutorService leakExecutor;

    public LeakyBucketRateLimiter(int capacity, int ratePerSecond) {
        this.bucket = new ArrayBlockingQueue<>(capacity);

        long intervalMs = 1000L / ratePerSecond;
        leakExecutor = Executors.newSingleThreadScheduledExecutor();
        leakExecutor.scheduleAtFixedRate(this::leak, 0, intervalMs, MILLISECONDS);
    }

    public boolean submit(Runnable task) {
        return bucket.offer(task);   // false = rejected (bucket full)
    }

    private void leak() {
        Runnable task = bucket.poll();
        if (task != null) task.run();
    }
}
```

**When to use**: outbound call throttling where you must guarantee a smooth request
stream to a downstream API, regardless of how requests arrive internally.

---

### 2.3 Fixed Window Counter

**Mental model**: divide time into fixed windows (e.g., 1-minute slots). Count requests
per window. Reject when count exceeds limit.

```
Window 1 [0–60s]      Window 2 [60–120s]
│ ██████████░░░░░░ │  │ ██░░░░░░░░░░░░░░ │
│ 80/100 used      │  │ 20/100 used      │
```

**Properties**:
- Very simple: one integer counter per window, reset on boundary
- **Boundary burst problem**: 100 requests at t=59s + 100 at t=61s = 200 in 2 seconds
  (within two windows but violates intent)

```
t=0                  t=60                t=120
│── Window 1 ────────│── Window 2 ────────│
             ████████ ████████
             99 reqs  99 reqs  ← 198 requests in 2s window centred at t=60
```

**When to use**: simple per-user API quotas where boundary bursts are tolerable,
or as a coarse first-pass filter before a more precise limiter.

**Implementation** (Redis atomic increment):

```java
// Spring Data Redis example
public boolean isAllowed(String userId, int limitPerMinute) {
    String key = "rl:" + userId + ":" + (System.currentTimeMillis() / 60_000);
    Long count = redisTemplate.opsForValue().increment(key);
    if (count == 1) redisTemplate.expire(key, 65, SECONDS);  // slight buffer
    return count <= limitPerMinute;
}
```

---

### 2.4 Sliding Window Log

**Mental model**: keep a log of every request timestamp. When a new request arrives,
discard entries older than the window, count remaining — reject if over limit.

```
Window = 60s, limit = 5

timestamps: [t-55, t-40, t-30, t-10, t-5]  → count=5  → block next
timestamps: [t-40, t-30, t-10, t-5]        (t-55 expired) → count=4 → allow
```

**Properties**:
- **No boundary burst** — exact rolling window
- High memory cost: O(requests) per user; impractical for high-traffic APIs
- Perfect accuracy — ideal for low-volume high-precision throttling

**When to use**: webhook callbacks, per-endpoint strict throttling where request volume
is low and accuracy matters more than memory.

**Implementation** (Redis sorted set):

```java
public boolean isAllowed(String userId, int limit, long windowMs) {
    String key = "rl:log:" + userId;
    long now = System.currentTimeMillis();
    long windowStart = now - windowMs;

    redisTemplate.opsForZSet().removeRangeByScore(key, 0, windowStart);
    Long count = redisTemplate.opsForZSet().zCard(key);
    if (count < limit) {
        redisTemplate.opsForZSet().add(key, String.valueOf(now), now);
        redisTemplate.expire(key, windowMs + 1000, MILLISECONDS);
        return true;
    }
    return false;
}
```

---

### 2.5 Sliding Window Counter (Hybrid)

**Mental model**: approximate sliding window using two fixed-window counters, weighted
by how much of the current window has elapsed.

```
Previous window    Current window
│████████████████│░░░░░░░░░░░░░░░│
  count_prev=80      count_curr=20
      weight = (1 - elapsed_fraction)

estimated_count = count_prev × (1 - 0.3) + count_curr
                = 80 × 0.7 + 20
                = 76
```

**Properties**:
- Low memory: only two counters
- ~0.003% error rate vs sliding window log at high traffic
- Production-grade: used by Nginx, Cloudflare, Redis-cell

**When to use**: high-traffic APIs where near-perfect accuracy is good enough and
memory efficiency is critical. The industry default for API gateways.

**Implementation** (Bucket4j sliding window approximation):

```java
Bandwidth slidingWindow = Bandwidth.builder()
    .capacity(100)
    .refillIntervally(100, Duration.ofMinutes(1))
    .build();

Bucket bucket = Bucket.builder()
    .addLimit(slidingWindow)
    .build();
```

---

## 3. Algorithm Comparison Matrix

| Algorithm | Burst | Memory | Accuracy | Complexity | Best For |
|---|---|---|---|---|---|
| Token Bucket | Yes (up to capacity) | O(1) | High | Low | Producer throttling, outbound calls |
| Leaky Bucket | No | O(queue) | Exact rate | Low | Smooth output (streaming, printing) |
| Fixed Window | Yes (at boundary) | O(1) | Low | Very Low | Coarse quota, simple counters |
| Sliding Window Log | No | O(requests) | Exact | Medium | Low-volume precise throttling |
| Sliding Window Counter | Partial | O(1) | ~99.9% | Medium | High-traffic API gateways |

---

## 4. C4 Context Diagram — Rate Limiter in an Enterprise System

```
╔══════════════════════════════════════════════════════════════════════════╗
║  [System Context]                                                        ║
║                                                                          ║
║   ┌────────────┐          ┌──────────────────────────────────────┐       ║
║   │  Mobile /  │  HTTP    │         API Gateway                  │       ║
║   │  Web       │─────────►│  [Rate Limiter: Sliding Window]      │       ║
║   │  Clients   │          │  per-user: 1000 req/min              │       ║
║   └────────────┘          └───────────┬──────────────────────────┘       ║
║                                       │ allowed requests                 ║
║   ┌────────────┐  events              ▼                                  ║
║   │  Internal  │         ┌────────────────────────┐                      ║
║   │  Services  │────────►│   Order Service        │                      ║
║   └────────────┘         │  [Rate Limiter:        │                      ║
║                          │   Token Bucket]        │                      ║
║                          │  publish: 500 msg/sec  │                      ║
║                          └──────────┬─────────────┘                      ║
║                                     │                                    ║
║                                     ▼                                    ║
║                          ┌─────────────────────┐                         ║
║                          │  AMPS / Kafka /      │                        ║
║                          │  Message Broker      │                        ║
║                          └─────────────────────┘                         ║
║                                     │                                    ║
║                                     ▼                                    ║
║                          ┌─────────────────────┐  ┌──────────────────┐  ║
║                          │  External Payment   │  │  Analytics DB    │  ║
║                          │  API                │  │  [Leaky Bucket]  │  ║
║                          │  [Client RL:        │  │  write: 200/sec  │  ║
║                          │   10 req/sec]       │  └──────────────────┘  ║
║                          └─────────────────────┘                        ║
╚══════════════════════════════════════════════════════════════════════════╝
```

---

## 5. C4 Container Diagram — Rate Limiter Placement

```
╔══════════════════════════════════════════════════════════════════════════╗
║  [Container Diagram — Order Processing System]                           ║
║                                                                          ║
║  ┌─────────────────────────────────────────────────────────────────┐    ║
║  │  API Gateway Container (Spring Cloud Gateway / Kong)            │    ║
║  │                                                                  │    ║
║  │  ┌──────────────────────────────────────────────────────────┐  │    ║
║  │  │  RateLimiterFilter                                        │  │    ║
║  │  │  ├── Redis sliding-window counter (per user/IP)          │  │    ║
║  │  │  ├── Returns 429 + Retry-After header on breach          │  │    ║
║  │  │  └── Passes X-RateLimit-Remaining downstream             │  │    ║
║  │  └──────────────────────────────────────────────────────────┘  │    ║
║  └───────────────────────────┬─────────────────────────────────────┘    ║
║                              │                                          ║
║  ┌───────────────────────────▼─────────────────────────────────────┐    ║
║  │  Order Service Container (Spring Boot)                          │    ║
║  │                                                                  │    ║
║  │  ┌──────────────┐    ┌────────────────────────────────────┐     │    ║
║  │  │  REST Layer   │    │  AmpsMessagePublisher               │    │    ║
║  │  │  (Inbound)    │    │  ┌──────────────────────────────┐  │    │    ║
║  │  │               │    │  │  PublisherRateLimiter          │  │    │    ║
║  │  │               │    │  │  Token Bucket: 500 msg/sec   │  │    │    ║
║  │  │               │    │  │  burst capacity: 500 tokens  │  │    │    ║
║  │  │               │    │  └──────────────────────────────┘  │    │    ║
║  │  └──────────────┘    └────────────────────────────────────┘     │    ║
║  └───────────────────────────┬─────────────────────────────────────┘    ║
║                              │                                          ║
║  ┌───────────────────────────▼─────────────────────────────────────┐    ║
║  │  External API Client Container                                  │    ║
║  │                                                                  │    ║
║  │  ┌────────────────────────────────────────────────────────┐     │    ║
║  │  │  Resilience4j RateLimiter (Leaky Bucket variant)       │     │    ║
║  │  │  limitForPeriod: 10, limitRefreshPeriod: 1s            │     │    ║
║  │  │  timeoutDuration: 500ms (fail fast on quota breach)    │     │    ║
║  │  └────────────────────────────────────────────────────────┘     │    ║
║  └─────────────────────────────────────────────────────────────────┘    ║
╚══════════════════════════════════════════════════════════════════════════╝
```

---

## 6. Message Flow — Token Bucket (Publisher → Broker)

```
Publisher VT-1 ──► acquire() ──────────────────────────────────────────────►
Publisher VT-2 ──► acquire() ──── [BLOCKED: bucket empty] ─────────────────►
Publisher VT-3 ──► acquire() ──── [BLOCKED: bucket empty] ─────────────────►
                       │
              Semaphore (bucket)
              ┌─────────────────────────────────────────────────────────┐
  t=0ms    ► │ ○ ○ ○ ○ ○  (5 tokens)                                   │
  VT-1 acq  │ ○ ○ ○ ○    (4 tokens)  → VT-1 publishes msg-1           │
  VT-2 acq  │ ○ ○ ○      (3 tokens)  → VT-2 publishes msg-2           │
  VT-3 acq  │ ○ ○        (2 tokens)  → VT-3 publishes msg-3           │
  t=100ms ► │ ○ ○ ○      refill +1                                     │
  VT-4 acq  │ ○ ○        (2 tokens)  → VT-4 publishes msg-4           │
              └─────────────────────────────────────────────────────────┘
                       │
                       ▼
              AMPS /queue/trades
              ┌─────────────────────────────────────────────────────────┐
              │  msg-1  msg-2  msg-3  msg-4  (metered, no overload)     │
              └─────────────────────────────────────────────────────────┘
                       │
              ▼ Subscriber semaphore (backpressure gate)
              MultiAmpsSubscriberPool
```

---

## 7. Message Flow — Sliding Window Counter (API Gateway)

```
Client A: 3 requests at t=0, t=20, t=40
Client B: rapid-fire 50 requests in t=0..2

                API Gateway (limit=10/min per client)
                ┌──────────────────────────────────────────┐
  Client A ───► │ key=A:window1  count=3  → ALLOW          │
  Client A ───► │ key=A:window1  count=3  → ALLOW (cached) │
  Client B ───► │ key=B:window1  count=1  → ALLOW          │
  Client B ───► │ key=B:window1  count=2  → ALLOW          │
  ...           │ ...                                       │
  Client B ───► │ key=B:window1  count=11 → 429 REJECT     │
                └──────────────────────────────────────────┘
                        │
                        │ allowed requests only
                        ▼
                  Order Service
```

---

## 8. Message Flow — Resilience4j Rate Limiter (Outbound Call)

```
Order Service
  │
  ├── OrderCreated event
  │
  ▼
PaymentClient.charge(orderId, amount)
  │
  ▼
┌──────────────────────────────────────────────────┐
│  Resilience4j RateLimiter (10 req/sec)           │
│                                                  │
│  Thread-1 ──► tryAcquirePermission(timeout=500ms)│
│               ├── permit available → proceed     │
│               │   ──► POST /payments/charge      │
│               │   ◄── 200 OK                     │
│               └── no permit within 500ms         │
│                   ──► throws RequestNotPermitted │
│                   ──► fallback: queue for retry  │
└──────────────────────────────────────────────────┘
```

---

## 9. Enterprise Use Cases

### 9.1 Inbound: Protect a REST API from overload

**Problem**: a public API receives bot traffic spikes that can overwhelm the service.

**Solution**: sliding window counter at the API gateway, keyed by API token or IP.

**Libraries**: Spring Cloud Gateway `RequestRateLimiter`, Kong rate-limiting plugin,
AWS API Gateway built-in throttling.

```yaml
# Spring Cloud Gateway
spring:
  cloud:
    gateway:
      routes:
        - id: order-api
          uri: lb://order-service
          filters:
            - name: RequestRateLimiter
              args:
                redis-rate-limiter.replenishRate: 100    # tokens/sec steady
                redis-rate-limiter.burstCapacity: 200    # burst up to 200
                redis-rate-limiter.requestedTokens: 1
                key-resolver: "#{@userKeyResolver}"
```

```java
@Bean
KeyResolver userKeyResolver() {
    return exchange -> Mono.justOrEmpty(
        exchange.getRequest().getHeaders().getFirst("X-User-Id")
    ).defaultIfEmpty("anonymous");
}
```

---

### 9.2 Internal: Publisher → Message Broker (this project)

**Problem**: a publisher running N virtual threads can saturate an AMPS queue,
causing subscriber backlog and lease expiry storms.

**Solution**: token-bucket rate limiter shared across all VT workers.

```java
// application-message-publisher.yaml
amps:
  publisher:
    rate-per-second: 500
    mode: RATE_LIMITED
    concurrent-workers: 20

// Each VT worker calls rateLimiter.acquire() before publish()
// → aggregate throughput bounded at 500 msg/sec regardless of worker count
```

---

### 9.3 Outbound: Third-Party API Quota (Payment, SMS, Email)

**Problem**: Stripe allows 100 req/sec per key; internal service sends 500 req/sec
during order spikes.

**Solution**: Resilience4j `RateLimiter` wrapping the HTTP client. Calls that exceed
quota block briefly (configurable timeout), then throw — caller queues for retry.

```java
@Configuration
public class PaymentClientConfig {

    @Bean
    public RateLimiter stripeRateLimiter() {
        RateLimiterConfig config = RateLimiterConfig.custom()
            .limitForPeriod(100)                    // 100 permits per period
            .limitRefreshPeriod(Duration.ofSeconds(1))
            .timeoutDuration(Duration.ofMillis(500)) // wait max 500ms
            .build();
        return RateLimiter.of("stripe", config);
    }
}

@Service
public class PaymentClient {

    private final RateLimiter rateLimiter;
    private final RestTemplate http;

    public PaymentResponse charge(String orderId, BigDecimal amount) {
        return RateLimiter.decorateSupplier(rateLimiter,
            () -> http.postForObject("/charges", new ChargeRequest(orderId, amount),
                                     PaymentResponse.class)
        ).get();
    }
}
```

---

### 9.4 Per-User Rate Limiting with Bucket4j + Redis (Distributed)

**Problem**: rate limit is per user across a horizontally scaled service (multiple JVMs).
A local in-JVM semaphore only limits one JVM.

**Solution**: Bucket4j with Redis proxy — bucket state lives in Redis, shared by all nodes.

```java
@Configuration
public class Bucket4jConfig {

    @Bean
    public ProxyManager<String> proxyManager(RedissonClient redisson) {
        return Bucket4jRedisson.entryPointToRedisson(redisson);
    }
}

@Service
public class RateLimitService {

    private final ProxyManager<String> proxyManager;

    public boolean isAllowed(String userId) {
        BucketConfiguration config = BucketConfiguration.builder()
            .addLimit(Bandwidth.classic(100, Refill.greedy(100, Duration.ofMinutes(1))))
            .build();

        Bucket bucket = proxyManager.builder().build(userId, config);
        return bucket.tryConsume(1);
    }
}
```

---

### 9.5 Adaptive Rate Limiting — Back-Pressure Feedback Loop

**Problem**: static rate limits are too conservative at low load, too aggressive at high load.

**Solution**: adaptive rate limiter that adjusts based on downstream latency / error rate.

```
                   ┌─────────────────────────────────────────┐
                   │          Adaptive Rate Limiter           │
                   │                                          │
  Producer ───────►│  current_rate = base_rate × adjustment  │───► Downstream
                   │                                          │◄─── latency P99
                   │  adjustment = 1.0                        │◄─── error rate
                   │  if p99 > threshold: adjustment *= 0.9  │
                   │  if p99 < low_mark:  adjustment *= 1.05 │
                   └─────────────────────────────────────────┘
```

Resilience4j `RateLimiter` + `CircuitBreaker` in combination achieves this pattern:

```java
@Bean
public RateLimiter adaptivePaymentLimiter(MeterRegistry registry) {
    // Start conservative; Sentinel / external config can adjust limitForPeriod at runtime
    RateLimiterConfig config = RateLimiterConfig.custom()
        .limitForPeriod(50)
        .limitRefreshPeriod(Duration.ofSeconds(1))
        .timeoutDuration(Duration.ofMillis(200))
        .build();
    return RateLimiter.of("payment-adaptive", config, registry);
}
```

---

## 10. Library Comparison

### 10.1 Resilience4j `RateLimiter`

**Algorithm**: semaphore-based, refreshed at fixed interval (effectively leaky bucket).  
**Scope**: single JVM.  
**Best for**: outbound HTTP calls, service-to-service calls.

```xml
<dependency>
    <groupId>io.github.resilience4j</groupId>
    <artifactId>resilience4j-spring-boot3</artifactId>
    <version>2.2.0</version>
</dependency>
```

```yaml
resilience4j:
  ratelimiter:
    instances:
      stripe:
        limit-for-period: 100
        limit-refresh-period: 1s
        timeout-duration: 500ms
        register-health-indicator: true
```

```java
@RateLimiter(name = "stripe", fallbackMethod = "chargeFallback")
public PaymentResponse charge(ChargeRequest req) { ... }

public PaymentResponse chargeFallback(ChargeRequest req, RequestNotPermitted ex) {
    // queue for retry or return degraded response
}
```

---

### 10.2 Bucket4j

**Algorithm**: token bucket (exact implementation).  
**Scope**: local or distributed (Redis, Hazelcast, Ignite, Infinispan).  
**Best for**: per-user/per-key API quota, distributed rate limiting.

```xml
<dependency>
    <groupId>com.bucket4j</groupId>
    <artifactId>bucket4j-core</artifactId>
    <version>8.10.1</version>
</dependency>
<!-- For Redis distributed: -->
<dependency>
    <groupId>com.bucket4j</groupId>
    <artifactId>bucket4j-redis</artifactId>
    <version>8.10.1</version>
</dependency>
```

```java
// Local token bucket — 500 tokens, refill 500 per second
Bucket bucket = Bucket.builder()
    .addLimit(Bandwidth.classic(500, Refill.greedy(500, Duration.ofSeconds(1))))
    .build();

// Consume or wait
if (!bucket.tryConsume(1)) {
    // rate limit exceeded
    throw new RateLimitException("quota exceeded");
}

// Or: blocking consume (waits up to timeout)
boolean acquired = bucket.asBlocking().tryConsume(1, Duration.ofMillis(200));
```

---

### 10.3 Guava `RateLimiter`

**Algorithm**: token bucket with smooth warm-up variant.  
**Scope**: single JVM (no distributed support).  
**Best for**: simple local rate limiting, prototyping.

```xml
<dependency>
    <groupId>com.google.guava</groupId>
    <artifactId>guava</artifactId>
    <version>33.2.1-jre</version>
</dependency>
```

```java
// Simple: 500 permits per second
RateLimiter limiter = RateLimiter.create(500.0);

// With warm-up: ramp up over 5 seconds (avoids cold-start spike)
RateLimiter warmUp = RateLimiter.create(500.0, Duration.ofSeconds(5));

// Usage: blocks until permit available
limiter.acquire();                    // blocking
boolean ok = limiter.tryAcquire();   // non-blocking
```

**Limitation**: `acquire()` blocks the calling thread — not suitable for reactive/VT
stacks where blocking is costly. Prefer Bucket4j `asBlocking()` + VT, or Resilience4j.

---

### 10.4 Spring Cloud Gateway `RequestRateLimiter`

**Algorithm**: Redis-backed token bucket (uses `redis-cell` module or Lua scripts).  
**Scope**: distributed (Redis).  
**Best for**: API gateway-level rate limiting in microservice architectures.

```yaml
spring:
  cloud:
    gateway:
      default-filters:
        - name: RequestRateLimiter
          args:
            redis-rate-limiter.replenishRate: 100
            redis-rate-limiter.burstCapacity: 200
            redis-rate-limiter.requestedTokens: 1
```

**Response headers returned to client**:
```
X-RateLimit-Remaining: 42
X-RateLimit-Burst-Capacity: 200
X-RateLimit-Replenish-Rate: 100
X-RateLimit-Requested-Tokens: 1
```

---

### 10.5 Alibaba Sentinel

**Algorithm**: sliding window, leaky bucket, system adaptive (CPU/load based).  
**Scope**: distributed with Sentinel dashboard.  
**Best for**: large microservice meshes, Alibaba Cloud, service mesh rate limiting.

```xml
<dependency>
    <groupId>com.alibaba.cloud</groupId>
    <artifactId>spring-cloud-starter-alibaba-sentinel</artifactId>
    <version>2023.0.1.2</version>
</dependency>
```

```java
@SentinelResource(
    value = "chargeOrder",
    blockHandler = "chargeBlockHandler",
    fallback = "chargeFallback"
)
public PaymentResponse charge(String orderId) { ... }

public PaymentResponse chargeBlockHandler(String orderId, BlockException ex) {
    return PaymentResponse.rateLimited();
}
```

---

### 10.6 Library Decision Matrix

| Library | Algorithm | Distributed | Spring Boot | Best For |
|---|---|---|---|---|
| Resilience4j | Semaphore/leaky | No | Native Boot 3 | Outbound HTTP calls |
| Bucket4j | Token bucket (exact) | Yes (Redis) | Via starter | Per-user API quotas |
| Guava | Token bucket | No | Manual | Simple local limiting |
| Spring Cloud Gateway | Token bucket | Yes (Redis) | Gateway only | API gateway ingress |
| Sentinel | Multiple | Yes (dashboard) | Via starter | Alibaba/large mesh |

---

## 11. Complete Enterprise Example: Order Service with Three Rate Limiters

This example shows all three gate placements working together: inbound API protection,
internal publisher rate limiting, and outbound payment API quota.

### Dependencies (`pom.xml`)

```xml
<!-- Resilience4j for outbound -->
<dependency>
    <groupId>io.github.resilience4j</groupId>
    <artifactId>resilience4j-spring-boot3</artifactId>
    <version>2.2.0</version>
</dependency>
<!-- Bucket4j for inbound per-user -->
<dependency>
    <groupId>com.bucket4j</groupId>
    <artifactId>bucket4j-core</artifactId>
    <version>8.10.1</version>
</dependency>
<dependency>
    <groupId>com.bucket4j</groupId>
    <artifactId>bucket4j-redis</artifactId>
    <version>8.10.1</version>
</dependency>
```

### Inbound: per-user API rate limit (filter)

```java
@Component
public class UserRateLimitFilter extends OncePerRequestFilter {

    private final ProxyManager<String> buckets;

    @Override
    protected void doFilterInternal(HttpServletRequest req,
                                    HttpServletResponse res,
                                    FilterChain chain) throws IOException, ServletException {
        String userId = req.getHeader("X-User-Id");
        if (userId == null) { chain.doFilter(req, res); return; }

        BucketConfiguration config = BucketConfiguration.builder()
            .addLimit(Bandwidth.classic(100, Refill.greedy(100, Duration.ofMinutes(1))))
            .build();

        Bucket bucket = buckets.builder().build(userId, config);

        if (bucket.tryConsume(1)) {
            res.setHeader("X-RateLimit-Remaining",
                          String.valueOf(bucket.getAvailableTokens()));
            chain.doFilter(req, res);
        } else {
            res.setStatus(429);
            res.setHeader("Retry-After", "60");
            res.getWriter().write("{\"error\":\"rate limit exceeded\"}");
        }
    }
}
```

### Internal: publisher rate limiter (this project's token bucket)

```java
// See PublisherRateLimiter.java — token bucket via Semaphore + ScheduledExecutorService
// Each VT publisher worker calls rateLimiter.acquire() before haClient.publish()
```

### Outbound: Resilience4j for Stripe

```java
@Service
public class StripePaymentGateway {

    private final RateLimiter rateLimiter;
    private final RestTemplate http;

    @RateLimiter(name = "stripe", fallbackMethod = "enqueueForRetry")
    public PaymentResult charge(String orderId, BigDecimal amount) {
        return http.postForObject(
            "https://api.stripe.com/v1/charges",
            Map.of("amount", amount, "order_id", orderId),
            PaymentResult.class
        );
    }

    public PaymentResult enqueueForRetry(String orderId, BigDecimal amount,
                                          RequestNotPermitted ex) {
        retryQueue.add(new PendingCharge(orderId, amount));
        return PaymentResult.queued();
    }
}
```

### Full data flow

```
Client ──[429 check: Bucket4j/Redis]──► OrderController.createOrder()
                                              │
                                    OrderService.process()
                                              │
                              AmpsMessagePublisher.publish()
                                              │
                                   [acquire: token bucket]
                                              │
                              haClient.publish("/queue/trades")
                                              │
                              AMPS Queue ─────┘
                                              │
                              MultiAmpsSubscriberPool
                                              │
                              MessageProcessor (transactional)
                                              │
                              StripePaymentGateway.charge()
                                              │
                                   [acquire: Resilience4j]
                                              │
                              POST Stripe API ─────────────► Stripe
```

---

## 12. Observability for Rate Limiters

### Micrometer metrics (Resilience4j auto-export)

```
resilience4j_ratelimiter_available_permissions{name="stripe"}  42
resilience4j_ratelimiter_waiting_threads{name="stripe"}         3
```

### Bucket4j metrics (manual)

```java
BucketListener metrics = new BucketListener() {
    @Override
    public void onConsumed(long tokens) {
        registry.counter("ratelimiter.consumed", "limiter", "user-api").increment(tokens);
    }
    @Override
    public void onRejected(long tokens) {
        registry.counter("ratelimiter.rejected", "limiter", "user-api").increment(tokens);
    }
};
```

### Grafana dashboard queries

```promql
# Rejection rate — how often we hit the limit
rate(ratelimiter_rejected_total[1m])

# Utilization — how close are we to the limit
ratelimiter_available_permissions / ratelimiter_capacity

# P99 wait time for rate-limited requests
histogram_quantile(0.99, rate(ratelimiter_wait_duration_seconds_bucket[5m]))
```

---

## 13. When to Use Each Algorithm — Decision Flowchart

```
Do you need distributed (multi-JVM) rate limiting?
│
├── YES
│     ├── Need exact per-user quota?     ──► Bucket4j + Redis (token bucket)
│     ├── API gateway?                   ──► Spring Cloud Gateway RequestRateLimiter
│     └── Large mesh / many services?    ──► Alibaba Sentinel
│
└── NO (single JVM)
      │
      ├── Need burst allowance?
      │     ├── YES ──► Token bucket (Guava, Bucket4j local, custom Semaphore)
      │     └── NO  ──► Leaky bucket (queue-based)
      │
      ├── Need outbound API throttle?    ──► Resilience4j RateLimiter
      │
      ├── Simple fixed quota per window? ──► Fixed window counter
      │
      └── Need exact sliding window?     ──► Sliding window log (low volume only)
```

---

## 14. Quick Reference

| Scenario | Algorithm | Library | Key Config |
|---|---|---|---|
| API gateway per-user | Sliding window counter | SCG + Redis | `replenishRate`, `burstCapacity` |
| Publisher → broker | Token bucket | Custom Semaphore / Bucket4j | `ratePerSecond`, burst cap |
| Outbound payment API | Leaky bucket (semaphore) | Resilience4j | `limitForPeriod`, `timeoutDuration` |
| Per-user HTTP quota | Token bucket | Bucket4j + Redis | `capacity`, `Refill.greedy` |
| Smooth output stream | Leaky bucket | Custom queue | capacity, drain interval |
| Simple local throttle | Token bucket | Guava `RateLimiter` | `create(rate)` |