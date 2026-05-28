package com.shan.mq.amps.ampsqueueconcurrency.publisher;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class PublisherRateLimiterTest {

    private PublisherRateLimiter limiter;

    @AfterEach
    void tearDown() {
        if (limiter != null) limiter.destroy();
    }

    // ── Rate-limited mode: tokens replenish over time ─────────────────────────

    @Test
    void rateLimited_acquiresUpToTokensPerInterval() throws Exception {
        limiter = buildLimiter(PublisherMode.RATE_LIMITED, 100, 100L); // 100msg/s, 100ms interval = 10tok/interval

        // Drain initial tokens (10 per interval at 100ms)
        int acquired = 0;
        while (limiter.tryAcquire()) {
            acquired++;
            if (acquired > 20) break; // safety
        }
        assertThat(acquired).isGreaterThan(0).isLessThanOrEqualTo(20);
    }

    // ── Tokens are refilled after interval ────────────────────────────────────

    @Test
    void rateLimited_refillsTokensAfterInterval() throws Exception {
        limiter = buildLimiter(PublisherMode.RATE_LIMITED, 100, 50L); // 50ms interval → 5 tokens

        // Drain all tokens
        while (limiter.tryAcquire()) { /* drain */ }

        // Wait for 2 replenishment cycles
        Thread.sleep(120);

        // Should now have tokens again
        assertThat(limiter.tryAcquire()).isTrue();
    }

    // ── BURST mode: acquire never blocks ────────────────────────────────────

    @Test
    void burstMode_allAcquiresSucceedImmediately() throws Exception {
        limiter = buildLimiter(PublisherMode.BURST, 10, 100L);

        AtomicInteger count = new AtomicInteger();
        CountDownLatch done = new CountDownLatch(100);

        for (int i = 0; i < 100; i++) {
            Thread.ofVirtual().start(() -> {
                try {
                    limiter.acquire();
                    count.incrementAndGet();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            });
        }

        assertThat(done.await(2, TimeUnit.SECONDS)).isTrue();
        assertThat(count.get()).isEqualTo(100);
    }

    // ── FIXED_COUNT / INFINITE alias to RATE_LIMITED ─────────────────────────

    @Test
    void infiniteMode_behaviourEqualsRateLimited() throws Exception {
        limiter = buildLimiter(PublisherMode.INFINITE, 50, 100L);

        // In INFINITE mode, the limiter applies the same token-bucket as RATE_LIMITED
        // Verify at least one token is available
        assertThat(limiter.tryAcquire()).isTrue();
    }

    // ── tryAcquire returns false when exhausted ───────────────────────────────

    @Test
    void rateLimited_tryAcquire_returnsFalseWhenExhausted() throws Exception {
        limiter = buildLimiter(PublisherMode.RATE_LIMITED, 100, 200L); // 100ms*100/1000 = ~10 tokens initially

        // Drain all
        int drained = 0;
        while (limiter.tryAcquire() && drained < 100) { drained++; }

        assertThat(limiter.tryAcquire()).isFalse();
    }

    // ── Helper ────────────────────────────────────────────────────────────────

    private static PublisherRateLimiter buildLimiter(PublisherMode mode, int msgPerSec, long intervalMs) {
        PublisherRateLimiter l = new PublisherRateLimiter();
        ReflectionTestUtils.setField(l, "mode", mode);
        ReflectionTestUtils.setField(l, "messagesPerSecond", msgPerSec);
        ReflectionTestUtils.setField(l, "replenishIntervalMs", intervalMs);
        l.init();
        return l;
    }
}
