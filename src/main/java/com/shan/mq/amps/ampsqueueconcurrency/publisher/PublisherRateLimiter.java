package com.shan.mq.amps.ampsqueueconcurrency.publisher;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.util.concurrent.Semaphore;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Token-bucket rate limiter for the message publisher.
 *
 * A scheduler refills the semaphore every {@code replenishIntervalMs} milliseconds
 * by releasing {@code tokensPerInterval} permits. Publisher VTs call {@link #acquire()}
 * which blocks when the bucket is empty, naturally throttling aggregate throughput.
 *
 * Effective rate = messagesPerSecond ± replenishIntervalMs granularity.
 */
@Slf4j
@Component
@Profile("message-publisher")
public class PublisherRateLimiter {

    @Value("${amps.publisher.messages-per-second:500}")
    private int messagesPerSecond;

    @Value("${amps.publisher.rate-replenish-interval-ms:100}")
    private long replenishIntervalMs;

    @Value("${amps.publisher.mode:RATE_LIMITED}")
    private PublisherMode mode;

    private Semaphore bucket;
    private ScheduledExecutorService scheduler;
    private ScheduledFuture<?> refillTask;

    @PostConstruct
    public void init() {
        if (mode == PublisherMode.BURST || mode == PublisherMode.INFINITE) {
            // No rate limiting — acquire() is a no-op
            bucket = new Semaphore(Integer.MAX_VALUE, false);
            log.info("Rate limiter disabled (mode={})", mode);
            return;
        }

        int tokensPerInterval = Math.max(1,
                (int) (messagesPerSecond * replenishIntervalMs / 1000.0));

        // Start with one interval's worth of tokens — avoids a burst at t=0
        bucket = new Semaphore(tokensPerInterval, false);

        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "amps-rate-refill");
            t.setDaemon(true);
            return t;
        });

        refillTask = scheduler.scheduleAtFixedRate(
                () -> refill(tokensPerInterval),
                replenishIntervalMs,
                replenishIntervalMs,
                TimeUnit.MILLISECONDS);

        log.info("Rate limiter: {}msg/s, {}ms interval, {}tok/interval",
                messagesPerSecond, replenishIntervalMs, tokensPerInterval);
    }

    /** Blocks until a token is available. */
    public void acquire() throws InterruptedException {
        bucket.acquire();
    }

    /** Non-blocking token acquisition. Returns false if no token available. */
    public boolean tryAcquire() {
        return bucket.tryAcquire();
    }

    @PreDestroy
    public void destroy() {
        if (refillTask != null) refillTask.cancel(false);
        if (scheduler != null)  scheduler.shutdownNow();
    }

    private void refill(int tokens) {
        // Cap permits to avoid unbounded accumulation when VTs are slower than the refill rate
        int current = bucket.availablePermits();
        int deficit = tokens - current;
        if (deficit > 0) {
            bucket.release(deficit);
        }
    }
}
