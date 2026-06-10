package com.shan.mq.amps.ampsqueueconcurrency.publisher;

import com.crankuptheamps.client.HAClient;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.util.Locale;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Publisher lifecycle component for the {@code message-publisher} profile.
 *
 * Starts N virtual-thread workers. Each worker loops:
 *   rateLimiter.acquire() → factory.generate() → haClient.publish()
 *
 * Phase = MAX_VALUE - 1 → stops BEFORE subscribers when profiles are combined.
 * Subscribers drain the queue; publisher does not publish new messages during drain.
 *
 * Auto-stop: after {@code amps.publisher.run-duration} the publisher calls stop()
 * automatically. Format: 30s / 5m / 2h. Default: 30m. Works orthogonally with all
 * modes — whichever limit (count or time) fires first wins.
 */
@Slf4j
@Component
@Profile("message-publisher")
@RequiredArgsConstructor
public class AmpsMessagePublisher implements SmartLifecycle {

    private final HAClient ampsPublisherClient;
    private final MessagePayloadFactory payloadFactory;
    private final PublisherRateLimiter rateLimiter;
    private final PublisherMetrics metrics;

    @Value("${amps.publisher.topic:/queue/trades}")
    private String topic;

    @Value("${amps.publisher.mode:RATE_LIMITED}")
    private PublisherMode mode;

    @Value("${amps.publisher.payload-template:TRADE}")
    private PayloadTemplate payloadTemplate;

    @Value("${amps.publisher.total-messages:0}")
    private long totalMessages;

    @Value("${amps.publisher.concurrent-publishers:5}")
    private int concurrentPublishers;

    @Value("${amps.publisher.log-progress-every:1000}")
    private long logProgressEvery;

    @Value("${amps.publisher.run-duration:30m}")
    private String runDuration;

    private final AtomicBoolean running   = new AtomicBoolean(false);
    private final AtomicLong    sequence  = new AtomicLong(0);
    private ExecutorService          workerPool;
    private ScheduledExecutorService stopScheduler;
    private ScheduledFuture<?>       stopFuture;

    // ── SmartLifecycle ────────────────────────────────────────────────────────

    @Override
    public void start() {
        running.set(true);
        workerPool = Executors.newThreadPerTaskExecutor(
                Thread.ofVirtual().name("amps-pub-vt-", 0).factory());

        long durationSeconds = parseDurationToSeconds(runDuration);
        stopScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "amps-pub-stop-scheduler");
            t.setDaemon(true);
            return t;
        });
        stopFuture = stopScheduler.schedule((Runnable) this::stop, durationSeconds, TimeUnit.SECONDS);

        for (int i = 0; i < concurrentPublishers; i++) {
            final int workerId = i + 1;
            workerPool.submit(() -> publishLoop(workerId));
        }
        log.info("AmpsMessagePublisher started: workers={} topic={} mode={} template={} runDuration={}",
                concurrentPublishers, topic, mode, payloadTemplate, runDuration);
    }

    @Override
    public void stop() {
        if (!running.compareAndSet(true, false)) return;

        if (stopFuture != null)    stopFuture.cancel(false);
        if (stopScheduler != null) stopScheduler.shutdown();

        workerPool.shutdown();
        try {
            if (!workerPool.awaitTermination(10, TimeUnit.SECONDS)) {
                workerPool.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            workerPool.shutdownNow();
        }
        log.info("AmpsMessagePublisher stopped — total published={} errors={}",
                metrics.totalPublished(), metrics.totalErrors());
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }

    @Override
    public int getPhase() {
        return Integer.MAX_VALUE - 1;  // stop before subscribers
    }

    // ── Worker loop ───────────────────────────────────────────────────────────

    private void publishLoop(int workerId) {
        metrics.workerStarted();
        MDC.put("publisherWorker", String.valueOf(workerId));
        try {
            while (running.get() && !shouldStop()) {
                rateLimiter.acquire();

                if (!running.get()) break;

                long seq     = sequence.incrementAndGet();
                String payload = payloadFactory.generate(payloadTemplate, seq);

                metrics.publishTimer().record(() -> {
                    try {
                        ampsPublisherClient.publish(topic, payload);
                        metrics.recordPublished();
                    } catch (Exception e) {
                        metrics.recordError();
                        log.warn("Publish error seq={} error={}", seq, e.getMessage());
                    }
                });

                if (seq % logProgressEvery == 0) {
                    log.info("Published seq={} total={} errors={}",
                            seq, metrics.totalPublished(), metrics.totalErrors());
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            metrics.workerStopped();
            MDC.clear();
            log.debug("Publisher worker #{} exited", workerId);
        }
    }

    private boolean shouldStop() {
        return mode == PublisherMode.FIXED_COUNT
                && totalMessages > 0
                && sequence.get() >= totalMessages;
    }

    // ── Duration parser ───────────────────────────────────────────────────────

    static long parseDurationToSeconds(String value) {
        String v = value.trim().toLowerCase(Locale.ROOT);
        if (v.endsWith("h")) return Long.parseLong(v.substring(0, v.length() - 1)) * 3600;
        if (v.endsWith("m")) return Long.parseLong(v.substring(0, v.length() - 1)) * 60;
        if (v.endsWith("s")) return Long.parseLong(v.substring(0, v.length() - 1));
        return Long.parseLong(v);
    }
}
