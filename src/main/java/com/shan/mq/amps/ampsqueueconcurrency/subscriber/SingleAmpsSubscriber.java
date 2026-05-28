package com.shan.mq.amps.ampsqueueconcurrency.subscriber;

import com.crankuptheamps.client.CommandId;
import com.crankuptheamps.client.HAClient;
import com.crankuptheamps.client.Message;
import com.shan.mq.amps.ampsqueueconcurrency.service.MessageDispatchService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Single-subscriber lifecycle component.
 *
 * One platform thread subscribes to the AMPS queue topic.
 * The AMPS client invokes {@link #handleMessage} on its internal delivery thread.
 * The handler acquires the semaphore (backpressure) and fans out to a virtual thread.
 *
 * Phase = MAX_VALUE → this subscriber stops LAST, after the publisher (MAX_VALUE - 1).
 */
@Slf4j
@Component
@Profile("single-subscriber")
@RequiredArgsConstructor
public class SingleAmpsSubscriber implements SmartLifecycle {

    private final HAClient ampsHaClient;
    private final Semaphore ampsSemaphore;
    private final MessageDispatchService dispatchService;

    @Value("${amps.queue.topic:/queue/trades}")
    private String topic;

    @Value("${amps.queue.filter:}")
    private String filter;

    @Value("${amps.consumer.max-concurrency:100}")
    private int maxConcurrency;

    @Value("${amps.consumer.shutdown-timeout-ms:30000}")
    private long shutdownTimeoutMs;

    private final AtomicBoolean running = new AtomicBoolean(false);
    private volatile CommandId subscriptionId;
    private volatile Thread subscriberThread;

    // ── SmartLifecycle ────────────────────────────────────────────────────────

    @Override
    public void start() {
        running.set(true);
        subscriberThread = Thread.ofPlatform()
                .name("amps-single-subscriber")
                .start(this::runSubscribeLoop);
        log.info("SingleAmpsSubscriber started topic={}", topic);
    }

    @Override
    public void stop() {
        running.set(false);
        unsubscribeSilently();
        if (subscriberThread != null) {
            subscriberThread.interrupt();
        }
        drainInFlight();
        log.info("SingleAmpsSubscriber stopped");
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }

    @Override
    public int getPhase() {
        return Integer.MAX_VALUE;
    }

    // ── Internal ──────────────────────────────────────────────────────────────

    private void runSubscribeLoop() {
        try {
            String effectiveFilter = (filter == null || filter.isBlank()) ? null : filter;
            subscriptionId = effectiveFilter != null
                    ? ampsHaClient.subscribe(this::handleMessage, topic, effectiveFilter, 5000L)
                    : ampsHaClient.subscribe(this::handleMessage, topic, 5000L);

            log.info("AMPS subscription active subId={}", subscriptionId);
            while (running.get()) {
                Thread.sleep(500);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.debug("Subscriber thread interrupted");
        } catch (Exception e) {
            if (running.get()) {
                log.error("Subscriber loop error", e);
            }
        }
    }

    private void handleMessage(Message message) {
        if (!running.get()) return;
        MDC.put("subscriberIndex", "1");
        try {
            dispatchService.dispatch(message, ampsHaClient, ampsSemaphore);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("Interrupted during backpressure acquire messageId={}", message.getBookmark());
        } catch (Exception e) {
            log.error("handleMessage error messageId={}", message.getBookmark(), e);
        } finally {
            MDC.remove("subscriberIndex");
        }
    }

    /** Wait for all in-flight VTs to return their semaphore permits. */
    private void drainInFlight() {
        long deadline = System.currentTimeMillis() + shutdownTimeoutMs;
        while (ampsSemaphore.availablePermits() < maxConcurrency) {
            if (System.currentTimeMillis() > deadline) {
                log.warn("Drain timeout — {} VTs still in-flight",
                        maxConcurrency - ampsSemaphore.availablePermits());
                break;
            }
            try { Thread.sleep(50); } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    private void unsubscribeSilently() {
        try {
            if (subscriptionId != null) {
                ampsHaClient.unsubscribe(subscriptionId);
            }
        } catch (Exception e) {
            log.warn("Error during unsubscribe: {}", e.getMessage());
        }
    }
}
