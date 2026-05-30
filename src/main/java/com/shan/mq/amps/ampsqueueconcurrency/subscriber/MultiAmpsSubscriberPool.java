package com.shan.mq.amps.ampsqueueconcurrency.subscriber;

import com.crankuptheamps.client.CommandId;
import com.crankuptheamps.client.Message;
import com.shan.mq.amps.ampsqueueconcurrency.model.SubscriberContext;
import com.shan.mq.amps.ampsqueueconcurrency.service.MessageDispatchService;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;


import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Multi-subscriber pool for the {@code multi-subscriber} and
 * {@code multi-jvm-subscriber} profiles.
 *
 * Creates one platform thread per {@link SubscriberContext}. Each thread subscribes
 * independently to the same AMPS queue topic — AMPS distributes messages across all
 * subscribers (competing consumers). Per-subscriber semaphores ensure one slow
 * subscriber cannot starve the others.
 */
@Slf4j
@Component
@Profile({"multi-subscriber", "multi-jvm-subscriber"})
@RequiredArgsConstructor
public class MultiAmpsSubscriberPool implements SmartLifecycle {

    private final List<SubscriberContext> subscriberContexts;
    private final MessageDispatchService dispatchService;

    @Value("${amps.queue.topic:/queue/trades}")
    private String topic;

    @Value("${amps.queue.filter:}")
    private String filter;

    @Value("${amps.consumer.max-concurrency-per-subscriber:50}")
    private int maxConcurrencyPerSubscriber;

    @Value("${amps.consumer.shutdown-timeout-ms:30000}")
    private long shutdownTimeoutMs;

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final List<Thread> subscriberThreads = new ArrayList<>();
    private final ConcurrentMap<Integer, CommandId> subscriptionIds = new ConcurrentHashMap<>();

    // ── SmartLifecycle ────────────────────────────────────────────────────────

    @Override
    public void start() {
        running.set(true);
        for (SubscriberContext ctx : subscriberContexts) {
            Thread t = Thread.ofPlatform()
                    .name("amps-subscriber-" + ctx.index())
                    .start(() -> runSubscribeLoop(ctx));
            subscriberThreads.add(t);
        }
        log.info("MultiAmpsSubscriberPool started: {} subscribers topic={}", subscriberContexts.size(), topic);
    }

    @Override
    public void stop() {
        running.set(false);
        unsubscribeAll();
        subscriberThreads.forEach(Thread::interrupt);
        drainAll();
        log.info("MultiAmpsSubscriberPool stopped");
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

    private void runSubscribeLoop(SubscriberContext ctx) {
        try {
            String effectiveFilter = (filter == null || filter.isBlank()) ? null : filter;
            CommandId subId = effectiveFilter != null
                    ? ctx.haClient().subscribe(msg -> handleMessage(msg, ctx), topic, effectiveFilter, 5000L)
                    : ctx.haClient().subscribe(msg -> handleMessage(msg, ctx), topic, 5000L);

            subscriptionIds.put(ctx.index(), subId);
            log.info("Subscriber #{} active subId={}", ctx.index(), subId);

            while (running.get()) {
                Thread.sleep(500);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.debug("Subscriber #{} thread interrupted", ctx.index());
        } catch (Exception e) {
            if (running.get()) {
                log.error("Subscriber #{} loop error", ctx.index(), e);
            }
        }
    }

    private void handleMessage(Message message, SubscriberContext ctx) {
        if (!running.get()) return;
        MDC.put("subscriberIndex", String.valueOf(ctx.index()));
        try {
            dispatchService.dispatch(message, ctx.haClient(), ctx.semaphore());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("Subscriber #{} interrupted on backpressure acquire messageId={}",
                    ctx.index(), message.getBookmark());
        } catch (Exception e) {
            log.error("Subscriber #{} handleMessage error messageId={}",
                    ctx.index(), message.getBookmark(), e);
        } finally {
            MDC.remove("subscriberIndex");
        }
    }

    private void unsubscribeAll() {
        for (SubscriberContext ctx : subscriberContexts) {
            CommandId subId = subscriptionIds.get(ctx.index());
            if (subId != null) {
                try {
                    ctx.haClient().unsubscribe(subId);
                } catch (Exception e) {
                    log.warn("Subscriber #{} unsubscribe error: {}", ctx.index(), e.getMessage());
                }
            }
        }
    }

    /** Wait for all in-flight VTs across all subscribers to drain. */
    private void drainAll() {
        long deadline = System.currentTimeMillis() + shutdownTimeoutMs;
        for (SubscriberContext ctx : subscriberContexts) {
            while (ctx.semaphore().availablePermits() < maxConcurrencyPerSubscriber) {
                if (System.currentTimeMillis() > deadline) {
                    log.warn("Subscriber #{} drain timeout — {} VTs still in-flight",
                            ctx.index(),
                            maxConcurrencyPerSubscriber - ctx.semaphore().availablePermits());
                    break;
                }
                try { Thread.sleep(50); } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
    }
}
