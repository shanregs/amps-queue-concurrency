package com.shan.mq.amps.ampsqueueconcurrency.publisher;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Micrometer metrics for the publisher module.
 *
 * Metrics exposed at /actuator/prometheus:
 *   amps_publisher_messages_published_total   — cumulative published count
 *   amps_publisher_messages_errors_total      — cumulative error count
 *   amps_publisher_active_workers             — current VT worker count
 *   amps_publisher_publish_duration_seconds   — per-publish latency histogram
 */
@Slf4j
@Component
@Profile("message-publisher")
public class PublisherMetrics {

    private final Counter publishedCounter;
    private final Counter errorCounter;
    private final Timer   publishTimer;
    private final AtomicLong activeWorkers = new AtomicLong(0);

    public PublisherMetrics(MeterRegistry registry) {
        publishedCounter = Counter.builder("amps.publisher.messages.published")
                .description("Total messages successfully published to AMPS")
                .register(registry);

        errorCounter = Counter.builder("amps.publisher.messages.errors")
                .description("Total publish failures")
                .register(registry);

        publishTimer = Timer.builder("amps.publisher.publish.duration")
                .description("Time to publish one message to AMPS")
                .publishPercentiles(0.5, 0.95, 0.99)
                .register(registry);

        registry.gauge("amps.publisher.active.workers", activeWorkers, AtomicLong::doubleValue);
    }

    public void recordPublished()               { publishedCounter.increment(); }
    public void recordError()                   { errorCounter.increment(); }
    public Timer publishTimer()                 { return publishTimer; }
    public void workerStarted()                 { activeWorkers.incrementAndGet(); }
    public void workerStopped()                 { activeWorkers.decrementAndGet(); }
    public long totalPublished()                { return (long) publishedCounter.count(); }
    public long totalErrors()                   { return (long) errorCounter.count(); }
}
