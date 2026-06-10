package com.shan.mq.amps.ampsqueueconcurrency.subscriber;

import com.shan.mq.amps.ampsqueueconcurrency.model.ProcessingResult;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Micrometer metrics for all subscriber profiles.
 *
 * Metrics exposed at /actuator/prometheus:
 *   amps_subscriber_messages_total{result="OK|DUPLICATE|DISCARD|FAIL"}  — cumulative outcome counts
 *   amps_subscriber_active_vt_workers                                   — virtual threads currently processing
 *   amps_subscriber_processing_duration_seconds                         — per-message processing histogram
 */
@Component
public class SubscriberMetrics {

    private final Map<ProcessingResult, Counter> resultCounters = new EnumMap<>(ProcessingResult.class);
    private final Timer processingTimer;
    private final AtomicLong activeVtWorkers = new AtomicLong(0);

    public SubscriberMetrics(MeterRegistry registry) {
        for (ProcessingResult r : ProcessingResult.values()) {
            resultCounters.put(r, Counter.builder("amps.subscriber.messages")
                    .tag("result", r.name())
                    .description("Subscriber message outcomes by result")
                    .register(registry));
        }

        processingTimer = Timer.builder("amps.subscriber.processing.duration")
                .description("Time to process one AMPS message in a virtual thread")
                .publishPercentiles(0.5, 0.95, 0.99)
                .register(registry);

        registry.gauge("amps.subscriber.active.vt.workers", activeVtWorkers, AtomicLong::doubleValue);
    }

    public void recordResult(ProcessingResult result) { resultCounters.get(result).increment(); }
    public Timer processingTimer()                    { return processingTimer; }
    public void vtStarted()                           { activeVtWorkers.incrementAndGet(); }
    public void vtStopped()                           { activeVtWorkers.decrementAndGet(); }
}
