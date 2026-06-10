package com.shan.mq.amps.ampsqueueconcurrency.service;

import com.crankuptheamps.client.HAClient;
import com.crankuptheamps.client.Message;
import com.shan.mq.amps.ampsqueueconcurrency.model.ProcessingResult;
import com.shan.mq.amps.ampsqueueconcurrency.processor.MessageProcessor;
import com.shan.mq.amps.ampsqueueconcurrency.subscriber.SubscriberMetrics;
import io.micrometer.core.instrument.Timer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.stereotype.Service;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;

/**
 * Fan-out gate between the AMPS subscriber platform thread and virtual-thread workers.
 *
 * Flow per message:
 *   1. semaphore.acquire()          — blocks subscriber thread when VTs are saturated (backpressure)
 *   2. subscriberMetrics.vtStarted()— increment active-worker gauge
 *   3. executor.submit()            — O(1), spawns one virtual thread
 *   4. VT: processor.process()      — @Transactional, idempotency, business logic
 *   5. VT: metrics.recordResult()   — counter by result tag
 *   6. VT: haClient.ack()           — only on OK | DUPLICATE | DISCARD
 *   7. VT finally: vtStopped() + semaphore.release() — unblocks subscriber thread
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class MessageDispatchService {

    private final MessageProcessor    processor;
    private final ExecutorService     ampsVirtualThreadExecutor;
    private final SubscriberMetrics   subscriberMetrics;

    public void dispatch(Message message, HAClient haClient, Semaphore semaphore)
            throws InterruptedException {

        semaphore.acquire();   // block subscriber thread — backpressure gate

        final String messageId = message.getBookmark();
        final String topic     = message.getTopic();

        subscriberMetrics.vtStarted();
        ampsVirtualThreadExecutor.submit(() -> {
            MDC.put("messageId", messageId);
            MDC.put("topic",     topic);
            Timer.Sample sample = Timer.start();
            try {
                ProcessingResult result = processor.process(message);
                subscriberMetrics.recordResult(result);

                if (result != ProcessingResult.FAIL) {
//                    haClient.ack(message);
                    message.ack();
                    log.debug("ACK result={} messageId={}", result, messageId);
                }
                // FAIL → no ACK → lease expires → AMPS re-delivers after TTL

            } catch (Exception e) {
                log.error("Unhandled dispatch error, no ACK issued messageId={}", messageId, e);
            } finally {
                sample.stop(subscriberMetrics.processingTimer());
                subscriberMetrics.vtStopped();
                semaphore.release();
                MDC.clear();
            }
        });
    }
}
