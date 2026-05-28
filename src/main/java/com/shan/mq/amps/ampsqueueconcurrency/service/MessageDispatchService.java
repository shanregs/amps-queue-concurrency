package com.shan.mq.amps.ampsqueueconcurrency.service;

import com.crankuptheamps.client.HAClient;
import com.crankuptheamps.client.Message;
import com.shan.mq.amps.ampsqueueconcurrency.model.ProcessingResult;
import com.shan.mq.amps.ampsqueueconcurrency.processor.MessageProcessor;
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
 *   1. semaphore.acquire()     — blocks subscriber thread when VTs are saturated (backpressure)
 *   2. executor.submit()       — O(1), spawns one virtual thread
 *   3. VT: processor.process() — @Transactional, idempotency, business logic
 *   4. VT: haClient.ack()      — only on OK | DUPLICATE | DISCARD
 *   5. VT: semaphore.release() — unblocks the subscriber thread for next message
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class MessageDispatchService {

    private final MessageProcessor processor;
    private final ExecutorService ampsVirtualThreadExecutor;

    public void dispatch(Message message, HAClient haClient, Semaphore semaphore)
            throws InterruptedException {

        semaphore.acquire();   // block subscriber thread — backpressure gate

        final String messageId = message.getBookmark();
        final String topic     = message.getTopic();

        ampsVirtualThreadExecutor.submit(() -> {
            MDC.put("messageId", messageId);
            MDC.put("topic",     topic);
            try {
                ProcessingResult result = processor.process(message);

                if (result != ProcessingResult.FAIL) {
                    haClient.ack(message);
                    log.debug("ACK result={} messageId={}", result, messageId);
                }
                // FAIL → no ACK → lease expires → AMPS re-delivers

            } catch (Exception e) {
                log.error("Unhandled dispatch error, no ACK issued messageId={}", messageId, e);
            } finally {
                semaphore.release();
                MDC.clear();
            }
        });
    }
}
