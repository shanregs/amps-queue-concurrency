package com.shan.mq.amps.ampsqueueconcurrency.processor;

import com.crankuptheamps.client.Message;
import com.shan.mq.amps.ampsqueueconcurrency.model.ProcessedMessage;
import com.shan.mq.amps.ampsqueueconcurrency.model.ProcessingResult;
import com.shan.mq.amps.ampsqueueconcurrency.model.ProcessingStatus;
import com.shan.mq.amps.ampsqueueconcurrency.repository.ProcessedMessageRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.net.InetAddress;
import java.time.Instant;
import java.util.Optional;

@Slf4j
@Service
@RequiredArgsConstructor
public class MessageProcessor {

    private final ProcessedMessageRepository repository;

    @Value("${amps.consumer.max-retries:3}")
    private int maxRetries;

    private static final String HOSTNAME = resolveHostname();

    /**
     * Core processing entry point called from a virtual thread per message.
     *
     * OK        — new message processed successfully → caller ACKs
     * DUPLICATE — already in DB (PROCESSED or DISCARDED) → caller ACKs
     * DISCARD   — maxRetries exceeded → caller ACKs with SEVERE log
     * FAIL      — transient failure → caller does NOT ACK; AMPS re-delivers after lease TTL
     */
    @Transactional
    public ProcessingResult process(Message message) {
        String messageId = message.getBookmark();
        String topic     = message.getTopic();
        String payload   = message.getData();
        Instant now      = Instant.now();

        Optional<ProcessedMessage> existing = repository.findByMessageId(messageId);

        if (existing.isPresent()) {
            return handleExisting(existing.get(), payload, now);
        }
        return handleFirstDelivery(messageId, topic, payload, now);
    }

    // ── First delivery ────────────────────────────────────────────────────────

    private ProcessingResult handleFirstDelivery(String messageId, String topic,
                                                  String payload, Instant now) {
        try {
            executeBusinessLogic(payload);

            repository.save(ProcessedMessage.builder()
                    .messageId(messageId)
                    .topic(topic)
                    .payload(payload)
                    .status(ProcessingStatus.PROCESSED)
                    .receivedAt(now)
                    .processedAt(now)
                    .lastAttemptAt(now)
                    .processedBy(HOSTNAME)
                    .build());

            log.info("OK messageId={} topic={}", messageId, topic);
            return ProcessingResult.OK;

        } catch (DataIntegrityViolationException e) {
            // Concurrent VT inserted the same bookmark — race on first delivery
            log.debug("DUPLICATE (concurrent insert) messageId={}", messageId);
            return ProcessingResult.DUPLICATE;

        } catch (Exception e) {
            log.warn("FAIL first delivery messageId={} error={}", messageId, e.getMessage());
            saveFailedRecord(messageId, topic, payload, 1, e.getMessage(), now);
            return ProcessingResult.FAIL;
        }
    }

    // ── Retry path ────────────────────────────────────────────────────────────

    private ProcessingResult handleExisting(ProcessedMessage pm, String payload, Instant now) {
        ProcessingStatus status = pm.getStatus();

        if (status == ProcessingStatus.PROCESSED || status == ProcessingStatus.DISCARDED) {
            log.debug("DUPLICATE messageId={} status={}", pm.getMessageId(), status);
            return ProcessingResult.DUPLICATE;
        }

        // FAILED → this is a retry attempt
        int newRetryCount = pm.getRetryCount() + 1;

        try {
            executeBusinessLogic(payload);

            pm.setStatus(ProcessingStatus.PROCESSED);
            pm.setProcessedAt(now);
            pm.setLastAttemptAt(now);
            pm.setLastError(null);
            repository.save(pm);

            log.info("OK retry={} messageId={}", newRetryCount, pm.getMessageId());
            return ProcessingResult.OK;

        } catch (Exception e) {
            pm.setRetryCount(newRetryCount);
            pm.setLastError(truncate(e.getMessage()));
            pm.setLastAttemptAt(now);

            if (newRetryCount >= maxRetries) {
                pm.setStatus(ProcessingStatus.DISCARDED);
                repository.save(pm);
                log.error("DISCARD after {} retries messageId={}", newRetryCount, pm.getMessageId());
                return ProcessingResult.DISCARD;
            }

            pm.setStatus(ProcessingStatus.FAILED);
            repository.save(pm);
            log.warn("FAIL retry={} messageId={} error={}", newRetryCount, pm.getMessageId(), e.getMessage());
            return ProcessingResult.FAIL;
        }
    }

    // ── Business logic placeholder ────────────────────────────────────────────

    /**
     * Replace with real domain logic: deserialize JSON, validate, call downstream.
     * Any exception triggers FAIL → AMPS re-delivers after lease TTL.
     */
    protected void executeBusinessLogic(String payload) {
        if (payload == null || payload.isBlank()) {
            throw new IllegalArgumentException("Empty payload");
        }
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private void saveFailedRecord(String messageId, String topic, String payload,
                                   int retryCount, String error, Instant now) {
        repository.save(ProcessedMessage.builder()
                .messageId(messageId)
                .topic(topic)
                .payload(payload)
                .status(ProcessingStatus.FAILED)
                .retryCount(retryCount)
                .lastError(truncate(error))
                .receivedAt(now)
                .lastAttemptAt(now)
                .processedBy(HOSTNAME)
                .build());
    }

    private static String truncate(String s) {
        return (s != null && s.length() > 500) ? s.substring(0, 500) : s;
    }

    private static String resolveHostname() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            return "unknown";
        }
    }
}
