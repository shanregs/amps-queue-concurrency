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
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Service
@RequiredArgsConstructor
public class MessageProcessor {

    private final ProcessedMessageRepository repository;

    @Value("${amps.consumer.max-retries:3}")
    private int maxRetries;

    // When false: retries are tracked in-memory only; no FAILED rows written to DB.
    // In both modes, exhausted retries → log ERROR + DISCARD (ACK, move to next message).
    // Keep true for multi-jvm-subscriber: in-memory counters don't survive restarts or cross-node re-delivery.
    @Value("${amps.consumer.retry-db-tracking-enabled:true}")
    private boolean dbTrackingEnabled;

    // Used only when dbTrackingEnabled=false.
    // Key: messageId, value: failed attempt count in this JVM instance.
    // Entry is removed on success or final discard.
    private final ConcurrentHashMap<String, Integer> inMemoryRetryCount = new ConcurrentHashMap<>();

    private static final String HOSTNAME = resolveHostname();

    /**
     * Core processing entry point called from a virtual thread per message.
     *
     * OK        — message processed successfully → caller ACKs
     * DUPLICATE — already in DB (PROCESSED or DISCARDED) → caller ACKs
     * DISCARD   — retries exhausted → caller ACKs, logs ERROR
     * FAIL      — transient failure → caller does NOT ACK; AMPS re-delivers after lease TTL
     */
    @Transactional
    public ProcessingResult process(Message message) {
        String messageId = message.getBookmark();
        String topic     = message.getTopic();
        String payload   = message.getData();
        Instant now      = Instant.now();

        return dbTrackingEnabled
                ? processWithDbTracking(messageId, topic, payload, now)
                : processWithoutDbTracking(messageId, topic, payload, now);
    }

    // ── DB-tracked path (retry-db-tracking-enabled=true) ─────────────────────
    // FAILED rows are written to DB; retry count and last error are persisted.
    // Safe for multi-jvm deployments.

    private ProcessingResult processWithDbTracking(String messageId, String topic,
                                                    String payload, Instant now) {
        Optional<ProcessedMessage> existing = repository.findByMessageId(messageId);
        if (existing.isPresent()) {
            return handleExisting(existing.get(), payload, now);
        }
        return handleFirstDelivery(messageId, topic, payload, now);
    }

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
                log.error("DISCARD messageId={} after {} retries — moving to next message",
                        pm.getMessageId(), newRetryCount);
                return ProcessingResult.DISCARD;
            }

            pm.setStatus(ProcessingStatus.FAILED);
            repository.save(pm);
            log.warn("FAIL retry={} messageId={} error={}", newRetryCount, pm.getMessageId(), e.getMessage());
            return ProcessingResult.FAIL;
        }
    }

    // ── In-memory retry path (retry-db-tracking-enabled=false) ───────────────
    // No FAILED rows written to DB; retry count lives in inMemoryRetryCount.
    // PROCESSED rows are still written for idempotency.
    // Not safe for multi-jvm: counter resets on JVM restart or cross-node re-delivery.

    private ProcessingResult processWithoutDbTracking(String messageId, String topic,
                                                       String payload, Instant now) {
        Optional<ProcessedMessage> existing = repository.findByMessageId(messageId);
        if (existing.isPresent()) {
            log.debug("DUPLICATE messageId={} status={}", messageId, existing.get().getStatus());
            return ProcessingResult.DUPLICATE;
        }

        int attempt = inMemoryRetryCount.merge(messageId, 1, Integer::sum);
        try {
            executeBusinessLogic(payload);

            inMemoryRetryCount.remove(messageId);
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

            log.info("OK attempt={} messageId={} topic={}", attempt, messageId, topic);
            return ProcessingResult.OK;

        } catch (DataIntegrityViolationException e) {
            inMemoryRetryCount.remove(messageId);
            log.debug("DUPLICATE (concurrent insert) messageId={}", messageId);
            return ProcessingResult.DUPLICATE;

        } catch (Exception e) {
            if (attempt >= maxRetries) {
                inMemoryRetryCount.remove(messageId);
                log.error("DISCARD messageId={} after {} attempts — moving to next message: {}",
                        messageId, attempt, e.getMessage());
                return ProcessingResult.DISCARD;
            }
            log.warn("FAIL attempt={} messageId={} error={}", attempt, messageId, e.getMessage());
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
