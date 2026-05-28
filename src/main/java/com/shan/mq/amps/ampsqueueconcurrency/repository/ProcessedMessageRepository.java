package com.shan.mq.amps.ampsqueueconcurrency.repository;

import com.shan.mq.amps.ampsqueueconcurrency.model.ProcessedMessage;
import com.shan.mq.amps.ampsqueueconcurrency.model.ProcessingStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

@Repository
public interface ProcessedMessageRepository extends JpaRepository<ProcessedMessage, Long> {

    /** Idempotency check — exists before attempting INSERT. */
    Optional<ProcessedMessage> findByMessageId(String messageId);

    /** Exists shortcut used in idempotency assertions. */
    boolean existsByMessageId(String messageId);

    /** Find all messages with a given status (e.g. FAILED, for monitoring). */
    List<ProcessedMessage> findByStatus(ProcessingStatus status);

    /** Per-node throughput — count messages processed by a specific JVM. */
    List<ProcessedMessage> findByProcessedBy(String hostname);

    /** Total messages processed after a given timestamp. */
    long countByReceivedAtAfter(Instant since);

    /** Count successfully processed messages since a given time (metrics). */
    @Query("""
           SELECT COUNT(pm) FROM ProcessedMessage pm
           WHERE pm.receivedAt >= :since
             AND pm.status = 'PROCESSED'
           """)
    long countProcessedSince(@Param("since") Instant since);

    /** Per-JVM throughput breakdown — useful for multi-JVM observability. */
    @Query("""
           SELECT pm.processedBy, COUNT(pm)
           FROM ProcessedMessage pm
           WHERE pm.receivedAt >= :since
           GROUP BY pm.processedBy
           ORDER BY COUNT(pm) DESC
           """)
    List<Object[]> throughputByNode(@Param("since") Instant since);

    /** Find messages that were retried more than N times (potential poison messages). */
    @Query("""
           SELECT pm FROM ProcessedMessage pm
           WHERE pm.retryCount >= :threshold
           ORDER BY pm.retryCount DESC
           """)
    List<ProcessedMessage> findHighRetryMessages(@Param("threshold") int threshold);
}
