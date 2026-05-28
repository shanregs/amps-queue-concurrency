package com.shan.mq.amps.ampsqueueconcurrency.model;

import jakarta.persistence.*;
import lombok.*;

import java.time.Instant;

/**
 * JPA entity representing one AMPS queue message that has been received.
 * <p>
 * {@code messageId} = AMPS bookmark — globally unique per queue message.
 * The UNIQUE constraint on this column is what makes the system idempotent:
 * if AMPS re-delivers a message (lease expiry, JVM crash), the second INSERT
 * raises a {@code DataIntegrityViolationException} which is caught and treated
 * as a duplicate.
 * <p>
 * Mutable fields ({@code status}, {@code retryCount}, {@code lastError},
 * {@code processedAt}, {@code lastAttemptAt}) are updated during retry cycles.
 */
@Entity
@Table(
    name = "processed_messages",
    indexes = {
        @Index(name = "idx_pm_message_id",   columnList = "message_id",   unique = true),
        @Index(name = "idx_pm_status",        columnList = "status"),
        @Index(name = "idx_pm_received_at",   columnList = "received_at"),
        @Index(name = "idx_pm_processed_by",  columnList = "processed_by")
    }
)
@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ProcessedMessage {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    /** AMPS bookmark — idempotency key. Immutable after first delivery. */
    @Column(name = "message_id", unique = true, nullable = false, length = 512)
    private String messageId;

    @Column(name = "topic", length = 255)
    private String topic;

    @Lob
    @Column(name = "payload", columnDefinition = "TEXT")
    private String payload;

    /** PROCESSED | FAILED | DISCARDED. Updated on retry. */
    @Setter
    @Enumerated(EnumType.STRING)
    @Column(name = "status", length = 20, nullable = false)
    private ProcessingStatus status;

    /** Number of failed processing attempts so far. */
    @Setter
    @Builder.Default
    @Column(name = "retry_count", nullable = false)
    private int retryCount = 0;

    /** Last exception message. Populated only on failure. */
    @Setter
    @Column(name = "last_error", columnDefinition = "TEXT")
    private String lastError;

    /** Wall-clock time when AMPS first delivered this message to this JVM. */
    @Column(name = "received_at", nullable = false)
    private Instant receivedAt;

    /** Wall-clock time of the most recent successful processing completion. */
    @Setter
    @Column(name = "processed_at")
    private Instant processedAt;

    /** Wall-clock time of the most recent attempt (success or failure). */
    @Setter
    @Column(name = "last_attempt_at")
    private Instant lastAttemptAt;

    /**
     * Hostname of the JVM that processed this message.
     * Used for debugging cross-JVM re-delivery in the multi-jvm-subscriber profile.
     */
    @Column(name = "processed_by", length = 255)
    private String processedBy;
}
