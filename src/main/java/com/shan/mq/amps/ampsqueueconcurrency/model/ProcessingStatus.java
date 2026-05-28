package com.shan.mq.amps.ampsqueueconcurrency.model;

/**
 * Lifecycle state of a message in the {@code processed_messages} table.
 *
 * <pre>
 *  (new delivery)  ──▶  PROCESSED   success on first or retry attempt
 *  (new delivery)  ──▶  FAILED      business logic threw an exception; will retry
 *  FAILED          ──▶  PROCESSED   succeeded on a subsequent retry
 *  FAILED          ──▶  DISCARDED   max-retries exceeded; ACK'd, removed from queue
 * </pre>
 */
public enum ProcessingStatus {
    PROCESSED,
    FAILED,
    DISCARDED
}
