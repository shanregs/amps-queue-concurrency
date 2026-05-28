package com.shan.mq.amps.ampsqueueconcurrency.model;

/**
 * Return value from {@link com.shan.mq.amps.ampsqueueconcurrency.processor.MessageProcessor#process}.
 * Tells the dispatch service whether and how to ACK the AMPS message.
 *
 * <pre>
 *  OK        → ACK  (new message, successfully processed)
 *  DUPLICATE → ACK  (already stored; idempotent)
 *  DISCARD   → ACK  (max retries exceeded; removed from queue with severe log)
 *  FAIL      → NO ACK (lease expires; AMPS re-delivers after TTL)
 * </pre>
 */
public enum ProcessingResult {
    OK,
    DUPLICATE,
    DISCARD,
    FAIL
}
