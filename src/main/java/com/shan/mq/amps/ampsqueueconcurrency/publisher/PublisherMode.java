package com.shan.mq.amps.ampsqueueconcurrency.publisher;

/**
 * Controls how AmpsMessagePublisher determines when to stop publishing.
 *
 * RATE_LIMITED  — publish indefinitely at up to N messages/second (token-bucket gate)
 * BURST         — publish as fast as the AMPS connection allows; no rate limit
 * FIXED_COUNT   — publish exactly {@code amps.publisher.total-messages} then stop
 * INFINITE      — alias for RATE_LIMITED with no total-messages cap
 */
public enum PublisherMode {
    RATE_LIMITED,
    BURST,
    FIXED_COUNT,
    INFINITE
}
