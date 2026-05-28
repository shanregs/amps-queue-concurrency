package com.shan.mq.amps.ampsqueueconcurrency.model;

import com.crankuptheamps.client.HAClient;

import java.util.concurrent.Semaphore;

/**
 * Immutable bundle of all state that belongs to one subscriber connection.
 * <p>
 * Used by the {@code multi-subscriber} and {@code multi-jvm-subscriber} profiles
 * where N HAClients are created and each subscriber platform thread owns exactly
 * one context.
 * <p>
 * Using a record ensures that subscriber threads cannot accidentally share or
 * swap each other's client or semaphore references.
 *
 * @param haClient  the HAClient connection this subscriber receives from AND ACKs on.
 *                  AMPS requires ACK on the same connection that received the message.
 * @param semaphore this subscriber's independent backpressure gate.
 *                  Per-subscriber semaphore ensures one slow subscriber does not
 *                  starve the others.
 * @param index     1-based subscriber index, used for logging and thread naming.
 */
public record SubscriberContext(
        HAClient haClient,
        Semaphore semaphore,
        int index
) {}
