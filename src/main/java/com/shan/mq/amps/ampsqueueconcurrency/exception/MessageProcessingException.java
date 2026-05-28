package com.shan.mq.amps.ampsqueueconcurrency.exception;

/**
 * Thrown by {@link com.shan.mq.amps.ampsqueueconcurrency.processor.MessageProcessor}
 * when business logic fails and the message should NOT be ACK'd.
 * <p>
 * AMPS will re-deliver the message after the lease TTL expires.
 * Retry count is tracked in the {@code processed_messages} table.
 */
public class MessageProcessingException extends RuntimeException {

    public MessageProcessingException(String message) {
        super(message);
    }

    public MessageProcessingException(String message, Throwable cause) {
        super(message, cause);
    }
}
