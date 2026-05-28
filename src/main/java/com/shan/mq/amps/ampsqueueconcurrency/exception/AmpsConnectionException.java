package com.shan.mq.amps.ampsqueueconcurrency.exception;

/**
 * Thrown when the AMPS HAClient cannot establish or maintain a TCP connection.
 * This is a non-retryable application startup failure — the application should
 * not attempt to receive messages if it cannot connect.
 */
public class AmpsConnectionException extends RuntimeException {

    public AmpsConnectionException(String message) {
        super(message);
    }

    public AmpsConnectionException(String message, Throwable cause) {
        super(message, cause);
    }
}
