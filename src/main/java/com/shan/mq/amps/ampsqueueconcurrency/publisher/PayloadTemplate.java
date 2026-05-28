package com.shan.mq.amps.ampsqueueconcurrency.publisher;

/**
 * Determines which JSON payload shape {@link MessagePayloadFactory} generates.
 *
 * TRADE   — equity trade execution (id, symbol, price, quantity, side, timestamp)
 * ORDER   — limit/market order (id, orderId, clientId, type, price, quantity, status)
 * RISK    — portfolio risk snapshot (id, portfolioId, exposure, var95, timestamp)
 * CUSTOM  — raw passthrough of {@code amps.publisher.custom-payload-template}
 */
public enum PayloadTemplate {
    TRADE,
    ORDER,
    RISK,
    CUSTOM
}
