package com.shan.mq.amps.ampsqueueconcurrency.publisher;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Generates JSON payloads for the four {@link PayloadTemplate} shapes.
 * All methods are thread-safe (stateless except for the symbols list).
 */
@Slf4j
@Component
public class MessagePayloadFactory {

    @Value("${amps.publisher.symbols:AAPL,MSFT,GOOGL,AMZN,TSLA,META,NVDA,JPM}")
    private List<String> symbols;

    @Value("${amps.publisher.custom-payload-template:}")
    private String customTemplate;

    private static final String[] SIDES       = {"BUY", "SELL"};
    private static final String[] ORDER_TYPES = {"LIMIT", "MARKET", "STOP", "STOP_LIMIT"};
    private static final String[] ORDER_STATUSES = {"NEW", "PARTIALLY_FILLED", "ACCEPTED"};

    public String generate(PayloadTemplate template, long sequence) {
        return switch (template) {
            case TRADE  -> trade(sequence);
            case ORDER  -> order(sequence);
            case RISK   -> risk(sequence);
            case CUSTOM -> custom(sequence);
        };
    }

    // ── Templates ─────────────────────────────────────────────────────────────

    private String trade(long seq) {
        ThreadLocalRandom rng = ThreadLocalRandom.current();
        String symbol   = randomSymbol(rng);
        double price    = 10 + rng.nextDouble(990);
        int    quantity = rng.nextInt(1, 10_001);
        String side     = SIDES[rng.nextInt(SIDES.length)];
        return """
               {"id":"%s","seq":%d,"symbol":"%s","price":%.4f,"quantity":%d,\
               "side":"%s","timestamp":"%s"}""".formatted(
                UUID.randomUUID(), seq, symbol, price, quantity, side, Instant.now());
    }

    private String order(long seq) {
        ThreadLocalRandom rng = ThreadLocalRandom.current();
        String symbol   = randomSymbol(rng);
        double price    = 10 + rng.nextDouble(990);
        int    quantity = rng.nextInt(1, 5_001);
        String type     = ORDER_TYPES[rng.nextInt(ORDER_TYPES.length)];
        String status   = ORDER_STATUSES[rng.nextInt(ORDER_STATUSES.length)];
        return """
               {"id":"%s","seq":%d,"orderId":"%s","clientId":"CLIENT-%04d","symbol":"%s",\
               "type":"%s","price":%.4f,"quantity":%d,"status":"%s","timestamp":"%s"}""".formatted(
                UUID.randomUUID(), seq, UUID.randomUUID(), rng.nextInt(1, 1000),
                symbol, type, price, quantity, status, Instant.now());
    }

    private String risk(long seq) {
        ThreadLocalRandom rng = ThreadLocalRandom.current();
        double exposure = rng.nextDouble(1_000_000, 100_000_000);
        double var95    = exposure * rng.nextDouble(0.01, 0.08);
        double pnl      = rng.nextDouble(-500_000, 500_000);
        return """
               {"id":"%s","seq":%d,"portfolioId":"PORT-%04d","exposure":%.2f,\
               "var95":%.2f,"pnl":%.2f,"timestamp":"%s"}""".formatted(
                UUID.randomUUID(), seq, rng.nextInt(1, 500), exposure, var95, pnl, Instant.now());
    }

    private String custom(long seq) {
        if (customTemplate == null || customTemplate.isBlank()) {
            return """
                   {"id":"%s","seq":%d,"timestamp":"%s"}""".formatted(
                    UUID.randomUUID(), seq, Instant.now());
        }
        return customTemplate
                .replace("${id}",        UUID.randomUUID().toString())
                .replace("${seq}",       String.valueOf(seq))
                .replace("${timestamp}", Instant.now().toString());
    }

    private String randomSymbol(Random rng) {
        if (symbols == null || symbols.isEmpty()) return "UNKNOWN";
        return symbols.get(rng.nextInt(symbols.size()));
    }
}
