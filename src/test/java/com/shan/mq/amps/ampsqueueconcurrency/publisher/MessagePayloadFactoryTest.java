package com.shan.mq.amps.ampsqueueconcurrency.publisher;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class MessagePayloadFactoryTest {

    private final MessagePayloadFactory factory = new MessagePayloadFactory();
    private final ObjectMapper mapper = new ObjectMapper();

    @BeforeEach
    void setUp() {
        ReflectionTestUtils.setField(factory, "symbols", List.of("AAPL", "MSFT", "GOOGL"));
        ReflectionTestUtils.setField(factory, "customTemplate", "");
    }

    // ── TRADE template ────────────────────────────────────────────────────────

    @Test
    void tradeTemplate_producesValidJson_withRequiredFields() throws Exception {
        String payload = factory.generate(PayloadTemplate.TRADE, 1L);

        JsonNode node = mapper.readTree(payload);
        assertThat(node.has("id")).isTrue();
        assertThat(node.has("seq")).isTrue();
        assertThat(node.has("symbol")).isTrue();
        assertThat(node.has("price")).isTrue();
        assertThat(node.has("quantity")).isTrue();
        assertThat(node.has("side")).isTrue();
        assertThat(node.has("timestamp")).isTrue();
        assertThat(node.get("seq").asLong()).isEqualTo(1L);
        assertThat(List.of("BUY", "SELL")).contains(node.get("side").asText());
        assertThat(List.of("AAPL", "MSFT", "GOOGL")).contains(node.get("symbol").asText());
    }

    // ── ORDER template ────────────────────────────────────────────────────────

    @Test
    void orderTemplate_producesValidJson_withRequiredFields() throws Exception {
        String payload = factory.generate(PayloadTemplate.ORDER, 42L);

        JsonNode node = mapper.readTree(payload);
        assertThat(node.has("orderId")).isTrue();
        assertThat(node.has("clientId")).isTrue();
        assertThat(node.has("type")).isTrue();
        assertThat(node.has("status")).isTrue();
        assertThat(node.get("seq").asLong()).isEqualTo(42L);
    }

    // ── RISK template ─────────────────────────────────────────────────────────

    @Test
    void riskTemplate_producesValidJson_withRequiredFields() throws Exception {
        String payload = factory.generate(PayloadTemplate.RISK, 7L);

        JsonNode node = mapper.readTree(payload);
        assertThat(node.has("portfolioId")).isTrue();
        assertThat(node.has("exposure")).isTrue();
        assertThat(node.has("var95")).isTrue();
        assertThat(node.has("pnl")).isTrue();
        assertThat(node.get("seq").asLong()).isEqualTo(7L);
    }

    // ── CUSTOM: blank template falls back to minimal JSON ─────────────────────

    @Test
    void customTemplate_blankConfig_producesFallbackJson() throws Exception {
        String payload = factory.generate(PayloadTemplate.CUSTOM, 99L);

        JsonNode node = mapper.readTree(payload);
        assertThat(node.has("id")).isTrue();
        assertThat(node.get("seq").asLong()).isEqualTo(99L);
    }

    // ── CUSTOM: with substitution tokens ─────────────────────────────────────

    @Test
    void customTemplate_withTokens_substitutesCorrectly() throws Exception {
        ReflectionTestUtils.setField(factory, "customTemplate",
                "{\"id\":\"${id}\",\"seq\":${seq},\"ts\":\"${timestamp}\"}");

        String payload = factory.generate(PayloadTemplate.CUSTOM, 5L);

        JsonNode node = mapper.readTree(payload);
        assertThat(node.has("id")).isTrue();
        assertThat(node.get("seq").asLong()).isEqualTo(5L);
        assertThat(node.has("ts")).isTrue();
    }

    // ── Sequence is correct per template ─────────────────────────────────────

    @Test
    void allTemplates_embedCorrectSequenceNumber() throws Exception {
        for (PayloadTemplate tpl : PayloadTemplate.values()) {
            String payload = factory.generate(tpl, 123L);
            JsonNode node  = mapper.readTree(payload);
            assertThat(node.get("seq").asLong())
                    .as("template %s should embed seq=123", tpl)
                    .isEqualTo(123L);
        }
    }
}
