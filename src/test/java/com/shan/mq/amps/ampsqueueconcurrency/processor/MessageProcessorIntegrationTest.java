package com.shan.mq.amps.ampsqueueconcurrency.processor;

import com.crankuptheamps.client.Message;
import com.shan.mq.amps.ampsqueueconcurrency.model.ProcessingResult;
import com.shan.mq.amps.ampsqueueconcurrency.model.ProcessingStatus;
import com.shan.mq.amps.ampsqueueconcurrency.repository.ProcessedMessageRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Integration test: real H2 database, real MessageProcessor, no AMPS connection.
 *
 * Profile "test" activates application-test.yaml (H2 in-memory).
 * No subscriber or publisher profile → no HAClient bean → no AMPS connection attempt.
 */
@SpringBootTest
@ActiveProfiles("test")
class MessageProcessorIntegrationTest {

    @Autowired
    private MessageProcessor processor;

    @Autowired
    private ProcessedMessageRepository repository;

    @BeforeEach
    void clearDb() {
        repository.deleteAll();
    }

    // ── OK: first delivery, persisted correctly ───────────────────────────────

    @Test
    void firstDelivery_validPayload_persistsProcessedRecord() {
        Message msg = message("int-bm-001", "/queue/trades", "{\"id\":\"1\",\"symbol\":\"AAPL\"}");

        ProcessingResult result = processor.process(msg);

        assertThat(result).isEqualTo(ProcessingResult.OK);
        assertThat(repository.existsByMessageId("int-bm-001")).isTrue();
        assertThat(repository.findByMessageId("int-bm-001"))
                .isPresent()
                .hasValueSatisfying(pm -> {
                    assertThat(pm.getStatus()).isEqualTo(ProcessingStatus.PROCESSED);
                    assertThat(pm.getRetryCount()).isEqualTo(0);
                    assertThat(pm.getProcessedAt()).isNotNull();
                    assertThat(pm.getProcessedBy()).isNotBlank();
                });
    }

    // ── DUPLICATE: second delivery of same bookmark ───────────────────────────

    @Test
    void secondDelivery_sameBenchmark_returnsDuplicate_noExtraRow() {
        Message msg = message("int-bm-002", "/queue/trades", "{\"id\":\"2\",\"symbol\":\"MSFT\"}");

        ProcessingResult first  = processor.process(msg);
        ProcessingResult second = processor.process(msg);

        assertThat(first).isEqualTo(ProcessingResult.OK);
        assertThat(second).isEqualTo(ProcessingResult.DUPLICATE);
        assertThat(repository.findByMessageId("int-bm-002")).isPresent();
        assertThat(repository.count()).isEqualTo(1);
    }

    // ── FAIL then OK on retry ─────────────────────────────────────────────────

    @Test
    void firstDeliveryFails_retryWithGoodPayload_returnsOK() {
        // First delivery: blank → FAIL
        Message badMsg  = message("int-bm-003", "/queue/trades", " ");
        ProcessingResult fail = processor.process(badMsg);
        assertThat(fail).isEqualTo(ProcessingResult.FAIL);

        // Retry: valid payload → OK
        Message goodMsg = message("int-bm-003", "/queue/trades", "{\"id\":\"3\"}");
        ProcessingResult ok = processor.process(goodMsg);
        assertThat(ok).isEqualTo(ProcessingResult.OK);

        assertThat(repository.findByMessageId("int-bm-003"))
                .isPresent()
                .hasValueSatisfying(pm -> assertThat(pm.getStatus()).isEqualTo(ProcessingStatus.PROCESSED));
    }

    // ── DISCARD: 3 consecutive failures ──────────────────────────────────────

    @Test
    void threeConsecutiveFailures_resultsInDiscard() {
        // maxRetries=3 per application-test.yaml
        Message msg = message("int-bm-004", "/queue/trades", " "); // always blank → always fails

        ProcessingResult r1 = processor.process(msg); // retryCount=1 → FAIL
        ProcessingResult r2 = processor.process(msg); // retryCount=2 → FAIL
        ProcessingResult r3 = processor.process(msg); // retryCount=3 → DISCARD

        assertThat(r1).isEqualTo(ProcessingResult.FAIL);
        assertThat(r2).isEqualTo(ProcessingResult.FAIL);
        assertThat(r3).isEqualTo(ProcessingResult.DISCARD);

        assertThat(repository.findByMessageId("int-bm-004"))
                .isPresent()
                .hasValueSatisfying(pm -> {
                    assertThat(pm.getStatus()).isEqualTo(ProcessingStatus.DISCARDED);
                    assertThat(pm.getRetryCount()).isEqualTo(3);
                });
    }

    // ── countProcessedSince query ─────────────────────────────────────────────

    @Test
    void countProcessedSince_returnsCorrectCount() {
        java.time.Instant before = java.time.Instant.now().minusSeconds(1);

        processor.process(message("int-bm-010", "/queue/trades", "{\"id\":\"10\"}"));
        processor.process(message("int-bm-011", "/queue/trades", "{\"id\":\"11\"}"));
        processor.process(message("int-bm-012", "/queue/trades", " "));  // FAIL, not counted

        long count = repository.countProcessedSince(before);
        assertThat(count).isEqualTo(2);
    }

    // ── Helper ────────────────────────────────────────────────────────────────

    private static Message message(String bookmark, String topic, String data) {
        Message m = mock(Message.class);
        when(m.getBookmark()).thenReturn(bookmark);
        when(m.getTopic()).thenReturn(topic);
        when(m.getData()).thenReturn(data);
        return m;
    }
}
