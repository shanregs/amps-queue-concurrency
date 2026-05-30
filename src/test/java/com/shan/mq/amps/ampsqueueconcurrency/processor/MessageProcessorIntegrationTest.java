package com.shan.mq.amps.ampsqueueconcurrency.processor;

import com.crankuptheamps.client.Message;
import com.shan.mq.amps.ampsqueueconcurrency.model.ProcessingResult;
import com.shan.mq.amps.ampsqueueconcurrency.model.ProcessingStatus;
import com.shan.mq.amps.ampsqueueconcurrency.repository.ProcessedMessageRepository;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.concurrent.ConcurrentHashMap;

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

    // ══════════════════════════════════════════════════════════════════════════
    // retry-db-tracking-enabled=true  (default, configured in application-test.yaml)
    // ══════════════════════════════════════════════════════════════════════════

    @Nested
    class WithDbTracking {

        @BeforeEach
        void enableDbTracking() {
            ReflectionTestUtils.setField(processor, "dbTrackingEnabled", true);
        }

        @Test
        void firstDelivery_validPayload_persistsProcessedRecord() {
            Message msg = message("int-bm-001", "/queue/trades", "{\"id\":\"1\",\"symbol\":\"AAPL\"}");

            ProcessingResult result = processor.process(msg);

            assertThat(result).isEqualTo(ProcessingResult.OK);
            assertThat(repository.findByMessageId("int-bm-001"))
                    .isPresent()
                    .hasValueSatisfying(pm -> {
                        assertThat(pm.getStatus()).isEqualTo(ProcessingStatus.PROCESSED);
                        assertThat(pm.getRetryCount()).isEqualTo(0);
                        assertThat(pm.getProcessedAt()).isNotNull();
                        assertThat(pm.getProcessedBy()).isNotBlank();
                    });
        }

        @Test
        void secondDelivery_sameBenchmark_returnsDuplicate_noExtraRow() {
            Message msg = message("int-bm-002", "/queue/trades", "{\"id\":\"2\",\"symbol\":\"MSFT\"}");

            ProcessingResult first  = processor.process(msg);
            ProcessingResult second = processor.process(msg);

            assertThat(first).isEqualTo(ProcessingResult.OK);
            assertThat(second).isEqualTo(ProcessingResult.DUPLICATE);
            assertThat(repository.count()).isEqualTo(1);
        }

        @Test
        void firstDeliveryFails_retryWithGoodPayload_returnsOK() {
            Message badMsg  = message("int-bm-003", "/queue/trades", " ");
            ProcessingResult fail = processor.process(badMsg);
            assertThat(fail).isEqualTo(ProcessingResult.FAIL);

            Message goodMsg = message("int-bm-003", "/queue/trades", "{\"id\":\"3\"}");
            ProcessingResult ok = processor.process(goodMsg);
            assertThat(ok).isEqualTo(ProcessingResult.OK);

            assertThat(repository.findByMessageId("int-bm-003"))
                    .isPresent()
                    .hasValueSatisfying(pm -> assertThat(pm.getStatus()).isEqualTo(ProcessingStatus.PROCESSED));
        }

        @Test
        void threeConsecutiveFailures_resultsInDiscard_persistsDiscardedRow() {
            // maxRetries=3 per application-test.yaml
            Message msg = message("int-bm-004", "/queue/trades", " ");

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

        @Test
        void countProcessedSince_returnsCorrectCount() {
            java.time.Instant before = java.time.Instant.now().minusSeconds(1);

            processor.process(message("int-bm-010", "/queue/trades", "{\"id\":\"10\"}"));
            processor.process(message("int-bm-011", "/queue/trades", "{\"id\":\"11\"}"));
            processor.process(message("int-bm-012", "/queue/trades", " "));  // FAIL, not counted

            long count = repository.countProcessedSince(before);
            assertThat(count).isEqualTo(2);
        }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // retry-db-tracking-enabled=false  (in-memory tracking)
    // ══════════════════════════════════════════════════════════════════════════

    @Nested
    class WithoutDbTracking {

        @BeforeEach
        void disableDbTracking() {
            ReflectionTestUtils.setField(processor, "dbTrackingEnabled", false);
            inMemoryMap().clear();
        }

        @AfterEach
        void restoreDbTracking() {
            ReflectionTestUtils.setField(processor, "dbTrackingEnabled", true);
            inMemoryMap().clear();
        }

        @Test
        void firstDelivery_validPayload_persistsProcessedRecord_noFailedRow() {
            Message msg = message("int-bm-101", "/queue/trades", "{\"id\":\"101\"}");

            ProcessingResult result = processor.process(msg);

            assertThat(result).isEqualTo(ProcessingResult.OK);
            assertThat(repository.findByMessageId("int-bm-101"))
                    .isPresent()
                    .hasValueSatisfying(pm -> assertThat(pm.getStatus()).isEqualTo(ProcessingStatus.PROCESSED));
            assertThat(repository.count()).isEqualTo(1);
        }

        @Test
        void firstFailure_returnsFAIL_noRowWrittenToDb() {
            Message msg = message("int-bm-102", "/queue/trades", " ");

            ProcessingResult result = processor.process(msg);

            assertThat(result).isEqualTo(ProcessingResult.FAIL);
            assertThat(repository.existsByMessageId("int-bm-102")).isFalse();
            assertThat(inMemoryMap().get("int-bm-102")).isEqualTo(1);
        }

        @Test
        void threeConsecutiveFailures_resultInDiscard_noRowsInDb() {
            // maxRetries=3: 3 failed attempts → DISCARD, no DB writes
            Message msg = message("int-bm-103", "/queue/trades", " ");

            ProcessingResult r1 = processor.process(msg); // attempt=1 → FAIL
            ProcessingResult r2 = processor.process(msg); // attempt=2 → FAIL
            ProcessingResult r3 = processor.process(msg); // attempt=3 → DISCARD

            assertThat(r1).isEqualTo(ProcessingResult.FAIL);
            assertThat(r2).isEqualTo(ProcessingResult.FAIL);
            assertThat(r3).isEqualTo(ProcessingResult.DISCARD);

            assertThat(repository.existsByMessageId("int-bm-103")).isFalse();
            assertThat(inMemoryMap()).doesNotContainKey("int-bm-103");
        }

        @Test
        void failThenSucceed_persistsProcessedRecord_noFailedRow() {
            Message badMsg  = message("int-bm-104", "/queue/trades", " ");
            Message goodMsg = message("int-bm-104", "/queue/trades", "{\"id\":\"104\"}");

            ProcessingResult fail = processor.process(badMsg);
            ProcessingResult ok   = processor.process(goodMsg);

            assertThat(fail).isEqualTo(ProcessingResult.FAIL);
            assertThat(ok).isEqualTo(ProcessingResult.OK);

            // Only the PROCESSED row — no FAILED row ever written
            assertThat(repository.count()).isEqualTo(1);
            assertThat(repository.findByMessageId("int-bm-104"))
                    .isPresent()
                    .hasValueSatisfying(pm -> assertThat(pm.getStatus()).isEqualTo(ProcessingStatus.PROCESSED));
            assertThat(inMemoryMap()).doesNotContainKey("int-bm-104");
        }

        @Test
        void alreadyProcessedMessage_returnsDUPLICATE() {
            // First call succeeds and writes PROCESSED row
            processor.process(message("int-bm-105", "/queue/trades", "{\"id\":\"105\"}"));

            // Re-delivery of same bookmark
            ProcessingResult result = processor.process(message("int-bm-105", "/queue/trades", "{\"id\":\"105\"}"));

            assertThat(result).isEqualTo(ProcessingResult.DUPLICATE);
            assertThat(repository.count()).isEqualTo(1);
        }

        @SuppressWarnings("unchecked")
        private ConcurrentHashMap<String, Integer> inMemoryMap() {
            return (ConcurrentHashMap<String, Integer>)
                    ReflectionTestUtils.getField(processor, "inMemoryRetryCount");
        }
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
