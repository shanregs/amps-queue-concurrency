package com.shan.mq.amps.ampsqueueconcurrency.processor;

import com.crankuptheamps.client.Message;
import com.shan.mq.amps.ampsqueueconcurrency.model.ProcessedMessage;
import com.shan.mq.amps.ampsqueueconcurrency.model.ProcessingResult;
import com.shan.mq.amps.ampsqueueconcurrency.model.ProcessingStatus;
import com.shan.mq.amps.ampsqueueconcurrency.repository.ProcessedMessageRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class MessageProcessorTest {

    @Mock
    private ProcessedMessageRepository repository;

    @InjectMocks
    private MessageProcessor processor;

    // ══════════════════════════════════════════════════════════════════════════
    // retry-db-tracking-enabled=true  (default)
    // ══════════════════════════════════════════════════════════════════════════

    @Nested
    class WithDbTracking {

        @BeforeEach
        void setUp() {
            ReflectionTestUtils.setField(processor, "maxRetries", 3);
            ReflectionTestUtils.setField(processor, "dbTrackingEnabled", true);
        }

        @Test
        void newValidMessage_returnsOK_savesProcessedRecord() {
            Message msg = message("bm-001", "/queue/trades", "{\"id\":\"1\",\"symbol\":\"AAPL\"}");
            when(repository.findByMessageId("bm-001")).thenReturn(Optional.empty());
            when(repository.save(any())).thenAnswer(inv -> inv.getArgument(0));

            ProcessingResult result = processor.process(msg);

            assertThat(result).isEqualTo(ProcessingResult.OK);
            verify(repository).save(argThat(pm ->
                    pm.getStatus() == ProcessingStatus.PROCESSED &&
                    "bm-001".equals(pm.getMessageId()) &&
                    pm.getProcessedAt() != null));
        }

        @Test
        void alreadyProcessedMessage_returnsDUPLICATE_noSave() {
            Message msg = message("bm-002", "/queue/trades", "{\"id\":\"2\"}");
            when(repository.findByMessageId("bm-002")).thenReturn(
                    Optional.of(processedRecord("bm-002", ProcessingStatus.PROCESSED, 0)));

            ProcessingResult result = processor.process(msg);

            assertThat(result).isEqualTo(ProcessingResult.DUPLICATE);
            verify(repository, never()).save(any());
        }

        @Test
        void alreadyDiscardedMessage_returnsDUPLICATE_noSave() {
            Message msg = message("bm-003", "/queue/trades", "{\"id\":\"3\"}");
            when(repository.findByMessageId("bm-003")).thenReturn(
                    Optional.of(processedRecord("bm-003", ProcessingStatus.DISCARDED, 3)));

            ProcessingResult result = processor.process(msg);

            assertThat(result).isEqualTo(ProcessingResult.DUPLICATE);
            verify(repository, never()).save(any());
        }

        @Test
        void firstDelivery_blankPayload_returnsFAIL_savesFailedRecord() {
            Message msg = message("bm-004", "/queue/trades", "  ");
            when(repository.findByMessageId("bm-004")).thenReturn(Optional.empty());
            when(repository.save(any())).thenAnswer(inv -> inv.getArgument(0));

            ProcessingResult result = processor.process(msg);

            assertThat(result).isEqualTo(ProcessingResult.FAIL);
            verify(repository).save(argThat(pm ->
                    pm.getStatus() == ProcessingStatus.FAILED &&
                    pm.getRetryCount() == 1 &&
                    pm.getLastError() != null));
        }

        @Test
        void failedRecord_retriedWithValidPayload_returnsOK() {
            Message msg = message("bm-005", "/queue/trades", "{\"id\":\"5\",\"valid\":true}");
            ProcessedMessage failed = processedRecord("bm-005", ProcessingStatus.FAILED, 1);
            when(repository.findByMessageId("bm-005")).thenReturn(Optional.of(failed));
            when(repository.save(any())).thenAnswer(inv -> inv.getArgument(0));

            ProcessingResult result = processor.process(msg);

            assertThat(result).isEqualTo(ProcessingResult.OK);
            assertThat(failed.getStatus()).isEqualTo(ProcessingStatus.PROCESSED);
            assertThat(failed.getProcessedAt()).isNotNull();
        }

        @Test
        void failedRecord_atMaxRetries_blankPayload_returnsDISCARD() {
            Message msg = message("bm-006", "/queue/trades", " ");
            ProcessedMessage failed = processedRecord("bm-006", ProcessingStatus.FAILED, 2);
            when(repository.findByMessageId("bm-006")).thenReturn(Optional.of(failed));
            when(repository.save(any())).thenAnswer(inv -> inv.getArgument(0));

            ProcessingResult result = processor.process(msg);

            assertThat(result).isEqualTo(ProcessingResult.DISCARD);
            assertThat(failed.getStatus()).isEqualTo(ProcessingStatus.DISCARDED);
            assertThat(failed.getRetryCount()).isEqualTo(3);
        }

        @Test
        void failedRecord_retryBelowMax_blankPayload_returnsFAIL() {
            Message msg = message("bm-007", "/queue/trades", " ");
            ProcessedMessage failed = processedRecord("bm-007", ProcessingStatus.FAILED, 1);
            when(repository.findByMessageId("bm-007")).thenReturn(Optional.of(failed));
            when(repository.save(any())).thenAnswer(inv -> inv.getArgument(0));

            ProcessingResult result = processor.process(msg);

            assertThat(result).isEqualTo(ProcessingResult.FAIL);
            assertThat(failed.getStatus()).isEqualTo(ProcessingStatus.FAILED);
            assertThat(failed.getRetryCount()).isEqualTo(2);
        }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // retry-db-tracking-enabled=false  (in-memory tracking)
    // ══════════════════════════════════════════════════════════════════════════

    @Nested
    class WithoutDbTracking {

        @BeforeEach
        void setUp() {
            ReflectionTestUtils.setField(processor, "maxRetries", 3);
            ReflectionTestUtils.setField(processor, "dbTrackingEnabled", false);
            inMemoryMap().clear();
        }

        @Test
        void newValidMessage_returnsOK_savesProcessedRecord_noFailedRow() {
            Message msg = message("bm-101", "/queue/trades", "{\"id\":\"101\"}");
            when(repository.findByMessageId("bm-101")).thenReturn(Optional.empty());
            when(repository.save(any())).thenAnswer(inv -> inv.getArgument(0));

            ProcessingResult result = processor.process(msg);

            assertThat(result).isEqualTo(ProcessingResult.OK);
            verify(repository).save(argThat(pm ->
                    pm.getStatus() == ProcessingStatus.PROCESSED &&
                    "bm-101".equals(pm.getMessageId())));
            assertThat(inMemoryMap()).doesNotContainKey("bm-101");
        }

        @Test
        void alreadyProcessedMessage_returnsDUPLICATE_noSave() {
            Message msg = message("bm-102", "/queue/trades", "{\"id\":\"102\"}");
            when(repository.findByMessageId("bm-102")).thenReturn(
                    Optional.of(processedRecord("bm-102", ProcessingStatus.PROCESSED, 0)));

            ProcessingResult result = processor.process(msg);

            assertThat(result).isEqualTo(ProcessingResult.DUPLICATE);
            verify(repository, never()).save(any());
        }

        @Test
        void firstFailure_returnsFAIL_noDbWrite_incrementsInMemoryCount() {
            Message msg = message("bm-103", "/queue/trades", "  ");
            when(repository.findByMessageId("bm-103")).thenReturn(Optional.empty());

            ProcessingResult result = processor.process(msg);

            assertThat(result).isEqualTo(ProcessingResult.FAIL);
            verify(repository, never()).save(any());
            assertThat(inMemoryMap().get("bm-103")).isEqualTo(1);
        }

        @Test
        void atMaxRetries_failure_returnsDISCARD_noDbWrite_clearsInMemoryCount() {
            // Pre-populate: 2 failures already recorded in memory
            inMemoryMap().put("bm-104", 2);
            Message msg = message("bm-104", "/queue/trades", "  ");
            when(repository.findByMessageId("bm-104")).thenReturn(Optional.empty());

            ProcessingResult result = processor.process(msg);

            assertThat(result).isEqualTo(ProcessingResult.DISCARD);
            verify(repository, never()).save(any());
            assertThat(inMemoryMap()).doesNotContainKey("bm-104");
        }

        @Test
        void retryAfterFailure_success_returnsOK_clearsInMemoryCount() {
            // 1 failure already in memory
            inMemoryMap().put("bm-105", 1);
            Message msg = message("bm-105", "/queue/trades", "{\"id\":\"105\"}");
            when(repository.findByMessageId("bm-105")).thenReturn(Optional.empty());
            when(repository.save(any())).thenAnswer(inv -> inv.getArgument(0));

            ProcessingResult result = processor.process(msg);

            assertThat(result).isEqualTo(ProcessingResult.OK);
            assertThat(inMemoryMap()).doesNotContainKey("bm-105");
        }

        @Test
        void belowMaxRetries_failure_returnsFAIL_noDbWrite() {
            inMemoryMap().put("bm-106", 1);
            Message msg = message("bm-106", "/queue/trades", "  ");
            when(repository.findByMessageId("bm-106")).thenReturn(Optional.empty());

            ProcessingResult result = processor.process(msg);

            assertThat(result).isEqualTo(ProcessingResult.FAIL);
            verify(repository, never()).save(any());
            assertThat(inMemoryMap().get("bm-106")).isEqualTo(2);
        }

        @SuppressWarnings("unchecked")
        private ConcurrentHashMap<String, Integer> inMemoryMap() {
            return (ConcurrentHashMap<String, Integer>)
                    ReflectionTestUtils.getField(processor, "inMemoryRetryCount");
        }
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static Message message(String bookmark, String topic, String data) {
        Message m = mock(Message.class);
        when(m.getBookmark()).thenReturn(bookmark);
        when(m.getTopic()).thenReturn(topic);
        when(m.getData()).thenReturn(data);
        return m;
    }

    private static ProcessedMessage processedRecord(String messageId,
                                                     ProcessingStatus status,
                                                     int retryCount) {
        return ProcessedMessage.builder()
                .messageId(messageId)
                .topic("/queue/trades")
                .payload("{}")
                .status(status)
                .retryCount(retryCount)
                .receivedAt(Instant.now())
                .processedBy("test-host")
                .build();
    }
}
