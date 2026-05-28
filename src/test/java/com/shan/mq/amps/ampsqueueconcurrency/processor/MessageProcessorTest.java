package com.shan.mq.amps.ampsqueueconcurrency.processor;

import com.crankuptheamps.client.Message;
import com.shan.mq.amps.ampsqueueconcurrency.model.ProcessedMessage;
import com.shan.mq.amps.ampsqueueconcurrency.model.ProcessingResult;
import com.shan.mq.amps.ampsqueueconcurrency.model.ProcessingStatus;
import com.shan.mq.amps.ampsqueueconcurrency.repository.ProcessedMessageRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.Instant;
import java.util.Optional;

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

    @BeforeEach
    void setUp() {
        ReflectionTestUtils.setField(processor, "maxRetries", 3);
    }

    // ── OK: new message, valid payload ────────────────────────────────────────

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

    // ── DUPLICATE: already PROCESSED ─────────────────────────────────────────

    @Test
    void alreadyProcessedMessage_returnsDUPLICATE_noSave() {
        Message msg = message("bm-002", "/queue/trades", "{\"id\":\"2\"}");
        when(repository.findByMessageId("bm-002")).thenReturn(
                Optional.of(processedRecord("bm-002", ProcessingStatus.PROCESSED, 0)));

        ProcessingResult result = processor.process(msg);

        assertThat(result).isEqualTo(ProcessingResult.DUPLICATE);
        verify(repository, never()).save(any());
    }

    // ── DUPLICATE: already DISCARDED ─────────────────────────────────────────

    @Test
    void alreadyDiscardedMessage_returnsDUPLICATE_noSave() {
        Message msg = message("bm-003", "/queue/trades", "{\"id\":\"3\"}");
        when(repository.findByMessageId("bm-003")).thenReturn(
                Optional.of(processedRecord("bm-003", ProcessingStatus.DISCARDED, 3)));

        ProcessingResult result = processor.process(msg);

        assertThat(result).isEqualTo(ProcessingResult.DUPLICATE);
        verify(repository, never()).save(any());
    }

    // ── FAIL: first delivery with blank payload ───────────────────────────────

    @Test
    void firstDelivery_blankPayload_returnsFAIL_savesFailedRecord() {
        Message msg = message("bm-004", "/queue/trades", "  ");  // blank → business logic throws
        when(repository.findByMessageId("bm-004")).thenReturn(Optional.empty());
        when(repository.save(any())).thenAnswer(inv -> inv.getArgument(0));

        ProcessingResult result = processor.process(msg);

        assertThat(result).isEqualTo(ProcessingResult.FAIL);
        verify(repository).save(argThat(pm ->
                pm.getStatus() == ProcessingStatus.FAILED &&
                pm.getRetryCount() == 1 &&
                pm.getLastError() != null));
    }

    // ── OK on retry ──────────────────────────────────────────────────────────

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

    // ── DISCARD: maxRetries exceeded ─────────────────────────────────────────

    @Test
    void failedRecord_atMaxRetries_blankPayload_returnsDISCARD() {
        Message msg = message("bm-006", "/queue/trades", " ");  // blank → throws
        ProcessedMessage failed = processedRecord("bm-006", ProcessingStatus.FAILED, 2); // retryCount=2, maxRetries=3
        when(repository.findByMessageId("bm-006")).thenReturn(Optional.of(failed));
        when(repository.save(any())).thenAnswer(inv -> inv.getArgument(0));

        ProcessingResult result = processor.process(msg);

        assertThat(result).isEqualTo(ProcessingResult.DISCARD);
        assertThat(failed.getStatus()).isEqualTo(ProcessingStatus.DISCARDED);
        assertThat(failed.getRetryCount()).isEqualTo(3);
    }

    // ── FAIL: retry, below maxRetries ────────────────────────────────────────

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
