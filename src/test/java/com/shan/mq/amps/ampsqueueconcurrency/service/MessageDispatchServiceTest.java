package com.shan.mq.amps.ampsqueueconcurrency.service;

import com.crankuptheamps.client.HAClient;
import com.crankuptheamps.client.Message;
import com.shan.mq.amps.ampsqueueconcurrency.model.ProcessingResult;
import com.shan.mq.amps.ampsqueueconcurrency.processor.MessageProcessor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class MessageDispatchServiceTest {

    @Mock private MessageProcessor processor;
    @Mock private HAClient haClient;

    private ExecutorService executor;
    private MessageDispatchService dispatchService;

    @BeforeEach
    void setUp() {
        executor = Executors.newVirtualThreadPerTaskExecutor();
        dispatchService = new MessageDispatchService(processor, executor);
    }

    // ── ACK on OK ─────────────────────────────────────────────────────────────

    @Test
    void dispatch_OK_acksMessage() throws Exception {
        Message msg = message("bm-1");
        when(processor.process(msg)).thenReturn(ProcessingResult.OK);

        CountDownLatch done = new CountDownLatch(1);
        doAnswer(inv -> { done.countDown(); return null; }).when(haClient).ack(msg);

        Semaphore sem = new Semaphore(5, true);
        dispatchService.dispatch(msg, haClient, sem);

        assertThat(done.await(2, TimeUnit.SECONDS)).isTrue();
        verify(haClient).ack(msg);
        assertThat(sem.availablePermits()).isEqualTo(5);  // permit released
    }

    // ── ACK on DUPLICATE ─────────────────────────────────────────────────────

    @Test
    void dispatch_DUPLICATE_acksMessage() throws Exception {
        Message msg = message("bm-2");
        when(processor.process(msg)).thenReturn(ProcessingResult.DUPLICATE);

        CountDownLatch done = new CountDownLatch(1);
        doAnswer(inv -> { done.countDown(); return null; }).when(haClient).ack(msg);

        Semaphore sem = new Semaphore(5, true);
        dispatchService.dispatch(msg, haClient, sem);

        assertThat(done.await(2, TimeUnit.SECONDS)).isTrue();
        verify(haClient).ack(msg);
    }

    // ── ACK on DISCARD ────────────────────────────────────────────────────────

    @Test
    void dispatch_DISCARD_acksMessage() throws Exception {
        Message msg = message("bm-3");
        when(processor.process(msg)).thenReturn(ProcessingResult.DISCARD);

        CountDownLatch done = new CountDownLatch(1);
        doAnswer(inv -> { done.countDown(); return null; }).when(haClient).ack(msg);

        Semaphore sem = new Semaphore(5, true);
        dispatchService.dispatch(msg, haClient, sem);

        assertThat(done.await(2, TimeUnit.SECONDS)).isTrue();
        verify(haClient).ack(msg);
    }

    // ── No ACK on FAIL ────────────────────────────────────────────────────────

    @Test
    void dispatch_FAIL_doesNotAckMessage() throws Exception {
        Message msg = message("bm-4");
        when(processor.process(msg)).thenReturn(ProcessingResult.FAIL);

        // Use a latch on processor.process to know when VT has finished
        CountDownLatch done = new CountDownLatch(1);
        when(processor.process(msg)).thenAnswer(inv -> {
            done.countDown();
            return ProcessingResult.FAIL;
        });

        Semaphore sem = new Semaphore(5, true);
        dispatchService.dispatch(msg, haClient, sem);

        assertThat(done.await(2, TimeUnit.SECONDS)).isTrue();
        Thread.sleep(50);  // allow VT finally block to run
        verify(haClient, never()).ack(any());
        assertThat(sem.availablePermits()).isEqualTo(5);  // permit still released
    }

    // ── Semaphore backpressure ─────────────────────────────────────────────

    @Test
    void dispatch_semaphorePermitAcquiredAndReleased() throws Exception {
        Message msg = message("bm-5");
        CountDownLatch done = new CountDownLatch(1);
        when(processor.process(msg)).thenAnswer(inv -> { done.countDown(); return ProcessingResult.OK; });
        doNothing().when(haClient).ack(any());

        Semaphore sem = new Semaphore(3, true);
        assertThat(sem.availablePermits()).isEqualTo(3);

        dispatchService.dispatch(msg, haClient, sem);
        // permit is acquired synchronously before submit, so 2 permits remain until VT releases
        // after VT finishes it should be 3 again
        assertThat(done.await(2, TimeUnit.SECONDS)).isTrue();
        Thread.sleep(50);
        assertThat(sem.availablePermits()).isEqualTo(3);
    }

    // ── Exception in processor does not leak permit ───────────────────────

    @Test
    void dispatch_processorThrows_permitStillReleased() throws Exception {
        Message msg = message("bm-6");
        CountDownLatch done = new CountDownLatch(1);
        when(processor.process(msg)).thenAnswer(inv -> {
            done.countDown();
            throw new RuntimeException("boom");
        });

        Semaphore sem = new Semaphore(5, true);
        dispatchService.dispatch(msg, haClient, sem);

        assertThat(done.await(2, TimeUnit.SECONDS)).isTrue();
        Thread.sleep(50);
        assertThat(sem.availablePermits()).isEqualTo(5);
        verify(haClient, never()).ack(any());
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static Message message(String bookmark) {
        Message m = mock(Message.class);
        when(m.getBookmark()).thenReturn(bookmark);
        when(m.getTopic()).thenReturn("/queue/trades");
        return m;
    }
}
