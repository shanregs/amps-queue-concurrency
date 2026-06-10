package com.shan.mq.amps.ampsqueueconcurrency.service;

import com.crankuptheamps.client.HAClient;
import com.crankuptheamps.client.Message;
import com.shan.mq.amps.ampsqueueconcurrency.model.ProcessingResult;
import com.shan.mq.amps.ampsqueueconcurrency.processor.MessageProcessor;
import com.shan.mq.amps.ampsqueueconcurrency.subscriber.SubscriberMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
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
        dispatchService = new MessageDispatchService(
                processor, executor, new SubscriberMetrics(new SimpleMeterRegistry()));
    }

    // ── ACK on OK ─────────────────────────────────────────────────────────────

    @Test
    void dispatch_OK_acksMessage() throws Exception {
        Message msg = message("bm-1");
        when(processor.process(msg)).thenReturn(ProcessingResult.OK);

        CountDownLatch done = new CountDownLatch(1);
        doAnswer(inv -> { done.countDown(); return null; }).when(msg).ack();

        Semaphore sem = new Semaphore(5, true);
        dispatchService.dispatch(msg, haClient, sem);

        assertThat(done.await(2, TimeUnit.SECONDS)).isTrue();
        Thread.sleep(200);  // allow finally block (timer.stop + vtStopped + semaphore.release) to complete
        verify(msg).ack();
        assertThat(sem.availablePermits()).isEqualTo(5);  // permit released
    }

    // ── ACK on DUPLICATE ─────────────────────────────────────────────────────

    @Test
    void dispatch_DUPLICATE_acksMessage() throws Exception {
        Message msg = message("bm-2");
        when(processor.process(msg)).thenReturn(ProcessingResult.DUPLICATE);

        CountDownLatch done = new CountDownLatch(1);
        doAnswer(inv -> { done.countDown(); return null; }).when(msg).ack();

        Semaphore sem = new Semaphore(5, true);
        dispatchService.dispatch(msg, haClient, sem);

        assertThat(done.await(2, TimeUnit.SECONDS)).isTrue();
        verify(msg).ack();
    }

    // ── ACK on DISCARD ────────────────────────────────────────────────────────

    @Test
    void dispatch_DISCARD_acksMessage() throws Exception {
        Message msg = message("bm-3");
        when(processor.process(msg)).thenReturn(ProcessingResult.DISCARD);

        CountDownLatch done = new CountDownLatch(1);
        doAnswer(inv -> { done.countDown(); return null; }).when(msg).ack();

        Semaphore sem = new Semaphore(5, true);
        dispatchService.dispatch(msg, haClient, sem);

        assertThat(done.await(2, TimeUnit.SECONDS)).isTrue();
        verify(msg).ack();
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
        verify(msg, never()).ack();
        assertThat(sem.availablePermits()).isEqualTo(5);  // permit still released
    }

    // ── Semaphore backpressure ─────────────────────────────────────────────

    @Test
    void dispatch_semaphorePermitAcquiredAndReleased() throws Exception {
        Message msg = message("bm-5");
        CountDownLatch done = new CountDownLatch(1);
        when(processor.process(msg)).thenAnswer(inv -> { done.countDown(); return ProcessingResult.OK; });
        doNothing().when(msg).ack();

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
        verify(msg, never()).ack();
    }

    // ── Concurrent dispatch ───────────────────────────────────────────────────

    @Test
    void dispatch_multipleConcurrentMessages_allAcked_permitsFullyRestored() throws Exception {
        int count = 5;
        CountDownLatch allAcked = new CountDownLatch(count);
        Semaphore sem = new Semaphore(count, true);

        // Register all stubs before dispatching to avoid Mockito strict-stub races
        // with concurrently running virtual threads.
        Message[] msgs = new Message[count];
        for (int i = 0; i < count; i++) {
            msgs[i] = message("bm-conc-" + i);
            when(processor.process(msgs[i])).thenReturn(ProcessingResult.OK);
            doAnswer(inv -> { allAcked.countDown(); return null; }).when(msgs[i]).ack();
        }
        for (int i = 0; i < count; i++) {
            dispatchService.dispatch(msgs[i], haClient, sem);
        }

        assertThat(allAcked.await(3, TimeUnit.SECONDS)).isTrue();
        Thread.sleep(200);
        assertThat(sem.availablePermits()).isEqualTo(count);
    }

    @Test
    void dispatch_mixedResults_onlyNonFailMessagesAcked_allPermitsRestored() throws Exception {
        Message okMsg   = message("bm-mix-1");
        Message failMsg = message("bm-mix-2");
        Message dupMsg  = message("bm-mix-3");

        CountDownLatch allDone = new CountDownLatch(3);
        when(processor.process(okMsg)).thenAnswer(inv -> { allDone.countDown(); return ProcessingResult.OK; });
        when(processor.process(failMsg)).thenAnswer(inv -> { allDone.countDown(); return ProcessingResult.FAIL; });
        when(processor.process(dupMsg)).thenAnswer(inv -> { allDone.countDown(); return ProcessingResult.DUPLICATE; });
        doNothing().when(okMsg).ack();
        doNothing().when(dupMsg).ack();

        Semaphore sem = new Semaphore(10, true);
        dispatchService.dispatch(okMsg,   haClient, sem);
        dispatchService.dispatch(failMsg, haClient, sem);
        dispatchService.dispatch(dupMsg,  haClient, sem);

        assertThat(allDone.await(3, TimeUnit.SECONDS)).isTrue();
        Thread.sleep(50);
        verify(okMsg).ack();
        verify(dupMsg).ack();
        verify(failMsg, never()).ack();
        assertThat(sem.availablePermits()).isEqualTo(10); // all permits returned regardless of result
    }

    @Test
    void dispatch_semaphoreAcquiredSynchronously_beforeVirtualThreadStarts() throws Exception {
        Semaphore sem = new Semaphore(1, true);
        CountDownLatch processorStarted  = new CountDownLatch(1);
        CountDownLatch releaseProcessor  = new CountDownLatch(1);

        Message msg = message("bm-sync-1");
        when(processor.process(msg)).thenAnswer(inv -> {
            processorStarted.countDown();
            releaseProcessor.await();
            return ProcessingResult.OK;
        });
        doNothing().when(msg).ack();

        // dispatch() acquires the semaphore in the caller thread, then submits the VT
        dispatchService.dispatch(msg, haClient, sem);

        // Permit must be taken synchronously — before the VT even starts
        assertThat(sem.availablePermits()).isEqualTo(0);

        // Let VT finish and release the permit
        assertThat(processorStarted.await(2, TimeUnit.SECONDS)).isTrue();
        releaseProcessor.countDown();
        Thread.sleep(100);
        assertThat(sem.availablePermits()).isEqualTo(1);
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static Message message(String bookmark) {
        Message m = mock(Message.class);
        when(m.getBookmark()).thenReturn(bookmark);
        when(m.getTopic()).thenReturn("/queue/trades");
        return m;
    }
}
