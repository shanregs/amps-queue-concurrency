package com.shan.mq.amps.ampsqueueconcurrency.subscriber;

import com.crankuptheamps.client.CommandId;
import com.crankuptheamps.client.HAClient;
import com.crankuptheamps.client.Message;
import com.shan.mq.amps.ampsqueueconcurrency.model.SubscriberContext;
import com.shan.mq.amps.ampsqueueconcurrency.service.MessageDispatchService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

import java.lang.reflect.Method;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.lenient;

@ExtendWith(MockitoExtension.class)
class MultiAmpsSubscriberPoolTest {

    @Mock HAClient              haClient1;
    @Mock HAClient              haClient2;
    @Mock MessageDispatchService dispatchService;
    @Mock CommandId             commandId;

    private SubscriberContext        ctx1;
    private SubscriberContext        ctx2;
    private MultiAmpsSubscriberPool  pool;

    @BeforeEach
    void setUp() {
        ctx1 = new SubscriberContext(haClient1, new Semaphore(5, true), 1);
        ctx2 = new SubscriberContext(haClient2, new Semaphore(5, true), 2);
        pool = new MultiAmpsSubscriberPool(List.of(ctx1, ctx2), dispatchService);
        ReflectionTestUtils.setField(pool, "topic",                    "/queue/trades");
        ReflectionTestUtils.setField(pool, "filter",                   "");
        ReflectionTestUtils.setField(pool, "maxConcurrencyPerSubscriber", 5);
        ReflectionTestUtils.setField(pool, "shutdownTimeoutMs",        1000L);
    }

    @AfterEach
    void tearDown() {
        if (pool.isRunning()) pool.stop();
    }

    // ── Lifecycle ─────────────────────────────────────────────────────────────

    @Test
    void isRunning_beforeStart_returnsFalse() {
        assertThat(pool.isRunning()).isFalse();
    }

    @Test
    void getPhase_returnsMaxInt() {
        assertThat(pool.getPhase()).isEqualTo(Integer.MAX_VALUE);
    }

    @Test
    void start_setsRunningTrue() throws Exception {
        CountDownLatch both = awaitBothSubscribed();
        pool.start();
        assertThat(both.await(2, TimeUnit.SECONDS)).isTrue();
        assertThat(pool.isRunning()).isTrue();
    }

    @Test
    void start_subscribesEachContextOnItsOwnHaClient() throws Exception {
        CountDownLatch both = awaitBothSubscribed();
        pool.start();
        both.await(2, TimeUnit.SECONDS);

        verify(haClient1).subscribe(any(), eq("/queue/trades"), anyLong());
        verify(haClient2).subscribe(any(), eq("/queue/trades"), anyLong());
    }

    @Test
    void stop_afterStart_setsRunningFalse() throws Exception {
        CountDownLatch both = awaitBothSubscribed();
        pool.start();
        both.await(2, TimeUnit.SECONDS);
        pool.stop();
        assertThat(pool.isRunning()).isFalse();
    }

    @Test
    void stop_unsubscribesAllSubscriberContexts() throws Exception {
        CountDownLatch both = awaitBothSubscribed();
        pool.start();
        both.await(2, TimeUnit.SECONDS);
        pool.stop();

        verify(haClient1).unsubscribe(commandId);
        verify(haClient2).unsubscribe(commandId);
    }

    // ── Message handling ──────────────────────────────────────────────────────

    @Test
    void handleMessage_whenRunning_dispatchesWithContextsHaClientAndSemaphore() throws Exception {
        CountDownLatch both = awaitBothSubscribed();
        pool.start();
        both.await(2, TimeUnit.SECONDS);

        Message msg = message("bm-multi-1");
        invokeHandleMessage(msg, ctx1);

        verify(dispatchService).dispatch(msg, ctx1.haClient(), ctx1.semaphore());
    }

    @Test
    void handleMessage_eachContextUsesItsOwnHaClientAndSemaphore() throws Exception {
        CountDownLatch both = awaitBothSubscribed();
        pool.start();
        both.await(2, TimeUnit.SECONDS);

        Message msg1 = message("bm-multi-2");
        Message msg2 = message("bm-multi-3");
        invokeHandleMessage(msg1, ctx1);
        invokeHandleMessage(msg2, ctx2);

        verify(dispatchService).dispatch(msg1, ctx1.haClient(), ctx1.semaphore());
        verify(dispatchService).dispatch(msg2, ctx2.haClient(), ctx2.semaphore());
        // Verify ctx1's semaphore was NOT used for ctx2's message and vice versa
        verify(dispatchService, never()).dispatch(msg1, ctx2.haClient(), ctx2.semaphore());
        verify(dispatchService, never()).dispatch(msg2, ctx1.haClient(), ctx1.semaphore());
    }

    @Test
    void handleMessage_whenStopped_skipsDispatch() throws Exception {
        // pool not started — running=false
        Message msg = message("bm-multi-4");
        invokeHandleMessage(msg, ctx1);
        verifyNoInteractions(dispatchService);
    }

    @Test
    void handleMessage_interruptedException_handledGracefully() throws Exception {
        CountDownLatch both = awaitBothSubscribed();
        pool.start();
        both.await(2, TimeUnit.SECONDS);

        doThrow(new InterruptedException("test"))
                .when(dispatchService).dispatch(any(), any(), any());

        Message msg = message("bm-multi-5");
        invokeHandleMessage(msg, ctx1); // must not propagate
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private CountDownLatch awaitBothSubscribed() throws Exception {
        CountDownLatch latch = new CountDownLatch(2);
        when(haClient1.subscribe(any(), anyString(), anyLong()))
                .thenAnswer(inv -> { latch.countDown(); return commandId; });
        when(haClient2.subscribe(any(), anyString(), anyLong()))
                .thenAnswer(inv -> { latch.countDown(); return commandId; });
        return latch;
    }

    private void invokeHandleMessage(Message msg, SubscriberContext ctx) throws Exception {
        Method m = MultiAmpsSubscriberPool.class
                .getDeclaredMethod("handleMessage", Message.class, SubscriberContext.class);
        m.setAccessible(true);
        m.invoke(pool, msg, ctx);
    }

    private static Message message(String bookmark) {
        Message m = mock(Message.class);
        lenient().when(m.getBookmark()).thenReturn(bookmark);
        return m;
    }
}
