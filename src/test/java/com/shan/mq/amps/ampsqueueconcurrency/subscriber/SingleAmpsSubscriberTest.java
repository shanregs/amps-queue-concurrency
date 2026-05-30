package com.shan.mq.amps.ampsqueueconcurrency.subscriber;

import com.crankuptheamps.client.CommandId;
import com.crankuptheamps.client.HAClient;
import com.crankuptheamps.client.Message;
import com.shan.mq.amps.ampsqueueconcurrency.service.MessageDispatchService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

import java.lang.reflect.Method;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.lenient;

@ExtendWith(MockitoExtension.class)
class SingleAmpsSubscriberTest {

    @Mock HAClient              haClient;
    @Mock MessageDispatchService dispatchService;
    @Mock CommandId             commandId;

    private Semaphore            semaphore;
    private SingleAmpsSubscriber subscriber;

    @BeforeEach
    void setUp() {
        semaphore  = new Semaphore(10, true);
        subscriber = new SingleAmpsSubscriber(haClient, semaphore, dispatchService);
        ReflectionTestUtils.setField(subscriber, "topic",              "/queue/trades");
        ReflectionTestUtils.setField(subscriber, "filter",             "");
        ReflectionTestUtils.setField(subscriber, "maxConcurrency",     10);
        ReflectionTestUtils.setField(subscriber, "shutdownTimeoutMs",  1000L);
    }

    @AfterEach
    void tearDown() {
        if (subscriber.isRunning()) subscriber.stop();
    }

    // ── Lifecycle ─────────────────────────────────────────────────────────────

    @Test
    void isRunning_beforeStart_returnsFalse() {
        assertThat(subscriber.isRunning()).isFalse();
    }

    @Test
    void getPhase_returnsMaxInt_stopsLastAfterPublisher() {
        assertThat(subscriber.getPhase()).isEqualTo(Integer.MAX_VALUE);
    }

    @Test
    void start_setsRunningTrue() throws Exception {
        CountDownLatch subscribed = awaitSubscribe();
        subscriber.start();
        assertThat(subscribed.await(2, TimeUnit.SECONDS)).isTrue();
        assertThat(subscriber.isRunning()).isTrue();
    }

    @Test
    void start_subscribesToConfiguredTopicWithLeaseTimeout() throws Exception {
        CountDownLatch subscribed = awaitSubscribe();
        subscriber.start();
        subscribed.await(2, TimeUnit.SECONDS);
        verify(haClient).subscribe(any(), eq("/queue/trades"), eq(5000L));
    }

    @Test
    void stop_afterStart_setsRunningFalse() throws Exception {
        CountDownLatch subscribed = awaitSubscribe();
        subscriber.start();
        subscribed.await(2, TimeUnit.SECONDS);
        subscriber.stop();
        assertThat(subscriber.isRunning()).isFalse();
    }

    @Test
    void stop_callsUnsubscribeOnHaClient() throws Exception {
        CountDownLatch subscribed = awaitSubscribe();
        subscriber.start();
        subscribed.await(2, TimeUnit.SECONDS);
        subscriber.stop();
        verify(haClient).unsubscribe(commandId);
    }

    // ── Message handling ──────────────────────────────────────────────────────

    @Test
    void handleMessage_whenRunning_forwardsToDispatchService() throws Exception {
        CountDownLatch subscribed = awaitSubscribe();
        subscriber.start();
        subscribed.await(2, TimeUnit.SECONDS);

        Message msg = message("bm-single-1");
        invokeHandleMessage(msg);

        verify(dispatchService).dispatch(msg, haClient, semaphore);
    }

    @Test
    void handleMessage_whenStopped_skipsDispatch() throws Exception {
        // Do not start — running=false
        Message msg = message("bm-single-2");
        invokeHandleMessage(msg);
        verifyNoInteractions(dispatchService);
    }

    @Test
    void handleMessage_interruptedException_handledGracefully() throws Exception {
        CountDownLatch subscribed = awaitSubscribe();
        subscriber.start();
        subscribed.await(2, TimeUnit.SECONDS);

        doThrow(new InterruptedException("test"))
                .when(dispatchService).dispatch(any(), any(), any());

        Message msg = message("bm-single-3");
        // Must not propagate — exception is caught inside handleMessage
        invokeHandleMessage(msg);
    }

    @Test
    void handleMessage_dispatchException_handledGracefully() throws Exception {
        CountDownLatch subscribed = awaitSubscribe();
        subscriber.start();
        subscribed.await(2, TimeUnit.SECONDS);

        doThrow(new RuntimeException("unexpected"))
                .when(dispatchService).dispatch(any(), any(), any());

        Message msg = message("bm-single-4");
        invokeHandleMessage(msg); // must not propagate
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private CountDownLatch awaitSubscribe() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        when(haClient.subscribe(any(), anyString(), anyLong()))
                .thenAnswer(inv -> { latch.countDown(); return commandId; });
        return latch;
    }

    private void invokeHandleMessage(Message msg) throws Exception {
        Method m = SingleAmpsSubscriber.class.getDeclaredMethod("handleMessage", Message.class);
        m.setAccessible(true);
        m.invoke(subscriber, msg);
    }

    private static Message message(String bookmark) {
        Message m = mock(Message.class);
        lenient().when(m.getBookmark()).thenReturn(bookmark);
        return m;
    }
}
