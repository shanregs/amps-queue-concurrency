package com.shan.mq.amps.ampsqueueconcurrency.publisher;

import com.crankuptheamps.client.HAClient;
import io.micrometer.core.instrument.Timer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class AmpsMessagePublisherTest {

    @Mock HAClient            ampsPublisherClient;
    @Mock MessagePayloadFactory payloadFactory;
    @Mock PublisherRateLimiter  rateLimiter;
    @Mock PublisherMetrics      metrics;

    @InjectMocks
    AmpsMessagePublisher publisher;

    @BeforeEach
    void setUp() {
        ReflectionTestUtils.setField(publisher, "topic",               "/queue/trades");
        ReflectionTestUtils.setField(publisher, "mode",                PublisherMode.RATE_LIMITED);
        ReflectionTestUtils.setField(publisher, "payloadTemplate",     PayloadTemplate.TRADE);
        ReflectionTestUtils.setField(publisher, "totalMessages",       0L);
        ReflectionTestUtils.setField(publisher, "concurrentPublishers", 1);
        ReflectionTestUtils.setField(publisher, "logProgressEvery",    1000L);
    }

    @AfterEach
    void tearDown() {
        if (publisher.isRunning()) publisher.stop();
    }

    // ── Lifecycle ─────────────────────────────────────────────────────────────

    @Test
    void isRunning_beforeStart_returnsFalse() {
        assertThat(publisher.isRunning()).isFalse();
    }

    @Test
    void start_setsRunningTrue() {
        publisher.start();
        assertThat(publisher.isRunning()).isTrue();
    }

    @Test
    void stop_afterStart_setsRunningFalse() {
        publisher.start();
        publisher.stop();
        assertThat(publisher.isRunning()).isFalse();
    }

    @Test
    void getPhase_returnsMaxIntMinusOne_stopsBeforeSubscriber() {
        assertThat(publisher.getPhase()).isEqualTo(Integer.MAX_VALUE - 1);
    }

    // ── Publish loop ──────────────────────────────────────────────────────────

    @Test
    void start_workersCallHaClientPublishOnCorrectTopic() throws Exception {
        Timer timer = timerThatRunsRunnable();
        when(metrics.publishTimer()).thenReturn(timer);
        when(payloadFactory.generate(any(), anyLong())).thenReturn("{\"seq\":1}");

        CountDownLatch firstPublish = new CountDownLatch(1);
        doAnswer(inv -> { firstPublish.countDown(); return null; })
                .when(ampsPublisherClient).publish(anyString(), anyString());

        publisher.start();
        assertThat(firstPublish.await(3, TimeUnit.SECONDS)).isTrue();

        verify(ampsPublisherClient, atLeastOnce()).publish(eq("/queue/trades"), anyString());
        verify(metrics, atLeastOnce()).recordPublished();
    }

    @Test
    void publishLoop_haClientThrows_recordsError_loopContinues() throws Exception {
        Timer timer = timerThatRunsRunnable();
        when(metrics.publishTimer()).thenReturn(timer);
        when(payloadFactory.generate(any(), anyLong())).thenReturn("{\"seq\":1}");

        // First call throws; second call succeeds → errorRecorded fires before the second publish
        AtomicInteger callCount = new AtomicInteger(0);
        CountDownLatch errorRecorded = new CountDownLatch(1);
        doAnswer(inv -> {
            if (callCount.incrementAndGet() == 1) throw new RuntimeException("AMPS unavailable");
            return null;
        }).when(ampsPublisherClient).publish(anyString(), anyString());
        doAnswer(inv -> { errorRecorded.countDown(); return null; }).when(metrics).recordError();

        publisher.start();
        assertThat(errorRecorded.await(3, TimeUnit.SECONDS)).isTrue();
        assertThat(publisher.isRunning()).isTrue(); // loop did not die
    }

    @Test
    void fixedCount_publishesExactlyTotalMessages_thenExitsLoop() throws Exception {
        ReflectionTestUtils.setField(publisher, "mode",          PublisherMode.FIXED_COUNT);
        ReflectionTestUtils.setField(publisher, "totalMessages", 3L);

        Timer timer = timerThatRunsRunnable();
        when(metrics.publishTimer()).thenReturn(timer);
        when(payloadFactory.generate(any(), anyLong())).thenReturn("{\"seq\":1}");

        CountDownLatch allPublished = new CountDownLatch(3);
        doAnswer(inv -> { allPublished.countDown(); return null; })
                .when(ampsPublisherClient).publish(anyString(), anyString());

        publisher.start();
        assertThat(allPublished.await(3, TimeUnit.SECONDS)).isTrue();

        // Let the loop fully settle before asserting the exact count
        Thread.sleep(100);
        publisher.stop();
        verify(ampsPublisherClient, times(3)).publish(anyString(), anyString());
    }

    @Test
    void stop_tracksWorkerLifecycle_viaMetrics() throws Exception {
        Timer timer = timerThatRunsRunnable();
        when(metrics.publishTimer()).thenReturn(timer);
        when(payloadFactory.generate(any(), anyLong())).thenReturn("{\"seq\":1}");

        CountDownLatch firstPublish = new CountDownLatch(1);
        doAnswer(inv -> { firstPublish.countDown(); return null; })
                .when(ampsPublisherClient).publish(anyString(), anyString());

        publisher.start();
        firstPublish.await(3, TimeUnit.SECONDS);
        publisher.stop();

        verify(metrics, atLeastOnce()).workerStarted();
        verify(metrics, atLeastOnce()).workerStopped();
    }

    @Test
    void start_multipleWorkers_allPublishConcurrently() throws Exception {
        ReflectionTestUtils.setField(publisher, "concurrentPublishers", 3);
        Timer timer = timerThatRunsRunnable();
        when(metrics.publishTimer()).thenReturn(timer);
        when(payloadFactory.generate(any(), anyLong())).thenReturn("{\"seq\":1}");

        // Expect at least 3 separate publish calls (one per worker)
        CountDownLatch threePublishes = new CountDownLatch(3);
        doAnswer(inv -> { threePublishes.countDown(); return null; })
                .when(ampsPublisherClient).publish(anyString(), anyString());

        publisher.start();
        assertThat(threePublishes.await(3, TimeUnit.SECONDS)).isTrue();
    }

    // ── Helper ────────────────────────────────────────────────────────────────

    private static Timer timerThatRunsRunnable() {
        Timer t = mock(Timer.class);
        doAnswer(inv -> { ((Runnable) inv.getArgument(0)).run(); return null; })
                .when(t).record(any(Runnable.class));
        return t;
    }
}
