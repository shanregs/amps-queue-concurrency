package com.shan.mq.amps.ampsqueueconcurrency.controller;

import com.shan.mq.amps.ampsqueueconcurrency.publisher.AmpsMessagePublisher;
import com.shan.mq.amps.ampsqueueconcurrency.subscriber.MultiAmpsSubscriberPool;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import static org.hamcrest.Matchers.*;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

/**
 * Unit tests for AmpsLifecycleController using MockMvc standalone setup.
 *
 * singleSubscriber is intentionally absent (not declared as @Mock) — simulates
 * the multi-subscriber profile where only MultiAmpsSubscriberPool is active.
 * Tests requiring a null subscriber or null publisher set the field to null via
 * ReflectionTestUtils inside those test methods.
 */
@ExtendWith(MockitoExtension.class)
class AmpsLifecycleControllerTest {

    @Mock MultiAmpsSubscriberPool multiSubscriberPool;
    @Mock AmpsMessagePublisher    publisher;

    @InjectMocks
    AmpsLifecycleController controller;

    MockMvc mockMvc;

    @BeforeEach
    void setUp() {
        mockMvc = MockMvcBuilders.standaloneSetup(controller).build();
    }

    // ── Combined status ───────────────────────────────────────────────────────

    @Nested
    class CombinedStatus {

        @Test
        void status_subscriberRunning_publisherUnavailable() throws Exception {
            when(multiSubscriberPool.isRunning()).thenReturn(true);
            nullifyPublisher();

            mockMvc.perform(get("/api/amps/status"))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$.subscriber.available").value(true))
                    .andExpect(jsonPath("$.subscriber.running").value(true))
                    .andExpect(jsonPath("$.subscriber.type").value("MultiAmpsSubscriberPool"))
                    .andExpect(jsonPath("$.publisher.available").value(false))
                    .andExpect(jsonPath("$.publisher.running").value(false));
        }

        @Test
        void status_subscriberStopped_publisherRunning() throws Exception {
            when(multiSubscriberPool.isRunning()).thenReturn(false);
            when(publisher.isRunning()).thenReturn(true);

            mockMvc.perform(get("/api/amps/status"))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$.subscriber.available").value(true))
                    .andExpect(jsonPath("$.subscriber.running").value(false))
                    .andExpect(jsonPath("$.publisher.available").value(true))
                    .andExpect(jsonPath("$.publisher.running").value(true))
                    .andExpect(jsonPath("$.publisher.type").value("AmpsMessagePublisher"));
        }

        @Test
        void status_noBeansActive_allUnavailable() throws Exception {
            nullifySubscriber();
            nullifyPublisher();

            mockMvc.perform(get("/api/amps/status"))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$.subscriber.available").value(false))
                    .andExpect(jsonPath("$.subscriber.running").value(false))
                    .andExpect(jsonPath("$.publisher.available").value(false))
                    .andExpect(jsonPath("$.publisher.running").value(false));
        }
    }

    // ── Subscriber: status ────────────────────────────────────────────────────

    @Nested
    class SubscriberStatus {

        @Test
        void status_whenRunning_returnsAvailableAndRunning() throws Exception {
            when(multiSubscriberPool.isRunning()).thenReturn(true);

            mockMvc.perform(get("/api/amps/subscriber/status"))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$.available").value(true))
                    .andExpect(jsonPath("$.running").value(true))
                    .andExpect(jsonPath("$.type").value("MultiAmpsSubscriberPool"));
        }

        @Test
        void status_whenStopped_returnsAvailableButNotRunning() throws Exception {
            when(multiSubscriberPool.isRunning()).thenReturn(false);

            mockMvc.perform(get("/api/amps/subscriber/status"))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$.available").value(true))
                    .andExpect(jsonPath("$.running").value(false));
        }

        @Test
        void status_noBeanActive_returnsUnavailable() throws Exception {
            nullifySubscriber();

            mockMvc.perform(get("/api/amps/subscriber/status"))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$.available").value(false))
                    .andExpect(jsonPath("$.running").value(false));
        }
    }

    // ── Subscriber: start ─────────────────────────────────────────────────────

    @Nested
    class SubscriberStart {

        @Test
        void start_whenStopped_callsStartAndReturnsStarted() throws Exception {
            when(multiSubscriberPool.isRunning()).thenReturn(false);

            mockMvc.perform(post("/api/amps/subscriber/start"))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$.action").value("STARTED"))
                    .andExpect(jsonPath("$.running").value(true))
                    .andExpect(jsonPath("$.component").value("subscriber"));

            verify(multiSubscriberPool).start();
        }

        @Test
        void start_whenAlreadyRunning_doesNotCallStartAgain() throws Exception {
            when(multiSubscriberPool.isRunning()).thenReturn(true);

            mockMvc.perform(post("/api/amps/subscriber/start"))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$.action").value("ALREADY_RUNNING"))
                    .andExpect(jsonPath("$.running").value(true));

            verify(multiSubscriberPool, never()).start();
        }

        @Test
        void start_noBeanActive_returnsNotAvailable() throws Exception {
            nullifySubscriber();

            mockMvc.perform(post("/api/amps/subscriber/start"))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$.action").value("NOT_AVAILABLE"));
        }
    }

    // ── Subscriber: stop ──────────────────────────────────────────────────────

    @Nested
    class SubscriberStop {

        @Test
        void stop_whenRunning_callsStopAndReturnsStopped() throws Exception {
            when(multiSubscriberPool.isRunning()).thenReturn(true);

            mockMvc.perform(post("/api/amps/subscriber/stop"))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$.action").value("STOPPED"))
                    .andExpect(jsonPath("$.running").value(false))
                    .andExpect(jsonPath("$.component").value("subscriber"));

            verify(multiSubscriberPool).stop();
        }

        @Test
        void stop_whenAlreadyStopped_doesNotCallStopAgain() throws Exception {
            when(multiSubscriberPool.isRunning()).thenReturn(false);

            mockMvc.perform(post("/api/amps/subscriber/stop"))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$.action").value("ALREADY_STOPPED"))
                    .andExpect(jsonPath("$.running").value(false));

            verify(multiSubscriberPool, never()).stop();
        }

        @Test
        void stop_noBeanActive_returnsNotAvailable() throws Exception {
            nullifySubscriber();

            mockMvc.perform(post("/api/amps/subscriber/stop"))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$.action").value("NOT_AVAILABLE"));
        }
    }

    // ── Publisher: status ─────────────────────────────────────────────────────

    @Nested
    class PublisherStatus {

        @Test
        void status_whenRunning_returnsAvailableAndRunning() throws Exception {
            when(publisher.isRunning()).thenReturn(true);

            mockMvc.perform(get("/api/amps/publisher/status"))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$.available").value(true))
                    .andExpect(jsonPath("$.running").value(true))
                    .andExpect(jsonPath("$.type").value("AmpsMessagePublisher"));
        }

        @Test
        void status_noBeanActive_returnsUnavailable() throws Exception {
            nullifyPublisher();

            mockMvc.perform(get("/api/amps/publisher/status"))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$.available").value(false))
                    .andExpect(jsonPath("$.running").value(false));
        }
    }

    // ── Publisher: start ──────────────────────────────────────────────────────

    @Nested
    class PublisherStart {

        @Test
        void start_whenStopped_callsStartAndReturnsStarted() throws Exception {
            when(publisher.isRunning()).thenReturn(false);

            mockMvc.perform(post("/api/amps/publisher/start"))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$.action").value("STARTED"))
                    .andExpect(jsonPath("$.running").value(true))
                    .andExpect(jsonPath("$.component").value("publisher"));

            verify(publisher).start();
        }

        @Test
        void start_whenAlreadyRunning_doesNotCallStartAgain() throws Exception {
            when(publisher.isRunning()).thenReturn(true);

            mockMvc.perform(post("/api/amps/publisher/start"))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$.action").value("ALREADY_RUNNING"));

            verify(publisher, never()).start();
        }

        @Test
        void start_noBeanActive_returnsNotAvailable() throws Exception {
            nullifyPublisher();

            mockMvc.perform(post("/api/amps/publisher/start"))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$.action").value("NOT_AVAILABLE"));
        }
    }

    // ── Publisher: stop ───────────────────────────────────────────────────────

    @Nested
    class PublisherStop {

        @Test
        void stop_whenRunning_callsStopAndReturnsStopped() throws Exception {
            when(publisher.isRunning()).thenReturn(true);

            mockMvc.perform(post("/api/amps/publisher/stop"))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$.action").value("STOPPED"))
                    .andExpect(jsonPath("$.running").value(false))
                    .andExpect(jsonPath("$.component").value("publisher"));

            verify(publisher).stop();
        }

        @Test
        void stop_whenAlreadyStopped_doesNotCallStopAgain() throws Exception {
            when(publisher.isRunning()).thenReturn(false);

            mockMvc.perform(post("/api/amps/publisher/stop"))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$.action").value("ALREADY_STOPPED"));

            verify(publisher, never()).stop();
        }

        @Test
        void stop_noBeanActive_returnsNotAvailable() throws Exception {
            nullifyPublisher();

            mockMvc.perform(post("/api/amps/publisher/stop"))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$.action").value("NOT_AVAILABLE"));
        }
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private void nullifySubscriber() {
        ReflectionTestUtils.setField(controller, "multiSubscriberPool", null);
    }

    private void nullifyPublisher() {
        ReflectionTestUtils.setField(controller, "publisher", null);
    }
}
