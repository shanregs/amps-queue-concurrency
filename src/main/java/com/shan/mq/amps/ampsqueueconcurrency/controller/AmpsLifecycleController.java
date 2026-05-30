package com.shan.mq.amps.ampsqueueconcurrency.controller;

import com.shan.mq.amps.ampsqueueconcurrency.publisher.AmpsMessagePublisher;
import com.shan.mq.amps.ampsqueueconcurrency.subscriber.MultiAmpsSubscriberPool;
import com.shan.mq.amps.ampsqueueconcurrency.subscriber.SingleAmpsSubscriber;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.SmartLifecycle;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * REST API for manually starting and stopping the AMPS publisher and subscriber
 * components at runtime. All operations are idempotent — starting an already-running
 * component or stopping an already-stopped one returns a descriptive status instead
 * of erroring.
 *
 * Endpoints:
 *   GET  /api/amps/status              — combined publisher + subscriber status
 *   GET  /api/amps/subscriber/status   — subscriber running state
 *   POST /api/amps/subscriber/start    — start subscriber (no-op if already running)
 *   POST /api/amps/subscriber/stop     — stop subscriber  (no-op if already stopped)
 *   GET  /api/amps/publisher/status    — publisher running state
 *   POST /api/amps/publisher/start     — start publisher  (no-op if already running)
 *   POST /api/amps/publisher/stop      — stop publisher   (no-op if already stopped)
 *
 * Beans are optional because they are profile-gated:
 *   SingleAmpsSubscriber   → "single-subscriber"
 *   MultiAmpsSubscriberPool→ "multi-subscriber" | "multi-jvm-subscriber"
 *   AmpsMessagePublisher   → "message-publisher"
 */
@Slf4j
@RestController
@RequestMapping("/api/amps")
public class AmpsLifecycleController {

    @Autowired(required = false) private SingleAmpsSubscriber singleSubscriber;
    @Autowired(required = false) private MultiAmpsSubscriberPool multiSubscriberPool;
    @Autowired(required = false) private AmpsMessagePublisher publisher;

    // ── Combined status ───────────────────────────────────────────────────────

    @GetMapping("/status")
    public ResponseEntity<Map<String, Object>> status() {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("subscriber", componentStatus(activeSubscriber()));
        result.put("publisher", componentStatus(publisher));
        return ResponseEntity.ok(result);
    }

    // ── Subscriber endpoints ──────────────────────────────────────────────────

    @GetMapping("/subscriber/status")
    public ResponseEntity<Map<String, Object>> subscriberStatus() {
        return ResponseEntity.ok(componentStatus(activeSubscriber()));
    }

    @PostMapping("/subscriber/start")
    public ResponseEntity<Map<String, Object>> startSubscriber() {
        SmartLifecycle sub = activeSubscriber();
        if (sub == null) {
            return ResponseEntity.ok(action("subscriber", false, "NOT_AVAILABLE",
                    "No subscriber bean is active — activate single-subscriber, multi-subscriber, or multi-jvm-subscriber profile"));
        }
        if (sub.isRunning()) {
            return ResponseEntity.ok(action("subscriber", true, "ALREADY_RUNNING",
                    "Subscriber is already running"));
        }
        sub.start();
        log.info("Subscriber {} started via REST", sub.getClass().getSimpleName());
        return ResponseEntity.ok(action("subscriber", true, "STARTED", "Subscriber started successfully"));
    }

    @PostMapping("/subscriber/stop")
    public ResponseEntity<Map<String, Object>> stopSubscriber() {
        SmartLifecycle sub = activeSubscriber();
        if (sub == null) {
            return ResponseEntity.ok(action("subscriber", false, "NOT_AVAILABLE",
                    "No subscriber bean is active in this profile"));
        }
        if (!sub.isRunning()) {
            return ResponseEntity.ok(action("subscriber", false, "ALREADY_STOPPED",
                    "Subscriber is already stopped"));
        }
        sub.stop();
        log.info("Subscriber {} stopped via REST", sub.getClass().getSimpleName());
        return ResponseEntity.ok(action("subscriber", false, "STOPPED", "Subscriber stopped successfully"));
    }

    // ── Publisher endpoints ───────────────────────────────────────────────────

    @GetMapping("/publisher/status")
    public ResponseEntity<Map<String, Object>> publisherStatus() {
        return ResponseEntity.ok(componentStatus(publisher));
    }

    @PostMapping("/publisher/start")
    public ResponseEntity<Map<String, Object>> startPublisher() {
        if (publisher == null) {
            return ResponseEntity.ok(action("publisher", false, "NOT_AVAILABLE",
                    "message-publisher profile is not active"));
        }
        if (publisher.isRunning()) {
            return ResponseEntity.ok(action("publisher", true, "ALREADY_RUNNING",
                    "Publisher is already running"));
        }
        publisher.start();
        log.info("Publisher started via REST");
        return ResponseEntity.ok(action("publisher", true, "STARTED", "Publisher started successfully"));
    }

    @PostMapping("/publisher/stop")
    public ResponseEntity<Map<String, Object>> stopPublisher() {
        if (publisher == null) {
            return ResponseEntity.ok(action("publisher", false, "NOT_AVAILABLE",
                    "message-publisher profile is not active"));
        }
        if (!publisher.isRunning()) {
            return ResponseEntity.ok(action("publisher", false, "ALREADY_STOPPED",
                    "Publisher is already stopped"));
        }
        publisher.stop();
        log.info("Publisher stopped via REST");
        return ResponseEntity.ok(action("publisher", false, "STOPPED", "Publisher stopped successfully"));
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    /** Returns whichever subscriber bean is present in the current profile. */
    private SmartLifecycle activeSubscriber() {
        if (singleSubscriber != null) return singleSubscriber;
        if (multiSubscriberPool != null) return multiSubscriberPool;
        return null;
    }

    /** Status-only response — no action field. */
    private Map<String, Object> componentStatus(SmartLifecycle component) {
        Map<String, Object> map = new LinkedHashMap<>();
        if (component == null) {
            map.put("available", false);
            map.put("running", false);
            return map;
        }
        map.put("available", true);
        map.put("type", component.getClass().getSimpleName());
        map.put("running", component.isRunning());
        return map;
    }

    /** Action response — includes action and message fields. */
    private Map<String, Object> action(String component, boolean running, String action, String message) {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("component", component);
        map.put("running", running);
        map.put("action", action);
        map.put("message", message);
        return map;
    }
}
