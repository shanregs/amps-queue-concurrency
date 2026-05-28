package com.shan.mq.amps.ampsqueueconcurrency.config;

import com.crankuptheamps.client.DefaultServerChooser;
import com.crankuptheamps.client.HAClient;
import com.crankuptheamps.client.LoggedBookmarkStore;
import com.crankuptheamps.client.ExponentialDelayStrategy;
import com.shan.mq.amps.ampsqueueconcurrency.exception.AmpsConnectionException;
import com.shan.mq.amps.ampsqueueconcurrency.model.SubscriberContext;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Semaphore;

/**
 * Spring configuration for the {@code multi-jvm-subscriber} profile.
 *
 * <p>Functionally identical to {@link MultiSubscriberConfig} but each AMPS client name
 * is qualified with the host's hostname:
 * <pre>
 *   Pattern:  {base}-{hostname}-sub-{index}
 *   Example:  amps-queue-concurrency-prod-node-01-sub-2
 * </pre>
 *
 * <h3>Why hostname-qualified names are mandatory</h3>
 * AMPS uses the client name as the bookmark identity. If two JVMs share the same client
 * name, their bookmark files point to the same AMPS cursor — one JVM may acknowledge
 * messages the other has not yet processed, causing silent message loss.
 * Hostname-qualification guarantees global uniqueness across the entire deployment.
 *
 * <h3>Database</h3>
 * This profile expects a shared PostgreSQL instance (see
 * {@code application-multi-jvm-subscriber.yaml}). All JVMs write to the same
 * {@code processed_messages} table. The UNIQUE constraint on {@code message_id} provides
 * cross-JVM idempotency: whichever JVM inserts first wins; the other catches a
 * {@code DataIntegrityViolationException} and ACKs the duplicate harmlessly.
 */
@Slf4j
@Configuration
@Profile("multi-jvm-subscriber")
public class MultiJvmSubscriberConfig {

    @Value("${amps.server.uri}")
    private String serverUri;

    @Value("${amps.client.name:amps-queue-concurrency}")
    private String clientNameBase;

    @Value("${amps.consumer.subscriber-count:3}")
    private int subscriberCount;

    @Value("${amps.consumer.max-concurrency-per-subscriber:50}")
    private int maxConcurrencyPerSubscriber;

    @Value("${amps.consumer.bookmark-dir:${user.home}/.amps/bookmarks}")
    private String bookmarkDir;

    /**
     * Creates {@code subscriberCount} subscriber contexts, each with a globally
     * unique client name ({@code base-hostname-sub-index}).
     */
    @Bean
    public List<SubscriberContext> subscriberContexts() {
        try {
            String hostname   = safeHostname();
            Path bookmarkPath = Path.of(bookmarkDir);
            Files.createDirectories(bookmarkPath);

            List<SubscriberContext> contexts = new ArrayList<>(subscriberCount);

            for (int i = 1; i <= subscriberCount; i++) {
                String clientName = clientNameBase + "-" + hostname + "-sub-" + i;
                HAClient client   = buildHaClient(clientName, bookmarkPath);
                Semaphore sem     = new Semaphore(maxConcurrencyPerSubscriber, true);
                contexts.add(new SubscriberContext(client, sem, i));
                log.info("Multi-JVM subscriber context #{} ready: name={} host={}",
                        i, clientName, hostname);
            }

            log.info("Multi-JVM pool created: {} subscribers on host={}", subscriberCount, hostname);
            return contexts;

        } catch (AmpsConnectionException e) {
            throw e;
        } catch (Exception e) {
            throw new AmpsConnectionException("Failed to build multi-JVM subscriber contexts", e);
        }
    }

    // ── Private helpers ──────────────────────────────────────────────────────

    private HAClient buildHaClient(String clientName, Path bookmarkPath) {
        try {
            HAClient client = new HAClient(clientName);

            DefaultServerChooser chooser = new DefaultServerChooser();
            chooser.add(serverUri);
            client.setServerChooser(chooser);

            String bookmarkFile = bookmarkPath + "/" + clientName + ".log";
            client.setBookmarkStore(new LoggedBookmarkStore(bookmarkFile));

            client.setReconnectDelayStrategy(new ExponentialDelayStrategy());
            client.connect(serverUri);
            return client;

        } catch (Exception e) {
            throw new AmpsConnectionException(
                    "Connection failed for multi-JVM client=" + clientName, e);
        }
    }

    /**
     * Returns a sanitized hostname safe for use in AMPS client names.
     * Non-alphanumeric characters (dots, underscores, etc.) are replaced with hyphens.
     */
    private String safeHostname() {
        try {
            return InetAddress.getLocalHost().getHostName()
                    .replaceAll("[^a-zA-Z0-9\\-]", "-")
                    .toLowerCase();
        } catch (Exception e) {
            log.warn("Cannot resolve hostname — using 'unknown'. " +
                     "Set amps.client.name explicitly to avoid naming collisions.");
            return "unknown";
        }
    }
}
