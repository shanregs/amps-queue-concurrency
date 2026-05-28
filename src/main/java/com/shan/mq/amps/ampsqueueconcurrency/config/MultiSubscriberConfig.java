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

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Semaphore;

/**
 * Spring configuration for the {@code multi-subscriber} profile.
 *
 * <p>Creates N isolated {@link SubscriberContext} instances within a <strong>single JVM</strong>.
 * Each context bundles its own HAClient connection and its own Semaphore, so:
 *
 * <ul>
 *   <li>Multiple platform subscriber threads pull from the same AMPS queue concurrently</li>
 *   <li>One slow subscriber does <em>not</em> starve the others (per-subscriber semaphores)</li>
 *   <li>Total in-flight capacity = {@code subscriberCount × maxConcurrencyPerSubscriber}</li>
 * </ul>
 *
 * <p>All connections are established eagerly at startup — fail-fast is intentional.
 * If any connection fails, Spring aborts boot rather than starting with a partial pool.
 */
@Slf4j
@Configuration
@Profile("multi-subscriber")
public class MultiSubscriberConfig {

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
     * Creates {@code subscriberCount} subscriber contexts, each with an isolated
     * HAClient and a per-subscriber Semaphore.
     *
     * <p>Client names follow the pattern {@code {base}-sub-{index}} (e.g.,
     * {@code amps-queue-concurrency-sub-1}). Each gets its own bookmark store file
     * so bookmark state does not cross-contaminate between subscriber threads.
     */
    @Bean
    public List<SubscriberContext> subscriberContexts() {
        try {
            Path bookmarkPath = Path.of(bookmarkDir);
            Files.createDirectories(bookmarkPath);

            List<SubscriberContext> contexts = new ArrayList<>(subscriberCount);

            for (int i = 1; i <= subscriberCount; i++) {
                String clientName = clientNameBase + "-sub-" + i;
                HAClient client   = buildHaClient(clientName, bookmarkPath);
                Semaphore sem     = new Semaphore(maxConcurrencyPerSubscriber, true);
                contexts.add(new SubscriberContext(client, sem, i));
                log.info("Subscriber context #{} ready: name={} permits={}",
                        i, clientName, maxConcurrencyPerSubscriber);
            }

            log.info("Multi-subscriber pool created: {} subscribers, {}×{} max in-flight",
                    subscriberCount, subscriberCount, maxConcurrencyPerSubscriber);
            return contexts;

        } catch (AmpsConnectionException e) {
            throw e;
        } catch (Exception e) {
            throw new AmpsConnectionException("Failed to build multi-subscriber contexts", e);
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
                    "Connection failed for subscriber name=" + clientName + " uri=" + serverUri, e);
        }
    }
}
