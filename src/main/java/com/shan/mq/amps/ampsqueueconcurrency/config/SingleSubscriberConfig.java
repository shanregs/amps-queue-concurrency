package com.shan.mq.amps.ampsqueueconcurrency.config;

import com.crankuptheamps.client.DefaultServerChooser;
import com.crankuptheamps.client.HAClient;
import com.crankuptheamps.client.LoggedBookmarkStore;
import com.crankuptheamps.client.ExponentialDelayStrategy;
import com.shan.mq.amps.ampsqueueconcurrency.exception.AmpsConnectionException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.Semaphore;

/**
 * Spring configuration for the {@code single-subscriber} profile.
 *
 * <p>Creates exactly <strong>one HAClient</strong> and <strong>one Semaphore</strong>.
 * The single platform subscriber thread uses the semaphore as a backpressure gate:
 * it blocks when {@code maxConcurrency} virtual threads are already processing,
 * naturally slowing AMPS message receipt without any active coordination.
 *
 * <h3>ACK constraint</h3>
 * AMPS requires ACKs to be sent on the <em>same connection</em> that received the
 * message. This HAClient bean is therefore injected all the way into the virtual-thread
 * dispatch — {@code haClient.ack(message)} is called there, not in the subscriber.
 */
@Slf4j
@Configuration
@Profile("single-subscriber")
public class SingleSubscriberConfig {

    @Value("${amps.server.uri}")
    private String serverUri;

    @Value("${amps.client.name:amps-queue-concurrency-sub-1}")
    private String clientName;

    @Value("${amps.consumer.max-concurrency:100}")
    private int maxConcurrency;

    @Value("${amps.consumer.bookmark-dir:${user.home}/.amps/bookmarks}")
    private String bookmarkDir;

    /**
     * Single AMPS subscriber connection.
     *
     * <p>Connection features:
     * <ul>
     *   <li>{@link DefaultServerChooser} — add more URIs for automatic failover</li>
     *   <li>{@link LoggedBookmarkStore} — persists bookmark to disk; on JVM restart AMPS
     *       replays all messages received since the last acknowledged bookmark</li>
     *   <li>{@link ExponentialDelayStrategy} — exponential back-off on reconnect
     *       (adjust min/max delays via SDK setters to match your environment)</li>
     * </ul>
     *
     * <p>The bean {@code destroyMethod = "close"} ensures TCP teardown during
     * graceful shutdown.
     */
    @Bean(destroyMethod = "close")
    public HAClient ampsHaClient() {
        try {
            Files.createDirectories(Path.of(bookmarkDir));

            HAClient client = new HAClient(clientName);

            // Server chooser — add additional URIs for HA failover
            DefaultServerChooser chooser = new DefaultServerChooser();
            chooser.add(serverUri);
            client.setServerChooser(chooser);

            // Durable bookmark store — survives JVM restarts
            String bookmarkFile = bookmarkDir + "/" + clientName + ".log";
            client.setBookmarkStore(new LoggedBookmarkStore(bookmarkFile));

            // Exponential back-off reconnect strategy.
            // Note: ExponentialDelayStrategy has no two-arg constructor in AMPS 5.x.
            // Use default constructor; configure min/max via setters if your SDK exposes them.
            client.setReconnectDelayStrategy(new ExponentialDelayStrategy());

            client.connect(serverUri);
            log.info("Single-subscriber HAClient connected: name={} uri={}", clientName, serverUri);
            return client;

        } catch (Exception e) {
            throw new AmpsConnectionException(
                    "Failed to connect single-subscriber HAClient to " + serverUri, e);
        }
    }

    /**
     * Backpressure gate for the single subscriber.
     *
     * <p>The subscriber platform thread calls {@code semaphore.acquire()} before
     * dispatching each message. When {@code maxConcurrency} permits are exhausted
     * (all VTs are busy), the subscriber blocks here — AMPS messages are not polled,
     * their leases remain active server-side, and the queue acts as the buffer.
     *
     * <p>Fair mode ({@code true}) prevents subscriber starvation under sustained load.
     */
    @Bean
    public Semaphore ampsSemaphore() {
        log.info("Single-subscriber semaphore: maxConcurrency={}", maxConcurrency);
        return new Semaphore(maxConcurrency, true /* fair */);
    }
}
