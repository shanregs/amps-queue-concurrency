package com.shan.mq.amps.ampsqueueconcurrency.config;

import com.crankuptheamps.client.DefaultServerChooser;
import com.crankuptheamps.client.HAClient;
import com.shan.mq.amps.ampsqueueconcurrency.exception.AmpsConnectionException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

/**
 * AMPS configuration for the {@code message-publisher} profile.
 *
 * A single shared HAClient is used by all publisher VT workers.
 * {@code publish()} is thread-safe in the AMPS client — no per-worker connection needed.
 *
 * No bookmark store is configured: publishers do not track acknowledgements.
 * No database: the publisher is stateless.
 */
@Slf4j
@Configuration
@Profile("message-publisher")
public class PublisherConfig {

    @Value("${amps.server.uri}")
    private String serverUri;

    @Value("${amps.client.name:amps-queue-concurrency-publisher}")
    private String clientName;

    @Bean(name = "ampsPublisherClient", destroyMethod = "close")
    public HAClient ampsPublisherClient() {
        try {
            HAClient client = new HAClient(clientName);

            DefaultServerChooser chooser = new DefaultServerChooser();
            chooser.add(serverUri);
            client.setServerChooser(chooser);

            client.connect(serverUri);
            log.info("Publisher HAClient connected: name={} uri={}", clientName, serverUri);
            return client;
        } catch (Exception e) {
            throw new AmpsConnectionException(
                    "Failed to connect publisher HAClient to " + serverUri, e);
        }
    }
}
