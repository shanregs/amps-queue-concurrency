package com.shan.mq.amps.ampsqueueconcurrency.health;

import com.crankuptheamps.client.ConnectionInfo;
import com.crankuptheamps.client.HAClient;
import com.shan.mq.amps.ampsqueueconcurrency.model.SubscriberContext;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Spring Actuator health indicator for AMPS connectivity.
 *
 * Works across all active profiles:
 *   single-subscriber     → checks ampsHaClient bean
 *   multi-subscriber      → checks first SubscriberContext
 *   multi-jvm-subscriber  → checks first SubscriberContext
 *   message-publisher     → checks ampsPublisherClient bean
 *
 * Exposed at GET /actuator/health (included in "health" group).
 */
@Slf4j
@Component
public class AmpsHealthIndicator implements HealthIndicator {

    @Autowired(required = false)
    private HAClient ampsHaClient;                      // single-subscriber

    @Autowired(required = false)
    private HAClient ampsPublisherClient;               // message-publisher

    @Autowired(required = false)
    private List<SubscriberContext> subscriberContexts; // multi profiles

    @Override
    public Health health() {
        if (ampsHaClient != null) {
            return probe(ampsHaClient, "single-subscriber");
        }
        if (subscriberContexts != null && !subscriberContexts.isEmpty()) {
            return probe(subscriberContexts.get(0).haClient(),
                    "multi-subscriber[" + subscriberContexts.size() + "]");
        }
        if (ampsPublisherClient != null) {
            return probe(ampsPublisherClient, "message-publisher");
        }
        return Health.unknown()
                .withDetail("message", "No AMPS client active in this profile")
                .build();
    }

    private Health probe(HAClient client, String mode) {
        try {

            ConnectionInfo connectionInfo = client.getConnectionInfo();
            log.info("AMPS health probe mode={} uri={} version={}", mode, connectionInfo.get("client.uri"), client.getServerVersion());
            return Health.up()
                    .withDetail("mode", mode)
                    .withDetail("serverVersion", client.getServerVersion())
                    .build();

        } catch (Exception e) {

            log.warn("AMPS health probe failed mode={} error={}", mode, e.getMessage());

            return Health.down(e)
                    .withDetail("mode", mode)
                    .withDetail("reason", "AMPS connection unavailable")
                    .build();
        }
/*        try {
            boolean connected = client.isConnected();
            if (connected) {
                return Health.up()
                        .withDetail("mode", mode)
                        .withDetail("serverUri", client.getServerVersion())
                        .build();
            }
            return Health.down()
                    .withDetail("mode", mode)
                    .withDetail("reason", "HAClient reports disconnected")
                    .build();
        } catch (Exception e) {
            log.warn("AMPS health probe failed mode={} error={}", mode, e.getMessage());
            return Health.down(e)
                    .withDetail("mode", mode)
                    .build();
        }*/
    }
}
