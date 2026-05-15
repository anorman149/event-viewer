package org.eventviewer.ingest.config;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import static org.assertj.core.api.Assertions.assertThat;

@SpringJUnitConfig(KafkaTopicPropertiesTest.Config.class)
@TestPropertySource(properties = {
        "event-ingest.kafka.topics[0].name=event-raw-1",
        "event-ingest.kafka.topics[0].partitions=2",
        "event-ingest.kafka.topics[0].replication-factor=1",
        "event-ingest.kafka.dead-letter-topics[0].name=event-raw-1.DLT",
        "event-ingest.kafka.dead-letter-topics[0].partitions=1",
        "event-ingest.kafka.dead-letter-topics[0].replication-factor=1",
        "event-ingest.kafka.lag-monitor.enabled=true",
        "event-ingest.kafka.lag-monitor.interval-ms=60000",
        "event-ingest.kafka.lag-monitor.consumer-group-ids=event-ingest-group",
})
class KafkaTopicPropertiesTest {

    @Configuration
    @EnableConfigurationProperties(EventKafkaProperties.class)
    static class Config {}

    @Autowired
    EventKafkaProperties properties;

    @Test
    void bindsTopicFields() {
        var topic = properties.topics().get(0);
        assertThat(topic.name()).isEqualTo("event-raw-1");
        assertThat(topic.partitions()).isEqualTo(2);
        assertThat(topic.replicationFactor()).isEqualTo(1);
    }

    @Test
    void bindsDeadLetterTopicFields() {
        var dlt = properties.deadLetterTopics().get(0);
        assertThat(dlt).isNotNull();
        assertThat(dlt.name()).isEqualTo("event-raw-1.DLT");
        assertThat(dlt.partitions()).isEqualTo(1);
        assertThat(dlt.replicationFactor()).isEqualTo(1);
    }

    @Test
    void bindsLagMonitorFields() {
        var lagMonitor = properties.lagMonitor();
        assertThat(lagMonitor.enabled()).isTrue();
        assertThat(lagMonitor.intervalMs()).isEqualTo(60000L);
        assertThat(lagMonitor.consumerGroupIds()).containsExactly("event-ingest-group");
    }

    @Test
    void lagMonitorDefaultIntervalMs() {
        var lagMonitor = new EventKafkaProperties.LagMonitor(true, 0L, null);
        assertThat(lagMonitor.intervalMs()).isEqualTo(60_000L);
        assertThat(lagMonitor.consumerGroupIds()).isEmpty();
    }
}
