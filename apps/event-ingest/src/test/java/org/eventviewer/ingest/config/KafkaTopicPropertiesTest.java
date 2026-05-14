package org.eventviewer.ingest.config;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(classes = KafkaTopicConfig.class)
@EnableConfigurationProperties(KafkaProperties.class)
@TestPropertySource(properties = {
        "event-ingest.kafka.topics[0].name=event-raw",
        "event-ingest.kafka.topics[0].partitions=3",
        "event-ingest.kafka.topics[0].replication-factor=1",
        "event-ingest.kafka.topics[0].dead-letter.name=event-raw-dlt",
        "event-ingest.kafka.topics[0].dead-letter.partitions=1",
        "event-ingest.kafka.topics[0].dead-letter.replication-factor=1",
        "event-ingest.kafka.lag-monitor.enabled=true",
        "event-ingest.kafka.lag-monitor.interval-ms=60000",
        "event-ingest.kafka.lag-monitor.consumer-group-ids=event-ingest-group",
        "spring.kafka.bootstrap-servers=localhost:29092",
        "spring.kafka.admin.fail-fast=false"
})
class KafkaTopicPropertiesTest {

    @Autowired
    KafkaProperties properties;

    @Test
    void bindsTopicFields() {
        var topic = properties.topics().get(0);
        assertThat(topic.name()).isEqualTo("event-raw");
        assertThat(topic.partitions()).isEqualTo(3);
        assertThat(topic.replicationFactor()).isEqualTo(1);
    }

    @Test
    void bindsDeadLetterFields() {
        var dlt = properties.topics().get(0).deadLetter();
        assertThat(dlt).isNotNull();
        assertThat(dlt.name()).isEqualTo("event-raw-dlt");
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
        var lagMonitor = new KafkaProperties.LagMonitor(true, 0L, null);
        assertThat(lagMonitor.intervalMs()).isEqualTo(60_000L);
        assertThat(lagMonitor.consumerGroupIds()).isEmpty();
    }
}
