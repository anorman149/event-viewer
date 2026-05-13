package org.eventviewer.ingest.config;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(classes = KafkaTopicConfig.class)
@EnableConfigurationProperties(KafkaTopicProperties.class)
@TestPropertySource(properties = {
        "event-ingest.kafka.topics[0].name=event-raw",
        "event-ingest.kafka.topics[0].partitions=3",
        "event-ingest.kafka.topics[0].replication-factor=1",
        "event-ingest.kafka.topics[0].dead-letter.name=event-raw-dlt",
        "event-ingest.kafka.topics[0].dead-letter.partitions=1",
        "event-ingest.kafka.topics[0].dead-letter.replication-factor=1",
        "spring.kafka.bootstrap-servers=localhost:29092",
        "spring.kafka.admin.fail-fast=false"
})
class KafkaTopicPropertiesTest {

    @Autowired
    KafkaTopicProperties properties;

    @Test
    void bindsMainTopicFields() {
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
}
