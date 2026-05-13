package org.eventviewer.ingest.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.List;

@ConfigurationProperties(prefix = "event-ingest.kafka")
public record KafkaTopicProperties(List<TopicDefinition> topics) {

    public record TopicDefinition(
            String name,
            int partitions,
            int replicationFactor,
            DeadLetter deadLetter
    ) {
        public record DeadLetter(
                String name,
                int partitions,
                int replicationFactor
        ) {}
    }
}
