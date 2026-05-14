package org.eventviewer.ingest.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;

import java.util.ArrayList;
import java.util.List;

@Configuration
@EnableConfigurationProperties(KafkaProperties.class)
public class KafkaTopicConfig {

    @Bean
    public KafkaAdmin.NewTopics allTopics(KafkaProperties properties) {
        List<NewTopic> topics = new ArrayList<>();
        for (KafkaProperties.TopicDefinition def : properties.topics()) {
            topics.add(TopicBuilder.name(def.name())
                    .partitions(def.partitions())
                    .replicas(def.replicationFactor())
                    .build());
            if (def.deadLetter() != null) {
                topics.add(TopicBuilder.name(def.deadLetter().name())
                        .partitions(def.deadLetter().partitions())
                        .replicas(def.deadLetter().replicationFactor())
                        .build());
            }
        }
        return new KafkaAdmin.NewTopics(topics.toArray(new NewTopic[0]));
    }
}
