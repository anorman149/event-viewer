package org.eventviewer.ingest.config;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.eventviewer.ingest.kafka.consumer.EventBatchListener;
import org.eventviewer.ingest.kafka.consumer.DltBatchMessageListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.util.backoff.ExponentialBackOff;
import org.springframework.util.backoff.FixedBackOff;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

@Configuration
@EnableConfigurationProperties({EventKafkaProperties.class, EventConsumerProperties.class})
public class KafkaConsumerConfig implements SmartLifecycle {
    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerConfig.class);

    private final List<ConcurrentMessageListenerContainer<String, String>> containers;
    private final EventConsumerProperties consumerProperties;
    private final EventKafkaProperties eventKafkaProperties;
    private final KafkaProperties kafkaProperties;
    private volatile boolean running = false;

    public KafkaConsumerConfig(
            EventKafkaProperties eventKafkaProperties,
            EventConsumerProperties consumerProperties,
            ConsumerFactory<String, String> consumerFactory,
            EventBatchListener eventBatchListener,
            DltBatchMessageListener dltBatchMessageListener,
            KafkaTemplate<String, String> kafkaTemplate,
            MeterRegistry meterRegistry,
            KafkaProperties kafkaProperties) {
        this.consumerProperties = consumerProperties;
        this.eventKafkaProperties = eventKafkaProperties;
        this.kafkaProperties = kafkaProperties;

        Counter dltExhaustedCounter = Counter.builder("dlt.exhausted")
                .description("DLT events that exhausted all retry attempts")
                .register(meterRegistry);

        List<ConcurrentMessageListenerContainer<String, String>> list = new ArrayList<>();
        for (EventKafkaProperties.TopicDefinition topic : eventKafkaProperties.topics()) {
            list.add(createMainContainer(topic.name(), consumerFactory, eventBatchListener, kafkaTemplate));

            if (topic.deadLetter() != null) {
                list.add(createDltContainer(
                        topic.deadLetter().name(), consumerProperties,
                        consumerFactory, dltBatchMessageListener, dltExhaustedCounter));
            }
        }
        this.containers = List.copyOf(list);
    }

    @Bean
    public AdminClient adminClient(KafkaAdmin kafkaAdmin) {
        return AdminClient.create(kafkaAdmin.getConfigurationProperties());
    }

    @Bean
    public KafkaAdmin.NewTopics allTopics(EventKafkaProperties properties) {
        List<NewTopic> topics = new ArrayList<>();
        for (EventKafkaProperties.TopicDefinition def : properties.topics()) {
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

    private ConcurrentMessageListenerContainer<String, String> createMainContainer(
            String topic,
            ConsumerFactory<String, String> consumerFactory,
            EventBatchListener listener,
            KafkaTemplate<String, String> kafkaTemplate) {

        ContainerProperties containerProps = new ContainerProperties(topic);
        containerProps.setAckMode(kafkaProperties.getListener().getAckMode());
        containerProps.setMessageListener(listener);

        // 3 retries: backoffs 1 s → 2 s → 4 s; failures beyond that route to {topic}.DLT
        ExponentialBackOff backOff = new ExponentialBackOff(1_000L, 2.0);
        backOff.setMaxAttempts(3);
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(
                new DeadLetterPublishingRecoverer(kafkaTemplate), backOff);
        errorHandler.addNotRetryableExceptions(JsonProcessingException.class);

        int partitions = eventKafkaProperties.topics().stream().findFirst().get().partitions();

        ConcurrentMessageListenerContainer<String, String> container =
                new ConcurrentMessageListenerContainer<>(consumerFactory, containerProps);
        container.setConcurrency(concurrency(partitions, consumerProperties.podCount()));
        container.setCommonErrorHandler(errorHandler);
        container.setBeanName("consumer-" + topic);
        return container;
    }

    private ConcurrentMessageListenerContainer<String, String> createDltContainer(
            String dltTopic,
            EventConsumerProperties consumerProperties,
            ConsumerFactory<String, String> consumerFactory,
            DltBatchMessageListener dltListener,
            Counter dltExhaustedCounter) {

        ContainerProperties containerProps = new ContainerProperties(dltTopic);
        containerProps.setAckMode(kafkaProperties.getListener().getAckMode());
        containerProps.setMessageListener(dltListener);

        Properties overrides = new Properties();
        overrides.setProperty(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG,
                consumerProperties.podName() + "-" + dltTopic);
        containerProps.setKafkaConsumerProperties(overrides);

        // 100 retries at 5 s fixed; on exhaustion meter and skip
        FixedBackOff backOff = new FixedBackOff(5_000L, 100L);
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(
                (record, ex) -> {
                    log.error("DLT event exhausted all retries: topic={} partition={} offset={}",
                            record.topic(), record.partition(), record.offset());
                    dltExhaustedCounter.increment();
                },
                backOff);

        int partitions = eventKafkaProperties.topics().stream().findFirst().get().partitions();

        ConcurrentMessageListenerContainer<String, String> container =
                new ConcurrentMessageListenerContainer<>(consumerFactory, containerProps);
        container.setConcurrency(concurrency(partitions, consumerProperties.podCount()));
        container.setCommonErrorHandler(errorHandler);
        container.setBeanName("dlt-consumer-" + dltTopic);
        return container;
    }

    @Override
    public void start() {
        containers.forEach(ConcurrentMessageListenerContainer::start);
        running = true;
        log.info("Started {} Kafka consumer containers", containers.size());
    }

    @Override
    public void stop() {
        containers.forEach(ConcurrentMessageListenerContainer::stop);
        running = false;
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public boolean isAutoStartup() {
        return true;
    }

    @Override
    public int getPhase() {
        return Integer.MAX_VALUE - 100;
    }

    public int concurrency(int dividend, int instancesDeployed) {
        int quotient = dividend / instancesDeployed;
        int remainder = dividend % instancesDeployed;

        //Check if the math holds true
        if(dividend == quotient * instancesDeployed + remainder) {
            log.info("Kafka Concurrency is derived as {}", quotient);

            return quotient;
        } else {
            throw new RuntimeException("Concurrency could not be derived");
        }
    }
}
