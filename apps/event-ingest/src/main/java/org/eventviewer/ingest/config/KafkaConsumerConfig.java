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

    private final ConcurrentMessageListenerContainer<String, String> mainContainer;
    private final ConcurrentMessageListenerContainer<String, String> dltContainer;
    private final List<String> mainTopics;
    private final List<String> dltTopics;
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

        Counter dltExhaustedCounter = Counter.builder("dlt.exhausted")
                .description("DLT events that exhausted all retry attempts")
                .register(meterRegistry);

        this.mainTopics = eventKafkaProperties.topics().stream()
                .map(EventKafkaProperties.TopicDefinition::name)
                .toList();
        this.dltTopics = eventKafkaProperties.deadLetterTopics().stream()
                .map(EventKafkaProperties.DltTopicDefinition::name)
                .toList();

        int mainTotalPartitions = eventKafkaProperties.topics().stream()
                .findFirst().map(EventKafkaProperties.TopicDefinition::partitions)
                .orElse(1);
        int dltTotalPartitions = eventKafkaProperties.deadLetterTopics().stream()
                .findFirst().map(EventKafkaProperties.DltTopicDefinition::partitions)
                .orElse(1);

        this.mainContainer = createMainContainer(
                mainTopics, mainTotalPartitions, consumerProperties,
                consumerFactory, eventBatchListener, kafkaTemplate, kafkaProperties);
        this.dltContainer = createDltContainer(
                dltTopics, dltTotalPartitions, consumerProperties,
                consumerFactory, dltBatchMessageListener, dltExhaustedCounter, kafkaProperties);
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
        }
        for (EventKafkaProperties.DltTopicDefinition def : properties.deadLetterTopics()) {
            topics.add(TopicBuilder.name(def.name())
                    .partitions(def.partitions())
                    .replicas(def.replicationFactor())
                    .build());
        }
        return new KafkaAdmin.NewTopics(topics.toArray(new NewTopic[0]));
    }

    private ConcurrentMessageListenerContainer<String, String> createMainContainer(
            List<String> topics,
            int totalPartitions,
            EventConsumerProperties consumerProperties,
            ConsumerFactory<String, String> consumerFactory,
            EventBatchListener listener,
            KafkaTemplate<String, String> kafkaTemplate,
            KafkaProperties kafkaProperties) {

        ContainerProperties containerProps = new ContainerProperties(topics.toArray(new String[0]));
        containerProps.setMessageListener(listener);
        containerProps.setAckMode(kafkaProperties.getListener().getAckMode());
        containerProps.setMicrometerEnabled(true);
        containerProps.setClientId(kafkaProperties.getClientId());
        containerProps.setGroupId(kafkaProperties.getConsumer().getGroupId());

        // 3 retries: backoffs 1 s → 2 s → 4 s; failures beyond that route to {topic}.DLT
        ExponentialBackOff backOff = new ExponentialBackOff(1_000L, 2.0);
        backOff.setMaxAttempts(3);
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(
                new DeadLetterPublishingRecoverer(kafkaTemplate), backOff);
        errorHandler.addNotRetryableExceptions(JsonProcessingException.class);

        ConcurrentMessageListenerContainer<String, String> container =
                new ConcurrentMessageListenerContainer<>(consumerFactory, containerProps);
        container.setConcurrency(concurrency(totalPartitions, consumerProperties.podCount()));
        container.setCommonErrorHandler(errorHandler);
        container.setBeanName("main-consumer");
        return container;
    }

    private ConcurrentMessageListenerContainer<String, String> createDltContainer(
            List<String> dltTopics,
            int totalPartitions,
            EventConsumerProperties consumerProperties,
            ConsumerFactory<String, String> consumerFactory,
            DltBatchMessageListener dltListener,
            Counter dltExhaustedCounter,
            KafkaProperties kafkaProperties) {

        ContainerProperties containerProps = new ContainerProperties(dltTopics.toArray(new String[0]));
        containerProps.setAckMode(kafkaProperties.getListener().getAckMode());
        containerProps.setMicrometerEnabled(true);
        containerProps.setClientId(kafkaProperties.getClientId() + "-dlt");
        containerProps.setGroupId(kafkaProperties.getConsumer().getGroupId() + "-dlt");
        containerProps.setMessageListener(dltListener);

        Properties overrides = new Properties();
        overrides.setProperty(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG,
                consumerProperties.podName() + "-dlt");
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

        ConcurrentMessageListenerContainer<String, String> container =
                new ConcurrentMessageListenerContainer<>(consumerFactory, containerProps);
        container.setConcurrency(concurrency(totalPartitions, consumerProperties.podCount()));
        container.setCommonErrorHandler(errorHandler);
        container.setBeanName("dlt-consumer");
        return container;
    }

    @Override
    public void start() {
        mainContainer.start();
        dltContainer.start();
        running = true;
        log.info("Started main consumer container supporting topics: {}", mainTopics);
        log.info("Started DLT consumer container supporting topics: {}", dltTopics);
    }

    @Override
    public void stop() {
        mainContainer.stop();
        dltContainer.stop();
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
