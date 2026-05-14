package org.eventviewer.ingest.monitor;

import io.micrometer.core.annotation.Timed;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.TopicPartition;
import org.eventviewer.ingest.config.KafkaProperties;
import org.eventviewer.leader.LeaderAwareScheduler;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

@Service
public class KafkaLagMonitor {
    private final AdminClient adminClient;
    private final LeaderAwareScheduler leaderAwareScheduler;
    private final MeterRegistry meterRegistry;
    private final KafkaProperties.LagMonitor lagMonitorConfig;
    private final Map<String, AtomicLong> lagGauges = new ConcurrentHashMap<>();

    public KafkaLagMonitor(
            AdminClient adminClient,
            LeaderAwareScheduler leaderAwareScheduler,
            MeterRegistry meterRegistry,
            KafkaProperties kafkaProperties) {
        this.adminClient = adminClient;
        this.leaderAwareScheduler = leaderAwareScheduler;
        this.meterRegistry = meterRegistry;
        this.lagMonitorConfig = kafkaProperties.lagMonitor();
    }

    @Timed(value = "kafka.lag.monitor.check", histogram = true)
    @Scheduled(
            fixedDelayString = "${event-ingest.kafka.lag-monitor.interval-ms:60000}",
            initialDelayString = "${event-ingest.kafka.lag-monitor.interval-ms:60000}")
    public void checkLag() throws Exception {
        if (!lagMonitorConfig.enabled()) return;
        leaderAwareScheduler.runIfLeader(this::doCheckLag);
    }

    private void doCheckLag() throws Exception {
        for (String groupId : lagMonitorConfig.consumerGroupIds()) {
            Map<TopicPartition, Long> committed = adminClient
                    .listConsumerGroupOffsets(groupId)
                    .partitionsToOffsetAndMetadata()
                    .get()
                    .entrySet().stream()
                    .collect(Collectors.toMap(
                            Map.Entry::getKey,
                            e -> e.getValue().offset()));

            if (committed.isEmpty()) continue;

            Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> latest = adminClient
                    .listOffsets(committed.keySet().stream()
                            .collect(Collectors.toMap(tp -> tp, _ -> OffsetSpec.latest())))
                    .all()
                    .get();

            committed.forEach((tp, committedOffset) -> {
                long latestOffset = latest.getOrDefault(tp,
                        new ListOffsetsResult.ListOffsetsResultInfo(committedOffset, -1L, java.util.Optional.empty()))
                        .offset();
                long lag = Math.max(0L, latestOffset - committedOffset);
                upsertLagGauge(groupId, tp.topic(), tp.partition(), lag);
            });
        }
    }

    private void upsertLagGauge(String group, String topic, int partition, long lag) {
        String key = group + ":" + topic + ":" + partition;
        AtomicLong holder = lagGauges.computeIfAbsent(key, _ -> {
            AtomicLong value = new AtomicLong(0L);
            Gauge.builder("kafka.consumer.lag", value, AtomicLong::doubleValue)
                    .description("Kafka consumer group lag per topic partition")
                    .tag("group", group)
                    .tag("topic", topic)
                    .tag("partition", String.valueOf(partition))
                    .register(meterRegistry);
            return value;
        });
        holder.set(lag);
    }
}
