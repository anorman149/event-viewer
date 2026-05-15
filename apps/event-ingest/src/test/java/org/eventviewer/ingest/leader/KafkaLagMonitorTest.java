package org.eventviewer.ingest.leader;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.eventviewer.ingest.config.EventKafkaProperties;
import org.eventviewer.ingest.monitor.KafkaLagMonitor;
import org.eventviewer.leader.LeaderAwareScheduler;
import org.eventviewer.leader.LeaderAwareSchedulerImpl;
import org.eventviewer.leader.LeaderElectionService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class KafkaLagMonitorTest {

    @Mock AdminClient adminClient;
    @Mock LeaderElectionService leaderElectionService;
    @Mock ListConsumerGroupOffsetsResult offsetsResult;
    @Mock ListOffsetsResult latestResult;

    SimpleMeterRegistry registry;
    KafkaLagMonitor monitor;

    @BeforeEach
    void setUp() {
        registry = new SimpleMeterRegistry();
        LeaderAwareScheduler scheduler = new LeaderAwareSchedulerImpl(leaderElectionService, registry);

        EventKafkaProperties eventKafkaProperties = new EventKafkaProperties(
                List.of(),
                List.of(),
                new EventKafkaProperties.LagMonitor(true, 60_000L, List.of("event-ingest-group")));

        monitor = new KafkaLagMonitor(adminClient, scheduler, registry, eventKafkaProperties);
    }

    @Test
    void noAdminClientCallsWhenNotLeader() throws Exception {
        when(leaderElectionService.isLeader()).thenReturn(false);

        monitor.checkLag();

        verifyNoInteractions(adminClient);
    }

    @Test
    void listConsumerGroupOffsetsCalledWithCorrectGroupId() throws Exception {
        TopicPartition tp = new TopicPartition("event-raw", 0);
        when(leaderElectionService.isLeader()).thenReturn(true);
        stubOffsets("event-ingest-group", tp, 90L, 100L);

        monitor.checkLag();

        verify(adminClient).listConsumerGroupOffsets("event-ingest-group");
    }

    @Test
    void listOffsetsCalledWithPartitionsFromGroupOffsets() throws Exception {
        TopicPartition tp0 = new TopicPartition("event-raw", 0);
        TopicPartition tp1 = new TopicPartition("event-raw", 1);
        when(leaderElectionService.isLeader()).thenReturn(true);

        KafkaFuture<Map<TopicPartition, OffsetAndMetadata>> commitFuture = KafkaFuture.completedFuture(Map.of(
                tp0, new OffsetAndMetadata(80L),
                tp1, new OffsetAndMetadata(90L)));
        when(adminClient.listConsumerGroupOffsets("event-ingest-group")).thenReturn(offsetsResult);
        when(offsetsResult.partitionsToOffsetAndMetadata()).thenReturn(commitFuture);

        @SuppressWarnings("unchecked")
        ArgumentCaptor<Map<TopicPartition, OffsetSpec>> captor = ArgumentCaptor.forClass(Map.class);
        KafkaFuture<Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo>> latestFuture =
                KafkaFuture.completedFuture(Map.of(
                        tp0, new ListOffsetsResult.ListOffsetsResultInfo(100L, -1L, Optional.empty()),
                        tp1, new ListOffsetsResult.ListOffsetsResultInfo(100L, -1L, Optional.empty())));
        when(adminClient.listOffsets(captor.capture())).thenReturn(latestResult);
        when(latestResult.all()).thenReturn(latestFuture);

        monitor.checkLag();

        Map<TopicPartition, OffsetSpec> passedPartitions = captor.getValue();
        assertThat(passedPartitions).containsKey(tp0);
        assertThat(passedPartitions).containsKey(tp1);
    }

    @Test
    void lagComputedCorrectly() throws Exception {
        TopicPartition tp = new TopicPartition("event-raw", 0);
        when(leaderElectionService.isLeader()).thenReturn(true);
        stubOffsets("event-ingest-group", tp, 90L, 100L);

        monitor.checkLag();

        var gauge = registry.find("kafka.consumer.lag")
                .tag("group", "event-ingest-group")
                .tag("topic", "event-raw")
                .tag("partition", "0")
                .gauge();
        assertThat(gauge).isNotNull();
        assertThat(gauge.value()).isEqualTo(10.0);
    }

    @Test
    void gaugeUpsertedNotDuplicated() throws Exception {
        TopicPartition tp = new TopicPartition("event-raw", 0);
        when(leaderElectionService.isLeader()).thenReturn(true);
        stubOffsets("event-ingest-group", tp, 90L, 100L);
        monitor.checkLag();

        stubOffsets("event-ingest-group", tp, 95L, 100L);
        monitor.checkLag();

        assertThat(registry.find("kafka.consumer.lag").gauges()).hasSize(1);
        assertThat(registry.find("kafka.consumer.lag")
                .tag("group", "event-ingest-group").gauge().value()).isEqualTo(5.0);
    }

    @Test
    void defaultLagMonitorIntervalMs() {
        var lagMonitor = new EventKafkaProperties.LagMonitor(true, 0L, List.of());
        assertThat(lagMonitor.intervalMs()).isEqualTo(60_000L);
    }

    private void stubOffsets(String groupId, TopicPartition tp, long committed, long latest) throws Exception {
        KafkaFuture<Map<TopicPartition, OffsetAndMetadata>> commitFuture =
                KafkaFuture.completedFuture(Map.of(tp, new OffsetAndMetadata(committed)));
        when(adminClient.listConsumerGroupOffsets(groupId)).thenReturn(offsetsResult);
        when(offsetsResult.partitionsToOffsetAndMetadata()).thenReturn(commitFuture);

        KafkaFuture<Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo>> latestFuture =
                KafkaFuture.completedFuture(Map.of(
                        tp, new ListOffsetsResult.ListOffsetsResultInfo(latest, -1L, Optional.empty())));
        when(adminClient.listOffsets(any())).thenReturn(latestResult);
        when(latestResult.all()).thenReturn(latestFuture);
    }
}
