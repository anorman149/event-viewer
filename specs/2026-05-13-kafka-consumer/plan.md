# Phase 4 — Kafka Consumer: Implementation Plan

---

## Group 1 — `RawEvent` Model & Topic Property Config

1. Add `RawEvent` Java record to `libs/event-api`:
   - `UUID eventId`, `String schemaType`, `String schemaVersion`, `Instant timestamp`, `JsonNode payload`
   - `@JsonProperty` annotations on all fields matching snake_case JSON keys (`event_id`, `schema_type`, etc.)
   - Unit test: round-trip serialization + deserialization via `ObjectMapper`

2. Update `EventConsumerProperties` (`@ConfigurationProperties(prefix = "kafka.consumer")`) in `apps/event-ingest`:
   - `List<String> topics` — e.g. `[event-raw-1, event-raw-2, event-raw-3, event-raw-4]`
   - `String groupId` — `event-ingest-group`
   - `int partitionsPerTopic` — default `80`; overridden to `2` locally; used by `computeConcurrency`
   - `short replicas` — default `1`
   - `int maxPollRecords` — default `500`; overridden to `50` locally
   - `long sessionTimeoutMs` — default `45000`
   - `long heartbeatIntervalMs` — default `3000`
   - `long maxPollIntervalMs` — default `300000`
   - `int fetchMinBytes` — default `1024`; overridden to `1` locally
   - `long fetchMaxWaitMs` — default `500`; overridden to `100` locally
   - `int fetchMaxBytes` — default `52428800`; overridden to `1048576` locally
   - `int maxPartitionFetchBytes` — default `1048576`
   - `String autoOffsetReset` — default `earliest`
   - `String securityProtocol` — default `PLAINTEXT`
   - `String saslMechanism` — default empty string
   - Validated with `@Validated`; `@NotEmpty` on `topics` and `groupId`

3. Update `apps/event-ingest/application.yml` with the full property block using the prod defaults from step 2

4. Update `apps/event-ingest/application-local.yml` with local overrides: `partitionsPerTopic=2`, `maxPollRecords=50`, `fetchMinBytes=1`, `fetchMaxWaitMs=100`, `fetchMaxBytes=1048576`; set `INGEST_POD_COUNT=1` in local Docker Compose env or `.env` file so `computeConcurrency(2, 1) = 2` threads locally

5. Unit test: `EventConsumerPropertiesTest` — verify binding, verify `@NotEmpty` rejects empty topics list

---

## Group 2 — Topic & DLT Provisioning

6. Create a `TopicProvisioningConfig` `@Configuration` that reads `EventConsumerProperties` and creates `NewTopic` beans dynamically (replacing/extending the Phase 2 topic config):
   - For each topic in `props.getTopics()`: `TopicBuilder.name(topic).partitions(props.getPartitionsPerTopic()).replicas(props.getReplicas()).build()`
   - For each DLT topic (`topic + ".DLT"`): 4 partitions, 1 replica (hardcoded; DLT does not need 80 partitions)
   - Return as `List<NewTopic>` — Spring Kafka's `KafkaAdmin` picks up all `NewTopic` beans automatically

7. Ensure `KafkaAdmin.autoCreate = true` is set in the consumer configuration (confirm from Phase 2 config)

8. Unit test: `TopicProvisioningConfigTest` — given 4 topics in properties, verify 8 `NewTopic` beans produced (4 main + 4 DLT) with correct partition counts

---

## Group 3 — Consumer Factory & SASL Config

9. `KafkaConsumerFactoryConfig` `@Configuration` — builds the base `Map<String, Object>` consumer properties from `EventConsumerProperties`:
   - Sets `ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG`, `GROUP_ID_CONFIG`, `ENABLE_AUTO_COMMIT_CONFIG = false`, `AUTO_OFFSET_RESET_CONFIG`, `MAX_POLL_RECORDS_CONFIG`, `SESSION_TIMEOUT_MS_CONFIG`, `HEARTBEAT_INTERVAL_MS_CONFIG`, `MAX_POLL_INTERVAL_MS_CONFIG`, `FETCH_MIN_BYTES_CONFIG`, `FETCH_MAX_WAIT_MS_CONFIG`, `FETCH_MAX_BYTES_CONFIG`, `MAX_PARTITION_FETCH_BYTES_CONFIG`
   - Sets `PARTITION_ASSIGNMENT_STRATEGY_CONFIG = CooperativeStickyAssignor.class.getName()`
   - Sets `KEY_DESERIALIZER_CLASS_CONFIG = StringDeserializer.class`, `VALUE_DESERIALIZER_CLASS_CONFIG = StringDeserializer.class`
   - If `securityProtocol` is not `PLAINTEXT`: sets `SECURITY_PROTOCOL_CONFIG`, `SASL_MECHANISM_CONFIG`; reads `sasl.jaas.config` from environment variable `KAFKA_SASL_JAAS_CONFIG` (never from committed config); sets `SaslConfigs.SASL_JAAS_CONFIG`

10. `topicConsumerFactory(String topic, String threadIndex)` helper method — returns a new `DefaultKafkaConsumerFactory<String, String>` with a copy of the base properties plus `GROUP_INSTANCE_ID_CONFIG = {MY_POD_NAME}-{topic}-{threadIndex}`; `MY_POD_NAME` read from `System.getenv("MY_POD_NAME")` with fallback to `InetAddress.getLocalHost().getHostName()` for non-K8s environments

11. Unit test: `KafkaConsumerFactoryConfigTest` — verify `CooperativeStickyAssignor` is in the props map, verify `GROUP_INSTANCE_ID_CONFIG` contains both pod name and topic, verify `SASL_JAAS_CONFIG` is not present when `securityProtocol=PLAINTEXT`

---

## Group 4 — `EventConsumerContainerFactory` (Dynamic Main Containers)

12. `ConcurrencyCalculator` — package-private static utility class (or inner static method on `EventConsumerContainerFactory`):
    - `static int computeConcurrency(int partitionsPerTopic, int podCount)` — returns `Math.max(1, partitionsPerTopic / podCount)`
    - `podCount` source: `Integer.parseInt(System.getenv("INGEST_POD_COUNT"))` with default 1 if env var is absent or blank
    - `static int resolvePodCount()` — reads `INGEST_POD_COUNT` env var; logs the resolved value at INFO on startup
    - Unit test: `ConcurrencyCalculatorTest` — verify: 80 partitions / 4 pods = 20; 80 / 1 = 80; 2 / 3 = 1 (never 0); missing env var defaults to 1

13. `EventConsumerContainerFactory` — `@Component` implementing `SmartLifecycle`:
    - Constructor-injects `EventConsumerProperties`, `KafkaConsumerFactoryConfig`, `EventBatchListener`, `DefaultErrorHandler`
    - `@PostConstruct` `init()` — resolves `concurrency = ConcurrencyCalculator.computeConcurrency(props.getPartitionsPerTopic(), ConcurrencyCalculator.resolvePodCount())`; iterates `props.getTopics()`; for each topic creates `concurrency` consumer factories (one per thread index); wraps them in a `ConcurrentMessageListenerContainer` with:
      - `ContainerProperties(topic)` with `AckMode.MANUAL_IMMEDIATE`
      - `setBatchAcknowledgingMessageListener(eventBatchListener)`
      - `setErrorHandler(errorHandler)`
      - `setConcurrency(concurrency)`
      - `setMissingTopicsFatal(false)`
      - `setListenerTaskExecutor(Executors.newVirtualThreadPerTaskExecutor())`
    - Stores containers in `Map<String, ConcurrentMessageListenerContainer> containersByTopic`
    - `start()` / `stop()` / `isRunning()` — delegates to all containers
    - `getPhase()` returns a phase after default beans so topics are created first
    - Exposes `Map<String, ConcurrentMessageListenerContainer> getContainers()` for testing

14. `DefaultErrorHandler` `@Bean`:
    - `ExponentialBackOffWithMaxRetries(3)` — initial interval 1000 ms, multiplier 2.0, max 4000 ms
    - `DeadLetterPublishingRecoverer(kafkaTemplate)` — publishes to `{originalTopic}.DLT`
    - `addNotRetryableExceptions(JsonProcessingException.class, MismatchedInputException.class)` — fast-path to DLT without 3 retries if these somehow escape the listener

15. Unit test: `EventConsumerContainerFactoryTest` — given 4 topics and `INGEST_POD_COUNT=4` (set via env in test), verify 4 containers created; verify concurrency = `partitionsPerTopic / 4`; verify containers started on `start()`; verify `AckMode.MANUAL_IMMEDIATE` on each container

---

## Group 5 — `EventBatchListener` & `IngestPipelineService` Stub

16. `EventBatchListener` implementing `BatchAcknowledgingMessageListener<String, String>`:
    - `onMessage(List<ConsumerRecord<String, String>> records, Acknowledgment acknowledgment)` — processes the batch as a whole:
      1. Deserialize all records: iterate the list, attempt `objectMapper.readValue(record.value(), RawEvent.class)` for each; collect successes into `validEvents`, count failures
         - On `JsonProcessingException`: log ERROR (topic, partition, offset, first 256 chars of raw value); increment `kafka.consumer.parse.failures` counter tagged with topic; do not rethrow
      2. If `validEvents` is not empty: call `ingestPipelineService.process(validEvents, topic)`
      3. **Explicit manual acknowledge**: call `acknowledgment.acknowledge()` — this is the only place an offset commit is triggered; called whether the batch was fully valid, partially valid, or entirely parse-failed
    - `@Timed(histogram=true, name="kafka.consumer.batch.listener")` on `onMessage`

17. `IngestPipelineService` `@Service` (stub for Phase 4):
    - `process(List<RawEvent> events, String topic)`:
      - Logs batch size + topic at DEBUG
      - Records `kafka.consumer.batch.size` `DistributionSummary` tagged with `topic`
      - Returns immediately
    - `@Timed(histogram=true, name="kafka.consumer.pipeline.process")`

19. Unit tests:
    - `EventBatchListenerTest` — mock `IngestPipelineService`; mock `Acknowledgment`; send batch of 5 records (3 valid, 2 invalid JSON); verify `ingestPipelineService.process()` called with exactly 3 `RawEvent` objects; verify `kafka.consumer.parse.failures` counter = 2; verify `acknowledgment.acknowledge()` called exactly once
    - `EventBatchListenerTest` — send batch of 5 records all invalid; verify `ingestPipelineService.process()` never called; verify `acknowledgment.acknowledge()` still called exactly once

---

## Group 6 — `DltConsumerContainerFactory` & `DltBatchMessageListener`

20. `DltBatchMessageListener` `@Component` implementing `BatchAcknowledgingMessageListener<String, String>`:
    - `onMessage(List<ConsumerRecord<String, String>> records, Acknowledgment acknowledgment)`:
      - Receives the full batch; processes as a unit
      - For each record: attempt `ingestPipelineService.process(...)` with up to 100 retries (fixed 5 s delay); implemented as a local retry loop — no Spring Retry annotations
      - On success within 100 tries: increment `kafka.consumer.dlt.recovered`
      - On 100th failure: log ERROR with full stack trace + topic + offset + raw bytes; increment `kafka.consumer.dlt.exhausted`
      - **Explicit manual acknowledge**: `acknowledgment.acknowledge()` called once after all records in the batch are handled — never called per-record
    - `@Timed(histogram=true, name="kafka.consumer.dlt.listener")`

21. `DltConsumerContainerFactory` `@Component` implementing `SmartLifecycle`:
    - Same structure as `EventConsumerContainerFactory` but iterates `{topic}.DLT` names
    - DLT containers always use `concurrency = 1` (DLT processing is sequential by design — no `computeConcurrency` call)
    - Container properties: `AckMode.MANUAL_IMMEDIATE`; `setBatchAcknowledgingMessageListener(dltBatchMessageListener)`
    - No `DefaultErrorHandler` on DLT containers — failures are handled inside `DltBatchMessageListener`; never route to another DLT
    - `group.id = event-ingest-dlt-group`; `group.instance.id = {MY_POD_NAME}-dlt-{topic}`

22. Unit test: `DltBatchMessageListenerTest` — mock `IngestPipelineService`; mock `Acknowledgment`; simulate 2 failures then success; verify `dlt.recovered` counter = 1; simulate 100 consecutive failures; verify `dlt.exhausted` counter = 1; verify `acknowledgment.acknowledge()` called exactly once in both cases

---

## Group 7 — Observability

21. Meters summary (all tagged with at minimum `topic`):
    - `kafka.consumer.parse.failures` — Counter — malformed JSON records skipped (`EventBatchListener`)
    - `kafka.consumer.batch.size` — DistributionSummary — events per batch after parse filtering (`IngestPipelineService`)
    - `kafka.consumer.dlt.received` — Counter — messages received by DLT consumer (increment at start of each record in `DltBatchMessageListener`)
    - `kafka.consumer.dlt.recovered` — Counter — DLT messages resolved within 100 retries
    - `kafka.consumer.dlt.exhausted` — Counter — DLT messages that failed all 100 retries
    - `kafka.consumer.batch.listener` — Timed histogram — end-to-end batch processing time

22. Confirm `KafkaLagMonitor` from Phase 3 is registered with `event-ingest-group` and `event-ingest-dlt-group` as consumer groups to monitor

23. Confirm virtual thread executor is applied to `@Scheduled` tasks (already from Phase 2); virtual thread executor on each container is set in step 13 (`EventConsumerContainerFactory`) — verify it is also applied to DLT containers in `DltConsumerContainerFactory`

---

## Group 8 — Integration Tests (`apps/event-ingest/src/itest`)

24. `BaseTest` (from Phase 2/3 itest) — confirm `KafkaTemplate` autowired for test publishing; confirm Docker Compose starts Kafka

25. `EventConsumerIT`:
    - Publish 100 valid `RawEvent` JSON messages to `event-raw-1`
    - Wait for `ingestPipelineService.process()` to be called (use `CountDownLatch` or `Awaitility`; `IngestPipelineService` is a real bean in this test but the stub body suffices)
    - Assert `kafka.consumer.batch.size` DistributionSummary count ≥ 1
    - Assert consumer group `event-ingest-group` has committed offsets for `event-raw-1` in Kafka (verify via `AdminClient.listConsumerGroupOffsets`)

26. `ParseFailureIT`:
    - Publish 5 valid records + 3 malformed JSON strings to `event-raw-1`
    - Await processing
    - Assert `kafka.consumer.parse.failures` counter = 3
    - Assert `kafka.consumer.batch.size` count reflects 5 valid events (not 8)
    - Assert no messages on `event-raw-1.DLT`

27. `DltRetryIT`:
    - Replace `IngestPipelineService` with a mock that throws `RuntimeException` for the first N calls
    - Publish 1 record; verify it reaches `event-raw-1.DLT` after 3 main-topic retry exhaustions
    - Verify `DltBatchMessageListener` receives and processes the DLT message
    - Verify `kafka.consumer.dlt.recovered` counter = 1 after mock recovers

---

## Group 9 — CI & Build

28. Confirm `apps/event-ingest` itest Docker Compose wiring (from Phase 2) still starts Kafka correctly with the new topic partition counts (local profile: 2 partitions)

29. Confirm `jvmArgs '-Dnet.bytebuddy.experimental=true'` covers `apps/event-ingest` test and itest tasks

30. Confirm `./gradlew :apps:event-ingest:itest` passes; confirm `./gradlew build` passes end-to-end
