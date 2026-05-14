# Phase 4 — Kafka Consumer: Implementation Plan

---

## Group 1 — `KafkaEventMessage` Update & Topic Property Config

1. Update `KafkaEventMessage` in `libs/event-api` — merge in the fields planned for `RawEvent` rather than creating a new class:
   - Add `@JsonProperty("schema_version") String schemaVersion`
   - Change `Object payload` to `JsonNode payload` with `@JsonProperty("payload")`
   - Add `@JsonProperty("timestamp")` to the existing `timestamp` field (was missing the annotation)
   - Remove `ingestTs` field — it is not part of the inbound event wire format and should not be on this record
   - Final shape: `UUID eventId`, `String schemaType`, `String schemaVersion`, `Instant timestamp`, `JsonNode payload` — all annotated with snake_case `@JsonProperty`
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
   - For each DLT topic (`topic + ".DLT"`): `props.getPartitionsPerTopic()` partitions, `props.getReplicas()` replicas — same as main topics so concurrency scales identically
   - Return as `List<NewTopic>` — Spring Kafka's `KafkaAdmin` picks up all `NewTopic` beans automatically

7. Ensure `KafkaAdmin.autoCreate = true` is set in the consumer configuration (confirm from Phase 2 config)

8. Unit test: `TopicProvisioningConfigTest` — given 4 topics in properties, verify 8 `NewTopic` beans produced (4 main + 4 DLT) with matching partition counts (DLT = main)

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
    - `onMessage(List<ConsumerRecord<String, String>> records, Acknowledgment acknowledgment)`:
      1. Deserialize all records: attempt `objectMapper.readValue(record.value(), KafkaEventMessage.class)` for each; collect successes into `validEvents`, count failures
         - On `JsonProcessingException`: log ERROR with topic, partition, and offset **only** — do NOT log the raw value or any portion of it (security: event payloads must not appear in logs); increment `kafka.consumer.parse.failures` counter tagged with `topic`; do not rethrow
      2. Record `kafka.consumer.batch.size` `DistributionSummary` tagged with `topic` (size = `validEvents.size()`)
      3. If `validEvents` is not empty: call `ingestPipelineService.process(validEvents)` — no topic argument; `IngestPipelineService` must not know the Kafka topic
      4. **Acknowledge only on success**: call `acknowledgment.acknowledge()` after `process()` returns normally — if `process()` throws, do NOT acknowledge; allow Spring Kafka to retry the batch and eventually route to DLT
    - `@Timed(histogram=true, name="kafka.consumer.batch.listener")` on `onMessage`

17. `IngestPipelineService` `@Service` (stub for Phase 4):
    - `process(List<KafkaEventMessage> events)` — no topic parameter; the service must not be aware of Kafka topics
      - Logs batch size at DEBUG (no topic, no event content)
      - Returns immediately
    - `@Timed(histogram=true, name="kafka.consumer.pipeline.process")`

19. Unit tests:
    - `EventBatchListenerTest` — mock `IngestPipelineService`; mock `Acknowledgment`; send batch of 5 records (3 valid, 2 invalid JSON); verify `ingestPipelineService.process()` called with exactly 3 `KafkaEventMessage` objects; verify `kafka.consumer.parse.failures` counter = 2; verify `kafka.consumer.batch.size` recorded as 3; verify `acknowledgment.acknowledge()` called exactly once
    - `EventBatchListenerTest` — send batch of 5 records all invalid; verify `ingestPipelineService.process()` never called; verify `acknowledgment.acknowledge()` still called (all-invalid batch is not a processing failure; parse failures are already counted)
    - `EventBatchListenerTest` — `ingestPipelineService.process()` throws `RuntimeException`; verify `acknowledgment.acknowledge()` is **never** called (Spring Kafka must retry)

---

## Group 6 — `DltConsumerContainerFactory` & `DltBatchMessageListener`

20. `DltBatchMessageListener` `@Component` implementing `BatchAcknowledgingMessageListener<String, String>`:
    - `onMessage(List<ConsumerRecord<String, String>> records, Acknowledgment acknowledgment)` — mirrors the main listener; Spring Kafka handles the retry loop via the container's error handler, not application code:
      1. Deserialize records: same parse logic as `EventBatchListener`; on `JsonProcessingException` log ERROR with topic/partition/offset only (no raw value); increment `kafka.consumer.parse.failures` tagged with topic
      2. Increment `kafka.consumer.dlt.received` counter tagged with topic (once per DLT message, before processing)
      3. Record `kafka.consumer.batch.size` `DistributionSummary` tagged with topic
      4. If `validEvents` is not empty: call `ingestPipelineService.process(validEvents)` — same method, same signature as the main listener
      5. **Acknowledge only on success**: `acknowledgment.acknowledge()` after `process()` returns normally; if `process()` throws, do NOT acknowledge — the container's `DefaultErrorHandler` retries up to 99 times; after all retries are exhausted the error handler's recoverer takes over (logging + counter; no further routing)
    - `@Timed(histogram=true, name="kafka.consumer.dlt.listener")`

21. `DltConsumerContainerFactory` `@Component` implementing `SmartLifecycle`:
    - Same structure as `EventConsumerContainerFactory` but iterates `{topic}.DLT` topic names
    - **Same concurrency algorithm**: `concurrency = ConcurrencyCalculator.computeConcurrency(props.getPartitionsPerTopic(), ConcurrencyCalculator.resolvePodCount())` — DLT topics have the same partition count as main topics so the same formula applies
    - Container properties: `AckMode.MANUAL_IMMEDIATE`; `setBatchAcknowledgingMessageListener(dltBatchMessageListener)`
    - Attach a `DefaultErrorHandler` with `ExponentialBackOffWithMaxRetries(99)` — Spring Kafka drives up to 99 retry attempts after the initial DLT delivery; on final exhaustion the recoverer logs ERROR (topic + offset, no payload) and increments `kafka.consumer.dlt.exhausted` — **no further routing**; the record is considered past control and processing stops
    - `addNotRetryableExceptions(JsonProcessingException.class, MismatchedInputException.class)` on the DLT error handler as well — parse errors are already swallowed inside the listener, but this is a safety net so they are never retried if they somehow escape
    - `group.id = event-ingest-dlt-group`; `group.instance.id = {MY_POD_NAME}-dlt-{topic}`

22. Unit test: `DltBatchMessageListenerTest` — mock `IngestPipelineService`; mock `Acknowledgment`; send batch of 3 DLT records (2 valid, 1 invalid); verify `ingestPipelineService.process()` called with 2 events; verify `dlt.received` counter = 2; verify `acknowledgment.acknowledge()` called once on success; verify `acknowledgment.acknowledge()` NOT called when `process()` throws

---

## Group 7 — Observability

21. Meters summary (all tagged with at minimum `topic`):
    - `kafka.consumer.parse.failures` — Counter — malformed JSON records skipped (`EventBatchListener` and `DltBatchMessageListener`)
    - `kafka.consumer.batch.size` — DistributionSummary — valid events per batch after parse filtering (`EventBatchListener` and `DltBatchMessageListener`; NOT in `IngestPipelineService`)
    - `kafka.consumer.dlt.received` — Counter — valid messages received by DLT consumer, incremented before `process()` (`DltBatchMessageListener`)
    - `kafka.consumer.dlt.exhausted` — Counter — DLT messages that exhausted all container retries (logged + counted in DLT container's `DefaultErrorHandler` recoverer)
    - `kafka.consumer.batch.listener` — Timed histogram — end-to-end batch processing time (`EventBatchListener`)
    - `kafka.consumer.dlt.listener` — Timed histogram — end-to-end DLT batch processing time (`DltBatchMessageListener`)

22. Confirm `KafkaLagMonitor` from Phase 3 is registered with `event-ingest-group` and `event-ingest-dlt-group` as consumer groups to monitor

23. Confirm virtual thread executor is applied to `@Scheduled` tasks (already from Phase 2); virtual thread executor on each container is set in step 13 (`EventConsumerContainerFactory`) — verify it is also applied to DLT containers in `DltConsumerContainerFactory`

---

## Group 8 — Integration Tests (`apps/event-ingest/src/itest`)

24. `BaseTest` (from Phase 2/3 itest) — confirm `KafkaTemplate` autowired for test publishing; confirm Docker Compose starts Kafka

25. `EventConsumerIT`:
    - Publish 100 valid `KafkaEventMessage` JSON messages to `event-raw-1`
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
    - Replace `IngestPipelineService` with a mock that throws `RuntimeException` for the first N calls then succeeds
    - Publish 1 record; verify it reaches `event-raw-1.DLT` after 3 main-topic retry exhaustions (container's `DefaultErrorHandler` routes it)
    - Verify `DltBatchMessageListener` receives the DLT message; Spring Kafka container retries on the DLT side until mock recovers
    - Verify `kafka.consumer.dlt.received` counter = 1 after the DLT message is processed successfully

---

## Group 9 — CI & Build

28. Confirm `apps/event-ingest` itest Docker Compose wiring (from Phase 2) still starts Kafka correctly with the new topic partition counts (local profile: 2 partitions)

29. Confirm `jvmArgs '-Dnet.bytebuddy.experimental=true'` covers `apps/event-ingest` test and itest tasks

30. Confirm `./gradlew :apps:event-ingest:itest` passes; confirm `./gradlew build` passes end-to-end
