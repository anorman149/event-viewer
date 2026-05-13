# Phase 4 — Kafka Consumer: Validation

A phase is complete and mergeable when every criterion below passes. Work through the groups in order.

---

## Group 1 — `RawEvent` Model & Properties

| # | Check | How to verify |
|---|---|---|
| 1.1 | `RawEvent` serializes and deserializes correctly | `RawEventTest` — round-trip via `ObjectMapper`; verify snake_case field names in JSON output |
| 1.2 | `EventConsumerProperties` binds all fields from YAML | `EventConsumerPropertiesTest` — bind from test YAML; assert every field has the expected value |
| 1.3 | `@NotEmpty` rejects empty `topics` list | `EventConsumerPropertiesTest` — bind with empty list; assert `ConstraintViolationException` |
| 1.4 | Local profile overrides applied correctly | Bind `application-local.yml` overrides on top of `application.yml`; assert `partitionsPerTopic=2`, `maxPollRecords=50`; assert `INGEST_POD_COUNT=1` env produces `computeConcurrency(2, 1) = 2` |
| 1.5 | `application.yml` has complete prod defaults | Manual review — every property in the requirements table is present in `application.yml` with the correct prod value |

---

## Group 2 — Topic Provisioning

| # | Check | How to verify |
|---|---|---|
| 2.1 | 4 main topic `NewTopic` beans produced | `TopicProvisioningConfigTest` — 4-topic input → 4 main `NewTopic` beans with `partitions=80`, `replicas=1` |
| 2.2 | 4 DLT topic `NewTopic` beans produced | Same test — 4 DLT `NewTopic` beans (`event-raw-1.DLT` … `event-raw-4.DLT`) with `partitions=4`, `replicas=1` |
| 2.3 | Local profile uses 2 partitions for main topics | Bind local profile; assert main topic beans have `partitions=2` |
| 2.4 | Topics actually created in Kafka during itest | `EventConsumerIT` — after context start, `AdminClient.listTopics()` includes all 8 topic names |

---

## Group 3 — Consumer Factory & SASL

| # | Check | How to verify |
|---|---|---|
| 3.1 | `CooperativeStickyAssignor` is in consumer props | `KafkaConsumerFactoryConfigTest` — assert `PARTITION_ASSIGNMENT_STRATEGY_CONFIG` equals `CooperativeStickyAssignor.class.getName()` |
| 3.2 | `ENABLE_AUTO_COMMIT_CONFIG = false` | Same test — assert false |
| 3.3 | `GROUP_INSTANCE_ID_CONFIG` contains pod name + topic | `topicConsumerFactory("event-raw-1", "0")` → assert value contains both `MY_POD_NAME` value and `event-raw-1` |
| 3.4 | `GROUP_INSTANCE_ID_CONFIG` is unique per thread index | Call factory for same topic with index 0 and index 1 → assert two different values |
| 3.5 | No SASL config when `securityProtocol = PLAINTEXT` | `KafkaConsumerFactoryConfigTest` — assert `SASL_JAAS_CONFIG` not present in props map |
| 3.6 | SASL config present when `securityProtocol = SASL_SSL` | Set env var `KAFKA_SASL_JAAS_CONFIG=test-value` in test; assert `SASL_JAAS_CONFIG = test-value` in props map |
| 3.7 | No SASL credential in any committed file | `git grep -r "sasl.jaas" -- "*.yml" "*.yaml" "*.properties"` returns no results |

---

## Group 4 — `EventConsumerContainerFactory`

| # | Check | How to verify |
|---|---|---|
| 4.1 | 4 containers created for 4 configured topics | `EventConsumerContainerFactoryTest` — configure 4 topics; assert `getContainers().size() == 4` |
| 4.2 | Concurrency is computed from `INGEST_POD_COUNT` | `ConcurrencyCalculatorTest`: 80/4=20, 80/1=80, 2/3=1, missing env=1; `EventConsumerContainerFactoryTest` with `INGEST_POD_COUNT=4`: assert container concurrency = `partitionsPerTopic / 4` |
| 4.3 | Each container has `AckMode.MANUAL_IMMEDIATE` | Inspect `ContainerProperties.getAckMode()` on each container — must be `MANUAL_IMMEDIATE`, not `BATCH` |
| 4.4 | `missingTopicsFatal = false` on each container | Inspect `ContainerProperties.isMissingTopicsFatal()` |
| 4.5 | All containers start when `EventConsumerContainerFactory.start()` called | Mock containers; verify `container.start()` called exactly 4 times |
| 4.6 | `DefaultErrorHandler` has 3-attempt exponential backoff | Inspect `DefaultErrorHandler` bean; verify back-off class and max retries |
| 4.7 | `JsonProcessingException` is non-retryable in `DefaultErrorHandler` | Assert `JsonProcessingException` is in the error handler's non-retryable exception set |
| 4.8 | Virtual thread executor set on each container | Assert `ContainerProperties.getListenerTaskExecutor()` is a virtual thread executor |

---

## Group 5 — `EventBatchListener` & Parse Failure Handling

| # | Check | How to verify |
|---|---|---|
| 5.1 | 3 valid + 2 invalid records → `process()` called with 3 events | `EventBatchListenerTest` — mock `IngestPipelineService`; verify `process()` argument list size = 3 |
| 5.2 | `kafka.consumer.parse.failures` counter = 2 for 2 invalid records | Same test — inspect `SimpleMeterRegistry` |
| 5.3 | `acknowledgment.acknowledge()` called exactly once per batch | Same test — mock `Acknowledgment`; verify single `acknowledge()` call |
| 5.4 | All 5 invalid records → `process()` never called | `EventBatchListenerTest` — all invalid; assert mock never invoked |
| 5.5 | `acknowledge()` still called when all records are invalid | Same test — verify `acknowledge()` called once |
| 5.6 | Parse failure log includes topic, partition, offset, and truncated raw value | Manual inspection of log output in test; raw value truncated to 256 chars |

---

## Group 6 — DLT Consumer

| # | Check | How to verify |
|---|---|---|
| 6.1 | `dlt.recovered` counter incremented when DLT message processed successfully | `DltBatchMessageListenerTest` — mock succeeds on 3rd attempt; assert `dlt.recovered = 1` |
| 6.2 | `dlt.exhausted` counter incremented after 100 failures | Mock throws on every attempt; assert `dlt.exhausted = 1` after 100th attempt |
| 6.3 | `acknowledge()` called in both recovered and exhausted cases | Mock `Acknowledgment`; verify single call in both scenarios |
| 6.4 | DLT container concurrency is always 1 | `DltConsumerContainerFactoryTest` — assert each DLT container's concurrency = 1 |
| 6.5 | DLT consumer group ID is `event-ingest-dlt-group` | Assert consumer factory props contain `event-ingest-dlt-group` as `GROUP_ID_CONFIG` |
| 6.6 | DLT `group.instance.id` contains `dlt` prefix | Assert value contains `dlt` and `MY_POD_NAME` |

---

## Group 7 — Observability

| # | Check | How to verify |
|---|---|---|
| 7.1 | All 5 required meters present in `/actuator/prometheus` | `EventConsumerIT` — process at least one batch; `GET /actuator/prometheus` body contains all meter names |
| 7.2 | `kafka.consumer.batch.size` histogram has non-zero count | After `EventConsumerIT` batch consumed, assert count ≥ 1 |
| 7.3 | `kafka.consumer.lag` from Phase 3 updated for `event-ingest-group` | After `EventConsumerIT` completes, wait one monitor interval (use `@TestPropertySource` to set 500 ms); assert gauge value = 0 (all offsets committed) |
| 7.4 | Virtual thread executor confirmed active | Run `EventConsumerIT` with JFR/debug thread names; container threads named `virtual-*` or confirmed virtual via `Thread.currentThread().isVirtual()` log line |

---

## Group 8 — Integration Tests

| # | Check | How to verify |
|---|---|---|
| 8.1 | `EventConsumerIT` passes | `./gradlew :apps:event-ingest:itest` — test green |
| 8.2 | Consumer offsets committed for `event-raw-1` after consuming 100 messages | `AdminClient.listConsumerGroupOffsets("event-ingest-group")` — offset ≥ 100 for `event-raw-1` partitions |
| 8.3 | `ParseFailureIT` passes | Test green; `parse.failures` counter = 3; `event-raw-1.DLT` empty |
| 8.4 | `DltRetryIT` passes | Test green; `dlt.recovered` counter = 1 |
| 8.5 | No messages remain on `event-raw-1.DLT` after `ParseFailureIT` | `AdminClient.listOffsets("event-raw-1.DLT")` — all partition end offsets = 0 |

---

## Group 9 — CI & Build

| # | Check | How to verify |
|---|---|---|
| 9.1 | `./gradlew :apps:event-ingest:test` passes | All unit tests green (Groups 1–7) |
| 9.2 | `./gradlew :apps:event-ingest:itest` passes | All integration tests green (Group 8) |
| 9.3 | `./gradlew build` passes end-to-end | No compile errors in any module |
| 9.4 | No ByteBuddy errors | `jvmArgs '-Dnet.bytebuddy.experimental=true'` confirmed on all test tasks |

---

## Merge Criteria Summary

- [ ] `RawEvent` round-trip serialization test passes
- [ ] 8 `NewTopic` beans produced (4 main + 4 DLT) with correct partition counts
- [ ] `CooperativeStickyAssignor` confirmed in consumer factory props
- [ ] `group.instance.id` = `{MY_POD_NAME}-{topic}-{threadIndex}` confirmed per consumer thread
- [ ] No SASL credential in any committed file (`git grep` clean)
- [ ] 4 main containers created dynamically with `AckMode.MANUAL_IMMEDIATE`, `BatchAcknowledgingMessageListener`, and virtual thread executor
- [ ] Parse failure: 3-valid + 2-invalid batch passes only 3 events to pipeline, counter = 2, `acknowledge()` called once
- [ ] DLT listener exhaustion path: `dlt.exhausted` counter increments after 100 failures, offset still committed
- [ ] DLT topics remain empty after parse-failure test
- [ ] All 5 required meters visible in `/actuator/prometheus`
- [ ] `EventConsumerIT`, `ParseFailureIT`, `DltRetryIT` all pass
- [ ] `./gradlew build` passes end-to-end
