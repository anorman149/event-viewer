# Phase 4 — Kafka Consumer: Requirements

## Scope

Build a production-grade Kafka consumer inside `apps/event-ingest`. All listeners are created **dynamically from properties** — no `@KafkaListener` annotations anywhere. The phase delivers: correct consumer group membership that survives pod restarts, cooperative partition rebalancing, accurate manual acknowledgment, SASL/SCRAM security, and a full retry → DLT pipeline. Parse failures are silently metered and skipped so they never pollute the DLT.

The downstream pipeline (`IngestPipelineService`) is a stub in this phase — it receives the deserialized batch and returns immediately. Phases 5 and 6 fill in the S3 and OpenSearch writes.

---

## Topic Topology

### Production

| Topic name | Partitions | Replicas | Purpose |
|---|---|---|---|
| `event-raw-1` | 80 | 1 | Primary ingest shard 1 |
| `event-raw-2` | 80 | 1 | Primary ingest shard 2 |
| `event-raw-3` | 80 | 1 | Primary ingest shard 3 |
| `event-raw-4` | 80 | 1 | Primary ingest shard 4 |
| `event-raw-1.DLT` | 4 | 1 | Dead-letter for shard 1 |
| `event-raw-2.DLT` | 4 | 1 | Dead-letter for shard 2 |
| `event-raw-3.DLT` | 4 | 1 | Dead-letter for shard 3 |
| `event-raw-4.DLT` | 4 | 1 | Dead-letter for shard 4 |

**Rationale for 4 × 80:** 320 total partitions across 4 independent topic shards. Independent topics allow per-shard retention policies, per-shard lag monitoring, and no single topic growing to 80 partitions in one Kafka admin view. With 4 pods × 4 containers × concurrency 20 = 320 consumer threads → exactly 1 partition per consumer thread at full scale.

### Local / CI

Same 4 topic names, 2 partitions each (configurable via `application-local.yml`). DLT topics at 1 partition. Concurrency set to 1.

---

## Consumer Settings

Every setting is justified for prod-scale operation. These are non-negotiable defaults; overrides require explicit documentation of why.

| Setting | Prod value | Local value | Rationale |
|---|---|---|---|
| `group.id` | `event-ingest-group` | same | Single consumer group for all pods and all topic containers |
| `group.instance.id` | `{MY_POD_NAME}-{topic}-{threadIndex}` | same | **Static membership.** Unique per consumer thread; stable across pod restarts. Broker waits `session.timeout.ms` before rebalancing a missing static member — a normal pod restart (< 30 s) rejoins before the timeout and causes zero rebalance. |
| `partition.assignment.strategy` | `CooperativeStickyAssignor` | same | Incremental cooperative rebalancing: only the partitions that actually need to move are revoked. No stop-the-world consumer pause. Sticky assignment minimises partition reshuffling across restarts. |
| `enable.auto.commit` | `false` | same | Manual commitment only. Offsets committed after batch fully processed. Prevents data loss if the process dies mid-batch. |
| `auto.offset.reset` | `earliest` | same | Begin from earliest uncommitted offset if no stored offset exists. |
| `heartbeat.interval.ms` | `3000` | same | Standard 3 s. Must be < `session.timeout.ms / 3` (45000 / 3 = 15000). Sends heartbeats 5× per session window. |
| `session.timeout.ms` | `45000` | same | 45 s. Long enough to survive large-batch processing pauses. With `group.instance.id` this is the grace window after a pod crash before Kafka reassigns partitions. |
| `max.poll.interval.ms` | `300000` | same | 5 min. Must exceed the maximum time to process a full batch including all downstream I/O (S3 flush + OpenSearch bulk index in later phases). Consumer is fenced if `poll()` is not called within this window. |
| `max.poll.records` | `500` | `50` | Max records returned per `poll()` per consumer thread. At 320 threads × 500 = 160,000 records per poll cycle across the group. Local uses 50 to keep itest fast. |
| `fetch.min.bytes` | `1024` | `1` | Broker waits until at least 1 KB is available before returning a fetch. Reduces empty-fetch round-trips under low load. Local uses 1 (respond immediately). |
| `fetch.max.wait.ms` | `500` | `100` | Max broker wait time to satisfy `fetch.min.bytes`. 500 ms balances throughput vs latency. |
| `fetch.max.bytes` | `52428800` | `1048576` | 50 MB max per fetch request total (prod). 1 MB local. |
| `max.partition.fetch.bytes` | `1048576` | `1048576` | 1 MB max data returned per partition per fetch. |
| `key.deserializer` | `StringDeserializer` | same | String keys |
| `value.deserializer` | `StringDeserializer` | same | Raw JSON string; domain deserialization happens inside the listener (parse failure can be caught and skipped at record level) |
| `security.protocol` | `SASL_SSL` | `PLAINTEXT` | SASL injected via env vars in prod; plain in local Docker Compose |
| `sasl.mechanism` | `SCRAM-SHA-512` | — | SCRAM-SHA-512 for prod; `sasl.jaas.config` injected via `KAFKA_SASL_JAAS_CONFIG` env var; never committed |
| `ack.mode` (Spring) | `MANUAL_IMMEDIATE` | same | Offset is committed immediately when the listener calls `acknowledgment.acknowledge()`. No auto-commit. No implicit commit after listener return. The listener owns when and whether offsets advance. |
| Concurrency | Computed at runtime | Computed at runtime | `computeConcurrency(partitionsPerTopic, podCount)` = `max(1, partitionsPerTopic / podCount)`. `podCount` read from `INGEST_POD_COUNT` env var (set in K8s deployment to match the replica count); defaults to 1 if absent. Example: 80 partitions / 4 pods = 20. Scales correctly as the deployment is resized without a config change. |

---

## Dynamic Listener Architecture

### Why No `@KafkaListener`

Annotation-based listeners are statically declared at compile time. With 4 topics from a property list, `@KafkaListener` would require hardcoded topic names or SpEL hacks. Dynamic creation gives:
- Topic list fully controlled by `application.yml` (no recompile to add/remove a shard)
- Per-container lifecycle management (start/stop/pause individual containers)
- Testable container factory (unit-testable bean, no Spring test context required)
- Clean support for per-container `group.instance.id` injection

### Container-per-Topic Model

One `ConcurrentMessageListenerContainer` is created per main topic. Each container:
- Has its own `ConsumerFactory` instance configured with the topic-specific `group.instance.id` base (`{MY_POD_NAME}-{topic}`)
- Runs at configured concurrency (each thread = one Kafka consumer in the group)
- Thread index appended to `group.instance.id`: `{MY_POD_NAME}-{topic}-0`, `{MY_POD_NAME}-{topic}-1`, …
- Has its own `DefaultErrorHandler` with `DeadLetterPublishingRecoverer` pointing to `{topic}.DLT`
- Shares the same `EventBatchListener` instance (which implements `BatchAcknowledgingMessageListener<String, String>`)

One `ConcurrentMessageListenerContainer` is also created per DLT topic (4 DLT containers). DLT containers use a separate `DltBatchMessageListener` (also implements `BatchAcknowledgingMessageListener<String, String>`) with its own 100-retry policy.

### Parse Failure Handling

Malformed JSON is caught **inside the `BatchAcknowledgingMessageListener` before it reaches the `DefaultErrorHandler`**. Behavior:
1. `ObjectMapper.readValue(rawValue, RawEvent.class)` throws `JsonProcessingException`
2. Catch block: log ERROR with topic, partition, offset, first 256 chars of raw value
3. Increment `kafka.consumer.parse.failures` counter tagged with `topic`
4. Continue to the next record in the batch — no rethrow, no DLT
5. The successfully deserialized records in the same batch are processed normally

The DLT therefore receives only infrastructure failures (S3 unavailable, OpenSearch unavailable, etc.) — never bad data. This keeps the DLT clean and its retry logic meaningful.

---

## Error Handling & DLT

### Main Topic Error Handler

`DefaultErrorHandler` configured on each main topic container:
- Back-off: `ExponentialBackOffWithMaxRetries(3)` — intervals: 1 s → 2 s → 4 s
- Recoverer: `DeadLetterPublishingRecoverer(kafkaTemplate)` → publishes to `{topic}.DLT` with original headers preserved
- Non-retryable: `JsonProcessingException` and `MismatchedInputException` are in `addNotRetryableExceptions` — if one somehow escapes the listener, it goes straight to DLT without 3 retries

### DLT Consumer

`DltBatchMessageListener` — retries the message up to 100 times with fixed 5-second delay:
- On success within 100 attempts: acknowledge + counter `kafka.consumer.dlt.recovered`
- On 100th failure: log ERROR (full stack trace + raw message bytes), increment `kafka.consumer.dlt.exhausted`, acknowledge (do not loop forever)
- DLT messages are processed in isolation — one at a time, concurrency = 1

---

## `IngestPipelineService` Stub

`IngestPipelineService.process(List<RawEvent> events, String topic)` — in Phase 4 this method:
- Logs batch size + topic at DEBUG
- Records `kafka.consumer.batch.size` `DistributionSummary`
- Returns immediately

Phases 5 and 6 replace the stub body with S3 flush and OpenSearch indexing. The interface does not change.

---

## SASL Security

| Environment | `security.protocol` | `sasl.mechanism` | Credential source |
|---|---|---|---|
| Local Docker Compose | `PLAINTEXT` | — | None |
| Production K8s | `SASL_SSL` | `SCRAM-SHA-512` | `KAFKA_SASL_JAAS_CONFIG` env var (K8s Secret → env mount) |

`sasl.jaas.config` is **never** written to any committed file. The `application-prod.yml` sets `security.protocol` and `sasl.mechanism` only; the JAAS config arrives at runtime via `spring.kafka.consumer.properties.sasl.jaas.config` environment variable override.

---

## `RawEvent` Model

Defined in `libs/event-api`. Fields:
- `eventId` (UUID, required)
- `schemaType` (String, required)
- `schemaVersion` (String, optional)
- `timestamp` (Instant, required)
- `payload` (JsonNode — raw JSON subtree of the event body)

`ObjectMapper` deserializes the raw JSON string into `RawEvent`. If the JSON is valid but missing required fields, the resulting `RawEvent` will have null fields — these are caught by rule validation in Phase 7, not here.

---

## Out of Scope

- S3 writes (Phase 5)
- OpenSearch indexing (Phase 6)
- Rule evaluation (Phase 7)
- Full ingest benchmark (Phase 8)
- Any PostgreSQL changes

---

## Non-Functional Requirements

| Concern | Requirement |
|---|---|
| Pod restart resilience | A pod that restarts within 45 s must rejoin its partitions without triggering a group rebalance |
| Rebalance impact | A full consumer group rebalance (pod scale-out) must not pause message processing for more than `session.timeout.ms` (45 s) |
| Parse failure isolation | A single malformed record must not delay or fail processing of the other records in its batch |
| DLT cleanliness | DLT topics must receive only infrastructure-failure messages, never parse-failure messages |
| No credential commits | `sasl.jaas.config` must not appear in any committed file |
| Observability | Five required meters: `kafka.consumer.parse.failures`, `kafka.consumer.dlt.received`, `kafka.consumer.dlt.recovered`, `kafka.consumer.dlt.exhausted`, `kafka.consumer.batch.size` DistributionSummary |

---

## Dependencies

- Phase 3 ✅ — `libs/leader` and `KafkaLagMonitor` exist; consumer group ID registered to `KafkaLagMonitor` at startup
- Phase 2 ✅ — Kafka topics provisioned via `KafkaAdmin`; topic property config pattern established in `event-ingest`
- `libs/event-api` — add `RawEvent` record
- `apps/event-ingest/build.gradle` — no new dependencies needed (Spring Kafka already present)
