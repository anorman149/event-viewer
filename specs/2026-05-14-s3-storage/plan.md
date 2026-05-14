# Phase 5 — S3 Storage: Implementation Plan

Task groups are ordered by dependency. Each group is independently committable.

---

## Group 1 — Dependencies & Autoconfiguration

**1.1** Add `aws-java-sdk-bom` (AWS SDK v2 BOM) to the root `build.gradle` dependency management block. Add `software.amazon.awssdk:s3` and `com.github.luben:zstd-jni` as `api` dependencies in `libs/s3-lib/build.gradle`.

**1.2** Create `S3Properties` in `libs/s3-lib` — `@ConfigurationProperties(prefix = "s3")` with fields: `bucket` (required), `region` (required), `prefix` (default `events`), `endpointOverride` (nullable, for LocalStack), `connectionPoolSize` (default `50`), `requestTimeoutMs` (default `5000`), `retryMaxAttempts` (default `3`), `compressionLevel` (default `3`).

**1.3** Create `S3AutoConfiguration` — `@AutoConfiguration` class that creates:
- An `software.amazon.awssdk.services.s3.S3Client` SDK bean (with `endpointOverride` applied when set, `ApacheHttpClient` connection pool, request timeout, retry policy from `S3Properties`)
- The `S3Client` facade bean (task 4.1), injecting the SDK client and `S3Properties`
- An `S3BucketInitializer` bean (task 1.4)
Wire via `META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports`.

**1.4** Create `S3BucketInitializer` — `SmartLifecycle` bean (or `ApplicationRunner`) that calls `PutBucketLifecycleConfigurationRequest` on startup. Applies one rule to the bucket: objects under `s3.prefix` expire after `s3.expiry-days` (default `5`). The rule ID is stable (`event-viewer-expiry`) so repeated calls are idempotent — AWS replaces the rule if it already exists with the same ID. Logs a single INFO line on success.

---

## Group 2 — HiveKeyBuilder

**2.1** Create `HiveKeyBuilder` in `libs/s3-lib` — static utility class with one method:
```java
public static String build(Instant timestamp, String podName)
```
Returns a path string (without bucket) matching: `{prefix}/year=YYYY/month=MM/day=DD/hour=HH/pod={podName}/{uuid}.zst`
where `{prefix}` comes from `S3Properties.prefix`. Pod name is read from `MY_POD_NAME` env var; method overload accepts explicit pod name for tests.

**2.2** Unit test `HiveKeyBuilderTest` — verify the path string for a known `Instant` and pod name; verify UUID segment is present and the extension is `.zst`.

---

## Group 3 — Event Frame Format

**3.1** Create `EventFrameEncoder` in `libs/s3-lib` — encodes a single raw event `byte[]`:
- Compress with ZSTD at the configured level using `com.github.luben.zstd.Zstd.compress()`
- Prepend a 4-byte big-endian `int32` = compressed byte count
- Return `EncodedFrame(byte[] framed, int compressedLength, int uncompressedLength)`

**3.2** Create `EventOffset` record in `libs/s3-lib` — `record EventOffset(long offset, int compressedLength)`. `offset` is the byte position of the compressed bytes within the assembled file (after the 4-byte header for that event).

**3.3** Create `AssembledBatch` record in `libs/s3-lib` — `record AssembledBatch(byte[] data, List<EventOffset> offsets, long totalCompressedBytes, long totalUncompressedBytes)`.

**3.4** Create `EventFrameAssembler` in `libs/s3-lib` — public API class:
```java
public AssembledBatch assemble(List<byte[]> rawEventPayloads, int compressionLevel)
```
Encodes each payload via `EventFrameEncoder`, concatenates frames into one `byte[]`, computes `EventOffset` for each event:
- `event[0].offset = 4`
- `event[k].offset = k*4 + sum(compressedLen[0..k-1]) + 4`
Returns `AssembledBatch`.

**3.5** Unit test `EventFrameAssemblerTest`:
- Assemble 3 known payloads; verify `data.length` equals sum of all frame sizes
- Verify `offset[0] == 4` and subsequent offsets account for preceding frame sizes
- For each event: slice `data[offset .. offset+compressedLength]`, ZSTD decompress, assert equals original payload
- Verify the 4-byte header at each frame start equals the `compressedLength` of that event

---

## Group 4 — S3Client Abstraction

**4.1** Create `S3Client` in `libs/s3-lib` — context holder with:
- Constructor accepting `software.amazon.awssdk.services.s3.S3Client sdkClient` and `S3Properties`
- Three factory methods: `create()`, `get()`, `delete()` returning typed Step Builders

**4.2** Create `CreateObjectBuilder` — Step Builder:
- Step 1: `.key(String relativeKey)` — required; appended to `s3Properties.prefix` to form the full key
- Step 2: `.body(byte[] data)` — required
- Terminal: `.execute()` → `String` (the full S3 key actually written); calls `PutObjectRequest` with bucket, key, content-length

**4.3** Create `GetObjectBuilder` — Step Builder:
- Step 1: `.key(String relativeKey)` — required
- Optional step: `.range(long offset, long length)` — sets `GetObjectRequest.range("bytes=offset-(offset+length-1)")`
- Terminal: `.execute()` → `byte[]`; calls `GetObjectRequest`; returns response bytes

**4.4** Create `DeleteObjectBuilder` — Step Builder:
- Step 1: `.key(String relativeKey)` — required
- Terminal: `.execute()` → `void`; calls `DeleteObjectRequest`

**4.5** Unit tests (mock `software.amazon.awssdk.services.s3.S3Client`):
- `CreateObjectBuilderTest` — verify `PutObjectRequest` has correct bucket, key, and content-length
- `GetObjectBuilderTest` — verify full-object request has no range header; verify ranged request sets `Range: bytes=100-199`
- `DeleteObjectBuilderTest` — verify `DeleteObjectRequest` has correct bucket and key

---

## Group 5 — Observability

**5.1** Add `@Timed(value = "s3.client.create", histogram = true)` on `CreateObjectBuilder.execute()`; `DistributionSummary` named `s3.client.bytes.written` recording body length; `Counter` named `s3.client.create.failures` incremented on SDK exceptions.

**5.2** Add `@Timed(value = "s3.client.get", histogram = true)` on `GetObjectBuilder.execute()`; `DistributionSummary` named `s3.client.bytes.read`; `Counter` named `s3.client.get.failures`.

**5.3** Add `@Timed(value = "s3.client.delete", histogram = true)` on `DeleteObjectBuilder.execute()`; `Counter` named `s3.client.delete.failures`.

---

## Group 6 — event-ingest Pipeline Wiring

**6.1** Add `libs/s3-lib` as an `implementation` dependency in `apps/event-ingest/build.gradle`.

**6.2** Add `S3FlushResult` record to `libs/s3-lib` — `record S3FlushResult(String s3Key, List<EventOffset> offsets, long totalCompressedBytes, long totalUncompressedBytes)`. This is the return type of `IngestPipelineService.process()`.

**6.3** Implement `IngestPipelineService.process(List<EventRecord> batch)` in `apps/event-ingest`:
1. Serialize each `EventRecord` to JSON `byte[]` via injected `ObjectMapper`
2. Call `EventFrameAssembler.assemble(payloads, compressionLevel)` → `AssembledBatch`
3. Build the Hive key: `HiveKeyBuilder.build(batchTimestamp, schemaType, podName)`
4. Call `s3Client.create().key(hiveKey).body(assembled.data()).execute()` → `String s3Key`
5. Return `S3FlushResult(s3Key, assembled.offsets(), assembled.totalCompressedBytes(), assembled.totalUncompressedBytes())`
6. OpenSearch indexing remains a stub log statement — Phase 7

**6.4** Add `@Timed(value = "ingest.pipeline.process", histogram = true)` on `IngestPipelineService.process()`; `DistributionSummary` named `ingest.pipeline.batch.events` recording batch size.

---

## Group 7 — Integration Tests (LocalStack)

**7.1** Add `s3.bucket`, `s3.region`, `s3.endpoint-override` to `apps/event-ingest/src/itest/resources/application-itest.yml`; ensure LocalStack is declared in `docker-compose-test.yml`.

**7.2** Write `S3StorageIT` in `apps/event-ingest/src/itest/` extending `BaseTest`:
- Build a list of 5 `EventRecord` objects using Java constructors (no raw JSON strings)
- Call `IngestPipelineService.process(batch)` directly (no HTTP layer needed)
- Assert the returned `S3FlushResult.s3Key()` is non-null and matches the expected Hive path pattern
- For each `EventOffset` in the result: call `s3Client.get().key(s3Key).range(offset, compressedLength).execute()`, ZSTD decompress the result, deserialize via `ObjectMapper`, assert the event fields match the original

**7.3** Write a second `S3ClientIT` in `libs/s3-lib/src/itest/` (if the lib has an itest source set) or add to the app itest:
- `create()` → object exists (verify with `get()`)
- `delete()` → subsequent `get()` throws / returns not-found
