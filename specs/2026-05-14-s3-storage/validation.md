# Phase 5 — S3 Storage: Validation

Each section maps to a deliverable. All checks must pass before the PR can merge.

---

## HiveKeyBuilder

- Given a fixed `Instant`, `schemaType = "order-created"`, `podName = "ingest-abc123"`, the returned path starts with `events/year=` and ends with `.zst`
- The path contains all six partition segments (`year=`, `month=`, `day=`, `hour=`, `schema_type=`, `pod=`) in the correct order
- The UUID segment is present and parses as a valid `UUID`
- Changing the `Instant` by one hour changes only the `hour=` segment

---

## Event Frame Format

- Calling `EventFrameAssembler.assemble()` on an empty list returns an `AssembledBatch` with a zero-length `data` array and an empty `offsets` list
- For a single-event batch: `data.length == 4 + compressedLength`; `offsets[0].offset == 4`; `offsets[0].compressedLength == data.length - 4`
- The 4 bytes at position 0 of `data`, read as a big-endian `int32`, equal `offsets[0].compressedLength`
- For a multi-event batch: for each event `k`, the 4 bytes at `data[offsets[k].offset - 4]` equal `offsets[k].compressedLength`
- For each event: `Arrays.copyOfRange(data, offset, offset + compressedLength)` ZSTD-decompresses to the original raw payload
- Offsets are strictly increasing and non-overlapping
- `totalCompressedBytes` equals `data.length - (4 * eventCount)` (header bytes excluded)
- `totalUncompressedBytes` equals the sum of all original payload lengths

---

## S3Client — Create

- `CreateObjectBuilder.execute()` calls `PutObjectRequest` with the correct bucket and the key formed by appending the caller's relative key to `s3Properties.prefix`
- The returned `String` equals the full S3 key (prefix + relative key)
- Calling `.execute()` without `.body()` throws an `IllegalStateException` (or equivalent compile-time enforcement via step type)

## S3Client — Get (full object)

- `GetObjectBuilder.execute()` (no `.range()`) issues a `GetObjectRequest` with no `Range` header
- The returned `byte[]` contains the full object bytes

## S3Client — Get (byte range)

- `GetObjectBuilder.range(100, 50).execute()` sets `Range: bytes=100-149` on the `GetObjectRequest`
- The returned `byte[]` contains exactly the bytes in that range

## S3Client — Delete

- `DeleteObjectBuilder.execute()` calls `DeleteObjectRequest` with the correct bucket and key
- A subsequent full `get()` on the same key throws or returns a not-found signal (verified in itest)

---

## S3AutoConfiguration

- When `s3.bucket`, `s3.region`, and `s3.endpoint-override` are set, the `S3Client` facade bean is present in the application context
- When `s3.endpoint-override` is set, the SDK client uses that URL (verified in itest by the LocalStack calls succeeding)
- When `s3.endpoint-override` is absent, no override is set on the SDK client (no test required — production path)

---

## IngestPipelineService.process()

- Given a list of `EventRecord` objects, `process()` returns a non-null `S3FlushResult`
- `S3FlushResult.s3Key()` is non-null and non-empty
- `S3FlushResult.offsets().size()` equals the number of events in the input list
- `S3FlushResult.totalCompressedBytes()` is less than `S3FlushResult.totalUncompressedBytes()` for any non-trivial JSON payload (compression sanity check)

---

## itest — end-to-end against LocalStack

- `IngestPipelineService.process(batch)` runs without exception against a live LocalStack S3 container
- The S3 object at `S3FlushResult.s3Key()` exists (verified via `S3Client.get()`)
- For every `EventOffset` in the result:
  - `s3Client.get().key(s3Key).range(offset, compressedLength).execute()` returns bytes that ZSTD decompress without error
  - The decompressed bytes deserialize to an `EventRecord` whose fields match the original
- Deleting the object and attempting a full `get()` does not return the old data

---

## Merge Criteria

- All unit tests pass (`./gradlew :libs:s3-lib:test :apps:event-ingest:test`)
- All itests pass (`./gradlew :apps:event-ingest:itest`)
- `libs/s3-lib` has no dependency on any `apps/*` module (verify in `libs/s3-lib/build.gradle`)
- No AWS credentials or endpoint URLs are hardcoded — only read from `S3Properties` / environment
- `S3Client` has no `@Component`, `@Service`, or `@Bean` annotations — all wiring is in `S3AutoConfiguration`
- Metric names use dot notation (no underscores per platform Rules)
- No raw JSON strings in test data — all test objects constructed via Java records / constructors
- Controller tests (if any) are in `src/itest/`; pure unit tests are in `src/test/`
