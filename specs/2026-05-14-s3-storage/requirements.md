# Phase 5 — S3 Storage: Requirements

## Scope

Two modules are touched in this phase:

| Module | Work |
|---|---|
| `libs/s3-lib` | `S3Client`, `HiveKeyBuilder`, `EventFrameAssembler`, `EventFrameEncoder`, `S3AutoConfiguration`, `S3Properties`, `S3BucketInitializer` |
| `apps/event-ingest` | Replace the `IngestPipelineService.process()` stub with real S3 flush logic |

`apps/event-read` depends on `libs/s3-lib` for byte-range payload retrieval but is **not** wired in this phase — that belongs to Phase 11 (Payload Retrieval & API Contract).

---

## Decisions

### Batch flush strategy — per Kafka poll

Each Kafka consumer poll produces one S3 object. Batch size equals whatever Kafka delivers (up to `max.poll.records = 500`). No accumulation buffer; no time-based flush trigger. Flush, ack, repeat.

The architecture doc's 5,000-event target is a throughput design point, not a hard gate. At 1M events/sec across ~200 pods each consuming at ~500 events/poll, flush frequency is already ~1,000 PUT/sec platform-wide. Simplicity is preferred.

### Binary frame format — 4-byte length prefix per event

Each compressed event blob is stored as a **frame**:

```
[ int32 big-endian: compressed_length ][ compressed_bytes... ]
```

The file is a flat sequence of frames — no outer container, no magic bytes. The header exists so the file can be scanned sequentially without out-of-band offset metadata. When reading by byte range (the hot path), the header is bypassed entirely.

**Why no uncompressed length in the header:** ZSTD frame metadata already encodes the original size; callers who need it can call `Zstd.decompressedSize(compressedBytes)` without a second header field. Keeping the header at 4 bytes minimises overhead per event.

### Hive key path — no schema_type partition

The key path does not include a `schema_type` segment:

```
{prefix}/year=YYYY/month=MM/day=DD/hour=HH/pod={podName}/{uuid}.zst
```

Schema type is stored in OpenSearch metadata and is not needed as a partition dimension on S3. Removing it keeps the path shorter and eliminates the need to pass schema type through to `HiveKeyBuilder`.

### S3 object lifecycle — 5-day expiry via bucket rule

`S3BucketInitializer` applies a lifecycle rule to the bucket on every startup: objects under `s3.prefix` are automatically deleted by S3 after `s3.expiry-days` days (default `5`). This aligns with the OpenSearch retention window — OpenSearch metadata is purged at 5 days, and S3 objects expire at the same time, so no orphaned payloads accumulate.

The rule is idempotent: the rule ID `event-viewer-expiry` is stable, so AWS replaces rather than duplicates it on repeated calls. This is consistent with the project's pattern of applying infrastructure setup at startup (`KafkaAdmin` for topics, `OsSchemaManager` for OpenSearch in Phase 6).

The `expiry-days` property is configurable to allow different retention windows per environment without code changes.

### Per-event offset semantics

`EventOffset(long offset, int compressedLength)` stored in OpenSearch represents:

- `offset` — the byte position of the **compressed bytes** within the file (i.e., _after_ the 4-byte header for that event)
- `compressedLength` — the number of compressed bytes for that event

For a batch of N events, the offsets are:

```
event 0:  offset = 4
event 1:  offset = 4 + compressedLen[0] + 4
event k:  offset = k*4 + sum(compressedLen[0..k-1]) + 4
```

A byte-range GET of `[offset, offset + compressedLength - 1]` returns exactly the compressed blob for that event, which ZSTD can decompress independently.

### S3Client — Step Builder design

`S3Client` is a context holder (bucket, region, key prefix) with three factory methods, each returning a typed Step Builder:

```java
// create
s3Client.create().key(hiveKey).body(assembledBytes).execute()   // → String (full S3 key)

// get — full object
s3Client.get().key(k).execute()                                 // → byte[]

// get — single event by byte range
s3Client.get().key(k).range(offset, length).execute()           // → byte[]

// delete
s3Client.delete().key(k).execute()                              // → void
```

`.range()` is an optional step; omitting it fetches the full object. The SDK `GetObjectRequest` range header is set to `bytes=offset-(offset+length-1)`.

`S3Client` owns no Spring lifecycle concerns — all bean wiring is in `S3AutoConfiguration`.

### Compression

ZSTD level 3 (the `com.github.luben:zstd-jni` library). Level is configurable via `s3.compression-level` (default `3`). Each event is compressed independently before framing — the stored file is **not** a single ZSTD stream.

---

## Context

### Why independent per-event compression

A single ZSTD stream over the whole batch would compress better, but it requires decompressing from the start of the stream to reach any individual event. Independent compression allows a single byte-range GET + decompress to retrieve any event in O(1) S3 calls without reading the rest of the file.

The trade-off: ~5–15% worse compression ratio vs. a stream. At ZSTD level 3 and typical JSON event payloads, per-event compressed size is still 60–80% smaller than raw JSON.

### Why the offset points past the header

The byte-range GET path (`S3Client.get().range(offset, length)`) returns compressed bytes ready for ZSTD decompression — no header parsing required in the hot read path. The header is only consumed by sequential scan tools (Athena, restore jobs).

### LocalStack in dev

`S3AutoConfiguration` detects `s3.endpoint-override` and routes to that URL. In Docker Compose `SPRING_S3_ENDPOINT_OVERRIDE=http://localhost:4566` and `AWS_ACCESS_KEY_ID=test` / `AWS_SECRET_ACCESS_KEY=test` are set. Production uses the default AWS credential chain (IAM role / instance profile).

### EventFrameAssembler is part of s3-lib's public API

Both `event-ingest` (write path) and future callers that need to understand the format depend on it. It is not internal.

### No OpenSearch indexing in this phase

`IngestPipelineService.process()` will call `EventFrameAssembler` + `S3Client.create()` and return the `S3FlushResult`. The part of the method that indexes the result into OpenSearch remains a stub — that is Phase 7.
