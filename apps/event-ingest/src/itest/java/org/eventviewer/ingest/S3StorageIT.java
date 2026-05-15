package org.eventviewer.ingest;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.eventviewer.api.ingest.KafkaEventMessage;
import org.eventviewer.s3.CreateResult;
import org.eventviewer.s3.HiveKeyBuilder;
import org.eventviewer.s3.S3Client;
import org.eventviewer.s3.ZstdCodec;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class S3StorageIT extends BaseTest {

    @Autowired
    private S3Client s3Client;

    @Autowired
    private HiveKeyBuilder hiveKeyBuilder;

    @Autowired
    private ZstdCodec zstdCodec;

    @Autowired
    private ObjectMapper objectMapper;

    @Test
    void write_zstdCompressedEvents_objectExistsInS3() throws Exception {
        List<KafkaEventMessage> events = buildEvents("order-created", 3);
        String key = writeToS3(events);

        byte[] fetched = s3Client.get().key(key).execute();

        assertThat(fetched).isNotEmpty();
    }

    @Test
    void write_multipleEvents_perEventByteOffsetsCorrect() throws Exception {
        List<KafkaEventMessage> events = buildEvents("payment-processed", 5);
        List<byte[]> blobs = buildBlobs(events);
        List<long[]> offsets = computeOffsets(blobs);

        byte[] body = concatenate(blobs);
        String key = hiveKeyBuilder.buildKey(Instant.now());
        s3Client.create().key(key).body(body).execute();

        for (int i = 0; i < events.size(); i++) {
            long offset = offsets.get(i)[0];
            int length = (int) offsets.get(i)[1];

            byte[] rangeBytes = s3Client.get().key(key).range(offset, length).execute();
            assertThat(rangeBytes).isEqualTo(blobs.get(i));
        }
    }

    @Test
    void write_multipleEvents_eachBlobIndependentlyDecompressible() throws Exception {
        List<KafkaEventMessage> events = buildEvents("user-signup", 4);
        List<byte[]> blobs = buildBlobs(events);
        List<long[]> offsets = computeOffsets(blobs);

        byte[] body = concatenate(blobs);
        String key = hiveKeyBuilder.buildKey(Instant.now());
        s3Client.create().key(key).body(body).execute();

        for (int i = 0; i < events.size(); i++) {
            long offset = offsets.get(i)[0];
            int length = (int) offsets.get(i)[1];

            byte[] rangeBytes = s3Client.get().key(key).range(offset, length).execute();

            // Parse self-describing blob: [4B uncompressed_len][ZSTD data]
            ByteBuffer bb = ByteBuffer.wrap(rangeBytes);
            int uncompressedLength = bb.getInt();
            byte[] zstdData = new byte[rangeBytes.length - 4];
            bb.get(zstdData);

            byte[] decompressed = zstdCodec.decompress(zstdData);

            assertThat(decompressed.length).isEqualTo(uncompressedLength);
            KafkaEventMessage recovered = objectMapper.readValue(
                    new String(decompressed, StandardCharsets.UTF_8), KafkaEventMessage.class);
            assertThat(recovered.eventId()).isEqualTo(events.get(i).eventId());
            assertThat(recovered.schemaType()).isEqualTo(events.get(i).schemaType());
        }
    }

    @Test
    void write_selfDescribingBlobHeader_containsCorrectUncompressedLength() throws Exception {
        List<KafkaEventMessage> events = buildEvents("header-check", 1);
        byte[] json = objectMapper.writeValueAsString(events.get(0)).getBytes(StandardCharsets.UTF_8);
        byte[] compressed = zstdCodec.compress(json);

        // Manually build blob as IngestPipelineService would
        byte[] blob = ByteBuffer.allocate(4 + compressed.length)
                .putInt(json.length)
                .put(compressed)
                .array();

        ByteBuffer bb = ByteBuffer.wrap(blob);
        int storedUncompressedLength = bb.getInt();

        assertThat(storedUncompressedLength).isEqualTo(json.length);
    }

    @Test
    void delete_removesObjectFromS3() throws Exception {
        List<KafkaEventMessage> events = buildEvents("delete-test", 1);
        String key = writeToS3(events);

        s3Client.delete().key(key).execute();

        // After deletion the key should no longer be retrievable.
        org.junit.jupiter.api.Assertions.assertThrows(Exception.class,
                () -> s3Client.get().key(key).execute());
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    private List<KafkaEventMessage> buildEvents(String schemaType, int count) {
        List<KafkaEventMessage> events = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            events.add(new KafkaEventMessage(
                    UUID.randomUUID(),
                    schemaType,
                    Instant.now(),
                    Instant.now(),
                    Map.of("index", i, "data", "payload-" + i)));
        }
        return events;
    }

    private String writeToS3(List<KafkaEventMessage> events) throws Exception {
        List<byte[]> blobs = buildBlobs(events);
        byte[] body = concatenate(blobs);
        String key = hiveKeyBuilder.buildKey(Instant.now());
        CreateResult result = s3Client.create().key(key).body(body).execute();
        return result.key();
    }

    /** Mirrors IngestPipelineService: each blob is [4B uncompressed_len][ZSTD bytes]. */
    private List<byte[]> buildBlobs(List<KafkaEventMessage> events) throws Exception {
        List<byte[]> blobs = new ArrayList<>();
        for (KafkaEventMessage event : events) {
            byte[] json = objectMapper.writeValueAsString(event).getBytes(StandardCharsets.UTF_8);
            byte[] compressed = zstdCodec.compress(json);
            byte[] blob = ByteBuffer.allocate(4 + compressed.length)
                    .putInt(json.length)
                    .put(compressed)
                    .array();
            blobs.add(blob);
        }
        return blobs;
    }

    private List<long[]> computeOffsets(List<byte[]> blobs) {
        List<long[]> offsets = new ArrayList<>();
        long position = 0;
        for (byte[] blob : blobs) {
            offsets.add(new long[]{position, blob.length});
            position += blob.length;
        }
        return offsets;
    }

    private byte[] concatenate(List<byte[]> blobs) {
        int totalCapacity = blobs.stream().mapToInt(b -> b.length).sum();
        ByteBuffer buffer = ByteBuffer.allocate(totalCapacity);
        blobs.forEach(buffer::put);
        return buffer.array();
    }
}
