package org.eventviewer.ingest.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.annotation.Timed;
import org.eventviewer.api.ingest.KafkaEventMessage;
import org.eventviewer.s3.CreateResult;
import org.eventviewer.s3.HiveKeyBuilder;
import org.eventviewer.s3.S3Client;
import org.eventviewer.s3.ZstdCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

@Service
public class IngestPipelineService {

    private static final Logger log = LoggerFactory.getLogger(IngestPipelineService.class);

    private final S3Client s3Client;
    private final HiveKeyBuilder hiveKeyBuilder;
    private final ZstdCodec zstdCodec;
    private final ObjectMapper objectMapper;

    public IngestPipelineService(S3Client s3Client,
                                  HiveKeyBuilder hiveKeyBuilder,
                                  ZstdCodec zstdCodec,
                                  ObjectMapper objectMapper) {
        this.s3Client = s3Client;
        this.hiveKeyBuilder = hiveKeyBuilder;
        this.zstdCodec = zstdCodec;
        this.objectMapper = objectMapper;
    }

    @Timed(value = "event.ingest.pipeline.process",
           description = "Time to process a batch of events through the ingest pipeline",
           histogram = true)
    public void process(List<KafkaEventMessage> messages) {
        if (messages.isEmpty()) {
            return;
        }

        List<byte[]> blobs = new ArrayList<>(messages.size());
        for (KafkaEventMessage event : messages) {
            blobs.add(serializeAndCompress(event));
        }

        byte[] body = concatenate(blobs);
        Instant timestamp = resolveTimestamp(messages);
        String key = hiveKeyBuilder.buildKey(timestamp);

        CreateResult result = s3Client.create().key(key).body(body).execute();
        log.debug("Wrote {} events ({} bytes) to S3 key {}", messages.size(), result.bytesWritten(), key);
    }

    /**
     * Returns a self-describing blob: [4-byte big-endian uncompressed length][ZSTD bytes].
     * The uncompressed length stored in the header allows byte-range callers to pre-allocate
     * the decompression buffer without parsing the ZSTD frame.
     */
    private byte[] serializeAndCompress(KafkaEventMessage event) {
        try {
            byte[] json = objectMapper.writeValueAsString(event).getBytes(StandardCharsets.UTF_8);
            byte[] compressed = zstdCodec.compress(json);
            return ByteBuffer.allocate(4 + compressed.length)
                    .putInt(json.length)
                    .put(compressed)
                    .array();
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize event " + event.eventId(), e);
        }
    }

    private byte[] concatenate(List<byte[]> blobs) {
        int totalCapacity = blobs.stream().mapToInt(b -> b.length).sum();
        ByteBuffer buffer = ByteBuffer.allocate(totalCapacity);
        blobs.forEach(buffer::put);
        return buffer.array();
    }

    private Instant resolveTimestamp(List<KafkaEventMessage> events) {
        return events.stream()
                .map(KafkaEventMessage::timestamp)
                .filter(ts -> ts != null)
                .findFirst()
                .orElse(Instant.now());
    }
}
