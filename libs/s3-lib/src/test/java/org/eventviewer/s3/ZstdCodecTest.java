package org.eventviewer.s3;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ZstdCodecTest {

    private final ZstdCodec codec = new ZstdCodec();

    @Test
    void compressAndDecompress_roundtrip() {
        byte[] original = "{\"event_id\":\"abc\",\"schema_type\":\"order-created\"}".getBytes(StandardCharsets.UTF_8);

        byte[] compressed = codec.compress(original);
        byte[] decompressed = codec.decompress(compressed);

        assertThat(decompressed).isEqualTo(original);
    }

    @Test
    void compress_reducesSizeForRepetitiveData() {
        String repeated = "x".repeat(10_000);
        byte[] original = repeated.getBytes(StandardCharsets.UTF_8);

        byte[] compressed = codec.compress(original);

        assertThat(compressed.length).isLessThan(original.length);
    }

    @Test
    void twoCompressedBlobs_areIndependentlyDecompressible() {
        byte[] event1 = "event-one".getBytes(StandardCharsets.UTF_8);
        byte[] event2 = "event-two".getBytes(StandardCharsets.UTF_8);

        byte[] blob1 = codec.compress(event1);
        byte[] blob2 = codec.compress(event2);

        assertThat(codec.decompress(blob1)).isEqualTo(event1);
        assertThat(codec.decompress(blob2)).isEqualTo(event2);
    }

    @Test
    void decompress_throwsOnInvalidData() {
        assertThatThrownBy(() -> codec.decompress(new byte[]{0, 1, 2, 3}))
                .isInstanceOf(Exception.class);
    }
}
