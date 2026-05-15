package org.eventviewer.s3;

import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

class HiveKeyBuilderTest {

    private final HiveKeyBuilder builder = new HiveKeyBuilder("events", "ingest-pod-1");

    @Test
    void buildKey_includesAllHivePartitions() {
        Instant ts = Instant.parse("2026-05-14T15:30:00Z");

        String key = builder.buildKey(ts);

        assertThat(key).startsWith("events/year=2026/month=05/day=14/hour=15/pod=ingest-pod-1/");
        assertThat(key).endsWith(".zst");
    }

    @Test
    void buildKey_twoCallsProduceDifferentUuids() {
        Instant ts = Instant.parse("2026-05-14T10:00:00Z");

        String key1 = builder.buildKey(ts);
        String key2 = builder.buildKey(ts);

        assertThat(key1).isNotEqualTo(key2);
    }

    @Test
    void buildKey_padsMonthAndDayAndHour() {
        Instant ts = Instant.parse("2026-01-05T03:00:00Z");

        String key = builder.buildKey(ts);

        assertThat(key).contains("year=2026/month=01/day=05/hour=03");
    }

    @Test
    void buildKey_doesNotContainSchemaType() {
        Instant ts = Instant.now();

        String key = builder.buildKey(ts);

        assertThat(key).doesNotContain("schema_type");
    }
}
