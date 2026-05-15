package org.eventviewer.s3;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.UUID;

public class HiveKeyBuilder {

    private final String prefix;
    private final String podName;

    public HiveKeyBuilder(String prefix, String podName) {
        this.prefix = prefix;
        this.podName = podName;
    }

    public String buildKey(Instant timestamp) {
        ZonedDateTime zdt = timestamp.atZone(ZoneOffset.UTC);
        return "%s/year=%04d/month=%02d/day=%02d/hour=%02d/pod=%s/%s.zst".formatted(
                prefix,
                zdt.getYear(),
                zdt.getMonthValue(),
                zdt.getDayOfMonth(),
                zdt.getHour(),
                podName,
                UUID.randomUUID()
        );
    }
}
