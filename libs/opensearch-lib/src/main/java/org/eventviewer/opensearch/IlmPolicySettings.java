package org.eventviewer.opensearch;

import java.time.Duration;

public class IlmPolicySettings {

    private String policyName;
    private long rolloverMaxSizeGb = 130;
    private Duration rolloverMaxAge = Duration.ofHours(12);
    private Duration warmRetention = Duration.ofDays(4);

    public String getPolicyName() { return policyName; }
    public void setPolicyName(String policyName) { this.policyName = policyName; }

    public long getRolloverMaxSizeGb() { return rolloverMaxSizeGb; }
    public void setRolloverMaxSizeGb(long rolloverMaxSizeGb) { this.rolloverMaxSizeGb = rolloverMaxSizeGb; }

    public Duration getRolloverMaxAge() { return rolloverMaxAge; }
    public void setRolloverMaxAge(Duration rolloverMaxAge) { this.rolloverMaxAge = rolloverMaxAge; }

    public Duration getWarmRetention() { return warmRetention; }
    public void setWarmRetention(Duration warmRetention) { this.warmRetention = warmRetention; }
}
