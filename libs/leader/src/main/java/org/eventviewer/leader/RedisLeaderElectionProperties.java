package org.eventviewer.leader;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "leader-election")
public class RedisLeaderElectionProperties {
    private String lockName = "leader:event-ingest";
    private long retryIntervalMs = 2000L;
    private long lockWatchdogTimeoutMs = 30000L;
    private boolean clustered = false;
    private String host = "localhost";

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    private String password;

    public boolean isWatchdogTimeoutGreaterThanRetryInterval() {
        return lockWatchdogTimeoutMs > retryIntervalMs;
    }

    public String getLockName() { return lockName; }
    public void setLockName(String lockName) { this.lockName = lockName; }

    public long getRetryIntervalMs() { return retryIntervalMs; }
    public void setRetryIntervalMs(long retryIntervalMs) { this.retryIntervalMs = retryIntervalMs; }

    public long getLockWatchdogTimeoutMs() { return lockWatchdogTimeoutMs; }
    public void setLockWatchdogTimeoutMs(long lockWatchdogTimeoutMs) { this.lockWatchdogTimeoutMs = lockWatchdogTimeoutMs; }

    public boolean isClustered() {
        return clustered;
    }

    public void setClustered(boolean clustered) {
        this.clustered = clustered;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }
}
