package org.eventviewer.s3;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("s3")
public class S3Properties {

    private String bucket;
    private String region = "us-east-1";
    private String prefix = "events";
    private String endpointOverride;
    private int connectionPoolSize = 50;
    private int requestTimeoutMs = 5000;
    private int maxRetries = 3;

    public String getBucket() { return bucket; }
    public void setBucket(String bucket) { this.bucket = bucket; }

    public String getRegion() { return region; }
    public void setRegion(String region) { this.region = region; }

    public String getPrefix() { return prefix; }
    public void setPrefix(String prefix) { this.prefix = prefix; }

    public String getEndpointOverride() { return endpointOverride; }
    public void setEndpointOverride(String endpointOverride) { this.endpointOverride = endpointOverride; }

    public int getConnectionPoolSize() { return connectionPoolSize; }
    public void setConnectionPoolSize(int connectionPoolSize) { this.connectionPoolSize = connectionPoolSize; }

    public int getRequestTimeoutMs() { return requestTimeoutMs; }
    public void setRequestTimeoutMs(int requestTimeoutMs) { this.requestTimeoutMs = requestTimeoutMs; }

    public int getMaxRetries() { return maxRetries; }
    public void setMaxRetries(int maxRetries) { this.maxRetries = maxRetries; }
}
