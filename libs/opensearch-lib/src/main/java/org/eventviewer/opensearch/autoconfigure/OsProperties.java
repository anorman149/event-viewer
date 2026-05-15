package org.eventviewer.opensearch.autoconfigure;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "opensearch")
public class OsProperties {

    private String host = "localhost";
    private int port = 9200;
    private boolean useSsl = false;
    private String username;
    private String password;
    private Bulk bulk = new Bulk();
    private Migration migration = new Migration();

    public String getHost() { return host; }
    public void setHost(String host) { this.host = host; }

    public int getPort() { return port; }
    public void setPort(int port) { this.port = port; }

    public boolean isUseSsl() { return useSsl; }
    public void setUseSsl(boolean useSsl) { this.useSsl = useSsl; }

    public String getUsername() { return username; }
    public void setUsername(String username) { this.username = username; }

    public String getPassword() { return password; }
    public void setPassword(String password) { this.password = password; }

    public Bulk getBulk() { return bulk; }
    public void setBulk(Bulk bulk) { this.bulk = bulk; }

    public Migration getMigration() { return migration; }
    public void setMigration(Migration migration) { this.migration = migration; }

    public static class Bulk {
        private int flushThreshold = 500;
        private long flushIntervalMs = 5000;

        public int getFlushThreshold() { return flushThreshold; }
        public void setFlushThreshold(int flushThreshold) { this.flushThreshold = flushThreshold; }

        public long getFlushIntervalMs() { return flushIntervalMs; }
        public void setFlushIntervalMs(long flushIntervalMs) { this.flushIntervalMs = flushIntervalMs; }
    }

    public static class Migration {
        private String indexName = "migrations";

        public String getIndexName() { return indexName; }
        public void setIndexName(String indexName) { this.indexName = indexName; }
    }
}
