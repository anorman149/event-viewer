package org.eventviewer.opensearch;

public class MigrationData {

    private IndexSettings indexSettings;
    private ClusterSettings clusterSettings;

    public IndexSettings getIndexSettings() { return indexSettings; }
    public void setIndexSettings(IndexSettings indexSettings) { this.indexSettings = indexSettings; }

    public ClusterSettings getClusterSettings() { return clusterSettings; }
    public void setClusterSettings(ClusterSettings clusterSettings) { this.clusterSettings = clusterSettings; }
}
