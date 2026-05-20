package org.eventviewer.opensearch;

public class MigrationData {

    private IlmPolicySettings ilmPolicySettings;
    private IndexSettings indexSettings;
    private ClusterSettings clusterSettings;

    public IlmPolicySettings getIlmPolicySettings() { return ilmPolicySettings; }
    public void setIlmPolicySettings(IlmPolicySettings ilmPolicySettings) { this.ilmPolicySettings = ilmPolicySettings; }

    public IndexSettings getIndexSettings() { return indexSettings; }
    public void setIndexSettings(IndexSettings indexSettings) { this.indexSettings = indexSettings; }

    public ClusterSettings getClusterSettings() { return clusterSettings; }
    public void setClusterSettings(ClusterSettings clusterSettings) { this.clusterSettings = clusterSettings; }
}
