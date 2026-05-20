package org.eventviewer.ingest.migration;

import org.eventviewer.opensearch.ClusterSettings;
import org.eventviewer.opensearch.MigrationData;
import org.eventviewer.opensearch.OsMigration;

import java.time.Duration;
import java.util.List;

public class EventReplicaMigration implements OsMigration {

    @Override
    public int order() {
        return 1;
    }

    @Override
    public String name() {
        return "001_cluster_replica_settings";
    }

    @Override
    public List<MigrationData> data() {
        ClusterSettings clusterSettings = new ClusterSettings();
        clusterSettings.setSearchMaxBuckets(10000);
        clusterSettings.setSearchCancelerAfter(Duration.ofSeconds(30));

        MigrationData data = new MigrationData();
        data.setClusterSettings(clusterSettings);
        return List.of(data);
    }
}
