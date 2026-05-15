package org.eventviewer.opensearch;

import java.time.Duration;

public class ClusterSettings {

    private int searchMaxBuckets = 10000;
    private Duration searchCancelerAfter = Duration.ofSeconds(30);

    public int getSearchMaxBuckets() { return searchMaxBuckets; }
    public void setSearchMaxBuckets(int searchMaxBuckets) { this.searchMaxBuckets = searchMaxBuckets; }

    public Duration getSearchCancelerAfter() { return searchCancelerAfter; }
    public void setSearchCancelerAfter(Duration searchCancelerAfter) { this.searchCancelerAfter = searchCancelerAfter; }
}
