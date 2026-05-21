package org.eventviewer.opensearch;

import java.util.List;

public interface OsMigration {

    int version();

    String description();

    List<MigrationData> data();
}
