package org.eventviewer.opensearch;

import java.util.List;

public interface OsMigration {

    int order();

    String name();

    List<MigrationData> data();
}
