package org.eventviewer.ingest.migration.dev;

import org.eventviewer.ingest.migration.EventStorageMigration;

public class DevEventStorageMigration extends EventStorageMigration {

    @Override
    protected int shards() { return 1; }

    @Override
    protected int replicas() { return 0; }

    @Override
    protected int refreshIntervalSecs() { return 1; }
}
