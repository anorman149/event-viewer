package org.eventviewer.ingest.migration.prod;

import org.eventviewer.ingest.migration.EventStorageMigration;

public class ProdEventStorageMigration extends EventStorageMigration {

    @Override
    protected int shards() { return 250; }

    @Override
    protected int replicas() { return 1; }

    @Override
    protected int refreshIntervalSecs() { return 60; }
}
