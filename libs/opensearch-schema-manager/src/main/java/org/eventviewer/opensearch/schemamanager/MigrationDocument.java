package org.eventviewer.opensearch.schemamanager;

import org.eventviewer.opensearch.Alias;
import org.eventviewer.opensearch.Id;
import org.eventviewer.opensearch.OsIndex;
import org.eventviewer.opensearch.Template;

import java.time.ZonedDateTime;

@OsIndex(
        indexPattern = "migrations",
        template = @Template(name = "migrations-template", pattern = "migrations-*"),
        alias = @Alias(write = "migrations_write", read = "migrations_read")
)
public class MigrationDocument {
    @Id
    private String version;

    private ZonedDateTime timestamp;

    public MigrationDocument() {}
    public MigrationDocument(String version, ZonedDateTime timestamp) {
        this.version = version;
        this.timestamp = timestamp;
    }

    public String getVersion() { return version; }
    public void setVersion(String version) { this.version = version; }

    public ZonedDateTime getTimestamp() { return timestamp; }
    public void setTimestamp(ZonedDateTime timestamp) { this.timestamp = timestamp; }
}
