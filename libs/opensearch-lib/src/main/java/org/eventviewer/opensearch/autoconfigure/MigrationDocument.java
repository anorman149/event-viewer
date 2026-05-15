package org.eventviewer.opensearch.autoconfigure;

public class MigrationDocument {

    private int version;
    private String name;
    private String appliedAt;

    public MigrationDocument() {}

    public MigrationDocument(int version, String name, String appliedAt) {
        this.version = version;
        this.name = name;
        this.appliedAt = appliedAt;
    }

    public int getVersion() { return version; }
    public void setVersion(int version) { this.version = version; }

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public String getAppliedAt() { return appliedAt; }
    public void setAppliedAt(String appliedAt) { this.appliedAt = appliedAt; }
}
