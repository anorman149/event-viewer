package org.eventviewer.opensearch;

import org.opensearch.client.opensearch._types.mapping.TypeMapping;

public class IndexSettings {

    private TypeMapping typeMapping;
    private Class<?> entity;
    private int shards = 1;
    private int replicas = 0;
    private int refreshIntervalSecs = 60;
    private String codec = "default";

    public TypeMapping getTypeMapping() { return typeMapping; }
    public void setTypeMapping(TypeMapping typeMapping) { this.typeMapping = typeMapping; }

    public Class<?> getEntity() { return entity; }
    public void setEntity(Class<?> entity) { this.entity = entity; }

    public int getShards() { return shards; }
    public void setShards(int shards) { this.shards = shards; }

    public int getReplicas() { return replicas; }
    public void setReplicas(int replicas) { this.replicas = replicas; }

    public int getRefreshIntervalSecs() { return refreshIntervalSecs; }
    public void setRefreshIntervalSecs(int refreshIntervalSecs) { this.refreshIntervalSecs = refreshIntervalSecs; }

    public String getCodec() { return codec; }
    public void setCodec(String codec) { this.codec = codec; }
}
