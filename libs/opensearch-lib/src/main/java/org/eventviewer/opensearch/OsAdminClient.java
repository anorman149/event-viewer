package org.eventviewer.opensearch;

public interface OsAdminClient {

    <T> void refresh(Class<T> clazz) throws OsException;

    <T> boolean indexExists(Class<T> clazz) throws OsException;

    void createIndex(IndexSettings settings) throws OsException;

    void createTemplate(IndexSettings settings) throws OsException;

    <T> boolean templateExists(Class<T> clazz) throws OsException;

    void clusterSettings(ClusterSettings settings) throws OsException;
}
