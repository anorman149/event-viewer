package org.eventviewer.opensearch;

import java.util.Collection;

public interface OsDocumentClient {

    <T> void save(Collection<T> items) throws OsException;

    <T> T get(String id, Class<T> clazz) throws OsException;

    <T> OsSearchResponse<T> search(Class<T> docClass, OsSearchRequest request) throws OsException;

    <T> void deleteByQuery(Search search, Class<T> entityClass) throws OsException;
}
