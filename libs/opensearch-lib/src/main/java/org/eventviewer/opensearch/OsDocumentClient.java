package org.eventviewer.opensearch;

import java.util.Collection;

public interface OsDocumentClient {

    <T> void save(Collection<T> items) throws OsException;

    <T> T get(String id, Class<T> clazz) throws OsException;

    <T> SearchResult<T> search(Search search, Class<T> entityClass) throws OsException;

    <T> void deleteByQuery(Search search, Class<T> entityClass) throws OsException;
}
