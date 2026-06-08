package org.eventviewer.opensearch;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class FieldNameMapper {

    private final ConcurrentHashMap<Class<?>, Map<String, String>> cache = new ConcurrentHashMap<>();

    public Map<String, String> getMappings(Class<?> docClass) {
        return cache.computeIfAbsent(docClass, this::buildMappings);
    }

    private Map<String, String> buildMappings(Class<?> docClass) {
        Map<String, String> map = new LinkedHashMap<>();
        for (Field field : docClass.getDeclaredFields()) {
            FieldName annotation = field.getAnnotation(FieldName.class);
            String osName = (annotation != null) ? annotation.value() : field.getName();
            map.put(osName, field.getName());
        }
        return Collections.unmodifiableMap(map);
    }
}
