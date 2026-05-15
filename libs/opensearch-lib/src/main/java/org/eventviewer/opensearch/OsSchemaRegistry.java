package org.eventviewer.opensearch;

import io.github.classgraph.ClassGraph;
import io.github.classgraph.ClassInfo;
import io.github.classgraph.ScanResult;
import io.micrometer.core.annotation.Timed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class OsSchemaRegistry {

    private static final Logger log = LoggerFactory.getLogger(OsSchemaRegistry.class);

    private final Map<Class<?>, OsIndexMetadata> registry = new ConcurrentHashMap<>();

    public OsSchemaRegistry() {
        try (ScanResult result = new ClassGraph()
                .enableAnnotationInfo()
                .enableClassInfo()
                .scan()) {
            for (ClassInfo info : result.getClassesWithAnnotation(OsIndex.class.getName())) {
                Class<?> clazz = info.loadClass();
                OsIndex annotation = clazz.getAnnotation(OsIndex.class);
                if (annotation == null) continue;
                OsIndexMetadata metadata = new OsIndexMetadata(
                        clazz,
                        annotation.indexPattern(),
                        annotation.templateName(),
                        annotation.alias().write(),
                        annotation.alias().read()
                );
                registry.put(clazz, metadata);
                log.debug("Registered @OsIndex class: {}", clazz.getName());
            }
        }
        log.debug("OsSchemaRegistry initialized with {} entries", registry.size());
    }

    @Timed(value = "os.schema.registry.get.metadata", histogram = true)
    public OsIndexMetadata getMetadata(Class<?> clazz) {
        OsIndexMetadata metadata = registry.get(clazz);
        if (metadata == null) {
            throw new IllegalArgumentException(
                    "Class " + clazz.getName() + " is not annotated with @OsIndex or was not found during classpath scan. " +
                    "Ensure the class is annotated and on the classpath at startup.");
        }
        return metadata;
    }
}
