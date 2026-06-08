package org.eventviewer.opensearch;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class FieldNameMapperTest {

    @OsIndex(
            indexPattern = "<test-{now/d}>",
            template = @Template(name = "test-template", pattern = "test-*"),
            alias = @Alias(write = "test_write", read = "test_read")
    )
    static class SampleDocument {
        @Id String id;
        String name;
        @FieldName("my_os_field") String javaField;
    }

    private FieldNameMapper mapper;

    @BeforeEach
    void setUp() {
        mapper = new FieldNameMapper();
    }

    @Test
    void getMappings_annotatedField_usesAnnotationValue() {
        Map<String, String> mappings = mapper.getMappings(SampleDocument.class);

        assertThat(mappings).containsEntry("my_os_field", "javaField");
        assertThat(mappings).doesNotContainKey("javaField");
    }

    @Test
    void getMappings_unannotatedField_usesJavaFieldName() {
        Map<String, String> mappings = mapper.getMappings(SampleDocument.class);

        assertThat(mappings).containsEntry("name", "name");
    }

    @Test
    void getMappings_allFieldsMapped() {
        Map<String, String> mappings = mapper.getMappings(SampleDocument.class);

        assertThat(mappings).containsOnlyKeys("id", "name", "my_os_field");
    }

    @Test
    void getMappings_cacheReturnsSameInstance() {
        Map<String, String> first = mapper.getMappings(SampleDocument.class);
        Map<String, String> second = mapper.getMappings(SampleDocument.class);

        assertThat(first).isSameAs(second);
    }
}
