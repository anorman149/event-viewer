package org.eventviewer.opensearch;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class OsSchemaRegistryTest {

    static class UnannotatedDocument {}

    private OsSchemaRegistry registry;

    @BeforeEach
    void setUp() {
        registry = new OsSchemaRegistry();
    }

    @Test
    void getMetadata_annotatedClass_returnsCorrectMetadata() {
        OsIndexMetadata metadata = registry.getMetadata(TestDocument.class);

        assertThat(metadata.getDocumentClass()).isEqualTo(TestDocument.class);
        assertThat(metadata.getIndexPattern()).isEqualTo("<test-events-{now/d}-000001>");
        assertThat(metadata.getTemplateName()).isEqualTo("test-events-template");
        assertThat(metadata.getWriteAlias()).isEqualTo("test_events_write");
        assertThat(metadata.getReadAlias()).isEqualTo("test_events_read");
    }

    @Test
    void getMetadata_unannotatedClass_throwsIllegalArgumentException() {
        assertThatThrownBy(() -> registry.getMetadata(UnannotatedDocument.class))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(UnannotatedDocument.class.getName());
    }

    @Test
    void getMetadata_unannotatedClass_doesNotThrowNullPointerException() {
        assertThatThrownBy(() -> registry.getMetadata(UnannotatedDocument.class))
                .isNotInstanceOf(NullPointerException.class);
    }
}
