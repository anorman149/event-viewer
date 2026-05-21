package org.eventviewer.opensearch;

import java.lang.reflect.Field;

public class OsIndexMetadata {
    private Class<?> documentClass;
    private String indexPattern;
    private String templateName;
    private String templatePattern;
    private String writeAlias;
    private String readAlias;
    private Field idField;

    public Class<?> getDocumentClass() {
        return documentClass;
    }

    public void setDocumentClass(Class<?> documentClass) {
        this.documentClass = documentClass;
    }

    public String getIndexPattern() {
        return indexPattern;
    }

    public void setIndexPattern(String indexPattern) {
        this.indexPattern = indexPattern;
    }

    public String getTemplateName() {
        return templateName;
    }

    public void setTemplateName(String templateName) {
        this.templateName = templateName;
    }

    public String getTemplatePattern() {
        return templatePattern;
    }

    public void setTemplatePattern(String templatePattern) {
        this.templatePattern = templatePattern;
    }

    public String getWriteAlias() {
        return writeAlias;
    }

    public void setWriteAlias(String writeAlias) {
        this.writeAlias = writeAlias;
    }

    public String getReadAlias() {
        return readAlias;
    }

    public void setReadAlias(String readAlias) {
        this.readAlias = readAlias;
    }

    public Field getIdField() {
        return idField;
    }

    public void setIdField(Field idField) {
        this.idField = idField;
    }
}
