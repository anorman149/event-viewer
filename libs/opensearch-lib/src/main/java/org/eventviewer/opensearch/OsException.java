package org.eventviewer.opensearch;

public class OsException extends Exception {

    public OsException(String message, Throwable cause) {
        super(message, cause);
    }

    public OsException(String message) {
        super(message);
    }
}
