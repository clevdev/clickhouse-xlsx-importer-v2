package com.example.xlsximporter.exception;

/**
 * Thrown when ClickHouse or PostgreSQL operations fail during import.
 */
public class ImportException extends RuntimeException {

    public ImportException(String message) {
        super(message);
    }

    public ImportException(String message, Throwable cause) {
        super(message, cause);
    }
}
