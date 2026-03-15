package com.example.xlsximporter.exception;

import java.util.List;

/**
 * Thrown when xlsx content fails validation.
 * Results in HTTP 400 from the global exception handler.
 */
public class ValidationException extends RuntimeException {

    private final List<String> errors;

    public ValidationException(List<String> errors) {
        super("Validation failed with " + errors.size() + " error(s)");
        this.errors = List.copyOf(errors);
    }

    public ValidationException(String message) {
        super(message);
        this.errors = List.of(message);
    }

    public List<String> getErrors() {
        return errors;
    }
}
