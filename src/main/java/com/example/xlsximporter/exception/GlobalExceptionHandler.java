package com.example.xlsximporter.exception;

import com.example.xlsximporter.dto.ErrorResponse;
import jakarta.servlet.http.HttpServletRequest;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.multipart.MaxUploadSizeExceededException;

import java.time.LocalDateTime;
import java.util.List;

@Slf4j
@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(ValidationException.class)
    public ResponseEntity<ErrorResponse> handleValidation(
            ValidationException ex, HttpServletRequest req) {
        log.warn("Validation error on {}: {}", req.getRequestURI(), ex.getErrors());
        return ResponseEntity.badRequest().body(ErrorResponse.builder()
                .status(400)
                .error("VALIDATION_ERROR")
                .message("Request validation failed")
                .details(ex.getErrors())
                .path(req.getRequestURI())
                .timestamp(LocalDateTime.now())
                .build());
    }

    @ExceptionHandler(ImportException.class)
    public ResponseEntity<ErrorResponse> handleImport(
            ImportException ex, HttpServletRequest req) {
        log.error("Import error on {}: {}", req.getRequestURI(), ex.getMessage(), ex);
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(ErrorResponse.builder()
                .status(500)
                .error("IMPORT_ERROR")
                .message(ex.getMessage())
                .path(req.getRequestURI())
                .timestamp(LocalDateTime.now())
                .build());
    }

    @ExceptionHandler(MaxUploadSizeExceededException.class)
    public ResponseEntity<ErrorResponse> handleMaxSize(
            MaxUploadSizeExceededException ex, HttpServletRequest req) {
        return ResponseEntity.status(HttpStatus.PAYLOAD_TOO_LARGE).body(ErrorResponse.builder()
                .status(413)
                .error("FILE_TOO_LARGE")
                .message("Uploaded file exceeds the maximum allowed size (50MB)")
                .path(req.getRequestURI())
                .timestamp(LocalDateTime.now())
                .build());
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<ErrorResponse> handleIllegalArg(
            IllegalArgumentException ex, HttpServletRequest req) {
        return ResponseEntity.badRequest().body(ErrorResponse.builder()
                .status(400)
                .error("BAD_REQUEST")
                .message(ex.getMessage())
                .details(List.of(ex.getMessage()))
                .path(req.getRequestURI())
                .timestamp(LocalDateTime.now())
                .build());
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ErrorResponse> handleGeneric(
            Exception ex, HttpServletRequest req) {
        log.error("Unexpected error on {}: {}", req.getRequestURI(), ex.getMessage(), ex);
        return ResponseEntity.internalServerError().body(ErrorResponse.builder()
                .status(500)
                .error("INTERNAL_ERROR")
                .message("An unexpected error occurred: " + ex.getMessage())
                .path(req.getRequestURI())
                .timestamp(LocalDateTime.now())
                .build());
    }
}
