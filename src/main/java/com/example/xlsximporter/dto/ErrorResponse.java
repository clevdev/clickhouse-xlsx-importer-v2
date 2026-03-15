package com.example.xlsximporter.dto;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Data;

import java.time.LocalDateTime;
import java.util.List;

@Data
@Builder
@Schema(description = "Error response body")
public class ErrorResponse {

    @Schema(description = "HTTP status code", example = "400")
    private int status;

    @Schema(description = "Short error type", example = "VALIDATION_ERROR")
    private String error;

    @Schema(description = "General error message")
    private String message;

    @Schema(description = "List of specific validation errors")
    private List<String> details;

    @Schema(description = "Request path")
    private String path;

    @Schema(description = "Timestamp of the error")
    private LocalDateTime timestamp;
}
