package com.example.xlsximporter.dto;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Data;

import java.time.LocalDateTime;

@Data
@Builder
@Schema(description = "Result of a successful xlsx import operation")
public class ImportResponse {

    @Schema(description = "Import log record ID in PostgreSQL", example = "1")
    private Long id;

    @Schema(description = "Name of the table created in ClickHouse", example = "sales_data")
    private String tableName;

    @Schema(description = "Number of rows inserted", example = "250")
    private int rowsInserted;

    @Schema(description = "Number of columns in the table", example = "6")
    private int columnCount;

    @Schema(description = "Timestamp of the import operation")
    private LocalDateTime operationDttm;

    @Schema(description = "ClickHouse node used", example = "Node1")
    private String processedByNode;

    @Schema(description = "DDL script executed to create the table")
    private String createScript;
}
