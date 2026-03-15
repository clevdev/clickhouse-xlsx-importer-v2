package com.example.xlsximporter.controller;

import com.example.xlsximporter.dto.ErrorResponse;
import com.example.xlsximporter.dto.ImportResponse;
import com.example.xlsximporter.model.ImportLog;
import com.example.xlsximporter.repository.ImportLogRepository;
import com.example.xlsximporter.service.ImportService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.util.List;

@Slf4j
@RestController
@RequestMapping("/api/v1/import")
@RequiredArgsConstructor
@Tag(name = "XLSX Import", description = "Upload xlsx files to create ClickHouse tables and import data")
public class ImportController {

    private final ImportService importService;
    private final ImportLogRepository importLogRepository;

    /**
     * POST /api/v1/import/xlsx
     *
     * Accepts:
     *   - tableName (query param): target ClickHouse table name
     *   - file (multipart): xlsx file where row 1 = column names, row 2 = types, rows 3+ = data
     */
    @PostMapping(value = "/xlsx", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    @Operation(
            summary = "Import xlsx file into ClickHouse",
            description = """
                    Upload an xlsx file and specify a table name.

                    **xlsx format:**
                    - Row 1: column names
                    - Row 2: ClickHouse types (e.g. `String`, `Int64`, `Date`, `Nullable(Float64)`)
                    - Row 3+: data rows

                    **What the API does:**
                    1. Parses the xlsx file
                    2. Validates column names (reserved keyword check), types, and data values
                    3. Creates the ClickHouse table (`ReplicatedMergeTree`) if it doesn't exist
                    4. Inserts data in batches of 1000 rows via `batchUpdate`
                    5. Logs the operation in PostgreSQL
                    6. Returns the result including the generated DDL script

                    **Auto-generated columns:**
                    - `<col>_str String` — for every `Date` or `DateTime` column
                    - `operation_dttm DateTime` — timestamp of the import
                    """
    )
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Import successful",
                    content = @Content(schema = @Schema(implementation = ImportResponse.class))),
            @ApiResponse(responseCode = "400", description = "Validation error",
                    content = @Content(schema = @Schema(implementation = ErrorResponse.class))),
            @ApiResponse(responseCode = "500", description = "ClickHouse or PostgreSQL error",
                    content = @Content(schema = @Schema(implementation = ErrorResponse.class)))
    })
    public ResponseEntity<ImportResponse> importXlsx(
            @Parameter(description = "Target ClickHouse table name", required = true, example = "sales_2024")
            @RequestParam("tableName") String tableName,

            @Parameter(description = "xlsx file (row1=names, row2=types, rows3+=data)", required = true)
            @RequestPart("file") MultipartFile file) {

        log.info("Received import request: table='{}', file='{}', size={}",
                tableName, file.getOriginalFilename(), file.getSize());

        ImportResponse response = importService.importFile(file, tableName);
        return ResponseEntity.ok(response);
    }

    /**
     * GET /api/v1/import/logs
     * Returns all import logs from PostgreSQL.
     */
    @GetMapping("/logs")
    @Operation(summary = "Get all import logs", description = "Returns all import operation records stored in PostgreSQL")
    @ApiResponse(responseCode = "200", description = "List of import logs")
    public ResponseEntity<List<ImportLog>> getLogs() {
        return ResponseEntity.ok(importLogRepository.findAll());
    }

    /**
     * GET /api/v1/import/logs/{tableName}
     * Returns import logs for a specific table.
     */
    @GetMapping("/logs/{tableName}")
    @Operation(summary = "Get import logs for a specific table")
    @ApiResponse(responseCode = "200", description = "Import logs for the given table")
    public ResponseEntity<List<ImportLog>> getLogsByTable(
            @PathVariable("tableName") String tableName) {
        return ResponseEntity.ok(
                importLogRepository.findByTableNameOrderByOperationDttmDesc(tableName));
    }

    /**
     * GET /api/v1/import/health
     */
    @GetMapping("/health")
    @Operation(summary = "Health check")
    public ResponseEntity<String> health() {
        return ResponseEntity.ok("{\"status\":\"UP\",\"service\":\"xlsx-importer\"}");
    }
}
