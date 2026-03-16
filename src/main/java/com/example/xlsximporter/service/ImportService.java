package com.example.xlsximporter.service;

import com.example.xlsximporter.dto.ImportResponse;
import com.example.xlsximporter.dto.SheetMeta;
import com.example.xlsximporter.exception.ImportException;
import com.example.xlsximporter.model.ImportLog;
import com.example.xlsximporter.repository.ImportLogRepository;
import com.example.xlsximporter.validation.XlsxValidator;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Import pipeline — STREAMING variant (v2-opt).
 *
 * <h3>Byte-array conversion</h3>
 * The very first thing {@link #importFile} does is call {@code file.getBytes()} and
 * convert the upload into a plain {@code byte[]}.  Every subsequent operation works
 * with that array — the {@link MultipartFile} is never touched again.
 *
 * <p>Reasons:
 * <ul>
 *   <li>The pipeline reads the file <em>twice</em> (pass 1 = meta, pass 2 = data rows).
 *       {@code MultipartFile.getInputStream()} is not guaranteed to be re-readable
 *       (some servlet containers back it with a single-use stream).</li>
 *   <li>A {@code byte[]} can be wrapped in a {@code ByteArrayInputStream} any number
 *       of times with zero I/O cost.</li>
 *   <li>The array is the single source of truth for the entire import transaction —
 *       no race conditions if the underlying temp file is cleaned up mid-request.</li>
 * </ul>
 *
 * <h3>Memory note</h3>
 * The {@code byte[]} itself holds the raw (compressed) xlsx bytes, which is roughly
 * equal to the file size (100 MB file → ~100 MB array).  The SAX parser still reads
 * from that array one row at a time, so the <em>additional</em> working memory remains
 * O(batchSize) ≈ 1–2 MB.
 */
@Slf4j
@Service
public class ImportService {

    private final XlsxParserService       parserService;
    private final XlsxValidator           validator;
    private final ClickHouseScriptBuilder scriptBuilder;
    private final ImportLogRepository     importLogRepository;
    private final JdbcTemplate            jdbcNode1;
    private final JdbcTemplate            jdbcNode2;

    @Value("${import.batch-size:1000}")
    private int batchSize;

    public ImportService(
            XlsxParserService parserService,
            XlsxValidator validator,
            ClickHouseScriptBuilder scriptBuilder,
            ImportLogRepository importLogRepository,
            @Qualifier("jdbcTemplateNode1") JdbcTemplate jdbcNode1,
            @Qualifier("jdbcTemplateNode2") JdbcTemplate jdbcNode2) {
        this.parserService       = parserService;
        this.validator           = validator;
        this.scriptBuilder       = scriptBuilder;
        this.importLogRepository = importLogRepository;
        this.jdbcNode1           = jdbcNode1;
        this.jdbcNode2           = jdbcNode2;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Public API
    // ─────────────────────────────────────────────────────────────────────────

    public ImportResponse importFile(MultipartFile file, String tableName) {

        // ── Convert MultipartFile → byte[] ONCE ──────────────────────────────
        // From this point on, 'file' is never used again.
        // All parser calls receive 'fileBytes' and wrap it in ByteArrayInputStream.
        final String originalFilename = file.getOriginalFilename();

        // Validate extension before reading bytes — fail fast without loading the file
        if (originalFilename == null
                || (!originalFilename.endsWith(".xlsx") && !originalFilename.endsWith(".xlsm"))) {
            throw new ImportException("Only .xlsx/.xlsm files accepted, got: " + originalFilename, null);
        }

        final byte[] fileBytes;
        try {
            fileBytes = file.getBytes();
            log.debug("Read {} bytes from upload '{}'", fileBytes.length, originalFilename);
        } catch (Exception e) {
            throw new ImportException("Cannot read uploaded file: " + e.getMessage(), e);
        }

        // ── Pass 1: read header only (O(columns) memory) ─────────────────────
        SheetMeta meta = parserService.readMeta(fileBytes);

        validator.validateMeta(meta, tableName);

        String        createScript  = scriptBuilder.buildCreateScript(tableName, meta);
        List<String>  insertColumns = scriptBuilder.buildInsertColumns(meta);
        String        insertSql     = scriptBuilder.buildInsertSql(tableName, insertColumns);
        LocalDateTime operationDttm = LocalDateTime.now();

        log.info("Import starting: table='{}', columns={}", tableName, meta.columnNames().size());

        executeDdl(tableName, createScript);

        // ── Pass 2: stream data rows → batch flush ────────────────────────────
        List<Object[]> batch     = new ArrayList<>(batchSize);
        AtomicInteger  totalRows = new AtomicInteger(0);

        parserService.streamRows(fileBytes, meta.columnNames().size(), rawRow -> {
            validator.validateRow(rawRow, meta);
            List<Object> values = scriptBuilder.buildRowValues(rawRow, meta, operationDttm);
            batch.add(values.toArray());

            if (batch.size() >= batchSize) {
                flushBatch(insertSql, batch, tableName);
                totalRows.addAndGet(batch.size());
                batch.clear();
            }
        });

        if (!batch.isEmpty()) {
            flushBatch(insertSql, batch, tableName);
            totalRows.addAndGet(batch.size());
        }

        if (totalRows.get() == 0) {
            throw new com.example.xlsximporter.exception.ValidationException(
                "File contains no data rows. " +
                "Row 1 must be column names, row 2 must be types, row 3+ must be data.");
        }

        log.info("Import complete: table='{}', total_rows={}", tableName, totalRows.get());

        ImportLog saved = importLogRepository.save(ImportLog.builder()
                .tableName(tableName)
                .operationDttm(operationDttm)
                .rowsInserted(totalRows.get())
                .processedByNode("Node1+Node2")
                .sourceFilename(originalFilename)
                .build());

        return ImportResponse.builder()
                .id(saved.getId())
                .tableName(tableName)
                .rowsInserted(totalRows.get())
                .columnCount(meta.columnNames().size())
                .operationDttm(operationDttm)
                .processedByNode("Node1+Node2")
                .createScript(createScript)
                .build();
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Private helpers
    // ─────────────────────────────────────────────────────────────────────────

    private void executeDdl(String tableName, String createScript) {
        try {
            jdbcNode1.execute(createScript);
            log.debug("DDL OK on Node1 for '{}'", tableName);
        } catch (Exception e) {
            throw new ImportException("DDL failed on Node1: " + e.getMessage(), e);
        }
        try {
            jdbcNode2.execute(createScript);
            log.debug("DDL OK on Node2 for '{}'", tableName);
        } catch (Exception e) {
            log.warn("DDL failed on Node2 (will sync via replication): {}", e.getMessage());
        }
    }

    private void flushBatch(String insertSql, List<Object[]> batch, String tableName) {
        try {
            // DEBUG: log SQL and first row values before executing
            // Helps diagnose "bad SQL grammar" / type mismatch errors
            if (log.isDebugEnabled()) {
                log.debug("[Node1] INSERT SQL: {}", insertSql);
                if (!batch.isEmpty()) {
                    Object[] firstRow = batch.get(0);
                    StringBuilder sb = new StringBuilder("[Node1] First row values (")
                            .append(firstRow.length).append(" cols):");
                    for (int i = 0; i < firstRow.length; i++) {
                        Object v = firstRow[i];
                        sb.append("\n  [").append(i).append("] ")
                          .append(v == null ? "NULL" : v.getClass().getSimpleName())
                          .append(" = ")
                          .append(v);
                    }
                    log.debug(sb.toString());
                }
            }
            jdbcNode1.batchUpdate(insertSql, batch);
            log.debug("Flushed {} rows to Node1 '{}'", batch.size(), tableName);
        } catch (Exception e) {
            // Log full diagnostics at ERROR level so they appear regardless of log level
            log.error("[Node1] batchUpdate failed on table '{}': {}", tableName, e.getMessage());
            log.error("[Node1] INSERT SQL: {}", insertSql);
            if (!batch.isEmpty()) {
                Object[] firstRow = batch.get(0);
                StringBuilder sb = new StringBuilder("[Node1] First row values (")
                        .append(firstRow.length).append(" cols):");
                for (int i = 0; i < firstRow.length; i++) {
                    Object v = firstRow[i];
                    sb.append("\n  [").append(i).append("] ")
                      .append(v == null ? "NULL" : v.getClass().getSimpleName())
                      .append(" = ")
                      .append(v);
                }
                log.error(sb.toString());
            }
            throw new ImportException(
                "Batch insert failed on Node1 (import aborted): " + e.getMessage(), e);
        }
        try {
            jdbcNode2.batchUpdate(insertSql, batch);
        } catch (Exception e) {
            log.warn("Batch insert failed on Node2 (will sync): {}", e.getMessage());
        }
    }
}
