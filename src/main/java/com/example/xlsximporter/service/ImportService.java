package com.example.xlsximporter.service;

import com.example.xlsximporter.dto.ImportResponse;
import com.example.xlsximporter.dto.SheetMeta;
import com.example.xlsximporter.exception.ImportException;
import com.example.xlsximporter.exception.ValidationException;
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
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Import pipeline — STREAMING variant (v2-opt).
 *
 * <h3>Pipeline flow</h3>
 * <pre>
 *   Pass 1: readMeta()    — reads only rows 0–1 (names + types) via SAX
 *                           validates DDL, runs CREATE TABLE on both nodes
 *   Pass 2: streamRows()  — rows 2…N delivered one-by-one via Consumer callback
 *                           accumulated into a batch of batchSize, then flushed
 *                           to ClickHouse; batch list is cleared and reused
 * </pre>
 *
 * <h3>Memory at any point in time</h3>
 * <pre>
 *   SheetMeta (names + types)  :  O(columns)  — tiny
 *   Current batch              :  O(batchSize) — typically 1 000 rows ≈ 1–2 MB
 *   Total regardless of file   :  ~5–15 MB
 * </pre>
 *
 * <h3>Trade-off vs v3</h3>
 * The file is read <em>twice</em> (once for meta, once for data).
 * For large files on slow storage this adds ~10–20 % time.
 * The gain is that zero rows are ever held in memory simultaneously,
 * which makes it viable even with heap as low as 256 MB.
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
        this.parserService      = parserService;
        this.validator          = validator;
        this.scriptBuilder      = scriptBuilder;
        this.importLogRepository = importLogRepository;
        this.jdbcNode1          = jdbcNode1;
        this.jdbcNode2          = jdbcNode2;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Public API
    // ─────────────────────────────────────────────────────────────────────────

    public ImportResponse importFile(MultipartFile file, String tableName) {

        // ── Pass 1: read header only (SAX, O(columns) memory) ─────────────────
        SheetMeta meta = parserService.readMeta(file);

        // Validate column names, types — throws ValidationException → HTTP 400
        validator.validateMeta(meta, tableName);

        // Prepare DDL and INSERT SQL once
        String       createScript  = scriptBuilder.buildCreateScript(tableName, meta);
        List<String> insertColumns = scriptBuilder.buildInsertColumns(meta);
        String       insertSql     = scriptBuilder.buildInsertSql(tableName, insertColumns);
        LocalDateTime operationDttm = LocalDateTime.now();

        log.info("Import starting: table='{}', columns={}", tableName, meta.columnNames().size());

        // Run CREATE TABLE on both nodes before streaming data
        executeDdl(tableName, createScript);

        // ── Pass 2: stream data rows → flush in batches ───────────────────────
        // Only ONE batch lives in memory at a time.
        // The batch list is reused (cleared after flush) — no extra allocation.
        List<Object[]>  batch     = new ArrayList<>(batchSize);
        AtomicInteger   totalRows = new AtomicInteger(0);

        parserService.streamRows(file, meta.columnNames().size(), rawRow -> {

            // Validate data row inline — throws ValidationException on bad cell
            validator.validateRow(rawRow, meta);

            List<Object> values = scriptBuilder.buildRowValues(rawRow, meta, operationDttm);
            batch.add(values.toArray());

            if (batch.size() >= batchSize) {
                flushBatch(insertSql, batch, tableName);
                totalRows.addAndGet(batch.size());
                batch.clear();          // reuse the list — no GC pressure
            }
        });

        // Flush remaining rows
        if (!batch.isEmpty()) {
            flushBatch(insertSql, batch, tableName);
            totalRows.addAndGet(batch.size());
        }

        log.info("Import complete: table='{}', total_rows={}", tableName, totalRows.get());

        // ── Save import log ───────────────────────────────────────────────────
        ImportLog saved = importLogRepository.save(ImportLog.builder()
                .tableName(tableName)
                .operationDttm(operationDttm)
                .rowsInserted(totalRows.get())
                .processedByNode("Node1+Node2")
                .sourceFilename(file.getOriginalFilename())
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

    /**
     * Sends a batch to Node1 (mandatory) and Node2 (best-effort).
     * The same batch list is passed to both — no copy is made.
     */
    private void flushBatch(String insertSql, List<Object[]> batch, String tableName) {
        try {
            jdbcNode1.batchUpdate(insertSql, batch);
            log.debug("Flushed {} rows to Node1 '{}'", batch.size(), tableName);
        } catch (Exception e) {
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
