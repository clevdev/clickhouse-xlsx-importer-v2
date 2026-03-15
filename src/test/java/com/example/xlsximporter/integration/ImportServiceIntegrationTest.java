package com.example.xlsximporter.integration;

import com.example.xlsximporter.dto.ImportResponse;
import com.example.xlsximporter.exception.ValidationException;
import com.example.xlsximporter.model.ImportLog;
import com.example.xlsximporter.repository.ImportLogRepository;
import com.example.xlsximporter.service.ImportService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.multipart.MultipartFile;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.*;

/**
 * Integration tests for {@link ImportService}.
 *
 * Uses Testcontainers: real PostgreSQL + two real ClickHouse instances.
 * Each test gets a unique table name to avoid conflicts.
 */
@SpringBootTest
class ImportServiceIntegrationTest extends AbstractIntegrationTest {

    @Autowired private ImportService importService;
    @Autowired private ImportLogRepository importLogRepository;

    @Autowired
    @Qualifier("jdbcTemplateNode1")
    private JdbcTemplate jdbcNode1;

    @Autowired
    @Qualifier("jdbcTemplateNode2")
    private JdbcTemplate jdbcNode2;

    private long testSequence;

    @BeforeEach
    void setUp() {
        testSequence = System.currentTimeMillis();
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Happy-path tests
    // ─────────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("Import creates table and inserts rows on both ClickHouse nodes")
    void shouldImportDataToBothNodes() throws Exception {
        String table = "it_basic_" + testSequence;

        MultipartFile file = XlsxTestHelper.buildXlsx(
                List.of("id", "name", "amount"),
                List.of("Int64", "String", "Float64"),
                List.of(
                        List.of(1, "Alice", 100.5),
                        List.of(2, "Bob",   200.75),
                        List.of(3, "Carol", 300.0)
                )
        );

        ImportResponse response = importService.importFile(file, table);

        assertThat(response.getRowsInserted()).isEqualTo(3);
        assertThat(response.getTableName()).isEqualTo(table);
        assertThat(response.getCreateScript()).contains("ReplicatedMergeTree");
        assertThat(response.getProcessedByNode()).contains("Node1");

        // Verify rows exist on Node 1
        Integer countNode1 = jdbcNode1.queryForObject("SELECT count() FROM " + table, Integer.class);
        assertThat(countNode1).isEqualTo(3);

        // Verify rows exist on Node 2
        Integer countNode2 = jdbcNode2.queryForObject("SELECT count() FROM " + table, Integer.class);
        assertThat(countNode2).isEqualTo(3);
    }

    @Test
    @DisplayName("Import log is persisted to PostgreSQL")
    void shouldSaveImportLogToPostgres() throws Exception {
        String table = "it_log_" + testSequence;

        MultipartFile file = XlsxTestHelper.buildXlsx(
                List.of("code", "label"),
                List.of("Int32", "String"),
                List.of(List.of(1, "one"), List.of(2, "two"))
        );

        ImportResponse response = importService.importFile(file, table);

        List<ImportLog> logs = importLogRepository.findByTableNameOrderByOperationDttmDesc(table);
        assertThat(logs).hasSize(1);
        ImportLog log = logs.get(0);
        assertThat(log.getId()).isEqualTo(response.getId());
        assertThat(log.getTableName()).isEqualTo(table);
        assertThat(log.getRowsInserted()).isEqualTo(2);
        assertThat(log.getOperationDttm()).isNotNull();
        assertThat(log.getSourceFilename()).isEqualTo("test.xlsx");
    }

    @Test
    @DisplayName("Date column gets _str String companion; Nullable(Date) gets Nullable(String) companion")
    void shouldCreateStrCompanionWithCorrectNullability() throws Exception {
        String table = "it_dates_" + testSequence;

        MultipartFile file = XlsxTestHelper.buildXlsx(
                List.of("event_date", "optional_date"),
                List.of("Date", "Nullable(Date)"),
                List.of(
                        List.of("2024-01-15", "2024-06-01"),
                        List.of("2024-02-20", "")    // null optional
                )
        );

        ImportResponse response = importService.importFile(file, table);
        assertThat(response.getRowsInserted()).isEqualTo(2);

        // Check DDL contains both companion types
        String ddl = response.getCreateScript();
        assertThat(ddl).contains("event_date_str String");
        assertThat(ddl).contains("optional_date_str Nullable(String)");

        // Verify data: event_date_str should have original raw string
        List<Map<String, Object>> rows = jdbcNode1.queryForList(
                "SELECT event_date_str, optional_date_str FROM " + table + " ORDER BY event_date");
        assertThat(rows).hasSize(2);
        assertThat(rows.get(0).get("event_date_str")).isEqualTo("2024-01-15");
        assertThat(rows.get(1).get("optional_date_str")).isNull();
    }

    @Test
    @DisplayName("Batch insert works correctly for > 1000 rows")
    void shouldInsertMoreThanOneBatch() throws Exception {
        String table = "it_batch_" + testSequence;

        List<List<Object>> rows = new java.util.ArrayList<>();
        for (int i = 1; i <= 2500; i++) {
            rows.add(List.of(i, "item_" + i, i * 1.5));
        }

        MultipartFile file = XlsxTestHelper.buildXlsx(
                List.of("id", "name", "value"),
                List.of("Int64", "String", "Float64"),
                rows
        );

        ImportResponse response = importService.importFile(file, table);
        assertThat(response.getRowsInserted()).isEqualTo(2500);

        Integer count = jdbcNode1.queryForObject("SELECT count() FROM " + table, Integer.class);
        assertThat(count).isEqualTo(2500);
    }

    @Test
    @DisplayName("Nullable fields accept null values without errors")
    void shouldHandleNullableFields() throws Exception {
        String table = "it_nullable_" + testSequence;

        MultipartFile file = XlsxTestHelper.buildXlsx(
                List.of("id", "score", "note"),
                List.of("Int64", "Nullable(Float64)", "Nullable(String)"),
                List.of(
                        List.of(1, 9.5, "good"),
                        List.of(2, "",  ""),     // both nullable → null
                        List.of(3, 7.0, "ok")
                )
        );

        ImportResponse response = importService.importFile(file, table);
        assertThat(response.getRowsInserted()).isEqualTo(3);

        List<Map<String, Object>> data = jdbcNode1.queryForList(
                "SELECT id, score, note FROM " + table + " ORDER BY id");
        assertThat(data.get(1).get("score")).isNull();
        assertThat(data.get(1).get("note")).isNull();
    }

    @Test
    @DisplayName("operation_dttm is populated on every row")
    void shouldPopulateOperationDttm() throws Exception {
        String table = "it_dttm_" + testSequence;

        MultipartFile file = XlsxTestHelper.buildXlsx(
                List.of("val"),
                List.of("String"),
                List.of(List.of("hello"))
        );

        importService.importFile(file, table);

        List<Map<String, Object>> rows = jdbcNode1.queryForList(
                "SELECT operation_dttm FROM " + table);
        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).get("operation_dttm")).isNotNull();
    }

    @Test
    @DisplayName("Idempotent: importing same table twice does not fail (IF NOT EXISTS)")
    void shouldBeIdempotentOnTableCreation() throws Exception {
        String table = "it_idem_" + testSequence;

        MultipartFile file1 = XlsxTestHelper.buildXlsx(
                List.of("id", "name"),
                List.of("Int64", "String"),
                List.of(List.of(1, "first"))
        );
        MultipartFile file2 = XlsxTestHelper.buildXlsx(
                List.of("id", "name"),
                List.of("Int64", "String"),
                List.of(List.of(2, "second"))
        );

        importService.importFile(file1, table);
        importService.importFile(file2, table);

        Integer count = jdbcNode1.queryForObject("SELECT count() FROM " + table, Integer.class);
        assertThat(count).isEqualTo(2);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Validation failure tests
    // ─────────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("Reserved column name triggers ValidationException with HTTP 400 details")
    void shouldRejectReservedColumnName() throws Exception {
        MultipartFile file = XlsxTestHelper.buildXlsx(
                List.of("select"),
                List.of("String"),
                List.of(List.of("value"))
        );

        assertThatThrownBy(() -> importService.importFile(file, "valid_table"))
                .isInstanceOf(ValidationException.class)
                .satisfies(ex -> {
                    ValidationException ve = (ValidationException) ex;
                    assertThat(ve.getErrors()).anyMatch(e -> e.contains("reserved"));
                });
    }

    @Test
    @DisplayName("Invalid type in row 2 triggers ValidationException")
    void shouldRejectInvalidType() throws Exception {
        MultipartFile file = XlsxTestHelper.buildXlsx(
                List.of("col1"),
                List.of("VARCHAR"),   // not a valid ClickHouse type
                List.of(List.of("x"))
        );

        assertThatThrownBy(() -> importService.importFile(file, "t_" + testSequence))
                .isInstanceOf(ValidationException.class)
                .satisfies(ex ->
                        assertThat(((ValidationException) ex).getErrors())
                                .anyMatch(e -> e.contains("VARCHAR")));
    }

    @Test
    @DisplayName("Non-numeric value in Int64 column triggers ValidationException")
    void shouldRejectWrongDataType() throws Exception {
        MultipartFile file = XlsxTestHelper.buildXlsx(
                List.of("id"),
                List.of("Int64"),
                List.of(List.of("not-a-number"))
        );

        assertThatThrownBy(() -> importService.importFile(file, "t_" + testSequence))
                .isInstanceOf(ValidationException.class);
    }

    @Test
    @DisplayName("Null in non-Nullable column triggers ValidationException")
    void shouldRejectNullInNonNullableColumn() throws Exception {
        MultipartFile file = XlsxTestHelper.buildXlsx(
                List.of("required_field"),
                List.of("Int64"),
                List.of(List.of(""))   // empty = null
        );

        assertThatThrownBy(() -> importService.importFile(file, "t_" + testSequence))
                .isInstanceOf(ValidationException.class)
                .satisfies(ex ->
                        assertThat(((ValidationException) ex).getErrors())
                                .anyMatch(e -> e.contains("non-Nullable")));
    }

    @Test
    @DisplayName("Reserved table name triggers ValidationException")
    void shouldRejectReservedTableName() throws Exception {
        MultipartFile file = XlsxTestHelper.buildXlsx(
                List.of("col1"),
                List.of("String"),
                List.of(List.of("x"))
        );

        assertThatThrownBy(() -> importService.importFile(file, "select"))
                .isInstanceOf(ValidationException.class);
    }

    @Test
    @DisplayName("Various date formats are all accepted and normalised")
    void shouldAcceptMultipleDateFormats() throws Exception {
        String table = "it_datefmt_" + testSequence;

        MultipartFile file = XlsxTestHelper.buildXlsx(
                List.of("id", "event_date"),
                List.of("Int64", "Date"),
                List.of(
                        List.of(1, "2024-01-15"),     // yyyy-MM-dd
                        List.of(2, "01/20/2024"),     // MM/dd/yyyy
                        List.of(3, "25.03.2024"),     // dd.MM.yyyy
                        List.of(4, "10-04-2024")      // dd-MM-yyyy
                )
        );

        ImportResponse response = importService.importFile(file, table);
        assertThat(response.getRowsInserted()).isEqualTo(4);

        // All dates normalised to yyyy-MM-dd in ClickHouse
        List<Map<String, Object>> rows = jdbcNode1.queryForList(
                "SELECT toString(event_date) AS d FROM " + table + " ORDER BY id");
        assertThat(rows).extracting(r -> r.get("d"))
                .containsExactly("2024-01-15", "2024-01-20", "2024-03-25", "2024-04-10");
    }
}
