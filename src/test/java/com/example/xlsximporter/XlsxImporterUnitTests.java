package com.example.xlsximporter;

import com.example.xlsximporter.dto.ParsedSheet;
import com.example.xlsximporter.exception.ValidationException;
import com.example.xlsximporter.service.ClickHouseScriptBuilder;
import com.example.xlsximporter.validation.ClickHouseTypeRegistry;
import com.example.xlsximporter.validation.DateParser;
import com.example.xlsximporter.validation.XlsxValidator;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.LocalDateTime;
import java.util.List;

import static org.assertj.core.api.Assertions.*;

/**
 * Unit tests — no Spring context, no containers.
 */
@ExtendWith(MockitoExtension.class)
class XlsxImporterUnitTests {

    @InjectMocks private XlsxValidator         validator;
    @InjectMocks private ClickHouseScriptBuilder scriptBuilder;

    // ── ClickHouseTypeRegistry ────────────────────────────────────────────────

    @Test @DisplayName("Valid bare ClickHouse types are accepted")
    void validBareTypes() {
        assertThat(ClickHouseTypeRegistry.isValidType("String")).isTrue();
        assertThat(ClickHouseTypeRegistry.isValidType("Int64")).isTrue();
        assertThat(ClickHouseTypeRegistry.isValidType("Float32")).isTrue();
        assertThat(ClickHouseTypeRegistry.isValidType("Date")).isTrue();
        assertThat(ClickHouseTypeRegistry.isValidType("DateTime")).isTrue();
        assertThat(ClickHouseTypeRegistry.isValidType("Bool")).isTrue();
        assertThat(ClickHouseTypeRegistry.isValidType("UUID")).isTrue();
        assertThat(ClickHouseTypeRegistry.isValidType("FixedString(10)")).isTrue();
        assertThat(ClickHouseTypeRegistry.isValidType("Decimal(18,4)")).isTrue();
    }

    @Test @DisplayName("Valid Nullable types are accepted")
    void validNullableTypes() {
        assertThat(ClickHouseTypeRegistry.isValidType("Nullable(String)")).isTrue();
        assertThat(ClickHouseTypeRegistry.isValidType("Nullable(Int64)")).isTrue();
        assertThat(ClickHouseTypeRegistry.isValidType("Nullable(Date)")).isTrue();
        assertThat(ClickHouseTypeRegistry.isValidType("Nullable(DateTime)")).isTrue();
        assertThat(ClickHouseTypeRegistry.isValidType("Nullable(Float64)")).isTrue();
    }

    @Test @DisplayName("Invalid types are rejected")
    void invalidTypes() {
        assertThat(ClickHouseTypeRegistry.isValidType("VARCHAR")).isFalse();
        assertThat(ClickHouseTypeRegistry.isValidType("INTEGER")).isFalse();
        assertThat(ClickHouseTypeRegistry.isValidType("")).isFalse();
        assertThat(ClickHouseTypeRegistry.isValidType(null)).isFalse();
        assertThat(ClickHouseTypeRegistry.isValidType("Nullable()")).isFalse();
    }

    @Test @DisplayName("isDateType distinguishes Date from DateTime")
    void isDateType() {
        assertThat(ClickHouseTypeRegistry.isDateType("Date")).isTrue();
        assertThat(ClickHouseTypeRegistry.isDateType("Date32")).isTrue();
        assertThat(ClickHouseTypeRegistry.isDateType("Nullable(Date)")).isTrue();
        assertThat(ClickHouseTypeRegistry.isDateType("DateTime")).isFalse();
        assertThat(ClickHouseTypeRegistry.isDateType("String")).isFalse();
    }

    @Test @DisplayName("isNullable correctly identifies Nullable wrapper")
    void isNullable() {
        assertThat(ClickHouseTypeRegistry.isNullable("Nullable(String)")).isTrue();
        assertThat(ClickHouseTypeRegistry.isNullable("Nullable(Date)")).isTrue();
        assertThat(ClickHouseTypeRegistry.isNullable("String")).isFalse();
        assertThat(ClickHouseTypeRegistry.isNullable("Date")).isFalse();
        assertThat(ClickHouseTypeRegistry.isNullable(null)).isFalse();
    }

    // ── DateParser ────────────────────────────────────────────────────────────

    @Test @DisplayName("All supported date formats parse correctly")
    void parseDateFormats() {
        assertThat(DateParser.parseDate("2004-05-25")).isPresent();
        assertThat(DateParser.parseDate("05/25/2004")).isPresent();
        assertThat(DateParser.parseDate("25.05.2004")).isPresent();
        assertThat(DateParser.parseDate("25-05-2004")).isPresent();
        assertThat(DateParser.parseDate("25/05/2004")).isPresent();
        assertThat(DateParser.parseDate("not-a-date")).isEmpty();
        assertThat(DateParser.parseDate(null)).isEmpty();
    }

    @Test @DisplayName("DateTime formats parse correctly")
    void parseDateTimeFormats() {
        assertThat(DateParser.parseDateTime("2024-03-07 14:30:00")).isPresent();
        assertThat(DateParser.parseDateTime("2024-03-07T14:30:00")).isPresent();
        assertThat(DateParser.parseDateTime("07.03.2024 14:30:00")).isPresent();
        assertThat(DateParser.parseDateTime("2024-03-07")).isPresent(); // date → midnight
    }

    @Test @DisplayName("toClickHouseDateString normalises to yyyy-MM-dd")
    void dateStringNormalisation() {
        assertThat(DateParser.toClickHouseDateString("05/25/2004")).isEqualTo("2004-05-25");
        assertThat(DateParser.toClickHouseDateString("25.05.2004")).isEqualTo("2004-05-25");
        assertThat(DateParser.toClickHouseDateString("25-05-2004")).isEqualTo("2004-05-25");
    }

    // ── XlsxValidator ─────────────────────────────────────────────────────────

    @Test @DisplayName("Valid sheet passes validation without errors")
    void validationPassesForGoodSheet() {
        ParsedSheet sheet = new ParsedSheet(
                List.of("id", "name", "event_date", "amount"),
                List.of("Int64", "String", "Date", "Nullable(Float64)"),
                List.of(
                        List.of("1", "Alice", "2024-01-15", "99.5"),
                        List.of("2", "Bob",   "01/20/2024",  null)
                )
        );
        assertThatNoException().isThrownBy(() -> validator.validate(sheet, "test_table"));
    }

    @Test @DisplayName("Reserved column name fails validation")
    void validationFailsForReservedColumnName() {
        ParsedSheet sheet = new ParsedSheet(
                List.of("select"),
                List.of("String"),
                List.of(List.of("value"))
        );
        assertThatThrownBy(() -> validator.validate(sheet, "my_table"))
                .isInstanceOf(ValidationException.class)
                .satisfies(ex ->
                        assertThat(((ValidationException) ex).getErrors())
                                .anyMatch(e -> e.contains("reserved")));
    }

    @Test @DisplayName("Invalid ClickHouse type fails validation")
    void validationFailsForBadType() {
        ParsedSheet sheet = new ParsedSheet(
                List.of("col1"), List.of("VARCHAR"), List.of(List.of("value")));
        assertThatThrownBy(() -> validator.validate(sheet, "my_table"))
                .isInstanceOf(ValidationException.class);
    }

    @Test @DisplayName("Null in non-Nullable column fails validation")
    void validationFailsForNullInNonNullableColumn() {
        ParsedSheet sheet = new ParsedSheet(
                List.of("amount"), List.of("Int64"), List.of(List.of((String) null)));
        assertThatThrownBy(() -> validator.validate(sheet, "my_table"))
                .isInstanceOf(ValidationException.class)
                .satisfies(ex ->
                        assertThat(((ValidationException) ex).getErrors())
                                .anyMatch(e -> e.contains("non-Nullable")));
    }

    @Test @DisplayName("Null in Nullable column passes validation")
    void validationAllowsNullInNullableColumn() {
        ParsedSheet sheet = new ParsedSheet(
                List.of("amount"), List.of("Nullable(Int64)"), List.of(List.of((String) null)));
        assertThatNoException().isThrownBy(() -> validator.validate(sheet, "my_table"));
    }

    @Test @DisplayName("Bad integer value fails validation")
    void validationFailsForBadInteger() {
        ParsedSheet sheet = new ParsedSheet(
                List.of("id"), List.of("Int64"), List.of(List.of("abc")));
        assertThatThrownBy(() -> validator.validate(sheet, "my_table"))
                .isInstanceOf(ValidationException.class);
    }

    @Test @DisplayName("Bad date value fails validation")
    void validationFailsForBadDate() {
        ParsedSheet sheet = new ParsedSheet(
                List.of("d"), List.of("Date"), List.of(List.of("not-a-date")));
        assertThatThrownBy(() -> validator.validate(sheet, "my_table"))
                .isInstanceOf(ValidationException.class);
    }

    // ── ClickHouseScriptBuilder — _str Nullable logic ─────────────────────────

    @Test @DisplayName("Date column gets 'String' companion (non-Nullable source)")
    void nonNullableDateGetsStringCompanion() {
        ParsedSheet sheet = new ParsedSheet(
                List.of("hire_date"),
                List.of("Date"),
                List.of()
        );
        String ddl = scriptBuilder.buildCreateScript("employees", sheet);
        assertThat(ddl).contains("hire_date Date");
        assertThat(ddl).contains("hire_date_str String");
        assertThat(ddl).doesNotContain("hire_date_str Nullable");
    }

    @Test @DisplayName("Nullable(Date) column gets 'Nullable(String)' companion")
    void nullableDateGetsNullableStringCompanion() {
        ParsedSheet sheet = new ParsedSheet(
                List.of("optional_date"),
                List.of("Nullable(Date)"),
                List.of()
        );
        String ddl = scriptBuilder.buildCreateScript("t", sheet);
        assertThat(ddl).contains("optional_date Nullable(Date)");
        assertThat(ddl).contains("optional_date_str Nullable(String)");
    }

    @Test @DisplayName("DateTime column gets 'String' companion (non-Nullable source)")
    void nonNullableDateTimeGetsStringCompanion() {
        ParsedSheet sheet = new ParsedSheet(
                List.of("created_at"),
                List.of("DateTime"),
                List.of()
        );
        String ddl = scriptBuilder.buildCreateScript("t", sheet);
        assertThat(ddl).contains("created_at DateTime");
        assertThat(ddl).contains("created_at_str String");
        assertThat(ddl).doesNotContain("created_at_str Nullable");
    }

    @Test @DisplayName("Nullable(DateTime) column gets 'Nullable(String)' companion")
    void nullableDateTimeGetsNullableStringCompanion() {
        ParsedSheet sheet = new ParsedSheet(
                List.of("updated_at"),
                List.of("Nullable(DateTime)"),
                List.of()
        );
        String ddl = scriptBuilder.buildCreateScript("t", sheet);
        assertThat(ddl).contains("updated_at_str Nullable(String)");
    }

    @Test @DisplayName("DDL always contains operation_dttm and ReplicatedMergeTree")
    void ddlContainsAuditFieldAndEngine() {
        ParsedSheet sheet = new ParsedSheet(
                List.of("val"), List.of("String"), List.of());
        String ddl = scriptBuilder.buildCreateScript("audit_test", sheet);
        assertThat(ddl).contains("operation_dttm DateTime");
        assertThat(ddl).contains("ReplicatedMergeTree");
        assertThat(ddl).contains("{shard}");
        assertThat(ddl).contains("{replica}");
    }

    @Test @DisplayName("buildInsertColumns includes _str companions and operation_dttm")
    void insertColumnsCorrect() {
        ParsedSheet sheet = new ParsedSheet(
                List.of("name", "created", "opt_date"),
                List.of("String", "DateTime", "Nullable(Date)"),
                List.of()
        );
        List<String> cols = scriptBuilder.buildInsertColumns(sheet);
        assertThat(cols).containsExactly(
                "name", "created", "created_str", "opt_date", "opt_date_str", "operation_dttm");
    }

    @Test @DisplayName("buildInsertSql generates correct placeholder count")
    void insertSqlPlaceholders() {
        List<String> cols = List.of("a", "b", "c");
        String sql = scriptBuilder.buildInsertSql("my_table", cols);
        assertThat(sql).contains("INSERT INTO my_table");
        assertThat(sql).contains("(a, b, c)");
        assertThat(sql).contains("VALUES (?, ?, ?)");
    }

    @Test @DisplayName("buildRowValues: Date cell normalises to ISO and preserves original in _str")
    void rowValuesDateNormalisation() {
        ParsedSheet sheet = new ParsedSheet(
                List.of("event_date"), List.of("Date"), List.of());
        List<Object> values = scriptBuilder.buildRowValues(
                List.of("05/25/2004"), sheet, LocalDateTime.of(2024, 1, 1, 12, 0));
        // [event_date, event_date_str, operation_dttm]
        assertThat(values.get(0)).isEqualTo("2004-05-25");       // normalised ISO
        assertThat(values.get(1)).isEqualTo("05/25/2004");       // original raw value
        assertThat(values.get(2)).isEqualTo("2024-01-01 12:00:00"); // operation_dttm
    }

    @Test @DisplayName("buildRowValues: Nullable(Date) with null cell → both null")
    void rowValuesNullableDate_nullCell() {
        ParsedSheet sheet = new ParsedSheet(
                List.of("d"), List.of("Nullable(Date)"), List.of());
        List<Object> values = scriptBuilder.buildRowValues(
                List.of((String) null), sheet, LocalDateTime.now());
        assertThat(values.get(0)).isNull(); // date field
        assertThat(values.get(1)).isNull(); // _str companion
    }
}
