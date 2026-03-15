package com.example.xlsximporter.service;

import com.example.xlsximporter.config.ClickHouseProperties;
import com.example.xlsximporter.dto.SheetMeta;
import com.example.xlsximporter.validation.ClickHouseTypeRegistry;
import com.example.xlsximporter.validation.DateParser;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Builds ClickHouse DDL (CREATE TABLE) and INSERT SQL from a {@link SheetMeta}.
 *
 * <p><b>_str companion column types:</b>
 * <ul>
 *   <li>{@code Date} → {@code <col>_str String}</li>
 *   <li>{@code Nullable(Date)} → {@code <col>_str Nullable(String)}</li>
 *   <li>{@code DateTime} → {@code <col>_str String}</li>
 *   <li>{@code Nullable(DateTime)} → {@code <col>_str Nullable(String)}</li>
 * </ul>
 *
 * <p><b>Engine selection</b> (controlled via {@code clickhouse.use-plain-merge-tree-in-tests}):
 * <ul>
 *   <li>{@code false} (production): {@code ReplicatedMergeTree('/clickhouse/tables/...', ...)}</li>
 *   <li>{@code true} (integration tests): {@code MergeTree()} — no ZooKeeper required</li>
 * </ul>
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class ClickHouseScriptBuilder {

    private static final DateTimeFormatter CH_DT_FMT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private final ClickHouseProperties chProps;

    // ─────────────────────────────────────────────────────────────────────────
    // DDL
    // ─────────────────────────────────────────────────────────────────────────

    public String buildCreateScript(String tableName, SheetMeta sheet) {
        List<String> names = sheet.columnNames();
        List<String> types = sheet.columnTypes();
        List<String> defs  = new ArrayList<>();

        for (int i = 0; i < names.size(); i++) {
            String name = names.get(i);
            String type = types.get(i);
            defs.add("    " + name + " " + type);

            if (ClickHouseTypeRegistry.isDateType(type)
                    || ClickHouseTypeRegistry.isDateTimeType(type)) {
                // Preserve Nullable wrapping for _str companion
                String strType = ClickHouseTypeRegistry.isNullable(type)
                        ? "Nullable(String)" : "String";
                defs.add("    " + name + "_str " + strType);
            }
        }
        defs.add("    operation_dttm DateTime");

        String engine = chProps.isUsePlainMergeTreeInTests()
                ? "ENGINE = MergeTree()"
                : "ENGINE = ReplicatedMergeTree(\n"
                  + "    '/clickhouse/tables/{shard}/" + tableName + "',\n"
                  + "    '{replica}'\n"
                  + ")";

        String ddl = "CREATE TABLE IF NOT EXISTS " + tableName + "\n(\n"
                + String.join(",\n", defs) + "\n)\n"
                + engine + "\n"
                + "ORDER BY tuple()\n"
                + "SETTINGS index_granularity = 8192";

        log.debug("DDL for '{}':\n{}", tableName, ddl);
        return ddl;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // INSERT helpers
    // ─────────────────────────────────────────────────────────────────────────

    public List<String> buildInsertColumns(SheetMeta sheet) {
        List<String> cols  = new ArrayList<>();
        List<String> names = sheet.columnNames();
        List<String> types = sheet.columnTypes();
        for (int i = 0; i < names.size(); i++) {
            cols.add(names.get(i));
            if (ClickHouseTypeRegistry.isDateType(types.get(i))
                    || ClickHouseTypeRegistry.isDateTimeType(types.get(i))) {
                cols.add(names.get(i) + "_str");
            }
        }
        cols.add("operation_dttm");
        return cols;
    }

    public String buildInsertSql(String tableName, List<String> insertColumns) {
        String ph = insertColumns.stream().map(c -> "?").collect(Collectors.joining(", "));
        return "INSERT INTO " + tableName
                + " (" + String.join(", ", insertColumns) + ")"
                + " VALUES (" + ph + ")";
    }

    public List<Object> buildRowValues(List<String> rawRow, SheetMeta sheet,
                                       LocalDateTime operationDttm) {
        List<Object> values = new ArrayList<>();
        List<String> names  = sheet.columnNames();
        List<String> types  = sheet.columnTypes();

        for (int i = 0; i < names.size(); i++) {
            String  type     = types.get(i);
            String  raw      = (i < rawRow.size()) ? rawRow.get(i) : null;
            boolean nullable = ClickHouseTypeRegistry.isNullable(type);
            boolean isDate   = ClickHouseTypeRegistry.isDateType(type);
            boolean isDt     = ClickHouseTypeRegistry.isDateTimeType(type);

            if (raw == null || raw.isBlank()) {
                values.add(nullable ? null : defaultForType(type));
                if (isDate || isDt) values.add(null);  // _str companion also null
                continue;
            }

            if (isDate) {
                values.add(DateParser.toClickHouseDateString(raw));
                values.add(raw);    // _str = original value from file
            } else if (isDt) {
                values.add(DateParser.toClickHouseDateTimeString(raw));
                values.add(raw);    // _str = original value from file
            } else {
                values.add(coerce(raw, type));
            }
        }

        values.add(operationDttm.format(CH_DT_FMT));
        return values;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Private
    // ─────────────────────────────────────────────────────────────────────────

    private Object coerce(String raw, String type) {
        String base = ClickHouseTypeRegistry.extractBaseTypeLower(type)
                .replaceAll("\\(.*\\)", "").trim();
        return switch (base) {
            // ── Integer types ────────────────────────────────────────────────
            // Strip thousands separators before parsing:
            // Excel formats 1000000 as "1 000 000" or "1.000.000" in some locales.
            case "int8", "int16", "int32"        -> Integer.parseInt(stripThousands(raw));
            case "int64"                          -> Long.parseLong(stripThousands(raw));
            case "uint8", "uint16", "uint32"     -> Long.parseLong(stripThousands(raw));
            case "uint64"                         -> Long.parseUnsignedLong(stripThousands(raw));
            // ── Float types ──────────────────────────────────────────────────
            // Normalise decimal separator: "1,23" → "1.23"
            case "float32"                        -> Float.parseFloat(normalizeNumber(raw));
            case "float64"                        -> Double.parseDouble(normalizeNumber(raw));
            // ── Decimal ──────────────────────────────────────────────────────
            // Returned as String — ClickHouse JDBC converts with full precision server-side
            case "decimal"                        -> normalizeDecimal(raw);
            case "bool", "boolean"               -> parseBool(raw);
            default                              -> raw;  // String, UUID, FixedString
        };
    }

    private Object defaultForType(String type) {
        String base = ClickHouseTypeRegistry.extractBaseTypeLower(type)
                .replaceAll("\\(.*\\)", "").trim();
        return switch (base) {
            case "int8", "int16", "int32",
                 "uint8", "uint16", "uint32"     -> 0;
            case "int64", "uint64"               -> 0L;
            case "float32"                        -> 0.0f;
            case "float64"                        -> 0.0d;
            case "bool", "boolean"               -> false;
            case "decimal"                        -> "0";
            case "date", "date32"                -> "1970-01-01";
            case "datetime", "datetime64"        -> "1970-01-01 00:00:00";
            default                              -> "";
        };
    }

    /**
     * Strips thousands separators that Excel inserts for integer columns:
     * <ul>
     *   <li>{@code "1 000 000"} — space (common in Russian/European locales)</li>
     *   <li>{@code "1\u00A0000"} — non-breaking space</li>
     *   <li>{@code "1.000.000"} — dot used as thousands separator (German/Portuguese locale)</li>
     * </ul>
     * Note: dot-as-thousands-separator is only safe for integer types.
     * For float/decimal use {@link #normalizeNumber} instead.
     */
    private String stripThousands(String raw) {
        return raw.trim()
                .replace("\u00A0", "")   // non-breaking space
                .replace(" ", "")        // regular space
                .replace(".", "");       // dot as thousands separator (integers only)
    }

    /**
     * Normalises a floating-point string from Excel for ClickHouse:
     * <ul>
     *   <li>Strips space / non-breaking-space thousands separators</li>
     *   <li>Replaces comma decimal separator with dot: {@code "1 234,56"} → {@code "1234.56"}</li>
     * </ul>
     */
    private String normalizeNumber(String raw) {
        return raw.trim()
                .replace("\u00A0", "")
                .replace(" ", "")
                .replace(",", ".");
    }

    /**
     * Normalises a Decimal string and validates it via {@link java.math.BigDecimal}
     * to catch non-numeric values early with a clear error message.
     */
    private String normalizeDecimal(String raw) {
        String cleaned = normalizeNumber(raw);
        try {
            new java.math.BigDecimal(cleaned);
        } catch (NumberFormatException e) {
            throw new com.example.xlsximporter.exception.ValidationException(
                "Cannot parse '" + raw + "' as Decimal: normalised value is '" + cleaned + "'");
        }
        return cleaned;
    }

    private boolean parseBool(String raw) {
        return raw.equalsIgnoreCase("true") || raw.equals("1");
    }
}
