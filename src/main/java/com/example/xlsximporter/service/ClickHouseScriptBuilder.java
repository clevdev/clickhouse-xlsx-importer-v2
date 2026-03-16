package com.example.xlsximporter.service;

import com.example.xlsximporter.config.ClickHouseProperties;
import com.example.xlsximporter.dto.SheetMeta;
import com.example.xlsximporter.validation.ClickHouseTypeRegistry;
import com.example.xlsximporter.validation.DateParser;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
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
     * Strips thousands separators from integer strings using the configured locale.
     */
    /**
     * Strips thousands separators from an integer string.
     */
    private String stripThousands(String raw) {
        String s = raw.trim()
                .replace("\u00A0", "")
                .replace(" ", "")
                .replace(",", "")
                .replace(".", "");
        try {
            Long.parseLong(s);
            return s;
        } catch (NumberFormatException e) {
            throw new com.example.xlsximporter.exception.ValidationException(
                "Cannot parse '" + raw + "' as integer: cleaned value is '" + s + "'");
        }
    }

    /**
     * Normalises a floating-point string ({@code Float32}, {@code Float64}) to dot-decimal format.
     * Fractional digits are preserved exactly as in the source — no zeros added or removed.
     */
    private String normalizeNumber(String raw) {
        return toPlainDecimalString(raw);
    }

    /**
     * Normalises a {@code Decimal(p,s)} string preserving full precision.
     * Fractional digits are preserved exactly as in the source — no zeros added or removed.
     * Supports up to 38 significant digits ({@code Decimal(38,6)}).
     */
    private String normalizeDecimal(String raw) {
        return toPlainDecimalString(raw);
    }

    /**
     * Core normalisation: strips thousands separators and converts to dot-decimal format
     * without modifying the fractional part in any way.
     *
     * <p>Rules:
     * <ul>
     *   <li>Whitespace (space, non-breaking space) — always removed (thousands separator)</li>
     *   <li>Both {@code ,} and {@code .} present — the <em>last</em> one is decimal,
     *       the earlier ones are thousands separators and are removed</li>
     *   <li>Only {@code ,} — decimal if it appears exactly once with ≤ 3 digits after it,
     *       otherwise thousands separator</li>
     *   <li>Only {@code .} — same logic as comma</li>
     * </ul>
     *
     * <p>The normalised string is validated via {@link java.math.BigDecimal} to catch
     * non-numeric values early. The value is then returned as {@code bd.toPlainString()}
     * which preserves <em>exactly</em> the digits present in the source:
     * <pre>
     *   "1234.5"       → "1234.5"        (fractional part unchanged)
     *   "1234.56"      → "1234.56"
     *   "1234.123456"  → "1234.123456"   (up to 6 decimal places kept as-is)
     *   "1,234.56"     → "1234.56"       (comma=thousands removed)
     *   "1.234,56"     → "1234.56"       (dot=thousands removed, comma→dot)
     *   "7,945.53"     → "7945.53"
     *   "1234"         → "1234"          (integer — no decimal point added)
     * </pre>
     */
    private String toPlainDecimalString(String raw) {
        String s = raw.trim()
                .replace("\u00A0", "")
                .replace(" ", "");

        int lastDot   = s.lastIndexOf('.');
        int lastComma = s.lastIndexOf(',');

        if (lastDot >= 0 && lastComma >= 0) {
            // Both separators present — the last one is decimal
            if (lastDot > lastComma) {
                s = s.replace(",", "");                   // "1,234.56"  → "1234.56"
            } else {
                s = s.replace(".", "").replace(",", "."); // "1.234,56"  → "1234.56"
            }
        } else if (lastComma >= 0) {
            // Only comma present — thousands separator if it appears exactly once
            // AND exactly 3 digits follow it (e.g. "1,234"); decimal separator otherwise.
            // We do NOT limit by ≤3 because decimal values like "1234,1234" (4 fractional
            // digits) or Decimal(38,6) values must also be treated as decimal separators.
            String beforeComma = s.substring(0, lastComma);
            String afterComma  = s.substring(lastComma + 1);
            boolean isThousands = s.indexOf(',') == lastComma          // only one comma
                    && afterComma.length() == 3                         // exactly 3 digits after
                    && afterComma.chars().allMatch(Character::isDigit)  // all digits
                    && !beforeComma.isEmpty()                           // something before it
                    && beforeComma.chars().allMatch(Character::isDigit);// all digits before too
            s = isThousands ? s.replace(",", "") : s.replace(",", ".");
        } else if (lastDot >= 0) {
            // Only dot present — same logic: thousands if exactly 3 digits follow
            String beforeDot = s.substring(0, lastDot);
            String afterDot  = s.substring(lastDot + 1);
            boolean isThousands = s.indexOf('.') == lastDot
                    && afterDot.length() == 3
                    && afterDot.chars().allMatch(Character::isDigit)
                    && !beforeDot.isEmpty()
                    && beforeDot.chars().allMatch(Character::isDigit);
            if (isThousands) s = s.replace(".", "");
        }

        // Validate via BigDecimal — toPlainString() returns exactly the digits
        // that were in the source string, with no rounding, no zero-padding, no
        // scientific notation.
        try {
            return new java.math.BigDecimal(s).toPlainString();
        } catch (NumberFormatException e) {
            throw new com.example.xlsximporter.exception.ValidationException(
                "Cannot parse '" + raw + "' as number: cleaned value is '" + s + "'");
        }
    }

    private boolean parseBool(String raw) {
        return raw.equalsIgnoreCase("true") || raw.equals("1");
    }
}
