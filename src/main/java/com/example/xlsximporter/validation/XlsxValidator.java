package com.example.xlsximporter.validation;

import com.example.xlsximporter.dto.SheetMeta;
import com.example.xlsximporter.exception.ValidationException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Validates header (names + types) and individual data rows.
 *
 * <p>Split into two methods to support the streaming pipeline:
 * <ul>
 *   <li>{@link #validateMeta} — called once before streaming, checks names + types</li>
 *   <li>{@link #validateRow}  — called per row during streaming, checks cell values</li>
 * </ul>
 */
@Slf4j
@Component
public class XlsxValidator {

    private static final Pattern COLUMN_NAME_PATTERN =
            Pattern.compile("^[a-zA-Z_][a-zA-Z0-9_]*$");
    private static final int MAX_COLUMN_NAME_LENGTH = 64;

    /** Validates table name, column names and column types. Called once before streaming. */
    public void validateMeta(SheetMeta meta, String tableName) {
        List<String> errors = new ArrayList<>();
        validateTableName(tableName, errors);
        validateColumnNames(meta.columnNames(), errors);
        validateColumnTypes(meta.columnTypes(), errors);
        if (!errors.isEmpty()) throw new ValidationException(errors);
        log.info("Meta validation passed: {} columns", meta.columnNames().size());
    }

    /** Validates a single data row. Called inline during streaming — throws on first error. */
    public void validateRow(List<String> row, SheetMeta meta) {
        List<String> types = meta.columnTypes();
        for (int c = 0; c < types.size(); c++) {
            String type      = types.get(c);
            String cellValue = c < row.size() ? row.get(c) : null;
            boolean nullable = ClickHouseTypeRegistry.isNullable(type);
            if (cellValue == null || cellValue.isBlank()) {
                if (!nullable)
                    throw new ValidationException(
                        "col " + (c+1) + ": null value not allowed for non-Nullable '" + type + "'");
                continue;
            }
            String err = validateCell(cellValue, type);
            if (err != null)
                throw new ValidationException("col " + (c+1) + ": " + err);
        }
    }

    // ── private helpers (same logic as v2) ───────────────────────────────────

    private void validateTableName(String tableName, List<String> errors) {
        if (tableName == null || tableName.isBlank()) {
            errors.add("Table name must not be blank"); return;
        }
        if (!COLUMN_NAME_PATTERN.matcher(tableName).matches())
            errors.add("Table name '" + tableName + "' contains invalid characters");
        if (ClickHouseTypeRegistry.RESERVED_KEYWORDS.contains(tableName.toLowerCase()))
            errors.add("Table name '" + tableName + "' is a reserved ClickHouse keyword");
    }

    private void validateColumnNames(List<String> names, List<String> errors) {
        if (names == null || names.isEmpty()) {
            errors.add("No column names in row 1"); return;
        }
        for (int i = 0; i < names.size(); i++) {
            String name = names.get(i); int col = i + 1;
            if (name == null || name.isBlank()) { errors.add("Column " + col + ": name is blank"); continue; }
            if (name.length() > MAX_COLUMN_NAME_LENGTH)
                errors.add("Column " + col + ": name too long (max " + MAX_COLUMN_NAME_LENGTH + ")");
            if (!COLUMN_NAME_PATTERN.matcher(name).matches())
                errors.add("Column " + col + ": name '" + name + "' is invalid");
            if (ClickHouseTypeRegistry.RESERVED_KEYWORDS.contains(name.toLowerCase()))
                errors.add("Column " + col + ": '" + name + "' is a reserved keyword");
        }
    }

    private void validateColumnTypes(List<String> types, List<String> errors) {
        if (types == null || types.isEmpty()) {
            errors.add("No column types in row 2"); return;
        }
        for (int i = 0; i < types.size(); i++) {
            String type = types.get(i); int col = i + 1;
            if (type == null || type.isBlank()) { errors.add("Column " + col + ": type is blank"); continue; }
            if (!ClickHouseTypeRegistry.isValidType(type))
                errors.add("Column " + col + ": type '" + type + "' is not a valid ClickHouse type");
        }
    }

    private String validateCell(String value, String rawType) {
        String base = ClickHouseTypeRegistry.extractBaseTypeLower(rawType)
                .replaceAll("\\(.*\\)", "").trim();
        return switch (base) {
            case "int8"   -> checkIntRange(value, Byte.MIN_VALUE,    Byte.MAX_VALUE,    rawType);
            case "int16"  -> checkIntRange(value, Short.MIN_VALUE,   Short.MAX_VALUE,   rawType);
            case "int32"  -> checkIntRange(value, Integer.MIN_VALUE, Integer.MAX_VALUE, rawType);
            case "int64"  -> checkLong(value, rawType);
            case "uint8"  -> checkIntRange(value, 0, 255,        rawType);
            case "uint16" -> checkIntRange(value, 0, 65535,      rawType);
            case "uint32" -> checkIntRange(value, 0, 4294967295L,rawType);
            case "uint64" -> checkULong(value, rawType);
            case "float32","float64"       -> checkDouble(value, rawType);
            case "bool","boolean"          -> checkBool(value, rawType);
            case "date","date32"           -> checkDate(value, rawType);
            case "datetime","datetime64"   -> checkDateTime(value, rawType);
            case "uuid"                    -> checkUuid(value, rawType);
            default -> null;
        };
    }

    private String checkIntRange(String v, long min, long max, String type) {
        try { long n = Long.parseLong(v.trim()); if (n < min || n > max) return "out of range for " + type; }
        catch (NumberFormatException e) { return "'" + v + "' is not valid integer for " + type; }
        return null;
    }
    private String checkLong(String v, String type) {
        try { Long.parseLong(v.trim()); } catch (NumberFormatException e) { return "'" + v + "' is not valid Int64"; }
        return null;
    }
    private String checkULong(String v, String type) {
        try { Long.parseUnsignedLong(v.trim()); } catch (NumberFormatException e) { return "'" + v + "' is not valid UInt64"; }
        return null;
    }
    private String checkDouble(String v, String type) {
        try { Double.parseDouble(v.trim()); } catch (NumberFormatException e) { return "'" + v + "' is not valid float"; }
        return null;
    }
    private String checkBool(String v, String type) {
        String lv = v.trim().toLowerCase();
        return (lv.equals("true")||lv.equals("false")||lv.equals("1")||lv.equals("0")) ? null
            : "'" + v + "' is not a valid boolean";
    }
    private String checkDate(String v, String type) {
        return DateParser.isValidDate(v.trim()) ? null : "'" + v + "' is not a parseable date";
    }
    private String checkDateTime(String v, String type) {
        return DateParser.isValidDateTime(v.trim()) ? null : "'" + v + "' is not a parseable datetime";
    }
    private String checkUuid(String v, String type) {
        try { java.util.UUID.fromString(v.trim()); return null; }
        catch (IllegalArgumentException e) { return "'" + v + "' is not a valid UUID"; }
    }
}
