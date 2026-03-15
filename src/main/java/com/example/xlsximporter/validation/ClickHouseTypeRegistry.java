package com.example.xlsximporter.validation;

import java.util.Set;
import java.util.regex.Pattern;

/**
 * Central registry of ClickHouse type rules and reserved keywords.
 */
public final class ClickHouseTypeRegistry {

    private ClickHouseTypeRegistry() {}

    /** Regex for a bare (non-Nullable) ClickHouse type token. */
    public static final Pattern BARE_TYPE_PATTERN = Pattern.compile(
            "^(String|Bool|Boolean|UUID" +
            "|Int8|Int16|Int32|Int64|Int128|Int256" +
            "|UInt8|UInt16|UInt32|UInt64|UInt128|UInt256" +
            "|Float32|Float64" +
            "|Decimal\\(\\d+,\\s*\\d+\\)" +
            "|Date|Date32" +
            "|DateTime(\\('.*'\\))?" +
            "|DateTime64\\(\\d+(,\\s*'.*')?\\)" +
            "|FixedString\\(\\d+\\)" +
            ")$",
            Pattern.CASE_INSENSITIVE
    );

    /** Regex for Nullable(T) — captures the inner type. */
    public static final Pattern NULLABLE_TYPE_PATTERN = Pattern.compile(
            "^Nullable\\((.+)\\)$",
            Pattern.CASE_INSENSITIVE
    );

    /** Types that map to Java LocalDate / date parsing. */
    public static final Set<String> DATE_BASE_TYPES = Set.of("date", "date32");

    /** Types that map to Java LocalDateTime / datetime parsing. */
    public static final Set<String> DATETIME_BASE_TYPES = Set.of("datetime", "datetime64");

    /** ClickHouse reserved keywords that must not be used as column names. */
    public static final Set<String> RESERVED_KEYWORDS = Set.of(
            "select", "from", "where", "insert", "into", "create", "table",
            "drop", "alter", "delete", "update", "join", "on", "as", "by",
            "group", "order", "limit", "offset", "having", "union", "all",
            "distinct", "with", "not", "and", "or", "in", "is", "null",
            "true", "false", "case", "when", "then", "else", "end",
            "if", "exists", "engine", "primary", "key", "partition",
            "ttl", "settings", "format", "sample", "prewhere",
            "array", "tuple", "map", "between", "like", "ilike",
            "global", "local", "materialized", "view", "database",
            "cluster", "shard", "replica", "distributed", "merge",
            "set", "show", "describe", "explain", "optimize", "check",
            "truncate", "rename", "exchange", "attach", "detach",
            "system", "kill", "query", "default", "values", "columns",
            "index", "constraint", "projection", "row", "rows",
            "interval", "second", "minute", "hour", "day", "week",
            "month", "quarter", "year", "any", "last", "first",
            "inner", "outer", "left", "right", "full", "cross",
            "using", "final", "replace", "deduplicate", "except",
            "intersect", "anti", "semi", "asof", "natural",
            "over", "window", "partition", "rank", "dense_rank",
            "row_number", "lead", "lag", "apply", "lateral",
            "foreach", "in_partition"
    );

    /**
     * Returns true if the given type string (possibly Nullable) is valid.
     */
    public static boolean isValidType(String rawType) {
        if (rawType == null || rawType.isBlank()) return false;
        String trimmed = rawType.trim();
        var nullableMatcher = NULLABLE_TYPE_PATTERN.matcher(trimmed);
        if (nullableMatcher.matches()) {
            String inner = nullableMatcher.group(1).trim();
            return BARE_TYPE_PATTERN.matcher(inner).matches();
        }
        return BARE_TYPE_PATTERN.matcher(trimmed).matches();
    }

    /**
     * Returns true if rawType (possibly Nullable) has a base date type.
     */
    public static boolean isDateType(String rawType) {
        return matchesBaseType(rawType, DATE_BASE_TYPES);
    }

    /**
     * Returns true if rawType (possibly Nullable) has a base datetime type.
     */
    public static boolean isDateTimeType(String rawType) {
        return matchesBaseType(rawType, DATETIME_BASE_TYPES);
    }

    /**
     * Returns true if rawType is nullable.
     */
    public static boolean isNullable(String rawType) {
        if (rawType == null) return false;
        return NULLABLE_TYPE_PATTERN.matcher(rawType.trim()).matches();
    }

    /**
     * Extracts the base (inner) type name in lower case from a possibly Nullable type.
     * E.g. "Nullable(Int64)" → "int64", "Date" → "date"
     */
    public static String extractBaseTypeLower(String rawType) {
        if (rawType == null) return "";
        String trimmed = rawType.trim();
        var nullableMatcher = NULLABLE_TYPE_PATTERN.matcher(trimmed);
        if (nullableMatcher.matches()) {
            return nullableMatcher.group(1).trim().toLowerCase();
        }
        return trimmed.toLowerCase();
    }

    private static boolean matchesBaseType(String rawType, Set<String> types) {
        if (rawType == null) return false;
        String base = extractBaseTypeLower(rawType);
        // strip precision qualifiers for datetime64(3) etc.
        String baseName = base.replaceAll("\\(.*\\)", "").trim();
        return types.contains(baseName);
    }
}
