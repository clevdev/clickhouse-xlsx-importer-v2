package com.example.xlsximporter.validation;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.Optional;

/**
 * Attempts to parse date / datetime strings using multiple common formats.
 */
public final class DateParser {

    private DateParser() {}

    private static final List<DateTimeFormatter> DATE_FORMATTERS = List.of(
            DateTimeFormatter.ofPattern("yyyy-MM-dd"),
            DateTimeFormatter.ofPattern("MM/dd/yyyy"),
            DateTimeFormatter.ofPattern("dd.MM.yyyy"),
            DateTimeFormatter.ofPattern("dd-MM-yyyy"),
            DateTimeFormatter.ofPattern("dd/MM/yyyy"),
            DateTimeFormatter.ofPattern("yyyyMMdd"),
            DateTimeFormatter.ofPattern("M/d/yyyy"),
            DateTimeFormatter.ofPattern("d.M.yyyy")
    );

    private static final List<DateTimeFormatter> DATETIME_FORMATTERS = List.of(
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"),
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss"),
            DateTimeFormatter.ofPattern("MM/dd/yyyy HH:mm:ss"),
            DateTimeFormatter.ofPattern("dd.MM.yyyy HH:mm:ss"),
            DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss"),
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm"),
            DateTimeFormatter.ofPattern("dd.MM.yyyy HH:mm")
    );

    public static Optional<LocalDate> parseDate(String value) {
        if (value == null || value.isBlank()) return Optional.empty();
        String v = value.trim();
        // Try numeric Excel serial (Apache POI gives us the string representation)
        for (DateTimeFormatter fmt : DATE_FORMATTERS) {
            try {
                return Optional.of(LocalDate.parse(v, fmt));
            } catch (DateTimeParseException ignored) {}
        }
        return Optional.empty();
    }

    public static Optional<LocalDateTime> parseDateTime(String value) {
        if (value == null || value.isBlank()) return Optional.empty();
        String v = value.trim();
        // Try datetime first, then date-only (at start of day)
        for (DateTimeFormatter fmt : DATETIME_FORMATTERS) {
            try {
                return Optional.of(LocalDateTime.parse(v, fmt));
            } catch (DateTimeParseException ignored) {}
        }
        // Fallback: try as plain date → midnight
        return parseDate(v).map(LocalDate::atStartOfDay);
    }

    public static boolean isValidDate(String value) {
        return parseDate(value).isPresent();
    }

    public static boolean isValidDateTime(String value) {
        return parseDateTime(value).isPresent();
    }

    /** Format to ClickHouse-compatible date string. */
    public static String toClickHouseDateString(String value) {
        return parseDate(value)
                .map(d -> d.format(DateTimeFormatter.ISO_LOCAL_DATE))
                .orElse(value);
    }

    /** Format to ClickHouse-compatible datetime string. */
    public static String toClickHouseDateTimeString(String value) {
        return parseDateTime(value)
                .map(dt -> dt.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")))
                .orElse(value);
    }
}
