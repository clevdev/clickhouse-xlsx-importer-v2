package com.example.xlsximporter.dto;

import java.util.List;

/**
 * Holds ONLY the header information (column names + types) from an xlsx file.
 * Data rows are never stored here — they are streamed directly to ClickHouse
 * via {@code XlsxParserService.streamRows()} callback.
 *
 * <p>This is the key difference from {@link ParsedSheet} in v2:
 * {@code ParsedSheet} kept ALL rows in memory; {@code SheetMeta} keeps zero rows.
 */
public record SheetMeta(
        List<String> columnNames,
        List<String> columnTypes
) {}
