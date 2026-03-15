package com.example.xlsximporter.dto;

import java.util.List;

/**
 * Holds the parsed content of the xlsx file.
 *
 * @param columnNames  Row 1 — column names
 * @param columnTypes  Row 2 — ClickHouse type strings
 * @param rows         Rows 3+ — raw cell values as strings
 */
public record ParsedSheet(
        List<String> columnNames,
        List<String> columnTypes,
        List<List<String>> rows
) {}
