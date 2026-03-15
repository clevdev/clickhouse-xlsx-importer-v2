package com.example.xlsximporter.integration;

import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.web.multipart.MultipartFile;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;

/**
 * Utility for building in-memory xlsx {@link MultipartFile} objects in tests.
 */
public final class XlsxTestHelper {

    private XlsxTestHelper() {}

    /**
     * Creates an xlsx MultipartFile with:
     * <ul>
     *   <li>Row 1: column names</li>
     *   <li>Row 2: column types</li>
     *   <li>Row 3+: data rows</li>
     * </ul>
     */
    public static MultipartFile buildXlsx(List<String> columnNames,
                                           List<String> columnTypes,
                                           List<List<Object>> dataRows) throws IOException {
        try (var wb = new XSSFWorkbook()) {
            var sheet = wb.createSheet("data");

            // Row 1 — names
            var nameRow = sheet.createRow(0);
            for (int i = 0; i < columnNames.size(); i++) {
                nameRow.createCell(i).setCellValue(columnNames.get(i));
            }

            // Row 2 — types
            var typeRow = sheet.createRow(1);
            for (int i = 0; i < columnTypes.size(); i++) {
                typeRow.createCell(i).setCellValue(columnTypes.get(i));
            }

            // Rows 3+ — data
            for (int r = 0; r < dataRows.size(); r++) {
                var row = sheet.createRow(r + 2);
                List<Object> values = dataRows.get(r);
                for (int c = 0; c < values.size(); c++) {
                    var cell = row.createCell(c);
                    Object v = values.get(c);
                    if (v == null) {
                        cell.setBlank();
                    } else if (v instanceof Number n) {
                        cell.setCellValue(n.doubleValue());
                    } else if (v instanceof Boolean b) {
                        cell.setCellValue(b);
                    } else {
                        cell.setCellValue(v.toString());
                    }
                }
            }

            var out = new ByteArrayOutputStream();
            wb.write(out);
            return new MockMultipartFile("file", "test.xlsx",
                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    out.toByteArray());
        }
    }
}
