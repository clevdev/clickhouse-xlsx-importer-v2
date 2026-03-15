package com.example.xlsximporter.service;

import com.example.xlsximporter.dto.SheetMeta;
import com.example.xlsximporter.exception.ValidationException;
import lombok.extern.slf4j.Slf4j;
import org.apache.poi.openxml4j.opc.OPCPackage;
import org.apache.poi.ss.util.CellReference;
import org.apache.poi.xssf.eventusermodel.XSSFReader;
import org.apache.poi.xssf.eventusermodel.XSSFSheetXMLHandler;
import org.apache.poi.xssf.eventusermodel.XSSFSheetXMLHandler.SheetContentsHandler;
import org.apache.poi.xssf.model.SharedStrings;
import org.apache.poi.xssf.model.StylesTable;
import org.apache.poi.xssf.usermodel.XSSFComment;
import org.springframework.stereotype.Service;
import org.xml.sax.InputSource;
import org.xml.sax.XMLReader;

import javax.xml.parsers.SAXParserFactory;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * SAX streaming xlsx parser that works exclusively with {@code byte[]}.
 */
@Slf4j
@Service
public class XlsxParserService {

    /**
     * Reads only rows 0–1 (column names + types) from the xlsx bytes.
     * File name validation and logging are the caller's responsibility.
     *
     * @param fileBytes raw xlsx bytes (from {@code MultipartFile.getBytes()})
     */
    public SheetMeta readMeta(byte[] fileBytes) {
        try (OPCPackage pkg = OPCPackage.open(new ByteArrayInputStream(fileBytes))) {

            XSSFReader    reader = new XSSFReader(pkg);
            SharedStrings sst   = reader.getSharedStringsTable();
            StylesTable   styles = reader.getStylesTable();
            MetaCollector meta   = new MetaCollector();

            parseSheet(reader, styles, sst, meta);
            return meta.build();

        } catch (ValidationException e) {
            throw e;
        } catch (Exception e) {
            throw new ValidationException("Failed to read xlsx metadata: " + e.getMessage());
        }
    }

    /**
     * Streams data rows (row index ≥ 2) to {@code rowConsumer} one row at a time.
     *
     * @param fileBytes   raw xlsx bytes — same array passed to {@link #readMeta}
     * @param columnCount number of columns (from {@link SheetMeta#columnNames()})
     * @param rowConsumer called once per data row
     */
    public void streamRows(byte[] fileBytes, int columnCount,
                           Consumer<List<String>> rowConsumer) {
        try (OPCPackage pkg = OPCPackage.open(new ByteArrayInputStream(fileBytes))) {

            XSSFReader    reader  = new XSSFReader(pkg);
            SharedStrings sst    = reader.getSharedStringsTable();
            StylesTable   styles = reader.getStylesTable();
            RowStreamer streamer  = new RowStreamer(columnCount, rowConsumer);

            parseSheet(reader, styles, sst, streamer);

        } catch (ValidationException e) {
            throw e;
        } catch (Exception e) {
            throw new ValidationException("Failed to stream xlsx rows: " + e.getMessage());
        }
    }

    // ── internals ─────────────────────────────────────────────────────────────

    private void parseSheet(XSSFReader reader, StylesTable styles,
                             SharedStrings sst, SheetContentsHandler handler) throws Exception {
        SAXParserFactory factory = SAXParserFactory.newInstance();
        factory.setNamespaceAware(true);
        XMLReader xml = factory.newSAXParser().getXMLReader();
        xml.setContentHandler(new XSSFSheetXMLHandler(styles, sst, handler, false));
        try (InputStream sheet = reader.getSheetsData().next()) {
            xml.parse(new InputSource(sheet));
        }
    }

    // ── SAX handlers ──────────────────────────────────────────────────────────

    private static class MetaCollector implements SheetContentsHandler {
        List<String> names, types;
        private List<String> current;
        private int rowIdx   = -1;
        private int maxRowSeen = -1;   // tracks the highest row index seen in the sheet

        @Override public void startRow(int r) { rowIdx = r; current = new ArrayList<>(); }

        @Override public void endRow(int r) {
            if (r > maxRowSeen) maxRowSeen = r;
            if      (r == 0) { trimTrailing(current); names = new ArrayList<>(current); }
            else if (r == 1) { types = new ArrayList<>(current); }
        }

        @Override public void cell(String ref, String value, XSSFComment c) {
            if (rowIdx > 1) return;
            int col = new CellReference(ref).getCol();
            while (current.size() < col) current.add(null);
            current.add(value == null || value.isBlank() ? null : value.trim());
        }

        SheetMeta build() {
            if (names == null || names.isEmpty())
                throw new ValidationException("Row 1 (column names) is missing or empty");
            if (types == null || types.isEmpty())
                throw new ValidationException("Row 2 (types) is missing or empty");
            if (types.size() < names.size())
                throw new ValidationException(
                    "Row 2 has fewer columns than row 1: " + types.size() + " vs " + names.size());
            // Row indices are 0-based: row 0 = names, row 1 = types, row 2+ = data.
            // If the highest row seen is < 2, the file has no data rows at all.
            if (maxRowSeen < 2)
                throw new ValidationException(
                    "File contains no data rows. " +
                    "Row 1 must be column names, row 2 must be types, row 3+ must be data.");
            log.info("xlsx meta parsed: {} columns", names.size());
            return new SheetMeta(names, new ArrayList<>(types.subList(0, names.size())));
        }

        private static void trimTrailing(List<String> l) {
            while (!l.isEmpty() && (l.get(l.size()-1) == null || l.get(l.size()-1).isBlank()))
                l.remove(l.size()-1);
        }
    }

    private static class RowStreamer implements SheetContentsHandler {
        private final int                    columnCount;
        private final Consumer<List<String>> consumer;
        private List<String>                 current;
        private int                          rowIdx = -1;

        RowStreamer(int columnCount, Consumer<List<String>> consumer) {
            this.columnCount = columnCount;
            this.consumer    = consumer;
        }

        @Override public void startRow(int r) { rowIdx = r; current = new ArrayList<>(); }

        @Override public void endRow(int r) {
            if (r < 2) return;
            while (current.size() < columnCount) current.add(null);
            consumer.accept(current);
            current = null;
        }

        @Override public void cell(String ref, String value, XSSFComment c) {
            if (rowIdx < 2 || current == null) return;
            int col = new CellReference(ref).getCol();
            if (col >= columnCount) return;
            while (current.size() < col) current.add(null);
            current.add(value == null || value.isBlank() ? null : value.trim());
        }
    }
}
