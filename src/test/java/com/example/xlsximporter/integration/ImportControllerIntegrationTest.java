package com.example.xlsximporter.integration;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.web.multipart.MultipartFile;

import java.util.List;

import static org.hamcrest.Matchers.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

/**
 * Integration tests for {@link com.example.xlsximporter.controller.ImportController}.
 *
 * Uses MockMvc against the full Spring context wired with Testcontainers.
 */
@SpringBootTest
@AutoConfigureMockMvc
class ImportControllerIntegrationTest extends AbstractIntegrationTest {

    @Autowired
    private MockMvc mockMvc;

    // ─────────────────────────────────────────────────────────────────────────
    // POST /api/v1/import/xlsx  — success cases
    // ─────────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("POST /xlsx returns 200 with correct response body")
    void postXlsx_success() throws Exception {
        String table = "ctrl_ok_" + System.currentTimeMillis();

        MultipartFile multipartFile = XlsxTestHelper.buildXlsx(
                List.of("id", "label"),
                List.of("Int64", "String"),
                List.of(List.of(1, "a"), List.of(2, "b"))
        );

        MockMultipartFile mockFile = new MockMultipartFile(
                "file", "test.xlsx",
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                multipartFile.getBytes()
        );

        mockMvc.perform(multipart("/api/v1/import/xlsx")
                        .file(mockFile)
                        .param("tableName", table))
                .andExpect(status().isOk())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
                .andExpect(jsonPath("$.tableName").value(table))
                .andExpect(jsonPath("$.rowsInserted").value(2))
                .andExpect(jsonPath("$.createScript").value(containsString("ReplicatedMergeTree")))
                .andExpect(jsonPath("$.operationDttm").isNotEmpty())
                .andExpect(jsonPath("$.id").isNumber());
    }

    @Test
    @DisplayName("POST /xlsx with reserved column name returns 400 with validation details")
    void postXlsx_reservedColumnName_returns400() throws Exception {
        String table = "ctrl_bad_col_" + System.currentTimeMillis();

        MultipartFile multipartFile = XlsxTestHelper.buildXlsx(
                List.of("from"),     // reserved keyword
                List.of("String"),
                List.of(List.of("x"))
        );

        MockMultipartFile mockFile = new MockMultipartFile(
                "file", "test.xlsx",
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                multipartFile.getBytes()
        );

        mockMvc.perform(multipart("/api/v1/import/xlsx")
                        .file(mockFile)
                        .param("tableName", table))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.status").value(400))
                .andExpect(jsonPath("$.error").value("VALIDATION_ERROR"))
                .andExpect(jsonPath("$.details").isArray())
                .andExpect(jsonPath("$.details[0]").value(containsString("reserved")));
    }

    @Test
    @DisplayName("POST /xlsx with invalid type returns 400")
    void postXlsx_invalidType_returns400() throws Exception {
        String table = "ctrl_bad_type_" + System.currentTimeMillis();

        MultipartFile multipartFile = XlsxTestHelper.buildXlsx(
                List.of("col1"),
                List.of("INTEGER"),   // not valid ClickHouse
                List.of(List.of("1"))
        );

        MockMultipartFile mockFile = new MockMultipartFile(
                "file", "test.xlsx",
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                multipartFile.getBytes()
        );

        mockMvc.perform(multipart("/api/v1/import/xlsx")
                        .file(mockFile)
                        .param("tableName", table))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error").value("VALIDATION_ERROR"))
                .andExpect(jsonPath("$.details[0]").value(containsString("INTEGER")));
    }

    @Test
    @DisplayName("POST /xlsx with wrong data value returns 400 with row/col detail")
    void postXlsx_wrongDataValue_returns400() throws Exception {
        String table = "ctrl_bad_data_" + System.currentTimeMillis();

        MultipartFile multipartFile = XlsxTestHelper.buildXlsx(
                List.of("count"),
                List.of("Int32"),
                List.of(List.of("not-an-int"))
        );

        MockMultipartFile mockFile = new MockMultipartFile(
                "file", "test.xlsx",
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                multipartFile.getBytes()
        );

        mockMvc.perform(multipart("/api/v1/import/xlsx")
                        .file(mockFile)
                        .param("tableName", table))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.details[0]").value(containsString("not-an-int")));
    }

    @Test
    @DisplayName("POST /xlsx with empty file returns 400")
    void postXlsx_emptyFile_returns400() throws Exception {
        MockMultipartFile emptyFile = new MockMultipartFile(
                "file", "empty.xlsx",
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                new byte[0]
        );

        mockMvc.perform(multipart("/api/v1/import/xlsx")
                        .file(emptyFile)
                        .param("tableName", "some_table"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error").value("VALIDATION_ERROR"));
    }

    @Test
    @DisplayName("POST /xlsx without tableName returns 400")
    void postXlsx_missingTableName_returns400() throws Exception {
        MockMultipartFile mockFile = new MockMultipartFile(
                "file", "test.xlsx",
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                new byte[]{1, 2, 3}
        );

        mockMvc.perform(multipart("/api/v1/import/xlsx")
                        .file(mockFile))  // no tableName param
                .andExpect(status().is4xxClientError());
    }

    // ─────────────────────────────────────────────────────────────────────────
    // GET /api/v1/import/logs
    // ─────────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("GET /logs returns 200 with a JSON array")
    void getLogs_returnsArray() throws Exception {
        mockMvc.perform(get("/api/v1/import/logs"))
                .andExpect(status().isOk())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
                .andExpect(jsonPath("$").isArray());
    }

    @Test
    @DisplayName("GET /logs/{tableName} returns logs for that table after import")
    void getLogsByTable_returnsMatchingLogs() throws Exception {
        String table = "ctrl_logs_" + System.currentTimeMillis();

        MultipartFile multipartFile = XlsxTestHelper.buildXlsx(
                List.of("x"),
                List.of("String"),
                List.of(List.of("hello"))
        );
        MockMultipartFile mockFile = new MockMultipartFile(
                "file", "test.xlsx",
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                multipartFile.getBytes()
        );

        // Do the import first
        mockMvc.perform(multipart("/api/v1/import/xlsx")
                        .file(mockFile).param("tableName", table))
                .andExpect(status().isOk());

        // Then query the log
        mockMvc.perform(get("/api/v1/import/logs/" + table))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$").isArray())
                .andExpect(jsonPath("$[0].tableName").value(table))
                .andExpect(jsonPath("$[0].rowsInserted").value(1));
    }

    // ─────────────────────────────────────────────────────────────────────────
    // GET /api/v1/import/health
    // ─────────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("GET /health returns 200 UP")
    void getHealth_returnsUp() throws Exception {
        mockMvc.perform(get("/api/v1/import/health"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("UP"));
    }
}
