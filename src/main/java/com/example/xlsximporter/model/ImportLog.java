package com.example.xlsximporter.model;

import jakarta.persistence.*;
import lombok.*;

import java.time.LocalDateTime;

/**
 * Persisted record of every successful xlsx import operation.
 * Stored in PostgreSQL.
 */
@Entity
@Table(name = "import_log")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ImportLog {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id")
    private Long id;

    /** Name of the ClickHouse table that was created/populated. */
    @Column(name = "table_name", nullable = false, length = 128)
    private String tableName;

    /** Timestamp of the import operation. */
    @Column(name = "operation_dttm", nullable = false)
    private LocalDateTime operationDttm;

    /** Number of rows inserted. */
    @Column(name = "rows_inserted", nullable = false)
    private int rowsInserted;

    /** ClickHouse node that processed the write. */
    @Column(name = "processed_by_node", length = 32)
    private String processedByNode;

    /** Original xlsx filename. */
    @Column(name = "source_filename", length = 256)
    private String sourceFilename;
}
