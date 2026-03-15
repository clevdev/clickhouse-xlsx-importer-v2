package com.example.xlsximporter.repository;

import com.example.xlsximporter.model.ImportLog;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface ImportLogRepository extends JpaRepository<ImportLog, Long> {

    List<ImportLog> findByTableNameOrderByOperationDttmDesc(String tableName);
}
