package com.example.xlsximporter.integration;

import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.ClickHouseContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Base class for integration tests.
 *
 * Starts:
 * <ul>
 *   <li>One PostgreSQL container → mapped to {@code spring.datasource.*}</li>
 *   <li>Two ClickHouse containers → mapped to {@code clickhouse.node1.*} and {@code clickhouse.node2.*}</li>
 * </ul>
 *
 * Containers are declared {@code static} so they are shared across all test methods in a class
 * (started once per class, not per test).
 */
@Testcontainers
public abstract class AbstractIntegrationTest {

    // ── PostgreSQL ────────────────────────────────────────────────────────────
    @Container
    protected static final PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>("postgres:16-alpine")
                    .withDatabaseName("importdb")
                    .withUsername("postgres")
                    .withPassword("postgres");

    // ── ClickHouse Node 1 ─────────────────────────────────────────────────────
    @Container
    protected static final ClickHouseContainer CLICKHOUSE_1 =
            new ClickHouseContainer("clickhouse/clickhouse-server:24.3");

    // ── ClickHouse Node 2 ─────────────────────────────────────────────────────
    @Container
    protected static final ClickHouseContainer CLICKHOUSE_2 =
            new ClickHouseContainer("clickhouse/clickhouse-server:24.3");

    /**
     * Injects container connection details as Spring properties at test startup.
     * Overrides {@code application.yml} settings so the application connects to containers.
     */
    @DynamicPropertySource
    static void overrideProperties(DynamicPropertyRegistry registry) {

        // PostgreSQL
        registry.add("spring.datasource.url",      POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username",  POSTGRES::getUsername);
        registry.add("spring.datasource.password",  POSTGRES::getPassword);

        // ClickHouse Node 1
        registry.add("clickhouse.node1.url", () ->
                "jdbc:ch://" + CLICKHOUSE_1.getHost() + ":" + CLICKHOUSE_1.getMappedPort(8123) + "/default");
        registry.add("clickhouse.node1.username", () -> "default");
        registry.add("clickhouse.node1.password", () -> "");

        // ClickHouse Node 2
        registry.add("clickhouse.node2.url", () ->
                "jdbc:ch://" + CLICKHOUSE_2.getHost() + ":" + CLICKHOUSE_2.getMappedPort(8123) + "/default");
        registry.add("clickhouse.node2.username", () -> "default");
        registry.add("clickhouse.node2.password", () -> "");
    }
}
