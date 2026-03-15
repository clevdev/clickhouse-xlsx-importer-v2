package com.example.xlsximporter.config;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean;
import org.springframework.orm.jpa.vendor.HibernateJpaVendorAdapter;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.Map;

@Slf4j
@Configuration
@RequiredArgsConstructor
public class DataSourceConfig {

    private final ClickHouseProperties clickHouseProperties;

    // ─────────────────────────────────────────────────────────────────────────
    // PostgreSQL — Primary DataSource (used by JPA/Hibernate for import logs)
    // ─────────────────────────────────────────────────────────────────────────

    @Primary
    @Bean(name = "postgresDataSourceProperties")
    @ConfigurationProperties("spring.datasource")
    public DataSourceProperties postgresDataSourceProperties() {
        return new DataSourceProperties();
    }

    @Primary
    @Bean(name = "postgresDataSource")
    @ConfigurationProperties("spring.datasource.hikari")
    public DataSource postgresDataSource(
            @Qualifier("postgresDataSourceProperties") DataSourceProperties props) {
        return props.initializeDataSourceBuilder()
                .type(HikariDataSource.class)
                .build();
    }

    @Primary
    @Bean(name = "entityManagerFactory")
    public LocalContainerEntityManagerFactoryBean entityManagerFactory(
            @Qualifier("postgresDataSource") DataSource dataSource) {

        LocalContainerEntityManagerFactoryBean em = new LocalContainerEntityManagerFactoryBean();
        em.setDataSource(dataSource);
        em.setPackagesToScan("com.example.xlsximporter.model");

        HibernateJpaVendorAdapter vendorAdapter = new HibernateJpaVendorAdapter();
        em.setJpaVendorAdapter(vendorAdapter);

        Map<String, Object> properties = new HashMap<>();
        properties.put("hibernate.hbm2ddl.auto", "update");
        properties.put("hibernate.dialect", "org.hibernate.dialect.PostgreSQLDialect");
        properties.put("hibernate.show_sql", "false");
        em.setJpaPropertyMap(properties);

        return em;
    }

    @Primary
    @Bean(name = "transactionManager")
    public PlatformTransactionManager transactionManager(
            @Qualifier("entityManagerFactory")
            LocalContainerEntityManagerFactoryBean entityManagerFactory) {
        JpaTransactionManager txManager = new JpaTransactionManager();
        txManager.setEntityManagerFactory(entityManagerFactory.getObject());
        return txManager;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // ClickHouse Node 1
    // ─────────────────────────────────────────────────────────────────────────

    @Bean(name = "dataSourceNode1")
    public DataSource dataSourceNode1() {
        return buildClickHouseDataSource(clickHouseProperties.getNode1());
    }

    @Bean(name = "jdbcTemplateNode1")
    public JdbcTemplate jdbcTemplateNode1(@Qualifier("dataSourceNode1") DataSource ds) {
        return new JdbcTemplate(ds);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // ClickHouse Node 2
    // ─────────────────────────────────────────────────────────────────────────

    @Bean(name = "dataSourceNode2")
    public DataSource dataSourceNode2() {
        return buildClickHouseDataSource(clickHouseProperties.getNode2());
    }

    @Bean(name = "jdbcTemplateNode2")
    public JdbcTemplate jdbcTemplateNode2(@Qualifier("dataSourceNode2") DataSource ds) {
        return new JdbcTemplate(ds);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Helper
    // ─────────────────────────────────────────────────────────────────────────

    private HikariDataSource buildClickHouseDataSource(ClickHouseProperties.NodeConfig node) {
        ClickHouseProperties.PoolConfig pool = node.getPool();

        HikariConfig cfg = new HikariConfig();
        cfg.setJdbcUrl(node.getUrl());
        cfg.setUsername(node.getUsername());
        cfg.setPassword(node.getPassword());
        cfg.setDriverClassName("com.clickhouse.jdbc.ClickHouseDriver");

        cfg.setPoolName(pool.getPoolName());
        cfg.setMaximumPoolSize(pool.getMaximumPoolSize());
        cfg.setMinimumIdle(pool.getMinimumIdle());
        cfg.setConnectionTimeout(pool.getConnectionTimeout());
        cfg.setIdleTimeout(pool.getIdleTimeout());
        cfg.setMaxLifetime(pool.getMaxLifetime());
        cfg.setConnectionTestQuery(pool.getConnectionTestQuery());

        log.info("Initializing HikariCP pool [{}] → {}", pool.getPoolName(), node.getUrl());
        return new HikariDataSource(cfg);
    }
}
