package com.example.xlsximporter.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Data
@Component
@ConfigurationProperties(prefix = "clickhouse")
public class ClickHouseProperties {

    private NodeConfig node1 = new NodeConfig();
    private NodeConfig node2 = new NodeConfig();

    @Data
    public static class NodeConfig {
        private String url;
        private String username = "default";
        private String password = "";
        private PoolConfig pool = new PoolConfig();
    }

    @Data
    public static class PoolConfig {
        private String poolName;
        private int maximumPoolSize = 10;
        private int minimumIdle = 2;
        private long connectionTimeout = 30000;
        private long idleTimeout = 600000;
        private long maxLifetime = 1800000;
        private String connectionTestQuery = "SELECT 1";
    }
}
