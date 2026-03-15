package com.example.xlsximporter.config;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Contact;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.info.License;
import io.swagger.v3.oas.models.servers.Server;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;

/**
 * Swagger / OpenAPI configuration.
 *
 * springdoc-openapi 2.8.x is required for Spring Boot 3.4+ / Spring MVC 6.2+.
 * The key incompatibility in earlier 2.x versions was the use of
 * {@code spring.mvc.pathmatch.use-suffix-pattern} (removed in Spring MVC 6.x).
 * springdoc 2.8.x uses the new {@code PathPatternParser} strategy natively.
 *
 * Access Swagger UI at: http://localhost:8080/swagger-ui.html
 */
@Configuration
public class OpenApiConfig {

    @Value("${server.port:8080}")
    private String serverPort;

    @Bean
    public OpenAPI customOpenAPI() {
        return new OpenAPI()
                .info(new Info()
                        .title("XLSX → ClickHouse Importer API")
                        .version("1.0.0")
                        .description("""
                                ## XLSX to ClickHouse Table Importer

                                Upload an `.xlsx` file to dynamically create a ClickHouse table \
                                and bulk-insert all data rows.

                                ### xlsx file format:
                                | Row | Content |
                                |-----|---------|
                                | 1   | Column **names** |
                                | 2   | Column **types** (e.g. `String`, `Int64`, `Date`, `Nullable(Float64)`) |
                                | 3+  | **Data** rows |

                                ### Supported ClickHouse types:
                                `String`, `Int8/16/32/64`, `UInt8/16/32/64`, `Float32/64`,
                                `Date`, `DateTime`, `Boolean`, `UUID`, `FixedString(N)`, `Decimal(p,s)`
                                — and any wrapped in `Nullable(...)`.

                                ### Accepted date formats in data cells:
                                `yyyy-MM-dd`, `MM/dd/yyyy`, `dd.MM.yyyy`, `dd-MM-yyyy`, `dd/MM/yyyy`

                                ### Auto-generated columns:
                                - `<col>_str String` (or `Nullable(String)` if source is `Nullable`) \
                                for every `Date` / `DateTime` column
                                - `operation_dttm DateTime` — import timestamp

                                ### Write strategy:
                                Data is written to **both** ClickHouse replicas (Node1 + Node2).
                                Node1 failure aborts the import; Node2 failure is a warning \
                                (replication ensures eventual consistency).
                                """)
                        .contact(new Contact().name("Dev Team").email("dev@example.com"))
                        .license(new License().name("Apache 2.0")
                                .url("https://www.apache.org/licenses/LICENSE-2.0")))
                .servers(List.of(
                        new Server()
                                .url("http://localhost:" + serverPort)
                                .description("Local Development Server")
                ));
    }
}
