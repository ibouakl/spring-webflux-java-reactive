package com.itskills.reactive.trading;

import com.itskills.reactive.trading.domain.Order;
import com.itskills.reactive.trading.domain.OrderSide;
import com.itskills.reactive.trading.repository.OrderRepository;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import reactor.test.StepVerifier;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@Testcontainers
class PostgreSqlR2dbcIntegrationTest {

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine")
            .withDatabaseName("trading_db")
            .withUsername("postgres")
            .withPassword("postgrespassword")
            .withInitScript("schema.sql");

    @DynamicPropertySource
    static void configureR2dbc(DynamicPropertyRegistry registry) {
        registry.add("spring.r2dbc.url", () -> String.format(
                "r2dbc:postgresql://%s:%d/%s",
                postgres.getHost(),
                postgres.getFirstMappedPort(),
                postgres.getDatabaseName()
        ));
        registry.add("spring.r2dbc.username", postgres::getUsername);
        registry.add("spring.r2dbc.password", postgres::getPassword);
        registry.add("spring.sql.init.mode", () -> "never");
    }

    @Autowired
    private OrderRepository orderRepository;

    @Test
    @DisplayName("Testcontainers & R2DBC : Persistance et lecture asynchrone dans un vrai PostgreSQL conteneurisé")
    void testPostgreSqlPersistenceWithR2dbc() {
        Order order = Order.createNew(
                "ORD-TC-001",
                "ACC-TESTCONTAINERS",
                "NVDA",
                OrderSide.BUY,
                BigDecimal.valueOf(140.50),
                BigDecimal.valueOf(10.0)
        );

        StepVerifier.create(orderRepository.save(order))
                .assertNext(saved -> {
                    assertThat(saved.id()).isNotNull();
                    assertThat(saved.orderId()).isEqualTo("ORD-TC-001");
                })
                .verifyComplete();

        StepVerifier.create(orderRepository.findByOrderId("ORD-TC-001"))
                .assertNext(found -> {
                    assertThat(found.symbol()).isEqualTo("NVDA");
                    assertThat(found.price()).isEqualByComparingTo("140.50");
                })
                .verifyComplete();
    }
}
