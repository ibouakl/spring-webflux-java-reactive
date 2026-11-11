package com.itskills.reactive.trading;

import com.itskills.reactive.trading.domain.OrderSide;
import com.itskills.reactive.trading.dto.CreateOrderRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.http.MediaType;
import org.springframework.security.test.context.support.WithMockUser;
import org.springframework.test.web.reactive.server.WebTestClient;

import java.math.BigDecimal;

import static org.springframework.security.test.web.reactive.server.SecurityMockServerConfigurers.springSecurity;

@SpringBootTest
class OrderControllerTest {

    @Autowired
    private ApplicationContext context;

    private WebTestClient webTestClient;

    @BeforeEach
    void setUp() {
        webTestClient = WebTestClient.bindToApplicationContext(context)
                .apply(springSecurity())
                .configureClient()
                .build();
    }

    @Test
    @WithMockUser(roles = "TRADER")
    @DisplayName("Création d'un ordre valide via l'API WebFlux réactive")
    void testCreateOrderSuccess() {
        CreateOrderRequest request = new CreateOrderRequest(
                "ACC-101",
                "ETH-USD",
                OrderSide.BUY,
                BigDecimal.valueOf(3400.0),
                BigDecimal.valueOf(2.0)
        );

        webTestClient.post()
                .uri("/api/orders")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(request)
                .exchange()
                .expectStatus().isCreated()
                .expectBody()
                .jsonPath("$.orderId").isNotEmpty()
                .jsonPath("$.symbol").isEqualTo("ETH-USD")
                .jsonPath("$.status").isEqualTo("PENDING");
    }

    @Test
    @WithMockUser(roles = "TRADER")
    @DisplayName("Validation réactive : rejet avec 400 Bad Request en cas de prix négatif")
    void testCreateOrderValidationFailure() {
        CreateOrderRequest invalidRequest = new CreateOrderRequest(
                "ACC-101",
                "ETH-USD",
                OrderSide.BUY,
                BigDecimal.valueOf(-50.0), // Prix invalide
                BigDecimal.valueOf(1.0)
        );

        webTestClient.post()
                .uri("/api/orders")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(invalidRequest)
                .exchange()
                .expectStatus().isBadRequest()
                .expectBody()
                .jsonPath("$.error").isEqualTo("VALIDATION_FAILED");
    }

    @Test
    @DisplayName("Sécurité réactive : rejet 401 Unauthorized sans authentification")
    void testUnauthorizedWithoutCredentials() {
        CreateOrderRequest request = new CreateOrderRequest(
                "ACC-101",
                "ETH-USD",
                OrderSide.BUY,
                BigDecimal.valueOf(3400.0),
                BigDecimal.valueOf(1.0)
        );

        webTestClient.post()
                .uri("/api/orders")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(request)
                .exchange()
                .expectStatus().isUnauthorized();
    }
}
