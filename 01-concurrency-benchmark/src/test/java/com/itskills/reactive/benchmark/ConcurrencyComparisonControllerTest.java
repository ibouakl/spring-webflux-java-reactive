package com.itskills.reactive.benchmark;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.test.web.reactive.server.WebTestClient;

import static org.assertj.core.api.Assertions.assertThat;

class ConcurrencyComparisonControllerTest {

    private WebTestClient webTestClient;

    @BeforeEach
    void setUp() {
        webTestClient = WebTestClient.bindToController(new ConcurrencyComparisonController()).build();
    }

    @Test
    @DisplayName("L'endpoint réactif doit répondre de manière asynchrone sans bloquer")
    void testReactiveEndpoint() {
        webTestClient.get()
                .uri("/api/benchmark/reactive?delayMs=50")
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.mode").isEqualTo("REACTIVE_NON_BLOCKING")
                .jsonPath("$.thread").value(thread -> assertThat((String) thread).isNotEmpty());
    }

    @Test
    @DisplayName("L'endpoint d'info de thread doit inspecter l'environnement d'exécution Java 25")
    void testThreadInfoEndpoint() {
        webTestClient.get()
                .uri("/api/benchmark/thread-info")
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.availableProcessors").isNumber()
                .jsonPath("$.activeThreadCount").isNumber();
    }
}
