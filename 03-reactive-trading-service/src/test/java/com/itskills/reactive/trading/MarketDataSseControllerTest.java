package com.itskills.reactive.trading;

import com.itskills.reactive.trading.domain.MarketQuote;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
class MarketDataSseControllerTest {

    @Autowired
    private ApplicationContext context;

    private WebTestClient webTestClient;

    @BeforeEach
    void setUp() {
        webTestClient = WebTestClient.bindToApplicationContext(context)
                .configureClient()
                .build();
    }

    @Test
    @DisplayName("Consommation non-bloquante du flux Server-Sent Events (SSE) avec WebTestClient")
    void testStreamQuotesSse() {
        ParameterizedTypeReference<ServerSentEvent<MarketQuote>> typeRef =
                new ParameterizedTypeReference<>() {};

        Flux<ServerSentEvent<MarketQuote>> sseFlux = webTestClient.get()
                .uri("/api/stream/quotes?symbol=BTC-USD&intervalMs=50")
                .accept(MediaType.TEXT_EVENT_STREAM)
                .exchange()
                .expectStatus().isOk()
                .returnResult(typeRef)
                .getResponseBody()
                .take(3); // Récupère les 3 premiers événements puis coupe la connexion proprement

        StepVerifier.create(sseFlux)
                .assertNext(event -> {
                    assertThat(event.event()).isEqualTo("quote-tick");
                    assertThat(event.data()).isNotNull();
                    assertThat(event.data().symbol()).isEqualTo("BTC-USD");
                })
                .assertNext(event -> assertThat(event.data()).isNotNull())
                .assertNext(event -> assertThat(event.data()).isNotNull())
                .thenCancel()
                .verify(Duration.ofSeconds(3));
    }
}
