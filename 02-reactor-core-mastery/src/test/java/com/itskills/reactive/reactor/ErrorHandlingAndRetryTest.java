package com.itskills.reactive.reactor;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class ErrorHandlingAndRetryTest {

    private final MarketDataFeedService service = new MarketDataFeedService();

    @Test
    @DisplayName("onErrorResume : bascule transparente sur un flux de secours")
    void testOnErrorResume() {
        Mono<String> failingMono = Mono.error(new IllegalStateException("Service indisponible"));

        Mono<String> recovered = failingMono.onErrorResume(err -> Mono.just("VALEUR_DE_SECOURS"));

        StepVerifier.create(recovered)
                .expectNext("VALEUR_DE_SECOURS")
                .verifyComplete();
    }

    @Test
    @DisplayName("retryWhen : succès après 2 échecs transitoires")
    void testRetrySuccessAfterTransientErrors() {
        AtomicInteger attempts = new AtomicInteger();

        Mono<MarketDataFeedService.PriceTick> remoteCall = Mono.defer(() -> {
            int current = attempts.incrementAndGet();
            if (current < 3) {
                return Mono.error(new RuntimeException("Timeout réseau temporaire (tentative " + current + ")"));
            }
            return Mono.just(new MarketDataFeedService.PriceTick("BTC-USD", BigDecimal.valueOf(96000), System.currentTimeMillis()));
        });

        Mono<MarketDataFeedService.PriceTick> resilientMono = service.fetchPriceWithRetry(remoteCall, 3, Duration.ofMillis(20));

        StepVerifier.create(resilientMono)
                .assertNext(tick -> {
                    assertThat(tick.symbol()).isEqualTo("BTC-USD");
                    assertThat(tick.price()).isEqualByComparingTo("96000");
                })
                .verifyComplete();

        assertThat(attempts.get()).isEqualTo(3);
    }

    @Test
    @DisplayName("retryWhen puis fallback quand toutes les tentatives échouent")
    void testRetryFallbackWhenExhausted() {
        Mono<MarketDataFeedService.PriceTick> alwaysFailing = Mono.error(new RuntimeException("Panne serveur durable"));

        Mono<MarketDataFeedService.PriceTick> result = service.fetchPriceWithRetry(alwaysFailing, 2, Duration.ofMillis(10));

        StepVerifier.create(result)
                .assertNext(tick -> assertThat(tick.symbol()).isEqualTo("FALLBACK"))
                .verifyComplete();
    }
}
