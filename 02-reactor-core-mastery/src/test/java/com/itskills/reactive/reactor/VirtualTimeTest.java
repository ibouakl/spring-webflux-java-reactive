package com.itskills.reactive.reactor;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.time.Duration;

class VirtualTimeTest {

    @Test
    @DisplayName("VirtualTimeScheduler : avancer 1 heure en quelques millisecondes sans bloquer le thread")
    void testVirtualTimeAcceleration() {
        // Flux qui émet un battement toutes les 10 minutes
        StepVerifier.withVirtualTime(() -> Flux.interval(Duration.ofMinutes(10)).take(6))
                .expectSubscription()
                .expectNoEvent(Duration.ofMinutes(10))
                .thenAwait(Duration.ofMinutes(10))
                .expectNext(0L)
                .thenAwait(Duration.ofMinutes(20))
                .expectNext(1L, 2L)
                .thenAwait(Duration.ofMinutes(30))
                .expectNext(3L, 4L, 5L)
                .verifyComplete();
    }
}
