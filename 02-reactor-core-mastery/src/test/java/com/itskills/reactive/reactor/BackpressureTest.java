package com.itskills.reactive.reactor;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class BackpressureTest {

    private final BackpressureDemoService service = new BackpressureDemoService();

    @Test
    @DisplayName("Backpressure : le consommateur contrôle le rythme avec request(n)")
    void testConsumerControlledBackpressure() {
        Flux<Integer> fastProducer = Flux.range(1, 100);

        StepVerifier.create(fastProducer, 0) // Demande initiale = 0
                .expectSubscription()
                .thenRequest(2)
                .expectNext(1, 2)
                .thenRequest(3)
                .expectNext(3, 4, 5)
                .thenCancel()
                .verify();
    }

    @Test
    @DisplayName("onBackpressureDrop : élimine les éléments émis sans demande du consommateur")
    void testOnBackpressureDrop() {
        List<Integer> droppedElements = new ArrayList<>();

        // TestPublisher configuré pour ignorer la demande et tester le comportement de drop
        reactor.test.publisher.TestPublisher<Integer> publisher =
                reactor.test.publisher.TestPublisher.createNoncompliant(
                        reactor.test.publisher.TestPublisher.Violation.REQUEST_OVERFLOW
                );

        Flux<Integer> regulated = service.withDropStrategy(publisher.flux(), droppedElements::add);

        StepVerifier.create(regulated, 0) // Demande initiale = 0
                .then(() -> publisher.next(1, 2, 3)) // Émis sans demande -> ils doivent être jetés
                .thenRequest(1) // Le client demande 1 élément
                .then(() -> publisher.next(100)) // Cet élément est demandé -> accepté
                .expectNext(100)
                .thenCancel()
                .verify();

        // Les éléments 1, 2, 3 émis sans demande ont bien été capturés par le drop callback
        assertThat(droppedElements).containsExactly(1, 2, 3);
    }
}
