package com.itskills.reactive.reactor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.math.BigDecimal;
import java.util.UUID;

/**
 * Pipeline de traitement d'ordres illustrant la gestion fine des Schedulers
 * et la règle d'isolation du code bloquant :
 * - subscribeOn() : impacte l'endroit où la source est exécutée.
 * - publishOn()   : impacte tous les opérateurs situés en aval (downstream).
 */
public class OrderProcessingPipeline {

    private static final Logger log = LoggerFactory.getLogger(OrderProcessingPipeline.class);

    public record RawOrder(String accountId, String symbol, BigDecimal price, BigDecimal quantity) {}
    public record ValidatedOrder(String orderId, String accountId, String symbol, BigDecimal price, BigDecimal quantity, long timestamp) {}

    /**
     * Valide et enrichit un ordre.
     */
    public Mono<ValidatedOrder> validateOrder(RawOrder raw) {
        return Mono.just(raw)
                .filter(o -> o.price() != null && o.price().compareTo(BigDecimal.ZERO) > 0)
                .filter(o -> o.quantity() != null && o.quantity().compareTo(BigDecimal.ZERO) > 0)
                .switchIfEmpty(Mono.error(new IllegalArgumentException("Prix et quantité doivent être strictement positifs")))
                .map(o -> new ValidatedOrder(
                        UUID.randomUUID().toString(),
                        o.accountId(),
                        o.symbol(),
                        o.price(),
                        o.quantity(),
                        System.currentTimeMillis()
                ));
    }

    /**
     * Simule un appel de persistance ou d'audit hérité (bloquant)
     * et l'isole sur le Scheduler boundedElastic pour ne jamais impacter l'Event Loop.
     */
    public Mono<ValidatedOrder> saveWithBlockingLegacySystem(ValidatedOrder order, Scheduler blockingScheduler) {
        return Mono.fromCallable(() -> {
            log.info("Appel bloquant hérité exécuté sur : {}", Thread.currentThread().getName());
            // Simulation I/O bloquant
            Thread.sleep(20);
            return order;
        }).subscribeOn(blockingScheduler);
    }

    /**
     * Chaîne complète avec bascule de threads via publishOn().
     */
    public Flux<ValidatedOrder> processBatchWithSchedulers(Flux<RawOrder> rawFlux) {
        return rawFlux
                // Étape 1 : validation CPU pure (parallélisable)
                .flatMap(this::validateOrder)
                // Étape 2 : Bascule sur boundedElastic pour l'opération I/O bloquante
                .publishOn(Schedulers.boundedElastic())
                .flatMap(order -> saveWithBlockingLegacySystem(order, Schedulers.boundedElastic()))
                // Étape 3 : Bascule sur parallel pour calculs statistiques en aval
                .publishOn(Schedulers.parallel())
                .doOnNext(saved -> log.info("Traitement final sur thread : {}", Thread.currentThread().getName()));
    }
}
