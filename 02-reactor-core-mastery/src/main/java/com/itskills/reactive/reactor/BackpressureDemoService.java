package com.itskills.reactive.reactor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;

import java.util.function.Consumer;

/**
 * Démonstration des stratégies de Backpressure :
 * Quand le producteur est beaucoup plus rapide que le consommateur.
 */
public class BackpressureDemoService {

    private static final Logger log = LoggerFactory.getLogger(BackpressureDemoService.class);

    /**
     * Stratégie BUFFER : stocke les éléments en mémoire jusqu'à la limite maximale configurée.
     * Si la capacité est dépassée, déclenche une BufferOverflowException.
     */
    public <T> Flux<T> withBufferStrategy(Flux<T> source, int maxSize) {
        return source.onBackpressureBuffer(
                maxSize,
                dropped -> log.warn("Buffer saturé, élément rejeté : {}", dropped)
        );
    }

    /**
     * Stratégie DROP : jette immédiatement les éléments émis tant que le consommateur
     * n'a pas explicitement demandé de nouveaux éléments via request(n).
     */
    public <T> Flux<T> withDropStrategy(Flux<T> source, Consumer<T> onDropCallback) {
        return source.onBackpressureDrop(onDropCallback);
    }

    /**
     * Stratégie LATEST : ne conserve en mémoire que le dernier élément émis si le consommateur
     * est trop lent, écrasant les précédents (idéal pour un ticker boursier temps réel).
     */
    public <T> Flux<T> withLatestStrategy(Flux<T> source) {
        return source.onBackpressureLatest();
    }

    /**
     * Stratégie LIMIT RATE : régule la demande amont par lots (ex: 10 par 10)
     * évitant d'inonder le pipeline.
     */
    public <T> Flux<T> withLimitRate(Flux<T> source, int highTide) {
        return source.limitRate(highTide);
    }
}
