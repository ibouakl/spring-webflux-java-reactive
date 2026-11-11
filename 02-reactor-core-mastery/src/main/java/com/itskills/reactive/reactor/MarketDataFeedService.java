package com.itskills.reactive.reactor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Service de simulation de flux financier haute fréquence.
 * Illustre les opérateurs réactifs fondamentaux de Project Reactor.
 */
public class MarketDataFeedService {

    private static final Logger log = LoggerFactory.getLogger(MarketDataFeedService.class);

    public record PriceTick(String symbol, BigDecimal price, long timestamp) {}

    public record AggregatedCandle(String symbol, BigDecimal open, BigDecimal high, BigDecimal low, BigDecimal close, int tickCount) {}

    /**
     * Génère un flux continu de cotations pour un symbole donné.
     */
    public Flux<PriceTick> streamTicks(String symbol, BigDecimal basePrice, Duration interval) {
        AtomicInteger counter = new AtomicInteger();
        return Flux.interval(interval)
                .map(tick -> {
                    double delta = (Math.sin(counter.incrementAndGet() * 0.2) * 2.0);
                    BigDecimal price = basePrice.add(BigDecimal.valueOf(delta)).setScale(2, RoundingMode.HALF_UP);
                    return new PriceTick(symbol, price, System.currentTimeMillis());
                })
                .doOnNext(tick -> log.debug("Tick émis : {}", tick));
    }

    /**
     * Agrège un flux de ticks en chandeliers financiers ("Candles") par fenêtre temporelle ou par nombre d'éléments.
     */
    public Flux<AggregatedCandle> aggregateCandles(Flux<PriceTick> ticks, int batchSize) {
        return ticks.buffer(batchSize)
                .filter(list -> !list.isEmpty())
                .map(this::toCandle);
    }

    private AggregatedCandle toCandle(List<PriceTick> batch) {
        String symbol = batch.getFirst().symbol();
        BigDecimal open = batch.getFirst().price();
        BigDecimal close = batch.getLast().price();
        BigDecimal high = batch.stream().map(PriceTick::price).max(BigDecimal::compareTo).orElse(open);
        BigDecimal low = batch.stream().map(PriceTick::price).min(BigDecimal::compareTo).orElse(open);
        return new AggregatedCandle(symbol, open, high, low, close, batch.size());
    }

    /**
     * Récupère une cotation avec résilience (retry avec backoff exponentiel en cas d'erreur réseau transitoire).
     */
    public Mono<PriceTick> fetchPriceWithRetry(Mono<PriceTick> remoteCall, int maxAttempts, Duration minBackoff) {
        return remoteCall
                .doOnError(err -> log.warn("Échec temporaire de récupération du prix : {}", err.getMessage()))
                .retryWhen(Retry.backoff(maxAttempts, minBackoff)
                        .maxBackoff(Duration.ofSeconds(2))
                        .jitter(0.5)
                        .doBeforeRetry(retrySignal -> log.info("Tentative de retry #{} suite à : {}",
                                retrySignal.totalRetries() + 1, retrySignal.failure().getMessage()))
                )
                .onErrorResume(err -> {
                    log.error("Toutes les tentatives de retry ont échoué. Déclenchement du fallback.");
                    return Mono.just(new PriceTick("FALLBACK", BigDecimal.ZERO, System.currentTimeMillis()));
                });
    }
}
