package com.itskills.reactive.trading.service;

import com.itskills.reactive.trading.domain.MarketQuote;
import com.itskills.reactive.trading.repository.MarketQuoteRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ThreadLocalRandom;

@Service
public class MarketDataStreamerService {

    private static final Logger log = LoggerFactory.getLogger(MarketDataStreamerService.class);

    private final MarketQuoteRepository quoteRepository;
    private final WebClient webClient;

    public MarketDataStreamerService(MarketQuoteRepository quoteRepository, WebClient.Builder webClientBuilder) {
        this.quoteRepository = quoteRepository;
        this.webClient = webClientBuilder.baseUrl("https://api.coinbase.com/v2").build();
    }

    /**
     * Flux SSE infini simulant les variations de cotation en direct pour les clients connectés.
     */
    public Flux<MarketQuote> streamLiveQuotes(String symbol, Duration interval) {
        return quoteRepository.findById(symbol)
                .defaultIfEmpty(new MarketQuote(symbol, BigDecimal.valueOf(100.0), BigDecimal.valueOf(99.9), BigDecimal.valueOf(100.1), BigDecimal.valueOf(1000000), Instant.now()))
                .flatMapMany(base -> Flux.interval(interval)
                        .onBackpressureDrop() // Protection Backpressure vitale pour flux infini temps réel
                        .map(tick -> {
                            double delta = (ThreadLocalRandom.current().nextDouble() - 0.5) * 1.5;
                            BigDecimal newPrice = base.lastPrice().add(BigDecimal.valueOf(delta)).setScale(2, RoundingMode.HALF_UP);
                            BigDecimal newBid = newPrice.subtract(BigDecimal.valueOf(0.1)).setScale(2, RoundingMode.HALF_UP);
                            BigDecimal newAsk = newPrice.add(BigDecimal.valueOf(0.1)).setScale(2, RoundingMode.HALF_UP);
                            return new MarketQuote(symbol, newPrice, newBid, newAsk, base.volume24h(), Instant.now());
                        })
                );
    }

    /**
     * Démonstration WebClient non-bloquant : appel vers un service externe sans bloquer l'Event Loop.
     */
    public Mono<String> fetchExternalSpotPrice(String currencyPair) {
        return webClient.get()
                .uri("/prices/{pair}/spot", currencyPair)
                .retrieve()
                .bodyToMono(String.class)
                .timeout(Duration.ofSeconds(2))
                .onErrorResume(err -> {
                    log.warn("WebClient fallback : impossible de contacter l'API externe ({})", err.getMessage());
                    return Mono.just("{\"data\":{\"amount\":\"FALLBACK_OFFLINE\"}}");
                });
    }
}
