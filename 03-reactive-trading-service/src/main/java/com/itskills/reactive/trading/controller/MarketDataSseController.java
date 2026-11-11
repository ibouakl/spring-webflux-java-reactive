package com.itskills.reactive.trading.controller;

import com.itskills.reactive.trading.domain.MarketQuote;
import com.itskills.reactive.trading.domain.Trade;
import com.itskills.reactive.trading.service.MarketDataStreamerService;
import com.itskills.reactive.trading.service.TradingEngineService;
import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

import java.time.Duration;

@RestController
@RequestMapping("/api/stream")
public class MarketDataSseController {

    private final MarketDataStreamerService marketDataStreamerService;
    private final TradingEngineService tradingEngineService;

    public MarketDataSseController(MarketDataStreamerService marketDataStreamerService,
                                   TradingEngineService tradingEngineService) {
        this.marketDataStreamerService = marketDataStreamerService;
        this.tradingEngineService = tradingEngineService;
    }

    /**
     * Streaming SSE standard : text/event-stream
     * Les navigateurs web peuvent consommer ce flux directement via EventSource() en JavaScript.
     */
    @GetMapping(value = "/quotes", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<ServerSentEvent<MarketQuote>> streamQuotesSse(
            @RequestParam(defaultValue = "BTC-USD") String symbol,
            @RequestParam(defaultValue = "500") long intervalMs) {

        return marketDataStreamerService.streamLiveQuotes(symbol, Duration.ofMillis(intervalMs))
                .map(quote -> ServerSentEvent.<MarketQuote>builder()
                        .event("quote-tick")
                        .data(quote)
                        .build()
                );
    }

    /**
     * Streaming NDJSON : application/x-ndjson
     * Flux d'objets JSON séparés par des retours à la ligne (optimal pour le flux de microservice à microservice).
     */
    @GetMapping(value = "/trades", produces = MediaType.APPLICATION_NDJSON_VALUE)
    public Flux<Trade> streamTradesNdJson(@RequestParam(required = false) String symbol) {
        return tradingEngineService.streamTrades(symbol);
    }
}
